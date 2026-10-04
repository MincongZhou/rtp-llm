package org.flexlb.balance.eviction;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.DecodeEndpoint.DecodeRequestView;
import org.flexlb.balance.preemption.CancelTarget;
import org.flexlb.balance.preemption.PreemptionCancelPhase;
import org.flexlb.balance.preemption.VictimTerminal;
import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.PreemptionRegistration;
import org.flexlb.balance.scheduler.RequestRepository;
import org.flexlb.balance.scheduler.RequestRequirements;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Executes one Engine-Cancel preemption transaction.
 *
 * <p>The scheduler supplies a pure plan and consumes one result.  This class
 * owns the two-phase protocol, token fencing and exactly-once child settlement.
 * Engine acknowledgement is only control evidence; the canonical victim
 * terminal transaction may complete before or after that acknowledgement.</p>
 */
@Component
public final class DecodePreemptionCoordinator implements AutoCloseable {

    public record PreemptionResult(
            DecodeEndpoint.ReservationHandle reservation, boolean controlFailure, String detail) {
        public boolean committed() { return reservation != null; }

        public PreemptionResult {
            if (reservation != null && controlFailure) {
                throw new IllegalArgumentException(
                        "committed preemption cannot be a control failure");
            }
            Objects.requireNonNull(detail, "detail");
        }
    }

    record PreemptionCommand(
            DecodeEndpoint endpoint,
            RequestRequirements request,
            List<DecodeRequestView> victims,
            long cancelAckTimeoutMs,
            long preemptionTimeoutMs,
            BooleanSupplier admissionOpen,
            String detail) {
        public PreemptionCommand {
            if (endpoint == null || victims == null || victims.isEmpty()) {
                throw new IllegalArgumentException("endpoint and victims are required");
            }
            victims = List.copyOf(victims);
            Objects.requireNonNull(request, "request");
            if (request.requestId() <= 0L) {
                throw new IllegalArgumentException(
                        "incoming request id must be positive");
            }
            Set<Long> victimIds = new LinkedHashSet<>();
            for (DecodeRequestView victim : victims) {
                if (victim.requestId() <= 0L
                        || victim.reservationToken() <= 0L) {
                    throw new IllegalArgumentException(
                            "victim requestId and reservation token must be positive");
                }
                if (victim.phase() == null
                        || !victim.phase().requiresEngineCancel()) {
                    throw new IllegalArgumentException(
                            "coordinator accepts only Engine-Cancel victims");
                }
                if (!victimIds.add(victim.requestId())) {
                    throw new IllegalArgumentException(
                            "duplicate victim " + victim.requestId());
                }
            }
            if (admissionOpen == null) {
                throw new IllegalArgumentException("admission gate is required");
            }
        }
    }

    private final EngineCancelChannel cancelChannel;
    private final RequestRepository requests;
    private final java.util.concurrent.ScheduledExecutorService timer;
    private final boolean ownsTimer;
    private final AtomicLong tokenSequence = new AtomicLong(1);

    @org.springframework.beans.factory.annotation.Autowired
    public DecodePreemptionCoordinator(EngineCancelChannel cancelChannel, RequestRepository requests,
                                       org.flexlb.balance.scheduler.SchedulerRuntime runtime) {
        this(cancelChannel, requests, runtime.cleanupExecutor(), false);
    }

    DecodePreemptionCoordinator(EngineCancelChannel cancelChannel, RequestRepository requests) {
        this(cancelChannel, requests, new java.util.concurrent.ScheduledThreadPoolExecutor(1,
                Thread.ofPlatform().daemon().name("flexlb-preemption-control-", 1).factory()), true);
    }

    private DecodePreemptionCoordinator(EngineCancelChannel cancelChannel, RequestRepository requests,
                                        java.util.concurrent.ScheduledExecutorService timer, boolean ownsTimer) {
        this.cancelChannel = Objects.requireNonNull(cancelChannel, "cancelChannel");
        this.requests = Objects.requireNonNull(requests, "requests");
        this.timer = Objects.requireNonNull(timer, "timer");
        this.ownsTimer = ownsTimer;
    }

    @Override
    @javax.annotation.PreDestroy
    public void close() { if (ownsTimer) { timer.shutdown(); } }

    CompletableFuture<PreemptionResult> preempt(
            PreemptionCommand command) {
        long token = nextToken();
        List<CancelTarget> targets =
                new ArrayList<>(command.victims().size());
        List<DecodeEndpoint.ReservationHandle> victimReservations =
                new ArrayList<>(command.victims().size());
        long endpointGenerationId =
                command.endpoint().getStatus().getGenerationId();
        for (DecodeRequestView victim : command.victims()) {
            var owner = requests.ownerOf(victim.requestId());
            Optional<CancelTarget> target = owner == null ? Optional.empty() : owner.findCancelTarget(
                    victim.requestId(), victim.reservationToken());
            if (target.isEmpty()) {
                return CompletableFuture.completedFuture(new PreemptionResult(
                        null, true,
                        "cancel_owner_missing:" + victim.requestId()));
            }
            targets.add(target.get());
            victimReservations.add(new DecodeEndpoint.ReservationHandle(
                    endpointGenerationId,
                    victim.requestId(),
                    victim.reservationToken()));
        }

        AttemptCapability capability = new AttemptCapability(command, token);
        try {
            for (int index = 0; index < command.victims().size(); index++) {
                DecodeRequestView victim = command.victims().get(index);
                var owner = requests.ownerOf(victim.requestId());
                Optional<PreemptionRegistration> claimAttempt = owner == null ? Optional.empty() : owner.tryClaim(
                        victim.requestId(), victim.reservationToken(),
                        token, command.detail());
                if (claimAttempt.isEmpty()) {
                    return CompletableFuture.completedFuture(capability.abort(
                            false, "victim_inflight_gone"));
                }
                PreemptionRegistration claim = claimAttempt.get();
                ClaimedVictim owned = new ClaimedVictim(victimReservations.get(index), targets.get(index), claim);
                capability.claims.add(owned);
                if (claim.requestId() != victim.requestId()
                        || claim.attemptToken() != token) {
                    return CompletableFuture.completedFuture(capability.abort(
                            true,
                            "lifecycle_returned_mismatched_claim:" + owned.requestId()));
                }
            }

            DecodeEndpoint.PreemptionBeginResult begin =
                    command.endpoint().beginPreemption(
                            token,
                            victimReservations,
                            command.request().requestId(),
                            command.request().hardKvTokens(),
                            command.request().expectedKvTokens(),
                            command.request().priority(),
                            command.request().capacity());
            if (begin != DecodeEndpoint.PreemptionBeginResult.SUCCESS) {
                return CompletableFuture.completedFuture(capability.abort(
                        begin == DecodeEndpoint.PreemptionBeginResult.ENDPOINT_RETIRED,
                        "begin_" + begin.name().toLowerCase()));
            }
            capability.endpointBegun = true;
            if (!command.endpoint().updatePreemption(token, DecodeEndpoint.PreemptionUpdate.cancelSending())) {
                return CompletableFuture.completedFuture(capability.abort(
                        true,
                        "endpoint_cancel_linearization_failed"));
            }
            for (ClaimedVictim owned : capability.claims) {
                if (!owned.claim.scheduler().updatePreemption(owned.claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT)) {
                    return CompletableFuture.completedFuture(capability.abort(
                            true,
                            "inflight_cancel_linearization_failed:"
                                    + owned.requestId()));
                }
            }
            // Capture every terminal capability before the first outbound
            // side effect. The exact claim remains the only lookup key.
            for (ClaimedVictim owned : capability.claims) {
                owned.terminalCompletion = owned.claim
                        .terminalObservation()
                        .handle((terminal, failure) -> failure == null
                                && capability.recordTerminal(owned, terminal))
                        .toCompletableFuture();
            }

            for (ClaimedVictim owned : capability.claims) {
                owned.acknowledgement = capability.outboundStarted(owned)
                        ? cancel(owned, command.cancelAckTimeoutMs())
                        : CompletableFuture.completedFuture(EngineCancelChannel.CancelAck.FAILED);
            }

            CompletableFuture<PreemptionResult> protocol = CompletableFuture.allOf(
                            capability.claims.stream().map(owned -> owned.acknowledgement)
                                    .toArray(CompletableFuture[]::new))
                    .thenCompose(ignored -> handleAcknowledgements(capability));
            return protocol.handle((result, failure) -> {
                if (failure != null) {
                    return capability.abort(
                            true,
                            "coordinator_continuation_failed:"
                                    + failureDetail(failure));
                }
                return result;
            });
        } catch (RuntimeException | Error failure) {
            return CompletableFuture.completedFuture(capability.abort(
                    true,
                    "coordinator_setup_failed:" + failureDetail(failure)));
        }
    }

    private CompletableFuture<PreemptionResult> handleAcknowledgements(
            AttemptCapability capability) {
        PreemptionCommand command = capability.command;
        // A transport-unknown ACK is not a negative acknowledgement: the
        // Prefill may have installed the intent before the reply was lost.
        // Such a child therefore waits for the canonical victim terminal
        // transaction exactly like an ACCEPTED child.
        List<ClaimedVictim> pendingTerminals = new ArrayList<>();
        boolean hasNotFound = false;

        for (ClaimedVictim owned : capability.claims) {
            if (owned.disposition == ClaimDisposition.TERMINAL) {
                continue;
            }
            EngineCancelChannel.CancelAck outcome = owned.acknowledgement.join();
            switch (outcome) {
                case ACCEPTED -> {
                    boolean transitioned =
                            command.endpoint().updatePreemption(
                                    capability.token,
                                    DecodeEndpoint.PreemptionUpdate.cancelReply(owned.requestId(), PreemptionCancelPhase.CANCEL_REQUESTED))
                            && owned.claim.scheduler().updatePreemption(owned.claim, PreemptionCancelPhase.CANCEL_REQUESTED);
                    if (transitioned) {
                        capability.transferred(owned);
                    } else {
                        capability.transferUnknown(owned);
                    }
                    pendingTerminals.add(owned);
                }
                case NOT_FOUND -> {
                    command.endpoint().updatePreemption(
                            capability.token,
                            DecodeEndpoint.PreemptionUpdate.cancelReply(owned.requestId(), PreemptionCancelPhase.NOT_FOUND_STALE));
                    owned.claim.scheduler().updatePreemption(owned.claim, PreemptionCancelPhase.NOT_FOUND_STALE);
                    capability.transferred(owned);
                    if (owned.disposition != ClaimDisposition.TERMINAL) {
                        hasNotFound = true;
                    }
                }
                case REQUEST_FENCED, REQUEST_CLEANED -> {
                    // Unlike NOT_FOUND, REQUEST_FENCED atomically proves absence
                    // and prevents every racing late Enqueue. It is therefore
                    // a terminal victim proof and contributes freed capacity
                    // to this same transaction.
                    settleRequestFenced(capability, owned);
                }
                case FAILED, UNSUPPORTED -> {
                    capability.transferUnknown(owned);
                    pendingTerminals.add(owned);
                }
            }
        }

        if (pendingTerminals.isEmpty()) {
            return CompletableFuture.completedFuture(
                    capability.finish(hasNotFound));
        }
        // The completion budget begins only after the ACK phase has ended; a
        // 40ms ACK followed by a 100ms cleanup therefore gets the full cleanup
        // window rather than sharing one 50ms deadline.
        CompletableFuture<?>[] terminals = pendingTerminals.stream()
                .map(pending -> pending.terminalCompletion)
                .toArray(CompletableFuture<?>[]::new);
        final boolean ackNotFound = hasNotFound;
        // Only this aggregate wait expires. Individual terminal observers stay
        // live so late worker facts can still settle their exact claims.
        CompletableFuture<Void> settlement = CompletableFuture.allOf(terminals);
        // Resolve on the executor, not the shared CompletableFuture timeout thread:
        // finishing an attempt may acquire endpoint and request locks.
        java.util.concurrent.ScheduledFuture<?> deadline = timer.schedule(() -> settlement.complete(null),
                Math.max(1, command.preemptionTimeoutMs()), TimeUnit.MILLISECONDS);
        settlement.whenComplete((unused, failure) -> deadline.cancel(false));
        return settlement.handle((ignored, failure) -> capability.finish(ackNotFound));
    }

    private void settleRequestFenced(
            AttemptCapability capability,
            ClaimedVictim owned) {
        PreemptionCommand command = capability.command;
        // Endpoint accounting remains the resource-owning CAS. The remaining
        // transitions are exact-token followers, but no WorkerStatus future
        // is required for this stronger proof.
        boolean endpointSettled = command.endpoint().updatePreemption(
                capability.token,
                DecodeEndpoint.PreemptionUpdate.fenced(owned.reservation));
        if (endpointSettled && owned.claim.scheduler().completePreemption(owned.claim, command.detail())) {
            capability.recordTerminal(owned);
        }
    }

    private CompletableFuture<EngineCancelChannel.CancelAck> cancel(
            ClaimedVictim victim,
            long timeoutMs) {
        try {
            CompletableFuture<EngineCancelChannel.CancelAck> stage =
                    cancelChannel.cancel(victim.target, victim.requestId(), CancelReason.PRIORITY_PREEMPTED, timeoutMs);
            if (stage == null) {
                return CompletableFuture.completedFuture(
                        EngineCancelChannel.CancelAck.FAILED);
            }
            return stage.handle((outcome, failure) -> failure == null
                            && outcome != null
                    ? outcome : EngineCancelChannel.CancelAck.FAILED);
        } catch (RuntimeException | Error failure) {
            // The capability marked this victim OUTBOUND before invocation,
            // so close conservatively transfers its exact claims to UNKNOWN.
            return CompletableFuture.completedFuture(
                    EngineCancelChannel.CancelAck.FAILED);
        }
    }

    private static String failureDetail(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return current.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ":" + message);
    }

    private enum ClaimDisposition {
        RELEASABLE,
        OUTBOUND,
        TRANSFERRED,
        TERMINAL
    }

    /** Exact opaque slot claim paired with its immutable endpoint victim. */
    private static final class ClaimedVictim {
        private final DecodeEndpoint.ReservationHandle reservation;
        private final CancelTarget target;
        private final PreemptionRegistration claim;
        private CompletableFuture<Boolean> terminalCompletion;
        private CompletableFuture<EngineCancelChannel.CancelAck> acknowledgement;
        private volatile ClaimDisposition disposition =
                ClaimDisposition.RELEASABLE;

        private ClaimedVictim(
                DecodeEndpoint.ReservationHandle reservation,
                CancelTarget target,
                PreemptionRegistration claim) {
            this.reservation = reservation;
            this.target = target;
            this.claim = claim;
        }

        private long requestId() {
            return reservation.requestId();
        }
    }

    /**
     * The one owner of endpoint admission plus every exact BalanceContext claim.
     * A non-committed close is total: uncertain outbound claims transfer to
     * reconciliation, the incoming endpoint reservation aborts, and only
     * claims which never crossed an outbound boundary are released.
     */
    private final class AttemptCapability implements AutoCloseable {
        private final PreemptionCommand command;
        private final long token;
        private final List<ClaimedVictim> claims = new ArrayList<>();
        private boolean endpointBegun;
        private boolean closed;
        private String cleanupFailure;

        private AttemptCapability(
                PreemptionCommand command, long token) {
            this.command = command;
            this.token = token;
        }

        private synchronized boolean outboundStarted(ClaimedVictim owned) {
            if (owned.disposition == ClaimDisposition.TERMINAL) {
                return false;
            }
            if (owned.disposition != ClaimDisposition.RELEASABLE) {
                throw new IllegalStateException(
                        "Cancel outbound ownership changed request_id="
                                + owned.requestId());
            }
            owned.disposition = ClaimDisposition.OUTBOUND;
            return true;
        }

        private synchronized boolean recordTerminal(
                ClaimedVictim owned,
                VictimTerminal terminal) {
            if (terminal == null
                    || terminal.requestId() != owned.requestId()) {
                return false;
            }
            recordTerminal(owned);
            return true;
        }

        /** Idempotent convergence for terminal facts from either callback. */
        private synchronized void recordTerminal(ClaimedVictim owned) {
            owned.disposition = ClaimDisposition.TERMINAL;
        }

        private synchronized boolean allVictimsTerminal() {
            if (claims.isEmpty()) {
                return false;
            }
            return claims.stream().allMatch(
                    owned -> owned.disposition == ClaimDisposition.TERMINAL);
        }

        private synchronized void transferred(ClaimedVictim owned) {
            if (owned.disposition != ClaimDisposition.TERMINAL) {
                owned.disposition = ClaimDisposition.TRANSFERRED;
            }
        }

        private void transferUnknown(ClaimedVictim owned) {
            if (owned.disposition != ClaimDisposition.OUTBOUND) {
                return;
            }
            command.endpoint().updatePreemption(
                    token,
                    DecodeEndpoint.PreemptionUpdate.cancelReply(owned.requestId(), PreemptionCancelPhase.CANCEL_UNKNOWN));
            owned.claim.scheduler().updatePreemption(owned.claim, PreemptionCancelPhase.CANCEL_UNKNOWN);
            transferred(owned);
        }

        private PreemptionResult finish(boolean hasNotFound) {
            DecodeEndpoint.ReservationHandle incoming = allVictimsTerminal()
                    && command.admissionOpen().getAsBoolean()
                    ? command.endpoint().commitPreemption(token) : null;
            if (incoming != null) {
                beginClose();
                return new PreemptionResult(incoming, false, "committed");
            }
            boolean cleanSingleNotFound = hasNotFound
                    && claims.size() == 1
                    && claims.get(0).disposition != ClaimDisposition.TERMINAL;
            return abort(
                    !cleanSingleNotFound,
                    cleanSingleNotFound
                            ? "cancel_not_found"
                            : "cancel_terminal_unknown");
        }

        private PreemptionResult abort(
                boolean controlFailure, String detail) {
            close();
            String resultDetail = cleanupFailure == null
                    ? detail : detail + ";cleanup_failed=" + cleanupFailure;
            return new PreemptionResult(
                    null, controlFailure, resultDetail);
        }

        @Override
        public void close() {
            if (!beginClose()) {
                return;
            }

            // OUTBOUND means the call may have reached Prefill even if its
            // Java invocation or continuation failed. Transfer before endpoint
            // abort so neither owner can be mistaken for locally releasable.
            for (ClaimedVictim owned : claims) {
                if (owned.disposition == ClaimDisposition.OUTBOUND) {
                    cleanup("transfer_unknown:" + owned.requestId(), () -> transferUnknown(owned));
                }
            }
            if (endpointBegun) {
                cleanup("endpoint_abort", () -> command.endpoint().abortPreemption(token));
            }
            for (ClaimedVictim owned : claims) {
                if (owned.disposition == ClaimDisposition.RELEASABLE) {
                    cleanup("release_claim:" + owned.requestId(), () -> owned.claim.scheduler().releasePreemption(owned.claim));
                }
            }
        }

        private synchronized boolean beginClose() {
            if (closed) {
                return false;
            }
            closed = true;
            return true;
        }

        private void cleanup(String operation, Runnable action) {
            try {
                action.run();
            } catch (RuntimeException | Error failure) {
                if (cleanupFailure == null) {
                    cleanupFailure = operation + ":" + failureDetail(failure);
                }
            }
        }
    }

    private long nextToken() {
        long token = tokenSequence.getAndIncrement();
        if (token <= 0) {
            throw new IllegalStateException("preemption attempt token exhausted");
        }
        return token;
    }
}
