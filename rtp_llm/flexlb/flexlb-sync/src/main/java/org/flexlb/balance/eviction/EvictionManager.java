package org.flexlb.balance.eviction;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.eviction.DecodePreemptionCoordinator.PreemptionResult;
import org.flexlb.balance.endpoint.DecodeEndpoint.DecodeRequestView;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.RequestRepository;
import org.flexlb.balance.scheduler.RequestRoute;
import org.flexlb.balance.scheduler.BalanceContext;
import org.flexlb.balance.scheduler.BalanceContext.AdmissionHandle;
import org.flexlb.util.Failures;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import static org.flexlb.dao.loadbalance.Response.buildErrorResponse;
import org.flexlb.balance.scheduler.RequestRequirements;
import org.flexlb.balance.scheduler.RequestRequirements.DecodeMode;
import org.flexlb.config.PreemptionConfig;
import org.flexlb.config.VictimStage;
import org.flexlb.enums.DecodeTaskPhase;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter.CancelEvent;
import org.flexlb.service.monitor.RequestSchedulerReporter.EvictionEvent;
import org.flexlb.util.Logger;
import org.flexlb.util.PriorityNormalizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Obtains Decode capacity through local withdrawal or the Engine cancellation protocol. */
@Component
public class EvictionManager {

    private final RequestSchedulerReporter reporter;

    private final EngineCancelChannel cancelChannel;

    private final DecodePreemptionCoordinator preemptionCoordinator;

    private final RequestRepository requests;

    private volatile boolean shutdown;

    @Autowired
    public EvictionManager(RequestSchedulerReporter reporter, EngineCancelChannel cancelChannel, DecodePreemptionCoordinator preemptionCoordinator, RequestRepository requests) {
        this.reporter = reporter;
        this.cancelChannel = cancelChannel;
        this.preemptionCoordinator = preemptionCoordinator;
        this.requests = Objects.requireNonNull(requests, "requests");
    }

    @PreDestroy
    public void shutdown() {
        shutdown = true;
    }

    /**
     * The caller retains its admission handle throughout this operation and consumes
     * any returned reservation. Null means no takeover occurred, including a local
     * victim conflict; an Engine attempt always returns its eventual terminal result.
     */
    public CompletableFuture<PreemptionResult> tryReserve(
            BalanceContext ctx, RequestRequirements request, WorkerEndpoint blockedEndpoint) {
        if (shutdown || ctx.getFuture().isDone()
                || ctx.requestExpired(System.currentTimeMillis())
                || !PriorityNormalizer.hasPriority(request.priority())
                || request.mode() != DecodeMode.PREEMPT_AT_PLACEMENT) {
            return null;
        }
        PreemptionConfig preemption = ctx.getConfig().isPriorityOrdering() ? ctx.getConfig().priorityOrdering().getPreemption() : null;
        if (preemption == null
                || !(blockedEndpoint instanceof DecodeEndpoint decodeEndpoint)
                || (!preemption.allows(VictimStage.DECODE_RESERVED)
                && !preemption.allows(VictimStage.DECODE_ENGINE_OWNED))) {
            return null;
        }
        DecodeEvictionProposal proposal = planDecodeEviction(request, preemption, decodeEndpoint);
        if (proposal == null || !ctx.scheduler().isAdmissionOpen(request.requestId(), ctx.getFuture())) {
            return null;
        }
        if (proposal.requiresEngineCancel()) {
            return startEngineCancelPreemption(ctx, preemption, proposal, decodeEndpoint, request);
        }
        List<DecodeEndpoint.ReservationHandle> victims = new ArrayList<>(proposal.victims().size());
        for (DecodeRequestView victim : proposal.victims()) {
            victims.add(new DecodeEndpoint.ReservationHandle(
                    decodeEndpoint.getStatus().getGenerationId(), victim.requestId(), victim.reservationToken()));
        }
        DecodeEndpoint.ReservationHandle incoming = replaceQueuedDecodeReservations(
                decodeEndpoint, victims, request.requestId(), request.hardKvTokens(),
                request.expectedKvTokens(), request.priority(), request.capacity());
        if (incoming == null) {
            reportEviction(EvictionEvent.COMMIT, ctx.getPriority(), ctx.getRequestId(), proposal.evictionCase(), "conflict");
            return null;
        }
        report(ctx.getRequestId(), "local preemption", () -> {
            for (DecodeRequestView victim : proposal.victims()) {
                reportRequeuedVictim(ctx, victim, proposal);
            }
            reportEviction(EvictionEvent.COMMIT, ctx.getPriority(), ctx.getRequestId(), proposal.evictionCase(), "success");
            recordDecodePlanObservability(ctx, proposal);
        });
        return CompletableFuture.completedFuture(new PreemptionResult(incoming, false, "committed"));
    }

    /** Metrics observe plans and commits without owning the reservation transaction. */
    private void reportEviction(EvictionEvent event, int priority, long requestId,
                                String evictionCase, String outcome) {
        report(requestId, event == EvictionEvent.PLAN ? "eviction plan" : "eviction commit",
                () -> reporter.reportEviction(event, priority, evictionCase, outcome));
    }

    // ==================== Decode eviction ====================
    /**
     * Build one side-effect-free plan from one exact cluster snapshot.
     */
    private DecodeEvictionProposal planDecodeEviction(
            RequestRequirements request,
            PreemptionConfig preemption,
            DecodeEndpoint selectedEndpoint) {
        DecodeEndpoint.ResourceSnapshot selected = selectedEndpoint.resourceSnapshot();
        if (selectedEndpoint.isRetired()) {
            return null;
        }
        String evictionCase = EvictionPlanner.decodeEvictionCase(
                request, selected);
        if (evictionCase == null) {
            return null;
        }

        Map<String, String> failures = new HashMap<>();
        DecodeEvictionProposal proposal = EvictionPlanner.planDecode(
                request, selected, preemption, preemption.allows(VictimStage.DECODE_ENGINE_OWNED)
                        && cancelChannel != null && cancelChannel.isSupported(selectedEndpoint), failures);
        if (proposal == null) {
            reportEviction(EvictionEvent.PLAN, request.priority(), request.requestId(),
                    evictionCase, "infeasible");
            Logger.debug(
                    "[eviction-manager] Decode eviction infeasible:"
                            + " request_id={} priority={} worker={} reasons={}",
                    request.requestId(), request.priority(), selected.routing().address(), failures);
            return null;
        }
        reportEviction(EvictionEvent.PLAN, request.priority(), request.requestId(),
                proposal.evictionCase(), "feasible");
        return proposal;
    }

    /**
     * Observability only: the registry owns non-terminal withdrawal and requeue.
     */
    private void reportRequeuedVictim(BalanceContext ctx, DecodeRequestView victim,
                                     DecodeEvictionProposal proposal) {
        String stage = "decode_reserved";
        report(ctx.getRequestId(), "requeued victim", () -> {
            reporter.reportVictim(victim.priority(), ctx.getPriority(),
                    stage, proposal.evictionCase());
            reporter.reportVictimKvTokens(
                    victim.priority(), stage, victim.kvTokens());
        });
        Logger.debug(
                "[eviction-manager] decode victim preempted: victim_id={} victim_priority={}"
                    + " stage={} outcome={} kv_tokens={} incoming_id={} incoming_priority={}"
                    + " worker={}",
                victim.requestId(),
                victim.priority(),
                stage,
                "requeued",
                victim.kvTokens(),
                ctx.getRequestId(),
                ctx.getPriority(),
                proposal.endpointId());
    }

    /**
     * Record the single committed Decode-eviction plan.
     */
    private static void recordDecodePlanObservability(BalanceContext ctx,
                                                      DecodeEvictionProposal proposal) {
        long totalCost = proposal.priorityHarmProfile().totalCost();
        ctx.setPlanType("decode_evict");
        ctx.setPlanCost(totalCost);
        ctx.setVictimCount(proposal.victims().size());
        Logger.debug(
                "[eviction-manager] decode eviction committed: request_id={} priority={} case={} "
                        + "victims={} total_cost={} freed_kv={} worker={}",
                ctx.getRequestId(),
                ctx.getPriority(),
                proposal.evictionCase(),
                proposal.victims().size(),
                totalCost,
                proposal.freedKvTokens(),
                proposal.endpointId());
    }

    private CompletableFuture<PreemptionResult> startEngineCancelPreemption(
            BalanceContext ctx, PreemptionConfig preemption, DecodeEvictionProposal proposal,
            DecodeEndpoint endpoint, RequestRequirements request) {
        var command = new DecodePreemptionCoordinator.PreemptionCommand(
                endpoint, request, proposal.victims(), 50L,
                preemption.getTimeoutMs(),
                () -> ctx.scheduler().isAdmissionOpen(request.requestId(), ctx.getFuture()),
                "preempted by higher-priority request " + ctx.getRequestId());
        reportCancelRequests(ctx, proposal);
        return preemptionCoordinator.preempt(command).whenComplete((result, error) ->
                report(ctx.getRequestId(), "engine preemption", () -> {
                    if (error != null || result == null || result.controlFailure()) {
                        reportCancelTimeout(ctx, proposal.endpointId());
                    } else if (result.committed()) {
                        reportCommittedEnginePreemption(ctx, proposal);
                        recordDecodePlanObservability(ctx, proposal);
                    }
                }));
    }

    /**
     * Metrics never participate in the committed reservation handoff.
     */
    private void reportCommittedEnginePreemption(
            BalanceContext ctx, DecodeEvictionProposal proposal) {
        report(ctx.getRequestId(), "committed preemption", () -> {
            for (DecodeRequestView victim : proposal.victims()) {
                String stage = victim.phase() == DecodeTaskPhase.RUNNING
                        ? "decode_running" : "decode_cancel";
                reporter.reportVictim(victim.priority(), ctx.getPriority(),
                        stage, proposal.evictionCase());
                reporter.reportVictimKvTokens(
                        victim.priority(), stage, victim.kvTokens());
                reporter.reportEngineCancel(CancelEvent.CONFIRM,
                        proposal.endpointId(), victim.priority());
            }
            reporter.reportEviction(EvictionEvent.COMMIT, ctx.getPriority(),
                    proposal.evictionCase(), "success");
        });
    }

    private void reportCancelRequests(BalanceContext ctx,
                                      DecodeEvictionProposal proposal) {
        report(ctx.getRequestId(), "cancel requests", () -> {
            for (DecodeRequestView victim : proposal.victims()) {
                reporter.reportEngineCancel(CancelEvent.REQUEST,
                        proposal.endpointId(), victim.priority());
                reporter.reportCancel(
                        victim.priority(), "PRIORITY_PREEMPTED");
            }
        });
    }

    private void reportCancelTimeout(BalanceContext ctx, String endpointId) {
        report(ctx.getRequestId(), "cancel timeout", () -> {
            reporter.reportEngineCancel(CancelEvent.TIMEOUT, endpointId, ctx.getPriority());
        });
    }

    private static void report(long requestId, String operation, Runnable metrics) {
        try {
            metrics.run();
        } catch (Throwable failure) {
            Logger.warn("[eviction-manager] failed to report {}: request_id={}",
                    operation, requestId, failure);
        }
    }

    public DecodeEndpoint.ReservationHandle replaceQueuedDecodeReservations(DecodeEndpoint endpoint, List<DecodeEndpoint.ReservationHandle> victims, long incomingRequestId, long hardKv, long expectedKv, int priority, DecodeEndpoint.AdmissionCapacity capacity) {
        List<AdmissionHandle> claimed = new ArrayList<>(victims.size());
        DecodeEndpoint.ReservationHandle incoming = null;
        try {
            for (DecodeEndpoint.ReservationHandle victim : victims) {
                var owner = requests.ownerOf(victim.requestId());
                AdmissionHandle withdrawal = owner == null ? null : owner.claimQueuedRoute(endpoint, victim, priority);
                if (withdrawal == null) {
                    return null;
                }
                claimed.add(withdrawal);
            }
            incoming = endpoint.replaceQueuedRequests(victims, incomingRequestId, hardKv, expectedKv, priority, capacity);
            return incoming;
        } finally {
            Throwable failure = null;
            for (AdmissionHandle withdrawal : claimed) {
                boolean committed = incoming != null;
                failure = Failures.run(failure, () -> withdrawal.owner().scheduler().completeWithdrawal(withdrawal, committed));
            }
            if (failure != null) {
                if (incoming != null) {
                    endpoint.release(incoming, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
                }
                Failures.rethrow(failure, "request cleanup failed");
            }
        }
    }

}
