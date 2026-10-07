package org.flexlb.balance.endpoint;

import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.scheduler.RequestRepository;
import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.config.RoutingConfig;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.DecodeTaskPhase;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

/** Decode worker boundary: lifecycle pins, resource operations and lock-free notifications.
 * DecodeState owns the generation-local resource ledger and its single mutation lock.
 */
public class DecodeEndpoint extends WorkerEndpoint {
    private static final Logger logger = LoggerFactory.getLogger("syncLogger");
    private final DecodeState state;
    private final RequestRepository requests;
    private final PlacementAvailability placementAvailability;
    private final Set<Runnable> engineDispatchCapacityListeners = ConcurrentHashMap.newKeySet();

    // Construction

    private void notifyWorkerFacts(List<WorkerStatusFact> facts) {
        for (WorkerStatusFact fact : facts) {
            var context = requests.findActive(fact.reservation().requestId());
            if (context != null) { context.scheduler().onDecodeStatus(this, List.of(fact)); }
        }
    }

    public DecodeEndpoint(WorkerStatus status, RequestRepository requests) {
        this(status, requests, new PlacementAvailability());
    }

    DecodeEndpoint(WorkerStatus status, RequestRepository requests,
                   PlacementAvailability placementAvailability) {
        super(status);
        this.state = new DecodeState(status);
        this.requests = java.util.Objects.requireNonNull(requests, "requests");
        this.placementAvailability = java.util.Objects.requireNonNull(placementAvailability, "placementAvailability");
    }

    // Reservation and release. Lifecycle pins remain valid for the entire handoff.

    public ReservationHandle reserve(GenerationPin pin, long requestId, long hardKv,
                                     long expectedKv, int priority, AdmissionCapacity capacity) {
        requirePinnedGeneration(pin);
        return state.reserve(requestId, hardKv, expectedKv, priority, true, capacity);
    }

    /** An engine-facing shadow for work already outside the local queue. */
    public ReservationHandle reserveUnqueued(GenerationPin pin, long requestId, long hardKv,
                                             long expectedKv, int priority) {
        requirePinnedGeneration(pin);
        ReservationHandle reservation = state.reserve(requestId, hardKv, expectedKv, priority, false, null);
        if (reservation == null) {
            throw new IllegalStateException("Decode request id is already owned: " + requestId);
        }
        return reservation;
    }

    /** LOCAL_ROLLBACK requires local ownership; other evidence may leave Engine/protocol ownership intact. */
    public ReservationReleaseResult release(ReservationHandle reservation, ReleaseReason reason) {
        ReservationReleaseResult result = state.release(reservation, reason);
        if (result == ReservationReleaseResult.RELEASED) { publishCapacityRelease(); }
        return result;
    }

    /** Select no user outcome here: Prefill rejection alone cannot release Decode ownership. */
    public boolean settleFailedRequest(ReservationHandle reservation, DeliveryResult.Status source) {
        if (source != DeliveryResult.Status.NOT_SENT && source != DeliveryResult.Status.PREFILL_REJECTED) {
            throw new IllegalArgumentException("expected a definite request failure");
        }
        if (reservation == null) { return true; }
        if (source == DeliveryResult.Status.NOT_SENT) { release(reservation, ReleaseReason.NOT_SENT); }
        return !state.hasOwnedResources(reservation);
    }

    public boolean isAcceptedByEngine(ReservationHandle reservation) { return state.isAcceptedByEngine(reservation); }

    public ReservationHandle reservationHandle(long requestId) {
        return isRetired() ? null : state.reservationHandle(requestId);
    }

    public record ReservationHandle(
            long endpointGenerationId,
            long requestId,
            long reservationToken) {

        public ReservationHandle {
            if (endpointGenerationId <= 0L || reservationToken <= 0L) {
                throw new IllegalArgumentException(
                        "Decode reservation identity must be positive");
            }
        }
    }

    public enum ReleaseReason {
        LOCAL_ROLLBACK, COUNTERPART_FINISHED, NOT_SENT, REMOTE_CLEANUP, EXPIRED
    }

    public enum ReservationReleaseResult {
        RELEASED,
        ENGINE_ACCEPTED,
        STILL_OWNED,
        STALE,
        CONFLICT;

        public boolean released() { return this == RELEASED; }
    }

    // Dispatch: acquire capacity, hand over ownership, or return the permit.

    public boolean markQueued(GenerationPin pin, ReservationHandle reservation) {
        requirePinnedGeneration(pin);
        boolean changed = state.markQueued(reservation);
        if (changed) { publishCapacityRelease(); }
        return changed;
    }

    public EngineDispatchPermitAcquisition acquireDispatchPermit(
            ReservationHandle reservation, AdmissionCapacity capacity) {
        java.util.Objects.requireNonNull(reservation, "reservation");
        java.util.Objects.requireNonNull(capacity, "capacity");
        GenerationPin pin = tryPinGeneration();
        if (pin == null) {
            return new EngineDispatchPermitAcquisition(EngineDispatchPermitAcquireStatus.ENDPOINT_RETIRED, null);
        }
        try (pin) {
            DecodeState.DispatchAcquisition acquired = state.acquireDispatchPermit(reservation, capacity);
            return new EngineDispatchPermitAcquisition(acquired.status(), acquired.permit() == null ? null
                    : new EngineDispatchPermit(this, acquired.permit()));
        }
    }

    /** Resolve a permit once. ENGINE_OWNED means handoff, not Worker acceptance. */
    public EngineDispatchPermitTransferStatus dispatch(EngineDispatchPermit permit, DispatchOutcome outcome) {
        java.util.Objects.requireNonNull(permit, "permit");
        java.util.Objects.requireNonNull(outcome, "outcome");
        if (permit.endpoint != this) { throw new IllegalArgumentException("Dispatch permit belongs to another endpoint"); }
        synchronized (permit) {
            if (permit.dispatchResult != null) {
                return outcome == DispatchOutcome.ENGINE_OWNED
                        ? permit.dispatchResult : EngineDispatchPermitTransferStatus.OWNERSHIP_LOST;
            }
            EngineDispatchPermitTransferStatus result;
            GenerationPin pin = outcome == DispatchOutcome.ENGINE_OWNED ? tryPinGeneration() : null;
            if (outcome == DispatchOutcome.ENGINE_OWNED && pin == null) {
                result = EngineDispatchPermitTransferStatus.ENDPOINT_RETIRED;
            } else {
                try (pin) {
                    DecodeState.DispatchResult applied = state.dispatch(permit.lease, outcome);
                    if (applied.capacityReleased()) {
                        if (outcome == DispatchOutcome.ABANDONED) { publishCapacityRelease(); } else { notifyEngineDispatchCapacityListeners(); }
                    }
                    result = applied.status();
                }
            }
            // Returning an unused permit succeeds once, but cannot grant sending ownership later.
            permit.dispatchResult = result == EngineDispatchPermitTransferStatus.TRANSFERRED
                    && outcome == DispatchOutcome.ABANDONED
                    ? EngineDispatchPermitTransferStatus.OWNERSHIP_LOST : result;
            return result;
        }
    }

    /** Lock-free waiter hint, including retirement or lost ownership. Acquisition still rechecks capacity. */
    public boolean shouldRetryDispatch(long requestId, AdmissionCapacity capacity) {
        return isRetired() || state.shouldRetryDispatch(requestId, capacity);
    }

    public static final class EngineDispatchPermit {

        private final DecodeEndpoint endpoint;
        private final DecodeState.DispatchLease lease;
        /** Null until resolved; cached result for dispatch, including ownership lost after release. */
        private EngineDispatchPermitTransferStatus dispatchResult;

        private EngineDispatchPermit(DecodeEndpoint endpoint, DecodeState.DispatchLease lease) {
            this.endpoint = endpoint;
            this.lease = lease;
        }

        /**
         * Transfer this acquired slot without a second capacity check.
         *
         * @return a typed distinction between transfer, prior request-owner
         *         loss, and endpoint-generation retirement
         */
        public EngineDispatchPermitTransferStatus dispatch() {
            return endpoint.dispatch(this, DispatchOutcome.ENGINE_OWNED);
        }

        public boolean release() {
            return endpoint.dispatch(this, DispatchOutcome.ABANDONED)
                    == EngineDispatchPermitTransferStatus.TRANSFERRED;
        }

    }

    public record EngineDispatchPermitAcquisition(
            EngineDispatchPermitAcquireStatus status,
            EngineDispatchPermit permit) {

        public EngineDispatchPermitAcquisition {
            if (status == null) {
                throw new IllegalArgumentException("permit acquisition status is required");
            }
            boolean ownsHandoff = status == EngineDispatchPermitAcquireStatus.ACQUIRED
                    || status == EngineDispatchPermitAcquireStatus.ALREADY_ACCEPTED;
            if (ownsHandoff != (permit != null)) {
                throw new IllegalArgumentException(
                        "only ACQUIRED or ALREADY_ACCEPTED results carry an engine dispatch permit");
            }
        }
    }

    public enum EngineDispatchPermitAcquireStatus {
        /** An exact-reservation permit now owns one Decode hard-gate slot. */
        ACQUIRED,
        /** Engine already owns this reservation; the permit carries identity without charging capacity. */
        ALREADY_ACCEPTED,
        /** Concurrency or Decode KV has no unreserved hard capacity. */
        CAPACITY_FULL,
        /** The request no longer owns a live shadow reservation. */
        NOT_OWNED,
        /** The reservation is already engine-facing rather than Prefill-queued. */
        NOT_QUEUED,
        /** Another pre-delivery attempt already owns this request's permit. */
        ALREADY_ACQUIRED,
        /** This exact Decode endpoint generation no longer accepts delivery. */
        ENDPOINT_RETIRED
    }

    public enum EngineDispatchPermitTransferStatus {
        TRANSFERRED,
        OWNERSHIP_LOST,
        ENDPOINT_RETIRED
    }

    public enum DispatchOutcome { ENGINE_OWNED, ABANDONED }

    public void addEngineDispatchCapacityListener(Runnable listener) {
        if (listener != null) {
            engineDispatchCapacityListeners.add(listener);
        }
    }

    public void removeEngineDispatchCapacityListener(Runnable listener) {
        if (listener != null) {
            engineDispatchCapacityListeners.remove(listener);
        }
    }

    // Preemption: atomic local replacement or remote cancellation.

    public ReservationHandle replaceQueuedRequests(List<ReservationHandle> victims, long incomingRequestId,
                                         long hardKv, long expectedKv, int priority, AdmissionCapacity capacity) {
        GenerationPin pin = tryPinGeneration();
        if (pin == null) { return null; }
        try (pin) {
            ReservationHandle replaced = state.replaceQueuedRequests(victims, incomingRequestId, hardKv, expectedKv, priority, capacity);
            if (replaced != null) { publishCapacityRelease(); }
            return replaced;
        }
    }

    public PreemptionBeginResult beginPreemption(long attemptToken, List<ReservationHandle> victims,
                                                 long incomingRequestId, long hardKv, long expectedKv,
                                                 int priority, AdmissionCapacity capacity) {
        if (attemptToken <= 0 || victims == null || victims.isEmpty()) {
            throw new IllegalArgumentException("attempt token and victims are required");
        }
        GenerationPin pin = tryPinGeneration();
        if (pin == null) { return PreemptionBeginResult.ENDPOINT_RETIRED; }
        try (pin) {
            return state.beginPreemption(attemptToken, victims, incomingRequestId, hardKv, expectedKv, priority, capacity);
        }
    }

    public boolean updatePreemption(long attemptToken, PreemptionUpdate update) {
        boolean changed = reconcilePreemptionResources(attemptToken, update);
        if (changed && update.releasesCapacity()) { publishCapacityRelease(); }
        return changed;
    }

    /** Change exact resource ownership without callbacks; Context publishes the resulting capacity edge after unlocking. */
    public boolean reconcilePreemptionResources(long attemptToken, PreemptionUpdate update) {
        return state.updatePreemption(attemptToken, update);
    }

    public ReservationHandle commitPreemption(long attemptToken) {
        return state.finishPreemption(attemptToken, true);
    }

    public boolean abortPreemption(long attemptToken) {
        boolean aborted = state.finishPreemption(attemptToken, false) != null;
        if (aborted) { publishCapacityRelease(); }
        return aborted;
    }

    public enum PreemptionBeginResult {
        SUCCESS,
        ENDPOINT_RETIRED,
        VICTIM_GONE,
        VICTIM_ALREADY_CLAIMED,
        INVALID_PRIORITY,
        INFEASIBLE,
        INCOMING_ALREADY_RESERVED,
        ATTEMPT_ALREADY_EXISTS
    }

    /** The caller has resolved request protocol; this operation changes only exact resource ownership. */
    public record PreemptionUpdate(Kind kind, ReservationHandle reservation) {
        public enum Kind { CANCEL_HANDED_OFF, CANCELED, REQUEST_FENCED, ACTIVE, FINISHED }

        public PreemptionUpdate {
            java.util.Objects.requireNonNull(kind, "kind");
            java.util.Objects.requireNonNull(reservation, "reservation");
        }
        public static PreemptionUpdate handedOff(ReservationHandle victim) { return new PreemptionUpdate(Kind.CANCEL_HANDED_OFF, victim); }
        public static PreemptionUpdate canceled(ReservationHandle victim) { return new PreemptionUpdate(Kind.CANCELED, victim); }
        public static PreemptionUpdate fenced(ReservationHandle victim) { return new PreemptionUpdate(Kind.REQUEST_FENCED, victim); }
        public static PreemptionUpdate active(ReservationHandle victim) { return new PreemptionUpdate(Kind.ACTIVE, victim); }
        public static PreemptionUpdate finished(ReservationHandle victim) { return new PreemptionUpdate(Kind.FINISHED, victim); }
        boolean releasesCapacity() { return kind != Kind.CANCEL_HANDED_OFF; }
    }

    // Calibration: publish facts only after the resource transaction returns.

    public Runnable applyPreparedStatus(WorkerStatus ws, WorkerStatus.PreparedStatus prepared) {
        requireStatusGeneration(ws);
        if (!prepared.observation().alive()) { beginRetirement(); }
        DecodeState.CalibrationResult result;
        boolean capacityImproved;
        var lock = state.ownershipLock();
        lock.lock();
        try {
            result = state.calibrateLocked(prepared.observation());
            ws.publishPreparedStatus(prepared);
            capacityImproved = DecodeState.placementCapacityImproved(result.before(), state.routingView());
        } catch (RuntimeException | Error failure) {
            beginRetirement();
            throw failure;
        } finally { lock.unlock(); }
        notifyEngineDispatchCapacityListeners();
        if (capacityImproved) { signalPlacementCapacityChanged(); }
        return () -> notifyWorkerFacts(result.facts());
    }

    public Runnable initializeFromPreparedStatus(WorkerStatus ws, WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        state.initialize(observation);
        return () -> { };
    }

    public Runnable observeStatusHeartbeat(WorkerStatus ws, WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        List<WorkerStatusFact> facts = state.observeHeartbeat(observation);
        return () -> notifyWorkerFacts(facts);
    }

    public record WorkerStatusFact(
            Kind kind,
            ReservationHandle reservation,
            long errorCode,
            boolean allocationObserved) {
        public WorkerStatusFact {
            java.util.Objects.requireNonNull(kind, "kind");
            java.util.Objects.requireNonNull(reservation, "reservation");
            if (kind != Kind.TERMINAL && errorCode != 0L) {
                throw new IllegalArgumentException(
                        "only a terminal Decode fact may carry an error code");
            }
        }

        public static WorkerStatusFact active(ReservationHandle reservation) {
            return new WorkerStatusFact(Kind.ACTIVE, reservation, 0L, false);
        }

        public static WorkerStatusFact allocated(ReservationHandle reservation) {
            return new WorkerStatusFact(Kind.ACTIVE, reservation, 0L, true);
        }

        public static WorkerStatusFact terminal(
                ReservationHandle reservation, long errorCode) {
            return new WorkerStatusFact(Kind.TERMINAL, reservation, errorCode, false);
        }

        public enum Kind {
            ACTIVE,
            TERMINAL
        }
    }

    // Read-only resource and capacity views.

    public AdmissionSummary admissionSummary() {
        return state.admissionSummary();
    }

    public DecodeRoutingView routingView() { return state.routingView(); }

    DecodeRoutingView routingViewSnapshot(String address) { return state.routingViewSnapshot(address); }

    public ResourceSnapshot resourceSnapshot() { return state.resourceSnapshot(); }

    public long placementVersion() { return state.placementVersion(); }

    public int getInflightCount() { return state.getInflightCount(); }

    public OptionalLong getLoadMetric() { return OptionalLong.of(state.getTotalLoad()); }

    /** One immutable request table and capacity view captured under the admission lock. */
    public record ResourceSnapshot(DecodeRoutingView routing,
                                   Map<Long, DecodeRequestView> requests,
                                   int queuedCount,
                                   int activeDispatchPermits) {
        public ResourceSnapshot {
            requests = Map.copyOf(requests);
        }

        public int reservedCount() {
            return requests.size() - confirmedCount();
        }

        public int confirmedCount() {
            return phaseCount(DecodeTaskPhase::isEngineConfirmed);
        }

        public int acceptedCount() {
            return phaseCount(phase -> phase == DecodeTaskPhase.ACCEPTED_NOT_RUNNING);
        }

        public int runningCount() {
            return phaseCount(phase -> phase == DecodeTaskPhase.RUNNING);
        }

        public int engineCapacityUsed() {
            return routing.engineCapacityUsed();
        }

        public boolean isQueued(long requestId) {
            DecodeRequestView request = requests.get(requestId);
            return request != null && request.queued();
        }

        public boolean isReserved(long requestId) {
            DecodeRequestView request = requests.get(requestId);
            return request != null && !request.phase().isEngineConfirmed();
        }

        private int phaseCount(Predicate<DecodeTaskPhase> matches) {
            int count = 0;
            for (DecodeRequestView task : requests.values()) {
                if (matches.test(task.phase())) {
                    count++;
                }
            }
            return count;
        }
    }

    public record DecodeRoutingView(
            String address,
            long generationId,
            WorkerStatus.TopologySnapshot topology,
            WorkerStatus.CommittedWorkerStatus workerStatus,
            long admissionVersion,
            int totalLoad,
            int engineLoad,
            CapacityUsage placementUsage,
            CapacityUsage dispatchUsage,
            long inflightHardKv,
            long inflightExpectedKv) {

        public int engineCapacityUsed() { return Math.toIntExact(dispatchUsage.occupiedRequests()); }
        public long realKvUsed() { return placementUsage.expectedKvUsed(); }
        public long realKvAvailable() { return placementUsage.hardKvAvailable(); }
        public long engineFacingKvUsed() { return dispatchUsage.expectedKvUsed(); }
        public long totalKv() { return placementUsage.totalKvTokens(); }

        public DecodeRoutingView {
            java.util.Objects.requireNonNull(address, "address");
            java.util.Objects.requireNonNull(topology, "topology");
            java.util.Objects.requireNonNull(workerStatus, "workerStatus");
            if (generationId <= 0L) {
                throw new IllegalArgumentException(
                        "Decode routing view requires a positive generation");
            }
        }
    }

    public record DecodeRequestView(long requestId,
                                    int priority,
                                    long kvTokens,
                                    long expectedKvTokens,
                                    DecodeTaskPhase phase,
                                    boolean priorityKnown,
                                    long reservationToken,
                                    boolean claimedForPreemption) {
        public boolean queued() { return phase.isMasterQueued(); }

        public CapacityRelease placementRelease() {
            return new CapacityRelease(1L, kvTokens, expectedKvTokens);
        }
    }

    /** Failure-only priority summary, shared until the admission revision changes. */
    public static final class AdmissionSummary {
        private final DecodeRoutingView routing;
        private final CapacityRelease[] placementOccupancy;
        private final CapacityRelease[] engineOccupancy;

        AdmissionSummary(DecodeRoutingView routing, CapacityRelease[] placementOccupancy,
                         CapacityRelease[] engineOccupancy) {
            this.routing = routing;
            this.placementOccupancy = placementOccupancy;
            this.engineOccupancy = engineOccupancy;
        }

        public DecodeRoutingView routing() { return routing; }
        public CapacityRelease placementOccupancy(int priority) { return placementOccupancy[priority]; }
        public CapacityRelease engineOccupancy(int priority) { return engineOccupancy[priority]; }
    }

    public record AdmissionCapacity(
            long maxEngineRequests,
            long maxKvUsagePercent) {

        public AdmissionCapacity {
            if (maxEngineRequests < 0L || maxKvUsagePercent <= 0L
                    || maxKvUsagePercent > RoutingConfig.PERCENTAGE_SCALE) {
                throw new IllegalArgumentException(
                        "Decode admission limits are outside their domain");
            }
        }

        /** Use the same occupancy scope for the observation and every exact victim release. */
        public CapacityDeficit evaluate(CapacityUsage usage, long hardKvTokens, long expectedKvTokens,
                                        CapacityRelease release) {
            java.util.Objects.requireNonNull(usage, "usage");
            java.util.Objects.requireNonNull(release, "release");
            if (hardKvTokens < 0L || expectedKvTokens < hardKvTokens) {
                throw new IllegalArgumentException("Decode demand must satisfy expected >= hard >= 0");
            }
            long requests = maxEngineRequests == 0L ? 0L
                    : shortfall(Math.max(0L, usage.occupiedRequests - release.requests),
                            1L, maxEngineRequests, 0L);
            if (usage.totalKvTokens == 0L) {
                return new CapacityDeficit(requests, 0L, 0L);
            }
            return new CapacityDeficit(requests,
                    shortfall(usage.hardReservedKvTokens, hardKvTokens,
                            usage.availableKvTokens, release.hardKvTokens),
                    shortfall(Math.max(0L, usage.expectedKvUsed - release.expectedKvTokens),
                            expectedKvTokens, kvBudget(usage.totalKvTokens), 0L));
        }

        /** Integer arithmetic never rounds the configured KV budget up. */
        public long kvBudget(long totalKv) {
            if (totalKv < 0L) {
                throw new IllegalArgumentException("negative KV capacity");
            }
            return totalKv / 100L * maxKvUsagePercent + totalKv % 100L * maxKvUsagePercent / 100L;
        }

        private static long shortfall(long used, long incoming, long capacity, long released) {
            long remainingUsed = Math.max(0L, used - released);
            long remainingCapacity = DecodeState.saturatedAddNonNegative(capacity, Math.max(0L, released - used));
            return remainingUsed > remainingCapacity
                    ? DecodeState.saturatedAddNonNegative(remainingUsed - remainingCapacity, incoming)
                    : Math.max(0L, incoming - (remainingCapacity - remainingUsed));
        }
    }

    public record CapacityUsage(long occupiedRequests, long totalKvTokens, long availableKvTokens,
                                long hardReservedKvTokens, long expectedKvUsed) {
        public CapacityUsage {
            if (occupiedRequests < 0L || totalKvTokens < 0L || availableKvTokens < 0L
                    || hardReservedKvTokens < 0L || expectedKvUsed < 0L) {
                throw new IllegalArgumentException("Decode occupancy must be non-negative");
            }
        }

        public long hardKvAvailable() {
            return Math.max(0L, availableKvTokens - hardReservedKvTokens);
        }
    }

    public record CapacityRelease(long requests, long hardKvTokens, long expectedKvTokens) {
        public static final CapacityRelease NONE = new CapacityRelease(0L, 0L, 0L);

        public CapacityRelease {
            if (requests < 0L || hardKvTokens < 0L || expectedKvTokens < hardKvTokens) {
                throw new IllegalArgumentException("invalid Decode capacity release");
            }
        }

        public CapacityRelease plus(CapacityRelease other) {
            return new CapacityRelease(DecodeState.saturatedAddNonNegative(requests, other.requests),
                    DecodeState.saturatedAddNonNegative(hardKvTokens, other.hardKvTokens),
                    DecodeState.saturatedAddNonNegative(expectedKvTokens, other.expectedKvTokens));
        }
    }

    public record CapacityDeficit(long requests, long hardKvTokens, long expectedKvTokens) {
        public boolean fits() { return requests == 0L && !needsKv(); }
        public boolean needsKv() { return hardKvTokens > 0L || expectedKvTokens > 0L; }
        public long kvTokens() { return Math.max(hardKvTokens, expectedKvTokens); }
    }

    // Retirement and orphan cleanup.

    public boolean isRetired() { return isGenerationRetiringOrRetired(); }

    protected void closeEndpoint() {
        List<ReservationHandle> reservations = state.retire();
        try { for (ReservationHandle reservation : reservations) {
            var context = requests.findActive(reservation.requestId());
            if (context != null) { context.scheduler().onDecodeGenerationRetired(this, List.of(reservation)); }
        } }
        finally { notifyEngineDispatchCapacityListeners(); }
    }

    public int evictExpiredRequests(long ttlMs, LongPredicate retainForSchedulerCleanup) {
        var orphanCandidates = state.cleanupCandidates();
        orphanCandidates.keySet().removeIf(retainForSchedulerCleanup::test);
        DecodeState.CleanupResult result = state.evictExpiredRequests(ttlMs, orphanCandidates);
        if (result.capacityReleased()) { publishCapacityRelease(); }
        return result.expiredReservations();
    }

    // Metrics and shared lock-free capacity notifications.

    public void reportBatchMetrics(BatchSchedulerReporter reporter) {
        DecodeState.Stats stats = state.stats();
        reporter.reportDecodeInflight(getIp(), stats.inflight(), stats.totalLoad(),
                stats.expectedKv(), stats.hardKv(), stats.oldestAgeMs());
    }

    public void reportAdmissionMetrics(RequestSchedulerReporter reporter) {
        reporter.reportDecodeAdmission(ipPort(), resourceSnapshot());
    }

    public void publishCapacityRelease() {
        notifyEngineDispatchCapacityListeners();
        signalPlacementCapacityChanged();
    }

    private void notifyEngineDispatchCapacityListeners() {
        for (Runnable listener : engineDispatchCapacityListeners) {
            try {
                listener.run();
            } catch (Throwable listenerFailure) {
                logger.warn("Decode capacity listener failed", listenerFailure);
            }
        }
    }

    private void signalPlacementCapacityChanged() {
        WorkerStatus.TopologySnapshot topology =
                getStatus().topologySnapshot();
        placementAvailability.changed(
                RoleType.DECODE, topology.group(), ipPort());
    }
}
