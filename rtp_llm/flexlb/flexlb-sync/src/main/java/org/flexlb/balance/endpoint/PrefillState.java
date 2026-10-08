package org.flexlb.balance.endpoint;

import org.flexlb.balance.eviction.EvictionPlanner;
import org.flexlb.balance.prediction.PrefillBatchFeatures;
import org.flexlb.balance.projection.WorkSnapshot.Phase;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.RequestRoute;
import org.flexlb.dao.loadbalance.AdmissionRejectReason;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.enums.PriorityPreemptionProgress;
import org.flexlb.enums.TaskPhase;
import org.flexlb.util.Failures;
import org.flexlb.util.PriorityNormalizer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.math.LongMath.saturatedAdd;

/**
 * Canonical Prefill request ownership for one worker generation.
 *
 * <p>Every known request id has exactly one {@link RequestEntry}. The worker
 * queue is only an ordered index over entries waiting for worker delivery;
 * an immediate admission owns the same resource record without a queue entry.
 * Callback and Engine progress mutate the same entry instead of moving ownership
 * between containers. Methods ending in {@code Locked} require the caller to
 * hold {@link #ownershipLock()}, shared with the worker queue. Other mutation
 * methods acquire that lock internally. Methods return resource facts; Endpoint
 * executes notifications and request continuations after unlocking.
 */
public final class PrefillState {

    public enum CapacityStatus {
        ACQUIRED,
        CAPACITY_FULL,
        REQUEST_NOT_ACTIVE,
        REQUEST_ALREADY_RESERVED,
        BATCH_ID_ALREADY_RESERVED,
        ENDPOINT_RETIRED
    }

    public record ReservationResult<R extends Reservation>(
            CapacityStatus status,
            R reservation) {
        public ReservationResult {
            Objects.requireNonNull(status, "status");
            checkArgument((status == CapacityStatus.ACQUIRED) == (reservation != null),
                    "only ACQUIRED may carry a reservation");
        }
    }

    /** Request status matched to its exact Prefill route, published after ledger reconciliation. */
    public record PrefillRequestStatus(
            RequestRoute route,
            Kind kind,
            long errorCode) {
        public PrefillRequestStatus {
            Objects.requireNonNull(route, "route");
            Objects.requireNonNull(kind, "kind");
            checkArgument(kind != Kind.ACTIVE || errorCode == 0L,
                    "an active Prefill request status cannot carry an error code");
        }

        public static PrefillRequestStatus active(RequestRoute route) {
            return new PrefillRequestStatus(route, Kind.ACTIVE, 0L);
        }

        public static PrefillRequestStatus terminal(
                RequestRoute route, Kind kind, long errorCode) {
            checkArgument(kind != Kind.ACTIVE, "terminal Prefill request status requires a terminal kind");
            return new PrefillRequestStatus(route, kind, errorCode);
        }

        public enum Kind {
            ACTIVE,
            COMPLETED,
            FAILED,
            PRIORITY_CANCELED
        }
    }

    public record StatusReconciliation(
            List<PrefillRequestStatus> requestStatuses,
            List<BatchCompletion> batchCompletions,
            boolean capacityReleased) {
        public StatusReconciliation {
            requestStatuses = List.copyOf(requestStatuses);
            batchCompletions = List.copyOf(batchCompletions);
        }
    }

    public record BatchCompletion(
            long batchId,
            PrefillBatchFeatures originalFeatures,
            long predictedWorkMs,
            long actualWorkMs,
            boolean successfulCompletion,
            boolean learningEligible) {
        public BatchCompletion {
            Objects.requireNonNull(originalFeatures, "originalFeatures");
        }
    }

    public record Retirement(
            List<RequestRoute> ownedItems,
            List<BatchCompletion> batchCompletions,
            Throwable invariantFailure,
            List<EndpointGenerationLifecycle.HandoffPermit> orphanedHandoffs) {
        public Retirement {
            ownedItems = List.copyOf(ownedItems);
            batchCompletions = List.copyOf(batchCompletions);
            orphanedHandoffs = List.copyOf(orphanedHandoffs);
        }
    }

    public record Stats(
            int locallyOwnedRequests,
            int individuallyOwnedRequests,
            int batchCount,
            long maxObservedAgeMs) {
        public Stats {
            checkArgument(locallyOwnedRequests >= 0
                    && individuallyOwnedRequests >= 0
                    && batchCount >= 0
                    && maxObservedAgeMs >= 0L,
                    "Prefill state stats must be non-negative");
        }
    }

    /** One-shot capability returned when an OPEN admission commits. */
    public static final class CommittedHandoff implements AutoCloseable {
        private final EndpointGenerationLifecycle.HandoffPermit generationHandoff;
        private final WorkCapture precedingWork;

        private CommittedHandoff(
                EndpointGenerationLifecycle.HandoffPermit generationHandoff,
                WorkCapture precedingWork) {
            this.generationHandoff = generationHandoff;
            this.precedingWork = Objects.requireNonNull(precedingWork, "precedingWork");
        }

        /** Other reserved work at the same ownership boundary that committed this admission. */
        public WorkCapture precedingWork() {
            return precedingWork;
        }

        @Override
        public synchronized void close() {
            generationHandoff.close();
        }
    }

    /** Exact preparation capability, consumed by commit or rollback. */
    public abstract static class Reservation {
        final PrefillState owner;
        /* Guarded by owner.lock; null means committed, rolled back or retired. */
        RequestEntry originalOwner;

        private Reservation(PrefillState owner, RequestEntry originalOwner) {
            this.owner = owner;
            this.originalOwner = Objects.requireNonNull(
                    originalOwner, "originalOwner");
        }
    }

    public static final class RouteReservation extends Reservation {
        /* guarded by PrefillState.lock until the reservation commits */
        private final long predictedWorkMs;

        private RouteReservation(PrefillState owner, RequestEntry originalOwner, long predictedWorkMs) {
            super(owner, originalOwner);
            this.predictedWorkMs = Math.clamp(predictedWorkMs, 0L, (long) Integer.MAX_VALUE);
        }
    }

    public static final class BatchReservation extends Reservation {
        private final long batchId;

        private BatchReservation(PrefillState owner, RequestEntry originalOwner,
                                 long batchId,
                                 EndpointGenerationLifecycle.HandoffPermit generationHandoff) {
            super(owner, originalOwner);
            this.batchId = batchId;
            this.generationHandoff = Objects.requireNonNull(
                    generationHandoff, "generationHandoff");
        }

        /* guarded by PrefillState.lock; non-null only while OPEN */
        private EndpointGenerationLifecycle.HandoffPermit generationHandoff;

        public long batchId() {
            return batchId;
        }

        /** Commit this exact batch lease while the caller holds ownershipLock(). */
        public CommittedHandoff commitLocked(
                List<RequestRoute> items,
                long predictedMs) {
            return owner.commitBatchLocked(this, items, predictedMs);
        }
    }

    /** Shared execution estimate referenced by every live member of one batch. */
    private static final class BatchWork {
        private final long batchId;
        private final Set<RequestEntry> members;
        private final long originalPredictionMs;
        private final PrefillBatchFeatures originalFeatures;
        private long remainingWorkMs;
        private Phase servicePhase = Phase.COMMITTED;
        /** A later queued observation cannot make started batch work repackable. */
        private boolean executionStarted;
        private long phaseBaseMs;
        private long lastObservedAtMs;
        private long maxExecutionTimeMs;
        private boolean successfulCompletion;
        private boolean learningEligible = true;

        private BatchWork(long batchId, Set<RequestEntry> members,
                          long predictedWorkMs,
                          PrefillBatchFeatures originalFeatures,
                          long nowMs) {
            checkArgument(batchId >= 0L, "batchId must be non-negative");
            checkArgument(predictedWorkMs >= 0L, "predicted batch work must be non-negative");
            this.batchId = batchId;
            this.members = members;
            this.originalPredictionMs = predictedWorkMs;
            this.originalFeatures = originalFeatures;
            this.remainingWorkMs = predictedWorkMs;
            this.phaseBaseMs = nowMs;
            this.lastObservedAtMs = nowMs;
        }

        private long remainingAt(long nowMs) {
            if (servicePhase != Phase.ENGINE_RUNNING) {
                return remainingWorkMs;
            }
            return Math.max(0L, remainingWorkMs - Math.max(0L, nowMs - phaseBaseMs));
        }

        private void observeTerminal(TerminalObservation terminal, long nowMs) {
            touch(nowMs);
            executionStarted |= terminal.workerObserved
                    && (terminal.errorCode == 0L || terminal.executionTimeMs > 0L);
            maxExecutionTimeMs = Math.max(maxExecutionTimeMs, terminal.executionTimeMs);
            successfulCompletion |= terminal.workerObserved
                    && terminal.errorCode == 0L;
            learningEligible &= terminal.workerObserved
                    && terminal.errorCode == 0L;
        }

        private void touch(long nowMs) {
            lastObservedAtMs = Math.max(lastObservedAtMs, nowMs);
        }

        private void observePhase(Phase phase, long nowMs) {
            remainingWorkMs = remainingAt(nowMs);
            phaseBaseMs = Math.max(phaseBaseMs, nowMs);
            touch(nowMs);
            executionStarted |= phase == Phase.ENGINE_RUNNING;
            servicePhase = phase;
        }

        /** Retirement is external termination and must never train prediction. */
        private BatchCompletion retirementCompletion() {
            return new BatchCompletion(
                    batchId,
                    originalFeatures,
                    originalPredictionMs,
                    maxExecutionTimeMs,
                    successfulCompletion,
                    false);
        }
    }

    /** Waiting-index membership is independent of admission and Engine ownership. */
    private enum QueueMembership { UNINDEXED, WAITING, STOP_DETACHED }

    /** Canonical resource facts for one exact route. Guarded by {@link #lock}. */
    private static final class RequestEntry {
        private QueueMembership queueMembership;
        private final RequestRoute item;
        private Phase individualPhase;
        private long remainingWorkMs;
        private long phaseBaseMs;
        private BatchWork batchWork;
        private Reservation reservation;
        private RequestEntry(RequestRoute item, QueueMembership queueMembership) {
            this.queueMembership = queueMembership;
            this.item = Objects.requireNonNull(item, "item");
        }

        private boolean isActive() {
            return individualPhase == null && batchWork == null;
        }

        private boolean activeIdentity(RequestRoute item) {
            return isActive() && this.item == item
                    && queueMembership != QueueMembership.STOP_DETACHED;
        }

        private void commitIndividual(long predictedMs, long nowMs) {
            if (!isActive()) {
                throw new IllegalStateException(
                        "request is not an ACTIVE route request_id=" + item.requestId());
            }
            remainingWorkMs = Math.clamp(predictedMs, 0L, (long) Integer.MAX_VALUE);
            phaseBaseMs = nowMs;
            queueMembership = QueueMembership.UNINDEXED;
            individualPhase = Phase.COMMITTED;
            reservation = null;
        }

        private void commitBatch(BatchWork work) {
            if (!isActive()) {
                throw new IllegalStateException(
                        "request is not an ACTIVE batch member request_id=" + item.requestId());
            }
            batchWork = work;
            reservation = null;
            queueMembership = QueueMembership.UNINDEXED;
        }

        private void observeIndividualPhase(Phase next, long nowMs) {
            checkArgument(next == Phase.ENGINE_QUEUED || next == Phase.ENGINE_RUNNING, "invalid Engine phase %s", next);
            if (batchWork != null || individualPhase == null) {
                throw new IllegalStateException(
                        "request is not individual request_id=" + item.requestId());
            }
            remainingWorkMs = remainingWorkAt(nowMs);
            phaseBaseMs = Math.max(phaseBaseMs, nowMs);
            individualPhase = next;
        }

        private long remainingWorkAt(long nowMs) {
            if (individualPhase != Phase.ENGINE_RUNNING) {
                return remainingWorkMs;
            }
            long elapsedMs = Math.max(0L, nowMs - phaseBaseMs);
            return Math.max(0L, remainingWorkMs - elapsedMs);
        }
    }

    public record ProjectionVersion(
            long queue,
            long schedulingInputs,
            long ownership) {
    }

    /** Queue and committed work captured at one ownership linearization point. */
    public record Snapshot(ProjectionVersion version, long capturedAtMs,
                           PrefillActiveIndex.Capture active,
                           WorkCapture work) {
        public Snapshot {
            Objects.requireNonNull(active, "missing active queue snapshot");
            Objects.requireNonNull(
                    work, "missing committed work snapshot");
        }
    }

    /** Immutable leaves copied under the ownership lock; projection runs outside it. */
    public static final class WorkCapture {
        private final long capturedAtMs;
        private final List<WorkSnapshot.RequestWork> requests;
        private final List<WorkSnapshot.BatchWork> batches;
        private final long unknownRequestCount;
        private volatile WorkSnapshot materialized;

        private WorkCapture(long capturedAtMs, List<WorkSnapshot.RequestWork> requests,
                            List<WorkSnapshot.BatchWork> batches, long unknownRequestCount) {
            this.capturedAtMs = capturedAtMs;
            this.requests = List.copyOf(requests);
            this.batches = List.copyOf(batches);
            this.unknownRequestCount = unknownRequestCount;
        }

        /** Shared lazy projection; callers never hold the endpoint ownership lock. */
        public WorkSnapshot materialize() {
            WorkSnapshot snapshot = materialized;
            if (snapshot != null) {
                return snapshot;
            }
            synchronized (this) {
                if (materialized == null) {
                    List<WorkSnapshot.RequestWork> orderedRequests = requests.stream()
                            .sorted(Comparator.comparingLong(WorkSnapshot.RequestWork::requestId)).toList();
                    List<WorkSnapshot.BatchWork> orderedBatches = batches.stream()
                            .sorted(Comparator.comparingLong(WorkSnapshot.BatchWork::batchId))
                            .map(batch -> new WorkSnapshot.BatchWork(batch.batchId(),
                                    batch.requestIds().stream().sorted().toList(),
                                    batch.phase(), batch.remainingWorkMs())).toList();
                    materialized = new WorkSnapshot(capturedAtMs, orderedRequests, orderedBatches, unknownRequestCount);
                }
                return materialized;
            }
        }
    }

    /** Terminal proof is bound to the canonical owner before any ledger mutation. */
    private record TerminalObservation(RequestEntry owner,
                                       long executionTimeMs,
                                       long errorCode,
                                       PriorityPreemptionProgress preemptionProgress,
                                       boolean workerObserved) {
        private static TerminalObservation from(RequestEntry owner, WorkerStatus.TaskObservation task) {
            return new TerminalObservation(owner, task.executionTimeMs(), task.errorCode(),
                    task.priorityPreemptionProgress(), true);
        }

        private static TerminalObservation external(RequestEntry owner) {
            return new TerminalObservation(owner, -1L, 0L, PriorityPreemptionProgress.NONE, false);
        }

        private TerminalObservation merge(TerminalObservation other) {
            checkArgument(owner == other.owner, "cannot merge different terminal owners");
            return new TerminalObservation(owner,
                    Math.max(executionTimeMs, other.executionTimeMs),
                    errorCode != 0L ? errorCode : other.errorCode,
                    PriorityPreemptionProgress.merge(preemptionProgress, other.preemptionProgress),
                    workerObserved || other.workerObserved);
        }
    }

    private final ReentrantLock lock;
    /** Non-owning index containing only ACTIVE RequestRoute identities. */
    private PrefillActiveIndex activeIndex;
    /** Canonical request ownership, changed only under the endpoint lock. */
    private final Map<Long, RequestEntry> requests = new HashMap<>();
    private final LongSupplier clock;
    /** Monotonic ownership/work revision used by projection snapshots. */
    private volatile long mutationVersion;
    private volatile long schedulingInputVersion;
    /** Published at ownership mutation boundaries; admission still checks under the lock. */
    private volatile long outstandingRequestCount;
    private long requestCountsVersion = -1;
    private long[] requestCountsByPriority;
    /** Derived immutable work; ACTIVE queue mutations leave committed work unchanged. */
    private WorkCapture committedWorkCapture;
    private long unknownEngineRequestCount;
    private final Map<Long, BatchReservation> preparedBatches = new HashMap<>();
    private final Map<Long, BatchWork> committedBatches = new HashMap<>();

    /** Publish the capacity summary before readers observe a new ownership revision. */
    private void recordMutationLocked() {
        publishRequestCountLocked();
        mutationVersion++;
    }

    private void publishRequestCountLocked() {
        requireLock();
        long count = saturatedAdd(requests.size(), unknownEngineRequestCount);
        if (outstandingRequestCount != count) {
            outstandingRequestCount = count;
        }
    }

    public PrefillState(ReentrantLock lock, PrefillActiveIndex activeIndex) {
        this(lock, activeIndex, System::currentTimeMillis);
    }

    public PrefillState(ReentrantLock lock, PrefillActiveIndex activeIndex,
                        LongSupplier clock) {
        this.lock = Objects.requireNonNull(lock, "lock");
        this.activeIndex = Objects.requireNonNull(activeIndex, "activeIndex");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Shared lock for atomic queue validation, delivery commit and condition waits. */
    public ReentrantLock ownershipLock() { return lock; }

    /** Enable waiting work without replacing the ledger of existing DIRECT reservations. */
    void enableQueueLocked(Comparator<RequestRoute> ordering) {
        requireLock();
        if (activeIndex == PrefillActiveIndex.disabled()) {
            activeIndex = PrefillActiveIndex.ordered(16, ordering);
            schedulingInputsChangedLocked();
            recordMutationLocked();
        }
    }

    /** One bounded, ordered queue view; all metadata belongs to the same ownership revision. */
    public record QueueSnapshot(long queueVersion, long schedulingInputVersion, List<RequestRoute> items) {
        public RequestRoute head() {
            return items.isEmpty() ? null : items.get(0);
        }
    }

    /** Diagnostic counters captured from one queue ownership revision. */
    public record QueueCounters(long version, int[] byPriority, long observedRequests, int batchSlots) { }

    public QueueCounters captureQueueCounters() {
        lock.lock();
        try {
            int[] byPriority = new int[PriorityNormalizer.MAX_PRIORITY + 1];
            for (int priority = 0; priority < byPriority.length; priority++) {
                byPriority[priority] = activeIndex.size(priority);
            }
            return new QueueCounters(activeIndex.version(), byPriority,
                    saturatedAdd(requests.size(), unknownEngineRequestCount), (preparedBatches.size() + committedBatches.size()));
        } finally {
            lock.unlock();
        }
    }

    /** Current queue depth for the endpoint's status view. */
    public int queueDepth() {
        lock.lock();
        try {
            return activeIndex.size();
        } finally {
            lock.unlock();
        }
    }

    public QueueSnapshot captureQueue(int limit) {
        checkArgument(limit > 0, "queue capture limit must be positive");
        lock.lock();
        try {
            List<RequestRoute> items = new ArrayList<>(Math.min(limit, activeIndex.size()));
            var ordered = activeIndex.iterator();
            while (items.size() < limit && ordered.hasNext()) {
                items.add(ordered.next());
            }
            return new QueueSnapshot(activeIndex.version(), schedulingInputVersion(), List.copyOf(items));
        } finally {
            lock.unlock();
        }
    }

    long schedulingInputVersion() { return schedulingInputVersion; }

    public void schedulingInputsChangedLocked() {
        requireLock();
        schedulingInputVersion++;
    }

    private boolean removeRequestLocked(long requestId, RequestEntry entry) {
        requireLock();
        if (!requests.remove(requestId, entry)) {
            return false;
        }
        if (entry.queueMembership == QueueMembership.UNINDEXED) {
            committedWorkCapture = null;
        }
        return true;
    }

    /** Advisory check; callers recheck under ownershipLock before replacing a projection. */
    public boolean isCurrentProjection(ProjectionVersion version) {
        return version.queue() == activeIndex.version()
                && version.schedulingInputs() == schedulingInputVersion
                && version.ownership() == mutationVersion;
    }

    /** Advisory revision read for the lock-free projection-cache fast path. */
    public long mutationVersion() {
        return mutationVersion;
    }

    public boolean enqueueActiveLocked(RequestRoute item, long maxOutstandingRequests) {
        requireLock();
        if (requests.containsKey(item.requestId())
                || (maxOutstandingRequests > 0L && !canAcceptRequestLocked(maxOutstandingRequests))) {
            return false;
        }
        RequestEntry entry = new RequestEntry(item, QueueMembership.WAITING);
        requests.put(item.requestId(), entry);
        try {
            activeIndex.add(item);
        } catch (RuntimeException | Error failure) {
            removeRequestLocked(item.requestId(), entry);
            throw failure;
        }
        recordMutationLocked();
        return true;
    }

    public boolean ownsSelectionLocked(List<RequestRoute> items, long nowMs) {
        requireLock();
        for (RequestRoute item : items) {
            if (!activeIndex.contains(item) || item.requestExpired(nowMs)) {
                return false;
            }
        }
        return true;
    }

    /** Remove an exact, still waiting and unexpired queue owner in the current transaction. */
    public boolean removeQueuedIfUnexpiredLocked(RequestRoute exact, long nowMs) {
        requireLock();
        return exact != null && !exact.requestExpired(nowMs) && removeQueuedLocked(exact);
    }

    /** Finish queue mutations after a committed selection and capture the resulting depth. */
    public SelectionRemainder finishPreparedSelectionLocked(RequestRoute failedMember, long nowMs) {
        requireLock();
        boolean removed = removeQueuedIfUnexpiredLocked(failedMember, nowMs);
        return new SelectionRemainder(activeIndex.size(), removed);
    }

    public record SelectionRemainder(int queueDepth, boolean removedBoundary) { }

    /** The queue part of a worker wait; the worker owns stop and control wakeups. */
    public boolean queueWaitCurrentLocked(
            RequestRoute head, long queueVersion, long inputVersion,
            boolean capacityWait, long nowMs) {
        requireLock();
        if (head == null) {
            return activeIndex.isEmpty();
        }
        if (activeIndex.peek() != head) {
            return false;
        }
        return capacityWait
                ? !head.requestExpired(nowMs)
                : activeIndex.version() == queueVersion && schedulingInputVersion == inputVersion;
    }

    /** Remove exact ACTIVE ownership; batch preparation retains its OPEN lease. */
    public boolean removeQueuedLocked(RequestRoute item) {
        requireLock();
        RequestEntry entry = requests.get(item.requestId());
        if (entry == null || !entry.activeIdentity(item)
                || entry.queueMembership != QueueMembership.WAITING) {
            return false;
        }
        Reservation lease = entry.reservation;
        checkState(lease == null || lease.originalOwner != null,
                "ACTIVE request owns a non-OPEN Prefill lease request_id=%s", item.requestId());
        removeValidatedActiveIndexLocked(item);
        // Batch preparation still owns its OPEN lease and generation handoff.
        removeRequestLocked(item.requestId(), entry);
        recordMutationLocked();
        return true;
    }

    /** Only failed admission needs priority provenance; reuse one summary per revision. */
    public AdmissionRejectReason admissionRejectReason(int priority, long requestLimit) {
        long[] counts;
        long occupiedRequests;
        lock.lock();
        try {
            if (requestCountsByPriority == null || requestCountsVersion != mutationVersion) {
                long[] updatedCounts = new long[PriorityNormalizer.MAX_PRIORITY + 1];
                updatedCounts[0] = unknownEngineRequestCount;
                for (RequestEntry entry : requests.values()) {
                    RequestRoute item = entry.item;
                    int occupant = item.priority();
                    updatedCounts[occupant > 0 && occupant <= PriorityNormalizer.MAX_PRIORITY ? occupant : 0]++;
                }
                requestCountsByPriority = updatedCounts;
                requestCountsVersion = mutationVersion;
            }
            counts = requestCountsByPriority;
            occupiedRequests = outstandingRequestCount;
        } finally {
            lock.unlock();
        }
        long residual = requestLimit > 0 ? Math.max(0L, occupiedRequests + 1L - requestLimit) : 0L;
        long higher = 0L;
        long same = 0L;
        for (int occupant = 1; occupant <= PriorityNormalizer.MAX_PRIORITY; occupant++) {
            if (occupant < priority) { residual = Math.max(0L, residual - counts[occupant]); }
            else if (occupant == priority) { same += counts[occupant]; } else { higher += counts[occupant]; }
        }
        if (residual > higher + same && counts[0] > 0) {
            return AdmissionRejectReason.UNSPECIFIED;
        }
        if (residual > 0 && higher + same >= residual) {
            return higher > 0 ? AdmissionRejectReason.HIGHER_PRIORITY_AHEAD
                    : AdmissionRejectReason.SAME_PRIORITY_AHEAD;
        }
        return AdmissionRejectReason.RESOURCE_EXHAUSTED;
    }

    public boolean canPreemptQueuedRequest(int priority, long requestLimit) {
        lock.lock();
        try {
            long required = requestSlotsToReleaseLocked(requestLimit);
            if (!PriorityNormalizer.hasPriority(priority) || required == 0L || required > activeIndex.size()) {
                return false;
            }
            for (RequestRoute item : activeIndex) {
                if (EvictionPlanner.isLowerPriority(priority, item.priority())
                        && isUncommittedQueuedRequest(item) && --required == 0L) { return true; }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /** Select only uncommitted requests; Engine work cannot release a local queue seat. */
    private List<RequestRoute> queuedPreemptionVictimsLocked(int priority, long requestLimit) {
        requireLock();
        long required = requestSlotsToReleaseLocked(requestLimit);
        if (!PriorityNormalizer.hasPriority(priority) || required == 0L || required > activeIndex.size()) {
            return List.of();
        }
        List<RequestRoute> candidates = new ArrayList<>();
        for (RequestRoute item : activeIndex) {
            if (isUncommittedQueuedRequest(item)) { candidates.add(item); }
        }
        return EvictionPlanner.selectPrefillVictims(candidates, priority, required);
    }

    private boolean isUncommittedQueuedRequest(RequestRoute item) {
        requireLock();
        RequestEntry entry = requests.get(item.requestId());
        return entry != null && entry.activeIdentity(item)
                && entry.queueMembership == QueueMembership.WAITING && entry.reservation == null;
    }

    private long requestSlotsToReleaseLocked(long requestLimit) {
        requireLock();
        return requestLimit <= 0L ? 0L
                : Math.max(0L, saturatedAdd(requests.size(), unknownEngineRequestCount) - requestLimit + 1L);
    }

    /** Replace lower-priority queued owners without exposing a partially transferred set. */
    public List<RequestRoute> replaceQueuedRoutesLocked(RequestRoute incoming, long requestLimit) {
        requireLock();
        if (requests.containsKey(incoming.requestId())) { return List.of(); }
        List<RequestRoute> victims = queuedPreemptionVictimsLocked(incoming.priority(), requestLimit);
        if (victims.isEmpty()) { return victims; }
        // The selected victims fund this seat; the shared lock hides the temporary excess.
        if (!enqueueActiveLocked(incoming, 0L)) { return List.of(); }
        for (RequestRoute victim : victims) {
            checkState(removeQueuedLocked(victim), "queued preemption lost its exact victim");
        }
        return victims;
    }

    /**
     * Detach one exact queue head for the stop callback without discarding its
     * canonical request owner. A failed callback therefore remains visible to
     * generation retirement, while a successful callback must explicitly
     * acknowledge the exact pending identity below.
     */
    public RequestRoute detachNextActiveForStop() {
        lock.lock();
        try {
            RequestRoute item = activeIndex.peek();
            if (item == null) { return null; }
            RequestEntry entry = requests.get(item.requestId());
            checkState(entry != null && entry.activeIdentity(item),
                    "stopped queue head has no canonical ACTIVE owner request_id=%s", item.requestId());
            Reservation lease = entry.reservation;
            checkState(lease == null || lease.originalOwner != null,
                    "stopped ACTIVE request owns a non-OPEN Prefill lease request_id=%s", item.requestId());
            removeValidatedActiveIndexLocked(item);
            entry.queueMembership = QueueMembership.STOP_DETACHED;
            recordMutationLocked();
            return item;
        } finally {
            lock.unlock();
        }
    }

    /** Remove only the exact stop-pending owner whose callback completed. */
    public boolean acknowledgeStopTerminalLocked(RequestRoute item) {
        requireLock();
        RequestEntry entry = requests.get(item.requestId());
        if (entry == null) { return true; }
        if (!entry.isActive() || entry.item != item
                || entry.queueMembership != QueueMembership.STOP_DETACHED
                || activeIndex.contains(item)) {
            return false;
        }
        boolean removed = removeRequestLocked(item.requestId(), entry);
        if (removed) {
            recordMutationLocked();
        }
        return removed;
    }

    ReservationResult<BatchReservation> reserveBatch(
            RequestRoute exactHead,
            long batchId,
            int maximum,
            EndpointGenerationLifecycle.HandoffPermit generationHandoff) {
        requirePositiveBatchLimit(maximum);
        RequestRoute head = exactHead;
        lock.lock();
        try {
            RequestEntry entry = requests.get(head.requestId());
            if (entry == null || !entry.activeIdentity(head)) {
                return new ReservationResult<>(
                        CapacityStatus.REQUEST_NOT_ACTIVE, null);
            }
            if (entry.reservation != null) {
                return new ReservationResult<>(
                        CapacityStatus.REQUEST_ALREADY_RESERVED, null);
            }
            if (preparedBatches.containsKey(batchId) || committedBatches.containsKey(batchId)) {
                return new ReservationResult<>(
                        CapacityStatus.BATCH_ID_ALREADY_RESERVED, null);
            }
            if ((preparedBatches.size() + committedBatches.size()) >= maximum) {
                return new ReservationResult<>(
                        CapacityStatus.CAPACITY_FULL, null);
            }
            BatchReservation lease = new BatchReservation(
                    this, entry, batchId, generationHandoff);
            preparedBatches.put(batchId, lease);
            entry.reservation = lease;
            recordMutationLocked();
            return new ReservationResult<>(CapacityStatus.ACQUIRED, lease);
        } finally {
            lock.unlock();
        }
    }

    public boolean batchCapacityAvailable(int maximum) {
        requirePositiveBatchLimit(maximum);
        lock.lock();
        try {
            return (preparedBatches.size() + committedBatches.size()) < maximum;
        } finally {
            lock.unlock();
        }
    }

    private static void requirePositiveBatchLimit(int maximum) {
        checkArgument(maximum > 0, "maximumInflightBatches must be positive");
    }

    CommittedHandoff commitRouteGroupLocked(
            List<RequestRoute> items,
            List<RouteReservation> exactReservations,
            EndpointGenerationLifecycle.HandoffPermit generationHandoff) {
        requireLock();
        Objects.requireNonNull(generationHandoff, "generationHandoff");
        checkArgument(!exactReservations.isEmpty(), "route commit requires at least one reservation");
        validateGroupLocked(items, false);
        checkArgument(items.size() == exactReservations.size(), "route commit requires one exact lease per member");
        for (int index = 0; index < items.size(); index++) {
            RequestEntry entry = requests.get(items.get(index).requestId());
            RouteReservation lease = exactReservations.get(index);
            checkArgument(lease != null && lease.owner == this, "route reservation belongs to another Prefill ledger");
            if (entry.reservation != lease
                    || lease.originalOwner != entry) {
                throw new IllegalStateException(
                        "route commit does not own exact OPEN lease request_id="
                                + items.get(index).requestId());
            }
        }
        long nowMs = clock.getAsLong();
        CommittedHandoff committedHandoff = new CommittedHandoff(generationHandoff,
                capturePrecedingWorkLocked(items, nowMs));
        // Every exact DIRECT token is validated before any ownership changes.
        for (int index = 0; index < items.size(); index++) {
            RequestRoute item = items.get(index);
            RequestEntry entry = requests.get(item.requestId());
            RouteReservation lease = exactReservations.get(index);
            entry.commitIndividual(lease.predictedWorkMs, nowMs);
            consumeReservationLocked(lease);
        }
        committedWorkCapture = null;
        recordMutationLocked();
        return committedHandoff;
    }

    /** Queued predictions belong to the transaction until exact queue ownership commits. */
    CommittedHandoff commitQueuedRoutesLocked(
            List<RequestRoute> items, long[] predictions,
            EndpointGenerationLifecycle.HandoffPermit generationHandoff) {
        requireLock();
        Objects.requireNonNull(generationHandoff, "generationHandoff");
        checkArgument(items.size() == predictions.length, "route commit requires one prediction per member");
        validateGroupLocked(items, true);
        for (RequestRoute item : items) {
            checkState(requests.get(item.requestId()).reservation == null,
                    "queued route commit cannot consume another preparation request_id=%s", item.requestId());
        }
        long nowMs = clock.getAsLong();
        CommittedHandoff committedHandoff = new CommittedHandoff(generationHandoff, captureWorkLocked(nowMs));
        for (int index = 0; index < items.size(); index++) {
            RequestRoute item = items.get(index);
            removeValidatedActiveIndexLocked(item);
            requests.get(item.requestId()).commitIndividual(predictions[index], nowMs);
        }
        committedWorkCapture = null;
        recordMutationLocked();
        return committedHandoff;
    }

    private CommittedHandoff commitBatchLocked(
            BatchReservation lease,
            List<RequestRoute> items,
            long predictedMs) {
        requireLock();
        validateGroupLocked(items, true);
        RequestEntry head = lease.originalOwner == null ? null
                : requests.get(lease.originalOwner.item.requestId());
        RequestRoute headItem = head == null || !head.isActive() ? null : head.item;
        if (head == null
                || head.reservation != lease
                || !items.contains(headItem)
                || lease.generationHandoff == null) {
            throw new IllegalStateException(
                    "batch commit does not own exact OPEN lease batch_id="
                            + lease.batchId);
        }
        for (RequestRoute item : items) {
            RequestEntry member = requests.get(item.requestId());
            Reservation expected = item == headItem ? lease : null;
            if (member.reservation != expected) {
                throw new IllegalStateException(
                        "batch member owns another exact reservation request_id="
                                + item.requestId());
            }
        }
        long nowMs = clock.getAsLong();
        Set<RequestEntry> members = new HashSet<>(items.size());
        for (RequestRoute item : items) { members.add(requests.get(item.requestId())); }
        BatchWork work = new BatchWork(
                lease.batchId, members,
                predictedMs,
                PrefillBatchFeatures.from(
                        items,
                        RequestRoute::seqLen,
                        RequestRoute::hitCache),
                nowMs);
        CommittedHandoff committedHandoff = new CommittedHandoff(lease.generationHandoff,
                captureWorkLocked(nowMs));
        committedBatches.put(lease.batchId, work);
        for (RequestRoute item : items) {
            removeValidatedActiveIndexLocked(item);
        }
        checkState(preparedBatches.remove(lease.batchId, lease), "batch preparation is not canonical");
        lease.generationHandoff = null;
        consumeReservationLocked(lease);
        for (RequestRoute item : items) {
            requests.get(item.requestId()).commitBatch(work);
        }
        committedWorkCapture = null;
        recordMutationLocked();
        return committedHandoff;
    }

    private void validateGroupLocked(List<RequestRoute> items, boolean queuedOnly) {
        requireLock();
        checkState(!items.isEmpty(), "committed group requires members");
        Set<RequestRoute> unique = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (RequestRoute item : items) {
            checkState(unique.add(item), "duplicate group member request_id=%s", item.requestId());
            RequestEntry entry = requests.get(item.requestId());
            checkState(entry != null && entry.activeIdentity(item),
                    "group member is not canonical ACTIVE request_id=%s", item.requestId());
            if (queuedOnly) {
                checkState(activeIndex.contains(item),
                        "canonical ACTIVE request has no queue index request_id=%s", item.requestId());
            } else {
                checkState(entry.queueMembership == QueueMembership.UNINDEXED && !activeIndex.contains(item),
                        "immediate admission cannot have a queue index request_id=%s", item.requestId());
            }
        }
    }

    private void removeValidatedActiveIndexLocked(RequestRoute item) {
        requireLock();
        boolean removed = activeIndex.remove(item);
        checkState(removed,
                "validated ACTIVE queue index disappeared request_id=%s", item.requestId());
    }

    /**
     * Reserve immediate route work against current ownership in one transaction.
     * Selection revisions are advisory; only exact identity and current capacity
     * decide admission. A zero request limit leaves count admission disabled.
     */
    public ReservationResult<RouteReservation> reserveUnqueuedRoute(
            RequestRoute item, long predictedMs, long maxOutstandingRequests) {
        Objects.requireNonNull(item, "item");
        checkArgument(maxOutstandingRequests >= 0L, "request limit must be non-negative");
        lock.lock();
        try {
            if (requests.containsKey(item.requestId())) {
                return new ReservationResult<>(CapacityStatus.REQUEST_ALREADY_RESERVED, null);
            }
            if (maxOutstandingRequests > 0L && !canAcceptRequestLocked(maxOutstandingRequests)) {
                return new ReservationResult<>(CapacityStatus.CAPACITY_FULL, null);
            }
            RequestEntry entry = new RequestEntry(item, QueueMembership.UNINDEXED);
            RouteReservation reservation = new RouteReservation(this, entry, predictedMs);
            ReservationResult<RouteReservation> result = new ReservationResult<>(CapacityStatus.ACQUIRED, reservation);
            entry.reservation = reservation;
            requests.put(item.requestId(), entry);
            committedWorkCapture = null;
            recordMutationLocked();
            return result;
        } finally {
            lock.unlock();
        }
    }

    enum RequestRelease { NONE, QUEUED, RESERVED, COMMITTED }

    /** Release only this exact request; preparations owned by a batch transaction survive. */
    RequestRelease releaseRequest(RequestRoute exactItem) {
        lock.lock();
        try {
            RequestEntry entry = requests.get(exactItem.requestId());
            if (entry == null || entry.item != exactItem) { return RequestRelease.NONE; }
            if (!entry.isActive()) {
                settleLocked(entry, TerminalObservation.external(entry), clock.getAsLong());
                return RequestRelease.COMMITTED;
            }
            if (entry.queueMembership == QueueMembership.UNINDEXED) {
                checkState(entry.reservation instanceof RouteReservation && entry.reservation.originalOwner != null,
                        "unqueued admission has no preparation request_id=%s", exactItem.requestId());
                closeOpenLeaseLocked(entry.reservation);
                return RequestRelease.RESERVED;
            }
            if (entry.queueMembership == QueueMembership.WAITING) {
                removeValidatedActiveIndexLocked(exactItem);
            }
            // STOP_DETACHED still occupies a request seat. Its callback keeps the exact
            // route independently; successful acknowledgement is idempotent after release.
            removeRequestLocked(exactItem.requestId(), entry);
            recordMutationLocked();
            return RequestRelease.QUEUED;
        } finally { lock.unlock(); }
    }

    public record HeartbeatReconciliation(List<PrefillRequestStatus> requestStatuses,
                                          boolean schedulingInputsChanged, boolean capacityReleased) {
        public HeartbeatReconciliation {
            requestStatuses = List.copyOf(requestStatuses);
        }
    }

    public HeartbeatReconciliation reconcileHeartbeat(WorkerStatus.StatusObservation observation) {
        List<PrefillRequestStatus> requestStatuses = new ArrayList<>(observation.runningTasks().size());
        boolean capacityReleased = false;
        boolean schedulingInputsChanged;
        lock.lock();
        try {
            ActiveObservation active = prepareActiveObservationsLocked(observation.engine(), Map.of(), requestStatuses);
            capacityReleased = active.unknownRequests < unknownEngineRequestCount;
            schedulingInputsChanged = applyActiveObservationsLocked(active, clock.getAsLong());
            if (schedulingInputsChanged) {
                committedWorkCapture = null;
                recordMutationLocked();
            }
        } finally {
            lock.unlock();
        }
        return new HeartbeatReconciliation(requestStatuses, schedulingInputsChanged, capacityReleased);
    }

    private record ActiveObservation(long unknownRequests,
                                     IdentityHashMap<RequestEntry, Phase> individuals,
                                     IdentityHashMap<BatchWork, Phase> batches) { }

    /** Both heartbeat and full status apply the same exact activity observations. */
    private boolean applyActiveObservationsLocked(ActiveObservation active, long nowMs) {
        boolean changed = active.unknownRequests != unknownEngineRequestCount;
        for (var observed : active.individuals.entrySet()) {
            changed |= observed.getKey().individualPhase != observed.getValue();
            observed.getKey().observeIndividualPhase(observed.getValue(), nowMs);
        }
        for (var observed : active.batches.entrySet()) {
            changed |= observed.getKey().servicePhase != observed.getValue();
            observed.getKey().observePhase(observed.getValue(), nowMs);
        }
        unknownEngineRequestCount = active.unknownRequests;
        return changed;
    }

    /** A prepared reduction contains facts only; prediction and publication belong to Endpoint. */
    public static final class StatusReduction {
        private final PrefillState owner;
        private final long version;
        private final long nowMs;
        private final Map<Long, TerminalObservation> terminals;
        private final ActiveObservation active;
        private final Map<BatchWork, BatchOutcome> batches;
        private final Map<Long, List<RequestRoute>> predictionInputs;
        private final StatusReconciliation result;

        private StatusReduction(PrefillState owner, long version, long nowMs,
                                Map<Long, TerminalObservation> terminals, ActiveObservation active,
                                Map<BatchWork, BatchOutcome> batches,
                                Map<Long, List<RequestRoute>> predictionInputs,
                                StatusReconciliation result) {
            this.owner = owner;
            this.version = version;
            this.nowMs = nowMs;
            this.terminals = terminals;
            this.active = active;
            this.batches = batches;
            this.predictionInputs = Map.copyOf(predictionInputs);
            this.result = result;
        }

        public Map<Long, List<RequestRoute>> predictionInputs() { return predictionInputs; }
    }

    private record BatchOutcome(long maxExecutionTimeMs, boolean successfulCompletion,
                                boolean learningEligible, boolean executionStarted) { }

    private ActiveObservation prepareActiveObservationsLocked(
            WorkerStatus.EngineObservation engine,
            Map<Long, TerminalObservation> terminals,
            List<PrefillRequestStatus> activeRequestStatuses) {
        requireLock();
        IdentityHashMap<RequestEntry, Phase> individualPhases = new IdentityHashMap<>();
        IdentityHashMap<BatchWork, Phase> batchPhases = new IdentityHashMap<>();
        Set<Long> unknownDetailed = new HashSet<>();
        Set<Long> knownObserved = new HashSet<>();
        for (WorkerStatus.TaskObservation task : engine.runningTaskList().values()) {
            if (terminals.containsKey(task.requestId())) {
                continue;
            }
            RequestEntry entry = requests.get(task.requestId());
            if (entry == null || !matchesObservedBatch(entry, task.batchId())) {
                if (!task.isPriorityCancelOverlayOnly()) {
                    unknownDetailed.add(task.requestId());
                }
                continue;
            }
            if (!task.isPriorityCancelOverlayOnly()) {
                knownObserved.add(task.requestId());
                if (!entry.isActive()) {
                    activeRequestStatuses.add(PrefillRequestStatus.active(entry.item));
                }
            }
            if (entry.isActive()) {
                continue;
            }
            Phase observed = task.phase() == TaskPhase.RUNNING
                    ? Phase.ENGINE_RUNNING : Phase.ENGINE_QUEUED;
            if (entry.batchWork == null) {
                individualPhases.put(entry, observed);
            } else {
                batchPhases.merge(
                        entry.batchWork,
                        observed,
                        PrefillState::strongerEnginePhase);
            }
        }
        long reportedActive = saturatedAdd(Math.max(0L, engine.waitingQueryLen()),
                Math.max(0L, engine.runningQueryLen()));
        long scalarUnknown = Math.max(0L, reportedActive - knownObserved.size());
        return new ActiveObservation(Math.max(unknownDetailed.size(), scalarUnknown),
                individualPhases, batchPhases);
    }

    /** Prepare exact identities and aggregate each affected batch once, without effects. */
    public StatusReduction prepareStatusLocked(WorkerStatus.StatusObservation observation) {
        requireLock();
        long nowMs = clock.getAsLong();
        var terminals = terminalObservationsLocked(observation.finishedTasks());
        List<PrefillRequestStatus> requestStatuses = new ArrayList<>(terminals.size()
                + observation.engine().runningTaskList().size());
        Set<BatchWork> changedBatches = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (TerminalObservation terminal : terminals.values()) {
            PrefillRequestStatus requestStatus = terminalRequestStatus(terminal.owner, terminal);
            if (requestStatus != null) { requestStatuses.add(requestStatus); }
            if (terminal.owner.batchWork != null) { changedBatches.add(terminal.owner.batchWork); }
        }
        ActiveObservation active = prepareActiveObservationsLocked(observation.engine(), terminals, requestStatuses);
        Map<BatchWork, BatchOutcome> batches = new IdentityHashMap<>();
        Map<Long, List<RequestRoute>> predictionInputs = new HashMap<>();
        List<BatchCompletion> completions = new ArrayList<>(changedBatches.size());
        for (BatchWork batch : changedBatches) {
            boolean started = batch.executionStarted || active.batches.get(batch) == Phase.ENGINE_RUNNING;
            long maxExecutionTimeMs = batch.maxExecutionTimeMs;
            boolean successful = batch.successfulCompletion;
            boolean eligible = batch.learningEligible;
            List<RequestRoute> survivors = new ArrayList<>(batch.members.size());
            for (RequestEntry member : batch.members) {
                TerminalObservation terminal = terminals.get(member.item.requestId());
                if (terminal == null) {
                    survivors.add(member.item);
                } else {
                    started |= terminal.workerObserved && (terminal.errorCode == 0L || terminal.executionTimeMs > 0L);
                    maxExecutionTimeMs = Math.max(maxExecutionTimeMs, terminal.executionTimeMs);
                    successful |= terminal.workerObserved && terminal.errorCode == 0L;
                    eligible &= terminal.workerObserved && terminal.errorCode == 0L;
                }
            }
            batches.put(batch, new BatchOutcome(maxExecutionTimeMs, successful, eligible, started));
            if (survivors.isEmpty()) {
                completions.add(new BatchCompletion(batch.batchId, batch.originalFeatures,
                        batch.originalPredictionMs, maxExecutionTimeMs, successful, eligible));
            } else if (!started) {
                survivors.sort(Comparator.comparingLong(RequestRoute::enqueueSeq).thenComparingLong(RequestRoute::requestId));
                predictionInputs.put(batch.batchId, List.copyOf(survivors));
            }
        }
        return new StatusReduction(this, mutationVersion, nowMs, terminals, active, batches,
                predictionInputs, new StatusReconciliation(requestStatuses, completions, false));
    }

    /** Null means the out-of-lock prediction was invalidated; no fact has changed. */
    public StatusReconciliation commitStatusLocked(StatusReduction reduction, Map<Long, Long> predictions) {
        requireLock();
        checkArgument(reduction.owner == this, "Status reduction belongs to another State");
        if (reduction.version != mutationVersion) { return null; }
        checkArgument(predictions.keySet().equals(reduction.predictionInputs.keySet()),
                "Predictions do not match the prepared batches");
        for (long prediction : predictions.values()) {
            checkArgument(prediction >= 0L, "Negative batch prediction");
        }
        // Allocate the result before the first ownership mutation.
        boolean capacityReleased = !reduction.terminals.isEmpty()
                || reduction.active.unknownRequests < unknownEngineRequestCount;
        var result = new StatusReconciliation(reduction.result.requestStatuses(),
                reduction.result.batchCompletions(), capacityReleased);
        for (var change : reduction.batches.entrySet()) {
            BatchWork batch = change.getKey();
            BatchOutcome outcome = change.getValue();
            batch.maxExecutionTimeMs = outcome.maxExecutionTimeMs;
            batch.successfulCompletion = outcome.successfulCompletion;
            batch.learningEligible = outcome.learningEligible;
            batch.executionStarted = outcome.executionStarted;
            batch.touch(reduction.nowMs);
        }
        for (TerminalObservation terminal : reduction.terminals.values()) { removeCommittedLocked(terminal.owner); }
        boolean workChanged = applyActiveObservationsLocked(reduction.active, reduction.nowMs)
                || !reduction.terminals.isEmpty() || !predictions.isEmpty();
        for (long batchId : reduction.predictionInputs.keySet()) {
            BatchWork batch = committedBatches.get(batchId);
            batch.remainingWorkMs = predictions.get(batchId);
            batch.phaseBaseMs = reduction.nowMs;
            batch.touch(reduction.nowMs);
        }
        if (workChanged) {
            committedWorkCapture = null;
            recordMutationLocked();
        }
        return result;
    }

    /**
     * End every owner for this retired Prefill generation in one queue-lock
     * transaction. All callback facts and completion DTOs are constructed
     * before the canonical table, queue index, and capacity counters are
     * cleared. Once clearing starts, the remaining operations are allocation-
     * free field updates; no failure can leave a partially retired registry.
     *
     * <p>Ordinarily {@link WorkerBatcher#stopAndAwait()} has already reduced every
     * ACTIVE item. Including a defensively remaining ACTIVE identity here makes
     * endpoint close total if that earlier invariant check failed.</p>
     */
    public Retirement retireGenerationOwnership() {
        List<EndpointGenerationLifecycle.HandoffPermit> orphanedHandoffs = new ArrayList<>();
        Retirement retirement;
        lock.lock();
        try {
            List<RequestRoute> ownedItems = requests.values().stream().map(entry -> entry.item)
                    .sorted(Comparator.comparingLong(RequestRoute::enqueueSeq).thenComparingLong(RequestRoute::requestId)).toList();
            List<BatchCompletion> completions = committedBatches.values().stream()
                    .map(BatchWork::retirementCompletion).sorted(Comparator.comparingLong(BatchCompletion::batchId)).toList();
            Throwable invariantFailure = null;
            for (BatchReservation reservation : preparedBatches.values()) {
                if (reservation.generationHandoff != null) {
                    orphanedHandoffs.add(reservation.generationHandoff);
                }
                invariantFailure = Failures.append(invariantFailure,
                        new IllegalStateException("retirement reached an OPEN Prefill batch lease"));
            }
            retirement = new Retirement(ownedItems, completions, invariantFailure, orphanedHandoffs);
            for (RequestEntry entry : requests.values()) {
                if (entry.reservation != null) { consumeReservationLocked(entry.reservation); }
                entry.reservation = null;
                entry.batchWork = null;
                entry.individualPhase = null;
                entry.queueMembership = QueueMembership.UNINDEXED;
            }
            for (BatchReservation reservation : preparedBatches.values()) {
                reservation.generationHandoff = null;
                consumeReservationLocked(reservation);
            }
            for (BatchWork batch : committedBatches.values()) { batch.members.clear(); }
            requests.clear();
            preparedBatches.clear();
            committedBatches.clear();
            activeIndex.clear();
            unknownEngineRequestCount = 0L;
            committedWorkCapture = null;
            recordMutationLocked();
        } finally {
            lock.unlock();
        }
        return retirement;
    }

    /** Capture exact committed identities before the workflow checks request ownership outside this lock. */
    public List<RequestRoute> cleanupCandidates() {
        lock.lock();
        try { return requests.values().stream().filter(entry -> !entry.isActive()).map(entry -> entry.item).toList(); }
        finally { lock.unlock(); }
    }

    /** One orphan pass: a retained member protects its whole batch. Counts batches and individuals. */
    public int evictExpiredInflight(long ttlMs, Set<RequestRoute> orphanCandidates) {
        int evicted = 0;
        lock.lock();
        try {
            long nowMs = clock.getAsLong();
            long ttl = Math.max(0L, ttlMs);
            for (BatchWork batch : List.copyOf(committedBatches.values())) {
                Set<RequestEntry> members = batch.members;
                boolean retained = nowMs - batch.lastObservedAtMs < ttl;
                for (RequestEntry entry : members) {
                    retained |= !orphanCandidates.contains(entry.item);
                }
                if (retained) { continue; }
                for (RequestEntry entry : List.copyOf(members)) {
                    settleLocked(entry, TerminalObservation.external(entry), nowMs);
                }
                evicted++;
            }
            List<RequestEntry> individuals = new ArrayList<>();
            for (RequestEntry entry : requests.values()) {
                if (entry.batchWork == null && !entry.isActive()
                        && nowMs - entry.phaseBaseMs >= ttl
                        && orphanCandidates.contains(entry.item)) {
                    individuals.add(entry);
                }
            }
            for (RequestEntry entry : individuals) {
                settleLocked(entry, TerminalObservation.external(entry), nowMs);
            }
            evicted += individuals.size();
        } finally {
            lock.unlock();
        }
        return evicted;
    }

    public Stats stats() {
        lock.lock();
        try {
            int locallyOwned = 0;
            int individual = 0;
            long maxAgeMs = 0L;
            long nowMs = clock.getAsLong();
            for (RequestEntry entry : requests.values()) {
                if (entry.isActive()) {
                    continue;
                }
                locallyOwned++;
                if (entry.batchWork == null) {
                    individual++;
                    maxAgeMs = Math.max(
                            maxAgeMs,
                            Math.max(0L, nowMs - entry.phaseBaseMs));
                }
            }
            for (BatchWork batch : committedBatches.values()) {
                maxAgeMs = Math.max(
                        maxAgeMs,
                        Math.max(0L, nowMs - batch.lastObservedAtMs));
            }
            return new Stats(
                    locallyOwned,
                    individual,
                    committedBatches.size(),
                    maxAgeMs);
        } finally {
            lock.unlock();
        }
    }

    /** Advisory selection check; a positive result does not reserve capacity. */
    public boolean canAcceptRequest(long requestLimit) {
        return outstandingRequestCount < requestLimit;
    }

    private boolean canAcceptRequestLocked(long maxOutstandingRequests) {
        requireLock();
        return requests.size() < maxOutstandingRequests
                && unknownEngineRequestCount < maxOutstandingRequests - requests.size();
    }

    public long observedRequestCount() {
        lock.lock();
        try {
            return saturatedAdd(
                    requests.size(),
                    unknownEngineRequestCount);
        } finally {
            lock.unlock();
        }
    }

    Snapshot snapshotLocked() {
        requireLock();
        ProjectionVersion version = new ProjectionVersion(
                activeIndex.version(), schedulingInputVersion, mutationVersion);
        long nowMs = clock.getAsLong();
        return new Snapshot(
                version, nowMs,
                activeIndex.capture(),
                captureWorkLocked(nowMs));
    }

    public WorkSnapshot committedSnapshot() {
        WorkCapture capture;
        lock.lock();
        try {
            capture = captureCurrentWorkLocked(clock.getAsLong(), Set.of());
        } finally {
            lock.unlock();
        }
        return capture.materialize();
    }

    private WorkCapture captureWorkLocked(long nowMs) {
        requireLock();
        if (committedWorkCapture == null || committedWorkCapture.capturedAtMs > nowMs) {
            committedWorkCapture = captureCurrentWorkLocked(nowMs, Set.of());
        }
        return committedWorkCapture;
    }

    private WorkCapture capturePrecedingWorkLocked(List<RequestRoute> members, long nowMs) {
        requireLock();
        Set<RequestEntry> excluded = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (RequestRoute member : members) {
            excluded.add(requests.get(member.requestId()));
        }
        return captureCurrentWorkLocked(nowMs, excluded);
    }

    private WorkCapture captureCurrentWorkLocked(long nowMs, Set<RequestEntry> excluded) {
        requireLock();
        List<WorkSnapshot.RequestWork> individual = new ArrayList<>();
        for (RequestEntry entry : requests.values()) {
            if (excluded.contains(entry) || entry.batchWork != null) { continue; }
            if (!entry.isActive()) {
                individual.add(new WorkSnapshot.RequestWork(entry.item.requestId(), entry.individualPhase,
                        entry.remainingWorkAt(nowMs)));
            } else if (entry.queueMembership == QueueMembership.UNINDEXED
                    && entry.reservation instanceof RouteReservation route) {
                individual.add(new WorkSnapshot.RequestWork(entry.item.requestId(), Phase.COMMITTED, route.predictedWorkMs));
            }
        }
        List<WorkSnapshot.BatchWork> batches = new ArrayList<>(committedBatches.size());
        for (BatchWork batch : committedBatches.values()) {
            List<Long> members = new ArrayList<>(batch.members.size());
            for (RequestEntry member : batch.members) {
                if (!excluded.contains(member)) { members.add(member.item.requestId()); }
            }
            if (!members.isEmpty()) {
                batches.add(new WorkSnapshot.BatchWork(batch.batchId, members, batch.servicePhase,
                        OptionalLong.of(batch.remainingAt(nowMs))));
            }
        }
        return new WorkCapture(nowMs, individual, batches, unknownEngineRequestCount);
    }

    private boolean settleLocked(RequestEntry entry, TerminalObservation terminal, long nowMs) {
        requireLock();
        if (entry.batchWork != null) { entry.batchWork.observeTerminal(terminal, nowMs); }
        removeCommittedLocked(entry);
        committedWorkCapture = null;
        recordMutationLocked();
        return true;
    }

    private void removeCommittedLocked(RequestEntry entry) {
        BatchWork batch = entry.batchWork;
        if (batch != null) {
            checkState(committedBatches.get(batch.batchId) == batch && batch.members.contains(entry),
                    "terminal member is not owned by its batch");
            batch.members.remove(entry);
            if (batch.members.isEmpty()) {
                committedBatches.remove(batch.batchId, batch);
            }
        }
        checkState(entry.reservation == null, "committed request retained a preparation");
        checkState(removeRequestLocked(entry.item.requestId(), entry),
                "terminal request is not canonical request_id=%s", entry.item.requestId());
    }

    private Map<Long, TerminalObservation> terminalObservationsLocked(
            Map<String, WorkerStatus.TaskObservation> finishedTasks) {
        requireLock();
        Map<Long, TerminalObservation> terminals = new HashMap<>();
        for (WorkerStatus.TaskObservation task : finishedTasks.values()) {
            RequestEntry entry = requests.get(task.requestId());
            if (entry == null || entry.isActive() || !matchesObservedBatch(entry, task.batchId())) {
                continue;
            }
            TerminalObservation terminal = TerminalObservation.from(entry, task);
            terminals.merge(
                    task.requestId(), terminal, TerminalObservation::merge);
        }
        return terminals;
    }

    private static PrefillRequestStatus terminalRequestStatus(
            RequestEntry entry,
            TerminalObservation terminal) {
        if (entry == null || entry.isActive()
                || !terminal.workerObserved) {
            return null;
        }
        PrefillRequestStatus.Kind kind = terminal.errorCode == 0L
                ? PrefillRequestStatus.Kind.COMPLETED
                : terminal.preemptionProgress
                        == PriorityPreemptionProgress.CANCELED
                    && terminal.errorCode
                            == StrategyErrorType.PRIORITY_PREEMPTED.getErrorCode()
                ? PrefillRequestStatus.Kind.PRIORITY_CANCELED
                : PrefillRequestStatus.Kind.FAILED;
        return PrefillRequestStatus.terminal(
                entry.item, kind, terminal.errorCode);
    }

    private static boolean matchesObservedBatch(
            RequestEntry entry, long observedBatchId) {
        if (entry.batchWork == null) {
            return true;
        }
        return observedBatchId > 0L
                && entry.batchWork.batchId == observedBatchId;
    }

    private static Phase strongerEnginePhase(Phase left, Phase right) {
        return left == Phase.ENGINE_RUNNING || right == Phase.ENGINE_RUNNING
                ? Phase.ENGINE_RUNNING : Phase.ENGINE_QUEUED;
    }

    /**
     * Roll back only a provisional lease. Once committed, the request table is
     * the sole owner and only its terminal reducer may release the capacity.
     */
    public record PreparationRollback(boolean released, EndpointGenerationLifecycle.HandoffPermit generationHandoff) {
        private static final PreparationRollback UNCHANGED = new PreparationRollback(false, null);
        private static final PreparationRollback ROUTE = new PreparationRollback(true, null);
    }

    /** Consume only open preparation and return any generation capability to its execution owner. */
    public PreparationRollback rollbackPreparation(Reservation reservation) {
        checkArgument(reservation.owner == this, "Preparation belongs to another State");
        checkState(!lock.isHeldByCurrentThread(), "Preparation rollback cannot run under ownershipLock");
        lock.lock();
        try {
            if (reservation.originalOwner == null) { return PreparationRollback.UNCHANGED; }
            var result = reservation instanceof BatchReservation batch
                    ? new PreparationRollback(true, batch.generationHandoff) : PreparationRollback.ROUTE;
            closeOpenLeaseLocked(reservation);
            return result;
        } finally { lock.unlock(); }
    }

    private void consumeReservationLocked(Reservation reservation) {
        requireLock();
        reservation.originalOwner = null;
    }

    /** Rollback returns only resources owned by this uncommitted preparation. */
    private void closeOpenLeaseLocked(Reservation lease) {
        requireLock();
        checkState(lease.originalOwner != null, "Prefill preparation was already consumed");
        RequestEntry owner = openLeaseOwnerLocked(lease);
        checkState(owner == null || owner.isActive(), "preparation has a committed owner");
        if (lease instanceof BatchReservation batch) {
            checkState(preparedBatches.remove(batch.batchId, batch), "batch preparation is not canonical");
            Objects.requireNonNull(batch.generationHandoff, "batch preparation lost its handoff");
            batch.generationHandoff = null;
        }
        consumeReservationLocked(lease);
        if (owner != null) {
            owner.reservation = null;
            if (owner.queueMembership == QueueMembership.UNINDEXED) {
                removeRequestLocked(owner.item.requestId(), owner);
                committedWorkCapture = null;
            }
        }
        recordMutationLocked();
    }

    private RequestEntry openLeaseOwnerLocked(Reservation lease) {
        requireLock();
        RequestEntry originalOwner = lease.originalOwner;
        RequestEntry current = requests.get(originalOwner.item.requestId());
        if (current == originalOwner) {
            if (current.reservation != lease) {
                throw new IllegalStateException(
                        "canonical Prefill lease owner lost its exact reservation"
                                + " request_id=" + originalOwner.item.requestId());
            }
            return current;
        }
        if (current != null && current.reservation == lease) {
            throw new IllegalStateException(
                    "replacement request cannot own an earlier Prefill lease"
                            + " request_id=" + originalOwner.item.requestId());
        }
        return null;
    }

    private void requireLock() {
        checkState(lock.isHeldByCurrentThread(),
                "Prefill ownership requires queueLock");
    }

}
