package org.flexlb.balance.endpoint;

import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.prediction.PrefillBatchFeatures;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.projection.WorkSnapshot.Phase;
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
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * Canonical Prefill request ownership for one worker generation.
 *
 * <p>Every known request id has exactly one {@link RequestEntry}. The worker
 * queue is only an ordered index over entries waiting for worker delivery;
 * an immediate admission owns the same resource record without a queue entry.
 * Callback and Engine progress mutate the same entry instead of moving ownership
 * between containers. All methods which end in {@code UnderLock} require the
 * worker's queue lock, which is the sole Prefill ownership lock.
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

    public record ReservationResult<R extends AutoCloseable>(
            CapacityStatus status,
            R reservation) {
        public ReservationResult {
            Objects.requireNonNull(status, "status");
            if ((status == CapacityStatus.ACQUIRED) != (reservation != null)) {
                throw new IllegalArgumentException(
                        "only ACQUIRED may carry a reservation");
            }
        }
    }

    public record WorkerStatusFact(
            RequestRoute item,
            Kind kind,
            long errorCode) {
        public WorkerStatusFact {
            Objects.requireNonNull(item, "item");
            Objects.requireNonNull(kind, "kind");
            if (kind == Kind.ACTIVE && errorCode != 0L) {
                throw new IllegalArgumentException(
                        "an active Prefill fact cannot carry an error code");
            }
        }

        public static WorkerStatusFact active(RequestRoute item) {
            return new WorkerStatusFact(item, Kind.ACTIVE, 0L);
        }

        public static WorkerStatusFact terminal(
                RequestRoute item, Kind kind, long errorCode) {
            if (kind == Kind.ACTIVE) {
                throw new IllegalArgumentException(
                        "terminal Prefill fact requires a terminal kind");
            }
            return new WorkerStatusFact(item, kind, errorCode);
        }

        public enum Kind {
            ACTIVE,
            COMPLETED,
            FAILED,
            PRIORITY_CANCELED
        }
    }

    public record StatusReconciliation(
            List<WorkerStatusFact> schedulerFacts,
            List<BatchCompletion> batchCompletions,
            Throwable publicationFailure) {
        public StatusReconciliation {
            schedulerFacts = List.copyOf(schedulerFacts);
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
            Throwable invariantFailure) {
        public Retirement {
            ownedItems = List.copyOf(ownedItems);
            batchCompletions = List.copyOf(batchCompletions);
        }
    }

    public record Stats(
            int locallyOwnedRequests,
            int individuallyOwnedRequests,
            int batchCount,
            long maxObservedAgeMs) {
        public Stats {
            if (locallyOwnedRequests < 0 || individuallyOwnedRequests < 0
                    || batchCount < 0 || maxObservedAgeMs < 0L) {
                throw new IllegalArgumentException(
                        "Prefill state stats must be non-negative");
            }
        }
    }

    enum LeaseState {
        OPEN,
        OWNED,
        CLOSED
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

    /** Exact request or batch ownership passed through admission and callback. */
    public abstract class Reservation implements AutoCloseable {
        /* guarded by PrefillState.lock */ LeaseState state =
                LeaseState.OPEN;
        final RequestEntry originalOwner;

        private Reservation(RequestEntry originalOwner) {
            this.originalOwner = Objects.requireNonNull(
                    originalOwner, "originalOwner");
        }

        /** Roll back an OPEN lease. Committed capacity stays Registry-owned. */
        @Override
        public final void close() {
            releaseOpenLease(this);
        }
    }

    public final class RouteReservation extends Reservation {
        private final PrefillState owner = PrefillState.this;
        /* guarded by PrefillState.lock until the reservation commits */
        private long predictedWorkMs;

        private RouteReservation(RequestEntry originalOwner, long predictedWorkMs) {
            super(originalOwner);
            this.predictedWorkMs = boundedPrediction(predictedWorkMs);
        }


    }

    public final class BatchReservation extends Reservation {
        private final long batchId;

        private BatchReservation(RequestEntry originalOwner,
                                 long batchId,
                                 EndpointGenerationLifecycle.HandoffPermit generationHandoff) {
            super(originalOwner);
            this.batchId = batchId;
            this.generationHandoff = Objects.requireNonNull(
                    generationHandoff, "generationHandoff");
        }

        /* guarded by PrefillState.lock; non-null only while OPEN */
        private EndpointGenerationLifecycle.HandoffPermit generationHandoff;

        public long batchId() {
            return batchId;
        }

        /** Atomically commit this exact batch lease. */
        public CommittedHandoff commit(
                List<RequestRoute> items,
                long predictedMs) {
            lock.lock();
            try {
                return commitBatchUnderLock(this, items, predictedMs);
            } finally {
                lock.unlock();
            }
        }
    }

    /** Shared execution estimate referenced by every live member of one batch. */
    private static final class BatchWork {
        private final BatchReservation lease;
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

        private BatchWork(BatchReservation lease,
                          long predictedWorkMs,
                          PrefillBatchFeatures originalFeatures,
                          long nowMs) {
            long batchId = lease.batchId;
            if (batchId < 0L) {
                throw new IllegalArgumentException("batchId must be non-negative");
            }
            if (predictedWorkMs < 0L) {
                throw new IllegalArgumentException(
                        "predicted batch work must be non-negative");
            }
            this.lease = lease;
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
                    lease.batchId,
                    originalFeatures,
                    originalPredictionMs,
                    maxExecutionTimeMs,
                    successfulCompletion,
                    false);
        }
    }

    /** Waiting-index membership is independent of admission and Engine ownership. */
    private enum QueueMembership { UNINDEXED, WAITING, STOP_DETACHED }

    /** The sole mutable request lifecycle record. Guarded by {@link #lock}. */
    private static final class RequestEntry {
        private final long requestId;
        private QueueMembership queueMembership;
        private RequestRoute item;
        private Phase individualPhase;
        private long remainingWorkMs;
        private long phaseBaseMs;
        private BatchWork batchWork;
        private Reservation reservation;
        private RequestEntry(RequestRoute item, QueueMembership queueMembership) {
            this.requestId = item.requestId();
            this.queueMembership = queueMembership;
            this.item = item;
        }

        private boolean isActive() {
            return item != null && individualPhase == null && batchWork == null;
        }

        private boolean activeIdentity(RequestRoute item) {
            return isActive() && this.item == item
                    && queueMembership != QueueMembership.STOP_DETACHED;
        }

        private void commitIndividual(RouteReservation lease, long nowMs) {
            if (!isActive() || reservation != lease) {
                throw new IllegalStateException(
                        "request is not an ACTIVE route request_id=" + requestId);
            }
            remainingWorkMs = lease.predictedWorkMs;
            phaseBaseMs = nowMs;
            queueMembership = QueueMembership.UNINDEXED;
            individualPhase = Phase.COMMITTED;
        }

        private void commitBatch(BatchWork work) {
            if (!isActive()) {
                throw new IllegalStateException(
                        "request is not an ACTIVE batch member request_id=" + requestId);
            }
            batchWork = work;
            reservation = null;
            queueMembership = QueueMembership.UNINDEXED;
        }

        private void observeIndividualPhase(Phase next, long nowMs) {
            if (next != Phase.ENGINE_QUEUED && next != Phase.ENGINE_RUNNING) {
                throw new IllegalArgumentException("invalid Engine phase " + next);
            }
            if (batchWork != null || individualPhase == null) {
                throw new IllegalStateException(
                        "request is not individual request_id=" + requestId);
            }
            remainingWorkMs = individualRemaining(this, nowMs);
            phaseBaseMs = Math.max(phaseBaseMs, nowMs);
            individualPhase = next;
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
            if (owner != other.owner) {
                throw new IllegalArgumentException("cannot merge different terminal owners");
            }
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
    private Map<Long, RequestEntry> requests = new HashMap<>();
    private final LongSupplier clock;
    private final Runnable capacityAvailable;
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
    private int batchLeasesInUse;

    /** Publish the capacity summary before readers observe a new ownership revision. */
    private void recordMutationUnderLock() {
        publishRequestCountUnderLock();
        mutationVersion++;
    }

    private void publishRequestCountUnderLock() {
        requireLock();
        long count = saturatedAdd(requests.size(), unknownEngineRequestCount);
        if (outstandingRequestCount != count) {
            outstandingRequestCount = count;
        }
    }

    public PrefillState(ReentrantLock lock, PrefillActiveIndex activeIndex,
                        Runnable capacityAvailable) {
        this(lock, activeIndex, System::currentTimeMillis, capacityAvailable);
    }

    public PrefillState(ReentrantLock lock, PrefillActiveIndex activeIndex,
                        LongSupplier clock, Runnable capacityAvailable) {
        this.lock = Objects.requireNonNull(lock, "lock");
        this.activeIndex = Objects.requireNonNull(activeIndex, "activeIndex");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.capacityAvailable = Objects.requireNonNull(
                capacityAvailable, "capacityAvailable");
    }

    public ReentrantLock ownershipLock() { return lock; }

    /** Enable waiting work without replacing the ledger of existing DIRECT reservations. */
    void enableQueueUnderLock(Comparator<RequestRoute> ordering) {
        requireLock();
        if (activeIndex == PrefillActiveIndex.disabled()) {
            activeIndex = PrefillActiveIndex.ordered(16, ordering);
            schedulingInputsChangedUnderLock();
            recordMutationUnderLock();
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
                    saturatedAdd(requests.size(), unknownEngineRequestCount), batchLeasesInUse);
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
        if (limit <= 0) {
            throw new IllegalArgumentException("queue capture limit must be positive");
        }
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

    public Runnable capacityAvailableSignal() { return capacityAvailable; }

    long schedulingInputVersion() { return schedulingInputVersion; }

    public void schedulingInputsChangedUnderLock() {
        requireLock();
        schedulingInputVersion++;
    }

    private boolean removeRequestUnderLock(long requestId, RequestEntry entry) {
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

    /** Publish ACTIVE membership and its NON_BATCH lease in one ownership-lock transaction. */
    public boolean enqueueForDeliveryUnderLock(RequestRoute item, long maxOutstandingRequests) {
        requireLock();
        if (!enqueueActiveUnderLock(item, maxOutstandingRequests)) {
            return false;
        }
        boolean published = false;
        try {
            if (item.requiresRouteReservation()) {
                if (item.prefillEp().isGenerationRetiringOrRetired()) {
                    return false;
                }
                reserveRouteUnderLock(item, 0L);
            }
            published = true;
            return true;
        } finally {
            if (!published) {
                requireState(removeQueuedUnderLock(item),
                        "fresh ACTIVE publication could not roll back: request_id=", item.requestId());
            }
        }
    }

    public boolean enqueueActiveUnderLock(RequestRoute item, long maxOutstandingRequests) {
        requireLock();
        if (requests.containsKey(item.requestId())
                || (maxOutstandingRequests > 0L && !canAcceptRequestUnderLock(maxOutstandingRequests))) {
            return false;
        }
        RequestEntry entry = new RequestEntry(item, QueueMembership.WAITING);
        requests.put(item.requestId(), entry);
        try {
            activeIndex.add(item);
        } catch (RuntimeException | Error failure) {
            removeRequestUnderLock(item.requestId(), entry);
            throw failure;
        }
        recordMutationUnderLock();
        return true;
    }

    public boolean ownsSelectionUnderLock(List<RequestRoute> items, long nowMs) {
        requireLock();
        for (RequestRoute item : items) {
            if (!activeIndex.contains(item) || item.requestExpired(nowMs)) {
                return false;
            }
        }
        return true;
    }

    private boolean removeSelectionBoundaryUnderLock(
            RequestRoute item,
            CapacityBoundary boundary,
            long nowMs) {
        requireLock();
        if (item == null
                || boundary.unavailable()
                || boundary == CapacityBoundary.OWNERSHIP_LOST) {
            return false;
        }
        return activeIndex.contains(item)
                && !item.requestExpired(nowMs)
                && removeQueuedUnderLock(item);
    }

    /** Finish the queue part of an already committed delivery under the same lock. */
    public SelectionRemainder finishPreparedSelectionUnderLock(
            RequestRoute blockedItem, CapacityBoundary blockedResult, long nowMs) {
        requireLock();
        boolean removed = removeSelectionBoundaryUnderLock(blockedItem, blockedResult, nowMs);
        return new SelectionRemainder(activeIndex.size(), removed);
    }

    public record SelectionRemainder(int queueDepth, boolean removedBoundary) { }

    /** Resolve an empty prepared selection against the current queue head. */
    public QueueBoundary resolveEmptySelectionUnderLock(
            RequestRoute item, CapacityBoundary boundary, long nowMs) {
        requireLock();
        if (boundary != null && boundary.unavailable()) {
            return activeIndex.peek() == item && !item.requestExpired(nowMs)
                    ? QueueBoundary.BLOCKED : QueueBoundary.UNCHANGED;
        }
        return removeSelectionBoundaryUnderLock(item, boundary, nowMs)
                ? QueueBoundary.REMOVED : QueueBoundary.UNCHANGED;
    }

    public enum QueueBoundary { UNCHANGED, BLOCKED, REMOVED }

    /** The queue part of a worker wait; the worker owns stop and control wakeups. */
    public boolean queueWaitCurrentUnderLock(
            RequestRoute head, long queueVersion, long inputVersion,
            CapacityBoundary.Availability capacity, long nowMs) {
        requireLock();
        if (head == null) {
            return activeIndex.isEmpty();
        }
        if (activeIndex.peek() != head) {
            return false;
        }
        return capacity != null
                ? !head.requestExpired(nowMs) && !capacity.isAvailable()
                : activeIndex.version() == queueVersion && schedulingInputVersion == inputVersion;
    }

    /** Remove exact ACTIVE ownership; batch preparation retains its OPEN lease. */
    public boolean removeQueuedUnderLock(RequestRoute item) {
        requireLock();
        RequestEntry entry = requests.get(item.requestId());
        if (entry == null || !entry.activeIdentity(item)) {
            return false;
        }
        Reservation lease = entry.reservation;
        requireState(lease == null || lease.state == LeaseState.OPEN,
                "ACTIVE request owns a non-OPEN Prefill lease request_id=", item.requestId());
        detachAdmissionIndexUnderLock(entry, item);
        // BATCH preparation still owns its OPEN lease and generation handoff.
        if (lease instanceof RouteReservation) { closeOpenLeaseUnderLock(lease); }
        removeRequestUnderLock(item.requestId(), entry);
        recordMutationUnderLock();
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
                    int occupant = item == null ? 0 : item.priority();
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
            long required = requestSlotsToReleaseUnderLock(requestLimit);
            if (!PriorityNormalizer.hasPriority(priority) || required == 0L || required > activeIndex.size()) {
                return false;
            }
            for (RequestRoute item : activeIndex) {
                if (isQueuedPreemptionCandidate(item, priority) && --required == 0L) { return true; }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /** Select only uncommitted requests; Engine work cannot release a local queue seat. */
    private List<RequestRoute> queuedPreemptionVictimsUnderLock(int priority, long requestLimit) {
        requireLock();
        long required = requestSlotsToReleaseUnderLock(requestLimit);
        if (!PriorityNormalizer.hasPriority(priority) || required == 0L || required > activeIndex.size()) {
            return List.of();
        }
        List<RequestRoute> candidates = new ArrayList<>();
        for (RequestRoute item : activeIndex) {
            if (isQueuedPreemptionCandidate(item, priority)) { candidates.add(item); }
        }
        if (candidates.size() < required) { return List.of(); }
        candidates.sort(Comparator.comparingInt(RequestRoute::priority)
                .thenComparing(Comparator.comparingLong(RequestRoute::enqueueSeq).reversed()));
        return candidates.subList(0, (int) required);
    }

    private boolean isQueuedPreemptionCandidate(RequestRoute item, int priority) {
        requireLock();
        RequestEntry entry = requests.get(item.requestId());
        return PriorityNormalizer.hasPriority(item.priority()) && item.priority() < priority
                && !item.future().isDone() && entry != null && entry.activeIdentity(item)
                && entry.reservation instanceof RouteReservation && entry.reservation.state == LeaseState.OPEN;
    }

    private long requestSlotsToReleaseUnderLock(long requestLimit) {
        requireLock();
        return requestLimit <= 0L ? 0L
                : Math.max(0L, saturatedAdd(requests.size(), unknownEngineRequestCount) - requestLimit + 1L);
    }

    /** Replace lower-priority queued owners without exposing a partially transferred set. */
    public List<RequestRoute> replaceQueuedRoutesUnderLock(RequestRoute incoming, long requestLimit) {
        requireLock();
        if (requests.containsKey(incoming.requestId())) { return List.of(); }
        List<RequestRoute> victims = queuedPreemptionVictimsUnderLock(incoming.priority(), requestLimit);
        if (victims.isEmpty()) { return victims; }
        // The selected victims fund this seat; the shared lock hides the temporary excess.
        if (!enqueueForDeliveryUnderLock(incoming, 0L)) { return List.of(); }
        for (RequestRoute victim : victims) {
            requireState(removeQueuedUnderLock(victim), "queued preemption lost its exact victim");
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
        boolean releasedRoute = false;
        lock.lock();
        try {
            RequestRoute item = activeIndex.peek();
            if (item == null) { return null; }
            RequestEntry entry = requests.get(item.requestId());
            requireState(entry != null && entry.activeIdentity(item),
                    "stopped queue head has no canonical ACTIVE owner request_id=", item.requestId());
            Reservation lease = entry.reservation;
            requireState(lease == null || lease.state == LeaseState.OPEN,
                    "stopped ACTIVE request owns a non-OPEN Prefill lease request_id=", item.requestId());
            removeValidatedActiveIndex(item);
            entry.queueMembership = QueueMembership.STOP_DETACHED;
            if (lease instanceof RouteReservation) {
                closeOpenLeaseUnderLock(lease);
                releasedRoute = true;
            }
            recordMutationUnderLock();
            return item;
        } finally {
            lock.unlock();
            notifyCapacityAvailable(releasedRoute);
        }
    }

    /** Remove only the exact stop-pending owner whose callback completed. */
    public boolean acknowledgeStopTerminalUnderLock(RequestRoute item) {
        requireLock();
        RequestEntry entry = requests.get(item.requestId());
        if (entry == null
                || !entry.isActive() || entry.item != item
                || entry.queueMembership != QueueMembership.STOP_DETACHED
                || activeIndex.contains(item)) {
            return false;
        }
        boolean removed = removeRequestUnderLock(item.requestId(), entry);
        if (removed) {
            recordMutationUnderLock();
        }
        return removed;
    }

    RouteReservation reserveRouteUnderLock(RequestRoute item, long predictedMs) {
        requireLock();
        RequestEntry entry = requests.get(item.requestId());
        requireState(entry != null && entry.activeIdentity(item) && entry.reservation == null,
                "route reservation requires an exact unreserved ACTIVE request");
        RouteReservation lease = new RouteReservation(entry, predictedMs);
        entry.reservation = lease;
        recordMutationUnderLock();
        return lease;
    }

    /** Borrow the canonical reservation for preparation; commit rechecks the exact lease. */
    RouteReservation prepareRoute(RequestRoute item, long predictedMs) {
        lock.lock();
        try {
            RequestEntry entry = requests.get(item.requestId());
            if (entry == null || !entry.activeIdentity(item)
                    || !(entry.reservation instanceof RouteReservation lease)
                    || lease.state != LeaseState.OPEN) {
                throw new IllegalStateException(
                        "route prediction no longer owns ACTIVE request_id=" + item.requestId());
            }
            lease.predictedWorkMs = boundedPrediction(predictedMs);
            if (entry.queueMembership == QueueMembership.UNINDEXED) {
                committedWorkCapture = null;
            }
            recordMutationUnderLock();
            return lease;
        } finally {
            lock.unlock();
        }
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
            if (findBatchReservationUnderLock(batchId) != null) {
                return new ReservationResult<>(
                        CapacityStatus.BATCH_ID_ALREADY_RESERVED, null);
            }
            if (batchLeasesInUse >= maximum) {
                return new ReservationResult<>(
                        CapacityStatus.CAPACITY_FULL, null);
            }
            BatchReservation lease = new BatchReservation(
                    entry, batchId, generationHandoff);
            entry.reservation = lease;
            batchLeasesInUse++;
            recordMutationUnderLock();
            return new ReservationResult<>(CapacityStatus.ACQUIRED, lease);
        } finally {
            lock.unlock();
        }
    }

    private boolean batchCapacityAvailable(int maximum) {
        lock.lock();
        try {
            return batchLeasesInUse < maximum;
        } finally {
            lock.unlock();
        }
    }

    public CapacityBoundary.Availability batchAvailability(int maximum) {
        requirePositiveBatchLimit(maximum);
        return new CapacityAvailability(maximum);
    }

    private static void requirePositiveBatchLimit(int maximum) {
        if (maximum <= 0) {
            throw new IllegalArgumentException("maximumInflightBatches must be positive");
        }
    }

    /** Exact wake capability permanently paired with this worker runtime. */
    private final class CapacityAvailability
            implements CapacityBoundary.Availability {
        private final int maximum;

        private CapacityAvailability(int maximum) {
            this.maximum = maximum;
        }

        @Override
        public boolean isAvailable() {
            return batchCapacityAvailable(maximum);
        }

        @Override
        public void addListener(Runnable listener) {
            if (listener != capacityAvailable) {
                throw new IllegalArgumentException(
                        "Prefill availability requires its exact worker wake callback");
            }
        }

        @Override
        public void removeListener(Runnable listener) {
            // The worker wake callback remains bound for the entire endpoint generation.
        }
    }

    CommittedHandoff commitRouteGroup(
            List<RequestRoute> items,
            List<RouteReservation> exactReservations,
            EndpointGenerationLifecycle.HandoffPermit generationHandoff) {
        Objects.requireNonNull(generationHandoff, "generationHandoff");
        if (exactReservations.isEmpty()) {
            throw new IllegalArgumentException(
                    "route commit requires at least one reservation");
        }
        List<RouteReservation> leases = new ArrayList<>(
                exactReservations.size());
        for (RouteReservation reservation : exactReservations) {
            if (reservation == null || reservation.owner != this) {
                throw new IllegalArgumentException(
                        "route reservation belongs to another Prefill ledger");
            }
            leases.add(reservation);
        }
        lock.lock();
        try {
            validateGroup(items, false);
            if (items.size() != leases.size()) {
                throw new IllegalArgumentException(
                        "route commit requires one exact lease per member");
            }
            for (int index = 0; index < items.size(); index++) {
                RequestEntry entry = requests.get(items.get(index).requestId());
                RouteReservation lease = leases.get(index);
                if (entry == null
                        || entry.reservation != lease
                        || lease.originalOwner != entry
                        || lease.state != LeaseState.OPEN) {
                    throw new IllegalStateException(
                            "route commit does not own exact OPEN lease request_id="
                                    + items.get(index).requestId());
                }
            }
            long nowMs = clock.getAsLong();
            CommittedHandoff committedHandoff = new CommittedHandoff(generationHandoff,
                    capturePrecedingWorkUnderLock(items, nowMs));
            for (RequestRoute item : items) {
                detachAdmissionIndexUnderLock(requests.get(item.requestId()), item);
            }
            // All validation and handoff allocation precede ownership changes. Queue removal
            // and stop detachment share this lock; DIRECT has no published item lease.
            for (int index = 0; index < items.size(); index++) {
                RequestRoute item = items.get(index);
                RequestEntry entry = requests.get(item.requestId());
                RouteReservation lease = leases.get(index);
                lease.state = LeaseState.OWNED;
                entry.commitIndividual(lease, nowMs);
            }
            committedWorkCapture = null;
            recordMutationUnderLock();
            return committedHandoff;
        } finally {
            lock.unlock();
        }
    }

    private CommittedHandoff commitBatchUnderLock(
            BatchReservation lease,
            List<RequestRoute> items,
            long predictedMs) {
        requireLock();
        validateGroup(items, true);
        RequestEntry head = requests.get(lease.originalOwner.requestId);
        RequestRoute headItem = head == null || !head.isActive() ? null : head.item;
        if (lease.state != LeaseState.OPEN || head == null
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
        BatchWork work = new BatchWork(
                lease,
                predictedMs,
                PrefillBatchFeatures.from(
                        items,
                        RequestRoute::seqLen,
                        RequestRoute::hitCache),
                nowMs);
        CommittedHandoff committedHandoff = new CommittedHandoff(lease.generationHandoff,
                captureWorkUnderLock(nowMs));
        for (RequestRoute item : items) {
            removeValidatedActiveIndex(item);
        }
        moveGenerationHandoffToOwnedUnderLock(lease, committedHandoff);
        for (RequestRoute item : items) {
            requests.get(item.requestId()).commitBatch(work);
        }
        committedWorkCapture = null;
        recordMutationUnderLock();
        return committedHandoff;
    }

    private void validateGroup(List<RequestRoute> items, boolean queuedOnly) {
        requireState(!items.isEmpty(), "committed group requires members");
        Set<RequestRoute> unique = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (RequestRoute item : items) {
            requireState(unique.add(item), "duplicate group member request_id=", item.requestId());
            RequestEntry entry = requests.get(item.requestId());
            requireState(entry != null && entry.activeIdentity(item),
                    "group member is not canonical ACTIVE request_id=", item.requestId());
            if (queuedOnly) {
                requireState(activeIndex.contains(item),
                        "canonical ACTIVE request has no queue index request_id=", item.requestId());
            } else {
                validateAdmissionIndexUnderLock(entry, item);
            }
        }
    }

    private void removeValidatedActiveIndex(RequestRoute item) {
        boolean removed = activeIndex.remove(item);
        requireState(removed,
                "validated ACTIVE queue index disappeared request_id=", item.requestId());
    }

    /**
     * Reserve immediate route work against current ownership in one transaction.
     * Selection revisions are advisory; only exact identity and current capacity
     * decide admission. A zero request limit leaves count admission disabled.
     */
    public ReservationResult<RouteReservation> reserveUnqueuedRoute(
            RequestRoute item, long predictedMs, long maxOutstandingRequests) {
        Objects.requireNonNull(item, "item");
        if (maxOutstandingRequests < 0L) {
            throw new IllegalArgumentException("request limit must be non-negative");
        }
        lock.lock();
        try {
            if (requests.containsKey(item.requestId())) {
                return new ReservationResult<>(CapacityStatus.REQUEST_ALREADY_RESERVED, null);
            }
            if (maxOutstandingRequests > 0L && !canAcceptRequestUnderLock(maxOutstandingRequests)) {
                return new ReservationResult<>(CapacityStatus.CAPACITY_FULL, null);
            }
            RequestEntry entry = new RequestEntry(item, QueueMembership.UNINDEXED);
            RouteReservation reservation = new RouteReservation(entry, predictedMs);
            ReservationResult<RouteReservation> result = new ReservationResult<>(CapacityStatus.ACQUIRED, reservation);
            entry.reservation = reservation;
            requests.put(item.requestId(), entry);
            committedWorkCapture = null;
            recordMutationUnderLock();
            return result;
        } finally {
            lock.unlock();
        }
    }

    private void validateAdmissionIndexUnderLock(RequestEntry entry, RequestRoute item) {
        requireState(entry.queueMembership != QueueMembership.STOP_DETACHED,
                "stopped admission cannot commit request_id=", item.requestId());
        requireState(activeIndex.contains(item) == (entry.queueMembership == QueueMembership.WAITING),
                "canonical admission and waiting index disagree request_id=", item.requestId());
    }

    private void detachAdmissionIndexUnderLock(RequestEntry entry, RequestRoute item) {
        validateAdmissionIndexUnderLock(entry, item);
        if (entry.queueMembership == QueueMembership.WAITING) {
            removeValidatedActiveIndex(item);
        }
        // The caller removes this entry or commits it before releasing the lock.
        // Keep its admission origin until then: removing queued-only work must
        // not invalidate the immutable committed-work projection.
    }

    /**
     * Total counterpart cleanup bound to one exact committed RequestRoute. A
     * reused request id or an ACTIVE item is a no-op.
     */
    public boolean terminalizeCommittedItem(RequestRoute exactItem) {
        boolean capacityReleased = false;
        lock.lock();
        try {
            RequestEntry entry = requests.get(exactItem.requestId());
            if (entry == null || entry.isActive()
                    || entry.item != exactItem) {
                return false;
            }
            Set<RequestEntry> members = entry.batchWork == null
                    ? null : batchMembersUnderLock(entry.batchWork);
            // Local cleanup does not prove that Engine omitted this member's work.
            capacityReleased = settleUnderLock(entry, TerminalObservation.external(entry),
                    members, clock.getAsLong());
            return true;
        } finally {
            lock.unlock();
            notifyCapacityAvailable(capacityReleased);
        }
    }

    public record HeartbeatReconciliation(List<WorkerStatusFact> schedulerFacts,
                                          boolean schedulingInputsChanged) {
        public HeartbeatReconciliation {
            schedulerFacts = List.copyOf(schedulerFacts);
        }
    }

    public HeartbeatReconciliation reconcileHeartbeat(WorkerStatus.StatusObservation observation) {
        List<WorkerStatusFact> facts = new ArrayList<>(observation.runningTasks().size());
        boolean capacityReleased = false;
        boolean schedulingInputsChanged;
        lock.lock();
        try {
            ActiveObservation active = prepareActiveObservationsUnderLock(observation.engine(), Map.of(), facts);
            capacityReleased = active.unknownRequests < unknownEngineRequestCount;
            schedulingInputsChanged = applyActiveObservationsUnderLock(active, clock.getAsLong());
            if (schedulingInputsChanged) {
                committedWorkCapture = null;
                recordMutationUnderLock();
            }
        } finally {
            lock.unlock();
            notifyCapacityAvailable(capacityReleased);
        }
        return new HeartbeatReconciliation(facts, schedulingInputsChanged);
    }

    private record ActiveObservation(long unknownRequests,
                                     IdentityHashMap<RequestEntry, Phase> individuals,
                                     IdentityHashMap<BatchWork, Phase> batches) { }

    /** Both heartbeat and full status apply the same exact activity observations. */
    private boolean applyActiveObservationsUnderLock(ActiveObservation active, long nowMs) {
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

    /** Prepare predictions and completed-batch facts before mutating any owner. */
    private List<BatchCompletion> prepareBatchOutcomesUnderLock(
            Map<BatchWork, Set<RequestEntry>> changedBatches,
            Map<Long, TerminalObservation> terminals,
            IdentityHashMap<BatchWork, Phase> batchPhases,
            ToLongFunction<List<RequestRoute>> repredictor,
            IdentityHashMap<BatchWork, Long> predictions) {
        requireLock();
        List<BatchCompletion> completions = new ArrayList<>(Math.min(terminals.size(), changedBatches.size()));
        for (var changed : changedBatches.entrySet()) {
            BatchWork batch = changed.getKey();
            Set<RequestEntry> members = changed.getValue();
            boolean repack = !batch.executionStarted
                    && batchPhases.get(batch) != Phase.ENGINE_RUNNING;
            List<RequestRoute> survivors = repack
                    ? new ArrayList<>(members.size()) : List.of();
            boolean allTerminal = true;
            long maxExecutionTimeMs = batch.maxExecutionTimeMs;
            boolean successfulCompletion = batch.successfulCompletion;
            boolean learningEligible = batch.learningEligible;
            for (RequestEntry member : members) {
                TerminalObservation terminal = terminals.get(member.requestId);
                if (terminal == null) {
                    allTerminal = false;
                    if (repack) {
                        survivors.add(member.item);
                    }
                } else {
                    // Completion can arrive before the first RUNNING observation.
                    repack &= terminal.errorCode != 0L && terminal.executionTimeMs <= 0L;
                    maxExecutionTimeMs = Math.max(maxExecutionTimeMs, terminal.executionTimeMs);
                    successfulCompletion |= terminal.workerObserved && terminal.errorCode == 0L;
                    learningEligible &= terminal.workerObserved && terminal.errorCode == 0L;
                }
                if (!allTerminal && !repack) {
                    break;
                }
            }
            if (allTerminal) {
                completions.add(new BatchCompletion(
                        batch.lease.batchId,
                        batch.originalFeatures,
                        batch.originalPredictionMs,
                        maxExecutionTimeMs,
                        successfulCompletion,
                        learningEligible));
            } else if (repack) {
                survivors.sort(Comparator.comparingLong(RequestRoute::enqueueSeq)
                        .thenComparingLong(RequestRoute::requestId));
                predictions.put(batch, repredictor.applyAsLong(survivors));
            }
        }
        return completions;
    }

    private ActiveObservation prepareActiveObservationsUnderLock(
            WorkerStatus.EngineObservation engine,
            Map<Long, TerminalObservation> terminals,
            List<WorkerStatusFact> activeFacts) {
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
                if (!entry.isActive() && entry.item != null) {
                    activeFacts.add(WorkerStatusFact.active(entry.item));
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

    /**
     * Reconcile ownership and publish its matching WorkerStatus before the
     * canonical queue lock is released. Projection readers therefore observe
     * either the previous pair or the fully reduced new pair.
     */
    public StatusReconciliation reconcileWorkerStatus(
            WorkerStatus.StatusObservation observation,
            ToLongFunction<List<RequestRoute>> repredictor,
            Runnable committedPublication,
            Runnable failedReduction) {
        boolean capacityReleased = false;
        WorkerStatus.EngineObservation engine = observation.engine();
        StatusReconciliation outcome = null;
        lock.lock();
        try {
            long nowMs = clock.getAsLong();
            Map<Long, TerminalObservation> terminals = terminalObservationsUnderLock(observation.finishedTasks());
            List<WorkerStatusFact> facts = new ArrayList<>(terminals.size() + engine.runningTaskList().size());
            Set<BatchWork> changedBatches = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
            for (TerminalObservation terminal : terminals.values()) {
                WorkerStatusFact fact = terminalFact(terminal.owner, terminal);
                if (fact != null) { facts.add(fact); }
                if (terminal.owner.batchWork != null) {
                    changedBatches.add(terminal.owner.batchWork);
                }
            }
            IdentityHashMap<BatchWork, Set<RequestEntry>> reductions = changedBatches.isEmpty()
                    ? new IdentityHashMap<>(0) : batchReductionsUnderLock(changedBatches::contains);
            ActiveObservation active = prepareActiveObservationsUnderLock(engine, terminals, facts);
            IdentityHashMap<BatchWork, Long> predictions = new IdentityHashMap<>();
            List<BatchCompletion> completions = prepareBatchOutcomesUnderLock(
                    reductions, terminals, active.batches, repredictor, predictions);
            // Freeze all externally observable facts before settling the first owner.
            outcome = new StatusReconciliation(facts, completions, null);

            // Everything below this boundary is assignment/removal against
            // exact prevalidated identities. Any invariant failure is captured
            // into the already materialized outcome and forces retirement.
            committedWorkCapture = null;
            recordMutationUnderLock();
            for (TerminalObservation terminal : terminals.values()) {
                RequestEntry entry = terminal.owner;
                capacityReleased |= settleUnderLock(entry, terminal,
                        entry.batchWork == null ? null : reductions.get(entry.batchWork), nowMs);
            }
            capacityReleased |= active.unknownRequests < unknownEngineRequestCount;
            applyActiveObservationsUnderLock(active, nowMs);
            predictions.forEach((batch, prediction) -> {
                batch.remainingWorkMs = prediction;
                batch.phaseBaseMs = nowMs;
                batch.touch(nowMs);
            });
            // Status reduction changes counts after invalidating the old work revision.
            publishRequestCountUnderLock();
            committedPublication.run();
        } catch (Throwable failure) {
            // A materialized outcome marks the mutation boundary. Preserve its exact facts
            // even if reduction/publication fails, so retirement can still settle owners.
            if (outcome != null) {
                outcome = new StatusReconciliation(outcome.schedulerFacts(), outcome.batchCompletions(), failure);
            }
            try {
                failedReduction.run();
            } catch (Throwable failClosedFailure) {
                if (outcome == null) { Failures.append(failure, failClosedFailure); }
            }
            if (outcome == null) { throw Failures.propagate(failure, "Prefill status reduction failed"); }
        } finally {
            lock.unlock();
            notifyCapacityAvailable(capacityReleased);
        }
        return outcome;
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
        List<RequestRoute> ownedItems = new ArrayList<>();
        List<BatchCompletion> completions = new ArrayList<>();
        List<EndpointGenerationLifecycle.HandoffPermit> orphanedHandoffs =
                new ArrayList<>();
        Set<Reservation> leases = java.util.Collections.newSetFromMap(
                new IdentityHashMap<>());
        Set<BatchWork> batches = java.util.Collections.newSetFromMap(
                new IdentityHashMap<>());
        Throwable invariantFailure = null;
        Retirement plannedRetirement;
        lock.lock();
        try {
            for (RequestEntry entry : requests.values()) {
                ownedItems.add(entry.item);
                if (entry.reservation != null) {
                    leases.add(entry.reservation);
                }
                if (entry.batchWork != null) {
                    batches.add(entry.batchWork);
                    leases.add(entry.batchWork.lease);
                }
            }

            for (Reservation lease : leases) {
                if (lease instanceof BatchReservation batch) {
                    if (batch.state == LeaseState.OPEN) {
                        if (batch.generationHandoff == null) {
                            invariantFailure = Failures.append(invariantFailure,
                                    new IllegalStateException(
                                            "OPEN Prefill batch lease lost its generation handoff"));
                        } else {
                            orphanedHandoffs.add(batch.generationHandoff);
                            invariantFailure = Failures.append(invariantFailure,
                                    new IllegalStateException(
                                            "retirement reached an OPEN Prefill batch lease"));
                        }
                    } else if (batch.state != LeaseState.OWNED
                            || batch.generationHandoff != null) {
                        invariantFailure = Failures.append(invariantFailure,
                                new IllegalStateException(
                                        "canonical retirement owner has an invalid batch lease state"));
                    }
                }
            }

            for (BatchWork batch : batches) {
                completions.add(batch.retirementCompletion());
            }
            ownedItems.sort(Comparator.comparingLong(RequestRoute::enqueueSeq)
                    .thenComparingLong(RequestRoute::requestId));
            completions.sort(Comparator.comparingLong(BatchCompletion::batchId));
            plannedRetirement = new Retirement(
                    ownedItems,
                    completions,
                    invariantFailure);

            // Canonical retirement commit. Everything which may allocate or
            // validate has completed above this line.
            for (RequestEntry entry : requests.values()) {
                entry.item = null;
                entry.reservation = null;
                entry.batchWork = null;
                entry.individualPhase = null;
                entry.queueMembership = QueueMembership.UNINDEXED;
            }
            for (Reservation lease : leases) {
                if (lease instanceof BatchReservation batch) {
                    batch.generationHandoff = null;
                }
                lease.state = LeaseState.CLOSED;
            }
            requests.clear();
            activeIndex.clear();
            batchLeasesInUse = 0;
            unknownEngineRequestCount = 0L;
            committedWorkCapture = null;
            recordMutationUnderLock();
        } finally {
            lock.unlock();
        }

        for (EndpointGenerationLifecycle.HandoffPermit handoff
                : orphanedHandoffs) {
            try {
                handoff.close();
            } catch (Throwable ignoredHandoffFailure) {
                // The OPEN-state invariant above is the fixed primary failure.
                // Canonical retirement has committed, so aggregation must not
                // allocate or prevent later exact handoffs from being closed.
            }
        }
        notifyCapacityAvailable(!leases.isEmpty());
        return plannedRetirement;
    }

    /** One orphan pass: a retained member protects its whole batch. Counts batches and individuals. */
    public int evictExpiredInflight(long ttlMs, java.util.function.LongPredicate retainForSchedulerCleanup) {
        int evicted = 0;
        boolean capacityReleased = false;
        lock.lock();
        try {
            long nowMs = clock.getAsLong();
            long ttl = Math.max(0L, ttlMs);
            for (var reduced : batchReductionsUnderLock(batch -> true).entrySet()) {
                Set<RequestEntry> members = reduced.getValue();
                boolean retained = nowMs - reduced.getKey().lastObservedAtMs < ttl;
                for (RequestEntry entry : members) {
                    retained |= retainForSchedulerCleanup.test(entry.requestId);
                }
                if (retained) { continue; }
                for (RequestEntry entry : List.copyOf(members)) {
                    capacityReleased |= settleUnderLock(entry,
                            TerminalObservation.external(entry), members, nowMs);
                }
                evicted++;
            }
            List<RequestEntry> individuals = new ArrayList<>();
            for (RequestEntry entry : requests.values()) {
                if (entry.batchWork == null && !entry.isActive()
                        && nowMs - entry.phaseBaseMs >= ttl
                        && !retainForSchedulerCleanup.test(entry.requestId)) {
                    individuals.add(entry);
                }
            }
            for (RequestEntry entry : individuals) {
                capacityReleased |= settleUnderLock(entry,
                        TerminalObservation.external(entry), null, nowMs);
            }
            evicted += individuals.size();
        } finally {
            lock.unlock();
            notifyCapacityAvailable(capacityReleased);
        }
        return evicted;
    }

    public Stats stats() {
        lock.lock();
        try {
            int locallyOwned = 0;
            int individual = 0;
            long maxAgeMs = 0L;
            Set<BatchWork> batches = java.util.Collections.newSetFromMap(
                    new IdentityHashMap<>());
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
                } else {
                    batches.add(entry.batchWork);
                }
            }
            for (BatchWork batch : batches) {
                maxAgeMs = Math.max(
                        maxAgeMs,
                        Math.max(0L, nowMs - batch.lastObservedAtMs));
            }
            return new Stats(
                    locallyOwned,
                    individual,
                    batches.size(),
                    maxAgeMs);
        } finally {
            lock.unlock();
        }
    }

    /** Advisory selection check; a positive result does not reserve capacity. */
    public boolean canAcceptRequest(long requestLimit) {
        return outstandingRequestCount < requestLimit;
    }

    private boolean canAcceptRequestUnderLock(long maxOutstandingRequests) {
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

    public Snapshot snapshotUnderLock() {
        requireLock();
        ProjectionVersion version = new ProjectionVersion(
                activeIndex.version(), schedulingInputVersion, mutationVersion);
        long nowMs = clock.getAsLong();
        return new Snapshot(
                version, nowMs,
                activeIndex.capture(),
                captureWorkUnderLock(nowMs));
    }

    public WorkSnapshot committedSnapshot() {
        WorkCapture capture;
        lock.lock();
        try {
            capture = captureCurrentWorkUnderLock(clock.getAsLong(), Set.of());
        } finally {
            lock.unlock();
        }
        return capture.materialize();
    }

    private WorkCapture captureWorkUnderLock(long nowMs) {
        requireLock();
        if (committedWorkCapture == null || committedWorkCapture.capturedAtMs > nowMs) {
            committedWorkCapture = captureCurrentWorkUnderLock(nowMs, Set.of());
        }
        return committedWorkCapture;
    }

    private WorkCapture capturePrecedingWorkUnderLock(List<RequestRoute> members, long nowMs) {
        requireLock();
        Set<RequestEntry> excluded = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (RequestRoute member : members) {
            excluded.add(requests.get(member.requestId()));
        }
        return captureCurrentWorkUnderLock(nowMs, excluded);
    }

    private WorkCapture captureCurrentWorkUnderLock(long nowMs, Set<RequestEntry> excluded) {
        requireLock();
        List<WorkSnapshot.RequestWork> individual = new ArrayList<>();
        IdentityHashMap<BatchWork, List<Long>> batchMembers =
                new IdentityHashMap<>();
        for (RequestEntry entry : requests.values()) {
            if (excluded.contains(entry)) {
                continue;
            }
            if (entry.isActive()) {
                if (entry.queueMembership == QueueMembership.UNINDEXED
                        && entry.reservation instanceof RouteReservation route) {
                    // Capacity has been reserved for immediate handoff. Later admissions
                    // must include this work even before its route is published.
                    individual.add(new WorkSnapshot.RequestWork(
                            entry.requestId, Phase.COMMITTED, route.predictedWorkMs));
                }
                continue;
            }
            if (entry.batchWork == null) {
                individual.add(new WorkSnapshot.RequestWork(
                        entry.requestId,
                        entry.individualPhase,
                        individualRemaining(entry, nowMs)));
            } else {
                batchMembers.computeIfAbsent(
                                entry.batchWork, ignored -> new ArrayList<>())
                        .add(entry.requestId);
            }
        }
        List<WorkSnapshot.BatchWork> batches =
                new ArrayList<>(batchMembers.size());
        for (Map.Entry<BatchWork, List<Long>> observed : batchMembers.entrySet()) {
            BatchWork batch = observed.getKey();
            batches.add(new WorkSnapshot.BatchWork(
                    batch.lease.batchId,
                    observed.getValue(),
                    batch.servicePhase,
                    OptionalLong.of(batch.remainingAt(nowMs))));
        }
        return new WorkCapture(
                nowMs,
                individual,
                batches,
                unknownEngineRequestCount);
    }

    private boolean settleUnderLock(
            RequestEntry entry,
            TerminalObservation terminal,
            Set<RequestEntry> members,
            long nowMs) {
        requireLock();
        BatchWork batch = entry.batchWork;
        Reservation lease;
        if (batch != null) {
            if (members == null || !members.contains(entry)) {
                throw new IllegalStateException(
                        "missing batch reduction for live request_id="
                                + entry.requestId);
            }
            lease = members.size() == 1 ? batch.lease : null;
        } else {
            lease = entry.reservation;
        }
        if (lease != null && lease.state != LeaseState.OWNED) {
            throw new IllegalStateException(
                    "terminal request owns a non-committed Prefill lease"
                            + " request_id=" + entry.requestId);
        }
        if (batch != null) {
            batch.observeTerminal(terminal, nowMs);
            members.remove(entry);
        }
        if (!removeRequestUnderLock(entry.requestId, entry)) {
            throw new IllegalStateException(
                    "terminal request is not canonical request_id="
                            + entry.requestId);
        }
        committedWorkCapture = null;
        recordMutationUnderLock();
        if (lease != null) {
            closeOwnedLeaseUnderLock(lease);
        }
        return true;
    }

    private Map<Long, TerminalObservation> terminalObservationsUnderLock(
            Map<String, WorkerStatus.TaskObservation> finishedTasks) {
        requireLock();
        Map<Long, TerminalObservation> terminals = new HashMap<>();
        for (WorkerStatus.TaskObservation task : finishedTasks.values()) {
            RequestEntry entry = requests.get(task.requestId());
            if (entry == null || !matchesObservedBatch(entry, task.batchId())) {
                continue;
            }
            TerminalObservation terminal = TerminalObservation.from(entry, task);
            terminals.merge(
                    task.requestId(), terminal, TerminalObservation::merge);
        }
        return terminals;
    }

    private static WorkerStatusFact terminalFact(
            RequestEntry entry,
            TerminalObservation terminal) {
        if (entry == null || entry.isActive() || entry.item == null
                || !terminal.workerObserved) {
            return null;
        }
        WorkerStatusFact.Kind kind = terminal.errorCode == 0L
                ? WorkerStatusFact.Kind.COMPLETED
                : terminal.preemptionProgress
                        == PriorityPreemptionProgress.CANCELED
                    && terminal.errorCode
                            == StrategyErrorType.PRIORITY_PREEMPTED.getErrorCode()
                ? WorkerStatusFact.Kind.PRIORITY_CANCELED
                : WorkerStatusFact.Kind.FAILED;
        return WorkerStatusFact.terminal(
                entry.item, kind, terminal.errorCode);
    }

    private static boolean matchesObservedBatch(
            RequestEntry entry, long observedBatchId) {
        if (entry.batchWork == null) {
            return true;
        }
        return observedBatchId > 0L
                && entry.batchWork.lease.batchId == observedBatchId;
    }

    private static Phase strongerEnginePhase(Phase left, Phase right) {
        return left == Phase.ENGINE_RUNNING || right == Phase.ENGINE_RUNNING
                ? Phase.ENGINE_RUNNING : Phase.ENGINE_QUEUED;
    }

    private IdentityHashMap<BatchWork, Set<RequestEntry>>
            batchReductionsUnderLock(Predicate<BatchWork> included) {
        requireLock();
        IdentityHashMap<BatchWork, Set<RequestEntry>> reductions =
                new IdentityHashMap<>();
        for (RequestEntry entry : requests.values()) {
            if (entry.batchWork != null && included.test(entry.batchWork)) {
                reductions.computeIfAbsent(
                                entry.batchWork, ignored -> java.util.Collections.newSetFromMap(new IdentityHashMap<>()))
                        .add(entry);
            }
        }
        return reductions;
    }

    /** Build only the exact batch needed by a single-item terminal path. */
    private Set<RequestEntry> batchMembersUnderLock(BatchWork exactBatch) {
        requireLock();
        Set<RequestEntry> members = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (RequestEntry entry : requests.values()) {
            if (entry.batchWork == exactBatch) {
                members.add(entry);
            }
        }
        return members;
    }

    private BatchReservation findBatchReservationUnderLock(long batchId) {
        for (RequestEntry entry : requests.values()) {
            if (entry.reservation instanceof BatchReservation reservation
                    && reservation.batchId == batchId) {
                return reservation;
            }
            if (entry.batchWork != null
                    && entry.batchWork.lease.batchId == batchId) {
                return entry.batchWork.lease;
            }
        }
        return null;
    }

    /**
     * Roll back only a provisional lease. Once committed, the request table is
     * the sole owner and only its terminal reducer may release the capacity.
     */
    private void releaseOpenLease(Reservation lease) {
        if (lock.isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "Prefill lease rollback cannot run under queueLock");
        }
        EndpointGenerationLifecycle.HandoffPermit generationHandoff;
        lock.lock();
        try {
            if (lease.state != LeaseState.OPEN) {
                return;
            }
            generationHandoff = closeOpenLeaseUnderLock(lease);
        } finally {
            lock.unlock();
        }
        try {
            if (generationHandoff != null) {
                Failures.rethrow(Failures.close(generationHandoff), "Prefill exact capacity cleanup failed");
            }
        } finally {
            notifyCapacityAvailable(true);
        }
    }

    private void notifyCapacityAvailable(boolean capacityReleased) {
        if (!capacityReleased) {
            return;
        }
        try {
            capacityAvailable.run();
        } catch (Throwable notificationFailure) {
            try {
                org.flexlb.util.Logger.error(
                        "Prefill capacity notification failed",
                        notificationFailure);
            } catch (Throwable ignoredLoggingFailure) {
                // Capacity ownership is already settled; diagnostics cannot
                // make the caller lose its prebuilt terminal facts.
            }
        }
    }

    /** Move the exact handoff out of the OPEN admission before commit publishes. */
    private void moveGenerationHandoffToOwnedUnderLock(
            BatchReservation lease,
            CommittedHandoff committedHandoff) {
        requireLock();
        if (lease.state != LeaseState.OPEN
                || lease.generationHandoff == null
                || committedHandoff.generationHandoff
                    != lease.generationHandoff) {
            throw new IllegalStateException(
                    "Prefill commit requires an exact OPEN generation handoff");
        }
        lease.generationHandoff = null;
        lease.state = LeaseState.OWNED;
    }

    /** OPEN rollback closes quota and any batch-owned generation handoff. */
    private EndpointGenerationLifecycle.HandoffPermit
            closeOpenLeaseUnderLock(Reservation lease) {
        requireLock();
        if (lease.state != LeaseState.OPEN) {
            throw new IllegalStateException(
                    "Prefill OPEN rollback lost its exact lease");
        }
        RequestEntry owner = openLeaseOwnerUnderLock(lease);
        if (owner != null
                && !owner.isActive()
                && owner.queueMembership != QueueMembership.STOP_DETACHED) {
            throw new IllegalStateException(
                    "OPEN Prefill lease has a non-ACTIVE canonical owner");
        }
        EndpointGenerationLifecycle.HandoffPermit generationHandoff = null;
        if (lease instanceof BatchReservation batch) {
            generationHandoff = batch.generationHandoff;
            if (generationHandoff == null) {
                throw new IllegalStateException(
                        "Prefill batch rollback lost its generation handoff");
            }
            batch.generationHandoff = null;
        }
        lease.state = LeaseState.CLOSED;
        releaseBatchSlotUnderLock(lease);
        if (owner != null) {
            owner.reservation = null;
            if (owner.queueMembership == QueueMembership.UNINDEXED) {
                removeRequestUnderLock(owner.requestId, owner);
                committedWorkCapture = null;
            }
        }
        recordMutationUnderLock();
        return generationHandoff;
    }

    /** Close exact ownership and return a batch slot when this is the last member. */
    private void closeOwnedLeaseUnderLock(Reservation lease) {
        requireLock();
        if (lease.state != LeaseState.OWNED
                || lease instanceof BatchReservation batch
                && batch.generationHandoff != null) {
            throw new IllegalStateException(
                    "Prefill OWNED terminal still owns an admission handoff");
        }
        lease.state = LeaseState.CLOSED;
        releaseBatchSlotUnderLock(lease);
    }

    private void releaseBatchSlotUnderLock(Reservation lease) {
        requireLock();
        if (lease instanceof BatchReservation) {
            if (batchLeasesInUse <= 0) {
                throw new IllegalStateException(
                        "Prefill batch capacity accounting underflow");
            }
            batchLeasesInUse--;
        } else if (!(lease instanceof RouteReservation)) {
            throw new IllegalStateException("unknown Prefill reservation type");
        }
    }

    private RequestEntry openLeaseOwnerUnderLock(Reservation lease) {
        requireLock();
        RequestEntry originalOwner = lease.originalOwner;
        RequestEntry current = requests.get(originalOwner.requestId);
        if (current == originalOwner) {
            if (current.reservation != lease) {
                throw new IllegalStateException(
                        "canonical Prefill lease owner lost its exact reservation"
                                + " request_id=" + originalOwner.requestId);
            }
            return current;
        }
        if (current != null && current.reservation == lease) {
            throw new IllegalStateException(
                    "replacement request cannot own an earlier Prefill lease"
                            + " request_id=" + originalOwner.requestId);
        }
        return null;
    }

    private void requireLock() {
        requireState(lock.isHeldByCurrentThread(),
                "Prefill ownership requires queueLock");
    }

    private static void requireState(boolean condition, String message, long requestId) {
        if (!condition) {
            throw new IllegalStateException(message + requestId);
        }
    }

    private static void requireState(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static long individualRemaining(RequestEntry entry, long nowMs) {
        if (entry.individualPhase != Phase.ENGINE_RUNNING) {
            return entry.remainingWorkMs;
        }
        return Math.max(0L, entry.remainingWorkMs
                - Math.max(0L, nowMs - entry.phaseBaseMs));
    }

    private static long boundedPrediction(long predictedMs) {
        return Math.min(Integer.MAX_VALUE, Math.max(0L, predictedMs));
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
