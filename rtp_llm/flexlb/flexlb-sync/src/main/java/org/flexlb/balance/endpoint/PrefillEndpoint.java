package org.flexlb.balance.endpoint;

import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.planner.GroupPlanner;
import org.flexlb.balance.prediction.FormulaPredictor;
import org.flexlb.balance.prediction.LearningPredictor;
import org.flexlb.balance.prediction.PrefillBatchFeatures;
import org.flexlb.balance.prediction.PrefillPredictionBoundary;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.projection.QueueSnapshot.AdmissionBlock;
import org.flexlb.balance.projection.RouteProjection;
import org.flexlb.balance.scheduler.PlacementAvailability;
import org.flexlb.balance.scheduler.RequestRoute;
import org.flexlb.balance.scheduler.WorkerBatcher;
import org.flexlb.balance.scheduler.QueueExecutionSettings;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.RoutingConfig;
import org.flexlb.dao.loadbalance.AdmissionRejectReason;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Failures;
import org.flexlb.util.PriorityOrdering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongPredicate;

public class PrefillEndpoint extends WorkerEndpoint {

    /**
     * Short-lived generation capability for committing one NON_BATCH route
     * group. Queued requests keep their canonical identity without keeping an
     * endpoint generation alive while waiting for delivery.
     */
    public final class RouteCommitAdmission implements AutoCloseable {

        private EndpointGenerationLifecycle.HandoffPermit generationHandoff;

        private RouteCommitAdmission(
                EndpointGenerationLifecycle.HandoffPermit generationHandoff) {
            this.generationHandoff = generationHandoff;
        }

        public PrefillState.CommittedHandoff commit(
                List<RequestRoute> exactItems,
                List<PrefillState.RouteReservation> exactReservations) {
            EndpointGenerationLifecycle.HandoffPermit exact = generationHandoff;
            if (exact == null) {
                throw new IllegalStateException(
                        "route commit no longer owns its generation handoff");
            }
            PrefillState.CommittedHandoff committed =
                    prefillState.commitRouteGroup(
                            exactItems, exactReservations, exact);
            generationHandoff = null;
            return committed;
        }

        @Override
        public void close() {
            EndpointGenerationLifecycle.HandoffPermit exact = generationHandoff;
            generationHandoff = null;
            if (exact != null) {
                exact.close();
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger("syncLogger");

    private static final PrefillTimePredictor.Evaluator DEFAULT_BATCH_PREDICTOR = new FormulaPredictor(
            new RoutingConfig.ExecutionTimeEstimatorConfig().getExpression());

    private final PrefillTimePredictor predictor;

    private final long inflightRequestLimit;

    private volatile WorkerBatcher runtime;
    private final DeliveryStrategy deliveryStrategy;
    private final DispatcherConfig.Type dispatcherType;
    private final RouteProjection.DeliveryProjection deliveryProjection;
    private volatile Comparator<GroupPlanner.Item> projectionOrder;
    private volatile ProjectionSource projectionSource;

    private static final Comparator<GroupPlanner.Item> PRIORITY_PROJECTION_ORDER =
            (left, right) -> PriorityOrdering.compareWithRequestId(
                    left.priority(), left.enqueueSeq(), left.requestId(),
                    right.priority(), right.enqueueSeq(), right.requestId());
    private static final Comparator<GroupPlanner.Item> FIFO_PROJECTION_ORDER =
            Comparator.comparingLong(GroupPlanner.Item::enqueueSeq)
                    .thenComparingLong(GroupPlanner.Item::requestId);

    private final PrefillState prefillState;

    private final BatchSchedulerReporter reporter;

    private final PlacementAvailability placementAvailability;

    PrefillEndpoint(WorkerStatus status,
                    FlexlbConfig config,
                    DeliveryStrategy deliveryStrategy,
                    BatchSchedulerReporter reporter,
                    PlacementAvailability placementAvailability) {
        super(status);
        this.reporter = java.util.Objects.requireNonNull(reporter, "reporter");
        this.placementAvailability = java.util.Objects.requireNonNull(
                placementAvailability, "placementAvailability");
        this.predictor = createPredictor(config);
        this.deliveryStrategy = deliveryStrategy;
        this.dispatcherType = config.getDispatcher().getType();
        this.inflightRequestLimit = config.getDispatcher().getType() == DispatcherConfig.Type.NON_BATCH
                ? config.getDispatcher().getMaxInflightPerPrefillWorker() : 0L;
        this.deliveryProjection = deliveryStrategy.projectionPolicy();
        this.projectionOrder = config.isPriorityOrdering() ? PRIORITY_PROJECTION_ORDER : FIFO_PROJECTION_ORDER;
        this.prefillState = new PrefillState(new ReentrantLock(), PrefillActiveIndex.disabled(), this::signalCapacityAvailable);

    }

    /**
     * Start the attached generation exactly once before routing publication.
     */
    void startGeneration() {
        if (runtime != null) { runtime.start(); }
    }

    /** Captured under prefillState.ownershipLock(); the shared result is built without that lock. */
    private final class ProjectionSource {
        private final PrefillState.ProjectionVersion version;
        private PrefillState.Snapshot ownership;
        private final GroupPlanner.Constraints constraints;
        private final AdmissionBlock admissionBlock;
        private final WorkerBatcher capturedRuntime;
        private final Comparator<GroupPlanner.Item> capturedOrder;
        private volatile RouteProjection.Inputs materialized;

        private ProjectionSource(PrefillState.Snapshot ownership,
                                 GroupPlanner.Constraints constraints, AdmissionBlock admissionBlock) {
            this.version = ownership.version();
            this.ownership = ownership;
            this.constraints = constraints;
            this.admissionBlock = admissionBlock;
            this.capturedRuntime = runtime;
            this.capturedOrder = projectionOrder;
        }

        private RouteProjection.Inputs materialize() {
            RouteProjection.Inputs result = materialized;
            if (result != null) {
                return result;
            }
            synchronized (this) {
                if (materialized == null) {
                    var queueSnapshot = new org.flexlb.balance.projection.QueueSnapshot(
                            ownership.capturedAtMs(), capturedRuntime != null, capturedRuntime == null ? null : capturedRuntime.groupingPolicy(), capturedOrder,
                            constraints, ownership.active().projectedItems(), admissionBlock);
                    materialized = new RouteProjection.Inputs(
                            queueSnapshot, ownership.work().materialize(), version.ownership());
                    // The cached projection must not retain completed request contexts.
                    ownership = null;
                }
                return materialized;
            }
        }
    }

    public RouteProjection.Inputs captureRouteProjectionInputs() {
        ProjectionSource source = projectionSource;
        if (source == null || !prefillState.isCurrentProjection(source.version)) {
            prefillState.ownershipLock().lock();
            try {
                source = projectionSource;
                if (source == null || !prefillState.isCurrentProjection(source.version)) {
                    source = captureProjectionSourceUnderLock();
                    projectionSource = source;
                }
            } finally {
                prefillState.ownershipLock().unlock();
            }
        }
        // Concurrent callers share one source per version. A late build only
        // fills its own source; it can never overwrite a newer capture.
        return source.materialize();
    }

    /** Caller holds the endpoint ownership lock. */
    private ProjectionSource captureProjectionSourceUnderLock() {
        PrefillState.Snapshot ownership = prefillState.snapshotUnderLock();
        return new ProjectionSource(ownership,
                runtime == null ? new GroupPlanner.Constraints(1, Long.MAX_VALUE, Long.MAX_VALUE, 0L, 0L)
                        : runtime.projectionConstraintsUnderLock(),
                runtime == null || ownership.active().isEmpty() ? null : runtime.admissionBlockUnderLock());
    }

    /**
     * Stable delivery semantics selected once for this endpoint generation.
     */
    public RouteProjection.DeliveryProjection deliveryProjection() {
        return deliveryProjection;
    }

    /**
     * Publish one exact route after validating its generation pin.
     */
    public boolean offerPinned(
            GenerationPin exactPin,
            RequestRoute exactItem, QueueExecutionSettings settings) {
        requirePinnedGeneration(exactPin);
        if (runtime == null) {
            enableQueueRuntime(settings);
        }
        return runtime.offer(exactItem);
    }

    public void enableQueueRuntime(QueueExecutionSettings settings) {
        prefillState.ownershipLock().lock();
        try {
            if (runtime != null) { return; }
            if (settings == null || settings.dispatcherType() != dispatcherType) {
                throw new IllegalArgumentException("queued admission requires a compatible QUEUE configuration");
            }
            prefillState.enableQueueUnderLock(settings.priorityOrdering()
                    ? WorkerBatcher.PRIORITY_QUEUE_ORDER : WorkerBatcher.FIFO_QUEUE_ORDER);
            WorkerBatcher worker = new WorkerBatcher(ipPort(), this, settings, deliveryStrategy, prefillState);
            projectionOrder = settings.priorityOrdering() ? PRIORITY_PROJECTION_ORDER : FIFO_PROJECTION_ORDER;
            projectionSource = null;
            runtime = worker;
            worker.start();
        } finally {
            prefillState.ownershipLock().unlock();
        }
    }

    /**
     * Publish a role/group-scoped edge after real queue or status progress.
     */
    public void signalPlacementCapacityChanged() {
        WorkerStatus.TopologySnapshot topology =
                getStatus().topologySnapshot();
        placementAvailability.changed(
                getStatus().getRole(), topology.group(), ipPort());
    }

    /**
     * Remove only the supplied canonical ACTIVE queue identity.
     */
    public boolean removeQueued(
            RequestRoute exactItem,
            String reason) {
        return runtime != null && runtime.removeQueued(exactItem, reason);
    }

    public void signalRouteReady() {
        signalSchedulingInputsChanged();
    }

    private void signalCapacityAvailable() {
        if (runtime != null) { runtime.signalDeliveryCapacityAvailable(); }
        signalPlacementCapacityChanged();
    }

    private void signalSchedulingInputsChanged() {
        if (runtime != null) {
            runtime.signalSchedulingInputsChanged();
        } else {
            prefillState.ownershipLock().lock();
            try {
                prefillState.schedulingInputsChangedUnderLock();
            } finally {
                prefillState.ownershipLock().unlock();
            }
        }
    }

    public boolean signalQueuedControl(RequestRoute exactItem) {
        return runtime != null && runtime.signalControl(exactItem);
    }

    /**
     * Read the last scheduling decision without traversing or locking the queue.
     */
    public Map<String, Object> queueWaitDiagnostics() {
        return runtime == null ? Map.of("cause", "waiting for Prefill decision") : runtime.waitDiagnostics();
    }

    public int queuedRequestCount() {
        return prefillState.queueDepth();
    }

    /**
     * Runs once, after every accepted generation handoff has released its pin.
     */
    @Override
    protected void closeEndpoint() {
        // A self-await invariant escapes here before ledger mutation/event
        // publication. Ordinary stop cleanup failures are returned only after
        // the exact worker has exited, and are aggregated below.
        Throwable retirementFailure = runtime == null ? null : runtime.stopAndAwait();
        try {
            PrefillState.Retirement retirement =
                    prefillState.retireGenerationOwnership();
            if (!retirement.ownedItems().isEmpty()) {
                try {
                    for (RequestRoute route : retirement.ownedItems()) {
                        route.ctx().scheduler().onPrefillGenerationRetired(this, List.of(route));
                    }
                } catch (Throwable callbackFailure) {
                    retirementFailure = Failures.append(
                            retirementFailure, callbackFailure);
                }
            }
            retirementFailure = Failures.append(
                    retirementFailure, retirement.invariantFailure());
            List<PrefillState.BatchCompletion> completions =
                    retirement.batchCompletions();
            for (int index = 0; index < completions.size(); index++) {
                PrefillState.BatchCompletion completion =
                        completions.get(index);
                try {
                    reportBatchCompletion(completion);
                } catch (Throwable reportingFailure) {
                    retirementFailure = Failures.append(
                            retirementFailure, reportingFailure);
                }
            }
        } catch (Throwable committedRetirementFailure) {
            retirementFailure = Failures.append(
                    retirementFailure, committedRetirementFailure);
        }
        Failures.rethrow(retirementFailure, "Prefill endpoint retirement failed");
    }

    private static PrefillTimePredictor createPredictor(FlexlbConfig config) {
        RoutingConfig.ExecutionTimeEstimatorConfig estimator = config.getRouter()
                .getRoles().getPrefill().getExecutionTimeEstimator();
        if (estimator.getType() == RoutingConfig.EstimatorType.LEARNING) {
            return new LearningPredictor();
        }
        return new FormulaPredictor(estimator.getExpression());
    }

    public PrefillState.ReservationResult<PrefillState.BatchReservation> reserveBatch(
            RequestRoute exactHead,
            long batchId,
            int maximumInflightBatches) {
        EndpointGenerationLifecycle.HandoffPermit handoffPermit =
                tryAcquireGenerationHandoff();
        if (handoffPermit == null) {
            return new PrefillState.ReservationResult<>(
                    PrefillState.CapacityStatus.ENDPOINT_RETIRED, null);
        }
        PrefillState.ReservationResult<PrefillState.BatchReservation> result = null;
        try {
            result = prefillState.reserveBatch(
                    exactHead,
                    batchId,
                    maximumInflightBatches,
                    handoffPermit);
            return result;
        } finally {
            if (result == null || result.reservation() == null) {
                handoffPermit.close();
            }
        }
    }

    /**
     * Acquire the generation capability only for the final route commit.
     */
    public RouteCommitAdmission tryBeginRouteCommitAdmission() {
        EndpointGenerationLifecycle.HandoffPermit handoffPermit =
                tryAcquireGenerationHandoff();
        return handoffPermit == null
                ? null : new RouteCommitAdmission(handoffPermit);
    }

    /**
     * Exact wake source for this generation's batch admission capacity.
     */
    public CapacityBoundary.Availability batchAdmissionAvailability(
            int maximumInflightBatches) {
        return prefillState.batchAvailability(maximumInflightBatches);
    }

    /**
     * Explain a failed admission using the current owner-priority summary.
     */
    public AdmissionRejectReason admissionRejectReason(int priority) {
        return prefillState.admissionRejectReason(priority, inflightRequestLimit);
    }

    /**
     * Advisory capacity; publication repeats the count check under the ownership lock.
     */
    public boolean canAcceptRequest() {
        return inflightRequestLimit == 0L || prefillState.canAcceptRequest(inflightRequestLimit);
    }

    public boolean canPreemptQueuedRequest(int priority) {
        return prefillState.canPreemptQueuedRequest(priority, inflightRequestLimit);
    }

    /**
     * Advisory endpoint ownership revision captured by queue placement.
     */
    public long placementVersion() {
        return prefillState.mutationVersion();
    }

    /**
     * Admit on the selected generation using its current canonical occupancy
     * and bound dispatcher policy. The pin preserves generation identity while
     * PrefillState checks and occupies capacity under its ownership lock.
     */
    public PrefillState.ReservationResult<PrefillState.RouteReservation> reserveUnqueuedRoute(
            GenerationPin pin, RequestRoute item, long predictedMs) {
        requirePinnedGeneration(pin);
        return prefillState.reserveUnqueuedRoute(item, predictedMs, inflightRequestLimit);
    }

    public PrefillState.RouteReservation prepareRoute(RequestRoute item, long predictedMs) {
        return prefillState.prepareRoute(item, predictedMs);
    }

    /**
     * Exact counterpart cleanup; stale item generations are a no-op.
     */
    public boolean releaseCommittedItem(RequestRoute exactItem) {
        return prefillState.terminalizeCommittedItem(exactItem);
    }

    /**
     * End the exact failed member, whether still queued or committed to a batch.
     */
    public void settleFailedRequest(RequestRoute exactItem) {
        removeQueued(exactItem, "REQUEST_FAILED");
        releaseCommittedItem(exactItem);
    }

    /** One consistent snapshot of batch, individual and total local ownership. */
    public PrefillState.Stats ownershipStats() {
        return prefillState.stats();
    }

    @Override
    public Runnable applyPreparedStatus(
            WorkerStatus ws,
            WorkerStatus.PreparedStatus prepared) {
        requireStatusGeneration(ws);
        WorkerStatus.StatusObservation observation = prepared.observation();
        PrefillState.StatusReconciliation reconciliation =
                prefillState.reconcileWorkerStatus(
                        observation,
                        this::predictRepackedBatchMs,
                        () -> {
                            if (!observation.alive()) {
                                beginRetirement();
                            }
                            signalSchedulingInputsChanged();
                            ws.publishPreparedStatus(prepared);
                        },
                        this::beginRetirement);
        reportBatchCompletionsNoFail(reconciliation.batchCompletions());
        Failures.rethrow(reconciliation.publicationFailure(), "Prefill status publication failed");
        List<PrefillState.WorkerStatusFact> facts =
                reconciliation.schedulerFacts();
        return () -> facts.forEach(fact -> fact.item().ctx().scheduler().onPrefillStatus(
                this, observation.role(), List.of(fact)));
    }

    @Override
    public Runnable initializeFromPreparedStatus(
            WorkerStatus ws,
            WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        PrefillState.StatusReconciliation reconciliation =
                prefillState.reconcileWorkerStatus(
                        observation,
                        this::predictRepackedBatchMs,
                        this::signalSchedulingInputsChanged,
                        this::beginRetirement);
        if (!reconciliation.schedulerFacts().isEmpty()
                || !reconciliation.batchCompletions().isEmpty()) {
            throw new IllegalStateException(
                    "Private Prefill candidate produced locally-owned status facts");
        }
        Failures.rethrow(reconciliation.publicationFailure(), "Prefill status publication failed");
        return () -> { };
    }

    @Override
    public Runnable observeStatusHeartbeat(
            WorkerStatus ws,
            WorkerStatus.StatusObservation observation) {
        requireStatusGeneration(ws);
        if (observation.owner() != ws) {
            throw new IllegalArgumentException(
                    "Status observation belongs to another Prefill generation");
        }
        PrefillState.HeartbeatReconciliation reconciliation =
                prefillState.reconcileHeartbeat(observation);
        if (reconciliation.schedulingInputsChanged()) {
            signalSchedulingInputsChanged();
        }
        return () -> reconciliation.schedulerFacts().forEach(fact -> fact.item().ctx().scheduler().onPrefillStatus(
                this, observation.role(), List.of(fact)));
    }

    private void reportBatchCompletionsNoFail(
            List<PrefillState.BatchCompletion> completions) {
        try {
            completions.forEach(this::reportBatchCompletion);
        } catch (Throwable reportingFailure) {
            try {
                logger.warn("Prefill status committed but completion reporting failed: engine={}",
                        getIp(), reportingFailure);
            } catch (Throwable ignoredLoggingFailure) {
                // Status facts must still reach the exact scheduler projection.
            }
        }
    }

    /**
     * Re-estimate surviving members, using the default formula if the configured predictor fails.
     */
    private long predictRepackedBatchMs(List<RequestRoute> survivingRequests) {
        PrefillBatchFeatures features = PrefillBatchFeatures.from(
                survivingRequests,
                item -> Math.max(0L, item.seqLen()),
                item -> Math.max(0L, Math.min(item.hitCache(), item.seqLen())));
        try {
            return PrefillPredictionBoundary.predictCommittedBatchMs(predictor.evaluator(), features);
        } catch (RuntimeException predictionFailure) {
            try {
                logger.error("Prefill batch repack prediction failed; using default formula "
                                + "engine={} surviving_requests={}",
                        getIp(), survivingRequests.size(), predictionFailure);
            } catch (RuntimeException ignoredLoggingFailure) {
                // Prediction logging cannot block membership settlement.
            }
            return PrefillPredictionBoundary.predictCommittedBatchMs(DEFAULT_BATCH_PREDICTOR, features);
        }
    }

    // ==================== Ownership diagnostics ====================
    /**
     * Diagnostic snapshot of canonical local plus worker-reported ownership.
     */
    public long observedRequestCount() {
        return prefillState.observedRequestCount();
    }

    /**
     * Evict endpoint orphans while retaining IDs still registered by the scheduler.
     */
    public int evictExpiredInflight(long ttlMs,
                                    LongPredicate retainForSchedulerCleanup) {
        return prefillState.evictExpiredInflight(ttlMs, retainForSchedulerCleanup);
    }

    @Override
    public OptionalLong getLoadMetric() {
        return prefillState.committedSnapshot()
                .totalRemainingWorkMs();
    }

    public PrefillTimePredictor getPredictor() {
        return predictor;
    }

    // ==================== Metrics ====================
    /**
     * Report per-worker batch metrics via the given reporter.
     * Called periodically by {@link org.flexlb.balance.scheduler.RequestRepository}.
     */
    public void reportBatchMetrics(BatchSchedulerReporter reporter) {
        int queueSize = queuedRequestCount();
        reporter.reportBatcherQueueSize(RoleType.PREFILL.name(), getIp(), queueSize);
        // Priority-bucketed batch queue length — single-report with priority tag.
        // Empty queue fallback: report priority=0 depth=0 so tagged panels don't gap.
        Map<Integer, Integer> sizeByPriority =
                runtime == null ? Map.of() : runtime.queueSizeByPriority();
        if (sizeByPriority.isEmpty()) {
            reporter.reportBatcherQueueDepthByPriority(RoleType.PREFILL.name(), getIp(), 0, 0);
        } else {
            sizeByPriority.forEach((priority, size) ->
                    reporter.reportBatcherQueueDepthByPriority(RoleType.PREFILL.name(), getIp(), priority, size));
        }
        reporter.reportPrefillInflight(getIp(), ownershipStats());
    }

    private void reportBatchCompletion(
            PrefillState.BatchCompletion completion) {
        long batchId = completion.batchId();
        long actualMs = completion.actualWorkMs();
        if (!completion.successfulCompletion() || actualMs <= 0) {
            logger.debug("batch completion not reportable: batchId={} success={} actualMs={}",
                    batchId, completion.successfulCompletion(), actualMs);
            return;
        }

        long predictedMs = completion.predictedWorkMs();
        long gapMs = actualMs - predictedMs;
        org.flexlb.util.Logger.debug(
                "flexlb_batch_complete batch_id={} predicted_ms={} actual_ms={} gap_ms={} batch_size={} engine={}",
                batchId, predictedMs, actualMs, gapMs,
                completion.originalFeatures().batchSize(), getIp());

        // A failed/removed member makes the original batch an invalid learning
        // sample even if another member completed successfully.
        if (completion.learningEligible()) {
            try {
                PrefillTimePredictor.LearningResult learningResult = predictor.learn(
                        completion.originalFeatures(), predictedMs, actualMs);
                if (learningResult
                        == PrefillTimePredictor.LearningResult.MODEL_UPDATED) {
                    signalSchedulingInputsChanged();
                }
            } catch (RuntimeException learningFailure) {
                logger.warn("batch predictor learning failed after settlement: batchId={} engine={}",
                        batchId, getIp(), learningFailure);
            }
        }

        try {
            reporter.reportBatchCompletion(getIp(), batchId, predictedMs, actualMs);
        } catch (RuntimeException telemetryFailure) {
            logger.warn("batch completion metrics failed: batchId={} engine={}",
                    batchId, getIp(), telemetryFailure);
        }
    }
}
