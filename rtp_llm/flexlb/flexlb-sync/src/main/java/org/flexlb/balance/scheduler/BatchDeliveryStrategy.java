package org.flexlb.balance.scheduler;

import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.planner.GroupPlanner;
import org.flexlb.balance.prediction.PrefillBatchFeatures;
import org.flexlb.balance.prediction.PrefillPredictionBoundary;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.projection.RouteProjection;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryClaim;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Failures;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.flexlb.balance.scheduler.PrefillAdmissionResources.missingEndpoint;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.prepareMember;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.preserveRejectedCause;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.rejectedPrefill;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.rollback;

/** EnqueueBatch admission, ownership, transport, telemetry, and projection. */
public final class BatchDeliveryStrategy implements DeliveryStrategy {

    private static final RouteProjection.DeliveryProjection PROJECTION =
            new BatchProjection();
    private final Supplier<CapacityBoundary.Attempt<PreparedSubmission>>
            prepareSubmission;
    private final LongSupplier batchIds;
    private final BatchSchedulerReporter telemetry;

    public BatchDeliveryStrategy(
            Supplier<CapacityBoundary.Attempt<PreparedSubmission>>
                    prepareSubmission,
            LongSupplier batchIds,
            BatchSchedulerReporter telemetry) {
        this.prepareSubmission = Objects.requireNonNull(
                prepareSubmission, "prepareSubmission");
        this.batchIds = Objects.requireNonNull(batchIds, "batchIds");
        this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
    }

    @Override
    public Transaction prepare(
            List<RequestRoute> candidates,
            PrefillTimePredictor.Evaluator evaluator,
            OptionalLong plannedPredictionMs) {
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException(
                    "batch delivery requires at least one candidate");
        }
        BatchTransaction transaction = new BatchTransaction(this, candidates.size());
        Throwable failure = null;
        try {
            for (RequestRoute item : candidates) {
                CapacityBoundary boundary = item.ctx().scheduler().prepareDispatch(item, transaction);
                if (boundary != null) {
                    transaction.blockedItem = item;
                    transaction.blockedResult = boundary;
                    break;
                }
            }
            if (!transaction.items.isEmpty()) {
                transaction.predictedMs = plannedPredictionMs.isPresent()
                        && transaction.items.size() == candidates.size()
                        ? plannedPredictionMs.getAsLong()
                        : PrefillPredictionBoundary.predictCommittedBatchMs(evaluator,
                                PrefillBatchFeatures.from(transaction.items,
                                        RequestRoute::seqLen, RequestRoute::hitCache));
                transaction.evaluator = evaluator;
                transaction.phase = BatchTransaction.Phase.PREPARED;
            }
            return transaction;
        } catch (Throwable preparationFailure) {
            failure = preparationFailure;
            throw Failures.propagate(failure, "batch delivery failed");
        } finally {
            if (transaction.phase != BatchTransaction.Phase.PREPARED) {
                Throwable cleanup = Failures.close(transaction);
                if (failure != null) {
                    Failures.append(failure, cleanup);
                } else if (cleanup != null) {
                    if (transaction.blockedResult != null) {
                        preserveRejectedCause(cleanup, transaction.blockedResult);
                    }
                    transaction.blockedResult = CapacityBoundary.failed(cleanup);
                }
            }
        }
    }

    @Override
    public GroupPlanner.PrefixPrediction<RequestRoute> newGroupPredictor(
            PrefillTimePredictor.Evaluator evaluator) {
        PrefillTimePredictor.BatchPrediction prediction = evaluator.newBatchPrediction();
        return (added, items) -> {
            return PrefillPredictionBoundary.requireValidDecisionGroupMs(
                    prediction.append(added.seqLen(), added.hitCache()));
        };
    }

    @Override
    public RouteProjection.DeliveryProjection projectionPolicy() {
        return PROJECTION;
    }

    private void deliverCommitted(
            BatchTransaction batch,
            String decisionReason,
            int remainingQueueDepth,
            WorkSnapshot precedingWork,
            BatchSender sender) {
        List<RequestRoute> original = batch.items();
        List<DeliveryClaim> claimed = new ArrayList<>(original.size());
        List<RequestRoute> submitted = List.of();
        DispatchGate gate = null;
        Throwable handoffFailure = null;
        long deliveredPredictionMs = batch.predictedMs;
        try {
            for (var member : batch.members) {
                RequestRoute item = member.item();
                try {
                    DeliveryClaim claim =
                            item.ctx().scheduler().claimDelivery(item, DeliveryClaimKind.BATCH_ENQUEUE,
                                    batch.batchId, member);
                    if (claim == null) {
                        continue;
                    }
                    claimed.add(claim);
                } catch (Throwable claimFailure) {
                    item.ctx().scheduler().failDeliveryPreparation(item, claimFailure);
                }
            }

            if (!claimed.isEmpty()) {
                submitted = List.of(claimed.stream()
                        .map(claim -> claim.item)
                        .toArray(RequestRoute[]::new));
                if (submitted.size() != original.size()) {
                    deliveredPredictionMs =
                            PrefillPredictionBoundary.predictCommittedBatchMs(
                                    batch.evaluator,
                                    PrefillBatchFeatures.from(
                                            submitted,
                                            RequestRoute::seqLen,
                                            RequestRoute::hitCache));
                }
                for (DeliveryClaim claim : claimed) {
                    claim.item.ctx().scheduler().setDeliveryPrediction(claim, precedingWork, deliveredPredictionMs);
                }
                gate = new DispatchGate(
                        claimed);
                if (remainingQueueDepth < 0) {
                    throw new IllegalArgumentException(
                            "remainingQueueDepth must be non-negative");
                }
                sender.sendBatch(submitted, batch.batchId, deliveredPredictionMs,
                        decisionReason, gate);
                batch.phase = BatchTransaction.Phase.INFLIGHT;
            }
        } catch (Throwable failure) {
            handoffFailure = failure;
        } finally {
            Throwable cleanup = Failures.run(null, batch::closeAdmission);
            cleanup = Failures.run(cleanup, batch::closeSubmission);
            handoffFailure = Failures.append(handoffFailure, cleanup);
            if (gate != null) {
                handoffFailure = Failures.run(handoffFailure, gate::open);
            }
        }

        if (handoffFailure != null) {
            if (batch.phase == BatchTransaction.Phase.INFLIGHT) {
                throw Failures.propagate(handoffFailure, "batch delivery failed");
            } else {
                Throwable completionFailure = null;
                for (DeliveryClaim claim : claimed) {
                    try {
                        claim.complete(DeliveryResult.notSent(handoffFailure));
                    } catch (Throwable failure) {
                        completionFailure = Failures.append(completionFailure, failure);
                    }
                }
                batch.phase = BatchTransaction.Phase.TERMINAL;
                if (completionFailure != null) {
                    throw Failures.propagate(Failures.append(
                            handoffFailure, completionFailure), "batch delivery failed");
                }
            }
        }

        if (batch.phase == BatchTransaction.Phase.INFLIGHT) {
            telemetry.reportDelivery(
                    batch.batchId,
                    decisionReason,
                    remainingQueueDepth,
                    submitted,
                    deliveredPredictionMs);
        }
    }

    /** One owner and one explicit state machine for the complete batch flow. */
    static final class BatchTransaction implements Transaction, PrefillAdmissionResources.Preparation {
        private enum Phase {
            PREPARING,
            PREPARED,
            COMMITTED,
            SUBMITTED,
            INFLIGHT,
            TERMINAL
        }

        private final BatchDeliveryStrategy owner;
        private long batchId;
        private PreparedSubmission submission;
        private PrefillState.BatchReservation reservation;
        private final ArrayList<PrefillAdmissionResources.Member> members;
        private PrefillState.CommittedHandoff committedHandoff;
        private final List<RequestRoute> items = new AbstractList<>() {
            @Override public RequestRoute get(int index) { return members.get(index).item(); }
            @Override public int size() { return members.size(); }
        };
        private long predictedMs;
        private PrefillTimePredictor.Evaluator evaluator;
        private RequestRoute blockedItem;
        private CapacityBoundary blockedResult;
        // After SUBMITTED, only the accepted executor task advances this
        // transaction. Scheduler abort/close may observe it but cannot reclaim it.
        private volatile Phase phase;

        private BatchTransaction(BatchDeliveryStrategy owner, int candidateCount) {
            this.owner = owner;
            this.members = new ArrayList<>(candidateCount);
            this.phase = Phase.PREPARING;
        }

        public synchronized CapacityBoundary append(RequestRoute exact) {
            requirePhase(Phase.PREPARING, "append");
            try {
                if (members.isEmpty()) {
                    PrefillEndpoint prefill;
                    try {
                        var attempt = Objects.requireNonNull(owner.prepareSubmission.get(), "submission attempt");
                        if (!attempt.accepted()) { return attempt.boundary(); }
                        submission = attempt.value();
                        batchId = owner.batchIds.getAsLong();
                        if (batchId <= 0L) {
                            throw new IllegalStateException("batch id supplier returned a non-positive id");
                        }
                        prefill = exact.prefillEp();
                        if (prefill == null) { throw missingEndpoint("Prefill", exact); }
                    } catch (Throwable failure) {
                        return CapacityBoundary.failed(Failures.run(failure, this::closeSubmission));
                    }
                    var result = prefill.reserveBatch(exact, batchId,
                            exact.requirements().maxInflightBatchesPerPrefillWorker());
                    if (result.status() != PrefillState.CapacityStatus.ACQUIRED) {
                        return rejectedPrefill(exact, result.status(), CapacityBoundary.deliveryUnavailable(
                                prefill.batchAdmissionAvailability(exact.requirements().maxInflightBatchesPerPrefillWorker())));
                    }
                    reservation = result.reservation();
                }
                var member = prepareMember(exact);
                if (!member.accepted()) {
                    return member.boundary();
                }
                members.add(member.value());
                return null;
            } catch (Throwable failure) {
                return CapacityBoundary.failed(failure);
            }
        }

        @Override
        public List<RequestRoute> items() {
            return items;
        }

        @Override
        public RequestRoute blockedItem() {
            return blockedItem;
        }

        @Override
        public CapacityBoundary blockedResult() {
            return blockedResult;
        }

        @Override
        public synchronized PrefillState.WorkCapture commitLocked() {
            requirePhase(Phase.PREPARED, "commit");
            PrefillState.CommittedHandoff handoff =
                    reservation.commitLocked(items, predictedMs);
            committedHandoff = handoff;
            reservation = null;
            phase = Phase.COMMITTED;
            return handoff.precedingWork();
        }

        @Override
        public synchronized void handoff(
                String decisionReason, int remainingQueueDepth,
                WorkSnapshot precedingWork) {
            requirePhase(Phase.COMMITTED, "submit delivery");
            Objects.requireNonNull(precedingWork, "precedingWork");
            phase = Phase.SUBMITTED;
            try {
                // The strategy retains admission ownership while waiting for
                // an executor. Request claims are acquired only when it runs.
                submission.submit(sender -> deliver(
                        decisionReason, remainingQueueDepth, precedingWork, sender));
            } catch (Throwable failure) {
                failUnsentDelivery(failure);
                throw Failures.propagate(failure, "batch delivery failed");
            }
        }

        private void deliver(
                String decisionReason, int remainingQueueDepth,
                WorkSnapshot precedingWork, BatchSender sender) {
            requirePhase(Phase.SUBMITTED, "deliver");
            try {
                owner.deliverCommitted(this, decisionReason, remainingQueueDepth,
                        precedingWork, sender);
            } catch (Throwable failure) {
                failUnsentDelivery(failure);
                throw Failures.propagate(failure, "batch delivery failed");
            }
            if (phase == Phase.SUBMITTED) {
                phase = Phase.TERMINAL;
            }
        }

        private void failUnsentDelivery(Throwable failure) {
            if (phase == Phase.SUBMITTED) {
                Failures.append(failure, failCommitted(failure));
            }
        }

        @Override
        public synchronized void abort(Throwable cause) {
            if (phase != Phase.COMMITTED) {
                return;
            }
            Failures.rethrow(failCommitted(cause != null ? cause
                    : new IllegalStateException("delivery returned without resolving owner")),
                    "batch delivery failed");
        }

        private Throwable failCommitted(Throwable cause) {
            Throwable cleanup = Failures.run(null, this::closeSubmission);
            try {
                for (RequestRoute item : items) {
                    cleanup = Failures.run(cleanup,
                            () -> item.ctx().scheduler().failDeliveryPreparation(item, cause));
                }
            } finally {
                cleanup = Failures.run(cleanup, this::closeAdmission);
                phase = Phase.TERMINAL;
            }
            return cleanup;
        }

        @Override
        public synchronized void close() {
            if (phase != Phase.PREPARING && phase != Phase.PREPARED) {
                return;
            }
            phase = Phase.TERMINAL;
            Throwable failure = null;
            try {
                for (var member : members) {
                    failure = rollback(member, failure);
                }
                failure = rollback(reservation, failure);
                reservation = null;
            } finally {
                failure = Failures.run(failure, this::closeSubmission);
            }
            Failures.rethrow(failure, "batch delivery failed");
        }

        private void closeAdmission() {
            PrefillState.CommittedHandoff handoff = committedHandoff;
            if (handoff != null) {
                committedHandoff = null;
                PrefillAdmissionResources.closeCommitted(members, handoff);
            }
        }

        private void closeSubmission() {
            PreparedSubmission exactSubmission = submission;
            if (exactSubmission != null) {
                submission = null;
                exactSubmission.close();
            }
        }

        private void requirePhase(Phase expected, String operation) {
            if (phase != expected) {
                throw new IllegalStateException(
                        "cannot " + operation + " batch transaction in " + phase);
            }
        }
    }

    /**
     * One executor-capacity permit prepared before commit. Successful submission
     * transfers the permit to the executor; close then becomes a no-op. The
     * strategy still owns the batch admission until Delivery runs and settles it.
     */
    public interface PreparedSubmission extends AutoCloseable {
        void submit(Delivery delivery);

        @Override
        void close();
    }

    /** Runs once on the dispatch executor, with no further queue before send. */
    @FunctionalInterface
    public interface Delivery {
        void run(BatchSender sender);
    }

    /** Transport consumes the strategy's final batch; it never selects members. */
    @FunctionalInterface
    public interface BatchSender {
        void sendBatch(
                List<RequestRoute> exactItems,
                long batchId,
                long predictedMs,
                String decisionReason,
                BiConsumer<RequestRoute, DeliveryResult> observer);
    }

    private static final class DispatchGate
            implements BiConsumer<RequestRoute, DeliveryResult> {
        private final Map<RequestRoute, DeliveryClaim>
                claimsByItem;
        private boolean deferred = true;
        private List<Event> events;

        private DispatchGate(
                List<DeliveryClaim> members) {

            this.claimsByItem = new IdentityHashMap<>(members.size());
            for (DeliveryClaim claim : members) {
                DeliveryClaim previous = claimsByItem.put(
                        claim.item, claim);
                if (previous != null) {
                    throw new IllegalArgumentException(
                            "duplicate batch delivery identity");
                }
            }
        }

        private void open() {
            List<Event> pending;
            synchronized (this) {
                deferred = false;
                pending = events;
                events = null;
            }
            if (pending != null) {
                Throwable failure = null;
                for (Event event : pending) {
                    failure = Failures.run(failure,
                            () -> invoke(event.item(), event.completion()));
                }
                if (failure != null) {
                    throw Failures.propagate(failure, "batch delivery failed");
                }
            }
        }

        @Override
        public void accept(
                RequestRoute item,
                DeliveryResult completion) {
            synchronized (this) {
                if (deferred) {
                    if (events == null) {
                        events = new ArrayList<>();
                    }
                    events.add(new Event(item, completion));
                    return;
                }
            }
            invoke(item, completion);
        }

        private void invoke(RequestRoute item, DeliveryResult completion) {
            DeliveryClaim claim = claimsByItem.get(item);
            if (claim == null) {
                throw new IllegalStateException(
                        "batch completion referenced an unsubmitted identity");
            }
            claim.complete(completion);
        }
    }

    private record Event(
            RequestRoute item,
            DeliveryResult completion) {
    }

    private static final class BatchProjection
            implements RouteProjection.DeliveryProjection {

        @Override
        public long singletonCompletionOffsetMs(
                long seqLen,
                long hitCache,
                RouteProjection.Predictions predictions) {
            // Committed prediction performs the same validated decision-group
            // evaluation before converting to lifecycle milliseconds. Running
            // the planning call first only evaluates the predictor twice for
            // every endpoint in the full-fleet singleton fast path.
            return predictions.singletonBatchDurationMs(seqLen, hitCache);
        }

        @Override
        public RouteProjection.GroupPlanning planning(
                RouteProjection.Predictions predictions) {
            return new AppendPlanning(predictions.newBatchPrediction());
        }

        @Override
        public long completionOffsetMs(List<GroupPlanner.Item> items, int memberIndex,
                RouteProjection.Predictions predictions, RouteProjection.GroupPlanning planning) {
            Objects.checkIndex(memberIndex, items.size());
            if (planning != null) {
                var cached = planning.predictedPrefixMs(memberIndex + 1);
                if (cached.isPresent()) {
                    return PrefillPredictionBoundary.committedDecisionGroupMs(cached.getAsDouble());
                }
            }
            return predictions.batchDurationMs(items.subList(0, memberIndex + 1));
        }

        /** One planner invocation; prefixes only grow and the probe boundary never moves backwards. */
        private static final class AppendPlanning implements RouteProjection.GroupPlanning {
            private final PrefillTimePredictor.BatchPrediction prediction;
            private int size;
            private double latestPredictionMs;
            private double previousPredictionMs;

            private AppendPlanning(PrefillTimePredictor.BatchPrediction prediction) {
                this.prediction = prediction;
            }

            @Override
            public double durationMs(List<GroupPlanner.Item> prefix, int through) {
                if (through < 0 || through >= prefix.size() || through + 1 < size) {
                    throw new IllegalArgumentException("Prediction requires a growing prefix");
                }
                while (size <= through) {
                    GroupPlanner.Item item = prefix.get(size);
                    double next = prediction.append(item.seqLen(), item.hitCache());
                    previousPredictionMs = latestPredictionMs;
                    latestPredictionMs = next;
                    size++;
                }
                return latestPredictionMs;
            }

            @Override
            public java.util.OptionalDouble predictedPrefixMs(int prefixSize) {
                // Selection uses the latest prefix, or the preceding one when
                // the last append exceeded the budget. Other callers recompute.
                if (prefixSize > 0 && prefixSize == size) {
                    return java.util.OptionalDouble.of(latestPredictionMs);
                }
                if (prefixSize > 0 && prefixSize == size - 1) {
                    return java.util.OptionalDouble.of(previousPredictionMs);
                }
                return java.util.OptionalDouble.empty();
            }
        }

    }
}
