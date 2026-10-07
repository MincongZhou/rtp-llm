package org.flexlb.balance.scheduler;

import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.planner.GroupPlanner;
import org.flexlb.balance.prediction.PrefillPredictionBoundary;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.projection.RouteProjection;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryClaim;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Failures;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.math.LongMath.saturatedAdd;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.missingEndpoint;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.prepareMember;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.rollback;

/** Individual route admission, ownership, publication, and projection. */
public final class RouteDeliveryStrategy implements DeliveryStrategy {

    private static final RouteProjection.DeliveryProjection PROJECTION =
            new RouteProjectionPolicy();
    private final BatchSchedulerReporter telemetry;

    public RouteDeliveryStrategy(
            BatchSchedulerReporter telemetry) {
        this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
    }

    @Override
    public Transaction prepare(
            List<RequestRoute> candidates,
            PrefillTimePredictor.Evaluator evaluator,
            OptionalLong plannedPredictionMs) {
        checkArgument(!candidates.isEmpty(), "route delivery requires at least one candidate");
        RequestRoute head = candidates.get(0);
        PrefillEndpoint prefill = head.prefillEp();
        RouteTransaction transaction = new RouteTransaction(this, prefill, evaluator, candidates.size());
        if (prefill == null) {
            transaction.blockedItem = head;
            transaction.blockedResult = CapacityBoundary.failed(missingEndpoint("Prefill", head));
            transaction.phase = RouteTransaction.Phase.CLOSED;
            return transaction;
        }
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
            if (!transaction.prepared.isEmpty()) {
                transaction.phase = RouteTransaction.Phase.PREPARED;
            }
            return transaction;
        } catch (Throwable preparationFailure) {
            failure = preparationFailure;
            throw Failures.propagate(failure, "route delivery failed");
        } finally {
            if (transaction.phase != RouteTransaction.Phase.PREPARED) {
                Throwable cleanup = Failures.close(transaction);
                if (failure == null) {
                    Failures.rethrow(cleanup, "route delivery failed");
                } else {
                    Failures.append(failure, cleanup);
                }
            }
        }
    }

    private void deliver(
            RouteTransaction transaction,
            int remainingQueueDepth,
            WorkSnapshot precedingWork) {
        Throwable deliveryFailure = null;
        List<RequestRoute> delivered = new ArrayList<>(transaction.prepared.size());
        List<ClaimedRoute> claimed = new ArrayList<>(transaction.prepared.size());
        PrefillState.CommittedHandoff handoff = transaction.takeCommitted();
        try {
            for (int index = 0; index < transaction.prepared.size(); index++) {
                var prepared = transaction.prepared.get(index);
                RequestRoute item = prepared.item();
                DeliveryClaim claim;
                try {
                    claim = item.ctx().scheduler().claimDelivery(item, DeliveryClaimKind.ROUTE_DECISION, 0L,
                            prepared.member());
                } catch (Throwable claimFailure) {
                    try {
                        item.ctx().scheduler().failDeliveryPreparation(item, claimFailure);
                    } catch (Throwable terminalFailure) {
                        claimFailure.addSuppressed(terminalFailure);
                        deliveryFailure = Failures.append(
                                deliveryFailure, claimFailure);
                    }
                    continue;
                }
                if (claim == null) {
                    continue;
                }
                claimed.add(new ClaimedRoute(claim, prepared.predictedMs()));
            }
            long unstartedWorkMs = 0L;
            for (ClaimedRoute route : claimed) {
                RequestRoute item = route.claim().item;
                try {
                    long itemWorkMs = route.predictedMs();
                    unstartedWorkMs = saturatedAdd(unstartedWorkMs, itemWorkMs);
                    route.claim().item.ctx().scheduler().publishRoute(route.claim(), precedingWork, unstartedWorkMs);
                    delivered.add(item);
                } catch (Throwable completionFailure) {
                    deliveryFailure = Failures.append(
                            deliveryFailure, completionFailure);
                }
            }
        } finally {
            PrefillAdmissionResources.closeCommitted(transaction.members, handoff);
        }
        if (!delivered.isEmpty()) {
            telemetry.reportDelivery(0L, null, remainingQueueDepth, delivered, 0L);
        }
        if (deliveryFailure != null) {
            throw Failures.propagate(deliveryFailure, "route delivery failed");
        }
    }

    private record ClaimedRoute(DeliveryClaim claim, long predictedMs) { }

    @Override
    public GroupPlanner.PrefixPrediction<RequestRoute> newGroupPredictor(
            PrefillTimePredictor.Evaluator evaluator) {
        return new GroupPlanner.PrefixPrediction<>() {
            private double totalMs;

            @Override
            public double append(RequestRoute added, List<RequestRoute> items) {
                totalMs += PrefillPredictionBoundary.predictSingleRequestMs(
                        evaluator, added.seqLen(), added.hitCache());
                return PrefillPredictionBoundary.requireValidDecisionGroupMs(totalMs);
            }
        };
    }

    @Override
    public RouteProjection.DeliveryProjection projectionPolicy() {
        return PROJECTION;
    }

    /** One ordered list owns each prepared member and its frozen prediction. */
    static final class RouteTransaction implements Transaction, PrefillAdmissionResources.Preparation {
        private enum Phase { PREPARING, PREPARED, COMMITTED, CLOSED }

        private record PreparedRoute(PrefillAdmissionResources.Member member,
                long predictedMs) {
            RequestRoute item() { return member.item(); }
        }

        private final RouteDeliveryStrategy owner;
        private final PrefillEndpoint prefill;
        private final ArrayList<PreparedRoute> prepared;
        private final List<PrefillAdmissionResources.Member> members = new AbstractList<>() {
            @Override public PrefillAdmissionResources.Member get(int index) { return prepared.get(index).member(); }
            @Override public int size() { return prepared.size(); }
        };
        private final List<RequestRoute> items = new AbstractList<>() {
            @Override public RequestRoute get(int index) { return prepared.get(index).item(); }
            @Override public int size() { return prepared.size(); }
        };
        private RequestRoute blockedItem;
        private CapacityBoundary blockedResult;
        private Phase phase = Phase.PREPARING;
        private PrefillState.CommittedHandoff committed;

        private final PrefillTimePredictor.Evaluator evaluator;

        private RouteTransaction(RouteDeliveryStrategy owner, PrefillEndpoint prefill, PrefillTimePredictor.Evaluator evaluator, int candidateCount) {
            this.prepared = new ArrayList<>(candidateCount);
            this.evaluator = evaluator;
            this.owner = owner;
            this.prefill = prefill;
        }

        public synchronized CapacityBoundary append(RequestRoute item) {
            requirePhase(Phase.PREPARING);
            long predictedMs = PrefillPredictionBoundary.predictSingleRequestMs(
                        evaluator, item.seqLen(), item.hitCache());
            PrefillAdmissionResources.Member member = null;
            try {
                var attempt = prepareMember(item);
                if (!attempt.accepted()) { return attempt.boundary(); }
                member = attempt.value();
                prepared.add(new PreparedRoute(member, predictedMs));
                return null;
            } catch (Throwable failure) {
                return CapacityBoundary.failed(rollback(member, failure));
            }
        }

        @Override public List<RequestRoute> items() { return items; }
        @Override public RequestRoute blockedItem() { return blockedItem; }
        @Override public CapacityBoundary blockedResult() { return blockedResult; }

        @Override
        public synchronized PrefillState.WorkCapture commitLocked() {
            requirePhase(Phase.PREPARED);
            try (var routeCommit = prefill.tryBeginRouteCommitAdmission()) {
                if (routeCommit == null) { throw PrefillAdmissionResources.retired("Prefill", items.getFirst()); }
                long[] predictions = new long[prepared.size()];
                for (int index = 0; index < predictions.length; index++) {
                    predictions[index] = prepared.get(index).predictedMs();
                }
                var handoff = routeCommit.commitQueuedLocked(items, predictions);
                committed = handoff;
                phase = Phase.COMMITTED;
                return handoff.precedingWork();
            }
        }

        private synchronized PrefillState.CommittedHandoff takeCommitted() {
            requirePhase(Phase.COMMITTED);
            phase = Phase.CLOSED;
            var admission = committed;
            committed = null;
            return admission;
        }

        @Override
        public void handoff(String decisionReason, int remainingQueueDepth, WorkSnapshot precedingWork) {
            owner.deliver(this, remainingQueueDepth,
                    Objects.requireNonNull(precedingWork, "precedingWork"));
        }

        @Override
        public void abort(Throwable cause) {
            PrefillState.CommittedHandoff handoff;
            synchronized (this) {
                if (phase != Phase.COMMITTED) { return; }
                handoff = takeCommitted();
            }
            Throwable failure = null;
            try {
                for (RequestRoute item : items) {
                    failure = Failures.run(failure, () -> item.ctx().scheduler().failDeliveryPreparation(item, cause));
                }
            } finally {
                PrefillAdmissionResources.closeCommitted(members, handoff);
            }
            Failures.rethrow(failure, "route delivery cleanup failed");
        }

        @Override
        public void close() {
            synchronized (this) {
                if (phase != Phase.PREPARING && phase != Phase.PREPARED) { return; }
                phase = Phase.CLOSED;
            }
            Throwable failure = null;
            for (PreparedRoute route : prepared) { failure = rollback(route.member(), failure); }
            Failures.rethrow(failure, "admission rollback failed");
        }

        private void requirePhase(Phase expected) {
            checkState(phase == expected, "expected %s route admission, was %s", expected, phase);
        }
    }

    private static final class RouteProjectionPolicy
            implements RouteProjection.DeliveryProjection {

        private static final ThreadLocal<RouteCursor> PLANNING =
                ThreadLocal.withInitial(RouteCursor::new);

        @Override
        public long singletonCompletionOffsetMs(
                long seqLen,
                long hitCache,
                RouteProjection.Predictions predictions) {
            return predictions.itemDurationMs(seqLen, hitCache);
        }

        @Override
        public RouteProjection.GroupPlanning planning(
                RouteProjection.Predictions predictions) {
            RouteCursor planning = PLANNING.get();
            planning.reset(predictions);
            return planning;
        }

        @Override
        public long completionOffsetMs(List<GroupPlanner.Item> items, int memberIndex,
                RouteProjection.Predictions predictions, RouteProjection.GroupPlanning planning) {
            Objects.checkIndex(memberIndex, items.size());
            // GroupPlanning belongs to this invocation and its exact selected prefix.
            if (planning instanceof RouteCursor cursor && cursor.predictions == predictions) {
                long cached = cursor.cachedDurationMs(memberIndex);
                if (cached >= 0L) { return cached; }
            }
            long durationMs = 0L;
            for (int i = 0; i <= memberIndex; i++) {
                durationMs = saturatedAdd(durationMs, predictions.itemDurationMs(items.get(i)));
            }
            return durationMs;
        }

        private static final class RouteCursor
                implements RouteProjection.GroupPlanning {
            private long durationMs;
            private long previousDurationMs;
            private int computedThrough;
            private RouteProjection.Predictions predictions;

            private void reset(RouteProjection.Predictions exactPredictions) {
                predictions = exactPredictions;
                computedThrough = -1;
                durationMs = 0L;
                previousDurationMs = 0L;
            }

            @Override
            public double durationMs(List<GroupPlanner.Item> prefix, int requiredThroughIndex) {
                if (requiredThroughIndex < 0 || requiredThroughIndex >= prefix.size()) {
                    throw new IndexOutOfBoundsException(requiredThroughIndex);
                }
                checkArgument(requiredThroughIndex >= computedThrough, "planning index must not decrease");
                while (computedThrough < requiredThroughIndex) {
                    int next = computedThrough + 1;
                    long itemMs = predictions.itemDurationMs(prefix.get(next));
                    previousDurationMs = durationMs;
                    durationMs = saturatedAdd(
                            durationMs, itemMs);
                    computedThrough = next;
                }
                return durationMs;
            }

            private long cachedDurationMs(int memberIndex) {
                if (memberIndex == computedThrough) { return durationMs; }
                // The last tentative member may have exceeded the budget and
                // been removed from the selected group.
                if (memberIndex == computedThrough - 1) { return previousDurationMs; }
                return -1L;
            }

        }

    }
}
