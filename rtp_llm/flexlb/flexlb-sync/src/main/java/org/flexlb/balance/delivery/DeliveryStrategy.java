package org.flexlb.balance.delivery;

import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.planner.GroupPlanner;
import org.flexlb.balance.projection.RouteProjection;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.RequestRoute;

import java.util.List;
import java.util.OptionalLong;

/**
 * Mode-specific part of delivery. Grouping chooses an ordered candidate group;
 * the scheduler coordinates prepare, queue commit, and handoff while this mode
 * owns only its exact capacity and transport resources.
 */
public interface DeliveryStrategy {

    /** Reserve the largest feasible prefix without mutating queue ownership. */
    Transaction prepare(
            List<RequestRoute> candidates,
            PrefillTimePredictor.Evaluator evaluator,
            OptionalLong plannedPredictionMs);

    /** Fresh callback for GroupPlanner's strictly growing prefixes in one select call. */
    GroupPlanner.PrefixPrediction<RequestRoute> newGroupPredictor(
            PrefillTimePredictor.Evaluator evaluator);

    /** Pure projection behavior paired with this live delivery strategy. */
    RouteProjection.DeliveryProjection projectionPolicy();

    /** One delivery transaction across prepare, queue commit, and handoff. */
    interface Transaction extends AutoCloseable {

        List<RequestRoute> items();

        /** First candidate not covered by this transaction, if any. */
        RequestRoute blockedItem();

        CapacityBoundary blockedResult();

        /** Commit ownership and capture preceding work under the same endpoint lock. */
        PrefillState.WorkCapture commitUnderLock();

        /** Transfer committed ownership to the configured delivery mode. */
        void handoff(String decisionReason, int remainingQueueDepth,
                     WorkSnapshot precedingWork);

        /** Resolve untransferred committed ownership; null means handoff returned without an exception. */
        void abort(Throwable cause);

        @Override
        void close();
    }
}
