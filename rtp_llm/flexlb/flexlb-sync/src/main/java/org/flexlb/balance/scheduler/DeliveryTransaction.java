package org.flexlb.balance.scheduler;

import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.prediction.PrefillBatchFeatures;
import org.flexlb.balance.prediction.PrefillPredictionBoundary;
import org.flexlb.balance.prediction.PrefillTimePredictor;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.util.Failures;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.missingEndpoint;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.prepareMember;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.preserveRejectedCause;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.rejectedPrefill;
import static org.flexlb.balance.scheduler.PrefillAdmissionResources.rollback;

/** Owns prepared members, then the committed generation handoff, until delivery resolves them. */
final class DeliveryTransaction implements DeliveryStrategy.Transaction {
    // SUBMITTED is used only by BATCH: abort/close must not reclaim executor-owned work.
    private enum Phase { PREPARING, PREPARED, COMMITTED, SUBMITTED, CLOSED }

    /** Resources that exist only for an executor-backed batch. */
    static final class BatchResources {
        long batchId;
        long predictedMs;
        BatchDeliveryStrategy.PreparedSubmission submission;
        PrefillState.BatchReservation reservation;
    }

    private final DeliveryStrategy owner;
    final BatchResources batch;
    final long[] routePredictions;
    final PrefillTimePredictor.Evaluator evaluator;
    final ArrayList<PrefillAdmissionResources.Member> members;
    private final List<RequestRoute> items = new AbstractList<>() {
        @Override public RequestRoute get(int index) { return members.get(index).item(); }
        @Override public int size() { return members.size(); }
    };
    private PrefillEndpoint prefill;
    private PrefillState.CommittedHandoff committed;
    private RequestRoute blockedItem;
    private CapacityBoundary blockedResult;
    private volatile Phase phase = Phase.PREPARING;

    private DeliveryTransaction(DeliveryStrategy owner, List<RequestRoute> candidates,
            PrefillTimePredictor.Evaluator evaluator) {
        this.owner = owner;
        this.evaluator = evaluator;
        members = new ArrayList<>(candidates.size());
        batch = owner instanceof BatchDeliveryStrategy ? new BatchResources() : null;
        routePredictions = batch == null ? new long[candidates.size()] : null;
        if (batch == null) { prefill = candidates.getFirst().prefillEp(); }
    }

    static DeliveryTransaction prepare(DeliveryStrategy owner, List<RequestRoute> candidates,
            PrefillTimePredictor.Evaluator evaluator, OptionalLong plannedPredictionMs) {
        var transaction = new DeliveryTransaction(owner, candidates, evaluator);
        if (transaction.batch == null && transaction.prefill == null) {
            transaction.blockedItem = candidates.getFirst();
            transaction.blockedResult = CapacityBoundary.failed(missingEndpoint("Prefill", candidates.getFirst()));
            transaction.phase = Phase.CLOSED;
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
            if (!transaction.members.isEmpty()) {
                if (transaction.batch != null) {
                    transaction.batch.predictedMs = plannedPredictionMs.isPresent()
                            && transaction.items.size() == candidates.size()
                            ? plannedPredictionMs.getAsLong()
                            : PrefillPredictionBoundary.predictCommittedBatchMs(evaluator,
                                    PrefillBatchFeatures.from(transaction.items, RequestRoute::seqLen, RequestRoute::hitCache));
                }
                transaction.phase = Phase.PREPARED;
            }
            return transaction;
        } catch (Throwable preparationFailure) {
            failure = preparationFailure;
            throw Failures.propagate(failure, transaction.failureMessage());
        } finally {
            if (transaction.phase != Phase.PREPARED) {
                Throwable cleanup = Failures.close(transaction);
                if (failure != null) {
                    Failures.append(failure, cleanup);
                } else if (transaction.batch == null) {
                    Failures.rethrow(cleanup, transaction.failureMessage());
                } else if (cleanup != null) {
                    if (transaction.blockedResult != null) { preserveRejectedCause(cleanup, transaction.blockedResult); }
                    transaction.blockedResult = CapacityBoundary.failed(cleanup);
                }
            }
        }
    }

    /** Called under the exact request monitor; null means the member is prepared. */
    public synchronized CapacityBoundary append(RequestRoute exact) {
        requirePhase(Phase.PREPARING, "append");
        long prediction = batch == null
                ? PrefillPredictionBoundary.predictSingleRequestMs(evaluator, exact.seqLen(), exact.hitCache()) : 0L;
        PrefillAdmissionResources.Member acquired = null;
        try {
            if (batch != null && members.isEmpty()) {
                var strategy = (BatchDeliveryStrategy) owner;
                try {
                    var attempt = Objects.requireNonNull(strategy.prepareSubmission.get(), "submission attempt");
                    if (!attempt.accepted()) { return attempt.boundary(); }
                    batch.submission = attempt.value();
                    batch.batchId = strategy.batchIds.getAsLong();
                    checkState(batch.batchId > 0L, "batch id supplier returned a non-positive id");
                    prefill = exact.prefillEp();
                    if (prefill == null) { throw missingEndpoint("Prefill", exact); }
                } catch (Throwable failure) {
                    return CapacityBoundary.failed(Failures.run(failure, this::closeSubmission));
                }
                var result = prefill.reserveBatch(exact, batch.batchId,
                        exact.requirements().maxInflightBatchesPerPrefillWorker());
                if (result.status() != PrefillState.CapacityStatus.ACQUIRED) {
                    return rejectedPrefill(exact, result.status(), CapacityBoundary.deliveryUnavailable(
                            prefill.batchAdmissionAvailability(exact.requirements().maxInflightBatchesPerPrefillWorker())));
                }
                batch.reservation = result.reservation();
            }
            var attempt = prepareMember(exact);
            if (!attempt.accepted()) { return attempt.boundary(); }
            acquired = attempt.value();
            if (routePredictions != null) { routePredictions[members.size()] = prediction; }
            members.add(acquired);
            return null;
        } catch (Throwable failure) {
            return CapacityBoundary.failed(rollback(acquired, failure));
        }
    }

    @Override public List<RequestRoute> items() { return items; }
    @Override public RequestRoute blockedItem() { return blockedItem; }
    @Override public CapacityBoundary blockedResult() { return blockedResult; }

    @Override
    public synchronized PrefillState.WorkCapture commitLocked() {
        requirePhase(Phase.PREPARED, "commit");
        if (batch != null) {
            committed = batch.reservation.commitLocked(items, batch.predictedMs);
            batch.reservation = null;
            prefill = null;
            phase = Phase.COMMITTED;
            return committed.precedingWork();
        }
        try (var routeCommit = prefill.tryBeginRouteCommitAdmission()) {
            if (routeCommit == null) { throw PrefillAdmissionResources.retired("Prefill", items.getFirst()); }
            committed = routeCommit.commitQueuedLocked(items, Arrays.copyOf(routePredictions, members.size()));
            phase = Phase.COMMITTED;
            return committed.precedingWork();
        }
    }

    synchronized PrefillState.CommittedHandoff takeCommitted() {
        requirePhase(Phase.COMMITTED, "deliver");
        phase = Phase.CLOSED;
        var handoff = committed;
        committed = null;
        return handoff;
    }

    @Override
    public void handoff(String decisionReason, int remainingQueueDepth, WorkSnapshot precedingWork) {
        if (batch == null) {
            ((RouteDeliveryStrategy) owner).deliver(this, remainingQueueDepth,
                    Objects.requireNonNull(precedingWork, "precedingWork"));
            return;
        }
        synchronized (this) {
            requirePhase(Phase.COMMITTED, "submit delivery");
            Objects.requireNonNull(precedingWork, "precedingWork");
            phase = Phase.SUBMITTED;
            try {
                batch.submission.submit(sender -> {
                    requirePhase(Phase.SUBMITTED, "deliver");
                    try {
                        ((BatchDeliveryStrategy) owner).deliverCommitted(this, decisionReason,
                                remainingQueueDepth, precedingWork, sender);
                    } catch (Throwable failure) {
                        failUnsentDelivery(failure);
                        throw Failures.propagate(failure, failureMessage());
                    }
                });
            } catch (Throwable failure) {
                failUnsentDelivery(failure);
                throw Failures.propagate(failure, failureMessage());
            }
        }
    }

    private void failUnsentDelivery(Throwable failure) {
        if (phase == Phase.SUBMITTED) { Failures.append(failure, settleCommitted(failure, null)); }
    }

    @Override
    public void abort(Throwable cause) {
        PrefillState.CommittedHandoff handoff;
        synchronized (this) {
            if (phase != Phase.COMMITTED) { return; }
            if (batch != null) {
                Failures.rethrow(settleCommitted(cause != null ? cause
                        : new IllegalStateException("delivery returned without resolving owner"), null), failureMessage());
                return;
            }
            handoff = takeCommitted();
        }
        Failures.rethrow(settleCommitted(cause, handoff), "route delivery cleanup failed");
    }

    private Throwable settleCommitted(Throwable cause, PrefillState.CommittedHandoff routeHandoff) {
        Throwable cleanup = batch == null ? null : Failures.run(null, this::closeSubmission);
        try {
            for (RequestRoute item : items) {
                cleanup = Failures.run(cleanup, () -> item.ctx().scheduler().failDeliveryPreparation(item, cause));
            }
        } finally {
            if (batch == null) {
                PrefillAdmissionResources.closeCommitted(members, routeHandoff);
            } else {
                cleanup = Failures.run(cleanup, this::closeAdmission);
                phase = Phase.CLOSED;
            }
        }
        return cleanup;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (phase != Phase.PREPARING && phase != Phase.PREPARED) { return; }
            phase = Phase.CLOSED;
            if (batch != null) {
                rollbackPrepared();
                return;
            }
        }
        rollbackPrepared();
    }

    private void rollbackPrepared() {
        Throwable failure = null;
        try {
            for (var member : members) { failure = rollback(member, failure); }
            if (batch != null && batch.reservation != null) {
                failure = Failures.run(failure, () -> prefill.rollbackReservation(batch.reservation));
                batch.reservation = null;
            }
        } finally {
            if (batch != null) { failure = Failures.run(failure, this::closeSubmission); }
        }
        Failures.rethrow(failure, batch == null ? "admission rollback failed" : failureMessage());
    }

    private void closeAdmission() {
        var handoff = committed;
        if (handoff != null) {
            committed = null;
            PrefillAdmissionResources.closeCommitted(members, handoff);
        }
    }

    private void closeSubmission() {
        var submission = batch.submission;
        if (submission != null) {
            batch.submission = null;
            submission.close();
        }
    }

    /** Close temporary ownership before DispatchGate is allowed to process early callbacks. */
    Throwable finishDelivery() {
        Throwable cleanup = Failures.run(null, this::closeAdmission);
        try {
            return Failures.run(cleanup, this::closeSubmission);
        } finally {
            phase = Phase.CLOSED;
        }
    }

    private String failureMessage() { return batch == null ? "route delivery failed" : "batch delivery failed"; }

    private void requirePhase(Phase expected, String operation) {
        if (phase != expected) {
            throw new IllegalStateException(batch == null ? "expected " + expected + " route admission, was " + phase
                    : "cannot " + operation + " batch transaction in " + phase);
        }
    }
}
