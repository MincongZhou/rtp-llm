package org.flexlb.balance.scheduler;

import org.flexlb.balance.preemption.PreemptionCancelPhase;
import org.flexlb.balance.preemption.CancelTarget;
import org.flexlb.balance.preemption.VictimResolution;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Exact ownership token for one priority-preemption attempt.
 *
 * <p>This class records the attempt-local cancel protocol. BalanceContext decides
 * which request transitions are legal; its scheduler executes the resulting effects.
 * Cancel acknowledgement, request resolution and Decode release proof remain separate facts.</p>
 */
public final class PreemptionRegistration {
    final BalanceContext owner;
    private final long attemptToken;
    private final String detail;
    private final CancelTarget cancelTarget;
    private final CompletableFuture<VictimResolution> resolution =
            new CompletableFuture<>();

    private PreemptionCancelPhase phase = PreemptionCancelPhase.CLAIMED;
    private boolean finished;
    private boolean cancelAcknowledged;
    private DeferredTerminal pendingTerminal;
    private boolean pendingDeliveryConfirmation;

    PreemptionRegistration(
            BalanceContext owner,
            long attemptToken,
            String detail,
            CancelTarget cancelTarget) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.attemptToken = attemptToken;
        this.detail = detail == null ? "priority preemption" : detail;
        this.cancelTarget = Objects.requireNonNull(cancelTarget, "cancelTarget");
    }

    public AbstractRequestScheduler scheduler() { return owner.scheduler(); }

    public CancelTarget cancelTarget() { return cancelTarget; }

    public long requestId() {
        return owner.getRequestId();
    }

    public long attemptToken() {
        return attemptToken;
    }

    public CompletionStage<VictimResolution> requestResolution() {
        return resolution;
    }

    boolean signalResolution(VictimResolution exactResolution) {
        return resolution.complete(exactResolution);
    }

    String detail() {
        return detail;
    }

    DeferredTerminal pendingTerminal() {
        return pendingTerminal;
    }

    boolean hasPendingDeliveryConfirmation() {
        return pendingDeliveryConfirmation;
    }

    boolean advanceTo(PreemptionCancelPhase next) {
        if (finished || !phase.canTransitionTo(next)) {
            return false;
        }
        phase = next;
        cancelAcknowledged |= next == PreemptionCancelPhase.CANCEL_REQUESTED;
        return true;
    }

    /** Record protocol completion once; its scheduler still owns resource cleanup and resolution notification. */
    boolean tryFinish() {
        if (finished) {
            return false;
        }
        finished = true;
        return true;
    }

    boolean isReleasable() {
        return !finished && phase.isLocallyReleasable();
    }

    boolean isCancelRequested() { return cancelAcknowledged; }

    boolean isNotFound() {
        return !finished && phase == PreemptionCancelPhase.NOT_FOUND_STALE;
    }

    boolean isUnknown() {
        return !finished && phase == PreemptionCancelPhase.CANCEL_UNKNOWN;
    }

    boolean isFinished() {
        return finished;
    }

    boolean canAcceptPriorityTerminal() { return !finished && phase.acceptsPriorityTerminal(); }

    boolean canCompletePreemption() {
        return !finished && phase.acceptsRequestFenced();
    }

    void storeTerminal(DeferredTerminal selected) {
        pendingTerminal = selected;
    }

    void recordDeliveryConfirmation() {
        pendingDeliveryConfirmation = true;
    }
}
