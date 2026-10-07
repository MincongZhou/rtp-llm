package org.flexlb.balance.scheduler;

import org.flexlb.balance.preemption.PreemptionCancelPhase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreemptionRegistrationTest {

    @Test
    void acceptedCancelFollowsTheSingleLegalPath() {
        PreemptionRegistration registration = registration();

        assertFalse(registration.canAcceptPriorityTerminal());
        assertTrue(registration.advanceTo(
                PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(registration.canAcceptPriorityTerminal());
        assertFalse(registration.advanceTo(
                PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(registration.advanceTo(
                PreemptionCancelPhase.CANCEL_REQUESTED));
        assertFalse(registration.advanceTo(
                PreemptionCancelPhase.CANCEL_REQUESTED));
        assertFalse(registration.advanceTo(
                PreemptionCancelPhase.NOT_FOUND_STALE));
        assertTrue(registration.advanceTo(
                PreemptionCancelPhase.CANCEL_UNKNOWN));
        assertTrue(registration.isUnknown());
        assertTrue(registration.canCompletePreemption());
        assertTrue(registration.tryFinish());
        assertFalse(registration.tryFinish());
        assertTrue(registration.isFinished());
        assertFalse(registration.canAcceptPriorityTerminal());
    }

    @Test
    void notFoundRetainsTheAttemptUntilEvidenceOrRequestExpiryFinishesIt() {
        PreemptionRegistration registration = registration();

        assertTrue(registration.advanceTo(
                PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(registration.advanceTo(
                PreemptionCancelPhase.NOT_FOUND_STALE));
        assertFalse(registration.advanceTo(
                PreemptionCancelPhase.CANCEL_UNKNOWN));
        assertTrue(registration.isNotFound());
        assertFalse(registration.isReleasable());
        assertTrue(registration.canCompletePreemption());
        assertTrue(registration.tryFinish());
        assertFalse(registration.canCompletePreemption());
        assertFalse(registration.advanceTo(PreemptionCancelPhase.CANCEL_IN_FLIGHT));
    }

    @Test
    void aClaimCanBeReleasedBeforeCancelIsAccepted() {
        PreemptionRegistration claimed = registration();
        PreemptionRegistration inFlight = registration();

        assertTrue(claimed.isReleasable());
        assertTrue(inFlight.advanceTo(
                PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(inFlight.isReleasable());
        assertTrue(inFlight.advanceTo(
                PreemptionCancelPhase.CANCEL_REQUESTED));
        assertFalse(inFlight.isReleasable());
    }

    @Test
    void requestIdentitySurvivesChangesToTheOriginalInput() {
        PreemptionRegistration registration = registration();
        registration.owner.getRequest().setRequestId(99L);

        assertEquals(7L, registration.requestId());
    }

    private static PreemptionRegistration registration() {
        BalanceContext context = RequestProtocolTestSupport.context(SchedulingTestConfig.newConfig(), 7L);
        context.activate(new BalanceContext.RequestFuture((completion, response, failure, interrupt) -> false));
        return new PreemptionRegistration(context, 11L, "test preemption", new org.flexlb.balance.preemption.CancelTarget("127.0.0.1", 8090));
    }
}
