package org.flexlb.balance.scheduler;

import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.preemption.PreemptionCancelPhase;
import org.flexlb.balance.scheduler.BalanceContext.AdmissionHandle;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.route.RoleType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicReference;

import static org.flexlb.balance.scheduler.SchedulingTestConfig.freezeInputs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RequestSlotTerminalSettlementTest {

    private static final DecodeEndpoint.ReservationHandle RESERVATION =
            new DecodeEndpoint.ReservationHandle(1L, 2L, 3L);

    @Test
    void latePlacementDiagnosticsCannotOverwriteQueueTimeoutEvidence() {
        var context = RequestProtocolTestSupport.context(SchedulingTestConfig.newConfig(), 42L);
        var queue = mock(QueuedRequestScheduler.class);
        var timeoutEvidence = java.util.Map.<String, Object>of("cause", "DECODE placement unavailable");
        when(queue.getLatestQueueWaitSnapshot()).thenReturn(timeoutEvidence);
        BalanceContext slot = context;
        AbstractRequestScheduler requestOwner = RequestProtocolTestSupport.initialize(mock(RequestCompletionPublisher.class), slot, mock(ExpirationTimer.class));
        org.mockito.Mockito.doReturn(timeoutEvidence).when((QueuedRequestScheduler) requestOwner).getLatestQueueWaitSnapshot();
        var admission = RequestProtocolTestSupport.beginAdmission(requestOwner, slot);
        assertNotNull(admission);
        admission.recordDiagnostics(java.util.Map.of("cause", "earlier placement"));
        assertEquals("earlier placement", context.getSchedulingDiagnostics().get("cause"));
        requestOwner.cancelRequest(slot, 0L, CancelReason.DEADLINE_EXCEEDED);
        admission.recordDiagnostics(java.util.Map.of("cause", "late placement"));
        assertEquals(timeoutEvidence, context.getSchedulingDiagnostics());
        assertFalse(slot.future().isDone(), "cancellation must still wait for the active admission");
    }

    @Test
    void oldDecodeRetirementCannotRemoveANewSchedulingGeneration() {
        var context = RequestProtocolTestSupport.context(SchedulingTestConfig.newConfig(), 4202L);
        BalanceContext slot = context;
        AbstractRequestScheduler requestOwner = RequestProtocolTestSupport.initialize(mock(RequestCompletionPublisher.class), slot, mock(ExpirationTimer.class));
        requestOwner.onDecodeGenerationRetired(mock(DecodeEndpoint.class), java.util.List.of(new DecodeEndpoint.ReservationHandle(1L, 4202L, 7L)));
        requestOwner.runtime.continuations().awaitIdle();
        assertTrue(slot.isOpen());
        assertFalse(slot.getFuture().isDone());
    }

    @Test
    void unknownCancelOutcomeKeepsTheAcknowledgedCancellationVisible() {
        Fixture f = fixture(true, true);
        PreemptionRegistration claim = f.slot().tryInstallPreemption(3L, 4L, "victim");
        assertNotNull(claim);
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_REQUESTED));
        assertEquals(RequestState.Phase.CANCEL_REQUESTED, f.slot().snapshot().state());
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_UNKNOWN));
        assertEquals(RequestState.Phase.CANCEL_REQUESTED, f.slot().snapshot().state());
        assertEquals(BalanceContext.RequestStage.READY_TO_DELIVER, f.slot().stage());
        assertFalse(f.slot().future().isDone());
    }

    @Test
    void queuedCancellationResumesAfterExactPreemptionRelease() {
        Fixture f = fixture(true, true);
        PreemptionRegistration claim = f.slot().tryInstallPreemption(3L, 4L, "victim");
        assertNotNull(claim);
        assertEquals(RequestState.Phase.CANCEL_REQUESTED, f.scheduler().cancelRequest(f.slot(), 0L, CancelReason.CLIENT_CANCELLED).state());
        assertFalse(f.slot().future().isDone());
        assertTrue(f.scheduler().releasePreemption(claim));
        assertFalse(f.slot().future().join().isSuccess());
        assertEquals(RequestState.Phase.CANCELLED, f.slot().snapshot().state());
        assertFalse(f.slot().future().join().isSuccess());
    }

    @Test
    void queuedDecodeRetirementFreezesTheResultBeforeAsyncCleanup() throws Exception {
        Fixture f = fixture(true, true);
        AtomicReference<String> cleanupThread = new AtomicReference<>();
        when(f.item().prefillEp().removeQueued(any(), any())).thenAnswer(call -> {
            cleanupThread.set(Thread.currentThread().getName());
            return true;
        });
        try (RequestContinuationExecutor continuations = new RequestContinuationExecutor()) {
            org.springframework.test.util.ReflectionTestUtils.setField(f.scheduler(), "continuations", continuations);
            org.mockito.Mockito.doCallRealMethod().when(f.scheduler()).onDecodeGenerationRetired(any(), any());
            f.scheduler().onDecodeGenerationRetired(f.item().decodeEp(), java.util.List.of(RESERVATION));
            assertFalse(f.slot().isOpen());
            assertEquals(RequestState.Phase.FAILED, f.slot().snapshot().state());
            continuations.awaitIdle();
            assertNotNull(cleanupThread.get());
            assertNotEquals(Thread.currentThread().getName(), cleanupThread.get());
        }
    }

    @Test
    void decodeTerminalIsAProofOfAlreadyCommittedEndpointSettlement() {
        Fixture f = fixture(true);
        var sender = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        assertTrue(sender.tryStartSend());
        sender.complete(DeliveryResult.uncertain(new IllegalStateException("reply lost")));
        PreemptionRegistration claim = f.slot().tryInstallPreemption(3L, 4L, "victim");
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        RequestProtocolTestSupport.observeDecode(f.scheduler(), f.slot(), f.item().decodeEp(), DecodeEndpoint.WorkerStatusFact.terminal(RESERVATION, 0L));
        verify(f.item().decodeEp(), never()).updatePreemption(anyLong(), argThat(update -> update.kind() == DecodeEndpoint.PreemptionUpdate.Kind.FINISHED));
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((f.slot().stage() == BalanceContext.RequestStage.FINISHED));
        assertTrue(claim.isFinished());
    }

    @Test
    void deliveryAckDuringPreemptionResumesWithTheOriginalBatchIdentity() {
        Fixture f = fixture(true);
        BalanceContext.DeliveryClaim delivery = f.scheduler().claimDelivery(
                f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        assertNotNull(delivery);
        PreemptionRegistration preemption = f.slot().tryInstallPreemption(3L, 9L, "victim");
        assertNotNull(preemption);
        assertTrue(f.scheduler().updatePreemption(preemption, PreemptionCancelPhase.CANCEL_IN_FLIGHT));

        delivery.complete(DeliveryResult.delivered());
        assertTrue(preemption.hasPendingDeliveryConfirmation());
        assertFalse(f.slot().future().isDone());
        assertNull(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.scheduler()).getRequestState(RESERVATION.requestId(), 8L));
        assertEquals(7L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.scheduler()).getRequestState(RESERVATION.requestId(), 7L).batchId());

        when(f.item().decodeEp().updatePreemption(9L,
                DecodeEndpoint.PreemptionUpdate.active(RESERVATION))).thenReturn(true);
        assertTrue(f.scheduler().updatePreemption(preemption, PreemptionCancelPhase.NOT_FOUND_STALE));
        assertTrue(f.slot().future().join().isSuccess());
        assertEquals(RequestState.Phase.ACKNOWLEDGED,
                org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.scheduler()).getRequestState(RESERVATION.requestId(), 7L).state());
        assertNull(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.scheduler()).getRequestState(RESERVATION.requestId(), 8L));
        verify(f.item().decodeEp(), times(2)).updatePreemption(9L,
                DecodeEndpoint.PreemptionUpdate.active(RESERVATION));
    }

    @Test
    void prefillBackedTerminalWaitsForTheExactDecodeClaimTransaction() {
        Fixture f = fixture(true);
        var sender = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        assertTrue(sender.tryStartSend());
        sender.complete(DeliveryResult.uncertain(new IllegalStateException("reply lost")));
        PreemptionRegistration claim = f.slot().tryInstallPreemption(3L, 4L, "victim");
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        when(f.item().decodeEp().updatePreemption(4L, DecodeEndpoint.PreemptionUpdate.finished(RESERVATION))).thenReturn(false, true);
        var failed = PrefillState.WorkerStatusFact.terminal(f.item(), PrefillState.WorkerStatusFact.Kind.FAILED, 9L);
        RequestProtocolTestSupport.observePrefill(f.scheduler(), f.slot(), f.item().prefillEp(), RoleType.PREFILL, failed);
        assertFalse((f.slot().stage() == BalanceContext.RequestStage.FINISHED));
        assertFalse(claim.isFinished());
        assertFalse(f.slot().future().isDone());
        RequestProtocolTestSupport.observePrefill(f.scheduler(), f.slot(), f.item().prefillEp(), RoleType.PREFILL, failed);
        verify(f.item().decodeEp(), times(2)).updatePreemption(4L, DecodeEndpoint.PreemptionUpdate.finished(RESERVATION));
        assertEquals(RequestState.Phase.FAILED, f.slot().snapshot().state());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((f.slot().stage() == BalanceContext.RequestStage.FINISHED));
    }

    @ParameterizedTest
    @EnumSource(value = PreemptionCancelPhase.class, names = { "NOT_FOUND_STALE", "CANCEL_UNKNOWN" })
    void requestExpiryClosesPreemptionAndIgnoresLateCallbacks(PreemptionCancelPhase outcome) {
        Fixture f = fixture(true);
        BalanceContext slot = f.slot();
        BalanceContext.DeliveryClaim delivery = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        PreemptionRegistration claim = slot.tryInstallPreemption(3L, 9L, "victim");
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(f.scheduler().updatePreemption(claim, outcome));
        f.scheduler().cancelRequest(slot, 7L, CancelReason.CLIENT_CANCELLED);
        var inactivity = mock(ExpirationTimer.InactivityDeadline.class);
        assertTrue(slot.installInactivityDeadline(inactivity));
        RequestProtocolTestSupport.expireInactivity(f.scheduler(), slot, inactivity, RequestProtocolTestSupport.<Long>inspect(f.scheduler(), slot, "inactivityExpiresAtMsLocked"));
        assertFalse(delivery.settlement().toCompletableFuture().isDone(), "sender has not exited");
        delivery.complete(DeliveryResult.notSent(new IllegalStateException("expired before send")));
        f.scheduler().runtime.continuations().awaitIdle();
        RequestState ended = slot.snapshot();
        assertEquals(RequestState.Phase.CANCELLED, ended.state());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((slot.stage() == BalanceContext.RequestStage.FINISHED));
        assertTrue(claim.isFinished());
        assertNull(slot.activeItem());
        assertEquals(CancelReason.CLIENT_CANCELLED, slot.cancellationReason(),
                "finished context preserves the original cancellation fact");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> delivery.complete(DeliveryResult.delivered()));
        assertFalse(f.scheduler().completePreemption(claim, "late Cancel ACK"));
        RequestProtocolTestSupport.observeDecode(f.scheduler(), slot, f.item().decodeEp(), DecodeEndpoint.WorkerStatusFact.terminal(RESERVATION, 0L));
        RequestProtocolTestSupport.expireInactivity(f.scheduler(), slot, inactivity, Long.MAX_VALUE);
        assertEquals(ended, slot.snapshot());
        verify(f.item().decodeEp()).release(RESERVATION, DecodeEndpoint.ReleaseReason.NOT_SENT);
        verify(f.item().prefillEp()).releaseCommittedItem(f.item());
    }

    @Test
    void completedDeliveryFutureCannotPreventRequestExpiry() {
        Fixture f = fixture(true);
        BalanceContext.DeliveryClaim claim = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        assertTrue(claim.tryStartSend());
        claim.complete(DeliveryResult.delivered());
        Response delivered = f.slot().future().join();
        assertTrue(delivered.isSuccess());
        RequestProtocolTestSupport.expireInactiveRequest(f.scheduler(), f.slot(), RequestProtocolTestSupport.<Long>inspect(f.scheduler(), f.slot(), "inactivityExpiresAtMsLocked"));
        assertEquals(RequestState.Phase.TIMED_OUT, f.slot().snapshot().state());
        assertSame(delivered, f.slot().future().join());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((f.slot().stage() == BalanceContext.RequestStage.FINISHED));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void decodeEndDuringAdmissionPreservesTheEarlierCancellationCause(boolean retirement) {
        Fixture f = fixture(false);
        f.scheduler().cancelRequest(f.slot(), 0L, CancelReason.CLIENT_CANCELLED);
        if (retirement) {
            f.scheduler().onDecodeGenerationRetired(f.item().decodeEp(), java.util.List.of(RESERVATION));
            f.scheduler().runtime.continuations().awaitIdle();
        } else {
            RequestProtocolTestSupport.observeDecode(f.scheduler(), f.slot(), f.item().decodeEp(),
                    DecodeEndpoint.WorkerStatusFact.terminal(RESERVATION, 0L));
        }
        assertFalse((f.slot().stage() == BalanceContext.RequestStage.FINISHED));
        assertFalse(f.slot().future().isDone());
        f.admission().finish();
        assertEquals(RequestState.Phase.CANCELLED, f.slot().snapshot().state());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((f.slot().stage() == BalanceContext.RequestStage.FINISHED));
        assertFalse(f.slot().future().join().isSuccess());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void workerProofExcludesLateAckBeforeUnlockedCleanupCommits(boolean preempting) {
        Fixture f = fixture(true);
        BalanceContext slot = f.slot();
        BalanceContext.DeliveryClaim delivery = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        if (preempting) {
            PreemptionRegistration claim = slot.tryInstallPreemption(3L, 9L, "victim");
            assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        }
        doAnswer(call -> {
            assertFalse(Thread.holdsLock(slot));
            assertFalse((slot.stage() == BalanceContext.RequestStage.FINISHED), "cleanup must precede final record commitment");
            assertFalse(slot.future().join().isSuccess(), "late ACK cannot replace the terminal response");
            return DecodeEndpoint.ReservationReleaseResult.RELEASED;
        }).when(f.item().decodeEp()).release(RESERVATION, DecodeEndpoint.ReleaseReason.REMOTE_CLEANUP);
        assertTrue(delivery.tryStartSend());
        RequestProtocolTestSupport.observeDecode(f.scheduler(), slot, f.item().decodeEp(), DecodeEndpoint.WorkerStatusFact.terminal(RESERVATION, 42L));
        assertEquals(RequestState.Phase.FAILED, slot.snapshot().state());
        delivery.complete(DeliveryResult.delivered());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((slot.stage() == BalanceContext.RequestStage.FINISHED));
        assertFalse(slot.future().join().isSuccess());
        verify(f.item().prefillEp()).releaseCommittedItem(f.item());
    }

    @Test
    void anotherSchedulerCannotAdvanceOrReleaseAnExactPreemptionClaim() {
        Fixture owner = fixture(true, true);
        Fixture other = fixture(true, true);
        PreemptionRegistration claim = owner.slot().tryInstallPreemption(3L, 4L, "victim");
        assertNotNull(claim);
        var before = owner.slot().snapshot();
        assertFalse(other.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertFalse(other.scheduler().releasePreemption(claim));
        assertFalse(other.scheduler().completePreemption(claim, "foreign callback"));
        assertEquals(before, owner.slot().snapshot());
        assertFalse(owner.slot().future().isDone());
        assertFalse(other.slot().future().isDone());
        assertTrue(owner.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(owner.scheduler().releasePreemption(claim));
    }

    private static Fixture fixture(boolean finishAdmission) {
        return fixture(finishAdmission, false);
    }

    private static Fixture fixture(boolean finishAdmission, boolean queueScheduling) {
        var config = SchedulingTestConfig.newConfig();
        BalanceContext context = RequestProtocolTestSupport.context(config, RESERVATION.requestId());
        var publisher = mock(RequestCompletionPublisher.class);
        var timer = mock(ExpirationTimer.class);
        BalanceContext slot = context;
        AbstractRequestScheduler requestOwner = RequestProtocolTestSupport.initialize(publisher, slot, timer);
        when(requestOwner.runtime.cancelChannel().cancel(any(), anyLong(), any(), anyLong()))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(org.flexlb.balance.eviction.EngineCancelChannel.CancelAck.REQUEST_CLEANED));
        when(publisher.tryReservePublication(any(), any())).thenAnswer(call -> new RequestCompletionPublisher.PublicationPermit(publisher, slot, call.getArgument(1)));
        doAnswer(call -> {
            ((RequestCompletionPublisher.SelectedPublication) call.getArgument(0)).complete();
            return null;
        }).when(publisher).submit(any());
        doAnswer(call -> {
            BalanceContext.DeliveryPublication delivery = call.getArgument(0);
            BalanceContext.selectPublication(slot, delivery.publication(),
                    RequestCompletionPublisher.ResponseCompletion.RESPONSE, delivery.response(), null, false).complete();
            return null;
        }).when(publisher).submitDelivery(any());
        context.setFuture(slot.future());
        RequestRoute item = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), new Response(), null, null, mock(PrefillEndpoint.class), mock(DecodeEndpoint.class), RESERVATION, System.currentTimeMillis());
        slot.configureInactivityTimeout(60_000L);
        AdmissionHandle admission = RequestProtocolTestSupport.beginAdmission(requestOwner, slot);
        assertNotNull(admission);
        assertEquals(org.flexlb.balance.PlacementResult.Status.SUCCESS, requestOwner.commitRoute(item, RequestProtocolTestSupport.publication(() -> true)));
        if (finishAdmission) {
            admission.finish();
        }
        return new Fixture(requestOwner, slot, item, admission);
    }

    private record Fixture(AbstractRequestScheduler scheduler, BalanceContext slot, RequestRoute item, AdmissionHandle admission) {
    }
}
