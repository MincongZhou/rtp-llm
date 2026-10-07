package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeResources;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RequestTerminalSettlementTest {

    private static final DecodeResources.ReservationHandle RESERVATION =
            new DecodeResources.ReservationHandle(1L, 2L, 3L);

    @Test
    void latePlacementDiagnosticsCannotOverwriteQueueTimeoutEvidence() {
        var context = RequestProtocolTestSupport.context(SchedulingTestConfig.newConfig(), 42L);
        var queue = mock(QueuedRequestScheduler.class);
        var timeoutEvidence = java.util.Map.<String, Object>of("cause", "DECODE placement unavailable");
        when(queue.getLatestQueueWaitSnapshot()).thenReturn(timeoutEvidence);
        AbstractRequestScheduler requestOwner = RequestProtocolTestSupport.initialize(mock(ResponseCompletionExecutor.class), context, mock(ExpirationTimer.class));
        org.mockito.Mockito.doReturn(timeoutEvidence).when((QueuedRequestScheduler) requestOwner).getLatestQueueWaitSnapshot();
        var admission = RequestProtocolTestSupport.beginAdmission(requestOwner, context);
        assertNotNull(admission);
        admission.recordDiagnostics(java.util.Map.of("cause", "earlier placement"));
        assertEquals("earlier placement", context.getSchedulingDiagnostics().get("cause"));
        requestOwner.cancelRequest(context, 0L, CancelReason.DEADLINE_EXCEEDED);
        admission.recordDiagnostics(java.util.Map.of("cause", "late placement"));
        assertEquals(timeoutEvidence, context.getSchedulingDiagnostics());
        assertFalse(context.future().isDone(), "cancellation must still wait for the active admission");
    }

    @Test
    void oldDecodeRetirementCannotRemoveANewSchedulingGeneration() {
        var context = RequestProtocolTestSupport.context(SchedulingTestConfig.newConfig(), 4202L);
        AbstractRequestScheduler requestOwner = RequestProtocolTestSupport.initialize(mock(ResponseCompletionExecutor.class), context, mock(ExpirationTimer.class));
        requestOwner.onDecodeGenerationRetired(RequestProtocolTestSupport.decodeEndpoint(), java.util.List.of(new DecodeResources.ReservationHandle(1L, 4202L, 7L)));
        requestOwner.runtime.continuations().awaitIdle();
        assertTrue(context.isOpen());
        assertFalse(context.getFuture().isDone());
    }

    @Test
    void unknownCancelOutcomeKeepsTheAcknowledgedCancellationVisible() {
        Fixture f = fixture(true, true);
        PreemptionRegistration claim = f.requestContext().tryInstallPreemption(RESERVATION, 4L, "victim");
        assertNotNull(claim);
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_REQUESTED));
        assertEquals(RequestState.Phase.CANCEL_REQUESTED, f.requestContext().snapshot().state());
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_UNKNOWN));
        assertEquals(RequestState.Phase.CANCEL_REQUESTED, f.requestContext().snapshot().state());
        assertEquals(BalanceContext.RequestStage.READY_TO_DELIVER, f.requestContext().stage());
        assertFalse(f.requestContext().future().isDone());
    }

    @Test
    void queuedCancellationResumesAfterExactPreemptionRelease() {
        Fixture f = fixture(true, true);
        PreemptionRegistration claim = f.requestContext().tryInstallPreemption(RESERVATION, 4L, "victim");
        assertNotNull(claim);
        assertEquals(RequestState.Phase.CANCEL_REQUESTED, f.scheduler().cancelRequest(f.requestContext(), 0L, CancelReason.CLIENT_CANCELLED).state());
        assertFalse(f.requestContext().future().isDone());
        assertTrue(f.scheduler().releasePreemption(claim));
        assertFalse(f.requestContext().future().join().isSuccess());
        assertEquals(RequestState.Phase.CANCELLED, f.requestContext().snapshot().state());
        assertFalse(f.requestContext().future().join().isSuccess());
    }

    @Test
    void queuedDecodeRetirementFreezesTheResultBeforeAsyncCleanup() throws Exception {
        Fixture f = fixture(true, true);
        AtomicReference<String> cleanupThread = new AtomicReference<>();
        when(f.item().prefillEp().releaseRequest(any())).thenAnswer(call -> {
            cleanupThread.set(Thread.currentThread().getName());
            return true;
        });
        try (RequestContinuationExecutor continuations = new RequestContinuationExecutor()) {
            org.springframework.test.util.ReflectionTestUtils.setField(f.scheduler(), "continuations", continuations);
            org.mockito.Mockito.doCallRealMethod().when(f.scheduler()).onDecodeGenerationRetired(any(), any());
            f.scheduler().onDecodeGenerationRetired(f.item().decodeEp(), java.util.List.of(RESERVATION));
            assertFalse(f.requestContext().isOpen());
            assertEquals(RequestState.Phase.FAILED, f.requestContext().snapshot().state());
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
        PreemptionRegistration claim = f.requestContext().tryInstallPreemption(RESERVATION, 4L, "victim");
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        RequestProtocolTestSupport.observeDecode(f.scheduler(), f.requestContext(), f.item().decodeEp(), DecodeResources.WorkerStatusFact.terminal(RESERVATION, 0L));
        verify(f.item().decodeEp(), never()).reconcilePreemptionResources(anyLong(), argThat(update -> update.kind() == DecodeResources.PreemptionUpdate.Kind.FINISHED));
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((f.requestContext().stage() == BalanceContext.RequestStage.FINISHED));
        assertTrue(claim.isFinished());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deliveryAckDuringPreemptionResumesWithTheOriginalBatchIdentity(boolean notificationFails) {
        Fixture f = fixture(true);
        BalanceContext.DeliveryClaim delivery = f.scheduler().claimDelivery(
                f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        assertNotNull(delivery);
        PreemptionRegistration preemption = f.requestContext().tryInstallPreemption(RESERVATION, 9L, "victim");
        assertNotNull(preemption);
        assertTrue(f.scheduler().updatePreemption(preemption, PreemptionCancelPhase.CANCEL_IN_FLIGHT));

        delivery.complete(DeliveryResult.delivered());
        assertTrue(preemption.hasPendingDeliveryConfirmation());
        assertFalse(f.requestContext().future().isDone());
        assertNull(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.scheduler()).getRequestState(RESERVATION.requestId(), 8L));
        assertEquals(7L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.scheduler()).getRequestState(RESERVATION.requestId(), 7L).batchId());

        when(f.item().decodeEp().reconcilePreemptionResources(9L,
                DecodeResources.PreemptionUpdate.active(RESERVATION))).thenReturn(true);
        var notificationFailure = new IllegalStateException("capacity listener failed");
        doAnswer(call -> {
            assertFalse(Thread.holdsLock(f.requestContext()));
            assertNull(f.requestContext().preemption());
            assertFalse(preemption.terminalObservation().toCompletableFuture().isDone());
            if (notificationFails) { throw notificationFailure; }
            return null;
        }).when(f.item().decodeEp()).publishCapacityRelease();
        if (notificationFails) {
            assertSame(notificationFailure, assertThrows(IllegalStateException.class,
                    () -> f.scheduler().updatePreemption(preemption, PreemptionCancelPhase.NOT_FOUND_STALE)));
        } else {
            assertTrue(f.scheduler().updatePreemption(preemption, PreemptionCancelPhase.NOT_FOUND_STALE));
        }
        assertTrue(f.requestContext().future().join().isSuccess());
        assertTrue(preemption.terminalObservation().toCompletableFuture().isDone());
        assertEquals(RequestState.Phase.ACKNOWLEDGED,
                org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.scheduler()).getRequestState(RESERVATION.requestId(), 7L).state());
        assertNull(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.scheduler()).getRequestState(RESERVATION.requestId(), 8L));
        verify(f.item().decodeEp(), times(2)).reconcilePreemptionResources(9L,
                DecodeResources.PreemptionUpdate.active(RESERVATION));
        verify(f.item().decodeEp()).publishCapacityRelease();
    }

    @Test
    void prefillBackedTerminalWaitsForTheExactDecodeClaimTransaction() {
        Fixture f = fixture(true);
        var sender = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        assertTrue(sender.tryStartSend());
        sender.complete(DeliveryResult.uncertain(new IllegalStateException("reply lost")));
        PreemptionRegistration claim = f.requestContext().tryInstallPreemption(RESERVATION, 4L, "victim");
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        when(f.item().decodeEp().reconcilePreemptionResources(4L, DecodeResources.PreemptionUpdate.finished(RESERVATION))).thenReturn(false, true);
        doAnswer(call -> {
            assertFalse(Thread.holdsLock(f.requestContext()));
            assertNull(f.requestContext().preemption());
            assertTrue(claim.isFinished());
            assertFalse(claim.terminalObservation().toCompletableFuture().isDone());
            return null;
        }).when(f.item().decodeEp()).publishCapacityRelease();
        var failed = PrefillState.WorkerStatusFact.terminal(f.item(), PrefillState.WorkerStatusFact.Kind.FAILED, 9L);
        RequestProtocolTestSupport.observePrefill(f.scheduler(), f.requestContext(), f.item().prefillEp(), RoleType.PREFILL, failed);
        assertFalse((f.requestContext().stage() == BalanceContext.RequestStage.FINISHED));
        assertFalse(claim.isFinished());
        assertFalse(f.requestContext().future().isDone());
        verify(f.item().decodeEp(), never()).publishCapacityRelease();
        RequestProtocolTestSupport.observePrefill(f.scheduler(), f.requestContext(), f.item().prefillEp(), RoleType.PREFILL, failed);
        verify(f.item().decodeEp(), times(2)).reconcilePreemptionResources(4L, DecodeResources.PreemptionUpdate.finished(RESERVATION));
        assertEquals(RequestState.Phase.FAILED, f.requestContext().snapshot().state());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((f.requestContext().stage() == BalanceContext.RequestStage.FINISHED));
        assertTrue(claim.terminalObservation().toCompletableFuture().isDone());
        verify(f.item().decodeEp()).publishCapacityRelease();
        verify(f.item().decodeEp(), never()).release(any(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void prefillFactsPublishReconciledPreemptionCapacityOutsideContext(boolean priorityCanceled) {
        Fixture f = fixture(true);
        var claim = f.requestContext().tryInstallPreemption(RESERVATION, 31L, "victim");
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(f.scheduler().updatePreemption(claim, priorityCanceled
                ? PreemptionCancelPhase.CANCEL_REQUESTED : PreemptionCancelPhase.NOT_FOUND_STALE));
        var update = priorityCanceled ? DecodeResources.PreemptionUpdate.canceled(RESERVATION)
                : DecodeResources.PreemptionUpdate.active(RESERVATION);
        when(f.item().decodeEp().reconcilePreemptionResources(31L, update)).thenReturn(true);
        doAnswer(call -> {
            assertFalse(Thread.holdsLock(f.requestContext()));
            assertNull(f.requestContext().preemption());
            assertEquals(priorityCanceled, claim.isFinished());
            assertFalse(claim.terminalObservation().toCompletableFuture().isDone());
            return null;
        }).when(f.item().decodeEp()).publishCapacityRelease();

        var fact = priorityCanceled ? PrefillState.WorkerStatusFact.terminal(f.item(),
                PrefillState.WorkerStatusFact.Kind.PRIORITY_CANCELED, 0L)
                : PrefillState.WorkerStatusFact.active(f.item());
        RequestProtocolTestSupport.observePrefill(f.scheduler(), f.requestContext(), f.item().prefillEp(), RoleType.PREFILL, fact);

        verify(f.item().decodeEp()).reconcilePreemptionResources(31L, update);
        verify(f.item().decodeEp()).publishCapacityRelease();
        assertEquals(priorityCanceled, claim.terminalObservation().toCompletableFuture().isDone());
        assertEquals(priorityCanceled, f.requestContext().future().isDone());
        if (priorityCanceled) {
            assertEquals(BalanceContext.RequestStage.FINISHED, f.requestContext().stage());
            assertFalse(f.requestContext().future().join().isSuccess());
        } else {
            assertEquals(BalanceContext.RequestStage.READY_TO_DELIVER, f.requestContext().stage());
        }
    }

    @ParameterizedTest
    @EnumSource(value = PreemptionCancelPhase.class, names = { "NOT_FOUND_STALE", "CANCEL_UNKNOWN" })
    void requestExpiryClosesPreemptionAndIgnoresLateCallbacks(PreemptionCancelPhase outcome) {
        Fixture f = fixture(true);
        BalanceContext requestContext = f.requestContext();
        BalanceContext.DeliveryClaim delivery = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        PreemptionRegistration claim = requestContext.tryInstallPreemption(RESERVATION, 9L, "victim");
        assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(f.scheduler().updatePreemption(claim, outcome));
        f.scheduler().cancelRequest(requestContext, 7L, CancelReason.CLIENT_CANCELLED);
        var inactivity = mock(ExpirationTimer.InactivityDeadline.class);
        assertTrue(requestContext.installInactivityDeadline(inactivity));
        RequestProtocolTestSupport.expireInactivity(f.scheduler(), requestContext, inactivity, RequestProtocolTestSupport.<Long>inspect(f.scheduler(), requestContext, "inactivityExpiresAtMsLocked"));
        assertFalse(delivery.settlement().toCompletableFuture().isDone(), "sender has not exited");
        delivery.complete(DeliveryResult.notSent(new IllegalStateException("expired before send")));
        f.scheduler().runtime.continuations().awaitIdle();
        RequestState ended = requestContext.snapshot();
        assertEquals(RequestState.Phase.CANCELLED, ended.state());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((requestContext.stage() == BalanceContext.RequestStage.FINISHED));
        assertTrue(claim.isFinished());
        assertNull(requestContext.activeRoute());
        assertEquals(CancelReason.CLIENT_CANCELLED, requestContext.cancellationReason(),
                "finished context preserves the original cancellation fact");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> delivery.complete(DeliveryResult.delivered()));
        assertFalse(f.scheduler().completePreemption(claim, "late Cancel ACK"));
        RequestProtocolTestSupport.observeDecode(f.scheduler(), requestContext, f.item().decodeEp(), DecodeResources.WorkerStatusFact.terminal(RESERVATION, 0L));
        RequestProtocolTestSupport.expireInactivity(f.scheduler(), requestContext, inactivity, Long.MAX_VALUE);
        assertEquals(ended, requestContext.snapshot());
        verify(f.item().decodeEp()).release(RESERVATION, DecodeResources.ReleaseReason.NOT_SENT);
        verify(f.item().prefillEp()).releaseRequest(f.item());
    }

    @Test
    void completedDeliveryFutureCannotPreventRequestExpiry() {
        Fixture f = fixture(true);
        BalanceContext.DeliveryClaim claim = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        assertTrue(claim.tryStartSend());
        claim.complete(DeliveryResult.delivered());
        Response delivered = f.requestContext().future().join();
        assertTrue(delivered.isSuccess());
        RequestProtocolTestSupport.expireInactiveRequest(f.scheduler(), f.requestContext(), RequestProtocolTestSupport.<Long>inspect(f.scheduler(), f.requestContext(), "inactivityExpiresAtMsLocked"));
        assertEquals(RequestState.Phase.TIMED_OUT, f.requestContext().snapshot().state());
        assertSame(delivered, f.requestContext().future().join());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((f.requestContext().stage() == BalanceContext.RequestStage.FINISHED));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void decodeEndDuringAdmissionPreservesTheEarlierCancellationCause(boolean retirement) {
        Fixture f = fixture(false);
        f.scheduler().cancelRequest(f.requestContext(), 0L, CancelReason.CLIENT_CANCELLED);
        if (retirement) {
            f.scheduler().onDecodeGenerationRetired(f.item().decodeEp(), java.util.List.of(RESERVATION));
            f.scheduler().runtime.continuations().awaitIdle();
        } else {
            RequestProtocolTestSupport.observeDecode(f.scheduler(), f.requestContext(), f.item().decodeEp(),
                    DecodeResources.WorkerStatusFact.terminal(RESERVATION, 0L));
        }
        assertFalse((f.requestContext().stage() == BalanceContext.RequestStage.FINISHED));
        assertFalse(f.requestContext().future().isDone());
        f.admission().finish();
        assertEquals(RequestState.Phase.CANCELLED, f.requestContext().snapshot().state());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((f.requestContext().stage() == BalanceContext.RequestStage.FINISHED));
        assertFalse(f.requestContext().future().join().isSuccess());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void workerProofExcludesLateAckBeforeUnlockedCleanupCommits(boolean preempting) {
        Fixture f = fixture(true);
        BalanceContext requestContext = f.requestContext();
        BalanceContext.DeliveryClaim delivery = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L, RequestProtocolTestSupport.handoff(() -> true));
        if (preempting) {
            PreemptionRegistration claim = requestContext.tryInstallPreemption(RESERVATION, 9L, "victim");
            assertTrue(f.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        }
        doAnswer(call -> {
            assertFalse(Thread.holdsLock(requestContext));
            assertFalse((requestContext.stage() == BalanceContext.RequestStage.FINISHED), "cleanup must precede final record commitment");
            assertFalse(requestContext.future().join().isSuccess(), "late ACK cannot replace the terminal response");
            return DecodeResources.ReservationReleaseResult.RELEASED;
        }).when(f.item().decodeEp()).release(RESERVATION, DecodeResources.ReleaseReason.REMOTE_CLEANUP);
        assertTrue(delivery.tryStartSend());
        RequestProtocolTestSupport.observeDecode(f.scheduler(), requestContext, f.item().decodeEp(), DecodeResources.WorkerStatusFact.terminal(RESERVATION, 42L));
        assertEquals(RequestState.Phase.FAILED, requestContext.snapshot().state());
        delivery.complete(DeliveryResult.delivered());
        f.scheduler().runtime.continuations().awaitIdle();
        assertTrue((requestContext.stage() == BalanceContext.RequestStage.FINISHED));
        assertFalse(requestContext.future().join().isSuccess());
        verify(f.item().prefillEp()).releaseRequest(f.item());
    }

    @Test
    void completedReleaseEvidenceSurvivesLateFrontendFailureAndWaitsForTerminalEffects() {
        Fixture f = fixture(true);
        BalanceContext context = f.requestContext();
        var delivery = f.scheduler().claimDelivery(f.item(), DeliveryClaimKind.BATCH_ENQUEUE, 7L,
                RequestProtocolTestSupport.handoff(() -> true));
        assertTrue(delivery.tryStartSend());
        delivery.complete(DeliveryResult.delivered());
        TerminalAction action;
        synchronized (context) {
            action = context.decideRequestEndLocked(DeferredTerminal.worker(WorkerTerminalSource.DECODE_ENDPOINT, true, 0L),
                    () -> f.scheduler().requirePublicationPermitLocked(context, BalanceContext.PublicationKind.TERMINAL));
        }
        assertNotNull(action);
        var inactivity = mock(ExpirationTimer.InactivityDeadline.class);
        assertTrue(context.installInactivityDeadline(inactivity));
        doAnswer(call -> {
            assertFalse(Thread.holdsLock(context));
            assertTrue(context.inactivityDeadlineAtMs().isEmpty(), "archive ownership closes timer registration");
            assertFalse(context.installInactivityDeadline(mock(ExpirationTimer.InactivityDeadline.class)));
            return null;
        }).when(inactivity).cancel();
        delivery.observeWorkerCompletion(f.item());
        f.scheduler().runtime.continuations().awaitIdle();
        assertEquals(BalanceContext.RequestStage.FINALIZING, context.stage(),
                "resource evidence cannot archive before the terminal execution owner finishes");
        f.scheduler().onResponseUndeliverable(context);
        assertFalse(delivery.cleanupRequired(), "late frontend failure cannot revoke completed execution evidence");
        f.scheduler().finalizationEffects(action, null).run();
        f.scheduler().runtime.continuations().awaitIdle();
        assertEquals(BalanceContext.RequestStage.FINISHED, context.stage());
        verify(f.item().prefillEp(), times(1)).releaseRequest(f.item());
    }

    @Test
    void anotherSchedulerCannotAdvanceOrReleaseAnExactPreemptionClaim() {
        Fixture owner = fixture(true, true);
        Fixture other = fixture(true, true);
        PreemptionRegistration claim = owner.requestContext().tryInstallPreemption(RESERVATION, 4L, "victim");
        assertNotNull(claim);
        var before = owner.requestContext().snapshot();
        assertFalse(other.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertFalse(other.scheduler().releasePreemption(claim));
        assertFalse(other.scheduler().completePreemption(claim, "foreign callback"));
        assertEquals(before, owner.requestContext().snapshot());
        assertFalse(owner.requestContext().future().isDone());
        assertFalse(other.requestContext().future().isDone());
        assertTrue(owner.scheduler().updatePreemption(claim, PreemptionCancelPhase.CANCEL_IN_FLIGHT));
        assertTrue(owner.scheduler().releasePreemption(claim));
    }

    private static Fixture fixture(boolean finishAdmission) {
        return fixture(finishAdmission, false);
    }

    private static Fixture fixture(boolean finishAdmission, boolean queueScheduling) {
        var config = SchedulingTestConfig.newConfig();
        BalanceContext context = RequestProtocolTestSupport.context(config, RESERVATION.requestId());
        var publisher = mock(ResponseCompletionExecutor.class);
        var timer = mock(ExpirationTimer.class);
        AbstractRequestScheduler requestOwner = RequestProtocolTestSupport.initialize(publisher, context, timer);
        when(requestOwner.runtime.cancelChannel().cancel(any(), anyLong(), any(), anyLong()))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(org.flexlb.balance.eviction.EngineCancelChannel.CancelAck.REQUEST_CLEANED));
        when(publisher.tryRegister()).thenAnswer(call -> new ResponseCompletionExecutor.CompletionRegistration(publisher));
        doAnswer(call -> {
            ((java.util.function.BooleanSupplier) call.getArgument(1)).getAsBoolean();
            return null;
        }).when(publisher).submit(any(), any());
        var prefillServer = new org.flexlb.dao.loadbalance.ServerStatus();
        prefillServer.setServerIp("127.0.0.1");
        prefillServer.setGrpcPort(8090);
        RequestRoute item = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), new Response(), prefillServer, null, mock(PrefillEndpoint.class), RequestProtocolTestSupport.decodeEndpoint(), RESERVATION, System.currentTimeMillis());
        context.configureInactivityTimeout(60_000L);
        AdmissionHandle admission = RequestProtocolTestSupport.beginAdmission(requestOwner, context);
        assertNotNull(admission);
        assertEquals(org.flexlb.balance.PlacementResult.Status.SUCCESS, requestOwner.commitRoute(item, RequestProtocolTestSupport.publication(() -> true)));
        if (finishAdmission) {
            admission.finish();
        }
        return new Fixture(requestOwner, context, item, admission);
    }

    private record Fixture(AbstractRequestScheduler scheduler, BalanceContext requestContext, RequestRoute item, AdmissionHandle admission) {
    }
}
