package org.flexlb.balance.scheduler;

import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.BalanceContext.AdmissionHandle;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryClaim;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.flexlb.balance.scheduler.SchedulingTestConfig.freezeInputs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RequestLifetimeTest {

    @Test
    void deadlineUsesTheActualDeliveryClockAndPreservesZeroAndSaturation() {
        WorkSnapshot preceding = emptyWork(1_000L);
        assertEquals(13_100L, visibilityDeadline(preceding, 1_000L, 2, 1_100L).orElseThrow());
        assertEquals(11_000L, visibilityDeadline(preceding, 0L, 2, 1_000L).orElseThrow());
        assertEquals(11_002L, visibilityDeadline(emptyWork(900L), 1L, 2, 1_000L).orElseThrow());
        assertEquals(Long.MAX_VALUE, visibilityDeadline(preceding, Long.MAX_VALUE, 2, 1_000L).orElseThrow());
        assertThrows(IllegalArgumentException.class, () -> visibilityDeadline(preceding, 1L, Double.NaN, 1_000L));
        assertThrows(IllegalArgumentException.class, () -> visibilityDeadline(preceding, -1L, 2, 1_000L));
    }

    @Test
    void slowPreparationCannotConsumeTheRequestsOwnWork() {
        assertEquals(56_000L, visibilityDeadline(emptyWork(1_000L), 20_000L, 1.5, 16_000L).orElseThrow());
        assertEquals(100_000L, visibilityDeadline(emptyWork(1_000L), 20_000L, 1.5, 60_000L).orElseThrow());
        WorkSnapshot preceding = new WorkSnapshot(1_000L, List.of(
                new WorkSnapshot.RequestWork(1L, WorkSnapshot.Phase.ENGINE_RUNNING, 20_000L),
                new WorkSnapshot.RequestWork(2L, WorkSnapshot.Phase.ENGINE_QUEUED, 3_000L)), List.of(), 0L);
        assertEquals(68_000L, visibilityDeadline(preceding, 20_000L, 1.5, 16_000L).orElseThrow());
    }

    @Test
    void onlyRunningPredecessorsAgeBeforeDelivery() {
        WorkSnapshot preceding = new WorkSnapshot(1_000L, List.of(
                new WorkSnapshot.RequestWork(1L, WorkSnapshot.Phase.ENGINE_RUNNING, 20_000L),
                new WorkSnapshot.RequestWork(2L, WorkSnapshot.Phase.ENGINE_QUEUED, 7_000L),
                new WorkSnapshot.RequestWork(3L, WorkSnapshot.Phase.COMMITTED, 3_000L)), List.of(
                new WorkSnapshot.BatchWork(4L, List.of(4L), WorkSnapshot.Phase.ENGINE_RUNNING, 8_000L),
                new WorkSnapshot.BatchWork(5L, List.of(5L), WorkSnapshot.Phase.ENGINE_QUEUED, 4_000L)), 0L);
        assertEquals(63_500L, visibilityDeadline(preceding, 11_000L, 1, 500L).orElseThrow());
        assertEquals(56_000L, visibilityDeadline(preceding, 11_000L, 1, 11_000L).orElseThrow());
        assertEquals(66_000L, visibilityDeadline(preceding, 11_000L, 1, 31_000L).orElseThrow());
    }

    @Test
    void unknownPredecessorsNeverBecomeACompleteEstimate() {
        WorkSnapshot unknownBatch = new WorkSnapshot(1_000L, List.of(), List.of(
                new WorkSnapshot.BatchWork(1L, List.of(1L), WorkSnapshot.Phase.COMMITTED,
                        OptionalLong.empty())), 0L);
        for (WorkSnapshot preceding : List.of(unknownWork(1_000L), unknownBatch)) {
            assertTrue(visibilityDeadline(preceding, 20_000L, 2, 60_000L).isEmpty());
        }
    }

    @Test
    void combinedWorkSaturatesWithoutWrapping() {
        WorkSnapshot preceding = new WorkSnapshot(1_000L, List.of(
                new WorkSnapshot.RequestWork(1L, WorkSnapshot.Phase.COMMITTED, Long.MAX_VALUE)), List.of(), 0L);
        assertEquals(Long.MAX_VALUE, visibilityDeadline(preceding, 1L, 2, 60_000L).orElseThrow());
    }

    @Test
    void latePrefillCompletionStartsOneFreshHandoffWindow() {
        Fixture fixture = fixture(true);
        synchronized (fixture.slot) {
            fixture.slot.updateDeliveryPredictionLocked(emptyWork(1_000L), 100L, 1_000L);
            assertEquals(11_200L, fixture.slot.decisionDeadlineAtMs().orElseThrow());
            observePrefillAt(fixture, false, 1_100L);
            assertTrue(fixture.slot.decisionDeadlineAtMs().isEmpty());
            observePrefillAt(fixture, true, 5_000L);
            assertEquals(15_000L, fixture.slot.decisionDeadlineAtMs().orElseThrow());
            observePrefillAt(fixture, true, 5_020L);
            assertEquals(15_000L, fixture.slot.decisionDeadlineAtMs().orElseThrow());
            var timer = mock(ExpirationTimer.DecisionDeadline.class);
            when(timer.deadlineAtMs()).thenReturn(15_000L);
            assertTrue(fixture.slot.installDecisionDeadline(timer));
            fixture.slot.onDecisionVisibilityDeadline(timer);
            assertTrue(fixture.slot.snapshot().detail().startsWith("SUSPECTED_LOST"));
            fixture.slot.markDecodeAcceptedLocked();
            assertTrue(fixture.slot.decisionDeadlineAtMs().isEmpty());
            assertFalse(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "needsDecisionConfirmationLocked"));
        }
    }

    @Test
    void completionAndAcceptanceBeforeDeliveryRemainAuthoritative() {
        Fixture fixture = fixture(true);
        synchronized (fixture.slot) {
            observePrefillAt(fixture, true, 1_000L);
            fixture.slot.updateDeliveryPredictionLocked(emptyWork(1_000L), 100L, 1_010L);
            assertEquals(11_000L, fixture.slot.decisionDeadlineAtMs().orElseThrow());
            fixture.slot.markDecodeAcceptedLocked();
            observePrefillAt(fixture, false, 2_000L);
            observePrefillAt(fixture, true, 2_000L);
            assertTrue(fixture.slot.decodeAccepted());
            assertTrue(fixture.slot.decisionDeadlineAtMs().isEmpty());
            assertThrows(IllegalStateException.class, () -> fixture.slot.updateDeliveryPredictionLocked(emptyWork(2_000L), 100L, 2_000L));
        }
    }

    @Test
    void unknownPredictionCanStillStartHandoffDetectionAfterPrefillCompletes() {
        Fixture fixture = fixture(true);
        synchronized (fixture.slot) {
            fixture.slot.updateDeliveryPredictionLocked(unknownWork(1_000L), 100L, 1_000L);
            assertTrue(fixture.slot.decisionDeadlineAtMs().isEmpty());
            assertFalse(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "needsDecisionConfirmationLocked"));
            observePrefillAt(fixture, true, 2_000L);
            assertEquals(12_000L, fixture.slot.decisionDeadlineAtMs().orElseThrow());
        }
    }

    @Test
    void longRunningPrefillStillReceivesTheFullHandoffWindow() {
        Fixture fixture = fixture(true);
        synchronized (fixture.slot) {
            fixture.slot.updateDeliveryPredictionLocked(emptyWork(1_000L), 100L, 1_000L);
            observePrefillAt(fixture, false, 1_100L);
            assertTrue(fixture.slot.decisionDeadlineAtMs().isEmpty());
            observePrefillAt(fixture, true, 3_601_000L);
            assertEquals(3_611_000L, fixture.slot.decisionDeadlineAtMs().orElseThrow());
        }
    }

    @Test
    void inactivityWatchSurvivesRouteResponseAndDecodeAcceptance() {
        Fixture fixture = fixture(true);
        var exact = mock(ExpirationTimer.InactivityDeadline.class);
        synchronized (fixture.slot) {
            assertTrue(fixture.slot.installInactivityDeadline(exact));
            RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
            RequestProtocolTestSupport.markAcknowledged(fixture.slot);
            fixture.slot.markDecodeAcceptedLocked();
            assertTrue(fixture.slot.future().completeOwned(new Response()));
            assertTrue(fixture.slot.future().isDone());
            assertTrue(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(fixture.slot, "consumeInactivityDeadlineLocked", exact));
            assertFalse(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(fixture.slot, "consumeInactivityDeadlineLocked", exact));
        }
    }

    @Test
    void missingEngineEvidenceMarksSuspicionWithoutReleasingRequestOwnership() {
        Fixture fixture = fixture(true);
        var exact = mock(ExpirationTimer.DecisionDeadline.class);
        synchronized (fixture.slot) {
            RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
            startPrediction(fixture.scheduler, fixture.slot);
            when(exact.deadlineAtMs()).thenReturn(fixture.slot.decisionDeadlineAtMs().orElseThrow());
            assertTrue(fixture.slot.installDecisionDeadline(exact));
            fixture.slot.onDecisionVisibilityDeadline(exact);
            assertSame(fixture.item, fixture.slot.activeItem());
            assertTrue(RequestProtocolTestSupport.<Boolean>field(fixture.slot, "decisionExpired"));
            assertTrue(RequestProtocolTestSupport.<java.util.OptionalLong>field(fixture.slot, "decisionExpiresAtMs").isEmpty());
            assertNull(fixture.slot.decisionDeadline());
            assertTrue(fixture.slot.isLiveGeneration());
            assertFalse(fixture.slot.snapshot().state().isTerminal());
            assertTrue(fixture.slot.snapshot().detail().contains("SUSPECTED_LOST"));
            assertStaleDecisionHasNoEffect(fixture, exact);
        }
    }

    @Test
    void runningPrefillMayExceedPredictionAndOnlyCompletedHandoffCanBecomeUnresolved() {
        Fixture fixture = fixture(true);
        var exact = mock(ExpirationTimer.DecisionDeadline.class);
        synchronized (fixture.slot) {
            RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
            startPrediction(fixture.scheduler, fixture.slot);
            when(exact.deadlineAtMs()).thenReturn(fixture.slot.decisionDeadlineAtMs().orElseThrow());
            assertTrue(fixture.slot.installDecisionDeadline(exact));
            observePrefill(fixture.scheduler, fixture.slot, fixture.item, false);
            assertStaleDecisionHasNoEffect(fixture, exact);
            assertFalse(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "needsDecisionConfirmationLocked"));
            observePrefill(fixture.scheduler, fixture.slot, fixture.item, true);
            assertTrue(fixture.slot.decisionDeadlineAtMs().orElseThrow() > System.currentTimeMillis());
            fixture.slot.markDecodeAcceptedLocked();
            assertFalse(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "needsDecisionConfirmationLocked"));
        }
    }

    @Test
    void pdfusionEvidenceEndsMissingRequestDetectionButKeepsInactivityWatch() {
        Fixture fixture = fixture(false);
        var full = mock(ExpirationTimer.InactivityDeadline.class);
        var decision = mock(ExpirationTimer.DecisionDeadline.class);
        synchronized (fixture.slot) {
            fixture.slot.installInactivityDeadline(full);
            RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
            startPrediction(fixture.scheduler, fixture.slot);
            when(decision.deadlineAtMs()).thenReturn(fixture.slot.decisionDeadlineAtMs().orElseThrow());
            fixture.slot.installDecisionDeadline(decision);
            observePrefill(fixture.scheduler, fixture.slot, fixture.item, false);
            assertStaleDecisionHasNoEffect(fixture, decision);
            assertTrue(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(fixture.slot, "consumeInactivityDeadlineLocked", full));
        }
    }

    @Test
    void committedInactivityCancellationWinsOverLateHandoffEvidence() {
        Fixture fixture = fixture(true);
        var decision = mock(ExpirationTimer.DecisionDeadline.class);
        synchronized (fixture.slot) {
            RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
            startPrediction(fixture.scheduler, fixture.slot);
            RequestProtocolTestSupport.recordCancellation(fixture.scheduler, fixture.slot, CancelReason.DEADLINE_EXCEEDED, "request inactive");
            when(decision.deadlineAtMs()).thenReturn(fixture.slot.decisionDeadlineAtMs().orElseThrow());
            fixture.slot.installDecisionDeadline(decision);
            fixture.slot.onDecisionVisibilityDeadline(decision);
            assertFalse(fixture.slot.snapshot().detail().startsWith("SUSPECTED_LOST"));
            assertEquals(CancelReason.DEADLINE_EXCEEDED, fixture.slot.cancellationReason());
            assertFalse(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "needsDecisionConfirmationLocked"));
            observePrefill(fixture.scheduler, fixture.slot, fixture.item, true);
            assertFalse(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "needsDecisionConfirmationLocked"));
        }
    }

    @Test
    void latePrefillEvidenceInvalidatesTheExpiredDecision() {
        Fixture fixture = fixture(true);
        synchronized (fixture.slot) {
            expireDecision(fixture);
            assertTrue(fixture.slot.snapshot().detail().startsWith("SUSPECTED_LOST"));
            observePrefill(fixture.scheduler, fixture.slot, fixture.item, false);
            fixture.slot.reconcileDecisionEvidenceLocked();
            assertFalse(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "needsDecisionConfirmationLocked"));
            assertFalse(fixture.slot.snapshot().detail().contains("SUSPECTED_LOST"));
            assertFalse(fixture.slot.decodeAccepted());
        }
    }

    @Test
    void delayedUncertaintyCannotReplaceMatchingEngineEvidence() {
        Fixture fixture = fixture(true);
        synchronized (fixture.slot) {
            expireDecision(fixture);
            observePrefill(fixture.scheduler, fixture.slot, fixture.item, false);
            fixture.slot.markAwaitingConfirmationLocked("late transport uncertainty");
            assertFalse(fixture.slot.snapshot().detail().contains("SUSPECTED_LOST"));
            assertFalse((fixture.slot.cancellationReason() != null));
            observePrefill(fixture.scheduler, fixture.slot, fixture.item, true);
            assertTrue(fixture.slot.decisionDeadlineAtMs().orElseThrow() > System.currentTimeMillis());
        }
    }

    @Test
    void uncertainDeliveryKeepsTheInactivityDeadlineAndDoesNotCancelTheRequest() {
        Fixture fixture = fixture(true);
        var deadline = mock(ExpirationTimer.InactivityDeadline.class);
        synchronized (fixture.slot) {
            RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
            startPrediction(fixture.scheduler, fixture.slot);
            assertTrue(fixture.slot.installInactivityDeadline(deadline));
            fixture.slot.markAwaitingConfirmationLocked("ambiguous transport");
            assertTrue(fixture.slot.snapshot().detail().contains("SUSPECTED_LOST"));
            assertFalse((fixture.slot.cancellationReason() != null));
            assertEquals(RequestState.Phase.DISPATCHING, fixture.slot.snapshot().state());
            assertTrue(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(fixture.slot, "consumeInactivityDeadlineLocked", deadline));
            assertTrue(fixture.slot.inactivityDeadlineAtMs().isPresent());
            assertSame(fixture.item, fixture.slot.activeItem());
        }
    }

    @Test
    void cancellationFirstCauseCannotSuppressTheInactivityWatch() {
        Fixture fixture = fixture(true);
        var deadline = mock(ExpirationTimer.InactivityDeadline.class);
        var renewed = mock(ExpirationTimer.InactivityDeadline.class);
        synchronized (fixture.slot) {
            RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
            assertTrue(fixture.slot.installInactivityDeadline(deadline));
            RequestProtocolTestSupport.recordCancellation(fixture.scheduler, fixture.slot, CancelReason.CLIENT_CANCELLED, "client cancellation");
            assertTrue(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(fixture.slot, "consumeInactivityDeadlineLocked", deadline));
            assertTrue(fixture.slot.installInactivityDeadline(renewed));
            assertFalse(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(fixture.slot, "consumeInactivityDeadlineLocked", deadline));
            assertTrue(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(fixture.slot, "consumeInactivityDeadlineLocked", renewed));
            assertEquals(CancelReason.CLIENT_CANCELLED, RequestProtocolTestSupport.<CancelReason>inspect(fixture.scheduler, fixture.slot, "requireCancellationFirstCauseLocked"));
        }
    }

    @ParameterizedTest
    @CsvSource({ "PREFILL,true", "DECODE,true", "PREFILL,false", "DECODE,false" })
    void uncertaintyAllowsActualAckIndependentlyOfMatchingEngineEvidence(RoleType evidenceSource, boolean ackBeforeEvidence) throws Exception {
        FlexlbConfig config = SchedulingTestConfig.batchConfig();
        ConfigService service = mock(ConfigService.class);
        when(service.loadBalanceConfig()).thenReturn(config);
        AbstractRequestScheduler registry = org.flexlb.balance.scheduler.SchedulerTestSupport.create(service, mock(BatchSchedulerReporter.class), mock(RequestSchedulerReporter.class),
                mock(RecentCacheKeyTraceReporter.class));
        try {
            BalanceContext context = RequestProtocolTestSupport.context(config, 202L);
            var future = RequestProtocolTestSupport.register(registry, context);
            BalanceContext slot = registry.requestSlot(202L);
            PrefillEndpoint prefill = mock(PrefillEndpoint.class);
            DecodeEndpoint decode = mock(DecodeEndpoint.class);
            var reservation = new DecodeEndpoint.ReservationHandle(1L, 202L, 1L);
            context.setFuture(future);
            RequestRoute item = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), new Response(), prefillServer(), null, prefill, decode, reservation, System.currentTimeMillis());
            RequestProtocolTestSupport.bind(registry, new RequestProtocolTestSupport.Registered(item, future));
            DeliveryClaim claim = RequestProtocolTestSupport.claimBatchWithoutPrediction(registry, item, 7L, () -> true);
            assertNotNull(claim);
            synchronized (slot) {
                startPrediction(registry, slot);
                var deadline = mock(ExpirationTimer.DecisionDeadline.class);
                when(deadline.deadlineAtMs()).thenReturn(slot.decisionDeadlineAtMs().orElseThrow());
                assertTrue(slot.installDecisionDeadline(deadline));
                slot.onDecisionVisibilityDeadline(deadline);
                assertTrue(slot.snapshot().detail().startsWith("SUSPECTED_LOST"));
                assertFalse((slot.cancellationReason() != null));
            }
            if (ackBeforeEvidence) {
                claim.complete(DeliveryResult.delivered());
            }
            if (ackBeforeEvidence) {
                assertTrue(future.get(1L, TimeUnit.SECONDS).isSuccess(), "uncertainty cannot hold a real EnqueueBatch ACK");
            } else {
                assertFalse(future.isDone());
            }
            AbstractRequestScheduler projector = registry;
            if (evidenceSource == RoleType.PREFILL) {
                projector.onPrefillStatus(prefill, RoleType.PREFILL, List.of(PrefillState.WorkerStatusFact.active(item)));
            } else {
                projector.onDecodeStatus(decode, List.of(DecodeEndpoint.WorkerStatusFact.active(reservation)));
            }
            if (!ackBeforeEvidence) {
                assertFalse(future.isDone(), "Engine activity cannot create an EnqueueBatch ACK");
                claim.complete(DeliveryResult.delivered());
            }
            assertTrue(future.get(1L, TimeUnit.SECONDS).isSuccess());
            RequestProtocolTestSupport.awaitCondition(() -> {
                synchronized (slot) {
                    return evidenceSource == RoleType.DECODE ? slot.decodeAccepted() : !RequestProtocolTestSupport.<Boolean>inspect(registry, slot, "needsDecisionConfirmationLocked");
                }
            });
            synchronized (slot) {
                assertEquals(evidenceSource == RoleType.DECODE, slot.decodeAccepted());
                assertTrue(slot.isLiveGeneration());
                observePrefill(registry, slot, item, true);
                assertFalse(RequestProtocolTestSupport.<Boolean>inspect(registry, slot, "needsDecisionConfirmationLocked"));
            }
        } finally {
            if (RequestProtocolTestSupport.closeAdmissionAndAwaitMutations(registry)) {
                registry.closeOutstandingAndTerminalize();
                org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(registry).timer().close();
                org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(registry).closeRequestExecutors();
            }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"install", "reject", "throw"})
    void earlyTimerFiresOnlyAfterSuccessfulInstallation(String outcome) throws Exception {
        AbstractRequestScheduler scheduler = mock(AbstractRequestScheduler.class);
        BalanceContext context = org.mockito.Mockito.spy(RequestProtocolTestSupport.context(SchedulingTestConfig.newConfig(), 901L));
        when(SchedulerTestSupport.repository(scheduler).isCurrent(context)).thenReturn(true);
        context.bindScheduler(scheduler);
        var exact = new java.util.concurrent.atomic.AtomicReference<ExpirationTimer.RequestDeadline>();
        var failure = new IllegalStateException("installation failed");
        try (var timer = new ExpirationTimer(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(scheduler))) {
            var executor = (java.util.concurrent.ScheduledThreadPoolExecutor)
                    org.springframework.test.util.ReflectionTestUtils.getField(timer, "executor");
            doAnswer(call -> {
                exact.set(call.getArgument(0));
                // The zero-delay task has run, but it cannot expire an uninstalled capability.
                executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
                verify(scheduler, never()).onSchedulingDeadline(any(), any());
                if ("throw".equals(outcome)) { throw failure; }
                return "install".equals(outcome);
            }).when(context).installRequestDeadline(any());

            if ("throw".equals(outcome)) {
                assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> timer.attachRequestDeadline(context, 0L)));
            } else {
                var installed = timer.attachRequestDeadline(context, 0L);
                if ("install".equals(outcome)) {
                    assertSame(exact.get(), installed);
                    verify(scheduler).onSchedulingDeadline(context, installed);
                } else {
                    assertNull(installed);
                }
            }
            assertNotNull(exact.get());
            assertFalse(exact.get().publishAfterInstall(), "consumed or canceled registration cannot fire again");
            assertFalse(exact.get().consume());
            assertFalse(exact.get().cancel());
            if (!"install".equals(outcome)) {
                verify(scheduler, never()).onSchedulingDeadline(any(), any());
            }
            assertTrue(executor.getQueue().isEmpty());
        }
    }

    @Test
    void timerChecksInactivityAfterThePublicFutureHasCompleted() throws Exception {
        Fixture fixture = fixture(true);
        AbstractRequestScheduler registry = fixture.scheduler;
        ConfigService config = mock(ConfigService.class);
        when(config.loadBalanceConfig()).thenReturn(fixture.config);
        doAnswer(invocation -> {
            synchronized (fixture.slot) {
                assertTrue(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(fixture.slot, "consumeInactivityDeadlineLocked", invocation.getArgument(1, ExpirationTimer.InactivityDeadline.class)));
                assertTrue(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "requestInactiveLocked", invocation.getArgument(2, Long.class)));
                finishInactivity(fixture.scheduler, fixture.slot);
            }
            invocation.getArgument(3, Runnable.class).run();
            return null;
        }).when(registry).enqueueInactivityDeadline(any(), any(), anyLong(), any());
        try (var timer = new ExpirationTimer(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry))) {
            synchronized (fixture.slot) {
                RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
                RequestProtocolTestSupport.markAcknowledged(fixture.slot);
                fixture.slot.future().completeOwned(new Response());
                // This test expires an already delivered request; shortening
                // the timeout before claim would instead reject the handoff.
                fixture.slot.configureInactivityTimeout(20L);
            }
            assertNotNull(timer.attachInactivityDeadline(fixture.slot));
            verify(registry, timeout(1000L).times(1)).enqueueInactivityDeadline(any(), any(), anyLong(), any());
            verify(registry, never()).cancelRequest(anyLong(), anyLong(), any());
        }
    }

    @Test
    void timerCloseWaitsForAnAlreadyStartedInactivityHandoff() throws Exception {
        Fixture fixture = fixture(true);
        AbstractRequestScheduler registry = fixture.scheduler;
        ConfigService config = mock(ConfigService.class);
        when(config.loadBalanceConfig()).thenReturn(fixture.config);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    release.await();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return null;
        }).when(registry).enqueueInactivityDeadline(any(), any(), anyLong(), any());
        ExpirationTimer timer = new ExpirationTimer(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry));
        Thread closer = null;
        try {
            fixture.slot.configureInactivityTimeout(20L);
            assertNotNull(timer.attachInactivityDeadline(fixture.slot));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer = new Thread(timer::close);
            closer.start();
            assertTrue(closer.isAlive(), "close must wait for the running timer producer");
        } finally {
            release.countDown();
            timer.close();
            if (closer != null) {
                closer.join(5_000);
            }
        }
        assertFalse(closer.isAlive());
    }

    @Test
    void renewedActivityRearmsTheTimerUntilTheNewInactivityDeadline() throws Exception {
        Fixture fixture = fixture(true);
        AbstractRequestScheduler registry = fixture.scheduler;
        ConfigService config = mock(ConfigService.class);
        when(config.loadBalanceConfig()).thenReturn(fixture.config);
        long start = fixture.slot.createdAtMs();
        AtomicLong now = new AtomicLong(start + 100L);
        AtomicInteger checks = new AtomicInteger();
        CountDownLatch expired = new CountDownLatch(1);
        doAnswer(invocation -> {
            synchronized (fixture.slot) {
                assertTrue(org.springframework.test.util.ReflectionTestUtils.<Boolean>invokeMethod(fixture.slot, "consumeInactivityDeadlineLocked", invocation.getArgument(1, ExpirationTimer.InactivityDeadline.class)));
                if (checks.incrementAndGet() == 1) {
                    // A matching status wins after the old timer fired but before cancellation.
                    fixture.slot.acceptPrefillStatus(fixture.item.prefillEp(), RoleType.PREFILL, PrefillState.WorkerStatusFact.active(fixture.item), start + 50L);
                    assertFalse(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "requestInactiveLocked", invocation.getArgument(2, Long.class)));
                    now.set(start + 150L);
                } else {
                    assertTrue(RequestProtocolTestSupport.<Boolean>inspect(fixture.scheduler, fixture.slot, "requestInactiveLocked", invocation.getArgument(2, Long.class)));
                    finishInactivity(fixture.scheduler, fixture.slot);
                    expired.countDown();
                }
            }
            invocation.getArgument(3, Runnable.class).run();
            return null;
        }).when(registry).enqueueInactivityDeadline(any(), any(), anyLong(), any());
        try (var timer = new ExpirationTimer(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry), now::get)) {
            synchronized (fixture.slot) {
                fixture.slot.configureInactivityTimeout(100L);
                RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
                RequestProtocolTestSupport.markAcknowledged(fixture.slot);
            }
            assertNotNull(timer.attachInactivityDeadline(fixture.slot));
            assertTrue(expired.await(1L, TimeUnit.SECONDS));
            assertEquals(2, checks.get(), "the renewed request must retain a timer for later silence");
        }
    }

    @Test
    void oldVisibilityTimerCannotInstallAfterPrefillCompletion() {
        Fixture fixture = fixture(true);
        synchronized (fixture.slot) {
            RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
            startPrediction(fixture.scheduler, fixture.slot);
            long oldDeadline = fixture.slot.decisionDeadlineAtMs().orElseThrow();
            var oldTimer = mock(ExpirationTimer.DecisionDeadline.class);
            when(oldTimer.deadlineAtMs()).thenReturn(oldDeadline);
            fixture.slot.acceptPrefillStatus(fixture.item.prefillEp(), RoleType.PREFILL, PrefillState.WorkerStatusFact.terminal(fixture.item, PrefillState.WorkerStatusFact.Kind.COMPLETED, 0L), oldDeadline + 100L);
            assertFalse(fixture.slot.installDecisionDeadline(oldTimer));
            var handoffTimer = mock(ExpirationTimer.DecisionDeadline.class);
            when(handoffTimer.deadlineAtMs()).thenReturn(fixture.slot.decisionDeadlineAtMs().orElseThrow());
            assertTrue(fixture.slot.installDecisionDeadline(handoffTimer));
            assertStaleDecisionHasNoEffect(fixture, oldTimer);
            fixture.slot.onDecisionVisibilityDeadline(handoffTimer);
            assertTrue(fixture.slot.snapshot().detail().startsWith("SUSPECTED_LOST"));
        }
    }

    @Test
    void decodeAcceptanceDetachesDecisionTimerButKeepsTheCancellationAndInactivityWatch() {
        Fixture fixture = fixture(true);
        synchronized (fixture.slot) {
            RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
            startPrediction(fixture.scheduler, fixture.slot);
            var timer = mock(ExpirationTimer.DecisionDeadline.class);
            when(timer.deadlineAtMs()).thenReturn(fixture.slot.decisionDeadlineAtMs().orElseThrow());
            assertTrue(fixture.slot.installDecisionDeadline(timer));
            RequestProtocolTestSupport.recordCancellation(fixture.scheduler, fixture.slot, CancelReason.CLIENT_CANCELLED, "client cancellation");
            var acceptance = fixture.slot.markDecodeAcceptedLocked();
            assertSame(timer, acceptance);
            assertStaleDecisionHasNoEffect(fixture, timer);
            assertEquals(CancelReason.CLIENT_CANCELLED, RequestProtocolTestSupport.<CancelReason>inspect(fixture.scheduler, fixture.slot, "requireCancellationFirstCauseLocked"));
            assertTrue(fixture.slot.inactivityDeadlineAtMs().isPresent());
        }
    }

    private static void finishInactivity(AbstractRequestScheduler scheduler, BalanceContext slot) {
        TerminalAction terminal = RequestProtocolTestSupport.claimTerminal(scheduler, slot, TerminalOutcome.timeout("request inactive"), null, false);
        assertNotNull(terminal);
        scheduler.commitTerminalRecord(slot, terminal);
        assertEquals(BalanceContext.RequestStage.FINISHED, slot.stage());
    }

    private static WorkSnapshot emptyWork(long capturedAtMs) {
        return new WorkSnapshot(capturedAtMs, List.of(), List.of(), 0L);
    }

    private static WorkSnapshot unknownWork(long capturedAtMs) {
        return new WorkSnapshot(capturedAtMs, List.of(), List.of(), 1L);
    }

    private static OptionalLong visibilityDeadline(WorkSnapshot preceding, long unstartedWorkMs, double lifetime, long deliveredAtMs) {
        Fixture fixture = fixture(true, lifetime);
        synchronized (fixture.slot) {
            fixture.slot.updateDeliveryPredictionLocked(preceding, unstartedWorkMs, deliveredAtMs);
            return fixture.slot.decisionDeadlineAtMs();
        }
    }

    private static void observePrefillAt(Fixture fixture, boolean completed, long nowMs) {
        fixture.slot.acceptPrefillStatus(fixture.item.prefillEp(), RoleType.PREFILL, completed ? PrefillState.WorkerStatusFact.terminal(fixture.item, PrefillState.WorkerStatusFact.Kind.COMPLETED, 0L) : PrefillState.WorkerStatusFact.active(fixture.item), nowMs);
    }

    private static Fixture fixture(boolean separateDecode) {
        return fixture(separateDecode, 2.0);
    }

    private static Fixture fixture(boolean separateDecode, double lifetime) {
        FlexlbConfig config = SchedulingTestConfig.newConfig();
        config.getRequestLifecycle().getDecision().setLifetime(lifetime);
        SchedulingTestConfig.useNonBatchDispatcher(config);
        BalanceContext context = RequestProtocolTestSupport.context(config, 101L);
        BalanceContext slot = context;
        AbstractRequestScheduler requestOwner = RequestProtocolTestSupport.initialize(mock(RequestCompletionPublisher.class), slot, mock(ExpirationTimer.class));
        PrefillEndpoint prefill = mock(PrefillEndpoint.class);
        DecodeEndpoint decode = separateDecode ? mock(DecodeEndpoint.class) : null;
        DecodeEndpoint.ReservationHandle reservation = separateDecode ? new DecodeEndpoint.ReservationHandle(1L, 101L, 1L) : null;
        context.setFuture(slot.future());
        RequestRoute item = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), new Response(), prefillServer(), null, prefill, decode, reservation, System.currentTimeMillis());
        AdmissionHandle mutation;
        synchronized (slot) {
            slot.configureInactivityTimeout(60_000L);
            mutation = RequestProtocolTestSupport.beginAdmission(requestOwner, slot);
            assertNotNull(mutation);
        }
        assertEquals(org.flexlb.balance.PlacementResult.Status.SUCCESS, requestOwner.commitRoute(item, RequestProtocolTestSupport.publication(() -> true)));
        mutation.finish();
        return new Fixture(requestOwner, config, slot, item);
    }

    private static Runnable observePrefill(AbstractRequestScheduler scheduler, BalanceContext slot, RequestRoute item, boolean completed) {
        return slot.acceptPrefillStatus(item.prefillEp(), RoleType.PREFILL, completed ? PrefillState.WorkerStatusFact.terminal(item, PrefillState.WorkerStatusFact.Kind.COMPLETED, 0L) : PrefillState.WorkerStatusFact.active(item), System.currentTimeMillis());
    }

    private static void startPrediction(AbstractRequestScheduler requestOwner, BalanceContext slot) {
        slot.updateDeliveryPredictionLocked(emptyWork(System.currentTimeMillis()), 100L, System.currentTimeMillis());
    }

    private static void assertStaleDecisionHasNoEffect(Fixture fixture, ExpirationTimer.DecisionDeadline stale) {
        var before = fixture.slot.snapshot();
        var installed = fixture.slot.decisionDeadline();
        var deadline = RequestProtocolTestSupport.<java.util.OptionalLong>field(fixture.slot, "decisionExpiresAtMs");
        boolean expired = RequestProtocolTestSupport.<Boolean>field(fixture.slot, "decisionExpired");
        fixture.slot.onDecisionVisibilityDeadline(stale);
        assertEquals(before, fixture.slot.snapshot());
        assertSame(installed, fixture.slot.decisionDeadline());
        assertEquals(deadline, RequestProtocolTestSupport.<java.util.OptionalLong>field(fixture.slot, "decisionExpiresAtMs"));
        assertEquals(expired, RequestProtocolTestSupport.<Boolean>field(fixture.slot, "decisionExpired"));
        assertSame(fixture.item, fixture.slot.activeItem());
    }

    private static void expireDecision(Fixture fixture) {
        RequestProtocolTestSupport.startRouteDelivery(fixture.scheduler, fixture.slot);
        startPrediction(fixture.scheduler, fixture.slot);
        var deadline = mock(ExpirationTimer.DecisionDeadline.class);
        when(deadline.deadlineAtMs()).thenReturn(fixture.slot.decisionDeadlineAtMs().orElseThrow());
        assertTrue(fixture.slot.installDecisionDeadline(deadline));
        fixture.slot.onDecisionVisibilityDeadline(deadline);
    }

    private static ServerStatus prefillServer() {
        ServerStatus server = new ServerStatus();
        server.setRole(RoleType.PREFILL);
        server.setServerIp("127.0.0.1");
        server.setGrpcPort(8081);
        return server;
    }

    private record Fixture(AbstractRequestScheduler scheduler, FlexlbConfig config, BalanceContext slot, RequestRoute item) {
    }
}
