package org.flexlb.balance.scheduler;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.eviction.EvictionManager;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.SchedulerConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RequestSchedulerContractTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void missingOwnerCannotActivateARequestOrPreventLaterRegistration(boolean queued) throws Exception {
        try (Fixture f = new Fixture(queued)) {
            var request = f.context(900);
            var originalFuture = request.getFuture();
            assertThrows(NullPointerException.class,
                    () -> f.requests.requests.register(request, null, new BalanceContext.RequestFuture((a, b, c, d) -> false)));
            assertSame(originalFuture, request.getFuture());
            assertNull(request.scheduler());
            assertNull(f.requests.requestSlot(900));

            var future = f.requests.register(request, StrategyErrorType.BATCH_SLO_EXPIRED);
            try (var admission = f.requests.claimAdmissionHandle(900, future); var admissionCompletion1 = RequestProtocolTestSupport.finishOnExit(admission)) {
                assertNotNull(admission);
                f.scheduler.cancel(900, 0, CancelReason.CLIENT_CANCELLED);
            }
            future.get(3, TimeUnit.SECONDS);
            f.scheduler.stopAccepting();
            f.scheduler.termination().toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void staleTimerCannotReopenADrainedGeneration() throws Exception {
        try (Fixture f = new Fixture(false)) {
            var request = f.context(901);
            var future = f.requests.register(request, StrategyErrorType.BATCH_SLO_EXPIRED);
            f.requests.expirationTimer().attachInactivityDeadline(request);
            ExpirationTimer.InactivityDeadline deadline = RequestProtocolTestSupport.field(request, "inactivityDeadline");
            assertNotNull(deadline);
            f.scheduler.cancel(901, 0, CancelReason.CLIENT_CANCELLED);
            future.get(3, TimeUnit.SECONDS);
            f.scheduler.stopAccepting();
            f.scheduler.termination().toCompletableFuture().get(3, TimeUnit.SECONDS);
            f.requests.enqueueInactivityDeadline(request, deadline, Long.MAX_VALUE,
                    () -> { throw new AssertionError("stale deadline must not rearm"); });
            f.requests.runtime.continuations().awaitIdle();
        }
    }

    @Test
    void queuedTerminationIncludesItsOwnWorkers() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.scheduler.stopAccepting();
            f.scheduler.termination().toCompletableFuture().get(3, TimeUnit.SECONDS);
            var decision = (Thread) org.springframework.test.util.ReflectionTestUtils.getField(f.scheduler, "decisionThread");
            var planners = (java.util.concurrent.ExecutorService)
                    org.springframework.test.util.ReflectionTestUtils.getField(f.scheduler, "planners");
            assertFalse(decision.isAlive());
            assertTrue(planners.isTerminated());
        }
    }

    @Test
    void failedGenerationStillClosesItsWorkersAfterTheLastObligation() throws Exception {
        try (Fixture f = new Fixture(true)) {
            var owner = (AbstractRequestScheduler) f.scheduler;
            var planners = (java.util.concurrent.ExecutorService)
                    org.springframework.test.util.ReflectionTestUtils.getField(owner, "planners");
            var decision = (Thread) org.springframework.test.util.ReflectionTestUtils.getField(owner, "decisionThread");
            var failure = new IllegalStateException("delivery cleanup failed");
            assertTrue(owner.beginSubmission());
            owner.recordFailure(failure);
            owner.stopAccepting();
            assertSame(failure, assertThrows(ExecutionException.class,
                    () -> owner.termination().toCompletableFuture().get()).getCause());
            assertFalse(planners.isShutdown(), "the outstanding submission still owns the generation");
            owner.release();
            assertTrue(planners.awaitTermination(3, TimeUnit.SECONDS),
                    "exceptional termination must not skip the final executor cleanup");
            decision.join(3_000);
            assertFalse(decision.isAlive());
        }
    }

    @Test
    void fixedSchedulerPreservesRequestOwnershipAndAcceptance() throws Exception {
        try (Fixture f = new Fixture(false)) {
            var service = mock(ConfigService.class);
            when(service.loadBalanceConfig()).thenReturn(f.config);
            var reporter = mock(BatchSchedulerReporter.class);
            var runtime = new SchedulerRuntime(new RequestRepository(), mock(EndpointRegistry.class),
                    reporter, mock(RequestSchedulerReporter.class), mock(DefaultBatchDispatcher.class),
                    service, mock(org.flexlb.service.RecentCacheKeyTraceReporter.class),
                    mock(org.flexlb.balance.eviction.EngineCancelChannel.class));
            runtime.initializeScheduler(PlacementConfiguration.create(runtime, f.config,
                    f.router, reporter, mock(EvictionManager.class), new PlacementAvailability()));
            try {
                var owner = (AbstractRequestScheduler) runtime.scheduler();
                var request = f.context(100);
                var future = owner.register(request, StrategyErrorType.BATCH_SLO_EXPIRED);

                assertSame(owner, runtime.scheduler());
                assertSame(owner, runtime.scheduler());
                assertSame(owner, runtime.scheduler());
                assertFalse(owner.termination().toCompletableFuture().isDone());
                var next = f.context(101);
                var nextFuture = owner.register(next, StrategyErrorType.BATCH_SLO_EXPIRED);
                assertSame(owner, runtime.scheduler());
                owner.cancel(100, 0, CancelReason.CLIENT_CANCELLED);
                owner.cancel(101, 0, CancelReason.CLIENT_CANCELLED);
                future.get(3, TimeUnit.SECONDS);
                nextFuture.get(3, TimeUnit.SECONDS);
                assertSame(owner, runtime.scheduler(), "terminal records preserve ownership too");
            } finally {
                runtime.shutdown();
            }
        }
    }

    @Test
    void observingTerminationDoesNotStopAcceptanceAndClientsCannotCompleteIt() throws Exception {
        try (Fixture f = new Fixture(false)) {
            RequestScheduler api = f.scheduler;
            var observation = api.termination().toCompletableFuture();
            assertFalse(observation.isDone());
            observation.complete(null);
            assertFalse(api.termination().toCompletableFuture().isDone());

            var request = f.context(1);
            when(f.router.select(request, null)).thenReturn(rejection());
            assertEquals(StrategyErrorType.NO_PREFILL_WORKER.getErrorCode(),
                    api.submit(request).get(3, TimeUnit.SECONDS).getCode());
            api.stopAccepting();
            api.stopAccepting();
            api.termination().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(StrategyErrorType.DISPATCH_FAILED.getErrorCode(),
                    api.submit(f.context(2)).join().getCode());
            verify(f.router, times(1)).select(any(), any());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stopRejectsNewRequestsButLetsAcceptedPlacementFinish(boolean queued) throws Exception {
        CountDownLatch selecting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (Fixture f = new Fixture(queued); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            RequestScheduler api = f.scheduler;
            var request = f.context(10);
            when(f.router.select(request, null)).thenAnswer(invocation -> {
                selecting.countDown();
                RequestProtocolTestSupport.await(release);
                return rejection();
            });
            var submission = executor.submit(() -> api.submit(request));
            try {
                assertTrue(selecting.await(3, TimeUnit.SECONDS));
                api.stopAccepting();
                assertFalse(api.termination().toCompletableFuture().isDone());
                assertFalse(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.requests).isClosed(), "draining must not close accepted admission");
                assertEquals(StrategyErrorType.DISPATCH_FAILED.getErrorCode(), api.submit(f.context(11)).join().getCode());
            } finally {
                release.countDown();
            }
            var future = submission.get(3, TimeUnit.SECONDS);
            assertSame(request.getFuture(), future);
            assertEquals(StrategyErrorType.NO_PREFILL_WORKER.getErrorCode(), future.get(3, TimeUnit.SECONDS).getCode());
            api.termination().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(0, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.requests).liveRequestCount());
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void registeredWorkCanStartAdmissionAfterStopAndCancellationStillWorks(boolean queued) throws Exception {
        try (Fixture f = new Fixture(queued)) {
            RequestScheduler api = f.scheduler;
            var request = f.context(20);
            var future = f.requests.register(request, StrategyErrorType.BATCH_SLO_EXPIRED);
            api.stopAccepting();
            try (var admission = f.requests.claimAdmissionHandle(20, future); var admissionCompletion3 = RequestProtocolTestSupport.finishOnExit(admission)) {
                assertNotNull(admission);
                assertNotNull(api.cancel(20, 0, CancelReason.CLIENT_CANCELLED));
                assertFalse(api.termination().toCompletableFuture().isDone());
            }
            assertEquals(StrategyErrorType.REQUEST_CANCELLED.getErrorCode(), future.get(3, TimeUnit.SECONDS).getCode());
            api.termination().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertNotNull(api.cancel(20, 0, CancelReason.CLIENT_CANCELLED));
            assertNull(api.cancel(999, 0, CancelReason.CLIENT_CANCELLED));
        }
    }

    @Test
    void cancelledFutureDoesNotHideAnOutstandingAdmission() throws Exception {
        try (Fixture f = new Fixture(true)) {
            RequestScheduler api = f.scheduler;
            var request = f.context(30);
            var future = f.requests.register(request, StrategyErrorType.BATCH_SLO_EXPIRED);
            try (var admission = f.requests.claimAdmissionHandle(30, future); var admissionCompletion4 = RequestProtocolTestSupport.finishOnExit(admission)) {
                assertNotNull(admission);
                assertTrue(future.cancel(true));
                api.stopAccepting();
                assertTrue(future.isDone());
                assertFalse(api.termination().toCompletableFuture().isDone());
            }
            api.termination().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(0, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.requests).liveRequestCount());
        }
    }

    @Test
    void stopFromPublicationCallbackDoesNotDeadlockOrFinishBeforePublicationDrains() throws Exception {
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (Fixture f = new Fixture(false)) {
            RequestScheduler api = f.scheduler;
            var request = f.context(40);
            var future = f.requests.register(request, StrategyErrorType.BATCH_SLO_EXPIRED);
            CompletableFuture<Response> callback = future.whenComplete((result, failure) -> {
                api.stopAccepting();
                publishing.countDown();
                RequestProtocolTestSupport.await(release);
            });
            try {
                assertTrue(f.requests.publishDecisionResponseAsync(40, future, Response.error(StrategyErrorType.NO_PREFILL_WORKER)));
                assertTrue(publishing.await(3, TimeUnit.SECONDS));
                assertTrue(future.isDone());
                assertEquals(0, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.requests).liveRequestCount());
                assertFalse(api.termination().toCompletableFuture().isDone());
            } finally {
                release.countDown();
            }
            callback.get(3, TimeUnit.SECONDS);
            api.termination().toCompletableFuture().get(3, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
    }

    private static PlacementResult<ProvisionalRoute, PlacementKey> rejection() {
        return PlacementResult.rejected(Response.error(StrategyErrorType.NO_PREFILL_WORKER));
    }

    @Test
    void cleanupFailureCannotBeReportedAsSuccessfulTermination() throws Exception {
        try (Fixture f = new Fixture(false)) {
            RequestScheduler api = f.scheduler;
            var request = f.context(50);
            var future = f.requests.register(request, StrategyErrorType.BATCH_SLO_EXPIRED);
            var endpoint = mock(PrefillEndpoint.class);
            var route = org.flexlb.balance.scheduler.RequestRoute.create(request, new Response(),
                    null, null, endpoint, null, null, System.currentTimeMillis());
            try (var admission = f.requests.claimAdmissionHandle(50, future); var admissionCompletion5 = RequestProtocolTestSupport.finishOnExit(admission)) {
                assertNotNull(admission);
                assertEquals(PlacementResult.Status.SUCCESS, f.requests.commitRoute(route, RequestProtocolTestSupport.publication(() -> true)));
            }
            var failure = new IllegalStateException("endpoint release failed");
            doThrow(failure).when(endpoint).releaseCommittedItem(route);
            api.stopAccepting();
            assertTrue(future.completeExceptionally(new IllegalStateException("request failed")));
            var terminationFailure = assertThrows(ExecutionException.class,
                    () -> api.termination().toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertSame(failure, terminationFailure.getCause());
            verify(endpoint).releaseCommittedItem(route);
        }
    }

    @Test
    void registrationKeepsContextAndRequirementsOnTheSameFrozenPriority() throws Exception {
        CountDownLatch capturing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (Fixture f = new Fixture(false); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var context = f.context(930L);
            context.setSchedulingMetadata(null);
            var request = org.mockito.Mockito.spy(context.getRequest());
            request.setPriority(19);
            org.mockito.Mockito.doAnswer(invocation -> {
                capturing.countDown();
                RequestProtocolTestSupport.await(release);
                return invocation.callRealMethod();
            }).when(request).getBlockCacheKeys();
            context.setRequest(request);
            var registration = executor.submit(() -> f.requests.register(context, StrategyErrorType.BATCH_SLO_EXPIRED));
            try {
                assertTrue(capturing.await(3, TimeUnit.SECONDS));
                request.setPriority(80);
            } finally {
                release.countDown();
            }
            var future = registration.get(3, TimeUnit.SECONDS);
            assertEquals(19, context.getRequirements().priority());
            assertEquals(context.getRequirements().priority(), context.getPriority(),
                    "context and resource admission must retain the same frozen priority");
            f.scheduler.cancel(930L, 0L, CancelReason.CLIENT_CANCELLED);
            future.get(3, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final FlexlbConfig config = SchedulingTestConfig.newConfig();
        final DefaultRouter router = mock(DefaultRouter.class);
        final AbstractRequestScheduler requests;
        final RequestScheduler scheduler;

        Fixture(boolean queued) {
            if (queued) { SchedulingTestConfig.useFifoQueue(config); } else { config.setScheduler(SchedulerConfig.direct()); }
            SchedulingTestConfig.useNonBatchDispatcher(config);
            ConfigService service = mock(ConfigService.class);
            when(service.loadBalanceConfig()).thenReturn(config);
            var reporter = mock(BatchSchedulerReporter.class);
            requests = org.flexlb.balance.scheduler.SchedulerTestSupport.create(service, reporter, mock(RequestSchedulerReporter.class),
                    mock(RecentCacheKeyTraceReporter.class));
            scheduler = org.flexlb.balance.scheduler.SchedulerTestSupport.configure(requests, service.loadBalanceConfig(), router, reporter, mock(EvictionManager.class), new PlacementAvailability());
        }

        BalanceContext context(long id) { return RequestProtocolTestSupport.context(config, id); }

        @Override
        public void close() {
            org.flexlb.balance.scheduler.SchedulerTestSupport.repository(requests).closeRegistration();
            RequestProtocolTestSupport.close(scheduler);
            requests.awaitAdmissionMutations();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(requests).timer().close();
            requests.runtime.continuations().awaitIdle();
            requests.closeOutstandingAndTerminalize();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(requests).closeRequestExecutors();
        }
    }
}
