package org.flexlb.balance.scheduler;

import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.scheduler.BalanceContext.AdmissionHandle;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryClaim;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.loadbalance.AdmissionRejectReason;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.flexlb.balance.scheduler.RequestProtocolTestSupport.await;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Frontend result selection precedes unlocked publication and cannot undo TTL cleanup. */
class RequestCompletionPublicationRaceTest {

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    @Timeout(15)
    void nestedSynchronousPublicationKeepsOuterCloseReentrant(boolean asyncOuter) throws Exception {
        var publisher = new RequestCompletionPublisher(1, mock(BatchSchedulerReporter.class));
        var context = RequestProtocolTestSupport.context(SchedulingTestConfig.batchConfig(), 510L);
        context.attachScheduler(RequestProtocolTestSupport.directOwner(mock(AbstractRequestScheduler.class)));
        var outerPermit = publisher.tryReservePublication(context, BalanceContext.PublicationKind.TERMINAL);
        var innerPermit = publisher.tryReservePublication(context, BalanceContext.PublicationKind.TERMINAL);
        var outer = new BalanceContext.RequestFuture((completion, response, failure, interrupt) -> false);
        var inner = new BalanceContext.RequestFuture((completion, response, failure, interrupt) -> false);
        var result = new BalanceContext.ResponseResult(RequestCompletionPublisher.ResponseCompletion.RESPONSE,
                new Response(), null, false);
        var innerCallback = inner.thenRun(publisher::close);
        var outerCallback = outer.thenRun(() -> {
            assertTrue(publisher.publishNow(new RequestCompletionPublisher.SelectedPublication(innerPermit, inner, result)));
            innerCallback.join();
            // Inner publication has returned, but this outer callback still owns its permit.
            publisher.close();
        });
        try {
            var publication = new RequestCompletionPublisher.SelectedPublication(outerPermit, outer, result);
            var execution = CompletableFuture.runAsync(() -> {
                if (asyncOuter) { publisher.submit(publication); }
                else { assertTrue(publisher.publishNow(publication)); }
            });
            outerCallback.get(3, TimeUnit.SECONDS);
            execution.get(3, TimeUnit.SECONDS);
            publisher.close();
            var executor = (ExecutorService) org.springframework.test.util.ReflectionTestUtils
                    .getField(publisher, "executor");
            assertTrue(executor.isTerminated());
        } finally {
            innerPermit.closePublication();
            outerPermit.closePublication();
            publisher.close();
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    @Timeout(15)
    void foreignPublicationRejectionReturnsTheOriginalOwnersPermit(boolean async) {
        try (var owner = new RequestCompletionPublisher(1, mock(BatchSchedulerReporter.class));
             var other = new RequestCompletionPublisher(1, mock(BatchSchedulerReporter.class))) {
            var context = RequestProtocolTestSupport.context(SchedulingTestConfig.batchConfig(), 511L);
            context.attachScheduler(RequestProtocolTestSupport.directOwner(mock(AbstractRequestScheduler.class)));
            var permit = owner.tryReservePublication(context, BalanceContext.PublicationKind.TERMINAL);
            try {
                var publication = new RequestCompletionPublisher.SelectedPublication(permit, null, null);
                assertThrows(IllegalStateException.class, () -> {
                    if (async) { other.submit(publication); }
                    else { other.publishNow(publication); }
                });
                assertEquals(0, org.springframework.test.util.ReflectionTestUtils
                        .getField(owner, "inFlightPublications"));
            } finally {
                permit.closePublication();
            }
        }
    }

    @Test
    void selectedCancellationCompletesWhenPublisherRejectsSubmission() throws Exception {
        var config = SchedulingTestConfig.batchConfig();
        ConfigService service = mock(ConfigService.class);
        when(service.loadBalanceConfig()).thenReturn(config);
        AbstractRequestScheduler registry = org.flexlb.balance.scheduler.SchedulerTestSupport.create(service,
                mock(BatchSchedulerReporter.class), mock(RequestSchedulerReporter.class),
                mock(RecentCacheKeyTraceReporter.class));
        try {
            CompletableFuture<Response> future = registry.register(RequestProtocolTestSupport.context(config, 503L), StrategyErrorType.BATCH_SLO_EXPIRED);
            var publisher = (RequestCompletionPublisher) org.springframework.test.util.ReflectionTestUtils
                    .getField(registry, "completionPublisher");
            var executor = (ExecutorService) org.springframework.test.util.ReflectionTestUtils
                    .getField(publisher, "executor");
            executor.shutdown();
            CompletableFuture<Thread> callbackThread = future.thenApply(ignored -> Thread.currentThread());

            registry.cancelRequest(503L, 0L, CancelReason.CLIENT_CANCELLED);
            // Waiting on the source future can help run its dependent callback on this thread.
            assertFalse(callbackThread.get(2L, TimeUnit.SECONDS) == Thread.currentThread());
            assertEquals(StrategyErrorType.REQUEST_CANCELLED.getErrorCode(),
                    future.get(2L, TimeUnit.SECONDS).getCode());
            assertEquals(RequestState.Phase.CANCELLED, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(503L, 0L).state());
        } finally {
            if (RequestProtocolTestSupport.closeAdmissionAndAwaitMutations(registry)) {
                registry.closeOutstandingAndTerminalize();
                org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(registry).timer().close();
                org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(registry).closeRequestExecutors();
            }
        }
    }

    @Test
    @Timeout(15)
    void callbackPublicationsDrainBeforeReentrantPublisherClose() throws Exception {
        var config = spy(SchedulingTestConfig.batchConfig());
        var runtime = spy(config.getInternalRuntime());
        when(runtime.getBatchDispatchCompletionThreads()).thenReturn(1);
        when(config.getInternalRuntime()).thenReturn(runtime);
        ConfigService service = mock(ConfigService.class);
        when(service.loadBalanceConfig()).thenReturn(config);
        AbstractRequestScheduler scheduler = org.flexlb.balance.scheduler.SchedulerTestSupport.create(service,
                mock(BatchSchedulerReporter.class), mock(RequestSchedulerReporter.class),
                mock(RecentCacheKeyTraceReporter.class));
        var publisher = (RequestCompletionPublisher) org.springframework.test.util.ReflectionTestUtils
                .getField(scheduler, "completionPublisher");
        int count = 256;
        var callbacks = new java.util.ArrayList<CompletableFuture<Void>>(count);
        var depth = new java.util.concurrent.atomic.AtomicInteger();
        var completed = new java.util.concurrent.atomic.AtomicInteger();
        try {
            for (int index = 0; index < count; index++) {
                long requestId = 10_000L + index;
                var context = RequestProtocolTestSupport.context(config, requestId);
                var future = RequestProtocolTestSupport.register(scheduler, context);
                boolean last = index == count - 1;
                callbacks.add(future.thenAccept(response -> {
                    assertFalse(Thread.holdsLock(context));
                    assertTrue(Thread.currentThread().getName().startsWith("request-completion-publisher-"));
                    assertEquals(1, depth.incrementAndGet(), "asynchronous publications must not recurse");
                    try {
                        assertEquals(StrategyErrorType.REQUEST_CANCELLED.getErrorCode(), response.getCode());
                        completed.incrementAndGet();
                        if (last) {
                            publisher.close();
                        } else {
                            scheduler.cancelRequest(requestId + 1, 0L, CancelReason.CLIENT_CANCELLED);
                        }
                    } finally {
                        depth.decrementAndGet();
                    }
                }));
            }
            scheduler.cancelRequest(10_000L, 0L, CancelReason.CLIENT_CANCELLED);
            CompletableFuture.allOf(callbacks.toArray(CompletableFuture[]::new)).get(10L, TimeUnit.SECONDS);
            publisher.close();
            assertEquals(count, completed.get());
            assertEquals(0, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(scheduler).liveRequestCount());
        } finally {
            RequestProtocolTestSupport.closeAdmissionAndAwaitMutations(scheduler);
            scheduler.closeOutstandingAndTerminalize();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(scheduler).timer().close();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(scheduler).closeRequestExecutors();
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = { false, true })
    @Timeout(15)
    void concurrentPublisherCloseSharesResultAndPreservesInterrupt(boolean shutdownFails) throws Exception {
        var publisher = new RequestCompletionPublisher(1, mock(BatchSchedulerReporter.class));
        var executor = (ExecutorService) org.springframework.test.util.ReflectionTestUtils
                .getField(publisher, "executor");
        RuntimeException failure = shutdownFails ? new IllegalStateException("shutdown failed") : null;
        var failedExecutor = shutdownFails ? mock(java.util.concurrent.ThreadPoolExecutor.class) : null;
        if (shutdownFails) {
            executor.shutdown();
            org.mockito.Mockito.doThrow(failure).when(failedExecutor).shutdown();
            org.springframework.test.util.ReflectionTestUtils.setField(publisher, "executor", failedExecutor);
        }
        var context = RequestProtocolTestSupport.context(SchedulingTestConfig.batchConfig(), 504L);
        context.attachScheduler(RequestProtocolTestSupport.directOwner(mock(AbstractRequestScheduler.class)));
        var permit = publisher.tryReservePublication(context, BalanceContext.PublicationKind.TERMINAL);
        assertNotNull(permit);
        var owner = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var follower = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var interruptPreserved = new java.util.concurrent.atomic.AtomicBoolean();
        var closers = Executors.newFixedThreadPool(2);
        try {
            var first = closers.submit(() -> {
                owner.set(Thread.currentThread());
                try {
                    publisher.close();
                    return null;
                } catch (Throwable problem) {
                    return problem;
                }
            });
            RequestProtocolTestSupport.awaitCondition(() -> owner.get() != null
                    && owner.get().getState() == Thread.State.WAITING);
            assertNull(publisher.tryReservePublication(context, BalanceContext.PublicationKind.TERMINAL),
                    "close must reject new reservations while an earlier publication is pending");
            var second = closers.submit(() -> {
                follower.set(Thread.currentThread());
                Thread.currentThread().interrupt();
                try {
                    publisher.close();
                    return null;
                } catch (Throwable problem) {
                    return problem;
                } finally {
                    interruptPreserved.set(Thread.currentThread().isInterrupted());
                }
            });
            RequestProtocolTestSupport.awaitCondition(() -> follower.get() != null
                    && follower.get().getState() == Thread.State.WAITING);
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            permit.abandonIfUnclaimed();
            assertSame(failure, first.get(5, TimeUnit.SECONDS));
            assertSame(failure, second.get(5, TimeUnit.SECONDS));
            assertTrue(interruptPreserved.get());
            if (shutdownFails) {
                assertSame(failure, assertThrows(IllegalStateException.class, publisher::close));
                verify(failedExecutor, org.mockito.Mockito.times(1)).shutdown();
            } else {
                publisher.close();
                assertTrue(executor.isTerminated());
            }
        } finally {
            permit.abandonIfUnclaimed();
            closers.shutdownNow();
            assertTrue(closers.awaitTermination(5, TimeUnit.SECONDS));
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    @Timeout(15)
    void inactivityWinsWhileAcknowledgementReportingAndTerminalCleanupAreBothPaused() throws Exception {
        var config = spy(SchedulingTestConfig.batchConfig());
        long timeoutMs = TimeUnit.HOURS.toMillis(1L);
        config.getRequestLifecycle().getRequest().setTimeoutMs(timeoutMs);
        var runtime = spy(config.getInternalRuntime());
        when(runtime.getBatchDispatchCompletionThreads()).thenReturn(1);
        when(config.getInternalRuntime()).thenReturn(runtime);
        ConfigService service = mock(ConfigService.class);
        when(service.loadBalanceConfig()).thenReturn(config);
        BatchSchedulerReporter reporter = mock(BatchSchedulerReporter.class);
        AbstractRequestScheduler registry = org.flexlb.balance.scheduler.SchedulerTestSupport.create(service, reporter,
                mock(RequestSchedulerReporter.class),
                mock(RecentCacheKeyTraceReporter.class));
        ExecutorService operations = Executors.newFixedThreadPool(2);
        CountDownLatch reportingEntered = new CountDownLatch(1);
        CountDownLatch resumeReporting = new CountDownLatch(1);
        CountDownLatch cleanupEntered = new CountDownLatch(1);
        CountDownLatch resumeCleanup = new CountDownLatch(1);
        try {
            BalanceContext context = RequestProtocolTestSupport.context(config, 501L);
            CompletableFuture<Response> future = RequestProtocolTestSupport.register(registry, context);
            BalanceContext slot = registry.requestSlot(501L);
            PrefillEndpoint prefill = mock(PrefillEndpoint.class);
            when(prefill.getIp()).thenReturn("prefill");
            DecodeEndpoint decode = mock(DecodeEndpoint.class);
            var reservation = new DecodeEndpoint.ReservationHandle(1L, 501L, 1L);
            context.setFuture(future);
            RequestRoute item = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), new Response(), null, null,
                    prefill, decode, reservation, slot.createdAtMs());
            RequestProtocolTestSupport.bind(registry,
                    new RequestProtocolTestSupport.Registered(item, future));
            DeliveryClaim claim = RequestProtocolTestSupport.claimBatch(
                    registry, item, 601L, () -> true);
            assertNotNull(claim);

            doAnswer(invocation -> {
                assertFalse(Thread.holdsLock(slot));
                reportingEntered.countDown();
                await(resumeReporting);
                return null;
            }).when(reporter).reportLatency(org.mockito.ArgumentMatchers.eq(BatchSchedulerReporter.Latency.DISPATCH_ACK), anyString(), anyString(), anyLong());
            doAnswer(invocation -> {
                assertFalse(Thread.holdsLock(slot));
                cleanupEntered.countDown();
                await(resumeCleanup);
                return DecodeEndpoint.ReservationReleaseResult.RELEASED;
            }).when(decode).release(reservation, DecodeEndpoint.ReleaseReason.REMOTE_CLEANUP);
            when(registry.runtime.cancelChannel().cancel(any(), anyLong(), any(), anyLong()))
                    .thenReturn(CompletableFuture.completedFuture(org.flexlb.balance.eviction.EngineCancelChannel.CancelAck.REQUEST_CLEANED));
            assertTrue(claim.tryStartSend());
            var completions = new java.util.concurrent.atomic.AtomicInteger();
            CompletableFuture<Void> callback = future.thenAccept(response -> {
                assertFalse(Thread.holdsLock(slot), "frontend callbacks must not hold the context lock");
                completions.incrementAndGet();
            });

            Future<?> acknowledgement = operations.submit(() -> claim.complete(DeliveryResult.delivered()));
            assertTrue(reportingEntered.await(2L, TimeUnit.SECONDS));
            assertEquals(RequestState.Phase.ACKNOWLEDGED, slot.snapshot().state());
            assertFalse(future.isDone());

            long handoffAtMs = (long) org.springframework.test.util.ReflectionTestUtils
                    .getField(slot, "batchEnqueueStartedAtMs");
            Future<?> expiry = operations.submit(() ->
                    RequestProtocolTestSupport.expireInactiveRequest(registry, slot, handoffAtMs + timeoutMs));
            assertTrue(cleanupEntered.await(2L, TimeUnit.SECONDS));
            assertEquals(RequestState.Phase.TIMED_OUT, slot.snapshot().state(),
                    "selected terminal is visible before endpoint cleanup completes");
            assertEquals(BalanceContext.RequestStage.FINALIZING, slot.stage());
            assertSame(slot, registry.requestSlot(501L));
            assertEquals(StrategyErrorType.INVALID_REQUEST.getErrorCode(),
                    registry.register(RequestProtocolTestSupport.context(config, 501L), StrategyErrorType.BATCH_SLO_EXPIRED).join().getCode());
            resumeReporting.countDown();
            acknowledgement.get(2L, TimeUnit.SECONDS);

            // A second response proves the obsolete ACK has run. The terminal response
            // may finish while cleanup is pending, but cannot archive the live identity.
            var barrier = registry.register(RequestProtocolTestSupport.context(config, 502L), StrategyErrorType.BATCH_SLO_EXPIRED);
            registry.cancelRequest(502L, 0L, CancelReason.CLIENT_CANCELLED);
            assertFalse(barrier.get(2L, TimeUnit.SECONDS).isSuccess());
            assertFalse(future.get(2L, TimeUnit.SECONDS).isSuccess(),
                    "an obsolete success permit cannot win after TTL claims cleanup");
            assertSame(slot, registry.requestSlot(501L));
            assertEquals(BalanceContext.RequestStage.FINALIZING, slot.stage());

            resumeCleanup.countDown();
            expiry.get(2L, TimeUnit.SECONDS);
            registry.runtime.continuations().awaitIdle();
            Response expired = future.get(2L, TimeUnit.SECONDS);
            callback.get(2L, TimeUnit.SECONDS);
            assertFalse(expired.isSuccess());
            assertEquals(StrategyErrorType.RESOURCE_EXHAUSTED.getErrorCode(), expired.getCode());
            assertEquals(AdmissionRejectReason.RESOURCE_EXHAUSTED,
                    expired.getAdmissionRejectReason());
            assertEquals(RequestState.Phase.TIMED_OUT, slot.snapshot().state());
            assertEquals(BalanceContext.RequestStage.FINISHED, slot.stage());
            assertNull(registry.requestSlot(501L));
            assertEquals(RequestState.Phase.TIMED_OUT, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(501L, 0L).state());
            assertEquals(1, completions.get());
            verify(decode).release(reservation, DecodeEndpoint.ReleaseReason.REMOTE_CLEANUP);
            verify(prefill).releaseCommittedItem(item);
        } finally {
            resumeReporting.countDown();
            resumeCleanup.countDown();
            operations.shutdownNow();
            operations.awaitTermination(5L, TimeUnit.SECONDS);
            if (RequestProtocolTestSupport.closeAdmissionAndAwaitMutations(registry)) {
                registry.closeOutstandingAndTerminalize();
                org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(registry).timer().close();
                org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(registry).closeRequestExecutors();
            }
        }
    }

    @Test
    void deliverySelectedBeforeExpiryKeepsItsResponseEvenBeforeTheFutureIsCompleted() {
        Fixture fixture = fixture();
        Response success = new Response();
        success.setSuccess(true);
        RequestCompletionPublisher.SelectedPublication publishSuccess = BalanceContext.selectPublication(fixture.slot(), fixture.delivery().publication(), RequestCompletionPublisher.ResponseCompletion.RESPONSE, success, null, false);
        assertFalse(fixture.slot().future().isDone());
        CompletableFuture<Void> callback = fixture.slot().future().thenAccept(response ->
                assertFalse(Thread.holdsLock(fixture.slot())));
        synchronized (fixture.slot()) {
            RequestProtocolTestSupport.recordCancellation(fixture.scheduler(), fixture.slot(), CancelReason.DEADLINE_EXCEEDED, "request inactive");
            TerminalAction terminal = RequestProtocolTestSupport.claimTerminal(fixture.scheduler(), fixture.slot(), TerminalOutcome.timeout("request inactive"), new Response(), true);
            assertNotNull(terminal);
            assertNull(terminal.publication(), "an already selected delivery owns the frontend result");
            fixture.scheduler().commitTerminalRecord(fixture.slot(), terminal);
            assertEquals(RequestState.Phase.TIMED_OUT, fixture.slot().snapshot().state());
        }
        assertTrue(publishSuccess.complete());
        assertSame(success, fixture.slot().future().join());
        callback.join();
    }

    @Test
    void futureCancelCannotReplaceSelectedUnpublishedDelivery() {
        Fixture fixture = fixture();
        Response success = new Response();
        success.setSuccess(true);
        RequestCompletionPublisher.SelectedPublication selected = BalanceContext.selectPublication(fixture.slot(), fixture.delivery().publication(), RequestCompletionPublisher.ResponseCompletion.RESPONSE, success, null, false);

        assertFalse(fixture.slot().future().cancel(false));
        assertFalse(fixture.slot().future().isDone());
        assertEquals(RequestState.Phase.ACKNOWLEDGED, fixture.slot().snapshot().state());
        assertTrue(selected.complete());
        assertSame(success, fixture.slot().future().join());
    }

    @ParameterizedTest
    @EnumSource(TerminalForm.class)
    void terminalSelectionInvalidatesAnUnpublishedAcknowledgementForEveryCompletionKind(TerminalForm form) {
        Fixture fixture = fixture();
        Response failure = new Response();
        failure.setSuccess(false);
        TerminalAction terminal;
        synchronized (fixture.slot()) {
            // ACKNOWLEDGED records the Engine fact, not a selected frontend result.
            terminal = RequestProtocolTestSupport.claimTerminal(fixture.scheduler(), fixture.slot(), TerminalOutcome.fail("worker failed before response publication"), failure, failure != null);
            assertNotNull(terminal.publication());
            fixture.scheduler().commitTerminalRecord(fixture.slot(), terminal);
        }
        Response success = new Response();
        success.setSuccess(true);
        assertFalse(BalanceContext.selectPublication(fixture.slot(), fixture.delivery().publication(), RequestCompletionPublisher.ResponseCompletion.RESPONSE, success, null, false).complete());
        assertFalse(fixture.slot().future().isDone());
        CompletableFuture<Void> callback = fixture.slot().future().handle((response, error) -> {
            assertFalse(Thread.holdsLock(fixture.slot()));
            return null;
        });
        RequestCompletionPublisher.SelectedPublication publication = switch (form) {
            case RESPONSE -> BalanceContext.selectPublication(fixture.slot(), terminal.publication(), RequestCompletionPublisher.ResponseCompletion.RESPONSE, failure, null, false);
            case FAILURE -> BalanceContext.selectPublication(fixture.slot(), terminal.publication(), RequestCompletionPublisher.ResponseCompletion.FAILURE, null, new IllegalStateException("worker failed"), false);
            case CANCELLATION -> BalanceContext.selectPublication(fixture.slot(), terminal.publication(), RequestCompletionPublisher.ResponseCompletion.CANCELLATION, null, null, false);
        };
        assertTrue(publication.complete());
        callback.join();
        assertTrue(fixture.slot().future().isDone());
        if (form == TerminalForm.RESPONSE) {
            assertSame(failure, fixture.slot().future().join());
        } else {
            assertTrue(fixture.slot().future().isCompletedExceptionally());
            assertEquals(form == TerminalForm.CANCELLATION, fixture.slot().future().isCancelled());
        }
    }

    @ParameterizedTest
    @EnumSource(TerminalForm.class)
    void externalFutureOperationUnderSlotLockLeavesRequestUnchanged(TerminalForm form) {
        BalanceContext slot = RequestProtocolTestSupport.context(SchedulingTestConfig.batchConfig(), 702L);
        AbstractRequestScheduler requestOwner = RequestProtocolTestSupport.initialize(mock(RequestCompletionPublisher.class), slot, mock(ExpirationTimer.class));
        synchronized (slot) {
            assertThrows(IllegalStateException.class, () -> {
                switch (form) {
                    case RESPONSE ->
                        slot.future().complete(new Response());
                    case FAILURE ->
                        slot.future().completeExceptionally(new IllegalStateException("failure"));
                    case CANCELLATION ->
                        slot.future().cancel(false);
                }
            });
            assertTrue(slot.isOpen());
            assertEquals(RequestState.Phase.QUEUED, slot.snapshot().state());
            assertFalse(slot.future().isDone());
        }
    }

    private static Fixture fixture() {
        RequestCompletionPublisher publisher = mock(RequestCompletionPublisher.class);
        var config = SchedulingTestConfig.batchConfig();
        BalanceContext context = RequestProtocolTestSupport.context(config, 701L);
        BalanceContext slot = context;
        AbstractRequestScheduler requestOwner = RequestProtocolTestSupport.initialize(publisher, slot, mock(ExpirationTimer.class));
        when(publisher.tryReservePublication(eq(slot), any())).thenAnswer(invocation -> new RequestCompletionPublisher.PublicationPermit(publisher, slot, invocation.getArgument(1)));
        context.setFuture(slot.future());
        RequestRoute item = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), new Response(), null, null, null, null, null, slot.createdAtMs());
        BalanceContext.DeliveryPublication delivery;
        AdmissionHandle admission;
        synchronized (slot) {
            admission = RequestProtocolTestSupport.beginAdmission(requestOwner, slot);
            assertNotNull(admission);
        }
        assertEquals(org.flexlb.balance.PlacementResult.Status.SUCCESS, requestOwner.commitRoute(item, RequestProtocolTestSupport.publication(() -> true)));
        admission.finish();
        Runnable acknowledgement;
        synchronized (slot) {
            RequestProtocolTestSupport.startBatchDelivery(requestOwner, slot, 801L);
            acknowledgement = RequestProtocolTestSupport.acknowledge(requestOwner, slot);
        }
        acknowledgement.run();
        var captured = org.mockito.ArgumentCaptor.forClass(BalanceContext.DeliveryPublication.class);
        org.mockito.Mockito.verify(publisher).submitDelivery(captured.capture());
        delivery = captured.getValue();
        assertNotNull(delivery);
        return new Fixture(requestOwner, slot, delivery);
    }

    private enum TerminalForm { RESPONSE, FAILURE, CANCELLATION }

    private record Fixture(AbstractRequestScheduler scheduler, BalanceContext slot, BalanceContext.DeliveryPublication delivery) { }
}
