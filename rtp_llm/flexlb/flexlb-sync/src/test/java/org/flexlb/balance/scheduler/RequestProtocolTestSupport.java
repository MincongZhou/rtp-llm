package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeResources;
import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.BalanceContext.AdmissionHandle;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryClaim;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.SchedulingMetadata;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared lifecycle primitives for scheduler contract tests.
 */
final class RequestProtocolTestSupport {
    static DecodeEndpoint decodeEndpoint() {
        DecodeEndpoint endpoint = org.mockito.Mockito.mock(DecodeEndpoint.class);
        org.mockito.Mockito.lenient().when(endpoint.release(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
                .thenReturn(DecodeResources.ReservationReleaseResult.RELEASED);
        return endpoint;
    }

    interface AdmissionCompletion extends AutoCloseable { @Override void close(); }

    static AdmissionCompletion finishOnExit(AdmissionHandle handle) {
        return () -> { if (handle != null) { handle.finish(); } };
    }

    static ProvisionalRoute.Publication publication(java.util.function.BooleanSupplier action) {
        return new ProvisionalRoute.Publication() {
            private boolean published;
            @Override public void publish() { published = action.getAsBoolean(); }
            @Override public boolean published() { return published; }
        };
    }

    static boolean publish(ProvisionalRoute.Publication action) { action.publish(); return action.published(); }

    static PrefillAdmissionResources.Member handoff(java.util.function.BooleanSupplier action) {
        var member = org.mockito.Mockito.mock(PrefillAdmissionResources.Member.class);
        org.mockito.Mockito.when(member.transferToEndpoint(org.mockito.ArgumentMatchers.any())).thenAnswer(unused -> action.getAsBoolean());
        return member;
    }

    static boolean closeAdmissionAndAwaitMutations(AbstractRequestScheduler scheduler) {
        if (!org.flexlb.balance.scheduler.SchedulerTestSupport.repository(scheduler).closeRegistration()) { return false; }
        scheduler.awaitAdmissionMutations();
        return true;
    }

    static void expireInactiveRequest(AbstractRequestScheduler scheduler, BalanceContext context, long nowMs) {
        if (context == null) { return; }
        Runnable effect = inspect(scheduler, context, "decideInactivityLocked", nowMs, null);
        ReflectionTestUtils.invokeMethod(scheduler, "execute", context, effect);
    }

    static AdmissionHandle beginAdmission(AbstractRequestScheduler scheduler, BalanceContext context) {
        return scheduler.claimAdmissionHandle(context.getRequestId(), context.getFuture());
    }

    @SuppressWarnings("unchecked")
    static <T> T field(BalanceContext context, String name) {
        return (T) ReflectionTestUtils.getField(context, name);
    }

    /**
     * Seed cancellation in state-only fixtures without exposing an internal production operation.
     */
    static boolean recordCancellation(AbstractRequestScheduler scheduler, BalanceContext requestContext, CancelReason reason, String message) {
        return Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(scheduler, "recordCancellationLocked", requestContext, reason, message));
    }

    /**
     * Inspect a private decision in state-only fixtures without widening the production API.
     */
    static <T> T inspect(AbstractRequestScheduler scheduler, BalanceContext requestContext, String decision, Object... arguments) {
        synchronized (requestContext) {
            boolean requestDecision = java.util.Arrays.stream(BalanceContext.class.getDeclaredMethods())
                    .anyMatch(method -> method.getName().equals(decision));
            return requestDecision ? ReflectionTestUtils.invokeMethod(requestContext, decision, arguments)
                    : ReflectionTestUtils.invokeMethod(scheduler, decision, prepend(requestContext, arguments));
        }
    }

    static QueuedRequestScheduler publication(AbstractRequestScheduler requests) { return (QueuedRequestScheduler) requests; }

    static QueuedRequestScheduler queue(org.flexlb.config.ConfigService config, DefaultRouter router,
            org.flexlb.service.monitor.BatchSchedulerReporter reporter,
            org.flexlb.balance.eviction.EvictionManager eviction, AbstractRequestScheduler owner, PlacementAvailability availability) {
        return (QueuedRequestScheduler) SchedulerTestSupport.configure(owner, config.loadBalanceConfig(), router, reporter, eviction, availability);
    }
    static RequestScheduler configure(AbstractRequestScheduler owner, org.flexlb.config.ConfigService config,
            DefaultRouter router, org.flexlb.service.monitor.BatchSchedulerReporter reporter,
            org.flexlb.balance.eviction.EvictionManager eviction, PlacementAvailability availability) {
        return SchedulerTestSupport.configure(owner, config.loadBalanceConfig(), router, reporter, eviction, availability);
    }

    static void close(RequestScheduler scheduler) {
        SchedulerTestSupport.runtime(scheduler).stopAccepting();
        if (scheduler instanceof QueuedRequestScheduler queue) { queue.close(); }
    }

    static int queuedCount(RequestScheduler scheduler) {
        return scheduler instanceof QueuedRequestScheduler queue ? queue.size() : 0;
    }

    static CompletableFuture<Response> register(AbstractRequestScheduler owner, BalanceContext context) {
        var future = owner.register(context, context.getConfig().isQueue()
                ? StrategyErrorType.RESOURCE_EXHAUSTED : StrategyErrorType.BATCH_SLO_EXPIRED);
        if (context.scheduler() == owner && !future.isDone()) {
            if (context.getConfig().isQueue()) { owner.expirationTimer().attachRequestDeadline(context, context.getRequestExpiresAtMs()); }
            owner.expirationTimer().attachInactivityDeadline(context);
        }
        return future;
    }

    static AbstractRequestScheduler directOwner(AbstractRequestScheduler requests) { return requests; }

    /** Isolates queue ordering from the common request protocol. */
    static AbstractRequestScheduler schedulerMock() {
        var config = SchedulingTestConfig.batchConfig();
        var service = org.mockito.Mockito.mock(org.flexlb.config.ConfigService.class);
        org.mockito.Mockito.when(service.loadBalanceConfig()).thenReturn(config);
        var runtime = new SchedulerRuntime(new RequestRepository(), org.mockito.Mockito.mock(org.flexlb.balance.endpoint.EndpointRegistry.class),
                org.mockito.Mockito.mock(org.flexlb.service.monitor.BatchSchedulerReporter.class),
                org.mockito.Mockito.mock(org.flexlb.service.monitor.RequestSchedulerReporter.class),
                org.mockito.Mockito.mock(DefaultBatchDispatcher.class), service,
                org.mockito.Mockito.mock(org.flexlb.service.RecentCacheKeyTraceReporter.class),
                org.mockito.Mockito.mock(org.flexlb.balance.eviction.EngineCancelChannel.class));
        var timer = org.mockito.Mockito.mock(ExpirationTimer.class);
        return org.mockito.Mockito.mock(QueuedRequestScheduler.class, org.mockito.Mockito.withSettings()
                .useConstructor(config, org.mockito.Mockito.mock(DefaultRouter.class), runtime.batchReporter(),
                        org.mockito.Mockito.mock(org.flexlb.balance.eviction.EvictionManager.class), runtime, new PlacementAvailability())
                .defaultAnswer(invocation -> {
                    String name = invocation.getMethod().getName();
                    if (name.equals("expirationTimer")) { return timer; }
                    if (invocation.getMethod().getDeclaringClass() == QueuedRequestScheduler.class) {
                        return invocation.callRealMethod();
                    }
                    return org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
                }));
    }

    static AbstractRequestScheduler initialize(ResponseCompletionExecutor publisher, BalanceContext context, ExpirationTimer timer) {
        var config = org.mockito.Mockito.mock(org.flexlb.config.ConfigService.class);
        org.mockito.Mockito.when(config.loadBalanceConfig()).thenReturn(context.getConfig());
        var owner = SchedulerTestSupport.create(config, org.mockito.Mockito.mock(org.flexlb.service.monitor.BatchSchedulerReporter.class),
                org.mockito.Mockito.mock(org.flexlb.service.monitor.RequestSchedulerReporter.class),
                org.mockito.Mockito.mock(org.flexlb.service.RecentCacheKeyTraceReporter.class));
        ReflectionTestUtils.setField(owner, "responseCompletions", publisher);
        ReflectionTestUtils.setField(owner, "expirationTimer", timer);
        register(owner, context);
        return owner;
    }

    static void observePrefill(AbstractRequestScheduler scheduler, PrefillEndpoint source,
            RoleType role, PrefillState.PrefillRequestStatus requestStatus) {
        observePrefill(scheduler, scheduler.findRequestContext(requestStatus.route().requestId()), source, role, requestStatus);
    }

    static void observePrefill(AbstractRequestScheduler scheduler, BalanceContext context,
            PrefillEndpoint source, RoleType role,
            PrefillState.PrefillRequestStatus requestStatus) {
        if (context != null) { run(context.scheduler().acceptPrefillStatus(context, source, role, requestStatus, System.currentTimeMillis())); }
    }

    static void observeDecode(AbstractRequestScheduler scheduler, DecodeEndpoint source,
            DecodeResources.DecodeRequestStatus requestStatus) {
        observeDecode(scheduler, scheduler.findRequestContext(requestStatus.reservation().requestId()), source, requestStatus);
    }

    static void observeDecode(AbstractRequestScheduler scheduler, BalanceContext context,
            DecodeEndpoint source, DecodeResources.DecodeRequestStatus requestStatus) {
        if (context != null) { run(context.scheduler().acceptDecodeStatus(context, source, requestStatus, System.currentTimeMillis())); }
    }

    static void expireInactivity(AbstractRequestScheduler scheduler, BalanceContext context,
            ExpirationTimer.InactivityDeadline deadline, long nowMs) {
        scheduler.enqueueInactivityDeadline(context, deadline, nowMs, () -> { });
        scheduler.runtime.continuations().awaitIdle();
    }

    private static void run(Runnable continuation) {
        if (continuation != null) { continuation.run(); }
    }

    private static Object[] prepend(BalanceContext context, Object[] arguments) {
        Object[] all = new Object[arguments.length + 1];
        all[0] = context;
        System.arraycopy(arguments, 0, all, 1, arguments.length);
        return all;
    }

    static TerminalAction claimTerminal(AbstractRequestScheduler scheduler, BalanceContext requestContext,
            TerminalOutcome outcome, Response response, boolean publish) {
        return requestContext.claimFinalizationLocked(null, outcome, response, publish,
                () -> ReflectionTestUtils.invokeMethod(scheduler, "requirePublicationPermitLocked",
                        requestContext, BalanceContext.PublicationKind.TERMINAL));
    }

    private RequestProtocolTestSupport() {
    }

    static BalanceContext context(FlexlbConfig config, long requestId) {
        Request request = new Request();
        request.setRequestId(requestId);
        request.setSeqLen(16L);
        SchedulingTestConfig.configureRequiredValues(config);
        BalanceContext context = new BalanceContext(config);
        context.setRequest(request);
        context.setGenerateInputPb(com.google.protobuf.ByteString.copyFromUtf8("test-input"));
        context.setSchedulingMetadata(SchedulingMetadata.explicit(
                50, System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1)));
        return context;
    }

    static void bind(AbstractRequestScheduler lifecycle, Registered registered) {
        try (AdmissionHandle admission = lifecycle.claimAdmissionHandle(registered.item().requestId(), registered.future()); var admissionCompletion1 = RequestProtocolTestSupport.finishOnExit(admission)) {
            assertNotNull(admission);
            assertTrue((lifecycle.commitRoute(registered.item(), RequestProtocolTestSupport.publication(() -> true)) == org.flexlb.balance.PlacementResult.Status.SUCCESS));
        }
    }

    static void bindRoute(AbstractRequestScheduler lifecycle, Registered registered) {
        assertEquals(PlacementResult.Status.SUCCESS, commitRoute(lifecycle, registered));
    }

    static PlacementResult.Status commitRoute(AbstractRequestScheduler lifecycle, Registered registered) {
        try (AdmissionHandle admission = lifecycle.claimAdmissionHandle(registered.item().requestId(), registered.future()); var admissionCompletion2 = RequestProtocolTestSupport.finishOnExit(admission)) {
            assertNotNull(admission);
            return lifecycle.commitRoute(registered.item(), RequestProtocolTestSupport.publication(() -> true));
        }
    }

    static DeliveryClaim claimRoute(AbstractRequestScheduler registry, RequestRoute item, BooleanSupplier handoff) {
        DeliveryClaim claim = claimRouteWithoutPrediction(registry, item, handoff);
        if (claim != null) {
            registry.setDeliveryPrediction(claim, emptyWork(), 30_000L);
        }
        return claim;
    }

    static DeliveryClaim claimBatch(AbstractRequestScheduler registry, RequestRoute item, long batchId, BooleanSupplier handoff) {
        DeliveryClaim claim = claimBatchWithoutPrediction(registry, item, batchId, handoff);
        if (claim != null) {
            registry.setDeliveryPrediction(claim, emptyWork(), 30_000L);
        }
        return claim;
    }

    static DeliveryClaim claimRouteWithoutPrediction(AbstractRequestScheduler registry, RequestRoute item, BooleanSupplier handoff) {
        return registry.claimDelivery(item, DeliveryClaimKind.ROUTE_DECISION, 0L, RequestProtocolTestSupport.handoff(handoff));
    }

    static DeliveryClaim claimBatchWithoutPrediction(AbstractRequestScheduler registry, RequestRoute item, long batchId, BooleanSupplier handoff) {
        return registry.claimDelivery(item, DeliveryClaimKind.BATCH_ENQUEUE, batchId, RequestProtocolTestSupport.handoff(handoff));
    }

    private static WorkSnapshot emptyWork() {
        return new WorkSnapshot(System.currentTimeMillis(), java.util.List.of(), java.util.List.of(), 0L);
    }

    // State-only fixtures deliberately seed a phase without executing publication or timers.
    static void startRouteDelivery(AbstractRequestScheduler scheduler, BalanceContext requestContext) {
        assertNotNull(scheduler.claimDelivery(requestContext.activeRoute(), DeliveryClaimKind.ROUTE_DECISION, 0L, RequestProtocolTestSupport.handoff(() -> true)));
    }

    static void startBatchDelivery(AbstractRequestScheduler scheduler, BalanceContext requestContext, long batchId) {
        assertNotNull(scheduler.claimDelivery(requestContext.activeRoute(), DeliveryClaimKind.BATCH_ENQUEUE, batchId, RequestProtocolTestSupport.handoff(() -> true)));
    }

    static void markAcknowledged(BalanceContext requestContext) {
        assertTrue(Thread.holdsLock(requestContext));
        org.springframework.test.util.ReflectionTestUtils.setField(requestContext, "deliveryAcknowledged", true);
        org.springframework.test.util.ReflectionTestUtils.setField(requestContext, "stage", BalanceContext.RequestStage.RESULT_PENDING);
    }

    static Runnable acknowledge(AbstractRequestScheduler scheduler, BalanceContext requestContext) {
        return scheduler.acknowledgeDeliveryLocked(requestContext, null);
    }

    static boolean prepareMember(AbstractRequestScheduler registry, RequestRoute item) {
        var transaction = org.mockito.Mockito.mock(DeliveryTransaction.class);
        org.mockito.Mockito.when(transaction.append(item)).thenReturn(null);
        return registry.prepareDispatch(item, transaction) == null;
    }

    static void awaitGlobalCapacityWaiters(RequestScheduler scheduler, int expected) throws InterruptedException {
        var queue = (QueuedRequestScheduler) scheduler;
        var lock = (ReentrantLock) ReflectionTestUtils.getField(queue, "lock");
        var waiting = (PlacementWaitQueue) ReflectionTestUtils.getField(queue, "waitingRequests");
        var membership = (Map<?, ?>) ReflectionTestUtils.getField(waiting, "membership");
        awaitCondition(() -> {
            lock.lock();
            try { return membership.size() == expected; }
            finally { lock.unlock(); }
        });
    }

    static void awaitGlobalCapacityWaiters(AbstractRequestScheduler scheduler, int expected)
            throws InterruptedException {
        Object coordinator = scheduler;
        var lock = (ReentrantLock)
                ReflectionTestUtils.getField(coordinator, "lock");
        var waitQueue = (PlacementWaitQueue) ReflectionTestUtils.getField(coordinator, "waitingRequests");
        var waiting = (Map<?, ?>)
                ReflectionTestUtils.getField(waitQueue, "membership");
        // A route's close callback runs before park. Observe actual wait registration
        // under the coordinator lock, rather than treating that callback as a barrier.
        awaitCondition(() -> {
            lock.lock();
            try {
                return waiting.keySet().stream().filter(entry -> waitQueue.isWaiting((GlobalQueueEntry) entry)).count() == expected;
            } finally {
                lock.unlock();
            }
        });
    }

    static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("latch was not released");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(
                    "interrupted while awaiting latch", interrupted);
        }
    }

    static void awaitCondition(BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(1L);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true");
    }

    record Registered(RequestRoute item, CompletableFuture<Response> future) {
    }
}
