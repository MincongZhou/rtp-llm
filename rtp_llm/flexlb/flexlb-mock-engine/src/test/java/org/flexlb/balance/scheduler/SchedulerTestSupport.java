package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.eviction.EngineCancelChannel;
import org.flexlb.balance.eviction.EvictionManager;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.Map;
import java.util.WeakHashMap;
import static org.mockito.Mockito.*;

/** Builds real request owners and shared facilities; contains no request state machine. */
public final class SchedulerTestSupport {
    private static final Map<AbstractRequestScheduler, RequestRepository> mockRepositories = new WeakHashMap<>();
    public static RequestRepository repository(RequestRepository value) { return value; }
    public static synchronized RequestRepository repository(AbstractRequestScheduler owner) {
        if (owner == null) { return null; }
        return owner.requests != null ? owner.requests : mockRepositories.computeIfAbsent(owner, SchedulerTestSupport::mockRepository);
    }
    private static RequestRepository mockRepository(AbstractRequestScheduler owner) {
        var repository = mock(RequestRepository.class);
        when(repository.ownerOf(anyLong())).thenReturn(owner);
        when(repository.findActive(anyLong())).thenAnswer(call -> {
            var context = mock(BalanceContext.class);
            when(context.scheduler()).thenReturn(owner);
            return context;
        });
        return repository;
    }
    public static void bindOwner(BalanceContext context, AbstractRequestScheduler owner) {
        if (context.scheduler() == null) { context.bindScheduler(owner); }
    }
    private static final Map<org.flexlb.balance.endpoint.PrefillEndpoint, RequestRepository> endpointRepositories = java.util.Collections.synchronizedMap(new WeakHashMap<>());
    public static void associateEndpoint(org.flexlb.balance.endpoint.PrefillEndpoint endpoint, RequestRepository repository) {
        endpointRepositories.put(endpoint, repository);
    }
    public static void bindEndpointOwner(org.flexlb.balance.endpoint.PrefillEndpoint endpoint, RequestRoute route) {
        if (route.ctx() == null) {
            var context = mock(BalanceContext.class);
            when(route.ctx()).thenReturn(context);
            var repository = endpointRepositories.get(endpoint);
            var owner = repository == null ? null : repository.ownerOf(route.requestId());
            when(context.scheduler()).thenReturn(owner == null ? mock(AbstractRequestScheduler.class) : owner);
        } else if (route.ctx().scheduler() == null) {
            var repository = endpointRepositories.get(endpoint);
            var owner = repository == null ? null : repository.ownerOf(route.requestId());
            bindOwner(route.ctx(), owner == null ? mock(AbstractRequestScheduler.class) : owner);
        }
    }
    public static SchedulerRuntime runtime(SchedulerRuntime value) { return value; }
    public static SchedulerRuntime runtime(RequestScheduler owner) { return ((AbstractRequestScheduler) owner).runtime; }
    public static FlexlbConfig config(AbstractRequestScheduler owner) { return owner.config; }
    public static EvictionManager eviction(AbstractRequestScheduler owner) {
        return new EvictionManager(mock(RequestSchedulerReporter.class), mock(EngineCancelChannel.class),
                mock(org.flexlb.balance.eviction.DecodePreemptionCoordinator.class), repository(owner));
    }
    static RequestRepository.TerminalRecord terminalRecord(AbstractRequestScheduler owner, RequestState state) {
        var record = repository(owner).findTerminal(state.requestId());
        return record != null && record.state() == state ? record : new RequestRepository.TerminalRecord(state, owner);
    }
    public static AbstractRequestScheduler create(ConfigService config, BatchSchedulerReporter batches,
            RequestSchedulerReporter requests, RecentCacheKeyTraceReporter trace) {
        var runtime = new SchedulerRuntime(new RequestRepository(), mock(EndpointRegistry.class), batches, requests,
                mock(DefaultBatchDispatcher.class), config, trace, mock(EngineCancelChannel.class));
        var snapshot = config.loadBalanceConfig();
        var settings = snapshot;
        AbstractRequestScheduler owner = snapshot.isQueue()
                ? mock(QueuedRequestScheduler.class, withSettings().useConstructor(settings, mock(DefaultRouter.class), batches, mock(EvictionManager.class), runtime, new PlacementAvailability()).defaultAnswer(CALLS_REAL_METHODS))
                : mock(DirectRequestScheduler.class, withSettings().useConstructor(mock(DefaultRouter.class), runtime, settings).defaultAnswer(CALLS_REAL_METHODS));
        if (owner instanceof QueuedRequestScheduler queue) {
            doAnswer(call -> { for (BalanceContext context : owner.requests.snapshotActive()) {
                if (context.getFuture() == call.getArgument(0)) { queue.processGlobalControl(context); }
            } return null; })
                    .when(queue).signalControl(org.mockito.ArgumentMatchers.any());
        }
        runtime.initializeScheduler(owner);
        return owner;
    }
    public static RequestScheduler configure(AbstractRequestScheduler owner, FlexlbConfig config,
            DefaultRouter router, BatchSchedulerReporter reporter, EvictionManager eviction, PlacementAvailability availability) {
        ReflectionTestUtils.setField(owner, "router", router);
        if (owner instanceof QueuedRequestScheduler queue) {
            ReflectionTestUtils.setField(queue, "evictionManager", eviction);
            ReflectionTestUtils.setField(queue, "reporter", reporter);
            ReflectionTestUtils.setField(queue, "plannerCount", Math.max(2, config.getInternalRuntime().getQueuePlannerThreads()));
            ReflectionTestUtils.setField(queue, "priorityOrdering", config.isPriorityOrdering());
            ReflectionTestUtils.setField(queue, "orderedQueue", new OrderedRequestQueue(config.isPriorityOrdering()));
            ReflectionTestUtils.setField(queue, "scanBudgetMultiplier", config.queueScheduler().getScanBudgetMultiplier());
            var oldAvailability = (PlacementAvailability) ReflectionTestUtils.getField(queue, "availability");
            var listener = (PlacementAvailability.Listener) ReflectionTestUtils.getField(queue, "availabilityListener");
            oldAvailability.removeListener(listener);
            ReflectionTestUtils.setField(queue, "availability", availability);
            ReflectionTestUtils.setField(queue, "waitingRequests", new PlacementWaitQueue(config.isPriorityOrdering(), availability));
            availability.addListener(listener);
            doCallRealMethod().when(queue).signalControl(org.mockito.ArgumentMatchers.any());
            if (!((Thread)ReflectionTestUtils.getField(queue, "decisionThread")).isAlive()) { queue.start(); }
        }
        return owner;
    }
}
