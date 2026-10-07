package org.flexlb.balance.scheduler;

import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.endpoint.PrefillActiveIndex;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.config.FlexlbConfig;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;



import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.projection.WorkSnapshot;

import java.util.List;

public final class WorkerBatcherTestSupport {

    private WorkerBatcherTestSupport() {
    }

    static DeliveryStrategy.Transaction boundaryOnly(
            RequestRoute item,
            CapacityBoundary boundary) {
        return new DeliveryStrategy.Transaction() {
            @Override
            public List<RequestRoute> items() {
                return List.of();
            }

            @Override
            public RequestRoute blockedItem() {
                return item;
            }

            @Override
            public CapacityBoundary blockedResult() {
                return boundary;
            }

            @Override
            public PrefillState.WorkCapture commitLocked() {
                throw new IllegalStateException(
                        "boundary-only preparation cannot commit");
            }

            @Override
            public void handoff(
                    String decisionReason, int remainingQueueDepth,
                    WorkSnapshot precedingWork) {
                throw new IllegalStateException(
                        "boundary-only preparation cannot hand off");
            }

            @Override
            public void abort(Throwable cause) {
            }

            @Override
            public void close() {
            }
        };
    }
    public static WorkerBatcher create(String key, PrefillEndpoint endpoint, FlexlbConfig config,
                                       DeliveryStrategy delivery, AbstractRequestScheduler scheduler) {
        var runtime = new AtomicReference<WorkerBatcher>();
        var index = PrefillActiveIndex.ordered(16, config.isPriorityOrdering()
                ? WorkerBatcher.PRIORITY_QUEUE_ORDER : WorkerBatcher.FIFO_QUEUE_ORDER);
        var state = new PrefillState(new ReentrantLock(), index);
        var worker = org.mockito.Mockito.mock(WorkerBatcher.class, org.mockito.Mockito.withSettings()
                .useConstructor(key, endpoint, QueueExecutionSettings.capture(config), delivery, state)
                .defaultAnswer(org.mockito.Mockito.CALLS_REAL_METHODS));
        org.mockito.Mockito.doAnswer(call -> {
            RequestRoute route = call.getArgument(0);
            if (route.ctx().scheduler() == null) { route.ctx().bindScheduler(scheduler); }
            return call.callRealMethod();
        }).when(worker).offer(org.mockito.ArgumentMatchers.any());
        org.springframework.test.util.ReflectionTestUtils.setField(endpoint, "runtime", worker);
        org.springframework.test.util.ReflectionTestUtils.setField(endpoint, "prefillState", state);
        runtime.set(worker);
        return runtime.get();
    }

    public static PrefillState.QueueSnapshot capture(WorkerBatcher runtime) {
        return state(runtime).captureQueue(Integer.MAX_VALUE);
    }

    public static PrefillState state(WorkerBatcher runtime) {
        return (PrefillState) ReflectionTestUtils.getField(runtime, "prefillState");
    }
}
