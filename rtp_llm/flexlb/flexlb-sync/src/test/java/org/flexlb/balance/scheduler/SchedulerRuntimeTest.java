package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.eviction.EngineCancelChannel;
import org.flexlb.balance.eviction.EvictionManager;
import org.flexlb.config.ConfigService;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.SchedulerConfig;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SchedulerRuntimeTest {
    @Test
    void directStartupUsesOneSchedulerAndSharedConfiguration() {
        try (var f = new Fixture(false)) {
            var scheduler = (AbstractRequestScheduler) f.runtime.scheduler();
            assertInstanceOf(DirectRequestScheduler.class, scheduler);
            assertSame(scheduler, f.runtime.scheduler());
            assertSame(f.config, scheduler.config);
            assertSame(f.config, new BalanceContext(f.config).getConfig());
            verify(f.endpoints, never()).configureQueue(any());
            assertThrows(IllegalStateException.class,
                    () -> f.runtime.initializeScheduler(mock(RequestScheduler.class)));
        }
    }

    @Test
    void queueStartupConfiguresSharedExecutionSettingsOnce() {
        try (var f = new Fixture(true)) {
            assertInstanceOf(QueuedRequestScheduler.class, f.runtime.scheduler());
            assertSame(f.runtime.scheduler(), f.runtime.scheduler());
            verify(f.endpoints, times(1)).configureQueue(QueueExecutionSettings.capture(f.config));
        }
    }

    @Test
    void stoppedRuntimeCannotBeInitializedAgain() {
        var f = new Fixture(false);
        f.close();
        assertThrows(IllegalStateException.class,
                () -> f.runtime.initializeScheduler(mock(RequestScheduler.class)));
    }

    private static final class Fixture implements AutoCloseable {
        final FlexlbConfig config = SchedulingTestConfig.newConfig();
        final EndpointRegistry endpoints = mock(EndpointRegistry.class);
        final SchedulerRuntime runtime;
        Fixture() { this(false); }
        Fixture(boolean queued) {
            config.setScheduler(queued ? new SchedulerConfig() : SchedulerConfig.direct());
            config.setDispatcher(DispatcherConfig.nonBatch());
            var service = mock(ConfigService.class);
            when(service.loadBalanceConfig()).thenReturn(config);
            var reporter = mock(BatchSchedulerReporter.class);
            runtime = new SchedulerRuntime(new RequestRepository(), endpoints, reporter,
                    mock(RequestSchedulerReporter.class), mock(DefaultBatchDispatcher.class), service,
                    mock(RecentCacheKeyTraceReporter.class), mock(EngineCancelChannel.class));
            runtime.initializeScheduler(PlacementConfiguration.create(runtime, config,
                    mock(DefaultRouter.class), reporter, mock(EvictionManager.class), new PlacementAvailability()));
        }
        public void close() { runtime.shutdown(); }
    }
}
