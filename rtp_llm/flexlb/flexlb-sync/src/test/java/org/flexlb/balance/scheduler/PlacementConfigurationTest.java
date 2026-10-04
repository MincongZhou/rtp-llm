package org.flexlb.balance.scheduler;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.eviction.EvictionManager;
import org.flexlb.config.ConfigService;
import org.flexlb.config.SchedulerConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PlacementConfigurationTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void springAssemblyInstallsStrategiesBeforeServingAndRuntimeClosesThem(boolean queued) throws Exception {
        var config = SchedulingTestConfig.batchConfig();
        if (!queued) {
            config.setScheduler(SchedulerConfig.direct());
            SchedulingTestConfig.useNonBatchDispatcher(config);
        }
        ConfigService service = mock(ConfigService.class);
        when(service.loadBalanceConfig()).thenReturn(config);
        DefaultRouter router = mock(DefaultRouter.class);
        var request = RequestProtocolTestSupport.context(config, 8972L);
        when(router.select(request, null)).thenReturn(
                PlacementResult.rejected(Response.error(StrategyErrorType.NO_PREFILL_WORKER)));
        try (var spring = new AnnotationConfigApplicationContext()) {
            spring.registerBean(ConfigService.class, () -> service);
            spring.registerBean(DefaultRouter.class, () -> router);
            spring.registerBean(EndpointRegistry.class, () -> mock(EndpointRegistry.class));
            spring.registerBean(EvictionManager.class, () -> mock(EvictionManager.class));
            spring.registerBean(BatchSchedulerReporter.class, () -> mock(BatchSchedulerReporter.class));
            spring.registerBean(RequestSchedulerReporter.class, () -> mock(RequestSchedulerReporter.class));
            spring.registerBean(RecentCacheKeyTraceReporter.class, () -> mock(RecentCacheKeyTraceReporter.class));
            spring.registerBean(org.flexlb.balance.eviction.EngineCancelChannel.class, () -> mock(org.flexlb.balance.eviction.EngineCancelChannel.class));
            spring.registerBean(DefaultBatchDispatcher.class, () -> mock(DefaultBatchDispatcher.class));
            spring.register(PlacementAvailability.class, RequestRepository.class, PlacementConfiguration.class, SchedulerRuntime.class);
            spring.refresh();
            request.setGenerateInputPb(com.google.protobuf.ByteString.copyFromUtf8("input"));
            var future = spring.getBean(SchedulerRuntime.class).scheduler().submit(request);
            assertSame(future, request.getFuture());
            assertEquals(StrategyErrorType.NO_PREFILL_WORKER.getErrorCode(), future.get(3, TimeUnit.SECONDS).getCode());
        }
    }
}
