package org.flexlb.balance.scheduler;

import org.flexlb.config.FlexlbConfig;
import org.flexlb.balance.eviction.EvictionManager;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.flexlb.config.ConfigService;
import org.springframework.context.annotation.Lazy;

/** Builds the fixed-mode scheduler selected at startup. */
@Configuration(proxyBeanMethods = false)
public class PlacementConfiguration {
    @Bean(destroyMethod = "")
    public RequestScheduler requestScheduler(@Lazy DefaultRouter router,
                                             BatchSchedulerReporter reporter, @Lazy EvictionManager eviction,
                                             PlacementAvailability availability, SchedulerRuntime runtime,
                                             ConfigService configService) {
        FlexlbConfig config = configService.loadBalanceConfig();
        RequestScheduler scheduler = create(runtime, config, router, reporter, eviction, availability);
        runtime.initializeScheduler(scheduler);
        return scheduler;
    }

    static RequestScheduler create(SchedulerRuntime runtime, FlexlbConfig config, DefaultRouter router,
                                   BatchSchedulerReporter reporter, EvictionManager eviction,
                                   PlacementAvailability availability) {
        if (config.isQueue()) {
            QueuedRequestScheduler queue = new QueuedRequestScheduler(config, router, reporter,
                    eviction, runtime, availability);
            queue.start();
            return queue;
        }
        return new DirectRequestScheduler(router, runtime, config);
    }
}
