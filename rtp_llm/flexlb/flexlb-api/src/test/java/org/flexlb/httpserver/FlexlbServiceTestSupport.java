package org.flexlb.httpserver;

import org.flexlb.config.FlexlbConfig;
import org.flexlb.balance.scheduler.*;
import org.flexlb.config.ConfigService;
import org.flexlb.consistency.MasterElectService;
import org.flexlb.service.monitor.*;

/** Assembles API fixtures through the same scheduler and configuration as production. */
final class FlexlbServiceTestSupport {
    static FlexlbServiceImpl create(RequestScheduler scheduler, RequestRepository requests,
            MasterElectService leadership, EngineHealthReporter health, FlexlbGrpcForwarder forwarder,
            ConfigService config, BatchSchedulerReporter batches, ServerScheduleLatencyRecorder latency,
            RequestSchedulerReporter reporter) {
        if (org.mockito.Mockito.mockingDetails(leadership).isMock()) {
            org.mockito.Mockito.doCallRealMethod().when(leadership).shouldForwardToMaster();
        }
        return new FlexlbServiceImpl(
                scheduler, config.loadBalanceConfig(), requests, leadership, health, forwarder, batches, latency, reporter);
    }
}
