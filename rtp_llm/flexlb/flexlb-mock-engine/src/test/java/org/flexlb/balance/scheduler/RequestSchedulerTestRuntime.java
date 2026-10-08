package org.flexlb.balance.scheduler;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.delivery.DeliveryStrategy;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.eviction.DecodeCapacityAcquirer;
import org.flexlb.balance.eviction.EngineCancelChannel;
import org.flexlb.balance.strategy.CostBasedPrefillStrategy;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.balance.strategy.RandomStrategy;
import org.flexlb.balance.strategy.SelectedRole;
import org.flexlb.cache.monitor.CacheMetricsReporter;
import org.flexlb.config.ConfigService;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.master.WorkerStatusResponse;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** Test-only composition root for mock-engine end-to-end scheduler tests. */
public final class RequestSchedulerTestRuntime implements AutoCloseable {

    private final RequestRepository requests = new RequestRepository();
    private final PlacementAvailability placementAvailability =
            new PlacementAvailability();
    private final BindingRouter router;
    private final EndpointRegistry registry;
    private final DecodeCapacityAcquirer decodeCapacity;
    private final RequestScheduler scheduler;
    private final SchedulerRuntime runtime;

    public RequestSchedulerTestRuntime(
            ConfigService configService,
            Supplier<CapacityBoundary.Attempt<
                    BatchDeliveryStrategy.PreparedSubmission>>
                    prepareBatchSubmission,
            BatchSchedulerReporter batchReporter,
            RequestSchedulerReporter requestReporter,
            EngineCancelChannel cancelChannel) {
        Objects.requireNonNull(
                prepareBatchSubmission, "prepareBatchSubmission");
        AtomicLong batchIds = new AtomicLong();
        DeliveryStrategy delivery = configService.loadBalanceConfig().getDispatcher().requiresGenerateInput()
                ? new BatchDeliveryStrategy(prepareBatchSubmission, batchIds::incrementAndGet, batchReporter)
                : new RouteDeliveryStrategy(batchReporter);
        this.registry = new EndpointRegistry(configService, requests, batchReporter, delivery, placementAvailability);
        this.router = new BindingRouter(registry);
        this.runtime = new SchedulerRuntime(requests, registry, batchReporter, requestReporter,
                org.mockito.Mockito.mock(DefaultBatchDispatcher.class), configService,
                new org.flexlb.service.RecentCacheKeyTraceReporter(), cancelChannel);
        this.decodeCapacity = new DecodeCapacityAcquirer(cancelChannel, requests, runtime, requestReporter);
        this.scheduler = PlacementConfiguration.create(runtime, configService.loadBalanceConfig(),
                router, batchReporter, decodeCapacity, placementAvailability);
        runtime.initializeScheduler(scheduler);
    }

    public RequestRepository requestRegistry() {
        return requests;
    }

    public RequestScheduler scheduler() {
        return scheduler;
    }

    public EndpointRegistry endpointRegistry() {
        return registry;
    }

    public PlacementAvailability placementAvailability() {
        return placementAvailability;
    }

    public void bindRouter(DefaultRouter exactRouter) {
        router.bind(exactRouter);
    }

    /** Translate fixture response metadata into an exact queue admission. */
    public PlacementResult<ProvisionalRoute, PlacementKey> routeResult(
            BalanceContext context, Response response) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(response, "response");
        if (!response.isSuccess()) {
            return PlacementResult.rejected(response);
        }
        if (response.getServerStatus() == null) {
            throw new IllegalArgumentException(
                    "successful fixture route has no worker metadata");
        }

        List<SelectedRole> selections = new ArrayList<>();
        try {
            for (ServerStatus status : response.getServerStatus()) {
                if (status == null) {
                    continue;
                }
                String address = status.getServerIp() + ":" + status.getHttpPort();
                WorkerEndpoint.GenerationPin pin = registry.capture(
                        status.getRole(), address);
                if (pin == null) {
                    throw new IllegalStateException(
                            "fixture route references an unpublished endpoint: "
                                    + status.getRole() + " " + address);
                }
                SelectedRole selected = select(pin, status);
                selections.add(selected);
            }
            var result = PlacementResult.<ProvisionalRoute, PlacementKey>success(
                    ProvisionalRoute.prepare(context, selections, response));
            selections.clear();
            return result;
        } finally {
            for (SelectedRole selection : selections) {
                selection.close();
            }
        }
    }

    /** Apply a newer worker status or refresh liveness from a same-version heartbeat. */
    public void applyStatus(
            WorkerStatus status, WorkerStatusResponse response) {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(response, "response");
        RoleType role = Objects.requireNonNull(response.getRole(), "response role");
        WorkerEndpoint endpoint = registry.get(
                role, status.getIpPort(), status);
        if (endpoint == null) {
            throw new IllegalStateException(
                    "status generation has no published endpoint: "
                            + status.getIpPort() + "#" + status.getGenerationId());
        }

        Runnable projection;
        status.lock.lock();
        try {
            long responseVersion = Objects.requireNonNull(
                    response.getStatusVersion(), "response status version");
            long committedVersion = status.appliedStatusCursor().statusVersion();
            if (responseVersion < committedVersion) {
                throw new IllegalArgumentException(
                        "worker status version regressed: committed="
                                + committedVersion + ", response=" + responseVersion);
            }
            WorkerStatus.StatusObservation observation =
                    status.freezeStatusResponse(response);
            if (responseVersion == committedVersion) {
                projection = endpoint.observeStatusHeartbeat(status, observation);
            } else {
                WorkerStatus.PreparedStatus prepared =
                        status.prepareNewStatus(observation);
                projection = endpoint.applyPreparedStatus(status, prepared);
            }
        } finally {
            status.lock.unlock();
        }
        projection.run();
    }

    private static SelectedRole select(
            WorkerEndpoint.GenerationPin pin, ServerStatus status) {
        try {
            return switch (status.getRole()) {
                case PREFILL, PDFUSION -> SelectedRole.prefill(
                        pin, status, Math.max(0L, status.getPrefillTime()),
                        ((PrefillEndpoint) pin.endpoint()).placementVersion());
                case DECODE -> SelectedRole.decode(
                        pin, status, ((DecodeEndpoint) pin.endpoint()).placementVersion());
                case VIT -> SelectedRole.stateless(pin, status);
                case FRONTEND -> throw new IllegalArgumentException(
                        "FRONTEND cannot be a worker route");
            };
        } catch (RuntimeException | Error failure) {
            pin.close();
            throw failure;
        }
    }

    @Override
    public void close() {
        decodeCapacity.shutdown();
        runtime.shutdown();
    }

    private static final class BindingRouter extends DefaultRouter {
        private DefaultRouter delegate;

        private BindingRouter(EndpointRegistry workers) {
            // Real constructor dependencies keep Mockito instrumentation out of
            // the selector classes exercised by the bound production router.
            super(new CostBasedPrefillStrategy(workers,
                            org.mockito.Mockito.mock(org.flexlb.cache.service.CacheAwareService.class),
                            org.mockito.Mockito.mock(org.flexlb.service.monitor.EngineHealthReporter.class), org.mockito.Mockito.mock(CacheMetricsReporter.class)),
                    new DecodeSelector(workers),
                    new RandomStrategy(workers),
                    emptyModelMeta());
        }

        private synchronized void bind(DefaultRouter exactRouter) {
            Objects.requireNonNull(exactRouter, "exactRouter");
            if (delegate != null) {
                throw new IllegalStateException("test router was already bound");
            }
            delegate = exactRouter;
        }

        @Override
        public PlacementResult<ProvisionalRoute, PlacementKey> select(
                BalanceContext context, String policyGroup) {
            return requireBound().select(context, policyGroup);
        }

        private synchronized DefaultRouter requireBound() {
            if (delegate == null) {
                throw new IllegalStateException("test router is not bound");
            }
            return delegate;
        }
    }

    private static ModelMetaConfig emptyModelMeta() {
        ModelMetaConfig meta = org.mockito.Mockito.mock(ModelMetaConfig.class);
        org.mockito.Mockito.when(meta.requiredRoles()).thenReturn(List.of());
        return meta;
    }
}
