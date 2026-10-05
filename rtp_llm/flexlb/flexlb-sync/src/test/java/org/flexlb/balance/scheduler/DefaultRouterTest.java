package org.flexlb.balance.scheduler;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.BalanceContext.AdmissionHandle;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryClaim;
import org.flexlb.balance.scheduler.RequestRequirements.DecodeMode;
import org.flexlb.balance.strategy.CostBasedPrefillStrategy;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.balance.strategy.RandomStrategy;
import org.flexlb.balance.strategy.SelectedRole;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.config.TrafficPolicyConfig;
import org.flexlb.config.VictimStage;
import org.flexlb.dao.SchedulingMetadata;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.flexlb.balance.scheduler.SchedulingTestConfig.freezeInputs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Final selector/pin ownership contracts for {@link DefaultRouter}.
 */
class DefaultRouterTest {

    private DeliveryClaim lastDelivery;

    private CostBasedPrefillStrategy prefillSelector;

    private DecodeSelector decodeSelector;

    private RandomStrategy vitSelector;

    private ConfigService configService;

    private ModelMetaConfig modelMeta;

    private AbstractRequestScheduler requests;

    @BeforeEach
    void setUp() {
        prefillSelector = mock(CostBasedPrefillStrategy.class);
        decodeSelector = mock(DecodeSelector.class);
        vitSelector = mock(RandomStrategy.class);
        configService = mock(ConfigService.class);
        modelMeta = mock(ModelMetaConfig.class);
        var config = SchedulingTestConfig.newConfig();
        config.setScheduler(org.flexlb.config.SchedulerConfig.direct());
        when(configService.loadBalanceConfig()).thenReturn(config);
        var runtime = SchedulerTestSupport.runtime(SchedulerTestSupport.create(configService,
                mock(org.flexlb.service.monitor.BatchSchedulerReporter.class),
                mock(org.flexlb.service.monitor.RequestSchedulerReporter.class),
                mock(org.flexlb.service.RecentCacheKeyTraceReporter.class)));
        requests = org.mockito.Mockito.mock(DirectRequestScheduler.class, org.mockito.Mockito.withSettings()
                .useConstructor(mock(DefaultRouter.class), runtime, config)
                .defaultAnswer(invocation -> {
                    if (invocation.getMethod().getDeclaringClass() == DirectRequestScheduler.class) { return invocation.callRealMethod(); }
                    if (invocation.getMethod().getName().equals("tryAcquireSubmissionPermit")) { return true; }
                    if (invocation.getMethod().getName().equals("expirationTimer")) { return mock(ExpirationTimer.class); }
                    return org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
                }));
        when(requests.claimAdmissionHandle(anyLong(), any())).thenReturn(mock(AdmissionHandle.class));
        when(requests.register(any(), org.mockito.ArgumentMatchers.any())).thenAnswer(call -> {
            var context = call.getArgument(0, BalanceContext.class);
            var future = new CompletableFuture<Response>();
            context.setFuture(future);
            context.attachScheduler(requests);
            return future;
        });
        when(requests.commitRoute(any(), any())).thenAnswer(call -> RequestProtocolTestSupport.publish(call.getArgument(1)) ? PlacementResult.Status.SUCCESS : PlacementResult.Status.BLOCKED);
        when(requests.claimDelivery(any(), eq(DeliveryClaimKind.ROUTE_DECISION), eq(0L), any())).thenAnswer(call -> {
            call.getArgument(3, PrefillAdmissionResources.Member.class).transferToEndpoint(call.getArgument(0));
            DeliveryClaim claim = mock(DeliveryClaim.class);
            RequestRoute item = call.getArgument(0);
            lastDelivery = claim;
            doAnswer(inv -> {
                item.future().complete(item.routeResponse());
                return null;
            }).when(requests).publishRoute(eq(claim), any(), anyLong());
            return claim;
        });
        when(requests.publishDecisionResponseAsync(anyLong(), any(), any())).thenAnswer(call -> call.getArgument(1, CompletableFuture.class).complete(call.getArgument(2)));
    }

    @Test
    void invalidRequestFailsBeforePolicyOrEndpointSelection() {
        when(modelMeta.requiredRoles()).thenReturn(List.of(RoleType.PREFILL));
        DefaultRouter router = router();
        org.mockito.Mockito.clearInvocations(configService);

        PlacementResult<ProvisionalRoute, PlacementKey> result = router.select(new BalanceContext(SchedulingTestConfig.newConfig()), null);

        assertEquals(PlacementResult.Status.REJECTED, result.status());
        assertEquals(StrategyErrorType.INVALID_REQUEST.getErrorCode(),
                result.failure().getCode());
        verify(configService, never()).loadBalanceConfig();
        verifyNoInteractions(prefillSelector, decodeSelector, vitSelector);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true"})
    void directRouteCommitsRolesAndReleasesGenerationPins(boolean pinCloseFails, boolean materializationFails) {
        when(modelMeta.requiredRoles()).thenReturn(
                List.of(RoleType.PREFILL, RoleType.DECODE));
        DefaultRouter router = router();
        BalanceContext context = directContext(7L);
        SelectionFixture prefill = selection(RoleType.PREFILL, 7L, "p", 8001, "g1");
        SelectionFixture decode = selection(RoleType.DECODE, 7L, "d", 8002, "g1");
        PrefillState.RouteReservation registration = mock(PrefillState.RouteReservation.class);
        DecodeEndpoint.ReservationHandle reservation = new DecodeEndpoint.ReservationHandle(1L, 7L, 2L);
        when(prefillSelector.select(context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(prefill.selection));
        when(decodeSelector.select(RequestRequirements.capture(context), "g1"))
                .thenReturn(PlacementResult.success(decode.selection));
        var capture = stubPrefillCommit((PrefillEndpoint) prefill.endpoint);
        when(((PrefillEndpoint) prefill.endpoint).reserveUnqueuedRoute(eq(prefill.pin), any(RequestRoute.class), eq(1L)))
                .thenReturn(new PrefillState.ReservationResult<>(PrefillState.CapacityStatus.ACQUIRED, registration));
        when(((DecodeEndpoint) decode.endpoint).reserve(eq(decode.pin), eq(7L), eq(32L), eq(48L), eq(50), isNull()))
                .thenReturn(reservation);
        var permit = stubDecodePermit((DecodeEndpoint) decode.endpoint, reservation);
        if (pinCloseFails) {
            doThrow(new IllegalStateException("pin close failed after direct handoff"))
                    .when(prefill.pin).close();
        }

        if (materializationFails) {
            when(capture.materialize()).thenThrow(new IllegalStateException("snapshot materialization failed"));
        }
        Response response = scheduler(router, context).submit(context).join();
        if (materializationFails) {
            assertEquals(StrategyErrorType.DISPATCH_FAILED.getErrorCode(), response.getCode());
            verify(permit, never()).dispatch();
            verify(permit, org.mockito.Mockito.times(2)).release();
            verify(registration).close();
            verify(prefill.pin).close();
            verify(decode.pin).close();
            verify(requests, never()).publishRoute(any(), any(), anyLong());
            return;
        }

        assertTrue(response.isSuccess());
        assertEquals(List.of(prefill.status, decode.status),
                response.getServerStatus());
        verify(registration).close();
        verify(permit).dispatch();
        verify((DecodeEndpoint) decode.endpoint, never())
                .release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
        verify(prefill.pin).close();
        verify(decode.pin).close();
        var order = org.mockito.Mockito.inOrder(decode.pin, prefill.pin, requests);
        order.verify(decode.pin).close();
        order.verify(prefill.pin).close();
        order.verify(requests).publishRoute(any(), any(), anyLong());
    }

    @Test
    void directRouteRollsBackDecodeWhenPrefillAdmissionIsFull() {
        when(modelMeta.requiredRoles()).thenReturn(List.of(RoleType.PREFILL, RoleType.DECODE));
        DefaultRouter router = router();
        BalanceContext context = directContext(8L);
        SelectionFixture prefill = selection(RoleType.PREFILL, 8L, "p", 8001, "g1");
        SelectionFixture decode = selection(RoleType.DECODE, 8L, "d", 8002, "g1");
        DecodeEndpoint.ReservationHandle reservation = new DecodeEndpoint.ReservationHandle(1L, 8L, 2L);
        when(prefillSelector.select(context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(prefill.selection));
        when(decodeSelector.select(RequestRequirements.capture(context), "g1"))
                .thenReturn(PlacementResult.success(decode.selection));
        stubPrefillCommit((PrefillEndpoint) prefill.endpoint);
        when(((DecodeEndpoint) decode.endpoint).reserve(eq(decode.pin), eq(8L), eq(32L), eq(48L), eq(50), isNull()))
                .thenReturn(reservation);
        stubDecodePermit((DecodeEndpoint) decode.endpoint, reservation);
        when(((PrefillEndpoint) prefill.endpoint).reserveUnqueuedRoute(eq(prefill.pin), any(RequestRoute.class), eq(1L)))
                .thenReturn(new PrefillState.ReservationResult<>(PrefillState.CapacityStatus.CAPACITY_FULL, null));

        assertEquals(StrategyErrorType.RESOURCE_EXHAUSTED.getErrorCode(),
                scheduler(router, context).submit(context).join().getCode());
        verify(prefillSelector).select(context, RoleType.PREFILL, null);
        verify((DecodeEndpoint) decode.endpoint).release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
        verify(prefill.pin).close();
        verify(decode.pin).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void directAdmissionPassesZeroAndUnknownTimeToLifecycle(boolean unknown) {
        when(modelMeta.requiredRoles()).thenReturn(List.of(RoleType.PREFILL));
        BalanceContext context = directContext(81L);
        SelectionFixture selected = selection(RoleType.PREFILL, 81L, "p", 8001, "g1");
        when(selected.selection.prefillWorkMs()).thenReturn(0L);
        when(prefillSelector.select(context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(selected.selection));
        PrefillEndpoint endpoint = (PrefillEndpoint) selected.endpoint;
        long committedAtMs = System.currentTimeMillis();
        stubRouteCommit(endpoint, new org.flexlb.balance.projection.WorkSnapshot(
                committedAtMs, List.of(), List.of(), unknown ? 1L : 0L));
        var registration = mock(PrefillState.RouteReservation.class);
        when(endpoint.reserveUnqueuedRoute(eq(selected.pin), any(), eq(0L)))
                .thenReturn(new PrefillState.ReservationResult<>(PrefillState.CapacityStatus.ACQUIRED, registration));

        assertTrue(scheduler(router(), context).submit(context).join().isSuccess());

        var precedingWork = org.mockito.ArgumentCaptor.forClass(org.flexlb.balance.projection.WorkSnapshot.class);
        verify(requests).publishRoute(eq(lastDelivery), precedingWork.capture(), eq(0L));
        assertEquals(unknown ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(0L),
                precedingWork.getValue().totalRemainingWorkMs());
        verify(registration).close();
    }

    @Test
    void directRequestRemainsInCanonicalLifecycleAfterResponseAndReconcilesEarlyDecodeEvidence() throws Exception {
        when(modelMeta.requiredRoles()).thenReturn(List.of(RoleType.PREFILL, RoleType.DECODE));
        BalanceContext context = directContext(9L);
        when(configService.loadBalanceConfig()).thenReturn(context.getConfig());
        requests = org.flexlb.balance.scheduler.SchedulerTestSupport.create(configService, mock(org.flexlb.service.monitor.BatchSchedulerReporter.class), mock(org.flexlb.service.monitor.RequestSchedulerReporter.class),
                mock(RecentCacheKeyTraceReporter.class));
        try {
            SelectionFixture prefill = selection(RoleType.PREFILL, 9L, "p", 8001, "g1");
            SelectionFixture decode = selection(RoleType.DECODE, 9L, "d", 8002, "g1");
            var reservation = new DecodeEndpoint.ReservationHandle(1L, 9L, 2L);
            when(prefillSelector.select(context, RoleType.PREFILL, null)).thenReturn(PlacementResult.success(prefill.selection));
            when(decodeSelector.select(RequestRequirements.capture(context), "g1")).thenReturn(PlacementResult.success(decode.selection));
            stubPrefillCommit((PrefillEndpoint) prefill.endpoint);
            when(((PrefillEndpoint) prefill.endpoint).reserveUnqueuedRoute(eq(prefill.pin), any(RequestRoute.class), eq(1L))).thenReturn(new PrefillState.ReservationResult<>(PrefillState.CapacityStatus.ACQUIRED, mock(PrefillState.RouteReservation.class)));
            when(((DecodeEndpoint) decode.endpoint).reserve(eq(decode.pin), eq(9L), eq(32L), eq(48L), eq(50), isNull())).thenReturn(reservation);
            stubDecodePermit((DecodeEndpoint) decode.endpoint, reservation);
            when(((DecodeEndpoint) decode.endpoint).isAcceptedByEngine(reservation)).thenReturn(true);
            assertTrue(scheduler(router(), context).submit(context).get(2L, TimeUnit.SECONDS).isSuccess());
            assertTrue(context.getFuture().get(2L, TimeUnit.SECONDS).isSuccess());
            BalanceContext slot = requests.requestSlot(9L);
            synchronized (slot) {
                assertTrue(slot.decodeAccepted());
                assertTrue(slot.isLiveGeneration());
                assertEquals(RequestState.Phase.ACKNOWLEDGED, slot.snapshot().state());
            }
            RequestProtocolTestSupport.expireInactiveRequest(requests, slot, System.currentTimeMillis() + context.getConfig().getRequestLifecycle().getRequest().getTimeoutMs());
            assertEquals(RequestState.Phase.TIMED_OUT, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(requests).getRequestState(9L, 0L).state());
            assertEquals(0, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(requests).liveRequestCount());
            verify((DecodeEndpoint) decode.endpoint).release(reservation, DecodeEndpoint.ReleaseReason.EXPIRED);
            verify((PrefillEndpoint) prefill.endpoint).releaseCommittedItem(any(RequestRoute.class));
        } finally {
            RequestProtocolTestSupport.closeAdmissionAndAwaitMutations(requests);
            requests.closeOutstandingAndTerminalize();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(requests).timer().close();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(requests).closeRequestExecutors();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void directSelectionExceptionPublishesFailureAndClosesLifecycle(boolean fatal) throws Exception {
        when(modelMeta.requiredRoles()).thenReturn(List.of(RoleType.PREFILL));
        BalanceContext context = directContext(10L);
        when(configService.loadBalanceConfig()).thenReturn(context.getConfig());
        requests = org.flexlb.balance.scheduler.SchedulerTestSupport.create(configService, mock(org.flexlb.service.monitor.BatchSchedulerReporter.class), mock(org.flexlb.service.monitor.RequestSchedulerReporter.class),
                mock(RecentCacheKeyTraceReporter.class));
        try {
            Throwable failure = fatal ? new AssertionError("selection failed") : new IllegalStateException("selection failed");
            when(prefillSelector.select(context, RoleType.PREFILL, null)).thenThrow(failure);
            if (fatal) {
                assertEquals(failure, assertThrows(AssertionError.class, () -> scheduler(router(), context).submit(context)));
            } else {
                var returned = scheduler(router(), context).submit(context);
                assertEquals(context.getFuture(), returned);
            }
            Response response = context.getFuture().get(2L, TimeUnit.SECONDS);
            assertEquals(8510, response.getCode());
            assertEquals("DISPATCH_FAILED", response.getErrorMessage());
            assertEquals(RequestState.Phase.FAILED, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(requests).getRequestState(10L, 0L).state());
            verifyNoInteractions(decodeSelector, vitSelector);
        } finally {
            RequestProtocolTestSupport.closeAdmissionAndAwaitMutations(requests);
            requests.closeOutstandingAndTerminalize();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(requests).timer().close();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(requests).closeRequestExecutors();
        }
    }

    @Test
    void frozenDemandUsesTheSamePromptLengthForPredictionAndReservation() {
        var config = SchedulingTestConfig.newConfig();
        var context = RequestProtocolTestSupport.context(config, 702L);
        var request = org.mockito.Mockito.spy(context.getRequest());
        request.setMaxNewTokens(16);
        when(request.getSeqLen()).thenReturn(32L, 4096L);
        context.setRequest(request);

        var frozen = RequestRequirements.capture(context);
        assertEquals(32L, frozen.seqLen());
        assertEquals(32L, frozen.hardKvTokens());
        assertEquals(48L, frozen.expectedKvTokens());
    }

    @Test
    void capturedRequestCapacityRetainsItsObservedLimit() {
        var config = SchedulingTestConfig.newConfig();
        var availability = config.getRouter().getRoles().getDecode().getAvailability();
        availability.setMaxEngineRequests(7L);
        var context = RequestProtocolTestSupport.context(config, 703L);
        var frozen = RequestRequirements.capture(context);
        availability.setMaxEngineRequests(null);
        assertEquals(7L, frozen.capacity().maxEngineRequests());
    }

    @ParameterizedTest
    @CsvSource({"32,16,32,48", "500,10000,500,10500",
            "9223372036854775806,100,9223372036854775806,9223372036854775807",
            "-1,100,0,100", "500,-1,500,500"})
    void queuedRouteRetainsSelectedDemandLimitsAndCostFormulaAcrossLaterContextChanges(
            long prompt, int output, long hardKv, long expectedKv) {
        var config = SchedulingTestConfig.newConfig();
        var limits = config.getRouter().getRoles().getDecode().getAvailability();
        limits.setMaxEngineRequests(1L);
        limits.setMaxKvUsagePercent(90L);
        var estimator = config.getRouter().getRoles().getDecode().getCostEstimator();
        estimator.setExpression("2 * running_size / max_running_size + 3 * kvcache_used_ratio");
        var context = RequestProtocolTestSupport.context(config, 701L);
        context.getRequest().setSeqLen(prompt);
        context.getRequest().setMaxNewTokens(output);
        context.setSchedulingMetadata(SchedulingMetadata.explicit(73, Long.MAX_VALUE));
        var frozen = SchedulingTestConfig.freezeInputs(context).getRequirements();
        var prefill = selection(RoleType.PREFILL, 701L, "10.0.0.1", 8080, "g1");
        var selectedDecode = selection(RoleType.DECODE, 701L, "10.0.0.2", 8080, "g1");
        var decode = (DecodeEndpoint) selectedDecode.endpoint();
        var reservation = new DecodeEndpoint.ReservationHandle(1L, 701L, 1L);
        when(decode.reserve(any(), anyLong(), anyLong(), anyLong(), anyInt(), eq(frozen.capacity())))
                .thenReturn(reservation);
        when(decode.acquireDispatchPermit(reservation, frozen.capacity())).thenReturn(
                new DecodeEndpoint.EngineDispatchPermitAcquisition(
                        DecodeEndpoint.EngineDispatchPermitAcquireStatus.CAPACITY_FULL, null));

        // Changes after selection must not change this request's publication mode, demand or cost.
        SchedulingTestConfig.allowVictim(config, VictimStage.DECODE_RESERVED);
        limits.setMaxEngineRequests(99L);
        limits.setMaxKvUsagePercent(1L);
        estimator.setExpression("7 * running_size / max_running_size + 11 * kvcache_used_ratio");
        assertNotSame(estimator.compiledFormula(), frozen.costFormula());
        assertEquals("2 * running_size / max_running_size + 3 * kvcache_used_ratio",
                frozen.costFormula().expression());
        context.getRequest().setSeqLen(4_096L);
        context.getRequest().setMaxNewTokens(1_024);
        context.setSchedulingMetadata(SchedulingMetadata.explicit(4, Long.MAX_VALUE));

        try (var route = ProvisionalRoute.prepare(context,
                List.of(prefill.selection(), selectedDecode.selection()), new Response())) {
            assertTrue(route.reserveDecode(context.getRequirements()));
            var item = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), route.response(),
                    ServerStatus.copyOf(route.prefillStatus()), ServerStatus.copyOf(route.decodeStatus()),
                    route.prefillEndpoint(), route.decodeEndpoint(), route.decodeReservation(),
                    System.currentTimeMillis());
            assertSame(frozen, context.getRequirements());
            assertSame(frozen, item.requirements());
            assertSame(frozen.capacity(), context.getRequirements().capacity());
            assertSame(frozen.capacity(), item.requirements().capacity());
            assertSame(frozen.costFormula(), context.getRequirements().costFormula());
            assertSame(frozen.costFormula(), item.requirements().costFormula());
            assertSame(reservation, item.decodeReservation());
            assertSame(decode, item.decodeEp());
            assertEquals(hardKv, item.seqLen());
            assertEquals(DecodeMode.WAIT_AT_PLACEMENT, frozen.mode());

            var delivery = PrefillAdmissionResources.prepareMember(item);
            assertFalse(delivery.accepted());
            assertTrue(delivery.boundary().unavailable());
            assertFalse(delivery.boundary().availability().isAvailable());
            verify(decode).shouldRetryDispatch(701L, frozen.capacity());
            verify(decode).acquireDispatchPermit(reservation, frozen.capacity());
            verify(decode).reserve(any(), eq(701L), eq(hardKv), eq(expectedKv), eq(73), eq(frozen.capacity()));
            verify(decode, never()).reserve(any(), anyLong(), anyLong(), anyLong(), anyInt(), isNull());
        }
        verify(decode).release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedDecodeAdoptionReleasesOnceAndPreservesOriginalFailure(boolean adoptionThrows) {
        var context = context(702L);
        var prefill = selection(RoleType.PREFILL, 702L, "p", 8001, "g1");
        var selectedDecode = selection(RoleType.DECODE, 702L, "d", 8002, "g1");
        var decode = (DecodeEndpoint) selectedDecode.endpoint();
        var reservation = new DecodeEndpoint.ReservationHandle(1L, 702L, 1L);
        var adoptionFailure = new IllegalStateException("adoption failed");
        var cleanupFailure = new IllegalStateException("cleanup failed");
        if (adoptionThrows) {
            when(decode.markQueued(selectedDecode.pin(), reservation)).thenThrow(adoptionFailure);
        }
        doThrow(cleanupFailure).when(decode)
                .release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);

        try (var route = ProvisionalRoute.prepare(context,
                List.of(prefill.selection(), selectedDecode.selection()), new Response())) {
            var actual = assertThrows(IllegalStateException.class,
                    () -> route.adoptDecodeReservation(decode, reservation));
            assertSame(adoptionThrows ? adoptionFailure : cleanupFailure, actual);
            assertEquals(adoptionThrows ? List.of(cleanupFailure) : List.of(),
                    List.of(actual.getSuppressed()));
        }
        verify(decode).release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
        verify(prefill.pin()).close();
        verify(selectedDecode.pin()).close();
    }

    @Test
    void committedQueueOwnsCapacityEvenWhenSelectionPinCleanupFails() {
        var context = context(703L);
        var prefill = selection(RoleType.PREFILL, 703L, "p", 8001, "g1");
        var decode = selection(RoleType.DECODE, 703L, "d", 8002, "g1");
        var endpoint = (DecodeEndpoint) decode.endpoint();
        var reservation = new DecodeEndpoint.ReservationHandle(1L, 703L, 1L);
        var cleanupFailure = new IllegalStateException("pin cleanup failed after publication");
        var route = ProvisionalRoute.prepare(context,
                List.of(prefill.selection(), decode.selection()), new Response());
        when(endpoint.markQueued(decode.pin(), reservation)).thenReturn(true);
        assertTrue(route.adoptDecodeReservation(endpoint, reservation));
        var item = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), route.response(),
                route.prefillStatus(), route.decodeStatus(), route.prefillEndpoint(), endpoint,
                reservation, System.currentTimeMillis());
        when(route.prefillEndpoint().offerPinned(prefill.pin(), item, org.flexlb.balance.scheduler.QueueExecutionSettings.capture(item.ctx().getConfig()))).thenReturn(true);
        doThrow(cleanupFailure).when(prefill.pin()).close();

        var publication = route.new QueuePublication(item,
                QueueExecutionSettings.capture(item.ctx().getConfig()));
        publication.publish();
        assertTrue(publication.published());
        verify(prefill.pin(), never()).close();
        verify(decode.pin(), never()).close();
        assertSame(cleanupFailure, assertThrows(IllegalStateException.class, route::close));
        route.close();
        verify(prefill.pin()).close();
        verify(decode.pin()).close();
        verify(endpoint, never()).release(reservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
    }

    private static PrefillState.WorkCapture stubPrefillCommit(PrefillEndpoint prefill) {
        return stubRouteCommit(prefill, new org.flexlb.balance.projection.WorkSnapshot(System.currentTimeMillis(), List.of(), List.of(), 0L));
    }

    private static PrefillState.WorkCapture stubRouteCommit(PrefillEndpoint endpoint,
            org.flexlb.balance.projection.WorkSnapshot precedingWork) {
        var commit = mock(PrefillEndpoint.RouteCommitAdmission.class);
        var handoff = mock(PrefillState.CommittedHandoff.class);
        var capture = mock(PrefillState.WorkCapture.class);
        when(capture.materialize()).thenReturn(precedingWork);
        when(handoff.precedingWork()).thenReturn(capture);
        when(endpoint.tryBeginRouteCommitAdmission()).thenReturn(commit);
        when(commit.commit(any(), any())).thenReturn(handoff);
        return capture;
    }

    private static DecodeEndpoint.EngineDispatchPermit stubDecodePermit(
            DecodeEndpoint endpoint, DecodeEndpoint.ReservationHandle reservation) {
        var permit = mock(DecodeEndpoint.EngineDispatchPermit.class);
        when(endpoint.acquireDispatchPermit(eq(reservation), any()))
                .thenReturn(new DecodeEndpoint.EngineDispatchPermitAcquisition(
                        DecodeEndpoint.EngineDispatchPermitAcquireStatus.ACQUIRED, permit));
        when(permit.dispatch())
                .thenReturn(DecodeEndpoint.EngineDispatchPermitTransferStatus.TRANSFERRED);
        return permit;
    }

    @ParameterizedTest
    @EnumSource(value = RoleType.class, names = {
            "PREFILL", "DECODE", "PDFUSION", "VIT"})
    void missingRequiredRoleReturnsItsExactWaitDomain(RoleType role) {
        when(modelMeta.requiredRoles()).thenReturn(List.of(role));
        DefaultRouter router = router();
        BalanceContext context = context(11L);
        stubQueueSelection(
                context, role, null, PlacementResult.blocked(role));

        PlacementResult<ProvisionalRoute, PlacementKey> blocked = router.select(freezeInputs(context), router.resolvePolicyGroup(context));
        assertEquals(PlacementResult.Status.BLOCKED, blocked.status());

        assertEquals(new PlacementKey(role, null, null), blocked.blocker());
    }

    @Test
    void projectedDecodeBlockUsesDecodeWaitDomain() {
        when(modelMeta.requiredRoles()).thenReturn(List.of(RoleType.PREFILL));
        DefaultRouter router = router();
        BalanceContext context = context(111L);
        when(prefillSelector.select(
                context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.blocked(RoleType.DECODE));

        PlacementResult<ProvisionalRoute, PlacementKey> blocked = router.select(freezeInputs(context), router.resolvePolicyGroup(context));
        assertEquals(PlacementResult.Status.BLOCKED, blocked.status());

        assertEquals(new PlacementKey(RoleType.DECODE, null, null),
                blocked.blocker());
    }

    @Test
    void staticCapacityFailureIsTerminalAndReleasesEarlierSelections() {
        when(modelMeta.requiredRoles()).thenReturn(
                List.of(RoleType.PREFILL, RoleType.DECODE));
        DefaultRouter router = router();
        BalanceContext context = context(12L);
        SelectionFixture prefill = selection(
                RoleType.PREFILL, 12L, "p", 8001, "g1");
        when(prefillSelector.select(
                context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(prefill.selection));
        when(decodeSelector.select(
                RequestRequirements.capture(context), "g1"))
                .thenReturn(PlacementResult.rejected(
                        Response.error(StrategyErrorType.RESOURCE_EXHAUSTED)));

        PlacementResult<ProvisionalRoute, PlacementKey> rejected = router.select(freezeInputs(context), router.resolvePolicyGroup(context));
        assertEquals(PlacementResult.Status.REJECTED, rejected.status());

        assertEquals(StrategyErrorType.RESOURCE_EXHAUSTED.getErrorCode(),
                rejected.failure().getCode());
        verify(prefill.selection).close();
    }

    @Test
    void selectionFailureClosesAllPinsInReverseOrderAndKeepsPrimaryCause() {
        when(modelMeta.requiredRoles()).thenReturn(List.of(RoleType.PREFILL, RoleType.DECODE, RoleType.VIT));
        BalanceContext context = context(120L);
        var prefill = selection(RoleType.PREFILL, 120L, "p", 8001, "g1");
        var decode = selection(RoleType.DECODE, 120L, "d", 8002, "g1");
        when(prefillSelector.select(context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(prefill.selection));
        when(decodeSelector.select(RequestRequirements.capture(context), "g1"))
                .thenReturn(PlacementResult.success(decode.selection));
        var primary = new IllegalStateException("selection failed");
        var decodeClose = new IllegalStateException("Decode pin close failed");
        var prefillClose = new IllegalStateException("Prefill pin close failed");
        when(vitSelector.select(context, RoleType.VIT, "g1")).thenThrow(primary);
        doThrow(decodeClose).when(decode.selection).close();
        doThrow(prefillClose).when(prefill.selection).close();

        assertSame(primary, assertThrows(IllegalStateException.class, () -> router().select(freezeInputs(context), null)));

        var order = org.mockito.Mockito.inOrder(decode.selection, prefill.selection);
        order.verify(decode.selection).close();
        order.verify(prefill.selection).close();
        assertEquals(List.of(decodeClose), List.of(primary.getSuppressed()));
        assertEquals(List.of(prefillClose), List.of(decodeClose.getSuppressed()));
    }

    @Test
    void queueRouteTransfersTheExactPrefillPinIntoAdmissionOwnership() {
        when(modelMeta.requiredRoles()).thenReturn(List.of(RoleType.PREFILL));
        DefaultRouter router = router();
        BalanceContext context = context(21L);
        SelectionFixture prefill = selection(
                RoleType.PREFILL, context.getRequestId(), "p", 8001, "g1");
        when(prefillSelector.select(
                context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(prefill.selection));

        PlacementResult<ProvisionalRoute, PlacementKey> admitted = router.select(freezeInputs(context), router.resolvePolicyGroup(context));
        assertEquals(PlacementResult.Status.SUCCESS, admitted.status());

        assertTrue(admitted.value().response().isSuccess());
        assertEquals(List.of(prefill.status),
                admitted.value().response().getServerStatus());
        verify(prefill.selection).transferToRoute();
        verify(prefill.pin, never()).close();

        admitted.value().close();
        verify(prefill.pin).close();
    }

    @Test
    void firstSelectedGroupChainsToLaterRolesWhenPolicyDidNotForceOne() {
        when(modelMeta.requiredRoles())
                .thenReturn(List.of(RoleType.PREFILL, RoleType.VIT));
        DefaultRouter router = router();
        BalanceContext context = context(31L);
        SelectionFixture prefill = selection(
                RoleType.PREFILL, 31L, "p", 8001, "selected-group");
        SelectionFixture vit = selection(
                RoleType.VIT, 31L, "v", 8002, "selected-group");
        when(prefillSelector.select(
                context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(prefill.selection));
        when(vitSelector.select(context, RoleType.VIT, "selected-group"))
                .thenReturn(vit.selection);

        PlacementResult<ProvisionalRoute, PlacementKey> admitted = router.select(freezeInputs(context), router.resolvePolicyGroup(context));
        assertEquals(PlacementResult.Status.SUCCESS, admitted.status());
        admitted.value().close();

        verify(prefillSelector).select(
                context, RoleType.PREFILL, null);
        verify(vitSelector).select(
                context, RoleType.VIT, "selected-group");
        verify(vit.pin).close();
        verify(prefill.pin).close();
    }

    @Test
    void policyGroupRemainsAuthoritativeAcrossEveryRole() {
        when(modelMeta.requiredRoles())
                .thenReturn(List.of(RoleType.PREFILL, RoleType.VIT));
        DefaultRouter router = router();
        FlexlbConfig config = SchedulingTestConfig.batchConfig();
        SchedulingTestConfig.usePriorityQueue(config);
        TrafficPolicyConfig groupSelector = new TrafficPolicyConfig();
        var target = new TrafficPolicyConfig.Target();
        target.setGroup("forced");
        groupSelector.setDefaultTargets(List.of(target));
        config.getRouter().setGroupSelector(groupSelector);
        BalanceContext context = context(41L, config);
        SelectionFixture prefill = selection(
                RoleType.PREFILL, 41L, "p", 8001, "other");
        SelectionFixture vit = selection(
                RoleType.VIT, 41L, "v", 8002, "other");
        when(prefillSelector.select(
                context, RoleType.PREFILL, "forced"))
                .thenReturn(PlacementResult.success(prefill.selection));
        when(vitSelector.select(context, RoleType.VIT, "forced"))
                .thenReturn(vit.selection);

        PlacementResult<ProvisionalRoute, PlacementKey> admitted = router.select(freezeInputs(context), router.resolvePolicyGroup(context));
        assertEquals(PlacementResult.Status.SUCCESS, admitted.status());
        admitted.value().close();

        verify(prefillSelector).select(
                context, RoleType.PREFILL, "forced");
        verify(vitSelector).select(context, RoleType.VIT, "forced");
    }

    @Test
    void laterSelectionFailureClosesEveryEarlierExactPinOwner() {
        when(modelMeta.requiredRoles())
                .thenReturn(List.of(RoleType.PREFILL, RoleType.VIT));
        DefaultRouter router = router();
        BalanceContext context = context(51L);
        SelectionFixture prefill = selection(
                RoleType.PREFILL, 51L, "p", 8001, "g1");
        when(prefillSelector.select(
                context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(prefill.selection));
        when(vitSelector.select(context, RoleType.VIT, "g1"))
                .thenReturn(null);

        PlacementResult<ProvisionalRoute, PlacementKey> blocked = router.select(freezeInputs(context), router.resolvePolicyGroup(context));
        assertEquals(PlacementResult.Status.BLOCKED, blocked.status());

        assertEquals(new PlacementKey(RoleType.VIT, "g1", null),
                blocked.blocker());
        verify(prefill.selection).close();
    }

    @Test
    void decodeMissAfterPrefillSuccessReleasesThePrefillPinOwner() {
        when(modelMeta.requiredRoles()).thenReturn(
                List.of(RoleType.PREFILL, RoleType.DECODE));
        DefaultRouter router = router();
        BalanceContext context = context(52L);
        SelectionFixture prefill = selection(
                RoleType.PREFILL, 52L, "p", 8001, "g1");
        when(prefillSelector.select(
                context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(prefill.selection));
        when(decodeSelector.select(
                RequestRequirements.capture(context), "g1"))
                .thenReturn(PlacementResult.blocked(RoleType.DECODE));

        PlacementResult<ProvisionalRoute, PlacementKey> blocked = router.select(freezeInputs(context), router.resolvePolicyGroup(context));
        assertEquals(PlacementResult.Status.BLOCKED, blocked.status());

        assertEquals(new PlacementKey(RoleType.DECODE, "g1", null),
                blocked.blocker());
        verify(prefill.selection).close();
        verify(prefill.selection, never()).transferToRoute();
    }

    @Test
    void mismatchedSelectedRequestFailsClosedAndReleasesPins() {
        when(modelMeta.requiredRoles()).thenReturn(List.of(RoleType.PREFILL));
        DefaultRouter router = router();
        BalanceContext context = context(61L);
        SelectionFixture foreign = selection(
                RoleType.PREFILL, 999L, "p", 8001, "g1");
        when(prefillSelector.select(
                context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(foreign.selection));

        assertThrows(IllegalStateException.class,
                () -> router.select(freezeInputs(context), router.resolvePolicyGroup(context)));

        verify(foreign.selection).close();
        verify(foreign.selection, never()).transferToRoute();
    }

    @Test
    void requiredTopologyIsSnapshottedAtConstruction() {
        List<RoleType> mutable = new ArrayList<>();
        mutable.add(RoleType.PREFILL);
        when(modelMeta.requiredRoles()).thenReturn(mutable);
        DefaultRouter router = router();
        mutable.clear();
        BalanceContext context = context(71L);
        SelectionFixture prefill = selection(
                RoleType.PREFILL, 71L, "p", 8001, "g1");
        when(prefillSelector.select(
                context, RoleType.PREFILL, null))
                .thenReturn(PlacementResult.success(prefill.selection));

        PlacementResult<ProvisionalRoute, PlacementKey> admitted = router.select(freezeInputs(context), router.resolvePolicyGroup(context));
        assertEquals(PlacementResult.Status.SUCCESS, admitted.status());
        admitted.value().close();

        verify(prefillSelector).select(
                context, RoleType.PREFILL, null);
    }

    private RequestScheduler scheduler(DefaultRouter router, BalanceContext context) {
        SchedulingTestConfig.freezeInputs(context);
        when(configService.loadBalanceConfig()).thenReturn(context.getConfig());
        return org.flexlb.balance.scheduler.SchedulerTestSupport.configure(requests, configService.loadBalanceConfig(), router, mock(org.flexlb.service.monitor.BatchSchedulerReporter.class), mock(org.flexlb.balance.eviction.EvictionManager.class), new PlacementAvailability());
    }

    private DefaultRouter router() {
        return new DefaultRouter(
                prefillSelector,
                decodeSelector,
                vitSelector,
                modelMeta);
    }

    private void stubQueueSelection(
            BalanceContext context,
            RoleType role,
            String group,
            PlacementResult<SelectedRole, RoleType> result) {
        switch (role) {
            case PREFILL, PDFUSION -> when(prefillSelector.select(
                    context, role, group)).thenReturn(result);
            case DECODE -> when(decodeSelector.select(
                    RequestRequirements.capture(context), group)).thenReturn(result);
            case VIT -> when(vitSelector.select(context, role, group))
                    .thenReturn(result.value());
            case FRONTEND -> throw new IllegalArgumentException();
        }
    }

    @ParameterizedTest
    @EnumSource(value = RoleType.class, names = {"PREFILL", "PDFUSION"})
    void reportedBlockerResolvesExactRoleAndCapturedCapacityVersion(RoleType prefillRole) {
        var context = context(990L);
        var prefill = selection(prefillRole, 990L, "shared", 8080, "g1");
        var decode = selection(RoleType.DECODE, 990L, "shared", 8080, "g1");
        when(prefill.endpoint().ipPort()).thenReturn("shared:8080");
        when(decode.endpoint().ipPort()).thenReturn("shared:8080");
        when(prefill.selection().placementVersion()).thenReturn(11L);
        when(decode.selection().placementVersion()).thenReturn(23L);
        var prefillEndpoint = (PrefillEndpoint) prefill.endpoint();
        var decodeEndpoint = (DecodeEndpoint) decode.endpoint();
        when(prefillEndpoint.placementVersion()).thenReturn(11L);
        when(decodeEndpoint.placementVersion()).thenReturn(23L);
        try (var route = ProvisionalRoute.prepare(context,
                List.of(prefill.selection(), decode.selection()), new Response())) {
            var prefillKey = route.prefillPlacementKey();
            var decodeKey = route.decodePlacementKey();
            assertSame(prefillEndpoint, route.blockedEndpointIfCurrent(prefillKey));
            assertSame(decodeEndpoint, route.blockedEndpointIfCurrent(decodeKey));
            // Changing one role does not invalidate the other role at the same address.
            when(decodeEndpoint.placementVersion()).thenReturn(24L);
            org.junit.jupiter.api.Assertions.assertNull(route.blockedEndpointIfCurrent(decodeKey));
            assertSame(prefillEndpoint, route.blockedEndpointIfCurrent(prefillKey));
            when(prefillEndpoint.placementVersion()).thenReturn(12L);
            org.junit.jupiter.api.Assertions.assertNull(route.blockedEndpointIfCurrent(prefillKey));
            assertThrows(IllegalArgumentException.class, () -> route.blockedEndpointIfCurrent(
                    PlacementKey.exact(RoleType.DECODE, "g1", "other:8080")));
            assertThrows(IllegalArgumentException.class, () -> route.blockedEndpointIfCurrent(
                    PlacementKey.exact(RoleType.DECODE, "other-group", "shared:8080")));
        }
        verify(prefill.pin()).close();
        verify(decode.pin()).close();
    }

    private static BalanceContext directContext(long requestId) {
        FlexlbConfig config = SchedulingTestConfig.newConfig();
        config.setScheduler(org.flexlb.config.SchedulerConfig.direct());
        SchedulingTestConfig.useNonBatchDispatcher(config);
        return context(requestId, config);
    }

    private static BalanceContext context(long requestId) {
        FlexlbConfig config = SchedulingTestConfig.batchConfig();
        SchedulingTestConfig.usePriorityQueue(config);
        return context(requestId, config);
    }

    private static BalanceContext context(long requestId, FlexlbConfig config) {
        Request request = new Request();
        request.setRequestId(requestId);
        request.setSeqLen(32L);
        request.setMaxNewTokens(16);
        BalanceContext context = new BalanceContext(config);
        context.setRequest(request);
        context.setSchedulingMetadata(SchedulingMetadata.explicit(
                50, System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1)));
        return context;
    }

    private static SelectionFixture selection(
            RoleType role,
            long requestId,
            String ip,
            int httpPort,
            String group) {
        SelectedRole selection = mock(SelectedRole.class);
        WorkerEndpoint.GenerationPin pin = mock(WorkerEndpoint.GenerationPin.class);
        WorkerEndpoint endpoint = switch (role) {
            case PREFILL, PDFUSION -> mock(PrefillEndpoint.class);
            case DECODE -> mock(DecodeEndpoint.class);
            default -> mock(WorkerEndpoint.class);
        };
        ServerStatus status = new ServerStatus();
        status.setSuccess(true);
        status.setRole(role);
        status.setRequestId(requestId);
        status.setServerIp(ip);
        status.setHttpPort(httpPort);
        status.setGroup(group);
        when(selection.serverStatus()).thenReturn(status);
        when(selection.prefillWorkMs()).thenReturn(1L);
        when(selection.generationPin()).thenReturn(pin);
        when(selection.endpoint()).thenReturn(endpoint);
        var selectionOpen = new java.util.concurrent.atomic.AtomicBoolean(true);
        org.mockito.Mockito.doAnswer(call -> {
            if (selectionOpen.compareAndSet(true, false)) { pin.close(); }
            return null;
        }).when(selection).close();
        when(pin.endpoint()).thenReturn(endpoint);
        return new SelectionFixture(selection, pin, endpoint, status);
    }

    private record SelectionFixture(SelectedRole selection, WorkerEndpoint.GenerationPin pin, WorkerEndpoint endpoint, ServerStatus status) {
    }
}
