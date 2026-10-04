package org.flexlb.balance.scheduler;

import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryClaim;
import org.flexlb.config.ConfigService;
import org.flexlb.config.SchedulerConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.master.TaskInfo;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.master.WorkerStatusResponse;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.flexlb.balance.scheduler.SchedulingTestConfig.freezeInputs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/** Resource assertions use real endpoint ledgers, not invocations of mocked release methods. */
class RequestResourceAccountingTest {
    private static final long ID = 101L;
    private static final long HARD_KV = 160L;
    private static final long EXPECTED_KV = 320L;
    private static final long TOTAL_KV = 10_000L;
    private static final long TTL = TimeUnit.HOURS.toMillis(1);

    @Test
    void partialCleanupFailureStillAttemptsOtherResourcesAndPreservesDecodeSettlement() {
        var context = RequestProtocolTestSupport.context(SchedulingTestConfig.batchConfig(), ID);
        var prefill = mock(PrefillEndpoint.class);
        var decode = mock(DecodeEndpoint.class);
        var reservation = new DecodeEndpoint.ReservationHandle(1L, ID, 1L);
        var item = RequestRoute.create(freezeInputs(context), new Response(), null, null,
                prefill, decode, reservation, System.currentTimeMillis());
        var queuedFailure = new IllegalStateException("queue removal failed");
        var committedFailure = new IllegalStateException("committed release failed");
        when(prefill.removeQueued(item, "TERMINAL_RELEASE")).thenThrow(queuedFailure);
        when(prefill.releaseCommittedItem(item)).thenThrow(committedFailure);
        var plan = new PrefillAdmissionResources.ReleasePlan(
                PrefillAdmissionResources.PrefillRelease.QUEUED_AND_COMMITTED,
                DecodeEndpoint.ReleaseReason.NOT_SENT, null);
        var first = PrefillAdmissionResources.settle(item, plan, false, false);
        assertFalse(first.prefillSettled());
        assertTrue(first.decodeSettled());
        assertSame(queuedFailure, first.failure());
        assertSame(committedFailure, queuedFailure.getSuppressed()[0]);
        org.mockito.Mockito.verify(prefill).releaseCommittedItem(item);
        org.mockito.Mockito.verify(decode).release(reservation, DecodeEndpoint.ReleaseReason.NOT_SENT);

        org.mockito.Mockito.doReturn(true).when(prefill).removeQueued(item, "TERMINAL_RELEASE");
        org.mockito.Mockito.doReturn(true).when(prefill).releaseCommittedItem(item);
        var retry = PrefillAdmissionResources.settle(item, plan, first.prefillSettled(), first.decodeSettled());
        assertTrue(retry.prefillSettled());
        assertTrue(retry.decodeSettled());
        assertNull(retry.failure());
        org.mockito.Mockito.verify(decode, org.mockito.Mockito.times(1))
                .release(reservation, DecodeEndpoint.ReleaseReason.NOT_SENT);
    }

    @ParameterizedTest
    @EnumSource(LocalEnd.class)
    void localTerminationReclaimsReservationAndPrefillInflight(LocalEnd end) throws Exception {
        try (Fixture f = new Fixture()) {
            f.assertReserved();
            switch (end) {
                case CANCEL -> f.requests.cancelRequest(ID, 0, CancelReason.CLIENT_CANCELLED);
                case FUTURE_CANCEL -> assertTrue(f.item.future().cancel(false));
                case EXPIRE -> f.expire();
                case SHUTDOWN -> {
                    assertTrue(RequestProtocolTestSupport.closeAdmissionAndAwaitMutations(f.requests));
                    f.requests.closeOutstandingAndTerminalize();
                }
            }
            f.assertEmpty();
            f.assertCapacityReusable();
        }
    }

    @Test
    void failedMaterializationBeforeDeliveryReclaimsCommittedRouteAndPreparedPermit() throws Exception {
        try (Fixture f = new Fixture()) {
            f.assertReserved();
            var prepared = PrefillAdmissionResources.prepareMember(f.item);
            assertTrue(prepared.accepted());
            assertEquals(1, f.decode.resourceSnapshot().activeDispatchPermits());
            try (var member = prepared.value()) {
                var failure = new IllegalStateException("snapshot materialization failed");
                f.requests.failDeliveryPreparation(f.item, failure);
                f.requests.failDeliveryPreparation(f.item, failure);
                assertFalse(f.item.future().get(2, TimeUnit.SECONDS).isSuccess());
            }
            f.assertEmpty();
            f.assertCapacityReusable();
        }
    }

    @ParameterizedTest
    @EnumSource(CancelReason.class)
    void admissionRetainsResourcesUntilItsOwnerCloses(CancelReason reason) throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.assertReserved();
            if (reason == CancelReason.CLIENT_CANCELLED) {
                f.requests.cancelRequest(ID, 0, reason);
            } else {
                f.expire();
            }
            assertFalse(f.item.future().isDone());
            f.assertReserved();
            f.admission.finish();
            f.assertEmpty();
            f.assertCapacityReusable();
        }
    }

    @Test
    void cancellationDuringPreparationAlsoReclaimsTheAcquiredDispatchPermit() throws Exception {
        try (Fixture f = new Fixture()) {
            var member = PrefillAdmissionResources.prepareMember(f.item);
            assertTrue(member.accepted());
            assertEquals(1, f.decode.resourceSnapshot().activeDispatchPermits());
            assertEquals(1, f.decode.routingView().engineCapacityUsed());
            f.requests.cancelRequest(ID, 0, CancelReason.CLIENT_CANCELLED);
            // The transaction's eventual rollback must remain harmless after request cleanup.
            assertNull(PrefillAdmissionResources.rollback(member.value(), null));
            f.assertEmpty();
            f.assertCapacityReusable();
        }
    }

    @ParameterizedTest
    @EnumSource(value = DeliveryResult.Status.class, names = {"DELIVERED", "UNCERTAIN"})
    void responseOrAmbiguousTransportRetainsResourcesUntilRemoteCleanup(DeliveryResult.Status result) throws Exception {
        try (Fixture f = new Fixture()) {
            DeliveryClaim claim = f.handoff();
            claim.complete(new DeliveryResult(result, result == DeliveryResult.Status.DELIVERED ? null : new IllegalStateException("reply lost")));
            if (result == DeliveryResult.Status.DELIVERED) {
                assertTrue(f.item.future().get(2, TimeUnit.SECONDS).isSuccess());
            }
            f.assertHandedOff();
            f.requests.cancelRequest(ID, 0, CancelReason.CLIENT_CANCELLED);
            f.assertHandedOff();
            f.expire();
            f.assertHandedOff();
            f.proveCleanup();
            f.assertEmpty();
            f.assertCapacityReusable();
        }
    }

    @Test
    void localRollbackCannotReleaseAnEngineOwnerWhoseProjectionHasNotArrived() throws Exception {
        try (Fixture f = new Fixture()) {
            var acquisition = f.decode.acquireDispatchPermit(f.reservation, f.capacity);
            assertEquals(DecodeEndpoint.EngineDispatchPermitAcquireStatus.ACQUIRED, acquisition.status());
            assertEquals(DecodeEndpoint.EngineDispatchPermitTransferStatus.TRANSFERRED,
                    acquisition.permit().dispatch());
            // Endpoint ownership can advance before its notification reaches the slot.
            f.requests.cancelRequest(ID, 0, CancelReason.CLIENT_CANCELLED);
            assertEquals(0, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.requests).liveRequestCount());
            assertEquals(0, f.prefill.ownershipStats().locallyOwnedRequests());
            assertEquals(HARD_KV, f.decode.routingView().inflightHardKv());
            assertEquals(1, f.decode.routingView().engineCapacityUsed());
            f.decodeStatus(Map.of(), Map.of("101", task(ID)), TOTAL_KV);
            f.assertEmpty();
            f.assertCapacityReusable();
        }
    }

    @Test
    void definiteDispatchRejectionReclaimsTransferredCapacity() throws Exception {
        try (Fixture f = new Fixture()) {
            DeliveryClaim claim = f.handoff();
            f.assertHandedOff();
            claim.complete(DeliveryResult.notSent(new IllegalStateException("not delivered")));
            assertFalse(f.item.future().get(2, TimeUnit.SECONDS).isSuccess());
            f.assertEmpty();
            f.assertCapacityReusable();
        }
    }

    @Test
    void runningDecodeWinsAgainstUncertainTransportAndTerminalReclaimsBothEndpoints() throws Exception {
        try (Fixture f = new Fixture()) {
            DeliveryClaim claim = f.handoff();
            TaskInfo running = task(ID);
            f.decodeStatus(Map.of("101", running), Map.of(), TOTAL_KV - HARD_KV);
            assertEquals(1, f.decode.resourceSnapshot().runningCount());
            assertEquals(1, f.decode.routingView().engineCapacityUsed());
            assertEquals(0, f.decode.routingView().inflightHardKv(), "Engine status replaces the local KV prediction");
            claim.complete(DeliveryResult.uncertain(new IllegalStateException("late transport failure")));
            assertTrue(f.item.future().get(2, TimeUnit.SECONDS).isSuccess());
            assertEquals(1, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.requests).liveRequestCount());
            assertEquals(1, f.decode.resourceSnapshot().runningCount());
            assertEquals(1, f.prefill.ownershipStats().locallyOwnedRequests());
            applyStatus(f.prefill, status(RoleType.PREFILL, 2L, Map.of(), Map.of("101", running), TOTAL_KV));
            f.decodeStatus(Map.of(), Map.of("101", running), TOTAL_KV);
            f.assertEmpty();
            f.assertCapacityReusable();
        }
    }

    @ParameterizedTest
    @CsvSource({"RUNNING, RECEIVED", "RUNNING, PENDING", "KV_ALLOCATED, RECEIVED", "KV_ALLOCATED, PENDING"})
    void decodePhaseRegressionStillDeliversTerminalWithoutInactivity(TaskPhase confirmed, TaskPhase regressed)
            throws Exception {
        try (Fixture f = new Fixture()) {
            f.handoff().complete(DeliveryResult.delivered());
            assertTrue(f.item.future().get(2, TimeUnit.SECONDS).isSuccess());
            applyStatus(f.prefill, status(RoleType.PREFILL, 2L, Map.of(), Map.of("101", task(ID)), TOTAL_KV));
            TaskInfo active = task(ID);
            active.setPhase(confirmed);
            f.decodeStatus(Map.of("101", active), Map.of(), TOTAL_KV - HARD_KV);
            TaskInfo stillPresent = task(ID);
            stillPresent.setPhase(regressed);
            f.decodeStatus(Map.of("101", stillPresent), Map.of(), TOTAL_KV);
            assertEquals(RequestState.Phase.ACKNOWLEDGED, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(f.requests).getRequestState(ID, 0).state());
            // No inactivity expiry, sweep, sleep or shutdown is allowed to make this assertion pass.
            f.decodeStatus(Map.of(), Map.of("101", task(ID)), TOTAL_KV);
            f.assertEmpty();
            f.assertCapacityReusable();
        }
    }

    @Test
    void inactivityWaitsForRemoteCleanupWithoutInventingPhysicalKvRelease() throws Exception {
        try (Fixture f = new Fixture()) {
            f.handoff().complete(DeliveryResult.delivered());
            f.decodeStatus(Map.of("101", task(ID)), Map.of(), TOTAL_KV - HARD_KV);
            assertEquals(1, f.decode.resourceSnapshot().runningCount());
            f.expire();
            assertEquals(1, f.decode.resourceSnapshot().runningCount());
            f.proveCleanup();
            f.assertEmpty();
            assertEquals(TOTAL_KV - HARD_KV, f.decode.routingView().realKvAvailable(),
                    "local expiry must not rewrite the last physical Engine KV sample");
            f.assertCapacityReusable();
        }
    }

    @Test
    void lateCallbacksAndOldReservationCannotReleaseReplacementCapacity() throws Exception {
        try (Fixture f = new Fixture()) {
            DeliveryClaim oldClaim = f.handoff();
            var oldReservation = f.reservation;
            f.expire();
            f.assertHandedOff();
            oldClaim.complete(DeliveryResult.delivered());
            f.proveCleanup();
            f.assertEmpty();
            var replacementRef = new AtomicReference<DecodeEndpoint.ReservationHandle>();
            RequestProtocolTestSupport.awaitCondition(() -> {
                if (replacementRef.get() != null) { return true; }
                f.decode.evictExpiredRequests(0, ignored -> false);
                try (var pin = f.decode.tryPinGeneration()) {
                    replacementRef.set(f.decode.reserve(pin, ID, HARD_KV * 2, EXPECTED_KV * 2, 50));
                }
                return replacementRef.get() != null;
            });
            var replacement = replacementRef.get();
            assertNotEquals(oldReservation.reservationToken(), replacement.reservationToken());
            assertThrows(IllegalStateException.class, () -> oldClaim.complete(DeliveryResult.delivered()));
            f.expire();
            assertFalse(f.decode.release(oldReservation, DecodeEndpoint.ReleaseReason.EXPIRED).released());
            assertFalse(f.decode.release(oldReservation, DecodeEndpoint.ReleaseReason.COUNTERPART_FINISHED).released());
            assertEquals(HARD_KV * 2, f.decode.routingView().inflightHardKv());
            assertEquals(EXPECTED_KV * 2, f.decode.routingView().inflightExpectedKv());
            assertEquals(replacement, f.decode.reservationHandle(ID));
            f.decode.release(replacement, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
            f.assertEmpty();
        }
    }

    private enum LocalEnd { CANCEL, FUTURE_CANCEL, EXPIRE, SHUTDOWN }

    private static final class Fixture implements AutoCloseable {
        final AbstractRequestScheduler requests;
        final SchedulerRuntime runtime;
        final PrefillEndpoint prefill;
        final DecodeEndpoint decode;
        final RequestRoute item;
        final DecodeEndpoint.ReservationHandle reservation;
        final PrefillState.RouteReservation routeReservation;
        final PrefillState.CommittedHandoff prefillHandoff;
        final BalanceContext.AdmissionHandle admission;
        final DecodeEndpoint.AdmissionCapacity capacity = new DecodeEndpoint.AdmissionCapacity(1, 90);
        long version = 1;
        final java.util.concurrent.CompletableFuture<org.flexlb.balance.eviction.EngineCancelChannel.CancelAck> cleanup = new java.util.concurrent.CompletableFuture<>();

        Fixture() { this(false); }

        Fixture(boolean keepAdmissionOpen) {
            var config = SchedulingTestConfig.newConfig();
            config.setScheduler(SchedulerConfig.direct());
            SchedulingTestConfig.useNonBatchDispatcher(config).setMaxInflightPerPrefillWorker(1);
            config.getRequestLifecycle().getRequest().setTimeoutMs(TTL);
            ConfigService service = mock(ConfigService.class);
            when(service.loadBalanceConfig()).thenReturn(config);
            var reporter = mock(BatchSchedulerReporter.class);
            var requestReporter = mock(RequestSchedulerReporter.class);
            requests = org.flexlb.balance.scheduler.SchedulerTestSupport.create(service, reporter, requestReporter,
                mock(RecentCacheKeyTraceReporter.class));
            var channel = mock(org.flexlb.balance.eviction.EngineCancelChannel.class);
            when(channel.cancel(any(), anyLong(), any(), anyLong())).thenReturn(cleanup);
            org.springframework.test.util.ReflectionTestUtils.setField(requests.runtime, "cancelChannel", channel);
            var projector = requests;
            var endpoints = new EndpointRegistry(service, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(projector), reporter, new RouteDeliveryStrategy(reporter), new PlacementAvailability());
            runtime = new SchedulerRuntime(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(requests), endpoints, reporter, requestReporter, org.mockito.Mockito.mock(DefaultBatchDispatcher.class), service, org.mockito.Mockito.mock(org.flexlb.service.RecentCacheKeyTraceReporter.class), org.mockito.Mockito.mock(org.flexlb.balance.eviction.EngineCancelChannel.class));
            var worker = WorkerStatus.createDiscovered(RoleType.PREFILL, "g", "127.0.0.1", 8080, 8081, "test");
            worker.lock.lock();
            try {
                prefill = (PrefillEndpoint) endpoints.publishPreparedEndpoint(worker.getIpPort(), worker,
                        worker.prepareNewStatus(worker.freezeStatusResponse(status(RoleType.PREFILL, 1, Map.of(), Map.of(), TOTAL_KV)))).endpoint();
            } finally { worker.lock.unlock(); }
            decode = new DecodeEndpoint(WorkerStatus.createDiscovered(RoleType.DECODE, "g", "127.0.0.2", 8080, 8081, "test"), org.flexlb.balance.scheduler.SchedulerTestSupport.repository(projector));
            decodeStatus(Map.of(), Map.of(), TOTAL_KV);
            try (var pin = decode.tryPinGeneration()) {
                reservation = decode.reserve(pin, ID, HARD_KV, EXPECTED_KV, 50);
            }
            assertNotNull(reservation);
            var context = RequestProtocolTestSupport.context(config, ID);
            var future = RequestProtocolTestSupport.register(requests, context);
            context.setFuture(future);
            item = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), new Response(), null, null, prefill, decode, reservation, System.currentTimeMillis());
            var held = new AtomicReference<PrefillState.RouteReservation>();
            admission = requests.claimAdmissionHandle(ID, future);
            assertNotNull(admission);
            try (var pin = prefill.tryPinGeneration()) {
                assertTrue((requests.commitRoute(item, RequestProtocolTestSupport.publication(() -> {
                    var result = prefill.reserveUnqueuedRoute(pin, item, 30_000);
                    assertEquals(PrefillState.CapacityStatus.ACQUIRED, result.status());
                    held.set(result.reservation());
                    return true;
                })) == org.flexlb.balance.PlacementResult.Status.SUCCESS));
            }
            routeReservation = held.get();
            try (var commit = prefill.tryBeginRouteCommitAdmission()) {
                assertNotNull(commit);
                prefillHandoff = commit.commit(List.of(item), List.of(routeReservation));
            }
            if (!keepAdmissionOpen) { admission.finish(); }
        }

        DeliveryClaim handoff() {
            var acquisition = decode.acquireDispatchPermit(reservation, capacity);
            assertEquals(DecodeEndpoint.EngineDispatchPermitAcquireStatus.ACQUIRED, acquisition.status());
            var member = new PrefillAdmissionResources.Member(item, acquisition.permit());
            try {
                var claim = RequestProtocolTestSupport.claimBatchWithoutPrediction(
                        requests, item, 1L, () -> member.transferToEndpoint(item));
                assertNotNull(claim);
                assertTrue(claim.tryStartSend());
                return claim;
            } finally {
                PrefillAdmissionResources.closeCommitted(List.of(member), prefillHandoff);
            }
        }

        void assertReserved() {
            assertEquals(1, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(requests).liveRequestCount());
            assertEquals(1, prefill.ownershipStats().locallyOwnedRequests());
            assertEquals(HARD_KV, decode.routingView().inflightHardKv());
            assertEquals(EXPECTED_KV, decode.routingView().inflightExpectedKv());
        }

        void assertHandedOff() {
            assertReserved();
            assertEquals(0, decode.resourceSnapshot().activeDispatchPermits());
            assertEquals(1, decode.routingView().engineCapacityUsed());
            assertFalse(capacity.evaluate(decode.routingView().dispatchUsage(), HARD_KV, EXPECTED_KV).fits());
        }

        void assertEmpty() {
            requests.runtime.continuations().awaitIdle();
            assertEquals(0, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(requests).liveRequestCount(), "request inflight");
            assertEquals(0, prefill.queuedRequestCount(), "Prefill queue membership");
            assertEquals(0, prefill.ownershipStats().locallyOwnedRequests(), "Prefill inflight");
            assertEquals(0, prefill.ownershipStats().individuallyOwnedRequests(), "Prefill individual lease");
            assertEquals(0, prefill.ownershipStats().batchCount(), "Prefill batch occupancy");
            var view = decode.resourceSnapshot();
            assertEquals(0, view.routing().inflightHardKv(), "Decode hard KV reservation");
            assertEquals(0, view.routing().inflightExpectedKv(), "Decode expected KV reservation");
            assertEquals(0, view.activeDispatchPermits(), "Decode dispatch permits");
            assertEquals(0, view.queuedCount(), "Decode queued ownership");
            assertEquals(0, view.acceptedCount(), "Decode accepted streams");
            assertEquals(0, view.runningCount(), "Decode running streams");
            assertEquals(0, view.engineCapacityUsed(), "Decode request capacity");
            assertTrue(view.reservedCount() == 0);
            assertTrue(view.confirmedCount() == 0);
        }

        void assertCapacityReusable() {
            try (var pin = decode.tryPinGeneration()) {
                var next = decode.reserve(pin, 999L, HARD_KV, EXPECTED_KV, 50);
                assertNotNull(next);
                var permit = decode.acquireDispatchPermit(next, capacity);
                assertEquals(DecodeEndpoint.EngineDispatchPermitAcquireStatus.ACQUIRED, permit.status());
                assertTrue(permit.permit().release());
                decode.release(next, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
            }
            // A second exact request must be able to use the sole Prefill slot too.
            var nextContext = RequestProtocolTestSupport.context(item.ctx().getConfig(), 999L);
            nextContext.setFuture(new CompletableFuture<>());
            var next = org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(nextContext),
                    new Response(), null, null, prefill, null, null, System.currentTimeMillis());
            try (var pin = prefill.tryPinGeneration()) {
                var result = prefill.reserveUnqueuedRoute(pin, next, 1);
                assertEquals(PrefillState.CapacityStatus.ACQUIRED, result.status());
                result.reservation().close();
            }
            assertEmpty();
        }

        void proveCleanup() throws Exception {
            cleanup.complete(org.flexlb.balance.eviction.EngineCancelChannel.CancelAck.REQUEST_CLEANED);
            item.ctx().delivery().settlement().toCompletableFuture().get(2, TimeUnit.SECONDS);
            requests.runtime.continuations().awaitIdle();
        }
        void expire() { RequestProtocolTestSupport.expireInactiveRequest(requests, requests.requestSlot(ID), System.currentTimeMillis() + TTL + 1); }
        void decodeStatus(Map<String, TaskInfo> running, Map<String, TaskInfo> finished, long freeKv) {
            applyStatus(decode, status(RoleType.DECODE, version++, running, finished, freeKv));
        }
        @Override
        public void close() {
            try {
                admission.finish();
                prefillHandoff.close();
                routeReservation.close();
                runtime.shutdown();
            } finally {
                decode.close();
            }
        }
    }

    private static TaskInfo task(long id) {
        var task = new TaskInfo();
        task.setRequestId(id);
        task.setInputLength(HARD_KV);
        task.setPhase(TaskPhase.RUNNING);
        return task;
    }

    private static WorkerStatusResponse status(RoleType role, long version, Map<String, TaskInfo> running,
                                                Map<String, TaskInfo> finished, long freeKv) {
        var response = new WorkerStatusResponse();
        response.setRole(role);
        response.setAlive(true);
        response.setStatusVersion(version);
        response.setLatestFinishedVersion(finished.isEmpty() ? 0L : version);
        response.setRunningTaskInfo(running);
        response.setRunningQueryLen((long) running.size());
        response.setFinishedTaskInfo(finished);
        response.setTotalKvCacheTokens(TOTAL_KV);
        response.setAvailableKvCacheTokens(freeKv);
        return response;
    }

    private static void applyStatus(WorkerEndpoint endpoint, WorkerStatusResponse response) {
        var worker = endpoint.getStatus();
        Runnable projection;
        worker.lock.lock();
        try { projection = endpoint.applyPreparedStatus(worker, worker.prepareNewStatus(worker.freezeStatusResponse(response))); }
        finally { worker.lock.unlock(); }
        projection.run();
    }
}
