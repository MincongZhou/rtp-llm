package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointCleanupTestSupport;
import org.flexlb.balance.endpoint.EndpointCleanupTestSupport.PrefillLedger;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongPredicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Cleanup uses current directory membership, including terminal records, rather than live Slot state. */
class EndpointCleanupOwnershipTest {
    private FlexlbConfig config;
    private AbstractRequestScheduler registry;

    @BeforeEach void setup() {
        config = SchedulingTestConfig.batchConfig();
        ConfigService service = mock(ConfigService.class);
        when(service.loadBalanceConfig()).thenReturn(config);
        registry = org.flexlb.balance.scheduler.SchedulerTestSupport.create(service, mock(BatchSchedulerReporter.class),
                mock(RequestSchedulerReporter.class),
                mock(RecentCacheKeyTraceReporter.class));
    }

    @AfterEach void close() {
        if (RequestProtocolTestSupport.closeAdmissionAndAwaitMutations(registry)) {
            registry.closeOutstandingAndTerminalize();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(registry).timer().close();
            org.flexlb.balance.scheduler.SchedulerTestSupport.runtime(registry).closeRequestExecutors();
        }
    }

    @Test void currentDirectoryProtectsReservationButEarlierSnapshotCanEvictIt() {
        Set<Long> beforeRegistration = org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).snapshotActive().stream()
                .map(BalanceContext::getRequestId).collect(java.util.stream.Collectors.toSet());
        long id = 7001L;
        registry.register(RequestProtocolTestSupport.context(config, id), StrategyErrorType.BATCH_SLO_EXPIRED);
        var endpoint = new DecodeEndpoint(WorkerStatus.createDiscovered(
                RoleType.DECODE, null, "127.0.0.1", 8080, 8081, null), org.flexlb.balance.scheduler.SchedulerTestSupport.repository(mock(AbstractRequestScheduler.class)));
        try (var pin = endpoint.tryPinGeneration()) {
            assertNotNull(pin);
            assertNotNull(endpoint.reserveUnqueued(pin, id, 1L, 1L, 50));
        }
        assertEquals(0, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertEquals(1, endpoint.getInflightCount());
        // Models moving the ownership scan outside the endpoint lock without revalidation.
        assertEquals(1, endpoint.evictExpiredRequests(-1L, beforeRegistration::contains));
        assertEquals(0, endpoint.getInflightCount());
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).retainsIdentity(id), "request is still registered despite premature eviction");
    }

    @Test
    void terminalRecordMembershipIsConservativeButNotEquivalentToLiveOwnership() throws Exception {
        long id = 7002L;
        var future = registry.register(RequestProtocolTestSupport.context(config, id), StrategyErrorType.BATCH_SLO_EXPIRED);
        BalanceContext original = registry.requestSlot(id);
        registry.cancelRequest(id, 0L, CancelReason.CLIENT_CANCELLED);
        future.get(5, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(0, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).liveRequestCount());
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).retainsIdentity(id));
        assertNull(registry.requestSlot(id), "finished requests leave the active context directory");
        assertEquals(RequestState.Phase.CANCELLED, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(id, 0L).state());
        RequestState retiredRecord0 = org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(original.getRequestId(), 0L);
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, retiredRecord0), Long.MAX_VALUE));
        assertFalse(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).retainsIdentity(id));
        var replacementFuture = registry.register(RequestProtocolTestSupport.context(config, id), StrategyErrorType.BATCH_SLO_EXPIRED);
        assertNotSame(original, registry.requestSlot(id));
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).retainsIdentity(id));
        assertFalse(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, retiredRecord0), Long.MAX_VALUE));
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).retainsIdentity(id), "old generation cleanup must preserve replacement");
        registry.cancelRequest(id, 0L, CancelReason.CLIENT_CANCELLED);
        replacementFuture.get(5, java.util.concurrent.TimeUnit.SECONDS);
        RequestState replacement = org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(id, 0L);
        assertEquals(RequestState.Phase.CANCELLED, replacement.state());
        assertNotSame(retiredRecord0, replacement);
        assertFalse(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, retiredRecord0), Long.MAX_VALUE));
        assertSame(replacement, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(id, 0L), "old sweeper cannot delete a new terminal record");
        RequestState equalCopy = new RequestState(replacement.requestId(), replacement.state(),
                replacement.deliveryClaimKind(), replacement.batchId(), replacement.createdAtMs(),
                replacement.updatedAtMs(), replacement.detail());
        assertEquals(replacement, equalCopy);
        assertFalse(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, equalCopy), Long.MAX_VALUE));
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, replacement), Long.MAX_VALUE));
    }

    @ParameterizedTest(name = "Decode confirmed={0}: absent lookup cannot release the replacement")
    @ValueSource(booleans = {false, true})
    void registrationAfterAbsentCheckAcquiresFreshReservation(boolean confirmed) throws Exception {
        long id = 7003L;
        var endpoint = new DecodeEndpoint(WorkerStatus.createDiscovered(
                RoleType.DECODE, null, "127.0.0.1", 8080, 8081, null), org.flexlb.balance.scheduler.SchedulerTestSupport.repository(mock(AbstractRequestScheduler.class)));
        var oldReservation = reserve(endpoint, id, 100L, 150L);
        if (confirmed) {
            EndpointCleanupTestSupport.confirmDecode(endpoint, id);
        }
        Runnable assertOldLedger = () -> {
            assertDecodeLedger(endpoint, confirmed ? 0 : 1, confirmed ? 1 : 0,
                    confirmed ? 0L : 100L, confirmed ? 0L : 150L);
            assertEquals(confirmed, endpoint.isAcceptedByEngine(oldReservation));
        };
        assertOldLedger.run();

        // Every owned entry now has one retention check in the canonical sweep.
        var result = raceRegistrationAfterAbsentCheck(id,
                retain -> endpoint.evictExpiredRequests(-1L, retain),
                () -> reserve(endpoint, id, 200L, 350L), assertOldLedger);
        assertEquals(confirmed ? 0 : 1, result.evicted(),
                "the return value counts shadow evictions, not confirmed-record purges");
        var replacement = result.owner();
        assertNotEquals(oldReservation.reservationToken(), replacement.reservationToken());
        assertDecodeLedger(endpoint, 1, 0, 200L, 350L);
        endpoint.release(oldReservation, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
        assertDecodeLedger(endpoint, 1, 0, 200L, 350L);
        assertEquals(0, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertDecodeLedger(endpoint, 1, 0, 200L, 350L);

        BalanceContext slot = registry.requestSlot(id);
        registry.cancelRequest(id, 0L, CancelReason.CLIENT_CANCELLED);
        assertEquals(0, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertDecodeLedger(endpoint, 1, 0, 200L, 350L);
        RequestState retiredRecord1 = org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(slot.getRequestId(), 0L);
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, retiredRecord1), Long.MAX_VALUE));
        assertEquals(1, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertDecodeLedger(endpoint, 0, 0, 0L, 0L);
        endpoint.release(replacement, DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
        assertEquals(0, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertDecodeLedger(endpoint, 0, 0, 0L, 0L);
    }

    @ParameterizedTest(name = "Prefill batch={0}: absent lookup cannot release the replacement")
    @ValueSource(booleans = {false, true})
    void prefillRegistrationAfterAbsentCheckPreservesFreshWork(boolean batch) throws Exception {
        long id = 7004L;
        var ledger = new PrefillLedger(batch);
        var old = ledger.commit(id, 1L, 20L);
        ledger.advanceBeyondTtl();
        var result = raceRegistrationAfterAbsentCheck(id, ledger::sweep,
                () -> ledger.commit(id, 2L, 70L), () -> ledger.assertOwned(old, 20L));
        assertEquals(1, result.evicted());
        var replacement = result.owner();
        assertNotSame(old.item(), replacement.item());
        assertNotSame(old.reservation(), replacement.reservation());
        ledger.assertOwned(replacement, 70L);
        ledger.assertStaleReleaseIsIgnored(old);
        ledger.assertOwned(replacement, 70L);

        ledger.advanceBeyondTtl();
        assertEquals(0, ledger.sweep(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        ledger.assertOwned(replacement, 70L);
        BalanceContext slot = registry.requestSlot(id);
        registry.cancelRequest(id, 0L, CancelReason.CLIENT_CANCELLED);
        assertEquals(0, ledger.sweep(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        ledger.assertOwned(replacement, 70L);
        RequestState retiredRecord2 = org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(slot.getRequestId(), 0L);
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, retiredRecord2), Long.MAX_VALUE));
        assertEquals(1, ledger.sweep(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        ledger.assertEmpty();
        ledger.assertStaleReleaseIsIgnored(replacement);
        assertEquals(0, ledger.sweep(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        ledger.assertEmpty();

        // Reacquisition with a limit of one checks that cleanup returned the
        // actual admission capacity, including the batch lease, exactly once.
        registry.register(RequestProtocolTestSupport.context(config, id), StrategyErrorType.BATCH_SLO_EXPIRED);
        var third = ledger.commit(id, 3L, 90L);
        ledger.assertOwned(third, 90L);
        ledger.assertStaleReleaseIsIgnored(replacement);
        ledger.assertOwned(third, 90L);
        registry.cancelRequest(id, 0L, CancelReason.CLIENT_CANCELLED);
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(id, 0L)), Long.MAX_VALUE));
        ledger.advanceBeyondTtl();
        assertEquals(1, ledger.sweep(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        ledger.assertEmpty();
    }

    @ParameterizedTest(name = "Prefill batch={0}: replacement directory entry retains old work")
    @ValueSource(booleans = {false, true})
    void prefillReplacementDirectoryEntryConservativelyRetainsOldWork(boolean batch) {
        long id = 7006L;
        registry.register(RequestProtocolTestSupport.context(config, id), StrategyErrorType.BATCH_SLO_EXPIRED);
        var ledger = new PrefillLedger(batch);
        var old = ledger.commit(id, 1L, 20L);
        ledger.advanceBeyondTtl();
        BalanceContext original = registry.requestSlot(id);
        registry.cancelRequest(id, 0L, CancelReason.CLIENT_CANCELLED);
        assertEquals(0, ledger.sweep(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        ledger.assertOwned(old, 20L);
        RequestState retiredRecord3 = org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(original.getRequestId(), 0L);
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, retiredRecord3), Long.MAX_VALUE));

        registry.register(RequestProtocolTestSupport.context(config, id), StrategyErrorType.BATCH_SLO_EXPIRED);
        assertFalse(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, retiredRecord3), Long.MAX_VALUE));
        assertEquals(0, ledger.sweep(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        ledger.assertOwned(old, 20L);
        registry.cancelRequest(id, 0L, CancelReason.CLIENT_CANCELLED);
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(id, 0L)), Long.MAX_VALUE));
        assertEquals(1, ledger.sweep(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        ledger.assertStaleReleaseIsIgnored(old);
        assertEquals(0, ledger.sweep(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        ledger.assertEmpty();
    }

    @Test void confirmedRecordIsRetainedUntilExactTerminalRecordRemoval() {
        long id = 7005L;
        registry.register(RequestProtocolTestSupport.context(config, id), StrategyErrorType.BATCH_SLO_EXPIRED);
        var endpoint = new DecodeEndpoint(WorkerStatus.createDiscovered(
                RoleType.DECODE, null, "127.0.0.1", 8080, 8081, null), org.flexlb.balance.scheduler.SchedulerTestSupport.repository(mock(AbstractRequestScheduler.class)));
        var reservation = reserve(endpoint, id, 100L, 150L);
        EndpointCleanupTestSupport.confirmDecode(endpoint, id);
        assertEquals(0, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertDecodeLedger(endpoint, 0, 1, 0L, 0L);
        BalanceContext original = registry.requestSlot(id);
        registry.cancelRequest(id, 0L, CancelReason.CLIENT_CANCELLED);
        assertEquals(0, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertDecodeLedger(endpoint, 0, 1, 0L, 0L);
        RequestState retiredRecord3 = org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(original.getRequestId(), 0L);
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, retiredRecord3), Long.MAX_VALUE));

        registry.register(RequestProtocolTestSupport.context(config, id), StrategyErrorType.BATCH_SLO_EXPIRED);
        assertFalse(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, retiredRecord3), Long.MAX_VALUE));
        assertEquals(0, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertTrue(endpoint.isAcceptedByEngine(reservation));
        assertDecodeLedger(endpoint, 0, 1, 0L, 0L);
        registry.cancelRequest(id, 0L, CancelReason.CLIENT_CANCELLED);
        assertTrue(org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).removeExactTerminal(org.flexlb.balance.scheduler.SchedulerTestSupport.terminalRecord(registry, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).getRequestState(id, 0L)), Long.MAX_VALUE));
        assertEquals(0, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertDecodeLedger(endpoint, 0, 0, 0L, 0L);
        assertFalse(endpoint.isAcceptedByEngine(reservation));
        assertEquals(0, endpoint.evictExpiredRequests(-1L, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry)::retainsIdentity));
        assertDecodeLedger(endpoint, 0, 0, 0L, 0L);
    }

    private static DecodeEndpoint.ReservationHandle reserve(
            DecodeEndpoint endpoint, long id, long hardKv, long expectedKv) {
        try (var pin = endpoint.tryPinGeneration()) {
            assertNotNull(pin);
            var reservation = endpoint.reserveUnqueued(pin, id, hardKv, expectedKv, 50);
            assertNotNull(reservation);
            return reservation;
        }
    }

    private static void assertDecodeLedger(DecodeEndpoint endpoint, int reserved, int confirmed,
                                          long hardKv, long expectedKv) {
        var view = endpoint.resourceSnapshot();
        assertEquals(reserved, endpoint.getInflightCount());
        assertEquals(reserved, view.reservedCount());
        assertEquals(confirmed, view.confirmedCount());
        assertEquals(reserved + confirmed, view.routing().totalLoad());
        // These are immediate reservations, so both unqueued shadows and
        // confirmed Engine owners occupy dispatch capacity.
        assertEquals(reserved + confirmed, view.engineCapacityUsed());
        assertEquals(hardKv, view.routing().inflightHardKv());
        assertEquals(expectedKv, view.routing().inflightExpectedKv());
    }

    private record CleanupRace<T>(int evicted, T owner) { }

    private <T> CleanupRace<T> raceRegistrationAfterAbsentCheck(
            long id, ToIntFunction<LongPredicate> sweep,
            Supplier<T> acquire, Runnable assertOldLedger) throws Exception {
        CountDownLatch checkedAbsent = new CountDownLatch(1);
        CountDownLatch registered = new CountDownLatch(1);
        AtomicReference<Thread> acquirer = new AtomicReference<>();
        AtomicInteger queries = new AtomicInteger();
        var executor = Executors.newFixedThreadPool(2, task -> {
            Thread thread = new Thread(task, "cleanup-registration-race");
            thread.setDaemon(true);
            return thread;
        });
        try {
            var cleanup = executor.submit(() -> sweep.applyAsInt(requestId -> {
                assertEquals(id, requestId);
                boolean retain = org.flexlb.balance.scheduler.SchedulerTestSupport.repository(registry).retainsIdentity(requestId);
                assertFalse(retain);
                queries.incrementAndGet();
                assertOldLedger.run();
                checkedAbsent.countDown();
                await(registered);
                // Do not unblock cleanup until the new generation has actually
                // attempted resource acquisition and is waiting on this lock.
                awaitEndpointLockWait(acquirer.get(), Thread.currentThread());
                assertOldLedger.run();
                return retain;
            }));
            var replacement = executor.submit(() -> {
                await(checkedAbsent);
                registry.register(RequestProtocolTestSupport.context(config, id), StrategyErrorType.BATCH_SLO_EXPIRED);
                acquirer.set(Thread.currentThread());
                registered.countDown();
                return acquire.get();
            });
            int evicted = cleanup.get(10, TimeUnit.SECONDS);
            T owner = replacement.get(10, TimeUnit.SECONDS);
            assertEquals(1, queries.get());
            return new CleanupRace<>(evicted, owner);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "race threads did not finish");
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "race barrier timed out");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static void awaitEndpointLockWait(Thread acquirer, Thread cleaner) {
        var threads = ManagementFactory.getThreadMXBean();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            var info = threads.getThreadInfo(acquirer.threadId());
            if (info != null && info.getLockOwnerId() == cleaner.threadId()) {
                return;
            }
            Thread.yield();
        }
        fail("new resource acquisition did not wait for the cleanup thread's endpoint lock");
    }
}
