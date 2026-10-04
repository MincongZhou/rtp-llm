package org.flexlb.balance.endpoint;

import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.BalanceContext;
import org.flexlb.balance.scheduler.RequestRoute;
import org.flexlb.balance.scheduler.AbstractRequestScheduler;
import org.flexlb.config.DispatcherConfig;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.SchedulingMetadata;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.master.TaskInfo;
import org.flexlb.dao.master.WorkerStatusResponse;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.TaskPhase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToLongFunction;

import static org.flexlb.balance.scheduler.SchedulingTestConfig.freezeInputs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PrefillStateSnapshotTest {
    private final AtomicLong clock = new AtomicLong(100);
    private final ReentrantLock lock = new ReentrantLock();
    private final PrefillActiveIndex waiting = PrefillActiveIndex.ordered(4,
            Comparator.comparingLong(RequestRoute::requestId));
    private final PrefillState state = new PrefillState(lock, waiting, clock::get, () -> { });
    private final EndpointGenerationLifecycle generation = new EndpointGenerationLifecycle(() -> { });

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentCommittedCloseReleasesOnlyItsHandoffAndKeepsRequestOwnership(boolean batch) throws Exception {
        AtomicInteger drained = new AtomicInteger();
        EndpointGenerationLifecycle lifecycle = new EndpointGenerationLifecycle(drained::incrementAndGet);
        RequestRoute request = item(1);
        PrefillState.CommittedHandoff handoff;
        if (batch) {
            enqueue(request);
            try (var lease = state.reserveBatch(request, 9L, 1, lifecycle.tryAcquireHandoff()).reservation()) {
                handoff = lease.commit(List.of(request), 10L);
            }
        } else {
            try (var lease = state.reserveUnqueuedRoute(request, 10L, Long.MAX_VALUE).reservation()) {
                handoff = state.commitRouteGroup(List.of(request), List.of(lease), lifecycle.tryAcquireHandoff());
            }
        }
        try (handoff; var otherHandoff = lifecycle.tryAcquireHandoff();
             var closers = Executors.newFixedThreadPool(2)) {
            lifecycle.beginRetirement();
            assertFalse(lifecycle.tryStartCleanup());
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> closes = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                closes.add(closers.submit(() -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    handoff.close();
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> close : closes) { close.get(5, TimeUnit.SECONDS); }
            assertEquals(0, drained.get(), "the other handoff still prevents retirement");
            otherHandoff.close();
            handoff.close();
            assertEquals(1, drained.get());
            assertEquals(1L, state.observedRequestCount(), "closing handoff cannot release committed capacity");
            assertTrue(state.terminalizeCommittedItem(request));
            assertEquals(0L, state.observedRequestCount());
        }
    }

    @Test
    void duplicateBatchMembersFailBeforeQueueMutationAndKeepTheLeaseRetryable() {
        RequestRoute first = item(1), second = item(2);
        enqueue(first);
        enqueue(second);
        try (var lease = state.reserveBatch(first, 9L, 1, generation.tryAcquireHandoff()).reservation()) {
            var before = state.captureQueue(4);
            assertThrows(IllegalStateException.class,
                    () -> lease.commit(List.of(first, first), 10L));
            assertEquals(before, state.captureQueue(4), "invalid input must not detach any ACTIVE member");
            assertEquals(1, state.captureQueueCounters().batchSlots());
            try (var committed = lease.commit(List.of(first, second), 10L)) {
                assertTrue(state.captureQueue(4).items().isEmpty());
                assertEquals(List.of(1L, 2L), state.committedSnapshot().batches().getFirst().requestIds());
            }
            assertTrue(state.terminalizeCommittedItem(first));
            assertEquals(1, state.captureQueueCounters().batchSlots());
            assertTrue(state.terminalizeCommittedItem(second));
            assertEquals(0, state.captureQueueCounters().batchSlots());
        }
    }

    @Test
    void queueCaptureKeepsBoundedOrderAndVersionsAcrossChanges() {
        RequestRoute first = item(1), second = item(2), third = item(3), tail = item(4);
        enqueue(first);
        enqueue(second);
        enqueue(third);

        PrefillState.QueueSnapshot bounded = state.captureQueue(2);
        assertEquals(List.of(first, second), bounded.items());
        assertSame(first, bounded.head());
        assertEquals(List.of(first, second, third), state.captureQueue(4).items());
        assertEquals(waiting.version(), bounded.queueVersion());
        assertEquals(state.schedulingInputVersion(), bounded.schedulingInputVersion());

        lock.lock();
        try {
            state.schedulingInputsChangedUnderLock();
        } finally {
            lock.unlock();
        }
        PrefillState.QueueSnapshot changedInputs = state.captureQueue(2);
        assertEquals(bounded.queueVersion(), changedInputs.queueVersion());
        assertTrue(changedInputs.schedulingInputVersion() > bounded.schedulingInputVersion());
        enqueue(tail);
        PrefillState.QueueSnapshot changedQueue = state.captureQueue(2);
        assertEquals(List.of(first, second), changedQueue.items());
        assertTrue(changedQueue.queueVersion() > bounded.queueVersion());
        assertEquals(List.of(first, second), bounded.items(), "an earlier capture keeps its identities");
        assertThrows(UnsupportedOperationException.class, () -> bounded.items().clear());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void batchCapacityReleaseAlwaysWakesItsBoundWorker(int subscriptionChanges) {
        AtomicInteger notifications = new AtomicInteger();
        Runnable wake = () -> {
            assertFalse(lock.isHeldByCurrentThread());
            notifications.incrementAndGet();
        };
        PrefillState ledger = new PrefillState(lock, waiting, clock::get, wake);
        var availability = ledger.batchAvailability(1);
        assertThrows(IllegalArgumentException.class, () -> availability.addListener(() -> { }));
        assertThrows(IllegalArgumentException.class, () -> availability.addListener(null));
        if (subscriptionChanges > 0) {
            availability.addListener(wake);
            availability.addListener(wake);
        }
        if (subscriptionChanges > 1) {
            availability.removeListener(wake);
        }
        var request = item(99L);
        lock.lock();
        try {
            assertTrue(ledger.enqueueActiveUnderLock(request, 0L));
        } finally {
            lock.unlock();
        }
        assertTrue(availability.isAvailable());
        try (var lease = ledger.reserveBatch(request, 9L, 1, generation.tryAcquireHandoff()).reservation()) {
            org.junit.jupiter.api.Assertions.assertNotNull(lease);
            assertFalse(availability.isAvailable());
        }
        assertTrue(availability.isAvailable());
        assertEquals(1, notifications.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void openLeaseRollbackNotifiesOnceOutsideLockEvenWhenHandoffFails(boolean batch) {
        AtomicInteger notifications = new AtomicInteger();
        PrefillState ledger = new PrefillState(lock, waiting, clock::get, () -> {
            assertFalse(lock.isHeldByCurrentThread());
            notifications.incrementAndGet();
            throw new IllegalStateException("observer failure");
        });
        RuntimeException failure = new IllegalStateException("generation drain failure");
        EndpointGenerationLifecycle retiring = new EndpointGenerationLifecycle(() -> { throw failure; });
        RequestRoute request = item(91L);
        lock.lock();
        try {
            assertTrue(ledger.enqueueActiveUnderLock(request, 10L));
        } finally {
            lock.unlock();
        }
        long queuedVersion = waiting.version();
        PrefillState.Reservation lease = batch
                ? ledger.reserveBatch(request, 9L, 1, retiring.tryAcquireHandoff()).reservation()
                : EndpointTestSupport.reserveRoute(ledger, request, 10L);
        if (batch) {
            retiring.beginRetirement();
            assertFalse(retiring.tryStartCleanup());
            assertSame(failure, assertThrows(IllegalStateException.class, lease::close));
        } else {
            lease.close();
        }
        assertEquals(1, notifications.get());
        lease.close();
        assertEquals(1, notifications.get(), "an already settled lease cannot notify twice");
        lock.lock();
        try {
            assertEquals(0, ledger.captureQueueCounters().batchSlots());
            assertTrue(waiting.contains(request), "rollback retains the queued request");
            assertEquals(queuedVersion, waiting.version(), "lease rollback does not change queue membership");
        } finally {
            lock.unlock();
        }
    }

    @Test
    void batchIdRemainsReservedUntilItsLastMemberSettles() {
        RequestRoute first = item(1), sibling = item(2), next = item(3);
        enqueue(first);
        enqueue(sibling);
        enqueue(next);
        try (var lease = state.reserveBatch(first, 10, 4, generation.tryAcquireHandoff()).reservation()) {
            try (var probe = generation.tryAcquireHandoff()) {
                assertEquals(PrefillState.CapacityStatus.BATCH_ID_ALREADY_RESERVED,
                        state.reserveBatch(next, 10, 4, probe).status());
            }
            try (var handoff = lease.commit(List.of(first, sibling), 30)) {
                assertEquals(1, state.stats().batchCount());
            }
            assertTrue(state.terminalizeCommittedItem(first));
            try (var probe = generation.tryAcquireHandoff()) {
                assertEquals(PrefillState.CapacityStatus.BATCH_ID_ALREADY_RESERVED,
                        state.reserveBatch(next, 10, 4, probe).status());
            }
            assertTrue(state.terminalizeCommittedItem(sibling));
            try (var replacement = state.reserveBatch(next, 10, 4,
                    generation.tryAcquireHandoff()).reservation()) {
                assertEquals(10, replacement.batchId());
                try (var handoff = replacement.commit(List.of(next), 20)) {
                    assertEquals(1, state.stats().batchCount());
                }
                assertTrue(state.terminalizeCommittedItem(next));
            }
        }
        assertEquals(0, state.stats().locallyOwnedRequests());
        assertEquals(0, state.stats().batchCount());
    }

    @Test
    void orphanSweepSharesTtlAndKeepsEveryMemberOfARetainedBatch() {
        RequestRoute first = item(1), sibling = item(2), individual = item(3), queued = item(4);
        enqueue(first);
        enqueue(sibling);
        enqueue(queued);
        long beforeCommitVersion = waiting.version();
        try (var batch = state.reserveBatch(first, 1, 4, generation.tryAcquireHandoff()).reservation();
             var handoff = batch.commit(List.of(first, sibling), 30)) {
            assertEquals(2, state.stats().locallyOwnedRequests());
            assertTrue(waiting.version() > beforeCommitVersion, "batch commit invalidates the queue revision");
            assertEquals(List.of(queued), waitingItems());
        }
        try (var reservation = state.reserveUnqueuedRoute(individual, 20, Long.MAX_VALUE).reservation();
             var handoff = state.commitRouteGroup(List.of(individual), List.of(reservation),
                     generation.tryAcquireHandoff())) {
            assertEquals(3, state.stats().locallyOwnedRequests());
        }
        clock.set(105);
        RequestRoute fresh = item(5);
        try (var reservation = state.reserveUnqueuedRoute(fresh, 20, Long.MAX_VALUE).reservation();
             var handoff = state.commitRouteGroup(List.of(fresh), List.of(reservation),
                     generation.tryAcquireHandoff())) {
            assertEquals(4, state.stats().locallyOwnedRequests());
        }
        clock.set(110);
        assertEquals(1, state.evictExpiredInflight(10, id -> id == first.requestId()));
        assertEquals(3, state.stats().locallyOwnedRequests(), "one retained member protects both batch members");
        assertEquals(1, state.stats().batchCount());
        assertEquals(1, state.stats().individuallyOwnedRequests(), "fresh individual survives");
        assertEquals(List.of(queued), waitingItems());
        assertEquals(1, state.evictExpiredInflight(10, ignored -> false), "a batch counts once");
        assertEquals(1, state.stats().locallyOwnedRequests());
        clock.set(115);
        assertEquals(1, state.evictExpiredInflight(10, ignored -> false));
        assertEquals(List.of(queued), waitingItems(), "orphan sweeps never remove queued ownership");
        assertTrue(state.committedSnapshot().batches().isEmpty());
        assertTrue(state.committedSnapshot().requests().isEmpty());
    }

    @Test
    void concurrentReadersShareACompleteImmutableMaterialization() throws Exception {
        var second = state.reserveUnqueuedRoute(item(2), 20, Long.MAX_VALUE).reservation();
        var first = state.reserveUnqueuedRoute(item(1), 10, Long.MAX_VALUE).reservation();
        var captured = capture();
        try (var executor = Executors.newFixedThreadPool(8)) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<WorkSnapshot>> readers = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                readers.add(executor.submit(() -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    return captured.work().materialize();
                }));
            }
            start.countDown();
            WorkSnapshot shared = readers.getFirst().get(5, TimeUnit.SECONDS);
            for (var reader : readers) {
                assertSame(shared, reader.get(5, TimeUnit.SECONDS));
            }
            assertEquals(List.of(1L, 2L), shared.requests().stream()
                    .map(WorkSnapshot.RequestWork::requestId).toList());
            second.close();
            first.close();
            assertEquals(2, shared.requests().size());
            assertTrue(capture().work().materialize().requests().isEmpty());
        }
    }

    @Test
    void committedWorkChangesReuseActiveMembershipButCaptureBothAtOnePoint() {
        var queued = item(1);
        enqueue(queued);
        var before = capture();
        var reservation = state.reserveUnqueuedRoute(item(2), 20, Long.MAX_VALUE).reservation();
        var after = capture();
        assertSame(before.active(), after.active());
        assertTrue(before.work().materialize().requests().isEmpty());
        assertEquals(1, after.work().materialize().requests().size());
        enqueue(item(3));
        var added = capture();
        assertNotSame(after.active(), added.active());
        assertSame(after.work(), added.work());
        assertEquals(List.of(queued.requestId()), before.active().projectedItems().stream()
                .map(org.flexlb.balance.planner.GroupPlanner.Item::requestId).toList());
        reservation.close();
        assertSame(added.active(), capture().active());
        assertTrue(capture().work().materialize().requests().isEmpty());
    }

    @Test
    void clockRollbackRecapturesWorkWithoutMutatingEarlierSnapshots() {
        state.reserveUnqueuedRoute(item(1), 10, Long.MAX_VALUE);
        var original = capture();
        clock.set(101);
        assertSame(original.work(), capture().work());
        clock.set(90);
        var rebased = capture();
        assertNotSame(original.work(), rebased.work());
        assertEquals(100, original.work().materialize().capturedAtMs());
        assertEquals(90, rebased.work().materialize().capturedAtMs());
        assertEquals(rebased.capturedAtMs(), rebased.work().materialize().capturedAtMs());
        assertEquals(original.work().materialize().requests(), rebased.work().materialize().requests());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void routeCommitValidatesEveryCanonicalLeaseBeforeTransferringAny(boolean replaced) {
        RequestRoute first = routeItem(101), second = routeItem(102);
        enqueue(first);
        enqueue(second);
        try (var firstLease = EndpointTestSupport.reserveRoute(state, first, 30L);
             var secondLease = EndpointTestSupport.reserveRoute(state, second, 40L)) {
            if (!replaced) { secondLease.close(); }
            try (var permit = generation.tryAcquireHandoff()) {
                assertThrows(IllegalStateException.class, () -> state.commitRouteGroup(
                        List.of(first, second), List.of(firstLease, replaced ? firstLease : secondLease), permit));
            }
            assertEquals(List.of(first, second), waitingItems());
            assertSame(firstLease, state.prepareRoute(first, 30L));
            assertFalse(state.terminalizeCommittedItem(first));
            var validSecond = replaced ? secondLease : EndpointTestSupport.reserveRoute(state, second, 40L);
            try (validSecond; var handoff = state.commitRouteGroup(List.of(first, second),
                    List.of(firstLease, validSecond), generation.tryAcquireHandoff())) {
                assertTrue(waitingItems().isEmpty());
                assertThrows(IllegalStateException.class, () -> state.prepareRoute(first, 50L));
                assertThrows(IllegalStateException.class, () -> state.prepareRoute(second, 50L));
                assertEquals(0L, handoff.precedingWork().materialize().totalRemainingWorkMs().orElseThrow());
            }
        }
        assertEquals(2, state.committedSnapshot().requests().size());
        assertTrue(state.terminalizeCommittedItem(first));
        assertTrue(state.terminalizeCommittedItem(second));
        assertTrue(state.committedSnapshot().requests().isEmpty());
    }

    private static RequestRoute routeItem(long id) {
        var config = new FlexlbConfig();
        config.getDispatcher().setType(DispatcherConfig.Type.NON_BATCH);
        var context = new BalanceContext(config);
        var request = new Request();
        request.setRequestId(id);
        request.setSeqLen(100L);
        context.setRequest(request);
        context.setSchedulingMetadata(SchedulingMetadata.explicit(50, Long.MAX_VALUE));
        return org.flexlb.balance.scheduler.RequestRoute.create(freezeInputs(context), null, null, null, null, null, null, 100L);
    }

    @Test
    void queuedAndImmediateRoutesUseOneCommitAndTerminalOwnership() {
        RequestRoute queued = item(1), immediate = item(2);
        enqueue(queued);
        try (var queuedReservation = EndpointTestSupport.reserveRoute(state, queued, 30L);
             var immediateReservation = state.reserveUnqueuedRoute(immediate, 40L, Long.MAX_VALUE).reservation()) {
            assertEquals(List.of(queued), waitingItems());
            assertEquals(1, state.committedSnapshot().requests().size());
            try (var handoff = state.commitRouteGroup(List.of(queued, immediate),
                    List.of(queuedReservation, immediateReservation), generation.tryAcquireHandoff())) {
                assertTrue(waitingItems().isEmpty());
                assertEquals(0L, handoff.precedingWork().materialize().totalRemainingWorkMs().orElseThrow(),
                        "the selected group's own provisional work is excluded");
                assertEquals(List.of(1L, 2L), state.committedSnapshot().requests().stream()
                        .map(work -> work.requestId()).toList());
                assertEquals(70L, state.committedSnapshot().knownRemainingWorkMsAt(System.currentTimeMillis()));
            }
        }
        assertEquals(2, state.committedSnapshot().requests().size(), "closing committed capabilities cannot release Engine ownership");
        assertTrue(state.terminalizeCommittedItem(queued));
        assertTrue(state.terminalizeCommittedItem(immediate));
        assertTrue(state.committedSnapshot().requests().isEmpty());
    }

    @Test
    void immediatePreparationIsVisibleToLaterPredictionsAndRollbackRemovesIt() {
        RequestRoute immediate = item(1);
        try (var reservation = state.reserveUnqueuedRoute(immediate, 40L, Long.MAX_VALUE).reservation()) {
            assertTrue(waitingItems().isEmpty());
            assertEquals(40L, capture().work().materialize().knownRemainingWorkMsAt(System.currentTimeMillis()));
            assertSame(reservation, state.prepareRoute(immediate, 80L));
            assertEquals(80L, capture().work().materialize().knownRemainingWorkMsAt(System.currentTimeMillis()));
        }
        assertTrue(capture().work().materialize().requests().isEmpty());
        assertTrue(state.committedSnapshot().requests().isEmpty());
    }

    @Test
    void leaseRollbackRetainsQueuedWorkButReleasesUnqueuedAdmission() {
        RequestRoute queued = item(1), immediate = item(2);
        enqueue(queued);
        EndpointTestSupport.reserveRoute(state, queued, 30L).close();
        state.reserveUnqueuedRoute(immediate, 40L, Long.MAX_VALUE).reservation().close();
        assertEquals(List.of(queued), waitingItems());
        assertTrue(state.committedSnapshot().requests().isEmpty());
        assertTrue(EndpointTestSupport.reserveRoute(state, queued, 35L) != null);
    }

    @Test
    void routeCommitRejectsMissingQueueIndexBeforeCommittingAnyMember() {
        RequestRoute queued = item(1), immediate = item(2);
        enqueue(queued);
        try (var queuedReservation = EndpointTestSupport.reserveRoute(state, queued, 30L);
             var immediateReservation = state.reserveUnqueuedRoute(immediate, 40L, Long.MAX_VALUE).reservation()) {
            assertTrue(waiting.remove(queued));
            try (var handoff = generation.tryAcquireHandoff()) {
                assertThrows(IllegalStateException.class, () -> state.commitRouteGroup(
                        List.of(immediate, queued), List.of(immediateReservation, queuedReservation), handoff));
            }
            assertFalse(state.terminalizeCommittedItem(immediate));
            assertTrue(waiting.add(queued));
            try (var handoff = state.commitRouteGroup(List.of(immediate, queued),
                    List.of(immediateReservation, queuedReservation), generation.tryAcquireHandoff())) {
                assertTrue(waiting.isEmpty());
            }
        }
    }

    @Test
    void retirementProjectsQueuedPreparedAndCommittedItemsWithoutModeExceptions() {
        RequestRoute queued = item(1), prepared = item(2), committed = item(3);
        enqueue(queued);
        var preparedReservation = state.reserveUnqueuedRoute(prepared, 20L, Long.MAX_VALUE).reservation();
        var committedReservation = state.reserveUnqueuedRoute(committed, 30L, Long.MAX_VALUE).reservation();
        try (var handoff = state.commitRouteGroup(List.of(committed), List.of(committedReservation),
                generation.tryAcquireHandoff())) {
            assertEquals(2, state.committedSnapshot().requests().size());
        }
        var retired = state.retireGenerationOwnership();
        assertNull(retired.invariantFailure());
        assertEquals(3, retired.ownedItems().size());
        assertTrue(retired.ownedItems().containsAll(List.of(queued, prepared, committed)));
        preparedReservation.close();
        committedReservation.close();
        assertTrue(state.committedSnapshot().requests().isEmpty());
        assertTrue(waiting.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retirementReleasesOpenBatchHandoffEvenWhenDrainCallbackFails(boolean callbackFails) {
        AtomicInteger drained = new AtomicInteger();
        var retiring = new EndpointGenerationLifecycle(() -> {
            assertFalse(lock.isHeldByCurrentThread());
            drained.incrementAndGet();
            if (callbackFails) { throw new IllegalStateException("drain callback failed"); }
        });
        RequestRoute request = item(1);
        enqueue(request);
        var lease = state.reserveBatch(request, 9L, 1, retiring.tryAcquireHandoff()).reservation();
        retiring.beginRetirement();
        assertFalse(retiring.tryStartCleanup());

        var retired = state.retireGenerationOwnership();
        assertEquals("retirement reached an OPEN Prefill batch lease", retired.invariantFailure().getMessage());
        assertEquals(List.of(request), retired.ownedItems());
        assertTrue(retired.batchCompletions().isEmpty());
        assertEquals(1, drained.get());
        assertEquals(0, state.captureQueueCounters().batchSlots());
        assertEquals(0, state.observedRequestCount());
        assertTrue(waiting.isEmpty());
        lease.close();
        assertEquals(1, drained.get(), "late lease cleanup cannot release the generation twice");
        var repeated = state.retireGenerationOwnership();
        assertTrue(repeated.ownedItems().isEmpty());
        assertNull(repeated.invariantFailure());
    }

    @Test
    void retirementEmitsOneCompletionForSharedBatchAndRetainsDetachedStopOwner() {
        RequestRoute first = item(1), second = item(2), detached = item(3);
        commitBatch(List.of(first, second), 100L);
        enqueue(detached);
        var routeLease = EndpointTestSupport.reserveRoute(state, detached, 20L);
        assertSame(detached, state.detachNextActiveForStop());

        var retired = state.retireGenerationOwnership();
        assertNull(retired.invariantFailure());
        assertEquals(List.of(first, second, detached), retired.ownedItems());
        assertEquals(1, retired.batchCompletions().size());
        assertEquals(10L, retired.batchCompletions().getFirst().batchId());
        assertFalse(retired.batchCompletions().getFirst().learningEligible());
        assertEquals(0, state.captureQueueCounters().batchSlots());
        assertEquals(0, state.observedRequestCount());
        routeLease.close();
        assertFalse(state.terminalizeCommittedItem(first));
        assertFalse(state.terminalizeCommittedItem(second));
        assertTrue(state.retireGenerationOwnership().batchCompletions().isEmpty());
    }

    @Test
    void cancellationBeforeCommitReleasesAnUnqueuedAdmissionExactlyOnce() {
        RequestRoute immediate = item(1);
        var reservation = state.reserveUnqueuedRoute(immediate, 40L, Long.MAX_VALUE).reservation();
        lock.lock();
        try {
            assertTrue(state.removeQueuedUnderLock(immediate));
            assertFalse(state.removeQueuedUnderLock(immediate));
        } finally {
            lock.unlock();
        }
        reservation.close();
        assertTrue(state.committedSnapshot().requests().isEmpty());
        assertTrue(capture().work().materialize().requests().isEmpty());
    }

    @Test
    void staleRouteCannotPrepareOrRemoveReplacementWithTheSameRequestId() {
        RequestRoute first = item(1), replacement = item(1);
        enqueue(first);
        var firstLease = EndpointTestSupport.reserveRoute(state, first, 10L);
        lock.lock();
        try {
            assertTrue(state.removeQueuedUnderLock(first));
            assertTrue(state.enqueueActiveUnderLock(replacement, 10L));
            var replacementLease = state.reserveRouteUnderLock(replacement, 20L);
            assertThrows(IllegalStateException.class, () -> state.prepareRoute(first, 999L));
            assertFalse(state.removeQueuedUnderLock(first));
            assertSame(replacementLease, state.prepareRoute(replacement, 20L));
        } finally {
            lock.unlock();
        }
        firstLease.close();
        assertEquals(List.of(replacement), waitingItems());
        assertEquals(1L, state.observedRequestCount());
        assertFalse(state.terminalizeCommittedItem(first));
    }

    @Test
    void stopClosesOnlyTheCanonicalRouteLeaseAndNotifiesOutsideTheLock() {
        AtomicInteger notifications = new AtomicInteger();
        var ledger = new PrefillState(lock, waiting, clock::get, () -> {
            assertFalse(lock.isHeldByCurrentThread());
            notifications.incrementAndGet();
        });
        RequestRoute item = item(1);
        lock.lock();
        PrefillState.RouteReservation lease;
        try {
            assertTrue(ledger.enqueueActiveUnderLock(item, 10L));
            lease = ledger.reserveRouteUnderLock(item, 20L);
        } finally {
            lock.unlock();
        }
        assertSame(item, ledger.detachNextActiveForStop());
        assertTrue(waitingItems().isEmpty());
        assertEquals(1L, ledger.observedRequestCount(), "stop callback still owns the retained identity");
        assertEquals(1, notifications.get());
        lease.close();
        assertEquals(1, notifications.get());
        assertThrows(IllegalStateException.class, () -> ledger.prepareRoute(item, 30L));
        lock.lock();
        try {
            assertTrue(ledger.acknowledgeStopTerminalUnderLock(item));
        } finally {
            lock.unlock();
        }
        assertEquals(0L, ledger.observedRequestCount());
    }

    @Test
    void activeRemovalLeavesOpenBatchLeaseWithPreparingTransaction() {
        AtomicInteger released = new AtomicInteger(), drained = new AtomicInteger();
        var ledger = new PrefillState(lock, waiting, clock::get, released::incrementAndGet);
        var retiring = new EndpointGenerationLifecycle(drained::incrementAndGet);
        RequestRoute request = item(1);
        lock.lock();
        try {
            assertTrue(ledger.enqueueActiveUnderLock(request, 10L));
        } finally {
            lock.unlock();
        }
        var lease = ledger.reserveBatch(request, 9L, 1, retiring.tryAcquireHandoff()).reservation();
        retiring.beginRetirement();
        assertFalse(retiring.tryStartCleanup());
        lock.lock();
        try {
            assertTrue(ledger.removeQueuedUnderLock(request));
            assertTrue(waiting.isEmpty());
            assertEquals(0L, ledger.observedRequestCount());
            assertEquals(1, ledger.captureQueueCounters().batchSlots());
            assertEquals(0, drained.get());
        } finally {
            lock.unlock();
        }
        lease.close();
        lease.close();
        assertEquals(1, released.get());
        assertEquals(1, drained.get());
        lock.lock();
        try {
            assertEquals(0, ledger.captureQueueCounters().batchSlots());
        } finally {
            lock.unlock();
        }
    }

    @Test
    void interleavedImmediateCommitsCaptureOtherWorkAtTheActualCommitBoundary() {
        RequestRoute first = item(1), second = item(2);
        try (var firstReservation = state.reserveUnqueuedRoute(first, 30L, Long.MAX_VALUE).reservation();
             var secondReservation = state.reserveUnqueuedRoute(second, 40L, Long.MAX_VALUE).reservation()) {
            try (var secondHandoff = state.commitRouteGroup(List.of(second), List.of(secondReservation),
                    generation.tryAcquireHandoff())) {
                assertEquals(30L, secondHandoff.precedingWork().materialize().totalRemainingWorkMs().orElseThrow());
            }
            try (var firstHandoff = state.commitRouteGroup(List.of(first), List.of(firstReservation),
                    generation.tryAcquireHandoff())) {
                assertEquals(40L, firstHandoff.precedingWork().materialize().totalRemainingWorkMs().orElseThrow(),
                        "the earlier reservation must see work committed before its own commit");
            }
        }
    }

    @Test
    void batchAndIndividualCommitsCaptureTheSamePrecedingTimeline() {
        RequestRoute immediate = item(1), batchMember = item(2), later = item(3);
        try (var immediateReservation = state.reserveUnqueuedRoute(immediate, 30L, Long.MAX_VALUE).reservation();
             var firstHandoff = state.commitRouteGroup(List.of(immediate), List.of(immediateReservation),
                     generation.tryAcquireHandoff())) {
            assertEquals(0L, firstHandoff.precedingWork().materialize().totalRemainingWorkMs().orElseThrow());
            enqueue(batchMember);
            try (var batchReservation = state.reserveBatch(batchMember, 10L, 2, generation.tryAcquireHandoff()).reservation();
                 var batchHandoff = batchReservation.commit(List.of(batchMember), 40L)) {
                assertEquals(30L, batchHandoff.precedingWork().materialize().totalRemainingWorkMs().orElseThrow());
            }
            try (var laterReservation = state.reserveUnqueuedRoute(later, 50L, Long.MAX_VALUE).reservation();
                 var laterHandoff = state.commitRouteGroup(List.of(later), List.of(laterReservation),
                         generation.tryAcquireHandoff())) {
                assertEquals(70L, laterHandoff.precedingWork().materialize().totalRemainingWorkMs().orElseThrow());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void batchHandoffFreezesPrecedingWorkAcrossClockAdvanceAndTerminalCleanup(boolean cached) {
        RequestRoute preceding = item(81), incoming = item(82);
        commitBatch(List.of(preceding), 300L);
        reconcile(Map.of(), Map.of("81", task(81, TaskPhase.RUNNING, 0, 0)), unused -> {
            throw new AssertionError("an unchanged batch needs no prediction");
        });
        PrefillState.WorkCapture earlier = cached ? capture().work() : null;
        clock.set(150L);
        enqueue(incoming);
        try (var reservation = state.reserveBatch(incoming, 82L, 2, generation.tryAcquireHandoff()).reservation();
             var handoff = reservation.commit(List.of(incoming), 50L)) {
            if (cached) { assertSame(earlier, handoff.precedingWork()); }
            assertTrue(state.terminalizeCommittedItem(preceding));
            assertTrue(state.terminalizeCommittedItem(incoming));
            clock.set(190L);
            WorkSnapshot frozen = handoff.precedingWork().materialize();
            assertEquals(List.of(81L), frozen.batches().getFirst().requestIds());
            assertEquals(250L, frozen.knownRemainingWorkMsAt(150L));
            assertEquals(210L, frozen.knownRemainingWorkMsAt(190L));
            assertFalse(frozen.containsRequest(82L), "the incoming batch is not preceding work");
            assertTrue(state.committedSnapshot().batches().isEmpty());
        }
    }

    @Test
    void localCleanupPreservesRunningBatchWorkAndItsClock() {
        RequestRoute a = item(1), b = item(2), c = item(3);
        commitBatch(List.of(a, b, c), 300L);
        reconcile(Map.of(), Map.of("1", task(1, TaskPhase.RUNNING, 0, 0),
                "2", task(2, TaskPhase.RUNNING, 0, 0),
                "3", task(3, TaskPhase.RUNNING, 0, 0)), unused -> {
                    throw new AssertionError("an unchanged batch needs no prediction");
                });

        clock.set(150);
        assertTrue(state.terminalizeCommittedItem(a));
        assertEquals(List.of(2L, 3L), state.committedSnapshot().batches().getFirst().requestIds());
        assertEquals(250L, remainingWork());
        clock.set(190);
        assertTrue(state.terminalizeCommittedItem(b));
        assertFalse(state.terminalizeCommittedItem(a));
        assertFalse(state.terminalizeCommittedItem(item(3)), "cleanup must match the exact item");
        assertEquals(210L, remainingWork());

        // Returning to QUEUED pauses elapsed-time accounting, but does not undo work already started.
        clock.set(230);
        reconcile(Map.of("1", task(1, null, 0, 100)),
                Map.of("3", task(3, TaskPhase.RECEIVED, 0, 0)), unused -> {
                    throw new AssertionError("local cancellation cannot shrink running work");
                });
        clock.set(250);
        assertEquals(170L, remainingWork());
        assertEquals(WorkSnapshot.Phase.ENGINE_QUEUED,
                state.committedSnapshot().batches().getFirst().phase());

        reconcile(Map.of("3", task(3, null, 0, 300)), Map.of(), unused -> {
            throw new AssertionError("a completed batch needs no prediction");
        });
        assertTrue(state.committedSnapshot().batches().isEmpty());
        assertEquals(0L, remainingWork());
    }

    @ParameterizedTest
    @CsvSource({"true,false,500,0", "false,true,500,0",
            "false,false,0,40", "false,false,500,40"})
    void executionEvidencePreservesBatchPrediction(
            boolean previouslyRunning, boolean currentlyRunning, long errorCode, long executionMs) {
        commitBatch(List.of(item(1), item(2), item(3)), 300L);
        if (previouslyRunning) {
            reconcile(Map.of(), Map.of("2", task(2, TaskPhase.RUNNING, 0, 0)), unused -> {
                throw new AssertionError("an unchanged batch needs no prediction");
            });
        }
        clock.set(150);
        Map<String, TaskInfo> active = currentlyRunning
                ? Map.of("2", task(2, TaskPhase.RUNNING, 0, 0)) : Map.of();
        reconcile(Map.of("1", task(1, null, errorCode, executionMs)), active, unused -> {
            throw new AssertionError("started Prefill must retain its batch prediction");
        });
        long remaining = previouslyRunning ? 250L : 300L;
        assertEquals(remaining, remainingWork());
        clock.set(190);
        long afterRunning = previouslyRunning || currentlyRunning ? remaining - 40L : remaining;
        assertEquals(afterRunning, remainingWork());
        assertFalse(state.committedSnapshot().hasUnknownWork());

        reconcile(Map.of("2", task(2, null, 500, 0)),
                Map.of("3", task(3, TaskPhase.RECEIVED, 0, 0)), unused -> {
                    throw new AssertionError("a queued observation cannot make started work repackable");
                });
        clock.set(230);
        assertEquals(afterRunning, remainingWork());
        reconcile(Map.of("3", task(3, null, 0, 300)), Map.of(), unused -> {
            throw new AssertionError("a completed batch needs no prediction");
        });
        assertTrue(state.committedSnapshot().batches().isEmpty());
    }

    @Test
    void onlyWorkerRemovalBeforeExecutionRepredictsTheQueuedBatch() {
        RequestRoute a = item(1), b = item(2);
        commitBatch(List.of(a, b), 300L);
        clock.set(1_000);
        reconcile(Map.of("1", task(1, null, 500, 0)),
                Map.of("2", task(2, TaskPhase.RECEIVED, 0, 0)), survivors -> {
                    assertEquals(List.of(b), survivors);
                    return 200L;
                });
        clock.set(1_200);
        assertEquals(200L, remainingWork(), "unstarted work does not age while queued");
        reconcile(Map.of(), Map.of("2", task(2, TaskPhase.RUNNING, 0, 0)), unused -> {
            throw new AssertionError("starting the batch uses its existing prediction");
        });
        clock.set(1_250);
        assertEquals(150L, remainingWork());
    }

    @Test
    void predictionFailureLeavesBatchAndWaitingOwnershipUnchanged() {
        RequestRoute a = item(1), b = item(2), queued = item(3);
        commitBatch(List.of(a, b), 300L);
        enqueue(queued);
        var before = capture();
        long version = state.mutationVersion();
        var failure = new IllegalStateException("prediction unavailable");
        var failedReductions = new AtomicInteger();
        var publications = new AtomicInteger();
        WorkerStatusResponse response = new WorkerStatusResponse();
        response.setRole(RoleType.PREFILL);
        response.setFinishedTaskInfo(Map.of("1", task(1, null, 500, 0)));
        response.setRunningTaskInfo(Map.of("2", task(2, TaskPhase.RECEIVED, 0, 0)));
        var observation = EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.1", 8080, 8090)
                .freezeStatusResponse(response);

        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> state.reconcileWorkerStatus(observation, survivors -> {
                    assertEquals(List.of(b), survivors);
                    throw failure;
                }, publications::incrementAndGet, failedReductions::incrementAndGet)));

        assertEquals(1, failedReductions.get());
        assertEquals(0, publications.get());
        assertEquals(version, state.mutationVersion());
        assertSame(before.work(), capture().work());
        assertEquals(List.of(1L, 2L), state.committedSnapshot().batches().getFirst().requestIds());
        assertEquals(300L, remainingWork());
        assertEquals(2, state.stats().locallyOwnedRequests());
        assertEquals(List.of(queued), waitingItems());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void terminalBatchAndUnchangedBatchRetainTheirOwnFactsAcrossPublication(boolean publicationFails) {
        RequestRoute a = item(1), b = item(2), c = item(3), d = item(4);
        commitBatch(List.of(a, b), 300L);
        enqueue(c);
        enqueue(d);
        try (var reservation = state.reserveBatch(c, 11L, 2,
                generation.tryAcquireHandoff()).reservation();
             var handoff = reservation.commit(List.of(c, d), 400L)) {
            assertTrue(waitingItems().isEmpty());
        }
        clock.set(200L);
        TaskInfo activeC = task(3L, TaskPhase.RUNNING, 0L, 0L);
        TaskInfo activeD = task(4L, TaskPhase.RUNNING, 0L, 0L);
        activeC.setBatchId(11L);
        activeD.setBatchId(11L);
        WorkerStatusResponse response = new WorkerStatusResponse();
        response.setRole(RoleType.PREFILL);
        response.setFinishedTaskInfo(Map.of("1", task(1, null, 0L, 300L),
                "2", task(2, null, 0L, 300L)));
        response.setRunningTaskInfo(Map.of("3", activeC, "4", activeD));
        var observation = EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.1", 8080, 8090)
                .freezeStatusResponse(response);
        var failure = new IllegalStateException("status publication failed");
        var failedReductions = new AtomicInteger();
        var outcome = state.reconcileWorkerStatus(observation, unused -> {
            throw new AssertionError("completed and unchanged batches need no new prediction");
        }, () -> {
            if (publicationFails) { throw failure; }
        }, () -> {
            failedReductions.incrementAndGet();
            throw new AssertionError("secondary retirement failure");
        });

        assertSame(publicationFails ? failure : null, outcome.publicationFailure());
        assertEquals(publicationFails ? 1 : 0, failedReductions.get());
        assertEquals(4, outcome.schedulerFacts().size());
        assertEquals(List.of(1L, 2L), outcome.schedulerFacts().stream()
                .filter(fact -> fact.kind() == PrefillState.WorkerStatusFact.Kind.COMPLETED)
                .map(fact -> fact.item().requestId()).sorted().toList());
        assertEquals(1, outcome.batchCompletions().size());
        var completion = outcome.batchCompletions().getFirst();
        assertEquals(10L, completion.batchId());
        assertEquals(300L, completion.actualWorkMs());
        assertTrue(completion.successfulCompletion());
        assertTrue(completion.learningEligible());
        assertEquals(1, state.stats().batchCount());
        assertEquals(2, state.stats().locallyOwnedRequests());
        clock.set(250L);
        assertEquals(350L, remainingWork(), "the batch without terminal events still advances to RUNNING");
        assertEquals(List.of(3L, 4L), state.committedSnapshot().batches().getFirst().requestIds());
    }

    @Test
    void partialSuccessThenFailureCompletesBatchOnceWithoutLearningOrRepacking() {
        commitBatch(List.of(item(1), item(2)), 400L);
        ToLongFunction<List<RequestRoute>> noRepacking = survivors -> {
            throw new AssertionError("execution evidence must prevent repacking");
        };
        var partial = reconcile(Map.of("1", task(1, null, 0L, 300L)),
                Map.of("2", task(2, TaskPhase.RUNNING, 0L, 0L)), noRepacking);
        assertTrue(partial.batchCompletions().isEmpty());
        assertEquals(1, state.stats().batchCount());
        assertEquals(1, state.stats().locallyOwnedRequests());

        var failed = Map.of("2", task(2, null, 1L, 200L));
        var settled = reconcile(failed, Map.of(), noRepacking);
        assertEquals(1, settled.batchCompletions().size());
        var completion = settled.batchCompletions().getFirst();
        assertEquals(10L, completion.batchId());
        assertEquals(300L, completion.actualWorkMs());
        assertTrue(completion.successfulCompletion());
        assertFalse(completion.learningEligible());
        assertEquals(0, state.stats().batchCount());
        assertEquals(0, state.stats().locallyOwnedRequests());
        assertTrue(reconcile(failed, Map.of(), noRepacking).batchCompletions().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void oldBatchCompletionDoesNotSettleAReusedMemberId(boolean workerTerminal) {
        RequestRoute first = item(1), sibling = item(2);
        commitBatch(List.of(first, sibling), 300L);
        assertTrue(state.terminalizeCommittedItem(first));

        RequestRoute replacement = item(1);
        enqueue(replacement);
        try (var lease = state.reserveBatch(replacement, 11L, 2,
                generation.tryAcquireHandoff()).reservation();
             var handoff = lease.commit(List.of(replacement), 100L)) {
            assertEquals(2, state.captureQueueCounters().batchSlots());
        }
        ToLongFunction<List<RequestRoute>> noRepacking = unused -> {
            throw new AssertionError("completed batches do not need reprediction");
        };
        if (workerTerminal) {
            var completion = reconcile(Map.of("2", task(2, null, 0L, 300L)), Map.of(), noRepacking);
            assertEquals(List.of(10L), completion.batchCompletions().stream()
                    .map(PrefillState.BatchCompletion::batchId).toList());
        } else {
            assertTrue(state.terminalizeCommittedItem(sibling));
        }
        assertEquals(1, state.captureQueueCounters().batchSlots());
        assertEquals(1, state.stats().locallyOwnedRequests());
        assertEquals(11L, state.committedSnapshot().batches().getFirst().batchId());
        assertEquals(List.of(1L), state.committedSnapshot().batches().getFirst().requestIds());
        assertFalse(state.terminalizeCommittedItem(first), "old item cannot settle the replacement");
        var stale = reconcile(Map.of("1", task(1, null, 0L, 300L)), Map.of(), noRepacking);
        assertTrue(stale.batchCompletions().isEmpty(), "old batch proof cannot settle the new batch");
        assertTrue(stale.schedulerFacts().isEmpty());
        assertEquals(1, state.captureQueueCounters().batchSlots());

        TaskInfo terminal = task(1, null, 0L, 100L);
        terminal.setBatchId(11L);
        var completion = reconcile(Map.of("1", terminal), Map.of(), noRepacking);
        assertEquals(List.of(11L), completion.batchCompletions().stream()
                .map(PrefillState.BatchCompletion::batchId).toList());
        assertEquals(0, state.captureQueueCounters().batchSlots());
        assertEquals(0, state.stats().locallyOwnedRequests());
    }

    private void commitBatch(List<RequestRoute> members, long predictedMs) {
        members.forEach(this::enqueue);
        try (var reservation = state.reserveBatch(members.getFirst(), 10L, 2,
                generation.tryAcquireHandoff()).reservation();
             var handoff = reservation.commit(members, predictedMs)) {
            assertTrue(waitingItems().isEmpty());
        }
    }

    private PrefillState.StatusReconciliation reconcile(Map<String, TaskInfo> finished, Map<String, TaskInfo> active,
                           ToLongFunction<List<RequestRoute>> repredictor) {
        WorkerStatusResponse response = new WorkerStatusResponse();
        response.setRole(RoleType.PREFILL);
        response.setFinishedTaskInfo(finished);
        response.setRunningTaskInfo(active);
        var observation = EndpointTestSupport.workerStatus(RoleType.PREFILL, "127.0.0.1", 8080, 8090)
                .freezeStatusResponse(response);
        var outcome = state.reconcileWorkerStatus(observation,
                repredictor, () -> { }, () -> { });
        assertNull(outcome.publicationFailure());
        return outcome;
    }

    private long remainingWork() {
        return state.committedSnapshot().totalRemainingWorkMsAt(clock.get()).orElseThrow();
    }

    private static TaskInfo task(long requestId, TaskPhase phase, long errorCode, long executionMs) {
        TaskInfo task = new TaskInfo();
        task.setRequestId(requestId);
        task.setBatchId(10L);
        task.setPhase(phase);
        task.setErrorCode(errorCode);
        task.setExecutionTimeMs(executionMs);
        return task;
    }

    private static RequestRoute item(long id) {
        var item = mock(RequestRoute.class);
        when(item.requestId()).thenReturn(id);
        when(item.seqLen()).thenReturn(100L);
        return item;
    }

    private PrefillState.Snapshot capture() {
        lock.lock();
        try {
            return state.snapshotUnderLock();
        } finally {
            lock.unlock();
        }
    }

    private void enqueue(RequestRoute item) {
        lock.lock();
        try {
            assertTrue(state.enqueueActiveUnderLock(item, 0L));
        } finally {
            lock.unlock();
        }
    }

    private List<RequestRoute> waitingItems() {
        return state.captureQueue(Integer.MAX_VALUE).items();
    }
}
