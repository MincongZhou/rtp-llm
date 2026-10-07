package org.flexlb.balance.eviction;

import org.flexlb.balance.endpoint.DecodeResources;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.DecodeResources.DecodeRequestView;
import org.flexlb.balance.prediction.DecodeCostFormula;
import org.flexlb.balance.preemption.CancelTarget;
import org.flexlb.balance.preemption.PreemptionCancelPhase;
import org.flexlb.balance.preemption.VictimTerminal;
import org.flexlb.balance.scheduler.CancelReason;
import org.flexlb.balance.scheduler.PreemptionRegistration;
import org.flexlb.balance.scheduler.AbstractRequestScheduler;
import org.flexlb.balance.scheduler.RequestRequirements;
import org.flexlb.balance.scheduler.RequestRequirements.DecodeMode;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.enums.DecodeTaskPhase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DecodePreemptionCoordinatorTest {

    private static RequestRequirements incoming(long maxRequests) {
        return new RequestRequirements(20L, 70, 64L,
                new DecodeResources.AdmissionCapacity(maxRequests, 100L),
                DecodeMode.PREEMPT_AT_PLACEMENT, mock(DecodeCostFormula.class), 64L, null, List.of(), 0L, true, 0);
    }

    @Test
    void commitsOnlyAfterEveryExactVictimIsTerminal() throws Exception {
        Fixture fixture = fixture();
        AbstractRequestScheduler requests = fixture.requests();
        DecodeEndpoint endpoint = fixture.endpoint();

        CompletableFuture<VictimTerminal> firstTerminal = new CompletableFuture<>();
        CompletableFuture<VictimTerminal> secondTerminal = new CompletableFuture<>();
        PreemptionRegistration first = claim(fixture.requests(), 11L, firstTerminal);
        PreemptionRegistration second = claim(fixture.requests(), 12L, secondTerminal);
        when(requests.tryClaim(any(DecodeResources.ReservationHandle.class), anyLong(), any()))
                .thenAnswer(invocation -> Optional.of(
                        invocation.<DecodeResources.ReservationHandle>getArgument(0).requestId() == 11L ? first : second));

        EngineCancelChannel cancelChannel = mock(EngineCancelChannel.class);
        when(cancelChannel.cancel(any(), anyLong(), org.mockito.ArgumentMatchers.eq(CancelReason.PRIORITY_PREEMPTED), anyLong())).thenReturn(
                CompletableFuture.completedFuture(
                        EngineCancelChannel.CancelAck.ACCEPTED));
        DecodePreemptionCoordinator coordinator =
                new DecodePreemptionCoordinator(cancelChannel, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(requests));
        CompletableFuture<DecodePreemptionCoordinator.PreemptionResult> result =
                coordinator.preempt(new DecodePreemptionCoordinator.PreemptionCommand(
                        endpoint, incoming(2L),
                        List.of(victim(11L, 101L), victim(12L, 102L)),
                        1_000L, 1_000L, () -> true, "test"));

        assertFalse(result.isDone());
        firstTerminal.complete(new VictimTerminal(11L));
        assertFalse(result.isDone(), "one terminal cannot release two victims");
        secondTerminal.complete(new VictimTerminal(12L));

        assertTrue(result.get(1, TimeUnit.SECONDS).committed());
        assertEquals(new DecodeResources.ReservationHandle(9L, 20L, 100L), result.join().reservation());
        verify(cancelChannel).cancel(eq(new CancelTarget("10.0.0.1", 9090)), eq(11L), org.mockito.ArgumentMatchers.eq(CancelReason.PRIORITY_PREEMPTED), anyLong());
        verify(cancelChannel).cancel(eq(new CancelTarget("10.0.0.1", 9090)), eq(12L), org.mockito.ArgumentMatchers.eq(CancelReason.PRIORITY_PREEMPTED), anyLong());
        verify(endpoint).commitPreemption(1L);
        verify(endpoint, never()).abortPreemption(anyLong());
    }

    @ParameterizedTest
    @EnumSource(value = EngineCancelChannel.CancelAck.class, names = {"ACCEPTED", "FAILED", "REQUEST_FENCED"})
    void timeoutBeginsAfterAckAndLateTerminalCannotReopenIncoming(
            EngineCancelChannel.CancelAck acknowledgement) throws Exception {
        Fixture fixture = fixture();
        CompletableFuture<VictimTerminal> terminal = new CompletableFuture<>();
        PreemptionRegistration victimClaim = claim(fixture.requests(), 11L, terminal);
        when(fixture.requests().tryClaim(any(DecodeResources.ReservationHandle.class), anyLong(), any()))
                .thenReturn(Optional.of(victimClaim));
        EngineCancelChannel channel = mock(EngineCancelChannel.class);
        CompletableFuture<EngineCancelChannel.CancelAck> ack = new CompletableFuture<>();
        when(channel.cancel(any(), anyLong(), org.mockito.ArgumentMatchers.eq(CancelReason.PRIORITY_PREEMPTED), anyLong())).thenReturn(ack);
        DecodePreemptionCoordinator coordinator =
                new DecodePreemptionCoordinator(channel, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(fixture.requests()));
        CompletableFuture<DecodePreemptionCoordinator.PreemptionResult> outcome =
                coordinator.preempt(new DecodePreemptionCoordinator.PreemptionCommand(
                        fixture.endpoint(), incoming(1L),
                        List.of(victim(11L, 101L)),
                        50L, 20L, () -> true, "test"));

        // Transport owns the ACK deadline. The terminal-wait budget starts
        // only when that phase resolves, even if the transport outcome is unknown.
        assertThrows(TimeoutException.class, () -> outcome.get(60L, TimeUnit.MILLISECONDS));
        ack.complete(acknowledgement);
        var timedOut = outcome.get(1L, TimeUnit.SECONDS);
        assertFalse(timedOut.committed());
        assertTrue(timedOut.controlFailure());
        assertEquals("cancel_terminal_unknown", timedOut.detail());
        verify(fixture.endpoint()).abortPreemption(1L);
        verify(fixture.requests(), never()).releasePreemption(victimClaim);
        assertFalse(terminal.isDone(), "timing out admission must retain the victim terminal observation");

        terminal.complete(new VictimTerminal(11L));
        assertSame(timedOut, outcome.join());
        verify(fixture.endpoint(), never()).commitPreemption(anyLong());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void partialTerminalTimeoutRetainsUnknownVictimAndNeverReopensIncoming(
            boolean terminalBeforeSend) throws Exception {
        Fixture fixture = fixture();
        CompletableFuture<VictimTerminal> firstTerminal = new CompletableFuture<>();
        CompletableFuture<VictimTerminal> secondTerminal = new CompletableFuture<>();
        PreemptionRegistration first = claim(fixture.requests(), 11L, firstTerminal);
        PreemptionRegistration second = claim(fixture.requests(), 12L, secondTerminal);
        when(fixture.requests().tryClaim(any(DecodeResources.ReservationHandle.class), anyLong(), any()))
                .thenAnswer(invocation -> Optional.of(
                        invocation.<DecodeResources.ReservationHandle>getArgument(0).requestId() == 11L ? first : second));
        EngineCancelChannel channel = mock(EngineCancelChannel.class);
        when(channel.cancel(any(), anyLong(), org.mockito.ArgumentMatchers.eq(CancelReason.PRIORITY_PREEMPTED), anyLong())).thenAnswer(invocation ->
                CompletableFuture.completedFuture(invocation.<Long>getArgument(1) == 11L
                        ? EngineCancelChannel.CancelAck.ACCEPTED : EngineCancelChannel.CancelAck.FAILED));
        DecodePreemptionCoordinator coordinator = new DecodePreemptionCoordinator(channel, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(fixture.requests()));
        if (terminalBeforeSend) {
            firstTerminal.complete(new VictimTerminal(11L));
        }
        var outcome = coordinator.preempt(new DecodePreemptionCoordinator.PreemptionCommand(
                fixture.endpoint(), incoming(2L),
                List.of(victim(11L, 101L), victim(12L, 102L)),
                50L, 1_000L, () -> true, "test"));
        if (!terminalBeforeSend) {
            verify(channel).cancel(any(), eq(11L), org.mockito.ArgumentMatchers.eq(CancelReason.PRIORITY_PREEMPTED), anyLong());
            firstTerminal.complete(new VictimTerminal(11L));
            assertFalse(outcome.isDone(), "one of two terminal observations cannot finish the aggregate");
        } else {
            verify(channel, never()).cancel(any(), eq(11L), org.mockito.ArgumentMatchers.eq(CancelReason.PRIORITY_PREEMPTED), anyLong());
        }
        var timedOut = outcome.get(3L, TimeUnit.SECONDS);
        assertFalse(timedOut.committed());
        assertTrue(timedOut.controlFailure());
        verify(channel).cancel(any(), eq(12L), org.mockito.ArgumentMatchers.eq(CancelReason.PRIORITY_PREEMPTED), anyLong());
        verify(fixture.requests(), never()).releasePreemption(first);
        verify(fixture.requests(), never()).releasePreemption(second);
        verify(fixture.requests()).updatePreemption(eq(second), eq(org.flexlb.balance.preemption.PreemptionCancelPhase.CANCEL_UNKNOWN));
        assertFalse(secondTerminal.isDone());
        verify(fixture.endpoint()).abortPreemption(1L);
        secondTerminal.complete(new VictimTerminal(12L));
        assertSame(timedOut, outcome.join());
        verify(fixture.endpoint(), never()).commitPreemption(anyLong());
    }

    @ParameterizedTest
    @EnumSource(value = EngineCancelChannel.CancelAck.class,
            names = {"FAILED", "NOT_FOUND", "REQUEST_FENCED", "REQUEST_CLEANED"})
    void reversedAcknowledgementsStayBoundToTheirVictims(
            EngineCancelChannel.CancelAck secondReply) throws Exception {
        Fixture fixture = fixture();
        CompletableFuture<VictimTerminal> firstTerminal = new CompletableFuture<>();
        CompletableFuture<VictimTerminal> secondTerminal = new CompletableFuture<>();
        PreemptionRegistration first = claim(fixture.requests(), 11L, firstTerminal);
        PreemptionRegistration second = claim(fixture.requests(), 12L, secondTerminal);
        when(fixture.requests().tryClaim(any(DecodeResources.ReservationHandle.class), anyLong(), any()))
                .thenAnswer(invocation -> Optional.of(
                        invocation.<DecodeResources.ReservationHandle>getArgument(0).requestId() == 11L ? first : second));
        when(fixture.requests().completePreemption(eq(second), any())).thenReturn(true);
        CompletableFuture<EngineCancelChannel.CancelAck> firstAck = new CompletableFuture<>();
        CompletableFuture<EngineCancelChannel.CancelAck> secondAck = new CompletableFuture<>();
        EngineCancelChannel channel = mock(EngineCancelChannel.class);
        when(channel.cancel(any(), anyLong(), org.mockito.ArgumentMatchers.eq(CancelReason.PRIORITY_PREEMPTED), anyLong())).thenAnswer(invocation ->
                invocation.<Long>getArgument(1) == 11L ? firstAck : secondAck);
        var coordinator = new DecodePreemptionCoordinator(channel, org.flexlb.balance.scheduler.SchedulerTestSupport.repository(fixture.requests()));
        var result = coordinator.preempt(new DecodePreemptionCoordinator.PreemptionCommand(
                fixture.endpoint(), incoming(2L),
                List.of(victim(11L, 101L), victim(12L, 102L)),
                1_000L, 1_000L, () -> true, "test"));

        secondAck.complete(secondReply);
        assertFalse(result.isDone());
        verify(fixture.requests(), never()).updatePreemption(first, PreemptionCancelPhase.CANCEL_REQUESTED);
        verify(fixture.requests(), never()).completePreemption(eq(second), any());
        firstAck.complete(EngineCancelChannel.CancelAck.ACCEPTED);
        verify(fixture.requests()).updatePreemption(first, PreemptionCancelPhase.CANCEL_REQUESTED);
        switch (secondReply) {
            case FAILED -> verify(fixture.requests()).updatePreemption(second, PreemptionCancelPhase.CANCEL_UNKNOWN);
            case NOT_FOUND -> verify(fixture.requests()).updatePreemption(second, PreemptionCancelPhase.NOT_FOUND_STALE);
            case REQUEST_FENCED -> verify(fixture.requests()).updatePreemption(second, PreemptionCancelPhase.CANCEL_REQUESTED);
            case REQUEST_CLEANED -> {
                verify(fixture.endpoint()).updatePreemption(1L,
                        DecodeResources.PreemptionUpdate.fenced(
                                new DecodeResources.ReservationHandle(9L, 12L, 102L)));
                verify(fixture.requests()).completePreemption(second, "test");
            }
            default -> throw new AssertionError("unexpected test reply");
        }
        assertFalse(result.isDone(), "ACKs cannot replace the first victim's terminal proof");
        if (secondReply != EngineCancelChannel.CancelAck.REQUEST_CLEANED) {
            secondTerminal.complete(new VictimTerminal(12L));
        }
        firstTerminal.complete(new VictimTerminal(11L));
        assertTrue(result.get(1L, TimeUnit.SECONDS).committed());
        verify(fixture.endpoint()).commitPreemption(1L);
        verify(fixture.endpoint(), never()).abortPreemption(anyLong());
    }

    @ParameterizedTest
    @EnumSource(value = EngineCancelChannel.CancelAck.class,
            names = {"REQUEST_FENCED", "REQUEST_CLEANED"})
    void onlyDownstreamCleanupProofCanReplaceDecodeTerminal(EngineCancelChannel.CancelAck reply) throws Exception {
        Fixture fixture = fixture();
        CompletableFuture<VictimTerminal> terminal = new CompletableFuture<>();
        PreemptionRegistration claim = claim(fixture.requests(), 11L, terminal);
        when(fixture.requests().tryClaim(any(DecodeResources.ReservationHandle.class), anyLong(), any()))
                .thenReturn(Optional.of(claim));
        when(fixture.requests().completePreemption(eq(claim), any())).thenReturn(true);
        EngineCancelChannel channel = mock(EngineCancelChannel.class);
        when(channel.cancel(any(), anyLong(), eq(CancelReason.PRIORITY_PREEMPTED), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(reply));
        var coordinator = new DecodePreemptionCoordinator(channel,
                org.flexlb.balance.scheduler.SchedulerTestSupport.repository(fixture.requests()));
        var outcome = coordinator.preempt(new DecodePreemptionCoordinator.PreemptionCommand(
                fixture.endpoint(), incoming(1L), List.of(victim(11L, 101L)),
                1_000L, 1_000L, () -> true, "test"));
        if (reply == EngineCancelChannel.CancelAck.REQUEST_FENCED) {
            assertFalse(outcome.isDone(), "Prefill fence alone does not prove Decode resource release");
            verify(fixture.requests(), never()).completePreemption(eq(claim), any());
            verify(fixture.endpoint(), never()).updatePreemption(1L,
                    DecodeResources.PreemptionUpdate.fenced(new DecodeResources.ReservationHandle(9L, 11L, 101L)));
            verify(fixture.endpoint(), never()).commitPreemption(anyLong());
            terminal.complete(new VictimTerminal(11L));
        }
        assertTrue(outcome.get(1L, TimeUnit.SECONDS).committed());
        verify(fixture.endpoint()).commitPreemption(1L);
    }

    @Test
    void laterVictimClaimFailureReleasesEarlierClaimBeforeAnyCancel() throws Exception {
        Fixture fixture = fixture();
        PreemptionRegistration first = claim(fixture.requests(), 11L, new CompletableFuture<>());
        when(fixture.requests().tryClaim(any(DecodeResources.ReservationHandle.class), anyLong(), any()))
                .thenReturn(Optional.of(first), Optional.empty());
        EngineCancelChannel channel = mock(EngineCancelChannel.class);
        try (var coordinator = new DecodePreemptionCoordinator(channel,
                org.flexlb.balance.scheduler.SchedulerTestSupport.repository(fixture.requests()))) {
            var result = coordinator.preempt(new DecodePreemptionCoordinator.PreemptionCommand(
                    fixture.endpoint(), incoming(2L), List.of(victim(11L, 101L), victim(12L, 102L)),
                    1000L, 1000L, () -> true, "test")).get(1, TimeUnit.SECONDS);
            assertFalse(result.committed());
            verify(fixture.requests()).releasePreemption(first);
            verify(fixture.endpoint(), never()).beginPreemption(anyLong(), anyList(), anyLong(),
                    anyLong(), anyLong(), anyInt(), any());
            org.mockito.Mockito.verifyNoInteractions(channel);
        }
    }

    @Test
    void invalidCancelTargetReleasesEarlierClaimAndReportsControlFailureWithoutRpc() throws Exception {
        Fixture fixture = fixture();
        PreemptionRegistration first = claim(fixture.requests(), 11L, new CompletableFuture<>());
        when(fixture.requests().tryClaim(any(DecodeResources.ReservationHandle.class), anyLong(), any()))
                .thenReturn(Optional.of(first))
                .thenThrow(new IllegalStateException("Priority victim has no routable Cancel target"));
        EngineCancelChannel channel = mock(EngineCancelChannel.class);
        try (var coordinator = new DecodePreemptionCoordinator(channel,
                org.flexlb.balance.scheduler.SchedulerTestSupport.repository(fixture.requests()))) {
            var result = coordinator.preempt(new DecodePreemptionCoordinator.PreemptionCommand(
                    fixture.endpoint(), incoming(2L), List.of(victim(11L, 101L), victim(12L, 102L)),
                    1000L, 1000L, () -> true, "test")).get(1, TimeUnit.SECONDS);
            assertFalse(result.committed());
            assertTrue(result.controlFailure());
            verify(fixture.requests()).releasePreemption(first);
            verify(fixture.endpoint(), never()).beginPreemption(anyLong(), anyList(), anyLong(),
                    anyLong(), anyLong(), anyInt(), any());
            org.mockito.Mockito.verifyNoInteractions(channel);
        }
    }

    @Test
    void staleEndpointGenerationCannotCancelANewerRouteWithTheSameRequestAndToken() throws Exception {
        Fixture fixture = fixture();
        var stale = new DecodeResources.ReservationHandle(9L, 11L, 101L);
        when(fixture.requests().tryClaim(eq(stale), anyLong(), any())).thenReturn(Optional.empty());
        EngineCancelChannel channel = mock(EngineCancelChannel.class);
        try (var coordinator = new DecodePreemptionCoordinator(channel,
                org.flexlb.balance.scheduler.SchedulerTestSupport.repository(fixture.requests()))) {
            var result = coordinator.preempt(new DecodePreemptionCoordinator.PreemptionCommand(
                    fixture.endpoint(), incoming(1L), List.of(victim(11L, 101L)),
                    1000L, 1000L, () -> true, "old endpoint")).get(1, TimeUnit.SECONDS);
            assertFalse(result.committed());
            assertFalse(result.controlFailure());
            verify(fixture.requests()).tryClaim(eq(stale), anyLong(), eq("old endpoint"));
            verify(fixture.endpoint(), never()).beginPreemption(anyLong(), anyList(), anyLong(),
                    anyLong(), anyLong(), anyInt(), any());
            org.mockito.Mockito.verifyNoInteractions(channel);
        }
    }

    private static Fixture fixture() {
        AbstractRequestScheduler requests = mock(AbstractRequestScheduler.class);
        when(requests.updatePreemption(any(), any())).thenReturn(true);
        DecodeEndpoint endpoint = mock(DecodeEndpoint.class);
        WorkerStatus status = mock(WorkerStatus.class);
        when(endpoint.getStatus()).thenReturn(status);
        when(status.getGenerationId()).thenReturn(9L);
        when(endpoint.beginPreemption(
                anyLong(),
                anyList(),
                anyLong(),
                anyLong(),
                anyLong(),
                anyInt(),
                any(DecodeResources.AdmissionCapacity.class)))
                .thenReturn(DecodeResources.PreemptionBeginResult.SUCCESS);
        when(endpoint.updatePreemption(anyLong(), any(DecodeResources.PreemptionUpdate.class))).thenReturn(true);
        when(endpoint.commitPreemption(anyLong()))
                .thenReturn(new DecodeResources.ReservationHandle(9L, 20L, 100L));
        return new Fixture(requests, endpoint);
    }

    private record Fixture(AbstractRequestScheduler requests, DecodeEndpoint endpoint) { }

    private static PreemptionRegistration claim(
            AbstractRequestScheduler owner, long requestId,
            CompletableFuture<VictimTerminal> terminal) {
        PreemptionRegistration claim = mock(PreemptionRegistration.class);
        when(claim.cancelTarget()).thenReturn(new CancelTarget("10.0.0.1", 9090));
        when(claim.requestId()).thenReturn(requestId);
        when(claim.scheduler()).thenReturn(owner);
        when(claim.attemptToken()).thenReturn(1L);
        when(claim.terminalObservation()).thenReturn(terminal);
        return claim;
    }

    private static DecodeRequestView victim(long requestId, long reservationToken) {
        return new DecodeRequestView(
                requestId, 30, 64L, 64L,
                DecodeTaskPhase.ACCEPTED_NOT_RUNNING,
                true, reservationToken, false);
    }
}
