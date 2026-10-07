package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeResources;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.strategy.SelectedRole;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.util.Failures;
import java.util.List;
import java.util.Objects;


/**
 * Selected workers and resources owned by one routing scope.
 * Closing releases generation pins and rolls back untransferred Decode capacity.
 * Successful publication transfers capacity to RequestRoute, but retains pins until close.
 */
public final class ProvisionalRoute implements AutoCloseable {
    private final Response response;
    private final SelectedRole prefill;
    private final SelectedRole decode;
    private final long requestId;
    private DecodeResources.ReservationHandle decodeReservation;
    private boolean provisional = true;

    private ProvisionalRoute(long requestId, Response response, SelectedRole prefill,
                           SelectedRole decode) {
        this.response = Objects.requireNonNull(response, "response");
        this.prefill = Objects.requireNonNull(prefill, "prefill");
        this.decode = decode;
        this.requestId = requestId;
    }

    /** Caller retains rollback ownership of selections until the complete routing result is returned. */
    static ProvisionalRoute prepare(BalanceContext context, List<SelectedRole> selectedRoles,
                                  Response response) {
        SelectedRole prefill = null;
        SelectedRole decode = null;
        for (SelectedRole selected : selectedRoles) {
            ServerStatus status = selected.serverStatus();
            if (status.getRequestId() != context.getRequestId()) {
                throw new IllegalStateException("selected role belongs to another request");
            }
            RoleType role = status.getRole();
            if (role != null && role.supportsPrefill()) {
                if (prefill != null || !(selected.endpoint() instanceof PrefillEndpoint)) {
                    throw new IllegalStateException("route requires one exact Prefill selection");
                }
                selected.prefillWorkMs();
                prefill = selected;
            } else if (role == RoleType.DECODE) {
                if (decode != null || !(selected.endpoint() instanceof DecodeEndpoint)) {
                    throw new IllegalStateException("route requires at most one exact Decode selection");
                }
                decode = selected;
            } else {
                selected.close();
            }
        }
        if (prefill == null) {
            throw new IllegalStateException("route has no Prefill endpoint generation");
        }
        ProvisionalRoute admission = new ProvisionalRoute(context.getRequestId(), response, prefill, decode);
        prefill.transferToRoute();
        if (decode != null) {
            decode.transferToRoute();
        }
        return admission;
    }

    /** Resolve the reported blocker against this exact selection; null requests replanning. */
    WorkerEndpoint blockedEndpointIfCurrent(PlacementKey blocker) {
        requireProvisional();
        SelectedRole selected = blocker.role() == RoleType.DECODE ? decode : prefill;
        if (selected == null || !blocker.equals(placementKey(selected))) {
            throw new IllegalArgumentException("blocker does not belong to the selected route");
        }
        WorkerEndpoint endpoint = selected.endpoint();
        long version = endpoint instanceof PrefillEndpoint worker ? worker.placementVersion()
                : ((DecodeEndpoint) endpoint).placementVersion();
        return version == selected.placementVersion() ? endpoint : null;
    }

    interface Publication {
        void publish();
        boolean published();
    }

    final class QueuePublication implements Publication {
        private final RequestRoute item;
        private final QueueExecutionSettings settings;
        private boolean published;

        QueuePublication(RequestRoute item, QueueExecutionSettings settings) {
            this.item = item;
            this.settings = settings;
        }

        @Override public void publish() {
            if (prefillEndpoint().offerPinned(prefillPin(), item, settings)) {
                published = true;
                transferReservationToRequest();
            }
        }
        @Override public boolean published() { return published; }
    }

    PlacementKey prefillPlacementKey() { return placementKey(prefill); }

    Response response() { return response; }
    long prefillWorkMs() { return prefill.prefillWorkMs(); }
    public PrefillEndpoint prefillEndpoint() { return (PrefillEndpoint) prefill.endpoint(); }
    public DecodeEndpoint decodeEndpoint() { return decode == null ? null : (DecodeEndpoint) decode.endpoint(); }
    private WorkerEndpoint.GenerationPin prefillPin() { return prefill.generationPin(); }
    private WorkerEndpoint.GenerationPin decodePin() { return decode == null ? null : decode.generationPin(); }
    ServerStatus prefillStatus() { return prefill.serverStatus(); }
    ServerStatus decodeStatus() { return decode == null ? null : decode.serverStatus(); }
    long requestId() { return requestId; }
    DecodeResources.ReservationHandle decodeReservation() { return decodeReservation; }

    boolean reserveDecode(RequestRequirements requirements) {
        requireProvisional();
        if (requirements.requestId() != requestId) {
            throw new IllegalArgumentException("request inputs do not match selected route");
        }
        if (decodeEndpoint() == null || decodeReservation != null) { return true; }
        DecodeResources.AdmissionCapacity capacity = switch (requirements.mode()) {
            case IMMEDIATE -> null;
            case WAIT_AT_PLACEMENT, PREEMPT_AT_PLACEMENT -> requirements.capacity();
        };
        DecodeResources.ReservationHandle reservation = decodeEndpoint().reserve(
                decodePin(), requirements.requestId(), requirements.hardKvTokens(),
                requirements.expectedKvTokens(), requirements.priority(), capacity);
        if (reservation == null) { return false; }
        decodeReservation = reservation;
        return true;
    }

    public boolean adoptDecodeReservation(DecodeEndpoint endpoint, DecodeResources.ReservationHandle reservation) {
        requireProvisional();
        boolean adopted = false;
        Throwable failure = null;
        try {
            if (endpoint == null || endpoint != decodeEndpoint() || reservation == null
                    || reservation.requestId() != requestId || decodeReservation != null) {
                return false;
            }
            endpoint.requirePinnedGeneration(decodePin());
            if (!endpoint.markQueued(decodePin(), reservation)) {
                return false;
            }
            decodeReservation = reservation;
            adopted = true;
            return true;
        } catch (RuntimeException | Error adoptionFailure) {
            failure = adoptionFailure;
            throw adoptionFailure;
        } finally {
            if (!adopted && endpoint != null && reservation != null) {
                try {
                    endpoint.release(reservation, DecodeResources.ReleaseReason.LOCAL_ROLLBACK);
                } catch (RuntimeException | Error cleanupFailure) {
                    if (failure == null) { throw cleanupFailure; }
                    Failures.append(failure, cleanupFailure);
                }
            }
        }
    }

    PlacementKey decodePlacementKey() { return placementKey(decode); }

    private static PlacementKey placementKey(SelectedRole selected) {
        ServerStatus status = selected.serverStatus();
        return PlacementKey.exact(status.getRole(), status.getGroup(), selected.endpoint().ipPort());
    }

    /** Holds acquisition output without mixing a failed publication with capacity pressure. */
    final class PrefillReservationAttempt implements Publication {
        private final RequestRoute item;
        private PrefillState.ReservationResult<PrefillState.RouteReservation> result;

        PrefillReservationAttempt(RequestRoute item) {
            this.item = item;
        }

        @Override
        public void publish() {
            result = prefillEndpoint().reserveUnqueuedRoute(prefillPin(), item, prefill.prefillWorkMs());
        }

        @Override public boolean published() {
            return result != null && result.status() == PrefillState.CapacityStatus.ACQUIRED;
        }

        PrefillState.RouteReservation reservation() {
            return result == null ? null : result.reservation();
        }

        StrategyErrorType failure() {
            if (result == null) { return StrategyErrorType.REQUEST_CANCELLED; }
            return switch (result.status()) {
                case CAPACITY_FULL -> StrategyErrorType.RESOURCE_EXHAUSTED;
                case ENDPOINT_RETIRED -> StrategyErrorType.DISPATCH_FAILED;
                case REQUEST_ALREADY_RESERVED, REQUEST_NOT_ACTIVE, BATCH_ID_ALREADY_RESERVED ->
                        StrategyErrorType.RESOURCE_EXHAUSTED;
                case ACQUIRED -> throw new IllegalStateException("successful admission cannot be rejected");
            };
        }
    }

    /** Called only after queue publication or direct delivery handoff succeeds. */
    void transferReservationToRequest() {
        requireProvisional();
        provisional = false;
    }

    void requireProvisional() {
        if (!provisional) { throw new IllegalStateException("route admission already resolved"); }
    }

    @Override
    public void close() {
        // Pins belong to this selection scope; only Decode capacity transfers to the request.
        try (prefill; decode) {
            if (provisional) {
                provisional = false;
                if (decodeReservation != null) {
                    decodeEndpoint().release(decodeReservation, DecodeResources.ReleaseReason.LOCAL_ROLLBACK);
                }
            }
        }
    }
}
