package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeResources;
import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.projection.RouteProjection;
import org.flexlb.dao.route.RoleType;
import org.flexlb.util.Failures;
import org.flexlb.util.Logger;

import java.util.List;
import java.util.Objects;

import static org.flexlb.balance.delivery.CapacityBoundary.Attempt.accepted;
import static org.flexlb.balance.delivery.CapacityBoundary.Attempt.rejected;

/**
 * Shared endpoint-capability mechanics for the two delivery transactions.
 *
 * <p>This class deliberately has no dispatcher selection logic. The active
 * transaction decides which Prefill reservation is prepared; this class owns the
 * optional per-request Decode permit and their rollback. The
 * transaction owns its members and committed generation handoff.</p>
 */
final class PrefillAdmissionResources {

    private static final RouteProjection.AdmissionBlockSemantics
            DECODE_BLOCK = new RouteProjection.AdmissionBlockSemantics(
                    "DELIVERY_CAPACITY_DECODE_ENGINE",
                    RouteProjection.AfterProbeAdmission.UNAVAILABLE,
                    "DECODE_CAPACITY_SCOPE_UNKNOWN",
                    RoleType.DECODE);
    private PrefillAdmissionResources() {
    }

    interface Preparation {
        /** Append the exact member; null means prepared, otherwise return its rejection. */
        CapacityBoundary append(RequestRoute exact);
    }

    /** Immutable association; the exact Decode permit owns its resolution state. */
    record Member(RequestRoute item, DecodeEndpoint.EngineDispatchPermit decode) implements AutoCloseable {
        Member {
            Objects.requireNonNull(item, "item");
        }

        boolean transferToEndpoint(RequestRoute exact) {
            if (item != exact) { throw new IllegalArgumentException("handoff belongs to another exact route"); }
            if (decode == null) { return true; }
            return switch (decode.dispatch()) {
                case TRANSFERRED -> true;
                case OWNERSHIP_LOST -> false;
                case ENDPOINT_RETIRED -> throw retired("Decode", item);
            };
        }

        @Override
        public void close() {
            if (decode != null) { decode.release(); }
        }
    }

    static CapacityBoundary.Attempt<Member> prepareMember(
            RequestRoute item) {
        RequestRequirements binding = item.requirements();
        if (item.decode() == null && item.decodeEp() == null && item.decodeReservation() == null) {
            // The committed topology has no independent Decode resource.
            // Its Prefill/PDFUSION reservation still follows the same handoff.
            return captureMember(item, null);
        }
        DecodeEndpoint decode = item.decodeEp();
        if (decode == null || item.decodeReservation() == null) {
            return failed(missingEndpoint("Decode reservation", item));
        }
        DecodeEndpoint.EngineDispatchPermitAcquisition acquisition;
        try {
            acquisition = decode.acquireDispatchPermit(item.decodeReservation(), binding.capacity());
        } catch (RuntimeException | Error failure) {
            return failed(failure);
        }
        return switch (acquisition.status()) {
            case ACQUIRED, ALREADY_ACCEPTED -> captureMember(item, acquisition.permit());
            case CAPACITY_FULL -> rejected(CapacityBoundary.unavailable(
                    new DecodeAvailability(item), DECODE_BLOCK));
            case NOT_OWNED, NOT_QUEUED -> rejected(
                    CapacityBoundary.OWNERSHIP_LOST);
            case ENDPOINT_RETIRED -> failed(retired("Decode", item));
            case ALREADY_ACQUIRED -> failed(new IllegalStateException(
                    "Decode dispatch permit already acquired: request_id="
                            + item.requestId()));
        };
    }

    /**
     * Capture the acquired Decode permit into its first owning value. Neither
     * {@link Member} nor the accepted-result wrapper exists before acquisition,
     * so both allocation windows are guarded by the exact permit rollback.
     */
    private static CapacityBoundary.Attempt<Member> captureMember(
            RequestRoute item,
            DecodeEndpoint.EngineDispatchPermit permit) {
        try {
            return accepted(new Member(item, permit));
        } catch (Throwable captureFailure) {
            return failed(Failures.run(captureFailure, permit == null ? null : permit::release));
        }
    }

    static CapacityBoundary rejectedPrefill(
            RequestRoute item,
            PrefillState.CapacityStatus status,
            CapacityBoundary capacityFull) {
        return switch (status) {
            case CAPACITY_FULL -> capacityFull;
            case REQUEST_NOT_ACTIVE -> CapacityBoundary.OWNERSHIP_LOST;
            case ENDPOINT_RETIRED -> CapacityBoundary.failed(retired("Prefill", item));
            case REQUEST_ALREADY_RESERVED, BATCH_ID_ALREADY_RESERVED ->
                    CapacityBoundary.failed(new IllegalStateException(
                            "Prefill admission owns another reservation: "
                                    + "request_id=" + item.requestId()
                                    + " status=" + status));
            case ACQUIRED -> throw new IllegalArgumentException(
                    "ACQUIRED must carry a reservation");
        };
    }

    static IllegalStateException missingEndpoint(
            String role,
            RequestRoute item) {
        return new IllegalStateException(
                role + " is unavailable: request_id=" + item.requestId());
    }

    static IllegalStateException retired(
            String role,
            RequestRoute item) {
        return new IllegalStateException(
                role + " endpoint generation retired: request_id="
                        + item.requestId());
    }

    static <T> CapacityBoundary.Attempt<T> failed(Throwable cause) {
        return rejected(CapacityBoundary.failed(cause));
    }

    static Throwable rollback(AutoCloseable resource, Throwable priorFailure) {
        return resource == null ? priorFailure
                : Failures.append(priorFailure, Failures.close(resource));
    }

    static void preserveRejectedCause(
            Throwable rollbackFailure,
            CapacityBoundary boundary) {
        if (boundary.status() == CapacityBoundary.Status.FAILED
                && boundary.cause() != rollbackFailure) {
            rollbackFailure.addSuppressed(boundary.cause());
        }
    }

    /** Close only locally retained permits; committed capacity remains endpoint-owned. */
    static void closeCommitted(List<Member> members, PrefillState.CommittedHandoff handoff) {
        try {
            for (int index = 0; index < members.size(); index++) {
                Throwable failure = Failures.close(members.get(index));
                if (failure != null) {
                    Logger.error("Committed admission cleanup isolated", failure);
                }
            }
        } finally {
            if (handoff != null) {
                Throwable failure = Failures.close(handoff);
                if (failure != null) {
                    Logger.error("Committed Prefill handoff cleanup isolated", failure);
                }
            }
        }
    }

    /** Decode is the exact event source for its request-scoped permit. */
    private record DecodeAvailability(RequestRoute item) implements CapacityBoundary.Availability {
        @Override
        public boolean isAvailable() {
            return item.decodeEp().shouldRetryDispatch(item.requestId(), item.requirements().capacity());
        }

        @Override
        public void addListener(Runnable listener) {
            item.decodeEp().addEngineDispatchCapacityListener(listener);
        }

        @Override
        public void removeListener(Runnable listener) {
            item.decodeEp().removeEngineDispatchCapacityListener(listener);
        }
    }

}
