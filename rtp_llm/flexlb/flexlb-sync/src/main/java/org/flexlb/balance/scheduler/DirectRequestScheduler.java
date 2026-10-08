package org.flexlb.balance.scheduler;

import org.flexlb.config.FlexlbConfig;
import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.BalanceContext.AdmissionHandle;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryClaim;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.util.Logger;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Accepts DIRECT requests and completes one immediate endpoint selection and handoff. */
public final class DirectRequestScheduler extends AbstractRequestScheduler {

    /** Exact committed route and its frozen prediction, before response publication. */
    record RouteDelivery(DeliveryClaim claim, WorkSnapshot precedingWork, long unstartedWorkMs) {
        RouteDelivery {
            Objects.requireNonNull(claim, "claim");
            Objects.requireNonNull(precedingWork, "precedingWork");
        }
    }

    private final DefaultRouter router;

    DirectRequestScheduler(DefaultRouter router, SchedulerRuntime runtime, FlexlbConfig config) {
        super(runtime, config);
        this.router = Objects.requireNonNull(router, "router");
    }

    @Override
    public CompletableFuture<Response> submit(BalanceContext context) {
        if (!runtime.isAccepting()) { return rejected(); }
        try {
            if (context != null && !context.getConfig().isDirect()) {
                return invalidMode();
            }
            CompletableFuture<Response> future = register(context, StrategyErrorType.BATCH_SLO_EXPIRED);
            if (!future.isDone()) {
                this.expirationTimer().scheduleInactivityDeadline(context);
                dispatchRegistered(context);
            }
            return future;
        } catch (RuntimeException failure) {
            return failSubmission(context, failure);
        }
    }

    private void dispatchRegistered(BalanceContext context) {
        Response failure = Response.error(StrategyErrorType.DISPATCH_FAILED);
        AdmissionHandle operation = this.claimAdmissionHandle(context.getRequestId(), context.getFuture());
        if (operation == null) { return; }
        try {
            failure = selectAndCommit(context, operation);
        } catch (RuntimeException selectionFailure) {
            Logger.warn("DIRECT admission failed: request_id={}", context.getRequestId(), selectionFailure);
        } finally {
            operation.finish();
            if (failure != null) {
                this.publishDecisionResponseAsync(context.getRequestId(), context.getFuture(), failure);
            }
        }
    }

    PlacementResult<RouteDelivery, PlacementKey> commitDirectRoute(
            BalanceContext context, ProvisionalRoute admission) {
        if (!admission.reserveDecode(context.getRequirements())) {
            return PlacementResult.blocked(admission.decodePlacementKey());
        }
        RequestRoute item = RequestRoute.create(context, admission, context.getEnqueueTime());
        CapacityBoundary.Attempt<PrefillAdmissionResources.Member> attempt = PrefillAdmissionResources.prepareMember(item);
        if (!attempt.accepted()) {
            return switch (attempt.boundary().status()) {
                case UNAVAILABLE -> PlacementResult.blocked(admission.decodePlacementKey());
                case OWNERSHIP_LOST -> PlacementResult.rejected(Response.error(StrategyErrorType.RESOURCE_EXHAUSTED));
                case FAILED -> throw new IllegalStateException("route admission failed", attempt.boundary().cause());
            };
        }
        PrefillAdmissionResources.Member member = attempt.value();
        try (member) {
            var reservationAttempt = admission.new PrefillReservationAttempt(item);
            try (PrefillEndpoint.RouteCommitAdmission routeCommit = admission.prefillEndpoint().tryBeginRouteCommitAdmission()) {
                if (routeCommit == null) {
                    return PlacementResult.rejected(Response.error(StrategyErrorType.DISPATCH_FAILED));
                }
                var members = List.of(member);
                PrefillState.CommittedHandoff handoff = null;
                try {
                    PlacementResult.Status result = this.commitRoute(item, reservationAttempt);
                    if (result != PlacementResult.Status.SUCCESS) {
                        return result == PlacementResult.Status.CLOSED ? PlacementResult.closed()
                                : PlacementResult.rejected(Response.error(reservationAttempt.failure()));
                    }
                    handoff = routeCommit.commit(List.of(item), List.of(reservationAttempt.reservation()));
                    WorkSnapshot precedingWork = handoff.precedingWork().materialize();
                    var claim = this.claimDelivery(item, DeliveryClaimKind.ROUTE_DECISION, 0L,
                            member);
                    if (claim == null) { return PlacementResult.closed(); }
                    admission.transferReservationToRequest();
                    return PlacementResult.success(new RouteDelivery(claim, precedingWork, admission.prefillWorkMs()));
                } finally {
                    PrefillAdmissionResources.closeCommitted(members, handoff);
                }
            } finally {
                if (reservationAttempt.reservation() != null) { admission.prefillEndpoint().rollbackReservation(reservationAttempt.reservation()); }
            }
        }
    }

    private Response selectAndCommit(BalanceContext context, AdmissionHandle operation) {
        PlacementResult<ProvisionalRoute, PlacementKey> selection = router.select(context, router.resolvePolicyGroup(context));
        context.setSchedulingDiagnostics(selection.diagnostics());
        return switch (selection.status()) {
            case SUCCESS -> {
                PlacementResult<RouteDelivery, PlacementKey> committed = null;
                try (ProvisionalRoute admission = selection.value()) {
                    committed = commitDirectRoute(context, admission);
                } catch (RuntimeException | Error cleanupFailure) {
                    if (committed == null || committed.status() != PlacementResult.Status.SUCCESS) {
                        throw cleanupFailure;
                    }
                    // Delivery ownership is already transferred; cleanup cannot retract publication.
                    Logger.warn("DIRECT selection cleanup failed after handoff: request_id={}",
                            context.getRequestId(), cleanupFailure);
                }
                yield switch (committed.status()) {
                    case SUCCESS -> {
                        RouteDelivery delivery = committed.value();
                        operation.finish();
                        this.publishRoute(delivery.claim(), delivery.precedingWork(), delivery.unstartedWorkMs());
                        yield null;
                    }
                    case REJECTED -> committed.failure();
                    case CLOSED -> Response.error(StrategyErrorType.REQUEST_CANCELLED);
                    case BLOCKED -> Response.error(StrategyErrorType.RESOURCE_EXHAUSTED);
                };
            }
            case BLOCKED -> selection.failure() != null
                    ? selection.failure() : Response.error(selection.blocker().role().getErrorType());
            case REJECTED -> selection.failure();
            case CLOSED -> Response.error(StrategyErrorType.REQUEST_CANCELLED);
        };
    }
}
