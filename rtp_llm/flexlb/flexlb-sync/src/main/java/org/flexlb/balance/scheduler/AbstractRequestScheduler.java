package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeResources;
import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.delivery.CapacityBoundary;
import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.preemption.PreemptionCancelPhase;
import org.flexlb.balance.preemption.VictimTerminal;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.BalanceContext.AdmissionHandle;
import org.flexlb.balance.scheduler.BalanceContext.CleanupNext;
import org.flexlb.balance.scheduler.BalanceContext.CleanupPass;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryClaim;
import org.flexlb.balance.scheduler.BalanceContext.DeliveryPublication;
import org.flexlb.balance.scheduler.BalanceContext.PendingPrefillRetirement;
import org.flexlb.balance.scheduler.BalanceContext.PublicationKind;
import org.flexlb.balance.scheduler.BalanceContext.PublicationPermit;
import org.flexlb.balance.scheduler.BalanceContext.RequestFuture;
import org.flexlb.balance.scheduler.BalanceContext.RequestStage;
import org.flexlb.balance.scheduler.BalanceContext.ResponseCompletion;
import org.flexlb.balance.scheduler.BalanceContext.SelectedResponse;
import org.flexlb.balance.scheduler.ExpirationTimer.DecisionDeadline;
import org.flexlb.balance.scheduler.ExpirationTimer.RequestDeadline;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.RecentCacheKeyTraceReporter;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.service.monitor.RequestSchedulerReporter;
import org.flexlb.telemetry.FlexlbTrace;
import org.flexlb.util.Failures;
import org.flexlb.util.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.flexlb.dao.loadbalance.Response.buildErrorResponse;

/** Shared request protocol; concrete schedulers own their mode-specific placement algorithm. */
public abstract class AbstractRequestScheduler implements RequestScheduler {
    protected final RequestRepository requests;
    protected final SchedulerRuntime runtime;
    protected final FlexlbConfig config;
    private final RecentCacheKeyTraceReporter recentCacheKeyTraceReporter;
    private final ResponseCompletionExecutor responseCompletions;
    private final RequestContinuationExecutor continuations;
    private final ExpirationTimer expirationTimer;
    private final Object admissionQuiescenceMonitor = new Object();
    private int inFlightAdmissionHandles;
    private final RequestSchedulerReporter requestReporter;

    protected AbstractRequestScheduler(SchedulerRuntime runtime, FlexlbConfig config) {
        this.runtime = Objects.requireNonNull(runtime);
        this.config = Objects.requireNonNull(config);
        this.requests = runtime.requests();
        this.recentCacheKeyTraceReporter = runtime.recentCacheKeyTraceReporter();
        this.responseCompletions = runtime.responseCompletions();
        this.continuations = runtime.continuations();
        this.expirationTimer = runtime.timer();
        this.requestReporter = runtime.requestReporter();
    }

    private boolean isCurrentContext(BalanceContext exact) {
        return exact != null && exact.scheduler() == this && requests.isCurrent(exact);
    }
    BalanceContext findRequestContext(long requestId) {
        BalanceContext exact = requests.findActive(requestId);
        return exact != null && exact.scheduler() == this ? exact : null;
    }
    private List<BalanceContext> ownedRequests() {
        return requests.snapshotActive().stream().filter(context -> context.scheduler() == this).toList();
    }
    final void recordFailure(Throwable cause) { runtime.recordFailure(cause); }

    @Override
    public final RequestState cancel(long requestId, long batchId, CancelReason reason) {
        Objects.requireNonNull(reason, "reason");
        BalanceContext context = requests.findActive(requestId);
        if (context != null) {
            return context.scheduler() == this ? cancelRequest(context, batchId, reason) : null;
        }
        RequestRepository.TerminalRecord terminal = requests.findTerminal(requestId);
        return terminal != null && terminal.owner() == this && terminal.state().matchesBatch(batchId)
                ? terminal.state() : null;
    }

    protected final CompletableFuture<Response> register(BalanceContext context, StrategyErrorType expiredError) {
        if (context == null || context.getRequest() == null) {
            return CompletableFuture.completedFuture(Response.error(StrategyErrorType.INVALID_REQUEST));
        }
        var config = context.getConfig();
        FlexlbTrace.setScheduleAttribute(context.getTraceContext(), FlexlbTrace.SCHEDULE_MODE,
                config.getDispatcher().requiresGenerateInput() ? "BATCH" : config.getScheduler().getType().name());
        if (config.getDispatcher().requiresGenerateInput()
                && !context.hasGenerateInput()) {
            return CompletableFuture.completedFuture(Response.buildErrorResponse(
                    StrategyErrorType.INVALID_REQUEST, "missing serialized generate_input for batch dispatch"));
        }
        CompletableFuture<Response> future = registerRequest(context, expiredError);
        if (!(context.getFuture() instanceof BalanceContext.RequestFuture)) { context.setFuture(future); }
        observeResponse(context, future);
        return future;
    }

    protected final CompletableFuture<Response> failSubmission(BalanceContext context, RuntimeException failure) {
        Response response = Response.buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, failure.getMessage());
        if (context == null || context.scheduler() != this) {
            return CompletableFuture.completedFuture(response);
        }
        terminateLocallyAndPublishResponse(context, response);
        return context.getFuture();
    }

    protected static CompletableFuture<Response> rejected() {
        return CompletableFuture.completedFuture(Response.buildErrorResponse(
                StrategyErrorType.DISPATCH_FAILED, "request scheduler is not accepting new requests"));
    }

    protected static CompletableFuture<Response> invalidMode() {
        return CompletableFuture.completedFuture(Response.buildErrorResponse(
                StrategyErrorType.DISPATCH_FAILED, "request configuration does not match scheduler mode"));
    }

    /**
     * Transfer queued Decode capacity, then return each victim to its original global queue identity.
     */
    public void completeWithdrawal(AdmissionHandle withdrawal, boolean committed) {
        checkArgument(withdrawal.owner().scheduler() == this, "foreign withdrawal");
        Throwable failure = null;
        BalanceContext context = withdrawal.owner();
        // Closing the handle clears its route; retain the exact old identity for requeue.
        RequestRoute item = withdrawal.withdrawingRoute();
        try {
            if (committed) {
                // Decode replacement has committed. Never take the Prefill lock under the context monitor.
                if (!item.prefillEp().removeQueued(item, "DECODE_RESERVATION_YIELDED")) {
                    throw new IllegalStateException("withdrawn route is no longer queued: " + context.getRequestId());
                }
                synchronized (context) {
                    context.detachWithdrawnRoute(withdrawal, item);
                }
            }
        } catch (Throwable detachFailure) {
            failure = Failures.append(failure, detachFailure);
            try {
                withdrawal.terminate(buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, "queued route withdrawal failed"));
            } catch (Throwable terminalFailure) {
                failure = Failures.append(failure, terminalFailure);
            }
        } finally {
            try {
                withdrawal.finish();
                if (committed && context.isOpen() && !Objects.requireNonNull(context.queueOwner(), "queued request owner").requeue(item)) {
                    item.future().complete(buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, "scheduler closed during route withdrawal"));
                }
            } catch (Throwable closeFailure) {
                failure = Failures.append(failure, closeFailure);
                try {
                    item.future().complete(buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, "queued route requeue failed"));
                } catch (Throwable terminalFailure) {
                    failure = Failures.append(failure, terminalFailure);
                }
            }
        }
        Failures.rethrow(failure, "queued route withdrawal failed");
    }

    public AdmissionHandle claimQueuedRoute(DecodeEndpoint endpoint, DecodeResources.ReservationHandle victim, int incomingPriority) {
        if (!enterAdmissionHandleGate()) {
            return null;
        }
        boolean transferred = false;
        try {
            BalanceContext requestContext = findRequestContext(victim.requestId());
            if (requestContext == null) {
                return null;
            }
            synchronized (requestContext) {
                RequestRoute item = requestContext.route();
                if (item == null || item.priority() >= incomingPriority
                        || item.decodeEp() != endpoint || !Objects.equals(item.decodeReservation(), victim)
                        || !requests.isCurrent(requestContext) || !requestContext.ownsPreparedDeliveryLocked(item)
                        || requestContext.admission() != null
                        || item.requestExpired(System.currentTimeMillis())) {
                    return null;
                }
                AdmissionHandle claim = requestContext.beginWithdrawal(item, (operation, response) -> finishAdmission(requestContext, operation, response));
                transferred = claim != null;
                return claim;
            }
        } finally {
            if (!transferred) {
                exitAdmissionHandleGate();
            }
        }
    }

    /** The resolved request retains its identity through archival; Context rejects retired routes. */
    public void onPrefillStatus(BalanceContext context, PrefillEndpoint source, RoleType role,
                                PrefillState.PrefillRequestStatus requestStatus) {
        try {
            if (context != null && context.scheduler() == this) {
                submitContinuation(context, context.acceptPrefillStatus(source, role, requestStatus, System.currentTimeMillis()));
            }
        } catch (Throwable failure) {
            logEndpointFailure("Prefill status", failure);
        }
    }

    public void onDecodeStatus(BalanceContext context, DecodeEndpoint source,
                               DecodeResources.DecodeRequestStatus requestStatus) {
        try {
            if (context != null && context.scheduler() == this) {
                submitContinuation(context, context.acceptDecodeStatus(source, requestStatus, System.currentTimeMillis()));
            }
        } catch (Throwable failure) {
            logEndpointFailure("Decode status", failure);
        }
    }

    private void submitContinuation(BalanceContext context, Runnable work) {
        if (work != null) {
            continuations.submit(context, work);
        }
    }

    /** One malformed update or callback must not strand the other committed endpoint updates. */
    private static <T> void forEachEndpointUpdate(String event, List<T> updates, Consumer<T> accept) {
        if (updates == null) {
            return;
        }
        try {
            for (T update : updates) {
                try {
                    accept.accept(update);
                } catch (Throwable failure) {
                    logEndpointFailure(event, failure);
                }
            }
        } catch (Throwable failure) {
            logEndpointFailure(event, failure);
        }
    }

    private static void logEndpointFailure(String event, Throwable failure) {
        try {
            Logger.error("Endpoint event isolated: event={}", event, failure);
        } catch (Throwable ignored) {
            // Diagnostics cannot prevent the remaining updates from being processed.
        }
    }

    public void onPrefillGenerationRetired(PrefillEndpoint source, List<RequestRoute> items) {
        if (source == null) {
            return;
        }
        forEachEndpointUpdate("Prefill retirement", items, exact -> {
            BalanceContext context = findRequestContext(exact.requestId());
            if (context != null) {
                DeliveryClaim delivery = context.delivery();
                if (delivery != null && delivery.item == exact) { delivery.observeRetirement(source); }
                submitContinuation(context, acceptPrefillRetirement(context, source, exact));
            }
        });
    }

    public void onDecodeGenerationRetired(DecodeEndpoint source,
                                          List<DecodeResources.ReservationHandle> reservations) {
        if (source == null) {
            return;
        }
        forEachEndpointUpdate("Decode retirement", reservations, exact -> {
            BalanceContext context = findRequestContext(exact.requestId());
            if (context != null) {
                DeliveryClaim delivery = context.delivery();
                if (delivery != null && Objects.equals(delivery.item.decodeReservation(), exact)) { delivery.observeRetirement(source); }
                Runnable work;
                synchronized (context) {
                    work = context.ownsDecodeReservationLocked(source, exact)
                            ? context.processRequestEndLocked(context.route(), DeferredTerminal.decodeGenerationRetired(
                                    "Decode endpoint generation retired: generation=" + exact.endpointGenerationId()))
                            : null;
                }
                submitContinuation(context, work);
            }
        });
    }

    public void enqueueInactivityDeadline(BalanceContext ctx, ExpirationTimer.InactivityDeadline exact, long nowMs, Runnable rearm) {
        Runnable effect;
        synchronized (ctx) {
            if (!ctx.consumeInactivityDeadlineLocked(exact)) { return; }
            effect = ctx.decideInactivityLocked(nowMs, null);
        }
        continuations.submit(ctx, () -> {
            try { execute(ctx, effect); }
            finally { rearm.run(); }
        });
    }

    private CompletableFuture<Response> registerRequest(BalanceContext context, StrategyErrorType expiredError) {
        if (context.requestExpired(System.currentTimeMillis())) {
            return CompletableFuture.completedFuture(buildErrorResponse(expiredError, "request scheduling deadline has expired before placement"));
        }
        RequestFuture response = new RequestFuture((completion, value, failure, interrupt) ->
                completeExternal(context, completion, value, failure, interrupt));
        var result = requests.register(context, this, response);
        if (result != RequestRepository.RegistrationResult.REGISTERED) {
            return CompletableFuture.completedFuture(buildErrorResponse(
                    result == RequestRepository.RegistrationResult.CLOSED ? StrategyErrorType.DISPATCH_FAILED : StrategyErrorType.INVALID_REQUEST,
                    result == RequestRepository.RegistrationResult.DUPLICATE_ID
                            ? "duplicate request_id: " + context.getRequestId() : "request registration rejected: " + result));
        }
        context.setEnqueueTime(System.currentTimeMillis());
        if (requests.isClosed()) {
            completeError(response, StrategyErrorType.DISPATCH_FAILED, "request scheduler is shutting down");
        } else if (context.requestExpired(System.currentTimeMillis())) {
            cancelRequest(context, 0L, CancelReason.DEADLINE_EXCEEDED);
        }
        return response;
    }

    public boolean isAdmissionOpen(long requestId, CompletableFuture<?> future) {
        if (requests.isClosed()) {
            return false;
        }
        BalanceContext requestContext = findRequestContext(requestId);
        if (requestContext == null || !requestContext.ownsFuture(future)) {
            return false;
        }
        synchronized (requestContext) {
            return requests.isCurrent(requestContext) && requestContext.isOpen();
        }
    }

    public AdmissionHandle claimAdmissionHandle(long requestId, CompletableFuture<?> future) {
        if (!enterAdmissionHandleGate()) {
            return null;
        }
        boolean transferred = false;
        try {
            BalanceContext requestContext = findRequestContext(requestId);
            if (requestContext == null || !requestContext.ownsFuture(future)) {
                return null;
            }
            AdmissionHandle handle;
            synchronized (requestContext) {
                handle = requests.isCurrent(requestContext) ? requestContext.beginAdmission((operation, response) -> finishAdmission(requestContext, operation, response)) : null;
            }
            transferred = handle != null;
            return handle;
        } finally {
            if (!transferred) {
                exitAdmissionHandleGate();
            }
        }
    }

    private boolean enterAdmissionHandleGate() {
        synchronized (admissionQuiescenceMonitor) {
            if (requests.isClosed()) {
                return false;
            }
            checkState(inFlightAdmissionHandles != Integer.MAX_VALUE, "admission handle counter overflow");
            inFlightAdmissionHandles++;
            return true;
        }
    }

    private void exitAdmissionHandleGate() {
        synchronized (admissionQuiescenceMonitor) {
            checkState(inFlightAdmissionHandles > 0, "admission handle counter underflow");
            inFlightAdmissionHandles--;
            if (inFlightAdmissionHandles == 0) {
                admissionQuiescenceMonitor.notifyAll();
            }
        }
    }

    void awaitAdmissionMutations() {
        boolean interrupted = false;
        synchronized (admissionQuiescenceMonitor) {
            while (inFlightAdmissionHandles != 0) {
                try {
                    admissionQuiescenceMonitor.wait();
                } catch (InterruptedException interruption) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public void onQueuedItemPreempted(RequestRoute victim, RequestRoute incoming) {
        try {
            BalanceContext context = findRouteContext(victim);
            if (context != null) {
                recordSchedulingFailure(context, StrategyErrorType.PRIORITY_PREEMPTED,
                        "preempted by higher-priority request " + incoming.requestId());
            }
            requestReporter.reportVictim(victim.priority(), incoming.priority(), "prefill_queued", "prefill_inflight_requests");
        } catch (Throwable failure) {
            logEndpointFailure("Queued preemption request_id=" + victim.requestId(), failure);
        }
    }

    public Optional<PreemptionRegistration> tryClaim(DecodeResources.ReservationHandle exact, long attemptToken, String detail) {
        Objects.requireNonNull(exact, "exact reservation");
        BalanceContext context = findRequestContext(exact.requestId());
        return context == null ? Optional.empty()
                : Optional.ofNullable(context.tryInstallPreemption(exact, attemptToken, detail));
    }

    public void onQueuedItemExpired(RequestRoute exact) {
        BalanceContext requestContext = findRouteContext(exact);
        if (requestContext != null) {
            cancelRequest(requestContext, 0L, CancelReason.DEADLINE_EXCEEDED);
        }
    }

    void onQueuedItemControl(RequestRoute exact) {
        BalanceContext requestContext = findRouteContext(exact);
        if (requestContext != null) {
            cancelInWorkerQueue(requestContext, exact);
        }
    }

    public void onGlobalControl(long requestId, CompletableFuture<Response> exactFuture) {
        BalanceContext requestContext = findRequestContext(requestId);
        if (requestContext != null && requestContext.ownsFuture(exactFuture)) {
            processGlobalControl(requestContext);
        }
    }

    public boolean settleGlobalQueueClose(long requestId, CompletableFuture<Response> exactFuture) {
        BalanceContext requestContext = findRequestContext(requestId);
        if (requestContext == null || !requestContext.ownsFuture(exactFuture)) {
            return false;
        }
        try {
            processGlobalControl(requestContext);
        } finally {
            executeFinalization(requestContext.claimShutdownAction(() -> requirePublicationPermitLocked(requestContext, PublicationKind.TERMINAL)));
        }
        return true;
    }

    public boolean canRestoreGlobalQueue(long requestId, CompletableFuture<Response> exactFuture) {
        BalanceContext requestContext = findRequestContext(requestId);
        return requestContext != null && requestContext.ownsFuture(exactFuture) && requestContext.canRestoreGlobalQueue();
    }

    public boolean hasPendingGlobalControl(long requestId, CompletableFuture<Response> exactFuture) {
        BalanceContext requestContext = findRequestContext(requestId);
        return requestContext != null && requestContext.ownsFuture(exactFuture) && requestContext.hasPendingGlobalControl();
    }

    public void onQueueOfferFailure(RequestRoute exact, Throwable error) {
        BalanceContext requestContext = findRouteContext(exact);
        if (requestContext != null) {
            cancelInWorkerQueue(requestContext, exact);
            recordSchedulingFailure(requestContext, StrategyErrorType.DISPATCH_FAILED, "Worker scheduling queue rejected request: " + (error == null ? "endpoint publication failed" : error.getMessage()));
        }
    }

    private BalanceContext findRouteContext(RequestRoute item) {
        BalanceContext context = item.ctx();
        synchronized (context) {
            return isCurrentContext(context) && context.ownsActiveRoute(item) ? context : null;
        }
    }

    private static void completeError(CompletableFuture<Response> future, StrategyErrorType errorType, String message) {
        if (future.isDone()) {
            return;
        }
        future.complete(buildErrorResponse(errorType, message));
    }

    public boolean publishDecisionResponseAsync(long requestId, CompletableFuture<Response> future, Response response) {
        BalanceContext requestContext = findRequestContext(requestId);
        return requestContext != null && requestContext.ownsFuture(future) && terminateLocallyAndPublishResponse(requestContext, response);
    }

    void closeOutstandingAndTerminalize() {
        checkState(requests.isClosed(), "admission must close before terminal shutdown");
        List<TerminalAction> actions = new ArrayList<>();
        for (BalanceContext requestContext : ownedRequests()) {
            TerminalAction action = requestContext.claimShutdownAction(() -> requirePublicationPermitLocked(requestContext, PublicationKind.TERMINAL));
            if (action != null) {
                actions.add(action);
            }
        }
        Throwable failure = null;
        for (TerminalAction action : actions) {
            failure = Failures.run(failure, () -> executeFinalization(action));
        }
        Failures.rethrow(failure, "request shutdown finalization failed");
    }

    ExpirationTimer expirationTimer() { return expirationTimer; }

    void observeResponse(BalanceContext context, CompletableFuture<Response> future) {
        future.whenComplete((response, failure) -> {
            if (failure != null) { return; }
            try {
                context.setResponse(response);
                if (response != null && response.isSuccess()) { recentCacheKeyTraceReporter.report(context); }
            } catch (RuntimeException observationFailure) {
                Logger.warn("Route completion side effect failed: request_id={}", context.getRequestId(), observationFailure);
            }
        });
    }

    PlacementResult.Status commitRoute(RequestRoute exact, ProvisionalRoute.Publication publication) {
        Objects.requireNonNull(publication, "publication");
        if (requests.isClosed() || exact == null || exact.future().isDone()) {
            return PlacementResult.Status.CLOSED;
        }
        BalanceContext ctx = exact.ctx();
        synchronized (ctx) {
            if (!isCurrentContext(ctx) || !ctx.bindRoute(exact)) {
                return PlacementResult.Status.CLOSED;
            }

        }
        // Only failed publication rolls back the binding. Later failures retain its exact owner.
        Throwable failure = null;
        try {
            publication.publish();
        } catch (RuntimeException | Error publicationFailure) {
            failure = publicationFailure;
            if (publication.published()) {
                synchronized (ctx) { ctx.confirmRoutePublication(exact); }
                if (exact.prefillEp() != null) {
                    Failures.run(publicationFailure, exact.prefillEp()::signalRouteReady);
                }
            }
            throw publicationFailure;
        } finally {
            if (!publication.published()) {
                try {
                    synchronized (ctx) {
                        ctx.rejectRoutePublication(exact);
                    }
                } catch (RuntimeException | Error rollbackFailure) {
                    if (failure == null) { throw rollbackFailure; }
                    Failures.append(failure, rollbackFailure);
                }
            }
        }
        if (!publication.published()) { return PlacementResult.Status.BLOCKED; }
        synchronized (ctx) {
            ctx.confirmRoutePublication(exact);
        }
        if (exact.prefillEp() != null) { exact.prefillEp().signalRouteReady(); }
        return PlacementResult.Status.SUCCESS;
    }

    /**
     * Close one admission and settle all facts retained while it owned the request.
     */
    private void finishAdmission(BalanceContext ctx, AdmissionHandle exact, Response failureResponse) {
        Throwable failure = null;
        try {
            try {
                Runnable effect = null;
                boolean cleanupPending;
                RequestRoute routeToCancel;
                RequestRoute restoredRoute = null;
                synchronized (ctx) {
                    if (ctx.admission() == exact) {
                        restoredRoute = ctx.finishAdmission(exact);
                        effect = ctx.settleAdmissionLocked(exact, failureResponse);
                    }
                    cleanupPending = ctx.hasCleanup();
                    routeToCancel = cleanupPending || effect != null ? null : ctx.pendingWorkerQueueCancellationLocked();
                }
                if (restoredRoute != null && restoredRoute.prefillEp() != null) {
                    restoredRoute.prefillEp().signalRouteReady();
                }
                if (routeToCancel != null) { scheduleWorkerQueueCancellation(ctx, routeToCancel); }
                if (effect != null) { execute(ctx, effect); }
                else if (cleanupPending) { resumeCleanup(ctx); }
            } catch (Throwable settlementFailure) {
                failure = settlementFailure;
            }
            // A failed settlement must not strand the expiry watch or the admission gate.
            failure = Failures.run(failure, () -> expirationTimer.attachInactivityDeadline(ctx));
            // Drain must see the failure before the last admission gate opens.
            if (failure != null) {
                runtime.recordFailure(failure);
            }
        } finally {
            exitAdmissionHandleGate();
        }
        Failures.rethrow(failure, "request cleanup failed");
    }

    /**
     * Eligibility and preparation are one transaction; null means prepared, otherwise the result is the exact rejection.
     * The operation must not publish or call user code.
     */
    public CapacityBoundary prepareDispatch(RequestRoute exact, DeliveryTransaction prepare) {
        BalanceContext ctx = exact.ctx();
        synchronized (ctx) {
            return ownsPreparedDeliveryLocked(ctx, exact) ? prepare.append(exact) : CapacityBoundary.OWNERSHIP_LOST;
        }
    }

    /**
     * Atomically arbitrate expiry against the one endpoint-ownership handoff.
     */
    public DeliveryClaim claimDelivery(RequestRoute exact, DeliveryClaimKind kind, long correlationId, PrefillAdmissionResources.Member handoff) {
        BalanceContext ctx = exact.ctx();
        Runnable expired;
        synchronized (ctx) {
            checkArgument(kind != DeliveryClaimKind.NONE
                    && (kind != DeliveryClaimKind.BATCH_ENQUEUE
                    || correlationId > 0L),
                    "invalid delivery identity");
            if (!ownsPreparedDeliveryLocked(ctx, exact)) {
                return null;
            }
            // Read time under the same lock as the claim. Timer execution can
            // lag, so an unconsumed timer is not proof that this item is live.
            long nowMs = System.currentTimeMillis();
            if (ctx.requestInactiveLocked(nowMs)) {
                expired = ctx.decideInactivityLocked(nowMs, null);
            } else if (exact.requestExpired(nowMs)) {
                ctx.recordCancellationLocked(CancelReason.DEADLINE_EXCEEDED, "request scheduling deadline exceeded before delivery");
                expired = finalizationEffects(ctx.tryTerminateCancellationLocked(() -> requirePublicationPermitLocked(ctx, PublicationKind.TERMINAL)), null);
            } else {
                if (!handoff.transferToEndpoint(exact)) {
                    throw new IllegalStateException("endpoint ownership lost for request " + ctx.getRequestId());
                }
                return ctx.beginDelivery(exact, kind, correlationId, nowMs, (claim, result) -> {
                    Runnable work = acceptDeliveryResult(claim, result);
                    if (work != null) { submitContinuation(claim.item.ctx(), work); }
                }, runtime::startDeliveryCleanup);
            }
        }
        execute(ctx, expired);
        return null;
    }

    /**
     * Queue publication makes an exact item claimable even while admission still pins its resources.
     */
    private boolean ownsPreparedDeliveryLocked(BalanceContext ctx, RequestRoute exact) {
        return isCurrentContext(ctx) && ctx.ownsPreparedDeliveryLocked(exact);
    }

    public void setDeliveryPrediction(DeliveryClaim claim, WorkSnapshot work, long predictedMs) {
        BalanceContext ctx = claim.item.ctx();
        DecisionDeadline obsolete;
        synchronized (ctx) {
            if (!ctx.ownsActiveRoute(claim.item)) {
                return;
            }
            obsolete = ctx.updateDeliveryPredictionLocked(work, predictedMs, System.currentTimeMillis());
        }
        ExpirationTimer.releaseDecisionDeadline(obsolete);
        expirationTimer.attachDecisionDeadline(ctx);
    }

    public void publishRoute(DeliveryClaim claim, WorkSnapshot work, long predictedMs) {
        BalanceContext ctx = claim.item.ctx();
        Runnable effect;
        DecisionDeadline obsolete;
        synchronized (ctx) {
            if (!ctx.ownsActiveRoute(claim.item)) {
                return;
            }
            checkArgument(ctx.deliveryClaimKind() == DeliveryClaimKind.ROUTE_DECISION,
                    "route publication requires a route claim");
            obsolete = ctx.updateDeliveryPredictionLocked(work, predictedMs, System.currentTimeMillis());
            effect = ctx.acknowledgeDeliveryLocked(null);
        }
        claim.completeRoute();
        executeEngineEffects(ctx, effect, obsolete);
    }

    private Runnable acceptDeliveryResult(DeliveryClaim claim, DeliveryResult result) {
        BalanceContext ctx = claim.item.ctx();
        Objects.requireNonNull(result, "delivery result");
        Runnable acknowledgement;
        synchronized (ctx) {
            if (!ctx.acceptDeliveryClaim(claim) || ctx.hasTerminalAction() || ctx.hasCleanup()) { return null; }
            if (result.failed()) {
                SelectedResponse response = ctx.selectDeliveryFailureLocked(claim.item, result.status(),
                        "Delivery failed: " + detailOf(result.cause()), () -> requirePublicationPermitLocked(ctx, PublicationKind.TERMINAL));
                return () -> publishFailureAndCleanUp(ctx, response);
            }
            if (!ctx.ownsActiveRoute(claim.item)) {
                return null;
            }
            if (result.status() == DeliveryResult.Status.DELIVERED) {
                ctx.setAckAtMs(System.currentTimeMillis());
                ctx.setAckAtNanos(System.nanoTime());
            } else if (!ctx.decodeAccepted()) {
                ctx.markAwaitingConfirmationLocked("Delivery outcome uncertain: " + detailOf(result.cause()));
                return null;
            }
            acknowledgement = ctx.acknowledgeDeliveryLocked(null);
        }
        return acknowledgement == null ? null : () -> execute(ctx, acknowledgement);
    }

    public void failDeliveryPreparation(RequestRoute exact, Throwable cause) {
        BalanceContext ctx = exact.ctx();
        SelectedResponse response;
        synchronized (ctx) {
            if (!ownsPreparedDeliveryLocked(ctx, exact)) {
                return;
            }
            response = ctx.selectDeliveryFailureLocked(exact, DeliveryResult.Status.NOT_SENT, "Delivery preparation failed: " + detailOf(cause), () -> requirePublicationPermitLocked(ctx, PublicationKind.TERMINAL));
        }
        publishFailureAndCleanUp(ctx, response);
    }

    private void publishFailureAndCleanUp(BalanceContext ctx, SelectedResponse response) {
        Throwable failure = Failures.run(null, response == null ? null : () -> submitSelectedResponse(response));
        failure = Failures.run(failure, () -> resumeCleanup(ctx));
        Failures.rethrow(failure, "request cleanup failed");
    }

    private static String detailOf(Throwable cause) {
        if (cause == null) {
            return "unknown delivery failure";
        }
        String message = cause.getMessage();
        return message == null || message.isBlank() ? cause.getClass().getSimpleName() : message;
    }

    // ── Worker 事实与 Endpoint 退出：接收、核验、推进 ──

    void executeEngineEffects(BalanceContext ctx, Runnable work, DecisionDeadline obsolete) {
        ExpirationTimer.releaseDecisionDeadline(obsolete);
        Throwable failure = Failures.run(null, () -> expirationTimer.attachDecisionDeadline(ctx));
        failure = Failures.run(failure, () -> execute(ctx, work));
        Failures.rethrow(failure, "request cleanup failed");
    }

    private Runnable acceptPrefillRetirement(BalanceContext ctx, PrefillEndpoint source, RequestRoute exact) {
        String detail = "Prefill endpoint generation retired: " + source.ipPort() + "#" + source.getStatus().getGenerationId();
        synchronized (ctx) {
            if (ctx.hasCleanup() && ctx.ownsPrefillRouteLocked(source, exact)) {
                ctx.recordCleanupSettlement(true, false, false);
                return () -> resumeCleanup(ctx);
            }
            return finalizationEffects(ctx.claimPrefillRetirementLocked(new PendingPrefillRetirement(source, exact, detail)), null);
        }
    }

    /**
     * Return the resulting request snapshot; accepting cancellation does not imply immediate cleanup.
     */
    protected void onCancellationRecorded(BalanceContext exact) { }

    public void onResponseUndeliverable(BalanceContext exact) {
        if (exact == null || exact.scheduler() != this) { return; }
        DeliveryClaim delivery = exact.delivery();
        if (delivery != null) { delivery.abandon(CancelReason.CLIENT_CANCELLED); }
        cancelRequest(exact, 0L, CancelReason.CLIENT_CANCELLED);
    }

    final RequestState cancelRequest(BalanceContext ctx, long expectedBatchId, CancelReason reason) {
        return cancelRequest(ctx, expectedBatchId, reason, null);
    }

    /** Records the first cancellation, then asks the resource owner to settle the request. */
    private RequestState cancelRequest(BalanceContext ctx, long expectedBatchId, CancelReason reason,
                                       RequestDeadline deadline) {
        Objects.requireNonNull(reason, "reason");
        TerminalAction action = null;
        RequestState result;
        RequestRoute routeToCancel;
        synchronized (ctx) {
            // Validate the cancellation target: exact timer, or expected batch (0 means any batch).
            if (deadline != null) {
                if (!ctx.consumeRequestDeadline(deadline) || !ctx.isOpen()) {
                    return null;
                }
            } else if (expectedBatchId != 0L && ctx.batchId() != expectedBatchId) {
                return null;
            }

            // A scheduling timeout cannot cancel delivery, except while admission is still running.
            // Only the exact scheduling timer can use that admission exception.
            boolean deadlineDuringAdmission = deadline != null && ctx.admission() != null;
            if (reason == CancelReason.DEADLINE_EXCEEDED && ctx.deliveryClaimKind() != DeliveryClaimKind.NONE
                    && !deadlineDuringAdmission) {
                return deadline == null ? ctx.snapshot() : null;
            }

            // Repeated cancellation only reads the state; the first caller owns cancellation work.
            String message = deadlineDuringAdmission
                    ? "request scheduling deadline exceeded during admission" : reason.getMessage();
            if (!ctx.recordCancellationLocked(reason, message)) {
                return deadline == null ? ctx.snapshot() : null;
            }

            // Queued requests are settled by their queue owner. Otherwise try to settle now;
            // an admission or delivery still in progress can defer that finalization.
            routeToCancel = ctx.pendingWorkerQueueCancellationLocked();
            boolean waitingInGlobalQueue = ctx.queueOwner() != null
                    && ctx.stage() == RequestStage.QUEUED && ctx.admission() == null;
            if (routeToCancel == null && !waitingInGlobalQueue) {
                action = ctx.tryTerminateCancellationLocked(
                        () -> requirePublicationPermitLocked(ctx, PublicationKind.TERMINAL));
            }
            result = deadline == null ? ctx.snapshot() : null;
        }

        // Notify delivery and queue owners outside the context lock; publish completion last.
        DeliveryClaim delivery = ctx.delivery();
        if (delivery != null) {
            delivery.abandon(reason);
            if (action == null) {
                synchronized (ctx) {
                    action = ctx.claimFinalizationLocked(null,
                            TerminalOutcome.cancellation(reason, reason.getMessage()),
                            Response.copyOf(ctx.cancellationResponse()), true,
                            () -> requirePublicationPermitLocked(ctx, PublicationKind.TERMINAL));
                }
            }
        }
        onCancellationRecorded(ctx);
        if (routeToCancel != null) {
            scheduleWorkerQueueCancellation(ctx, routeToCancel);
        }
        executeFinalization(action);
        return result;
    }

    void scheduleWorkerQueueCancellation(BalanceContext ctx, RequestRoute exact) {
        if (!exact.prefillEp().signalQueuedControl(exact)) {
            continuations.submit(ctx, () -> cancelInWorkerQueue(ctx, exact));
        }
    }

    void cancelInWorkerQueue(BalanceContext ctx, RequestRoute exact) {
        TerminalAction action;
        synchronized (ctx) {
            if (exact == null || ctx.pendingWorkerQueueCancellationLocked() != exact) {
                return;
            }
            action = ctx.tryTerminateCancellationLocked(
                    () -> requirePublicationPermitLocked(ctx, PublicationKind.TERMINAL));
        }
        executeFinalization(action);
    }

    void processGlobalControl(BalanceContext ctx) {
        TerminalAction action;
        synchronized (ctx) {
            action = ctx.stage() == RequestStage.QUEUED && ctx.admission() == null && ctx.cancellationReason() != null ? ctx.tryTerminateCancellationLocked(() -> requirePublicationPermitLocked(ctx, PublicationKind.TERMINAL)) : null;
        }
        executeFinalization(action);
    }

    void recordSchedulingFailure(BalanceContext ctx, StrategyErrorType error, String detail) {
        Runnable work;
        synchronized (ctx) {
            work = ctx.processRequestEndLocked(ctx.activeRoute(), DeferredTerminal.failure(error, detail));
        }
        execute(ctx, work);
    }

    // ── 调度期限：安装与到期 ──

    public void onSchedulingDeadline(BalanceContext ctx, RequestDeadline exact) {
        cancelRequest(ctx, 0L, CancelReason.DEADLINE_EXCEEDED, Objects.requireNonNull(exact, "deadline"));
    }

    // ── 抢占协议：注册、进展、释放与完成 ──

    public boolean updatePreemption(PreemptionRegistration claim, PreemptionCancelPhase next) {
        BalanceContext ctx = claim.owner;
        if (next == null) { return false; }
        Runnable work;
        synchronized (ctx) {
            if (!isCurrentContext(ctx) || !ctx.advancePreemption(claim, next)) { return false; }
            work = ctx.hasCleanup() ? () -> resumeCleanup(ctx)
                    : switch (next) {
                        case NOT_FOUND_STALE -> ctx.processPendingEventsUnderPreemptionLocked(claim, false, claim);
                        case CANCEL_UNKNOWN -> ctx.processPendingEventsUnderPreemptionLocked(claim, true, claim);
                        default -> null;
                    };
        }
        execute(ctx, work);
        return true;
    }

    public boolean releasePreemption(PreemptionRegistration claim) {
        BalanceContext ctx = claim.owner;
        Runnable work;
        boolean cleanupPending;
        RequestRoute routeToCancel;
        synchronized (ctx) {
            if (!isCurrentContext(ctx) || !ctx.ownsResourceTrackingLocked() || ctx.preemption() != claim || !claim.isReleasable()) { return false; }
            ctx.detachPreemptionOwnerLocked(claim);
            cleanupPending = ctx.hasCleanup();
            work = cleanupPending ? null : ctx.processPendingEventsUnderPreemptionLocked(claim, false, claim);
            routeToCancel = cleanupPending || work != null ? null : ctx.pendingWorkerQueueCancellationLocked();
        }
        if (cleanupPending) { resumeCleanup(ctx); } else { execute(ctx, work); }
        if (routeToCancel != null) { scheduleWorkerQueueCancellation(ctx, routeToCancel); }
        return true;
    }

    public boolean completePreemption(PreemptionRegistration claim, String detail) {
        BalanceContext ctx = claim.owner;
        Runnable work;
        synchronized (ctx) {
            if (!isCurrentContext(ctx) || !ctx.ownsResourceTrackingLocked() || ctx.preemption() != claim
                    || !claim.canCompletePreemption() || !claim.tryFinish()) { return false; }
            work = ctx.finishPreemptedRequestLocked(claim, detail, false);
        }
        execute(ctx, work);
        return true;
    }

    /**
     * The only close gate: no event may discard an outstanding cleanup obligation.
     */

    void commitTerminalRecord(BalanceContext ctx, TerminalAction action) {
        ExpirationTimer.DetachedDeadlines deadlines;
        synchronized (ctx) { deadlines = ctx.detachDeadlines(); }
        deadlines.release();
        RequestState terminal;
        synchronized (ctx) { terminal = ctx.finishTerminal(action); }
        requests.archive(ctx, terminal);
    }

    private void executeFinalization(TerminalAction action) {
        if (action == null) { return; }
        Throwable failure = Failures.run(null, () -> finishTerminal(action));
        failure = Failures.run(failure, () -> publishTerminal(action));
        Failures.rethrow(failure, "request finalization failed");
    }

    private void publishTerminal(TerminalAction action) {
        if (action.publication() != null && action.response() != null) {
            submitSelectedResponse(BalanceContext.selectPublication(action.requestContext(), action.publication(),
                    ResponseCompletion.RESPONSE, action.response(), null, false));
        }
    }

    private PublicationPermit finishTerminal(TerminalAction action) {
        BalanceContext context = action.requestContext();
        context.requireCleanupOwner(action);
        Throwable cleanupFailure = null;
        cleanupFailure = Failures.run(cleanupFailure, () -> action.terminalResources().release());
        cleanupFailure = Failures.run(cleanupFailure, action.preemption() == null ? null : () -> action.preemption().signalTerminal(new VictimTerminal(context.getRequestId())));
        if (cleanupFailure != null) { recordFailure(cleanupFailure); }
        DeliveryClaim delivery = context.delivery();
        boolean successfulWorker = action.event() != null && action.event().kind() == DeferredTerminal.Kind.WORKER
                && action.event().workerSuccessful();
        if (delivery != null) {
            if (successfulWorker && context.cancellationReason() == null && !delivery.cleanupRequired()) {
                delivery.observeWorkerCompletion(action.item());
            } else {
                delivery.abandon(context.cancellationReason() == null ? CancelReason.CLIENT_CANCELLED : context.cancellationReason());
            }
        }
        boolean archive;
        boolean batchDelivery;
        synchronized (context) {
            if (cleanupFailure == null) { context.finishTerminalEffectsLocked(action); }
            archive = context.claimArchiveLocked(action);
            batchDelivery = context.deliveryClaimKind() == DeliveryClaimKind.BATCH_ENQUEUE;
        }
        if (archive) { commitTerminalRecord(context, action); }
        else if (batchDelivery) { enqueueCleanup(context); }
        else {
            Throwable releaseFailure = Failures.run(null, () -> resumeCleanup(context));
            if (releaseFailure != null) { recordFailure(releaseFailure); }
        }
        return action.publication();
    }

    private static void execute(BalanceContext ctx, Runnable effect) {
        if (effect == null) { return; }
        ctx.requireOutsideContextLock("request effects");
        effect.run();
    }

    Runnable finalizationEffects(TerminalAction action, PreemptionRegistration signal) {
        if (action == null) { return null; }
        return () -> {
            Throwable failure = Failures.run(null, () -> executeFinalization(action));
            failure = Failures.run(failure, signal == null ? null
                    : () -> signal.signalTerminal(new VictimTerminal(action.requestContext().getRequestId())));
            Failures.rethrow(failure, "request cleanup failed");
        };
    }

    Runnable deliveryEffects(BalanceContext ctx, DeliveryPublication delivery, PreemptionRegistration signal) {
        return () -> {
            Throwable failure = Failures.run(null, () -> submitDeliveryResponse(delivery));
            failure = Failures.run(failure, signal == null ? null
                    : () -> signal.signalTerminal(new VictimTerminal(ctx.getRequestId())));
            Failures.rethrow(failure, "request cleanup failed");
        };
    }

    /** Keep ACK selection on the response execution queue, so newer terminal facts can win. */
    private void submitDeliveryResponse(DeliveryPublication delivery) {
        try {
            delivery.item().ctx().requireOutsideContextLock("delivery response submission");
            try {
                if (delivery.requestDeadline() != null) { delivery.requestDeadline().cancel(); }
            } catch (Throwable failure) {
                Logger.error("Delivery deadline cancellation failed request_id={}", delivery.item().requestId(), failure);
            }
            responseCompletions.submit(delivery.publication().registration, () -> {
                try {
                    if (delivery.batchEnqueueStartedAtMs() > 0L && delivery.item().ctx().getAckAtMs() > 0L) {
                        runtime.batchReporter().reportLatency(BatchSchedulerReporter.Latency.DISPATCH_ACK,
                                RoleType.PREFILL.name(),
                                delivery.item().prefillEp() == null ? "" : delivery.item().prefillEp().getIp(),
                                Math.max(0L, delivery.item().ctx().getAckAtMs() - delivery.batchEnqueueStartedAtMs()));
                    }
                } catch (Throwable failure) {
                    Logger.error("Delivery ACK reporting failed request_id={}", delivery.item().requestId(), failure);
                }
                return completeFutureResult(BalanceContext.selectPublication(delivery.item().ctx(), delivery.publication(),
                        ResponseCompletion.RESPONSE, delivery.response(), null, false));
            });
        } catch (RuntimeException | Error failure) {
            delivery.publication().abandonIfUnused();
            throw failure;
        }
    }

    static boolean completeFutureResult(SelectedResponse response) {
        response.permit().requestContext().requireOutsideContextLock("response completion");
        if (response.result() == null) { return false; }
        var result = response.result();
        return switch (result.completion()) {
            case RESPONSE -> response.future().completeOwned(result.response());
            case FAILURE -> response.future().completeExceptionallyOwned(result.failure());
            case CANCELLATION -> response.future().cancelOwned(result.interrupt());
        };
    }

    private void submitSelectedResponse(SelectedResponse response) {
        try {
            response.permit().requestContext().requireOutsideContextLock("response submission");
            responseCompletions.submit(response.permit().registration, () -> completeFutureResult(response));
        } catch (RuntimeException | Error failure) {
            response.permit().closePublication();
            throw failure;
        }
    }

    private boolean completeSelectedResponseNow(SelectedResponse response) {
        return responseCompletions.completeNow(response.permit().registration, () -> completeFutureResult(response));
    }

    // ── 响应：本地结束、结果仲裁与发布交接 ──
    boolean terminateLocallyAndPublishResponse(BalanceContext ctx, Response response) {
        PublicationPermit permit = terminateLocallyAndAcquirePublication(ctx, responseOutcome(response));
        if (permit == null) {
            return false;
        }
        submitSelectedResponse(BalanceContext.selectPublication(ctx, permit, ResponseCompletion.RESPONSE, response, null, false));
        return true;
    }

    boolean completeExternal(BalanceContext ctx, ResponseCompletion completion, Response response, Throwable error, boolean interrupt) {
        ctx.requireOutsideContextLock("external Future completion");
        if (completion == ResponseCompletion.CANCELLATION) {
            Boolean queued = cancelQueuedExternal(ctx, interrupt);
            if (queued != null) {
                return queued;
            }
        }
        TerminalOutcome outcome = switch(completion) {
            case RESPONSE ->
                responseOutcome(response);
            case FAILURE ->
                {
                    Objects.requireNonNull(error, "error");
                    yield TerminalOutcome.fail("external future failure" + (error.getMessage() == null ? "" : ": " + error.getMessage()));
                }
            case CANCELLATION ->
                TerminalOutcome.cancel(CancelReason.CLIENT_CANCELLED.getMessage());
        };
        PublicationPermit permit = terminateLocallyAndAcquirePublication(ctx, outcome);
        return permit != null && completeSelectedResponseNow(BalanceContext.selectPublication(ctx, permit, completion, response, error, interrupt));
    }

    /**
     * Complete the Java Future synchronously; the exact queue owner performs cleanup.
     */
    private Boolean cancelQueuedExternal(BalanceContext ctx, boolean interrupt) {
        PublicationPermit permit;
        RequestRoute routeToCancel;
        boolean globalControl;
        synchronized (ctx) {
            globalControl = ctx.queueOwner() != null
                    && (ctx.stage() == RequestStage.QUEUED || ctx.stage() == RequestStage.ROUTING);
            boolean local = ctx.queueOwner() != null && ctx.stage() == RequestStage.READY_TO_DELIVER && ctx.route() != null && ctx.route().prefillEp() != null;
            if (!globalControl && !local) {
                return null;
            }
            // External Future.cancel cannot select another response after any completion.
            if (ctx.future().isDone()) {
                return false;
            }
            boolean admissionPending = ctx.admission() != null && ctx.ownsActiveGenerationLocked() && ctx.preemption() == null && !ctx.decodeAccepted() && !ctx.deliveryClaimKind().isClaimed();
            if (!(ctx.canFinalizeBeforeExecutionLocked() || admissionPending) || ctx.selectedResponse() != null || ctx.cancellationReason() == CancelReason.DEADLINE_EXCEEDED) {
                return false;
            }
            permit = requirePublicationPermitLocked(ctx, PublicationKind.TERMINAL);
            try {
                if (ctx.cancellationReason() == null && !ctx.recordCancellationLocked(CancelReason.CLIENT_CANCELLED, CancelReason.CLIENT_CANCELLED.getMessage())) {
                    permit.abandonIfUnused();
                    return false;
                }
                ctx.selectQueuedCancellation(interrupt);
                routeToCancel = local ? ctx.route() : null;
            } catch (RuntimeException | Error failure) {
                permit.abandonIfUnused();
                throw failure;
            }
        }
        try {
            return completeSelectedResponseNow(BalanceContext.selectPublication(ctx, permit, ResponseCompletion.CANCELLATION, null, null, interrupt));
        } catch (RuntimeException | Error failure) {
            ctx.future().cancelOwned(interrupt);
            throw failure;
        } finally {
            if (routeToCancel != null) {
                scheduleWorkerQueueCancellation(ctx, routeToCancel);
            }
            if (globalControl) {
                ctx.queueOwner().signalControl(ctx);
            }
        }
    }

    private static TerminalOutcome responseOutcome(Response response) {
        String detail = response != null && response.getErrorMessage() != null ? response.getErrorMessage() : "external future completion";
        return response != null && !response.isSuccess() ? TerminalOutcome.fail(detail) : TerminalOutcome.complete(detail);
    }

    private PublicationPermit terminateLocallyAndAcquirePublication(BalanceContext ctx, TerminalOutcome transition) {
        TerminalAction action;
        synchronized (ctx) {
            if (ctx.future().isDone() || ctx.selectedResponse() != null) {
                return null;
            }
            if (ctx.cancellationReason() != null || !ctx.canFinalizeBeforeExecutionLocked()) {
                return null;
            }
            if (transition.phase() == RequestState.Phase.COMPLETED
                    && ctx.deliveryClaimKind() == DeliveryClaimKind.NONE) {
                return null;
            }
            action = ctx.claimFinalizationLocked(null, transition, null, true, () -> requirePublicationPermitLocked(ctx, PublicationKind.TERMINAL));
        }
        if (action == null) { return null; }
        try {
            return finishTerminal(action);
        } catch (RuntimeException | Error failure) {
            if (action.publication() != null) { Failures.run(failure, action.publication()::abandonIfUnused); }
            throw failure;
        }
    }

    PublicationPermit requirePublicationPermitLocked(BalanceContext ctx, PublicationKind kind) {
        ctx.requireContextLock("publication registration");
        var registration = responseCompletions.tryRegister();
        if (registration == null) {
            throw new IllegalStateException("frontend publication is closed for request " + ctx.getRequestId());
        }
        return new PublicationPermit(registration, ctx, kind);
    }

    record Settlement(boolean prefillSettled, boolean decodeSettled, Throwable failure) { }

    static Settlement releaseResources(RequestRoute exact, DecodeResources.ReleaseReason releaseReason,
                             org.flexlb.balance.delivery.DeliveryResult.Status source,
                             boolean prefillSettled, boolean decodeSettled) {
        if (exact == null) { return new Settlement(true, true, null); }
        Throwable failure = null;
        try {
            if (!prefillSettled && exact.prefillEp() != null) { exact.prefillEp().releaseRequest(exact); }
            prefillSettled = true;
        } catch (Throwable problem) { failure = problem; }
        try {
            if (!decodeSettled) {
                if (exact.decodeEp() == null || exact.decodeReservation() == null) { decodeSettled = true; }
                else if (releaseReason != null) {
                    DecodeResources.ReservationReleaseResult released = Objects.requireNonNull(
                            exact.decodeEp().release(exact.decodeReservation(), releaseReason), "Decode release result");
                    decodeSettled = released == DecodeResources.ReservationReleaseResult.RELEASED
                            || released == DecodeResources.ReservationReleaseResult.STALE;
                } else {
                    decodeSettled = exact.decodeEp().settleFailedRequest(exact.decodeReservation(), source);
                }
            }
        } catch (Throwable problem) { failure = Failures.append(failure, problem); }
        return new Settlement(prefillSettled, decodeSettled, failure);
    }

    /** Resume the one request cleanup owner when delivery release evidence becomes complete. */
    void enqueueCleanup(BalanceContext ctx) {
        BalanceContext.CleanupQueue queued;
        synchronized (ctx) { queued = ctx.tryQueueCleanupLocked(); }
        if (queued == null) { return; }
        try {
            submitContinuation(ctx, () -> {
                synchronized (ctx) { ctx.releaseCleanupQueueLocked(queued); }
                resumeCleanup(ctx);
            });
        } catch (RuntimeException | Error failure) {
            synchronized (ctx) { ctx.releaseCleanupQueueLocked(queued); }
            throw failure;
        }
    }

    void resumeCleanup(BalanceContext ctx) {
        ctx.requireOutsideContextLock("request cleanup");
        DeliveryClaim delivery;
        boolean failedDelivery;
        synchronized (ctx) {
            if (!ctx.hasCleanup()) { return; }
            delivery = ctx.delivery();
            failedDelivery = ctx.cleanupSource() != null;
        }
        if (delivery != null && failedDelivery) {
            delivery.abandon(ctx.cancellationReason() == null ? CancelReason.CLIENT_CANCELLED : ctx.cancellationReason());
        }
        Throwable error = null;
        try {
            while (true) {
                CleanupPass pass;
                synchronized (ctx) { pass = ctx.beginCleanup(); }
                if (pass == null) { break; }
                error = Failures.run(error, pass.requestDeadline() == null ? null : pass.requestDeadline()::cancel);
                error = Failures.run(error, () -> ExpirationTimer.releaseDecisionDeadline(pass.decisionDeadline()));
                var settlement = releaseResources(pass.route(), pass.releaseReason(), pass.source(),
                        pass.prefillSettled(), pass.decodeSettled());
                error = Failures.append(error, settlement.failure());
                TerminalAction completed;
                TerminalAction start;
                synchronized (ctx) {
                    CleanupNext next = ctx.finishCleanup(pass, settlement.prefillSettled(), settlement.decodeSettled());
                    if (next == CleanupNext.STALE) { break; }
                    if (next == CleanupNext.REPEAT) { continue; }
                    completed = ctx.completedCleanupActionLocked();
                    start = completed == null ? ctx.tryFinishCleanupLocked() : null;
                }
                if (completed != null) { commitTerminalRecord(ctx, completed); }
                else { executeFinalization(start); }
                break;
            }
        } catch (Throwable problem) { error = Failures.append(error, problem); }
        Failures.rethrow(error, "request cleanup failed");
    }

}

/**
 * Endpoint that published the terminal request status; Decode statuses follow its ledger update.
 */
enum WorkerTerminalSource {
    PREFILL_ENDPOINT, DECODE_ENDPOINT
}

/**
 * Terminal event, retained across admission/preemption and carried through lock-free cleanup.
 */
