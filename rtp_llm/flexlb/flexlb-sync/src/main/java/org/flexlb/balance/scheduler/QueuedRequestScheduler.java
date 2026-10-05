package org.flexlb.balance.scheduler;

import org.flexlb.config.FlexlbConfig;
import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.eviction.EvictionManager;
import org.flexlb.balance.scheduler.BalanceContext.AdmissionHandle;
import org.flexlb.dao.loadbalance.AdmissionRejectReason;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Failures;
import org.flexlb.util.Logger;
import org.flexlb.util.PriorityNormalizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.flexlb.dao.loadbalance.Response.buildErrorResponse;

/**
 * The single QUEUE admission owner for one FlexLB model.
 *
 * <p>This queue orders the start of placement attempts for the model. A request is
 * selected from the complete live candidate fleet before the endpoint runtime
 * receives it. A bounded set of in-flight requests controls how many
 * independent routes can be prepared together; request collection and grouping
 * remain exclusively endpoint-runtime concerns. The queue lock protects only
 * index operations; route projection and RPCs are never performed while it is
 * held.</p>
 *
 * <p>Ready requests receive planning slots in FIFO/priority order. A request
 * that cannot be admitted waits for a relevant capacity event; it does not fence
 * later requests from trying the same worker with different resource demands.
 * Planning is bounded and parallel; completed plans are published without waiting
 * for earlier planners. Arrivals and wakeups do not invalidate in-flight work.
 * WorkerBatcher owns SINGLE/FIXED_WINDOW grouping.</p>
 */
public final class QueuedRequestScheduler extends AbstractRequestScheduler implements AutoCloseable {

    private static final int MIN_PLANNER_THREADS = 1;

    private final DefaultRouter router;
    private final BatchSchedulerReporter reporter;
    private final EvictionManager evictionManager;
    private final PlacementAvailability availability;
    private final QueueExecutionSettings queueSettings;
    private final boolean priorityOrdering;
    private final int plannerCount;
    private final double scanBudgetMultiplier;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final OrderedRequestQueue orderedQueue;
    private final PlacementWaitQueue waitingRequests;
    /** Entries exist only while the scheduler owns global placement. */
    private final Map<BalanceContext, GlobalQueueEntry> queuedEntries = new IdentityHashMap<>();
    private final ArrayDeque<QueueEvent> events = new ArrayDeque<>();
    private final Set<Plan> pendingPreemptions = Collections.newSetFromMap(new IdentityHashMap<>());
    /** Waiting on local preemption slots, independent of physical Worker capacity edges. */
    private final Set<GlobalQueueEntry> preemptionQuotaWaiters =
            Collections.newSetFromMap(new IdentityHashMap<>());
    // Protected by lock. A slot stays occupied until its result has been handled,
    // including when the request is cancelled while planning.
    private final Set<GlobalQueueEntry> inFlight =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private final ArrayDeque<Plan> completedPlans = new ArrayDeque<>();
    private final ArrayDeque<GlobalQueueEntry> controlInbox = new ArrayDeque<>();
    /** Published by the decision thread; timeout readers never acquire the queue lock. */
    private volatile Map<String, Object> latestQueueWaitSnapshot = Map.of("cause", "waiting for placement");
    private final ExecutorService planners;
    private final Thread decisionThread;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final PlacementAvailability.Listener availabilityListener =
            key -> postEvent(new QueueEvent(EventKind.CAPACITY, null, key), false);

    QueuedRequestScheduler(FlexlbConfig config, DefaultRouter router,
            BatchSchedulerReporter reporter, EvictionManager evictionManager,
            SchedulerRuntime runtime, PlacementAvailability availability) {
        super(runtime, config);
        this.queueSettings = QueueExecutionSettings.capture(config);
        this.router = Objects.requireNonNull(router, "router");
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.evictionManager = Objects.requireNonNull(
                evictionManager, "evictionManager");
        this.availability = Objects.requireNonNull(availability, "availability");
        this.priorityOrdering = config.isPriorityOrdering();
        this.orderedQueue = new OrderedRequestQueue(priorityOrdering);
        this.waitingRequests = new PlacementWaitQueue(priorityOrdering, availability);
        this.scanBudgetMultiplier = config.queueScheduler().getScanBudgetMultiplier();

        this.plannerCount = Math.max(MIN_PLANNER_THREADS,
                config.getInternalRuntime()
                        .getQueuePlannerThreads());
        this.planners = Executors.newFixedThreadPool(this.plannerCount,
                Thread.ofPlatform().daemon().name("flexlb-global-planner-", 1).factory());
        this.decisionThread = new Thread(this::runDecisionLoop,
                "flexlb-global-decision");
        this.decisionThread.setDaemon(true);
        this.decisionThread.setUncaughtExceptionHandler((thread, failure) -> {
            Logger.error("Global queue decision thread failed", failure);
            close();
        });
        availability.addListener(availabilityListener);
    }

    /** Start scheduler workers before accepting requests. */
    void start() {
        decisionThread.start();
    }

    /** Latest queue wait cause and counters; this snapshot is not specific to the current request. */
    Map<String, Object> getLatestQueueWaitSnapshot() {
        return latestQueueWaitSnapshot;
    }

    @Override
    protected void onCancellationRecorded(BalanceContext exact) { signalControl(exact); }

    /** Enqueue without selecting an endpoint on the ingress thread. */
    @Override
    public CompletableFuture<Response> submit(BalanceContext context) {
        return submit(context, () -> {});
    }

    @Override
    public CompletableFuture<Response> submit(BalanceContext context, Runnable onRegistered) {
        Objects.requireNonNull(onRegistered, "onRegistered");
        if (!tryAcquireSubmissionPermit()) { return rejected(); }
        try {
            if (context != null && !context.getConfig().isQueue()) {
                return invalidMode();
            }
            if (context != null && context.requestExpired(System.currentTimeMillis())) {
                context.setSchedulingDiagnostics(getLatestQueueWaitSnapshot());
            }
            CompletableFuture<Response> future = register(context, StrategyErrorType.RESOURCE_EXHAUSTED);
            if (!future.isDone()) {
                // Registration has bound the owner; cancellation can now safely reach it.
                onRegistered.run();
            }
            if (!future.isDone() && context.isOpen()) {
                this.expirationTimer().attachRequestDeadline(context, context.getRequestExpiresAtMs());
                this.expirationTimer().attachInactivityDeadline(context);
                if (!trySubmitRegistered(context)) {
                    this.settleGlobalQueueClose(context.getRequestId(), future);
                }
            } else if (context != null && context.scheduler() == this) {
                this.onGlobalControl(context.getRequestId(), future);
            }
            return future;
        } catch (RuntimeException failure) {
            return failSubmission(context, failure);
        } finally {
            release();
        }
    }

    boolean trySubmitRegistered(BalanceContext context) {
        try {
            Objects.requireNonNull(context, "context");
            CompletableFuture<Response> future = Objects.requireNonNull(context.getFuture(), "registered future");
            GlobalQueueEntry entry = new GlobalQueueEntry(context, PriorityNormalizer.normalize(context.getPriority(), null),
                    router.resolvePolicyGroup(context));
            if (!postEvent(new QueueEvent(EventKind.SUBMIT, entry, null), true)) { return false; }
            future.whenComplete((ignored, failure) -> signalControl(context));
            return true;
        } catch (RuntimeException failure) {
            // Once registration has been handed here, this mode owns execution failures.
            if (!this.publishDecisionResponseAsync(context.getRequestId(), context.getFuture(),
                    Response.buildErrorResponse(StrategyErrorType.DISPATCH_FAILED,
                            "Queue submission failed: " + failure.getMessage()))) {
                this.onGlobalControl(context.getRequestId(), context.getFuture());
            }
            return true;
        }
    }

    int size() {
        lock.lock();
        try {
            return orderedQueue.size() + (int) events.stream()
                    .filter(event -> event.kind == EventKind.SUBMIT || event.kind == EventKind.REQUEUE).count();
        } finally {
            lock.unlock();
        }
    }

    /** A withdrawal is not a new request: keep its sequence, context, Future and absolute deadline. */
    public boolean requeue(RequestRoute previous) {
        if (previous.future().isDone()) { return true; }
        BalanceContext context = previous.ctx();
        GlobalQueueEntry entry = new GlobalQueueEntry(context,
                PriorityNormalizer.normalize(context.getPriority(), null), router.resolvePolicyGroup(context));
        entry.sequence = context.placementSequence();
        return postEvent(new QueueEvent(EventKind.REQUEUE, entry, null), true);
    }

    private boolean postEvent(QueueEvent event, boolean retainEntry) {
        lock.lock();
        try {
            if (closed.get()) { return false; }
            if (retainEntry) { retain(); }
            events.addLast(event);
            changed.signal();
            return true;
        } finally { lock.unlock(); }
    }

    void signalControl(BalanceContext context) {
        postEvent(new QueueEvent(EventKind.CONTROL, new GlobalQueueEntry(context, 0, null), null), false);
    }

    private void consumeEventsUnderLock() {
        QueueEvent event;
        while ((event = events.pollFirst()) != null) {
            switch (event.kind) {
                case CAPACITY -> waitingRequests.capacityChanged(event.key);
                case CONTROL -> {
                    GlobalQueueEntry current = queuedEntries.get(event.entry.context);
                    if (current != null) { controlInbox.addLast(current); }
                }
                case SUBMIT, REQUEUE -> {
                    GlobalQueueEntry entry = event.entry;
                    if (queuedEntries.putIfAbsent(entry.context, entry) != null) {
                        release();
                        throw new IllegalStateException("duplicate global queue identity");
                    }
                    if (event.kind == EventKind.REQUEUE) {
                        entry.context.setPlanType("");
                        entry.context.setPlanCost(0L);
                        entry.context.setVictimCount(0);
                        orderedQueue.restore(entry);
                    } else {
                        orderedQueue.add(entry);
                        entry.context.placementSequence(entry.sequence);
                    }
                    if (entry.context.getFuture().isDone() || hasPendingGlobalControl(entry.context.getRequestId(), entry.context.getFuture())) {
                        controlInbox.addLast(entry);
                    }
                }
            }
        }
    }

    private void processControlUnderLock(List<GlobalQueueEntry> readyControls) {
        GlobalQueueEntry entry;
        while ((entry = controlInbox.pollFirst()) != null) {
            orderedQueue.remove(entry);
            waitingRequests.remove(entry);
            readyControls.add(entry);
        }
    }

    private void runControlActions(List<GlobalQueueEntry> readyControls) {
        for (GlobalQueueEntry entry : readyControls) {
            try {
                this.onGlobalControl(entry.context.getRequestId(), entry.context.getFuture());
                if (this.canRestoreGlobalQueue(entry.context.getRequestId(), entry.context.getFuture())) {
                    lock.lock();
                    try {
                        if (!closed.get() && queuedEntries.get(entry.context) == entry && entry.removed) {
                            orderedQueue.restore(entry);
                            changed.signal();
                        }
                    } finally {
                        lock.unlock();
                    }
                } else { removeRequest(entry); }
            } catch (Throwable failure) {
                Logger.error("Global queue control failed: request_id={}",
                        entry.context.getRequestId(), failure);
                completeDecisionResponse(entry, buildErrorResponse(StrategyErrorType.DISPATCH_FAILED,
                        "global queue control failed"));
            }
        }
        readyControls.clear();
    }

    private void runDecisionLoop() {
        try {
            List<GlobalQueueEntry> readyControls = new ArrayList<>();
            boolean drained = false;
            while (!closed.get() || hasPendingOperations()) {
                if (closed.get() && !drained) { drainOnClose(); drained = true; }
                Plan completed = pollCompletedPlan(readyControls);
                runControlActions(readyControls);
                if (completed != null) {
                    processCompletedPlan(completed);
                }
                // Reuse released slots even when other completed plans are buffered.
                List<GlobalQueueEntry> candidates = claimPlanningSlots(readyControls);
                runControlActions(readyControls);
                candidates.forEach(this::submitPlan);
                awaitIfNoWork(drained);
            }
        } finally {
            closed.set(true);
            availability.removeListener(availabilityListener);
            planners.shutdown();
            drainOnClose();
        }
    }

    private Plan pollCompletedPlan(List<GlobalQueueEntry> readyControls) {
        lock.lock();
        try {
            consumeEventsUnderLock();
            processControlUnderLock(readyControls);
            return completedPlans.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    private List<GlobalQueueEntry> claimPlanningSlots(List<GlobalQueueEntry> readyControls) {
        lock.lock();
        try {
            consumeEventsUnderLock();
            processControlUnderLock(readyControls);
            int slots = plannerCount - inFlight.size();
            if (closed.get() || slots == 0) {
                return List.of();
            }
            List<GlobalQueueEntry> candidates = planningCandidates(slots);
            inFlight.addAll(candidates);
            return candidates;
        } finally {
            lock.unlock();
        }
    }

    private void awaitIfNoWork(boolean drained) {
        lock.lock();
        try {
            // Check under the same lock as arrivals and result publication: a signal
            // between processing and this check must not leave completed work asleep.
            while (events.isEmpty() && controlInbox.isEmpty() && completedPlans.isEmpty()
                    && (!closed.get() || drained && hasPendingOperations())
                    && (inFlight.size() == plannerCount
                        || (!orderedQueue.hasUnscannedRequests()
                            && !waitingRequests.hasReadyRequests()))) {
                awaitChanged();
            }
        } finally {
            lock.unlock();
        }
    }

    private void submitPlan(GlobalQueueEntry entry) {
        try {
            planners.execute(() -> {
                Plan result;
                try {
                    result = plan(entry);
                } catch (Throwable failure) {
                    result = Plan.failure(entry, failure, availability.sequence());
                }
                publishPlan(result);
            });
        } catch (Throwable failure) {
            publishPlan(Plan.failure(entry, failure, availability.sequence()));
        }
    }

    private void publishPlan(Plan plan) {
        lock.lock();
        try {
            completedPlans.addLast(plan);
            changed.signal();
        } finally {
            lock.unlock();
        }
    }

    private void processCompletedPlan(Plan plan) {
        boolean retry = false;
        boolean completingPreemption = plan.preemptionCompleted;
        Map<String, Object> diagnostics = plan.result.diagnostics();
        try {
            if (closed.get() || completingPreemption && !isQueued(plan.entry)) {
                try { settlePreemptionResult(plan, false); }
                finally { closePlan(plan); }
                return;
            }
            Outcome outcome = Outcome.DONE;
            Throwable commitFailure = null;
            try {
                Failures.rethrow(plan.planningFailure, "placement planning failed");
                outcome = completingPreemption ? finishPreemption(plan) : commit(plan);
            } catch (Throwable failure) {
                commitFailure = failure;
            } finally {
                if (completingPreemption || !pendingPreemptions.contains(plan)) {
                    commitFailure = Failures.run(commitFailure, () -> closePlan(plan));
                }
            }
            Failures.rethrow(commitFailure, "placement commit failed");
            retry = outcome == Outcome.REPLAN
                    || (outcome == Outcome.BLOCKED && !park(plan, diagnostics));
        } catch (Throwable failure) {
            removeRequest(plan.entry);
            completeDecisionResponse(plan.entry, buildErrorResponse(
                    StrategyErrorType.DISPATCH_FAILED,
                    "Placement failed: " + failure.getMessage()));
            Logger.error("Global queue commit failed: request_id={}",
                    plan.entry.context.getRequestId(), failure);
        } finally {
            // Plan ownership has settled before this slot becomes eligible again.
            lock.lock();
            try {
                inFlight.remove(plan.entry);
                if (completingPreemption) {
                    pendingPreemptions.remove(plan);
                    for (GlobalQueueEntry waiter : preemptionQuotaWaiters) {
                        waitingRequests.remove(waiter);
                        if (isQueued(waiter)) { orderedQueue.markRequestReadyForRetry(waiter); }
                    }
                    preemptionQuotaWaiters.clear();
                }
                if (retry && isQueued(plan.entry)) {
                    orderedQueue.markRequestReadyForRetry(plan.entry);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void closePlan(Plan plan) {
        try {
            plan.close();
        } catch (Throwable failure) {
            recordFailure(failure);
            Logger.warn("Failed to close global queue route plan", failure);
        } finally { plan.finishAdmission(); }
    }

    /** Caller holds lock; every examined entry counts against the scan budget. */
    private List<GlobalQueueEntry> planningCandidates(int slots) {
        waitingRequests.resumeReady(slots, orderedQueue::markRequestReadyForRetry);
        return orderedQueue.scanForPlanningCandidates(
                slots, calculateScanBudget(slots), entry -> {
                    if (entry.context.getFuture().isDone()) {
                        removeRequestUnderLock(entry);
                        return false;
                    }
                    return !entry.removed && !inFlight.contains(entry)
                            && pendingPreemptions.stream().noneMatch(plan -> plan.entry == entry)
                            && !waitingRequests.isWaiting(entry);
                });
    }

    private Plan plan(GlobalQueueEntry entry) {
        Plan plan = new Plan(entry, availability.sequence());
        if (!isQueued(entry)) { return plan; }
        if (entry.context.getConfig().getDispatcher().requiresGenerateInput()) {
            entry.context.prepareGenerateInput();
        }
        // A delayed timer must not admit a request past its absolute deadline.
        if (entry.context.requestExpired(System.currentTimeMillis())) {
            cancelRequest(entry.context, 0L, CancelReason.DEADLINE_EXCEEDED);
            return plan;
        }
        plan.handle = this.claimAdmissionHandle(entry.context.getRequestId(), entry.context.getFuture());
        if (plan.handle == null) { return plan; }
        try {
            // Every retry makes a fresh fleet-wide selection.
            plan.result = Objects.requireNonNull(router.select(entry.context, entry.routingGroup), "placement result");
            plan.handle.recordDiagnostics(plan.result.diagnostics());
            plan.waitKey = plan.result.blocker();
        } catch (Throwable cause) {
            plan.planningFailure = cause;
        }
        return plan;
    }

    private Outcome commit(Plan plan) {
        GlobalQueueEntry entry = plan.entry;
        if (isQueued(entry)) {
            PlacementResult<ProvisionalRoute, PlacementKey> result = plan.result;
            switch (result.status()) {
                case REJECTED -> {
                    plan.finishAdmission();
                    completeDecisionResponse(entry, result.failure());
                }
                case CLOSED -> { }
                case BLOCKED -> { return Outcome.BLOCKED; }
                case SUCCESS -> {
                    ProvisionalRoute admission = result.value();
                    var publication = submitRoute(entry, admission);
                    if (publication.status() == PlacementResult.Status.BLOCKED) {
                        plan.waitKey = publication.blocker();
                        WorkerEndpoint blocked = admission.blockedEndpointIfCurrent(plan.waitKey);
                        if (blocked == null) { return Outcome.REPLAN; }
                        if (!tryPriorityRescue(plan, blocked)) { return Outcome.BLOCKED; }
                        return Outcome.PENDING;
                    }
                }
            }
        }
        removeRequest(entry);
        return Outcome.DONE;
    }

    private boolean tryPriorityRescue(Plan plan, WorkerEndpoint blockedEndpoint) {
        GlobalQueueEntry entry = plan.entry;
        if (!priorityOrdering || entry.context.getFuture().isDone()) { return false; }
        if (pendingPreemptions.size() >= plannerCount) {
            preemptionQuotaWaiters.add(entry);
            return false;
        }
        var execution = evictionManager.tryReserve(entry.context, entry.context.getRequirements(), blockedEndpoint);
        if (execution == null) {
            return false;
        }
        pendingPreemptions.add(plan);
        execution.whenComplete((result, failure) -> {
            plan.preemptionResult = result;
            plan.preemptionFailure = failure;
            plan.preemptionCompleted = true;
            publishPlan(plan);
        });
        return true;
    }

    private Outcome finishPreemption(Plan plan) {
        String rejection = null;
        try {
            if (plan.preemptionFailure != null || plan.preemptionResult == null) {
                rejection = "Decode eviction control failed before commit";
            } else if (!plan.preemptionResult.committed()) {
                rejection = plan.preemptionResult.detail();
            } else if (!settlePreemptionResult(plan, true)) {
                rejection = "Decode generation retired before canonical placement";
            } else if (submitRoute(plan.entry, plan.result.value()).status() != PlacementResult.Status.SUCCESS) {
                rejection = "selected Prefill capacity changed before canonical placement";
            }
        } catch (RuntimeException | Error failure) {
            rejection = "Decode eviction placement failed: " + failure.getMessage();
        }
        if (rejection != null) {
            plan.handle.terminate(Response.error(StrategyErrorType.RESOURCE_EXHAUSTED,
                    AdmissionRejectReason.RESOURCE_EXHAUSTED, rejection));
        }
        removeRequest(plan.entry);
        return Outcome.DONE;
    }

    private boolean settlePreemptionResult(Plan plan, boolean adopt) {
        var result = plan.preemptionResult;
        if (result == null || !result.committed()) { return false; }
        plan.preemptionResult = null;
        ProvisionalRoute route = plan.result.value();
        if (adopt) { return route.adoptDecodeReservation(route.decodeEndpoint(), result.reservation()); }
        try {
            route.decodeEndpoint().release(result.reservation(), org.flexlb.balance.endpoint.DecodeEndpoint.ReleaseReason.LOCAL_ROLLBACK);
        } catch (RuntimeException | Error failure) {
            recordFailure(failure);
            throw failure;
        }
        return false;
    }

    private boolean hasPendingOperations() {
        lock.lock();
        try { return !inFlight.isEmpty() || !pendingPreemptions.isEmpty(); }
        finally { lock.unlock(); }
    }

    PlacementResult<RequestRoute, PlacementKey> enqueueRoute(BalanceContext context, ProvisionalRoute admission) {
        if (!admission.reserveDecode(context.getRequirements())) {
            return PlacementResult.blocked(admission.decodePlacementKey());
        }
        RequestRoute item = RequestRoute.create(context, admission, System.currentTimeMillis());
        context.setRouteSubmittedNanos(System.nanoTime());
        return switch (this.commitRoute(item, admission.new QueuePublication(item, queueSettings))) {
            case SUCCESS -> PlacementResult.success(item);
            case BLOCKED -> PlacementResult.blocked(admission.prefillPlacementKey());
            case CLOSED -> PlacementResult.closed();
            case REJECTED -> throw new IllegalStateException("route commit cannot reject with a response");
        };
    }

    private PlacementResult<RequestRoute, PlacementKey> submitRoute(
            GlobalQueueEntry entry, ProvisionalRoute admission) {
        var result = enqueueRoute(entry.context, admission);
        if (result.status() == PlacementResult.Status.SUCCESS) {
            RequestRoute item = result.value();
            try {
                reporter.reportLatency(BatchSchedulerReporter.Latency.ROUTE_SUBMIT,
                        item.prefill().getRole().name(), item.prefillEp().getIp(),
                        System.currentTimeMillis() - entry.context.getStartTime());
            } catch (Throwable failure) {
                Logger.warn("Failed to record route-submit telemetry", failure);
            }
        }
        return result;
    }

    private static boolean isQueued(GlobalQueueEntry entry) {
        return !entry.removed && !entry.context.getFuture().isDone();
    }

    private boolean park(Plan plan, Map<String, Object> diagnostics) {
        int depth;
        int[] counts;
        boolean parked;
        lock.lock();
        try {
            if (!isQueued(plan.entry)) { return true; }
            parked = waitingRequests.park(plan.entry, plan.waitKey, plan.availabilitySequence);
            depth = orderedQueue.size();
            counts = orderedQueue.priorityCounts();
        } finally {
            lock.unlock();
        }
        Map<Integer, Integer> priorityCounts = new LinkedHashMap<>();
        for (int priority = 0; priority < counts.length; priority++) {
            if (counts[priority] > 0) { priorityCounts.put(priority, counts[priority]); }
        }
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("cause", plan.waitKey.role().name() + " placement unavailable");
        details.put("role", plan.waitKey.role().name());
        if (plan.waitKey.group() != null) { details.put("group", plan.waitKey.group()); }
        if (plan.waitKey.endpoint() != null) { details.put("endpoint", plan.waitKey.endpoint()); }
        details.put("capturedAtMs", System.currentTimeMillis());
        details.put("queueDepth", depth);
        details.put("priorityCounts", Collections.unmodifiableMap(priorityCounts));
        if (diagnostics != null) {
            details.put("decision", diagnostics);
        }
        latestQueueWaitSnapshot = Collections.unmodifiableMap(details);
        return parked;
    }

    private void removeRequest(GlobalQueueEntry entry) {
        lock.lock();
        try {
            removeRequestUnderLock(entry);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove from both indexes atomically. Publication and completion callbacks
     * share this boundary and return any active retry opportunity to its domain.
     * Caller holds {@link #lock}.
     */
    private void removeRequestUnderLock(GlobalQueueEntry entry) {
        orderedQueue.remove(entry);
        waitingRequests.remove(entry);
        preemptionQuotaWaiters.remove(entry);
        if (queuedEntries.remove(entry.context, entry)) { release(); }
        changed.signal();
    }

    private int calculateScanBudget(int candidates) {
        return (int) Math.min(Integer.MAX_VALUE, Math.ceil(candidates * scanBudgetMultiplier));
    }

    private void awaitChanged() {
        try {
            changed.await();
        } catch (InterruptedException interruption) {
            if (closed.get()) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void signal() {
        lock.lock();
        try {
            changed.signal();
        } finally {
            lock.unlock();
        }
    }

    private void completeDecisionResponse(GlobalQueueEntry entry, Response response) {
        try {
            this.publishDecisionResponseAsync(
                    entry.context.getRequestId(), entry.context.getFuture(), response);
        } catch (Throwable failure) {
            Logger.error(
                    "Global queue response publication failed: request_id={}",
                    entry.context.getRequestId(), failure);
        }
    }

    private void drainOnClose() {
        List<GlobalQueueEntry> abandoned;
        int registrations;
        lock.lock();
        try {
            Set<GlobalQueueEntry> all = Collections.newSetFromMap(new IdentityHashMap<>());
            all.addAll(orderedQueue.drain());
            all.addAll(queuedEntries.values());
            waitingRequests.clear();
            preemptionQuotaWaiters.clear();
            registrations = queuedEntries.size();
            for (QueueEvent event : events) {
                if (event.kind == EventKind.SUBMIT || event.kind == EventKind.REQUEUE) {
                    all.add(event.entry);
                    registrations++;
                }
            }
            abandoned = List.copyOf(all);
            events.clear();
            queuedEntries.clear();
            controlInbox.clear();
        } finally {
            lock.unlock();
        }
        for (GlobalQueueEntry entry : abandoned) {
            try {
                if (!this.settleGlobalQueueClose(entry.context.getRequestId(), entry.context.getFuture())) {
                    completeDecisionResponse(entry, buildErrorResponse(
                            StrategyErrorType.DISPATCH_FAILED,
                            "request scheduler is shutting down"));
                }
            } catch (Throwable failure) {
                Logger.error("Global queue close settlement failed: request_id={}",
                        entry.context.getRequestId(), failure);
            }
        }
        for (int i = 0; i < registrations; i++) { release(); }
    }

    @Override
    protected void closeOwnedResources() {
        close();
    }

    @Override
    public void close() {
        stopAccepting();
        if (closed.compareAndSet(false, true)) {
            availability.removeListener(availabilityListener);
            signal();
            // Late planner results release their own route ownership.
            planners.shutdown();
        }
        // Every caller observes the same drain, including a second shutdown owner.
        boolean interrupted = false;
        if (Thread.currentThread() != decisionThread) {
            while (decisionThread.isAlive()) {
                try {
                    decisionThread.join();
                } catch (InterruptedException interruption) {
                    interrupted = true;
                }
            }
        }
        while (!planners.isTerminated()) {
            try {
                planners.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException interruption) {
                interrupted = true;
            }
        }
        if (interrupted) { Thread.currentThread().interrupt(); }
    }

    private enum Outcome {
        DONE,
        BLOCKED,
        REPLAN,
        PENDING
    }

    private enum EventKind { SUBMIT, REQUEUE, CONTROL, CAPACITY }
    private record QueueEvent(EventKind kind, GlobalQueueEntry entry, PlacementKey key) { }

    private static final class Plan implements AutoCloseable {
        private final GlobalQueueEntry entry;
        private AdmissionHandle handle;
        private Throwable planningFailure;
        private volatile boolean preemptionCompleted;
        private org.flexlb.balance.eviction.DecodePreemptionCoordinator.PreemptionResult preemptionResult;
        private Throwable preemptionFailure;
        // A successful result owns its admission until transfer or close.
        private PlacementResult<ProvisionalRoute, PlacementKey> result;
        private final long availabilitySequence;
        // Retained after admission closes so the request can be parked.
        private PlacementKey waitKey;

        private Plan(GlobalQueueEntry entry, long availabilitySequence) {
            this.entry = entry;
            this.availabilitySequence = availabilitySequence;
            this.result = PlacementResult.closed();
        }

        static Plan failure(GlobalQueueEntry entry, Throwable failure, long availabilitySequence) {
            Plan plan = new Plan(entry, availabilitySequence);
            plan.planningFailure = failure;
            return plan;
        }

        void finishAdmission() {
            AdmissionHandle owned = handle;
            handle = null;
            if (owned != null) {
                owned.finish();
            }
        }

        @Override
        public void close() {
            ProvisionalRoute admission = result == null ? null : result.value();
            result = null;
            if (admission != null) { admission.close(); }
        }
    }
}
