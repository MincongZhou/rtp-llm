package org.flexlb.balance.scheduler;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.DecodeResources;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.preemption.CancelTarget;
import org.flexlb.balance.preemption.PreemptionCancelPhase;
import org.flexlb.balance.preemption.VictimResolution;
import org.flexlb.balance.projection.WorkSnapshot;
import org.flexlb.balance.scheduler.ExpirationTimer.DecisionDeadline;
import org.flexlb.balance.scheduler.ExpirationTimer.InactivityDeadline;
import org.flexlb.balance.scheduler.ExpirationTimer.RequestDeadline;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.SchedulingMetadata;
import org.flexlb.dao.loadbalance.AdmissionRejectReason;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService.GenerateInputPB;
import org.flexlb.util.Failures;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.math.LongMath.saturatedAdd;
import static org.flexlb.dao.loadbalance.Response.buildErrorResponse;
import static org.flexlb.dao.loadbalance.Response.buildSuccessResponse;

import io.opentelemetry.context.Context;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 单个请求的运行状态：输入快照、路由与投递所有权、前端结果、取消及资源清理进度。
 *
 * <p>Review 时需要区分三个完成点：future 完成表示调度结果已发布；finalOutcome 表示
 * 请求最终结果已冻结；FINISHED 表示本地资源已结算并移交了需要发布的结果。
 * 调度成功回包后，Engine 执行和资源追踪仍可能继续。
 *
 * <p>生命周期的复合判断与修改由本对象的 monitor 保护。带 Locked 后缀的方法要求
 * 调用者持有 synchronized (context)；synchronized 方法自行取得锁。部分包内访问器
 * 不自行加锁，需要结合调用方的持锁范围阅读。volatile 字段不能替代复合操作的锁。
 * 输入及观测用 setter 不全部受此锁保护，应结合初始化与异步发布时机阅读。
 *
 * <p>锁内记录请求事实、校验精确身份并原子选择响应和终态。Scheduler 组织 Endpoint
 * 账本复验、容量通知和后续执行；Future 完成及资源清理在 context 锁外执行。
 */
@ToString(onlyExplicitlyIncluded = true)
public class BalanceContext {

    /** 请求注册后持有其调度器，供异步回调处理请求状态。 */
    private volatile AbstractRequestScheduler scheduler;

    public AbstractRequestScheduler scheduler() { return scheduler; }

    QueuedRequestScheduler queueOwner() {
        return scheduler instanceof QueuedRequestScheduler queue ? queue : null;
    }

    /** 绑定本请求的调度所有者；RequestRepository 在 context 锁内调用，绑定后不可更换 owner。 */
    void bindScheduler(AbstractRequestScheduler owner) {
        checkState(scheduler == null || scheduler == owner, "request scheduler ownership cannot change");
        scheduler = Objects.requireNonNull(owner, "scheduler owner");
    }

    //======================== 输入与观测 =======================//
    @Getter
    private final FlexlbConfig config;

    @Getter
    @Setter
    // 原始请求供初始化和观测使用；注册后的调度需求由 requirements 快照提供。
    private Request request;

    @Getter
    @Setter
    // 完成回调保存的观测结果；不是唯一回包的仲裁状态，仲裁由 selectedResponse 负责。
    private Response response;

    @ToString.Exclude
    private volatile FutureTask<GenerateInputPB> generateInput;

    /** 初始化投递输入；FutureTask 使同一个输入任务最多解析一次，并保留解析异常。 */
    public void setGenerateInputPb(ByteString bytes) {
        generateInput = bytes == null || bytes.isEmpty() ? null
                : new FutureTask<>(() -> GenerateInputPB.parseFrom(bytes));
    }

    boolean hasGenerateInput() {
        return generateInput != null;
    }

    void prepareGenerateInput() {
        FutureTask<GenerateInputPB> input = generateInput;
        if (input != null) { input.run(); }
    }

    /** Read the immutable input; a failed parse is reported only when delivery needs it. */
    GenerateInputPB getGenerateInput() throws InvalidProtocolBufferException, InterruptedException {
        FutureTask<GenerateInputPB> input = generateInput;
        if (input == null) { return null; }
        input.run();
        try {
            return input.get();
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof InvalidProtocolBufferException malformed) { throw malformed; }
            throw Failures.propagate(cause, "Generate input parsing failed");
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw failure;
        }
    }

    /**
     * Captured at RPC entry and carried through asynchronous scheduling.
     */
    @ToString.Exclude
    @Getter
    @Setter
    private Context traceContext;

    //======================== 注册与前端结果 ========================//
    @Getter
    // 注册后是 RequestFuture；投递 ACK 可使它完成，但不会因此结束 Engine 资源追踪。
    private volatile CompletableFuture<Response> future;

    /** Scheduling input frozen before request registration becomes visible. */
    @Getter
    private RequestRequirements requirements;

    /** External callers may initialize a context; registered futures cannot be replaced or rebound. */
    public synchronized void setFuture(CompletableFuture<Response> future) {
        checkState(!(this.future instanceof RequestFuture)
                && !(future instanceof RequestFuture)
                || this.future == future,
                "registered request future cannot be replaced or rebound");
        this.future = future;
    }

    //======================== Meters =======================//
    @Getter
    @Setter
    private long startTime = System.currentTimeMillis();

    /**
     * Monotonic timestamp captured when server-side request processing starts.
     */
    @Getter
    @Setter
    private long serviceStartNanos = System.nanoTime();

    /**
     * Timestamp (ms) when the request entered the gRPC server pipeline,
     * recorded by {@code GrpcServerTimingInterceptor}. Used to split the
     * total arrival delay into network delay and gRPC server processing time.
     * Remains 0 if the interceptor did not set it (e.g. non-gRPC code path).
     */
    @Getter
    @Setter
    private long grpcEntryTime;

    /**
     * Monotonic counterpart of {@link #grpcEntryTime} for duration measurements.
     */
    @Getter
    @Setter
    private long grpcEntryNanos;

    /**
     * Monotonic timestamp immediately before the request enters its worker batcher.
     */
    @Getter
    @Setter
    private long routeSubmittedNanos;

    /**
     * Monotonic timestamp immediately before the batch is dispatched to the engine.
     */
    @Getter
    @Setter
    private long batchDispatchedNanos;

    @Getter
    @Setter
    private long enqueueTime;

    /**
     * Stable worker-queue age and tie-breaker across local route withdrawals.
     */
    @Getter
    private long firstWorkerEnqueueTime;

    @Getter
    private long workerEnqueueSequence;

    /**
     * Timestamp (ms) when the engine acknowledges the batch in BATCH mode.
     * Set when the owning scheduler accepts a successful batch delivery result.
     * Used to measure acknowledgement-to-response latency in the API layer.
     * Remains 0 for non-BATCH paths or when ACK was not received.
     */
    @Getter
    @Setter
    private long ackAtMs;

    /**
     * Monotonic counterpart of {@link #ackAtMs}.
     */
    @Getter
    @Setter
    private long ackAtNanos;

    /**
     * Failure-time scheduling evidence exported in the completed PV log.
     */
    @Getter
    @Setter
    private volatile Map<String, Object> schedulingDiagnostics;

    //===================== Scheduling =================//
    /**
     * 注册后不可替换的优先级与绝对调度截止时间，时间单位为 Unix epoch 毫秒。
     * 当前 RPC 入口根据 QUEUE 配置和 startTime 计算截止时间；DIRECT 使用 Long.MAX_VALUE。
     * BATCH 是投递方式，不另起一份调度超时；重试和路由撤回不重置该时间。
     * 未显式设置 metadata 的内部 context 在 activate 时从 config 补齐。
     */
    @Getter
    private SchedulingMetadata schedulingMetadata;

    public synchronized void setSchedulingMetadata(SchedulingMetadata metadata) {
        checkState(!(this.future instanceof RequestFuture) || schedulingMetadata == metadata,
                "registered scheduling metadata is immutable");
        schedulingMetadata = metadata;
    }

    /**
     * priority scheduling plan type that finally placed the request:
     * normal / decode_evict. Empty when not applicable.
     */
    @Getter
    @Setter
    private String planType = "";

    /**
     * Cost of the committed eviction plan; 0 for normal placement.
     */
    @Getter
    @Setter
    private long planCost;

    /**
     * Victims preempted to place this request; 0 for normal placement.
     */
    @Getter
    @Setter
    private int victimCount;

    //===================== Method ===================//
    public BalanceContext(FlexlbConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    @ToString.Include
    public long getRequestId() {
        return future instanceof RequestFuture ? requirements.requestId() : request.getRequestId();
    }

    /**
     * Normalized priority of the request (1-100, higher = more important).
     * Immutable scheduling metadata is authoritative; the request fallback
     * supports manually constructed internal contexts.
     */
    public int getPriority() {
        return schedulingMetadata != null ? schedulingMetadata.priority() : request.getPriority();
    }

    /**
     * 调度阶段的绝对截止时间，单位为 Unix epoch 毫秒；不是 Engine 生成超时。
     * metadata 优先，未初始化时由 config 和 startTime 计算。
     */
    public long getRequestExpiresAtMs() {
        if (schedulingMetadata != null) {
            return schedulingMetadata.expiresAtMs();
        }
        return config.resolveExpiresAtMs(startTime);
    }

    public boolean requestExpired(long nowMs) {
        long expiresAtMs = getRequestExpiresAtMs();
        return expiresAtMs <= 0 || nowMs >= expiresAtMs;
    }

    //======================== 生命周期与资源所有权 ========================//
    // 下列运行状态通常在 context 锁内访问；注册前不要调用依赖 RequestFuture 的方法。
    private long createdAtMs;

    /** 已确认投递并准备发布成功结果；不代表 Engine 已执行完成。 */
    private boolean deliveryAcknowledged;

    /** 首次终态决策冻结的结果；此时资源仍可能处于 FINALIZING 清理阶段。 */
    private TerminalOutcome finalOutcome;

    private long updatedAtMs;

    private String detail = "queued";

    /** 当前投递的精确身份及发送/清理事实，不能只凭 requestId 接收异步回调。 */
    private DeliveryClaim delivery;
    /** 已取得的终态执行权，防止多个结束事件重复领取清理和发布责任。 */
    private TerminalAction terminalAction;

    public synchronized DeliveryClaim delivery() { return delivery; }
    boolean hasTerminalAction() { requireContextLock("terminal action lookup"); return terminalAction != null; }

    private long batchId;

    private long batchEnqueueStartedAtMs;

    /**
     * Routing identity survives handoff and is used to reject late facts.
     */
    private RequestRoute route;

    /**
     * The sole writable request stage; public Phase is projected from this and exact facts.
     */
    private volatile RequestStage stage = RequestStage.QUEUED;
    private volatile long placementSequence;

    long placementSequence(long initial) {
        if (placementSequence == 0L) { placementSequence = initial; }
        return placementSequence;
    }

    long placementSequence() { return placementSequence; }

    /** 已有 Decode 接收/结束证据；与投递 ACK 分开记录。 */
    private boolean decodeAccepted;

    /** 已观察到 Prefill 活跃事实；完成时间另由 prefillCompletedAtMs 表示。 */
    private boolean prefillObserved;

    /**
     * Frozen with the first cancellation; later cleanup cannot reclassify it.
     */
    private Response cancellationResponse;

    /** 首个取消原因，与 cancellationResponse 一起冻结，后续清理不覆盖。 */
    private CancelReason cancellationReason;

    /**
     * 锁内选定的唯一前端结果；真正的 Future 完成在锁外执行。
     * 因此 selectedResponse 非空而 future 尚未完成，是合法的发布中间状态。
     */
    private ResponseResult selectedResponse;

    /** 调度截止定时器：跟踪注册到投递确认，ACK 时移交以取消；终态也可提前移交。 */
    private RequestDeadline requestDeadline;

    /** 预测的 Engine 可见性检查；到期仅标记待确认，不直接证明请求丢失。 */
    private DecisionDeadline decisionDeadline;

    /** 无匹配 Worker 状态的超时检查；在请求执行和必要的清理期间继续追踪。 */
    private InactivityDeadline inactivityDeadline;

    private long inactivityTimeoutMs;

    /** 最近匹配的 Worker 状态时间；注册时间作为初值，BATCH 发送开始也参与计算。 */
    private long lastWorkerStatusAtMs;

    private boolean deliveryPredictionConsumed;

    private long prefillCompletedAtMs;

    private OptionalLong decisionExpiresAtMs = OptionalLong.empty();

    private boolean decisionExpired;

    /** 当前选路/撤回操作；操作期间到达的终态证据先暂存，待 admission 结算。 */
    private AdmissionHandle admission;
    /** 投递失败后的清理进度；响应可以先发布，资源必须等待结算证据。 */
    private CleanupProgress cleanup;
    /** 当前抢占操作的身份及待处理证据；不能与 admission 同时持有。 */
    private PreemptionRegistration preemption;
    private static final long DECODE_HANDOFF_GRACE_MS = 10_000L;

    /**
     * Exact ownership token for one priority-preemption attempt.
     *
     * <p>This class records the attempt-local cancel protocol. BalanceContext decides
     * which request transitions are legal; its scheduler executes the resulting effects.
     * Cancel acknowledgement, request resolution and Decode release proof remain separate facts.</p>
     */
    public static final class PreemptionRegistration {
        final BalanceContext owner;
        private final long attemptToken;
        private final String detail;
        private final CancelTarget cancelTarget;
        private final CompletableFuture<VictimResolution> resolution =
                new CompletableFuture<>();

        private PreemptionCancelPhase phase = PreemptionCancelPhase.CLAIMED;
        private boolean finished;
        private boolean cancelAcknowledged;
        private DeferredTerminal pendingTerminal;
        private boolean pendingDeliveryConfirmation;

        PreemptionRegistration(
                BalanceContext owner,
                long attemptToken,
                String detail,
                CancelTarget cancelTarget) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.attemptToken = attemptToken;
            this.detail = detail == null ? "priority preemption" : detail;
            this.cancelTarget = Objects.requireNonNull(cancelTarget, "cancelTarget");
        }

        public AbstractRequestScheduler scheduler() { return owner.scheduler(); }

        public CancelTarget cancelTarget() { return cancelTarget; }

        public long requestId() {
            return owner.getRequestId();
        }

        public long attemptToken() {
            return attemptToken;
        }

        public CompletionStage<VictimResolution> requestResolution() {
            return resolution;
        }

        boolean signalResolution(VictimResolution exactResolution) {
            return resolution.complete(exactResolution);
        }

        String detail() {
            return detail;
        }

        DeferredTerminal pendingTerminal() {
            return pendingTerminal;
        }

        boolean hasPendingDeliveryConfirmation() {
            return pendingDeliveryConfirmation;
        }

        private boolean advanceTo(PreemptionCancelPhase next) {
            owner.requireContextLock("preemption phase change");
            if (finished || !phase.canTransitionTo(next)) {
                return false;
            }
            phase = next;
            cancelAcknowledged |= next == PreemptionCancelPhase.CANCEL_REQUESTED;
            return true;
        }

        /** Record protocol completion once; its scheduler still owns resource cleanup and resolution notification. */
        private boolean tryFinish() {
            owner.requireContextLock("preemption completion");
            if (finished) {
                return false;
            }
            finished = true;
            return true;
        }

        boolean isReleasable() {
            return !finished && phase.isLocallyReleasable();
        }

        boolean isCancelRequested() { return cancelAcknowledged; }

        boolean isNotFound() {
            return !finished && phase == PreemptionCancelPhase.NOT_FOUND_STALE;
        }

        boolean isUnknown() {
            return !finished && phase == PreemptionCancelPhase.CANCEL_UNKNOWN;
        }

        boolean isFinished() {
            return finished;
        }

        boolean canAcceptPriorityTerminal() { return !finished && phase.acceptsPriorityTerminal(); }

        boolean canCompletePreemption() {
            return !finished && phase.acceptsRequestFenced();
        }

        private void storeTerminal(DeferredTerminal selected) {
            owner.requireContextLock("preemption terminal retention");
            pendingTerminal = selected;
        }

    }

    RequestFuture future() {
        return (RequestFuture) this.future;
    }

    boolean ownsFuture(CompletableFuture<?> expected) {
        return this.future() == expected;
    }

    RequestState snapshot() {
        synchronized (this) {
            return new RequestState(this.getRequestId(), this.publicPhaseLocked(), this.deliveryClaimKind(), this.batchId, this.createdAtMs, this.updatedAtMs, this.detail);
        }
    }

    /** 对外 Phase 是投影：最终结果 > 取消请求 > 投递 ACK > 当前阶段，不是第二份状态机。 */
    private RequestState.Phase publicPhaseLocked() {
        if (this.finalOutcome != null) {
            return this.finalOutcome.phase();
        }
        if ((this.cancellationReason != null || preemption != null && preemption.isCancelRequested())) {
            return RequestState.Phase.CANCEL_REQUESTED;
        }
        if (this.deliveryAcknowledged) {
            return RequestState.Phase.ACKNOWLEDGED;
        }
        return this.stage == RequestStage.DELIVERING || this.deliveryClaimKind() != DeliveryClaimKind.NONE ? RequestState.Phase.DISPATCHING : RequestState.Phase.QUEUED;
    }

    RequestRoute activeRoute() {
        synchronized (this) {
            return this.ownsActiveGenerationLocked() ? this.route : null;
        }
    }

    boolean ownsActiveRoute(RequestRoute expected) {
        synchronized (this) {
            return this.ownsActiveGenerationLocked() && this.route == expected;
        }
    }

    boolean isOpen() {
        synchronized (this) {
            return this.stage.isActive() && this.cancellationReason == null && !this.future.isDone() && this.finalOutcome == null;
        }
    }

    boolean isLiveGeneration() {
        synchronized (this) {
            return this.stage != RequestStage.FINISHED;
        }
    }

    boolean ownsActiveGenerationLocked() {
        this.requireContextLock("active generation lookup");
        return this.stage.isActive() && this.finalOutcome == null;
    }

    /** FINALIZING 仍可能接受结算证据，不能把“已有最终结果”当作“无需再追踪资源”。 */
    boolean ownsResourceTrackingLocked() {
        this.requireContextLock("resource tracking lookup");
        return (this.stage.isActive()
                || this.stage == RequestStage.FINALIZING && (this.cleanup != null || delivery != null));
    }

    private void beginFinalizationLocked(TerminalOutcome outcome) {
        this.requireContextLock("begin finalization");
        if (this.finalOutcome != null) { return; }
        this.advanceStageLocked(RequestStage.FINALIZING);
        this.finalOutcome = Objects.requireNonNull(outcome, "outcome");
        this.detail = outcome.detail() == null ? "" : outcome.detail();
        this.updatedAtMs = System.currentTimeMillis();
        this.assertInvariantLocked();
    }

    private void assertInvariantLocked() {
        this.requireContextLock("request context invariant");
        boolean inconsistentRoute = switch (stage) {
            case QUEUED -> route != null && admission == null;
            case READY_TO_DELIVER -> route == null || delivery != null;
            case DELIVERING -> route == null || deliveryClaimKind() == DeliveryClaimKind.NONE;
            default -> false;
        };
        if (inconsistentRoute) {
            throw new IllegalStateException("request stage has inconsistent route for " + this.getRequestId());
        }
        if (admission != null && preemption != null) {
            throw new IllegalStateException("admission handle overlaps preemption for " + this.getRequestId());
        }
        if (this.stage == RequestStage.FINALIZING && this.cleanup == null && admission != null) {
            throw new IllegalStateException("terminalizing request still owns admission " + this.getRequestId());
        }
        if (this.stage != RequestStage.FINISHED) {
            return;
        }
        if (this.route != null || preemption != null || admission != null || this.requestDeadline != null || this.decisionDeadline != null || this.inactivityDeadline != null) {
            throw new IllegalStateException("terminal record retains request-owned state for " + this.getRequestId());
        }
        if (this.finalOutcome == null) {
            throw new IllegalStateException("terminal record lifecycle is not terminal for " + this.getRequestId());
        }
    }

    void retainAdmissionTerminalLocked(DeferredTerminal candidate) {
        DeferredTerminal previous = admission.observedTerminal;
        if (previous == null || !previous.endpointAlreadyRetired() && (candidate.endpointAlreadyRetired() || !previous.authoritativeWorker() && candidate.authoritativeWorker())) {
            admission.observedTerminal = candidate;
        }
    }

    void retainAdmissionPrefillRetirementLocked(PendingPrefillRetirement candidate) {
        if (admission.prefillRetirement == null) {
            admission.prefillRetirement = candidate;
            return;
        }
        if (admission.prefillRetirement.source != candidate.source || admission.prefillRetirement.item != candidate.item) {
            throw new IllegalStateException("admission observed another Prefill generation for request " + this.getRequestId());
        }
    }

    boolean ownsPrefillRouteLocked(PrefillEndpoint source, RequestRoute expected) {
        this.requireContextLock("Prefill route ownership lookup");
        return this.ownsResourceTrackingLocked() && this.route == expected && expected.prefillEp() == source;
    }

    boolean ownsDecodeReservationLocked(DecodeEndpoint source, DecodeResources.ReservationHandle reservation) {
        this.requireContextLock("Decode reservation ownership lookup");
        return this.ownsResourceTrackingLocked() && this.route != null && this.route.decodeEp() == source && reservation.equals(this.route.decodeReservation());
    }

    DecisionDeadline markDecodeAcceptedLocked() {
        this.requireContextLock("Decode acceptance");
        if (!this.ownsActiveGenerationLocked()) {
            return null;
        }
        this.decodeAccepted = true;
        if (this.stage == RequestStage.DELIVERING) {
            this.advanceStageLocked(RequestStage.RESULT_PENDING);
        }
        this.setDecisionDeadlineLocked(OptionalLong.empty());
        this.reconcileDecisionEvidenceLocked();
        DecisionDeadline detachedDeadline = this.detachDecisionDeadlineLocked();
        this.assertInvariantLocked();
        return detachedDeadline;
    }

    /** Returns the exact route whose pending cancellation the Worker queue can process now. */
    RequestRoute pendingWorkerQueueCancellationLocked() {
        requireContextLock("worker queue cancellation");
        if (queueOwner() == null || stage != RequestStage.READY_TO_DELIVER) {
            return null;
        }
        // Admission and preemption must settle before the queue can cancel this route.
        if (admission != null || preemption != null || cancellationReason == null) {
            return null;
        }
        // READY_TO_DELIVER guarantees a route and no delivery claim.
        return route.prefillEp() == null ? null : route;
    }

    boolean hasPendingGlobalControl() {
        synchronized (this) {
            return this.stage == RequestStage.QUEUED && this.finalOutcome == null && (this.cancellationReason != null);
        }
    }

    boolean canRestoreGlobalQueue() {
        synchronized (this) {
            return this.stage == RequestStage.QUEUED && admission == null && this.isOpen();
        }
    }

    CancelReason requireCancellationFirstCauseLocked() {
        if (this.cancellationReason == null) {
            throw new IllegalStateException("missing cancellation first cause for request " + this.getRequestId());
        }
        return this.cancellationReason;
    }

    StrategyErrorType cancellationErrorTypeLocked(CancelReason reason) {
        this.requireContextLock("cancellation error lookup");
        return reason == CancelReason.DEADLINE_EXCEEDED ? StrategyErrorType.BATCH_SLO_EXPIRED : StrategyErrorType.REQUEST_CANCELLED;
    }

    /** Local termination is available before execution; a selected response is already owned. */
    boolean canFinalizeBeforeExecutionLocked() {
        this.requireContextLock("local terminal eligibility");
        if (!ownsActiveGenerationLocked() || admission != null || preemption != null) {
            return false;
        }
        if (decodeAccepted || deliveryAcknowledged || deliveryClaimKind().isClaimed()) {
            return false;
        }
        return !future().isDone() || future().isCancelled();
    }

    boolean installRequestDeadline(RequestDeadline exact) {
        synchronized (this) {
            if (!this.isOpen()) {
                return false;
            }
            if (this.requestDeadline != null) {
                throw new IllegalStateException("request deadline already installed for " + this.getRequestId());
            }
            this.requestDeadline = exact;
            this.assertInvariantLocked();
            return true;
        }
    }

    void configureInactivityTimeout(long timeoutMs) {
        synchronized (this) {
            checkArgument(timeoutMs > 0L, "request inactivity timeout must be positive");
            this.inactivityTimeoutMs = timeoutMs;
        }
    }

    OptionalLong inactivityDeadlineAtMs() {
        synchronized (this) {
            return this.ownsResourceTrackingLocked() && this.inactivityDeadline == null && (this.cleanup == null || !this.cleanup.expired && this.cleanup.phase != CleanupProgress.Phase.FINISHED) && this.inactivityTimeoutMs > 0L ? OptionalLong.of(this.inactivityExpiresAtMsLocked()) : OptionalLong.empty();
        }
    }

    boolean installInactivityDeadline(InactivityDeadline exact) {
        synchronized (this) {
            if (this.inactivityDeadlineAtMs().isEmpty()) {
                return false;
            }
            this.inactivityDeadline = exact;
            return true;
        }
    }

    boolean consumeInactivityDeadlineLocked(InactivityDeadline exact) {
        this.requireContextLock("request inactivity check");
        if (this.inactivityDeadline != exact || !this.ownsResourceTrackingLocked()) {
            return false;
        }
        this.inactivityDeadline = null;
        return true;
    }

    boolean requestInactiveLocked(long nowMs) {
        return this.inactivityTimeoutMs > 0L && nowMs >= this.inactivityExpiresAtMsLocked();
    }

    private long inactivityExpiresAtMsLocked() {
        long observedSince = this.lastWorkerStatusAtMs;
        if (this.deliveryClaimKind() == DeliveryClaimKind.BATCH_ENQUEUE) {
            observedSince = Math.max(observedSince, this.batchEnqueueStartedAtMs);
        }
        return deadlineAfter(observedSince, this.inactivityTimeoutMs);
    }

    /**
     * 消费一次投递工作量预测，推算 Engine 可见性截止时间。
     * 真实的 Prefill/Decode 证据优先于预测；这里只记录请求的预测事实。
     */
    void recordDeliveryPredictionLocked(WorkSnapshot precedingWork, long unstartedWorkMs, long nowMs) {
        this.requireContextLock("delivery prediction consumption");
        Objects.requireNonNull(precedingWork, "precedingWork");
        checkArgument(unstartedWorkMs >= 0L, "unstarted work must be non-negative");
        checkState(!this.deliveryPredictionConsumed, "delivery prediction already consumed");
        double lifetime = this.route.ctx().getConfig().getRequestLifecycle().getDecision().getLifetime();
        checkArgument(Double.isFinite(lifetime) && !(lifetime < 1.0), "invalid decision lifetime");
        this.deliveryPredictionConsumed = true;
        if (!this.decodeAccepted) {
            if (this.prefillCompletedAtMs > 0L) {
                if (this.route.decodeEp() != null) {
                    this.decisionExpiresAtMs = OptionalLong.of(deadlineAfter(this.prefillCompletedAtMs, DECODE_HANDOFF_GRACE_MS));
                }
            } else if (!this.prefillObserved) {
                OptionalLong precedingMs = precedingWork.totalRemainingWorkMsAt(nowMs);
                if (precedingMs.isPresent()) {
                    long remainingMs = saturatedAdd(precedingMs.getAsLong(), unstartedWorkMs);
                    double scaled = Math.ceil(remainingMs * lifetime);
                    long durationMs = scaled >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) scaled;
                    this.decisionExpiresAtMs = OptionalLong.of(deadlineAfter(nowMs, saturatedAdd(durationMs, DECODE_HANDOFF_GRACE_MS)));
                }
            }
        }
    }

    private void setDecisionDeadlineLocked(OptionalLong deadline) {
        this.decisionExpiresAtMs = deadline;
        this.decisionExpired = false;
    }

    OptionalLong decisionDeadlineAtMs() {
        synchronized (this) {
            return this.ownsActiveGenerationLocked() && this.decisionDeadline == null ? this.decisionExpiresAtMs : OptionalLong.empty();
        }
    }

    boolean installDecisionDeadline(DecisionDeadline exact) {
        synchronized (this) {
            if (!this.ownsActiveGenerationLocked() || this.decisionDeadline != null || !this.decisionExpiresAtMs.equals(OptionalLong.of(exact.deadlineAtMs()))) {
                return false;
            }
            this.decisionDeadline = exact;
            return true;
        }
    }

    /** 只消费当前定时器身份；过期的旧回调不能覆盖已到达的 Engine 事实。 */
    void onDecisionVisibilityDeadline(DecisionDeadline exact) {
        synchronized (this) {
            if (this.decisionDeadline != exact) {
                return;
            }
            this.decisionDeadline = null;
            // An Engine fact may have changed the phase before the old timer fired.
            if (!this.decisionExpiresAtMs.equals(OptionalLong.of(exact.deadlineAtMs()))) {
                return;
            }
            this.decisionExpired = true;
            this.decisionExpiresAtMs = OptionalLong.empty();
            if (this.needsDecisionConfirmationLocked()) {
                this.markAwaitingConfirmationLocked(this.prefillCompletedAtMs == 0L ? "no Engine request evidence before visibility deadline" : "Decode acceptance missing after Prefill completion");
            }
            this.assertInvariantLocked();
        }
    }

    boolean needsDecisionConfirmationLocked() {
        return this.ownsActiveGenerationLocked() && this.route != null && this.cancellationReason == null && (this.decisionExpired && (!this.decodeAccepted && (!this.prefillObserved || this.prefillCompletedAtMs > 0L)));
    }

    void markAwaitingConfirmationLocked(String message) {
        this.requireContextLock("delivery confirmation wait");
        if (!this.ownsActiveGenerationLocked() || this.cancellationReason != null || this.decodeAccepted || this.prefillObserved && this.prefillCompletedAtMs == 0L) {
            return;
        }
        this.detail = "SUSPECTED_LOST: " + Objects.requireNonNull(message, "message");
        this.updatedAtMs = System.currentTimeMillis();
    }

    void reconcileDecisionEvidenceLocked() {
        this.requireContextLock("decision evidence reconciliation");
        if (!this.ownsActiveGenerationLocked() || (!this.prefillObserved && !this.decodeAccepted && this.prefillCompletedAtMs == 0L) || this.needsDecisionConfirmationLocked() || this.cancellationReason != null) {
            return;
        }
        if (this.detail.startsWith("SUSPECTED_LOST")) {
            this.detail = "Engine request observed; waiting for completion";
        }
    }

    DecisionDeadline detachObsoleteDecisionDeadlineLocked() {
        this.requireContextLock("decision deadline reconciliation");
        if (this.decisionDeadline == null || this.decisionExpiresAtMs.equals(OptionalLong.of(this.decisionDeadline.deadlineAtMs()))) {
            return null;
        }
        return this.detachDecisionDeadlineLocked();
    }

    private DecisionDeadline detachDecisionDeadlineLocked() {
        DecisionDeadline deadline = this.decisionDeadline;
        this.decisionDeadline = null;
        return deadline;
    }

    ExpirationTimer.DetachedDeadlines detachDeadlines() {
        synchronized (this) {
            ExpirationTimer.DetachedDeadlines detached = new ExpirationTimer.DetachedDeadlines(this.requestDeadline, this.decisionDeadline, this.inactivityDeadline);
            this.requestDeadline = null;
            this.decisionDeadline = null;
            this.inactivityDeadline = null;
            this.assertInvariantLocked();
            return detached;
        }
    }

    PreemptionRegistration tryInstallPreemption(DecodeResources.ReservationHandle exact, long attemptToken, String detail) {
        synchronized (this) {
            DecodeResources.ReservationHandle reservation = this.route == null ? null : this.route.decodeReservation();
            if (!this.ownsActiveGenerationLocked() || admission != null || preemption != null || this.cancellationReason != null || reservation == null || !Objects.equals(reservation, exact) || (this.deliveryClaimKind() == DeliveryClaimKind.ROUTE_DECISION && !this.deliveryAcknowledged)) {
                return null;
            }
            var prefill = route.prefill();
            CancelTarget cancelTarget = prefill == null ? null
                    : new CancelTarget(prefill.getServerIp(), prefill.getGrpcPort());
            if (cancelTarget == null || !cancelTarget.isRoutable()) {
                throw new IllegalStateException("Priority victim has no routable Cancel target request_id=" + getRequestId());
            }
            preemption = new PreemptionRegistration(this, attemptToken, detail, cancelTarget);
            this.assertInvariantLocked();
            return preemption;
        }
    }

    void retainPreemptionTerminalLocked(PreemptionRegistration exact, DeferredTerminal candidate) {
        this.requireContextLock("preemption terminal evidence");
        DeferredTerminal previous = exact.pendingTerminal();
        if (previous == null || !previous.authoritativeWorker() && candidate.authoritativeWorker()) {
            exact.storeTerminal(candidate);
        }
    }

    void detachPreemptionOwnerLocked(PreemptionRegistration exact) {
        this.requireContextLock("preemption detach");
        if (exact != null && preemption == exact) {
            this.preemption = null;
            this.assertInvariantLocked();
        }
    }

    /** Apply an exact Decode reconciliation to request participation and its selected end atomically. */
    TerminalAction applyPreemptionReconciliationLocked(PreemptionRegistration exact, DeferredTerminal terminal,
            boolean decodeSettled, Supplier<PublicationPermit> publication) {
        requireContextLock("preemption resource reconciliation");
        checkArgument(exact.owner == this, "preemption belongs to another request");
        if (terminal != null) {
            retainPreemptionTerminalLocked(exact, terminal);
            exact.tryFinish();
        }
        if (cleanup != null) {
            if (terminal != null) { recordCleanupSettlement(false, true, false); }
            else { detachPreemptionOwnerLocked(exact); }
            return null;
        }
        detachPreemptionOwnerLocked(exact);
        if (terminal == null) { return null; }
        TerminalAction action = decideRequestEndLocked(terminal, publication);
        if (action != null && decodeSettled) { recordCleanupSettlement(false, true, false); }
        return action;
    }

    /** Record proved priority cancellation and claim the existing request-finalization protocol. */
    TerminalAction claimPreemptedRequestEndLocked(PreemptionRegistration exact, String detail,
            boolean prefillSettled, Supplier<PublicationPermit> publication) {
        requireContextLock("priority cancellation settlement");
        checkState(preemption == exact && ownsResourceTrackingLocked() && !exact.isFinished(),
                "priority cancellation must own the current unfinished claim");
        exact.tryFinish();
        if (cleanup != null) {
            recordCleanupSettlement(prefillSettled, true, false);
            return null;
        }
        DeferredTerminal terminal = DeferredTerminal.priority(detail);
        retainPreemptionTerminalLocked(exact, terminal);
        detachPreemptionOwnerLocked(exact);
        TerminalAction action = decideRequestEndLocked(terminal, publication);
        if (action != null) { recordCleanupSettlement(prefillSettled, true, false); }
        return action;
    }

    void requireCleanupOwner(TerminalAction action) {
        synchronized (this) {
            if (this.stage != RequestStage.FINALIZING || action.requestContext() != this || action.item() != this.route) {
                throw new IllegalStateException("cleanup does not own request " + this.getRequestId());
            }
        }
    }

    ResponseResult claimPublicationResultLocked(PublicationKind kind,
            ResponseCompletion completion, Response response, Throwable failure, boolean interrupt) {
        this.requireContextLock("frontend result selection");
        if (this.future().isDone()) { return null; }
        ResponseResult selected = this.selectedResponse;
        if (selected != null) {
            return selected.completion() == completion && selected.response() == response
                    && selected.failure() == failure && selected.interrupt() == interrupt ? selected : null;
        }
        if (kind == PublicationKind.DELIVERY && (!this.ownsActiveGenerationLocked()
                || !this.deliveryAcknowledged || this.cancellationReason != null)) {
            return null;
        }
        if (kind == PublicationKind.TERMINAL && this.finalOutcome == null && this.cancellationReason == null) {
            return null;
        }
        this.selectedResponse = new ResponseResult(completion, response, failure, interrupt);
        return this.selectedResponse;
    }

    void requireContextLock(String operation) {
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException(operation + " requires context lock for request " + this.getRequestId());
        }
    }

    void requireOutsideContextLock(String operation) {
        checkState(!Thread.holdsLock(this), "%s must run outside the BalanceContext lock", operation);
    }

    private void advanceStageLocked(RequestStage next) {
        this.requireContextLock("request stage transition");
        boolean allowed = switch(this.stage) {
            case QUEUED ->
                next == RequestStage.ROUTING || next == RequestStage.FINALIZING;
            case ROUTING ->
                next == RequestStage.QUEUED || next == RequestStage.READY_TO_DELIVER || next == RequestStage.FINALIZING;
            case READY_TO_DELIVER ->
                next == RequestStage.ROUTING || next == RequestStage.DELIVERING || next == RequestStage.FINALIZING;
            case DELIVERING ->
                next == RequestStage.RESULT_PENDING || next == RequestStage.FINALIZING;
            case RESULT_PENDING ->
                next == RequestStage.FINALIZING;
            case FINALIZING ->
                next == RequestStage.FINISHED;
            case FINISHED ->
                false;
        };
        if (!allowed) {
            throw new IllegalStateException("invalid request stage " + this.stage + " -> " + next);
        }
        this.stage = next;
    }

    private static long deadlineAfter(long startedAtMs, long durationMs) {
        checkArgument(startedAtMs >= 0L && durationMs > 0L, "deadline requires a valid start and positive duration");
        return saturatedAdd(startedAtMs, durationMs);
    }

    /**
     * 一次选路或撤回操作的身份；finish/terminate 通过 CAS 只提交一次完成回调。
     * close 只检查是否显式结束，不自动回滚或完成操作。
     */
    public static final class AdmissionHandle implements AutoCloseable {

        private final AtomicBoolean resolved = new AtomicBoolean();

        private final BalanceContext owner;
        private RequestRoute withdrawingRoute;
        private boolean inactivityExpired;
        private DeferredTerminal observedTerminal;
        private PendingPrefillRetirement prefillRetirement;

        private final BiConsumer<AdmissionHandle, Response> completion;

        private AdmissionHandle(BalanceContext owner, BiConsumer<AdmissionHandle, Response> completion) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.completion = Objects.requireNonNull(completion, "completion");
        }

        public BalanceContext owner() { return owner; }
        public RequestRoute withdrawingRoute() { return withdrawingRoute; }

        /**
         * A late placement attempt must not overwrite the frozen cancellation evidence.
         */
        void recordDiagnostics(Map<String, Object> diagnostics) {
            if (diagnostics == null) {
                return;
            }
            synchronized (owner) {
                if (!resolved.get() && owner.admission == this && owner.cancellationResponse == null) {
                    owner.setSchedulingDiagnostics(diagnostics);
                }
            }
        }

        /**
         * Transfer this exact admission attempt to canonical terminal ownership.
         */
        public void terminate(Response failure) {
            Objects.requireNonNull(failure, "failure");
            checkArgument(!failure.isSuccess(), "admission termination requires a failure");
            if (resolved.compareAndSet(false, true)) {
                completion.accept(this, failure);
            }
        }

        /**
         * Finish this admission attempt after success or without committed side effects.
         */
        public void finish() {
            if (resolved.compareAndSet(false, true)) {
                completion.accept(this, null);
            }
        }

        @Override
        public void close() {
            checkState(resolved.get(), "admission must be explicitly finished");
        }
    }

    /** Exact route delivery identity; completion is consumed under the request monitor. */
    public record DeliverySettlement(RequestRoute route, DeliveryClaim.SendOutcome sendOutcome,
                                     CancelReason abandonmentReason, boolean prefillReleaseProven, boolean decodeReleaseProven) { }

    /**
     * 一次已领取的投递及其结算责任，内部事实由 owner 的 monitor 保护。
     * UNKNOWN 表示可能已经发送，不能按 NOT_SENT 释放资源。
     * settlement 等待发送方结束，并取得执行完成或必要的远端清理证据；它不等于调度回包。
     */
    public static final class DeliveryClaim {
        public enum SendOutcome { NOT_STARTED, SENDING, NOT_SENT, PREFILL_REJECTED, DELIVERED, UNKNOWN }
        final RequestRoute item;
        private final DeliveryClaimKind kind;
        private final BalanceContext owner;
        private final java.util.function.Consumer<DeliveryClaim> cleanupStarter;
        private final BiConsumer<DeliveryClaim, DeliveryResult> completion;
        private final CompletableFuture<DeliverySettlement> settled = new CompletableFuture<>();
        private SendOutcome sendOutcome = SendOutcome.NOT_STARTED;
        private boolean senderFinished;
        private CancelReason abandonmentReason;
        private boolean prefillReleaseProven;
        private boolean decodeReleaseProven;
        private boolean executionFinished;

        private DeliveryClaim(RequestRoute item, DeliveryClaimKind kind,
                              BiConsumer<DeliveryClaim, DeliveryResult> completion,
                              java.util.function.Consumer<DeliveryClaim> cleanupStarter) {
            this.item = item;
            this.owner = item.ctx();
            this.kind = kind;
            this.completion = completion;
            this.cleanupStarter = Objects.requireNonNull(cleanupStarter);
            this.decodeReleaseProven = item.decodeEp() == null || item.decodeReservation() == null;
        }

        /** The final request check immediately before adding this member to the actual RPC. */
        public boolean tryStartSend() {
            boolean allowed;
            CancelReason refused;
            synchronized (owner) {
                if (kind != DeliveryClaimKind.BATCH_ENQUEUE || sendOutcome != SendOutcome.NOT_STARTED) {
                    return false;
                }
                long now = System.currentTimeMillis();
                allowed = owner.delivery == this && owner.ownsActiveGenerationLocked()
                        && owner.cancellationReason == null && abandonmentReason == null
                        && !owner.requestExpired(now) && !owner.requestInactiveLocked(now);
                refused = owner.cancellationReason != null ? owner.cancellationReason
                        : abandonmentReason != null ? abandonmentReason
                        : !owner.ownsActiveGenerationLocked() ? CancelReason.SHUTDOWN : CancelReason.DEADLINE_EXCEEDED;
                if (allowed) { sendOutcome = SendOutcome.SENDING; }
            }
            if (!allowed) { abandon(refused); }
            return allowed;
        }

        public void complete(DeliveryResult result) {
            Objects.requireNonNull(result, "delivery result");
            synchronized (owner) {
                checkState(kind == DeliveryClaimKind.BATCH_ENQUEUE, "not a batch delivery");
                if (senderFinished) { throw new IllegalStateException("delivery already completed: " + item.requestId()); }
                senderFinished = true;
                sendOutcome = switch (result.status()) {
                    case NOT_SENT -> SendOutcome.NOT_SENT;
                    case PREFILL_REJECTED -> SendOutcome.PREFILL_REJECTED;
                    case DELIVERED -> SendOutcome.DELIVERED;
                    case UNCERTAIN -> SendOutcome.UNKNOWN;
                };
            }
            if (result.failed()) { abandon(CancelReason.CLIENT_CANCELLED); }
            try { completion.accept(this, result); }
            finally { publishSettlement(); }
        }

        void completeRoute() {
            synchronized (owner) { senderFinished = true; sendOutcome = SendOutcome.DELIVERED; }
            publishSettlement();
        }

        public void abandon(CancelReason reason) {
            boolean start;
            synchronized (owner) {
                if (abandonmentReason != null || canReleaseLocked()) { return; }
                abandonmentReason = Objects.requireNonNull(reason);
                if (kind == DeliveryClaimKind.ROUTE_DECISION) { senderFinished = true; }
                start = kind == DeliveryClaimKind.BATCH_ENQUEUE && !settled.isDone();
            }
            if (start) { cleanupStarter.accept(this); }
            publishSettlement();
        }

        record CleanupEvidence(CancelReason reason, boolean needsRemoteCancel, boolean remoteSettled) { }

        CleanupEvidence cleanupEvidence() {
            synchronized (owner) {
                return new CleanupEvidence(abandonmentReason,
                        sendOutcome != SendOutcome.NOT_STARTED && sendOutcome != SendOutcome.NOT_SENT,
                        prefillReleaseProven && decodeReleaseProven);
            }
        }

        void acceptCleanupAck(org.flexlb.balance.eviction.EngineCancelChannel.CancelAck ack) {
            synchronized (owner) {
                if (ack == org.flexlb.balance.eviction.EngineCancelChannel.CancelAck.REQUEST_CLEANED) {
                    prefillReleaseProven = true;
                    decodeReleaseProven = true;
                } else if (ack == org.flexlb.balance.eviction.EngineCancelChannel.CancelAck.REQUEST_FENCED) {
                    prefillReleaseProven = true;
                }
            }
            publishSettlement();
        }

        void recordDecodeTerminalStatus(DecodeEndpoint source, DecodeResources.DecodeRequestStatus requestStatus) {
            synchronized (owner) {
                if (source != item.decodeEp() || !Objects.equals(requestStatus.reservation(), item.decodeReservation())
                        || requestStatus.kind() != DecodeResources.DecodeRequestStatus.Kind.TERMINAL) { return; }
                decodeReleaseProven = true;
                executionFinished = true;
            }
            publishSettlement();
        }

        void recordWorkerCompletion(RequestRoute exact) {
            synchronized (owner) {
                if (exact != item) { return; }
                executionFinished = true;
                // A terminal Route decision revokes address publication; Batch still awaits its sender.
                if (kind == DeliveryClaimKind.ROUTE_DECISION) { senderFinished = true; }
            }
            publishSettlement();
        }

        void recordEndpointRetirement(org.flexlb.balance.endpoint.WorkerEndpoint source) {
            synchronized (owner) {
                if (source instanceof DecodeEndpoint decode && !decode.isRetired()) { return; }
                if (source == item.prefillEp()) { prefillReleaseProven = true; }
                if (source == item.decodeEp()) { decodeReleaseProven = true; executionFinished = true; }
            }
            publishSettlement();
        }

        private boolean canReleaseLocked() {
            owner.requireContextLock("delivery release evidence");
            return senderFinished && (kind == DeliveryClaimKind.ROUTE_DECISION || sendOutcome == SendOutcome.NOT_SENT
                    || (abandonmentReason == null ? executionFinished : prefillReleaseProven && decodeReleaseProven));
        }

        /** Queue resource cleanup before publishing the evidence awaited by shutdown. */
        private void publishSettlement() {
            DeliverySettlement result;
            boolean cleanupPending;
            synchronized (owner) {
                result = !settled.isDone() && canReleaseLocked()
                        ? new DeliverySettlement(item, sendOutcome, abandonmentReason, prefillReleaseProven, decodeReleaseProven) : null;
                cleanupPending = owner.cleanup != null;
            }
            if (result != null) {
                try {
                    if (cleanupPending && kind == DeliveryClaimKind.BATCH_ENQUEUE) { owner.scheduler().enqueueCleanup(owner); }
                } finally { settled.complete(result); }
            }
        }

        DecodeResources.ReleaseReason provenReleaseReason() {
            synchronized (owner) {
                if (sendOutcome == SendOutcome.NOT_SENT && senderFinished) {
                    return DecodeResources.ReleaseReason.NOT_SENT;
                }
                if (canReleaseLocked() && abandonmentReason != null && prefillReleaseProven && decodeReleaseProven) {
                    return DecodeResources.ReleaseReason.REMOTE_CLEANUP;
                }
                return null;
            }
        }

        public java.util.concurrent.CompletionStage<DeliverySettlement> settlement() { return settled.minimalCompletionStage(); }
        boolean cleanupRequired() { synchronized (owner) { return abandonmentReason != null; } }
    }

    static SelectedResponse selectPublication(BalanceContext ctx, PublicationPermit permit, ResponseCompletion completion, Response response, Throwable failure, boolean mayInterruptIfRunning) {
        ctx.requireOutsideContextLock("response selection");
        checkArgument(permit.requestContext == ctx
                && (completion == ResponseCompletion.RESPONSE
                || permit.kind == PublicationKind.TERMINAL),
                "incompatible publication permit");
        permit.consumeForSelection();
        try {
            synchronized (ctx) {
                return new SelectedResponse(permit, ctx.future(), ctx.claimPublicationResultLocked(permit.kind, completion, response, failure, mayInterruptIfRunning));
            }
        } catch (RuntimeException | Error selectionFailure) {
            permit.closePublication();
            throw selectionFailure;
        }
    }

    /**
     * Single-use response selection attempt for an exact request and publication kind.
     * Consuming this permit does not select a winner; selectedResponse records that decision.
     * The associated execution registration keeps shutdown waiting until completion or abandonment.
     */
    static final class PublicationPermit {

        final ResponseCompletionExecutor.CompletionRegistration registration;

        final BalanceContext requestContext;

        final PublicationKind kind;

        private final AtomicBoolean consumed = new AtomicBoolean();

        PublicationPermit(ResponseCompletionExecutor.CompletionRegistration registration, BalanceContext requestContext, PublicationKind kind) {
            this.registration = Objects.requireNonNull(registration);
            this.requestContext = requestContext;
            this.kind = kind;
        }

        BalanceContext requestContext() {
            return requestContext;
        }

        void closePublication() { registration.close(); }

        /**
         * Abandon a permit only when no other submitter consumed it.
         */
        void abandonIfUnused() {
            if (consumed.compareAndSet(false, true)) {
                closePublication();
            }
        }

        void consumeForSelection() {
            if (!consumed.compareAndSet(false, true)) {
                throw new IllegalStateException("response selection permit already consumed for request " + requestContext.getRequestId());
            }
        }
    }

    enum ResponseCompletion {

        RESPONSE, FAILURE, CANCELLATION
    }

    /**
     * Frozen response selection; a null result publishes nothing.
     */
    record SelectedResponse(PublicationPermit permit, RequestFuture future, ResponseResult result) { }

    record ResponseResult(ResponseCompletion completion, Response response, Throwable failure, boolean interrupt) { }

    record PendingPrefillRetirement(PrefillEndpoint source, RequestRoute item, String detail) {
    }

    /**
     * 唯一可写的调度阶段；合法迁移集中在 advanceStageLocked。
     * 正常路径：QUEUED -> ROUTING -> READY_TO_DELIVER -> DELIVERING -> RESULT_PENDING
     * -> FINALIZING -> FINISHED。选路失败/撤回可回退，取消或失败可提前进入 FINALIZING。
     */
    enum RequestStage {

        /** Waiting for a routing attempt. */
        QUEUED,
        /** Selecting endpoints, committing a route, or withdrawing an unsent route. */
        ROUTING,
        /** Route and resource reservations are committed; sending has not started. */
        READY_TO_DELIVER,
        /** Sending ownership has transferred; acceptance is not yet confirmed. */
        DELIVERING,
        /** Delivery or engine acceptance is confirmed; request work remains. */
        RESULT_PENDING,
        /** Outcome is frozen; resource settlement or terminal publication remains. */
        FINALIZING,
        /** Local resources are settled and the selected result has a publication owner. */
        FINISHED;

        boolean isActive() {
            return this != FINALIZING && this != FINISHED;
        }
    }

    enum PublicationKind {

        DELIVERY, TERMINAL
    }

    record DeliveryPublication(RequestRoute item, Response response, PublicationPermit publication, RequestDeadline requestDeadline, long batchEnqueueStartedAtMs) {
    }

    /**
     * 将 complete/completeExceptionally/cancel 交回调度器仲裁。
     * 发布方通过 *Owned 方法完成底层 Future；完成时的用户回调应在 context 锁外运行。
     * 这里只拦截上述三个入口，并未封装 CompletableFuture 的全部可写接口。
     */
    static final class RequestFuture extends CompletableFuture<Response> {
        @FunctionalInterface
        interface CompletionTarget {
            boolean complete(ResponseCompletion completion, Response response, Throwable failure, boolean interrupt);
        }
        private volatile CompletionTarget target;

        RequestFuture(CompletionTarget target) {
            this.target = Objects.requireNonNull(target, "target");
        }

        @Override
        public boolean complete(Response response) {
            CompletionTarget current = target;
            return current != null && current.complete(
                    ResponseCompletion.RESPONSE, response, null, false);
        }

        @Override
        public boolean completeExceptionally(Throwable error) {
            CompletionTarget current = target;
            return current != null && current.complete(
                    ResponseCompletion.FAILURE, null, error, false);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            CompletionTarget current = target;
            return current != null && current.complete(
                    ResponseCompletion.CANCELLATION, null, null, interrupt);
        }

        boolean completeOwned(Response response) {
            try { return super.complete(response); } finally { target = null; }
        }

        boolean completeExceptionallyOwned(Throwable error) {
            try { return super.completeExceptionally(error); } finally { target = null; }
        }

        boolean cancelOwned(boolean interrupt) {
            try { return super.cancel(interrupt); } finally { target = null; }
        }
    }

    /**
     * 本请求唯一的 Prefill/Decode 结算进度，由 context 锁保护。
     * RUN_AGAIN 表示当前锁外清理期间又来了事件，当前轮完成后必须再执行一轮。
     * expired 是清理阶段的超时事实，不等于两端资源已释放。
     */
    private static final class CleanupProgress {

        // PENDING blocks close before the first pass or while a requested follow-up has not started.
        enum Phase { PENDING, QUEUED, RUNNING, RUN_AGAIN, WAITING, FINISHED }

        final DeliveryResult.Status source;

        Phase phase = Phase.PENDING;

        boolean prefillSettled;

        boolean decodeSettled;

        /**
         * Sticky decision: later Worker activity cannot revoke an already requested expiry.
         */
        boolean expired;
        boolean terminalEffectsFinished;

        CleanupProgress(RequestRoute item, DeliveryResult.Status source) {
            this.source = source;
            prefillSettled = item == null || item.prefillEp() == null;
            decodeSettled = item == null || item.decodeEp() == null || item.decodeReservation() == null;
        }

        boolean ready() {
            return phase == Phase.WAITING && prefillSettled && decodeSettled;
        }
    }
    /**
     * 锁内领取一次终态执行权：冻结结果，并移交路由、抢占和定时器等清理责任。
     * 返回 null 表示当前不能领取或已有执行方；非空 action 由 Scheduler 在锁外执行。
     * 本方法不直接清理 Endpoint，也不直接完成 Future。
     */
    TerminalAction claimFinalizationLocked(DeferredTerminal event, TerminalOutcome transition, Response response, boolean requestPublication, Supplier<PublicationPermit> publication) {
        this.requireContextLock("terminal claim");
        if (transition == null) {
            throw new IllegalStateException("terminal transition is required for request " + this.getRequestId());
        }
        if (terminalAction != null || !this.ownsResourceTrackingLocked() || admission != null || this.cleanup != null && (!this.cleanup.ready() || preemption != null && !preemption.isFinished())) {
            return null;
        }
        boolean publishable = requestPublication && this.selectedResponse == null && !this.future().isDone();
        PublicationPermit permit = publishable ? publication.get() : null;
        boolean transferred = false;
        try {
            PreemptionRegistration claimedPreemption = preemption;
            this.preemption = null;
            if (claimedPreemption != null) {
                claimedPreemption.tryFinish();
            }
            ExpirationTimer.DetachedDeadlines terminalResources = new ExpirationTimer.DetachedDeadlines(
                    requestDeadline, detachDecisionDeadlineLocked(), null);
            requestDeadline = null;
            TerminalAction action = new TerminalAction(this, this.route, claimedPreemption, terminalResources, event, publishable ? response : null, permit);
            terminalAction = action;
            // The selected outcome is visible while unlocked endpoint cleanup runs.
            this.beginFinalizationLocked(transition);
            if (cleanup == null) { cleanup = new CleanupProgress(route, null); }
            if (event != null && (event.decodeTerminalAlreadyApplied() || event.endpointAlreadyRetired())) {
                cleanup.decodeSettled = true;
            }
            if (prefillCompletedAtMs > 0L || event != null && event.kind() == DeferredTerminal.Kind.WORKER
                    && event.workerSource() == WorkerTerminalSource.PREFILL_ENDPOINT) {
                cleanup.prefillSettled = true;
            }
            cleanup.expired |= event != null && event.kind() == DeferredTerminal.Kind.INACTIVITY_EXPIRED;
            transferred = true;
            this.assertInvariantLocked();
            return action;
        } finally {
            if (!transferred && permit != null) {
                permit.abandonIfUnused();
            }
        }
    }
    /** 投递失败可先发布错误，再在 FINALIZING 中等待资源结算；不得据此立即归档请求。 */
    SelectedResponse selectDeliveryFailureLocked(RequestRoute exact, DeliveryResult.Status source, String detail, Supplier<PublicationPermit> publication) {
        this.requireContextLock("request failure");
        if (!this.ownsActiveRoute(exact) || this.cleanup != null) {
            return null;
        }
        CancelReason cancellation = this.cancellationReason;
        String message = cancellation == null ? detail : cancellation.getMessage() + "; " + detail;
        TerminalOutcome outcome = cancellation == null ? TerminalOutcome.fail(message) : TerminalOutcome.cancellation(cancellation, message);
        Response response = cancellation == null ? buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, message) : Response.copyOf(this.cancellationResponse);
        PublicationPermit permit = this.selectedResponse == null && !this.future().isDone() ? publication.get() : null;
        this.cleanup = new CleanupProgress(exact, source);

        this.beginFinalizationLocked(outcome);
        this.decisionExpiresAtMs = OptionalLong.empty();
        if (permit == null) {
            return null;
        }
        this.selectedResponse = new ResponseResult(ResponseCompletion.RESPONSE, response, null, false);
        permit.consumeForSelection();
        return new SelectedResponse(permit, this.future(), this.selectedResponse);
    }

    /** 注册时冻结调度需求并安装受控 Future；RequestRepository 在此后发布 context。 */
    synchronized void activate(RequestFuture prepared) {
        if (future instanceof RequestFuture) {
            throw new IllegalStateException("request context is already registered: " + getRequestId());
        }
        configureInactivityTimeout(config.getRequestLifecycle().getRequest().getTimeoutMs());
        if (schedulingMetadata == null) {
            schedulingMetadata = SchedulingMetadata.explicit(getPriority(), getRequestExpiresAtMs());
        }
        requirements = RequestRequirements.capture(this);
        createdAtMs = System.currentTimeMillis();
        updatedAtMs = createdAtMs;
        lastWorkerStatusAtMs = createdAtMs;
        future = prepared;
    }

    synchronized void initializeWorkerQueue(long nowMs, LongSupplier sequence) {
        if (workerEnqueueSequence == 0L) {
            firstWorkerEnqueueTime = nowMs;
            workerEnqueueSequence = sequence.getAsLong();
        }
    }

    synchronized AdmissionHandle admission() { return admission; }
    synchronized PreemptionRegistration preemption() { return preemption; }
    boolean hasCleanup() { requireContextLock("cleanup lookup"); return cleanup != null; }
    DeliveryResult.Status cleanupSource() { requireContextLock("cleanup lookup"); return cleanup.source; }

    synchronized AdmissionHandle beginAdmission(BiConsumer<AdmissionHandle, Response> completion) {
        if (stage != RequestStage.QUEUED || !isOpen() || route != null || admission != null || preemption != null) {
            return null;
        }
        admission = new AdmissionHandle(this, completion);
        advanceStageLocked(RequestStage.ROUTING);
        assertInvariantLocked();
        return admission;
    }

    AdmissionHandle beginWithdrawal(RequestRoute exact, BiConsumer<AdmissionHandle, Response> completion) {
        requireContextLock("route withdrawal");
        if (!ownsPreparedDeliveryLocked(exact) || admission != null) { return null; }
        admission = new AdmissionHandle(this, completion);
        admission.withdrawingRoute = exact;
        advanceStageLocked(RequestStage.ROUTING);
        assertInvariantLocked();
        return admission;
    }

    void detachWithdrawnRoute(AdmissionHandle operation, RequestRoute exact) {
        requireContextLock("route withdrawal");
        if (admission != operation || route != exact) {
            throw new IllegalStateException("route withdrawal lost its owner: " + getRequestId());
        }
        route = null;
        detail = "queued after Decode reservation withdrawal";
        updatedAtMs = System.currentTimeMillis();
        assertInvariantLocked();
    }

    boolean bindRoute(RequestRoute exact) {
        requireContextLock("route binding");
        if (stage != RequestStage.ROUTING || !isOpen() || route != null || admission == null || exact.requestId() != getRequestId()) {
            return false;
        }
        route = exact;
        assertInvariantLocked();
        return true;
    }

    void rejectRoutePublication(RequestRoute exact) {
        requireContextLock("route publication rollback");
        if (stage != RequestStage.ROUTING || route != exact || admission == null) {
            throw new IllegalStateException("request route publication ownership changed for " + getRequestId());
        }
        route = null;
        assertInvariantLocked();
    }

    void confirmRoutePublication(RequestRoute exact) {
        requireContextLock("route publication confirmation");
        if (route != exact || stage != RequestStage.ROUTING) {
            throw new IllegalStateException("route publication lost its owner for " + getRequestId());
        }
        advanceStageLocked(RequestStage.READY_TO_DELIVER);
        assertInvariantLocked();
    }

    RequestRoute finishAdmission(AdmissionHandle exact) {
        requireContextLock("admission completion");
        if (admission != exact) { return null; }
        admission = null;
        exact.withdrawingRoute = null;
        if (stage == RequestStage.ROUTING) {
            advanceStageLocked(route == null ? RequestStage.QUEUED : RequestStage.READY_TO_DELIVER);
            return route;
        }
        return null;
    }

    record AdmissionResult(boolean inactive, DeferredTerminal terminal, PendingPrefillRetirement retirement) { }

    AdmissionResult consumeAdmissionFacts(AdmissionHandle exact) {
        requireContextLock("admission settlement");
        AdmissionResult result = new AdmissionResult(exact.inactivityExpired, exact.observedTerminal, exact.prefillRetirement);
        exact.inactivityExpired = false;
        exact.observedTerminal = null;
        exact.prefillRetirement = null;
        return result;
    }

    void retainAdmissionExpiry() {
        requireContextLock("admission expiry");
        admission.inactivityExpired = true;
    }

    boolean retainCleanupFacts(boolean inactive, DeferredTerminal terminal) {
        requireContextLock("cleanup evidence");
        if (cleanup == null) { return false; }
        cleanup.expired |= inactive;
        if (terminal != null && terminal.decodeTerminalAlreadyApplied()) {
            recordCleanupSettlement(false, true, true);
        }
        return true;
    }

    boolean ownsPreparedDeliveryLocked(RequestRoute exact) {
        requireContextLock("delivery eligibility");
        if (stage != RequestStage.READY_TO_DELIVER || route != exact || !isOpen()) {
            return false;
        }
        if (preemption != null) {
            return false;
        }
        // Delivery may race admission completion, but never route withdrawal.
        return admission == null || admission.withdrawingRoute == null;
    }

    DeliveryClaim beginDelivery(RequestRoute exact, DeliveryClaimKind kind, long correlationId, long nowMs,
                                BiConsumer<DeliveryClaim, DeliveryResult> completion,
                                java.util.function.Consumer<DeliveryClaim> cleanupStarter) {
        requireContextLock("delivery claim");
        DeliveryClaim claim = new DeliveryClaim(exact, kind, completion, cleanupStarter);
        delivery = claim;
        advanceStageLocked(RequestStage.DELIVERING);
        batchId = correlationId;
        if (kind == DeliveryClaimKind.BATCH_ENQUEUE) { batchEnqueueStartedAtMs = nowMs; }
        detail = kind == DeliveryClaimKind.BATCH_ENQUEUE ? "batch enqueue started" : "route decision delivery started";
        updatedAtMs = nowMs;
        assertInvariantLocked();
        return claim;
    }

    boolean acceptDeliveryClaim(DeliveryClaim claim) {
        requireContextLock("delivery result");
        return delivery == claim;
    }

    void confirmDelivery() {
        requireContextLock("delivery confirmation");
        if (stage == RequestStage.DELIVERING) { advanceStageLocked(RequestStage.RESULT_PENDING); }
        if (cancellationReason == null && preemption != null && !preemption.isFinished()) {
            preemption.pendingDeliveryConfirmation = true;
        }
    }

    /** 锁内记录 ACK 并移交调度定时器；返回的发布任务和定时器取消由调用方执行。 */
    DeliveryPublication acknowledgeDelivery(PublicationPermit permit, long nowMs) {
        requireContextLock("delivery acknowledgement");
        Response response = buildSuccessResponse(route.routeResponse(), deliveryClaimKind() == DeliveryClaimKind.BATCH_ENQUEUE);
        deliveryAcknowledged = true;
        detail = deliveryClaimKind() == DeliveryClaimKind.BATCH_ENQUEUE ? "batch enqueue acknowledged" : "route decision delivered";
        updatedAtMs = nowMs;
        DeliveryPublication result = new DeliveryPublication(route, response, permit, requestDeadline, batchEnqueueStartedAtMs);
        requestDeadline = null;
        assertInvariantLocked();
        return result;
    }

    void recordPrefillProgressLocked(RoleType role, PrefillState.PrefillRequestStatus.Kind kind, long nowMs) {
        requireContextLock("Prefill execution evidence");
        if (kind == PrefillState.PrefillRequestStatus.Kind.ACTIVE) {
            if (cleanup == null && !prefillObserved && !decodeAccepted && prefillCompletedAtMs == 0L) {
                prefillObserved = true;
                setDecisionDeadlineLocked(OptionalLong.empty());
            }
        } else if (kind == PrefillState.PrefillRequestStatus.Kind.COMPLETED && prefillCompletedAtMs == 0L) {
            boolean separateDecode = role != RoleType.PDFUSION && route.decodeEp() != null;
            prefillCompletedAtMs = nowMs;
            if (!decodeAccepted) {
                setDecisionDeadlineLocked(separateDecode && deliveryPredictionConsumed
                        ? OptionalLong.of(deadlineAfter(nowMs, DECODE_HANDOFF_GRACE_MS)) : OptionalLong.empty());
            }
        }
    }

    DecisionDeadline recordDecodeProgressLocked(DecodeResources.DecodeRequestStatus.Kind kind, long nowMs) {
        requireContextLock("Decode execution evidence");
        recordWorkerActivityLocked(nowMs);
        if (cleanup != null) { return null; }
        if (kind == DecodeResources.DecodeRequestStatus.Kind.ACTIVE) { return markDecodeAcceptedLocked(); }
        setDecisionDeadlineLocked(OptionalLong.empty());
        decodeAccepted = true;
        return null;
    }

    void recordWorkerActivityLocked(long nowMs) {
        requireContextLock("worker observation");
        lastWorkerStatusAtMs = Math.max(lastWorkerStatusAtMs, nowMs);
    }

    boolean consumeRequestDeadline(RequestDeadline exact) {
        requireContextLock("scheduling deadline");
        if (requestDeadline != exact) { return false; }
        requestDeadline = null;
        return true;
    }

    boolean advancePreemption(PreemptionRegistration exact, PreemptionCancelPhase next) {
        requireContextLock("preemption progress");
        if (!ownsResourceTrackingLocked() || preemption != exact
                || next == PreemptionCancelPhase.CANCEL_IN_FLIGHT && cancellationReason != null
                || !exact.advanceTo(next)) { return false; }
        if (next == PreemptionCancelPhase.CANCEL_REQUESTED && finalOutcome == null) {
            detail = exact.detail();
            updatedAtMs = System.currentTimeMillis();
        }
        return true;
    }

    /** 终态 action 的本地清理完成后提交 FINISHED；此前仍保留 route 以匹配迟到事件。 */
    RequestState finishTerminal(TerminalAction action) {
        requireContextLock("terminal commit");
        if (stage != RequestStage.FINALIZING || action.requestContext() != this || route != action.item()) {
            if (action.publication() != null) { action.publication().abandonIfUnused(); }
            throw new IllegalStateException("terminal context identity changed: request_id=" + getRequestId());
        }
        RequestState terminal = snapshot();
        route = null;
        cleanup = null;
        advanceStageLocked(RequestStage.FINISHED);
        assertInvariantLocked();
        return terminal;
    }

    void selectQueuedCancellation(boolean interrupt) {
        requireContextLock("queued cancellation");
        checkState(selectedResponse == null && cancellationReason != null,
                "queued cancellation has no response ownership");
        selectedResponse = new ResponseResult(ResponseCompletion.CANCELLATION, null, null, interrupt);
    }

    void recordCleanupSettlement(boolean prefill, boolean decode, boolean finishPreemption) {
        requireContextLock("cleanup settlement");
        cleanup.prefillSettled |= prefill;
        cleanup.decodeSettled |= decode;
        if (finishPreemption && preemption != null) { preemption.tryFinish(); }
    }

    boolean expireCleanup(long nowMs) {
        requireContextLock("cleanup expiry");
        if (cleanup == null) { return false; }
        if (ownsResourceTrackingLocked() && requestInactiveLocked(nowMs)) { cleanup.expired = true; }
        return cleanup.expired;
    }

    /** Frozen facts for one unlocked endpoint cleanup pass; progress is its opaque identity. */
    record CleanupPass(CleanupProgress progress, RequestRoute route, boolean prefillSettled, boolean decodeSettled,
                       DecodeResources.ReleaseReason releaseReason, DeliveryResult.Status source,
                       RequestDeadline requestDeadline, DecisionDeadline decisionDeadline) { }

    /** An unstarted pass owns QUEUED; running notifications leave their phase unchanged. */
    record CleanupQueue(CleanupProgress progress, boolean queued) { }

    CleanupQueue tryQueueCleanupLocked() {
        requireContextLock("cleanup queue claim");
        if (cleanup == null || admission != null || cleanup.phase == CleanupProgress.Phase.FINISHED
                || cleanup.phase == CleanupProgress.Phase.QUEUED
                || delivery != null && !delivery.canReleaseLocked()) { return null; }
        boolean queued = cleanup.phase == CleanupProgress.Phase.PENDING || cleanup.phase == CleanupProgress.Phase.WAITING;
        if (queued) { cleanup.phase = CleanupProgress.Phase.QUEUED; }
        return new CleanupQueue(cleanup, queued);
    }

    /** Only the accepted task or its rejected submission can surrender its queue claim. */
    void releaseCleanupQueueLocked(CleanupQueue exact) {
        requireContextLock("cleanup queue release");
        if (exact.queued() && cleanup == exact.progress() && cleanup.phase == CleanupProgress.Phase.QUEUED) {
            cleanup.phase = CleanupProgress.Phase.PENDING;
        }
    }

    CleanupPass beginCleanup() {
        requireContextLock("cleanup pass");
        CleanupProgress progress = cleanup;
        if (progress == null || admission != null || progress.phase == CleanupProgress.Phase.FINISHED
                || progress.phase == CleanupProgress.Phase.QUEUED
                || delivery != null && !delivery.canReleaseLocked()) { return null; }
        if (progress.phase == CleanupProgress.Phase.RUNNING || progress.phase == CleanupProgress.Phase.RUN_AGAIN) {
            progress.phase = CleanupProgress.Phase.RUN_AGAIN;
            return null;
        }
        progress.phase = CleanupProgress.Phase.RUNNING;
        RequestDeadline request = requestDeadline;
        requestDeadline = null;
        DecisionDeadline decision = detachDecisionDeadlineLocked();
        DecodeResources.ReleaseReason proof = delivery == null ? null : delivery.provenReleaseReason();
        if (proof == null) {
            proof = progress.expired ? DecodeResources.ReleaseReason.EXPIRED
                    : progress.source == null ? DecodeResources.ReleaseReason.COUNTERPART_FINISHED : null;
        }
        return new CleanupPass(progress, route, progress.prefillSettled, progress.decodeSettled,
                proof, progress.source, request, decision);
    }

    void finishTerminalEffectsLocked(TerminalAction action) {
        requireContextLock("terminal effects");
        checkState(terminalAction == action && cleanup != null, "stale terminal effects");
        cleanup.terminalEffectsFinished = true;
    }

    boolean claimArchiveLocked(TerminalAction action) {
        requireContextLock("archive claim");
        if (terminalAction != action || cleanup == null || !cleanup.terminalEffectsFinished || !cleanup.ready()) { return false; }
        cleanup.phase = CleanupProgress.Phase.FINISHED;
        return true;
    }

    TerminalAction completedCleanupActionLocked() {
        requireContextLock("cleanup action");
        return terminalAction != null && claimArchiveLocked(terminalAction) ? terminalAction : null;
    }

    enum CleanupNext { STALE, TRY_FINISH, REPEAT }

    CleanupNext finishCleanup(CleanupPass pass, boolean prefillDone, boolean decodeDone) {
        requireContextLock("cleanup pass completion");
        CleanupProgress progress = pass.progress();
        if (progress == null || cleanup != progress) { return CleanupNext.STALE; }
        progress.phase = progress.phase == CleanupProgress.Phase.RUN_AGAIN ? CleanupProgress.Phase.PENDING : CleanupProgress.Phase.WAITING;
        progress.prefillSettled |= prefillDone;
        progress.decodeSettled |= decodeDone;
        if (preemption != null && decodeDone && (progress.expired || progress.source == DeliveryResult.Status.NOT_SENT
                || delivery != null && delivery.kind == DeliveryClaimKind.BATCH_ENQUEUE && delivery.cleanupRequired() && delivery.canReleaseLocked())) {
            preemption.tryFinish();
        }
        return progress.phase == CleanupProgress.Phase.PENDING ? CleanupNext.REPEAT : CleanupNext.TRY_FINISH;
    }

    // 以下为包内状态访问器，多数不自行同步；调用方需按生命周期操作的持锁约定读取。
    long createdAtMs() { return createdAtMs; }

    boolean deliveryAcknowledged() { return deliveryAcknowledged; }

    DeliveryClaimKind deliveryClaimKind() { return delivery == null ? DeliveryClaimKind.NONE : delivery.kind; }

    long batchId() { return batchId; }

    /** Bound route identity, retained through resource finalization to match late facts. */
    RequestRoute route() { return route; }

    RequestStage stage() { return stage; }

    boolean decodeAccepted() { return decodeAccepted; }

    Response cancellationResponse() { return cancellationResponse; }

    CancelReason cancellationReason() { return cancellationReason; }

    ResponseResult selectedResponse() { return selectedResponse; }

    TerminalAction tryFinishCleanupLocked() {
        this.requireContextLock("cleanup completion");
        if (!this.hasCleanup()) {
            return null;
        }
        return this.claimFinalizationLocked(null, Objects.requireNonNull(this.finalOutcome, "delivery failure terminal"), null, false, null);
    }
    TerminalAction tryTerminateCancellationLocked(Supplier<PublicationPermit> publication) {
        this.requireContextLock("local cancellation termination");
        if (!this.ownsActiveGenerationLocked() || this.admission() != null || this.cancellationReason() == null) {
            return null;
        }
        RequestRoute active = this.activeRoute();
        if (active != null && !this.canFinalizeBeforeExecutionLocked()) {
            return null;
        }
        String message = this.cancellationReason().getMessage();
        return this.claimFinalizationLocked(null, TerminalOutcome.cancellation(this.cancellationReason(), message), Response.copyOf(this.cancellationResponse()), true, publication);
    }
    TerminalAction decideRequestEndLocked(DeferredTerminal event, Supplier<PublicationPermit> publication) {
        this.requireContextLock("request completion");
        Objects.requireNonNull(event, "request end event");
        if (this.hasCleanup()) {
            return tryFinishCleanupLocked();
        }
        DeferredTerminal.Kind kind = event.kind();
        String message = event.detail();
        if (kind == DeferredTerminal.Kind.DECODE_GENERATION_RETIRED && message == null) {
            message = "Decode endpoint generation retired";
        }
        TerminalOutcome outcome;
        StrategyErrorType error;
        boolean cancellationWins = kind == DeferredTerminal.Kind.INACTIVITY_EXPIRED
                || this.cancellationReason() != null
                && (kind != DeferredTerminal.Kind.FAILURE || this.deliveryClaimKind() == DeliveryClaimKind.NONE);
        if (cancellationWins) {
            CancelReason cause = this.requireCancellationFirstCauseLocked();
            String terminalDetail = switch (kind) {
                case FAILURE, PRIORITY -> cause.getMessage();
                case INACTIVITY_EXPIRED -> message;
                case DECODE_GENERATION_RETIRED -> cause.getMessage() + "; " + message;
                case WORKER -> cause.getMessage() + "; "
                        + (event.workerSource() == WorkerTerminalSource.PREFILL_ENDPOINT
                                ? "Prefill terminal observed after cancellation"
                                : "Decode terminal observed after cancellation");
            };
            outcome = TerminalOutcome.cancellation(cause, terminalDetail);
            error = this.cancellationErrorTypeLocked(cause);
            if (kind != DeferredTerminal.Kind.FAILURE) { message = terminalDetail; }
        } else if (kind == DeferredTerminal.Kind.WORKER && event.workerSuccessful()) {
            return this.claimFinalizationLocked(event, TerminalOutcome.complete("decode completed"),
                    buildSuccessResponse(this.activeRoute().routeResponse(),
                            this.deliveryClaimKind() == DeliveryClaimKind.BATCH_ENQUEUE), true, publication);
        } else {
            error = switch (kind) {
                case FAILURE -> event.errorType();
                case PRIORITY -> StrategyErrorType.PRIORITY_PREEMPTED;
                case DECODE_GENERATION_RETIRED -> StrategyErrorType.DISPATCH_FAILED;
                case WORKER -> StrategyErrorType.WORKER_EXECUTION_FAILED;
                case INACTIVITY_EXPIRED -> throw new IllegalStateException("inactivity requires cancellation");
            };
            if (kind == DeferredTerminal.Kind.WORKER) {
                message = "worker error code " + event.workerErrorCode();
            }
            outcome = kind == DeferredTerminal.Kind.PRIORITY
                    ? TerminalOutcome.cancel(message) : TerminalOutcome.fail(message);
        }
        Response response = this.cancellationReason() != null && this.cancellationResponse() != null ? Response.copyOf(this.cancellationResponse()) : buildErrorResponse(error, message);
        return this.claimFinalizationLocked(event, outcome, response, true, publication);
    }
    TerminalAction claimShutdownAction(Supplier<PublicationPermit> publication) {
        synchronized (this) {
            if (this.stage() == RequestStage.FINISHED) {
                return null;
            }
            String message = "request scheduler is shutting down";
            if (this.admission() != null) {
                this.retainAdmissionTerminalLocked(DeferredTerminal.failure(StrategyErrorType.DISPATCH_FAILED, message));
                return null;
            }
            if (!this.canFinalizeBeforeExecutionLocked()) {
                return null;
            }
            if (this.cancellationReason() != null) {
                return tryTerminateCancellationLocked(publication);
            }
            return this.claimFinalizationLocked(null, TerminalOutcome.fail(message), buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, message), true, publication);
        }
    }

    void recordCancellationLocked(CancelReason reason, String message, Map<String, Object> diagnostics) {
        requireContextLock("cancellation facts");
        if (reason == CancelReason.DEADLINE_EXCEEDED && queueOwner() != null) {
            cancellationResponse = Response.error(StrategyErrorType.RESOURCE_EXHAUSTED,
                    AdmissionRejectReason.RESOURCE_EXHAUSTED, String.valueOf(diagnostics.get("cause")));
            setSchedulingDiagnostics(diagnostics);
        } else {
            cancellationResponse = buildErrorResponse(cancellationErrorTypeLocked(reason), message);
        }
        cancellationReason = reason;
        detail = message;
        updatedAtMs = System.currentTimeMillis();
    }

    TerminalAction claimPrefillRetirementLocked(PendingPrefillRetirement pending, Supplier<PublicationPermit> publication) {
        this.requireContextLock("Prefill retirement");
        if (pending == null || !this.ownsPrefillRouteLocked(pending.source(), pending.item()) || this.decodeAccepted() || this.preemption() != null || this.deliveryClaimKind().isClaimed()) {
            return null;
        }
        if (this.admission() != null) {
            this.retainAdmissionPrefillRetirementLocked(pending);
            return null;
        }
        TerminalAction action = this.cancellationReason() != null && this.deliveryClaimKind() == DeliveryClaimKind.NONE
                ? this.tryTerminateCancellationLocked(publication) : null;
        if (action == null) {
            action = this.claimFinalizationLocked(null, TerminalOutcome.fail(pending.detail()),
                    buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, pending.detail()), true,
                    publication);
        }
        if (action != null) { this.recordCleanupSettlement(true, false, false); }
        return action;
    }

}

record DeferredTerminal(Kind kind, StrategyErrorType errorType, String detail, WorkerTerminalSource workerSource, boolean workerSuccessful, long workerErrorCode) {

    enum Kind {

        FAILURE,
        WORKER,
        PRIORITY,
        DECODE_GENERATION_RETIRED,
        INACTIVITY_EXPIRED
    }

    DeferredTerminal {
        Objects.requireNonNull(kind, "kind");
        boolean valid = switch(kind) {
            case FAILURE ->
                errorType != null && workerSource == null;
            case WORKER ->
                errorType == null && workerSource != null;
            case INACTIVITY_EXPIRED, PRIORITY, DECODE_GENERATION_RETIRED ->
                errorType == null && workerSource == null;
        };
        checkArgument(valid, "deferred terminal kind requires its exact payload");
    }

    static DeferredTerminal failure(StrategyErrorType errorType, String detail) {
        return new DeferredTerminal(Kind.FAILURE, errorType, detail, null, false, 0L);
    }

    static DeferredTerminal inactivityExpired(String detail) {
        return new DeferredTerminal(Kind.INACTIVITY_EXPIRED, null, detail, null, false, 0L);
    }

    static DeferredTerminal worker(WorkerTerminalSource source, boolean successful, long errorCode) {
        return new DeferredTerminal(Kind.WORKER, null, null, Objects.requireNonNull(source, "source"), successful, errorCode);
    }

    static DeferredTerminal priority(String detail) {
        return new DeferredTerminal(Kind.PRIORITY, null, detail, null, false, 0L);
    }

    static DeferredTerminal decodeGenerationRetired(String detail) {
        return new DeferredTerminal(Kind.DECODE_GENERATION_RETIRED, null, detail, null, false, 0L);
    }

    boolean authoritativeWorker() {
        return kind == Kind.WORKER || kind == Kind.DECODE_GENERATION_RETIRED;
    }

    boolean endpointAlreadyRetired() {
        return kind == Kind.DECODE_GENERATION_RETIRED;
    }

    boolean decodeTerminalAlreadyApplied() {
        return kind == Kind.WORKER && workerSource == WorkerTerminalSource.DECODE_ENDPOINT;
    }
}

/**
 * 一次终态执行任务：领取时存入 context.terminalAction 防止重复领取，随后交给 Scheduler。
 * 包含精确路由、定时器和可选发布许可；不是可重新计算或任意重试的普通结果对象。
 */
record TerminalAction(BalanceContext requestContext, RequestRoute item, BalanceContext.PreemptionRegistration preemption, ExpirationTimer.DetachedDeadlines terminalResources, DeferredTerminal event, Response response, BalanceContext.PublicationPermit publication) {

}
