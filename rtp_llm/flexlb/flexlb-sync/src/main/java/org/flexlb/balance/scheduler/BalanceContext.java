package org.flexlb.balance.scheduler;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.context.Context;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.flexlb.balance.delivery.DeliveryResult;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.PrefillState;
import org.flexlb.balance.preemption.PreemptionCancelPhase;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.flexlb.dao.loadbalance.Response.buildErrorResponse;
import static org.flexlb.dao.loadbalance.Response.buildSuccessResponse;

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
 * <p>通常在锁内确认状态和操作身份，再由 Scheduler 在锁外执行返回的 action/Runnable。
 * 本类也会调用 Scheduler 构造这些执行任务，以及查询 Endpoint 事实；Future 完成与
 * Endpoint 清理需遵循各自的锁外执行约定。
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
        if (scheduler != null && scheduler != owner) {
            throw new IllegalStateException("request scheduler ownership cannot change");
        }
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
        if ((this.future instanceof RequestFuture
                || future instanceof RequestFuture) && this.future != future) {
            throw new IllegalStateException("registered request future cannot be replaced or rebound");
        }
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
        if (this.future instanceof RequestFuture && schedulingMetadata != metadata) {
            throw new IllegalStateException("registered scheduling metadata is immutable");
        }
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
    private RequestRoute item;

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

    RequestRoute activeItem() {
        synchronized (this) {
            return this.ownsActiveGenerationLocked() ? this.item : null;
        }
    }

    boolean ownsActiveItem(RequestRoute expected) {
        synchronized (this) {
            return this.ownsActiveGenerationLocked() && this.item == expected;
        }
    }

    RequestRoute activeItemForReservation(long reservationToken) {
        synchronized (this) {
            DecodeEndpoint.ReservationHandle reservation = this.item == null ? null : this.item.decodeReservation();
            return this.ownsActiveGenerationLocked() && reservation != null && reservation.reservationToken() == reservationToken ? this.item : null;
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
        if (this.stage == RequestStage.QUEUED && this.item != null && admission == null || this.stage == RequestStage.READY_TO_DELIVER && this.item == null || this.stage == RequestStage.DELIVERING && (this.item == null || this.deliveryClaimKind() == DeliveryClaimKind.NONE)) {
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
        if (this.item != null || preemption != null || admission != null || this.requestDeadline != null || this.decisionDeadline != null || this.inactivityDeadline != null) {
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

    boolean ownsPrefillFactLocked(PrefillEndpoint source, RequestRoute expected) {
        this.requireContextLock("Prefill fact ownership lookup");
        return this.ownsResourceTrackingLocked() && this.item == expected && expected.prefillEp() == source;
    }

    boolean ownsDecodeFactLocked(DecodeEndpoint source, DecodeEndpoint.ReservationHandle reservation) {
        this.requireContextLock("Decode fact ownership lookup");
        return this.ownsResourceTrackingLocked() && this.item != null && this.item.decodeEp() == source && reservation.equals(this.item.decodeReservation());
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

    boolean queuedLocalControlLocked() {
        this.requireContextLock("queued control ownership");
        return queueOwner() != null && this.stage == RequestStage.READY_TO_DELIVER && admission == null && this.item != null && this.item.prefillEp() != null && this.cancellationReason != null && preemption == null && this.deliveryClaimKind() == DeliveryClaimKind.NONE;
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

    boolean canClaimLocalTerminalLocked(boolean queuedExternalCancel) {
        this.requireContextLock("local terminal eligibility");
        return this.ownsActiveGenerationLocked() && (!this.future().isDone() || queuedExternalCancel && this.future().isCancelled()) && admission == null && preemption == null && !this.decodeAccepted && !this.deliveryAcknowledged && !this.deliveryClaimKind().isClaimed();
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
            if (timeoutMs <= 0L) {
                throw new IllegalArgumentException("request inactivity timeout must be positive");
            }
            this.inactivityTimeoutMs = timeoutMs;
        }
    }

    OptionalLong inactivityDeadlineAtMs() {
        synchronized (this) {
            return this.ownsResourceTrackingLocked() && this.inactivityDeadline == null && (this.cleanup == null || !this.cleanup.expired) && this.inactivityTimeoutMs > 0L ? OptionalLong.of(this.inactivityExpiresAtMsLocked()) : OptionalLong.empty();
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
     * 真实的 Prefill/Decode 证据优先于预测；返回的旧定时器交给调用方在锁外取消。
     */
    DecisionDeadline updateDeliveryPredictionLocked(WorkSnapshot precedingWork, long unstartedWorkMs, long nowMs) {
        this.requireContextLock("delivery prediction consumption");
        Objects.requireNonNull(precedingWork, "precedingWork");
        if (unstartedWorkMs < 0L) {
            throw new IllegalArgumentException("unstarted work must be non-negative");
        }
        if (this.deliveryPredictionConsumed) {
            throw new IllegalStateException("delivery prediction already consumed");
        }
        double lifetime = this.item.ctx().getConfig().getRequestLifecycle().getDecision().getLifetime();
        if (!Double.isFinite(lifetime) || lifetime < 1.0) {
            throw new IllegalArgumentException("invalid decision lifetime");
        }
        this.deliveryPredictionConsumed = true;
        if (!this.decodeAccepted) {
            if (this.prefillCompletedAtMs > 0L) {
                if (this.item.decodeEp() != null) {
                    this.decisionExpiresAtMs = OptionalLong.of(deadlineAfter(this.prefillCompletedAtMs, DECODE_HANDOFF_GRACE_MS));
                }
            } else if (!this.prefillObserved) {
                OptionalLong precedingMs = precedingWork.totalRemainingWorkMsAt(nowMs);
                if (precedingMs.isPresent()) {
                    long remainingMs = addWork(precedingMs.getAsLong(), unstartedWorkMs);
                    double scaled = Math.ceil(remainingMs * lifetime);
                    long durationMs = scaled >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) scaled;
                    this.decisionExpiresAtMs = OptionalLong.of(deadlineAfter(nowMs, addWork(durationMs, DECODE_HANDOFF_GRACE_MS)));
                }
            }
        }
        // Reconcile the exact reservation in this decision. Engine acceptance overrides the prediction.
        if (this.item.decodeEp() != null && this.item.decodeEp().isAcceptedByEngine(this.item.decodeReservation())) {
            this.lastWorkerStatusAtMs = Math.max(this.lastWorkerStatusAtMs, nowMs);
            return this.markDecodeAcceptedLocked();
        }
        return null;
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
        return this.ownsActiveGenerationLocked() && this.item != null && this.cancellationReason == null && (this.decisionExpired && (!this.decodeAccepted && (!this.prefillObserved || this.prefillCompletedAtMs > 0L)));
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

    PreemptionRegistration tryInstallPreemption(long reservationToken, long attemptToken, String detail) {
        synchronized (this) {
            DecodeEndpoint.ReservationHandle reservation = this.item == null ? null : this.item.decodeReservation();
            if (!this.ownsActiveGenerationLocked() || admission != null || preemption != null || this.cancellationReason != null || reservation == null || reservation.reservationToken() != reservationToken || (this.deliveryClaimKind() == DeliveryClaimKind.ROUTE_DECISION && !this.deliveryAcknowledged)) {
                return null;
            }
            preemption = new PreemptionRegistration(this, attemptToken, detail);
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

    void requireCleanupOwner(TerminalAction action) {
        synchronized (this) {
            if (this.stage != RequestStage.FINALIZING || action.requestContext() != this || action.item() != this.item) {
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
        if (Thread.holdsLock(this)) {
            throw new IllegalStateException(operation + " must run outside the BalanceContext lock");
        }
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

    private static long addWork(long precedingMs, long unstartedMs) {
        return precedingMs > Long.MAX_VALUE - unstartedMs ? Long.MAX_VALUE : precedingMs + unstartedMs;
    }

    private static long deadlineAfter(long startedAtMs, long durationMs) {
        if (startedAtMs < 0L || durationMs <= 0L) {
            throw new IllegalArgumentException("deadline requires a valid start and positive duration");
        }
        return startedAtMs > Long.MAX_VALUE - durationMs ? Long.MAX_VALUE : startedAtMs + durationMs;
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
            if (failure.isSuccess()) {
                throw new IllegalArgumentException("admission termination requires a failure");
            }
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
            if (!resolved.get()) { throw new IllegalStateException("admission must be explicitly finished"); }
        }
    }

    /** Exact route delivery identity; completion is consumed under the request monitor. */
    public record DeliverySettlement(RequestRoute route, DeliveryClaim.SendOutcome sendOutcome,
                                     CancelReason abandonmentReason, boolean prefillSettled, boolean decodeSettled) { }

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
        private boolean prefillSettled;
        private boolean decodeSettled;
        private boolean executionFinished;
        private boolean cleanupContinuationAttached;

        private DeliveryClaim(RequestRoute item, DeliveryClaimKind kind,
                              BiConsumer<DeliveryClaim, DeliveryResult> completion,
                              java.util.function.Consumer<DeliveryClaim> cleanupStarter) {
            this.item = item;
            this.owner = item.ctx();
            this.kind = kind;
            this.completion = completion;
            this.cleanupStarter = Objects.requireNonNull(cleanupStarter);
            this.decodeSettled = item.decodeEp() == null || item.decodeReservation() == null;
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
                if (kind != DeliveryClaimKind.BATCH_ENQUEUE) { throw new IllegalStateException("not a batch delivery"); }
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
                if (abandonmentReason != null) { return; }
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
                        prefillSettled && decodeSettled);
            }
        }

        void acceptCleanupAck(org.flexlb.balance.eviction.EngineCancelChannel.CancelAck ack) {
            synchronized (owner) {
                if (ack == org.flexlb.balance.eviction.EngineCancelChannel.CancelAck.REQUEST_CLEANED) {
                    prefillSettled = true;
                    decodeSettled = true;
                } else if (ack == org.flexlb.balance.eviction.EngineCancelChannel.CancelAck.REQUEST_FENCED) {
                    prefillSettled = true;
                }
            }
            publishSettlement();
        }

        void observeDecodeSettlement(DecodeEndpoint source, DecodeEndpoint.WorkerStatusFact fact) {
            synchronized (owner) {
                if (source != item.decodeEp() || !Objects.equals(fact.reservation(), item.decodeReservation())
                        || fact.kind() != DecodeEndpoint.WorkerStatusFact.Kind.TERMINAL) { return; }
                decodeSettled = true;
                executionFinished = true;
            }
            publishSettlement();
        }

        void observeWorkerCompletion(RequestRoute exact) {
            synchronized (owner) {
                if (exact != item) { return; }
                executionFinished = true;
            }
            publishSettlement();
        }

        void observeRetirement(org.flexlb.balance.endpoint.WorkerEndpoint source) {
            synchronized (owner) {
                if (source instanceof DecodeEndpoint decode && !decode.isRetired()) { return; }
                if (source == item.prefillEp()) { prefillSettled = true; }
                if (source == item.decodeEp()) { decodeSettled = true; executionFinished = true; }
            }
            publishSettlement();
        }

        /** 锁内生成结算快照，锁外完成 Future，避免在 owner 锁内运行下游回调。 */
        private void publishSettlement() {
            DeliverySettlement result;
            synchronized (owner) {
                boolean done = senderFinished && (kind == DeliveryClaimKind.ROUTE_DECISION || sendOutcome == SendOutcome.NOT_SENT
                        || (abandonmentReason == null ? executionFinished : prefillSettled && decodeSettled));
                result = done ? new DeliverySettlement(item, sendOutcome, abandonmentReason, prefillSettled, decodeSettled) : null;
            }
            if (result != null) { settled.complete(result); }
        }

        DecodeEndpoint.ReleaseReason provenReleaseReason() {
            synchronized (owner) {
                if (sendOutcome == SendOutcome.NOT_SENT && senderFinished) {
                    return DecodeEndpoint.ReleaseReason.NOT_SENT;
                }
                if (cleanupComplete() && abandonmentReason != null && prefillSettled && decodeSettled) {
                    return DecodeEndpoint.ReleaseReason.REMOTE_CLEANUP;
                }
                return null;
            }
        }

        public java.util.concurrent.CompletionStage<DeliverySettlement> settlement() { return settled.minimalCompletionStage(); }
        boolean attachCleanupContinuation() {
            synchronized (owner) {
                if (cleanupContinuationAttached) { return false; }
                cleanupContinuationAttached = true;
                return true;
            }
        }
        boolean cleanupRequired() { synchronized (owner) { return abandonmentReason != null; } }
        boolean cleanupComplete() { return settled.isDone(); }
        public SendOutcome sendOutcome() { synchronized (owner) { return sendOutcome; } }
    }

    static SelectedResponse selectPublication(BalanceContext ctx, PublicationPermit permit, ResponseCompletion completion, Response response, Throwable failure, boolean mayInterruptIfRunning) {
        ctx.requireOutsideContextLock("response selection");
        if (permit.requestContext != ctx || completion != ResponseCompletion.RESPONSE && permit.kind != PublicationKind.TERMINAL) {
            throw new IllegalArgumentException("incompatible publication permit");
        }
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
     * 投递失败后 Prefill/Decode 的结算进度，由 context 锁保护。
     * RUN_AGAIN 表示当前锁外清理期间又来了事件，当前轮完成后必须再执行一轮。
     * expired 是清理阶段的超时事实，不等于两端资源已释放。
     */
    private static final class CleanupProgress {

        // PENDING blocks close before the first pass or while a requested follow-up has not started.
        enum Phase {

            PENDING, RUNNING, RUN_AGAIN, WAITING
        }

        final DeliveryResult.Status source;

        Phase phase = Phase.PENDING;

        boolean prefillSettled;

        boolean decodeSettled;

        /**
         * Sticky decision: later Worker activity cannot revoke an already requested expiry.
         */
        boolean expired;

        CleanupProgress(RequestRoute item, DeliveryResult.Status source) {
            this.source = source;
            prefillSettled = item.prefillEp() == null;
            decodeSettled = item.decodeEp() == null || item.decodeReservation() == null;
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
            ExpirationTimer.DetachedDeadlines terminalResources = this.detachDeadlines();
            TerminalAction action = new TerminalAction(this, this.item, this.deliveryClaimKind(), this.cleanup != null, claimedPreemption, terminalResources, event, publishable ? response : null, permit);
            terminalAction = action;
            // The selected outcome is visible while unlocked endpoint cleanup runs.
            this.beginFinalizationLocked(transition);
            this.cleanup = null;
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
        if (!this.ownsActiveItem(exact) || this.cleanup != null) {
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
        if (stage != RequestStage.QUEUED || !isOpen() || item != null || admission != null || preemption != null) {
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
        if (admission != operation || item != exact) {
            throw new IllegalStateException("route withdrawal lost its owner: " + getRequestId());
        }
        item = null;
        detail = "queued after Decode reservation withdrawal";
        updatedAtMs = System.currentTimeMillis();
        assertInvariantLocked();
    }

    boolean bindRoute(RequestRoute exact) {
        requireContextLock("route binding");
        if (stage != RequestStage.ROUTING || !isOpen() || item != null || admission == null || exact.requestId() != getRequestId()) {
            return false;
        }
        item = exact;
        assertInvariantLocked();
        return true;
    }

    void rejectRoutePublication(RequestRoute exact) {
        requireContextLock("route publication rollback");
        if (stage != RequestStage.ROUTING || item != exact || admission == null) {
            throw new IllegalStateException("request item publication ownership changed for " + getRequestId());
        }
        item = null;
        assertInvariantLocked();
    }

    void confirmRoutePublication(RequestRoute exact) {
        requireContextLock("route publication confirmation");
        if (item != exact || stage != RequestStage.ROUTING) {
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
            advanceStageLocked(item == null ? RequestStage.QUEUED : RequestStage.READY_TO_DELIVER);
            return item;
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
        return stage == RequestStage.READY_TO_DELIVER && item == exact && isOpen() && preemption == null
                && (admission == null || admission.withdrawingRoute == null) && deliveryClaimKind() == DeliveryClaimKind.NONE;
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
    }

    /** 锁内记录 ACK 并移交调度定时器；返回的发布任务和定时器取消由调用方执行。 */
    DeliveryPublication acknowledgeDelivery(PublicationPermit permit, long nowMs) {
        requireContextLock("delivery acknowledgement");
        Response response = buildSuccessResponse(item.routeResponse(), deliveryClaimKind() == DeliveryClaimKind.BATCH_ENQUEUE);
        deliveryAcknowledged = true;
        detail = deliveryClaimKind() == DeliveryClaimKind.BATCH_ENQUEUE ? "batch enqueue acknowledged" : "route decision delivered";
        updatedAtMs = nowMs;
        DeliveryPublication result = new DeliveryPublication(item, response, permit, requestDeadline, batchEnqueueStartedAtMs);
        requestDeadline = null;
        assertInvariantLocked();
        return result;
    }

    void observeWorker(long nowMs) {
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
        if (stage != RequestStage.FINALIZING || action.requestContext() != this || item != action.item()) {
            if (action.publication() != null) { action.publication().abandonIfUnused(); }
            throw new IllegalStateException("terminal context identity changed: request_id=" + getRequestId());
        }
        RequestState terminal = snapshot();
        item = null;
        advanceStageLocked(RequestStage.FINISHED);
        assertInvariantLocked();
        return terminal;
    }

    void selectQueuedCancellation(boolean interrupt) {
        requireContextLock("queued cancellation");
        if (selectedResponse != null || cancellationReason == null) {
            throw new IllegalStateException("queued cancellation has no response ownership");
        }
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
    record CleanupPass(CleanupProgress progress, boolean prefillSettled, boolean decodeSettled, boolean expired,
                       RequestDeadline requestDeadline, DecisionDeadline decisionDeadline) { }

    CleanupPass beginCleanup(RequestRoute exact) {
        requireContextLock("cleanup pass");
        CleanupProgress progress = item == exact ? cleanup : null;
        if (progress != null && admission != null) { return null; }
        if (progress != null && (progress.phase == CleanupProgress.Phase.RUNNING || progress.phase == CleanupProgress.Phase.RUN_AGAIN)) {
            progress.phase = CleanupProgress.Phase.RUN_AGAIN;
            return null;
        }
        RequestDeadline request = null;
        DecisionDeadline decision = null;
        if (progress != null) {
            progress.phase = CleanupProgress.Phase.RUNNING;
            request = requestDeadline;
            requestDeadline = null;
            decision = detachDecisionDeadlineLocked();
        }
        return new CleanupPass(progress, progress != null && progress.prefillSettled,
                progress != null && progress.decodeSettled, progress != null && progress.expired, request, decision);
    }

    enum CleanupNext { STALE, TRY_FINISH, REPEAT }

    CleanupNext finishCleanup(CleanupPass pass, boolean prefillDone, boolean decodeDone) {
        requireContextLock("cleanup pass completion");
        CleanupProgress progress = pass.progress();
        if (progress == null || cleanup != progress) { return CleanupNext.STALE; }
        progress.phase = progress.phase == CleanupProgress.Phase.RUN_AGAIN ? CleanupProgress.Phase.PENDING : CleanupProgress.Phase.WAITING;
        progress.prefillSettled |= prefillDone;
        progress.decodeSettled |= decodeDone;
        if (preemption != null && decodeDone && (pass.expired() || progress.source == DeliveryResult.Status.NOT_SENT
                || delivery != null && delivery.kind == DeliveryClaimKind.BATCH_ENQUEUE && delivery.cleanupRequired() && delivery.cleanupComplete())) {
            preemption.tryFinish();
        }
        return progress.phase == CleanupProgress.Phase.PENDING ? CleanupNext.REPEAT : CleanupNext.TRY_FINISH;
    }

    // 以下为包内状态访问器，多数不自行同步；调用方需按生命周期操作的持锁约定读取。
    long createdAtMs() { return createdAtMs; }

    boolean deliveryAcknowledged() { return deliveryAcknowledged; }

    long updatedAtMs() { return updatedAtMs; }

    DeliveryClaimKind deliveryClaimKind() { return delivery == null ? DeliveryClaimKind.NONE : delivery.kind; }

    long batchId() { return batchId; }

    RequestRoute item() { return item; }

    RequestStage stage() { return stage; }

    boolean decodeAccepted() { return decodeAccepted; }

    Response cancellationResponse() { return cancellationResponse; }

    CancelReason cancellationReason() { return cancellationReason; }

    ResponseResult selectedResponse() { return selectedResponse; }

    RequestDeadline requestDeadline() { return requestDeadline; }

    DecisionDeadline decisionDeadline() { return decisionDeadline; }

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
        RequestRoute active = this.activeItem();
        if (active != null && !this.canClaimLocalTerminalLocked(true)) {
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
                    buildSuccessResponse(this.activeItem().routeResponse(),
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
            if (!this.canClaimLocalTerminalLocked(true)) {
                return null;
            }
            if (this.cancellationReason() != null) {
                return tryTerminateCancellationLocked(publication);
            }
            return this.claimFinalizationLocked(null, TerminalOutcome.fail(message), buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, message), true, publication);
        }
    }

    boolean recordCancellationLocked(CancelReason reason, String message) {
        this.requireContextLock("record cancellation");
        Objects.requireNonNull(reason, "reason");
        if (!this.ownsActiveGenerationLocked() || this.cancellationReason() != null) {
            return false;
        }
        Map<String, Object> diagnostics = null;
        if (reason == CancelReason.DEADLINE_EXCEEDED && this.queueOwner() != null) {
            RequestRoute item = this.activeItem();
            if (this.deliveryClaimKind() != DeliveryClaimKind.NONE) {
                diagnostics = Map.of("cause", message);
            } else if (item != null && item.prefillEp() != null) {
                diagnostics = item.prefillEp().getLatestQueueWaitSnapshot();
            } else {
                diagnostics = this.queueOwner().getLatestQueueWaitSnapshot();
            }
        }
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
        return true;
    }

    Runnable acceptPrefillStatus(PrefillEndpoint source, RoleType role,
                                 PrefillState.WorkerStatusFact fact, long nowMs) {
        Runnable work;
        DecisionDeadline obsolete;
        boolean resume;
        RequestRoute localControl;
        synchronized (this) {
            if (!this.ownsPrefillFactLocked(source, fact.item())) {
                return null;
            }
            PreemptionRegistration previous = this.preemption();
            boolean cleaning = this.hasCleanup();
            work = applyPrefillStatusLocked(role, fact, nowMs);
            obsolete = cleaning ? null : this.detachObsoleteDecisionDeadlineLocked();
            resume = previous != null && this.preemption() == null && this.hasCleanup() && work == null;
            localControl = previous != null && this.preemption() == null && this.queuedLocalControlLocked()
                    ? this.item() : null;
        }
        return () -> {
            if (resume) {
                scheduler.resumeCleanup(this);
            } else {
                scheduler.executeEngineEffects(this, work, obsolete);
            }
            if (localControl != null) {
                scheduler.signalOrSettleLocalControl(this, localControl);
            }
        };
    }

    private Runnable applyPrefillStatusLocked(RoleType role, PrefillState.WorkerStatusFact fact, long nowMs) {
        this.requireContextLock("Prefill fact reduction");
        this.observeWorker(nowMs);
        boolean cleaning = this.hasCleanup();
        if (cleaning && fact.kind() != PrefillState.WorkerStatusFact.Kind.ACTIVE) {
            this.recordCleanupSettlement(true, false, false);
            PreemptionRegistration claim = this.preemption();
            if (fact.kind() != PrefillState.WorkerStatusFact.Kind.PRIORITY_CANCELED
                    || claim == null || claim.isFinished()) {
                return scheduler.finalizationEffects(this.tryFinishCleanupLocked(), null);
            }
        }
        Runnable transition = switch(fact.kind()) {
            case ACTIVE ->
                {
                    if (cleanup == null && !prefillObserved && !decodeAccepted && prefillCompletedAtMs == 0L) {
                        prefillObserved = true;
                        setDecisionDeadlineLocked(OptionalLong.empty());
                    }

                    PreemptionRegistration claim = this.preemption();
                    if (claim != null && claim.isNotFound()) {
                        DecodeEndpoint decode = fact.item().decodeEp();
                        if (decode == null || decode.updatePreemption(claim.attemptToken(),
                                DecodeEndpoint.PreemptionUpdate.active(fact.item().decodeReservation()))) {
                            this.detachPreemptionOwnerLocked(claim);
                        }
                    }
                    yield null;
                }
            case COMPLETED ->
                {
                    if (!decodeAccepted && prefillCompletedAtMs == 0L) {
                        boolean separateDecode = role != RoleType.PDFUSION && item.decodeEp() != null;
                        prefillCompletedAtMs = nowMs;
                        setDecisionDeadlineLocked(separateDecode && deliveryPredictionConsumed
                                ? OptionalLong.of(deadlineAfter(nowMs, DECODE_HANDOFF_GRACE_MS)) : OptionalLong.empty());
                    }

                    yield role == RoleType.PDFUSION ? this.processRequestEndLocked(fact.item(), DeferredTerminal.worker(WorkerTerminalSource.PREFILL_ENDPOINT, true, fact.errorCode())) : null;
                }
            case FAILED ->
                this.processRequestEndLocked(fact.item(), DeferredTerminal.worker(WorkerTerminalSource.PREFILL_ENDPOINT, false, fact.errorCode()));
            case PRIORITY_CANCELED -> {
                PreemptionRegistration claim = this.preemption();
                DecodeEndpoint decode = fact.item().decodeEp();
                if (claim == null || claim.isFinished() || decode == null || fact.item().decodeReservation() == null
                        || !decode.updatePreemption(claim.attemptToken(),
                                DecodeEndpoint.PreemptionUpdate.canceled(fact.item().decodeReservation()))
                        // Capacity listeners may reenter the scheduler during the Decode update.
                        || !this.ownsResourceTrackingLocked() || this.preemption() != claim || !claim.tryFinish()) {
                    yield null;
                }
                yield this.finishPreemptedRequestLocked(claim, "priority victim canceled by worker", true);
            }
        };
        if (!cleaning) { this.reconcileDecisionEvidenceLocked(); }
        return transition;
    }

    Runnable acceptDecodeStatus(DecodeEndpoint source, DecodeEndpoint.WorkerStatusFact fact, long nowMs) {
        Runnable work = null;
        DecisionDeadline obsolete = null;
        synchronized (this) {
            if (!this.ownsDecodeFactLocked(source, fact.reservation())) {
                return null;
            }
            this.observeWorker(nowMs);
            if (fact.kind() == DecodeEndpoint.WorkerStatusFact.Kind.TERMINAL) {
                if (!this.hasCleanup()) {
                    setDecisionDeadlineLocked(OptionalLong.empty());
                    decodeAccepted = true;
                }
                work = this.processRequestEndLocked(this.item(), DeferredTerminal.worker(
                        WorkerTerminalSource.DECODE_ENDPOINT, fact.errorCode() == 0L, fact.errorCode()));
                obsolete = this.detachObsoleteDecisionDeadlineLocked();
            } else if (!this.hasCleanup()) {
                // Active membership proves Decode ownership, including before KV allocation.
                obsolete = this.markDecodeAcceptedLocked();
            }
        }
        Runnable effect = work;
        DecisionDeadline deadline = obsolete;
        return () -> {
            DeliveryClaim delivery = this.delivery();
            if (delivery != null) { delivery.observeDecodeSettlement(source, fact); }
            scheduler.executeEngineEffects(this, effect, deadline);
        };
    }

    /**
     * 选路/撤回结束后消费暂存事实：权威 Worker 终态优先，再处理退休、失败或取消。
     * 返回的执行任务由调用方在 context 锁外运行。
     */
    Runnable settleAdmissionLocked(AdmissionHandle operation, Response failure) {
        this.requireContextLock("admission settlement");
        CancelReason pendingCancellation = this.cancellationReason();
        AdmissionResult retained = this.consumeAdmissionFacts(operation);
        boolean inactive = retained.inactive();
        DeferredTerminal pending = retained.terminal();
        PendingPrefillRetirement retirement = retained.retirement();
        if (this.item() == null && pending != null && pending.kind() == DeferredTerminal.Kind.FAILURE) {
            if (failure == null) {
                failure = buildErrorResponse(pending.errorType(), pending.detail());
            }
            pending = null;
        }
        if (this.retainCleanupFacts(inactive, pending)) {
            return null;
        }
        // Authoritative completion takes precedence over retirement and admission failure.
        if (pending != null && pending.authoritativeWorker()) {
            return this.processRequestEndLocked(this.item(), pending);
        }
        TerminalAction retired = this.claimPrefillRetirementLocked(retirement);
        if (retired != null) {
            return scheduler.finalizationEffects(retired, null);
        }
        if (inactive && pendingCancellation != null) {
            return scheduler.finalizationEffects(this.decideRequestEndLocked(DeferredTerminal.inactivityExpired("REQUEST_INACTIVE: no matching Engine request status before inactivity timeout"), () -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL)), null);
        }
        Runnable effect = null;
        if (pending != null) {
            effect = this.processRequestEndLocked(this.item(), pending);
        } else if (failure != null) {
            String message = pendingCancellation != null ? pendingCancellation.getMessage() : failure.getErrorMessage() == null ? "eviction admission failed" : failure.getErrorMessage();
            TerminalOutcome outcome = pendingCancellation == null ? TerminalOutcome.fail(message) : TerminalOutcome.cancellation(pendingCancellation, message);
            Response response = pendingCancellation == null ? failure : Response.copyOf(this.cancellationResponse());
            effect = scheduler.finalizationEffects(this.claimFinalizationLocked(null, outcome, response, true, () -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL)), null);
        }
        if (effect != null || pendingCancellation == null || !this.ownsActiveGenerationLocked()) {
            return effect;
        }
        if (this.queuedLocalControlLocked() && !inactive) {
            return effect;
        }
        TerminalAction cancelled = pendingCancellation == CancelReason.DEADLINE_EXCEEDED || this.requestInactiveLocked(System.currentTimeMillis()) ? this.decideRequestEndLocked(DeferredTerminal.inactivityExpired(pendingCancellation.getMessage()), () -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL)) : this.tryTerminateCancellationLocked(() -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL));
        return cancelled == null ? effect : scheduler.finalizationEffects(cancelled, null);
    }

    TerminalAction claimPrefillRetirementLocked(PendingPrefillRetirement pending) {
        this.requireContextLock("Prefill retirement");
        if (pending == null || !this.ownsPrefillFactLocked(pending.source(), pending.item()) || this.decodeAccepted() || this.preemption() != null || this.deliveryClaimKind().isClaimed()) {
            return null;
        }
        if (this.admission() != null) {
            this.retainAdmissionPrefillRetirementLocked(pending);
            return null;
        }
        if (this.cancellationReason() != null && this.deliveryClaimKind() == DeliveryClaimKind.NONE) {
            TerminalAction cancelled = this.tryTerminateCancellationLocked(() -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL));
            if (cancelled != null) {
                return cancelled;
            }
        }
        return this.claimFinalizationLocked(null, TerminalOutcome.fail(pending.detail()),
                buildErrorResponse(StrategyErrorType.DISPATCH_FAILED, pending.detail()), true, () -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL));
    }

    Runnable decideInactivityLocked(long nowMs, PreemptionRegistration signal) {
        if (this.expireCleanup(nowMs)) {
            return () -> scheduler.resumeCleanup(this);
        }
        if (!this.ownsActiveGenerationLocked() || !this.requestInactiveLocked(nowMs)) {
            return null;
        }
        String message = "REQUEST_INACTIVE: no matching Engine request status before inactivity timeout";
        this.recordCancellationLocked(CancelReason.DEADLINE_EXCEEDED, message);
        if (this.admission() != null) {
            this.retainAdmissionExpiry();
            return null;
        }
        return scheduler.finalizationEffects(this.decideRequestEndLocked(DeferredTerminal.inactivityExpired(message), () -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL)), signal);
    }

    /**
     * 先校验 route 身份，再按清理、admission、抢占所有权决定立即处理还是暂存事件。
     * Worker 权威终态也可用于 FINALIZING 的资源结算；旧 route 的事件不影响当前请求。
     */
    Runnable processRequestEndLocked(RequestRoute expected, DeferredTerminal event) {
        this.requireContextLock("request end");
        boolean workerProof = event.authoritativeWorker();
        if (expected == null || !this.ownsResourceTrackingLocked() || this.item() != expected || !workerProof && !this.ownsActiveItem(expected)) {
            return null;
        }
        if (this.hasCleanup()) {
            if (event.decodeTerminalAlreadyApplied() || event.endpointAlreadyRetired()) {
                this.recordCleanupSettlement(false, true, true);
            } else {
                this.recordCleanupSettlement(true, false, false);
            }
            return scheduler.finalizationEffects(this.tryFinishCleanupLocked(), null);
        }
        if (this.admission() != null) {
            this.retainAdmissionTerminalLocked(event);
            return null;
        }
        PreemptionRegistration exact = this.preemption();
        if (exact == null) {
            return scheduler.finalizationEffects(this.decideRequestEndLocked(event, () -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL)), null);
        }
        if (event.endpointAlreadyRetired()) {
            this.retainPreemptionTerminalLocked(exact, event);
            exact.tryFinish();
            this.detachPreemptionOwnerLocked(exact);
            return scheduler.finalizationEffects(this.decideRequestEndLocked(event, () -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL)), exact);
        }
        if (exact.isFinished()) {
            return null;
        }
        this.retainPreemptionTerminalLocked(exact, event);
        if (!workerProof && !exact.isNotFound() && !exact.isUnknown()) {
            return null;
        }
        return this.processPendingEventsUnderPreemptionLocked(exact, !workerProof && exact.isUnknown(), exact);
    }

    Runnable processPendingEventsUnderPreemptionLocked(PreemptionRegistration exact, boolean transportUnknown, PreemptionRegistration signal) {
        DeferredTerminal terminal = exact.pendingTerminal();
        boolean terminalWins = terminal != null && (!transportUnknown || terminal.authoritativeWorker());
        if (!terminalWins && (transportUnknown || !exact.hasPendingDeliveryConfirmation())) {
            return null;
        }
        RequestRoute active = this.activeItem();
        DecodeEndpoint decode = active == null ? null : active.decodeEp();
        // Decode terminal facts already committed its ledger; all other evidence must reconcile it first.
        if (decode != null && !(terminalWins && terminal.decodeTerminalAlreadyApplied())
                && !decode.updatePreemption(exact.attemptToken(), terminalWins
                        ? DecodeEndpoint.PreemptionUpdate.finished(active.decodeReservation())
                        : DecodeEndpoint.PreemptionUpdate.active(active.decodeReservation()))) {
            return null;
        }
        if (terminalWins) { exact.tryFinish(); }
        this.detachPreemptionOwnerLocked(exact);
        return terminalWins
                ? scheduler.finalizationEffects(this.decideRequestEndLocked(terminal, () -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL)), signal)
                : this.acknowledgeDeliveryLocked(signal);
    }

    Runnable finishPreemptedRequestLocked(PreemptionRegistration exact,
                                                  String detail, boolean prefillSettled) {
        if (this.hasCleanup()) {
            this.recordCleanupSettlement(prefillSettled, true, false);
            return scheduler.finalizationEffects(this.tryFinishCleanupLocked(), null);
        }
        DeferredTerminal terminal = DeferredTerminal.priority(detail);
        this.retainPreemptionTerminalLocked(exact, terminal);
        this.detachPreemptionOwnerLocked(exact);
        return scheduler.finalizationEffects(this.decideRequestEndLocked(terminal, () -> scheduler.requirePublicationPermitLocked(this, PublicationKind.TERMINAL)), exact);
    }

    Runnable acknowledgeDeliveryLocked(PreemptionRegistration signal) {
        this.requireContextLock("delivery acknowledgement");
        if (this.item() == null || !this.ownsActiveItem(this.item())) {
            return null;
        }
        this.confirmDelivery();
        if (this.cancellationReason() != null) {
            return null;
        }
        PreemptionRegistration blocked = this.preemption();
        if (blocked != null) {
            if (blocked.isFinished()) {
                return null;
            }
            blocked.recordDeliveryConfirmation();
            return this.processPendingEventsUnderPreemptionLocked(blocked, false, null);
        }
        if (this.deliveryAcknowledged()) {
            return null;
        }
        // A delayed timer continuation cannot let an expired silent request publish a late ACK.
        long nowMs = System.currentTimeMillis();
        if (this.requestInactiveLocked(nowMs)) {
            return this.decideInactivityLocked(nowMs, signal);
        }
        PublicationPermit permit = scheduler.requirePublicationPermitLocked(this, PublicationKind.DELIVERY);
        try {
            DeliveryPublication publication = this.acknowledgeDelivery(permit, nowMs);
            return scheduler.deliveryEffects(this, publication, signal);
        } catch (RuntimeException | Error failure) {
            permit.abandonIfUnused();
            throw failure;
        }
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
        if (!valid) {
            throw new IllegalArgumentException("deferred terminal kind requires its exact payload");
        }
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
record TerminalAction(BalanceContext requestContext, RequestRoute item, DeliveryClaimKind deliveryKind, boolean endpointsSettled, PreemptionRegistration preemption, ExpirationTimer.DetachedDeadlines terminalResources, DeferredTerminal event, Response response, BalanceContext.PublicationPermit publication) {

}
