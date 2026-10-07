package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeResources;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.planner.GroupPlanner;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import java.util.concurrent.CompletableFuture;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * One exact worker assignment for a request, created before publication.
 * BalanceContext records the whole request lifecycle; this identity fences callbacks
 * from earlier assignments after retries or preemption. It owns committed capacity
 * only after ProvisionalRoute transfers it at queue publication or direct handoff.
 */
public final class RequestRoute implements GroupPlanner.Input {

    private static final AtomicLong WORKER_ENQUEUE_SEQUENCE = new AtomicLong();

    public static RequestRoute create(
            BalanceContext context, Response response, ServerStatus prefill, ServerStatus decode,
            PrefillEndpoint prefillEndpoint, DecodeEndpoint decodeEndpoint,
            DecodeResources.ReservationHandle decodeReservation, long enqueuedAtMs) {
        Objects.requireNonNull(context, "context");
        RequestRequirements frozenDecode = Objects.requireNonNull(context.getRequirements(), "registered request inputs");
        checkArgument(decodeReservation == null || decodeReservation.requestId() == frozenDecode.requestId(),
                "Decode reservation belongs to another request");
        if (context.getWorkerEnqueueSequence() == 0L) {
            context.initializeWorkerQueue(enqueuedAtMs, WORKER_ENQUEUE_SEQUENCE::incrementAndGet);
        }
        return new RequestRoute(context, response, prefill, prefillEndpoint,
                decode, decodeEndpoint, decodeReservation);
    }

    static RequestRoute create(
            BalanceContext context, ProvisionalRoute admission, long enqueuedAtMs) {
        admission.requireProvisional();
        checkArgument(context.getRequestId() == admission.requestId(), "admission cannot build another request");
        return create(context, admission.response(),
                ServerStatus.copyOf(admission.prefillStatus()), ServerStatus.copyOf(admission.decodeStatus()),
                admission.prefillEndpoint(), admission.decodeEndpoint(), admission.decodeReservation(), enqueuedAtMs);
    }

    private final BalanceContext ctx;
    private final Response routeResponse;
    private final ServerStatus prefill;
    private final PrefillEndpoint prefillEp;
    private final ServerStatus decode;
    private final DecodeEndpoint decodeEp;
    private final DecodeResources.ReservationHandle decodeReservation;
    private final long hitCache;
    RequestRoute(BalanceContext ctx, Response routeResponse, ServerStatus prefill,
                     PrefillEndpoint prefillEp, ServerStatus decode,
                     DecodeEndpoint decodeEp, DecodeResources.ReservationHandle decodeReservation) {
        this.ctx = ctx;
        this.routeResponse = routeResponse;
        this.prefill = prefill;
        this.prefillEp = prefillEp;
        this.decode = decode;
        this.decodeEp = decodeEp;
        this.decodeReservation = decodeReservation;
        this.hitCache = hitCacheOf(prefill);

    }

    // -- accessors --

    public BalanceContext ctx() { return ctx; }
    public CompletableFuture<Response> future() { return ctx.getFuture(); }
    public Response routeResponse() { return routeResponse; }
    public ServerStatus prefill() { return prefill; }
    public ServerStatus decode() { return decode; }
    public PrefillEndpoint prefillEp() { return prefillEp; }
    public DecodeEndpoint decodeEp() { return decodeEp; }
    public DecodeResources.ReservationHandle decodeReservation() {
        return decodeReservation;
    }

    public RequestRequirements requirements() {
        return ctx.getRequirements();
    }
    public long enqueuedAtMs() { return ctx.getFirstWorkerEnqueueTime(); }
    public long expiresAtMs() { return ctx.getRequestExpiresAtMs(); }
    public boolean requestExpired(long nowMs) {
        return ctx.requestExpired(nowMs);
    }

    /** Whether ACTIVE publication must atomically own one route reservation. */
    public boolean requiresRouteReservation() {
        return requirements().requiresRouteReservation();
    }

    /**
     * Normalized request priority for the per-worker ordered active index.
     */
    public int priority() {
        return requirements().priority();
    }

    /**
     * Unique monotonic enqueue sequence used by FIFO and as the same-priority
     * tie-break in {@link org.flexlb.util.PriorityOrdering}. A re-offer
     * keeps the original item and therefore the original queue position.
     */
    public long enqueueSeq() {
        return ctx.getWorkerEnqueueSequence();
    }

    // -- derived accessors --

    public long requestId() {
        return requirements().requestId();
    }

    /** Total sequence length of this request. */
    public long seqLen() {
        return requirements().hardKvTokens();
    }

    /** Cache-hit tokens on the assigned prefill endpoint. */
    public long hitCache() {
        return hitCache;
    }

    /** Extract cache-hit length from a {@link ServerStatus} debug info. */
    private static long hitCacheOf(ServerStatus ss) {
        return ss != null && ss.getDebugInfo() != null
                ? ss.getDebugInfo().getHitCacheLen() : 0;
    }

}
