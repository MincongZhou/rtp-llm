package org.flexlb.balance.scheduler;

import org.flexlb.dao.loadbalance.Response;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Public request scheduling and graceful-drain contract. */
public interface RequestScheduler {
    /**
     * Accept a request and return its scheduling result, not an enqueue acknowledgement.
     * The returned future retains caller cancellation semantics. Response completion
     * does not imply that the request's resource obligations have ended.
     */
    CompletableFuture<Response> submit(BalanceContext request);

    /**
     * Install caller cancellation after the owner and result future are bound.
     * QUEUE overrides this to invoke the callback before enqueueing. The default preserves
     * DIRECT's immediate submission and invokes the callback after submit returns.
     * Rejected registrations do not invoke it.
     */
    default CompletableFuture<Response> submit(BalanceContext request, Runnable onRegistered) {
        java.util.Objects.requireNonNull(onRegistered, "onRegistered");
        CompletableFuture<Response> result = submit(request);
        if (request != null && request.scheduler() != null && request.getFuture() == result) {
            onRegistered.run();
        }
        return result;
    }

    /**
     * Request cancellation from the owning scheduler. Acceptance does not imply
     * immediate resource release. Returns null when no matching request is known.
     */
    RequestState cancel(long requestId, long batchId, CancelReason reason);

    /**
     * Idempotently stop accepting new requests without cancelling accepted work.
     * Accepted requests, including queued requests, continue to make progress.
     */
    void stopAccepting();

    /**
     * Complete after admission has stopped and accepted requests, their resource
     * obligations and pending response publications have drained. Observing this
     * stage does not initiate shutdown. Cleanup failure completes it exceptionally.
     * Shared endpoints and runtime infrastructure are not closed by this contract.
     */
    CompletionStage<Void> termination();
}
