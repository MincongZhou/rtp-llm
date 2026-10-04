package org.flexlb.balance.scheduler;

import org.flexlb.util.Logger;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Shared workers for exact request facts after they leave queue ownership.
 */
final class RequestContinuationExecutor implements AutoCloseable {

    private final ExecutorService workers;
    private final ExecutorService recovery = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("request-continuation-recovery-", 1).factory());

    private final Object lifecycle = new Object();

    /** An entry remains present while its current fact runs, even when its deque is empty. */
    private final Map<BalanceContext, ArrayDeque<Runnable>> queues = new IdentityHashMap<>();

    private boolean accepting = true;

    RequestContinuationExecutor() {
        int count = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors()));
        workers = Executors.newFixedThreadPool(count,
                Thread.ofPlatform().daemon().name("request-continuation-", 1).factory());
    }

    void submit(BalanceContext context, Runnable fact) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(fact, "fact");
        boolean start;
        synchronized (lifecycle) {
            if (!accepting) {
                throw new IllegalStateException("request continuation executor is closed");
            }
            AbstractRequestScheduler owner = context.scheduler();
            if (!owner.retainIfActive()) { return; }
            start = !queues.containsKey(context);
            queues.computeIfAbsent(context, ignored -> new ArrayDeque<>()).addLast(() -> {
                try { fact.run(); }
                catch (Throwable failure) {
                    owner.recordFailure(failure);
                    throw failure;
                } finally { owner.release(); }
            });
        }
        if (!start) {
            return;
        }
        try {
            workers.execute(() -> drain(context));
        } catch (RuntimeException rejected) {
            recovery.execute(() -> drain(context));
        }
    }

    private void drain(BalanceContext context) {
        while (true) {
            Runnable fact;
            synchronized (lifecycle) {
                fact = queues.get(context).pollFirst();
                if (fact == null) {
                    queues.remove(context);
                    if (queues.isEmpty()) { lifecycle.notifyAll(); }
                    return;
                }
            }
            try {
                fact.run();
            } catch (Throwable failure) {
                try {
                    logFailure(context.getRequestId(), failure);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    void awaitIdle() {
        boolean interrupted = false;
        synchronized (lifecycle) {
            while (!queues.isEmpty()) {
                try { lifecycle.wait(); }
                catch (InterruptedException ignored) { interrupted = true; }
            }
        }
        if (interrupted) { Thread.currentThread().interrupt(); }
    }

    @Override
    public void close() {
        boolean interrupted = false;
        synchronized (lifecycle) {
            // Keep the monitor through the empty check and admission closure.
            awaitIdle();
            accepting = false;
        }
        workers.shutdown();
        recovery.shutdown();
        while (!workers.isTerminated() || !recovery.isTerminated()) {
            try { workers.awaitTermination(1, TimeUnit.DAYS);
                recovery.awaitTermination(1, TimeUnit.DAYS); }
            catch (InterruptedException ignored) { interrupted = true; }
        }
        if (interrupted) { Thread.currentThread().interrupt(); }
    }

    static void logFailure(long requestId, Throwable failure) {
        Logger.error("Request continuation failed: request_id={}", requestId, failure);
    }
}
