package org.flexlb.balance.scheduler;

import org.flexlb.util.Failures;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Runs frontend completion operations outside request locks and scheduler threads.
 * Owns accepted publication accounting and drain, including callback-reentrant close.
 * Request arbitration, deadlines and ACK reporting belong to the scheduler.
 */
final class ResponseCompletionExecutor implements AutoCloseable {

    /** One accepted execution obligation; contains no request state or response policy. */
    static final class CompletionRegistration {
        private final ResponseCompletionExecutor executor;
        private final AtomicBoolean closed = new AtomicBoolean();

        CompletionRegistration(ResponseCompletionExecutor executor) {
            this.executor = Objects.requireNonNull(executor);
        }

        void close() {
            if (closed.compareAndSet(false, true)) { executor.exitCompletion(this); }
        }
    }

    private static final int DEFAULT_COMPLETION_WORKERS = 8;

    private final ThreadPoolExecutor executor;
    private final java.util.concurrent.ExecutorService recovery = java.util.concurrent.Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("response-completion-recovery-", 1).factory());

    private final Object lifecycleMonitor = new Object();

    private final ThreadLocal<Boolean> completionActive =
            new ThreadLocal<>();

    /** Null while open; otherwise the shared, uninterruptible close result. */
    private CompletableFuture<Throwable> closeCompletion;

    private final java.util.Set<CompletionRegistration> registrations =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    ResponseCompletionExecutor(int configuredWorkers) {
        int workers = configuredWorkers > 0
                ? configuredWorkers : DEFAULT_COMPLETION_WORKERS;
        executor = new ThreadPoolExecutor(
                workers,
                workers,
                0L,
                TimeUnit.MILLISECONDS,
                // Queue completions so a busy completion executor never runs client callbacks
                // inline on a decision thread. BalanceContext owns request lifetime; the
                // executor owns only these in-flight frontend completions.
                new LinkedBlockingQueue<>(),
                Thread.ofPlatform().daemon().name("response-completion-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
        executor.prestartAllCoreThreads();
    }

    CompletionRegistration tryRegister() {
        synchronized (lifecycleMonitor) {
            if (closeCompletion != null) { return null; }
            var registration = new CompletionRegistration(this);
            registrations.add(registration);
            return registration;
        }
    }

    private void exitCompletion(CompletionRegistration registration) {
        synchronized (lifecycleMonitor) {
            registrations.remove(registration);
            if (registrations.isEmpty()) { lifecycleMonitor.notifyAll(); }
        }
    }

    private void requireOwnedRegistration(CompletionRegistration registration) {
        if (registration.executor != this) {
            throw new IllegalStateException("completion registration belongs to another executor");
        }
    }

    /** Runs a scheduler-owned completion operation, without interpreting request facts. */
    void submit(CompletionRegistration registration, BooleanSupplier completion) {
        try {
            requireOwnedRegistration(registration);
            try {
                executor.execute(() -> completeNow(registration, completion));
            } catch (RejectedExecutionException closed) {
                // An accepted operation must not run client callbacks on its submitter.
                recovery.execute(() -> completeNow(registration, completion));
            }
        } catch (RuntimeException | Error failure) {
            registration.close();
            throw failure;
        }
    }

    /** External Future mutations stay synchronous and share callback-reentrant drain. */
    boolean completeNow(CompletionRegistration registration, BooleanSupplier completion) {
        boolean outermost = false;
        try {
            requireOwnedRegistration(registration);
            outermost = completionActive.get() == null;
            if (outermost) { completionActive.set(Boolean.TRUE); }
            return completion.getAsBoolean();
        } finally {
            if (outermost) { completionActive.remove(); }
            registration.close();
        }
    }

    // ── 关闭：停止接收、等待在途发布、关闭线程池 ──
    @Override
    public void close() {
        boolean reentrant = completionActive.get() != null;
        boolean alreadyClosing;
        CompletableFuture<Throwable> completion;
        synchronized (lifecycleMonitor) {
            alreadyClosing = closeCompletion != null;
            if (!alreadyClosing) {
                closeCompletion = new CompletableFuture<>();
            }
            completion = closeCompletion;
        }
        if (alreadyClosing) {
            // A callback cannot wait for itself; external callers join the same result.
            if (!reentrant || completion.isDone()) {
                Failures.rethrow(completion.join(), "response completion executor close failed");
            }
            return;
        }

        if (reentrant) {
            try {
                Thread closer = new Thread(
                        this::finishClose,
                        "response-completion-close");
                closer.setDaemon(false);
                closer.start();
            } catch (RuntimeException | Error startFailure) {
                try {
                    executor.shutdown();
                } catch (Throwable shutdownFailure) {
                    startFailure.addSuppressed(shutdownFailure);
                }
                completion.complete(startFailure);
                throw startFailure;
            }
            return;
        }
        finishClose();
        Failures.rethrow(completion.join(), "response completion executor close failed");
    }

    private void finishClose() {
        boolean interrupted = false;
        synchronized (lifecycleMonitor) {
            while (!registrations.isEmpty()) {
                try {
                    lifecycleMonitor.wait();
                } catch (InterruptedException interruption) {
                    interrupted = true;
                }
            }
        }

        Throwable failure = null;
        try {
            executor.shutdown();
            recovery.shutdown();
            while (!executor.isTerminated() || !recovery.isTerminated()) {
                try {
                    executor.awaitTermination(1, TimeUnit.DAYS);
                    recovery.awaitTermination(1, TimeUnit.DAYS);
                } catch (InterruptedException interruption) {
                    interrupted = true;
                }
            }
        } catch (Throwable shutdownFailure) {
            failure = shutdownFailure;
        } finally {
            closeCompletion.complete(failure);
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
