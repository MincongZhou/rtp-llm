package org.flexlb.balance.scheduler;

import org.flexlb.balance.scheduler.BalanceContext.PublicationKind;
import org.flexlb.balance.scheduler.BalanceContext.RequestFuture;
import org.flexlb.balance.scheduler.BalanceContext.ResponseResult;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Failures;
import org.flexlb.util.Logger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Publishes frontend completions without running user continuations on a
 * scheduler, endpoint, or transport critical path.
 *
 * <p>BalanceContext alone arbitrates frontend results. Delivery acknowledgement
 * reporting runs here before asking the context to select its response; a terminal
 * fact recorded in the meantime can invalidate that acknowledgement. Already
 * selected terminal responses are queued directly. This executor owns execution,
 * in-flight accounting and shutdown, while request resource settlement remains with BalanceContext.
 * External Future operations execute synchronously; internal responses are
 * queued so user continuations run outside scheduler and endpoint locks.
 */
final class RequestCompletionPublisher implements AutoCloseable {

    /**
     * Invocation-local proof that one exact lifecycle edge owns one frontend
     * publication. The capability is never stored in a slot or registry.
     */
    static final class PublicationPermit {

        private final RequestCompletionPublisher publisher;

        final BalanceContext slot;

        final PublicationKind kind;

        private final AtomicBoolean claimed = new AtomicBoolean();

        private final AtomicBoolean closed = new AtomicBoolean();

        PublicationPermit(RequestCompletionPublisher publisher, BalanceContext slot, PublicationKind kind) {
            this.publisher = Objects.requireNonNull(publisher, "publisher");
            this.slot = slot;
            this.kind = kind;
        }

        BalanceContext slot() {
            return slot;
        }

        void closePublication() {
            if (closed.compareAndSet(false, true)) {
                publisher.exitPublication(this);
            }
        }

        /**
         * Abandon a permit only when no other submitter consumed it.
         */
        void abandonIfUnclaimed() {
            if (claimed.compareAndSet(false, true)) {
                closePublication();
            }
        }

        void claim() {
            if (!claimed.compareAndSet(false, true)) {
                throw new IllegalStateException("publication permit already consumed for request " + slot.getRequestId());
            }
        }
    }

    enum ResponseCompletion {

        RESPONSE, FAILURE, CANCELLATION
    }

    /**
     * Frozen response selection; a null result publishes nothing.
     */
    record SelectedPublication(PublicationPermit permit, RequestFuture future, ResponseResult result) {

        boolean complete() {
            if (result == null) {
                return false;
            }
            return switch (result.completion()) {
                case RESPONSE ->
                    future.completeOwned(result.response());
                case FAILURE ->
                    future.completeExceptionallyOwned(result.failure());
                case CANCELLATION ->
                    future.cancelOwned(result.interrupt());
            };
        }
    }

    private static final int DEFAULT_PUBLISHER_WORKERS = 8;

    private final org.flexlb.service.monitor.BatchSchedulerReporter reporter;

    private final ThreadPoolExecutor executor;
    private final java.util.concurrent.ExecutorService recovery = java.util.concurrent.Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("request-completion-publisher-recovery-", 1).factory());

    private final Object lifecycleMonitor = new Object();

    private final ThreadLocal<Boolean> publicationActive =
            new ThreadLocal<>();

    /** Null while open; otherwise the shared, uninterruptible close result. */
    private CompletableFuture<Throwable> closeCompletion;

    private final java.util.Set<PublicationPermit> publications =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    RequestCompletionPublisher(int configuredWorkers, org.flexlb.service.monitor.BatchSchedulerReporter reporter) {
        this.reporter = reporter;
        int workers = configuredWorkers > 0
                ? configuredWorkers : DEFAULT_PUBLISHER_WORKERS;
        executor = new ThreadPoolExecutor(
                workers,
                workers,
                0L,
                TimeUnit.MILLISECONDS,
                // Queue completions so a busy publisher never runs client callbacks
                // inline on a decision thread. Slot owns request lifetime; the
                // publisher owns only these in-flight frontend completions.
                new LinkedBlockingQueue<>(),
                Thread.ofPlatform().daemon().name("request-completion-publisher-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
        executor.prestartAllCoreThreads();
    }

    // ── 发布许可：记录实际尚未结束的回包 ──
    RequestCompletionPublisher.PublicationPermit tryReservePublication(BalanceContext exactSlot, BalanceContext.PublicationKind kind) {
        synchronized (lifecycleMonitor) {
            if (closeCompletion != null) {
                return null;
            }
            var permit = new PublicationPermit(this, exactSlot, kind);
            publications.add(permit);
            return permit;
        }
    }

    private void exitPublication(PublicationPermit permit) {
        synchronized (lifecycleMonitor) {
            publications.remove(permit);
            if (publications.isEmpty()) { lifecycleMonitor.notifyAll(); }
        }
    }

    private void requireOwnedPermit(
            RequestCompletionPublisher.PublicationPermit permit) {
        if (permit.publisher != this) {
            throw new IllegalStateException(
                    "publication permit belongs to another publisher");
        }
    }

    // ── 响应入口：ACK、异步提交与同步 Future 完成 ──
    void submitDelivery(BalanceContext.DeliveryPublication delivery) {
        try {
            if (delivery.requestDeadline() != null) {
                delivery.requestDeadline().cancel();
            }
        } catch (Throwable failure) {
            Logger.error("Delivery deadline cancellation failed request_id={}", delivery.item().requestId(), failure);
        }
        try {
            enqueue(() -> {
                try {
                    if (delivery.batchEnqueueStartedAtMs() > 0L && delivery.item().ctx().getAckAtMs() > 0L) {
                        reporter.reportLatency(BatchSchedulerReporter.Latency.DISPATCH_ACK,
                                org.flexlb.dao.route.RoleType.PREFILL.name(),
                                delivery.item().prefillEp() == null ? "" : delivery.item().prefillEp().getIp(),
                                Math.max(0L, delivery.item().ctx().getAckAtMs() - delivery.batchEnqueueStartedAtMs()));
                    }
                } catch (Throwable failure) {
                    Logger.error("Delivery ACK reporting failed request_id={}", delivery.item().requestId(), failure);
                }
                publishNow(BalanceContext.selectPublication(delivery.publication().slot(), delivery.publication(),
                        ResponseCompletion.RESPONSE, delivery.response(), null, false));
            });
        } catch (RuntimeException | Error failure) {
            delivery.publication().closePublication();
            throw failure;
        }
    }

    /**
     * Queue an already-selected result; all request arbitration has finished.
     */
    void submit(RequestCompletionPublisher.SelectedPublication publication) {
        RequestCompletionPublisher.PublicationPermit permit = publication.permit();
        permit.slot().requireOutsideSlotLock("response submission");
        try {
            requireOwnedPermit(permit);
            enqueue(() -> publishNow(publication));
        } catch (RuntimeException | Error enqueueFailure) {
            permit.closePublication();
            throw enqueueFailure;
        }
    }

    // The unbounded executor queue also owns publications submitted by callbacks.
    // No second thread-local queue is needed for reentrant submissions.
    private void enqueue(Runnable publication) {
        try {
            executor.execute(publication);
        } catch (RejectedExecutionException closed) {
            // The result and permit have already been selected. Rejection must
            // not strand the Future or run client callbacks on a scheduler owner.
            recovery.execute(publication);
        }
    }

    /** Execute a selected result synchronously, including nested external Future completions. */
    boolean publishNow(RequestCompletionPublisher.SelectedPublication publication) {
        RequestCompletionPublisher.PublicationPermit permit = publication.permit();
        boolean outermost = false;
        try {
            permit.slot().requireOutsideSlotLock("response completion");
            requireOwnedPermit(permit);
            outermost = publicationActive.get() == null;
            if (outermost) {
                publicationActive.set(Boolean.TRUE);
            }
            return publication.complete();
        } finally {
            if (outermost) {
                publicationActive.remove();
            }
            permit.closePublication();
        }
    }

    // ── 关闭：停止接收、等待在途发布、关闭线程池 ──
    @Override
    public void close() {
        boolean reentrant = publicationActive.get() != null;
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
                Failures.rethrow(completion.join(), "completion publisher close failed");
            }
            return;
        }

        if (reentrant) {
            try {
                Thread closer = new Thread(
                        this::finishClose,
                        "request-completion-publisher-close");
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
        Failures.rethrow(completion.join(), "completion publisher close failed");
    }

    private void finishClose() {
        boolean interrupted = false;
        synchronized (lifecycleMonitor) {
            while (!publications.isEmpty()) {
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
