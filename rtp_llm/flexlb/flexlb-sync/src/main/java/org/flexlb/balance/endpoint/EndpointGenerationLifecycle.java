package org.flexlb.balance.endpoint;

import org.flexlb.util.Failures;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** One endpoint-generation admission gate and its retirement drain. */
final class EndpointGenerationLifecycle {

    private static final long DEFAULT_RETIREMENT_TIMEOUT_MS =
            Long.getLong("flexlb.endpoint.retirement.timeout.ms", 30_000L);

    private enum RetirementPhase {
        ACCEPTING_HANDOFFS,
        RETIRING,
        WAITING_HANDOFFS,
        CLEANUP_SCHEDULED,
        CLEANING,
        RETIRED
    }

    private volatile RetirementPhase phase =
            RetirementPhase.ACCEPTING_HANDOFFS;
    private final Runnable handoffsDrained;
    private int activeHandoffs;
    private Thread cleanupThread;
    private Throwable retirementFailure;

    EndpointGenerationLifecycle(Runnable handoffsDrained) {
        this.handoffsDrained = Objects.requireNonNull(
                handoffsDrained, "handoffsDrained");
    }

    synchronized HandoffPermit tryAcquireHandoff() {
        if (phase != RetirementPhase.ACCEPTING_HANDOFFS) {
            return null;
        }
        activeHandoffs++;
        return new HandoffPermit(this);
    }

    boolean isRetiringOrRetired() {
        return phase != RetirementPhase.ACCEPTING_HANDOFFS;
    }

    /** Close the gate without waiting or running endpoint cleanup. */
    synchronized void beginRetirement() {
        if (phase == RetirementPhase.ACCEPTING_HANDOFFS) {
            phase = RetirementPhase.RETIRING;
        }
    }

    /** Claim cleanup atomically; return true only when this caller should run it. */
    synchronized boolean tryStartCleanup() {
        if (phase == RetirementPhase.ACCEPTING_HANDOFFS) {
            throw new IllegalStateException(
                    "endpoint retirement gate is still open");
        }
        if (phase != RetirementPhase.RETIRING) {
            return false;
        }
        phase = activeHandoffs == 0
                ? RetirementPhase.CLEANUP_SCHEDULED : RetirementPhase.WAITING_HANDOFFS;
        return phase == RetirementPhase.CLEANUP_SCHEDULED;
    }

    /** Bind the claimed cleanup to its execution thread for reentrant close. */
    synchronized void beginCleanup() {
        if (phase != RetirementPhase.CLEANUP_SCHEDULED || activeHandoffs != 0) {
            throw new IllegalStateException(
                    "endpoint retirement cleanup owner is invalid");
        }
        phase = RetirementPhase.CLEANING;
        cleanupThread = Thread.currentThread();
    }

    synchronized void completeRetirement(Throwable failure) {
        if (phase != RetirementPhase.CLEANING || activeHandoffs != 0) {
            throw new IllegalStateException(
                    "endpoint generation cleanup is not ready to complete");
        }
        retirementFailure = failure;
        cleanupThread = null;
        phase = RetirementPhase.RETIRED;
        notifyAll();
    }

    void awaitRetirement() {
        awaitRetirement(DEFAULT_RETIREMENT_TIMEOUT_MS);
    }

    void awaitRetirement(long timeoutMs) {
        if (timeoutMs <= 0L) {
            throw new IllegalArgumentException(
                    "endpoint retirement timeout must be positive");
        }
        boolean interrupted = false;
        Throwable failure;
        long deadlineNanos = System.nanoTime()
                + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        try {
            synchronized (this) {
                if (phase == RetirementPhase.ACCEPTING_HANDOFFS) {
                    throw new IllegalStateException(
                            "endpoint retirement has not begun");
                }
                if (phase == RetirementPhase.RETIRING) {
                    throw new IllegalStateException(
                            "endpoint retirement cleanup has not been initiated");
                }
                if (phase == RetirementPhase.CLEANING
                        && cleanupThread == Thread.currentThread()) {
                    throw new IllegalStateException(
                            "endpoint retirement cleanup cannot await itself");
                }
                while (phase != RetirementPhase.RETIRED) {
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0L) {
                        throw new IllegalStateException(
                                "endpoint retirement timed out after "
                                        + timeoutMs + "ms: phase=" + phase
                                        + ", activeHandoffs=" + activeHandoffs);
                    }
                    try {
                        long waitMillis = Math.max(
                                1L,
                                java.util.concurrent.TimeUnit.NANOSECONDS
                                        .toMillis(remainingNanos));
                        wait(waitMillis);
                    } catch (InterruptedException interruption) {
                        interrupted = true;
                    }
                }
                failure = retirementFailure;
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        Failures.rethrow(failure, "endpoint generation retirement failed");
    }

    private void releaseHandoff() {
        boolean runContinuation = false;
        synchronized (this) {
            if (activeHandoffs <= 0) {
                throw new IllegalStateException(
                        "endpoint handoff permit released more than once");
            }
            activeHandoffs--;
            if (activeHandoffs == 0 && phase == RetirementPhase.WAITING_HANDOFFS) {
                phase = RetirementPhase.CLEANUP_SCHEDULED;
                runContinuation = true;
            }
        }
        if (runContinuation) {
            handoffsDrained.run();
        }
    }

    static final class HandoffPermit implements AutoCloseable {
        private final EndpointGenerationLifecycle lifecycle;
        private final AtomicBoolean open = new AtomicBoolean(true);

        private HandoffPermit(EndpointGenerationLifecycle lifecycle) {
            this.lifecycle = lifecycle;
        }

        @Override
        public void close() {
            if (open.compareAndSet(true, false)) {
                lifecycle.releaseHandoff();
            }
        }

        boolean isOpen() {
            return open.get();
        }
    }
}
