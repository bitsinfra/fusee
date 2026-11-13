package dev.fusee;

import java.time.Duration;

/**
 * How a failing step is retried. Attempts includes the first; the delay
 * doubles from initial up to max (exponential, the default) or stays initial
 * (fixed). Immutable.
 */
public final class RetryPolicy {
    /** Three attempts, 100 ms doubling up to 10 s. */
    public static final RetryPolicy DEFAULT = new RetryPolicy(3, false, 100, 10_000);

    final int attempts;
    final boolean fixed;
    final long initialMs, maxMs;
    private final java.util.Set<String> nonRetryable;
    private final java.util.function.BiPredicate<Failure, Integer> retryIf;
    /** Null: the engine's {@link Options#onExhausted}. */
    final Exhausted onExhausted;

    private RetryPolicy(int attempts, boolean fixed, long initialMs, long maxMs) {
        this(attempts, fixed, initialMs, maxMs, java.util.Set.of(), null, null);
    }

    private RetryPolicy(int attempts, boolean fixed, long initialMs, long maxMs, java.util.Set<String> nonRetryable,
            java.util.function.BiPredicate<Failure, Integer> retryIf, Exhausted onExhausted) {
        this.attempts = attempts;
        this.fixed = fixed;
        this.initialMs = initialMs;
        this.maxMs = maxMs;
        this.nonRetryable = nonRetryable;
        this.retryIf = retryIf;
        this.onExhausted = onExhausted;
    }

    /**
     * What the step does when its attempts run out ({@link Exhausted#PAUSE}:
     * the run waits for {@link Run#resume()}).
     */
    public RetryPolicy onExhausted(Exhausted then) {
        return new RetryPolicy(attempts, fixed, initialMs, maxMs, nonRetryable, retryIf, then);
    }

    /** Failures of these types (the failure's or a cause's type: a class's simple name) are not retried. */
    public RetryPolicy nonRetryable(String... types) {
        return new RetryPolicy(attempts, fixed, initialMs, maxMs, java.util.Set.of(types), retryIf, onExhausted);
    }

    /** Retried only while this returns true (the failure, the attempt that failed). */
    public RetryPolicy retryIf(java.util.function.BiPredicate<Failure, Integer> test) {
        return new RetryPolicy(attempts, fixed, initialMs, maxMs, nonRetryable, test, onExhausted);
    }

    /** Whether a failed attempt is retried: not final, none of nonRetryable, and retryIf agrees. */
    boolean retries(Failure f, long attempt) {
        if (f.isFinal()) return false;
        for (Failure c = f; c != null; c = c.cause()) {
            if (c.type() != null && nonRetryable.contains(c.type())) return false;
        }
        return retryIf == null || retryIf.test(f, (int) attempt);
    }

    /** This many attempts, with the default delays. */
    public static RetryPolicy attempts(int n) {
        return DEFAULT.withAttempts(n);
    }

    /** One attempt: never retried. */
    public static RetryPolicy none() {
        return attempts(1);
    }

    public RetryPolicy withAttempts(int n) {
        return new RetryPolicy(n, fixed, initialMs, maxMs, nonRetryable, retryIf, onExhausted);
    }

    /** The first delay. */
    public RetryPolicy initial(Duration d) {
        return new RetryPolicy(attempts, fixed, d.toMillis(), maxMs, nonRetryable, retryIf, onExhausted);
    }

    /** The largest delay. */
    public RetryPolicy max(Duration d) {
        return new RetryPolicy(attempts, fixed, initialMs, d.toMillis(), nonRetryable, retryIf, onExhausted);
    }

    /** Every delay is the initial one. */
    public RetryPolicy fixed() {
        return new RetryPolicy(attempts, true, initialMs, maxMs, nonRetryable, retryIf, onExhausted);
    }

    public RetryPolicy exponential() {
        return new RetryPolicy(attempts, false, initialMs, maxMs, nonRetryable, retryIf, onExhausted);
    }

    public int attempts() {
        return attempts;
    }

    /** The delay after the failed-th attempt failed, in ms. */
    long delay(long failed) {
        if (fixed) return Math.min(maxMs, initialMs);
        double d = initialMs * Math.pow(2, failed - 1);
        return Math.min(maxMs, (long) d);
    }
}
