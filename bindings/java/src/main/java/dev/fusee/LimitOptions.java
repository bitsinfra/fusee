package dev.fusee;

/** The size of a concurrency and rate limit that steps take permits of. */
public final class LimitOptions {
    int concurrent;
    double perSecond;

    public LimitOptions() {}

    /** At most n steps hold a permit at once (0: no bound). */
    public static LimitOptions concurrent(int n) {
        return new LimitOptions().withConcurrent(n);
    }

    /** At most n permits are granted per second (0: no bound). */
    public static LimitOptions perSecond(double n) {
        return new LimitOptions().withPerSecond(n);
    }

    public LimitOptions withConcurrent(int n) {
        concurrent = n;
        return this;
    }

    public LimitOptions withPerSecond(double n) {
        perSecond = n;
        return this;
    }
}
