package dev.fusee;

import java.time.Duration;

/** How a step runs. Setters return this options object; it is read when the step begins. */
public final class StepOptions {
    RetryPolicy retry;
    long timeoutMs, heartbeatTimeoutMs;
    String limit;
    int priority;
    String fairnessKey = "";
    int fairnessWeight;
    boolean readOnly, inline;

    public StepOptions() {}

    public static StepOptions defaults() {
        return new StepOptions();
    }

    /** The retry policy (default {@link RetryPolicy#DEFAULT}). */
    public StepOptions retry(RetryPolicy p) {
        retry = p;
        return this;
    }

    /**
     * An attempt that takes longer fails with a TimedOutException; its
     * thread is interrupted and a late result is ignored.
     */
    public StepOptions timeout(Duration d) {
        timeoutMs = d.toMillis();
        return this;
    }

    /** An attempt that sends no heartbeat (Ctx.heartbeat) for this long fails. */
    public StepOptions heartbeatTimeout(Duration d) {
        heartbeatTimeoutMs = d.toMillis();
        return this;
    }

    /** Each attempt first takes a permit of this limit (Engine.limit). */
    public StepOptions limit(String name) {
        limit = name;
        return this;
    }

    /** While waiting for a permit, a higher priority goes first. Not journaled. */
    public StepOptions priority(int p) {
        priority = p;
        return this;
    }

    /**
     * Steps of one priority take turns by fairness key, each key weight
     * permits per turn (0: 1). Not journaled.
     */
    public StepOptions fairness(String key, int weight) {
        fairnessKey = key == null ? "" : key;
        fairnessWeight = weight;
        return this;
    }

    public StepOptions fairness(String key) {
        return fairness(key, 0);
    }

    /**
     * Run at once, without waiting for earlier records to be durable. Only
     * for steps that change nothing outside the process: a crash can make
     * them run again with different inputs.
     */
    public StepOptions readOnly() {
        readOnly = true;
        return this;
    }

    /**
     * Run the function on the workflow's thread while it holds the engine,
     * instead of on a thread of its own: no handoff and the order it starts
     * in, but nothing else on this engine runs until it returns, and a
     * timeout cannot cut it short. For fast, CPU-only functions.
     */
    public StepOptions inline() {
        inline = true;
        return this;
    }
}
