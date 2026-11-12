package dev.fusee;

/**
 * What a step's function knows about its attempt, passed to a function that
 * takes one ({@link StepFn}): its name, attempt, idempotency key and run, and
 * its heartbeat. Use it from the function's own thread.
 */
public final class StepCtx {
    private final Ctx ctx;
    private final String name;

    StepCtx(Ctx ctx, String name) {
        this.ctx = ctx;
        this.name = name;
    }

    /** The step's name. */
    public String name() {
        return name;
    }

    /** This attempt, from 1. */
    public long attempt() {
        StepInfo s = StepInfo.CURRENT.get();
        return s == null ? 1 : s.attempt();
    }

    /** A key stable across retries and recovery: pass it to services that take idempotency keys. */
    public String key() {
        return ctx.key();
    }

    /** The run's workflow ID. */
    public String workflowId() {
        return ctx.workflowId();
    }

    /** Reports progress and resets the step's heartbeat timeout. */
    public void heartbeat(Object progress) {
        ctx.heartbeat(progress);
    }
}
