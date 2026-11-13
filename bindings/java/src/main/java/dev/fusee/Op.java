package dev.fusee;

import java.lang.reflect.Type;

/**
 * An operation workflow code started and may wait for later: a step, sleep,
 * wait, child, async completion, or code started with {@link Ctx#async}.
 * Pass several to {@link Ctx#all} or {@link Ctx#race}. An Op belongs to the
 * run that made it; use it only in that run's workflow code.
 */
public final class Op<T> {
    final RunExec x;
    final OpState o;
    private final Type type;

    Op(RunExec x, OpState o, Type type) {
        this.x = x;
        this.o = o;
        this.type = type;
    }

    /** Waits for the operation and returns its result (null for a sleep or a wait that timed out). */
    @SuppressWarnings("unchecked")
    public T get() {
        if (!o.done) x.ctx.await(o);
        if (!o.done) throw Errors.ended();
        if (o.err != null) throw Ctx.rethrow(o.err);
        switch (o.kind) {
            case OpState.TASK, OpState.GROUP:
                return (T) o.value;
            case OpState.SLEEP:
                return null;
            default:
                break;
        }
        if (o.timedOut) return null;
        return (T) x.e.codec.decode(o.payload, type);
    }

    /** Whether the operation finished (without waiting). */
    public boolean isDone() {
        return o.done;
    }

    /** Whether a wait (Ctx.startWait) ended by its timeout. */
    public boolean timedOut() {
        return o.timedOut;
    }
}
