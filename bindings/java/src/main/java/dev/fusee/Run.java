package dev.fusee;

import java.lang.reflect.Type;
import java.time.Duration;

/** A handle to the run of a workflow ID (following continue-as-new and reset). */
public final class Run<O> {
    private final Engine e;
    private final String id;
    private final Type output;

    Run(Engine e, String id, Type output) {
        this.e = e;
        this.id = id;
        this.output = output;
    }

    public String id() {
        return id;
    }

    /** Sends a signal; returns once it is durable. */
    public void signal(String name, Object value) {
        e.signalWire(id, name, e.codec.encode(value));
    }

    /** Waits for the run's result; a failed, cancelled, timed-out or terminated run throws. */
    public O result() {
        return result(-1);
    }

    /** Waits at most this long (a TimedOutException after). */
    public O result(Duration timeout) {
        return result(timeout.toNanos());
    }

    @SuppressWarnings("unchecked")
    private O result(long nanos) {
        Engine.ResultMsg m = e.result(id, nanos);
        FuseeException err = Errors.settled(m.status(), m.payload());
        if (err != null) throw err;
        return (O) e.codec.decode(m.payload(), output);
    }

    /** Waits until a run with this ID exists (a debounced or batched run starts when its window closes). */
    public void started() {
        started(null);
    }

    public void started(Duration timeout) {
        long deadline = timeout == null ? Long.MAX_VALUE : System.nanoTime() + timeout.toNanos();
        long pause = 1;
        while (!e.exists(id)) {
            if (System.nanoTime() > deadline) throw new TimedOutException("run '" + id + "' did not start");
            try {
                Thread.sleep(pause);
            } catch (InterruptedException ie) {
                throw Errors.interrupted();
            }
            pause = Math.min(pause * 2, 50);
        }
    }

    /** Sends an update and waits for its handler's durable result. */
    public <R> R update(String name, Object arg, Class<R> result) {
        return update(name, arg, result, null);
    }

    @SuppressWarnings("unchecked")
    public <R> R update(String name, Object arg, Class<R> result, UpdateOptions opts) {
        byte[] p = e.updateWire(id, name, opts == null ? "" : opts.id, e.codec.encode(arg));
        return (R) e.codec.decode(p, result);
    }

    /** Calls a query handler; the answer waits until the state it read is durable. */
    @SuppressWarnings("unchecked")
    public <R> R query(String name, Object arg, Class<R> result) {
        return (R) e.codec.decode(e.query(id, name, e.codec.encode(arg)), result);
    }

    public <R> R query(String name, Class<R> result) {
        return query(name, null, result);
    }

    /** Asks the run to cancel: its pending operations fail with a CancelledException, and its code may compensate. */
    public void cancel(String reason) {
        e.cancelWire(id, reason, false);
    }

    /**
     * Resumes a run that paused because a step ran out of retries
     * ({@link RetryPolicy#onExhausted}): the step runs again at once, with a
     * fresh set of attempts, on the code this process has now. A
     * {@link RejectedException} if nothing is paused.
     */
    public void resume() {
        e.resumeWire(id);
    }

    /** Ends the run at once; children follow their parent-close policy. */
    public void terminate(String reason) {
        e.cancelWire(id, reason, true);
    }

    /** Starts a new run whose journal is this one's cut before the step beforeStep. */
    public void reset(String beforeStep) {
        reset(beforeStep, null);
    }

    public void reset(String beforeStep, ResetOptions opts) {
        ResetOptions o = opts == null ? new ResetOptions() : opts;
        e.resetWire(id, beforeStep, !o.dropSignals, o.version);
    }

    /** A reader of the run's stream name from its first chunk. */
    public StreamReader stream(String name) {
        return stream(name, 0);
    }

    /** A reader of the run's stream name from offset from, in this process. */
    public StreamReader stream(String name, long from) {
        return new StreamReader((run, at, waitMs) -> Codec.utf8(e.streamReadWire(id, run, name, at, waitMs)), from);
    }

    /** The run's summary and normalized journal, or null. */
    public RunDetail inspect() {
        byte[] j = e.hold(() -> e.inspectRaw(id));
        return j == null ? null : RunDetail.of(j);
    }

    @Override
    public String toString() {
        return "Run(" + id + ")";
    }
}
