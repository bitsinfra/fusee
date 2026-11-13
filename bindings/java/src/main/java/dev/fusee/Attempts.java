package dev.fusee;

import java.util.ArrayList;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;

/**
 * Runs a step's function (or an async completion's send function) under its
 * gate, permit, timeouts and retry policy. Every method is called by the
 * thread holding the engine.
 */
final class Attempts {
    /** One attempt: the encoded result. */
    interface Call {
        byte[] call() throws Exception;
    }

    /** Prepares an attempt holding the engine (an await's token). */
    interface Launch {
        Call launch();
    }

    /** Records the final outcome. */
    interface Done {
        void done(boolean ok, byte[] data, String msg);
    }

    private final RunExec x;
    private final Engine e;
    private final int seq;
    private long gate;
    private long attempt;

    /** The attempt running now, from 1. */
    long attempt() {
        return attempt;
    }
    private long notBefore;
    private final Launch launch;
    private final StepOptions o;
    private final RetryPolicy retry;
    private final Done done;
    // What begin found for the first attempt's gate: -1 unknown, 0 shut (the
    // engine wakes the run when it may open), 1 open.
    private int known;
    private long ticket;
    private boolean hasTicket, running, settled;
    // The one pending timer that calls tryRun again (a retry delay or a rate
    // limit's refill), by generation.
    private Engine.Timer later;
    private long laterGen, gen;
    private final ArrayList<Engine.Timer> timers = new ArrayList<>(2);
    private FutureTask<Void> future;
    /** A step (not an await's send) pauses when its retries run out. */
    boolean pausable;
    /** The step's name, for the paused event. */
    String name = "";
    /** The retry time that means "until resumed" (the engine's u64::MAX). */
    static final long PAUSED = -1L;

    Attempts(RunExec x, int seq, long gate, long attempt, long notBefore, Launch launch, StepOptions o,
            RetryPolicy retry, int known, Done done) {
        this.x = x;
        this.e = x.e;
        this.seq = seq;
        this.gate = gate;
        this.attempt = attempt;
        this.notBefore = notBefore;
        this.launch = launch;
        this.o = o;
        this.retry = retry;
        this.known = known;
        this.done = done;
    }

    /** Whether attempt generation g is the one running; holder only. */
    boolean live(long g) {
        return running && gen == g;
    }

    private boolean limited() {
        return o.limit != null && !o.limit.isEmpty();
    }

    private void cancelLater() {
        if (later != null) {
            later.cancel();
            later = null;
        }
    }

    private void retryAt(long at) {
        cancelLater();
        long g = ++laterGen;
        later = e.at(at, () -> {
            if (laterGen != g) return;
            later = null;
            tryRun();
        });
    }

    private void release() {
        if (hasTicket && limited()) {
            hasTicket = false;
            try {
                e.host(x.shard, x.run, Native.OP_RELEASE, ticket, 0, 0, Codec.utf8(o.limit), null, null, null);
            } catch (RuntimeException ignored) {
                // The run ended: its permits went with it.
            }
        }
    }

    /** Settles the step's operation with err. */
    private void fail(RuntimeException err) {
        settled = true;
        OpState op = x.pending.remove(seq);
        if (op != null) x.settle(op, x.note(err));
        x.runReady();
    }

    void tryRun() {
        // A retry timer and a wake can both get here: one attempt runs.
        if (running || settled) return;
        if (x.ended || !x.pending.containsKey(seq)) {
            x.ungate(seq);
            cancelLater();
            release();
            return;
        }
        // A paused step waits for its resume, which wakes the run. What begin
        // found for its gate is stale by then: ask again.
        if (notBefore == PAUSED) {
            known = -1;
            long[] r = new long[5];
            try {
                e.host(x.shard, x.run, Native.OP_STEP_PAUSED, seq & 0xffffffffL, 0, 0, null, null, null, r);
            } catch (RuntimeException err) {
                fail(err);
                return;
            }
            if (r[0] != 0) {
                x.gate(seq, this::tryRun);
                return;
            }
            // Resumed: a fresh set of attempts, counted from here.
            notBefore = 0;
            attempt = 1;
        }
        // A wake during the retry delay cannot let the attempt run: the
        // delay's timer calls back.
        if (later != null && Engine.nowMs() < notBefore) return;
        int k = known;
        known = -1;
        boolean open = k == 1;
        if (k == -1) {
            long g = o.readOnly ? 0 : gate;
            try {
                open = x.mayRun(g, notBefore);
            } catch (RuntimeException err) {
                fail(err);
                return;
            }
        }
        if (!open) {
            x.gate(seq, this::tryRun);
            if (notBefore > Engine.nowMs()) retryAt(notBefore);
            return;
        }
        if (limited()) {
            try {
                long[] r = new long[5];
                if (!hasTicket) {
                    e.host(x.shard, x.run, Native.OP_LIMIT_TICKET, 0, 0, 0, null, null, null, r);
                    ticket = r[0];
                    hasTicket = true;
                }
                e.host(x.shard, x.run, Native.OP_ACQUIRE, ticket, o.priority, o.fairnessWeight & 0xffffffffL,
                        Codec.utf8(o.limit), Codec.utf8(o.fairnessKey), null, r);
                if (r[0] != 0) {
                    // Woken when the permit may be ours (or at the refill).
                    x.gate(seq, this::tryRun);
                    if (r[0] == 1) retryAt(r[1]);
                    return;
                }
            } catch (RuntimeException err) {
                fail(err);
                return;
            }
        }
        x.ungate(seq);
        cancelLater();
        Call fn = null;
        RuntimeException launchErr = null;
        try {
            fn = launch.launch();
        } catch (RuntimeException err) {
            launchErr = err;
        }
        running = true;
        long g = ++gen;
        x.beats.put(seq, Engine.nowMs());
        if (launchErr != null) {
            finished(g, null, launchErr);
            return;
        }
        if (o.inline) {
            StepInfo info = new StepInfo(x, seq, true, this, g);
            StepInfo prev = StepInfo.CURRENT.get();
            StepInfo.CURRENT.set(info);
            x.current = seq;
            byte[] data = null;
            Throwable err = null;
            try {
                data = fn.call();
            } catch (Task.Killed killed) {
                throw killed;
            } catch (Throwable t) {
                err = t;
            } finally {
                x.current = -1;
                StepInfo.CURRENT.set(prev);
            }
            finished(g, data, err);
            return;
        }
        StepInfo info = new StepInfo(x, seq, false, this, g);
        Call call = fn;
        FutureTask<Void> ft = new FutureTask<>(() -> {
            StepInfo.CURRENT.set(info);
            byte[] data = null;
            Throwable err = null;
            try {
                data = call.call();
            } catch (Throwable t) {
                err = t;
            } finally {
                StepInfo.CURRENT.remove();
            }
            byte[] d = data;
            Throwable er = err;
            e.post(() -> finished(g, d, er));
        }, null);
        future = ft;
        x.running.add(this);
        if (o.timeoutMs > 0) {
            long t = o.timeoutMs;
            timers.add(e.at(Engine.nowMs() + t, () -> expire(g, "step timed out after " + t + " ms")));
        }
        if (o.heartbeatTimeoutMs > 0) {
            long hb = o.heartbeatTimeoutMs;
            timers.add(e.at(Engine.nowMs() + hb, new Runnable() {
                @Override
                public void run() {
                    if (gen != g || !running) return;
                    Long beat = x.beats.get(seq);
                    long left = (beat == null ? 0 : beat) + hb - Engine.nowMs();
                    if (left <= 0) {
                        expire(g, "step sent no heartbeat for " + hb + " ms");
                        return;
                    }
                    timers.add(e.at(Engine.nowMs() + left, this));
                }
            }));
        }
        try {
            e.stepExecutor.execute(ft);
        } catch (RejectedExecutionException err) {
            finished(g, null, Errors.failed("the step executor refused the step: " + Errors.message(err)));
        }
    }

    /** The attempt took too long: its thread is interrupted and a late result ignored. */
    private void expire(long g, String msg) {
        if (gen != g || !running) return;
        FutureTask<Void> f = future;
        if (f != null) f.cancel(true);
        finished(g, null, new TimedOutException(msg));
    }

    /** The run ended: stop the attempt's timers and interrupt its thread. */
    void abandon() {
        for (Engine.Timer t : timers) t.cancel();
        timers.clear();
        cancelLater();
        FutureTask<Void> f = future;
        if (f != null && running) f.cancel(true);
        future = null;
        running = false;
    }

    void finished(long g, byte[] data, Throwable err) {
        if (g != gen || !running) return; // a late result of an attempt that timed out
        running = false;
        for (Engine.Timer t : timers) t.cancel();
        timers.clear();
        future = null;
        x.running.remove(this);
        x.beats.remove(seq);
        release();
        if (x.ended || !x.pending.containsKey(seq) || settled) return;
        if (err == null) {
            settled = true;
            try {
                done.done(true, data, "");
            } catch (RuntimeException ex) {
                fail(ex);
                return;
            }
            x.pump();
            return;
        }
        Failure f = Failure.of(err);
        String msg = f.toJson();
        boolean retryable = retry.retries(f, attempt);
        Exhausted then = retry.onExhausted != null ? retry.onExhausted : e.onExhausted;
        if (retryable && (attempt < retry.attempts || (pausable && then == Exhausted.PAUSE))) {
            if (attempt < retry.attempts) {
                notBefore = Engine.nowMs() + retry.delay(attempt);
            } else {
                // Out of retries: the run waits for a resume.
                notBefore = PAUSED;
                e.emitPaused(x.id, "step '" + name + "' ran out of retries: " + f.message());
            }
            try {
                gate = e.stepRetry(x.shard, x.run, seq, notBefore, msg);
            } catch (RuntimeException ex) {
                fail(ex);
                return;
            }
            attempt++;
            e.kicked = true;
            tryRun();
            return;
        }
        settled = true;
        try {
            done.done(false, null, msg);
        } catch (RuntimeException ex) {
            fail(ex);
            return;
        }
        x.pump();
    }
}
