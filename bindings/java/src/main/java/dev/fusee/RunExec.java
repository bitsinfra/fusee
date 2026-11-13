package dev.fusee;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;

/**
 * A run's code: its tasks, pending operations and handlers. Between two
 * deliveries the run's code runs until every task is blocked, as on replay,
 * so the code makes the same journal operations in the same order live and
 * replaying. Every method is called by the thread holding the engine.
 */
final class RunExec {
    interface QueryHandler {
        byte[] query(byte[] arg) throws Exception;
    }

    interface UpdateBody {
        Object run(byte[] arg) throws Exception;
    }

    static final class UpdateHandler {
        final Validator<byte[]> validate;
        final UpdateBody handle;

        UpdateHandler(Validator<byte[]> validate, UpdateBody handle) {
            this.validate = validate;
            this.handle = handle;
        }
    }

    private static final class Gated {
        final int seq;
        Runnable retry;

        Gated(int seq, Runnable retry) {
            this.seq = seq;
            this.retry = retry;
        }
    }

    final Engine e;
    final int shard;
    final long run;
    final String id, wtype;
    final int version;
    final long startedMs;
    final Ctx ctx;
    final HashMap<Integer, OpState> pending = new HashMap<>();
    private final ArrayList<Gated> gated = new ArrayList<>();
    final HashMap<Integer, Long> beats = new HashMap<>();
    final HashMap<String, QueryHandler> queries = new HashMap<>();
    final HashMap<String, UpdateHandler> updates = new HashMap<>();
    /** An object type's handlers (a key's run), looked up when a call is delivered; else null. */
    HashMap<String, UpdateHandler> object;
    /** Encoded step results this code recorded and has not had delivered. */
    final HashMap<Integer, byte[]> held = new HashMap<>();
    /** Tasks to resume (Task) and functions to call (Runnable). */
    final ArrayDeque<Object> ready = new ArrayDeque<>();
    final LinkedHashSet<Task> tasks = new LinkedHashSet<>();
    /** Steps running on threads of their own, cancelled when the run ends. */
    final LinkedHashSet<Attempts> running = new LinkedHashSet<>();
    // A scheduler loop is on the stack (deliveries wait for it), and a
    // delivery was asked for meanwhile.
    boolean scheduling, deliver, ended, reaped;
    /** The engine stopped the run on a determinism error. */
    boolean stuck;
    /** The inline step running now, or -1. */
    long current = -1;

    RunExec(Engine e, Engine.StartEvent ev) {
        this.e = e;
        this.shard = ev.shard();
        this.run = ev.run();
        this.id = ev.id();
        this.wtype = ev.wtype();
        this.version = ev.version();
        this.startedMs = ev.startedMs();
        this.ctx = new Ctx(this);
    }

    Task newTask(Task.Body body, Task.Done onDone) {
        Task t = new Task(this, body, onDone);
        tasks.add(t);
        return t;
    }

    /** Marks a determinism error: the engine stopped the run. */
    RuntimeException note(RuntimeException err) {
        if (err instanceof DeterminismException) stuck = true;
        return err;
    }

    // --------------------------------------------------------- handoffs

    /** Runs t until it blocks or returns. */
    void resume(Task t) {
        Task prev = e.running;
        e.running = t;
        t.back = false;
        t.resumer = Thread.currentThread();
        if (!t.started) {
            t.started = true;
            t.thread = Threads.task(t::main, "fusee " + wtype + " " + id);
            t.thread.start();
        } else {
            t.resumeTask();
        }
        t.awaitBack();
        e.running = prev;
    }

    /** Ends the threads of a run that ended. */
    void reap() {
        if (reaped || scheduling) return;
        reaped = true;
        ready.clear();
        for (Task t : new ArrayList<>(tasks)) {
            if (t.started && !t.exited) {
                t.kill = true;
                resume(t);
            }
        }
        tasks.clear();
        for (OpState o : pending.values()) o.waiters = null;
        for (Attempts a : new ArrayList<>(running)) a.abandon();
        running.clear();
    }

    // ------------------------------------------------------------- ready

    void settle(OpState o, Throwable err) {
        if (o.done) return;
        o.done = true;
        o.err = err;
        if (o.waiters != null) {
            ready.addAll(o.waiters);
            o.waiters = null;
        }
    }

    private void drain() {
        while (!ready.isEmpty() && !ended) {
            Object it = ready.poll();
            if (it instanceof Task t) {
                if (!t.exited) resume(t);
            } else {
                ((Runnable) it).run();
            }
        }
    }

    private void afterSchedule() {
        if (ended) reap();
    }

    /** Runs every runnable task until all are blocked. */
    void runReady() {
        if (scheduling) return; // the loop on the stack runs them
        scheduling = true;
        drain();
        scheduling = false;
        if (deliver) {
            pump();
            return;
        }
        afterSchedule();
    }

    /** Hands the run its deliveries one at a time; between two, its code runs until every task is blocked. */
    void pump() {
        if (scheduling) {
            // Called from running code: the loop on the stack delivers once
            // every task is blocked.
            deliver = true;
            return;
        }
        scheduling = true;
        long[] out = e.dout;
        while (!ended) {
            int kind;
            try {
                kind = e.quiescent(shard, run, out);
            } catch (RuntimeException err) {
                if (failAll(err)) continue;
                break;
            }
            if (kind == Native.D_NONE) break;
            apply(kind, out);
            drain();
        }
        scheduling = false;
        deliver = false;
        e.kicked = true;
        afterSchedule();
    }

    /**
     * Called by task t as it is about to block, with the driver's scheduler
     * loop below it on the stack: if a delivery is due and every other task
     * of the run is blocked, t takes the delivery here (exactly what the
     * driver would do once t blocked) instead of handing the engine back for
     * it. Returns true if the delivery readied t alone, which then goes on.
     * Without force, only when the code asked for a delivery meanwhile.
     */
    boolean deliverHere(Task t, boolean force) {
        if (!scheduling || !(deliver || force) || ended || !ready.isEmpty()) return false;
        long[] out = e.dout;
        int kind;
        try {
            kind = e.quiescent(shard, run, out);
        } catch (RuntimeException err) {
            return false; // the driver asks again and handles it
        }
        if (kind == Native.D_NONE) {
            deliver = false;
            e.kicked = true;
            return false;
        }
        apply(kind, out);
        deliver = true;
        if (ready.size() == 1 && ready.peek() == t) {
            ready.poll();
            return true;
        }
        return false;
    }

    /** The engine refused a delivery, so every pending operation fails. True if the code then asked for another. */
    private boolean failAll(RuntimeException err) {
        Integer[] seqs = pending.keySet().toArray(new Integer[0]);
        Arrays.sort(seqs);
        for (Integer s : seqs) settle(pending.get(s), err);
        pending.clear();
        deliver = false;
        drain();
        return deliver;
    }

    void wake() {
        for (Gated g : new ArrayList<>(gated)) g.retry.run();
        pump();
    }

    void gate(int seq, Runnable retry) {
        for (Gated g : gated) {
            if (g.seq == seq) {
                g.retry = retry;
                return;
            }
        }
        gated.add(new Gated(seq, retry));
    }

    void ungate(int seq) {
        for (int i = 0; i < gated.size(); i++) {
            if (gated.get(i).seq == seq) {
                gated.remove(i);
                return;
            }
        }
    }

    /** Whether a step gated on gate may run now: open by the commit position the last tick reported, without a call. */
    boolean mayRun(long gate, long notBefore) {
        if (ended) throw Errors.ended();
        if (Long.compareUnsigned(gate, e.commit(shard)) <= 0 && (notBefore == 0 || notBefore <= Engine.nowMs())) {
            return true;
        }
        return e.stepGate(shard, run, gate, notBefore, e.inlineOK());
    }

    // -------------------------------------------------------- deliveries

    private void apply(int kind, long[] out) {
        int seq = (int) out[0];
        int status = (int) out[1];
        switch (kind) {
            case Native.D_RESOLVE, Native.D_HELD -> {
                byte[] kept = held.remove(seq);
                OpState o = pending.remove(seq);
                if (o == null) return;
                if (kind == Native.D_HELD) {
                    if (kept == null) {
                        settle(o, Errors.failed("the result of step " + seq + " was not kept"));
                        return;
                    }
                    resolve(o, status, kept);
                    return;
                }
                resolve(o, status, out[5] > 0 ? e.deliveryBytes() : null);
            }
            case Native.D_CANCEL -> {
                String reason = Codec.text(out[5] > 0 ? e.deliveryBytes() : null);
                for (int s : e.deliverySeqs()) {
                    OpState o = pending.remove(s);
                    ungate(s);
                    held.remove(s);
                    if (o != null) settle(o, new CancelledException(reason));
                }
            }
            case Native.D_UPDATE -> {
                int pl = (int) out[5], nl = (int) out[6], il = (int) out[7];
                byte[] all = pl + nl + il > 0 ? e.deliveryBytes() : new byte[0];
                byte[] payload = Arrays.copyOfRange(all, 0, pl);
                String name = new String(all, pl, nl, java.nio.charset.StandardCharsets.UTF_8);
                String uid = new String(all, pl + nl, il, java.nio.charset.StandardCharsets.UTF_8);
                startUpdate(out[3], out[4], name, uid, payload);
            }
            default -> {}
        }
    }

    private void resolve(OpState o, int status, byte[] payload) {
        switch (status) {
            case Native.S_OK -> {
                o.payload = payload;
                settle(o, null);
            }
            case Native.S_FAILED -> {
                Failure f = Failure.parse(Codec.text(payload));
                if (o.kind == OpState.STEP || o.kind == OpState.AWAIT) settle(o, new StepFailedException(o.name, f));
                else if (o.kind == OpState.CHILD) settle(o, new WorkflowFailedException("ChildFailed", o.name, f));
                else settle(o, new WorkflowFailedException("WorkflowFailed", null, f));
            }
            case Native.S_CANCELLED -> settle(o, new CancelledException(Codec.text(payload)));
            case Native.S_TIMED_OUT -> {
                if (o.kind == OpState.WAIT) {
                    o.timedOut = true;
                    settle(o, null);
                    return;
                }
                String msg = Codec.text(payload);
                settle(o, new TimedOutException(msg.isEmpty() ? "timed out" : msg));
            }
            default -> settle(o, new TerminatedException(Codec.text(payload)));
        }
    }

    private void startUpdate(long ordinal, long token, String name, String uid, byte[] arg) {
        UpdateHandler found = updates.get(name);
        if (found == null && object != null) found = object.get(name);
        final UpdateHandler h = found;
        if (token >= 0) {
            String refusal = null;
            if (h == null) {
                refusal = "no handler for update '" + name + "'";
            } else if (h.validate != null) {
                try {
                    h.validate.validate(arg);
                } catch (Task.Killed k) {
                    throw k;
                } catch (Throwable t) {
                    refusal = Errors.message(t);
                }
            }
            long o;
            try {
                o = e.decideUpdate(shard, token, refusal);
            } catch (RuntimeException err) {
                return;
            }
            if (refusal != null || o < 0) return;
            ordinal = o;
        } else if (h == null) {
            String msg = "determinism error: the journal delivers update '" + name + "' (id '" + uid
                    + "') but the code registered no handler for it";
            ended = true;
            stuck = true;
            try {
                e.stuck(shard, run, msg);
            } catch (RuntimeException err) {
                // The run stops either way.
            }
            return;
        }
        final long ord = ordinal;
        Task t = newTask(() -> h.handle.run(arg), (v, err) -> {
            if (ended) return;
            int status;
            byte[] data;
            if (err == null) {
                try {
                    data = e.codec.encode(v);
                    status = Native.S_OK;
                } catch (RuntimeException enc) {
                    status = Native.S_FAILED;
                    data = Codec.utf8("update result is not JSON: " + Errors.message(enc));
                }
            } else {
                status = Errors.status(err);
                data = Codec.utf8(Errors.text(err));
            }
            try {
                e.host(shard, run, Native.OP_UPDATE_DONE, ord, status, 0, data, null, null, null);
            } catch (RuntimeException ignored) {
                // The run ended meanwhile.
            }
            e.kicked = true;
        });
        ready.add(t);
    }

    /** Drops an operation the code no longer waits for (a race's loser). */
    void forget(int seq) {
        if (pending.remove(seq) == null) return;
        ungate(seq);
        held.remove(seq);
        try {
            e.host(shard, run, Native.OP_FORGET, seq & 0xffffffffL, 0, 0, null, null, null, null);
        } catch (RuntimeException ignored) {
            // Nothing waits for it either way.
        }
    }

    /** Records a step's final result (kept here, so its delivery does not carry it back) or failure. */
    void stepDone(int seq, boolean ok, byte[] data, String msg) {
        if (ended) throw Errors.ended();
        if (ok) {
            held.put(seq, data);
            e.stepDone(shard, run, seq, Native.DONE_OK | Native.DONE_KEEP, data);
        } else {
            e.stepDone(shard, run, seq, 0, Codec.utf8(msg));
        }
    }
}
