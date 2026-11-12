package dev.fusee;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * The workflow context, one per run. Its operations are journal operations:
 * recorded in the engine's log and replayed after a crash. Workflow code
 * (the function given to {@code Engine.register}, functions started with
 * {@link #async}, update handlers) must be deterministic:
 *
 * <ul>
 *   <li>wait, branch on time or randomness, and run code concurrently only
 *       through the Ctx: {@code step}, {@code sleep}, {@code waitFor},
 *       {@code child}, {@code all}, {@code race}, {@code async},
 *       {@code now}, {@code random}, {@code uuid};
 *   <li>start no threads or executors, and use no {@code CompletableFuture},
 *       {@code synchronized} waits, locks, {@code Thread.sleep}, timers,
 *       {@code System.currentTimeMillis}, {@code Instant.now},
 *       {@code Math.random}, {@code Random} or {@code UUID.randomUUID} in
 *       workflow code (step functions may do anything);
 *   <li>do not let {@code HashMap} or {@code HashSet} iteration order decide
 *       which journal operations run (use {@code LinkedHashMap} or sort);
 *   <li>do not call {@code Engine} or {@code Run} methods from workflow code
 *       (they wait for the engine, which the caller holds): use
 *       {@code ctx.signal}, {@code ctx.child}, {@code ctx.cancelWorkflow};
 *   <li>do not catch {@code Throwable} or {@code Error} around Ctx calls
 *       (a run that ends unwinds its threads with an Error), and do not
 *       swallow a {@link DeterminismException}.
 * </ul>
 *
 * The Ctx's methods may be called only from the run's own workflow code, on
 * the thread the engine runs it on; {@link #key}, {@link #heartbeat} and
 * {@link #progress} are for step functions, on the step's thread.
 */
public final class Ctx {
    private static final long DETACHED = 1L << 6;
    private static final long NO_SEQ = 0xFFFFFFFFL;

    private final RunExec x;
    private final Engine e;

    Ctx(RunExec x) {
        this.x = x;
        this.e = x.e;
    }

    /** The run's workflow ID. */
    public String workflowId() {
        return x.id;
    }

    /** The run's workflow type. */
    public String workflowType() {
        return x.wtype;
    }

    /** The code version the run started under. */
    public int version() {
        return x.version;
    }

    /** The run's number (a reset or continue-as-new makes a new one). */
    public long runId() {
        return x.run;
    }

    // ------------------------------------------------------------ plumbing

    /** Checks the caller holds the engine as this run's code; returns the task. */
    Task enter() {
        Task t = e.running;
        if (t == null || t.x != x || t.thread != Thread.currentThread()) {
            throw new IllegalStateException("fusee: a Ctx was used outside its workflow's code (start concurrent "
                    + "workflow code with ctx.async, not a thread or executor)");
        }
        return t;
    }

    /** Blocks the task until o settles (or the run ends). */
    void await(OpState o) {
        Task t = enter();
        while (!o.done) {
            if (x.ended) return;
            o.addWaiter(t);
            if (x.deliverHere(t, false) || e.drive(t, x)) continue;
            t.park();
        }
    }

    static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException r) return r;
        if (t instanceof Error err) throw err;
        return new FuseeException("Failed", Errors.message(t), t);
    }

    private OpState newOp(int kind, String name, long seq) {
        OpState o = new OpState(kind, name, seq);
        x.pending.put((int) seq, o);
        e.kicked = true;
        return o;
    }

    private byte[] host(long[] r, int op, long n0, long n1, long n2, byte[] s0, byte[] s1, byte[] s2) {
        enter();
        if (x.ended) throw Errors.ended();
        try {
            return e.host(x.shard, x.run, op, n0, n1, n2, s0, s1, s2, r);
        } catch (RuntimeException err) {
            throw x.note(err);
        }
    }

    /** The run's code (package use: objects). */
    RunExec exec() {
        return x;
    }

    /** A journal operation from this run's code: its numbers in r, its data returned. */
    byte[] op(long[] r, int op, long n0, byte[] s0, byte[] s1, byte[] s2) {
        return host(r, op, n0, 0, 0, s0, s1, s2);
    }

    /** An operation the code waits for as for a child's result. */
    <T> Op<T> childOp(String name, long seq, Type output) {
        return new Op<>(x, newOp(OpState.CHILD, name, seq), output);
    }

    static <T> Op<T> failedOp(RunExec x, RuntimeException err) {
        return new Op<>(x, OpState.failed(err), null);
    }

    /** Blocks this code until the run ends (an object's main code). */
    void untilEnded() {
        OpState never = new OpState(OpState.TASK, "", -1);
        while (!x.ended) await(never);
    }

    /**
     * Key key of object type type, called from this workflow code: each call
     * is a journal operation, sent through the durable outbox and run once,
     * and its result is recorded.
     */
    public ObjectCall object(String type, String key) {
        return new ObjectCall(this, type, key);
    }

    private <T> Op<T> failed(RuntimeException err) {
        return new Op<>(x, OpState.failed(err), null);
    }

    private static Type type(TypeRef<?> t) {
        return t == null ? Object.class : t.type();
    }

    // --------------------------------------------------------------- steps

    /**
     * Runs fn as a durable step and returns its result: recorded once,
     * replayed after a crash. fn runs once everything the run recorded before
     * is durable (at once with readOnly), on a thread of its own (or inline);
     * it never runs again once its result is recorded. It must not read or
     * write workflow state that workflow code changes meanwhile.
     */
    public <T> T step(String name, Class<T> type, Callable<T> fn) {
        return startStep(name, type, fn, null).get();
    }

    /** A step whose function takes its {@link StepCtx} (key, attempt, heartbeat). */
    public <T> T step(String name, Class<T> type, StepFn<T> fn) {
        return step(name, type, fn, null);
    }

    public <T> T step(String name, Class<T> type, StepFn<T> fn, StepOptions opts) {
        StepCtx s = new StepCtx(this, name);
        return startStep(name, type, () -> fn.run(s), opts).get();
    }

    public <T> T step(String name, TypeRef<T> type, StepFn<T> fn) {
        return step(name, type, fn, null);
    }

    public <T> T step(String name, TypeRef<T> type, StepFn<T> fn, StepOptions opts) {
        StepCtx s = new StepCtx(this, name);
        return this.<T>startStep(name, type(type), () -> fn.run(s), opts).get();
    }

    public <T> T step(String name, Class<T> type, Callable<T> fn, StepOptions opts) {
        return startStep(name, type, fn, opts).get();
    }

    public <T> T step(String name, TypeRef<T> type, Callable<T> fn) {
        return this.<T>startStep(name, type(type), fn, null).get();
    }

    public <T> T step(String name, TypeRef<T> type, Callable<T> fn, StepOptions opts) {
        return this.<T>startStep(name, type(type), fn, opts).get();
    }

    /** Begins a step without waiting for it. */
    public <T> Op<T> startStep(String name, Class<T> type, Callable<T> fn) {
        return startStep(name, (Type) type, fn, null);
    }

    public <T> Op<T> startStep(String name, Class<T> type, Callable<T> fn, StepOptions opts) {
        return startStep(name, (Type) type, fn, opts);
    }

    public <T> Op<T> startStep(String name, TypeRef<T> type, Callable<T> fn) {
        return startStep(name, type(type), fn, null);
    }

    public <T> Op<T> startStep(String name, TypeRef<T> type, Callable<T> fn, StepOptions opts) {
        return startStep(name, type(type), fn, opts);
    }

    private <T> Op<T> startStep(String name, Type type, Callable<T> fn, StepOptions opts) {
        enter();
        StepOptions o = opts == null ? new StepOptions() : opts;
        if (x.ended) return failed(Errors.ended());
        long[] s = new long[6];
        int flags = (o.readOnly ? Native.STEP_READ_ONLY : 0) | (e.inlineOK() ? Native.STEP_INLINE : 0);
        try {
            e.beginStep(x.shard, x.run, name, flags, s);
        } catch (RuntimeException err) {
            return failed(x.note(err));
        }
        int seq = (int) s[0];
        OpState op = newOp(OpState.STEP, name, seq);
        if (s[1] == 0) {
            Codec codec = e.codec;
            Attempts a = new Attempts(x, seq, s[2], s[3], s[5], () -> () -> codec.encode(fn.call()), o,
                    o.retry != null ? o.retry : RetryPolicy.DEFAULT, s[4] != 0 ? 1 : 0,
                    (ok, data, msg) -> x.stepDone(seq, ok, data, msg));
            a.pausable = true;
            a.name = name;
            a.tryRun();
        }
        return new Op<>(x, op, type);
    }

    /**
     * A step completed from outside: send(token) starts work elsewhere, and
     * the step waits until something holding the token calls
     * {@code engine.complete(token, value)} (or {@code fail}). send is retried
     * under the retry policy, each attempt with a new token.
     */
    public <T> T awaitCompletion(String name, Class<T> type, CompletionSender send) {
        return this.<T>startAwaitCompletion(name, (Type) type, send, null).get();
    }

    public <T> T awaitCompletion(String name, Class<T> type, CompletionSender send, StepOptions opts) {
        return this.<T>startAwaitCompletion(name, (Type) type, send, opts).get();
    }

    public <T> Op<T> startAwaitCompletion(String name, Class<T> type, CompletionSender send, StepOptions opts) {
        return startAwaitCompletion(name, (Type) type, send, opts);
    }

    private <T> Op<T> startAwaitCompletion(String name, Type type, CompletionSender send, StepOptions opts) {
        long[] r = new long[5];
        try {
            host(r, Native.OP_BEGIN_AWAIT, 0, 0, 0, Codec.utf8(name), null, null);
        } catch (RuntimeException err) {
            return failed(err);
        }
        int seq = (int) r[0];
        OpState op = newOp(OpState.AWAIT, name, seq);
        if (r[1] == 0) {
            StepOptions o = opts == null ? new StepOptions() : opts;
            StepOptions ao = new StepOptions();
            ao.timeoutMs = o.timeoutMs;
            ao.inline = o.inline;
            long useq = seq & 0xffffffffL;
            Attempts a = new Attempts(x, seq, r[2], r[3], r[4], () -> {
                String token = Codec.utf8(e.host(x.shard, x.run, Native.OP_TOKEN, useq, 0, 0, null, null, null, null));
                return () -> {
                    send.send(token);
                    return null;
                };
            }, ao, o.retry != null ? o.retry : RetryPolicy.DEFAULT, -1, (ok, data, msg) -> {
                if (ok) e.host(x.shard, x.run, Native.OP_AWAITING, useq, 0, 0, null, null, null, null);
                else x.stepDone(seq, false, null, msg);
            });
            a.tryRun();
        }
        return new Op<>(x, op, type);
    }

    /**
     * A key stable across retries and recovery for the step running on this
     * thread (distinct for each run of an ID), for idempotent calls to other
     * services. Outside a step, in workflow code: a key for the run.
     */
    public String key() {
        StepInfo s = StepInfo.CURRENT.get();
        if (s != null && s.x == x) return s.key();
        return Codec.utf8(host(null, Native.OP_KEY, NO_SEQ, 0, 0, null, null, null));
    }

    /**
     * Reports the running step's progress (from its function's thread):
     * coalesced, never waits for the disk, and resets its heartbeat timeout.
     */
    public void heartbeat(Object progress) {
        StepInfo s = StepInfo.CURRENT.get();
        if (s == null || s.x != x) throw new IllegalStateException("fusee: heartbeat called outside a step");
        byte[] data = e.codec.encode(progress);
        Runnable beat = () -> {
            if (x.ended) return;
            x.beats.put(s.seq, Engine.nowMs());
            try {
                e.host(x.shard, x.run, Native.OP_HEARTBEAT, s.seq & 0xffffffffL, 0, 0, data, null, null, null);
            } catch (RuntimeException ignored) {
                // The run ended meanwhile.
            }
            e.kicked = true;
        };
        if (s.inline) beat.run();
        else e.post(beat);
    }

    /**
     * Opens the run's stream name for appending (readers follow it with
     * {@code run.stream(name, from)}). In workflow code it opens a stream of
     * the run: its appends are recorded once (a replay appends nothing), and
     * {@link StreamWriter#close} or the run's end completes it; the run's
     * step functions may append to it too. Inside a step's function (on its
     * thread) it opens a stream of the step, which the step's final result
     * completes; an attempt that runs again finds what earlier attempts
     * recorded in {@link StreamWriter#recorded} and continues after it.
     */
    public StreamWriter stream(String name) {
        return StreamWriter.open(x, name);
    }

    /** The latest progress the running step reported, also from before a crash. */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> progress(Class<T> type) {
        StepInfo s = StepInfo.CURRENT.get();
        if (s == null || s.x != x) return Optional.empty();
        byte[] data = s.within(() -> {
            if (x.ended) throw Errors.ended();
            long[] r = new long[5];
            byte[] b = e.host(x.shard, x.run, Native.OP_PROGRESS, s.seq & 0xffffffffL, 0, 0, null, null, null, r);
            return r[0] == 1 ? (b == null ? new byte[0] : b) : null;
        });
        if (data == null) return Optional.empty();
        return Optional.ofNullable((T) e.codec.decode(data, type));
    }

    // ------------------------------------------------------------- waiting

    /** Waits for a durable timer; it survives restarts. */
    public void sleep(String name, Duration d) {
        startSleep(name, d).get();
    }

    /** Starts a durable timer. */
    public Op<Void> startSleep(String name, Duration d) {
        long[] r = new long[5];
        try {
            host(r, Native.OP_BEGIN_SLEEP, Engine.nowMs() + Math.max(0, d.toMillis()), 0, 0, Codec.utf8(name), null,
                    null);
        } catch (RuntimeException err) {
            return failed(err);
        }
        return new Op<>(x, newOp(OpState.SLEEP, name, (int) r[0]), null);
    }

    /** Waits for the next signal name. */
    public <T> T waitFor(String signal, Class<T> type) {
        return this.<T>startWait(signal, (Type) type, null).get();
    }

    public <T> T waitFor(String signal, TypeRef<T> type) {
        return this.<T>startWait(signal, type(type), null).get();
    }

    /** Waits for the next signal name; empty on timeout (or a null signal: see startWait). */
    public <T> Optional<T> waitFor(String signal, Class<T> type, Duration timeout) {
        Op<T> op = this.<T>startWait(signal, (Type) type, timeout);
        T v = op.get();
        return op.timedOut() ? Optional.empty() : Optional.ofNullable(v);
    }

    public <T> Optional<T> waitFor(String signal, TypeRef<T> type, Duration timeout) {
        Op<T> op = this.<T>startWait(signal, type(type), timeout);
        T v = op.get();
        return op.timedOut() ? Optional.empty() : Optional.ofNullable(v);
    }

    /** Waits for the next signal without blocking; on timeout the value is null and timedOut() is true. */
    public <T> Op<T> startWait(String signal, Class<T> type) {
        return startWait(signal, (Type) type, null);
    }

    public <T> Op<T> startWait(String signal, Class<T> type, Duration timeout) {
        return startWait(signal, (Type) type, timeout);
    }

    public <T> Op<T> startWait(String signal, TypeRef<T> type, Duration timeout) {
        return startWait(signal, type(type), timeout);
    }

    private <T> Op<T> startWait(String signal, Type type, Duration timeout) {
        long deadline = timeout != null && timeout.toMillis() > 0 ? Engine.nowMs() + timeout.toMillis() : 0;
        long[] r = new long[5];
        try {
            host(r, Native.OP_BEGIN_WAIT, deadline, 0, 0, Codec.utf8(signal), null, null);
        } catch (RuntimeException err) {
            return failed(err);
        }
        return new Op<>(x, newOp(OpState.WAIT, signal, (int) r[0]), type);
    }

    // --------------------------------------------------- children, signals

    /** Runs a child workflow and returns its result. */
    public <O> O child(String type, String id, Object input, Class<O> output) {
        return this.<O>startChild(type, id, input, (Type) output, null).get();
    }

    public <O> O child(String type, String id, Object input, Class<O> output, ChildOptions opts) {
        return this.<O>startChild(type, id, input, (Type) output, opts).get();
    }

    public <O> O child(String type, String id, Object input, TypeRef<O> output, ChildOptions opts) {
        return this.<O>startChild(type, id, input, type(output), opts).get();
    }

    /** Starts a child workflow without waiting for its result. */
    public <O> Op<O> startChild(String type, String id, Object input, Class<O> output) {
        return startChild(type, id, input, (Type) output, null);
    }

    public <O> Op<O> startChild(String type, String id, Object input, Class<O> output, ChildOptions opts) {
        return startChild(type, id, input, (Type) output, opts);
    }

    private <O> Op<O> startChild(String type, String id, Object input, Type output, ChildOptions opts) {
        ChildOptions o = opts == null ? new ChildOptions() : opts;
        long[] r = new long[5];
        try {
            byte[] data = e.codec.encode(input);
            long deadline = o.timeoutMs > 0 ? Engine.nowMs() + o.timeoutMs : 0;
            host(r, Native.OP_BEGIN_CHILD, o.closeBits(), deadline, 0, Codec.utf8(type), Codec.utf8(id), data);
        } catch (RuntimeException err) {
            return failed(err);
        }
        return new Op<>(x, newOp(OpState.CHILD, type, (int) r[0]), output);
    }

    /**
     * Starts a child without waiting for it. By default it is terminated when
     * this workflow closes; parentClose("abandon") lets it outlive it.
     */
    public void spawnChild(String type, String id, Object input) {
        spawnChild(type, id, input, null);
    }

    public void spawnChild(String type, String id, Object input, ChildOptions opts) {
        ChildOptions o = opts == null ? new ChildOptions() : opts;
        byte[] data = e.codec.encode(input);
        long deadline = o.timeoutMs > 0 ? Engine.nowMs() + o.timeoutMs : 0;
        host(null, Native.OP_BEGIN_CHILD, o.closeBits() | DETACHED, deadline, 0, Codec.utf8(type), Codec.utf8(id),
                data);
        e.kicked = true;
    }

    /** {@link #signal} under the name every SDK uses. */
    public void signalWorkflow(String target, String name, Object value) {
        signal(target, name, value);
    }

    /** Compensations, newest last; null until one is registered. */
    private ArrayList<Object[]> undo;

    /**
     * Registers a step that undoes what the workflow did so far: if the
     * workflow fails or is cancelled, every registered compensation runs as a
     * step named {@code name}, newest first, then the failure is thrown.
     * Register it after the step it undoes succeeded.
     */
    public void compensate(String name, Callable<?> fn) {
        if (undo == null) undo = new ArrayList<>();
        undo.add(new Object[] {name, fn});
    }

    boolean hasCompensations() {
        return undo != null && !undo.isEmpty();
    }

    /** Runs the compensations, newest first; one that fails is reported and the others still run. */
    void runCompensations() {
        while (undo != null && !undo.isEmpty()) {
            Object[] k = undo.remove(undo.size() - 1);
            String name = (String) k[0];
            Callable<?> fn = (Callable<?>) k[1];
            try {
                step(name, Object.class, () -> {
                    fn.call();
                    return null;
                });
            } catch (RuntimeException ex) {
                System.err.println("fusee: workflow '" + x.id + "': compensation '" + name + "' failed: " + ex.getMessage());
            }
        }
    }

    /** Waits for a durable timer that fires at {@code at}; it survives restarts. */
    public void sleepUntil(String name, Instant at) {
        long[] r = new long[5];
        host(r, Native.OP_BEGIN_SLEEP, at.toEpochMilli(), 0, 0, Codec.utf8(name), null, null);
        new Op<>(x, newOp(OpState.SLEEP, name, (int) r[0]), null).get();
    }

    /** Sends a signal to another workflow once this point is durable. */
    public void signal(String target, String name, Object value) {
        byte[] data = e.codec.encode(value);
        host(null, Native.OP_SEND, 0, 0, 0, Codec.utf8(target), Codec.utf8(name), data);
        e.kicked = true;
    }

    /** Asks another workflow to cancel, once this point is durable. */
    public void cancelWorkflow(String target, String reason) {
        host(null, Native.OP_CANCEL_WORKFLOW, 0, 0, 0, Codec.utf8(target), Codec.utf8(reason), null);
        e.kicked = true;
    }

    // --------------------------------------------------------- concurrency

    /**
     * Runs fn as more workflow code of this run, concurrently with the caller
     * (it starts when the caller next waits). Its result is the Op's. It may
     * use this Ctx.
     */
    public <T> Op<T> async(Callable<T> fn) {
        enter();
        OpState o = new OpState(OpState.TASK, "", -1);
        Task t = x.newTask(fn::call, (v, err) -> {
            o.value = v;
            x.settle(o, err);
        });
        x.ready.add(t);
        return new Op<>(x, o, null);
    }

    /** Waits for every operation (results in their Ops); throws the first failure as soon as one fails. */
    public void all(Op<?>... ops) {
        all(List.of(ops));
    }

    public void all(Collection<? extends Op<?>> ops) {
        enter();
        if (ops.isEmpty()) return;
        OpState g = new OpState(OpState.GROUP, "", -1);
        int[] left = {ops.size()};
        for (Op<?> f : ops) {
            OpState o = f.o;
            Runnable member = () -> {
                if (g.done) return;
                if (o.err != null) {
                    x.settle(g, o.err);
                    return;
                }
                if (--left[0] == 0) x.settle(g, null);
            };
            x.ready.add((Runnable) () -> o.whenDone(member));
        }
        await(g);
        if (!g.done) throw Errors.ended();
        if (g.err != null) throw rethrow(g.err);
    }

    /**
     * Waits for the first operation to finish and returns its index (its
     * value is in its Op; a failure is thrown); replay picks the same one.
     * The losing journal operations are dropped (a dropped wait takes no
     * signal).
     */
    public int race(Op<?>... ops) {
        return race(List.of(ops));
    }

    public int race(List<? extends Op<?>> ops) {
        enter();
        OpState g = new OpState(OpState.GROUP, "", -1);
        List<OpState> list = new ArrayList<>(ops.size());
        for (Op<?> f : ops) list.add(f.o);
        for (int i = 0; i < list.size(); i++) {
            OpState o = list.get(i);
            int index = i;
            Runnable member = () -> {
                if (g.done) return;
                g.value = index;
                x.settle(g, o.err);
                for (OpState l : list) {
                    if (!l.done && l.seq >= 0 && l.kind != OpState.TASK && l.kind != OpState.GROUP) {
                        x.forget((int) l.seq);
                    }
                }
            };
            x.ready.add((Runnable) () -> o.whenDone(member));
        }
        await(g);
        if (!g.done) throw Errors.ended();
        if (g.err != null) throw rethrow(g.err);
        return (Integer) g.value;
    }

    // -------------------------------------------------- queries and updates

    /**
     * Registers a read-only query handler (Run.query). It runs while the
     * workflow's code is blocked; the answer is released once the state it
     * read is durable.
     */
    @SuppressWarnings("unchecked")
    public <A, R> void onQuery(String name, Class<A> arg, QueryFn<A, R> fn) {
        enter();
        Codec codec = e.codec;
        x.queries.put(name, p -> codec.encode(fn.query((A) codec.decode(p, arg))));
    }

    /** Registers a query handler that takes no argument. */
    public <R> void onQuery(String name, Supplier<R> fn) {
        enter();
        Codec codec = e.codec;
        x.queries.put(name, p -> codec.encode(fn.get()));
    }

    /**
     * Registers an update (Run.update): validate (may be null) runs first, and
     * throwing refuses the update and records nothing; handle runs once per
     * accepted update, as workflow code (it may run steps).
     */
    @SuppressWarnings("unchecked")
    public <A, R> void onUpdate(String name, Class<A> arg, UpdateFn<A, R> handle, Validator<A> validate) {
        enter();
        Codec codec = e.codec;
        x.updates.put(name, new RunExec.UpdateHandler(
                validate == null ? null : p -> validate.validate((A) codec.decode(p, arg)),
                p -> handle.update((A) codec.decode(p, arg))));
    }

    public <A, R> void onUpdate(String name, Class<A> arg, UpdateFn<A, R> handle) {
        onUpdate(name, arg, handle, null);
    }

    // ----------------------------------------------------- durable values

    /** The current time, recorded (the same on replay). */
    public Instant now() {
        byte[] v = host(null, Native.OP_VALUE, 0, 0, 0, Codec.le64(Engine.nowMs()), null, null);
        if (v == null || v.length < 8) throw Errors.failed("no recorded time");
        return Instant.ofEpochMilli(Codec.le64(v));
    }

    /** A number in [0, 1), recorded. */
    public double random() {
        byte[] v = host(null, Native.OP_VALUE, 1, 0, 0, Codec.randomBytes(8), null, null);
        if (v == null || v.length < 8) throw Errors.failed("no recorded number");
        return (Codec.le64(v) >>> 11) / (double) (1L << 53);
    }

    /** A random UUID, recorded. */
    public UUID uuid() {
        byte[] b = Codec.randomBytes(16);
        b[6] = (byte) ((b[6] & 0x0f) | 0x40);
        b[8] = (byte) ((b[8] & 0x3f) | 0x80);
        byte[] v = host(null, Native.OP_VALUE, 2, 0, 0, b, null, null);
        if (v == null || v.length < 16) throw Errors.failed("no recorded UUID");
        long msb = 0, lsb = 0;
        for (int i = 0; i < 8; i++) msb = (msb << 8) | (v[i] & 0xff);
        for (int i = 8; i < 16; i++) lsb = (lsb << 8) | (v[i] & 0xff);
        return new UUID(msb, lsb);
    }

    /** How many journal operations the run made so far (deterministic). */
    public int journalSize() {
        long[] r = new long[5];
        host(r, Native.OP_JOURNAL_SIZE, 0, 0, 0, null, null, null);
        return (int) r[0];
    }

    // -------------------------------------------- versions and attributes

    /**
     * True for new runs, and for running ones the first time they get here
     * after the deploy; false while replaying an older journal.
     */
    public boolean patched(String id) {
        long[] r = new long[5];
        host(r, Native.OP_PATCHED, 0, 0, 0, Codec.utf8(id), null, null);
        return r[0] == 1;
    }

    /** Marks a patch whose old branch no running journal needs. */
    public void deprecatePatch(String id) {
        host(null, Native.OP_PATCHED, 1, 0, 0, Codec.utf8(id), null, null);
    }

    /** Records search attributes (a null value removes a key). They are never encrypted. */
    public void setAttributes(Map<String, ?> attributes) {
        byte[] data = e.codec.encode(attributes);
        host(null, Native.OP_SET_ATTRIBUTES, 0, 0, 0, data, null, null);
    }

    /**
     * Ends this run and starts a new run of the same ID with input, in one
     * record. It does not return: write {@code return ctx.continueAsNew(next);}.
     */
    public <T> T continueAsNew(Object input) {
        return continueAsNew(input, 0);
    }

    /** Continue-as-new under another code version (0: this run's). */
    public <T> T continueAsNew(Object input, int version) {
        byte[] data = e.codec.encode(input);
        host(null, Native.OP_CONTINUE_AS_NEW, version, 0, 0, data, null, null);
        e.kicked = true;
        // The run ended: wait until its code is dropped.
        OpState never = new OpState(OpState.TASK, "", -1);
        while (!x.ended) await(never);
        throw Errors.ended();
    }
}
