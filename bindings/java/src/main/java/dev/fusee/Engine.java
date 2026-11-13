package dev.fusee;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * An engine: it runs workflows and records their journals. Open one with
 * {@link #open(Options)}; close it with {@link #close()} (runs left
 * unfinished resume when an engine opens the same directory again).
 *
 * <p>An engine has one loop thread that holds the native engine. Workflow
 * code runs on threads the engine schedules one at a time (see {@link Ctx});
 * step functions run on threads of their own. The engine's methods may be
 * called from any thread except workflow code and inline steps (which hold
 * the engine already: use the Ctx there).
 */
public final class Engine implements AutoCloseable {
    /** Gates a lone workflow may flush inline per tick before the loop looks at other work. */
    private static final int INLINE_PER_TICK = 32;
    private static final ConcurrentHashMap<Long, Engine> ENGINES = new ConcurrentHashMap<>();
    private static final AtomicLong KEYS = new AtomicLong();

    record RunKey(int shard, long run) {}

    record StartEvent(int shard, long run, int version, long startedMs, String id, String wtype, byte[] input) {}

    record ResultMsg(int status, byte[] payload) {}

    interface Code {
        Object run(Ctx ctx, byte[] input) throws Exception;
    }

    interface Issue {
        void issue(long token);
    }

    private record Pending(CompletableFuture<byte[]> reply, boolean value) {}

    /** A request that must hear about a close that drops it. */
    private abstract static class Req implements Runnable {
        abstract void cancel(RuntimeException why);
    }

    /** A timer the loop runs (holder only). */
    static final class Timer implements Comparable<Timer> {
        final Engine e;
        final long at, order;
        final Runnable r;
        boolean cancelled;

        Timer(Engine e, long at, long order, Runnable r) {
            this.e = e;
            this.at = at;
            this.order = order;
            this.r = r;
        }

        void cancel() {
            if (!cancelled) e.cancelledTimers++;
            cancelled = true;
        }

        @Override
        public int compareTo(Timer o) {
            int c = Long.compare(at, o.at);
            return c != 0 ? c : Long.compare(order, o.order);
        }
    }

    final long h;
    private final long key;
    final Codec codec;
    final Executor stepExecutor;
    private final ByteBuffer io, commits;
    private final int shardCount;
    final Thread loopThread;
    private final ConcurrentLinkedQueue<Runnable> reqs = new ConcurrentLinkedQueue<>();
    private volatile boolean loopParked, notified, done;
    /** The thread waiting for work (the loop, or a task driving it): producers unpark it. */
    private volatile Thread waiter;
    /** The task that may spin while it waits to be resumed (the most recent to block). */
    volatile Task spinner;
    private final CountDownLatch doneLatch = new CountDownLatch(1);
    private volatile RuntimeException closeErr;
    private final boolean serveRemote;
    /** Stream appends step threads queued for the loop. */
    final StreamWriter.Queue streams = new StreamWriter.Queue(this);

    // Owned by the thread holding the engine: the loop, or the task it
    // handed the engine to.
    private boolean closing;
    private RuntimeException failure;
    private final HashMap<String, HashMap<Integer, Code>> types = new HashMap<>();
    private final HashMap<RunKey, RunExec> runs = new HashMap<>();
    private final HashMap<String, ArrayList<StartEvent>> awaiting = new HashMap<>();
    private final HashMap<Long, Pending> tokens = new HashMap<>();
    private final HashMap<RunKey, ArrayList<CompletableFuture<ResultMsg>>> waiters = new HashMap<>();
    private long nextToken = 1;
    boolean kicked;
    private final PriorityQueue<Timer> timers = new PriorityQueue<>();
    private long timerOrder, cancelledTimers;
    private long engineWakeAt = -1;
    private int inlineLeft = INLINE_PER_TICK;
    Task running;
    // A task drives the loop (see drive); a tick's events are being handled.
    private boolean driving, ticking;
    private long ticks, calls;
    private long[] events = new long[Native.STRIDE * 64];
    final long[] dout = new long[8];
    // The native engine's input and output buffers (see Native).
    private ByteBuffer inBuf = ByteBuffer.allocateDirect(64 << 10).order(ByteOrder.nativeOrder());
    private final ByteBuffer outBuf = ByteBuffer.allocateDirect(64 << 10).order(ByteOrder.nativeOrder());
    private final java.nio.LongBuffer outLongs = outBuf.asLongBuffer();
    private int inPos;
    private byte[] lastDelivery;

    private Engine(Options o) {
        Native.init();
        this.codec = new Codec(o.json);
        this.onExhausted = o.onExhausted;
        this.stepExecutor = o.stepExecutor != null ? o.stepExecutor : Threads.steps();
        this.serveRemote = "server".equals(o.durability);
        this.key = KEYS.incrementAndGet();
        this.loopThread = Threads.task(this::loop, "fusee-engine-" + key);
        ENGINES.put(key, this);
        long handle;
        try {
            int n = o.codec.size();
            byte[][] ids = new byte[n][], keys = new byte[n][];
            int i = 0;
            for (Map.Entry<String, byte[]> k : o.codec.entrySet()) {
                ids[i] = Codec.utf8(k.getKey());
                keys[i++] = k.getValue();
            }
            byte[][] peers = new byte[o.peers.size()][];
            for (int p = 0; p < peers.length; p++) peers[p] = Codec.utf8(o.peers.get(p));
            handle = Native.open(Codec.utf8(o.durability), Codec.utf8(o.dir), Codec.utf8(o.address),
                    "replicated".equals(o.durability) ? o.replicaId : -1, peers, o.shards, o.keepHistory,
                    o.archiveHistory, o.inlineFlush == null ? -1 : o.inlineFlush ? 1 : 0, ms(o.parkAfter),
                    ms(o.retention), ms(o.checkpointInterval), o.segmentBytes > 0 ? o.segmentBytes : -1,
                    Codec.utf8(o.flush), ids, keys, key);
        } catch (RuntimeException e) {
            ENGINES.remove(key);
            throw e;
        }
        this.h = handle;
        Native.buffers(handle, inBuf, outBuf);
        this.io = Native.io(handle).order(ByteOrder.nativeOrder());
        this.commits = Native.commits(handle).order(ByteOrder.nativeOrder());
        this.shardCount = io.getInt(4);
        this.kicked = true;
    }

    private static long ms(Duration d) {
        return d == null || d.isNegative() || d.isZero() ? -1 : d.toMillis();
    }

    /**
     * Opens an engine. In server mode it connects to the server (and may wait
     * for the previous worker's lease); in replicated mode it waits until this
     * member leads.
     */
    public static Engine open(Options options) {
        Options o = options == null ? new Options() : options;
        Engine e = new Engine(o);
        e.loopThread.start();
        // Runs recovered before their code is registered wait for it.
        for (WorkflowDef<?, ?> d : o.workflows) d.register(e);
        return e;
    }

    static Engine byKey(long key) {
        return ENGINES.get(key);
    }

    static long nowMs() {
        return System.currentTimeMillis();
    }

    /** The native engine's notify callback, from any thread. */
    void notifyFromNative() {
        notified = true;
        if (loopParked) LockSupport.unpark(waiter);
    }

    // ------------------------------------------------------------- the loop

    private void loop() {
        try {
            while (iteration()) {
                // Each iteration waits for work, runs requests and timers, and ticks.
            }
            shutdown();
        } catch (Throwable t) {
            // A bug in the SDK: stop, failing every waiter.
            stopAfterFailure(Errors.unwrap(t));
            try {
                shutdown();
            } catch (Throwable ignored) {
                // Nothing more to do.
            }
        } finally {
            done = true;
            cancelQueued();
            doneLatch.countDown();
        }
    }

    /** One turn of the loop; false once the engine is closing (the caller shuts it down). */
    private boolean iteration() {
        if (!work()) return false;
        tick();
        return true;
    }

    /** A turn's first half: wait for work, run requests and timers; false once closing. */
    private boolean work() {
        if (!kicked) waitForWork();
        kicked = false;
        notified = false;
        for (int i = 0; i < 1024 && !closing; i++) {
            Runnable f = reqs.poll();
            if (f == null) break;
            runSafely(f);
        }
        runTimers();
        return !closing;
    }

    /**
     * Task t of run x is about to block with nothing to deliver: rather than
     * hand the engine back to the loop (which waits for the next event and
     * hands it to t again), t runs the loop's turns itself until x has a
     * delivery or a ready task. True if that readied t alone, which then goes
     * on; false to block as usual (the driver below t on the stack resumes
     * the run's tasks). One task drives at a time, never inside a tick.
     */
    boolean drive(Task t, RunExec x) {
        if (driving || ticking || closing || !x.scheduling || x.ended || !x.ready.isEmpty()) return false;
        // The driver below may pump once t blocks (a new run's first
        // deliveries, say): take what is due first.
        if (x.deliverHere(t, true)) return true;
        if (!x.ready.isEmpty() || x.ended) return false;
        driving = true;
        try {
            for (;;) {
                if (!work()) {
                    kicked = true; // the loop below shuts down
                    return false;
                }
                int r = readied(t, x);
                if (r >= 0) return r == 1;
                tick();
                r = readied(t, x);
                if (r >= 0) return r == 1;
            }
        } finally {
            driving = false;
        }
    }

    /** While t drives: 1 if t alone is ready (taken off the queue), 0 to stop driving, -1 to go on. */
    private static int readied(Task t, RunExec x) {
        if (x.ended) return 0;
        if (x.deliver && x.ready.isEmpty() && x.deliverHere(t, false)) return 1;
        if (x.ready.size() == 1 && x.ready.peek() == t) {
            x.ready.poll();
            return 1;
        }
        return x.ready.isEmpty() ? -1 : 0;
    }

    private void runSafely(Runnable f) {
        try {
            f.run();
        } catch (Task.Killed k) {
            throw k;
        } catch (RuntimeException e) {
            stopAfterFailure(e);
        }
    }

    private long nextDue() {
        long due = engineWakeAt >= 0 ? engineWakeAt : Long.MAX_VALUE;
        Timer t = timers.peek();
        while (t != null && t.cancelled) {
            timers.poll();
            cancelledTimers--;
            t = timers.peek();
        }
        if (t != null && t.at < due) due = t.at;
        return due;
    }

    private void waitForWork() {
        waiter = Thread.currentThread();
        long spinEnd = Threads.WAIT_SPIN_NS > 0 ? System.nanoTime() + Threads.WAIT_SPIN_NS : 0;
        int i = 0;
        for (;;) {
            if (notified || !reqs.isEmpty()) return;
            long now = nowMs();
            long due = nextDue();
            if (due <= now) return;
            if (spinEnd != 0) {
                Thread.onSpinWait();
                if ((++i & 63) == 0 && System.nanoTime() > spinEnd) spinEnd = 0;
                continue;
            }
            loopParked = true;
            if (notified || !reqs.isEmpty()) {
                loopParked = false;
                return;
            }
            if (due == Long.MAX_VALUE) LockSupport.park(this);
            else LockSupport.parkNanos(this, TimeUnit.MILLISECONDS.toNanos(due - now));
            loopParked = false;
            Thread.interrupted();
        }
    }

    private void wakeLoop() {
        if (loopParked) LockSupport.unpark(waiter);
    }

    /** Runs f on the loop, later; nothing if the engine closed. */
    void post(Runnable f) {
        reqs.add(f);
        wakeLoop();
        if (done) cancelQueued();
    }

    private void cancelQueued() {
        RuntimeException why = null;
        for (Runnable r; (r = reqs.poll()) != null;) {
            if (r instanceof Req q) {
                if (why == null) why = closedErr();
                q.cancel(why);
            }
        }
    }

    /** A timer at Unix ms; holder only. */
    Timer at(long atMs, Runnable r) {
        Timer t = new Timer(this, atMs, timerOrder++, r);
        timers.add(t);
        if (cancelledTimers > 1024 && cancelledTimers > timers.size() / 2) {
            timers.removeIf(x -> x.cancelled);
            cancelledTimers = 0;
        }
        return t;
    }

    private void runTimers() {
        long now = nowMs();
        for (Timer t; (t = timers.peek()) != null && t.at <= now;) {
            timers.poll();
            if (t.cancelled) {
                cancelledTimers--;
                continue;
            }
            t.cancelled = true;
            runSafely(t.r);
        }
    }

    private void checkOutside() {
        Thread me = Thread.currentThread();
        Task r = running;
        if (me == loopThread || me == waiter || (r != null && r.thread == me)) {
            throw new IllegalStateException("fusee: Engine and Run methods cannot be called from workflow code or "
                    + "inline steps (they hold the engine): use the Ctx");
        }
    }

    /** Runs f holding the engine and waits for it. */
    <T> T hold(Supplier<T> f) {
        checkOutside();
        CompletableFuture<T> cf = new CompletableFuture<>();
        if (done) throw closedErr();
        post(new Req() {
            @Override
            public void run() {
                if (closing) {
                    cf.completeExceptionally(closedErr());
                    return;
                }
                try {
                    cf.complete(f.get());
                } catch (Throwable t) {
                    cf.completeExceptionally(t);
                }
            }

            @Override
            void cancel(RuntimeException why) {
                cf.completeExceptionally(why);
            }
        });
        return join(cf, -1);
    }

    static <T> T join(CompletableFuture<T> cf, long timeoutNanos) {
        try {
            return timeoutNanos < 0 ? cf.get() : cf.get(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw Errors.interrupted();
        } catch (ExecutionException e) {
            throw Errors.unwrap(e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new TimedOutException("timed out waiting");
        }
    }

    /** Issues a call the engine answers with a token event; the reply completes when it does. */
    CompletableFuture<byte[]> request(boolean value, Issue issue) {
        CompletableFuture<byte[]> reply = new CompletableFuture<>();
        hold(() -> {
            long tok = nextToken++;
            tokens.put(tok, new Pending(reply, value));
            try {
                issue.issue(tok);
            } catch (RuntimeException e) {
                tokens.remove(tok);
                throw e;
            }
            kicked = true;
            return null;
        });
        return reply;
    }

    byte[] call(boolean value, Issue issue) {
        return join(request(value, issue), -1);
    }

    private RuntimeException closedErr() {
        return failure != null ? failure : Errors.closed();
    }

    /**
     * Closes the engine: flushes, checkpoints and frees it. Runs left
     * unfinished resume when an engine opens the same directory again.
     */
    @Override
    public void close() {
        checkOutside();
        if (!done) {
            try {
                hold(() -> {
                    closing = true;
                    return null;
                });
            } catch (FuseeException ignored) {
                // Closing already.
            }
            boolean interrupted = false;
            for (;;) {
                try {
                    doneLatch.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }
        RuntimeException e = closeErr;
        if (e != null) throw e;
    }

    private void shutdown() {
        RuntimeException reason = closedErr();
        for (Pending p : tokens.values()) p.reply.completeExceptionally(reason);
        tokens.clear();
        for (List<CompletableFuture<ResultMsg>> ws : waiters.values()) {
            for (CompletableFuture<ResultMsg> w : ws) w.completeExceptionally(reason);
        }
        waiters.clear();
        for (RunExec x : new ArrayList<>(runs.values())) {
            x.ended = true;
            x.reap();
        }
        runs.clear();
        timers.clear();
        try {
            Native.close(h);
        } catch (RuntimeException e) {
            if (failure == null) closeErr = e;
        }
        ENGINES.remove(key);
    }

    void stopAfterFailure(RuntimeException err) {
        if (failure == null) failure = err;
        closing = true;
    }

    /** Spends the tick's budget of inline flushes. */
    boolean inlineOK() {
        if (inlineLeft <= 0) return false;
        inlineLeft--;
        return true;
    }

    // -------------------------------------------------------- native calls

    long commit(int shard) {
        return shard < shardCount ? commits.getLong(shard * 8) : 0;
    }

    private boolean owed() {
        if (io.getInt(0) != 0) {
            io.putInt(0, 0);
            return true;
        }
        return false;
    }

    private void ensureIn(int need) {
        if (need <= inBuf.capacity()) return;
        ByteBuffer b = ByteBuffer.allocateDirect(Math.max(need, inBuf.capacity() * 2)).order(ByteOrder.nativeOrder());
        ByteBuffer old = inBuf.duplicate();
        old.position(0).limit(inPos);
        b.put(old);
        inBuf = b;
        Native.buffers(h, inBuf, outBuf);
    }

    /** Appends an argument to the input buffer; its length. */
    private int in(byte[] b) {
        if (b == null || b.length == 0) return 0;
        ensureIn(inPos + b.length);
        inBuf.put(inPos, b);
        inPos += b.length;
        return b.length;
    }

    private int in(String s) {
        if (s == null || s.isEmpty()) return 0;
        int n = s.length();
        ensureIn(inPos + n);
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            if (c >= 0x80) return in(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            inBuf.put(inPos + i, (byte) c);
        }
        inPos += n;
        return n;
    }

    private long slot(int i) {
        return outLongs.get(i);
    }

    /** A hot call's bytes: the array it returned, else the output buffer's data. */
    private byte[] data(byte[] returned) {
        if (returned != null) return returned;
        long n = slot(Native.OUT_LEN);
        if (n <= 0) return null;
        byte[] b = new byte[(int) n];
        outBuf.get(Native.OUT_DATA, b);
        return b;
    }

    void beginStep(int shard, long run, String name, int flags, long[] out) {
        inPos = 0;
        int l = in(name);
        calls++;
        Native.beginStep(h, shard, run, l, flags);
        for (int i = 0; i < 6; i++) out[i] = slot(i);
    }

    boolean stepGate(int shard, long run, long gate, long notBefore, boolean inline) {
        calls++;
        return Native.stepGate(h, shard, run, gate, notBefore, inline);
    }

    void stepDone(int shard, long run, int seq, int flags, byte[] payload) {
        inPos = 0;
        int l = in(payload);
        calls++;
        Native.stepDone(h, shard, run, seq, flags, l);
    }

    long stepRetry(int shard, long run, int seq, long at, String msg) {
        inPos = 0;
        int l = in(msg);
        calls++;
        return Native.stepRetry(h, shard, run, seq, at, l);
    }

    byte[] host(int shard, long run, int op, long n0, long n1, long n2, byte[] s0, byte[] s1, byte[] s2,
            long[] out) {
        inPos = 0;
        int l0 = in(s0), l1 = in(s1), l2 = in(s2);
        calls++;
        byte[] returned = Native.host(h, shard, run, op, n0, n1, n2, l0, l1, l2);
        if (out != null) {
            for (int i = 0; i < 5; i++) out[i] = slot(i);
        }
        return data(returned);
    }

    int quiescent(int shard, long run, long[] out) {
        calls++;
        int kind = Native.quiescent(h, shard, run);
        if (kind != Native.D_NONE) {
            for (int i = 0; i < 8; i++) out[i] = slot(i);
            long n = slot(Native.OUT_LEN);
            lastDelivery = n == -2 ? Native.deliveryBytes(h) : data(null);
        }
        return kind;
    }

    /** The last delivery's payload, name and id, concatenated. */
    byte[] deliveryBytes() {
        return lastDelivery;
    }

    int[] deliverySeqs() {
        return Native.deliverySeqs(h);
    }

    long decideUpdate(int shard, long token, String refusal) {
        calls++;
        return Native.decideUpdate(h, shard, token, refusal == null, Codec.utf8(refusal));
    }

    void finish(int shard, long run, int status, byte[] payload) {
        inPos = 0;
        int l = in(payload);
        calls++;
        Native.finish(h, shard, run, status, l);
    }

    void stuck(int shard, long run, String msg) {
        calls++;
        Native.stuck(h, shard, run, Codec.utf8(msg));
    }

    // ----------------------------------------------------------------- tick

    private void tick() {
        if (closing) return;
        ticks++;
        inlineLeft = INLINE_PER_TICK;
        io.putInt(0, 0);
        int n;
        try {
            calls++;
            n = Native.tick(h);
        } catch (RuntimeException e) {
            stopAfterFailure(e);
            return;
        }
        long next = slot(0);
        long[] ev = events;
        if (n * Native.STRIDE > ev.length) {
            ev = new long[n * Native.STRIDE * 2];
            events = ev;
        }
        if (slot(1) == 1) outLongs.get(Native.OUT_DATA / 8, ev, 0, n * Native.STRIDE);
        else Native.tickEvents(h, ev);
        if (serveRemote) {
            for (;;) {
                byte[] j;
                try {
                    calls++;
                    j = Native.nextCall(h);
                } catch (RuntimeException e) {
                    stopAfterFailure(e);
                    return;
                }
                if (j == null) break;
                byte[] msg = j;
                try {
                    stepExecutor.execute(() -> Remote.handle(this, msg));
                } catch (RuntimeException e) {
                    Threads.platform(() -> Remote.handle(this, msg), "fusee-remote").start();
                }
            }
        }
        ticking = true;
        try {
            for (int i = 0; i < n; i++) handle(ev, i * Native.STRIDE);
        } finally {
            ticking = false;
        }
        long now = nowMs();
        if (engineWakeAt >= 0 && engineWakeAt <= now) engineWakeAt = -1;
        long at = -1;
        if (next >= 0) at = next;
        else if (!tokens.isEmpty() || !waiters.isEmpty()) at = now + 50;
        if (at < 0) {
            engineWakeAt = -1;
        } else if (at <= now) {
            kicked = true;
        } else if (engineWakeAt < 0 || at < engineWakeAt) {
            // A wake due no later than needed stays: an early tick only looks.
            engineWakeAt = at;
        }
        if (n > 0 || owed()) kicked = true;
    }

    private byte[] extra(long i) {
        return data(Native.tickExtra(h, (int) i));
    }

    private void handle(long[] ev, int at) {
        int kind = (int) ev[at];
        switch (kind) {
            case Native.EV_WAKE -> {
                RunExec x = runs.get(new RunKey((int) ev[at + 1], ev[at + 2]));
                if (x != null) x.wake();
            }
            case Native.EV_ACK, Native.EV_REJECT, Native.EV_REPLY, Native.EV_REFUSE -> {
                Pending p = tokens.remove(ev[at + 1]);
                if (p == null) return;
                switch (kind) {
                    case Native.EV_REPLY -> {
                        byte[] payload = extra(ev[at + 5]);
                        if (ev[at + 2] == Native.S_OK) p.reply.complete(payload);
                        else p.reply.completeExceptionally(
                                new WorkflowFailedException("WorkflowFailed", null, Failure.parse(Codec.text(payload))));
                    }
                    case Native.EV_REFUSE -> p.reply.completeExceptionally(
                            new RejectedException(Codec.text(extra(ev[at + 5]))));
                    case Native.EV_REJECT -> p.reply.completeExceptionally(new FuseeException(
                            p.value ? "AlreadyExists" : "Rejected",
                            "the workflow has already finished, or its ID is in use"));
                    default -> p.reply.complete(null);
                }
            }
            case Native.EV_RESULT -> {
                ArrayList<CompletableFuture<ResultMsg>> ws = waiters.remove(new RunKey((int) ev[at + 1], ev[at + 2]));
                if (ws == null || ws.isEmpty()) return;
                ResultMsg m = new ResultMsg((int) ev[at + 3], extra(ev[at + 5]));
                for (CompletableFuture<ResultMsg> w : ws) w.complete(m);
            }
            case Native.EV_START -> {
                long x = ev[at + 5];
                onStart(new StartEvent((int) ev[at + 1], ev[at + 2], (int) ev[at + 3], ev[at + 4],
                        Codec.utf8(extra(x)), Codec.utf8(extra(x + 1)), extra(x + 2)));
            }
            case Native.EV_STUCK -> {
                RunExec x = runs.get(new RunKey((int) ev[at + 1], ev[at + 2]));
                if (x != null && !stuckHandlers.isEmpty()) {
                    String msg = ev[at + 5] >= 0 ? Codec.utf8(extra(ev[at + 5])) : "";
                    for (java.util.function.BiConsumer<String, String> h : stuckHandlers) h.accept(x.id, msg);
                }
                onEnded((int) ev[at + 1], ev[at + 2]);
            }
            case Native.EV_ENDED -> onEnded((int) ev[at + 1], ev[at + 2]);
            default -> {}
        }
    }

    private void onStart(StartEvent ev) {
        if (!instantiate(ev)) awaiting.computeIfAbsent(ev.wtype(), k -> new ArrayList<>()).add(ev);
    }

    private void onEnded(int shard, long run) {
        RunExec x = runs.remove(new RunKey(shard, run));
        if (x != null) {
            x.ended = true;
            x.reap();
        }
    }

    private boolean instantiate(StartEvent ev) {
        HashMap<Integer, Code> m = types.get(ev.wtype());
        Code fn = m == null ? null : m.get(ev.version());
        if (fn == null) return false;
        RunKey k = new RunKey(ev.shard(), ev.run());
        if (runs.containsKey(k)) return true;
        RunExec x = new RunExec(this, ev);
        runs.put(k, x);
        byte[] input = ev.input();
        Task root = x.newTask(() -> fn.run(x.ctx, input), (v, err) -> finished(x, v, err));
        x.ready.add(root);
        x.runReady();
        x.pump();
        return true;
    }

    void register(String name, int version, Code fn) {
        hold(() -> {
            types.computeIfAbsent(name, k -> new HashMap<>()).put(version, fn);
            ArrayList<StartEvent> waiting = awaiting.remove(name);
            if (waiting != null) {
                ArrayList<StartEvent> keep = new ArrayList<>();
                for (StartEvent ev : waiting) {
                    if (!instantiate(ev)) keep.add(ev);
                }
                if (!keep.isEmpty()) awaiting.put(name, keep);
            }
            return null;
        });
    }

    /** The newest registered version of a type (at least version). */
    int latest(String wtype, int version) {
        HashMap<Integer, Code> m = types.get(wtype);
        if (m != null) {
            for (int v : m.keySet()) {
                if (v > version) version = v;
            }
        }
        return version == 0 ? 1 : version;
    }

    /** Records a run's result when its code returned. */
    private void finished(RunExec x, Object v, Throwable err) {
        if (x.ended) return;
        x.ended = true;
        if (x.stuck) return; // the engine stopped the run; its Stuck event drops it
        runs.remove(new RunKey(x.shard, x.run));
        int status;
        byte[] data;
        if (err == null) {
            try {
                data = codec.encode(v);
                status = Native.S_OK;
            } catch (RuntimeException enc) {
                status = Native.S_FAILED;
                data = Codec.utf8("result is not JSON: " + Errors.message(enc));
            }
        } else {
            status = Errors.status(err);
            data = Codec.utf8(Errors.text(err));
        }
        try {
            finish(x.shard, x.run, status, data);
        } catch (RuntimeException e) {
            stopAfterFailure(e);
        }
        kicked = true;
    }

    // ---------------------------------------------------------- requests

    void startWire(String wtype, int version, String id, byte[] input, StartOptions o) {
        int policy = StartOptions.policy(o.ifRunning, o.ifFinished);
        long deadline = o.timeoutMs > 0 ? nowMs() + o.timeoutMs : 0;
        call(false, tok -> {
            int v = o.version != 0 ? o.version : latest(wtype, version);
            calls++;
            Native.start(h, Codec.utf8(id), Codec.utf8(wtype), input, v, policy, deadline, tok, false, null, null,
                    null, -1);
        });
    }

    byte[] updateWithStartWire(String wtype, int version, String id, byte[] input, String update, byte[] arg,
            StartOptions o) {
        String running = o.ifRunning;
        if (running == null || running.isEmpty() || running.equals("fail")) running = "use-existing";
        int policy = StartOptions.policy(running, o.ifFinished);
        long deadline = o.timeoutMs > 0 ? nowMs() + o.timeoutMs : 0;
        return call(true, tok -> {
            int v = o.version != 0 ? o.version : latest(wtype, version);
            calls++;
            Native.start(h, Codec.utf8(id), Codec.utf8(wtype), input, v, policy, deadline, -1, true,
                    Codec.utf8(update), Codec.utf8(o.updateId), arg, tok);
        });
    }

    /**
     * Signal-with-start: the start (the running run is used if there is one)
     * and the signal reach the engine in one hold, so the signal is recorded
     * before a new run's code first runs; returns once both are durable.
     */
    void signalWithStartWire(String wtype, int version, String id, byte[] input, StartOptions o, String signal,
            byte[] payload) {
        String running = o.ifRunning;
        if (running == null || running.isEmpty() || running.equals("fail")) running = "use-existing";
        int policy = StartOptions.policy(running, o.ifFinished);
        long deadline = o.timeoutMs > 0 ? nowMs() + o.timeoutMs : 0;
        CompletableFuture<byte[]> started = new CompletableFuture<>();
        CompletableFuture<byte[]> signalled = new CompletableFuture<>();
        hold(() -> {
            long t1 = nextToken++;
            tokens.put(t1, new Pending(started, false));
            int v = o.version != 0 ? o.version : latest(wtype, version);
            try {
                calls++;
                Native.start(h, Codec.utf8(id), Codec.utf8(wtype), input, v, policy, deadline, t1, false, null, null,
                        null, -1);
            } catch (RuntimeException ex) {
                tokens.remove(t1);
                throw ex;
            }
            long t2 = nextToken++;
            tokens.put(t2, new Pending(signalled, false));
            try {
                calls++;
                Native.signal(h, Codec.utf8(id), Codec.utf8(signal), payload, t2);
            } catch (RuntimeException ex) {
                tokens.remove(t2);
                throw ex;
            }
            kicked = true;
            return null;
        });
        join(started, -1);
        join(signalled, -1);
    }

    void signalWire(String id, String name, byte[] payload) {
        call(false, tok -> {
            calls++;
            Native.signal(h, Codec.utf8(id), Codec.utf8(name), payload, tok);
        });
    }

    void resumeWire(String id) {
        call(false, tok -> {
            calls++;
            Native.resume(h, Codec.utf8(id), tok);
        });
    }

    void cancelWire(String id, String reason, boolean terminate) {
        call(false, tok -> {
            calls++;
            Native.cancel(h, Codec.utf8(id), Codec.utf8(reason), terminate, tok);
        });
    }

    byte[] updateWire(String id, String name, String updateId, byte[] arg) {
        return call(true, tok -> {
            calls++;
            Native.update(h, Codec.utf8(id), Codec.utf8(name), Codec.utf8(updateId), arg, tok);
        });
    }

    void resetWire(String id, String before, boolean reapply, int version) {
        call(false, tok -> {
            calls++;
            Native.reset(h, Codec.utf8(id), Codec.utf8(before), reapply, version, tok);
        });
    }

    byte[] queryNativeWire(String id, String name, byte[] arg) {
        return call(true, tok -> {
            calls++;
            Native.queryNative(h, Codec.utf8(id), Codec.utf8(name), arg, tok);
        });
    }

    /** One read of a stream (run -1: the ID's newest run): the read's JSON. */
    byte[] streamReadWire(String id, long run, String name, long from, long waitMs) {
        return call(true, tok -> {
            calls++;
            Native.streamRead(h, Codec.utf8(id), run, Codec.utf8(name), from, waitMs, tok);
        });
    }

    /** A run's result, following continue-as-new and reset; timeoutNanos -1 for none. */
    ResultMsg result(String id, long timeoutNanos) {
        long deadline = timeoutNanos < 0 ? 0 : System.nanoTime() + timeoutNanos;
        for (;;) {
            CompletableFuture<ResultMsg> cf = new CompletableFuture<>();
            long[] st = new long[4];
            byte[] payload = hold(() -> {
                calls++;
                byte[] p = Native.result(h, Codec.utf8(id), st);
                if (st[0] == 1) {
                    waiters.computeIfAbsent(new RunKey((int) st[1], st[2]), k -> new ArrayList<>()).add(cf);
                    kicked = true;
                }
                return p;
            });
            if (st[0] == 0) throw new FuseeException("NotFound", "workflow '" + id + "' not found");
            if (st[0] == 2) return new ResultMsg((int) st[3], payload);
            ResultMsg m = join(cf, timeoutNanos < 0 ? -1 : Math.max(0, deadline - System.nanoTime()));
            if (m.status() == Native.S_CONTINUED || m.status() == Native.S_RESET) continue;
            return m;
        }
    }

    /** Holder only. */
    byte[] inspectRaw(String id) {
        calls++;
        return Native.inspect(h, Codec.utf8(id));
    }

    boolean exists(String id) {
        return hold(() -> {
            long[] st = new long[4];
            calls++;
            Native.result(h, Codec.utf8(id), st);
            return st[0] != 0;
        });
    }

    /** Asks a run's query handler; the answer waits until the state it read is durable. */
    byte[] query(String id, String name, byte[] arg) {
        for (int i = 0; i < 4000; i++) {
            long[] point = new long[4];
            Object[] answer = new Object[1];
            int what = hold(() -> {
                calls++;
                Native.queryPoint(h, Codec.utf8(id), point);
                int shard = (int) point[0];
                long run = point[1];
                RunExec x = runs.get(new RunKey(shard, run));
                calls++;
                int state = Native.codeState(h, shard, run);
                boolean slot = (state & 1) != 0, replaying = (state & 2) != 0;
                if (x == null && point[3] == 0) {
                    if (!slot) throw Errors.failed("workflow '" + id + "' is not running code");
                    return 1; // built-in code answers it
                }
                if (x == null || (slot && replaying)) {
                    kicked = true;
                    return 2; // retry
                }
                RunExec.QueryHandler qh = x.queries.get(name);
                if (qh == null) throw new RejectedException("no handler for query '" + name + "'");
                try {
                    answer[0] = qh.query(arg);
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw Errors.failed(Errors.message(e));
                }
                return 0;
            });
            if (what == 1) return queryNativeWire(id, name, arg);
            if (what == 2) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw Errors.interrupted();
                }
                continue;
            }
            int shard = (int) point[0];
            long lsn = point[2];
            call(false, tok -> {
                calls++;
                Native.ackAt(h, shard, lsn, tok);
            });
            return (byte[]) answer[0];
        }
        throw Errors.failed("the workflow did not finish replaying");
    }

    // ------------------------------------------------------------ the API

    /** Registers a workflow type. Runs of the type waiting for code (after a restart) start replaying now. */
    public <I, O> Workflow<I, O> register(String name, Class<I> input, Class<O> output, WorkflowFn<I, O> fn) {
        return register(name, 1, TypeRef.of(input), TypeRef.of(output), fn);
    }

    /** Registers a version of a workflow type: new runs start under the newest; a run keeps the version it started under. */
    public <I, O> Workflow<I, O> register(String name, int version, Class<I> input, Class<O> output,
            WorkflowFn<I, O> fn) {
        return register(name, version, TypeRef.of(input), TypeRef.of(output), fn);
    }

    public <I, O> Workflow<I, O> register(String name, TypeRef<I> input, TypeRef<O> output, WorkflowFn<I, O> fn) {
        return register(name, 1, input, output, fn);
    }

    @SuppressWarnings("unchecked")
    public <I, O> Workflow<I, O> register(String name, int version, TypeRef<I> input, TypeRef<O> output,
            WorkflowFn<I, O> fn) {
        if (version <= 0) version = 1;
        java.lang.reflect.Type in = input.type();
        register(name, version, (ctx, bytes) -> {
            I v;
            try {
                v = (I) codec.decode(bytes, in);
            } catch (RuntimeException e) {
                throw Errors.failed("workflow input: " + Errors.message(e));
            }
            try {
                return fn.run(ctx, v);
            } catch (DeterminismException d) {
                throw d;
            } catch (Exception | Error ex) {
                // Failed or cancelled: undo what is registered first.
                if (ctx.hasCompensations()) ctx.runCompensations();
                throw ex;
            }
        });
        return new Workflow<>(this, name, version, input.type(), output.type());
    }

    /**
     * An invoked worker's invocation (InvocationHandler): "running" until
     * every held shard is idle ("idle"), the budget ends ("expired") or the
     * server is lost ("lost"); "" for other engines.
     */
    String invocationState() {
        return hold(() -> {
            List<byte[]> types = new ArrayList<>();
            for (Map.Entry<String, ArrayList<StartEvent>> en : awaiting.entrySet()) {
                if (!en.getValue().isEmpty()) types.add(Codec.utf8(en.getKey()));
            }
            calls++;
            return Codec.utf8(Native.invocationState(h, types.toArray(new byte[0][])));
        });
    }

    // ----------------------------------------------------------- objects

    /**
     * Registers an object type and its handlers (docs/features.md, "Objects
     * (keyed state)"). Register it right after the engine opens: a call from
     * workflow code to a type no process registered waits until one does.
     * Keys whose runs were waiting for this code resume.
     */
    public ObjectType registerObject(String name, Map<String, ObjectHandler> handlers) {
        return registerObject(name, 1, handlers);
    }

    /** A version of an object type: it takes over each key at its next compaction. */
    public ObjectType registerObject(String name, int version, Map<String, ObjectHandler> handlers) {
        Keyed.checkType(name);
        if (version <= 0) version = 1;
        Map<String, ObjectHandler> table = new java.util.LinkedHashMap<>(handlers);
        List<byte[]> shared = new ArrayList<>();
        for (Map.Entry<String, ObjectHandler> en : table.entrySet()) {
            if (en.getValue() == null) throw Errors.invalid("handler '" + en.getKey() + "' of '" + name + "' is null");
            if (en.getValue().shared) shared.add(Codec.utf8(en.getKey()));
        }
        byte[][] names = shared.toArray(new byte[0][]);
        hold(() -> {
            calls++;
            Native.registerObject(h, Codec.utf8(name), names);
            return null;
        });
        register(name, version, objectMain(table));
        return new ObjectType(this, name, version);
    }

    /**
     * An object type's code: its handlers are the run's updates, looked up
     * when a call is delivered, and it answers the state query. It never
     * returns (the engine compacts the key's run).
     */
    private Code objectMain(Map<String, ObjectHandler> table) {
        return (ctx, input) -> {
            RunExec x = ctx.exec();
            HashMap<String, RunExec.UpdateHandler> m = new HashMap<>();
            for (Map.Entry<String, ObjectHandler> en : table.entrySet()) {
                ObjectHandler oh = en.getValue();
                ObjectCtx oc = new ObjectCtx(ctx, !oh.shared);
                m.put(en.getKey(), new RunExec.UpdateHandler(null, p -> oh.fn.call(oc, codec.decode(p, oh.arg))));
            }
            // Every type takes edits of its state from outside (the engine applies them).
            m.put(Keyed.EDIT_HANDLER, new RunExec.UpdateHandler(null,
                    p -> new Json(Codec.utf8(host(x.shard, x.run, Native.OP_OBJECT_EDIT, 0, 0, 0, p, null, null, null)))));
            x.object = m;
            x.queries.put(Keyed.STATE_QUERY, arg -> {
                if (x.ended) throw Errors.ended();
                return host(x.shard, x.run, Native.OP_OBJECT_STATE, 0, 0, 0, null, null, null, null);
            });
            ctx.untilEnded();
            throw Errors.ended();
        };
    }

    /** Key key of object type type, called from outside workflow code. */
    public ObjectRef object(String type, String key) {
        return new ObjectRef(this, type, key);
    }

    /** The keys of object type type (up to limit per shard; 0: 1,000). */
    public List<String> objects(String type, int limit) {
        List<String> keys = new ArrayList<>();
        for (RunSummary s : list(ListOptions.where("type", type).limit(limit <= 0 ? 1000 : limit))) {
            if (s.object() && s.finishedMs() == null && s.id().startsWith(type + "/")) {
                keys.add(s.id().substring(type.length() + 1));
            }
        }
        return keys;
    }

    /** A handle to start runs of a type registered elsewhere (another process, or another engine through a server). */
    public <I, O> Workflow<I, O> workflow(String name, Class<I> input, Class<O> output) {
        return new Workflow<>(this, name, 0, input, output);
    }

    public <I, O> Workflow<I, O> workflow(String name, TypeRef<I> input, TypeRef<O> output) {
        return new Workflow<>(this, name, 0, input.type(), output.type());
    }

    // The verbs a Client has, with its arguments: one interface for code that
    // starts runs, in process or through a server.

    /** Starts a run of workflow type wtype (as {@link Client#start}). */
    public Run<Object> start(String wtype, String id, Object input) {
        return start(wtype, id, input, null);
    }

    public Run<Object> start(String wtype, String id, Object input, StartOptions opts) {
        return workflow(wtype, Object.class, Object.class).start(id, input, opts);
    }

    /** Signals run id, starting it first with input if it is not running (as {@link Client#signalWithStart}). */
    public Run<Object> signalWithStart(String wtype, String id, Object input, String signal, Object value) {
        return signalWithStart(wtype, id, input, signal, value, null);
    }

    public Run<Object> signalWithStart(String wtype, String id, Object input, String signal, Object value,
            StartOptions opts) {
        return workflow(wtype, Object.class, Object.class).signalWithStart(id, input, signal, value, opts);
    }

    /** Starts the run if needed and delivers an update in the same record; returns the handler's result. */
    public Json updateWithStart(String wtype, String id, Object input, String update, Object arg) {
        return updateWithStart(wtype, id, input, update, arg, null);
    }

    public Json updateWithStart(String wtype, String id, Object input, String update, Object arg, StartOptions opts) {
        return workflow(wtype, Object.class, Object.class)
                .updateWithStart(id, input, update, arg, Json.class, opts)
                .result();
    }

    /** Debounces a start of workflow type wtype under key (see {@link Workflow#debounce}). */
    public Run<Object> debounce(String wtype, String key, Object input, DebounceOptions o) {
        return workflow(wtype, Object.class, Object.class).debounce(key, input, o);
    }

    /** Adds an event to wtype's batch under key (see {@link Workflow#batch}). */
    public Run<Object> batch(String wtype, String key, Object event, BatchOptions o) {
        return workflow(wtype, Object.class, Object.class).batch(key, event, o);
    }

    /** A handle to the run of a workflow ID (following continue-as-new and reset), its result untyped. */
    public Run<Object> run(String id) {
        return new Run<>(this, id, Object.class);
    }

    public <O> Run<O> run(String id, Class<O> output) {
        return new Run<>(this, id, output);
    }

    public <O> Run<O> run(String id, TypeRef<O> output) {
        return new Run<>(this, id, output.type());
    }

    /** Lists runs. */
    public List<RunSummary> list(ListOptions o) {
        ListOptions opts = o == null ? new ListOptions() : o;
        byte[] filter = opts.where.isEmpty() ? null : codec.encode(opts.where);
        int limit = opts.limit > 0 ? opts.limit : 100;
        byte[] out = hold(() -> {
            calls++;
            return Native.list(h, filter, limit);
        });
        return RunSummary.listOf(out);
    }

    public List<RunSummary> list() {
        return list(new ListOptions());
    }

    /** Defines (or resizes) a limit that steps take a permit of (StepOptions.limit). */
    public void limit(String name, LimitOptions o) {
        hold(() -> {
            calls++;
            Native.limit(h, Codec.utf8(name), o.concurrent, o.perSecond);
            limitSpecs.put(name, o);
            return null;
        });
    }

    /** Limits defined through this engine (limits()); held under the engine. */
    private final java.util.TreeMap<String, LimitOptions> limitSpecs = new java.util.TreeMap<>();

    /** A limit and its permits now. */
    public record LimitInfo(String name, int concurrent, double perSecond, int inUse, long waiting) {}

    /** Every limit this engine defined, by name, with its permits now. */
    public List<LimitInfo> limits() {
        java.util.TreeMap<String, LimitOptions> specs = hold(() -> new java.util.TreeMap<>(limitSpecs));
        List<LimitInfo> out = new ArrayList<>();
        for (Map.Entry<String, LimitOptions> en : specs.entrySet()) {
            long[] u = limitUsage(en.getKey());
            out.add(new LimitInfo(en.getKey(), en.getValue().concurrent, en.getValue().perSecond, (int) u[0], u[1]));
        }
        return out;
    }

    /** The engine's counters summed over its shards. */
    public Map<String, Long> metrics() {
        Map<String, Long> total = new java.util.TreeMap<>();
        for (Map<String, Long> m : shardMetrics()) {
            for (Map.Entry<String, Long> en : m.entrySet()) {
                total.merge(en.getKey(), en.getValue(), Long::sum);
            }
        }
        return total;
    }

    private long[] limitUsage(String name) {
        return hold(() -> {
            long[] out = new long[2];
            calls++;
            Native.limitUsage(h, Codec.utf8(name), out);
            return out;
        });
    }

    /** How many permits of a limit are held right now. */
    public int limitInUse(String name) {
        return (int) limitUsage(name)[0];
    }

    /** How many of this process's steps wait for a permit. */
    public long limitWaiting(String name) {
        return limitUsage(name)[1];
    }

    /**
     * Finishes an async completion with its token; returns once the value is
     * durable. A stale token (an earlier attempt's) is rejected.
     */
    public void complete(String token, Object value) {
        byte[] data = codec.encode(value);
        call(false, tok -> {
            calls++;
            Native.complete(h, Codec.utf8(token), true, data, tok);
        });
    }

    /** Fails an async completion with its token. */
    public void fail(String token, String message) {
        call(false, tok -> {
            calls++;
            Native.complete(h, Codec.utf8(token), false, Codec.utf8(message), tok);
        });
    }

    /** A run's journal as an export file (.fjnl), or null. */
    public byte[] exportJournal(String id) {
        return hold(() -> {
            calls++;
            return Native.exportJournal(h, Codec.utf8(id));
        });
    }

    /** Each shard's counters, in shard order (durations in microseconds). */
    @SuppressWarnings("unchecked")
    public List<Map<String, Long>> shardMetrics() {
        byte[] out = hold(() -> {
            calls++;
            return Native.shardMetrics(h);
        });
        List<Map<String, Long>> m = new ArrayList<>();
        for (Object o : (List<Object>) MiniJson.parseTree(Codec.utf8(out))) {
            Map<String, Long> row = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, Object> e : ((Map<String, Object>) o).entrySet()) {
                row.put(e.getKey(), ((Number) e.getValue()).longValue());
            }
            m.add(row);
        }
        return m;
    }

    /** Counts of the engine's work in this process. */
    public record Stats(long nativeCalls, long ticks) {}

    public Stats stats() {
        return hold(() -> new Stats(calls, ticks));
    }

    /** The engine's shard count. */
    public int shards() {
        return shardCount;
    }

    /**
     * Creates a schedule (or updates its spec) that starts workflow with
     * input on its cron expression or interval.
     */
    public Schedule schedule(String name, String workflow, Object input, ScheduleOptions o) {
        return Schedule.create(this, name, workflow, input, o);
    }

    /** A handle to an existing schedule. */
    public Schedule scheduleHandle(String name) {
        return new Schedule(this, name);
    }

    /** The schedules: {@code get(name)} and {@code list()}. */
    public Schedules schedules() {
        return new Schedules(this);
    }

    /** Called on the engine's thread with the workflow ID and the message. */
    private final ArrayList<java.util.function.BiConsumer<String, String>> stuckHandlers = new ArrayList<>();
    private final ArrayList<java.util.function.BiConsumer<String, String>> pausedHandlers = new ArrayList<>();
    /** What a step does when its retries run out, unless its policy says (Options.onExhausted). */
    Exhausted onExhausted = Exhausted.FAIL;

    /**
     * Calls {@code handler(workflowId, message)} when a run sticks (its code
     * no longer matches its journal: {@code "stuck"}) or when a step of this
     * process runs out of retries and pauses its run ({@code "paused"}).
     */
    public void on(String event, java.util.function.BiConsumer<String, String> handler) {
        if (!event.equals("stuck") && !event.equals("paused")) {
            throw new RejectedException("the events are: stuck, paused, not '" + event + "'");
        }
        hold(() -> {
            (event.equals("paused") ? pausedHandlers : stuckHandlers).add(handler);
            return null;
        });
    }

    /** Tells the "paused" handlers; holder only. */
    void emitPaused(String workflowId, String message) {
        for (java.util.function.BiConsumer<String, String> h : pausedHandlers) h.accept(workflowId, message);
    }

    /** The native library's version. */
    public static String libraryVersion() {
        Native.init();
        return Codec.utf8(Native.version());
    }

    /** Whether workflow code and step functions run on virtual threads (Java 21 and later). */
    public static boolean virtualThreads() {
        return Threads.virtual();
    }
}
