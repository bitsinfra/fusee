package dev.fusee;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * A writer of one of the run's streams ({@link Ctx#stream}; see
 * docs/features.md#streams). Opened in workflow code it writes the run's
 * stream, which {@link #close} or the run's end completes; the run's step
 * functions may append to it too. Opened in a step's function it writes the
 * step's stream, which the step's final result completes.
 *
 * <p>Appending never waits for the disk: from a step function's thread an
 * append is queued for the engine's loop (one step's appends stay in order,
 * and before its result) and goes to the log with whatever flushes next; a
 * failure is thrown by the next {@code append} or {@code close}. Readers
 * see a chunk only once it is durable. A step that runs again (a retry, or
 * after a crash) keeps the chunks its earlier attempts recorded and
 * continues after them ({@link #recorded}); workflow code that replays
 * appends nothing.
 */
public final class StreamWriter {
    private final RunExec x;
    private final Engine e;
    private final String name;
    private final byte[] nameBytes;
    /** The step attempt that opened it; null for workflow code. */
    private final StepInfo step;
    private final long recorded;
    private final boolean closed;
    /** The first failure of an append queued for the loop. */
    private volatile RuntimeException failure;

    private StreamWriter(RunExec x, String name, StepInfo step, long recorded, boolean closed) {
        this.x = x;
        this.e = x.e;
        this.name = name;
        this.nameBytes = Codec.utf8(name);
        this.step = step;
        this.recorded = recorded;
        this.closed = closed;
    }

    static FuseeException over() {
        return new FuseeException("Failed",
                "the step's attempt is over (it timed out or the run ended); its stream operations are dropped");
    }

    /** Opens stream name for workflow code, or for the step whose function runs on this thread. */
    static StreamWriter open(RunExec x, String name) {
        Engine e = x.e;
        byte[] nb = Codec.utf8(name);
        long[] r = new long[5];
        StepInfo s = StepInfo.CURRENT.get();
        if (s != null && s.x == x) {
            s.within(() -> {
                if (!s.live()) throw over();
                try {
                    e.host(x.shard, x.run, Native.OP_STREAM_OPEN, s.seq & 0xffffffffL, 0, 0, nb, null, null, r);
                } catch (RuntimeException err) {
                    throw x.note(err);
                }
                return null;
            });
            return new StreamWriter(x, name, s, r[0], r[1] != 0);
        }
        x.ctx.enter();
        if (x.ended) throw Errors.ended();
        try {
            e.host(x.shard, x.run, Native.OP_STREAM_OPEN, Native.NO_SEQ, 0, 0, nb, null, null, r);
        } catch (RuntimeException err) {
            throw x.note(err);
        }
        return new StreamWriter(x, name, null, r[0], r[1] != 0);
    }

    public String name() {
        return name;
    }

    /** How many chunks the step's earlier attempts recorded (after a crash, the durable ones); 0 on a first attempt and in workflow code. */
    public long recordedCount() {
        return recorded;
    }

    /** The stream was complete when it was opened: an earlier attempt of the step completed it (appends are dropped), or it was closed. */
    public boolean closed() {
        return closed;
    }

    /** The step attempt an operation on this thread belongs to, or null for workflow code. */
    private StepInfo writer() {
        if (step != null) return step;
        StepInfo s = StepInfo.CURRENT.get();
        return s != null && s.x == x ? s : null;
    }

    /** Appends a chunk: a {@code byte[]} as bytes, anything else as JSON. Never waits for the disk. */
    public void append(Object value) {
        byte[] p = e.codec.encode(value);
        RuntimeException f = failure;
        if (f != null) throw f;
        StepInfo s = writer();
        if (s == null) {
            x.ctx.enter();
            direct(Native.OP_STREAM_APPEND, Native.NO_SEQ, p);
            return;
        }
        if (step != null && closed) return;
        if (s.inline) {
            if (!s.live()) throw over();
            direct(Native.OP_STREAM_APPEND, s.seq & 0xffffffffL, p);
            return;
        }
        e.streams.add(this, s, p);
    }

    /** Completes the stream: readers end once they have every chunk. */
    public void close() {
        StepInfo s = writer();
        if (s == null) {
            x.ctx.enter();
            direct(Native.OP_STREAM_CLOSE, Native.NO_SEQ, null);
            return;
        }
        // After the appends queued before it: the loop runs them in order.
        s.within(() -> {
            if (!s.live()) throw over();
            direct(Native.OP_STREAM_CLOSE, s.seq & 0xffffffffL, null);
            return null;
        });
        RuntimeException f = failure;
        if (f != null) throw f;
    }

    /** The chunks the step's earlier attempts recorded, as maps, lists, strings, numbers, booleans and byte arrays. */
    public List<Object> recorded() {
        return recordedAs(Object.class);
    }

    /**
     * The chunks the step's earlier attempts recorded, decoded: a step that
     * runs again continues after them (for a model, by passing them back as
     * the start of its answer), so readers see no chunk twice.
     */
    public <T> List<T> recorded(Class<T> type) {
        return recordedAs(type);
    }

    public <T> List<T> recorded(TypeRef<T> type) {
        return recordedAs(type.type());
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> recordedAs(Type t) {
        List<T> out = new ArrayList<>();
        StepInfo s = step;
        if (s == null || recorded == 0) return out;
        byte[] framed = s.within(() -> {
            if (x.ended) throw Errors.ended();
            return e.host(x.shard, x.run, Native.OP_STREAM_RECORDED, s.seq & 0xffffffffL, recorded, 0, nameBytes,
                    null, null, null);
        });
        if (framed == null) return out;
        for (int at = 0; at + 4 <= framed.length;) {
            int n = (framed[at] & 0xff) | (framed[at + 1] & 0xff) << 8 | (framed[at + 2] & 0xff) << 16
                    | (framed[at + 3] & 0xff) << 24;
            if (n < 0 || at + 4 + n > framed.length) break;
            byte[] p = new byte[n];
            System.arraycopy(framed, at + 4, p, 0, n);
            out.add((T) e.codec.decode(p, t));
            at += 4 + n;
        }
        return out;
    }

    /** Runs a stream operation; holder only. */
    private void direct(int op, long seq, byte[] p) {
        if (x.ended) throw Errors.ended();
        try {
            e.host(x.shard, x.run, op, seq, 0, 0, nameBytes, p, null, null);
        } catch (RuntimeException err) {
            throw x.note(err);
        }
        e.kicked = true;
    }

    private void fail(RuntimeException err) {
        if (failure == null) failure = err;
    }

    @Override
    public String toString() {
        return "StreamWriter(" + name + ")";
    }

    /**
     * The appends step threads made since the loop last took them: the first
     * posts one flush, which records all of them, in order, holding the
     * engine.
     */
    static final class Queue {
        private record Pending(StreamWriter w, StepInfo s, byte[] chunk) {}

        private final Engine e;
        private final Object lock = new Object();
        private ArrayList<Pending> ops = new ArrayList<>(), spare = new ArrayList<>();
        private boolean posted;
        private final Runnable flush = this::flush;

        Queue(Engine e) {
            this.e = e;
        }

        void add(StreamWriter w, StepInfo s, byte[] chunk) {
            boolean post;
            synchronized (lock) {
                ops.add(new Pending(w, s, chunk));
                post = !posted;
                posted = true;
            }
            if (post) e.post(flush);
        }

        private void flush() {
            ArrayList<Pending> mine;
            synchronized (lock) {
                mine = ops;
                ops = spare;
                spare = null;
                posted = false;
            }
            for (Pending p : mine) {
                if (!p.s.live()) {
                    p.w.fail(over());
                    continue;
                }
                try {
                    p.w.direct(Native.OP_STREAM_APPEND, p.s.seq & 0xffffffffL, p.chunk);
                } catch (RuntimeException err) {
                    p.w.fail(err);
                }
            }
            mine.clear();
            synchronized (lock) {
                spare = mine;
            }
        }
    }
}
