package dev.fusee;

import java.nio.ByteBuffer;

/**
 * The JNI layer over libfusee_c (bindings/c/include/fusee.h), one static
 * method per C function. A failed call throws the exception {@link #fail}
 * builds from its status code and message. Every engine method is called by
 * the thread holding that engine (see Engine).
 *
 * <p>The calls on the step path take their byte arguments from the engine's
 * input buffer (consecutive, by length) and leave numbers in the first 16
 * slots of its output buffer and bytes after them (slot 15 their length: -1
 * none, -2 returned as an array instead), so they make no JNI call back into
 * the JVM: each of those (an array access) costs about 70 ns on macOS/arm64.
 * The other calls take UTF-8 byte arrays and return arrays.
 */
final class Native {
    private Native() {}

    /** The C interface version this SDK speaks (2: streams; 3: objects). */
    static final int ABI = 4;

    static {
        NativeLoader.load();
        int abi = abiVersion();
        if (abi != ABI) {
            throw new UnsatisfiedLinkError("libfusee_jni speaks C interface version " + abi + "; this SDK needs " + ABI);
        }
    }

    /** Ensures the library is loaded (runs the static initializer). */
    static void init() {}

    // Status codes (FUSEE_*).
    static final int OK = 0, FAILED = 1, BUSY = 2, NOT_FOUND = 3, ALREADY_EXISTS = 4, UNKNOWN_WORKFLOW = 5,
            DETERMINISM = 6, REJECTED = 7, IO = 8, CLOSED = 9, INVALID = 10, PANIC = 11;

    // Tick events.
    static final int STRIDE = 6;
    static final int EV_WAKE = 1, EV_ACK = 2, EV_REJECT = 3, EV_START = 4, EV_STUCK = 5, EV_ENDED = 6,
            EV_RESULT = 7, EV_REPLY = 8, EV_REFUSE = 9;

    // Statuses of steps, runs and replies.
    static final int S_OK = 0, S_FAILED = 1, S_CANCELLED = 2, S_TIMED_OUT = 3, S_TERMINATED = 4,
            S_CONTINUED = 5, S_RESET = 6;

    // Deliveries.
    static final int D_NONE = 0, D_RESOLVE = 1, D_HELD = 2, D_CANCEL = 3, D_UPDATE = 4;

    // Step flags.
    static final int STEP_READ_ONLY = 1, STEP_INLINE = 2, DONE_OK = 1, DONE_KEEP = 2;

    // Journal operations (fusee_engine_host).
    static final int OP_BEGIN_AWAIT = 1, OP_BEGIN_SLEEP = 2, OP_BEGIN_WAIT = 3, OP_BEGIN_CHILD = 4, OP_SEND = 5,
            OP_AWAITING = 6, OP_HEARTBEAT = 7, OP_PATCHED = 8, OP_SET_ATTRIBUTES = 9, OP_UPDATE_DONE = 10,
            OP_CONTINUE_AS_NEW = 11, OP_JOURNAL_SIZE = 12, OP_CANCEL_WORKFLOW = 13, OP_FORGET = 14,
            OP_LIMIT_TICKET = 15, OP_ACQUIRE = 16, OP_RELEASE = 17, OP_WAKE_AT = 18, OP_VALUE = 19, OP_KEY = 20,
            OP_TOKEN = 21, OP_PROGRESS = 22, OP_STREAM_OPEN = 23, OP_STREAM_APPEND = 24, OP_STREAM_CLOSE = 25,
            OP_STREAM_RECORDED = 26, OP_OBJECT_GET = 27, OP_OBJECT_SET = 28, OP_OBJECT_KEYS = 29,
            OP_OBJECT_STATE = 30, OP_BEGIN_CALL = 31, OP_STEP_PAUSED = 32, OP_OBJECT_EDIT = 33;

    /** A stream operation's writer when it is workflow code (FUSEE_NO_SEQ). */
    static final long NO_SEQ = 0xFFFFFFFFL;
    /** Called by the native layer to build a failed call's exception. */
    static RuntimeException fail(int code, byte[] message) {
        return Errors.fromCode(code, Codec.utf8(message));
    }

    /** The engine's notify callback, from any thread (never from inside a call on it). */
    static void notify(long key) {
        Engine e = Engine.byKey(key);
        if (e != null) {
            e.notifyFromNative();
        }
    }

    // ------------------------------------------------------------- library

    static native int abiVersion();

    static native byte[] version();

    /** Output buffer: where the bytes start, and the slot of their length. */
    static final int OUT_DATA = 128, OUT_LEN = 15;

    // ----------------------------------------------------------- lifecycle

    /** Returns the engine handle. inlineFlush: -1 default, 0 off, 1 on; times -1 for each default. */
    static native long open(byte[] durability, byte[] dir, byte[] address, int replicaId, byte[][] peers, int shards,
            boolean keepHistory, boolean archiveHistory, int inlineFlush, long parkAfterMs, long retentionMs,
            long checkpointIntervalMs, long segmentBytes, byte[] flush, byte[][] keyIds, byte[][] keys,
            long notifyKey);

    static native void close(long h);

    /** fusee_io: owed (u32) at 0, shards (u32) at 4. */
    static native ByteBuffer io(long h);

    /** The per-shard commit positions (u64 each) fusee_io points at. */
    static native ByteBuffer commits(long h);

    /** Registers the engine's input and output buffers (direct, native order). */
    static native void buffers(long h, ByteBuffer in, ByteBuffer out);

    // ------------------------------------------------------------- driving

    /**
     * Ticks: returns the number of events; slot 0 is next_wake_ms, slot 1 is
     * 1 if the event rows are in the output buffer's data (else fetch them
     * with tickEvents).
     */
    static native int tick(long h);

    /** The last tick's event rows (valid until the next tick). */
    static native void tickEvents(long h, long[] events);

    /** The last tick's extra i, in the output buffer (or as an array). */
    static native byte[] tickExtra(long h, int i);

    /** Server mode: the next relayed call's JSON, or null. */
    static native byte[] nextCall(long h);

    static native void replyCall(long h, long relay, byte[] json);

    /** An invoked worker's state ("running", "idle", "expired", "lost"), empty for other engines. */
    static native byte[] invocationState(long h, byte[][] awaiting);

    // ------------------------------------------------------------ requests

    /** Returns whether the ID existed. */
    static native boolean start(long h, byte[] id, byte[] wtype, byte[] input, int version, int policy,
            long deadlineMs, long token, boolean hasUpdate, byte[] updateName, byte[] updateId, byte[] updateArg,
            long updateToken);

    static native void signal(long h, byte[] id, byte[] name, byte[] payload, long token);

    static native void cancel(long h, byte[] id, byte[] reason, boolean terminate, long token);

    static native void resume(long h, byte[] id, long token);

    static native void update(long h, byte[] id, byte[] name, byte[] updateId, byte[] arg, long token);

    static native void complete(long h, byte[] completion, boolean ok, byte[] payload, long token);

    static native void reset(long h, byte[] id, byte[] beforeStep, boolean reapply, int version, long token);

    static native void ackAt(long h, int shard, long lsn, long token);

    static native void queryNative(long h, byte[] id, byte[] name, byte[] arg, long token);

    /** A stream read (run -1: the ID's newest run): REPLY with the read's JSON. */
    static native void streamRead(long h, byte[] id, long run, byte[] name, long from, long waitMs, long token);

    // --------------------------------------------------------------- reads

    /** out: state (0 not found, 1 running, 2 done), shard, run, status; returns the payload. */
    static native byte[] result(long h, byte[] id, long[] out);

    /** Null if there is no such run. */
    static native byte[] inspect(long h, byte[] id);

    static native byte[] list(long h, byte[] filter, int limit);

    /** Null if there is no such run. */
    static native byte[] exportJournal(long h, byte[] id);

    static native byte[] shardMetrics(long h);

    /** out: shard, run, lsn, foreign. */
    static native void queryPoint(long h, byte[] id, long[] out);

    static native int codeState(long h, int shard, long run);

    static native void limit(long h, byte[] name, int concurrent, double perSecond);

    /** out: held, waiting. */
    static native void limitUsage(long h, byte[] name, long[] out);

    /** Marks name as an object type; the handlers named in shared are shared. */
    static native void registerObject(long h, byte[] name, byte[][] shared);

    // -------------------------------------------------------- workflow code

    /** The name in the input buffer; slots: seq, replay, gate, attempt, open, not_before_ms. */
    static native void beginStep(long h, int shard, long run, int nameLen, int flags);

    static native boolean stepGate(long h, int shard, long run, long gate, long notBeforeMs, boolean inline);

    /** The payload in the input buffer. */
    static native void stepDone(long h, int shard, long run, int seq, int flags, int len);

    /** The error in the input buffer; returns the next attempt's gate. */
    static native long stepRetry(long h, int shard, long run, int seq, long retryAtMs, int len);

    /**
     * A journal operation: its strings in the input buffer (by length), its
     * five numbers in slots 0-4, its data in the output buffer (or as an
     * array).
     */
    static native byte[] host(long h, int shard, long run, int op, long n0, long n1, long n2, int l0, int l1, int l2);

    /**
     * The run's next delivery: returns its kind; slots: seq, status, n_seqs,
     * ordinal, token, payload length, name length, id length, and its
     * payload, name and id concatenated in the data (or -2: deliveryBytes).
     */
    static native int quiescent(long h, int shard, long run);

    /** The last delivery's payload, name and id, concatenated. */
    static native byte[] deliveryBytes(long h);

    /** The last delivery's cancelled seqs. */
    static native int[] deliverySeqs(long h);

    /** Returns the ordinal to run the handler under, or -1. */
    static native long decideUpdate(long h, int shard, long token, boolean ok, byte[] message);

    /** The payload in the input buffer. */
    static native void finish(long h, int shard, long run, int status, int len);

    static native void stuck(long h, int shard, long run, byte[] message);

    // ------------------------------------------------------- free functions

    /** Coordinator type, coordinator ID, start input and update argument. */
    static native byte[][] queueRequest(int kind, byte[] workflow, byte[] key, byte[] eventId, byte[] input, double a,
            double b, double c);

    /** Null if the spec is valid, else the problem. */
    static native byte[] validateSchedule(byte[] spec);

    // --------------------------------------------------------------- client

    static native long clientConnect(byte[] address);

    static native byte[] clientCall(long c, byte[] method, byte[] args);

    static native void clientClose(long c);

    static native void clientFree(long c);
}
