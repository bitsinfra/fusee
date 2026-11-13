package dev.fusee;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Sends calls through a Fusee server to whichever worker holds the
 * workflow's shards, in any language. Safe for concurrent use; each call
 * blocks until the worker answers.
 */
public final class Client implements AutoCloseable {
    private final long c;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean closed;
    private final Codec codec = new Codec(null);

    private Client(long c) {
        this.c = c;
    }

    /** Connects: host:port, unix:/path.sock or replicas://..., with options such as ?ca=ca.pem&amp;token-file=token. */
    public static Client connect(String address) {
        Native.init();
        return new Client(Native.clientConnect(Codec.utf8(address)));
    }

    /** Sends a protocol call and waits for the worker's answer. */
    public Json call(String method, Map<String, ?> args) {
        byte[] b = MiniJson.INSTANCE.encode(args);
        lock.readLock().lock();
        try {
            if (closed) throw Errors.closed();
            return Json.of(Codec.utf8(Native.clientCall(c, Codec.utf8(method), b)));
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Closes the connection, failing calls in progress. */
    @Override
    public void close() {
        Native.clientClose(c);
        lock.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                Native.clientFree(c);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    Json value(Object v) {
        if (v == null || v instanceof Json) return (Json) v;
        return Json.of(Codec.raw(codec.encode(v)));
    }

    private static Map<String, Object> options(StartOptions o) {
        Map<String, Object> policy = new LinkedHashMap<>();
        policy.put("running", o.ifRunning == null || o.ifRunning.isEmpty() ? "fail" : o.ifRunning);
        policy.put("finished", o.ifFinished == null || o.ifFinished.isEmpty() ? "allow" : o.ifFinished);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("timeout", o.timeoutMs > 0 ? o.timeoutMs : null);
        m.put("idPolicy", policy);
        return m;
    }

    /** Key key of object type type, called through the server. */
    public RemoteObjectRef object(String type, String key) {
        return new RemoteObjectRef(this, type, key);
    }

    /** Starts a run of workflow type wtype. */
    public RemoteRun start(String wtype, String id, Object input) {
        return start(wtype, id, input, null);
    }

    public RemoteRun start(String wtype, String id, Object input, StartOptions opts) {
        StartOptions o = opts == null ? new StartOptions() : opts;
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("type", wtype);
        a.put("id", id);
        a.put("input", value(input));
        a.put("version", o.version == 0 ? null : o.version);
        a.put("options", options(o));
        call("start", a);
        return run(id);
    }

    /** Signals run id, starting it first with input if it is not running (as {@link Engine#signalWithStart}). */
    public RemoteRun signalWithStart(String wtype, String id, Object input, String signal, Object value) {
        return signalWithStart(wtype, id, input, signal, value, null);
    }

    public RemoteRun signalWithStart(String wtype, String id, Object input, String signal, Object value,
            StartOptions opts) {
        StartOptions o = opts == null ? new StartOptions() : opts;
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("type", wtype);
        a.put("id", id);
        a.put("input", value(input));
        a.put("name", signal);
        a.put("value", value(value));
        a.put("version", o.version == 0 ? null : o.version);
        a.put("options", options(o));
        call("signalWithStart", a);
        return run(id);
    }

    /** Starts the workflow if needed and delivers an update in the same record; returns the handler's result. */
    public Json updateWithStart(String wtype, String id, Object input, String update, Object arg) {
        return updateWithStart(wtype, id, input, update, arg, null);
    }

    public Json updateWithStart(String wtype, String id, Object input, String update, Object arg, StartOptions opts) {
        StartOptions o = opts == null ? new StartOptions() : opts.copy();
        if (o.ifRunning == null || o.ifRunning.isEmpty()) o.ifRunning("use-existing");
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("type", wtype);
        a.put("id", id);
        a.put("input", value(input));
        a.put("name", update);
        a.put("arg", value(arg));
        a.put("updateId", o.updateId);
        a.put("version", o.version == 0 ? null : o.version);
        a.put("options", options(o));
        return call("updateWithStart", a);
    }

    private RemoteRun enqueue(byte[][] q, String eventId) {
        Map<String, Object> policy = new LinkedHashMap<>();
        policy.put("running", "use-existing");
        policy.put("finished", "allow");
        Map<String, Object> opts = new LinkedHashMap<>();
        opts.put("timeout", null);
        opts.put("idPolicy", policy);
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("type", Codec.utf8(q[0]));
        a.put("id", Codec.utf8(q[1]));
        a.put("input", Json.of(Codec.utf8(q[2])));
        a.put("name", Workflow.EVENT_UPDATE);
        a.put("arg", Json.of(Codec.utf8(q[3])));
        a.put("updateId", eventId);
        a.put("version", 1);
        a.put("options", opts);
        String id = Workflow.untilTaken(Codec.utf8(q[1]), () -> Codec.utf8(call("updateWithStart", a).text()));
        return run(id);
    }

    /** Debounces a start of workflow type wtype under key (see Workflow.debounce). */
    public RemoteRun debounce(String wtype, String key, Object input, DebounceOptions o) {
        if (o == null || o.windowMs <= 0) throw Errors.invalid("debounce needs a window");
        return enqueue(Workflow.queueEvent(codec, 0, wtype, key, o.id, input, o.windowMs, Workflow.nanOr(o.maxWaitMs),
                Workflow.nanOr(o.dedupWindowMs)), o.id);
    }

    /** Adds an event to the batch of workflow type wtype under key (see Workflow.batch). */
    public RemoteRun batch(String wtype, String key, Object event, BatchOptions opts) {
        BatchOptions o = opts == null ? new BatchOptions() : opts;
        return enqueue(Workflow.queueEvent(codec, 1, wtype, key, o.id, event, o.maxEvents > 0 ? o.maxEvents : Double.NaN,
                Workflow.nanOr(o.maxWaitMs), Workflow.nanOr(o.dedupWindowMs)), o.id);
    }

    /** Lists runs across the server's shards. */
    public List<RunSummary> list(ListOptions opts) {
        ListOptions o = opts == null ? new ListOptions() : opts;
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("limit", o.limit > 0 ? o.limit : 100);
        a.put("where", o.where);
        return RunSummary.listOf(Codec.utf8(call("list", a).text()));
    }

    /** Finishes an async completion. */
    public void complete(String token, Object value) {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("token", token);
        a.put("value", value(value));
        call("complete", a);
    }

    /** Fails an async completion. */
    public void fail(String token, String message) {
        call("fail", Map.of("token", token, "error", message));
    }

    /** A handle to a run through the server. */
    public RemoteRun run(String id) {
        return new RemoteRun(this, id);
    }
}
