package dev.fusee;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.regex.Pattern;

/** A registered workflow type: I is its input, O its result. */
public final class Workflow<I, O> {
    static final String EVENT_UPDATE = "fusee.event";
    private static final String ENDED_TEXT = "the workflow ended before the update finished";
    private static final Pattern WORKFLOW_FAILED = Pattern.compile("^(?:workflow failed: )+");

    private final Engine e;
    private final String name;
    private final int version;
    private final Type input, output;

    Workflow(Engine e, String name, int version, Type input, Type output) {
        this.e = e;
        this.name = name;
        this.version = version;
        this.input = input;
        this.output = output;
    }

    public String name() {
        return name;
    }

    /** The registered version (0 for a handle to a type registered elsewhere). */
    public int version() {
        return version;
    }

    /** Starts a run; returns once the start is durable. */
    public Run<O> start(String id, I in) {
        return start(id, in, null);
    }

    public Run<O> start(String id, I in, StartOptions opts) {
        e.startWire(name, version, id, e.codec.encode(in), opts == null ? new StartOptions() : opts);
        return new Run<>(e, id, output);
    }

    /**
     * Signals the run {@code id}, starting it first with {@code in} if it is
     * not running: the signal is recorded before a new run's code first runs.
     */
    public Run<O> signalWithStart(String id, I in, String signal, Object value) {
        return signalWithStart(id, in, signal, value, null);
    }

    public Run<O> signalWithStart(String id, I in, String signal, Object value, StartOptions opts) {
        e.signalWithStartWire(name, version, id, e.codec.encode(in), opts == null ? new StartOptions() : opts, signal,
                e.codec.encode(value));
        return new Run<>(e, id, output);
    }

    /** A run started by update-with-start, and the update handler's result. */
    public record Started<O, R>(Run<O> run, R result) {}

    /**
     * Starts the workflow if needed (ifRunning defaults to "use-existing") and
     * delivers an update in the same record; returns the run and the update
     * handler's result.
     */
    public <R> Started<O, R> updateWithStart(String id, I in, String update, Object arg, Class<R> result) {
        return updateWithStart(id, in, update, arg, result, null);
    }

    @SuppressWarnings("unchecked")
    public <R> Started<O, R> updateWithStart(String id, I in, String update, Object arg, Class<R> result,
            StartOptions opts) {
        byte[] p = e.updateWithStartWire(name, version, id, e.codec.encode(in), update, e.codec.encode(arg),
                opts == null ? new StartOptions() : opts);
        return new Started<>(new Run<>(e, id, output), (R) e.codec.decode(p, result));
    }

    /**
     * Debounces a start under key. Returns once this start is durable, with
     * the handle of the run it joined; the run exists once the window closes
     * (Run.started).
     */
    public Run<O> debounce(String key, I in, DebounceOptions o) {
        if (o == null || o.windowMs <= 0) throw Errors.invalid("debounce needs a window");
        byte[][] q = queueEvent(e.codec, 0, name, key, o.id, in, o.windowMs, nanOr(o.maxWaitMs), nanOr(o.dedupWindowMs));
        return new Run<>(e, enqueue(e, q, o.id), output);
    }

    /**
     * Adds an event to the batch under key: once the batch is full or its
     * wait passed, one run starts with all its events, in the order they were
     * accepted, as its input (a list). Returns once the event is durable, with
     * the handle of that run.
     */
    public Run<O> batch(String key, Object event, BatchOptions o) {
        BatchOptions b = o == null ? new BatchOptions() : o;
        byte[][] q = queueEvent(e.codec, 1, name, key, b.id, event, b.maxEvents > 0 ? b.maxEvents : Double.NaN,
                nanOr(b.maxWaitMs), nanOr(b.dedupWindowMs));
        return new Run<>(e, enqueue(e, q, b.id), output);
    }

    static double nanOr(long ms) {
        return ms <= 0 ? Double.NaN : ms;
    }

    /** A debounced start (kind 0) or batched event (1) as the update-with-start that carries it. */
    static byte[][] queueEvent(Codec codec, int kind, String workflow, String key, String eventId, Object value,
            double a, double b, double c) {
        if (value instanceof byte[]) {
            throw Errors.invalid("a " + (kind == 0 ? "debounced input" : "batched event") + " is JSON, not bytes");
        }
        byte[] input = codec.encode(value);
        return Native.queueRequest(kind, Codec.utf8(workflow), Codec.utf8(key), Codec.utf8(eventId), input, a, b, c);
    }

    interface Send {
        byte[] send();
    }

    /** Sends an event until a coordinator takes it (one that was ending refused it); the receipt's run ID. */
    @SuppressWarnings("unchecked")
    static String untilTaken(String coordinator, Send send) {
        for (int i = 0; i < 100; i++) {
            byte[] p;
            try {
                p = send.send();
            } catch (FuseeException err) {
                if (!(err.getClass() == FuseeException.class)
                        || !WORKFLOW_FAILED.matcher(err.getMessage()).replaceAll("").equals(ENDED_TEXT)) {
                    throw err;
                }
                continue;
            }
            Object receipt = MiniJson.parseTree(Codec.raw(p));
            if (receipt instanceof Map<?, ?> m && m.get("id") instanceof String id) return id;
            throw Errors.failed("a coordinator's receipt has no id: " + Codec.raw(p));
        }
        throw Errors.failed("coordinator '" + coordinator + "' kept ending before taking the event");
    }

    private static String enqueue(Engine e, byte[][] q, String eventId) {
        String wtype = Codec.utf8(q[0]), coordinator = Codec.utf8(q[1]);
        StartOptions o = new StartOptions().ifRunning("use-existing").version(1).updateId(eventId);
        return untilTaken(coordinator, () -> e.updateWithStartWire(wtype, 1, coordinator, q[2], EVENT_UPDATE, q[3], o));
    }
}
