package dev.fusee;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A failure as every SDK records and reads it: the failure envelope
 * (docs/errors.md#the-failure-envelope). Throw a {@link FailureException}
 * to set its type, code and details, or a {@link FatalException} to stop
 * retries; any other exception is recorded as its message, its class's
 * simple name (not for plain exception types) and its cause.
 */
public record Failure(String message, String type, String code, Json details, Boolean retryable, String stack,
        Failure cause) {

    /** Exception types whose name is not recorded: they say nothing about the failure. */
    private static final Set<String> PLAIN = Set.of("Throwable", "Exception", "RuntimeException", "Error",
            "FuseeException", "FailureException", "FatalException", "CompletionException", "ExecutionException");

    /** Whether no retry policy retries this failure (it or a cause is not retryable). */
    public boolean isFinal() {
        for (Failure f = this; f != null; f = f.cause) {
            if (Boolean.FALSE.equals(f.retryable)) return true;
        }
        return false;
    }

    /** {@code t} as the envelope it is recorded as. */
    public static Failure of(Throwable t) {
        if (t == null) return null;
        if (t instanceof StepFailedException s) {
            return new Failure(s.getMessage(), "StepFailed", null, null, null, null, s.failure());
        }
        if (t instanceof WorkflowFailedException w) {
            return new Failure(w.getMessage(), w.code(), null, null, null, null, w.failure());
        }
        String name = t.getClass().getSimpleName();
        String type = PLAIN.contains(name) || name.isEmpty() ? null : name;
        String code = null;
        Json details = null;
        Boolean retryable = null;
        if (t instanceof FailureException f) {
            if (f.type() != null) type = f.type();
            code = f.failureCode();
            details = f.details();
            retryable = f.retryable();
        }
        Throwable c = t.getCause();
        return new Failure(Errors.message(t), type, code, details, retryable, null, c == null || c == t ? null : of(c));
    }

    /** As recorded: JSON with absent fields left out. */
    String toJson() {
        return MiniJson.write(tree());
    }

    private Map<String, Object> tree() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("message", message == null ? "" : message);
        if (type != null) m.put("type", type);
        if (code != null) m.put("code", code);
        if (details != null) m.put("details", details);
        if (retryable != null) m.put("retryable", retryable);
        if (stack != null) m.put("stack", stack);
        if (cause != null) m.put("cause", cause.tree());
        return m;
    }

    /** A recorded failure: an envelope, or (recorded before the envelope) its text. */
    static Failure parse(String text) {
        if (text != null && text.startsWith("{")) {
            try {
                if (MiniJson.parseTree(text) instanceof Map<?, ?> m && (m.get("message") != null || m.get("type") != null)) {
                    return from(m);
                }
            } catch (RuntimeException notJson) {
                // fall through: plain text
            }
        }
        return new Failure(text == null ? "" : text, null, null, null, null, null, null);
    }

    private static Failure from(Map<?, ?> m) {
        Object d = m.get("details");
        Object r = m.get("retryable");
        Object c = m.get("cause");
        return new Failure(str(m.get("message")), str(m.get("type")), str(m.get("code")),
                d == null ? null : Json.from(d), r instanceof Boolean b ? b : null, str(m.get("stack")),
                c instanceof Map<?, ?> cm ? from(cm) : null);
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }
}
