package dev.fusee;

import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

/** Status codes and outcomes. */
final class Errors {
    private Errors() {}

    private static final String[] NAMES = {"Ok", "Failed", "Busy", "NotFound", "AlreadyExists", "UnknownWorkflow",
        "Determinism", "Rejected", "Io", "Closed", "Invalid", "Panic"};

    static FuseeException fromCode(int code, String msg) {
        if (code == Native.DETERMINISM) return new DeterminismException(msg);
        if (code == Native.REJECTED) {
            return new RejectedException(msg.startsWith("rejected: ") ? msg.substring(10) : msg);
        }
        String name = code > 0 && code < NAMES.length ? NAMES[code] : "Failed";
        return new FuseeException(name, msg);
    }

    static FuseeException closed() {
        return new FuseeException("Closed", "engine closed");
    }

    static FuseeException ended() {
        return new FuseeException("Failed", "workflow execution has ended");
    }

    static FuseeException invalid(String msg) {
        return new FuseeException("Invalid", msg);
    }

    static FuseeException failed(String msg) {
        return new FuseeException("Failed", msg);
    }

    /** An exception's message as the journal records it. */
    static String message(Throwable t) {
        String m = t.getMessage();
        return m != null ? m : t.getClass().getName();
    }

    /** The code another SDK's caller sees for an error. */
    static String code(Throwable t) {
        return t instanceof FuseeException f ? f.code() : "Failed";
    }

    /** What a relayed call answers with: a failure's envelope (a failed run's is what it recorded). */
    static String reply(Throwable t) {
        if (t instanceof WorkflowFailedException w && w.code().equals("WorkflowFailed")) return w.failure().toJson();
        if (t instanceof StepFailedException || t instanceof WorkflowFailedException || t instanceof FailureException) {
            return Failure.of(t).toJson();
        }
        return message(t);
    }

    private static final Pattern CAMEL = Pattern.compile("([a-z])([A-Z])");

    /** A code in the protocol's snake case ("NotFound": "not_found"). */
    static String kind(Throwable t) {
        return CAMEL.matcher(code(t)).replaceAll("$1_$2").toLowerCase(Locale.ROOT);
    }

    /** How an error ends a run or an update: a status. */
    static int status(Throwable t) {
        if (t instanceof CancelledException) return Native.S_CANCELLED;
        if (t instanceof TimedOutException) return Native.S_TIMED_OUT;
        if (t instanceof TerminatedException) return Native.S_TERMINATED;
        return Native.S_FAILED;
    }

    /** The text recorded with that status: a failure's envelope. */
    static String text(Throwable t) {
        if (t instanceof CancelledException c) return c.reason();
        if (status(t) == Native.S_FAILED) return Failure.of(t).toJson();
        return message(t);
    }

    /** A finished run's result as an exception, or null for success. */
    static FuseeException settled(int status, byte[] payload) {
        String msg = Codec.text(payload);
        switch (status) {
            case Native.S_OK:
                return null;
            case Native.S_CANCELLED:
                return new CancelledException(msg);
            case Native.S_TIMED_OUT:
                return new TimedOutException(msg);
            case Native.S_TERMINATED:
            case Native.S_CONTINUED:
            case Native.S_RESET:
                return new TerminatedException(msg);
            default:
                return new WorkflowFailedException("WorkflowFailed", null, Failure.parse(msg));
        }
    }

    /** The exception a blocked call ends with, unwrapped. */
    static RuntimeException unwrap(Throwable t) {
        while ((t instanceof ExecutionException || t instanceof CompletionException) && t.getCause() != null) {
            t = t.getCause();
        }
        if (t instanceof RuntimeException r) return r;
        if (t instanceof Error e) throw e;
        return new FuseeException("Failed", message(t), t);
    }

    static FuseeException interrupted() {
        Thread.currentThread().interrupt();
        return new FuseeException("Interrupted", "interrupted while waiting");
    }
}
