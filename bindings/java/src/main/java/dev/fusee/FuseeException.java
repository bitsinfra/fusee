package dev.fusee;

/**
 * A failure the engine or the SDK reported. {@link #code()} is one of
 * "Failed", "Busy", "NotFound", "AlreadyExists", "UnknownWorkflow", "Io",
 * "Closed", "Invalid", "Panic" or "Interrupted", or, for the subclasses,
 * "Step", "Cancelled", "TimedOut", "Terminated", "Determinism" and
 * "Rejected" (the names the other SDKs use).
 */
public class FuseeException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String code;

    public FuseeException(String code, String message) {
        super(message);
        this.code = code;
    }

    FuseeException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
