package dev.fusee;

/**
 * Cancellation reaching an operation. A workflow that throws one finishes as
 * cancelled, with its reason.
 */
public final class CancelledException extends FuseeException {
    private static final long serialVersionUID = 1L;
    private final String reason;

    public CancelledException(String reason) {
        super("Cancelled", "cancelled: " + reason);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
