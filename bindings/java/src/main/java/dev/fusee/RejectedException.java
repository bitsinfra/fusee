package dev.fusee;

/**
 * An update a validator refused (nothing was recorded), a stale completion
 * token, or a query or update with no handler.
 */
public final class RejectedException extends FuseeException {
    private static final long serialVersionUID = 1L;
    private final String reason;

    public RejectedException(String reason) {
        super("Rejected", "rejected: " + reason);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
