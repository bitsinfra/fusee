package dev.fusee;

/** A run that was terminated (or replaced). */
public final class TerminatedException extends FuseeException {
    private static final long serialVersionUID = 1L;

    public TerminatedException(String message) {
        super("Terminated", message == null || message.isEmpty() ? "terminated" : message);
    }
}
