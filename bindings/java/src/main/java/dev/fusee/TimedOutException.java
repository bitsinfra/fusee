package dev.fusee;

/** A step's, wait's or workflow's timeout. A workflow that throws one finishes as timed out. */
public final class TimedOutException extends FuseeException {
    private static final long serialVersionUID = 1L;

    public TimedOutException(String message) {
        super("TimedOut", message == null || message.isEmpty() ? "timed out" : message);
    }
}
