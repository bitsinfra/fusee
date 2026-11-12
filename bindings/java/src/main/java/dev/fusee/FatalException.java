package dev.fusee;

/** A failure no retry policy retries: a declined card, an invalid request. */
public class FatalException extends FailureException {
    private static final long serialVersionUID = 1L;

    public FatalException(String message) {
        super(message);
        retryable(false);
    }

    public FatalException(String message, Throwable cause) {
        super(message, cause);
        retryable(false);
    }
}
