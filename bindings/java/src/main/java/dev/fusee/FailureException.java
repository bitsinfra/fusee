package dev.fusee;

/**
 * Throw one from a step or a workflow to give its failure a type, a code and
 * details that reach the caller in any language (see {@link Failure}).
 * Subclass it to name the type by class: {@code class CardDeclined extends
 * FailureException}.
 */
public class FailureException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private String type;
    private String code;
    private transient Json details;
    private Boolean retryable;

    public FailureException(String message) {
        super(message);
    }

    public FailureException(String message, Throwable cause) {
        super(message, cause);
    }

    /** The failure's type (default: this class's simple name, unless it is a plain one). */
    public FailureException type(String type) {
        this.type = type;
        return this;
    }

    /** An application code, such as "card_declined". */
    public FailureException code(String code) {
        this.code = code;
        return this;
    }

    /** Details, encoded as JSON. */
    public FailureException details(Object details) {
        this.details = details instanceof Json j ? j : Json.from(details);
        return this;
    }

    /** {@code false}: no retry policy retries it. */
    public FailureException retryable(boolean retryable) {
        this.retryable = retryable;
        return this;
    }

    String type() {
        return type;
    }

    String failureCode() {
        return code;
    }

    Json details() {
        return details;
    }

    Boolean retryable() {
        return retryable;
    }

    /** This exception as recorded. */
    public Failure failure() {
        return Failure.of(this);
    }
}
