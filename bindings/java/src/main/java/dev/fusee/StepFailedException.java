package dev.fusee;

/** A step's final failure, after its last attempt. */
public final class StepFailedException extends FuseeException {
    private static final long serialVersionUID = 1L;
    private final String step;
    private final String detail;
    private final transient Failure failure;

    public StepFailedException(String step, String detail) {
        this(step, new Failure(detail, null, null, null, null, null, null));
    }

    StepFailedException(String step, Failure failure) {
        super("StepFailed", "step '" + step + "' failed: " + failure.message());
        this.step = step;
        this.detail = failure.message();
        this.failure = failure;
    }

    /** What the step's function failed with: its type, code and details. */
    public Failure failure() {
        return failure;
    }

    public String step() {
        return step;
    }

    /** The last attempt's error message. */
    public String detail() {
        return detail;
    }
}
