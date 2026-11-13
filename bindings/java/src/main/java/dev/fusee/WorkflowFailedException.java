package dev.fusee;

/**
 * A workflow's, a child's or an update's failure; {@link #failure()} is what
 * its code recorded (type, code, details and causes).
 */
public final class WorkflowFailedException extends FuseeException {
    private static final long serialVersionUID = 1L;
    private final transient Failure failure;
    private final String id;

    WorkflowFailedException(String code, String id, Failure failure) {
        super(code, (code.equals("ChildFailed") ? "child workflow" : "workflow")
                + (id == null ? "" : " '" + id + "'") + " failed: " + failure.message());
        this.failure = failure;
        this.id = id;
    }

    /** What the workflow recorded. */
    public Failure failure() {
        return failure;
    }

    /** The child's workflow ID, for a child's failure. */
    public String workflowId() {
        return id;
    }
}
