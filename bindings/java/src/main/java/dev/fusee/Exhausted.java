package dev.fusee;

/** What a step does when its retries run out on a failure that could be retried. */
public enum Exhausted {
    /** The step fails, and with it the run unless the code catches it. */
    FAIL,
    /**
     * The run pauses: its parent and callers keep waiting until
     * {@link Run#resume()} runs the step again, with a fresh set of attempts,
     * on the code the process has then. A final failure still fails at once.
     */
    PAUSE
}
