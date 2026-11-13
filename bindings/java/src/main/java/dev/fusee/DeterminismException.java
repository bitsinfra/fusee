package dev.fusee;

/**
 * Code that made different journal operations than its journal records. The
 * run stops (status "stuck") until code that matches is deployed. Do not
 * catch it in workflow code: the engine has stopped the run.
 */
public final class DeterminismException extends FuseeException {
    private static final long serialVersionUID = 1L;

    public DeterminismException(String message) {
        super("Determinism", message);
    }
}
