package dev.fusee;

/**
 * A workflow's code: it runs (and replays) under the engine, and must be
 * deterministic (see {@link Ctx}). Returning finishes the run with the value;
 * throwing fails it, or cancels it (a {@link CancelledException}), or times
 * it out (a {@link TimedOutException}).
 */
@FunctionalInterface
public interface WorkflowFn<I, O> {
    O run(Ctx ctx, I input) throws Exception;
}
