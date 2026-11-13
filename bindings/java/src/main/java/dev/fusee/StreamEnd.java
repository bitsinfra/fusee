package dev.fusee;

/**
 * How a stream ended.
 *
 * @param status "ok" for a stream its writer closed or whose step succeeded;
 *     the step's status ("failed", "cancelled") when its step ended
 *     otherwise; the run's status when the run ended with the stream open
 * @param closed a writer or a step completed it (rather than the run's end)
 */
public record StreamEnd(String status, boolean closed) {}
