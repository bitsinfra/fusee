package dev.fusee;

/** How a run is reset. */
public final class ResetOptions {
    boolean dropSignals;
    int version;

    public ResetOptions() {}

    /** Signals received after the cut are not delivered again (by default they are). */
    public ResetOptions dropSignals() {
        dropSignals = true;
        return this;
    }

    /** The new run's code version (0: the old run's). */
    public ResetOptions version(int v) {
        version = v;
        return this;
    }
}
