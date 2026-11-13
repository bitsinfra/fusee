package dev.fusee;

import java.time.Duration;

/** A debounced start. */
public final class DebounceOptions {
    long windowMs, maxWaitMs, dedupWindowMs;
    String id = "";

    public DebounceOptions() {}

    /**
     * Starts inside the window (after the latest start) collapse into one
     * run, which begins with the latest start's input. Required.
     */
    public static DebounceOptions window(Duration d) {
        DebounceOptions o = new DebounceOptions();
        o.windowMs = d.toMillis();
        return o;
    }

    /** The run begins at most this long after the first start (default: no bound). */
    public DebounceOptions maxWait(Duration d) {
        maxWaitMs = d.toMillis();
        return this;
    }

    /** How long an event ID deduplicates after the key's last event (default ten minutes). */
    public DebounceOptions dedupWindow(Duration d) {
        dedupWindowMs = d.toMillis();
        return this;
    }

    /** The event's ID: a retried call with the same ID joins once. */
    public DebounceOptions id(String eventId) {
        id = eventId == null ? "" : eventId;
        return this;
    }

    DebounceOptions copy() {
        DebounceOptions o = new DebounceOptions();
        o.windowMs = windowMs;
        o.maxWaitMs = maxWaitMs;
        o.dedupWindowMs = dedupWindowMs;
        o.id = id;
        return o;
    }
}
