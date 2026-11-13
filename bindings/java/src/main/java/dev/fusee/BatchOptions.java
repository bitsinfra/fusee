package dev.fusee;

import java.time.Duration;

/** A batched event. */
public final class BatchOptions {
    int maxEvents;
    long maxWaitMs, dedupWindowMs;
    String id = "";

    public BatchOptions() {}

    /** The batch closes at this many events (default 100, at most 10,000). */
    public BatchOptions maxEvents(int n) {
        maxEvents = n;
        return this;
    }

    /** ... or this long after its first event (default one second). */
    public BatchOptions maxWait(Duration d) {
        maxWaitMs = d.toMillis();
        return this;
    }

    public BatchOptions dedupWindow(Duration d) {
        dedupWindowMs = d.toMillis();
        return this;
    }

    /** The event's ID: a retried call with the same ID adds the event once. */
    public BatchOptions id(String eventId) {
        id = eventId == null ? "" : eventId;
        return this;
    }
}
