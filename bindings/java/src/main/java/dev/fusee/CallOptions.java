package dev.fusee;

/** A call to a key of an object type from outside workflow code. */
public final class CallOptions {
    String id = "";

    public CallOptions() {}

    /**
     * An idempotency ID: calls with the same ID run once, and each gets the
     * recorded result (a key remembers them across compaction for an hour,
     * its latest 1,024). Without one a retry is a new call.
     */
    public static CallOptions id(String id) {
        CallOptions o = new CallOptions();
        o.id = id == null ? "" : id;
        return o;
    }
}
