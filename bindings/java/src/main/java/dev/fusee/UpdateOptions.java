package dev.fusee;

/** How an update is sent. */
public final class UpdateOptions {
    String id = "";

    public UpdateOptions() {}

    /** The update's ID: a retried call with the same ID gets the recorded result. */
    public static UpdateOptions id(String id) {
        UpdateOptions o = new UpdateOptions();
        o.id = id == null ? "" : id;
        return o;
    }
}
