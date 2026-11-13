package dev.fusee;

import java.time.Duration;

/** How a run starts. */
public final class StartOptions {
    long timeoutMs;
    String ifRunning = "", ifFinished = "";
    int version;
    String updateId = "";

    public StartOptions() {}

    /** The run's deadline from now. */
    public StartOptions timeout(Duration d) {
        timeoutMs = d.toMillis();
        return this;
    }

    /** When a run of the ID is running: "fail" (the default), "use-existing" or "terminate-existing". */
    public StartOptions ifRunning(String policy) {
        ifRunning = policy;
        return this;
    }

    /**
     * When the ID's last run finished: "allow" (the default),
     * "allow-if-failed", "refuse" or "use-existing".
     */
    public StartOptions ifFinished(String policy) {
        ifFinished = policy;
        return this;
    }

    /** The code version (0: the newest registered). */
    public StartOptions version(int v) {
        version = v;
        return this;
    }

    /** Update-with-start: the update's ID (repeating it gets the recorded result). */
    public StartOptions updateId(String id) {
        updateId = id;
        return this;
    }

    StartOptions copy() {
        StartOptions o = new StartOptions();
        o.timeoutMs = timeoutMs;
        o.ifRunning = ifRunning;
        o.ifFinished = ifFinished;
        o.version = version;
        o.updateId = updateId;
        return o;
    }

    static int policy(String running, String finished) {
        int r = switch (running == null ? "" : running) {
            case "", "fail" -> 0;
            case "use-existing" -> 1;
            case "terminate-existing" -> 2;
            default -> -1;
        };
        int f = switch (finished == null ? "" : finished) {
            case "", "allow" -> 0;
            case "allow-if-failed" -> 1;
            case "refuse" -> 2;
            case "use-existing" -> 3;
            default -> -1;
        };
        if (r < 0 || f < 0) throw Errors.invalid("bad ID policy '" + running + "' / '" + finished + "'");
        return r | (f << 2);
    }
}
