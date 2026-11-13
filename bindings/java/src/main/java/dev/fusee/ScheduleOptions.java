package dev.fusee;

import java.time.Duration;

/** When a schedule starts runs: a cron expression (in a time zone) or an interval. */
public final class ScheduleOptions {
    String cron;
    long everyMs, offsetMs, jitterMs, catchupWindowMs;
    String timezone = "UTC";
    String overlap = "skip";
    boolean paused;

    public ScheduleOptions() {}

    /** Five-field cron ("0 2 * * *") or @daily, @hourly, ... */
    public static ScheduleOptions cron(String expression) {
        ScheduleOptions o = new ScheduleOptions();
        o.cron = expression;
        return o;
    }

    /** A fixed interval. */
    public static ScheduleOptions every(Duration d) {
        ScheduleOptions o = new ScheduleOptions();
        o.everyMs = d.toMillis();
        return o;
    }

    public ScheduleOptions offset(Duration d) {
        offsetMs = d.toMillis();
        return this;
    }

    /** IANA time zone for cron (default UTC). */
    public ScheduleOptions timezone(String tz) {
        timezone = tz;
        return this;
    }

    /** "skip" (the default), "buffer-one", "buffer-all", "cancel-other", "terminate-other" or "allow-all". */
    public ScheduleOptions overlap(String policy) {
        overlap = policy;
        return this;
    }

    public ScheduleOptions jitter(Duration d) {
        jitterMs = d.toMillis();
        return this;
    }

    /** How far back missed times are caught up (default a year). */
    public ScheduleOptions catchupWindow(Duration d) {
        catchupWindowMs = d.toMillis();
        return this;
    }

    public ScheduleOptions paused(boolean p) {
        paused = p;
        return this;
    }
}
