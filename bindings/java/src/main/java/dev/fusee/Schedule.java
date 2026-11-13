package dev.fusee;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** A schedule, addressed by name: built-in code that starts runs of a workflow. */
public final class Schedule {
    static final String TYPE = "fusee.schedule";
    static final String CONTROL = "fusee.schedule.control";

    private final Engine e;
    private final String name;
    private final String id;

    Schedule(Engine e, String name) {
        this.e = e;
        this.name = name;
        this.id = "fusee.schedule/" + name;
    }

    public String name() {
        return name;
    }

    /** The schedule's workflow ID. */
    public String id() {
        return id;
    }

    static Schedule create(Engine e, String name, String workflow, Object input, ScheduleOptions o) {
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("cron", o.cron == null || o.cron.isEmpty() ? null : o.cron);
        spec.put("everyMs", o.everyMs > 0 ? o.everyMs : null);
        spec.put("offsetMs", o.offsetMs);
        spec.put("timezone", o.timezone == null || o.timezone.isEmpty() ? "UTC" : o.timezone);
        spec.put("overlap", o.overlap == null || o.overlap.isEmpty() ? "skip" : o.overlap);
        spec.put("jitterMs", o.jitterMs);
        spec.put("catchupWindowMs", o.catchupWindowMs > 0 ? o.catchupWindowMs : 365L * 86_400_000L);
        spec.put("paused", o.paused);
        spec.put("workflow", workflow);
        spec.put("input", input instanceof Json || input == null ? input : Json.of(Codec.raw(e.codec.encode(input))));
        byte[] b = MiniJson.INSTANCE.encode(spec);
        byte[] problem = Native.validateSchedule(b);
        if (problem != null) throw Errors.invalid(Codec.utf8(problem));
        Schedule s = new Schedule(e, name);
        Json specJson = Json.of(Codec.utf8(b));
        Map<String, Object> start = new LinkedHashMap<>();
        start.put("name", name);
        start.put("spec", specJson);
        e.startWire(TYPE, 1, s.id, MiniJson.INSTANCE.encode(start),
                new StartOptions().ifRunning("use-existing").version(1));
        s.send(Map.of("op", "update", "spec", specJson));
        return s;
    }

    private void send(Map<String, Object> cmd) {
        e.signalWire(id, CONTROL, MiniJson.INSTANCE.encode(cmd));
    }

    /** Stops starting runs until resume. */
    public void pause() {
        send(Map.of("op", "pause"));
    }

    /** Starts runs again. */
    public void resume() {
        send(Map.of("op", "resume"));
    }

    /** Starts one run now. */
    public void trigger() {
        send(Map.of("op", "trigger"));
    }

    /** Starts a run for every scheduled time in [from, to]. */
    public void backfill(Instant from, Instant to) {
        send(Map.of("op", "backfill", "fromMs", from.toEpochMilli(), "toMs", to.toEpochMilli()));
    }

    /** The schedule's spec and state. */
    public Json describe() {
        return Json.of(Codec.raw(e.queryNativeWire(id, "describe", Codec.utf8("null"))));
    }

    /** Stops the schedule; runs it started keep running. */
    public void delete() {
        e.cancelWire(id, "schedule deleted", true);
    }
}
