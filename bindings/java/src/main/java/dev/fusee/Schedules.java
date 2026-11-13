package dev.fusee;

import java.util.ArrayList;
import java.util.List;

/** An engine's schedules (Engine.schedules()). */
public final class Schedules {
    private static final String PREFIX = "fusee.schedule/";
    private final Engine e;

    Schedules(Engine e) {
        this.e = e;
    }

    /** A handle to the schedule {@code name}. */
    public Schedule get(String name) {
        return new Schedule(e, name);
    }

    /** Every schedule that exists (its coordinator run has not ended). */
    public List<Schedule> list() {
        List<Schedule> out = new ArrayList<>();
        for (RunSummary r : e.list()) {
            if (r.id().startsWith(PREFIX) && r.finishedMs() == null) {
                out.add(new Schedule(e, r.id().substring(PREFIX.length())));
            }
        }
        return out;
    }
}
