// A scheduled job in Java: a schedule starts the report workflow on a cron
// expression; each scheduled time runs once, even across restarts.
// Run: java -cp <fusee classes> ScheduledJob.java
import dev.fusee.Engine;
import dev.fusee.Options;
import dev.fusee.Schedule;
import dev.fusee.ScheduleOptions;
import dev.fusee.WorkflowDef;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduledJob {
    static final AtomicInteger SENT = new AtomicInteger();

    static final WorkflowDef<Object, Boolean> REPORT = WorkflowDef.workflow("report", Object.class, Boolean.class,
            (ctx, input) -> {
                String id = ctx.workflowId();
                ctx.step("send-report", Boolean.class, () -> {
                    System.out.println("  " + id + " sent");
                    return true;
                });
                SENT.incrementAndGet();
                return true;
            });

    public static void main(String[] args) throws Exception {
        try (Engine e = Engine.open(Options.local("./data/scheduled-job").workflows(REPORT))) {
            // A real job: ScheduleOptions.cron("0 2 * * *").timezone("Europe/London").
            Schedule s = e.schedule("nightly-report", "report", null,
                    ScheduleOptions.every(Duration.ofSeconds(1)).overlap("skip"));
            while (SENT.get() < 3) Thread.sleep(50);
            Object next = ((Map<?, ?>) s.describe().value()).get("nextMs");
            System.out.println("next: " + next);
            s.delete();
        }
    }
}
