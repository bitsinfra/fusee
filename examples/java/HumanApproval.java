// Human approval in Java: a workflow that asks a person, waits up to three
// days for the answer (durably: a crash meanwhile loses nothing), and acts on
// it. Run: java -cp <fusee classes> HumanApproval.java
import dev.fusee.Engine;
import dev.fusee.Options;
import dev.fusee.Run;
import dev.fusee.StartOptions;
import dev.fusee.WorkflowDef;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class HumanApproval {
    @SuppressWarnings("rawtypes")
    static final WorkflowDef<Map, String> EXPENSE = WorkflowDef.workflow("expense", Map.class, String.class,
            (ctx, claim) -> {
                ctx.step("ask", Boolean.class, () -> {
                    System.out.println("  asked finance to approve " + claim.get("cents") + " cents for " + claim.get("by"));
                    return true;
                });
                Optional<Map> answer = ctx.waitFor("decision", Map.class, Duration.ofDays(3));
                if (answer.isEmpty()) return "expired: nobody answered in three days";
                Map a = answer.get();
                if (!Boolean.TRUE.equals(a.get("approved"))) return "rejected by " + a.get("by");
                String paid = ctx.step("pay", String.class, () -> "paid " + claim.get("cents") + " cents");
                return paid + " (approved by " + a.get("by") + ")";
            });

    public static void main(String[] args) {
        try (Engine e = Engine.open(Options.local("./data/human-approval").workflows(EXPENSE))) {
            Run<String> run = EXPENSE.on(e).start("expense-7", Map.of("by", "ana", "cents", 12000),
                    new StartOptions().ifRunning("use-existing").ifFinished("use-existing"));
            run.signal("decision", Map.of("approved", true, "by", "dana"));
            System.out.println("result: " + run.result());
        }
    }
}
