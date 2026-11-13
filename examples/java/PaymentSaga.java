// A payment saga in Java: each step that changes the world registers the
// step that undoes it; if the order fails or is cancelled, the compensations
// run, newest first. Run: java -cp <fusee classes> PaymentSaga.java
import dev.fusee.Engine;
import dev.fusee.FatalException;
import dev.fusee.Options;
import dev.fusee.RetryPolicy;
import dev.fusee.Run;
import dev.fusee.StartOptions;
import dev.fusee.StepOptions;
import dev.fusee.WorkflowDef;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

public class PaymentSaga {
    /** A short ID made from a step's idempotency key (as a service would): FNV-1a. */
    static String shortId(String key) {
        int h = 0x811c9dc5;
        for (byte b : key.getBytes(StandardCharsets.UTF_8)) h = (h ^ (b & 0xff)) * 0x01000193;
        return String.format("%08x", h);
    }

    @SuppressWarnings("rawtypes")
    static final WorkflowDef<Map, String> CHECKOUT = WorkflowDef.workflow("checkout", Map.class, String.class,
            (ctx, order) -> {
                String reservation = ctx.step("reserve", String.class,
                        s -> "res-" + order.get("sku") + "-" + shortId(s.key()));
                ctx.compensate("release", () -> {
                    System.out.println("  released " + reservation);
                    return null;
                });
                String charge = ctx.step("charge", String.class, s -> {
                    if (((Number) order.get("cents")).longValue() > 1_000_000) throw new FatalException("card declined");
                    return "ch-" + shortId(s.key());
                }, StepOptions.defaults().retry(RetryPolicy.attempts(5).initial(Duration.ofMillis(200))
                        .max(Duration.ofMinutes(1))));
                ctx.compensate("refund", () -> {
                    System.out.println("  refunded " + charge);
                    return null;
                });
                boolean shipped = ctx.waitFor("shipped", Boolean.class, Duration.ofDays(1)).orElse(false);
                if (!shipped) throw new FatalException("not shipped in a day");
                return charge;
            });

    public static void main(String[] args) {
        try (Engine e = Engine.open(Options.local("./data/payment-saga").workflows(CHECKOUT))) {
            Run<String> run = CHECKOUT.on(e).start("order-123", Map.of("sku", "SKU-9", "cents", 4200),
                    new StartOptions().ifRunning("use-existing").ifFinished("use-existing"));
            run.signal("shipped", true);
            System.out.println("charged: " + run.result());
        }
    }
}
