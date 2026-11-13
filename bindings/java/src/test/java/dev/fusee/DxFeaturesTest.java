package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Compensation, signal-with-start, limits() and metrics(). */
class DxFeaturesTest {
    @Test
    void compensationsRunNewestFirstOnFailure() throws Exception {
        try (Engine e = Engine.open(Options.memory().shards(1))) {
            List<String> undone = Collections.synchronizedList(new ArrayList<>());
            Workflow<Boolean, String> trip = e.register("trip", Boolean.class, String.class, (ctx, fail) -> {
                for (String what : List.of("flight", "hotel")) {
                    ctx.step("book " + what, Integer.class, () -> 1);
                    ctx.compensate("cancel " + what, () -> undone.add(what));
                }
                ctx.step("pay", Integer.class, () -> {
                    throw new RuntimeException("card declined");
                }, StepOptions.defaults().retry(RetryPolicy.none()));
                return "paid";
            });
            Run<String> run = trip.start("t-1", true);
            assertThrows(RuntimeException.class, run::result);
            assertEquals(List.of("hotel", "flight"), undone);
        }
    }

    @Test
    void signalWithStartLimitsAndMetrics() throws Exception {
        try (Engine e = Engine.open(Options.memory().shards(1))) {
            @SuppressWarnings("unchecked")
            Workflow<List, List> cart = e.register("cart", List.class, List.class, (ctx, items) -> {
                List<Object> out = new ArrayList<>(items);
                while (out.size() < 2) {
                    out.add(ctx.waitFor("add", String.class));
                }
                return out;
            });
            Run<List> run = cart.signalWithStart("cart-1", List.of(), "add", "apple");
            cart.signalWithStart("cart-1", List.of(), "add", "pear");
            assertEquals(List.of("apple", "pear"), run.result());
            e.limit("stripe", LimitOptions.concurrent(2));
            List<Engine.LimitInfo> ls = e.limits();
            assertEquals(1, ls.size());
            assertEquals("stripe", ls.get(0).name());
            assertEquals(2, ls.get(0).concurrent());
            assertTrue(e.metrics().getOrDefault("runs_started", 0L) >= 1, e.metrics().toString());
            e.schedule("nightly", "cart", List.of(), ScheduleOptions.cron("0 3 * * *"));
            List<Schedule> all = e.schedules().list();
            assertEquals(1, all.size());
            assertEquals("nightly", all.get(0).name());
            assertThrows(RejectedException.class, () -> e.on("nope", (id, m) -> {}));
            e.on("stuck", (id, m) -> {});
        }
    }
}
