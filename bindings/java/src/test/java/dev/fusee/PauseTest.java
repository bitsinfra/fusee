package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** A step whose retries run out pauses its run; resume runs it again with a fresh set of attempts. */
class PauseTest {
    static final AtomicBoolean BROKEN = new AtomicBoolean(true);
    static final AtomicInteger CALLS = new AtomicInteger();

    static final WorkflowDef<Integer, Integer> PAY = WorkflowDef.workflow("pay", Integer.class, Integer.class,
            (ctx, n) -> ctx.step("charge", Integer.class, () -> {
                CALLS.incrementAndGet();
                if (BROKEN.get()) throw new RuntimeException("payments are down");
                return n * 10;
            }, StepOptions.defaults().inline().retry(RetryPolicy.attempts(2).initial(Duration.ofMillis(1))
                    .onExhausted(Exhausted.PAUSE))) + 1);

    static final WorkflowDef<Integer, Integer> ORDER = WorkflowDef.workflow("order", Integer.class, Integer.class,
            (ctx, n) -> ctx.child("pay", "pay-1", n, Integer.class));

    static String status(Engine e, String id) {
        try {
            return e.run(id).inspect().status();
        } catch (RuntimeException err) {
            return "";
        }
    }

    @Test
    void aPausedChildKeepsItsParentWaitingUntilResumed() throws Exception {
        BROKEN.set(true);
        CALLS.set(0);
        try (Engine e = Engine.open(Options.memory().shards(2).keepHistory(true).workflows(PAY, ORDER))) {
            List<String> paused = new ArrayList<>();
            e.on("paused", (id, msg) -> paused.add(id + ": " + msg));
            Run<Integer> run = ORDER.on(e).start("order-1", 4);
            long deadline = System.currentTimeMillis() + 10_000;
            while (!status(e, "pay-1").equals("paused")) {
                assertTrue(System.currentTimeMillis() < deadline, "no pause: " + status(e, "pay-1"));
                Thread.sleep(5);
            }
            Thread.sleep(50);
            assertEquals(2, CALLS.get(), "nothing retries while paused");
            assertEquals(List.of("pay-1: step 'charge' ran out of retries: payments are down"), paused);
            BROKEN.set(false);
            e.run("pay-1").resume();
            assertEquals(41, run.result());
            assertEquals(3, CALLS.get());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> journal = (List<Map<String, Object>>) e.run("pay-1").inspect().journal().value();
            List<Object> kinds = journal.stream().map(r -> r.get("kind")).toList();
            assertEquals(List.of("start", "attempt", "attempt", "resume", "step", "done"), kinds);
            assertThrows(RejectedException.class, () -> e.run("pay-1").resume());
        }
    }
}
