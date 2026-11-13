package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** The failure envelope, the step context, definitions apart from the engine and the shared client verbs. */
class FailuresTest {
    static final class CardDeclined extends FatalException {
        private static final long serialVersionUID = 1L;

        CardDeclined(String last4) {
            super("card declined");
            code("card_declined").details(Map.of("last4", last4));
        }
    }

    static final class Flaky extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Flaky() {
            super("try again");
        }
    }

    static final AtomicInteger TRIES = new AtomicInteger();

    static final WorkflowDef<String, Integer> PAY = WorkflowDef.workflow("pay", String.class, Integer.class,
            (ctx, kind) -> ctx.step("charge", Integer.class, s -> {
                TRIES.incrementAndGet();
                if (kind.equals("fatal")) throw new CardDeclined("4242");
                if (kind.equals("flaky")) throw new Flaky();
                return (int) s.attempt();
            }, StepOptions.defaults().retry(RetryPolicy.attempts(3).initial(Duration.ofMillis(1))
                    .max(Duration.ofMillis(1)).nonRetryable("NeverThis"))));

    @Test
    void failuresTravelAsTheEnvelope() throws Exception {
        try (Engine e = Engine.open(Options.memory().shards(1).workflows(PAY))) {
            TRIES.set(0);
            Run<Integer> fatal = PAY.on(e).start("p-1", "fatal");
            WorkflowFailedException wf = assertThrows(WorkflowFailedException.class, fatal::result);
            Failure f = wf.failure();
            assertEquals("StepFailed", f.type());
            assertEquals("CardDeclined", f.cause().type());
            assertEquals("card_declined", f.cause().code());
            assertEquals(Map.of("last4", "4242"), f.cause().details().value());
            assertEquals(1, TRIES.get(), "a fatal failure is not retried");

            TRIES.set(0);
            Run<Integer> flaky = PAY.on(e).start("p-2", "flaky");
            WorkflowFailedException wf2 = assertThrows(WorkflowFailedException.class, flaky::result);
            assertEquals("Flaky", wf2.failure().cause().type());
            assertEquals(3, TRIES.get(), "a plain failure is retried under the policy");

            // The step context: the attempt (and its key) inside the function.
            assertEquals(1, PAY.on(e).start("p-3", "ok").result());
        }
    }

    @Test
    void nonRetryableStopsRetries() throws Exception {
        try (Engine e = Engine.open(Options.memory().shards(1))) {
            AtomicInteger tries = new AtomicInteger();
            Workflow<String, Integer> w = e.register("once", String.class, Integer.class,
                    (ctx, x) -> ctx.step("s", Integer.class, () -> {
                        tries.incrementAndGet();
                        throw new Flaky();
                    }, StepOptions.defaults().retry(RetryPolicy.attempts(5).initial(Duration.ofMillis(1))
                            .nonRetryable("Flaky"))));
            assertThrows(WorkflowFailedException.class, () -> w.start("o-1", "x").result());
            assertEquals(1, tries.get());
        }
    }

    @Test
    void theEngineHasTheClientsVerbs() throws Exception {
        try (Engine e = Engine.open(Options.memory().shards(1))) {
            e.register("echo", String.class, String.class, (ctx, s) -> {
                return s + ctx.waitFor("more", String.class, Duration.ofSeconds(10)).orElse("");
            });
            Run<Object> r = e.signalWithStart("echo", "e-1", "a", "more", "b");
            assertEquals("ab", r.result());
            Run<Object> r2 = e.start("echo", "e-2", "x");
            r2.signal("more", "y");
            assertEquals("xy", r2.result());
            assertTrue(e.list().size() >= 2);
        }
    }
}
