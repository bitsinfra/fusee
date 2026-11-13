package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class BasicsTest {
    Engine e;

    record Order(int n) {}

    @BeforeEach
    void open() {
        e = Engine.open(Options.memory().shards(2).keepHistory(true));
    }

    @AfterEach
    void close() {
        e.close();
    }

    @Test
    void steps() {
        Workflow<Order, Map<String, Integer>> wf = e.register("seq", TypeRef.of(Order.class),
                new TypeRef<Map<String, Integer>>() {}, (ctx, in) -> {
                    int a = ctx.step("a", Integer.class, () -> in.n() + 1);
                    int b = ctx.step("b", Integer.class, () -> a * 2);
                    return ctx.step("c", new TypeRef<Map<String, Integer>>() {}, () -> {
                        Map<String, Integer> m = new LinkedHashMap<>();
                        m.put("a", a);
                        m.put("b", b);
                        return m;
                    });
                });
        Run<Map<String, Integer>> run = wf.start("seq-1", new Order(3));
        Map<String, Integer> out = run.result();
        assertEquals(4, out.get("a"));
        assertEquals(8, out.get("b"));
        RunDetail d = run.inspect();
        assertNotNull(d);
        assertEquals("ok", d.status());
        assertTrue(d.journal().text().contains("\"name\":\"b\""), d.journal().text());
        assertNull(e.run("nope").inspect());
    }

    private static void assertNull(Object o) {
        assertEquals(null, o);
    }

    @Test
    void retriesAndFailure() {
        AtomicInteger tries = new AtomicInteger();
        RetryPolicy r3 = RetryPolicy.attempts(3).initial(Duration.ofMillis(1)).max(Duration.ofMillis(1));
        Workflow<Object, String> wf = e.register("retry", Object.class, String.class, (ctx, in) -> {
            String v = ctx.step("flaky", String.class, () -> {
                int n = tries.incrementAndGet();
                if (n < 3) throw new IllegalStateException("try " + n);
                return "ok";
            }, StepOptions.defaults().retry(r3));
            StepFailedException se = assertThrows(StepFailedException.class, () -> ctx.step("boom", Integer.class,
                    () -> {
                        throw new java.io.IOException("boom");
                    }, StepOptions.defaults().retry(RetryPolicy.none())));
            assertEquals("boom", se.detail());
            assertEquals("boom", se.step());
            return v;
        });
        assertEquals("ok", wf.start("retry-1", null).result());
        assertEquals(3, tries.get());
        String journal = e.run("retry-1").inspect().journal().text();
        assertTrue(journal.contains("try 1") && journal.contains("try 2"), journal);
    }

    @Test
    void signalsSleepsChildrenRacesAsync() {
        e.register("double", Integer.class, Integer.class, (ctx, x) -> ctx.step("mul", Integer.class, () -> x * 2));
        Workflow<Object, List<Object>> wf = e.register("main", TypeRef.of(Object.class),
                new TypeRef<List<Object>>() {}, (ctx, in) -> {
                    ctx.sleep("nap", Duration.ofMillis(1));
                    int child = ctx.child("double", ctx.workflowId() + "/c", 21, Integer.class);
                    String v = ctx.waitFor("go", String.class);
                    // The signal "x" never comes: the timer wins and the wait is dropped.
                    Op<String> w = ctx.startWait("x", String.class);
                    int i = ctx.race(w, ctx.startSleep("short", Duration.ofMillis(1)));
                    boolean late = ctx.waitFor("late", String.class, Duration.ofMillis(5)).isPresent();
                    Op<Integer> fx = ctx.startStep("x", Integer.class, () -> 1);
                    Op<Integer> fy = ctx.startStep("y", Integer.class, () -> 2);
                    ctx.all(fx, fy);
                    Op<Integer> g = ctx.async(() -> ctx.step("in-async", Integer.class, () -> 7));
                    int z = g.get();
                    List<Object> out = new ArrayList<>();
                    out.add(child);
                    out.add(v);
                    out.add(i);
                    out.add(late);
                    out.add(fx.get() + fy.get());
                    out.add(z);
                    out.add(ctx.random() < 1);
                    out.add(ctx.uuid().toString().length());
                    out.add(ctx.now().toEpochMilli() > 946684800000L);
                    return out;
                });
        Run<List<Object>> run = wf.start("main-1", null);
        run.signal("go", "hi");
        assertEquals(List.of(42, "hi", 1, false, 3, 7, true, 36, true), run.result());
        assertEquals(42, e.run("main-1/c", Integer.class).result());
    }

    @Test
    void asyncCodeAndAllFailures() {
        Workflow<Object, String> wf = e.register("fan", Object.class, String.class, (ctx, in) -> {
            List<Op<Integer>> ops = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                int k = i;
                ops.add(ctx.async(() -> {
                    ctx.sleep("s" + k, Duration.ofMillis(1 + k));
                    return ctx.step("v" + k, Integer.class, () -> k * k);
                }));
            }
            ctx.all(ops);
            int sum = 0;
            for (Op<Integer> o : ops) sum += o.get();
            Op<Integer> bad = ctx.startStep("bad", Integer.class, () -> {
                throw new IllegalArgumentException("nope");
            }, StepOptions.defaults().retry(RetryPolicy.none()));
            Op<Void> slow = ctx.startSleep("slow", Duration.ofSeconds(30));
            StepFailedException se = assertThrows(StepFailedException.class, () -> ctx.all(bad, slow));
            return sum + ":" + se.detail();
        });
        assertEquals("30:nope", wf.start("fan-1", null).result());
    }

    @Test
    void workflowFailuresAndMisuse() {
        Ctx[] leaked = new Ctx[1];
        Workflow<Object, Integer> broken = e.register("broken", Object.class, Integer.class, (ctx, in) -> {
            leaked[0] = ctx;
            throw new IllegalStateException("workflow bug");
        });
        FuseeException err = assertThrows(FuseeException.class, () -> broken.start("broken-1", null).result());
        assertTrue(err.getMessage().contains("workflow bug"), err.getMessage());
        assertEquals("WorkflowFailed", err.code());
        assertThrows(IllegalStateException.class, () -> leaked[0].now());
        Workflow<Object, Object> calls = e.register("calls", Object.class, Object.class, (ctx, in) -> {
            IllegalStateException ise = assertThrows(IllegalStateException.class, () -> e.run("x").signal("a", 1));
            return ise.getMessage().contains("workflow code");
        });
        assertEquals(true, calls.start("calls-1", null).result());
        FuseeException nf = assertThrows(FuseeException.class, () -> e.run("nope").result());
        assertEquals("NotFound", nf.code());
    }

    @Test
    void rawBytes() {
        Workflow<byte[], byte[]> wf = e.register("bytes", byte[].class, byte[].class, (ctx, in) ->
                ctx.step("rev", byte[].class, () -> {
                    byte[] b = in.clone();
                    for (int i = 0, j = b.length - 1; i < j; i++, j--) {
                        byte t = b[i];
                        b[i] = b[j];
                        b[j] = t;
                    }
                    return b;
                }));
        byte[] out = wf.start("b-1", new byte[] {0, 1, 2, (byte) 255}).result();
        assertEquals(4, out.length);
        assertEquals((byte) 255, out[0]);
        assertEquals(0, out[3]);
        String j = e.run("b-1").inspect().journal().text();
        assertTrue(j.contains("\"kind\":\"start\""), j);
        Object untyped = e.run("b-1").result();
        assertInstanceOf(byte[].class, untyped);
    }

    /** Payloads past the 64 KB buffers the step path shares with the native side. */
    @Test
    void largePayloads() {
        String big = "x".repeat(300_000) + "é";
        Workflow<String, String> wf = e.register("big", String.class, String.class, (ctx, in) -> {
            String a = ctx.step("a", String.class, () -> in + "a");
            String b = ctx.step("b", String.class, () -> a + "b", StepOptions.defaults().inline());
            String s = ctx.waitFor("s", String.class);
            String c = ctx.child("echo-big", ctx.workflowId() + "/c", s, String.class);
            return b.length() + ":" + c.length() + ":" + c.endsWith("é");
        });
        e.register("echo-big", String.class, String.class, (ctx, in) -> ctx.step("v", String.class, () -> in));
        Run<String> run = wf.start("big-1", big);
        run.signal("s", big);
        assertEquals((big.length() + 2) + ":" + big.length() + ":true", run.result());
        assertEquals(big, e.run("big-1/c", String.class).result());
    }

    @Test
    void inlineStepsAndKeys() {
        Workflow<Integer, List<String>> wf = e.register("inline", TypeRef.of(Integer.class),
                new TypeRef<List<String>>() {}, (ctx, n) -> {
                    List<String> state = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        int k = i;
                        state.add(ctx.step("s" + i, String.class, () -> "v" + k + ":" + ctx.key().isEmpty(),
                                StepOptions.defaults().inline()));
                    }
                    String own = ctx.step("own", String.class, ctx::key);
                    String again = ctx.step("own2", String.class, ctx::key);
                    assertFalse(own.isEmpty());
                    assertFalse(own.equals(again));
                    // ctx.key() outside a step stops the run (there is no
                    // step to key): see docs/errors.md, "key() outside a step".
                    return state;
                });
        List<String> out = wf.start("inline-1", 50).result();
        assertEquals(50, out.size());
        assertEquals("v49:false", out.get(49));
    }
}
