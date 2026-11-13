package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class FeaturesTest {
    Engine e;

    @BeforeEach
    void open() {
        e = Engine.open(Options.memory().shards(2).keepHistory(true));
    }

    @AfterEach
    void close() {
        e.close();
    }

    static RunDetail waitStatus(Engine e, String id, String... statuses) {
        for (int i = 0; i < 2000; i++) {
            RunDetail d = e.run(id).inspect();
            if (d != null) {
                for (String s : statuses) {
                    if (s.equals(d.status())) return d;
                }
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
        throw new AssertionError(id + " never reached " + List.of(statuses) + ": " + e.run(id).inspect());
    }

    @Test
    void updatesQueriesAndUpdateWithStart() {
        Workflow<Integer, Integer> counter = e.register("counter", Integer.class, Integer.class, (ctx, start) -> {
            int[] n = {start};
            ctx.onUpdate("add", Integer.class, by -> {
                int d = ctx.step("apply", Integer.class, () -> by);
                n[0] += d;
                return n[0];
            }, by -> {
                if (by < 0) throw new IllegalArgumentException("negative");
            });
            ctx.onQuery("n", () -> n[0]);
            ctx.onQuery("plus", Integer.class, k -> n[0] + k);
            ctx.waitFor("stop", Object.class);
            return n[0];
        });
        Workflow.Started<Integer, Integer> st = counter.updateWithStart("c-1", 10, "add", 5, Integer.class,
                new StartOptions().updateId("u1"));
        assertEquals(15, st.result());
        Run<Integer> run = st.run();
        assertEquals(15, run.update("add", 5, Integer.class, UpdateOptions.id("u1")));
        RejectedException rj = assertThrows(RejectedException.class, () -> run.update("add", -1, Integer.class));
        assertEquals("negative", rj.reason());
        assertThrows(RejectedException.class, () -> run.update("nope", 1, Integer.class));
        assertEquals(15, run.query("n", Integer.class));
        assertEquals(17, run.query("plus", 2, Integer.class));
        assertThrows(RejectedException.class, () -> run.query("nope", Integer.class));
        run.signal("stop", null);
        assertEquals(15, run.result());
    }

    @Test
    void cancelTerminateAndStartPolicies() {
        AtomicBoolean undone = new AtomicBoolean();
        Workflow<Object, String> wf = e.register("waiter", Object.class, String.class, (ctx, in) -> {
            try {
                return ctx.waitFor("never", String.class);
            } catch (CancelledException ce) {
                ctx.step("undo", Boolean.class, () -> {
                    undone.set(true);
                    return true;
                });
                throw new CancelledException("undone: " + ce.reason());
            }
        });
        Run<String> run = wf.start("w-1", null);
        assertThrows(FuseeException.class, () -> wf.start("w-1", null));
        wf.start("w-1", null, new StartOptions().ifRunning("use-existing"));
        waitStatus(e, "w-1", "suspended", "parked");
        run.cancel("stop");
        CancelledException ce = assertThrows(CancelledException.class, run::result);
        assertEquals("undone: stop", ce.reason());
        assertTrue(undone.get());
        Run<String> run2 = wf.start("w-2", null);
        waitStatus(e, "w-2", "suspended", "parked");
        run2.terminate("enough");
        assertThrows(TerminatedException.class, run2::result);
        // Refused after it finished:
        assertThrows(FuseeException.class, () -> wf.start("w-2", null, new StartOptions().ifFinished("refuse")));
        // A workflow timeout.
        Run<String> run3 = wf.start("w-3", null, new StartOptions().timeout(Duration.ofMillis(30)));
        assertThrows(TimedOutException.class, run3::result);
    }

    @Test
    void continueAsNewPatchesAttributesAndList() {
        Workflow<Integer, Integer> wf = e.register("loop", Integer.class, Integer.class, (ctx, left) -> {
            if (ctx.patched("p1")) ctx.deprecatePatch("p0");
            ctx.setAttributes(Map.of("left", left, "kind", "loop"));
            int v = ctx.step("work", Integer.class, () -> left);
            if (left == 0) return v;
            return ctx.continueAsNew(left - 1);
        });
        assertEquals(0, wf.start("loop-1", 3).result());
        List<RunSummary> runs = e.list(ListOptions.where("kind", "loop").and("status", "ok"));
        assertEquals(1, runs.size(), runs.toString());
        assertEquals("loop-1", runs.get(0).id());
        assertEquals(0, ((Number) runs.get(0).attributes().get("left")).intValue());
        assertTrue(e.list(ListOptions.where("left", Map.of("gt", 5))).isEmpty());
        assertFalse(e.list().isEmpty());
    }

    @Test
    void limitsTimeoutsHeartbeats() {
        e.limit("one", LimitOptions.concurrent(1));
        AtomicInteger inside = new AtomicInteger(), most = new AtomicInteger();
        Workflow<Integer, Integer> limited = e.register("limited", Integer.class, Integer.class, (ctx, i) ->
                ctx.step("work", Integer.class, () -> {
                    int n = inside.incrementAndGet();
                    most.accumulateAndGet(n, Math::max);
                    Thread.sleep(2);
                    inside.decrementAndGet();
                    return i;
                }, StepOptions.defaults().limit("one").priority(i).fairness("k")));
        List<Run<Integer>> runs = new ArrayList<>();
        for (int i = 0; i < 5; i++) runs.add(limited.start("lim-" + i, i));
        for (int i = 0; i < 5; i++) assertEquals(i, runs.get(i).result());
        assertEquals(1, most.get());
        assertEquals(0, e.limitInUse("one"));
        assertEquals(0, e.limitWaiting("one"));

        RetryPolicy one = RetryPolicy.none();
        Workflow<Object, String> slow = e.register("slow", Object.class, String.class, (ctx, in) -> {
            StepFailedException t = assertThrows(StepFailedException.class, () -> ctx.step("late", Integer.class, () -> {
                Thread.sleep(10_000);
                return 0;
            }, StepOptions.defaults().timeout(Duration.ofMillis(20)).retry(one)));
            assertTrue(t.getMessage().contains("timed out after 20 ms"), t.getMessage());
            StepFailedException hb = assertThrows(StepFailedException.class, () -> ctx.step("silent", Integer.class,
                    () -> {
                        ctx.heartbeat("started");
                        Thread.sleep(10_000);
                        return 0;
                    }, StepOptions.defaults().heartbeatTimeout(Duration.ofMillis(20)).retry(one)));
            assertTrue(hb.getMessage().contains("no heartbeat"), hb.getMessage());
            int progress = ctx.step("beats", Integer.class, () -> {
                for (int i = 1; i <= 3; i++) {
                    ctx.heartbeat(i);
                    Thread.sleep(5);
                }
                return ctx.progress(Integer.class).orElse(-1);
            }, StepOptions.defaults().heartbeatTimeout(Duration.ofMillis(500)));
            assertEquals(3, progress);
            return "ok";
        });
        assertEquals("ok", slow.start("slow-1", null).result());
    }

    @Test
    void completionTokensSchedulesQueues() throws Exception {
        BlockingQueue<String> tokens = new ArrayBlockingQueue<>(2);
        Workflow<Object, String> approval = e.register("approval", Object.class, String.class,
                (ctx, in) -> ctx.awaitCompletion("decision", String.class, tokens::put));
        Run<String> run = approval.start("ap-1", null);
        String token = tokens.poll(10, TimeUnit.SECONDS);
        assertNotNull(token);
        waitStatus(e, "ap-1", "suspended", "parked");
        e.complete(token, "yes");
        assertEquals("yes", run.result());
        assertThrows(RejectedException.class, () -> e.complete("not a token", 1));
        Run<String> run2 = approval.start("ap-2", null);
        String t2 = tokens.poll(10, TimeUnit.SECONDS);
        waitStatus(e, "ap-2", "suspended", "parked");
        e.fail(t2, "no");
        FuseeException failed = assertThrows(FuseeException.class, run2::result);
        assertEquals("workflow failed: step 'decision' failed: no", failed.getMessage());

        Workflow<Integer, Integer> ticks = e.register("tick", Integer.class, Integer.class, (ctx, n) -> n);
        Schedule s = e.schedule("every", ticks.name(), 7, ScheduleOptions.every(Duration.ofHours(1)).paused(true));
        s.trigger();
        Json desc = s.describe();
        assertTrue(desc.text().contains("paused"), desc.text());
        s.backfill(Instant.now().minusSeconds(3600 * 3), Instant.now());
        s.resume();
        s.pause();
        assertThrows(FuseeException.class, () -> e.schedule("bad", ticks.name(), null, ScheduleOptions.cron("not cron")));
        s.delete();

        Workflow<List<String>, List<String>> collect = e.register("collect", new TypeRef<List<String>>() {},
                new TypeRef<List<String>>() {}, (ctx, events) -> events);
        List<String> ids = new ArrayList<>();
        for (String ev : List.of("a", "b", "b")) {
            ids.add(collect.batch("k", ev, new BatchOptions().maxEvents(3).maxWait(Duration.ofMillis(50)).id(ev)).id());
        }
        assertEquals(ids.get(0), ids.get(1));
        assertEquals(ids.get(1), ids.get(2));
        Run<List<String>> b = e.run(ids.get(0), new TypeRef<List<String>>() {});
        b.started();
        assertEquals(List.of("a", "b"), b.result());
        Workflow<Integer, Integer> echo = e.register("echo", Integer.class, Integer.class, (ctx, v) -> v);
        Run<Integer> d1 = echo.debounce("k", 1, DebounceOptions.window(Duration.ofMillis(30)).id("d1"));
        Run<Integer> d2 = echo.debounce("k", 2, DebounceOptions.window(Duration.ofMillis(30)).id("d2"));
        assertEquals(d1.id(), d2.id());
        d1.started(Duration.ofSeconds(10));
        assertEquals(2, d1.result());
        assertThrows(FuseeException.class, () -> e.workflow("echo", Object.class, Object.class).debounce("k",
                new byte[] {1}, DebounceOptions.window(Duration.ofMillis(5))));
        byte[] exported = e.exportJournal(ids.get(0));
        assertNotNull(exported);
        assertTrue(exported.length > 0);
        assertEquals(null, e.exportJournal("nope"));
        assertEquals(2, e.shardMetrics().size());
        assertTrue(e.stats().nativeCalls() > 0);
        assertEquals(2, e.shards());
    }

    @Test
    void resetRunsFromAStep() {
        AtomicInteger cs = new AtomicInteger();
        Workflow<Object, Integer> wf = e.register("reset", Object.class, Integer.class, (ctx, in) -> {
            int a = ctx.step("a", Integer.class, () -> 1);
            int b = ctx.step("b", Integer.class, () -> a + 1);
            return ctx.step("c", Integer.class, () -> b + cs.incrementAndGet());
        });
        Run<Integer> run = wf.start("r-1", null);
        assertEquals(3, run.result());
        run.reset("b");
        for (int i = 0; i < 2000 && cs.get() < 2; i++) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
        assertEquals(4, run.result());
    }

    @Test
    void jsonCodec() {
        record Item(String name, int qty, List<Double> prices, Map<String, Boolean> flags, Kind kind) {}
        Item item = new Item("a\"b\n", 3, List.of(1.5, 2.0), Map.of("x", true), Kind.BIG);
        Json j = Json.from(item);
        Item back = j.as(Item.class);
        assertEquals(item, back);
        assertEquals("{\"name\":\"a\\\"b\\n\",\"qty\":3,\"prices\":[1.5,2],\"flags\":{\"x\":true},\"kind\":\"BIG\"}",
                j.text());
        Object tree = Json.of("{\"a\":[1,2.5,\"x\",null,true,12345678901]}").value();
        assertEquals(Map.of("a", java.util.Arrays.asList(1, 2.5, "x", null, true, 12345678901L)), tree);
        Pojo p = Json.of("{\"name\":\"n\",\"n\":4}").as(Pojo.class);
        assertEquals("n", p.name);
        assertEquals(4, p.n);
        assertEquals("{\"name\":\"n\",\"n\":4}", Json.from(p).text());
        assertEquals("\"\\u0001é\"", Json.from("\u0001é").text());
    }

    enum Kind {
        BIG
    }

    public static class Pojo {
        public String name;
        public int n;
    }
}
