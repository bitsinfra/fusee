package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Keyed objects (docs/features.md, "Objects (keyed state)"). */
@Timeout(120)
class ObjectsTest {
    @TempDir
    Path dir;

    static final TypeRef<List<String>> STRINGS = new TypeRef<>() {};

    static void eventually(String what, BooleanSupplier f) throws InterruptedException {
        for (int i = 0; i < 4000; i++) {
            if (f.getAsBoolean()) return;
            Thread.sleep(2);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> state(ObjectRef k) {
        return (Map<String, Object>) k.state().value();
    }

    static List<String> items(ObjectState s) {
        List<String> v = s.get("items", STRINGS);
        return v == null ? new ArrayList<>() : new ArrayList<>(v);
    }

    /** The cart type of these tests; ran counts its step's runs. */
    static ObjectType cart(Engine e, AtomicInteger ran) {
        Map<String, ObjectHandler> h = new LinkedHashMap<>();
        h.put("add", ObjectHandler.exclusive(String.class, (c, item) -> {
            String priced = c.ctx().step("price", String.class, () -> {
                ran.incrementAndGet();
                return item + "$";
            });
            List<String> all = items(c.state());
            all.add(priced);
            c.state().set("items", all);
            return all.size();
        }));
        h.put("get", ObjectHandler.shared(Object.class, (c, ignored) -> items(c.state())));
        h.put("fail", ObjectHandler.exclusive(Object.class, (c, ignored) -> {
            c.state().set("items", List.of("lost"));
            throw new RuntimeException("out of stock");
        }));
        h.put("key", ObjectHandler.shared(Object.class, (c, ignored) -> c.key()));
        h.put("write", ObjectHandler.shared(Object.class, (c, ignored) -> {
            c.state().set("x", 1);
            return null;
        }));
        return e.registerObject("cart", h);
    }

    /**
     * Calls change state, a failure changes nothing and records its bare
     * message, a retry with an ID runs once, and a restart keeps the state
     * and the IDs (a one-way call sent before the close runs after it).
     */
    @Test
    void stateRetriesAndRestart() throws Exception {
        AtomicInteger ran = new AtomicInteger();
        Engine e = Engine.open(Options.local(dir.toString()).flush("fsync"));
        ObjectRef k = cart(e, ran).key("u-1");
        assertEquals(1, k.call("add", "apple", Integer.class, CallOptions.id("a")));
        assertEquals(2, k.call("add", "pear", Integer.class, CallOptions.id("b")));
        assertEquals(2, k.call("add", "pear", Integer.class, CallOptions.id("b")));
        assertEquals(2, ran.get());
        assertEquals(List.of("apple$", "pear$"), k.call("get", null, STRINGS, null));
        WorkflowFailedException failed = assertThrows(WorkflowFailedException.class, () -> k.call("fail", null));
        assertEquals("out of stock", failed.failure().message());
        FuseeException write = assertThrows(FuseeException.class, () -> k.call("write", null));
        assertTrue(write.getMessage().contains("shared handler cannot change state"), write.getMessage());
        assertEquals(Map.of("items", List.of("apple$", "pear$")), state(k));
        assertEquals("u-1", k.call("key", null, String.class));
        assertEquals(Map.of(), state(e.object("cart", "nobody")));
        RunDetail d = k.inspect();
        assertNotNull(d.object());
        assertEquals(2, ((List<?>) ((Map<?, ?>) ((Map<?, ?>) d.object().value()).get("state")).get("items")).size());
        boolean recorded = false;
        for (Object r : (List<?>) d.journal().value()) {
            Map<?, ?> m = (Map<?, ?>) r;
            // A failure is recorded as its envelope ({"message": ...}), as
            // every SDK reads it back.
            Object p = m.get("payload");
            recorded |= "update_done".equals(m.get("kind")) && "failed".equals(m.get("status"))
                    && p instanceof Map<?, ?> pm && "out of stock".equals(pm.get("message"));
        }
        assertTrue(recorded, "the failure is recorded with its message: " + d.journal());
        k.send("add", "fig", CallOptions.id("c"));
        assertEquals(List.of("u-1"), e.objects("cart", 0));
        e.close();

        e = Engine.open(Options.local(dir.toString()).flush("fsync"));
        try {
            ObjectRef k2 = cart(e, ran).key("u-1");
            eventually("the send", () -> ((List<?>) state(k2).get("items")).size() == 3);
            assertEquals(2, k2.call("add", "pear", Integer.class, CallOptions.id("b")));
            assertEquals(3, ran.get());
            k2.delete();
            // The next call starts with empty state.
            assertEquals(1, k2.call("add", "kiwi", Integer.class));
        } finally {
            e.close();
        }
    }

    /**
     * Exclusive calls take turns in arrival order; a shared call does not
     * wait for a running exclusive one, and reads the committed state.
     */
    @Test
    void exclusiveOrderAndShared() throws Exception {
        try (Engine e = Engine.open(Options.memory().shards(2))) {
            AtomicInteger busy = new AtomicInteger(), overlaps = new AtomicInteger();
            CountDownLatch release = new CountDownLatch(1);
            Map<String, ObjectHandler> h = new LinkedHashMap<>();
            h.put("bump", ObjectHandler.exclusive(Integer.class, (c, who) -> {
                if (!busy.compareAndSet(0, 1)) overlaps.incrementAndGet();
                c.ctx().step("work", Integer.class, () -> {
                    Thread.sleep(1);
                    return 0;
                });
                List<Integer> seen = c.state().get("seen", new TypeRef<List<Integer>>() {});
                List<Integer> next = seen == null ? new ArrayList<>() : new ArrayList<>(seen);
                next.add(who);
                busy.set(0);
                c.state().set("seen", next);
                return next.size();
            }));
            h.put("hold", ObjectHandler.exclusive(Object.class, (c, ignored) -> {
                c.state().set("held", true);
                c.ctx().step("hold", Object.class, () -> {
                    release.await();
                    return null;
                });
                return null;
            }));
            h.put("read", ObjectHandler.shared(Object.class, (c, ignored) -> {
                Boolean held = c.state().get("held", Boolean.class);
                return held != null && held;
            }));
            ObjectRef k = e.registerObject("doc", h).key("d");
            int[] places = new int[30];
            List<CompletableFuture<Void>> all = new ArrayList<>();
            for (int who = 0; who < places.length; who++) {
                int w = who;
                all.add(CompletableFuture.runAsync(() -> places[w] = k.call("bump", w, Integer.class)));
            }
            CompletableFuture.allOf(all.toArray(new CompletableFuture[0])).join();
            assertEquals(0, overlaps.get(), "exclusive calls overlapped");
            @SuppressWarnings("unchecked")
            List<Number> seen = (List<Number>) state(k).get("seen");
            for (int who = 0; who < places.length; who++) {
                assertEquals(who, seen.get(places[who] - 1).intValue(), "call " + who + " place " + places[who]);
            }
            CompletableFuture<Json> held = CompletableFuture.supplyAsync(() -> k.call("hold", null));
            eventually("hold to start", () -> {
                RunDetail d = k.inspect();
                return d != null && d.object() != null
                        && ((List<?>) ((Map<?, ?>) d.object().value()).get("running")).size() == 1;
            });
            // The exclusive call holds the key; a shared read answers at once,
            // with the committed state.
            assertFalse(k.call("read", null, Boolean.class));
            CompletableFuture<Integer> next = CompletableFuture.supplyAsync(() -> k.call("bump", 99, Integer.class));
            eventually("bump to wait", () -> {
                RunDetail d = k.inspect();
                if (d == null || d.object() == null) return false;
                List<?> waiting = (List<?>) ((Map<?, ?>) d.object().value()).get("waiting");
                return waiting.size() == 1 && "bump".equals(((Map<?, ?>) waiting.get(0)).get("handler"));
            });
            release.countDown();
            held.join();
            assertEquals(31, next.join());
            assertTrue(k.call("read", null, Boolean.class));
        }
    }

    public record Shopped(int a, int placed, List<String> items) {}

    /**
     * A workflow and an object's handler call objects exactly once: calls
     * are journal operations, and each handler's step ran once.
     */
    @Test
    void callsFromWorkflowsAndObjects() {
        try (Engine e = Engine.open(Options.memory().shards(2).keepHistory(true))) {
            AtomicInteger ran = new AtomicInteger();
            cart(e, ran);
            e.registerObject("order", Map.of("place", ObjectHandler.exclusive(STRINGS, (c, items) -> {
                int n = 0;
                for (String i : items) n = c.ctx().object("cart", c.key()).call("add", i, Integer.class);
                c.state().set("placed", n);
                return n;
            })));
            Workflow<String, Shopped> wf = e.register("shopper", String.class, Shopped.class, (ctx, key) -> {
                int a = ctx.object("cart", key).call("add", "tea", Integer.class);
                ctx.object("cart", key).send("add", "milk");
                int placed = ctx.object("order", key).call("place", List.of("jam", "bread"), Integer.class);
                List<String> items = ctx.object("cart", key).call("get", null, STRINGS);
                return new Shopped(a, placed, items);
            });
            Run<Shopped> run = wf.start("shop-1", "s1");
            Shopped o = run.result();
            List<String> items = new ArrayList<>(o.items());
            items.sort(null);
            assertEquals(1, o.a());
            assertEquals(List.of("bread$", "jam$", "milk$", "tea$"), items);
            assertEquals(4, ran.get());
            int calls = 0;
            for (Object r : (List<?>) run.inspect().journal().value()) {
                if ("call".equals(((Map<?, ?>) r).get("kind"))) calls++;
            }
            assertEquals(4, calls, run.inspect().journal().text());
            // A call to a type with a bad name fails in the workflow.
            Workflow<Object, String> bad = e.register("bad_caller", Object.class, String.class, (ctx, in) -> {
                try {
                    ctx.object("no/type", "k").call("x", null);
                    return "no error";
                } catch (RuntimeException err) {
                    return err.getMessage();
                }
            });
            assertTrue(bad.start("bad-1", null).result().contains("has no '/'"));
        }
    }

    /**
     * A key's history is compacted: its journal stays short, and its state
     * and the results of calls with IDs survive (a retry gets its recorded
     * result).
     */
    @Test
    void compactionKeepsStateAndIDs() {
        try (Engine e = Engine.open(Options.memory().shards(2))) {
            AtomicInteger ran = new AtomicInteger();
            ObjectRef k = e.registerObject("tally", Map.of("add", ObjectHandler.exclusive(Integer.class, (c, n) -> {
                ran.incrementAndGet();
                Integer sum = c.state().get("sum", Integer.class);
                int next = (sum == null ? 0 : sum) + n;
                c.state().set("sum", next);
                return next;
            }))).key("t");
            for (int n = 1; n <= 300; n++) {
                assertEquals(n * (n + 1) / 2, k.call("add", n, Integer.class, CallOptions.id("id-" + n)));
            }
            RunDetail d = k.inspect();
            assertTrue(((List<?>) d.journal().value()).size() <= 2 * 128 + 4, d.journal().text());
            assertEquals("{\"sum\":45150}", Json.from(((Map<?, ?>) d.object().value()).get("state")).text());
            int before = ran.get();
            assertEquals(15, k.call("add", 5, Integer.class, CallOptions.id("id-5")));
            assertEquals(before, ran.get(), "a retry ran again");
        }
    }
}
