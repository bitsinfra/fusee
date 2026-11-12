package dev.fusee.cmd;

import dev.fusee.BatchOptions;
import dev.fusee.CallOptions;
import dev.fusee.CancelledException;
import dev.fusee.ChildOptions;
import dev.fusee.Chunk;
import dev.fusee.DebounceOptions;
import dev.fusee.Engine;
import dev.fusee.Exhausted;
import dev.fusee.Json;
import dev.fusee.LimitOptions;
import dev.fusee.ObjectHandler;
import dev.fusee.ObjectRef;
import dev.fusee.ObjectState;
import dev.fusee.Op;
import dev.fusee.Options;
import dev.fusee.RetryPolicy;
import dev.fusee.Run;
import dev.fusee.RunDetail;
import dev.fusee.StartOptions;
import dev.fusee.StepFailedException;
import dev.fusee.StepOptions;
import dev.fusee.StreamReader;
import dev.fusee.StreamWriter;
import dev.fusee.TypeRef;
import dev.fusee.UpdateOptions;
import dev.fusee.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Conformance runner (Java SDK): the scenarios of conformance/README.md,
 * printed as each run's normalized journal for conformance/compare.py.
 *
 * <pre>java -cp target/classes:target/test-classes dev.fusee.cmd.Conformance &gt; java.json</pre>
 */
public final class Conformance {
    private Conformance() {}

    private static final Map<String, Json> OUT = new LinkedHashMap<>();
    private static final Map<String, Integer> TRIES = new ConcurrentHashMap<>();

    private static int tries(String id) {
        return TRIES.merge(id, 1, Integer::sum);
    }

    public record SeqIn(int n) {}

    public record PrioIn(int level, String key, int weight) {}

    /** Pause: see pause in crates/fusee/examples/conformance.rs. */
    private static void pause(Engine e) throws Exception {
        int[] calls = {0};
        Workflow<Object, String> w = e.register("pause", Object.class, String.class, (ctx, in) ->
                ctx.step("flaky", String.class, () -> {
                    calls[0]++;
                    if (calls[0] < 3) throw new RuntimeException("try " + calls[0]);
                    return "ok";
                }, StepOptions.defaults().inline().retry(RetryPolicy.attempts(2).initial(Duration.ofMillis(1))
                        .max(Duration.ofMillis(1)).onExhausted(Exhausted.PAUSE))));
        w.start("pause", null);
        until(e, "pause", "paused");
        e.run("pause").resume();
        journal(e, "pause", "pause");
    }

    private static void journal(Engine e, String name, String id) {
        try {
            e.run(id).result();
        } catch (RuntimeException ignored) {
            // Failed and cancelled runs have journals too.
        }
        RunDetail d = e.run(id).inspect();
        if (d == null) throw new IllegalStateException("no run " + id);
        OUT.put(name, d.journal());
    }

    /** Waits for a run to reach one of the states. */
    private static void until(Engine e, String id, String... states) throws InterruptedException {
        for (int i = 0; i < 2000; i++) {
            RunDetail d = e.run(id).inspect();
            if (d != null) {
                for (String s : states) {
                    if (s.equals(d.status())) return;
                }
            }
            Thread.sleep(1);
        }
        throw new IllegalStateException(id + " never reached " + List.of(states));
    }

    public static void main(String[] args) throws Exception {
        try (Engine e = Engine.open(Options.memory().shards(2).keepHistory(true))) {
            basics(e);
            features(e);
            streams(e);
            queues(e);
            pause(e);
            objects();
            StringBuilder sb = new StringBuilder("{\n");
            boolean first = true;
            for (Map.Entry<String, Json> en : OUT.entrySet()) {
                if (!first) sb.append(",\n");
                first = false;
                sb.append("  ").append(Json.from(en.getKey()).text()).append(": ").append(en.getValue().text());
            }
            sb.append("\n}\n");
            System.out.print(sb);
        }
    }

    private static void basics(Engine e) throws Exception {
        RetryPolicy r3 = RetryPolicy.attempts(3).initial(Duration.ofMillis(1)).max(Duration.ofMillis(1));

        Workflow<SeqIn, Map<String, Integer>> seq = e.register("seq", TypeRef.of(SeqIn.class),
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
        Workflow<Integer, Integer> parallel = e.register("parallel", Integer.class, Integer.class, (ctx, n) -> {
            // Step functions run on threads of their own unless inline, so two
            // parallel steps record in the order they finish; inline, they run
            // in the order they start, as in the other SDKs.
            StepOptions in = StepOptions.defaults().inline();
            Op<Integer> x = ctx.startStep("x", Integer.class, () -> n, in);
            Op<Integer> y = ctx.startStep("y", Integer.class, () -> n * 10, in);
            ctx.all(x, y);
            int xv = x.get(), yv = y.get();
            return ctx.step("z", Integer.class, () -> xv + yv);
        });
        Workflow<Object, String> retry = e.register("retry", Object.class, String.class, (ctx, in) -> {
            String id = ctx.workflowId();
            return ctx.step("flaky", String.class, () -> {
                int n = tries(id);
                if (n < 3) throw new RuntimeException("try " + n);
                return "ok";
            }, StepOptions.defaults().retry(r3));
        });
        Workflow<Object, String> readOnly = e.register("read_only", Object.class, String.class, (ctx, in) -> {
            String id = ctx.workflowId();
            String v = ctx.step("fetch", String.class, () -> {
                int n = tries(id);
                if (n < 2) throw new RuntimeException("try " + n);
                return "data";
            }, StepOptions.defaults().retry(r3).readOnly());
            return ctx.step("store", String.class, () -> v + "!");
        });
        Workflow<Object, String> signal = e.register("signal", Object.class, String.class, (ctx, in) -> {
            String v = ctx.waitFor("go", String.class);
            return ctx.step("use", String.class, () -> v + "!");
        });
        e.register("double", Integer.class, Integer.class, (ctx, x) -> ctx.step("mul", Integer.class, () -> x * 2));
        Workflow<Object, Integer> child = e.register("child", Object.class, Integer.class,
                (ctx, in) -> ctx.child("double", ctx.workflowId() + "/c", 21, Integer.class));
        Workflow<Object, String> sleep = e.register("sleep", Object.class, String.class, (ctx, in) -> {
            ctx.sleep("nap", Duration.ofMillis(1));
            return ctx.step("after", String.class, () -> "woke");
        });
        Workflow<Object, String> values = e.register("values", Object.class, String.class, (ctx, in) -> {
            ctx.now();
            ctx.random();
            ctx.uuid();
            return "values";
        });
        Workflow<Object, String> cancel = e.register("cancel", Object.class, String.class, (ctx, in) -> {
            try {
                return ctx.waitFor("never", String.class);
            } catch (CancelledException ce) {
                ctx.step("undo", String.class, ce::reason);
                throw new CancelledException("undone: " + ce.reason());
            }
        });
        Workflow<Object, String> fail = e.register("fail", Object.class, String.class, (ctx, in) -> {
            try {
                ctx.step("boom", String.class, () -> {
                    throw new RuntimeException("boom");
                }, StepOptions.defaults().retry(RetryPolicy.attempts(1)));
                return "unexpected";
            } catch (StepFailedException se) {
                return ctx.step("recover", String.class, () -> "recovered");
            }
        });
        Workflow<Object, String> listener = e.register("listener", Object.class, String.class,
                (ctx, in) -> ctx.waitFor("ping", String.class));
        Workflow<String, String> send = e.register("send", String.class, String.class, (ctx, target) -> {
            ctx.signal(target, "ping", "hello");
            return "sent";
        });
        Workflow<Object, String> timeoutWait = e.register("timeout_wait", Object.class, String.class, (ctx, in) -> {
            boolean ok = ctx.waitFor("never", String.class, Duration.ofMillis(5)).isPresent();
            return ctx.step("after", String.class, () -> ok ? "some" : "none");
        });

        seq.start("seq", new SeqIn(3));
        parallel.start("parallel", 5);
        retry.start("retry", null);
        readOnly.start("read_only", null);
        signal.start("signal", null);
        e.run("signal").signal("go", "hi");
        child.start("child", null);
        sleep.start("sleep", null);
        values.start("values", null);
        cancel.start("cancel", null);
        fail.start("fail", null);
        listener.start("listener", null);
        send.start("send", "listener");
        timeoutWait.start("timeout_wait", null);
        for (int i = 0; i < 100; i++) {
            RunDetail d = e.run("cancel").inspect();
            if (d != null && (d.status().equals("suspended") || d.status().equals("parked"))) break;
            Thread.sleep(1);
        }
        e.run("cancel").cancel("stop");
        for (String id : List.of("seq", "parallel", "retry", "read_only", "signal", "child", "child/c", "sleep",
                "values", "cancel", "fail", "listener", "send", "timeout_wait")) {
            journal(e, id, id);
        }
    }

    private static void features(Engine e) throws Exception {
        Workflow<Object, Integer> updates = e.register("updates", Object.class, Integer.class, (ctx, in) -> {
            int[] total = {0};
            ctx.onUpdate("add", Integer.class, v -> {
                int d = ctx.step("apply", Integer.class, () -> v * 2);
                total[0] += d;
                return total[0];
            }, v -> {
                if (v < 0) throw new IllegalArgumentException("negative");
            });
            ctx.onQuery("total", () -> total[0]);
            ctx.waitFor("done", Object.class);
            return total[0];
        });
        Workflow<Integer, Integer> updateStart = e.register("update_start", Integer.class, Integer.class,
                (ctx, start) -> {
                    int[] n = {start};
                    ctx.onUpdate("inc", Integer.class, by -> {
                        n[0] += by;
                        return n[0];
                    });
                    ctx.waitFor("stop", Object.class);
                    return n[0];
                });
        Workflow<Object, Integer> heartbeat = e.register("heartbeat", Object.class, Integer.class,
                (ctx, in) -> ctx.step("work", Integer.class, () -> {
                    ctx.heartbeat(1);
                    Thread.sleep(30);
                    return ctx.progress(Integer.class).orElse(0);
                }));
        BlockingQueue<String> tokens = new ArrayBlockingQueue<>(1);
        Workflow<Object, String> await = e.register("await", Object.class, String.class,
                (ctx, in) -> ctx.awaitCompletion("approval", String.class, tokens::offer));
        Workflow<Integer, Integer> continued = e.register("continued", Integer.class, Integer.class, (ctx, left) -> {
            int v = ctx.step("work", Integer.class, () -> left);
            if (left == 0) return v;
            return ctx.continueAsNew(left - 1);
        });
        Workflow<Object, String> patched = e.register("patched", Object.class, String.class, (ctx, in) -> {
            ctx.deprecatePatch("old");
            String label = ctx.patched("new") ? "new" : "old";
            return ctx.step("after", String.class, () -> label);
        });
        Workflow<Object, String> attributes = e.register("attributes", Object.class, String.class, (ctx, in) -> {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("customer", "c-1");
            a.put("amount", 5);
            ctx.setAttributes(a);
            ctx.setAttributes(Map.of("stage", "paid"));
            return "attributes";
        });
        e.register("kid", Object.class, String.class, (ctx, in) -> ctx.waitFor("go", String.class));
        Workflow<Object, String> parentClose = e.register("parent_close", Object.class, String.class, (ctx, in) -> {
            ctx.spawnChild("kid", "parent_close/terminate", null);
            ctx.spawnChild("kid", "parent_close/abandon", null, new ChildOptions().parentClose("abandon"));
            ctx.waitFor("never", Object.class);
            return "unreachable";
        });
        Workflow<Object, Integer> reset = e.register("reset", Object.class, Integer.class, (ctx, in) -> {
            int a = ctx.step("a", Integer.class, () -> 1);
            int b = ctx.step("b", Integer.class, () -> a + 1);
            return ctx.step("c", Integer.class, () -> b + 1);
        });
        Workflow<Object, String> victim = e.register("victim", Object.class, String.class, (ctx, in) -> {
            try {
                ctx.waitFor("never", Object.class);
                return "unexpected";
            } catch (CancelledException ce) {
                ctx.step("undo", String.class, ce::reason);
                throw new CancelledException("undone");
            }
        });
        Workflow<Object, String> canceller = e.register("canceller", Object.class, String.class, (ctx, in) -> {
            ctx.cancelWorkflow("victim", "stop it");
            return "sent";
        });
        Workflow<Object, String> raceWait = e.register("race_wait", Object.class, String.class, (ctx, in) -> {
            ctx.race(ctx.startWait("x", String.class), ctx.startSleep("nap", Duration.ofMillis(1)));
            return ctx.waitFor("x", String.class);
        });

        updates.start("updates", null, new StartOptions().ifRunning("use-existing"));
        Run<Integer> up = e.run("updates", Integer.class);
        int a = up.update("add", 3, Integer.class, UpdateOptions.id("u1"));
        int b = up.update("add", 3, Integer.class, UpdateOptions.id("u1"));
        RuntimeException refused = null;
        try {
            up.update("add", -1, Integer.class);
        } catch (RuntimeException r) {
            refused = r;
        }
        int total = up.query("total", Integer.class);
        if (a != 6 || b != 6 || refused == null || total != 6) {
            throw new IllegalStateException("updates: " + a + " " + b + " " + refused + " " + total);
        }
        up.signal("done", null);
        Workflow.Started<Integer, Integer> st = updateStart.updateWithStart("update_start", 10, "inc", 5, Integer.class);
        if (st.result() != 15) throw new IllegalStateException("update with start: " + st.result());
        e.run("update_start").signal("stop", null);
        heartbeat.start("heartbeat", null);
        await.start("await", null);
        String token = tokens.take();
        until(e, "await", "suspended", "parked");
        e.complete(token, "approved");
        continued.start("continued", 2);
        patched.start("patched", null);
        attributes.start("attributes", null);
        parentClose.start("parent_close", null);
        until(e, "parent_close/terminate", "suspended", "parked");
        until(e, "parent_close/abandon", "suspended", "parked");
        e.run("parent_close").terminate("stop");
        until(e, "parent_close/terminate", "terminated");
        e.run("parent_close/abandon").signal("go", "free");
        reset.start("reset", null);
        e.run("reset").result();
        e.run("reset").reset("b");
        victim.start("victim", null);
        until(e, "victim", "suspended", "parked");
        canceller.start("canceller", null);
        raceWait.start("race_wait", null);
        for (int i = 0; i < 2000; i++) {
            RunDetail d = e.run("race_wait").inspect();
            if (d != null && d.journal().value() instanceof List<?> j && j.size() >= 6) break;
            Thread.sleep(1);
        }
        e.run("race_wait").signal("x", "late");
        for (String id : List.of("updates", "update_start", "heartbeat", "await", "continued", "patched",
                "attributes", "parent_close", "parent_close/terminate", "parent_close/abandon", "reset", "victim",
                "canceller", "race_wait")) {
            journal(e, id, id);
        }
    }

    /**
     * Streams: a workflow's own stream (appended once, closed by the code), a
     * step's stream (JSON and bytes chunks, ended by the step's result), and a
     * step whose retry continues after what its failed attempt recorded. The
     * reader's view of the workflow's stream is compared too.
     */
    private static void streams(Engine e) {
        RetryPolicy r3 = RetryPolicy.attempts(3).initial(Duration.ofMillis(1)).max(Duration.ofMillis(1));
        e.register("streams", Object.class, String.class, (ctx, in) -> {
            StreamWriter p = ctx.stream("progress");
            p.append("start");
            String text = ctx.step("generate", String.class, () -> {
                StreamWriter out = ctx.stream("tokens");
                for (String t : List.of("a", "b", "c")) out.append(t);
                out.append(new byte[] {1, 2});
                return "abc";
            });
            p.append(Map.of("text", text));
            p.close();
            return text;
        });
        e.register("stream_retry", Object.class, Integer.class, (ctx, in) -> ctx.step("generate", Integer.class, () -> {
            StreamWriter out = ctx.stream("tokens");
            List<String> prev = out.recorded(String.class);
            if (prev.isEmpty()) {
                out.append("x");
                out.append("y");
                throw new RuntimeException("cut off");
            }
            out.append("z");
            return prev.size() + 1;
        }, StepOptions.defaults().retry(r3)));
        for (String id : List.of("streams", "stream_retry")) {
            e.workflow(id, Object.class, Object.class).start(id, null);
            journal(e, id, id);
        }
        StreamReader r = e.run("streams").stream("progress");
        List<String> chunks = new ArrayList<>();
        for (Chunk c : r) chunks.add(c.json().text());
        OUT.put("streams/read", Json.of("{\"chunks\":[" + String.join(",", chunks) + "],\"status\":"
                + Json.from(r.status()).text() + "}"));
    }

    /** A run's journal now (a key's run never finishes). */
    private static void inspected(Engine e, String name, String id) {
        RunDetail d = e.run(id).inspect();
        if (d == null) throw new IllegalStateException("no run " + id);
        OUT.put(name, d.journal());
    }

    private static final TypeRef<List<String>> STRINGS = new TypeRef<>() {};

    private static List<String> items(ObjectState s) {
        List<String> v = s.get("items", STRINGS);
        return v == null ? new ArrayList<>() : new ArrayList<>(v);
    }

    /**
     * Keyed objects: exclusive calls with a step, a retried call ID, a failed
     * call that changes nothing, a shared read, a one-way call, calls from a
     * workflow (one awaited, one sent), clearing, and a key compacted after
     * 128 operations. On an engine that never parks, so a compaction happens
     * only where the scenario makes it.
     */
    private static void objects() throws Exception {
        try (Engine e = Engine.open(Options.memory().shards(2).keepHistory(true).parkAfter(Duration.ofHours(1)))) {
            Map<String, ObjectHandler> cart = new LinkedHashMap<>();
            cart.put("add", ObjectHandler.exclusive(String.class, (c, item) -> {
                String priced = c.ctx().step("price", String.class, () -> item + "$");
                List<String> all = items(c.state());
                all.add(priced);
                c.state().set("items", all);
                c.state().set("last", all.get(all.size() - 1));
                return all.size();
            }));
            cart.put("remove", ObjectHandler.exclusive(String.class, (c, item) -> {
                List<String> all = items(c.state());
                c.state().set("touched", true);
                int at = all.indexOf(item);
                if (at < 0) throw new RuntimeException("missing " + item);
                all.remove(at);
                c.state().set("items", all);
                return all.size();
            }));
            cart.put("get", ObjectHandler.shared(Object.class, (c, ignored) -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("items", c.state().raw("items"));
                m.put("keys", c.state().keys());
                return m;
            }));
            cart.put("clear", ObjectHandler.exclusive(Object.class, (c, ignored) -> {
                c.state().clearAll();
                return c.state().keys().size();
            }));
            e.registerObject("conf_cart", cart);
            e.registerObject("conf_tally", Map.of("add", ObjectHandler.exclusive(Long.class, (c, n) -> {
                Long sum = c.state().get("sum", Long.class);
                long next = (sum == null ? 0 : sum) + n;
                c.state().set("sum", next);
                return next;
            })));
            Workflow<String, Long> caller = e.register("obj_caller", String.class, Long.class, (ctx, key) -> {
                long n = ctx.object("conf_cart", key).call("add", "d", Long.class);
                ctx.object("conf_cart", key).send("add", "e");
                return n;
            });
            ObjectRef k = e.object("conf_cart", "k1");
            List<Json> calls = new ArrayList<>();
            java.util.function.BiConsumer<String[], Object> call = (hid, arg) -> {
                Json v;
                try {
                    v = k.call(hid[0], arg, CallOptions.id(hid[1]));
                } catch (RuntimeException err) {
                    v = Json.of("{\"error\":true}");
                }
                calls.add(v);
            };
            call.accept(new String[] {"add", "c1"}, "a");
            call.accept(new String[] {"add", "c2"}, "b");
            call.accept(new String[] {"add", "c2"}, "b");
            call.accept(new String[] {"remove", "c3"}, "zz");
            call.accept(new String[] {"get", "c4"}, null);
            k.send("add", "c", CallOptions.id("s1"));
            Run<Long> run = caller.start("obj_caller", "k1");
            calls.add(Json.from(run.result()));
            for (int i = 0; i < 2000; i++) {
                Object items = ((Map<?, ?>) k.state().value()).get("items");
                if (items instanceof List<?> l && l.size() == 5) break;
                Thread.sleep(1);
            }
            call.accept(new String[] {"get", "c5"}, null);
            call.accept(new String[] {"clear", "c6"}, null);
            call.accept(new String[] {"add", "c7"}, "f");
            call.accept(new String[] {"fusee.state.edit", "c8"}, Map.of("set", Map.of("owner", "ana"),
                    "patch", List.of(Map.of("op", "add", "path", "/items/-", "value", "g"))));
            calls.add(k.state());
            OUT.put("obj/calls", Json.from(calls));
            inspected(e, "obj/cart", k.id());
            inspected(e, "obj/caller", "obj_caller");
            ObjectRef t = e.object("conf_tally", "t");
            List<Long> sums = new ArrayList<>();
            for (long n = 1; n <= 130; n++) sums.add(t.call("add", n, Long.class, CallOptions.id("t-" + n)));
            // A retry from before the compaction gets its recorded result.
            sums.add(t.call("add", 5, Long.class, CallOptions.id("t-5")));
            OUT.put("obj/sums", Json.from(List.of(sums.get(0), sums.get(127), sums.get(129), sums.get(130))));
            OUT.put("obj/tally_state", t.state());
            inspected(e, "obj/tally", t.id());
        }
    }

    private static void queues(Engine e) throws Exception {
        e.limit("conf", LimitOptions.concurrent(1));
        Workflow<PrioIn, String> prio = e.register("prio", PrioIn.class, String.class, (ctx, p) -> {
            int first = ctx.step("first", Integer.class, p::level,
                    StepOptions.defaults().limit("conf").priority(p.level()).fairness(p.key(), p.weight()));
            return ctx.step("second", String.class, () -> p.key() + ":" + first,
                    StepOptions.defaults().limit("conf").priority(-p.level()).fairness("other"));
        });
        Workflow<Object, Object> deb = e.register("deb_target", Object.class, Object.class,
                (ctx, v) -> ctx.step("got", Object.class, () -> v));
        Workflow<List<Object>, List<Object>> bat = e.register("batch_target", new TypeRef<List<Object>>() {},
                new TypeRef<List<Object>>() {}, (ctx, events) -> ctx.step("got", new TypeRef<List<Object>>() {},
                        () -> events));
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String key = i % 2 == 1 ? "odd" : "even";
            String id = "prio/" + i;
            prio.start(id, new PrioIn(i, key, i + 1));
            ids.add(id);
        }
        // Three starts in order inside the window: one run with the latest
        // input; a retried event ID joins once and its input is not used.
        List<Run<Object>> runs = new ArrayList<>();
        for (int n = 1; n <= 3; n++) {
            runs.add(deb.debounce("k", n, DebounceOptions.window(Duration.ofMillis(500)).id("d" + n)));
        }
        Run<Object> again = deb.debounce("k", 99, DebounceOptions.window(Duration.ofMillis(500)).id("d2"));
        runs.add(again);
        for (Run<Object> r : runs) {
            if (!r.id().equals(runs.get(0).id())) throw new IllegalStateException("debounced starts joined different runs");
        }
        runs.get(0).started();
        journal(e, "debounce/run", runs.get(0).id());
        List<Run<List<Object>>> batch = new ArrayList<>();
        for (String[] ev : new String[][] {{"b1", "a"}, {"b2", "b"}, {"b2", "x"}, {"b3", "c"}}) {
            batch.add(bat.batch("k", ev[1], new BatchOptions().maxEvents(3).maxWait(Duration.ofMinutes(1)).id(ev[0])));
        }
        for (Run<List<Object>> r : batch) {
            if (!r.id().equals(batch.get(0).id())) throw new IllegalStateException("batched events joined different runs");
        }
        batch.get(0).started();
        journal(e, "batch/full", batch.get(0).id());
        Run<List<Object>> timed = bat.batch("t", "z", new BatchOptions().maxEvents(100).maxWait(Duration.ofMillis(20)));
        timed.started();
        journal(e, "batch/timed", timed.id());
        OUT.put("debounce/describe", e.run("fusee.debounce/deb_target/k").query("describe", Json.class));
        OUT.put("batch/describe", e.run("fusee.batch/batch_target/k").query("describe", Json.class));
        for (String id : ids) journal(e, id, id);
    }
}
