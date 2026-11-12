package dev.fusee.cmd;

import dev.fusee.BatchOptions;
import dev.fusee.CallOptions;
import dev.fusee.Chunk;
import dev.fusee.Client;
import dev.fusee.DebounceOptions;
import dev.fusee.Engine;
import dev.fusee.Json;
import dev.fusee.LimitOptions;
import dev.fusee.ListOptions;
import dev.fusee.ObjectHandler;
import dev.fusee.Options;
import dev.fusee.RemoteObjectRef;
import dev.fusee.RemoteRun;
import dev.fusee.RunSummary;
import dev.fusee.StartOptions;
import dev.fusee.StepOptions;
import dev.fusee.StreamReader;
import dev.fusee.StreamWriter;
import dev.fusee.TypeRef;
import dev.fusee.UpdateOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Companion to conformance/remote.py: {@code Remote worker ADDRESS} serves the
 * workflows through a Fusee server; {@code Remote client ADDRESS [TAG]}
 * drives a worker in any language through it (TAG makes its workflow IDs
 * unique).
 */
public final class Remote {
    private Remote() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: Remote worker|client ADDRESS [TAG]");
            System.exit(2);
        }
        if (args[0].equals("worker")) worker(args[1]);
        else client(args[1], args.length > 2 ? args[2] : "");
    }

    static void worker(String address) throws Exception {
        Engine e = Engine.open(Options.server(address));
        e.register("approval", TypeRef.of(Integer.class), new TypeRef<List<Object>>() {}, (ctx, initial) -> {
            int[] count = {initial};
            String[] token = {null};
            ctx.onUpdate("add", Integer.class, n -> {
                count[0] += n;
                return count[0];
            });
            ctx.onQuery("count", () -> count[0]);
            ctx.onQuery("token", () -> token[0]);
            // Inline: the send function sets workflow state.
            boolean approved = ctx.awaitCompletion("decision", Boolean.class, t -> token[0] = t,
                    StepOptions.defaults().inline());
            return List.of(count[0], approved);
        });
        // Debounced and batched runs, their steps queued for a limit by priority.
        e.limit("remote", LimitOptions.concurrent(1));
        e.register("echo", Object.class, Object.class, (ctx, v) -> ctx.step("v", Object.class, () -> v,
                StepOptions.defaults().limit("remote").priority(2).fairness("t", 2)));
        e.register("collect", new TypeRef<List<Object>>() {}, new TypeRef<List<Object>>() {},
                (ctx, events) -> ctx.step("all", new TypeRef<List<Object>>() {}, () -> events,
                        StepOptions.defaults().limit("remote").priority(-1)));
        // A step that streams tokens, then bytes.
        e.register("talk", Integer.class, Integer.class, (ctx, n) -> ctx.step("generate", Integer.class, () -> {
            StreamWriter out = ctx.stream("tokens");
            for (int i = 0; i < n; i++) {
                out.append("t" + i);
                Thread.sleep(5);
            }
            out.append(new byte[] {1, 2});
            return n;
        }));
        // A key of an object type, called by every client.
        e.registerObject("tally", Map.of("add", ObjectHandler.exclusive(Long.class, (c, n) -> {
            Long sum = c.state().get("sum", Long.class);
            long next = (sum == null ? 0 : sum) + n;
            c.state().set("sum", next);
            return next;
        })));
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            e.close();
            stopped.countDown();
        }));
        System.out.println("READY");
        System.out.flush();
        stopped.await();
    }

    private static String text(Chunk c) {
        return c.isBytes() ? "bytes:" + java.util.Arrays.toString(c.bytes()) : c.json().text();
    }

    private static void check(boolean ok, String what) {
        if (!ok) {
            System.err.println("FAIL Java client: " + what);
            System.exit(1);
        }
    }

    static void client(String address, String tag) throws Exception {
        try (Client c = Client.connect(address)) {
            String name = "from-java" + tag;
            Json v = c.updateWithStart("approval", name, 4, "add", 3, new StartOptions().updateId("once"));
            check(v.text().equals("7"), "update with start: " + v);
            RemoteRun run = c.run(name);
            v = run.update("add", 3, UpdateOptions.id("once"));
            check(v.text().equals("7"), "a repeated update: " + v);
            check(run.query("count", null).text().equals("7"), "query");
            String token = null;
            for (int i = 0; i < 100 && token == null; i++) {
                token = run.query("token", null, String.class);
                if (token == null) Thread.sleep(10);
            }
            check(token != null, "no token");
            c.complete(token, true);
            check(run.result().value().equals(List.of(7, true)), "result: " + run.result());
            boolean found = false;
            for (RunSummary r : c.list(ListOptions.where("status", "ok"))) found |= r.id().equals(name) && r.startedMs() > 0;
            check(found, "listing");
            // Two starts inside the window: one run, the latest input.
            RemoteRun first = c.debounce("echo", "d-" + name, 1, DebounceOptions.window(Duration.ofMillis(500)).id("d1"));
            RemoteRun latest = c.debounce("echo", "d-" + name, 2, DebounceOptions.window(Duration.ofMillis(500)).id("d2"));
            check(first.id().equals(latest.id()), "debounce joined " + first.id() + " " + latest.id());
            first.started(Duration.ofSeconds(30));
            check(first.result().text().equals("2"), "debounced result: " + first.result());
            // A repeated event ID adds nothing; the batch closes on its wait.
            List<RemoteRun> events = new ArrayList<>();
            for (String ev : List.of("a", "b", "b")) {
                events.add(c.batch("collect", "b-" + name, ev,
                        new BatchOptions().maxEvents(3).maxWait(Duration.ofMillis(300)).id(ev)));
            }
            check(events.get(0).id().equals(events.get(1).id()) && events.get(1).id().equals(events.get(2).id()),
                    "batched events joined different runs");
            events.get(0).started(Duration.ofSeconds(30));
            check(events.get(0).result().text().equals("[\"a\",\"b\"]"), "batch result: " + events.get(0).result());
            // A stream read through the server, then again from where it stopped.
            c.start("talk", "s-" + name, 10);
            List<String> got = new ArrayList<>();
            StreamReader r = c.run("s-" + name).stream("tokens");
            while (got.size() < 4 && r.hasNext()) got.add(text(r.next()));
            StreamReader rest = c.run("s-" + name).stream("tokens", r.offset());
            for (Chunk ch : rest) got.add(text(ch));
            List<String> want = new ArrayList<>();
            for (int i = 0; i < 10; i++) want.add("\"t" + i + "\"");
            want.add("bytes:[1, 2]");
            check(got.equals(want), "stream chunks: " + got);
            check("ok".equals(rest.status()) && rest.offset() == 11,
                    "stream end: " + rest.status() + " at " + rest.offset());
            // A key of an object type: a call, its retry, a one-way call, the state.
            RemoteObjectRef k = c.object("tally", name);
            for (int i = 0; i < 2; i++) {
                Json sum = k.call("add", 2, CallOptions.id("o1"));
                check(sum.text().equals("2"), "object call " + i + ": " + sum);
            }
            k.send("add", 3, CallOptions.id("o2"));
            String st = "";
            for (int i = 0; i < 200; i++) {
                st = k.state().text();
                if (st.equals("{\"sum\":5}")) break;
                Thread.sleep(10);
            }
            check(st.equals("{\"sum\":5}"), "object state " + st);
            System.out.println("PASS Java client");
        }
    }
}
