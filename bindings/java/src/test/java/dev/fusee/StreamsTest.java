package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Durable streams (docs/features.md#streams). */
@Timeout(120)
class StreamsTest {
    @TempDir
    Path dir;

    Engine e;

    @BeforeEach
    void open() {
        e = Engine.open(Options.memory().shards(2).keepHistory(true));
    }

    @AfterEach
    void close() {
        if (e != null) e.close();
    }

    /** Each chunk as text: its JSON, or "bytes:[...]". */
    static String text(Chunk c) {
        return c.isBytes() ? "bytes:" + Arrays.toString(c.bytes()) : c.json().text();
    }

    static List<String> readAll(StreamReader r) {
        List<String> out = new ArrayList<>();
        for (Chunk c : r) out.add(text(c));
        return out;
    }

    static List<String> tokens(int from, int to) {
        List<String> out = new ArrayList<>();
        for (int i = from; i < to; i++) out.add("\"t" + i + "\"");
        return out;
    }

    @SuppressWarnings("unchecked")
    static int count(Engine e, String id, String kind) {
        int n = 0;
        for (Object r : (List<Object>) e.run(id).inspect().journal().value()) {
            if (kind.equals(((Map<String, Object>) r).get("kind"))) n++;
        }
        return n;
    }

    /**
     * A reader that follows a step's stream while it grows gets every chunk,
     * in order, each once, JSON and bytes, and then the end; the run's own
     * stream carries workflow code's appends.
     */
    @Test
    void orderAndCompleteness() throws Exception {
        int n = 2000;
        CountDownLatch release = new CountDownLatch(1);
        Workflow<Object, Integer> gen = e.register("gen", Object.class, Integer.class, (ctx, in) -> {
            StreamWriter p = ctx.stream("progress");
            p.append("start");
            int v = ctx.step("generate", Integer.class, () -> {
                StreamWriter out = ctx.stream("tokens");
                if (out.recordedCount() != 0 || out.closed()) throw new IllegalStateException("not a first attempt");
                for (int i = 0; i < n; i++) {
                    if (i == n / 2) release.await();
                    out.append("t" + i);
                }
                out.append(new byte[] {0, 1, 2});
                return n;
            });
            p.append(Map.of("n", v));
            p.close();
            return v;
        });
        Run<Integer> run = gen.start("gen-1", null);
        StreamReader r = run.stream("tokens", 0);
        for (int i = 0; i < n / 2; i++) {
            assertTrue(r.hasNext());
            Chunk c = r.next();
            assertEquals(i, c.offset());
            assertEquals("t" + i, c.as(String.class));
        }
        assertNull(r.status());
        release.countDown();
        List<String> want = tokens(n / 2, n);
        want.add("bytes:[0, 1, 2]");
        assertEquals(want, readAll(r));
        assertEquals("ok", r.status());
        assertTrue(r.end().closed());
        assertEquals(n + 1, r.offset());
        assertEquals(n, run.result());
        assertEquals(List.of("\"start\"", "{\"n\":2000}"), readAll(run.stream("progress")));
        assertEquals(n + 3, count(e, "gen-1", "chunk"));
        assertEquals(2, count(e, "gen-1", "stream_end"));
        FuseeException nf = assertThrows(FuseeException.class, () -> e.run("nobody").stream("tokens").hasNext());
        assertEquals("NotFound", nf.code());
        assertEquals(List.of(), readAll(run.stream("nothing")));
    }

    /** A reader that reconnects with the offset it reached continues exactly there. */
    @Test
    void reconnectFromAnOffset() {
        Workflow<Integer, Integer> talk = e.register("talk", Integer.class, Integer.class, (ctx, n) ->
                ctx.step("generate", Integer.class, () -> {
                    StreamWriter out = ctx.stream("tokens");
                    for (int i = 0; i < n; i++) {
                        out.append("t" + i);
                        Thread.sleep(2);
                    }
                    return n;
                }));
        Run<Integer> run = talk.start("talk-1", 20);
        StreamReader r = run.stream("tokens").waitFor(Duration.ofMillis(5));
        List<String> got = new ArrayList<>();
        while (got.size() < 7 && r.hasNext()) got.add(text(r.next()));
        assertEquals(7, r.offset());
        assertTrue(r.runId() > 0);
        StreamReader rest = run.stream("tokens", r.offset()).ofRun(r.runId());
        got.addAll(readAll(rest));
        assertEquals(tokens(0, 20), got);
        assertEquals(20, rest.offset());
        assertEquals("ok", rest.status());
        assertEquals(List.of(), readAll(run.stream("tokens", 20)));
    }

    /**
     * A step that runs again continues after what its failed attempt
     * recorded; workflow code's appends are recorded once, also when its code
     * replays after the engine reopened; a step appends to the run's stream.
     */
    @Test
    void retryContinuesAndReplayAppendsNothing() {
        e.close();
        e = null;
        AtomicInteger attempts = new AtomicInteger();
        java.util.function.Consumer<Engine> register = eng -> eng.register("agent", Object.class, String.class,
                (ctx, in) -> {
                    StreamWriter events = ctx.stream("events");
                    events.append("plan");
                    String text = ctx.step("generate", String.class, () -> {
                        attempts.incrementAndGet();
                        StreamWriter out = ctx.stream("tokens");
                        List<String> prev = out.recorded(String.class);
                        assertEquals(out.recordedCount(), prev.size());
                        if (prev.isEmpty()) {
                            out.append("a");
                            out.append("b");
                            throw new RuntimeException("cut off");
                        }
                        // The workflow's writer, from the step's thread.
                        events.append("generated");
                        out.append("c");
                        return String.join("", prev) + "c";
                    }, StepOptions.defaults().retry(RetryPolicy.attempts(3).initial(Duration.ofMillis(1))
                            .max(Duration.ofMillis(1))));
                    events.append("waiting");
                    ctx.waitFor("go", Object.class);
                    events.append("done");
                    events.close();
                    return text;
                });
        try (Engine e1 = RecoveryTest.local(dir)) {
            register.accept(e1);
            e1.workflow("agent", Object.class, String.class).start("agent-1", null);
            FeaturesTest.waitStatus(e1, "agent-1", "suspended", "parked");
        }
        try (Engine e2 = RecoveryTest.local(dir)) {
            register.accept(e2);
            Run<String> run = e2.run("agent-1", String.class);
            run.signal("go", null);
            assertEquals("abc", run.result());
            assertEquals(2, attempts.get());
            assertEquals(List.of("\"a\"", "\"b\"", "\"c\""), readAll(run.stream("tokens")));
            assertEquals(List.of("\"plan\"", "\"generated\"", "\"waiting\"", "\"done\""), readAll(run.stream("events")));
            assertEquals(7, count(e2, "agent-1", "chunk"));
            assertEquals(2, count(e2, "agent-1", "stream_end"));
        }
    }

    /** An attempt that timed out cannot append after the next one started; an inline step appends directly. */
    @Test
    void staleAttemptsAndInlineSteps() throws Exception {
        CompletableFuture<Throwable> late = new CompletableFuture<>();
        AtomicInteger tries = new AtomicInteger();
        Workflow<Object, Integer> slow = e.register("slow", Object.class, Integer.class, (ctx, in) -> {
            int v = ctx.step("s", Integer.class, () -> {
                StreamWriter out = ctx.stream("tokens");
                if (tries.incrementAndGet() == 1) {
                    try {
                        Thread.sleep(10_000);
                    } catch (InterruptedException timedOut) {
                        // The attempt timed out.
                    }
                    Thread.sleep(20);
                    out.append("late");
                    Thread.sleep(20);
                    try {
                        out.append("later");
                        late.complete(null);
                    } catch (RuntimeException err) {
                        late.complete(err);
                    }
                    return 0;
                }
                out.append("fresh");
                return 2;
            }, StepOptions.defaults().timeout(Duration.ofMillis(30))
                    .retry(RetryPolicy.attempts(2).initial(Duration.ofMillis(1)).max(Duration.ofMillis(1))));
            int w = ctx.step("inline", Integer.class, () -> {
                StreamWriter out = ctx.stream("inline");
                out.append("i");
                out.close();
                return 1;
            }, StepOptions.defaults().inline());
            return v + w;
        });
        Run<Integer> run = slow.start("slow-1", null);
        assertEquals(3, run.result());
        assertNotNull(late.get(10, TimeUnit.SECONDS), "a stale attempt appended");
        assertEquals(List.of("\"fresh\""), readAll(run.stream("tokens")));
        StreamReader r = run.stream("inline");
        assertEquals(List.of("\"i\""), readAll(r));
        assertTrue(r.end().closed());
    }

    /** Workflow code cannot write a stream a step's result completed. */
    @Test
    void ownership() {
        Workflow<Object, String> owner = e.register("owner", Object.class, String.class, (ctx, in) -> {
            ctx.step("s", Integer.class, () -> {
                ctx.stream("mine").append(1);
                return 0;
            });
            try {
                ctx.stream("mine").append(2);
                return "appended";
            } catch (FuseeException err) {
                return err.getMessage();
            }
        });
        assertTrue(owner.start("owner-1", null).result().contains("closed"));
    }

    /** The child half of crashMidStream: five tokens, a reader that saw four, then a wait to be killed. */
    public static void main(String[] args) throws Exception {
        Engine e = RecoveryTest.local(Path.of(args[0]));
        crash(e, out -> {
            for (int i = 0; i < 5; i++) out.append("t" + i);
            Thread.sleep(60_000);
        });
        Run<Integer> run = e.workflow("crash", Object.class, Integer.class).start("crash-1", null);
        StreamReader r = run.stream("tokens");
        for (int i = 0; i < 4; i++) r.next();
        System.out.println("ready " + r.offset());
        System.out.flush();
        Thread.sleep(60_000);
    }

    interface Body {
        void run(StreamWriter out) throws Exception;
    }

    static void crash(Engine e, Body body) {
        e.register("crash", Object.class, Integer.class, (ctx, in) -> ctx.step("generate", Integer.class, () -> {
            StreamWriter out = ctx.stream("tokens");
            body.run(out);
            return (int) out.recordedCount();
        }));
    }

    /**
     * A process killed mid-stream: the step runs again after the restart,
     * finds the durable chunks and continues after them, and a reader that
     * resumes at its offset sees every chunk once.
     */
    @Test
    void crashMidStream() throws Exception {
        e.close();
        e = null;
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String cp = Path.of(Engine.class.getProtectionDomain().getCodeSource().getLocation().toURI()) + File.pathSeparator
                + Path.of(StreamsTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Process p = new ProcessBuilder(java, "-cp", cp, StreamsTest.class.getName(), dir.toString())
                .redirectErrorStream(true).start();
        BlockingQueue<String> lines = new ArrayBlockingQueue<>(100);
        Thread reader = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                for (String l; (l = r.readLine()) != null;) lines.offer(l);
            } catch (IOException ignored) {
                // The process died.
            }
        });
        reader.setDaemon(true);
        reader.start();
        String line;
        StringBuilder seen = new StringBuilder();
        while ((line = lines.poll(60, TimeUnit.SECONDS)) != null && !line.startsWith("ready")) {
            seen.append(line).append('\n');
        }
        assertNotNull(line, "the child never got ready:\n" + seen);
        long offset = Long.parseLong(line.substring("ready ".length()));
        p.destroyForcibly();
        assertTrue(p.waitFor(30, TimeUnit.SECONDS));
        AtomicReference<List<String>> recorded = new AtomicReference<>();
        try (Engine e2 = RecoveryTest.local(dir)) {
            crash(e2, out -> {
                List<String> prev = out.recorded(String.class);
                recorded.set(prev);
                for (int i = prev.size(); i < 10; i++) out.append("t" + i);
            });
            Run<Integer> run = e2.run("crash-1", Integer.class);
            int n = run.result();
            assertTrue(n >= offset && recorded.get().size() == n, n + " recorded, the reader had seen " + offset);
            List<String> rest = readAll(run.stream("tokens", offset));
            assertEquals(tokens((int) offset, 10), rest);
            List<String> all = readAll(run.stream("tokens"));
            Set<String> once = new HashSet<>(all);
            assertEquals(all.size(), once.size(), "a chunk twice: " + all);
            assertEquals(tokens(0, 10), all);
        }
    }

    /**
     * Append cost and reader latency, printed (FUSEE_STREAM_COST=1): 100,000
     * small chunks from one step in memory mode, then chunks appended every
     * 2 ms to a local engine (fsync) and the time from each append to a
     * reader in the same process holding it.
     */
    @Test
    void cost() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("FUSEE_STREAM_COST") != null,
                "set FUSEE_STREAM_COST=1");
        int n = 100_000;
        for (int round = 0; round < 3; round++) {
            Workflow<Integer, Long> bulk = e.register("bulk", Integer.class, Long.class, (ctx, count) ->
                    ctx.step("generate", Long.class, () -> {
                        StreamWriter out = ctx.stream("tokens");
                        long t = System.nanoTime();
                        for (int i = 0; i < count; i++) out.append(i);
                        return System.nanoTime() - t;
                    }));
            long t0 = System.nanoTime();
            long ns = bulk.start("b-" + round, n).result();
            // Until the result: every chunk recorded by the loop as well.
            long whole = System.nanoTime() - t0;
            long t = System.nanoTime();
            long seen = 0;
            for (Chunk c : e.run("b-" + round).stream("tokens")) {
                assertEquals(seen++, c.offset());
            }
            assertEquals(n, seen);
            double read = (System.nanoTime() - t) / 1e9;
            System.out.printf("java append: %.0f chunks/s (%d ns each; %d ns each until the result); read back %.0f"
                    + " chunks/s%n", n / (ns / 1e9), ns / n, whole / n, n / read);
        }
        long[] sent = new long[200];
        try (Engine le = Engine.open(Options.local(dir.toString()).flush("fsync"))) {
            Workflow<Integer, Integer> paced = le.register("paced", Integer.class, Integer.class, (ctx, count) ->
                    ctx.step("generate", Integer.class, () -> {
                        StreamWriter out = ctx.stream("tokens");
                        for (int i = 0; i < count; i++) {
                            Thread.sleep(2);
                            sent[i] = System.nanoTime();
                            out.append(i);
                        }
                        return count;
                    }));
            Run<Integer> run = paced.start("l-1", sent.length);
            long[] lat = new long[sent.length];
            int k = 0;
            for (Chunk c : run.stream("tokens")) lat[k++] = (System.nanoTime() - sent[(int) c.offset()]) / 1000;
            Arrays.sort(lat);
            System.out.printf("java reader latency (append to reader, local fsync): p50 %d us, p90 %d us, p99 %d us%n",
                    lat[lat.length / 2], lat[lat.length * 9 / 10], lat[lat.length * 99 / 100]);
        }
    }

    /** Readers through a server: a client reads a Java worker's stream (the relayed streamRead), stops, continues. */
    @Test
    void remoteReaders() throws Exception {
        e.close();
        e = null;
        ServerTest st = new ServerTest();
        st.dir = dir;
        try {
            String addr = st.startServer();
            try (Engine w = Engine.open(Options.server(addr)); Client c = Client.connect(addr)) {
                w.register("talk", Integer.class, Integer.class, (ctx, n) -> ctx.step("generate", Integer.class, () -> {
                    StreamWriter out = ctx.stream("tokens");
                    for (int i = 0; i < n; i++) {
                        out.append("t" + i);
                        Thread.sleep(3);
                    }
                    out.append(new byte[] {1, 2});
                    return n;
                }));
                c.start("talk", "talk-r", 12);
                StreamReader r = c.run("talk-r").stream("tokens");
                List<String> got = new ArrayList<>();
                while (got.size() < 5 && r.hasNext()) got.add(text(r.next()));
                got.addAll(readAll(c.run("talk-r").stream("tokens", r.offset())));
                List<String> want = tokens(0, 12);
                want.add("bytes:[1, 2]");
                assertEquals(want, got);
                // Several readers at once (each read is its own relayed call).
                List<CompletableFuture<List<String>>> many = new ArrayList<>();
                for (int i = 0; i < 4; i++) {
                    many.add(CompletableFuture.supplyAsync(() -> readAll(c.run("talk-r").stream("tokens"))));
                }
                for (CompletableFuture<List<String>> f : many) assertEquals(want, f.get(30, TimeUnit.SECONDS));
                assertEquals(List.of("\"t11\"", "bytes:[1, 2]"), readAll(w.run("talk-r").stream("tokens", 11)));
                assertThrows(FuseeException.class, () -> c.run("nobody").stream("tokens").hasNext());
                StreamReader done = c.run("talk-r").stream("tokens", 13);
                assertFalse(done.hasNext());
                assertEquals("ok", done.status());
            }
        } finally {
            st.stop();
        }
    }
}
