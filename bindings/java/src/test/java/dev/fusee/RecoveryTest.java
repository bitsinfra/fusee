package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(120)
class RecoveryTest {
    @TempDir
    Path dir;

    static Engine local(Path dir) {
        return Engine.open(Options.local(dir.toString()).keepHistory(true));
    }

    static Workflow<Integer, Integer> pay(Engine e, AtomicInteger charges) {
        return e.register("pay", Integer.class, Integer.class, (ctx, amount) -> {
            int id = ctx.step("charge", Integer.class, () -> {
                charges.incrementAndGet();
                return amount * 10;
            });
            int bonus = ctx.waitFor("go", Integer.class);
            return ctx.step("settle", Integer.class, () -> id + bonus);
        });
    }

    /** A run left unfinished by a clean close resumes in the next engine without running its recorded step again. */
    @Test
    void closeAndReopen() {
        AtomicInteger charges = new AtomicInteger();
        Engine e = local(dir);
        Workflow<Integer, Integer> wf = pay(e, charges);
        wf.start("pay-1", 4);
        FeaturesTest.waitStatus(e, "pay-1", "suspended", "parked");
        e.close();
        FuseeException closed = assertThrows(FuseeException.class, () -> wf.start("pay-2", 1));
        assertEquals("Closed", closed.code());
        try (Engine e2 = local(dir)) {
            pay(e2, charges);
            Run<Integer> run = e2.run("pay-1", Integer.class);
            run.signal("go", 2);
            assertEquals(42, run.result());
            assertEquals(1, charges.get());
        }
    }

    /** The child half of crash(): starts a run, waits until its step is durable, says so, and waits to be killed. */
    public static void main(String[] args) throws Exception {
        Engine e = local(Path.of(args[0]));
        pay(e, new AtomicInteger());
        e.workflow("pay", Integer.class, Integer.class).start("crash-1", 5);
        FeaturesTest.waitStatus(e, "crash-1", "suspended", "parked");
        System.out.println("ready");
        System.out.flush();
        Thread.sleep(60_000);
    }

    private static String classPath() throws Exception {
        String main = Path.of(Engine.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        String test = Path.of(RecoveryTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        return main + File.pathSeparator + test;
    }

    /** A process killed mid-run: the next engine finds the run, replays the recorded step and finishes it. */
    @Test
    void killedProcessReopened() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder pb = new ProcessBuilder(java, "-cp", classPath(), RecoveryTest.class.getName(), dir.toString());
        pb.redirectErrorStream(true);
        Process p = pb.start();
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
        while ((line = lines.poll(60, TimeUnit.SECONDS)) != null && !line.equals("ready")) seen.append(line).append('\n');
        assertEquals("ready", line, "the child never got ready:\n" + seen);
        p.destroyForcibly(); // SIGKILL
        assertTrue(p.waitFor(30, TimeUnit.SECONDS));
        assertEquals(137, p.exitValue());
        AtomicInteger charges = new AtomicInteger();
        try (Engine e = local(dir)) {
            pay(e, charges);
            Run<Integer> run = e.run("crash-1", Integer.class);
            run.signal("go", 7);
            assertEquals(57, run.result());
            assertEquals(0, charges.get(), "the recorded charge ran again");
        }
    }

    /** Code that changed under a recorded run stops it with a determinism error naming both steps; the run waits as "stuck". */
    @Test
    void determinismErrorAfterACodeChange() throws Exception {
        AtomicInteger charges = new AtomicInteger();
        Engine e = local(dir);
        pay(e, charges).start("pay-1", 4);
        FeaturesTest.waitStatus(e, "pay-1", "suspended", "parked");
        e.close();
        BlockingQueue<Throwable> seen = new ArrayBlockingQueue<>(4);
        try (Engine e2 = local(dir)) {
            e2.register("pay", Integer.class, Integer.class, (ctx, amount) -> {
                try {
                    return ctx.step("refund", Integer.class, () -> amount);
                } catch (RuntimeException err) {
                    seen.offer(err);
                    throw err;
                }
            });
            RunDetail d = FeaturesTest.waitStatus(e2, "pay-1", "stuck");
            assertTrue(d.error() != null && d.error().contains("'charge'") && d.error().contains("'refund'"),
                    String.valueOf(d.error()));
            assertInstanceOf(DeterminismException.class, seen.poll(10, TimeUnit.SECONDS));
        }
    }

    /** Payloads are encrypted before they become records; the next engine reads them with the key. */
    @Test
    void encryption() throws Exception {
        byte[] k1 = new byte[32], k2 = new byte[32];
        for (int i = 0; i < 32; i++) {
            k1[i] = (byte) i;
            k2[i] = (byte) (100 + i);
        }
        String secret = "card-4242-4242-secret";
        try (Engine e = Engine.open(Options.local(dir.toString()).keepHistory(true).codecKey("k1", k1))) {
            Workflow<String, String> wf = e.register("vault", String.class, String.class,
                    (ctx, s) -> ctx.step("keep", String.class, () -> s + "!"));
            assertEquals(secret + "!", wf.start("v-1", secret).result());
            BlockingQueue<String> signals = new ArrayBlockingQueue<>(1);
            e.register("waits", String.class, String.class, (ctx, s) -> {
                String v = ctx.waitFor("s", String.class);
                signals.offer(v);
                return v;
            }).start("v-2", secret);
            FeaturesTest.waitStatus(e, "v-2", "suspended", "parked");
        }
        try (Stream<Path> files = Files.walk(dir)) {
            for (Path f : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                String text = new String(Files.readAllBytes(f), StandardCharsets.ISO_8859_1);
                assertFalse(text.contains(secret), "plaintext in " + f);
            }
        }
        // Rotated: k2 encrypts, k1 still decrypts.
        try (Engine e = Engine.open(Options.local(dir.toString()).keepHistory(true).codecKey("k2", k2).codecKey("k1", k1))) {
            e.register("vault", String.class, String.class, (ctx, s) -> ctx.step("keep", String.class, () -> s + "!"));
            e.register("waits", String.class, String.class, (ctx, s) -> ctx.waitFor("s", String.class) + s);
            assertEquals(secret + "!", e.run("v-1", String.class).result());
            e.run("v-2").signal("s", "x");
            assertEquals("x" + secret, e.run("v-2", String.class).result());
        }
        assertThrows(FuseeException.class, () -> Options.memory().codecKey("short", new byte[5]));
    }
}
