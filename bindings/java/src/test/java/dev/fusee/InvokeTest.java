package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Invoked workers in the Java SDK: a Fusee server started with --invoke
 * POSTs to the JDK's HttpServer, whose handler opens a fresh engine per
 * invocation; workflows sleep, take signals and finish with no engine open
 * between invocations.
 */
@Timeout(120)
class InvokeTest {
    static final byte[] SECRET = "an invocation secret of 32 bytes".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path dir;

    Process server;
    HttpServer http;

    @AfterEach
    void stop() throws Exception {
        if (http != null) http.stop(0);
        if (server != null) {
            server.destroyForcibly().waitFor(8, TimeUnit.SECONDS);
        }
    }

    static String sign(byte[] body, long t) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET, "HmacSHA256"));
        mac.update((t + ".").getBytes(StandardCharsets.US_ASCII));
        return "t=" + t + ",v1=" + HexFormat.of().formatHex(mac.doFinal(body));
    }

    @Test
    void signatures() throws Exception {
        byte[] body = "{\"version\":1}".getBytes(StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();
        assertNull(InvocationHandler.verifySignature(SECRET, sign(body, now), body, now));
        assertTrue(InvocationHandler.verifySignature(SECRET, sign(body, now), "{}".getBytes(), now).contains("does not match"));
        assertTrue(InvocationHandler.verifySignature(SECRET, sign(body, now - 600_000), body, now).contains("too old"));
        assertTrue(InvocationHandler.verifySignature(SECRET, "v1=00", body, now).contains("no timestamp"));
    }

    @Test
    void badRequestsAreRefusedBeforeConnecting() throws Exception {
        AtomicInteger opened = new AtomicInteger();
        InvocationHandler h = new InvocationHandler("127.0.0.1:1", SECRET, null, e -> opened.incrementAndGet());
        assertThrows(FuseeException.class, () -> new InvocationHandler("x", "short".getBytes(), null, e -> {}));
        byte[] body = "{\"version\":1,\"invocation\":\"00000000000000aa\",\"shards\":[0],\"budgetMs\":1000}"
                .getBytes(StandardCharsets.UTF_8);
        assertEquals(401, h.invoke(null, body).status());
        byte[] changed = (new String(body, StandardCharsets.UTF_8) + " ").getBytes(StandardCharsets.UTF_8);
        assertEquals(401, h.invoke(sign(body, System.currentTimeMillis()), changed).status());
        byte[] empty = "{}".getBytes(StandardCharsets.UTF_8);
        assertEquals(400, h.invoke(sign(empty, System.currentTimeMillis()), empty).status());
        assertEquals(0, opened.get());
    }

    @Test
    void workflowsSleepTakeSignalsAndFinishWithNoEngineBetweenInvocations() throws Exception {
        String cli = System.getProperty("fusee.cli", "../../target/release/fusee");
        File f = new File(cli);
        assumeTrue(f.canExecute(), "no fusee CLI at " + cli + " (cargo build --release -p fusee-cli)");
        Path secret = dir.resolve("secret");
        Files.write(secret, SECRET);
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        String address = "127.0.0.1:" + port;
        AtomicInteger opened = new AtomicInteger();
        Map<String, Integer> steps = new ConcurrentHashMap<>();
        InvocationHandler handler = new InvocationHandler(address, SECRET, null, e -> {
            opened.incrementAndGet();
            e.register("sleeper", Integer.class, Integer.class, (ctx, n) -> {
                int a = ctx.step("first", Integer.class, () -> {
                    steps.merge(ctx.workflowId() + "/first", 1, Integer::sum);
                    return n + 1;
                });
                ctx.sleep("nap", Duration.ofMillis(1200));
                int go = ctx.waitFor("go", Integer.class);
                return ctx.step("second", Integer.class, () -> {
                    steps.merge(ctx.workflowId() + "/second", 1, Integer::sum);
                    return a * 10 + go;
                });
            });
        });
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/invoke", handler.httpHandler());
        http.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        http.start();
        Path log = dir.resolve("server.log");
        server = new ProcessBuilder(f.getAbsolutePath(), "server", "--dir", dir.resolve("data").toString(), "--listen",
                address, "--shards", "2", "--invoke", "http://127.0.0.1:" + http.getAddress().getPort() + "/invoke",
                "--invoke-secret-file", secret.toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        for (int i = 0; i < 200; i++) {
            try (Socket c = new Socket()) {
                c.connect(new InetSocketAddress("127.0.0.1", port), 100);
                break;
            } catch (java.io.IOException notYet) {
                Thread.sleep(20);
            }
        }
        try (Client c = Client.connect(address)) {
            List<String> ids = List.of("s-0", "s-1", "s-2");
            for (int k = 0; k < ids.size(); k++) c.start("sleeper", ids.get(k), k);
            // The runs sleep with nobody up, then wait for their signals.
            Thread.sleep(2500);
            for (int k = 0; k < ids.size(); k++) {
                c.run(ids.get(k)).signal("go", k + 5);
                assertEquals(String.valueOf((k + 1) * 10 + k + 5), c.run(ids.get(k)).result().text());
            }
            for (String id : ids) {
                assertEquals(1, steps.get(id + "/first"), id);
                assertEquals(1, steps.get(id + "/second"), id);
            }
            assertTrue(opened.get() >= 2, opened.get() + " engines");
            Map<?, ?> m = (Map<?, ?>) c.call("invocations", Map.of()).value();
            Map<?, ?> e = (Map<?, ?>) ((List<?>) m.get("endpoints")).get(0);
            long bad = ((Number) e.get("failed")).longValue() + ((Number) e.get("timedOut")).longValue()
                    + ((Number) e.get("unleased")).longValue();
            assertEquals(0, bad, m.toString());
        }
    }
}
