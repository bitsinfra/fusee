package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Server mode against `fusee server` (the system property fusee.cli, else ../../target/release/fusee). */
@Timeout(120)
class ServerTest {
    @TempDir
    Path dir;

    Process server;

    @AfterEach
    void stop() throws Exception {
        if (server != null) {
            server.destroy();
            if (!server.waitFor(8, TimeUnit.SECONDS)) server.destroyForcibly().waitFor();
        }
    }

    String startServer() throws Exception {
        String cli = System.getProperty("fusee.cli", "../../target/release/fusee");
        File f = new File(cli);
        assumeTrue(f.canExecute(), "no fusee CLI at " + cli + " (cargo build --release -p fusee-cli)");
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        String addr = "127.0.0.1:" + port;
        Path log = dir.resolve("server.log");
        server = new ProcessBuilder(f.getAbsolutePath(), "server", "--dir", dir.resolve("data").toString(), "--listen",
                addr, "--shards", "2").redirectErrorStream(true).redirectOutput(log.toFile()).start();
        for (int i = 0; i < 200 && server.isAlive(); i++) {
            try (Socket c = new Socket()) {
                c.connect(new InetSocketAddress("127.0.0.1", port), 100);
                return addr;
            } catch (java.io.IOException notYet) {
                Thread.sleep(20);
            }
        }
        throw new AssertionError("the server did not listen on " + addr + ":\n" + java.nio.file.Files.readString(log));
    }

    @Test
    void serverModeWithAClient() throws Exception {
        String addr = startServer();
        try (Engine e = Engine.open(Options.server(addr))) {
            assertEquals(2, e.shards());
            Workflow<Integer, List<Object>> approval = e.register("approval", TypeRef.of(Integer.class),
                    new TypeRef<List<Object>>() {}, (ctx, initial) -> {
                        int[] count = {initial};
                        String[] token = {""};
                        ctx.onUpdate("add", Integer.class, n -> {
                            count[0] += n;
                            return count[0];
                        });
                        ctx.onQuery("count", () -> count[0]);
                        ctx.onQuery("token", () -> token[0]);
                        int v = ctx.step("double", Integer.class, () -> initial * 2);
                        // Inline: the send function writes workflow state, so it runs
                        // holding the engine rather than on a thread of its own.
                        boolean approved = ctx.awaitCompletion("decision", Boolean.class, t -> token[0] = t,
                                StepOptions.defaults().inline());
                        return List.of(count[0], v, approved);
                    });
            // The worker's own API.
            Run<List<Object>> run = approval.start("local-1", 1);
            String token = "";
            for (int i = 0; i < 500 && token.isEmpty(); i++) {
                token = run.query("token", String.class);
                Thread.sleep(5);
            }
            e.complete(token, true);
            assertEquals(List.of(1, 2, true), run.result());
            // A client reaching this worker through the server.
            try (Client c = Client.connect(addr)) {
                Json v = c.updateWithStart("approval", "remote-1", 4, "add", 3, new StartOptions().updateId("once"));
                assertEquals("7", v.text());
                RemoteRun rr = c.run("remote-1");
                assertEquals("7", rr.update("add", 3, UpdateOptions.id("once")).text());
                String t = "";
                for (int i = 0; i < 500 && t.isEmpty(); i++) {
                    t = rr.query("token", null, String.class);
                    Thread.sleep(5);
                }
                c.complete(t, true);
                assertEquals("[7,8,true]", rr.result().text());
                assertEquals(2, c.list(ListOptions.where("status", "ok")).size());
                FuseeException nf = assertThrows(FuseeException.class, () -> c.run("nope").result());
                assertEquals("NotFound", nf.code());
                c.start("approval", "remote-2", 5);
                RemoteRun r2 = c.run("remote-2");
                r2.terminate("stop");
                assertThrows(FuseeException.class, r2::result);
            }
        }
        // A fresh engine on the same server finds the runs (after the lease moves to it).
        try (Engine e2 = Engine.open(Options.server(addr))) {
            assertEquals(List.of(1, 2, true), e2.run("local-1", new TypeRef<List<Object>>() {}).result());
        }
    }
}
