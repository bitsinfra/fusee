package dev.fusee;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * The worker side of invocations over HTTP, suspend to zero
 * (docs/server-mode.md, "Invoked workers"): a Fusee server started with
 * {@code --invoke URL} POSTs to that URL when shards no worker holds have
 * work due; each request opens a fresh engine for the invocation's shards,
 * runs them until they are idle or the time budget ends, and closes it.
 * Serve it with {@link #httpHandler} on the JDK's
 * {@code com.sun.net.httpserver.HttpServer}, or call {@link #invoke} from
 * any framework (or a Lambda handler).
 */
public final class InvocationHandler {
    /** The request header that carries the signature: t=&lt;unix ms&gt;,v1=&lt;hex HMAC-SHA256&gt;. */
    public static final String SIGNATURE_HEADER = "fusee-signature";

    private static final Pattern ID = Pattern.compile("^[0-9a-f]{16}$");

    /** An HTTP answer: a status and a JSON body. */
    public record Reply(int status, String body) {}

    private final String address;
    private final byte[] secret;
    private final Supplier<Options> options;
    private final Consumer<Engine> register;

    /**
     * address: the server with this worker's connection options
     * ({@code host:port?ca=...&token-file=...}); secret: the endpoint's
     * signing secret (the server's {@code --invoke-secret-file}; null accepts
     * unsigned requests, only behind something that authenticates); options:
     * the engine options of each invocation (durability and address are set
     * here; parkAfter defaults to 50 ms), or null; register: registers the
     * workflows on each invocation's fresh engine.
     */
    public InvocationHandler(String address, byte[] secret, Supplier<Options> options, Consumer<Engine> register) {
        if (secret != null && secret.length < 16) {
            throw Errors.invalid("an invocation secret needs at least 16 bytes; use 32 random bytes");
        }
        if (address == null || address.isEmpty() || register == null) {
            throw Errors.invalid("an invocation handler needs an address and a register function");
        }
        this.address = address;
        this.secret = secret == null ? null : secret.clone();
        this.options = options;
        this.register = register;
    }

    /** Checks a fusee-signature value against the body, within five minutes of nowMs; null if it holds. */
    public static String verifySignature(byte[] secret, String header, byte[] body, long nowMs) {
        Long t = null;
        List<String> sigs = new java.util.ArrayList<>();
        for (String part : header.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length != 2 || kv[1].isEmpty()) continue;
            if (kv[0].equals("t")) {
                try {
                    t = Long.parseLong(kv[1]);
                } catch (NumberFormatException e) {
                    // No timestamp.
                }
            } else if (kv[0].equals("v1")) {
                sigs.add(kv[1]);
            }
        }
        if (t == null) return "the signature has no timestamp";
        if (Math.abs(nowMs - t) > 300_000) return "the signature is too old (or the clocks differ by over five minutes)";
        byte[] want;
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            mac.update((t + ".").getBytes(StandardCharsets.US_ASCII));
            want = mac.doFinal(body);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            return "HMAC-SHA256: " + e.getMessage();
        }
        for (String s : sigs) {
            try {
                if (MessageDigest.isEqual(HexFormat.of().parseHex(s), want)) return null;
            } catch (IllegalArgumentException e) {
                // Not hex: not this signature.
            }
        }
        return "the signature does not match";
    }

    private static Reply json(int status, Map<String, Object> v) {
        return new Reply(status, Json.from(v).text());
    }

    private static Reply error(int status, String msg) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("error", msg);
        return json(status, m);
    }

    /** Verifies a request, runs its invocation and answers: 200 with a summary, 401, 400 or 503. */
    public Reply invoke(String signature, byte[] body) {
        if (secret != null) {
            if (signature == null || signature.isEmpty()) return error(401, "unsigned request");
            String problem = verifySignature(secret, signature, body, System.currentTimeMillis());
            if (problem != null) return error(401, problem);
        }
        Object id;
        try {
            Object tree = MiniJson.parseTree(new String(body, StandardCharsets.UTF_8));
            id = tree instanceof Map<?, ?> m ? m.get("invocation") : null;
        } catch (RuntimeException e) {
            return error(400, Errors.message(e));
        }
        if (!(id instanceof String s) || !ID.matcher(s).matches() || s.chars().allMatch(ch -> ch == '0')) {
            return error(400, "invalid invocation ID");
        }
        String invocation = (String) id;
        long began = System.nanoTime();
        Options o = options == null ? new Options() : options.get();
        if (o.parkAfter == null) o.parkAfter(java.time.Duration.ofMillis(50));
        o.durability("server").address(address + (address.contains("?") ? "&" : "?") + "invocation=" + invocation);
        Engine e;
        try {
            e = Engine.open(o);
        } catch (RuntimeException err) {
            String msg = Errors.message(err);
            // The server refused: the invocation is over or its shards are held.
            if (msg.contains("refused this worker")) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("invocation", invocation);
                m.put("skipped", msg);
                return json(200, m);
            }
            return error(503, msg);
        }
        double openMs = (System.nanoTime() - began) / 1e6;
        String outcome = "running";
        String err = null;
        try {
            register.accept(e);
            for (;;) {
                String state = e.invocationState();
                outcome = state.isEmpty() ? "idle" : state;
                if (!outcome.equals("running")) break;
                Thread.sleep(5);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            err = "interrupted";
        } catch (RuntimeException ex) {
            err = Errors.message(ex);
        }
        long steps = 0;
        try {
            for (Map<String, Long> m : e.shardMetrics()) steps += m.getOrDefault("steps_run", 0L);
        } catch (RuntimeException ignored) {
            // The engine closed itself: its connection to the server is gone.
        }
        try {
            e.close();
        } catch (RuntimeException ex) {
            if (err == null) err = Errors.message(ex);
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("invocation", invocation);
        m.put("outcome", outcome);
        m.put("steps", steps);
        m.put("openMs", openMs);
        m.put("ms", (System.nanoTime() - began) / 1e6);
        if (err != null) m.put("error", err);
        return json(200, m);
    }

    /** A handler for the JDK's com.sun.net.httpserver.HttpServer. */
    public HttpHandler httpHandler() {
        return this::serve;
    }

    private void serve(HttpExchange x) throws IOException {
        byte[] body;
        try (InputStream in = x.getRequestBody()) {
            body = in.readNBytes(1 << 20);
        }
        Reply r;
        try {
            r = invoke(x.getRequestHeaders().getFirst(SIGNATURE_HEADER), body);
        } catch (RuntimeException e) {
            r = error(500, Errors.message(e));
        }
        byte[] out = r.body().getBytes(StandardCharsets.UTF_8);
        x.getResponseHeaders().set("content-type", "application/json");
        x.sendResponseHeaders(r.status(), out.length);
        try (OutputStream os = x.getResponseBody()) {
            os.write(out);
        }
    }
}
