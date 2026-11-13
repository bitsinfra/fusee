package dev.fusee;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Server mode: calls a Client sent through the server reach the worker
 * holding the shards, here, and are answered with the engine's own API.
 * Each runs on a thread of its own (it may wait for a run's result).
 */
final class Remote {
    private Remote() {}

    @SuppressWarnings("unchecked")
    static void handle(Engine e, byte[] msg) {
        Map<String, Object> c;
        long relay;
        try {
            c = (Map<String, Object>) MiniJson.parseTree(Codec.utf8(msg));
            relay = Long.parseUnsignedLong(String.valueOf(c.get("relay")));
        } catch (RuntimeException bad) {
            return;
        }
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            Object args = c.get("args");
            Json value = serve(e, String.valueOf(c.get("method")),
                    args instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of());
            response.put("value", value == null ? Json.of("null") : value);
        } catch (Throwable t) {
            response.put("error", Errors.reply(t));
            response.put("kind", Errors.kind(t));
        }
        byte[] out = MiniJson.INSTANCE.encode(response);
        try {
            e.hold(() -> {
                try {
                    Native.replyCall(e.h, relay, out);
                } catch (RuntimeException err) {
                    e.stopAfterFailure(err);
                }
                return null;
            });
        } catch (FuseeException closed) {
            // The engine closed meanwhile.
        }
    }

    private static String str(Map<String, Object> a, String k) {
        Object v = a.get(k);
        return v == null ? "" : String.valueOf(v);
    }

    private static byte[] json(Map<String, Object> a, String k) {
        return MiniJson.INSTANCE.encode(a.get(k));
    }

    @SuppressWarnings("unchecked")
    private static Json serve(Engine e, String method, Map<String, Object> a) {
        String id = str(a, "id");
        StartOptions o = new StartOptions();
        if (a.get("version") instanceof Number n) o.version(n.intValue());
        if (a.get("options") instanceof Map<?, ?> opts) {
            if (opts.get("timeout") instanceof Number t) o.timeoutMs = t.longValue();
            if (opts.get("idPolicy") instanceof Map<?, ?> p) {
                if (p.get("running") instanceof String r) o.ifRunning(r);
                if (p.get("finished") instanceof String f) o.ifFinished(f);
            }
        }
        switch (method) {
            case "start":
                e.startWire(str(a, "type"), 1, id, json(a, "input"), o);
                return null;
            case "updateWithStart":
                o.updateId(str(a, "updateId"));
                return Json.of(Codec.raw(e.updateWithStartWire(str(a, "type"), 1, id, json(a, "input"), str(a, "name"),
                        json(a, "arg"), o)));
            case "signal":
                e.signalWire(id, str(a, "name"), json(a, "value"));
                return null;
            case "query":
                return Json.of(Codec.raw(e.query(id, str(a, "name"), json(a, "arg"))));
            case "update":
                return Json.of(Codec.raw(e.updateWire(id, str(a, "name"), str(a, "updateId"), json(a, "arg"))));
            case "result": {
                Engine.ResultMsg m = e.result(id, -1);
                FuseeException err = Errors.settled(m.status(), m.payload());
                if (err != null) throw err;
                return Json.of(Codec.raw(m.payload()));
            }
            case "complete":
                e.complete(str(a, "token"), Json.of(Codec.utf8(json(a, "value"))));
                return null;
            case "fail":
                e.fail(str(a, "token"), str(a, "error"));
                return null;
            case "cancel":
                e.cancelWire(id, str(a, "reason"), false);
                return null;
            case "resume":
                e.resumeWire(id);
                return null;
            case "terminate":
                e.cancelWire(id, str(a, "reason"), true);
                return null;
            case "reset": {
                boolean reapply = !(a.get("reapplySignals") instanceof Boolean b) || b;
                e.resetWire(id, str(a, "beforeStep"), reapply, o.version);
                return null;
            }
            case "list": {
                ListOptions l = new ListOptions();
                if (a.get("limit") instanceof Number n && n.intValue() > 0) l.limit(n.intValue());
                if (a.get("where") instanceof Map<?, ?> w) l.where((Map<String, ?>) w);
                List<RunSummary> runs = e.list(l);
                return Json.from(runs);
            }
            case "streamRead": {
                // One read (a long poll of at most a minute): the reader's JSON.
                long run = a.get("run") instanceof Number r ? r.longValue() : -1;
                long from = a.get("from") instanceof Number f ? f.longValue() : 0;
                long wait = a.get("waitMs") instanceof Number w ? Math.min(Math.max(0, w.longValue()), 60_000) : 30_000;
                return Json.of(Codec.utf8(e.streamReadWire(id, run, str(a, "name"), from, wait)));
            }
            default:
                throw Errors.failed("unknown remote call '" + method + "'");
        }
    }
}
