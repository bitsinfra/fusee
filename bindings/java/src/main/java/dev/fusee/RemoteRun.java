package dev.fusee;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/** A run reached through a server (Client.run). */
public final class RemoteRun {
    private final Client c;
    private final String id;

    RemoteRun(Client c, String id) {
        this.c = c;
        this.id = id;
    }

    public String id() {
        return id;
    }

    private Map<String, Object> args() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("id", id);
        return a;
    }

    /** Waits for the run's result. */
    public Json result() {
        return c.call("result", args());
    }

    public <O> O result(Class<O> type) {
        return result().as(type);
    }

    /** Waits until a run with this ID exists. */
    public void started() {
        started(null);
    }

    public void started(Duration timeout) {
        long deadline = timeout == null ? Long.MAX_VALUE : System.nanoTime() + timeout.toNanos();
        long pause = 1;
        while (c.list(ListOptions.where("id", id).limit(1)).isEmpty()) {
            if (System.nanoTime() > deadline) throw new TimedOutException("run '" + id + "' did not start");
            try {
                Thread.sleep(pause);
            } catch (InterruptedException ie) {
                throw Errors.interrupted();
            }
            pause = Math.min(pause * 2, 50);
        }
    }

    public void signal(String name, Object value) {
        Map<String, Object> a = args();
        a.put("name", name);
        a.put("value", c.value(value));
        c.call("signal", a);
    }

    public Json query(String name, Object arg) {
        Map<String, Object> a = args();
        a.put("name", name);
        a.put("arg", c.value(arg));
        return c.call("query", a);
    }

    public <R> R query(String name, Object arg, Class<R> type) {
        return query(name, arg).as(type);
    }

    public Json update(String name, Object arg) {
        return update(name, arg, null);
    }

    public Json update(String name, Object arg, UpdateOptions opts) {
        Map<String, Object> a = args();
        a.put("name", name);
        a.put("arg", c.value(arg));
        a.put("updateId", opts == null ? "" : opts.id);
        return c.call("update", a);
    }

    public void cancel(String reason) {
        Map<String, Object> a = args();
        a.put("reason", reason);
        c.call("cancel", a);
    }

    /** Resumes a paused run: its step runs again with a fresh set of attempts. */
    public void resume() {
        c.call("resume", args());
    }

    public void terminate(String reason) {
        Map<String, Object> a = args();
        a.put("reason", reason);
        c.call("terminate", a);
    }

    /** A reader of the run's stream name from its first chunk, answered by the worker holding its shard. */
    public StreamReader stream(String name) {
        return stream(name, 0);
    }

    /** A reader of the run's stream name from offset from, answered by the worker holding its shard. */
    public StreamReader stream(String name, long from) {
        return new StreamReader((run, at, waitMs) -> {
            Map<String, Object> a = args();
            a.put("run", run < 0 ? null : run);
            a.put("name", name);
            a.put("from", at);
            a.put("waitMs", waitMs);
            return c.call("streamRead", a).text();
        }, from);
    }

    public void reset(String beforeStep, boolean reapplySignals) {
        Map<String, Object> a = args();
        a.put("beforeStep", beforeStep);
        a.put("reapplySignals", reapplySignals);
        c.call("reset", a);
    }
}
