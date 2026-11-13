package dev.fusee;

import java.util.LinkedHashMap;
import java.util.Map;

/** A key of an object type reached through a server ({@link Client#object}). */
public final class RemoteObjectRef {
    private final Client c;
    private final String type, key;

    RemoteObjectRef(Client c, String type, String key) {
        this.c = c;
        this.type = type;
        this.key = key;
    }

    /** The key's workflow ID ("type/key"). */
    public String id() {
        return type + "/" + key;
    }

    /** Calls a handler and waits for its durable result. */
    public Json call(String handler, Object arg) {
        return call(handler, arg, (CallOptions) null);
    }

    public Json call(String handler, Object arg, CallOptions opts) {
        Keyed.checkType(type);
        return c.updateWithStart(type, id(), null, handler, arg,
                new StartOptions().updateId(opts == null ? "" : opts.id));
    }

    public <R> R call(String handler, Object arg, Class<R> result, CallOptions opts) {
        return call(handler, arg, opts).as(result);
    }

    /** Calls a handler without waiting for its result: returns once the call is accepted. */
    public void send(String handler, Object arg) {
        send(handler, arg, null);
    }

    public void send(String handler, Object arg, CallOptions opts) {
        Keyed.checkType(type);
        c.updateWithStart(type, id(), null, handler, arg,
                new StartOptions().updateId(Keyed.SEND_PREFIX + (opts == null ? "" : opts.id)));
    }

    /** Edits the state from outside its handlers (see {@link ObjectRef#editState}). */
    public Json editState(Map<String, Object> edit, String ifVersion) {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("id", id());
        a.put("edit", Keyed.edit(edit, ifVersion));
        a.put("updateId", "");
        return c.call("editState", a);
    }

    public Json setState(String field, Object value) {
        return editState(Keyed.set(field, value), null);
    }

    public Json patchState(java.util.List<Map<String, Object>> ops) {
        return editState(Map.of("patch", ops), null);
    }

    public Json clearState(String field) {
        return editState(Keyed.clear(field), null);
    }

    /** The committed state ({} for a key never called). */
    public Json state() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("id", id());
        a.put("name", Keyed.STATE_QUERY);
        a.put("arg", null);
        try {
            return c.call("query", a);
        } catch (RuntimeException err) {
            if (Keyed.notFound(err)) return Json.of(Keyed.EMPTY_STATE);
            throw err;
        }
    }
}
