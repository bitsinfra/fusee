package dev.fusee;

import java.lang.reflect.Type;

/** A key of an object type, called from outside workflow code. */
public final class ObjectRef {
    private final Engine e;
    private final String type, key;

    ObjectRef(Engine e, String type, String key) {
        this.e = e;
        this.type = type;
        this.key = key;
    }

    public String type() {
        return type;
    }

    public String key() {
        return key;
    }

    /** The key's workflow ID ("type/key"). */
    public String id() {
        return type + "/" + key;
    }

    private byte[] send(String handler, Object arg, String id) {
        Keyed.checkType(type);
        byte[] data = e.codec.encode(arg);
        StartOptions o = new StartOptions().ifRunning("use-existing").updateId(id);
        return e.updateWithStartWire(type, 1, id(), e.codec.encode(null), handler, data, o);
    }

    /** Calls a handler and waits for its durable result. */
    public <R> R call(String handler, Object arg, Class<R> result) {
        return call(handler, arg, (Type) result, null);
    }

    public <R> R call(String handler, Object arg, Class<R> result, CallOptions opts) {
        return call(handler, arg, (Type) result, opts);
    }

    public <R> R call(String handler, Object arg, TypeRef<R> result, CallOptions opts) {
        return call(handler, arg, result.type(), opts);
    }

    /** Calls a handler and returns its result as JSON. */
    public Json call(String handler, Object arg) {
        return call(handler, arg, Json.class, null);
    }

    public Json call(String handler, Object arg, CallOptions opts) {
        return call(handler, arg, Json.class, opts);
    }

    @SuppressWarnings("unchecked")
    private <R> R call(String handler, Object arg, Type result, CallOptions opts) {
        byte[] p = send(handler, arg, opts == null ? "" : opts.id);
        return (R) e.codec.decode(p, result);
    }

    /** Calls a handler without waiting for its result: returns once the call is accepted. */
    public void send(String handler, Object arg) {
        send(handler, arg, (CallOptions) null);
    }

    public void send(String handler, Object arg, CallOptions opts) {
        send(handler, arg, Keyed.SEND_PREFIX + (opts == null ? "" : opts.id));
    }

    /**
     * Edits the state from outside its handlers: an exclusive call, so it
     * waits for the key and commits as a handler's writes do. The edit has
     * clearAll, clear (fields), set (fields and values; null removes one)
     * and patch (a JSON Patch over the state as one object), applied in that
     * order; ifVersion (null: any) refuses it unless the state is still at
     * that version. Returns {"version", "changed"}.
     */
    public Json editState(java.util.Map<String, Object> edit, String ifVersion) {
        return call(Keyed.EDIT_HANDLER, Keyed.edit(edit, ifVersion));
    }

    /** Sets one field of the state. */
    public Json setState(String field, Object value) {
        return editState(Keyed.set(field, value), null);
    }

    /** Applies a JSON Patch (RFC 6902) to the state as one JSON object. */
    public Json patchState(java.util.List<java.util.Map<String, Object>> ops) {
        return editState(java.util.Map.of("patch", ops), null);
    }

    /** Clears one field, or (null) the whole state; the key remains. */
    public Json clearState(String field) {
        return editState(Keyed.clear(field), null);
    }

    /** The committed state, a JSON object ({} for a key never called), answered once it is durable. */
    public Json state() {
        try {
            return new Json(Codec.utf8(e.query(id(), Keyed.STATE_QUERY, null)));
        } catch (RuntimeException err) {
            if (Keyed.notFound(err)) return Json.of(Keyed.EMPTY_STATE);
            throw err;
        }
    }

    /** The key's run: its journal, and in object() its state and pending calls; null for a key never called. */
    public RunDetail inspect() {
        return e.run(id()).inspect();
    }

    /** Deletes the key: its run ends and its state with it. The next call starts with empty state. */
    public void delete() {
        try {
            e.run(id()).terminate("object deleted");
        } catch (RuntimeException err) {
            if (!Keyed.notFound(err)) throw err;
        }
    }

    @Override
    public String toString() {
        return "ObjectRef(" + id() + ")";
    }
}
