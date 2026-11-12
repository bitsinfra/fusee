package dev.fusee;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * A key's state as a handler sees it: JSON values under string keys. Reads
 * are synchronous and not journaled (the engine holds the state next to the
 * run); writes are an exclusive handler's own until it returns. Use it from
 * the handler's workflow code (and code it starts with {@link Ctx#async}).
 */
public final class ObjectState {
    private final Ctx ctx;
    private final boolean exclusive;

    ObjectState(Ctx ctx, boolean exclusive) {
        this.ctx = ctx;
        this.exclusive = exclusive;
    }

    private long flag() {
        return exclusive ? 1 : 0;
    }

    /** A value's JSON as recorded, or null when the key holds none. */
    public Json raw(String key) {
        long[] r = new long[5];
        byte[] data = ctx.op(r, Native.OP_OBJECT_GET, flag(), Codec.utf8(key), null, null);
        if (r[0] == 0) return null;
        return Json.of(Codec.utf8(data));
    }

    /** Whether the key holds a value. */
    public boolean has(String key) {
        return raw(key) != null;
    }

    /** A value decoded as a T, or null when the key holds none. */
    public <T> T get(String key, Class<T> type) {
        return get(key, (Type) type);
    }

    public <T> T get(String key, TypeRef<T> type) {
        return get(key, type.type());
    }

    @SuppressWarnings("unchecked")
    private <T> T get(String key, Type type) {
        long[] r = new long[5];
        byte[] data = ctx.op(r, Native.OP_OBJECT_GET, flag(), Codec.utf8(key), null, null);
        if (r[0] == 0) return null;
        return (T) ctx.exec().e.codec.decode(data, type);
    }

    private void write(String key, byte[] value, boolean set) {
        if (!exclusive) throw new RejectedException("a shared handler cannot change state");
        ctx.op(null, Native.OP_OBJECT_SET, set ? 1 : 0, Codec.utf8(key), value, null);
    }

    /**
     * Sets a value (JSON; null, or anything that encodes as null, clears the
     * key). Only an exclusive handler may; the change is committed when it
     * returns.
     */
    public void set(String key, Object value) {
        if (value instanceof byte[]) throw Errors.invalid("state values are JSON, not bytes");
        byte[] p = ctx.exec().e.codec.encode(value);
        if ("null".equals(Codec.utf8(p))) {
            write(key, null, false);
            return;
        }
        write(key, p, true);
    }

    /** Clears a key. */
    public void clear(String key) {
        write(key, null, false);
    }

    /** Clears every key. */
    public void clearAll() {
        for (String k : keys()) clear(k);
    }

    /** The keys that hold a value, sorted. */
    public List<String> keys() {
        byte[] data = ctx.op(null, Native.OP_OBJECT_KEYS, flag(), null, null, null);
        List<String> out = new ArrayList<>();
        if (data == null || data.length == 0) return out;
        Object tree = MiniJson.parseTree(Codec.utf8(data));
        if (tree instanceof List<?> l) {
            for (Object o : l) out.add(String.valueOf(o));
        }
        return out;
    }
}
