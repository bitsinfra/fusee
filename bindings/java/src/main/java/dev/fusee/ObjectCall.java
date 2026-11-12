package dev.fusee;

import java.lang.reflect.Type;

/**
 * A key of an object type called from workflow code (or from another
 * object's handler): {@link Ctx#object}. Each call is a journal operation,
 * sent through the durable outbox and run exactly once.
 */
public final class ObjectCall {
    private final Ctx c;
    private final String type, key;

    ObjectCall(Ctx c, String type, String key) {
        this.c = c;
        this.type = type;
        this.key = key;
    }

    private long begin(String handler, Object arg, boolean oneWay) {
        Keyed.checkType(type);
        byte[] data = c.exec().e.codec.encode(arg);
        long[] r = new long[5];
        c.op(r, Native.OP_BEGIN_CALL, oneWay ? 1 : 0, Codec.utf8(type + "/" + key), Codec.utf8(handler), data);
        c.exec().e.kicked = true;
        return r[0];
    }

    /** Calls a handler without waiting: the Op's result is the handler's. */
    public <R> Op<R> startCall(String handler, Object arg, Class<R> result) {
        return startCall(handler, arg, (Type) result);
    }

    public <R> Op<R> startCall(String handler, Object arg, TypeRef<R> result) {
        return startCall(handler, arg, result.type());
    }

    private <R> Op<R> startCall(String handler, Object arg, Type result) {
        long seq;
        try {
            seq = begin(handler, arg, false);
        } catch (RuntimeException err) {
            return Ctx.failedOp(c.exec(), err);
        }
        return c.childOp(type + "." + handler, seq, result);
    }

    /** Calls a handler and waits for its result. */
    public <R> R call(String handler, Object arg, Class<R> result) {
        return this.<R>startCall(handler, arg, (Type) result).get();
    }

    public <R> R call(String handler, Object arg, TypeRef<R> result) {
        return this.<R>startCall(handler, arg, result.type()).get();
    }

    /** Calls a handler and waits for its result, as JSON. */
    public Json call(String handler, Object arg) {
        return call(handler, arg, Json.class);
    }

    /** Calls a handler without waiting for it: delivered once this point of the journal is durable, run once. */
    public void send(String handler, Object arg) {
        begin(handler, arg, true);
    }
}
