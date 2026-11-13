package dev.fusee;

import java.lang.reflect.Type;

/**
 * One handler of an object type (docs/features.md, "Objects (keyed state)"):
 * exclusive or shared.
 */
public final class ObjectHandler {
    final boolean shared;
    final Type arg;
    final ObjectFn<Object, ?> fn;

    @SuppressWarnings("unchecked")
    private ObjectHandler(boolean shared, Type arg, ObjectFn<?, ?> fn) {
        if (fn == null) throw Errors.invalid("an object handler needs code");
        this.shared = shared;
        this.arg = arg == null ? Object.class : arg;
        this.fn = (ObjectFn<Object, ?>) fn;
    }

    /**
     * A handler that runs one call at a time per key, in the order the calls
     * reach the key, and may change its state. Its writes take effect for
     * everyone else when it returns (with its result, durable together); a
     * handler that fails changes nothing.
     */
    public static <A, R> ObjectHandler exclusive(Class<A> arg, ObjectFn<A, R> fn) {
        return new ObjectHandler(false, arg, fn);
    }

    public static <A, R> ObjectHandler exclusive(TypeRef<A> arg, ObjectFn<A, R> fn) {
        return new ObjectHandler(false, arg.type(), fn);
    }

    /**
     * A handler that runs as soon as its call arrives, next to other calls
     * (an exclusive one included), and only reads the committed state.
     */
    public static <A, R> ObjectHandler shared(Class<A> arg, ObjectFn<A, R> fn) {
        return new ObjectHandler(true, arg, fn);
    }

    public static <A, R> ObjectHandler shared(TypeRef<A> arg, ObjectFn<A, R> fn) {
        return new ObjectHandler(true, arg.type(), fn);
    }

    /** Whether the handler is shared. */
    public boolean isShared() {
        return shared;
    }
}
