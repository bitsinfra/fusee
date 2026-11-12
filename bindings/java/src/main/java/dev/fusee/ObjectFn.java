package dev.fusee;

/** An object handler's code: workflow code of the key's run (see {@link ObjectCtx}). */
@FunctionalInterface
public interface ObjectFn<A, R> {
    R call(ObjectCtx ctx, A arg) throws Exception;
}
