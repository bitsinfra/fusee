package dev.fusee;

/** A read-only query handler: it must not change workflow state or call the Ctx. */
@FunctionalInterface
public interface QueryFn<A, R> {
    R query(A arg) throws Exception;
}
