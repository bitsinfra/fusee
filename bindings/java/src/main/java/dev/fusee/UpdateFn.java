package dev.fusee;

/** An update handler: workflow code (it may run steps), run once per accepted update. */
@FunctionalInterface
public interface UpdateFn<A, R> {
    R update(A arg) throws Exception;
}
