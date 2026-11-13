package dev.fusee;

/**
 * An update's validator: it runs before the update is recorded, against the
 * current state; throwing refuses the update (the caller gets a
 * {@link RejectedException} with the message) and records nothing.
 */
@FunctionalInterface
public interface Validator<A> {
    void validate(A arg) throws Exception;
}
