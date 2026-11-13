package dev.fusee;

/** A step's function that takes its {@link StepCtx}: {@code ctx.step("charge", Charge.class, s -> pay(s.key()))}. */
@FunctionalInterface
public interface StepFn<T> {
    T run(StepCtx s) throws Exception;
}
