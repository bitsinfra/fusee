package dev.fusee;

/**
 * A workflow type apart from any engine: its name, version, types and code.
 * Define one as a constant, register it on each engine that runs it
 * ({@link Options#workflows}, or {@link #register}), and start runs through
 * the handle that returns.
 *
 * <pre>{@code
 * static final WorkflowDef<String, String> GREET = WorkflowDef.workflow("greet", String.class, String.class,
 *         (ctx, name) -> ctx.step("compose", String.class, () -> "hello " + name));
 * Engine e = Engine.open(Options.memory().workflows(GREET));
 * GREET.on(e).start("g-1", "ana");
 * }</pre>
 */
public final class WorkflowDef<I, O> {
    private final String name;
    private final int version;
    private final TypeRef<I> input;
    private final TypeRef<O> output;
    private final WorkflowFn<I, O> fn;

    private WorkflowDef(String name, int version, TypeRef<I> input, TypeRef<O> output, WorkflowFn<I, O> fn) {
        this.name = name;
        this.version = version;
        this.input = input;
        this.output = output;
        this.fn = fn;
    }

    /** A workflow type apart from any engine. */
    public static <I, O> WorkflowDef<I, O> workflow(String name, Class<I> input, Class<O> output, WorkflowFn<I, O> fn) {
        return new WorkflowDef<>(name, 1, TypeRef.of(input), TypeRef.of(output), fn);
    }

    public static <I, O> WorkflowDef<I, O> workflow(String name, TypeRef<I> input, TypeRef<O> output,
            WorkflowFn<I, O> fn) {
        return new WorkflowDef<>(name, 1, input, output, fn);
    }

    /** The same definition as code version {@code v} (new runs start under the newest registered). */
    public WorkflowDef<I, O> version(int v) {
        return new WorkflowDef<>(name, v, input, output, fn);
    }

    public String name() {
        return name;
    }

    public int version() {
        return version;
    }

    /** Registers the definition on {@code e}; returns the handle that starts its runs. */
    public Workflow<I, O> register(Engine e) {
        return e.register(name, version, input, output, fn);
    }

    /** The handle that starts runs of this type on {@code e} (registered there, or run by another engine of its server). */
    public Workflow<I, O> on(Engine e) {
        return e.workflow(name, input, output);
    }
}
