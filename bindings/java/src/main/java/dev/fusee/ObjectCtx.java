package dev.fusee;

/**
 * What an object's handler gets: the workflow context of the key's run
 * ({@link #ctx}: steps, sleeps, children, calls to other objects all work),
 * its state and its key. A handler is workflow code: the rules of
 * {@link Ctx} apply.
 */
public final class ObjectCtx {
    private final Ctx ctx;
    private final boolean exclusive;
    private final ObjectState state;

    ObjectCtx(Ctx ctx, boolean exclusive) {
        this.ctx = ctx;
        this.exclusive = exclusive;
        this.state = new ObjectState(ctx, exclusive);
    }

    /** The key's workflow context. */
    public Ctx ctx() {
        return ctx;
    }

    /** The object type. */
    public String type() {
        return ctx.workflowType();
    }

    /** The object's key. */
    public String key() {
        String id = ctx.workflowId();
        int at = ctx.workflowType().length() + 1;
        return at <= id.length() ? id.substring(at) : id;
    }

    /** Whether the handler is exclusive (it may change state). */
    public boolean isExclusive() {
        return exclusive;
    }

    /**
     * The key's state. In an exclusive handler it includes the handler's own
     * writes; in a shared one it is the committed state.
     */
    public ObjectState state() {
        return state;
    }
}
