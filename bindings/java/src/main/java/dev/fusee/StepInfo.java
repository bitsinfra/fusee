package dev.fusee;

import java.util.function.Supplier;

/** The step attempt running on this thread (its function's thread, or the workflow's for an inline step). */
final class StepInfo {
    static final ThreadLocal<StepInfo> CURRENT = new ThreadLocal<>();

    final RunExec x;
    final int seq;
    final boolean inline;
    /** The attempt and its generation: a stream operation of an attempt that is over is dropped. */
    private final Attempts a;
    private final long gen;
    private String key;

    StepInfo(RunExec x, int seq, boolean inline, Attempts a, long gen) {
        this.x = x;
        this.seq = seq;
        this.inline = inline;
        this.a = a;
        this.gen = gen;
    }

    /** Runs f holding the engine: directly for an inline step (which holds it already), through the loop otherwise. */
    <T> T within(Supplier<T> f) {
        if (inline) return f.get();
        return x.e.hold(f);
    }

    /** This attempt, from 1. */
    long attempt() {
        return a == null ? 1 : a.attempt();
    }

    /** Whether this attempt still runs; holder only. */
    boolean live() {
        return !x.ended && (a == null || a.live(gen));
    }

    String key() {
        if (key == null) {
            key = within(() -> {
                if (x.ended) throw Errors.ended();
                return Codec.utf8(x.e.host(x.shard, x.run, Native.OP_KEY, seq & 0xffffffffL, 0, 0, null, null, null,
                        null));
            });
        }
        return key;
    }
}
