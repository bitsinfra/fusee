package dev.fusee;

import java.util.ArrayList;

/** A journal operation (or a task, or a group) that workflow code waits for. */
final class OpState {
    static final int STEP = 0, AWAIT = 1, SLEEP = 2, WAIT = 3, CHILD = 4, TASK = 5, GROUP = 6;

    final int kind;
    final String name;
    /** The journal position, or -1 (tasks, groups, failed starts). */
    final long seq;
    boolean done;
    Throwable err;
    byte[] payload;
    boolean timedOut;
    Object value;
    /** Tasks to resume and functions to call when it settles. */
    ArrayList<Object> waiters;

    OpState(int kind, String name, long seq) {
        this.kind = kind;
        this.name = name;
        this.seq = seq;
    }

    static OpState failed(Throwable err) {
        OpState o = new OpState(TASK, "", -1);
        o.done = true;
        o.err = err;
        return o;
    }

    void addWaiter(Object w) {
        if (waiters == null) waiters = new ArrayList<>(2);
        waiters.add(w);
    }

    /** Calls f now if done, else when it settles. */
    void whenDone(Runnable f) {
        if (done) f.run();
        else addWaiter(f);
    }
}
