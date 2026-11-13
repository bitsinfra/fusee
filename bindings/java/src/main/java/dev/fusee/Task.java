package dev.fusee;

import java.util.concurrent.locks.LockSupport;

/**
 * A thread of a run's workflow code (the workflow function, a function
 * started with Ctx.async, an update handler). Exactly one task runs at a
 * time, engine-wide: the engine's loop thread hands the engine to a task
 * and waits until the task blocks on a journal operation or returns.
 * Every handoff is a volatile flag write and read (with a park/unpark), so
 * whatever one thread wrote, the next sees.
 */
final class Task {
    interface Body {
        Object run() throws Exception;
    }

    interface Done {
        void done(Object value, Throwable err);
    }

    /** Unwinds a task of a run that ended (do not catch it in workflow code). */
    static final class Killed extends Error {
        private static final long serialVersionUID = 1L;

        Killed() {
            super("the workflow's run ended", null, false, false);
        }
    }

    static final Killed KILLED = new Killed();

    final RunExec x;
    final Body body;
    final Done onDone;
    Thread thread;
    boolean started, exited, kill;
    /** Set by the loop to resume the task. */
    volatile boolean go;
    /** The task is parked (or about to park) waiting for go. */
    volatile boolean parked;
    /** Set by the task giving the engine back to its resumer (the thread that resumed it). */
    volatile boolean back;
    Thread resumer;
    volatile boolean resumerParked;

    Task(RunExec x, Body body, Done onDone) {
        this.x = x;
        this.body = body;
        this.onDone = onDone;
    }

    void main() {
        Object v = null;
        Throwable err = null;
        boolean killed = false;
        try {
            v = body.run();
        } catch (Killed k) {
            killed = true;
        } catch (Throwable t) {
            err = t;
        }
        try {
            if (!killed && !kill) onDone.done(v, err);
        } catch (Killed k) {
            // The run ended while recording its outcome.
        } catch (Throwable t) {
            x.e.stopAfterFailure(Errors.unwrap(t));
        } finally {
            exited = true;
            x.tasks.remove(this);
            giveBack();
        }
    }

    /** Gives the engine back and waits to be resumed; a killed task unwinds. */
    void park() {
        giveBack();
        awaitGo();
        if (kill) throw KILLED;
    }

    private void giveBack() {
        back = true;
        if (resumerParked) LockSupport.unpark(resumer);
    }

    /** The resumer waits for the task to give the engine back. */
    void awaitBack() {
        if (Threads.SPIN_NS > 0) {
            long end = System.nanoTime() + Threads.SPIN_NS;
            int i = 0;
            while (!back) {
                Thread.onSpinWait();
                if ((++i & 63) == 0 && System.nanoTime() > end) break;
            }
        }
        while (!back) {
            resumerParked = true;
            if (!back) LockSupport.park(this);
            resumerParked = false;
            Thread.interrupted();
        }
    }

    /** Resumes this (started) task; called by the loop. */
    void resumeTask() {
        go = true;
        if (parked) LockSupport.unpark(thread);
    }

    private void awaitGo() {
        if (Threads.SPIN_NS > 0) {
            Engine e = x.e;
            e.spinner = this;
            long end = System.nanoTime() + Threads.SPIN_NS;
            int i = 0;
            while (!go && e.spinner == this) {
                Thread.onSpinWait();
                if ((++i & 63) == 0 && System.nanoTime() > end) break;
            }
        }
        while (!go) {
            parked = true;
            if (!go) LockSupport.park(this);
            parked = false;
            Thread.interrupted();
        }
        go = false;
    }
}
