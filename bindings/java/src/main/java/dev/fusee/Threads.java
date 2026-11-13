package dev.fusee;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.Executor;

/**
 * The threads an engine uses: its loop, workflow code and step functions.
 *
 * <p>On Java 21 and later the loop and workflow code run on virtual threads
 * (found by reflection, so the SDK runs on Java 17 too): a handoff between
 * them is a park and an unpark that the virtual-thread scheduler usually
 * serves on the same carrier, and a run waiting for its code costs no OS
 * thread. Before Java 21 they are platform threads, and a thread waiting for
 * a handoff spins briefly before it parks (only the most recent waiter of
 * each kind spins). Step functions run on a cached pool of platform threads
 * whose most recently idle worker spins briefly, so a step reaches a running
 * thread without a wake; the thread waiting for the engine's next event (the
 * loop, or a workflow thread driving it) spins briefly too.
 *
 * <p>System properties: {@code fusee.threads} ("virtual" or "platform"),
 * {@code fusee.steps=virtual} (a virtual thread per step attempt instead of
 * the pool), and the spins in microseconds: {@code fusee.spin.us} (handoffs),
 * {@code fusee.wait.spin.us} (waiting for work), {@code fusee.pool.spin.us}
 * (idle step workers); 0 turns a spin off.
 */
final class Threads {
    private Threads() {}

    private static final MethodHandle OF_VIRTUAL;
    private static final MethodHandle UNSTARTED;
    private static final MethodHandle NAME;

    static {
        MethodHandle ofVirtual = null, unstarted = null, name = null;
        String mode = System.getProperty("fusee.threads", "");
        if (!mode.equals("platform")) {
            try {
                MethodHandles.Lookup l = MethodHandles.publicLookup();
                Class<?> builder = Class.forName("java.lang.Thread$Builder");
                ofVirtual = l.findStatic(Thread.class, "ofVirtual",
                        MethodType.methodType(Class.forName("java.lang.Thread$Builder$OfVirtual")));
                unstarted = l.findVirtual(builder, "unstarted", MethodType.methodType(Thread.class, Runnable.class));
                name = l.findVirtual(builder, "name", MethodType.methodType(builder, String.class));
            } catch (ReflectiveOperationException | LinkageError e) {
                ofVirtual = null;
            }
        }
        OF_VIRTUAL = ofVirtual;
        UNSTARTED = unstarted;
        NAME = name;
    }

    /** Whether the engine's threads are virtual threads. */
    static boolean virtual() {
        return OF_VIRTUAL != null;
    }

    /** How long a thread waiting for a handoff spins before it parks. */
    static final long SPIN_NS = Long.getLong("fusee.spin.us", virtual() ? 0 : 50) * 1000;
    /**
     * How long the thread waiting for work (the loop, or a task driving it)
     * spins before it parks: not at all on a virtual-thread scheduler of one
     * or two carriers, where it would hold the carrier the next thread needs.
     */
    static final long WAIT_SPIN_NS = Long.getLong("fusee.wait.spin.us",
            !virtual() ? 50 : Integer.getInteger("jdk.virtualThreadScheduler.parallelism", 64) <= 2 ? 0 : 30) * 1000;
    /** How long an idle worker of the step pool spins. */
    static final long POOL_SPIN_NS = Long.getLong("fusee.pool.spin.us", 50) * 1000;
    /** fusee.steps=virtual: step functions on a virtual thread each instead of the pool. */
    private static final boolean VIRTUAL_STEPS = "virtual".equals(System.getProperty("fusee.steps"));

    private static Thread newVirtual(Runnable r, String name) {
        try {
            Object b = OF_VIRTUAL.invoke();
            b = NAME.invoke(b, name);
            return (Thread) UNSTARTED.invoke(b, r);
        } catch (Throwable e) {
            return null;
        }
    }

    /** An unstarted thread for the loop or workflow code. */
    static Thread task(Runnable r, String name) {
        if (OF_VIRTUAL != null) {
            Thread t = newVirtual(r, name);
            if (t != null) return t;
        }
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** A platform daemon thread. */
    static Thread platform(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    private static volatile Executor pool;

    /** The default executor for step functions. */
    static Executor steps() {
        if (OF_VIRTUAL != null && VIRTUAL_STEPS) {
            return r -> task(r, "fusee-step").start();
        }
        Executor p = pool;
        if (p == null) {
            synchronized (Threads.class) {
                p = pool;
                if (p == null) {
                    p = new SpinPool(POOL_SPIN_NS);
                    pool = p;
                }
            }
        }
        return p;
    }
}
