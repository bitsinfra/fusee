package dev.fusee;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * A cached pool of daemon platform threads for step functions whose idle
 * workers spin briefly before they park: a step is handed to the most
 * recently idle worker, which is usually still spinning, without waking a
 * thread. At most a few workers spin at once; the pool grows without bound
 * (a blocked step holds its thread) and a worker idle for a minute exits.
 */
final class SpinPool implements Executor {
    private static final Runnable EXIT = () -> {};
    private static final long KEEP_ALIVE_NS = TimeUnit.SECONDS.toNanos(60);
    private static final AtomicLong COUNT = new AtomicLong();

    private final ConcurrentLinkedDeque<Worker> idle = new ConcurrentLinkedDeque<>();
    private final AtomicInteger spinning = new AtomicInteger();
    private final long spinNs;
    private static final int SPINNERS = Integer.getInteger("fusee.pool.spinners", 4);

    SpinPool(long spinNs) {
        this.spinNs = spinNs;
    }

    @Override
    public void execute(Runnable r) {
        for (;;) {
            Worker w = idle.pollFirst();
            if (w == null) {
                Thread t = Threads.platform(new Worker(r), "fusee-step-" + COUNT.incrementAndGet());
                t.start();
                return;
            }
            if (w.offer(r)) return;
        }
    }

    private final class Worker implements Runnable {
        private final AtomicReference<Runnable> task = new AtomicReference<>();
        private volatile Thread thread;
        private volatile boolean parked;
        private Runnable first;

        Worker(Runnable first) {
            this.first = first;
        }

        boolean offer(Runnable r) {
            if (!task.compareAndSet(null, r)) return false;
            if (parked) LockSupport.unpark(thread);
            return true;
        }

        @Override
        public void run() {
            thread = Thread.currentThread();
            Runnable r = first;
            first = null;
            while (r != null) {
                try {
                    r.run();
                } catch (Throwable t) {
                    Thread th = Thread.currentThread();
                    th.getUncaughtExceptionHandler().uncaughtException(th, t);
                }
                Thread.interrupted();
                idle.addFirst(this);
                r = next();
            }
        }

        private Runnable take() {
            Runnable r = task.get();
            if (r == null || r == EXIT) return null;
            task.set(null);
            return r;
        }

        /** The next task, or null once this worker exits. */
        private Runnable next() {
            Runnable r;
            if (spinNs > 0 && spinning.incrementAndGet() <= SPINNERS) {
                try {
                    long end = System.nanoTime() + spinNs;
                    int i = 0;
                    for (;;) {
                        if ((r = take()) != null) return r;
                        Thread.onSpinWait();
                        if ((++i & 63) == 0 && System.nanoTime() > end) break;
                    }
                } finally {
                    spinning.decrementAndGet();
                }
            } else if (spinNs > 0) {
                spinning.decrementAndGet();
            }
            long idleSince = System.nanoTime();
            for (;;) {
                if ((r = take()) != null) return r;
                parked = true;
                if ((r = take()) != null) {
                    parked = false;
                    return r;
                }
                LockSupport.parkNanos(this, KEEP_ALIVE_NS);
                parked = false;
                if (System.nanoTime() - idleSince >= KEEP_ALIVE_NS && task.compareAndSet(null, EXIT)) {
                    idle.remove(this);
                    return null;
                }
            }
        }
    }
}
