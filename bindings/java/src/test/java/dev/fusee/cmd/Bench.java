package dev.fusee.cmd;

import dev.fusee.Engine;
import dev.fusee.Options;
import dev.fusee.Run;
import dev.fusee.StepOptions;
import dev.fusee.Workflow;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Per-step overhead of a sequential no-op chain, closed loop, as
 * bindings/go/cmd/bench, bindings/node/bench.mjs and bindings/python/bench.py
 * measure it, then the throughput of 1,000 concurrent 20-step workflows:
 *
 * <pre>java -cp target/classes:target/test-classes dev.fusee.cmd.Bench [memory|local|fsync]</pre>
 */
public final class Bench {
    private Bench() {}

    private static final long[] STAMPS = new long[1 << 20];
    private static int count;

    private static synchronized void stamp() {
        STAMPS[count++] = System.nanoTime();
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "memory";
        Options opts = Options.memory();
        if (!mode.equals("memory")) {
            Path dir = Path.of(System.getProperty("java.io.tmpdir"), "fusee-java-bench");
            if (Files.exists(dir)) {
                try (var s = Files.walk(dir)) {
                    s.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
                }
            }
            opts = Options.local(dir.toString());
            if (mode.equals("fsync")) opts.flush("fsync");
        }
        int n = mode.equals("local") ? 300 : 3000;
        try (Engine e = Engine.open(opts)) {
            Workflow<Integer, Integer> chain = e.register("chain", Integer.class, Integer.class, chain(false));
            Workflow<Integer, Integer> inline = e.register("chain-inline", Integer.class, Integer.class, chain(true));
            System.out.printf("java %s, %s threads%n", System.getProperty("java.version"),
                    Engine.virtualThreads() ? "virtual" : "platform");
            for (int round = 0; round < 3; round++) {
                for (Workflow<Integer, Integer> w : List.of(chain, inline)) {
                    count = 0;
                    Engine.Stats before = e.stats();
                    w.start("solo-" + w.name() + "-" + round, n).result();
                    Engine.Stats after = e.stats();
                    double[] gaps = new double[count - 1];
                    for (int i = 1; i < count; i++) gaps[i - 1] = (STAMPS[i] - STAMPS[i - 1]) / 1e3;
                    Arrays.sort(gaps);
                    if (round == 2) {
                        System.out.printf(
                                "java %s %s: step-to-step p50 %.1f µs  p99 %.1f µs  (%d steps; %.1f engine calls and %.2f ticks per step)%n",
                                mode, w.name(), gaps[(gaps.length - 1) / 2], gaps[(int) ((gaps.length - 1) * 0.99)], n,
                                (after.nativeCalls() - before.nativeCalls()) / (double) n,
                                (after.ticks() - before.ticks()) / (double) n);
                    }
                }
            }
            int W = 1000, S = 20;
            for (int round = 0; round < 2; round++) {
                long t0 = System.nanoTime();
                List<Run<Integer>> runs = new ArrayList<>();
                for (int i = 0; i < W; i++) runs.add(chain.start("w" + round + "-" + i, S));
                double started = (System.nanoTime() - t0) / 1e9;
                for (Run<Integer> r : runs) r.result();
                double el = (System.nanoTime() - t0) / 1e9;
                if (round == 1) {
                    System.out.printf("  %d durable steps in %.2f s = %.0f steps/s (the %d starts returned after %.2f s)%n",
                            W * S, el, W * S / el, W, started);
                }
            }
        }
    }

    private static dev.fusee.WorkflowFn<Integer, Integer> chain(boolean inline) {
        StepOptions o = inline ? StepOptions.defaults().inline() : StepOptions.defaults();
        return (ctx, n) -> {
            for (int i = 0; i < n; i++) {
                int v = i;
                ctx.step("noop", Integer.class, () -> {
                    stamp();
                    return v;
                }, o);
            }
            return n;
        };
    }
}
