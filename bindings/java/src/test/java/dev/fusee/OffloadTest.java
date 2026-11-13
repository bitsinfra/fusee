package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Payloads over the payload offload threshold (64 KiB by default) go to the shared log's blob area: a 1 MiB step
 * result and a 1 MiB signal round trip exactly, before and after a reopen.
 */
@Timeout(120)
class OffloadTest {
    @TempDir
    Path dir;

    static final int MIB = 1 << 20;

    static String page(int seed, int n) {
        int x = seed | 1;
        StringBuilder b = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            x = x * 1103515245 + 12345;
            b.append((char) ('a' + Integer.remainderUnsigned(x, 26)));
        }
        return b.toString();
    }

    static Workflow<Integer, String> big(Engine e, AtomicInteger fetched) {
        return e.register("big", Integer.class, String.class, (ctx, n) -> {
            String body = ctx.step("fetch", String.class, () -> {
                fetched.incrementAndGet();
                return page(7, n);
            });
            String note = ctx.waitFor("note", String.class);
            return body.length() + " " + body.equals(page(7, n)) + " " + note.length() + " "
                    + note.equals(page(9, MIB));
        });
    }

    @Test
    void largeResultsAndSignalsRoundTripThroughTheBlobArea() {
        AtomicInteger fetched = new AtomicInteger();
        String want = MIB + " true " + MIB + " true";
        try (Engine e = RecoveryTest.local(dir)) {
            Workflow<Integer, String> wf = big(e, fetched);
            Run<String> a = wf.start("a", MIB);
            // A second run stays suspended across the reopen, its result recorded.
            wf.start("b", MIB);
            a.signal("note", page(9, MIB));
            assertEquals(want, a.result());
            String[] blobs = new File(dir.toFile(), "wal/blobs").list();
            assertTrue(blobs != null && blobs.length > 0, "the blob area was written");
        }
        try (Engine e = RecoveryTest.local(dir)) {
            big(e, fetched);
            assertEquals(want, e.run("a", String.class).result());
            Run<String> b = e.run("b", String.class);
            b.signal("note", page(9, MIB));
            assertEquals(want, b.result());
            assertEquals(2, fetched.get(), "recorded results replay, steps do not run again");
        }
    }
}
