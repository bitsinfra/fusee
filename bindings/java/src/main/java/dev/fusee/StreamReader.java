package dev.fusee;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A reader of a run's stream ({@code run.stream(name, from)}): an iterator
 * whose {@code hasNext} waits until the next chunk is durable, in order, and
 * returns false once the stream is complete and every chunk was returned.
 * {@link #offset} is where a reader that reconnects continues (in this
 * process, through a server, or after the worker failed over). Not safe for
 * concurrent use.
 *
 * <pre>{@code
 * StreamReader tokens = engine.run(id).stream("tokens", lastSeen);
 * for (Chunk c : tokens) out.write(c.as(String.class));
 * // tokens.offset(): where to reconnect; tokens.status(): how it ended
 * }</pre>
 */
public final class StreamReader implements Iterator<Chunk>, Iterable<Chunk> {
    /** One read: the read's JSON. run is -1 until the first read found it. */
    interface Read {
        String read(long run, long from, long waitMs);
    }

    private final Read read;
    private long run = -1;
    private long next;
    private long waitMs = 30_000;
    private final ArrayDeque<Chunk> buf = new ArrayDeque<>();
    private StreamEnd end;

    StreamReader(Read read, long from) {
        this.read = read;
        this.next = from;
    }

    /** How long one read waits for a new chunk before it asks again (default 30 s); it only bounds a request. */
    public StreamReader waitFor(Duration d) {
        waitMs = Math.max(0, d.toMillis());
        return this;
    }

    /** Reads the stream of run (an earlier reader's {@link #runId}), not the workflow ID's newest run. */
    public StreamReader ofRun(long run) {
        this.run = run;
        return this;
    }

    /** The offset of the chunk {@link #next} returns next. */
    public long offset() {
        Chunk c = buf.peekFirst();
        return c != null ? c.offset() : next;
    }

    /** The run being read, once the first read found it (else -1). */
    public long runId() {
        return run;
    }

    /** How the stream ended, once {@link #hasNext} returned false (else null). */
    public StreamEnd end() {
        return buf.isEmpty() ? end : null;
    }

    /** The end's status ("ok", a failed step's "failed", or the run's status), or null before the end. */
    public String status() {
        StreamEnd e = end();
        return e == null ? null : e.status();
    }

    @Override
    public Iterator<Chunk> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        while (buf.isEmpty()) {
            if (end != null) return false;
            take(read.read(run, next, waitMs));
        }
        return true;
    }

    @Override
    public Chunk next() {
        if (!hasNext()) throw new NoSuchElementException("the stream is complete");
        return buf.pollFirst();
    }

    // ------------------------------------------------------------ parsing

    /** Takes one read's answer: {"run", "from", "next", "chunks", "bytes", "end"}. */
    @SuppressWarnings("unchecked")
    private void take(String s) {
        long from = 0, nxt = 0;
        List<int[]> chunks = new ArrayList<>();
        Set<Integer> raw = new HashSet<>();
        StreamEnd ended = null;
        int i = ws(s, 0);
        if (i >= s.length() || s.charAt(i) != '{') throw malformed();
        i = ws(s, i + 1);
        while (i < s.length() && s.charAt(i) != '}') {
            int k = i;
            i = string(s, i);
            String key = (String) MiniJson.parseTree(s.substring(k, i));
            i = ws(s, i);
            if (i >= s.length() || s.charAt(i) != ':') throw malformed();
            i = ws(s, i + 1);
            int v = i;
            i = value(s, i);
            String text = s.substring(v, i);
            switch (key) {
                case "run" -> run = number(text);
                case "from" -> from = number(text);
                case "next" -> nxt = number(text);
                case "chunks" -> {
                    int j = ws(s, v + 1);
                    while (j < i - 1 && s.charAt(j) != ']') {
                        int start = j;
                        j = value(s, j);
                        chunks.add(new int[] {start, j});
                        j = ws(s, j);
                        if (j < s.length() && s.charAt(j) == ',') j = ws(s, j + 1);
                    }
                }
                case "bytes" -> {
                    if (MiniJson.parseTree(text) instanceof List<?> l) {
                        for (Object o : l) raw.add(((Number) o).intValue());
                    }
                }
                case "end" -> {
                    if (MiniJson.parseTree(text) instanceof Map<?, ?> m) {
                        ended = new StreamEnd(String.valueOf(m.get("status")), Boolean.TRUE.equals(m.get("closed")));
                    }
                }
                default -> {}
            }
            i = ws(s, i);
            if (i < s.length() && s.charAt(i) == ',') i = ws(s, i + 1);
        }
        for (int c = 0; c < chunks.size(); c++) {
            int[] span = chunks.get(c);
            String t = s.substring(span[0], span[1]);
            long offset = from + c;
            if (raw.contains(c)) {
                buf.addLast(new Chunk(offset, null, Base64.getDecoder().decode((String) MiniJson.parseTree(t))));
            } else {
                buf.addLast(new Chunk(offset, Json.of(t), null));
            }
        }
        next = Math.max(next, nxt);
        end = ended;
    }

    private static FuseeException malformed() {
        return Errors.failed("malformed stream read");
    }

    private static long number(String t) {
        try {
            return Long.parseLong(t.trim());
        } catch (NumberFormatException e) {
            return (long) Double.parseDouble(t.trim());
        }
    }

    private static int ws(String s, int i) {
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c != ' ' && c != '\n' && c != '\r' && c != '\t') break;
            i++;
        }
        return i;
    }

    /** The end of the string starting at s[i] (a quote). */
    private static int string(String s, int i) {
        if (i >= s.length() || s.charAt(i) != '"') throw malformed();
        for (i++; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') i++;
            else if (c == '"') return i + 1;
        }
        throw malformed();
    }

    /** The end of the JSON value starting at s[i]. */
    private static int value(String s, int i) {
        if (i >= s.length()) throw malformed();
        char c = s.charAt(i);
        if (c == '"') return string(s, i);
        if (c == '{' || c == '[') {
            int depth = 0;
            while (i < s.length()) {
                char d = s.charAt(i);
                if (d == '"') {
                    i = string(s, i);
                    continue;
                }
                if (d == '{' || d == '[') depth++;
                else if ((d == '}' || d == ']') && --depth == 0) return i + 1;
                i++;
            }
            throw malformed();
        }
        while (i < s.length() && ",}] \t\n\r".indexOf(s.charAt(i)) < 0) i++;
        return i;
    }
}
