package dev.fusee;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * One run in a listing. finishedMs and error are null while it runs; object
 * is true for a key of an object type.
 */
public record RunSummary(String id, String type, int version, String status, long startedMs, Long finishedMs,
        String error, int shard, long run, Map<String, Object> attributes, boolean object) {

    @SuppressWarnings("unchecked")
    static RunSummary of(Map<String, Object> m) {
        Object attrs = m.get("attributes");
        return new RunSummary((String) m.get("id"), (String) m.get("type"), num(m.get("version")).intValue(),
                (String) m.get("status"), num(m.get("startedMs")).longValue(),
                m.get("finishedMs") == null ? null : num(m.get("finishedMs")).longValue(), (String) m.get("error"),
                num(m.get("shard")).intValue(), num(m.get("run")).longValue(),
                attrs instanceof Map<?, ?> a ? Collections.unmodifiableMap((Map<String, Object>) a) : Map.of(),
                Boolean.TRUE.equals(m.get("object")));
    }

    private static Number num(Object o) {
        return o instanceof Number n ? n : 0;
    }

    @SuppressWarnings("unchecked")
    static List<RunSummary> listOf(byte[] json) {
        List<RunSummary> out = new ArrayList<>();
        Object tree = MiniJson.parseTree(Codec.utf8(json));
        if (tree instanceof List<?> l) {
            for (Object o : l) out.add(of((Map<String, Object>) o));
        }
        return out;
    }
}
