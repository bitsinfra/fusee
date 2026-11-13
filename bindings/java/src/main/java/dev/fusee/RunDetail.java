package dev.fusee;

import java.util.Map;

/**
 * A run's summary and its normalized journal (a JSON array of records); for
 * a key of an object type, object is {"state": {...}, "running": [...],
 * "waiting": [...]} (else null).
 */
public record RunDetail(RunSummary summary, Json journal, Json object) {

    public String id() {
        return summary.id();
    }

    public String status() {
        return summary.status();
    }

    public String error() {
        return summary.error();
    }

    @SuppressWarnings("unchecked")
    static RunDetail of(byte[] json) {
        Map<String, Object> m = (Map<String, Object>) MiniJson.parseTree(Codec.utf8(json));
        Object journal = m.get("journal");
        Object object = m.get("object");
        return new RunDetail(RunSummary.of(m), Json.from(journal), object == null ? null : Json.from(object));
    }
}
