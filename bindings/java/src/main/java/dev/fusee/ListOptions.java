package dev.fusee;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A listing's filter: each condition matches the run's type, status, id,
 * startedMs or a search attribute, with equality or an operator object
 * ({@code Map.of("gte", 100)}; eq, ne, gt, gte, lt, lte).
 */
public final class ListOptions {
    int limit = 100;
    final Map<String, Object> where = new LinkedHashMap<>();

    public ListOptions() {}

    public static ListOptions all() {
        return new ListOptions();
    }

    public static ListOptions where(String field, Object condition) {
        return new ListOptions().and(field, condition);
    }

    public ListOptions and(String field, Object condition) {
        where.put(field, condition);
        return this;
    }

    public ListOptions where(Map<String, ?> conditions) {
        where.putAll(conditions);
        return this;
    }

    /** At most this many runs (default 100). */
    public ListOptions limit(int n) {
        limit = n;
        return this;
    }
}
