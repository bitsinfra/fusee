package dev.fusee;

/** Shared pieces of keyed objects. */
final class Keyed {
    private Keyed() {}

    /** The query every object answers with its committed state. */
    static final String STATE_QUERY = "fusee.state";
    /** Update IDs of one-way calls start with this. */
    static final String SEND_PREFIX = "fusee.send/";
    /** The handler every object type has that edits its state from outside. */
    static final String EDIT_HANDLER = "fusee.state.edit";

    /** An edit with its expected version (null: any). */
    static java.util.Map<String, Object> edit(java.util.Map<String, Object> edit, String ifVersion) {
        java.util.Map<String, Object> e = new java.util.LinkedHashMap<>(edit);
        if (ifVersion != null) e.put("ifVersion", ifVersion);
        return e;
    }

    static java.util.Map<String, Object> clear(String field) {
        return field == null ? java.util.Map.of("clearAll", true) : java.util.Map.of("clear", java.util.List.of(field));
    }

    static java.util.Map<String, Object> set(String field, Object value) {
        java.util.Map<String, Object> set = new java.util.LinkedHashMap<>();
        set.put(field, value);
        return java.util.Map.of("set", set);
    }
    /** The state of a key never called (or deleted). */
    static final String EMPTY_STATE = "{}";

    static void checkType(String name) {
        if (name == null || name.isEmpty() || name.contains("/")) {
            throw new RejectedException("an object type's name is not empty and has no '/': '" + name + "'");
        }
    }

    static boolean notFound(RuntimeException err) {
        if (!(err instanceof FuseeException f)) return false;
        String m = f.getMessage() == null ? "" : f.getMessage().toLowerCase(java.util.Locale.ROOT);
        return "NotFound".equals(f.code()) || m.contains("not found");
    }
}
