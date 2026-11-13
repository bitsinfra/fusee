package dev.fusee;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

/**
 * A JSON value as text, as recorded: a payload passed as a {@code Json} is
 * recorded as the JSON it holds, and a result asked for as {@code Json}
 * comes back unconverted.
 */
public final class Json {
    private final String text;

    Json(String text) {
        this.text = text;
    }

    /** JSON text (not checked). */
    public static Json of(String text) {
        return new Json(text == null || text.isEmpty() ? "null" : text);
    }

    /** A value encoded with the built-in codec. */
    public static Json from(Object value) {
        return new Json(MiniJson.write(value));
    }

    public String text() {
        return text;
    }

    /** The value as maps, lists, strings, numbers and booleans. */
    public Object value() {
        return MiniJson.parseTree(text);
    }

    /** The value decoded into a type with the built-in codec. */
    @SuppressWarnings("unchecked")
    public <T> T as(Class<T> type) {
        return (T) MiniJson.INSTANCE.decode(text.getBytes(StandardCharsets.UTF_8), type);
    }

    @SuppressWarnings("unchecked")
    public <T> T as(TypeRef<T> type) {
        return (T) MiniJson.INSTANCE.decode(text.getBytes(StandardCharsets.UTF_8), type.type());
    }

    Object as(Type type) {
        return MiniJson.INSTANCE.decode(text.getBytes(StandardCharsets.UTF_8), type);
    }

    @Override
    public String toString() {
        return text;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Json j && j.text.equals(text);
    }

    @Override
    public int hashCode() {
        return text.hashCode();
    }
}
