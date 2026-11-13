package dev.fusee;

import java.util.Arrays;

/**
 * One chunk of a stream as a reader receives it: JSON, or bytes.
 *
 * @param offset its place in the stream: 0, 1, 2, ...
 * @param json the chunk's JSON; null for a chunk of bytes
 * @param bytes the chunk's bytes; null for a JSON chunk
 */
public record Chunk(long offset, Json json, byte[] bytes) {
    /** Whether the chunk was appended as bytes. */
    public boolean isBytes() {
        return json == null;
    }

    /** The chunk as maps, lists, strings, numbers and booleans, or its bytes. */
    public Object value() {
        return isBytes() ? bytes : json.value();
    }

    /** The chunk decoded with the built-in codec (byte[] or Object for a chunk of bytes). */
    @SuppressWarnings("unchecked")
    public <T> T as(Class<T> type) {
        if (isBytes()) {
            if (type == byte[].class || type == Object.class) return (T) bytes;
            throw Errors.failed("chunk " + offset + " holds bytes, not JSON");
        }
        return json.as(type);
    }

    public <T> T as(TypeRef<T> type) {
        if (isBytes()) throw Errors.failed("chunk " + offset + " holds bytes, not JSON");
        return json.as(type);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Chunk c && c.offset == offset && java.util.Objects.equals(c.json, json)
                && Arrays.equals(c.bytes, bytes);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(offset) * 31 + (json != null ? json.hashCode() : Arrays.hashCode(bytes));
    }

    @Override
    public String toString() {
        return offset + ":" + (isBytes() ? "bytes" + Arrays.toString(bytes) : json.text());
    }
}
