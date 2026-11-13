package dev.fusee;

import java.lang.reflect.Type;

/**
 * Turns values into JSON text and back. Payloads (workflow inputs and
 * results, step results, signals, update arguments, query answers) are
 * recorded as JSON, except {@code byte[]}, which is recorded as raw bytes, as
 * in the other SDKs.
 *
 * <p>The built-in codec ({@link #builtIn()}) has no dependencies and handles
 * {@code null}, strings, numbers, booleans, enums (by name), records,
 * classes with public fields (or getters and setters) and a public no-argument
 * constructor, {@code List}, {@code Set}, {@code Map} with string keys,
 * arrays, {@code Optional}, {@code UUID} and {@code java.time} values (as
 * their ISO text), and {@link Json}. To use Jackson instead:
 *
 * <pre>{@code
 * ObjectMapper m = new ObjectMapper();
 * JsonCodec jackson = new JsonCodec() {
 *     public byte[] encode(Object v) throws Exception { return m.writeValueAsBytes(v); }
 *     public Object decode(byte[] json, Type t) throws Exception { return m.readValue(json, m.constructType(t)); }
 * };
 * Engine.open(Options.memory().json(jackson));
 * }</pre>
 */
public interface JsonCodec {
    /** The value as UTF-8 JSON text. */
    byte[] encode(Object value) throws Exception;

    /** UTF-8 JSON text as a value of the type ({@code Object}: maps, lists, strings, numbers, booleans). */
    Object decode(byte[] json, Type type) throws Exception;

    /** The built-in codec. */
    static JsonCodec builtIn() {
        return MiniJson.INSTANCE;
    }
}
