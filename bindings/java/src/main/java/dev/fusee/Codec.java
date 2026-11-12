package dev.fusee;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

/**
 * Payloads in their recorded form: JSON text, or a zero byte followed by raw
 * bytes (a {@code byte[]} value), as in the other SDKs. An empty payload is
 * none (null).
 */
final class Codec {
    private static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);
    private static final SecureRandom RANDOM = new SecureRandom();

    final JsonCodec json;

    Codec(JsonCodec json) {
        this.json = json == null ? MiniJson.INSTANCE : json;
    }

    byte[] encode(Object v) {
        if (v == null) return NULL;
        if (v instanceof byte[] b) {
            byte[] out = new byte[b.length + 1];
            System.arraycopy(b, 0, out, 1, b.length);
            return out;
        }
        if (v instanceof Json j) return utf8(j.text());
        try {
            return json.encode(v);
        } catch (RuntimeException e) {
            throw Errors.invalid("a " + v.getClass().getName() + " is not JSON: " + Errors.message(e));
        } catch (Exception e) {
            throw Errors.invalid("a " + v.getClass().getName() + " is not JSON: " + Errors.message(e));
        }
    }

    /** A recorded payload as a value of the type: empty is null, raw bytes decode into byte[] (or Object). */
    Object decode(byte[] p, Type t) {
        if (t == null) t = Object.class;
        if (p == null || p.length == 0) {
            return t == Json.class ? Json.of("null") : primitiveZero(t);
        }
        if (p[0] == 0) {
            if (t == byte[].class || t == Object.class) {
                byte[] b = new byte[p.length - 1];
                System.arraycopy(p, 1, b, 0, b.length);
                return b;
            }
            if (t == Json.class) return new Json(raw(p));
            throw Errors.failed("a raw payload does not decode into " + t.getTypeName());
        }
        if (t == Json.class) return new Json(new String(p, StandardCharsets.UTF_8));
        try {
            return json.decode(p, t);
        } catch (Exception e) {
            throw Errors.failed("a recorded value does not decode into " + t.getTypeName() + ": " + Errors.message(e));
        }
    }

    private static Object primitiveZero(Type t) {
        if (t instanceof Class<?> c && c.isPrimitive()) return MiniJson.bind(null, c);
        return null;
    }

    /** A payload as text (error messages and reasons). */
    static String text(byte[] p) {
        if (p == null || p.length == 0) return "";
        if (p[0] == 0) return new String(p, 1, p.length - 1, StandardCharsets.UTF_8);
        return new String(p, StandardCharsets.UTF_8);
    }

    /** A payload as JSON text (raw bytes become a JSON string of their text). */
    static String raw(byte[] p) {
        if (p == null || p.length == 0) return "null";
        if (p[0] == 0) {
            StringBuilder sb = new StringBuilder();
            MiniJson.quote(sb, new String(p, 1, p.length - 1, StandardCharsets.UTF_8));
            return sb.toString();
        }
        return new String(p, StandardCharsets.UTF_8);
    }

    static byte[] utf8(String s) {
        return s == null || s.isEmpty() ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    static String utf8(byte[] b) {
        return b == null ? "" : new String(b, StandardCharsets.UTF_8);
    }

    static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        RANDOM.nextBytes(b);
        return b;
    }

    static long le64(byte[] b) {
        long v = 0;
        for (int i = 7; i >= 0; i--) v = (v << 8) | (b[i] & 0xff);
        return v;
    }

    static byte[] le64(long v) {
        byte[] b = new byte[8];
        for (int i = 0; i < 8; i++) b[i] = (byte) (v >>> (8 * i));
        return b;
    }
}
