package dev.fusee;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** The built-in JSON codec (see {@link JsonCodec}). */
final class MiniJson implements JsonCodec {
    static final MiniJson INSTANCE = new MiniJson();

    private MiniJson() {}

    @Override
    public byte[] encode(Object value) {
        StringBuilder sb = new StringBuilder(32);
        write(sb, value, 0);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object decode(byte[] json, Type type) {
        Object tree = parse(new String(json, StandardCharsets.UTF_8));
        return bind(tree, type);
    }

    /** JSON text as a tree of maps, lists, strings, numbers (Integer, Long, BigInteger, Double), booleans. */
    static Object parseTree(String text) {
        return bind(parse(text), Object.class);
    }

    static String write(Object value) {
        StringBuilder sb = new StringBuilder(32);
        write(sb, value, 0);
        return sb.toString();
    }

    // ------------------------------------------------------------- writing

    private static void write(StringBuilder sb, Object v, int depth) {
        if (depth > 512) {
            throw new IllegalArgumentException("a value nested too deeply (a cycle?)");
        }
        if (v == null) {
            sb.append("null");
        } else if (v instanceof String s) {
            quote(sb, s);
        } else if (v instanceof Boolean b) {
            sb.append(b.booleanValue());
        } else if (v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte
                || v instanceof BigInteger || v instanceof java.util.concurrent.atomic.AtomicInteger
                || v instanceof java.util.concurrent.atomic.AtomicLong) {
            sb.append(v);
        } else if (v instanceof Double || v instanceof Float) {
            number(sb, ((Number) v).doubleValue());
        } else if (v instanceof BigDecimal d) {
            sb.append(d.toString());
        } else if (v instanceof Number n) {
            number(sb, n.doubleValue());
        } else if (v instanceof Json j) {
            sb.append(j.text());
        } else if (v instanceof Character c) {
            quote(sb, String.valueOf(c));
        } else if (v instanceof Enum<?> e) {
            quote(sb, e.name());
        } else if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> en : m.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                Object k = en.getKey();
                quote(sb, k instanceof Enum<?> e ? e.name() : String.valueOf(k));
                sb.append(':');
                write(sb, en.getValue(), depth + 1);
            }
            sb.append('}');
        } else if (v instanceof Iterable<?> it) {
            sb.append('[');
            boolean first = true;
            for (Object o : it) {
                if (!first) sb.append(',');
                first = false;
                write(sb, o, depth + 1);
            }
            sb.append(']');
        } else if (v instanceof byte[] b) {
            quote(sb, Base64.getEncoder().encodeToString(b));
        } else if (v.getClass().isArray()) {
            sb.append('[');
            int n = Array.getLength(v);
            for (int i = 0; i < n; i++) {
                if (i > 0) sb.append(',');
                write(sb, Array.get(v, i), depth + 1);
            }
            sb.append(']');
        } else if (v instanceof Optional<?> o) {
            write(sb, o.orElse(null), depth + 1);
        } else if (v instanceof UUID || v instanceof TemporalAccessor || v instanceof Duration
                || v instanceof java.time.Period || v instanceof java.time.ZoneId) {
            quote(sb, v.toString());
        } else if (v instanceof Record) {
            sb.append('{');
            boolean first = true;
            for (Prop p : props(v.getClass())) {
                if (!first) sb.append(',');
                first = false;
                quote(sb, p.name);
                sb.append(':');
                write(sb, p.get(v), depth + 1);
            }
            sb.append('}');
        } else {
            List<Prop> ps = props(v.getClass());
            if (ps.isEmpty()) {
                throw new IllegalArgumentException(
                        "a " + v.getClass().getName() + " is not JSON: no public fields or getters");
            }
            sb.append('{');
            boolean first = true;
            for (Prop p : ps) {
                if (!first) sb.append(',');
                first = false;
                quote(sb, p.name);
                sb.append(':');
                write(sb, p.get(v), depth + 1);
            }
            sb.append('}');
        }
    }

    private static void number(StringBuilder sb, double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            sb.append("null");
        } else if (d == Math.rint(d) && Math.abs(d) < 1e15) {
            sb.append((long) d);
        } else {
            sb.append(d);
        }
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    static void quote(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append("\\u00").append(HEX[c >> 4]).append(HEX[c & 15]);
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    // ------------------------------------------------------------- parsing

    /** A number as written, converted when bound. */
    private record Num(String text) {}

    private static Object parse(String s) {
        Parser p = new Parser(s);
        p.ws();
        Object v = p.value(0);
        p.ws();
        if (p.i != s.length()) {
            throw p.error("trailing characters");
        }
        return v;
    }

    private static final class Parser {
        final String s;
        int i;

        Parser(String s) {
            this.s = s;
        }

        IllegalArgumentException error(String what) {
            return new IllegalArgumentException("bad JSON at " + i + ": " + what);
        }

        void ws() {
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == ' ' || c == '\n' || c == '\r' || c == '\t') i++;
                else break;
            }
        }

        Object value(int depth) {
            if (depth > 512) throw error("nested too deeply");
            if (i >= s.length()) throw error("unexpected end");
            char c = s.charAt(i);
            switch (c) {
                case '{': {
                    i++;
                    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
                    ws();
                    if (peek() == '}') {
                        i++;
                        return m;
                    }
                    for (;;) {
                        ws();
                        if (peek() != '"') throw error("expected a key");
                        String k = string();
                        ws();
                        expect(':');
                        ws();
                        m.put(k, value(depth + 1));
                        ws();
                        char d = next();
                        if (d == '}') return m;
                        if (d != ',') throw error("expected , or }");
                    }
                }
                case '[': {
                    i++;
                    ArrayList<Object> l = new ArrayList<>();
                    ws();
                    if (peek() == ']') {
                        i++;
                        return l;
                    }
                    for (;;) {
                        ws();
                        l.add(value(depth + 1));
                        ws();
                        char d = next();
                        if (d == ']') return l;
                        if (d != ',') throw error("expected , or ]");
                    }
                }
                case '"':
                    return string();
                case 't':
                    word("true");
                    return Boolean.TRUE;
                case 'f':
                    word("false");
                    return Boolean.FALSE;
                case 'n':
                    word("null");
                    return null;
                default:
                    if (c == '-' || (c >= '0' && c <= '9')) return number();
                    throw error("unexpected '" + c + "'");
            }
        }

        char peek() {
            return i < s.length() ? s.charAt(i) : 0;
        }

        char next() {
            if (i >= s.length()) throw error("unexpected end");
            return s.charAt(i++);
        }

        void expect(char c) {
            if (next() != c) throw error("expected " + c);
        }

        void word(String w) {
            if (!s.startsWith(w, i)) throw error("expected " + w);
            i += w.length();
        }

        Num number() {
            int start = i;
            if (peek() == '-') i++;
            while (i < s.length()) {
                char c = s.charAt(i);
                if ((c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') i++;
                else break;
            }
            return new Num(s.substring(start, i));
        }

        String string() {
            expect('"');
            int start = i;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == '"') {
                    String r = s.substring(start, i);
                    i++;
                    return r;
                }
                if (c == '\\') break;
                i++;
            }
            StringBuilder sb = new StringBuilder(s.substring(start, i));
            for (;;) {
                char c = next();
                if (c == '"') return sb.toString();
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                char e = next();
                switch (e) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        if (i + 4 > s.length()) throw error("bad \\u escape");
                        sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                    }
                    default -> throw error("bad escape");
                }
            }
        }
    }

    // ------------------------------------------------------------- binding

    private static Class<?> raw(Type t) {
        if (t instanceof Class<?> c) return c;
        if (t instanceof ParameterizedType p) return (Class<?>) p.getRawType();
        if (t instanceof GenericArrayType g) return Array.newInstance(raw(g.getGenericComponentType()), 0).getClass();
        if (t instanceof WildcardType w) return raw(w.getUpperBounds()[0]);
        if (t instanceof TypeVariable<?> v) return raw(v.getBounds()[0]);
        return Object.class;
    }

    private static Type arg(Type t, int i) {
        if (t instanceof ParameterizedType p && p.getActualTypeArguments().length > i) {
            return p.getActualTypeArguments()[i];
        }
        return Object.class;
    }

    private static IllegalArgumentException mismatch(Object tree, Type t) {
        String what = tree == null ? "null" : tree instanceof Num ? "a number" : tree instanceof String ? "a string"
                : tree instanceof Boolean ? "a boolean" : tree instanceof List ? "an array" : "an object";
        return new IllegalArgumentException(what + " does not decode into " + t.getTypeName());
    }

    static Object bind(Object tree, Type t) {
        if (t == null) t = Object.class;
        if (t instanceof TypeVariable<?> || t instanceof WildcardType) t = raw(t);
        Class<?> c = raw(t);
        if (c == Object.class) return generic(tree);
        if (c == Json.class) return new Json(write(generic(tree)));
        if (c == Optional.class) return Optional.ofNullable(tree == null ? null : bind(tree, arg(t, 0)));
        if (tree == null) {
            if (c.isPrimitive()) return zero(c);
            return null;
        }
        if (c == String.class) {
            if (tree instanceof String s) return s;
            throw mismatch(tree, t);
        }
        if (tree instanceof Num n) {
            return number(n.text, c, t);
        }
        if (c == boolean.class || c == Boolean.class) {
            if (tree instanceof Boolean b) return b;
            throw mismatch(tree, t);
        }
        if (c == char.class || c == Character.class) {
            if (tree instanceof String s && s.length() == 1) return s.charAt(0);
            throw mismatch(tree, t);
        }
        if (c.isEnum()) {
            if (tree instanceof String s) {
                for (Object e : c.getEnumConstants()) {
                    if (((Enum<?>) e).name().equals(s)) return e;
                }
                throw new IllegalArgumentException("'" + s + "' is not a " + c.getName());
            }
            throw mismatch(tree, t);
        }
        if (c == byte[].class) {
            if (tree instanceof String s) return Base64.getDecoder().decode(s);
            if (tree instanceof List<?> l) {
                byte[] b = new byte[l.size()];
                for (int i = 0; i < b.length; i++) b[i] = (Byte) bind(l.get(i), byte.class);
                return b;
            }
            throw mismatch(tree, t);
        }
        if (c.isArray()) {
            if (!(tree instanceof List<?> l)) throw mismatch(tree, t);
            Type ct = t instanceof GenericArrayType g ? g.getGenericComponentType() : c.getComponentType();
            Object a = Array.newInstance(c.getComponentType(), l.size());
            for (int i = 0; i < l.size(); i++) Array.set(a, i, bind(l.get(i), ct));
            return a;
        }
        if (Collection.class.isAssignableFrom(c) || c == Iterable.class) {
            if (!(tree instanceof List<?> l)) throw mismatch(tree, t);
            Collection<Object> out = Set.class.isAssignableFrom(c) ? new LinkedHashSet<>() : new ArrayList<>();
            if (!c.isInterface() && !Modifier.isAbstract(c.getModifiers()) && !c.isInstance(out)) {
                out = newCollection(c);
            }
            Type et = arg(t, 0);
            for (Object o : l) out.add(bind(o, et));
            return out;
        }
        if (Map.class.isAssignableFrom(c)) {
            if (!(tree instanceof Map<?, ?> m)) throw mismatch(tree, t);
            Map<Object, Object> out = SortedMap.class.isAssignableFrom(c) ? new TreeMap<>() : new LinkedHashMap<>();
            Type kt = arg(t, 0), vt = arg(t, 1);
            for (Map.Entry<?, ?> e : m.entrySet()) {
                out.put(key((String) e.getKey(), kt), bind(e.getValue(), vt));
            }
            return out;
        }
        if (c == UUID.class) {
            if (tree instanceof String s) return UUID.fromString(s);
            throw mismatch(tree, t);
        }
        if (TemporalAccessor.class.isAssignableFrom(c) || c == Duration.class || c == java.time.Period.class
                || java.time.ZoneId.class.isAssignableFrom(c)) {
            if (!(tree instanceof String s)) throw mismatch(tree, t);
            try {
                Method parse = java.time.ZoneId.class.isAssignableFrom(c)
                        ? java.time.ZoneId.class.getMethod("of", String.class)
                        : c.getMethod("parse", CharSequence.class);
                return parse.invoke(null, s);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(e.getCause().getMessage(), e.getCause());
            } catch (ReflectiveOperationException e) {
                throw mismatch(tree, t);
            }
        }
        if (c.isRecord()) {
            if (!(tree instanceof Map<?, ?> m)) throw mismatch(tree, t);
            Shape sh = shape(c);
            Object[] args = new Object[sh.props.size()];
            for (int i = 0; i < args.length; i++) {
                Prop p = sh.props.get(i);
                args[i] = bind(m.get(p.name), p.type);
            }
            try {
                return sh.ctor.newInstance(args);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(e.getCause().toString(), e.getCause());
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("cannot build a " + c.getName() + ": " + e, e);
            }
        }
        if (tree instanceof Map<?, ?> m) {
            Shape sh = shape(c);
            if (sh.ctor == null) {
                throw new IllegalArgumentException(c.getName() + " has no public no-argument constructor");
            }
            try {
                Object o = sh.ctor.newInstance();
                for (Prop p : sh.props) {
                    if (m.containsKey(p.name) && p.canSet()) p.set(o, bind(m.get(p.name), p.type));
                }
                return o;
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(e.getCause().toString(), e.getCause());
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("cannot build a " + c.getName() + ": " + e, e);
            }
        }
        throw mismatch(tree, t);
    }

    @SuppressWarnings("unchecked")
    private static Collection<Object> newCollection(Class<?> c) {
        try {
            return (Collection<Object>) c.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("cannot build a " + c.getName(), e);
        }
    }

    private static Object key(String k, Type t) {
        Class<?> c = raw(t);
        if (c == Object.class || c == String.class) return k;
        if (c.isEnum()) return bind(k, c);
        return number(k, c, t);
    }

    private static Object zero(Class<?> c) {
        if (c == boolean.class) return false;
        if (c == char.class) return (char) 0;
        if (c == double.class) return 0.0;
        if (c == float.class) return 0f;
        if (c == long.class) return 0L;
        if (c == int.class) return 0;
        if (c == short.class) return (short) 0;
        return (byte) 0;
    }

    private static boolean integral(String s) {
        return s.indexOf('.') < 0 && s.indexOf('e') < 0 && s.indexOf('E') < 0;
    }

    private static Object number(String s, Class<?> c, Type t) {
        try {
            if (c == int.class || c == Integer.class) return integral(s) ? Integer.parseInt(s) : exact(s).intValueExact();
            if (c == long.class || c == Long.class) return integral(s) ? Long.parseLong(s) : exact(s).longValueExact();
            if (c == double.class || c == Double.class) return Double.parseDouble(s);
            if (c == float.class || c == Float.class) return Float.parseFloat(s);
            if (c == short.class || c == Short.class) return exact(s).shortValueExact();
            if (c == byte.class || c == Byte.class) return exact(s).byteValueExact();
            if (c == BigDecimal.class) return new BigDecimal(s);
            if (c == BigInteger.class) return exact(s).toBigIntegerExact();
            if (c == Number.class) return generic(new Num(s));
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException(s + " does not decode into " + t.getTypeName());
        }
        throw new IllegalArgumentException("a number does not decode into " + t.getTypeName());
    }

    private static BigDecimal exact(String s) {
        return new BigDecimal(s);
    }

    /** The tree with numbers as Integer, Long, BigInteger or Double. */
    private static Object generic(Object tree) {
        if (tree instanceof Num n) {
            String s = n.text;
            if (integral(s)) {
                if (s.length() < 10) return Integer.parseInt(s);
                try {
                    long l = Long.parseLong(s);
                    if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) return (int) l;
                    return l;
                } catch (NumberFormatException e) {
                    return new BigInteger(s);
                }
            }
            return Double.parseDouble(s);
        }
        if (tree instanceof Map<?, ?> m) {
            LinkedHashMap<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : m.entrySet()) out.put((String) e.getKey(), generic(e.getValue()));
            return out;
        }
        if (tree instanceof List<?> l) {
            ArrayList<Object> out = new ArrayList<>(l.size());
            for (Object o : l) out.add(generic(o));
            return out;
        }
        return tree;
    }

    // ---------------------------------------------------------- reflection

    /** A property: a record component, a public field, or a getter/setter pair. */
    private static final class Prop {
        final String name;
        final Type type;
        final Method getter, setter;
        final Field field;

        Prop(String name, Type type, Method getter, Method setter, Field field) {
            this.name = name;
            this.type = type;
            this.getter = getter;
            this.setter = setter;
            this.field = field;
        }

        Object get(Object o) {
            try {
                return getter != null ? getter.invoke(o) : field.get(o);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException(e.getCause().toString(), e.getCause());
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("cannot read " + name + ": " + e, e);
            }
        }

        boolean canSet() {
            return setter != null || (field != null && !Modifier.isFinal(field.getModifiers()));
        }

        void set(Object o, Object v) throws ReflectiveOperationException {
            if (setter != null) setter.invoke(o, v);
            else field.set(o, v);
        }
    }

    private record Shape(List<Prop> props, Constructor<?> ctor) {}

    private static final ConcurrentHashMap<Class<?>, Shape> SHAPES = new ConcurrentHashMap<>();

    private static List<Prop> props(Class<?> c) {
        return shape(c).props;
    }

    private static <T extends java.lang.reflect.AccessibleObject> T open(T m) {
        try {
            m.setAccessible(true);
        } catch (RuntimeException e) {
            // A public member of a public class needs no access check.
        }
        return m;
    }

    private static Shape shape(Class<?> c) {
        return SHAPES.computeIfAbsent(c, MiniJson::inspect);
    }

    private static Shape inspect(Class<?> c) {
        List<Prop> ps = new ArrayList<>();
        if (c.isRecord()) {
            RecordComponent[] rc = c.getRecordComponents();
            Class<?>[] types = new Class<?>[rc.length];
            for (int i = 0; i < rc.length; i++) {
                types[i] = rc[i].getType();
                ps.add(new Prop(rc[i].getName(), rc[i].getGenericType(), open(rc[i].getAccessor()), null, null));
            }
            try {
                return new Shape(ps, open(c.getDeclaredConstructor(types)));
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("record " + c.getName() + " has no canonical constructor", e);
            }
        }
        Map<String, Prop> byName = new LinkedHashMap<>();
        for (Field f : c.getFields()) {
            int m = f.getModifiers();
            if (Modifier.isStatic(m) || Modifier.isTransient(m)) continue;
            byName.put(f.getName(), new Prop(f.getName(), f.getGenericType(), null, null, open(f)));
        }
        if (byName.isEmpty()) {
            Map<String, Method> setters = new LinkedHashMap<>();
            for (Method m : c.getMethods()) {
                if (Modifier.isStatic(m.getModifiers()) || m.getDeclaringClass() == Object.class) continue;
                String n = m.getName();
                if (m.getParameterCount() == 1 && n.length() > 3 && n.startsWith("set")) {
                    setters.put(decap(n.substring(3)), m);
                }
            }
            for (Method m : c.getMethods()) {
                if (Modifier.isStatic(m.getModifiers()) || m.getDeclaringClass() == Object.class
                        || m.getParameterCount() != 0 || m.getReturnType() == void.class) continue;
                String n = m.getName(), prop = null;
                if (n.length() > 3 && n.startsWith("get")) prop = decap(n.substring(3));
                else if (n.length() > 2 && n.startsWith("is") && (m.getReturnType() == boolean.class)) {
                    prop = decap(n.substring(2));
                }
                if (prop == null) continue;
                byName.put(prop, new Prop(prop, m.getGenericReturnType(), open(m), setters.get(prop), null));
            }
        }
        ps.addAll(byName.values());
        Constructor<?> ctor = null;
        try {
            ctor = open(c.getConstructor());
        } catch (NoSuchMethodException e) {
            try {
                ctor = open(c.getDeclaredConstructor());
            } catch (NoSuchMethodException e2) {
                ctor = null;
            }
        }
        return new Shape(ps, ctor);
    }

    private static String decap(String s) {
        if (s.length() > 1 && Character.isUpperCase(s.charAt(1))) return s;
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }
}
