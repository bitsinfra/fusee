package dev.fusee;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * A generic type, for results whose class alone does not say what to
 * decode: {@code new TypeRef<List<String>>() {}}.
 */
public abstract class TypeRef<T> {
    private final Type type;

    protected TypeRef() {
        Type s = getClass().getGenericSuperclass();
        if (!(s instanceof ParameterizedType p)) {
            throw new IllegalArgumentException("write new TypeRef<...>() {} with a type argument");
        }
        type = p.getActualTypeArguments()[0];
    }

    private TypeRef(Type type) {
        this.type = type;
    }

    /** The type of a class. */
    public static <T> TypeRef<T> of(Class<T> c) {
        return new TypeRef<T>(c) {};
    }

    /** A type built elsewhere (unchecked). */
    public static <T> TypeRef<T> of(Type t) {
        return new TypeRef<T>(t) {};
    }

    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return type.getTypeName();
    }
}
