package dev.fusee;

/** A registered object type. */
public final class ObjectType {
    private final Engine e;
    private final String name;
    private final int version;

    ObjectType(Engine e, String name, int version) {
        this.e = e;
        this.name = name;
        this.version = version;
    }

    public String name() {
        return name;
    }

    public int version() {
        return version;
    }

    /** The object with key key. */
    public ObjectRef key(String key) {
        return e.object(name, key);
    }
}
