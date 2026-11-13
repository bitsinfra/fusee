// A cart object in Java: one object per user, with its own state; calls to
// a key take turns, and a call sent again with the same ID runs once.
// Run: java -cp <fusee classes> CartObject.java
import dev.fusee.CallOptions;
import dev.fusee.Engine;
import dev.fusee.ObjectCtx;
import dev.fusee.ObjectHandler;
import dev.fusee.ObjectRef;
import dev.fusee.Options;
import dev.fusee.TypeRef;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CartObject {
    static final TypeRef<List<Map<String, Object>>> ITEMS = new TypeRef<>() {};

    static List<Map<String, Object>> items(ObjectCtx c) {
        List<Map<String, Object>> all = c.state().get("items", ITEMS);
        return all == null ? new ArrayList<>() : new ArrayList<>(all);
    }

    static long sum(List<Map<String, Object>> all) {
        return all.stream().mapToLong(i -> ((Number) i.get("cents")).longValue()).sum();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void main(String[] args) {
        try (Engine e = Engine.open(Options.local("./data/cart-object"))) {
            e.registerObject("cart", Map.of(
                    "add", ObjectHandler.exclusive(Map.class, (c, item) -> {
                        List<Map<String, Object>> all = items(c);
                        all.add(item);
                        c.state().set("items", all);
                        return all.size();
                    }),
                    "total", ObjectHandler.shared(Object.class, (c, ignored) -> sum(items(c))),
                    "checkout", ObjectHandler.exclusive(Object.class, (c, ignored) -> {
                        List<Map<String, Object>> all = items(c);
                        String receipt = c.ctx().step("charge", String.class,
                                () -> "charged " + sum(all) + " cents for " + all.size() + " items");
                        c.state().clear("items");
                        return receipt;
                    })));
            ObjectRef user = e.object("cart", "user-7");
            Map<String, Object> tea = Map.of("sku", "tea", "cents", 450);
            user.call("add", tea, Integer.class, CallOptions.id("r1"));
            System.out.println("items: " + user.call("add", tea, Integer.class, CallOptions.id("r1"))); // sent again: once
            System.out.println("items: " + user.call("add", Map.of("sku", "milk", "cents", 120), Integer.class,
                    CallOptions.id("r2")));
            System.out.println("total: " + user.call("total", null, Long.class));
            System.out.println("receipt: " + user.call("checkout", null, String.class, CallOptions.id("c1")));
        }
    }
}
