package dev.fusee;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Edits of a key's state from outside its handlers. */
class StateEditTest {
    @Test
    @SuppressWarnings("unchecked")
    void editsSetClearAndPatchState() {
        try (Engine e = Engine.open(Options.memory().shards(2))) {
            e.registerObject("cart", Map.of("add", ObjectHandler.exclusive(String.class, (ctx, item) -> {
                List<String> items = ctx.state().get("items", List.class);
                List<String> next = items == null ? new ArrayList<>() : new ArrayList<>(items);
                next.add(item);
                ctx.state().set("items", next);
                return next.size();
            })));
            ObjectRef c = e.object("cart", "u-1");
            assertEquals(1, c.call("add", "apple", Integer.class));
            assertEquals(List.of("owner"), ((Map<String, Object>) c.setState("owner", "ana").value()).get("changed"));
            c.patchState(List.of(Map.of("op", "add", "path", "/items/-", "value", "pear")));
            assertEquals(Map.of("items", List.of("apple", "pear"), "owner", "ana"), c.state().value());
            String version = (String) ((Map<String, Object>) c.inspect().object().value()).get("version");
            Json ok = c.editState(Map.of("clear", List.of("owner")), version);
            assertNotEquals(version, ((Map<String, Object>) ok.value()).get("version"));
            RuntimeException stale = assertThrows(RuntimeException.class, () -> c.editState(Map.of("clearAll", true), version));
            assertTrue(stale.getMessage().contains("changed since version"), stale.getMessage());
            c.clearState(null);
            assertEquals(Map.of(), c.state().value());
        }
    }
}
