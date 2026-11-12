// An AI agent loop with tool calls in Java: every model and tool call is a
// step. Run: java -cp <fusee classes> AgentLoop.java
import dev.fusee.Engine;
import dev.fusee.Options;
import dev.fusee.StartOptions;
import dev.fusee.WorkflowDef;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class AgentLoop {
    @SuppressWarnings("rawtypes")
    static Map model(List<String> history) {
        for (String h : history) {
            if (h.startsWith("tool:")) return Map.of("answer", "Pack an umbrella (" + history.get(history.size() - 1) + ").");
        }
        return Map.of("tool", "weather", "args", "Lisbon");
    }

    static final Map<String, Function<String, String>> TOOLS = Map.of("weather", city -> "tool: rain in " + city);

    @SuppressWarnings("rawtypes")
    static final WorkflowDef<String, String> AGENT = WorkflowDef.workflow("agent", String.class, String.class,
            (ctx, question) -> {
                List<String> history = new ArrayList<>(List.of("user: " + question));
                for (int turn = 0; turn < 10; turn++) {
                    List<String> seen = List.copyOf(history);
                    Map t = ctx.step("model-" + turn, Map.class, () -> model(seen));
                    if (t.get("answer") != null) return (String) t.get("answer");
                    String tool = (String) t.get("tool");
                    history.add(ctx.step("tool-" + turn + "-" + tool, String.class,
                            () -> TOOLS.get(tool).apply((String) t.get("args"))));
                }
                throw new IllegalStateException("no answer after 10 turns");
            });

    public static void main(String[] args) {
        try (Engine e = Engine.open(Options.local("./data/agent-loop").workflows(AGENT))) {
            System.out.println(AGENT.on(e).start("trip-question-1", "Do I need an umbrella in Lisbon?",
                    new StartOptions().ifRunning("use-existing").ifFinished("use-existing")).result());
        }
    }
}
