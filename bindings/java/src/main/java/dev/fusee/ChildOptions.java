package dev.fusee;

import java.time.Duration;

/** How a child workflow runs. */
public final class ChildOptions {
    String parentClose = "terminate";
    long timeoutMs;

    public ChildOptions() {}

    /**
     * What happens to the child when its parent closes: "terminate" (the
     * default), "request-cancel" or "abandon".
     */
    public ChildOptions parentClose(String policy) {
        parentClose = policy;
        return this;
    }

    /** The child's deadline from now. */
    public ChildOptions timeout(Duration d) {
        timeoutMs = d.toMillis();
        return this;
    }

    long closeBits() {
        return switch (parentClose == null ? "" : parentClose) {
            case "request-cancel" -> 1L << 4;
            case "abandon" -> 2L << 4;
            default -> 0;
        };
    }
}
