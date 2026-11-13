package dev.fusee;

/**
 * Starts work elsewhere for an async completion: whoever receives the token
 * finishes the step with {@code engine.complete(token, value)} (or
 * {@code fail}). Runs as a step attempt does, and again with a new token on
 * each retry.
 */
@FunctionalInterface
public interface CompletionSender {
    void send(String token) throws Exception;
}
