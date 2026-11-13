package dev.fusee;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * How to open an engine. The default is a local engine whose data directory
 * is ./fusee-data. Setters return this options object.
 */
public final class Options {
    String durability = "local";
    String dir;
    String address;
    int replicaId = -1;
    final List<String> peers = new ArrayList<>();
    int shards;
    Duration retention, parkAfter, checkpointInterval;
    String flush;
    long segmentBytes;
    boolean keepHistory, archiveHistory;
    Exhausted onExhausted = Exhausted.FAIL;
    final LinkedHashMap<String, byte[]> codec = new LinkedHashMap<>();
    Boolean inlineFlush;
    JsonCodec json;
    Executor stepExecutor;
    final List<WorkflowDef<?, ?>> workflows = new ArrayList<>();

    public Options() {}

    /** Workflow definitions ({@link WorkflowDef}) to register as the engine opens. */
    public Options workflows(WorkflowDef<?, ?>... defs) {
        workflows.addAll(List.of(defs));
        return this;
    }

    /** A local engine in this data directory. */
    public static Options local(String dir) {
        return new Options().dir(dir);
    }

    /** An engine whose log is memory only (tests, benchmarks). */
    public static Options memory() {
        return new Options().durability("memory");
    }

    /** A worker of a Fusee server (host:port, unix:/path.sock, with ?options). */
    public static Options server(String address) {
        return new Options().durability("server").address(address);
    }

    /** A member of a replicated group: its index in peers, every member's address, its data directory. */
    public static Options replicated(int replicaId, List<String> peers, String dir) {
        Options o = new Options().durability("replicated").replicaId(replicaId).dir(dir);
        o.peers.addAll(peers);
        return o;
    }

    /** "local" (the default), "memory", "server" or "replicated". */
    public Options durability(String d) {
        durability = d;
        return this;
    }

    /** Local: the data directory. Replicated: this member's directory. */
    public Options dir(String d) {
        dir = d;
        return this;
    }

    /** Server: host:port. Replicated: this member's connection options ({@code ?cert=..&key=..&ca=..&token-file=..}). */
    public Options address(String a) {
        address = a;
        return this;
    }

    public Options replicaId(int id) {
        replicaId = id;
        return this;
    }

    public Options peers(String... addresses) {
        peers.clear();
        peers.addAll(Arrays.asList(addresses));
        return this;
    }

    /** Shards of a new engine (default 1); a server's engine has the server's count. */
    public Options shards(int n) {
        shards = n;
        return this;
    }

    public Options retention(Duration d) {
        retention = d;
        return this;
    }

    public Options parkAfter(Duration d) {
        parkAfter = d;
        return this;
    }

    public Options checkpointInterval(Duration d) {
        checkpointInterval = d;
        return this;
    }

    /** "fsync", "fdatasync", "fullfsync" or "dsync" (default: F_FULLFSYNC on macOS, fdatasync on Linux). */
    public Options flush(String method) {
        flush = method;
        return this;
    }

    public Options segmentBytes(long n) {
        segmentBytes = n;
        return this;
    }

    /** Keep finished runs' journals (inspect, export, reset). */
    /**
     * What a step does when its retries run out, for every step whose retry
     * policy does not say: {@link Exhausted#PAUSE} pauses its run until
     * {@link Run#resume()}. Default {@link Exhausted#FAIL}.
     */
    public Options onExhausted(Exhausted then) {
        onExhausted = then;
        return this;
    }

    public Options keepHistory(boolean on) {
        keepHistory = on;
        return this;
    }

    public Options archiveHistory(boolean on) {
        archiveHistory = on;
        return this;
    }

    /**
     * Adds an AES-256-GCM payload key (32 bytes): the first key added
     * encrypts, every key decrypts.
     */
    public Options codecKey(String id, byte[] key) {
        if (key == null || key.length != 32) throw Errors.invalid("key '" + id + "' is not 32 bytes");
        codec.put(id, key.clone());
        return this;
    }

    /** The keys in order (see codecKey). */
    public Options codec(Map<String, byte[]> keys) {
        codec.clear();
        keys.forEach(this::codecKey);
        return this;
    }

    /** Off: every flush goes to the log thread, so engine calls never wait for the disk (default on). */
    public Options inlineFlush(boolean on) {
        inlineFlush = on;
        return this;
    }

    /** The JSON codec for payloads (default {@link JsonCodec#builtIn()}). */
    public Options json(JsonCodec c) {
        json = c;
        return this;
    }

    /**
     * Where step functions run (default: a cached pool of daemon platform
     * threads whose idle workers spin briefly before they park, shared by
     * every engine). For many concurrent steps that block on I/O, pass
     * {@code Executors.newVirtualThreadPerTaskExecutor()} on Java 21 and later.
     */
    public Options stepExecutor(Executor e) {
        stepExecutor = e;
        return this;
    }
}
