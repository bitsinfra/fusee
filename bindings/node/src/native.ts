// The native module: how the engine is loaded and the calls it answers.
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { workflow } from "./definitions.js";
import { shared } from "./objects.js";

// ---------------------------------------------------------------- native

/** A payload as it crosses from native code: JSON or text as a string, raw bytes as a Buffer. */
export type Wire = string | Buffer | null | undefined;

/** A delivery as the native layer describes it for cancellations and updates. */
export interface NativeDelivery {
  kind: "resolve" | "cancel" | "update";
  seq: number;
  status: number;
  payload: Buffer;
  seqs: number[];
  ordinal: number;
  name?: string;
  id?: string;
  token: number;
}

/** The next item for a run whose code is blocked. */
export interface Delivery {
  kind: "resolve" | "cancel" | "update";
  seq: number;
  status: number;
  /** For a result: its payload, unless `held` (the run's code recorded it and keeps it). */
  payload: Wire;
  held: boolean;
  seqs?: number[];
  ordinal?: number;
  name?: string;
  id?: string;
  token?: number;
}

// The scratch array shared with the native engine (see native/lib.rs): hot
// calls write their results there instead of returning objects.
export const IO_OWED = 8;
export const IO_NEXT = 9;
export const IO_EVENTS = 10;
/** Set while the native driver runs a turn; a kick then sets `IO_KICK` (see native/driver.rs). */
export const IO_DRIVING = 11;
export const IO_KICK = 12;
/** Set before a tick with nothing else waiting for this thread: the tick may wait briefly for a flush in flight. */
export const IO_IDLE = 13;
/** Set before a tick of a turn that only pumps deliveries: the engine is skipped unless it asked for a tick. */
export const IO_LIGHT = 14;
/** What the native driver's current turn was asked for: 2 only deliveries (see `kick`). */
export const IO_TURN = 15;
/** Turns only for deliveries in a row before a full one (background wakes wait at most that long). */
export const DELIVERY_TURNS = 64;
export const IO_COMMIT = 16;
export const IO_EV = IO_COMMIT + 1024;
export const EV_STRIDE = 6;
export const IO_LEN = IO_EV + EV_STRIDE * 2048;
export const enum Ev {
  Wake = 1,
  Ack,
  Reject,
  Start,
  Stuck,
  Ended,
  Result,
  Reply,
  Refuse,
}

/**
 * The native engine. The calls on a step's path are module functions taking
 * the engine's slot (`bindHot`): napi-rs serializes method calls across
 * threads.
 */
export interface NativeEngine {
  registerObject(name: string, shared: string[]): void;
  /** This engine's slot for the `engine*` module functions, on this thread. */
  slot(): number;
  /** Whether this engine holds the ID's shard (false only for another worker's shard). */
  ownsId(id: string): boolean;
  /** 2 replay, 1 run now, 0 wait; `[seq, replay, gate, attempt, notBefore]` in the shared array. */
  beginStep(shard: number, run: number, name: string, readOnly: boolean): number;
  /** Keeps a step name for `beginStepNamed`; returns its number. */
  stepName(name: string): number;
  /** `beginStep` with a name `stepName` kept. */
  beginStepNamed(shard: number, run: number, name: number, readOnly: boolean): number;
  stepGate(shard: number, run: number, gate: number, notBefore: number, inline?: boolean): boolean;
  finishStep(shard: number, run: number, seq: number, ok: boolean, value: string): void;
  finishStepBytes(shard: number, run: number, seq: number, value: Uint8Array): void;
  nextCall(): string | null;
  replyCall(relay: string, response: string): void;
  serve(addr: string, token?: string | null, insecure?: boolean | null, profile?: string | null): string;
  /** The test kit: the earliest timer of this engine's shards. */
  nextTimerMs(): number | null;
  /** The test kit's crash: loads an exported journal as a run. */
  importJournal(file: Buffer, beforeStep?: number | null): void;
  /** An invoked worker: "running", "idle", "expired" or "lost"; null otherwise. */
  invocationState(awaiting: string[]): string | null;
  shards(): number;
  /** Queues the notify callback, as a background wake does. */
  wake(): void;
  /** Whether a native driver runs the turns. */
  driving(): boolean;
  start(
    id: string,
    wtype: string,
    version: number,
    input: string | Uint8Array,
    deadlineMs: number,
    token: number,
    policy: number,
    updateName?: string | null,
    updateId?: string | null,
    updateArg?: string | Uint8Array | null,
    updateToken?: number | null,
    trace?: string | null,
  ): number;
  signal(id: string, name: string, value: string | Uint8Array, token: number, by?: string): void;
  cancel(id: string, reason: string, token: number, by?: string): void;
  resume(id: string, token: number, by?: string): void;
  terminate(id: string, reason: string, token: number, by?: string): void;
  result(id: string): { state: string; shard: number; run: number; status: number; payload?: Buffer };
  inspect(id: string): string | null;
  list(limit: number): string;
  listWhere(filter: string, limit: number): string;
  hostCall(shard: number, op: string, run: number, args: string[], bufs: Buffer[]): number[];
  hostBytes(shard: number, op: string, run: number, args: string[], bufs: Buffer[]): Buffer | null;
  /** `ctx.now()`: the recorded time, or `liveMs` recorded now (`value` of kind 0 without buffers). */
  valueNow(shard: number, run: number, liveMs: number): number;
  /** `ctx.sleep`: the sleep's sequence number (`beginSleep` without string arguments). */
  beginSleep(shard: number, run: number, name: string, atMs: number): number;
  /** `[kind, seq, status, payload state]` in the shared array (see native/lib.rs). */
  quiescent(shard: number, run: number): string | Buffer | NativeDelivery | null;
  /** `finishStep`, then `quiescent`, in one call. */
  finishStepDeliver(shard: number, run: number, seq: number, ok: boolean, value: string): string | Buffer | NativeDelivery | null;
  decideUpdate(shard: number, token: number, ok: boolean, message: string): number;
  update(id: string, name: string, updateId: string, arg: string | Uint8Array, token: number): void;
  queryPoint(id: string): number[];
  queryNative(id: string, name: string, arg: Buffer, token: number): void;
  /** Reads a stream (run -1: the ID's newest); the reply is the read's answer as JSON. */
  streamRead(id: string, run: number, name: string, from: number, waitMs: number, token: number): void;
  ackAt(shard: number, lsn: number, token: number): void;
  complete(token: string, ok: boolean, value: string | Uint8Array, reply: number): void;
  reset(id: string, beforeStep: string, reapplySignals: boolean, version: number, token: number, by?: string): void;
  limit(name: string, concurrent: number, perSecond: number): void;
  /** `[permits held, steps waiting]`. */
  limitUsage(name: string): number[];
  validateSchedule(spec: string): string | null;
  shardMetrics(): string;
  stuck(shard: number, run: number, message: string): void;
  codeState(shard: number, run: number): boolean[];
  exportJournal(id: string): Buffer | null;
  finish(shard: number, run: number, status: number, value: string | Uint8Array): void;
  /** Events in the shared array; null, or `[overflowing events | null, ...strings and payloads]`. */
  tick(): (Float64Array | Wire)[] | null;
  close(): void;
}

export interface NativeReplay {
  import(file: Buffer): string;
  tick(): { events: NativeEvent[] };
  state(run: number): string[];
  hostCall(op: string, run: number, args: string[], bufs: Buffer[]): number[];
  hostBytes(op: string, run: number, args: string[], bufs: Buffer[]): Buffer | null;
  quiescent(run: number): NativeDelivery | null;
  decideUpdate(token: number, ok: boolean, message: string): number;
  finish(run: number, status: number, value: Buffer): void;
  close(): void;
}

/** A run's code starts (or resumes, replaying its journal). */
export interface StartEvent {
  shard: number;
  run: number;
  id: string;
  wtype: string;
  version: number;
  startedMs: number;
  input: Wire;
  /** The W3C traceparent the run joins, if its start was traced. */
  trace?: string;
}

export interface NativeEvent {
  kind: "start" | "wake" | "stuck" | "ended" | "ack" | "reject" | "result" | "reply" | "refuse";
  shard: number;
  run: number;
  token: number;
  id?: string;
  wtype?: string;
  version: number;
  startedMs: number;
  input?: Buffer;
  status: number;
  payload?: Buffer;
  message?: string;
}

export interface NativeClient { call(method: string, args: string): Promise<string>; close(): void }

/** One engine's shards split across worker threads (see `Workers`). */
export interface NativeGroup {
  /** `[id, shards, workers]`. */
  info(): number[];
  /** The worker that owns an ID's shard (or a completion token's); -1 for a malformed token. */
  route(key: string, token: boolean): number;
  limit(name: string, concurrent: number, perSecond: number): void;
  limitUsage(name: string): number[];
  list(limit: number): string;
  listWhere(filter: string, limit: number): string;
  inspect(id: string): string | null;
  exportJournal(id: string): Buffer | null;
  shardMetrics(): string;
  close(): void;
}

/** The calls on a step's path, by engine slot (see `NativeEngine`). */
type Hot = "ownsId" | "beginStep" | "beginStepNamed" | "stepGate" | "finishStep" | "finishStepDeliver" | "finishStepBytes" | "wake" | "hostCall" | "hostBytes" | "valueNow" | "beginSleep" | "quiescent" | "finish" | "tick";
type HotFns = {
  [K in Hot as `engine${Capitalize<K>}`]: (slot: number, ...a: Parameters<NativeEngine[K]>) => ReturnType<NativeEngine[K]>;
};

interface NativeModule extends HotFns {
  connectClient(address: string): Promise<NativeClient>;
  /** The test kit: moves the virtual clock of memory engines (process-wide). */
  setTestClock(offsetMs: number): void;
  NativeEngine: { open(opts: object, notify: () => void, connection: object | undefined, io: Float64Array): NativeEngine };
  NativeGroup: { open(opts: object, workers: number): NativeGroup };
  NativeReplicaMetrics: new () => {snapshot(): string | null};
  prepareConnection(mode: string, address?: string, id?: number, peers?: string[], dir?: string, flush?: string, metrics?: object): Promise<object>;
  NativeReplay: { open(codecKeys?: { id: string; key: Buffer }[] | null): NativeReplay };
  /**
   * `fusee::queue::request` for one debounced start (`a` window, `b` longest
   * wait) or batched event (`a` most events, `b` longest wait), `c` the dedup
   * window, in ms (absent: the defaults): `[coordinator type, coordinator ID,
   * start input JSON, update argument JSON]`.
   */
  queueRequest(kind: "debounce" | "batch", workflow: string, key: string, eventId: string, input: string,
               a?: number, b?: number, c?: number): string[];
}

function loadNative(): NativeModule {
  const req = createRequire(import.meta.url);
  const here = dirname(fileURLToPath(import.meta.url));
  const platform = `${process.platform}-${process.arch}`;
  const candidates = [
    process.env.FUSEE_NATIVE,
    join(here, "..", `fusee.${platform}.node`),
    join(here, `fusee.${platform}.node`),
  ].filter((p): p is string => !!p);
  for (const c of candidates) {
    if (existsSync(c)) return req(c) as NativeModule;
  }
  // Prebuilt per-platform packages (napi-rs layout).
  if (process.platform === "linux") {
    const report = process.report?.getReport() as { header?: { glibcVersionRuntime?: string } } | undefined;
    const libc = report?.header?.glibcVersionRuntime ? "gnu" : "musl";
    return req(`fusee-${platform}-${libc}`) as NativeModule;
  }
  return req(`fusee-${platform}`) as NativeModule;
}

export const native = loadNative();

/**
 * Binds the calls on a step's path to the engine's slot: napi-rs registers
 * every method call on a native object in one process-wide map behind a
 * mutex, which serializes worker threads, while module functions skip it.
 */
export function bindHot(n: NativeEngine): NativeEngine {
  const s = n.slot();
  const m = native;
  return Object.assign(n, {
    ownsId: (id: string) => m.engineOwnsId(s, id),
    beginStep: (shard: number, run: number, name: string, readOnly: boolean) => m.engineBeginStep(s, shard, run, name, readOnly),
    beginStepNamed: (shard: number, run: number, name: number, readOnly: boolean) => m.engineBeginStepNamed(s, shard, run, name, readOnly),
    stepGate: (shard: number, run: number, gate: number, notBefore: number, inline?: boolean) =>
      m.engineStepGate(s, shard, run, gate, notBefore, inline),
    finishStep: (shard: number, run: number, seq: number, ok: boolean, value: string) => m.engineFinishStep(s, shard, run, seq, ok, value),
    finishStepDeliver: (shard: number, run: number, seq: number, ok: boolean, value: string) =>
      m.engineFinishStepDeliver(s, shard, run, seq, ok, value),
    finishStepBytes: (shard: number, run: number, seq: number, value: Uint8Array) => m.engineFinishStepBytes(s, shard, run, seq, value),
    wake: () => m.engineWake(s),
    hostCall: (shard: number, op: string, run: number, args: string[], bufs: Buffer[]) => m.engineHostCall(s, shard, op, run, args, bufs),
    hostBytes: (shard: number, op: string, run: number, args: string[], bufs: Buffer[]) => m.engineHostBytes(s, shard, op, run, args, bufs),
    valueNow: (shard: number, run: number, liveMs: number) => m.engineValueNow(s, shard, run, liveMs),
    beginSleep: (shard: number, run: number, name: string, atMs: number) => m.engineBeginSleep(s, shard, run, name, atMs),
    quiescent: (shard: number, run: number) => m.engineQuiescent(s, shard, run),
    finish: (shard: number, run: number, status: number, value: string | Uint8Array) => m.engineFinish(s, shard, run, status, value),
    tick: () => m.engineTick(s),
  });
}

export const IS_BUN = typeof process.versions.bun === "string";
/**
 * Node: a tick the SDK asks for goes through the same native callback as a
 * background wake (one uv_async per turn), which on macOS costs a fraction
 * of a setImmediate turn under load. `FUSEE_KICK=immediate` restores
 * setImmediate. Bun runs those callbacks back to back without draining
 * microtasks, so it keeps setImmediate.
 */
export const KICK_NATIVE = !IS_BUN && process.env.FUSEE_KICK !== "immediate";
/** The default for `OpenOptions.driver` (`FUSEE_DRIVER` overrides it). */
export const DRIVER = process.env.FUSEE_DRIVER === "macrotask" ? "macrotask" : "native";

/** @internal The test kit's virtual time: ms ahead of the wall clock (`fusee/testing`). */
export const TEST_CLOCK = {
  offset: 0,
  /** Moves the clock of this module and of the native engines' memory env. */
  set(offset: number): void {
    this.offset = offset;
    native.setTestClock(offset);
  },
};
/** Unix ms by the engine's clock (the test kit's virtual time moves it). */
export const nowMs = (): number => Date.now() + TEST_CLOCK.offset;
