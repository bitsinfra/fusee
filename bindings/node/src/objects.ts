// Objects: keyed state and their handlers.
import { dec, decode, encode, ms, parseJson, tagged, text, wire } from "./codec.js";
import { Engine } from "./engine.js";
import { FuseeError } from "./errors.js";
import { Ctx, RunExec } from "./execution.js";
import { Duration, USE_EXISTING, WorkflowFn } from "./types.js";

// ---------------------------------------------------------------- objects

/** The query every object answers with its committed state (`fusee::object::STATE_QUERY`). */
export const STATE_QUERY = "fusee.state";
/** Update IDs of one-way calls start with this (`fusee::object::SEND_PREFIX`). */
export const SEND_PREFIX = "fusee.send/";
/** The handler every object type has that edits its state from outside (`fusee::object::EDIT_HANDLER`). */
export const EDIT_HANDLER = "fusee.state.edit";

/** One operation of a JSON Patch (RFC 6902). */
export type JsonPatchOp =
  | { op: "add" | "replace" | "test"; path: string; value: unknown }
  | { op: "remove"; path: string }
  | { op: "move" | "copy"; from: string; path: string };

/**
 * An edit of a key's state from outside its handlers, applied in this
 * order: `clearAll`, `clear`, `set` (a `null` value removes the field),
 * `patch` (a JSON Patch over the state as one object).
 */
export interface StateEdit {
  clearAll?: boolean;
  clear?: string[];
  set?: Record<string, unknown>;
  patch?: JsonPatchOp[];
}

/** Options of a state edit. */
export interface EditOptions extends CallOptions {
  /** Refuse the edit unless the state is still at this version (`inspect().object.version`). */
  ifVersion?: string;
}

/** What a state edit answers: the state's new version and the fields it changed. */
export interface EditResult {
  version: string;
  changed: string[];
}

/** An edit with its options, as the edit handler takes it. */
export function editArg(edit: StateEdit, opts: EditOptions): Record<string, unknown> {
  return opts.ifVersion === undefined ? { ...edit } : { ...edit, ifVersion: opts.ifVersion };
}

/** An object's handler: workflow code with the key's state. */
export type ObjectHandler<A = any, R = any> = (ctx: ObjectCtx, arg: A) => R | Promise<R>;

/** A handler marked with `shared`. */
export interface SharedHandler<A = any, R = any> {
  readonly shared: true;
  readonly handle: ObjectHandler<A, R>;
}

/** An object type's handlers by name: exclusive unless marked with `shared`. */
export type ObjectHandlers = Record<string, ObjectHandler | SharedHandler>;

/**
 * Marks a handler as shared: it runs as soon as its call arrives, next to
 * other calls (an exclusive one included), and only reads state.
 */
export function shared<A, R>(handle: ObjectHandler<A, R>): SharedHandler<A, R> {
  return { shared: true, handle };
}

/** A key's state as a handler sees it: JSON values under string keys. */
export class ObjectState {
  /** @internal */
  constructor(
    private readonly x: RunExec,
    private readonly exclusive: boolean,
  ) {}

  /** The value, or `undefined` when the key has none. An exclusive handler sees its own writes. */
  get<T = unknown>(key: string): T | undefined {
    const b = this.x.hb("objectGet", [key, this.exclusive ? "1" : "0"]);
    return b && b.length ? (parseJson(dec.decode(b)) as T) : undefined;
  }

  /** Sets a value (`null` or `undefined` clears it); committed when the handler returns. */
  set(key: string, value: unknown): void {
    if (value === undefined || value === null) return this.clear(key);
    if (value instanceof Uint8Array) throw new FuseeError("state values are JSON, not bytes");
    this.writable();
    this.x.h("objectSet", [key, "1"], [encode(value)]);
  }

  clear(key: string): void {
    this.writable();
    this.x.h("objectSet", [key, "0"]);
  }

  /** Clears every key. */
  clearAll(): void {
    for (const k of this.keys()) this.clear(k);
  }

  /** The keys that hold a value, sorted. */
  keys(): string[] {
    const b = this.x.hb("objectKeys", [this.exclusive ? "1" : "0"]);
    return b ? (JSON.parse(dec.decode(b)) as string[]) : [];
  }

  private writable(): void {
    if (!this.exclusive) throw new FuseeError("a shared handler cannot change state");
  }
}

/** The context of an object's handler: the workflow context of the key's run, its state and its key. */
export class ObjectCtx extends Ctx {
  /** The key's state. */
  readonly state: ObjectState;
  /** @internal */
  constructor(
    x: RunExec,
    /** Whether this handler is exclusive (it may change state). */
    readonly exclusive: boolean,
  ) {
    super(x);
    this.state = new ObjectState(x, exclusive);
  }

  /** The object's key. */
  get objectKey(): string {
    return this.workflowId.slice(this.workflowType.length + 1);
  }
}

/** A key called from workflow code (`ctx.object(type, key)`). */
export class ObjectCall {
  /** @internal */
  constructor(
    private readonly x: RunExec,
    readonly type: string,
    readonly key: string,
  ) {}

  /** Calls a handler and waits for its result: a journal operation, run exactly once. */
  call<R = unknown, A = unknown>(handler: string, arg?: A): Promise<R> {
    const x = this.x;
    const [seq = 0] = x.h("beginCall", [this.type, this.key, handler, "0"], [encode(arg)]);
    x.be.kick();
    return x.op<R>("child", `${this.type}.${handler}`, seq);
  }

  /** Calls a handler without waiting: the outbox delivers the call once this point is durable. */
  send<A = unknown>(handler: string, arg?: A): void {
    this.x.h("beginCall", [this.type, this.key, handler, "1"], [encode(arg)]);
    this.x.be.kick();
  }
}

/** The run behind a workflow context. */
function execOf(ctx: Ctx): RunExec {
  return (ctx as unknown as { x: RunExec }).x;
}

/** An object type's code: its handlers as updates of the key's run, and a state query. It never returns. */
export function objectMain(handlers: ObjectHandlers): WorkflowFn<unknown, unknown> {
  const list = Object.entries(handlers).map(([name, h]) =>
    typeof h === "function" ? { name, exclusive: true, handle: h } : { name, exclusive: !h.shared, handle: h.handle },
  );
  return (ctx) => {
    const x = execOf(ctx);
    for (const h of list) {
      const octx = new ObjectCtx(x, h.exclusive);
      ctx.onUpdate(h.name, (arg) => h.handle(octx, arg));
    }
    // Every type takes edits of its state from outside (the engine applies them).
    ctx.onUpdate(EDIT_HANDLER, (edit) => JSON.parse(text(x.hb("objectEdit", [], [Buffer.from(JSON.stringify(edit))]) ?? undefined)));
    ctx.onQuery(STATE_QUERY, () => JSON.parse(text(x.hb("objectState") ?? undefined) || "{}"));
    // An object's code waits for its calls, with nothing pending.
    x.idle = true;
    return new Promise<never>(() => {});
  };
}

/** An object type, to address its keys. */
export class ObjectType {
  /** @internal */
  constructor(
    private readonly engine: Engine,
    readonly name: string,
  ) {}

  /** The object with key `key`. */
  key(key: string): ObjectRef {
    return this.engine.objectRef(this.name, key);
  }
}

/** Options of a call from outside workflow code. */
export interface CallOptions {
  /** An idempotency ID: calls with the same ID run once, and each gets the recorded result. */
  id?: string;
}

/** A key of an object type, called from outside workflow code. */
export class ObjectRef {
  /** @internal */
  constructor(
    private readonly engine: Engine,
    readonly type: string,
    readonly key: string,
  ) {}

  /** The key's workflow ID (`<type>/<key>`). */
  get id(): string {
    return `${this.type}/${this.key}`;
  }

  /** Calls a handler and waits for its durable result. */
  call<R = unknown, A = unknown>(handler: string, arg?: A, opts: CallOptions = {}): Promise<R> {
    return this.engine.updateWithStartWire(
      this.type, 1, this.id, "null", undefined, 0, USE_EXISTING, handler, opts.id ?? "", wire(arg),
    ) as Promise<R>;
  }

  /** Calls a handler without waiting for its result: resolves once the call is accepted. */
  async send<A = unknown>(handler: string, arg?: A, opts: CallOptions = {}): Promise<void> {
    await this.engine.updateWithStartWire(
      this.type, 1, this.id, "null", undefined, 0, USE_EXISTING, handler, SEND_PREFIX + (opts.id ?? ""), wire(arg),
    );
  }

  /**
   * Edits the state from outside its handlers: an exclusive call, so it
   * waits for the key and commits as a handler's writes do.
   */
  editState(edit: StateEdit, opts: EditOptions = {}): Promise<EditResult> {
    return this.call<EditResult>(EDIT_HANDLER, editArg(edit, opts), opts);
  }

  /** Sets one field of the state. */
  setState(field: string, value: unknown, opts: EditOptions = {}): Promise<EditResult> {
    return this.editState({ set: { [field]: value } }, opts);
  }

  /** Applies a JSON Patch (RFC 6902) to the state as one JSON object. */
  patchState(ops: JsonPatchOp[], opts: EditOptions = {}): Promise<EditResult> {
    return this.editState({ patch: ops }, opts);
  }

  /** Clears one field, or (none named) the whole state; the key remains. */
  clearState(field?: string, opts: EditOptions = {}): Promise<EditResult> {
    return this.editState(field === undefined ? { clearAll: true } : { clear: [field] }, opts);
  }

  /** The committed state (`{}` for a key never called), once it is durable. */
  async state<T = Record<string, unknown>>(): Promise<T> {
    try {
      return await this.engine.query<T>(this.id, STATE_QUERY, null);
    } catch (e) {
      if (e instanceof FuseeError && e.code === "NotFound") return {} as T;
      throw e;
    }
  }

  /** The key's run: its journal, and `object` with its state and pending calls. */
  inspect(): any {
    return this.engine.run(this.id).inspect();
  }

  /** Deletes the key: its run ends and its state with it; the next call starts empty. */
  async delete(): Promise<void> {
    try {
      await this.engine.run(this.id).terminate("object deleted");
    } catch (e) {
      if (!(e instanceof FuseeError && e.code === "NotFound")) throw e;
    }
  }
}

/** Payloads framed by the native side: each a 4-byte little-endian length, then it. */
function unframe(b: Buffer | null): Buffer[] {
  const out: Buffer[] = [];
  for (let at = 0; b && at + 4 <= b.length; ) {
    const n = b.readUInt32LE(at);
    out.push(b.subarray(at + 4, at + 4 + n));
    at += 4 + n;
  }
  return out;
}

/** A writer of one of the run's streams (`ctx.stream`). */
export class StreamWriter {
  /** @internal */
  constructor(
    private readonly x: RunExec,
    readonly name: string,
    private readonly step: number | undefined,
    /** Chunks the step's earlier attempts recorded (0 on a first attempt and in workflow code). */
    readonly recordedCount: number,
    /** The stream was complete when it was opened (an earlier attempt of the step completed it: appends are dropped). */
    readonly closed: boolean,
    private readonly seq: () => number | undefined,
  ) {}

  /** Appends a chunk: a `Uint8Array` as bytes, anything else as JSON. Never waits for the disk. */
  append(value: unknown): void {
    this.x.h("streamAppend", [this.name, this.seq() ?? "w"], [tagged(wire(value))]);
    this.x.be.kick();
  }

  /** Completes the stream: readers end once they have every chunk. */
  close(): void {
    this.x.h("streamClose", [this.name, this.seq() ?? "w"]);
    this.x.be.kick();
  }

  /**
   * The chunks the step's earlier attempts recorded (after a crash, the
   * durable ones): continue after them, so readers see no chunk twice.
   */
  recorded<T = unknown>(): T[] {
    if (this.step === undefined || this.recordedCount === 0) return [];
    return unframe(this.x.hb("streamRecorded", [this.name, this.step, this.recordedCount])).map((p) => decode<T>(p));
  }
}

/** A read's answer (see `fusee::stream::StreamBatch`). */
interface StreamBatch {
  run: number;
  from: number;
  next: number;
  chunks: unknown[];
  /** Indexes of chunks that are bytes (base64). */
  bytes?: number[];
  end: { status: string; closed: boolean } | null;
}

export interface StreamOptions {
  /** The offset to start at (default 0): where a reader that reconnects continues. */
  from?: number;
  /** How long one read waits for a chunk before it asks again (default 30 s). */
  wait?: Duration;
}

/**
 * A reader of a run's stream: yields each chunk once it is durable, in
 * order, and ends once the stream is complete. `offset` is where a reader
 * that reconnects continues; `status` says how the stream ended.
 */
export class StreamReader<T = unknown> implements AsyncIterable<T> {
  /** The offset of the next chunk. */
  offset: number;
  /** The run being read, once the first read found it (pass it back to keep reading it). */
  run: number | undefined;
  /** How the stream ended: `ok`, a failed step's `failed`, or the run's status if it ended first. */
  status: string | undefined;
  /** @internal */
  constructor(
    private readonly read: (run: number | undefined, from: number, waitMs: number) => Promise<unknown>,
    opts: StreamOptions & { run?: number } = {},
  ) {
    this.offset = opts.from ?? 0;
    this.run = opts.run;
    this.waitMs = ms(opts.wait) ?? 30_000;
  }
  private readonly waitMs: number;

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    while (this.status === undefined) {
      const b = (await this.read(this.run, this.offset, this.waitMs)) as StreamBatch;
      this.run = b.run;
      const bytes = b.bytes?.length ? new Set(b.bytes) : undefined;
      for (let i = 0; i < b.chunks.length; i++) {
        this.offset = b.from + i + 1;
        yield (bytes?.has(i) ? Buffer.from(b.chunks[i] as string, "base64") : b.chunks[i]) as T;
      }
      this.offset = Math.max(this.offset, b.next);
      if (b.end) this.status = b.end.status;
    }
  }
}
