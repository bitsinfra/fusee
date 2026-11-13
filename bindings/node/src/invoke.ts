// Workers a server invokes over HTTP.
import { createHmac, timingSafeEqual } from "node:crypto";
import { isMainThread, workerData } from "node:worker_threads";
import { Engine } from "./engine.js";
import { FuseeError } from "./errors.js";
import { delay } from "./execution.js";
import { nowMs } from "./native.js";
import { OpenOptions } from "./types.js";
import { WORKER_KEY, bootWorker } from "./workers.js";

/** The header a Fusee server signs invocations in. */
export const SIGNATURE_HEADER = "fusee-signature";

export interface InvocationHandlerOptions extends Omit<OpenOptions, "durability" | "address" | "workers" | "module"> {
  /** The server, with this worker's connection options: `host:port?ca=...&token-file=...`. */
  address: string;
  /**
   * The endpoint's signing secret (the server's `--invoke-secret-file`).
   * Leave it out only behind mutual TLS or on a network only the server reaches.
   */
  secret?: string | Uint8Array;
  /** Registers the workflows on each invocation's fresh engine. */
  register: (engine: Engine) => void | Promise<void>;
}

/** An HTTP answer: a status and a JSON body. */
export interface InvocationReply {
  statusCode: number;
  headers: Record<string, string>;
  body: string;
}

type Headers = Record<string, string | string[] | undefined> | { get(name: string): string | null };

function headerValue(h: Headers | undefined, name: string): string | undefined {
  if (!h) return undefined;
  if (typeof (h as { get?: unknown }).get === "function") return (h as { get(n: string): string | null }).get(name) ?? undefined;
  for (const [k, v] of Object.entries(h as Record<string, string | string[] | undefined>)) {
    if (k.toLowerCase() === name) return Array.isArray(v) ? v[0] : v;
  }
  return undefined;
}

/** Checks a `fusee-signature` value (`t=<unix ms>,v1=<hex HMAC-SHA256>`) within five minutes. */
export function verifySignature(secret: string | Uint8Array, header: string, body: Uint8Array, nowMs = Date.now()): string | null {
  let t: number | undefined;
  const sigs: string[] = [];
  for (const part of header.split(",")) {
    const [k, v] = part.trim().split("=", 2);
    if (k === "t" && v) t = Number(v);
    else if (k === "v1" && v) sigs.push(v);
  }
  if (t === undefined || !Number.isFinite(t)) return "the signature has no timestamp";
  if (Math.abs(nowMs - t) > 300_000) return "the signature is too old (or the clocks differ by over five minutes)";
  const mac = createHmac("sha256", secret).update(`${t}.`).update(body).digest();
  for (const s of sigs) {
    const got = Buffer.from(s, "hex");
    if (got.length === mac.length && timingSafeEqual(got, mac)) return null;
  }
  return "the signature does not match";
}

/** `address` with `invocation=<id>` among its options. */
function withInvocation(address: string, id: string): string {
  return `${address}${address.includes("?") ? "&" : "?"}invocation=${id}`;
}

/**
 * The worker side of invocations over HTTP (suspend to zero): when a Fusee
 * server started with `--invoke URL` has work for shards no worker holds, it
 * POSTs to that URL; each request opens a fresh engine for the invocation's
 * shards, runs them until they are idle or the budget ends, and closes it.
 * Mount `node` on `node:http` or Express (before any body parser, or after
 * `express.raw()`), export `lambda` as an AWS Lambda handler, or use `fetch`
 * with the Fetch API (Bun, Deno, Vercel, Cloudflare).
 */
export class InvocationHandler {
  constructor(private readonly opts: InvocationHandlerOptions) {
    if (opts.secret !== undefined && Buffer.byteLength(opts.secret) < 16)
      throw new FuseeError("an invocation secret needs at least 16 bytes; use 32 random bytes");
  }

  /** Verifies, runs the invocation, and answers: 200 with a summary, 401, 400 or 503. */
  async invoke(req: { headers?: Headers; body: string | Uint8Array }): Promise<InvocationReply> {
    const body = typeof req.body === "string" ? Buffer.from(req.body) : Buffer.from(req.body);
    const reply = (statusCode: number, v: unknown): InvocationReply => ({
      statusCode,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(v),
    });
    if (this.opts.secret !== undefined) {
      const sig = headerValue(req.headers, SIGNATURE_HEADER);
      if (!sig) return reply(401, { error: "unsigned request" });
      const problem = verifySignature(this.opts.secret, sig, body);
      if (problem) return reply(401, { error: problem });
    }
    let request: { invocation?: unknown };
    try {
      request = JSON.parse(body.toString("utf8"));
    } catch (e) {
      return reply(400, { error: String(e) });
    }
    const id = request?.invocation;
    if (typeof id !== "string" || !/^[0-9a-f]{16}$/.test(id) || /^0+$/.test(id)) return reply(400, { error: "invalid invocation ID" });
    const began = performance.now();
    let engine: Engine;
    try {
      const { address, secret: _s, register: _r, ...rest } = this.opts;
      // Runs park soon after they suspend, so the next invocation's open stays cheap.
      engine = await Engine.open({ parkAfter: 50, ...rest, durability: "server", address: withInvocation(address, id) });
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      // The server refused: the invocation is over or its shards are held.
      if (/refused this worker/.test(message)) return reply(200, { invocation: id, skipped: message });
      return reply(503, { error: message });
    }
    let outcome = "running";
    let error: string | undefined;
    const openMs = performance.now() - began;
    try {
      await this.opts.register(engine);
      for (;;) {
        outcome = engine.invocationState() ?? "idle";
        if (outcome !== "running") break;
        await delay(5);
      }
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
    let steps = 0;
    try {
      steps = engine.shardMetrics().reduce((a, m) => a + m.steps_run, 0);
    } catch {
      // The engine closed itself: its connection to the server is gone.
    }
    try {
      await engine.close();
    } catch (e) {
      error ??= e instanceof Error ? e.message : String(e);
    }
    return reply(200, { invocation: id, outcome, steps, openMs, ms: performance.now() - began, error });
  }

  /** For `node:http`, Express and other `(req, res)` frameworks. */
  get node(): (req: any, res: any) => void {
    return (req, res) => {
      const finish = (body: Uint8Array | string) =>
        this.invoke({ headers: req.headers, body })
          .catch((e): InvocationReply => ({
            statusCode: 500,
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ error: e instanceof Error ? e.message : String(e) }),
          }))
          .then((r) => {
            res.writeHead(r.statusCode, r.headers);
            res.end(r.body);
          });
      if (req.body !== undefined && (typeof req.body === "string" || req.body instanceof Uint8Array)) {
        void finish(req.body);
        return;
      }
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => void finish(Buffer.concat(chunks)));
    };
  }

  /** An AWS Lambda handler (function URL, API Gateway or ALB proxy events). */
  get lambda(): (event: { headers?: Record<string, string | undefined>; body?: string | null; isBase64Encoded?: boolean }) => Promise<InvocationReply> {
    return (event) => {
      const raw = event.body ?? "";
      const body = event.isBase64Encoded ? Buffer.from(raw, "base64") : Buffer.from(raw);
      return this.invoke({ headers: event.headers ?? {}, body });
    };
  }

  /** For the Fetch API: `(request: Request) => Promise<Response>`. */
  get fetch(): (request: Request) => Promise<Response> {
    return async (request) => {
      const r = await this.invoke({ headers: request.headers, body: new Uint8Array(await request.arrayBuffer()) });
      return new Response(r.body, { status: r.statusCode, headers: r.headers });
    };
  }
}

/** See {@link InvocationHandler}. */
export function invocationHandler(opts: InvocationHandlerOptions): InvocationHandler {
  return new InvocationHandler(opts);
}

// Loaded as a worker thread's entry point by `Workers.start`.
if (!isMainThread && workerData && typeof workerData === "object" && WORKER_KEY in workerData)
  void bootWorker((workerData as Record<string, Parameters<typeof bootWorker>[0]>)[WORKER_KEY]!);
