/**
 * Durable agents on the Vercel AI SDK: every model call and every tool
 * call is a step, so an agent resumes after a crash at the call it was
 * making, and nothing it finished runs again.
 *
 *     import { generateText, stepCountIs } from "ai";
 *     import { openai } from "@ai-sdk/openai";
 *     import { durableModel, durableTools } from "fusee/ai/vercel";
 *
 *     const answer = workflow("answer", async (ctx, question: string) => {
 *       const { text } = await generateText({
 *         model: durableModel(ctx, openai("gpt-5")),
 *         tools: durableTools(ctx, { search, refund }, {
 *           approve: { tools: ["refund"], ask: (cb, call) => slack.ask(`Refund ${call.input.amount}?`, cb.url) },
 *         }),
 *         maxRetries: 0, // the steps retry, and their retries are recorded
 *         stopWhen: stepCountIs(8),
 *         prompt: question,
 *       });
 *       return text;
 *     });
 *
 * The model and tools are the AI SDK's own: the adapter wraps their calls
 * and imports nothing from `ai`, so it works with any provider. A streamed
 * call (`streamText`) records the whole response; while it runs, readers
 * follow its text with `run.stream("model#1")` (the run's first model call,
 * `model#2` the second, and so on). A `null` chunk in that stream means a
 * failed attempt is starting over: drop the text before it.
 */
import type { Completion } from "./types.js";
import type { Ctx, StepCtx } from "./execution.js";
import type { StepOptions } from "./types.js";

/** A language model as the AI SDK calls it (`LanguageModelV2` to `LanguageModelV4`; tested with the AI SDK 7). */
export interface LanguageModelLike {
  readonly provider: string;
  readonly modelId: string;
  doGenerate(options: any): PromiseLike<any>;
  doStream(options: any): PromiseLike<{ stream: ReadableStream<any> } & Record<string, unknown>>;
}

/** A tool as the AI SDK calls it: `execute` runs on the server; a tool without one is the client's. */
export interface ToolLike {
  execute?: (input: any, options: any) => unknown;
  [key: string]: unknown;
}

/** Options of `durableModel`. */
export interface ModelOptions extends StepOptions {
  /** The steps' name (default `model`); each call's stream is `<name>#<n>`. */
  name?: string;
}

/** A tool call waiting for a person. */
export interface ToolCall {
  tool: string;
  input: unknown;
  toolCallId?: string;
}

/** Options of `durableTools`. */
export interface ToolOptions {
  /** Each tool call's step options (retries, timeout, limit). */
  step?: StepOptions;
  /**
   * Tools that wait for a person before each call: `ask` sends the
   * completion's URL (or token) where someone can answer, and the call runs
   * once it is completed with `true` (or `{ approved: true }`). A refusal
   * (`false`, or `{ approved: false, reason }`) is the tool's result, so the
   * model sees it.
   */
  approve?: { tools: readonly string[]; ask: (completion: Completion, call: ToolCall) => unknown };
}

/** Model calls made so far by each run's code, for stream names. */
const calls = new WeakMap<Ctx, number>();

function nextCall(ctx: Ctx): number {
  const n = (calls.get(ctx) ?? 0) + 1;
  calls.set(ctx, n);
  return n;
}

/**
 * The model, its calls made as steps: a call the run recorded is answered
 * from its record, and a failed one is retried under the step's policy.
 */
export function durableModel<M extends LanguageModelLike>(ctx: Ctx, model: M, opts: ModelOptions = {}): M {
  const { name = "model", ...step } = opts;
  const doGenerate = async (options: any): Promise<any> => {
    nextCall(ctx);
    const r = await ctx.step(
      name,
      async (s: StepCtx) => toJson(await model.doGenerate(withSignal(options, s))),
      step,
    );
    return revive(r);
  };
  const doStream = async (options: any): Promise<any> => {
    const stream = `${name}#${nextCall(ctx)}`;
    const r = (await ctx.step(
      name,
      async (s: StepCtx) => {
        const w = s.stream(stream);
        // An earlier attempt's text is void: readers drop what they have.
        if (w.recordedCount > 0 && !w.closed) w.append(null);
        const { stream: body, ...rest } = await model.doStream(withSignal(options, s));
        const parts: unknown[] = [];
        const reader = body.getReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          parts.push(toJson(value));
          const delta = textDelta(value);
          if (delta) w.append(delta);
        }
        w.close();
        return { parts, rest: toJson(rest) };
      },
      step,
    )) as { parts: unknown[]; rest: Record<string, unknown> };
    const parts = revive(r.parts) as unknown[];
    return {
      ...(revive(r.rest) as Record<string, unknown>),
      stream: new ReadableStream({
        start(c) {
          for (const p of parts) c.enqueue(p);
          c.close();
        },
      }),
    };
  };
  return new Proxy(model, {
    get(target, prop, receiver) {
      if (prop === "doGenerate") return doGenerate;
      if (prop === "doStream") return doStream;
      return Reflect.get(target, prop, receiver);
    },
  });
}

/**
 * The tools, each call made as a step named `tool:<name>`: a call the run
 * recorded is answered from its record. Tools without `execute` (the
 * client's) are left as they are.
 */
export function durableTools<T extends Record<string, ToolLike>>(ctx: Ctx, tools: T, opts: ToolOptions = {}): T {
  const out: Record<string, ToolLike> = {};
  const asked = new Set(opts.approve?.tools ?? []);
  for (const [name, tool] of Object.entries(tools)) {
    const execute = tool.execute;
    if (typeof execute !== "function") {
      out[name] = tool;
      continue;
    }
    out[name] = {
      ...tool,
      execute: async (input: unknown, options: any) => {
        if (asked.has(name) && opts.approve) {
          const ask = opts.approve.ask;
          const call: ToolCall = { tool: name, input, toolCallId: options?.toolCallId };
          const decision = await ctx.awaitCompletion<unknown>(`approve:${name}`, (cb) => ask(cb, call));
          const approved = decision === true || (typeof decision === "object" && decision !== null && (decision as { approved?: unknown }).approved === true);
          if (!approved) {
            const reason = typeof decision === "object" && decision !== null ? (decision as { reason?: unknown }).reason : undefined;
            return { approved: false, ...(reason === undefined ? {} : { reason }) };
          }
        }
        const r = await ctx.step(
          `tool:${name}`,
          async (s: StepCtx) => toJson(await last(execute(input, withSignal(options ?? {}, s)))),
          opts.step ?? {},
        );
        return revive(r);
      },
    };
  }
  return out as T;
}

/** The call's options with the step's abort signal when the caller gave none. */
function withSignal(options: any, s: StepCtx): any {
  return options?.abortSignal ? options : { ...options, abortSignal: s.signal };
}

/** A streamed tool result (an async iterable of previews) is its last value. */
async function last(v: unknown): Promise<unknown> {
  if (v && typeof v === "object" && Symbol.asyncIterator in v) {
    let out: unknown;
    for await (const x of v as AsyncIterable<unknown>) out = x;
    return out;
  }
  return v;
}

function textDelta(part: any): string | undefined {
  if (part?.type !== "text-delta") return undefined;
  const d = part.delta ?? part.textDelta;
  return typeof d === "string" && d ? d : undefined;
}

/**
 * A value as JSON keeps it: dates, bytes and errors tagged, so `revive`
 * gives them back as they were. Functions and undefined are dropped.
 */
export function toJson(v: unknown): unknown {
  if (v instanceof Date) return { $date: v.toISOString() };
  if (v instanceof Uint8Array) return { $bytes: Buffer.from(v).toString("base64") };
  if (v instanceof URL) return { $url: v.href };
  if (v instanceof Error) return { $error: v.message, name: v.name };
  if (Array.isArray(v)) return v.map(toJson);
  if (v && typeof v === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, x] of Object.entries(v)) {
      if (x === undefined || typeof x === "function") continue;
      out[k] = toJson(x);
    }
    return out;
  }
  return typeof v === "function" ? undefined : v;
}

/** The inverse of `toJson`. */
export function revive(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(revive);
  if (v && typeof v === "object") {
    const o = v as Record<string, unknown>;
    if (typeof o.$date === "string") return new Date(o.$date);
    if (typeof o.$bytes === "string") return new Uint8Array(Buffer.from(o.$bytes, "base64"));
    if (typeof o.$url === "string") return new URL(o.$url);
    if (typeof o.$error === "string") {
      const e = new Error(o.$error);
      if (typeof o.name === "string") e.name = o.name;
      return e;
    }
    const out: Record<string, unknown> = {};
    for (const [k, x] of Object.entries(o)) out[k] = revive(x);
    return out;
  }
  return v;
}
