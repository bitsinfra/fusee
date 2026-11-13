/**
 * Durable agents on the Vercel AI SDK: every model call and every tool
 * call is a step, so an agent resumes after a crash at the call it was
 * making, and nothing it finished runs again.
 *
 *     import { generateText, stepCountIs } from "ai";
 *     import { openai } from "@ai-sdk/openai";
 *     import { durableModel, durableTools } from "@bitsinfra/fusee/ai/vercel";
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
import type { Ctx } from "./execution.js";
import type { StepOptions } from "./types.js";
/** A language model as the AI SDK calls it (`LanguageModelV2` to `LanguageModelV4`; tested with the AI SDK 7). */
export interface LanguageModelLike {
    readonly provider: string;
    readonly modelId: string;
    doGenerate(options: any): PromiseLike<any>;
    doStream(options: any): PromiseLike<{
        stream: ReadableStream<any>;
    } & Record<string, unknown>>;
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
    approve?: {
        tools: readonly string[];
        ask: (completion: Completion, call: ToolCall) => unknown;
    };
}
/**
 * The model, its calls made as steps: a call the run recorded is answered
 * from its record, and a failed one is retried under the step's policy.
 */
export declare function durableModel<M extends LanguageModelLike>(ctx: Ctx, model: M, opts?: ModelOptions): M;
/**
 * The tools, each call made as a step named `tool:<name>`: a call the run
 * recorded is answered from its record. Tools without `execute` (the
 * client's) are left as they are.
 */
export declare function durableTools<T extends Record<string, ToolLike>>(ctx: Ctx, tools: T, opts?: ToolOptions): T;
/**
 * A value as JSON keeps it: dates, bytes and errors tagged, so `revive`
 * gives them back as they were. Functions and undefined are dropped.
 */
export declare function toJson(v: unknown): unknown;
/** The inverse of `toJson`. */
export declare function revive(v: unknown): unknown;
