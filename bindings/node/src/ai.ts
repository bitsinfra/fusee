/**
 * Agent helpers: a model call as a step whose tokens stream to readers as
 * they come, and survive a crash or a retry.
 *
 *     import { modelCall } from "fusee/ai";
 *
 *     const answer = await modelCall(ctx, "draft", async function* (resume) {
 *       // `resume`: the text an earlier attempt already streamed ("" at first).
 *       const stream = await llm.stream({ messages, prefill: resume });
 *       for await (const chunk of stream) yield chunk.text;
 *     });
 *
 * Readers follow the tokens with `run.stream("draft")`. The call is a step:
 * its result (the whole text) is recorded, so a replay does not call the
 * model again; an attempt that failed midway leaves its tokens recorded, and
 * the next attempt gets them as `resume` and streams only what follows.
 */
import type { Ctx, StepCtx, StepOptions } from "./index.js";

export async function modelCall(
  ctx: Ctx,
  name: string,
  call: (resume: string, s: StepCtx) => AsyncIterable<string>,
  opts: StepOptions & { stream?: string } = {},
): Promise<string> {
  const { stream = name, ...stepOpts } = opts;
  return ctx.step(
    name,
    async (s: StepCtx) => {
      const w = s.stream(stream);
      const resume = w.recorded<string>().join("");
      let text = resume;
      for await (const token of call(resume, s)) {
        if (!token) continue;
        w.append(token);
        text += token;
      }
      w.close();
      return text;
    },
    stepOpts,
  );
}
