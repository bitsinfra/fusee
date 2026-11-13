"""Agent helpers: a model call as a step whose tokens stream to readers as
they come, and survive a crash or a retry::

    from fusee.ai import model_call

    async def tokens(resume: str):
        # resume: the text an earlier attempt already streamed ("" at first).
        async for chunk in llm.stream(messages, prefill=resume):
            yield chunk.text

    answer = await model_call(ctx, "draft", tokens)

Readers follow the tokens with ``run.stream("draft")``. The call is a step: its
result (the whole text) is recorded, so a replay does not call the model
again; an attempt that failed midway leaves its tokens recorded, and the next
attempt gets them as ``resume`` and streams only what follows.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable, Optional

from .._runtime import Ctx, StepCtx

__all__ = ["model_call"]


async def model_call(ctx: Ctx, name: str, call: Callable[[str], AsyncIterator[str]], *,
                     stream: Optional[str] = None, **step_options: Any) -> str:
    async def run(s: StepCtx) -> str:
        w = s.stream(stream or name)
        resume = "".join(w.recorded())
        text = resume
        async for token in call(resume):
            if not token:
                continue
            w.append(token)
            text += token
        w.close()
        return text

    return await ctx.step(name, run, **step_options)
