"""fusee.testing: mocks, step lists, snapshots and strict replay checks."""

import asyncio

import fusee
from fusee import Ctx, StepCtx
from fusee.testing import TestEngine

calls = {"charge": 0}


@fusee.workflow("checkout")
async def checkout(ctx: Ctx, cents: int) -> str:
    quote = await ctx.step("quote", lambda: cents + 1)

    def charge(s: StepCtx):
        calls["charge"] += 1
        return "real"

    ch = await ctx.step("charge", charge, retry=fusee.Retry(attempts=3, initial=0.001))
    return f"{quote}:{ch}"


flips = {"n": 0}


@fusee.workflow("nondet")
async def nondet(ctx: Ctx, _) -> int:
    flips["n"] += 1
    return await ctx.step("a" if flips["n"] % 2 else "b", lambda: 1)


async def test_mocks_steps_and_check():
    t = await TestEngine.open(workflows=[checkout])
    t.mock_step(checkout, "charge", lambda attempt: f"ch_{attempt}")
    run = await t.engine.start(checkout, "o-1", 41)
    assert await run.result() == "42:ch_1"
    assert calls["charge"] == 0
    assert await t.steps("o-1") == ["quote", "charge"]
    snap = await t.snapshot("o-1")
    assert [r["kind"] for r in snap][0] == "start", snap
    await t.check()
    await t.close()


async def test_a_failing_mock_is_retried():
    t = await TestEngine.open(workflows=[checkout])

    def flaky(attempt):
        if attempt < 3:
            raise RuntimeError("unavailable")
        return "third"

    t.mock_step("checkout", "charge", flaky)
    assert await (await t.engine.start(checkout, "o-2", 1)).result() == "2:third"
    await t.close()


async def test_check_finds_a_nondeterministic_workflow():
    t = await TestEngine.open(workflows=[nondet])
    await (await t.engine.start(nondet, "n-1")).result()
    try:
        await t.check()
        raise AssertionError("expected DeterminismError")
    except fusee.DeterminismError as e:
        assert "n-1 (nondet)" in str(e) and "'a'" in str(e) and "'b'" in str(e), e
    await t.close()


renewals = {"charged": 0}
approve = fusee.signal("approve", bool)


@fusee.workflow("renew")
async def renew(ctx: Ctx, plan: str) -> str:
    q = await ctx.step("quote", lambda: 42)
    await ctx.sleep("a day", "1d")

    def charge():
        renewals["charged"] += 1
        return "ok"

    c = await ctx.step("charge", charge)
    ok = await ctx.wait_for(approve, timeout="7d")
    return f"{plan}:{q}:{c}:{ok}"


async def test_virtual_time_and_crash():
    import time

    t = await TestEngine.open(workflows=[renew])
    began, before = time.time(), t.now_ms()
    await t.start(renew, "r-1", "pro")
    # A day's sleep, then a week's wait that times out: skipped.
    assert await t.result("r-1") == "pro:42:ok:None"
    assert time.time() - began < 2, time.time() - began
    assert t.now_ms() - before >= 8 * 86_400_000
    await t.check()

    renewals["charged"] = 0
    await t.start(renew, "r-2", "basic")
    for _ in range(400):
        if "charge" in await t.steps("r-2"):
            break
        await t._skip()
        await asyncio.sleep(0.005)
    assert renewals["charged"] == 1
    await t.crash()
    await t.run("r-2").signal(approve, True)
    assert await t.result("r-2") == "basic:42:ok:True"
    # Recorded before the crash: not run again.
    assert renewals["charged"] == 1
    await t.close()


def payments_case(planted: bool):
    from fusee.testing import Case

    charges, emails = set(), [0]

    @fusee.workflow("order")
    async def order(ctx: Ctx, cents: int) -> int:
        await ctx.step("charge", lambda s: charges.add(s.key))
        def email():
            if planted:
                emails[0] += 1  # not idempotent: a second run sends a second email
            else:
                emails[0] = 1
        await ctx.step("email", email)
        return cents

    return Case([order], order, "o-1", 500, lambda: {"charges": len(charges), "emails": emails[0]})


async def test_crash_everywhere():
    from fusee.testing import crash_everywhere

    await crash_everywhere(lambda: payments_case(False))
    try:
        await crash_everywhere(lambda: payments_case(True))
        raise RuntimeError("expected the planted step to be found")
    except AssertionError as e:
        assert "'emails': 2" in str(e) and "step '" in str(e), e


async def test_model_call():
    from fusee.ai import model_call

    calls = []

    @fusee.workflow("draft")
    async def draft(ctx: Ctx, topic: str) -> str:
        async def tokens(resume: str):
            calls.append(resume)
            words = ["Fusee ", "keeps ", "going ", "after ", "a ", "crash."]
            for w in words[len(resume.split(" ")) - 1:]:
                yield w
                if len(calls) == 1 and w == "going ":
                    raise ConnectionError("connection reset")

        return await model_call(ctx, "draft", tokens, retry=fusee.Retry(attempts=2, initial=0.001))

    engine = await fusee.Engine.open(durability="memory", workflows=[draft])
    run = await engine.start(draft, "d-1", "durability")
    assert await run.result() == "Fusee keeps going after a crash."
    assert calls == ["", "Fusee keeps going "], calls
    got = [c async for c in run.stream("draft")]
    assert "".join(got) == "Fusee keeps going after a crash.", got
    await engine.close()


async def main():
    await test_model_call()
    await test_crash_everywhere()
    await test_virtual_time_and_crash()
    await test_mocks_steps_and_check()
    await test_a_failing_mock_is_retried()
    await test_check_finds_a_nondeterministic_workflow()
    print("ok testing")


if __name__ == "__main__":
    asyncio.run(main())
