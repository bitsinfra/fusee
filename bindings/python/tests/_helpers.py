"""Shared by the tests: define-and-register in one decorator, and the
journal and status of a run."""

import fusee


def on(engine, name, **kw):
    """``@on(engine, "name")``: defines a workflow and registers it with
    ``engine`` (tests define workflows next to the state they close over)."""

    def define(fn):
        wf = fusee.workflow(name, **kw)(fn)
        engine.register(wf)
        return wf

    return define


async def journal(engine, id):
    d = await engine.run(id).describe()
    return d.journal


async def status(engine, id):
    try:
        return (await engine.run(id).status()).status
    except fusee.NotFoundError:
        return None
