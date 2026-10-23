"""The async SDK: same checkpoints as the sync one, handlers as coroutines."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from unhinged import AsyncUnhinged, TaskCancelled, TimeoutError, Unhinged, get_current_context

UTC = timezone.utc


@pytest.fixture
def app(tmp_path):
    a = AsyncUnhinged(str(tmp_path / "aio.db"), queue_name="q")
    try:
        yield a
    finally:
        a.close()


def test_handler_must_be_async(app):
    with pytest.raises(TypeError, match="async def"):

        @app.register_task("sync")
        def sync_handler(params, ctx):
            return 1


async def test_steps_replay_after_failure(app):
    calls = {"a": 0, "b": 0, "handler": 0}

    @app.register_task("flaky", default_max_attempts=3)
    async def flaky(params, ctx):
        calls["handler"] += 1
        assert get_current_context() is ctx

        async def a():
            calls["a"] += 1
            await asyncio.sleep(0)
            return params["x"] * 2

        def b():
            calls["b"] += 1
            return 1

        a_val = await ctx.step("a", a)
        b_val = await ctx.step("b", b)  # sync callables are fine too
        if ctx.attempt == 1:
            raise RuntimeError("transient")
        return a_val + b_val

    t = app.spawn("flaky", {"x": 21}, retry_strategy={"kind": "none"})
    await app.run_until_idle()
    snap = app.fetch_task_result(t["task_id"])
    assert snap.state == "completed" and snap.result == 43
    assert calls == {"a": 1, "b": 1, "handler": 2}


async def test_sync_and_async_contexts_share_checkpoints(tmp_path):
    """A task started by the sync worker can be finished by the async one."""
    db = str(tmp_path / "shared.db")
    ran = []

    def sync_handler(params, ctx):
        ran.append(("sync", ctx.step("one", lambda: 1)))
        raise RuntimeError("hand over")

    async def async_handler(params, ctx):
        ran.append(("async", await ctx.step("one", lambda: 999)))
        return "done"

    sync_app = Unhinged(db, queue_name="q")
    sync_app.register_task("t")(sync_handler)
    t = sync_app.spawn("t", None, retry_strategy={"kind": "none"})
    sync_app.work_batch()
    sync_app.close()

    aio = AsyncUnhinged(db, queue_name="q")
    aio.register_task("t")(async_handler)
    await aio.run_until_idle()
    assert ran == [("sync", 1), ("async", 1)]  # the async run saw the sync run's checkpoint
    assert aio.fetch_task_result(t["task_id"]).result == "done"
    aio.close()


async def test_sleep_and_events(app):
    base = datetime(2024, 6, 1, tzinfo=UTC)
    app.engine.set_fake_now(base)
    log = []

    @app.register_task("order")
    async def order(params, ctx):
        log.append("start")
        await ctx.sleep_for("cooldown", 60)
        shipped = await ctx.await_event("shipped", timeout=3600)
        return shipped

    t = app.spawn("order", None)
    await app.run_until_idle()
    assert app.fetch_task_result(t["task_id"]).state == "sleeping"
    app.engine.set_fake_now(base + timedelta(seconds=60))
    await app.run_until_idle()
    assert app.fetch_task_result(t["task_id"]).state == "sleeping"  # now waiting for the event
    app.emit_event("shipped", {"tracking": "XYZ"})
    await app.run_until_idle()
    assert app.fetch_task_result(t["task_id"]).result == {"tracking": "XYZ"}
    assert log == ["start"] * 3


async def test_await_event_timeout(app):
    base = datetime(2024, 6, 1, tzinfo=UTC)
    app.engine.set_fake_now(base)

    @app.register_task("w", default_max_attempts=1)
    async def w(params, ctx):
        try:
            return await ctx.await_event("never", timeout=10)
        except TimeoutError:
            return "timed-out"

    t = app.spawn("w", None)
    await app.run_until_idle()
    app.engine.set_fake_now(base + timedelta(seconds=11))
    await app.run_until_idle()
    assert app.fetch_task_result(t["task_id"]).result == "timed-out"


async def test_concurrent_handlers_on_one_loop(app):
    active = {"now": 0, "max": 0}

    @app.register_task("slow")
    async def slow(params, ctx):
        active["now"] += 1
        active["max"] = max(active["max"], active["now"])
        await ctx.step("io", lambda: asyncio.sleep(0.05))
        active["now"] -= 1
        return params

    ids = [app.spawn("slow", i)["task_id"] for i in range(12)]
    await app.start_worker(concurrency=4, poll_interval=0.01, stop_when_idle=True)
    assert all(app.fetch_task_result(i).state == "completed" for i in ids)
    assert active["max"] == 4


async def test_cancellation_at_heartbeat(app):
    seen = []

    @app.register_task("long")
    async def long_task(params, ctx):
        app.cancel_task(ctx.task_id)
        try:
            await ctx.heartbeat()
        except TaskCancelled:
            seen.append(True)
            raise
        return "unreachable"

    t = app.spawn("long", None)
    await app.run_until_idle()
    assert seen == [True]
    assert app.fetch_task_result(t["task_id"]).state == "cancelled"


async def test_await_task_result_across_queues(tmp_path):
    db = str(tmp_path / "x.db")
    parent, child = AsyncUnhinged(db, queue_name="parents"), AsyncUnhinged(db, queue_name="children")
    try:

        @child.register_task("double")
        async def double(params, ctx):
            return params * 2

        @parent.register_task("fan-in")
        async def fan_in(params, ctx):
            spawned = await ctx.step("spawn", lambda: child.spawn("double", params))
            snap = await ctx.await_task_result(spawned["task_id"], queue_name="children", timeout=5)
            return snap.result

        t = parent.spawn("fan-in", 21)
        stop = asyncio.Event()

        async def child_worker():
            while not stop.is_set():
                if await child.work_batch("c") == 0:
                    await asyncio.sleep(0.02)

        worker = asyncio.create_task(child_worker())
        await parent.run_until_idle()
        stop.set()
        await worker
        assert parent.fetch_task_result(t["task_id"]).result == 42
    finally:
        parent.close()
        child.close()
