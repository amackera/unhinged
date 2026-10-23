"""SDK tests: steps, sleeps, events, retries, threads, and real crash recovery."""

import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from unhinged import TaskCancelled, TimeoutError, Unhinged

UTC = timezone.utc


@pytest.fixture
def app(tmp_path):
    a = Unhinged(str(tmp_path / "sdk.db"), queue_name="q")
    try:
        yield a
    finally:
        a.close()


def test_steps_run_once_and_replay_after_failure(app: Unhinged):
    calls = {"a": 0, "b": 0, "handler": 0}

    @app.register_task("flaky", default_max_attempts=3)
    def flaky(params, ctx):
        calls["handler"] += 1

        @ctx.run_step("a")
        def a():
            calls["a"] += 1
            return {"a": params["x"] * 2}

        @ctx.run_step()
        def b():
            calls["b"] += 1
            return a["a"] + 1

        if ctx.attempt == 1:
            raise RuntimeError("transient")
        return {"a": a, "b": b, "attempt": ctx.attempt}

    spawned = app.spawn("flaky", {"x": 21}, retry_strategy={"kind": "none"})
    app.run_until_idle()
    snap = app.fetch_task_result(spawned["task_id"])
    assert snap.state == "completed"
    assert snap.result == {"a": {"a": 42}, "b": 43, "attempt": 2}
    assert calls == {"a": 1, "b": 1, "handler": 2}
    runs = app.engine.get_runs("q", spawned["task_id"])
    assert [r["state"] for r in runs] == ["failed", "completed"]
    assert runs[0]["failure_reason"]["name"] == "RuntimeError"
    assert "transient" in runs[0]["failure_reason"]["traceback"]


def test_duplicate_step_names_get_suffixed(app: Unhinged):
    @app.register_task("dups")
    def dups(params, ctx):
        return [ctx.step("s", lambda: 1), ctx.step("s", lambda: 2), ctx.step("s", lambda: 3)]

    t = app.spawn("dups", None)
    app.run_until_idle()
    assert app.fetch_task_result(t["task_id"]).result == [1, 2, 3]
    names = sorted(
        r["checkpoint_name"] for r in app.engine.connection.execute('select checkpoint_name from "c_q"').fetchall()
    )
    assert names == ["s", "s#2", "s#3"]


def test_begin_and_complete_step(app: Unhinged):
    seen = []

    @app.register_task("two-phase")
    def two_phase(params, ctx):
        handle = ctx.begin_step("persist")
        seen.append(handle.done)
        if not handle.done:
            ctx.complete_step(handle, {"turn": 1})
            raise RuntimeError("after checkpoint")
        return handle.state

    t = app.spawn("two-phase", None, retry_strategy={"kind": "none"})
    app.run_until_idle()
    assert seen == [False, True]
    assert app.fetch_task_result(t["task_id"]).result == {"turn": 1}


def test_sleep_for_suspends_and_resumes(app: Unhinged):
    base = datetime(2024, 6, 1, tzinfo=UTC)
    app.engine.set_fake_now(base)
    log = []

    @app.register_task("napper")
    def napper(params, ctx):
        log.append("before")
        ctx.sleep_for("nap", 3600)
        log.append("after")
        return "rested"

    t = app.spawn("napper", None)
    assert app.run_until_idle() == 1
    assert log == ["before"]
    run = app.engine.get_run("q", t["run_id"])
    assert run["state"] == "sleeping"
    assert run["available_at"] == base + timedelta(hours=1)

    app.engine.set_fake_now(base + timedelta(minutes=30))
    assert app.run_until_idle() == 0

    app.engine.set_fake_now(base + timedelta(hours=1))
    assert app.run_until_idle() == 1
    # Code outside steps re-runs on resume; only the sleep itself is skipped.
    assert log == ["before", "before", "after"]
    assert app.fetch_task_result(t["task_id"]).result == "rested"
    # the same run resumed, no new attempt
    assert len(app.engine.get_runs("q", t["task_id"])) == 1


def test_await_event_and_emit(app: Unhinged):
    @app.register_task("order")
    def order(params, ctx):
        payment = ctx.step("pay", lambda: {"paid": params["amount"]})
        shipment = ctx.await_event(f"shipped:{params['id']}")
        return {"payment": payment, "tracking": shipment["tracking"]}

    t = app.spawn("order", {"id": 7, "amount": 5})
    app.run_until_idle()
    assert app.fetch_task_result(t["task_id"]).state == "sleeping"

    app.emit_event("shipped:7", {"tracking": "XYZ"})
    app.run_until_idle()
    assert app.fetch_task_result(t["task_id"]).result == {"payment": {"paid": 5}, "tracking": "XYZ"}


def test_await_event_timeout_raises_in_handler(app: Unhinged):
    base = datetime(2024, 6, 1, tzinfo=UTC)
    app.engine.set_fake_now(base)
    outcome = []

    @app.register_task("waiter", default_max_attempts=1)
    def waiter(params, ctx):
        try:
            return ctx.await_event("never", timeout=10)
        except TimeoutError as e:
            outcome.append(str(e))
            return "timed-out"

    t = app.spawn("waiter", None)
    app.run_until_idle()
    app.engine.set_fake_now(base + timedelta(seconds=11))
    app.run_until_idle()
    assert outcome == ['Timed out waiting for event "never"']
    assert app.fetch_task_result(t["task_id"]).result == "timed-out"


def test_event_emitted_before_await_is_consumed(app: Unhinged):
    app.emit_event("early", {"x": 1})

    @app.register_task("late-waiter")
    def late_waiter(params, ctx):
        return ctx.await_event("early")

    t = app.spawn("late-waiter", None)
    app.run_until_idle()
    assert app.fetch_task_result(t["task_id"]).result == {"x": 1}


def test_cancellation_detected_at_heartbeat(app: Unhinged):
    stopped = []

    @app.register_task("long")
    def long_task(params, ctx):
        app.cancel_task(ctx.task_id)  # someone cancels us mid-flight
        try:
            ctx.heartbeat()
        except TaskCancelled:
            stopped.append(True)
            raise
        return "unreachable"

    t = app.spawn("long", None)
    app.run_until_idle()
    assert stopped == [True]
    assert app.fetch_task_result(t["task_id"]).state == "cancelled"


def test_idempotency_key_through_sdk(app: Unhinged):
    @app.register_task("once")
    def once(params, ctx):
        return params

    a = app.spawn("once", {"n": 1}, idempotency_key="welcome:42")
    b = app.spawn("once", {"n": 2}, idempotency_key="welcome:42")
    assert a["task_id"] == b["task_id"]
    assert b["created"] is False
    app.run_until_idle()
    assert app.fetch_task_result(a["task_id"]).result == {"n": 1}


def test_unregistered_task_requires_queue_and_is_deferred(app: Unhinged):
    with pytest.raises(ValueError, match="not registered"):
        app.spawn("ghost", None)
    base = datetime(2024, 6, 1, tzinfo=UTC)
    app.engine.set_fake_now(base)
    t = app.spawn("ghost", None, queue="q")
    app.run_until_idle()
    run = app.engine.get_run("q", t["run_id"])
    assert run["state"] == "sleeping"
    assert base + timedelta(seconds=5) <= run["available_at"] <= base + timedelta(seconds=10)


def test_await_task_result_across_queues(tmp_path):
    db = str(tmp_path / "x.db")
    parent = Unhinged(db, queue_name="parents")
    child = Unhinged(db, queue_name="children")
    try:

        @child.register_task("double")
        def double(params, ctx):
            return params * 2

        @parent.register_task("fan-in")
        def fan_in(params, ctx):
            spawned = ctx.step("spawn", lambda: child.spawn("double", params))
            snap = ctx.await_task_result(spawned["task_id"], queue_name="children", timeout=5)
            return snap.result

        t = parent.spawn("fan-in", 21)
        with pytest.raises(ValueError, match="same queue"):
            fan_in(1, _FakeCtx(parent))  # guard against deadlocking yourself

        done = threading.Event()

        def run_child():
            while not done.is_set():
                if child.work_batch("c") == 0:
                    time.sleep(0.02)

        th = threading.Thread(target=run_child, daemon=True)
        th.start()
        parent.run_until_idle()
        done.set()
        th.join()
        assert parent.fetch_task_result(t["task_id"]).result == 42
    finally:
        parent.close()
        child.close()


class _FakeCtx:
    def __init__(self, app):
        self._app = app

    def step(self, name, fn):
        return {"task_id": "x"}

    def await_task_result(self, task_id, queue_name=None, timeout=None):
        from unhinged.sdk import TaskContext

        ctx = TaskContext(self._app.engine, self._app.queue_name, {"task_id": "t", "run_id": "r", "attempt": 1}, 10)
        return ctx.await_task_result(task_id, queue_name=self._app.queue_name)


def test_threaded_worker_processes_everything(tmp_path):
    app = Unhinged(str(tmp_path / "threads.db"), queue_name="q")
    try:
        seen = []
        lock = threading.Lock()

        @app.register_task("work")
        def work(params, ctx):
            ctx.step("touch", lambda: params)
            time.sleep(0.01)
            with lock:
                seen.append(threading.get_ident())
            return params

        ids = [app.spawn("work", i)["task_id"] for i in range(40)]
        app.start_worker(claim_timeout=30, concurrency=4, poll_interval=0.01, stop_when_idle=True)
        assert len(seen) == 40
        assert len(set(seen)) > 1  # more than one thread actually ran tasks
        assert all(app.fetch_task_result(t).state == "completed" for t in ids)
        assert sorted(app.fetch_task_result(t).result for t in ids) == list(range(40))
    finally:
        app.close()


WORKER_SCRIPT = textwrap.dedent(
    """
    import os, sys, time
    from unhinged import Unhinged

    app = Unhinged(sys.argv[1], queue_name="q")
    crash_after = int(sys.argv[2])

    @app.register_task("pipeline", default_max_attempts=5)
    def pipeline(params, ctx):
        out = []
        for i in range(5):
            def work(i=i):
                with open(sys.argv[3], "a") as f:
                    f.write(f"{os.getpid()} step-{i}\\n")
                return i * i
            out.append(ctx.step(f"step-{i}", work))
            if i == crash_after:
                os.kill(os.getpid(), 9)  # simulate a hard crash mid-task
        return out

    task_id = sys.argv[4]
    while app.fetch_task_result(task_id).state not in ("completed", "failed", "cancelled"):
        if app.work_batch("w", claim_timeout=2) == 0:
            time.sleep(0.05)
    """
)


def test_crash_recovery_across_processes(tmp_path):
    """A worker is SIGKILLed after step 2; a fresh process finishes steps 3-4 only."""
    db = str(tmp_path / "crash.db")
    log = tmp_path / "steps.log"
    script = tmp_path / "worker.py"
    script.write_text(WORKER_SCRIPT)

    app = Unhinged(db, queue_name="q")
    t = app.spawn("pipeline", None, queue="q")

    env = {**os.environ, "PYTHONPATH": os.getcwd()}
    tid = t["task_id"]
    first = subprocess.run([sys.executable, str(script), db, "2", str(log), tid], env=env, timeout=30)
    assert first.returncode == -signal.SIGKILL

    assert app.fetch_task_result(t["task_id"]).state == "running"  # lease still held by the corpse
    time.sleep(2.1)  # let the 2s claim expire

    second = subprocess.run([sys.executable, str(script), db, "99", str(log), tid], env=env, timeout=30)
    assert second.returncode == 0

    lines = log.read_text().splitlines()
    pids = {line.split()[0] for line in lines}
    steps = [line.split()[1] for line in lines]
    assert len(pids) == 2
    assert steps == ["step-0", "step-1", "step-2", "step-3", "step-4"]  # no step re-ran

    snap = app.fetch_task_result(t["task_id"])
    assert snap.state == "completed"
    assert snap.result == [0, 1, 4, 9, 16]
    runs = app.engine.get_runs("q", t["task_id"])
    assert [r["state"] for r in runs] == ["failed", "completed"]
    assert runs[0]["failure_reason"]["name"] == "$ClaimTimeout"
    app.close()
