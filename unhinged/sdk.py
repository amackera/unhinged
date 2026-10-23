"""Thin SDK on top of :class:`unhinged.engine.Engine`.

A port of the synchronous half of ``absurd_sdk``: register task handlers,
spawn tasks, and run a worker that claims tasks and executes them with a
:class:`TaskContext` offering steps, sleeps, events and heartbeats.
"""

from __future__ import annotations

import concurrent.futures
import contextvars
import os
import socket
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, TypeVar, Union, overload

from .engine import (
    Engine,
    EnginePool,
    JsonValue,
    RunAlreadyFailed,
    TaskCancelled,
    UnhingedError,
    validate_queue_name,
)

R = TypeVar("R")
UTC = timezone.utc

_UNKNOWN_TASK_DEFER_BASE_SECONDS = 5
_UNKNOWN_TASK_DEFER_JITTER_SECONDS = 5
_CHECKPOINT_NOT_FOUND = object()
_current_task_context: contextvars.ContextVar[Optional["TaskContext"]] = contextvars.ContextVar(
    "unhinged_task_context", default=None
)


def get_current_context() -> Optional["TaskContext"]:
    return _current_task_context.get()


class SuspendTask(Exception):
    """Raised inside a handler to suspend the run (sleep / await)."""


class TimeoutError(Exception):  # noqa: A001 - mirrors absurd_sdk
    """Waiting for an event or a task result timed out."""


@dataclass
class StepHandle:
    name: str
    checkpoint_name: str
    done: bool
    state: JsonValue = None


@dataclass
class TaskResultSnapshot:
    task_id: str
    state: str
    result: JsonValue = None
    failure: JsonValue = None

    @property
    def terminal(self) -> bool:
        return self.state in ("completed", "failed", "cancelled")


def _serialize_error(err: BaseException) -> dict:
    return {
        "name": err.__class__.__name__,
        "message": str(err),
        "traceback": "".join(traceback.format_exception(err.__class__, err, err.__traceback__))
        if err.__traceback__
        else None,
    }


def _snapshot(row: Optional[dict]) -> Optional[TaskResultSnapshot]:
    if row is None:
        return None
    return TaskResultSnapshot(
        task_id=row["task_id"], state=row["state"], result=row["result"], failure=row["failure_reason"]
    )


def _await_with_backoff(
    fetch: Callable[[], Optional[TaskResultSnapshot]],
    task_id: str,
    timeout: Optional[float],
    before_sleep: Optional[Callable[[], None]] = None,
) -> TaskResultSnapshot:
    started = time.monotonic()
    delay = 0.05
    while True:
        snapshot = fetch()
        if snapshot is None:
            raise UnhingedError(f'Task "{task_id}" not found')
        if snapshot.terminal:
            return snapshot
        sleep = delay
        if timeout is not None:
            remaining = timeout - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError(f'Timed out waiting for task "{task_id}"')
            sleep = min(sleep, remaining)
        if before_sleep is not None:
            before_sleep()
        time.sleep(max(sleep, 0))
        delay = min(delay * 2, 1.0)


# --------------------------------------------------------------------------


class TaskContext:
    """Handed to task handlers; everything here is durable."""

    def __init__(self, engine: Engine, queue: str, task: dict, claim_timeout: int) -> None:
        self._engine = engine
        self._queue = queue
        self._task = task
        self._claim_timeout = claim_timeout
        self._cache: Dict[str, JsonValue] = {}
        self._step_counter: Dict[str, int] = {}
        self.task_id: str = task["task_id"]
        self.run_id: str = task["run_id"]
        self.attempt: int = task["attempt"]

    @property
    def headers(self) -> Mapping[str, JsonValue]:
        return self._task.get("headers") or {}

    @property
    def queue(self) -> str:
        return self._queue

    # -- steps -------------------------------------------------------------

    def step(self, name: str, fn: Callable[[], R]) -> R:
        """Run ``fn`` once ever under ``name``; replays return the stored value."""
        handle = self.begin_step(name)
        if handle.done:
            return handle.state  # type: ignore[return-value]
        return self.complete_step(handle, fn())

    def begin_step(self, name: str) -> StepHandle:
        checkpoint_name = self._checkpoint_name(name)
        state = self._lookup(checkpoint_name)
        if state is not _CHECKPOINT_NOT_FOUND:
            return StepHandle(name, checkpoint_name, True, state)
        return StepHandle(name, checkpoint_name, False)

    def complete_step(self, handle: StepHandle, value: R) -> R:
        if handle.done:
            return handle.state  # type: ignore[return-value]
        self._persist(handle.checkpoint_name, value)
        return value

    @overload
    def run_step(self, name_or_fn: str) -> Callable[[Callable[[], R]], R]: ...
    @overload
    def run_step(self, name_or_fn: Callable[[], R]) -> R: ...
    @overload
    def run_step(self) -> Callable[[Callable[[], R]], R]: ...

    def run_step(self, name_or_fn: Any = None) -> Any:
        """Decorator form: ``@ctx.run_step("name")`` replaces the function with its result."""
        if callable(name_or_fn):
            return self.step(name_or_fn.__name__, name_or_fn)
        custom = name_or_fn if isinstance(name_or_fn, str) else None

        def decorator(fn: Callable[[], R]) -> R:
            return self.step(custom if custom is not None else fn.__name__, fn)

        return decorator

    # -- sleeping ----------------------------------------------------------

    def sleep_for(self, step_name: str, duration: float) -> None:
        self.sleep_until(step_name, self._engine.now() + timedelta(seconds=duration))

    def sleep_until(self, step_name: str, wake_at: Union[datetime, int, float]) -> None:
        if isinstance(wake_at, (int, float)):
            wake_at = datetime.fromtimestamp(wake_at, UTC)
        if wake_at.tzinfo is None:
            wake_at = wake_at.replace(tzinfo=UTC)
        checkpoint_name = self._checkpoint_name(step_name)
        state = self._lookup(checkpoint_name)
        if state is not _CHECKPOINT_NOT_FOUND:
            actual = datetime.fromisoformat(state) if isinstance(state, str) else wake_at
        else:
            actual = wake_at
            self._persist(checkpoint_name, wake_at.isoformat())
        if (actual - self._engine.now()).total_seconds() > 0:
            self._engine.schedule_run(self._queue, self.run_id, actual)
            raise SuspendTask()

    # -- events ------------------------------------------------------------

    def await_event(
        self, event_name: str, step_name: Optional[str] = None, timeout: Optional[float] = None
    ) -> JsonValue:
        """Suspend until ``event_name`` is emitted; returns its payload."""
        step_name = step_name or f"$awaitEvent:{event_name}"
        checkpoint_name = self._checkpoint_name(step_name)
        cached = self._lookup(checkpoint_name)
        if cached is not _CHECKPOINT_NOT_FOUND:
            return cached
        if self._task.get("wake_event") == event_name and self._task.get("event_payload") is None:
            self._task["wake_event"] = None
            raise TimeoutError(f'Timed out waiting for event "{event_name}"')
        should_suspend, payload = self._engine.await_event(
            self._queue, self.task_id, self.run_id, checkpoint_name, event_name, timeout
        )
        if not should_suspend:
            self._cache[checkpoint_name] = payload
            self._task["event_payload"] = None
            return payload
        raise SuspendTask()

    def emit_event(self, event_name: str, payload: JsonValue = None) -> None:
        self._engine.emit_event(self._queue, event_name, payload)

    def await_task_result(
        self,
        task_id: str,
        queue_name: Optional[str] = None,
        timeout: Optional[float] = None,
        step_name: Optional[str] = None,
    ) -> TaskResultSnapshot:
        """Durably wait for a child task in another queue (heartbeating meanwhile)."""
        queue = validate_queue_name(queue_name or self._queue)
        if queue == self._queue:
            raise ValueError(
                "await_task_result cannot wait on tasks in the same queue because this can "
                "deadlock workers. Spawn the child in a different queue and pass queue_name."
            )
        step = step_name or f"$awaitTaskResult:{task_id}"

        def wait() -> dict:
            interval = max(0.5, self._claim_timeout / 2)
            next_beat = time.monotonic() + interval

            def maybe_heartbeat() -> None:
                nonlocal next_beat
                if time.monotonic() >= next_beat:
                    self.heartbeat()
                    next_beat = time.monotonic() + interval

            snap = _await_with_backoff(
                lambda: _snapshot(self._engine.get_task_result(queue, task_id)), task_id, timeout, maybe_heartbeat
            )
            return {"task_id": snap.task_id, "state": snap.state, "result": snap.result, "failure": snap.failure}

        stored = self.step(step, wait)
        return TaskResultSnapshot(**stored)

    def heartbeat(self, seconds: Optional[int] = None) -> None:
        """Extend the lease; raises :class:`TaskCancelled` if the task was cancelled."""
        self._engine.extend_claim(self._queue, self.run_id, seconds if seconds is not None else self._claim_timeout)

    # -- internals ---------------------------------------------------------

    def _checkpoint_name(self, name: str) -> str:
        count = self._step_counter.get(name, 0) + 1
        self._step_counter[name] = count
        return name if count == 1 else f"{name}#{count}"

    def _lookup(self, checkpoint_name: str) -> Any:
        if checkpoint_name in self._cache:
            return self._cache[checkpoint_name]
        row = self._engine.get_task_checkpoint_state(self._queue, self.task_id, checkpoint_name)
        if row is None:
            return _CHECKPOINT_NOT_FOUND
        self._cache[checkpoint_name] = row["state"]
        return row["state"]

    def _persist(self, checkpoint_name: str, value: JsonValue) -> None:
        self._engine.set_task_checkpoint_state(
            self._queue, self.task_id, checkpoint_name, value, self.run_id, self._claim_timeout
        )
        self._cache[checkpoint_name] = value


# --------------------------------------------------------------------------


class Unhinged:
    """Client + worker. ``path`` defaults to ``$UNHINGED_DATABASE`` or ``unhinged.db``."""

    def __init__(
        self,
        path: Optional[str] = None,
        queue_name: str = "default",
        default_max_attempts: int = 5,
        busy_timeout: float = 5.0,
        synchronous: str = "full",
        fullfsync: bool = False,
    ) -> None:
        self.path = path or os.environ.get("UNHINGED_DATABASE", "unhinged.db")
        self._queue = validate_queue_name(queue_name)
        self._default_max_attempts = default_max_attempts
        self._pool = EnginePool(self.path, busy_timeout=busy_timeout, synchronous=synchronous, fullfsync=fullfsync)
        self._registry: Dict[str, dict] = {}
        self._running = False
        self.engine.create_queue(self._queue)

    @property
    def engine(self) -> Engine:
        """The calling thread's engine."""
        return self._pool.get()

    @property
    def queue_name(self) -> str:
        return self._queue

    def close(self) -> None:
        self.stop_worker()
        self._pool.close()

    # -- registration ------------------------------------------------------

    def register_task(
        self,
        name: str,
        queue: Optional[str] = None,
        default_max_attempts: Optional[int] = None,
        default_cancellation: Optional[dict] = None,
    ) -> Callable[[Callable[[Any, TaskContext], Any]], Callable[[Any, TaskContext], Any]]:
        def decorator(handler: Callable[[Any, TaskContext], Any]) -> Callable[[Any, TaskContext], Any]:
            q = validate_queue_name(queue or self._queue)
            if q != self._queue:
                self.engine.create_queue(q)
            self._registry[name] = {
                "name": name,
                "queue": q,
                "default_max_attempts": default_max_attempts,
                "default_cancellation": default_cancellation,
                "handler": handler,
            }
            return handler

        return decorator

    # -- queues ------------------------------------------------------------

    def create_queue(self, queue: Optional[str] = None, **policy: Any) -> None:
        q = validate_queue_name(queue or self._queue)
        self.engine.create_queue(q)
        if policy:
            self.engine.set_queue_policy(q, policy)

    def drop_queue(self, queue: Optional[str] = None) -> None:
        self.engine.drop_queue(queue or self._queue)

    def list_queues(self) -> List[str]:
        return self.engine.list_queues()

    def cleanup(self, queue: Optional[str] = None) -> List[dict]:
        return self.engine.cleanup_all_queues(queue)

    # -- client side -------------------------------------------------------

    def spawn(
        self,
        task_name: str,
        params: JsonValue = None,
        *,
        max_attempts: Optional[int] = None,
        retry_strategy: Optional[dict] = None,
        headers: Optional[dict] = None,
        queue: Optional[str] = None,
        cancellation: Optional[dict] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        reg = self._registry.get(task_name)
        if reg:
            actual_queue = reg["queue"]
            if queue is not None and queue != actual_queue:
                raise ValueError(
                    f'Task "{task_name}" is registered for queue "{actual_queue}" but spawn requested queue "{queue}".'
                )
        elif queue is None:
            raise ValueError(f'Task "{task_name}" is not registered. Provide queue when spawning unregistered tasks.')
        else:
            actual_queue = queue

        if max_attempts is None and reg:
            max_attempts = reg["default_max_attempts"]
        if max_attempts is None:
            max_attempts = self._default_max_attempts
        if cancellation is None and reg:
            cancellation = reg["default_cancellation"]

        options = {
            "max_attempts": max_attempts,
            "retry_strategy": retry_strategy,
            "headers": headers,
            "cancellation": cancellation,
            "idempotency_key": idempotency_key,
        }
        options = {k: v for k, v in options.items() if v is not None}
        return self.engine.spawn_task(validate_queue_name(actual_queue), task_name, params, options)

    def fetch_task_result(self, task_id: str, queue: Optional[str] = None) -> Optional[TaskResultSnapshot]:
        return _snapshot(self.engine.get_task_result(queue or self._queue, task_id))

    def await_task_result(
        self, task_id: str, queue: Optional[str] = None, timeout: Optional[float] = None
    ) -> TaskResultSnapshot:
        q = queue or self._queue
        return _await_with_backoff(lambda: self.fetch_task_result(task_id, q), task_id, timeout)

    def emit_event(self, event_name: str, payload: JsonValue = None, queue: Optional[str] = None) -> None:
        self.engine.emit_event(queue or self._queue, event_name, payload)

    def cancel_task(self, task_id: str, queue: Optional[str] = None) -> None:
        self.engine.cancel_task(queue or self._queue, task_id)

    def retry_task(
        self, task_id: str, queue: Optional[str] = None, *, spawn_new: bool = False, max_attempts: Optional[int] = None
    ) -> dict:
        opts: dict = {"spawn_new": spawn_new}
        if max_attempts is not None:
            opts["max_attempts"] = max_attempts
        return self.engine.retry_task(queue or self._queue, task_id, opts)

    # -- worker side -------------------------------------------------------

    def claim_tasks(self, batch_size: int = 1, claim_timeout: int = 120, worker_id: str = "worker") -> List[dict]:
        return self.engine.claim_task(self._queue, worker_id, claim_timeout, batch_size)

    def work_batch(self, worker_id: str = "worker", claim_timeout: int = 120, batch_size: int = 1) -> int:
        """Claim and run up to ``batch_size`` tasks on this thread. Returns how many ran."""
        tasks = self.claim_tasks(batch_size, claim_timeout, worker_id)
        for task in tasks:
            self._execute_task(task, claim_timeout)
        return len(tasks)

    def run_until_idle(self, worker_id: str = "worker", claim_timeout: int = 120, max_iterations: int = 100000) -> int:
        """Drain the queue on this thread; handy for tests and scripts."""
        done = 0
        for _ in range(max_iterations):
            n = self.work_batch(worker_id, claim_timeout, 1)
            if n == 0:
                return done
            done += n
        return done

    def start_worker(
        self,
        worker_id: Optional[str] = None,
        claim_timeout: int = 120,
        concurrency: int = 1,
        batch_size: Optional[int] = None,
        poll_interval: float = 0.25,
        stop_when_idle: bool = False,
        maintenance_interval: Optional[float] = None,
    ) -> None:
        """Poll the queue forever (or until idle) and execute claimed tasks.

        With ``concurrency > 1`` tasks run in a thread pool; each thread opens
        its own SQLite connection. ``maintenance_interval`` (seconds) makes
        this worker apply the queue's cleanup policy periodically; SQLite has
        no pg_cron, so this is the stand-in.
        """
        worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}"
        batch = batch_size or concurrency
        self._running = True
        next_maintenance = time.monotonic() + maintenance_interval if maintenance_interval else None

        def maybe_maintain() -> None:
            nonlocal next_maintenance
            if next_maintenance is not None and time.monotonic() >= next_maintenance:
                self.cleanup(self._queue)
                next_maintenance = time.monotonic() + maintenance_interval  # type: ignore[operator]

        if concurrency <= 1:
            while self._running:
                maybe_maintain()
                tasks = self.claim_tasks(batch, claim_timeout, worker_id)
                if not tasks:
                    if stop_when_idle:
                        break
                    time.sleep(poll_interval)
                    continue
                for task in tasks:
                    self._execute_task(task, claim_timeout)
            return

        executing: set[concurrent.futures.Future[None]] = set()

        def drain(done: set[concurrent.futures.Future[None]]) -> None:
            for f in done:
                executing.discard(f)
                f.result()

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            while self._running:
                maybe_maintain()
                drain({f for f in executing if f.done()})
                capacity = concurrency - len(executing)
                if capacity <= 0:
                    done, _ = concurrent.futures.wait(executing, timeout=poll_interval, return_when="FIRST_COMPLETED")
                    drain(done)
                    continue
                tasks = self.claim_tasks(min(batch, capacity), claim_timeout, worker_id)
                if not tasks:
                    if stop_when_idle and not executing:
                        break
                    if executing:
                        done, _ = concurrent.futures.wait(executing, timeout=poll_interval, return_when="FIRST_COMPLETED")
                        drain(done)
                    else:
                        time.sleep(poll_interval)
                    continue
                for task in tasks:
                    executing.add(pool.submit(self._execute_task, task, claim_timeout))
            for f in concurrent.futures.as_completed(executing):
                f.result()

    def stop_worker(self) -> None:
        self._running = False

    def _execute_task(self, task: dict, claim_timeout: int) -> None:
        engine = self.engine  # this thread's connection
        reg = self._registry.get(task["task_name"])
        if reg is None:
            # Unknown here; defer so a worker that knows it can pick it up.
            jitter = sum(ord(ch) for ch in task["run_id"]) % (_UNKNOWN_TASK_DEFER_JITTER_SECONDS + 1)
            try:
                engine.schedule_run(
                    self._queue, task["run_id"], engine.now() + timedelta(seconds=_UNKNOWN_TASK_DEFER_BASE_SECONDS + jitter)
                )
            except Exception as err:  # pragma: no cover - defensive
                self._fail_quietly(engine, task["run_id"], err)
            return

        if reg["queue"] != self._queue:
            self._fail_quietly(engine, task["run_id"], UnhingedError("Misconfigured task (queue mismatch)"))
            return

        ctx = TaskContext(engine, self._queue, task, claim_timeout)
        token = _current_task_context.set(ctx)
        try:
            result = reg["handler"](task["params"], ctx)
            engine.complete_run(self._queue, task["run_id"], result)
        except (SuspendTask, TaskCancelled, RunAlreadyFailed):
            pass
        except Exception as err:
            self._fail_quietly(engine, task["run_id"], err)
        finally:
            _current_task_context.reset(token)

    def _fail_quietly(self, engine: Engine, run_id: str, err: BaseException) -> None:
        try:
            engine.fail_run(self._queue, run_id, _serialize_error(err))
        except (TaskCancelled, RunAlreadyFailed):
            pass
