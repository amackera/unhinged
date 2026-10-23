"""The asynchronous half of the SDK: ``AsyncUnhinged`` and ``AsyncTaskContext``.

Same checkpoint names and semantics as the synchronous :class:`unhinged.sdk.TaskContext`,
so a task started by one can be resumed by the other. Handlers are ``async def``
and the worker runs them as asyncio tasks on one event loop.

SQLite calls are synchronous and short, so they run inline on the loop rather
than on a thread. A step that fsyncs with ``fullfsync`` on a Mac blocks the loop
for a few milliseconds; everything else is well under one.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import socket
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

from .engine import Engine, EnginePool, JsonValue, RunAlreadyFailed, TaskCancelled, UnhingedError, validate_queue_name
from .sdk import (
    _CHECKPOINT_NOT_FOUND,
    _UNKNOWN_TASK_DEFER_BASE_SECONDS,
    _UNKNOWN_TASK_DEFER_JITTER_SECONDS,
    StepHandle,
    SuspendTask,
    TaskResultSnapshot,
    TimeoutError,
    Unhinged,
    _current_task_context,
    _serialize_error,
    _snapshot,
)

UTC = timezone.utc


async def _await_with_backoff_async(
    fetch: Callable[[], Optional[TaskResultSnapshot]],
    task_id: str,
    timeout: Optional[float],
    before_sleep: Optional[Callable[[], Awaitable[None]]] = None,
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
            await before_sleep()
        await asyncio.sleep(max(sleep, 0))
        delay = min(delay * 2, 1.0)


class AsyncTaskContext:
    """Handed to async task handlers. Every method here is durable."""

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
    def headers(self) -> dict:
        return self._task.get("headers") or {}

    @property
    def queue(self) -> str:
        return self._queue

    # -- steps -------------------------------------------------------------

    async def step(self, name: str, fn: Callable[[], Union[Any, Awaitable[Any]]]) -> Any:
        """Run ``fn`` (sync or async) once ever under ``name``; replays return the stored value."""
        handle = await self.begin_step(name)
        if handle.done:
            return handle.state
        value = fn()
        if inspect.isawaitable(value):
            value = await value
        return await self.complete_step(handle, value)

    async def begin_step(self, name: str) -> StepHandle:
        checkpoint_name = self._checkpoint_name(name)
        state = self._lookup(checkpoint_name)
        if state is not _CHECKPOINT_NOT_FOUND:
            return StepHandle(name, checkpoint_name, True, state)
        return StepHandle(name, checkpoint_name, False)

    async def complete_step(self, handle: StepHandle, value: Any) -> Any:
        if handle.done:
            return handle.state
        self._persist(handle.checkpoint_name, value)
        return value

    # -- sleeping ----------------------------------------------------------

    async def sleep_for(self, step_name: str, duration: float) -> None:
        await self.sleep_until(step_name, self._engine.now() + timedelta(seconds=duration))

    async def sleep_until(self, step_name: str, wake_at: Union[datetime, int, float]) -> None:
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

    async def await_event(
        self, event_name: str, step_name: Optional[str] = None, timeout: Optional[float] = None
    ) -> JsonValue:
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

    async def emit_event(self, event_name: str, payload: JsonValue = None) -> None:
        self._engine.emit_event(self._queue, event_name, payload)

    async def await_task_result(
        self,
        task_id: str,
        queue_name: Optional[str] = None,
        timeout: Optional[float] = None,
        step_name: Optional[str] = None,
    ) -> TaskResultSnapshot:
        queue = validate_queue_name(queue_name or self._queue)
        if queue == self._queue:
            raise ValueError(
                "await_task_result cannot wait on tasks in the same queue because this can "
                "deadlock workers. Spawn the child in a different queue and pass queue_name."
            )
        step = step_name or f"$awaitTaskResult:{task_id}"

        async def wait() -> dict:
            interval = max(0.5, self._claim_timeout / 2)
            next_beat = time.monotonic() + interval

            async def maybe_heartbeat() -> None:
                nonlocal next_beat
                if time.monotonic() >= next_beat:
                    await self.heartbeat()
                    next_beat = time.monotonic() + interval

            snap = await _await_with_backoff_async(
                lambda: _snapshot(self._engine.get_task_result(queue, task_id)), task_id, timeout, maybe_heartbeat
            )
            return {"task_id": snap.task_id, "state": snap.state, "result": snap.result, "failure": snap.failure}

        stored = await self.step(step, wait)
        return TaskResultSnapshot(**stored)

    async def heartbeat(self, seconds: Optional[int] = None) -> None:
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


class AsyncUnhinged:
    """Async client and worker. Handlers are ``async def handler(params, ctx)``.

    Queue operations and spawning are the synchronous ones underneath; only
    handler execution and waiting are async.
    """

    def __init__(
        self,
        path: Optional[str] = None,
        queue_name: str = "default",
        default_max_attempts: int = 5,
        busy_timeout: float = 5.0,
        synchronous: str = "full",
        fullfsync: bool = False,
    ) -> None:
        self._sync = Unhinged(
            path, queue_name=queue_name, default_max_attempts=default_max_attempts,
            busy_timeout=busy_timeout, synchronous=synchronous, fullfsync=fullfsync,
        )
        self._registry: Dict[str, dict] = {}
        self._running = False

    @property
    def path(self) -> str:
        return self._sync.path

    @property
    def engine(self) -> Engine:
        return self._sync.engine

    @property
    def queue_name(self) -> str:
        return self._sync.queue_name

    def close(self) -> None:
        self.stop_worker()
        self._sync.close()

    # -- registration and client side (sync underneath) ----------------------

    def register_task(
        self,
        name: str,
        queue: Optional[str] = None,
        default_max_attempts: Optional[int] = None,
        default_cancellation: Optional[dict] = None,
    ):
        def decorator(handler):
            if not inspect.iscoroutinefunction(handler):
                raise TypeError(f"AsyncUnhinged handlers must be `async def`; {name!r} is not")
            q = validate_queue_name(queue or self.queue_name)
            if q != self.queue_name:
                self.engine.create_queue(q)
            self._registry[name] = {
                "name": name, "queue": q, "default_max_attempts": default_max_attempts,
                "default_cancellation": default_cancellation, "handler": handler,
            }
            self._sync._registry[name] = self._registry[name]  # so spawn() sees the defaults
            return handler

        return decorator

    def spawn(self, task_name: str, params: JsonValue = None, **options: Any) -> dict:
        return self._sync.spawn(task_name, params, **options)

    def fetch_task_result(self, task_id: str, queue: Optional[str] = None) -> Optional[TaskResultSnapshot]:
        return self._sync.fetch_task_result(task_id, queue)

    async def await_task_result(
        self, task_id: str, queue: Optional[str] = None, timeout: Optional[float] = None
    ) -> TaskResultSnapshot:
        return await _await_with_backoff_async(lambda: self.fetch_task_result(task_id, queue), task_id, timeout)

    def emit_event(self, event_name: str, payload: JsonValue = None, queue: Optional[str] = None) -> None:
        self._sync.emit_event(event_name, payload, queue)

    def cancel_task(self, task_id: str, queue: Optional[str] = None) -> None:
        self._sync.cancel_task(task_id, queue)

    def retry_task(self, task_id: str, queue: Optional[str] = None, **options: Any) -> dict:
        return self._sync.retry_task(task_id, queue, **options)

    def create_queue(self, queue: Optional[str] = None, **policy: Any) -> None:
        self._sync.create_queue(queue, **policy)

    def drop_queue(self, queue: Optional[str] = None) -> None:
        self._sync.drop_queue(queue)

    def list_queues(self) -> List[str]:
        return self._sync.list_queues()

    def cleanup(self, queue: Optional[str] = None) -> List[dict]:
        return self._sync.cleanup(queue)

    # -- worker side ---------------------------------------------------------

    def claim_tasks(self, batch_size: int = 1, claim_timeout: int = 120, worker_id: str = "worker") -> List[dict]:
        return self._sync.claim_tasks(batch_size, claim_timeout, worker_id)

    async def work_batch(self, worker_id: str = "worker", claim_timeout: int = 120, batch_size: int = 1) -> int:
        tasks = self.claim_tasks(batch_size, claim_timeout, worker_id)
        for task in tasks:
            await self._execute_task(task, claim_timeout)
        return len(tasks)

    async def run_until_idle(self, worker_id: str = "worker", claim_timeout: int = 120, max_iterations: int = 100000) -> int:
        done = 0
        for _ in range(max_iterations):
            n = await self.work_batch(worker_id, claim_timeout, 1)
            if n == 0:
                return done
            done += n
        return done

    async def start_worker(
        self,
        worker_id: Optional[str] = None,
        claim_timeout: int = 120,
        concurrency: int = 1,
        batch_size: Optional[int] = None,
        poll_interval: float = 0.25,
        stop_when_idle: bool = False,
        maintenance_interval: Optional[float] = None,
    ) -> None:
        """Poll and run handlers as asyncio tasks, up to ``concurrency`` at once."""
        worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}"
        batch = batch_size or concurrency
        self._running = True
        next_maintenance = time.monotonic() + maintenance_interval if maintenance_interval else None
        executing: set[asyncio.Task[None]] = set()

        try:
            while self._running:
                if next_maintenance is not None and time.monotonic() >= next_maintenance:
                    self.cleanup(self.queue_name)
                    next_maintenance = time.monotonic() + maintenance_interval  # type: ignore[operator]
                executing = {t for t in executing if not t.done()}
                capacity = concurrency - len(executing)
                tasks = self.claim_tasks(min(batch, capacity), claim_timeout, worker_id) if capacity > 0 else []
                if not tasks:
                    if stop_when_idle and not executing:
                        break
                    if executing:
                        await asyncio.wait(executing, timeout=poll_interval, return_when=asyncio.FIRST_COMPLETED)
                    else:
                        await asyncio.sleep(poll_interval)
                    continue
                for task in tasks:
                    executing.add(asyncio.create_task(self._execute_task(task, claim_timeout)))
        finally:
            if executing:
                await asyncio.gather(*executing, return_exceptions=True)

    def stop_worker(self) -> None:
        self._running = False

    async def _execute_task(self, task: dict, claim_timeout: int) -> None:
        engine = self.engine
        queue = self.queue_name
        reg = self._registry.get(task["task_name"])
        if reg is None:
            jitter = sum(ord(ch) for ch in task["run_id"]) % (_UNKNOWN_TASK_DEFER_JITTER_SECONDS + 1)
            try:
                engine.schedule_run(queue, task["run_id"], engine.now() + timedelta(seconds=_UNKNOWN_TASK_DEFER_BASE_SECONDS + jitter))
            except Exception as err:  # pragma: no cover - defensive
                self._fail_quietly(engine, task["run_id"], err)
            return
        if reg["queue"] != queue:
            self._fail_quietly(engine, task["run_id"], UnhingedError("Misconfigured task (queue mismatch)"))
            return

        ctx = AsyncTaskContext(engine, queue, task, claim_timeout)
        token = _current_task_context.set(ctx)  # type: ignore[arg-type]
        try:
            result = await reg["handler"](task["params"], ctx)
            engine.complete_run(queue, task["run_id"], result)
        except (SuspendTask, TaskCancelled, RunAlreadyFailed):
            pass
        except asyncio.CancelledError:
            raise
        except Exception as err:
            self._fail_quietly(engine, task["run_id"], err)
        finally:
            _current_task_context.reset(token)

    def _fail_quietly(self, engine: Engine, run_id: str, err: BaseException) -> None:
        try:
            engine.fail_run(self.queue_name, run_id, _serialize_error(err))
        except (TaskCancelled, RunAlreadyFailed):
            pass
