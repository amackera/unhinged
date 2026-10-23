"""The Absurd state machine, ported from plpgsql to Python over sqlite3.

Every public method on :class:`Engine` corresponds to one ``absurd.*``
function in ``sql/absurd.sql`` and keeps its name, arguments, and semantics.
Where Absurd relies on row locks (``FOR UPDATE``, ``FOR UPDATE SKIP LOCKED``,
``FOR SHARE``) this port relies on the fact that a SQLite write transaction
is exclusive: each method runs inside one ``BEGIN IMMEDIATE`` block, so the
whole claim / complete / emit step is atomic and there is nothing to skip.
Multiple worker processes coordinate through the database file (WAL mode plus
``busy_timeout``), which is the same pull model Absurd uses on Postgres.

Error codes are preserved as exception classes:

* ``AB001`` -> :class:`TaskCancelled`
* ``AB002`` -> :class:`RunAlreadyFailed`
* ``AB003`` -> :class:`InvalidRetryStrategy`
"""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Optional

from . import schema
from .schema import table

JsonValue = Any

MAX_QUEUE_NAME_BYTES = 57
RETRY_DELAY_LIMIT = 86400.0
INFINITY = "9999-12-31T23:59:59.999999"
EPOCH = "1970-01-01T00:00:00.000000"
UTC = timezone.utc


# --------------------------------------------------------------------------
# errors


class UnhingedError(Exception):
    """Base class for engine errors. ``code`` mirrors Absurd's SQLSTATE."""

    code: Optional[str] = None


class TaskCancelled(UnhingedError):
    code = "AB001"

    def __init__(self, message: str = "Task has been cancelled") -> None:
        super().__init__(message)


class RunAlreadyFailed(UnhingedError):
    code = "AB002"


class InvalidRetryStrategy(UnhingedError):
    code = "AB003"


# --------------------------------------------------------------------------
# small helpers


def uuidv7() -> str:
    """Port of ``absurd.portable_uuidv7``."""
    fn = getattr(uuid, "uuid7", None)
    if fn is not None:
        return str(fn())
    ts_ms = int(datetime.now(UTC).timestamp() * 1000)
    rnd = bytearray(os.urandom(16))
    for i in range(6):
        rnd[i] = (ts_ms >> ((5 - i) * 8)) & 0xFF
    rnd[6] = (rnd[6] & 0x0F) | 0x70
    rnd[8] = (rnd[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(rnd)))


def ts(dt: datetime) -> str:
    """Encode an aware datetime as the fixed-width UTC text the tables use."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")


def parse_ts(text: Optional[str]) -> Optional[datetime]:
    if text is None:
        return None
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=UTC)


def dumps(value: JsonValue) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def loads(text: Optional[str]) -> JsonValue:
    return None if text is None else json.loads(text)


_INTERVAL_UNITS = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
    "w": 604800, "week": 604800, "weeks": 604800,
}


def interval_seconds(value: Any) -> int:
    """Accept an int/float of seconds or Postgres-ish text like ``'30 days'``."""
    if isinstance(value, bool):
        raise UnhingedError("interval must be a number of seconds or a text interval")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, timedelta):
        return int(value.total_seconds())
    if not isinstance(value, str):
        raise UnhingedError("interval must be a number of seconds or a text interval")
    total = 0
    found = False
    for num, unit in re.findall(r"(-?\d+(?:\.\d+)?)\s*([a-zA-Z]+)", value):
        if unit.lower() not in _INTERVAL_UNITS:
            raise UnhingedError(f'Unknown interval unit "{unit}"')
        total += float(num) * _INTERVAL_UNITS[unit.lower()]
        found = True
    if not found:
        raise UnhingedError(f'Could not parse interval "{value}"')
    return int(total)


def retry_delay_seconds(strategy: Optional[dict], attempt: Optional[int]) -> float:
    """Port of ``absurd.retry_delay_seconds`` (global cap of one day)."""
    if strategy is None:
        return 0.0
    if not isinstance(strategy, dict):
        raise InvalidRetryStrategy("retry_strategy must be a JSON object")

    kind = strategy.get("kind", "none") or "none"
    if kind not in ("none", "fixed", "exponential"):
        raise InvalidRetryStrategy(f'Unsupported retry strategy kind "{kind}"')
    if kind == "none":
        return 0.0

    def num(key: str, default: float) -> float:
        raw = strategy.get(key)
        if raw is None:
            return default
        try:
            value = float(raw)
        except (TypeError, ValueError):
            raise InvalidRetryStrategy("retry_strategy contains an invalid number")
        return value

    base = num("base_seconds", 60.0 if kind == "fixed" else 30.0)
    if not (0 <= base <= RETRY_DELAY_LIMIT):
        raise InvalidRetryStrategy("retry_strategy.base_seconds must be between 0 and 86400")
    if kind == "fixed":
        return base

    factor = num("factor", 2.0)
    max_seconds = num("max_seconds", RETRY_DELAY_LIMIT)
    if factor < 0 or math.isnan(factor) or math.isinf(factor):
        raise InvalidRetryStrategy("retry_strategy.factor must be a finite non-negative number")
    if not (0 <= max_seconds <= RETRY_DELAY_LIMIT):
        raise InvalidRetryStrategy("retry_strategy.max_seconds must be between 0 and 86400")
    if base == 0:
        return 0.0

    exponent = max((attempt or 1) - 1, 0)
    try:
        delay = base * (factor ** exponent)
    except OverflowError:
        return 0.0 if factor < 1 else max_seconds
    if math.isinf(delay):
        return 0.0 if factor < 1 else max_seconds
    return min(delay, max_seconds)


def validate_queue_name(name: Optional[str]) -> str:
    if not name:
        raise UnhingedError("Queue name must be provided")
    if len(name.encode("utf-8")) > MAX_QUEUE_NAME_BYTES:
        raise UnhingedError(f'Queue name "{name}" is too long (max {MAX_QUEUE_NAME_BYTES} bytes).')
    return name


def _row(cursor_row: Optional[sqlite3.Row]) -> Optional[dict]:
    """Convert a row to a dict, decoding JSON and timestamp columns by name."""
    if cursor_row is None:
        return None
    out: dict[str, Any] = {}
    for key in cursor_row.keys():
        value = cursor_row[key]
        if key in _JSON_COLUMNS:
            value = loads(value)
        elif key.endswith("_at") and value is not None:
            value = parse_ts(value)
        out[key] = value
    return out


_JSON_COLUMNS = frozenset(
    {
        "params", "headers", "retry_strategy", "cancellation", "completed_payload",
        "event_payload", "result", "failure_reason", "state_json", "payload",
    }
)


# --------------------------------------------------------------------------
# engine


class Engine:
    """One sqlite3 connection plus the Absurd procedures.

    Not thread-safe (like a database connection). Open one per thread.
    """

    def __init__(
        self,
        path: str = "unhinged.db",
        *,
        busy_timeout: float = 5.0,
        clock: Optional[Callable[[], datetime]] = None,
        synchronous: str = "full",
    ) -> None:
        """Open (and if needed initialise) the database at ``path``.

        ``synchronous`` is passed to SQLite. The default ``"full"`` fsyncs on
        every commit, matching Postgres' durability: a checkpoint that was
        acknowledged survives a power cut. ``"normal"`` is roughly twice as
        fast and still survives process crashes, but an OS crash or power loss
        can drop the last few commits, so a step may run again after reboot.
        """
        if synchronous.lower() not in ("off", "normal", "full", "extra"):
            raise ValueError(f"unsupported synchronous mode {synchronous!r}")
        self.path = path
        self._clock = clock
        self._fake_now: Optional[datetime] = None
        self._tx_now: Optional[datetime] = None
        self._depth = 0
        self._conn = sqlite3.connect(
            path,
            timeout=busy_timeout,
            isolation_level=None,  # we manage transactions explicitly
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("pragma journal_mode = wal")
        self._conn.execute(f"pragma synchronous = {synchronous.lower()}")
        self._conn.execute("pragma foreign_keys = on")
        self._conn.execute(f"pragma busy_timeout = {int(busy_timeout * 1000)}")
        self._conn.executescript(schema.QUEUES_DDL)

    # -- plumbing ----------------------------------------------------------

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def now(self) -> datetime:
        """Port of ``absurd.current_time`` including the fake-clock hook.

        Inside a transaction the clock is read once and reused, so every row a
        procedure touches (and any procedure it calls, such as the lease sweep
        inside ``claim_task``) sees the same instant.
        """
        if self._fake_now is not None:
            return self._fake_now
        if self._depth > 0 and self._tx_now is not None:
            return self._tx_now
        current = self._clock() if self._clock is not None else datetime.now(UTC)
        if self._depth > 0:
            self._tx_now = current
        return current

    def set_fake_now(self, when: Optional[datetime]) -> None:
        if when is not None and when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        self._fake_now = when

    def clear_fake_now(self) -> None:
        self._fake_now = None

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """``BEGIN IMMEDIATE`` unless we're already inside a transaction."""
        if self._depth > 0:
            self._depth += 1
            try:
                yield self._conn
            finally:
                self._depth -= 1
            return
        self._conn.execute("begin immediate")
        self._depth = 1
        self._tx_now = None
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("rollback")
            raise
        else:
            self._conn.execute("commit")
        finally:
            self._depth = 0

    def _one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        return self._conn.execute(sql, params).fetchone()

    def _all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return self._conn.execute(sql, params).fetchall()

    def _exec(self, sql: str, params: tuple = ()) -> int:
        return self._conn.execute(sql, params).rowcount

    # -- queues ------------------------------------------------------------

    def get_schema_version(self) -> str:
        return schema.SCHEMA_VERSION

    def create_queue(self, queue: str) -> None:
        """Port of ``absurd.create_queue`` (unpartitioned only)."""
        queue = validate_queue_name(queue)
        with self.transaction():
            self._exec(
                "insert into queues (queue_name, created_at) values (?, ?) "
                "on conflict (queue_name) do nothing",
                (queue, ts(self.now())),
            )
            for stmt in schema.queue_ddl(queue):
                self._conn.execute(stmt)

    def drop_queue(self, queue: str) -> None:
        with self.transaction():
            if self._one("select 1 from queues where queue_name = ?", (queue,)) is None:
                return
            for stmt in schema.queue_drop_ddl(queue):
                self._conn.execute(stmt)
            self._exec("delete from queues where queue_name = ?", (queue,))

    def list_queues(self) -> list[str]:
        return [r["queue_name"] for r in self._all("select queue_name from queues order by queue_name")]

    def get_queue_policy(self, queue: str) -> Optional[dict]:
        row = self._one(
            "select queue_name, cleanup_ttl, cleanup_limit from queues where queue_name = ?",
            (queue,),
        )
        return dict(row) if row else None

    def set_queue_policy(self, queue: str, policy: Optional[dict]) -> None:
        """Port of ``absurd.set_queue_policy`` for the keys that survive the port."""
        queue = validate_queue_name(queue)
        policy = policy or {}
        if not isinstance(policy, dict):
            raise UnhingedError("Queue policy must be a JSON object")
        for key in policy:
            if key not in ("cleanup_ttl", "cleanup_limit"):
                raise UnhingedError(f'Unsupported queue policy key "{key}"')
        with self.transaction():
            current = self.get_queue_policy(queue)
            if current is None:
                raise UnhingedError(f'Queue "{queue}" does not exist')
            ttl = interval_seconds(policy["cleanup_ttl"]) if "cleanup_ttl" in policy else current["cleanup_ttl"]
            limit = int(policy["cleanup_limit"]) if "cleanup_limit" in policy else current["cleanup_limit"]
            if ttl < 0:
                raise UnhingedError("cleanup_ttl must be non-negative")
            if limit < 1:
                raise UnhingedError("cleanup_limit must be at least 1")
            self._exec(
                "update queues set cleanup_ttl = ?, cleanup_limit = ? where queue_name = ?",
                (ttl, limit, queue),
            )

    # -- tasks -------------------------------------------------------------

    def get_task_result(self, queue: str, task_id: str) -> Optional[dict]:
        """Port of ``absurd.get_task_result``."""
        queue = validate_queue_name(queue)
        row = self._one(
            f"""
            select t.task_id,
                   t.state,
                   case when t.state = 'completed' then t.completed_payload end as result,
                   case when t.state = 'failed' then r.failure_reason end as failure_reason
              from {table('t', queue)} t
              left join {table('r', queue)} r on r.run_id = t.last_attempt_run
             where t.task_id = ?
            """,
            (task_id,),
        )
        return _row(row)

    def spawn_task(
        self,
        queue: str,
        task_name: str,
        params: JsonValue = None,
        options: Optional[dict] = None,
    ) -> dict:
        """Port of ``absurd.spawn_task``.

        Returns ``{"task_id", "run_id", "attempt", "created"}``. With an
        ``idempotency_key`` that already exists the existing task is returned
        with ``created`` False.
        """
        if not task_name or not task_name.strip():
            raise UnhingedError("task_name must be provided")
        options = options or {}
        headers = options.get("headers")
        retry_strategy = options.get("retry_strategy")
        retry_delay_seconds(retry_strategy, 1)  # validate
        max_attempts = options.get("max_attempts")
        if max_attempts is not None:
            max_attempts = int(max_attempts)
            if max_attempts < 1:
                raise UnhingedError("max_attempts must be >= 1")
        cancellation = options.get("cancellation")
        idempotency_key = options.get("idempotency_key")

        task_id = uuidv7()
        run_id = uuidv7()
        t, r = table("t", queue), table("r", queue)

        with self.transaction():
            now = ts(self.now())
            inserted = self._exec(
                f"""
                insert into {t} (task_id, task_name, params, headers, retry_strategy,
                                 max_attempts, cancellation, enqueue_at, first_started_at,
                                 state, attempts, last_attempt_run, completed_payload,
                                 cancelled_at, idempotency_key)
                values (?, ?, ?, ?, ?, ?, ?, ?, null, 'pending', 1, ?, null, null, ?)
                on conflict (idempotency_key) do nothing
                """,
                (
                    task_id, task_name, dumps(params),
                    dumps(headers) if headers is not None else None,
                    dumps(retry_strategy) if retry_strategy is not None else None,
                    max_attempts,
                    dumps(cancellation) if cancellation is not None else None,
                    now, run_id, idempotency_key,
                ),
            )
            if inserted == 0:
                existing = self._one(
                    f"select task_id, last_attempt_run, attempts from {t} where idempotency_key = ?",
                    (idempotency_key,),
                )
                return {
                    "task_id": existing["task_id"],
                    "run_id": existing["last_attempt_run"],
                    "attempt": existing["attempts"],
                    "created": False,
                }
            self._exec(
                f"""
                insert into {r} (run_id, task_id, attempt, state, available_at, wake_event,
                                 event_payload, result, failure_reason, created_at)
                values (?, ?, 1, 'pending', ?, null, null, null, null, ?)
                """,
                (run_id, task_id, now, now),
            )
        return {"task_id": task_id, "run_id": run_id, "attempt": 1, "created": True}

    def claim_task(
        self,
        queue: str,
        worker_id: str = "worker",
        claim_timeout: int = 30,
        qty: int = 1,
    ) -> list[dict]:
        """Port of ``absurd.claim_task``.

        Applies cancellation policies, fails expired leases (bounded by
        ``qty``), then hands up to ``qty`` runs to the worker.
        """
        queue = validate_queue_name(queue)
        claim_timeout = 30 if claim_timeout is None else max(claim_timeout, 0)
        worker_id = worker_id or "worker"
        qty = max(qty or 1, 1)
        t, r, w = table("t", queue), table("r", queue), table("w", queue)

        with self.transaction():
            now_dt = self.now()
            now = ts(now_dt)
            claim_until = ts(now_dt + timedelta(seconds=claim_timeout)) if claim_timeout > 0 else None

            # Cancellation policies (max_delay before first start, max_duration
            # after). The elapsed-time comparison is done in Python because
            # SQLite's julianday() arithmetic is not exact to the microsecond.
            for cand in self._all(
                f"""
                select task_id, enqueue_at, first_started_at, cancellation
                  from {t}
                 where state in ('pending', 'sleeping', 'running')
                   and cancellation is not null
                 order by task_id
                """
            ):
                policy = loads(cand["cancellation"]) or {}
                max_delay = policy.get("max_delay")
                max_duration = policy.get("max_duration")
                started = parse_ts(cand["first_started_at"])
                if started is None and max_delay is not None:
                    elapsed = (now_dt - parse_ts(cand["enqueue_at"])).total_seconds()
                    if elapsed >= float(max_delay):
                        self.cancel_task(queue, cand["task_id"])
                elif started is not None and max_duration is not None:
                    if (now_dt - started).total_seconds() >= float(max_duration):
                        self.cancel_task(queue, cand["task_id"])

            # Expired leases -> fail_run with $ClaimTimeout, at most qty per call.
            for expired in self._all(
                f"""
                select run_id, claimed_by, claim_expires_at, attempt
                  from {r}
                 where state = 'running'
                   and claim_expires_at is not null
                   and claim_expires_at <= ?
                 order by claim_expires_at, run_id
                 limit ?
                """,
                (now, qty),
            ):
                reason = {
                    "name": "$ClaimTimeout",
                    "message": "worker did not finish task within claim interval",
                    "workerId": expired["claimed_by"],
                    "claimExpiredAt": parse_ts(expired["claim_expires_at"]).isoformat(),
                    "attempt": expired["attempt"],
                }
                self.fail_run(queue, expired["run_id"], {k: v for k, v in reason.items() if v is not None})

            candidates = [
                row["run_id"]
                for row in self._all(
                    f"""
                    select r.run_id
                      from {r} r
                      join {t} t on t.task_id = r.task_id
                     where r.state in ('pending', 'sleeping')
                       and t.state in ('pending', 'sleeping', 'running')
                       and r.available_at <= ?
                     order by r.available_at, r.run_id
                     limit ?
                    """,
                    (now, qty),
                )
            ]
            if not candidates:
                return []

            marks = ",".join("?" * len(candidates))
            self._exec(
                f"""
                update {r}
                   set state = 'running', claimed_by = ?, claim_expires_at = ?,
                       started_at = ?, available_at = ?
                 where run_id in ({marks})
                """,
                (worker_id, claim_until, now, now, *candidates),
            )
            self._exec(
                f"""
                update {t}
                   set state = 'running',
                       attempts = max(attempts, (select attempt from {r} where run_id = u.run_id)),
                       first_started_at = coalesce(first_started_at, ?),
                       last_attempt_run = u.run_id
                  from (select run_id, task_id from {r} where run_id in ({marks})) as u
                 where {t}.task_id = u.task_id
                """,
                (now, *candidates),
            )
            self._exec(
                f"delete from {w} where run_id in ({marks}) and timeout_at is not null and timeout_at <= ?",
                (*candidates, now),
            )
            rows = self._all(
                f"""
                select r.run_id, r.task_id, r.attempt, t.task_name, t.params, t.retry_strategy,
                       t.max_attempts, t.headers, r.wake_event, r.event_payload
                  from {r} r
                  join {t} t on t.task_id = r.task_id
                 where r.run_id in ({marks})
                 order by r.available_at, r.run_id
                """,
                tuple(candidates),
            )
            return [_row(row) for row in rows]

    def complete_run(self, queue: str, run_id: str, state: JsonValue = None) -> None:
        """Port of ``absurd.complete_run``."""
        t, r, w = table("t", queue), table("r", queue), table("w", queue)
        with self.transaction():
            now = ts(self.now())
            run = self._one(f"select task_id, state from {r} where run_id = ?", (run_id,))
            if run is None:
                raise UnhingedError(f'Run "{run_id}" not found in queue "{queue}"')
            if run["state"] != "running":
                if run["state"] == "cancelled":
                    raise TaskCancelled()
                if run["state"] == "failed":
                    raise RunAlreadyFailed(f'Run "{run_id}" has already failed in queue "{queue}"')
                raise UnhingedError(f'Run "{run_id}" is not currently running in queue "{queue}"')
            payload = dumps(state) if state is not None else None
            self._exec(
                f"update {r} set state = 'completed', completed_at = ?, result = ? where run_id = ?",
                (now, payload, run_id),
            )
            self._exec(
                f"update {t} set state = 'completed', completed_payload = ?, last_attempt_run = ? where task_id = ?",
                (payload, run_id, run["task_id"]),
            )
            self._exec(f"delete from {w} where run_id = ?", (run_id,))

    def schedule_run(self, queue: str, run_id: str, wake_at: datetime) -> None:
        """Port of ``absurd.schedule_run`` (sleep until ``wake_at``)."""
        t, r = table("t", queue), table("r", queue)
        with self.transaction():
            run = self._one(f"select task_id from {r} where run_id = ? and state = 'running'", (run_id,))
            if run is None:
                raise UnhingedError(f'Run "{run_id}" is not currently running in queue "{queue}"')
            self._exec(
                f"""
                update {r}
                   set state = 'sleeping', claimed_by = null, claim_expires_at = null,
                       available_at = ?, wake_event = null
                 where run_id = ?
                """,
                (ts(wake_at), run_id),
            )
            self._exec(f"update {t} set state = 'sleeping' where task_id = ?", (run["task_id"],))

    def fail_run(
        self,
        queue: str,
        run_id: str,
        reason: JsonValue,
        retry_at: Optional[datetime] = None,
    ) -> None:
        """Port of ``absurd.fail_run``: fail the run, schedule a retry or finish the task."""
        t, r, w = table("t", queue), table("r", queue), table("w", queue)
        with self.transaction():
            now_dt = self.now()
            now = ts(now_dt)
            run = self._one(f"select task_id, attempt, state from {r} where run_id = ?", (run_id,))
            if run is None:
                raise UnhingedError(f'Run "{run_id}" cannot be failed in queue "{queue}"')
            if run["state"] == "cancelled":
                raise TaskCancelled()
            if run["state"] == "failed":
                raise RunAlreadyFailed(f'Run "{run_id}" has already failed in queue "{queue}"')
            if run["state"] not in ("running", "sleeping"):
                raise UnhingedError(f'Run "{run_id}" cannot be failed in queue "{queue}"')

            task_id, attempt = run["task_id"], run["attempt"]
            task = self._one(
                f"select retry_strategy, max_attempts, first_started_at, cancellation from {t} where task_id = ?",
                (task_id,),
            )
            retry_strategy = loads(task["retry_strategy"])
            max_attempts = task["max_attempts"]
            first_started = parse_ts(task["first_started_at"])
            cancellation = loads(task["cancellation"])

            self._exec(
                f"update {r} set state = 'failed', wake_event = null, failed_at = ?, failure_reason = ? where run_id = ?",
                (now, dumps(reason) if reason is not None else None, run_id),
            )

            next_attempt = attempt + 1
            task_state_after = "failed"
            recorded_attempt = attempt
            last_attempt_run = run_id
            cancelled_at = None
            task_cancel = False

            if max_attempts is None or next_attempt <= max_attempts:
                if retry_at is not None:
                    next_available: Optional[datetime] = retry_at
                else:
                    try:
                        delay = retry_delay_seconds(retry_strategy, attempt)
                        next_available = now_dt + timedelta(seconds=delay)
                    except InvalidRetryStrategy:
                        # Legacy strategy predating validation: fail permanently
                        # rather than wedging the queue.
                        next_available = None
                if next_available is not None and next_available < now_dt:
                    next_available = now_dt

                if cancellation is not None and next_available is not None:
                    max_duration = cancellation.get("max_duration")
                    if max_duration is not None and first_started is not None:
                        if (next_available - first_started).total_seconds() >= float(max_duration):
                            task_cancel = True

                if not task_cancel and next_available is not None:
                    task_state_after = "sleeping" if next_available > now_dt else "pending"
                    new_run_id = uuidv7()
                    recorded_attempt = next_attempt
                    last_attempt_run = new_run_id
                    self._exec(
                        f"""
                        insert into {r} (run_id, task_id, attempt, state, available_at, wake_event,
                                         event_payload, result, failure_reason, created_at)
                        values (?, ?, ?, ?, ?, null, null, null, null, ?)
                        """,
                        (new_run_id, task_id, next_attempt, task_state_after, ts(next_available), now),
                    )

            if task_cancel:
                task_state_after = "cancelled"
                cancelled_at = now
                recorded_attempt = max(recorded_attempt, attempt)
                last_attempt_run = run_id

            self._exec(
                f"""
                update {t}
                   set state = ?, attempts = max(attempts, ?), last_attempt_run = ?,
                       cancelled_at = coalesce(cancelled_at, ?)
                 where task_id = ?
                """,
                (task_state_after, recorded_attempt, last_attempt_run, cancelled_at, task_id),
            )
            self._exec(f"delete from {w} where run_id = ?", (run_id,))

    def retry_task(self, queue: str, task_id: str, options: Optional[dict] = None) -> dict:
        """Port of ``absurd.retry_task``: revive a failed task in place or as a new task."""
        options = options or {}
        spawn_new = bool(options.get("spawn_new", False))
        requested_max = options.get("max_attempts")
        if requested_max is not None:
            requested_max = int(requested_max)
            if requested_max < 1:
                raise UnhingedError("max_attempts must be >= 1")
        t, r = table("t", queue), table("r", queue)
        with self.transaction():
            now = ts(self.now())
            task = self._one(
                f"""
                select task_name, params, headers, retry_strategy, max_attempts,
                       cancellation, attempts, state
                  from {t} where task_id = ?
                """,
                (task_id,),
            )
            if task is None:
                raise UnhingedError(f'Task "{task_id}" not found in queue "{queue}"')
            if task["state"] != "failed":
                raise UnhingedError(f'Task "{task_id}" is not currently failed in queue "{queue}"')

            if spawn_new:
                opts = {
                    "headers": loads(task["headers"]),
                    "retry_strategy": loads(task["retry_strategy"]),
                    "max_attempts": requested_max if requested_max is not None else task["max_attempts"],
                    "cancellation": loads(task["cancellation"]),
                }
                opts = {k: v for k, v in opts.items() if v is not None}
                return self.spawn_task(queue, task["task_name"], loads(task["params"]), opts)

            attempts = task["attempts"]
            if requested_max is None:
                requested_max = (task["max_attempts"] if task["max_attempts"] is not None else attempts) + 1
            if requested_max <= attempts:
                raise UnhingedError(
                    f"max_attempts ({requested_max}) must be greater than current attempts ({attempts})"
                )
            new_run_id = uuidv7()
            new_attempt = attempts + 1
            self._exec(
                f"""
                insert into {r} (run_id, task_id, attempt, state, available_at, wake_event,
                                 event_payload, result, failure_reason, created_at)
                values (?, ?, ?, 'pending', ?, null, null, null, null, ?)
                """,
                (new_run_id, task_id, new_attempt, now, now),
            )
            self._exec(
                f"""
                update {t}
                   set state = 'pending', attempts = max(attempts, ?), max_attempts = ?,
                       last_attempt_run = ?, cancelled_at = null
                 where task_id = ?
                """,
                (new_attempt, requested_max, new_run_id, task_id),
            )
            return {"task_id": task_id, "run_id": new_run_id, "attempt": new_attempt, "created": False}

    # -- checkpoints -------------------------------------------------------

    def set_task_checkpoint_state(
        self,
        queue: str,
        task_id: str,
        step_name: str,
        state: JsonValue,
        owner_run: str,
        extend_claim_by: Optional[int] = None,
    ) -> None:
        """Port of ``absurd.set_task_checkpoint_state``.

        Writes are rejected once the task is cancelled (AB001) or the run has
        failed (AB002). A checkpoint owned by a later attempt is never
        overwritten by an earlier one.
        """
        if not step_name or not step_name.strip():
            raise UnhingedError("step_name must be provided")
        t, r, c = table("t", queue), table("r", queue), table("c", queue)
        with self.transaction():
            now_dt = self.now()
            now = ts(now_dt)
            run = self._one(
                f"""
                select r.attempt, r.state as run_state, t.state as task_state
                  from {r} r join {t} t on t.task_id = r.task_id
                 where r.run_id = ?
                """,
                (owner_run,),
            )
            if run is None:
                raise UnhingedError(f'Run "{owner_run}" not found for checkpoint')
            if run["task_state"] == "cancelled":
                raise TaskCancelled()
            if run["run_state"] == "failed":
                raise RunAlreadyFailed(f'Run "{owner_run}" has already failed in queue "{queue}"')

            if extend_claim_by is not None and extend_claim_by > 0:
                self._exec(
                    f"""
                    update {r} set claim_expires_at = ?
                     where run_id = ? and state = 'running' and claim_expires_at is not null
                    """,
                    (ts(now_dt + timedelta(seconds=extend_claim_by)), owner_run),
                )

            existing = self._one(
                f"""
                select c.owner_run_id, r.attempt
                  from {c} c left join {r} r on r.run_id = c.owner_run_id
                 where c.task_id = ? and c.checkpoint_name = ?
                """,
                (task_id, step_name),
            )
            if (
                existing is None
                or existing["owner_run_id"] is None
                or existing["attempt"] is None
                or run["attempt"] >= existing["attempt"]
            ):
                self._upsert_checkpoint(c, task_id, step_name, state, owner_run, now)

    def _upsert_checkpoint(self, c: str, task_id: str, name: str, state: JsonValue, owner_run: str, now: str) -> None:
        self._exec(
            f"""
            insert into {c} (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
            values (?, ?, ?, 'committed', ?, ?)
            on conflict (task_id, checkpoint_name)
            do update set state = excluded.state, status = excluded.status,
                          owner_run_id = excluded.owner_run_id, updated_at = excluded.updated_at
            """,
            (task_id, name, dumps(state), owner_run, now),
        )

    def extend_claim(self, queue: str, run_id: str, extend_by: int) -> None:
        """Port of ``absurd.extend_claim`` (the heartbeat)."""
        if extend_by is None or extend_by <= 0:
            raise UnhingedError("extend_by must be > 0")
        t, r = table("t", queue), table("r", queue)
        with self.transaction():
            now_dt = self.now()
            run = self._one(
                f"""
                select r.state as run_state, r.claim_expires_at, t.state as task_state
                  from {r} r join {t} t on t.task_id = r.task_id
                 where r.run_id = ?
                """,
                (run_id,),
            )
            if run is None:
                raise UnhingedError(f'Run "{run_id}" not found in queue "{queue}"')
            if run["task_state"] == "cancelled":
                raise TaskCancelled()
            if run["run_state"] != "running":
                if run["run_state"] == "failed":
                    raise RunAlreadyFailed(f'Run "{run_id}" has already failed in queue "{queue}"')
                raise UnhingedError(f'Run "{run_id}" is not currently running in queue "{queue}"')
            if run["claim_expires_at"] is None:
                raise UnhingedError(f'Run "{run_id}" does not have an active claim in queue "{queue}"')
            self._exec(
                f"update {r} set claim_expires_at = ? where run_id = ?",
                (ts(now_dt + timedelta(seconds=extend_by)), run_id),
            )

    def get_task_checkpoint_state(
        self, queue: str, task_id: str, step_name: str, include_pending: bool = False
    ) -> Optional[dict]:
        """Port of ``absurd.get_task_checkpoint_state``. Returns None when missing."""
        c = table("c", queue)
        row = self._one(
            f"""
            select checkpoint_name, state as state_json, status, owner_run_id, updated_at
              from {c}
             where task_id = ? and checkpoint_name = ? and (? or status = 'committed')
            """,
            (task_id, step_name, 1 if include_pending else 0),
        )
        return self._checkpoint_row(row)

    def get_task_checkpoint_states(self, queue: str, task_id: str, run_id: str) -> list[dict]:
        """Port of ``absurd.get_task_checkpoint_states``: checkpoints visible to ``run_id``."""
        r, c = table("r", queue), table("c", queue)
        run = self._one(f"select task_id, attempt from {r} where run_id = ?", (run_id,))
        if run is None:
            raise UnhingedError(f'Run "{run_id}" not found in queue "{queue}"')
        if run["task_id"] != task_id:
            raise UnhingedError(f'Run "{run_id}" does not belong to task "{task_id}" in queue "{queue}"')
        rows = self._all(
            f"""
            select c.checkpoint_name, c.state as state_json, c.status, c.owner_run_id, c.updated_at
              from {c} c left join {r} o on o.run_id = c.owner_run_id
             where c.task_id = ? and c.status = 'committed'
               and (o.attempt is null or o.attempt <= ?)
             order by c.updated_at asc
            """,
            (task_id, run["attempt"]),
        )
        return [self._checkpoint_row(row) for row in rows]

    @staticmethod
    def _checkpoint_row(row: Optional[sqlite3.Row]) -> Optional[dict]:
        out = _row(row)
        if out is not None:
            out["state"] = out.pop("state_json")
        return out

    # -- events ------------------------------------------------------------

    def await_event(
        self,
        queue: str,
        task_id: str,
        run_id: str,
        step_name: str,
        event_name: str,
        timeout: Optional[float] = None,
    ) -> tuple[bool, JsonValue]:
        """Port of ``absurd.await_event``.

        Returns ``(should_suspend, payload)``. When the event already fired
        the payload is checkpointed under ``step_name`` and returned at once.
        Otherwise a wait is registered and the run is put to sleep.
        """
        if not event_name or not event_name.strip():
            raise UnhingedError("event_name must be provided")
        t, r, c, e, w = (table(p, queue) for p in "trcew")
        with self.transaction():
            now_dt = self.now()
            now = ts(now_dt)
            timeout_at: Optional[str] = None
            if timeout is not None:
                if timeout < 0:
                    raise UnhingedError("timeout must be non-negative")
                timeout_at = ts(now_dt + timedelta(seconds=float(timeout)))
            available_at = timeout_at if timeout_at is not None else INFINITY

            checkpoint = self._one(
                f"select state from {c} where task_id = ? and checkpoint_name = ?",
                (task_id, step_name),
            )
            if checkpoint is not None and checkpoint["state"] is not None:
                return False, loads(checkpoint["state"])

            # Ensure an event row exists; payload NULL is the "not emitted" sentinel.
            self._exec(
                f"insert into {e} (event_name, payload, emitted_at) values (?, null, ?) "
                "on conflict (event_name) do nothing",
                (event_name, EPOCH),
            )

            run = self._one(
                f"""
                select r.state as run_state, r.event_payload, r.wake_event, t.state as task_state
                  from {r} r join {t} t on t.task_id = r.task_id
                 where r.run_id = ?
                """,
                (run_id,),
            )
            if run is None:
                raise UnhingedError(f'Run "{run_id}" not found while awaiting event')
            if run["task_state"] == "cancelled":
                raise TaskCancelled()

            event_payload_text = self._one(f"select payload from {e} where event_name = ?", (event_name,))["payload"]
            existing_payload_text = run["event_payload"]
            resolved: Optional[str] = None

            if existing_payload_text is not None:
                self._exec(f"update {r} set event_payload = null where run_id = ?", (run_id,))
                if event_payload_text is not None and loads(event_payload_text) == loads(existing_payload_text):
                    resolved = existing_payload_text

            if run["run_state"] != "running":
                raise UnhingedError(f'Run "{run_id}" must be running to await events')

            if resolved is None and event_payload_text is not None:
                resolved = event_payload_text

            if resolved is not None:
                self._upsert_checkpoint(c, task_id, step_name, loads(resolved), run_id, now)
                return False, loads(resolved)

            # Woke up because the wait timed out: don't re-sleep.
            if run["wake_event"] == event_name and existing_payload_text is None:
                self._exec(f"update {r} set wake_event = null where run_id = ?", (run_id,))
                return False, None

            self._exec(
                f"""
                insert into {w} (task_id, run_id, step_name, event_name, timeout_at, created_at)
                values (?, ?, ?, ?, ?, ?)
                on conflict (run_id, step_name)
                do update set event_name = excluded.event_name, timeout_at = excluded.timeout_at,
                              created_at = excluded.created_at
                """,
                (task_id, run_id, step_name, event_name, timeout_at, now),
            )
            self._exec(
                f"""
                update {r}
                   set state = 'sleeping', claimed_by = null, claim_expires_at = null,
                       available_at = ?, wake_event = ?, event_payload = null
                 where run_id = ?
                """,
                (available_at, event_name, run_id),
            )
            self._exec(f"update {t} set state = 'sleeping' where task_id = ?", (task_id,))
            return True, None

    def emit_event(self, queue: str, event_name: str, payload: JsonValue = None) -> None:
        """Port of ``absurd.emit_event``: first write wins, wakes every waiter."""
        if not event_name or not event_name.strip():
            raise UnhingedError("event_name must be provided")
        t, r, c, e, w = (table(p, queue) for p in "trcew")
        payload_text = dumps(payload)
        with self.transaction():
            now = ts(self.now())
            applied = self._exec(
                f"""
                insert into {e} (event_name, payload, emitted_at) values (?, ?, ?)
                on conflict (event_name)
                do update set payload = excluded.payload, emitted_at = excluded.emitted_at
                where {e}.payload is null
                """,
                (event_name, payload_text, now),
            )
            if applied == 0:
                return  # already emitted; cached payload stands

            self._exec(
                f"delete from {w} where event_name = ? and timeout_at is not null and timeout_at <= ?",
                (event_name, now),
            )
            affected = self._all(
                f"""
                select w.run_id, w.task_id, w.step_name
                  from {w} w join {r} r on r.run_id = w.run_id
                 where w.event_name = ? and (w.timeout_at is null or w.timeout_at > ?)
                   and r.state = 'sleeping'
                """,
                (event_name, now),
            )
            if not affected:
                return
            run_ids = [a["run_id"] for a in affected]
            marks = ",".join("?" * len(run_ids))
            self._exec(
                f"""
                update {r}
                   set state = 'pending', available_at = ?, wake_event = null, event_payload = ?,
                       claimed_by = null, claim_expires_at = null
                 where run_id in ({marks}) and state = 'sleeping'
                """,
                (now, payload_text, *run_ids),
            )
            for a in affected:
                self._upsert_checkpoint(c, a["task_id"], a["step_name"], payload, a["run_id"], now)
            self._exec(
                f"update {t} set state = 'pending' where task_id in (select task_id from {r} where run_id in ({marks}))",
                tuple(run_ids),
            )
            self._exec(
                f"delete from {w} where event_name = ? and run_id in ({marks})",
                (event_name, *run_ids),
            )

    # -- cancellation ------------------------------------------------------

    def cancel_task(self, queue: str, task_id: str) -> None:
        """Port of ``absurd.cancel_task``. Terminal tasks are left alone."""
        t, r, w = table("t", queue), table("r", queue), table("w", queue)
        with self.transaction():
            now = ts(self.now())
            task = self._one(f"select state from {t} where task_id = ?", (task_id,))
            if task is None:
                raise UnhingedError(f'Task "{task_id}" not found in queue "{queue}"')
            if task["state"] in ("completed", "failed", "cancelled"):
                return
            self._exec(
                f"update {t} set state = 'cancelled', cancelled_at = coalesce(cancelled_at, ?) where task_id = ?",
                (now, task_id),
            )
            self._exec(
                f"""
                update {r} set state = 'cancelled', claimed_by = null, claim_expires_at = null
                 where task_id = ? and state not in ('completed', 'failed', 'cancelled')
                """,
                (task_id,),
            )
            self._exec(f"delete from {w} where task_id = ?", (task_id,))

    # -- cleanup -----------------------------------------------------------

    def cleanup_all_queues(self, queue: Optional[str] = None) -> list[dict]:
        """Port of ``absurd.cleanup_all_queues`` using each queue's stored policy."""
        if queue is not None:
            queue = validate_queue_name(queue)
            if self.get_queue_policy(queue) is None:
                raise UnhingedError(f'Queue "{queue}" does not exist')
        out = []
        for row in self._all(
            "select queue_name, cleanup_ttl, cleanup_limit from queues where ? is null or queue_name = ? order by queue_name",
            (queue, queue),
        ):
            out.append(
                {
                    "queue_name": row["queue_name"],
                    "tasks_deleted": self.cleanup_tasks(row["queue_name"], row["cleanup_ttl"], row["cleanup_limit"]),
                    "events_deleted": self.cleanup_events(row["queue_name"], row["cleanup_ttl"], row["cleanup_limit"]),
                }
            )
        return out

    def cleanup_tasks(self, queue: str, ttl_seconds: int, limit: int = 1000) -> int:
        """Port of ``absurd.cleanup_tasks``: delete terminal tasks older than the TTL."""
        if ttl_seconds is None or ttl_seconds < 0:
            raise UnhingedError("TTL must be a non-negative number of seconds")
        t, r, c, w = (table(p, queue) for p in "trcw")
        with self.transaction():
            cutoff = ts(self.now() - timedelta(seconds=ttl_seconds))
            victims = [
                row["task_id"]
                for row in self._all(
                    f"""
                    select t.task_id,
                           case when t.state = 'completed' then r.completed_at
                                when t.state = 'failed' then r.failed_at
                                when t.state = 'cancelled' then t.cancelled_at
                           end as terminal_at
                      from {t} t left join {r} r on r.run_id = t.last_attempt_run
                     where t.state in ('completed', 'failed', 'cancelled')
                       and terminal_at is not null and terminal_at < ?
                     order by terminal_at
                     limit ?
                    """,
                    (cutoff, limit),
                )
            ]
            if not victims:
                return 0
            marks = ",".join("?" * len(victims))
            for tbl in (w, c, r):
                self._exec(f"delete from {tbl} where task_id in ({marks})", tuple(victims))
            return self._exec(f"delete from {t} where task_id in ({marks})", tuple(victims))

    def cleanup_events(self, queue: str, ttl_seconds: int, limit: int = 1000) -> int:
        """Port of ``absurd.cleanup_events``."""
        if ttl_seconds is None or ttl_seconds < 0:
            raise UnhingedError("TTL must be a non-negative number of seconds")
        e = table("e", queue)
        with self.transaction():
            cutoff = ts(self.now() - timedelta(seconds=ttl_seconds))
            return self._exec(
                f"""
                delete from {e} where event_name in (
                    select event_name from {e} where emitted_at < ? order by emitted_at limit ?
                )
                """,
                (cutoff, limit),
            )

    # -- inspection (not in Absurd's SQL, used by tests and the CLI) ---------

    def get_task(self, queue: str, task_id: str) -> Optional[dict]:
        return _row(self._one(f"select * from {table('t', queue)} where task_id = ?", (task_id,)))

    def get_run(self, queue: str, run_id: str) -> Optional[dict]:
        return _row(self._one(f"select * from {table('r', queue)} where run_id = ?", (run_id,)))

    def get_runs(self, queue: str, task_id: str) -> list[dict]:
        rows = self._all(f"select * from {table('r', queue)} where task_id = ? order by attempt", (task_id,))
        return [_row(r) for r in rows]

    def get_checkpoint(self, queue: str, task_id: str, name: str) -> Optional[dict]:
        return self._checkpoint_row(
            self._one(
                f"select task_id, checkpoint_name, state as state_json, status, owner_run_id, updated_at "
                f"from {table('c', queue)} where task_id = ? and checkpoint_name = ?",
                (task_id, name),
            )
        )

    def list_tasks(self, queue: str, state: Optional[str] = None, limit: int = 100) -> list[dict]:
        rows = self._all(
            f"select * from {table('t', queue)} where ? is null or state = ? order by enqueue_at desc limit ?",
            (state, state, limit),
        )
        return [_row(r) for r in rows]

    def count(self, prefix: str, queue: str) -> int:
        return self._one(f"select count(*) as n from {table(prefix, queue)}")["n"]

    def queue_stats(self, queue: str) -> dict:
        rows = self._all(f"select state, count(*) as n from {table('t', queue)} group by state")
        stats = {s: 0 for s in schema.STATES}
        stats.update({r["state"]: r["n"] for r in rows})
        stats["waits"] = self.count("w", queue)
        stats["events"] = self.count("e", queue)
        stats["checkpoints"] = self.count("c", queue)
        return stats


# --------------------------------------------------------------------------
# per-thread engines, for the SDK's worker pool


class EnginePool:
    """Hands out one :class:`Engine` per thread for a given database path."""

    def __init__(self, path: str, **kwargs: Any) -> None:
        self.path = path
        self._kwargs = kwargs
        self._local = threading.local()
        self._all: list[Engine] = []
        self._lock = threading.Lock()

    def get(self) -> Engine:
        engine = getattr(self._local, "engine", None)
        if engine is None:
            engine = Engine(self.path, **self._kwargs)
            self._local.engine = engine
            with self._lock:
                self._all.append(engine)
        return engine

    def close(self) -> None:
        with self._lock:
            for engine in self._all:
                try:
                    engine.close()
                except sqlite3.ProgrammingError:
                    pass
            self._all.clear()
