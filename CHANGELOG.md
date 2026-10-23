# Changelog

## Unreleased

- An async SDK: `AsyncUnhinged` and `AsyncTaskContext`, with the same
  checkpoints as the sync one, so a task can move between the two.
- `unhinged.pydantic_ai.UnhingedDurability`, a durable-execution backend for
  Pydantic AI agents with the same step names as Absurd's. Install with
  `unhinged[pydantic-ai]`.

## 0.1.0 (2026-10-23)

First release. A port of Absurd 0.5.1's schema and procedures to SQLite.

- Tasks, runs, checkpoints, events and waits, same tables and columns as
  Absurd; every procedure is an `Engine` method running in one
  `BEGIN IMMEDIATE` transaction.
- A synchronous SDK: `register_task`, `spawn`, steps, `sleep_for`,
  `sleep_until`, `await_event`, `emit_event`, heartbeats, cross-queue
  `await_task_result`, a polling worker with a thread pool, and periodic
  cleanup.
- Several worker processes may share one database file. The chaos test
  kills them at random and checks no run is ever executed twice.
- Commits fsync by default. `fullfsync=True` for power-safety on macOS.
- `python -m unhinged`, a small `absurdctl`.
- Examples: a crash demo, a release checklist that parks on approval, a
  resumable batch pipeline, a durable agent loop, a benchmark that
  calibrates the disk first, and the chaos harness.

Not ported: partitioned queues, pg_cron maintenance, the async SDK, hooks.
