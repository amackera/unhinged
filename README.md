# unhinged

[Absurd](https://github.com/earendil-works/absurd) is Armin Ronacher's durable
execution engine that lives entirely inside Postgres: one SQL file of tables and
stored procedures, plus a thin SDK. **unhinged** is that schema and state
machine ported to SQLite. Same tables, same procedures, same semantics, no
server, zero dependencies.

```python
from unhinged import Unhinged

app = Unhinged("app.db", queue_name="orders")

@app.register_task("fulfil-order")
def fulfil_order(params, ctx):
    payment = ctx.step("charge", lambda: charge_card(params["card"], params["amount"]))
    ctx.sleep_for("cooldown", 60)
    shipment = ctx.await_event(f"shipped:{params['order_id']}", timeout=86400)
    return {"payment": payment, "tracking": shipment["tracking"]}

app.spawn("fulfil-order", {"order_id": 42, "card": "tok_1", "amount": 1999})
app.start_worker(concurrency=4)
```

Steps run once and are checkpointed in SQLite. If the worker crashes, is
`kill -9`ed, or the task sleeps or waits for an event, the next run replays the
handler and completed steps return their stored value instead of executing.
`tests/test_sdk.py::test_crash_recovery_across_processes` SIGKILLs a worker
after step 2 and checks that a fresh process runs only steps 3 and 4.

## What was ported

| Absurd (Postgres)                         | unhinged (SQLite)                                  |
|-------------------------------------------|----------------------------------------------------|
| `absurd` schema                           | tables at the top level of one `.db` file          |
| `t_`/`r_`/`c_`/`e_`/`w_` tables per queue | same names, same columns                           |
| `uuid`, `jsonb`, `timestamptz`, `interval`| TEXT uuidv7, TEXT json, fixed-width UTC text, int seconds |
| plpgsql procedures (`absurd.spawn_task`…) | `Engine` methods with the same names and arguments |
| `FOR UPDATE SKIP LOCKED`, row locks       | one `BEGIN IMMEDIATE` transaction per procedure    |
| SQLSTATE `AB001`/`AB002`/`AB003`          | `TaskCancelled` / `RunAlreadyFailed` / `InvalidRetryStrategy` |
| `absurd.fake_now` for tests               | `engine.set_fake_now()`                            |
| `absurdctl`                               | `python -m unhinged`                               |
| partitioned storage mode, `i_` table      | dropped (Postgres-only)                            |
| pg_cron maintenance jobs                  | dropped; `start_worker(maintenance_interval=…)` or `app.cleanup()` instead |

Every `absurd.*` function has a counterpart on `unhinged.Engine`: `create_queue`,
`drop_queue`, `list_queues`, `get_queue_policy`, `set_queue_policy`,
`spawn_task`, `claim_task`, `complete_run`, `schedule_run`, `fail_run`,
`retry_task`, `set_task_checkpoint_state`, `get_task_checkpoint_state(s)`,
`extend_claim`, `await_event`, `emit_event`, `cancel_task`, `cleanup_tasks`,
`cleanup_events`, `cleanup_all_queues`, `get_task_result`. The engine tests in
`tests/test_engine.py` are ports of Absurd's own test suite.

Absurd is pull-based on Postgres too, so nothing was lost there: workers poll.
Multiple worker processes share one database file through WAL mode and
`busy_timeout`; threads inside a worker each get their own connection.

## Run it

```bash
uv venv && uv pip install -e '.[dev]'
pytest                                   # 44 tests, ~3s
python examples/crash_demo.py            # kill -9 a worker mid-task, watch it resume
python examples/bench.py --tasks 2000 --workers 4
```

Quickstart (two terminals):

```bash
python examples/quickstart_worker.py
python examples/quickstart_client.py alice --await
python -m unhinged --db quickstart.db emit user-activated:alice '{"activated_at":"now"}'
python -m unhinged --db quickstart.db stats
```

## Caveats

* SQLite has one writer at a time. Every claim, checkpoint and completion is a
  short write transaction, so throughput tops out around a few thousand tasks
  per second on a laptop regardless of worker count. Plenty for one machine,
  not a replacement for Postgres across a fleet.
* Nothing deletes old rows unless you ask. Pass `maintenance_interval` to
  `start_worker`, call `app.cleanup()` from a cron job, or run
  `python -m unhinged cleanup`. Retention per queue is set with
  `set_queue_policy` (default 30 days, 1000 rows per sweep).
* The connection uses `synchronous=NORMAL` in WAL mode: safe against process
  crashes, but a power loss can lose the last few transactions. Set
  `pragma synchronous=full` on `engine.connection` if that matters.
* Only the synchronous SDK was ported. The async client, hooks, and `absurdctl`
  partition commands were not.
