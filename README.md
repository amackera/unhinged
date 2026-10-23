# Unhinged

[Absurd](https://github.com/earendil-works/absurd) is Armin Ronacher's durable
execution engine: one SQL file that installs tasks, steps, checkpoints, sleeps
and events into Postgres, and a thin SDK on top. Unhinged is the same engine on
SQLite. No server, no dependencies, one file on disk.

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

Steps run once. Their results go to SQLite, and when the handler runs again
after a retry, a sleep, an event, or a worker that got `kill -9`ed, a step that
already finished returns its stored value instead of executing. The test suite
does exactly that: it kills a worker after step 2 of 5, starts a fresh process,
and checks that only steps 3 and 4 run.

## How it works

Absurd keeps five tables per queue (tasks, runs, checkpoints, events, waits) and
does all of its work in stored procedures: `spawn_task`, `claim_task`,
`complete_run`, `fail_run`, `await_event`, `emit_event` and so on. Unhinged keeps
the same five tables with the same columns, and each procedure is a method on
`Engine` with the same name and arguments. The plpgsql became Python. The SQL
mostly came across as it was.

What Postgres gave Absurd for free was row locking. `FOR UPDATE SKIP LOCKED`
lets many workers claim from one queue at the same time. SQLite has one writer
at a time, so each procedure runs inside a single `BEGIN IMMEDIATE` transaction
and there is nothing to skip. Workers poll, the way Absurd's do. Several worker
processes can share one database file through WAL mode, and threads inside a
worker each get their own connection.

Every commit is fsynced. A checkpoint that was acknowledged survives a power
cut, which is what Postgres guarantees and what a step like "charge the card"
needs. `Unhinged(..., synchronous="normal")` is about 40% faster and still
survives a killed process, but can lose the last few commits if the machine
goes down.

## Run it

```bash
uv venv && uv pip install -e '.[dev]'
pytest
python examples/crash_demo.py
python examples/bench.py --tasks 2000 --workers 4
```

The crash demo spawns a five-step task, runs a worker that kills itself after
step 2, waits for the lease to expire, and starts a second worker that finishes
the other three. The bench drains a few thousand three-step tasks across worker
processes. On a laptop it lands around 3,000 tasks a second, and more workers
don't raise that because SQLite serialises the writes. Enough for one machine.

The quickstart is two terminals:

```bash
python examples/quickstart_worker.py
python examples/quickstart_client.py alice --await
python -m unhinged --db quickstart.db emit user-activated:alice '{"activated_at":"now"}'
```

`python -m unhinged` is a small `absurdctl`: create and drop queues, spawn,
inspect a task and its runs, cancel, retry, emit, clean up.

Two fuller apps live in `examples/`, each with its own README:

- [`pipeline/`](examples/pipeline/) turns every file in a directory into a
  three-step task. Ctrl-C the worker and start it again; finished steps stay
  finished. `--flaky` injects failures so you can watch the retries.
- [`agent/`](examples/agent/) is an agent loop where every model call and
  tool call is a step. The checkpoints are the transcript. It can ask a
  question and park for a week, and a restarted worker replays the stored
  turns without calling the model again. Runs on Claude, or on a scripted
  model with no network.

## What's not here

Partitioned queues and the pg_cron jobs that maintain them are Postgres features
and stayed behind. Nothing deletes finished tasks unless you ask: pass
`maintenance_interval` to `start_worker`, call `app.cleanup()`, or run
`python -m unhinged cleanup`. Retention is per queue through `set_queue_policy`,
30 days by default.

Only the synchronous SDK was ported. Absurd's async client and hooks were not.

Two places the semantics differ on purpose. The clock is read once per
transaction rather than per statement, so a lease that expires inside
`claim_task` is handed out again in the same call. And cancellation policies
are checked in Python, because SQLite's `julianday` arithmetic is not exact to
the microsecond.

Needs Python 3.10+ and SQLite 3.33 or newer.

## License

Apache-2.0, same as Absurd. `NOTICE` has the attribution.
