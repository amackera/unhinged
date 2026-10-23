# Unhinged

[Absurd](https://github.com/earendil-works/absurd) is Armin Ronacher's durable
execution engine for Postgres. Unhinged is the same engine on SQLite. No
server, no dependencies, one file on disk.

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

Steps run once. If the worker crashes, sleeps, or waits for an event, the
next run skips the steps that already finished.

## How it works

Same tables as Absurd, same procedure names. The stored procedures are
Python methods on `Engine`, each one a `BEGIN IMMEDIATE` transaction.
Workers poll. Every commit is fsynced.

## Run it

```bash
uv venv && uv pip install -e '.[dev]'
pytest
python examples/crash_demo.py
```

`examples/pipeline/` is a resumable batch job. `examples/agent/` is an agent
loop where every model call is a step. `python -m unhinged` is the CLI.

## What's not here

Partitioned queues, pg_cron, and the async SDK. Nothing deletes old tasks
unless you call `app.cleanup()` or pass `maintenance_interval` to
`start_worker`.

## License

Apache-2.0, same as Absurd.
