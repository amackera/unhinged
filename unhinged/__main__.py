"""A small ``absurdctl``: ``python -m unhinged <command>``."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Any

from .engine import Engine, UnhingedError
from .schema import table
from .sdk import Unhinged


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


def _print(value: Any) -> None:
    print(json.dumps(_jsonable(value), indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="unhinged", description="Inspect and drive an Unhinged database.")
    p.add_argument("--db", default=None, help="database path (default: $UNHINGED_DATABASE or unhinged.db)")
    p.add_argument("-q", "--queue", default="default")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("create-queue")
    sub.add_parser("drop-queue")
    sub.add_parser("list-queues")
    sub.add_parser("stats", help="task counts by state")
    sub.add_parser("version")

    s = sub.add_parser("spawn", help="enqueue a task")
    s.add_argument("task_name")
    s.add_argument("params", nargs="?", default="null", help="JSON")
    s.add_argument("--max-attempts", type=int)
    s.add_argument("--idempotency-key")
    s.add_argument("--retry", help='JSON, e.g. \'{"kind":"exponential","base_seconds":2}\'')

    s = sub.add_parser("tasks", help="list tasks")
    s.add_argument("--state")
    s.add_argument("--limit", type=int, default=20)

    s = sub.add_parser("task", help="show a task, its runs and checkpoints")
    s.add_argument("task_id")

    s = sub.add_parser("result", help="show a task's result snapshot")
    s.add_argument("task_id")

    s = sub.add_parser("cancel")
    s.add_argument("task_id")

    s = sub.add_parser("retry", help="retry a failed task")
    s.add_argument("task_id")
    s.add_argument("--spawn-new", action="store_true")
    s.add_argument("--max-attempts", type=int)

    s = sub.add_parser("emit", help="emit an event")
    s.add_argument("event_name")
    s.add_argument("payload", nargs="?", default="null", help="JSON")

    s = sub.add_parser("cleanup", help="apply each queue's cleanup policy")
    s.add_argument("--all", action="store_true", help="all queues, not just --queue")

    s = sub.add_parser("policy", help="show or set the queue's cleanup policy")
    s.add_argument("--cleanup-ttl", help="seconds or e.g. '7 days'")
    s.add_argument("--cleanup-limit", type=int)

    args = p.parse_args(argv)
    app = Unhinged(args.db, queue_name=args.queue)
    eng: Engine = app.engine
    q = args.queue
    try:
        if args.cmd == "create-queue":
            eng.create_queue(q)
        elif args.cmd == "drop-queue":
            eng.drop_queue(q)
        elif args.cmd == "list-queues":
            _print(eng.list_queues())
        elif args.cmd == "stats":
            _print(eng.queue_stats(q))
        elif args.cmd == "version":
            print(eng.get_schema_version())
        elif args.cmd == "spawn":
            _print(
                app.spawn(
                    args.task_name,
                    json.loads(args.params),
                    queue=q,
                    max_attempts=args.max_attempts,
                    idempotency_key=args.idempotency_key,
                    retry_strategy=json.loads(args.retry) if args.retry else None,
                )
            )
        elif args.cmd == "tasks":
            _print(eng.list_tasks(q, args.state, args.limit))
        elif args.cmd == "task":
            task = eng.get_task(q, args.task_id)
            if task is None:
                raise UnhingedError(f"task {args.task_id} not found")
            runs = eng.get_runs(q, args.task_id)
            cps = eng.connection.execute(
                f"select checkpoint_name, state, owner_run_id, updated_at from {table('c', q)} "
                "where task_id = ? order by updated_at",
                (args.task_id,),
            ).fetchall()
            _print({"task": task, "runs": runs, "checkpoints": [dict(c) for c in cps]})
        elif args.cmd == "result":
            _print(eng.get_task_result(q, args.task_id))
        elif args.cmd == "cancel":
            eng.cancel_task(q, args.task_id)
        elif args.cmd == "retry":
            _print(app.retry_task(args.task_id, q, spawn_new=args.spawn_new, max_attempts=args.max_attempts))
        elif args.cmd == "emit":
            eng.emit_event(q, args.event_name, json.loads(args.payload))
        elif args.cmd == "cleanup":
            _print(eng.cleanup_all_queues(None if args.all else q))
        elif args.cmd == "policy":
            policy = {}
            if args.cleanup_ttl is not None:
                policy["cleanup_ttl"] = args.cleanup_ttl
            if args.cleanup_limit is not None:
                policy["cleanup_limit"] = args.cleanup_limit
            if policy:
                eng.set_queue_policy(q, policy)
            _print(eng.get_queue_policy(q))
    except UnhingedError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    finally:
        app.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
