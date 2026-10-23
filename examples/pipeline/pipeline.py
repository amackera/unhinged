"""A resumable batch pipeline on Unhinged.

Every file in a directory becomes a task. Each task runs three steps
(checksum, analyze, write a sidecar JSON) and each step is checkpointed, so
you can Ctrl-C the worker, or kill -9 it, and start it again: finished steps
are skipped, half-finished tasks pick up at the step they died in.

    python examples/pipeline/pipeline.py generate ./corpus --count 200
    python examples/pipeline/pipeline.py enqueue ./corpus
    python examples/pipeline/pipeline.py work --concurrency 4 --flaky 0.2
    python examples/pipeline/pipeline.py status
    python examples/pipeline/pipeline.py report

Enqueueing uses the file path as an idempotency key, so running `enqueue`
twice does not process anything twice. `--flaky` makes the analyze step fail
at random so you can watch retries with exponential backoff do their job.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import random
import re
import signal
import sys
import time

from unhinged import Unhinged

QUEUE = "pipeline"
DEFAULT_DB = "pipeline.db"
WORDS = "durable sqlite absurd postgres checkpoint worker queue lease retry event sleep step task run".split()


def make_app(db: str) -> Unhinged:
    return Unhinged(db, queue_name=QUEUE)


def register(app: Unhinged, out_dir: str, flaky: float = 0.0) -> None:
    @app.register_task("process-file", default_max_attempts=6)
    def process_file(params, ctx):
        path = params["path"]

        @ctx.run_step("checksum")
        def checksum():
            h = hashlib.sha256()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 16), b""):
                    h.update(chunk)
            return h.hexdigest()

        @ctx.run_step("analyze")
        def analyze():
            if flaky and random.random() < flaky:
                raise RuntimeError("simulated flaky failure in analyze")
            with open(path, encoding="utf-8", errors="replace") as f:
                text = f.read()
            words = re.findall(r"[a-zA-Z']+", text.lower())
            top = collections.Counter(words).most_common(3)
            return {"lines": text.count("\n"), "words": len(words), "top": top}

        @ctx.run_step("write-sidecar")
        def write_sidecar():
            os.makedirs(out_dir, exist_ok=True)
            sidecar = os.path.join(out_dir, os.path.basename(path) + ".json")
            with open(sidecar, "w") as f:
                json.dump({"path": path, "sha256": checksum, **analyze}, f, indent=2)
            return sidecar

        return {"path": path, "sha256": checksum, "words": analyze["words"], "sidecar": write_sidecar}


# -- commands ----------------------------------------------------------------


def cmd_generate(args) -> None:
    os.makedirs(args.dir, exist_ok=True)
    rng = random.Random(args.seed)
    for i in range(args.count):
        n = rng.randint(50, 2000)
        text = "\n".join(" ".join(rng.choice(WORDS) for _ in range(rng.randint(5, 15))) for _ in range(n // 10))
        with open(os.path.join(args.dir, f"doc-{i:04d}.txt"), "w") as f:
            f.write(text + "\n")
    print(f"wrote {args.count} files to {args.dir}")


def cmd_enqueue(args) -> None:
    app = make_app(args.db)
    created = skipped = 0
    for name in sorted(os.listdir(args.dir)):
        path = os.path.abspath(os.path.join(args.dir, name))
        if not os.path.isfile(path):
            continue
        # This process has no handler registered, so the retry policy is
        # spelled out here rather than inherited from register_task.
        spawned = app.spawn(
            "process-file",
            {"path": path},
            queue=QUEUE,
            idempotency_key=path,
            max_attempts=6,
            retry_strategy={"kind": "exponential", "base_seconds": 0.5, "factor": 2, "max_seconds": 30},
        )
        created += spawned["created"]
        skipped += not spawned["created"]
    print(f"enqueued {created} new tasks, {skipped} already queued")
    app.close()


def cmd_work(args) -> None:
    app = make_app(args.db)
    register(app, args.out, args.flaky)
    signal.signal(signal.SIGINT, lambda *_: app.stop_worker())
    started = time.monotonic()
    before = app.engine.queue_stats(QUEUE)["completed"]
    print(f"worker {os.getpid()} on {args.db}; Ctrl-C to stop (progress is kept)")
    app.start_worker(claim_timeout=30, concurrency=args.concurrency, poll_interval=0.05, stop_when_idle=args.once)
    done = app.engine.queue_stats(QUEUE)["completed"] - before
    print(f"stopped after {time.monotonic() - started:.1f}s, {done} tasks completed in this session")
    app.close()


def cmd_status(args) -> None:
    app = make_app(args.db)
    stats = app.engine.queue_stats(QUEUE)
    for state in ("pending", "running", "sleeping", "completed", "failed", "cancelled"):
        print(f"{state:>10} {stats[state]}")
    failed = app.engine.list_tasks(QUEUE, state="failed", limit=10)
    for task in failed:
        snap = app.fetch_task_result(task["task_id"])
        reason = (snap.failure or {}).get("message", "?") if snap else "?"
        print(f"  failed {task['task_id']} after {task['attempts']} attempts: {reason}")
    app.close()


def cmd_report(args) -> None:
    app = make_app(args.db)
    rows = app.engine.list_tasks(QUEUE, state="completed", limit=10**9)
    total_words = sum(t["completed_payload"]["words"] for t in rows)
    biggest = max(rows, key=lambda t: t["completed_payload"]["words"], default=None)
    print(f"{len(rows)} files, {total_words:,} words")
    if biggest:
        print(f"largest: {os.path.basename(biggest['completed_payload']['path'])} ({biggest['completed_payload']['words']} words)")
    app.close()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", default=DEFAULT_DB)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("generate", help="write sample text files")
    s.add_argument("dir")
    s.add_argument("--count", type=int, default=100)
    s.add_argument("--seed", type=int, default=1)
    s.set_defaults(fn=cmd_generate)

    s = sub.add_parser("enqueue", help="one task per file; safe to re-run")
    s.add_argument("dir")
    s.set_defaults(fn=cmd_enqueue)

    s = sub.add_parser("work", help="process tasks until Ctrl-C")
    s.add_argument("--out", default="pipeline-out")
    s.add_argument("--concurrency", type=int, default=4)
    s.add_argument("--flaky", type=float, default=0.0, help="probability the analyze step fails")
    s.add_argument("--once", action="store_true", help="exit when the queue is drained")
    s.set_defaults(fn=cmd_work)

    s = sub.add_parser("status", help="task counts and recent failures")
    s.set_defaults(fn=cmd_status)

    s = sub.add_parser("report", help="aggregate the completed results")
    s.set_defaults(fn=cmd_report)

    args = p.parse_args(argv)
    args.fn(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
