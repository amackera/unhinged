"""Kill workers at random for a while and check that nothing was lost.

    python examples/chaos.py --workers 4 --tasks 200 --seconds 20

Several worker processes share one database file. A supervisor SIGKILLs a
random one every few hundred milliseconds and starts a replacement. Every
step records that it ran (task, step, run, pid) in a side table, so at the
end we can check the things the README claims:

* every task completed
* no run was ever executed by two processes
* no step ran twice inside one run
* a step ran twice across runs only when the earlier run died mid-step
  (its lease expired and was reclaimed), which is the documented
  at-least-once window
* no worker ever saw "database is locked" escape the engine
* the database passes integrity_check and the WAL did not run away
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import random
import signal
import subprocess
import sys
import tempfile
import textwrap
import time

from unhinged import Engine, Unhinged

QUEUE = "chaos"

WORKER = textwrap.dedent(
    """
    import os, sys, time, traceback
    from unhinged import Unhinged

    db, worker_id, steps, step_ms, errors_path = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4]), sys.argv[5]
    app = Unhinged(db, queue_name="chaos")
    conn = app.engine.connection  # autocommit; the effect is its own transaction

    @app.register_task("chaos", default_max_attempts=1000)
    def chaos(params, ctx):
        for i in range(steps):
            def effect(i=i):
                time.sleep(step_ms / 1000)
                conn.execute(
                    "insert into effects (task_id, step, run_id, pid) values (?, ?, ?, ?)",
                    (ctx.task_id, i, ctx.run_id, os.getpid()),
                )
                return i
            ctx.step(f"s{i}", effect)
        return "ok"

    try:
        app.start_worker(worker_id=worker_id, claim_timeout=2, poll_interval=0.01, batch_size=1)
    except Exception:
        with open(errors_path, "a") as f:
            f.write(f"{worker_id} pid {os.getpid()}\\n{traceback.format_exc()}\\n")
        raise
    """
)


def run(
    db: str,
    *,
    workers: int = 4,
    tasks: int = 200,
    steps: int = 5,
    step_ms: float = 10.0,
    seconds: float = 20.0,
    kill_every: float = 0.15,
    timeout: float = 180.0,
    seed: int = 1,
    quiet: bool = False,
) -> dict:
    rng = random.Random(seed)
    tmp = tempfile.mkdtemp()
    script = os.path.join(tmp, "worker.py")
    errors_path = os.path.join(tmp, "errors.log")
    with open(script, "w") as f:
        f.write(WORKER)
    env = {**os.environ, "PYTHONPATH": os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}

    app = Unhinged(db, queue_name=QUEUE)
    app.engine.connection.execute(
        "create table if not exists effects (task_id text, step integer, run_id text, pid integer)"
    )
    with app.engine.transaction():
        for i in range(tasks):
            app.spawn("chaos", {"i": i}, queue=QUEUE, retry_strategy={"kind": "fixed", "base_seconds": 0})

    def start(idx: int) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, script, db, f"w{idx}", str(steps), str(step_ms), errors_path],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    procs = {i: start(i) for i in range(workers)}
    kills = 0
    killed_pids = set()
    t0 = time.monotonic()
    next_kill = t0 + kill_every
    stats = app.engine.queue_stats(QUEUE)
    while stats["completed"] < tasks:
        now = time.monotonic()
        if now - t0 > timeout:
            break
        if now - t0 < seconds and now >= next_kill:
            idx = rng.choice(list(procs))
            p = procs[idx]
            if p.poll() is None:
                killed_pids.add(p.pid)
                p.send_signal(signal.SIGKILL)
                p.wait()
                kills += 1
            procs[idx] = start(idx)
            next_kill = now + kill_every
        for idx, p in list(procs.items()):
            if p.poll() is not None and p.pid not in killed_pids:
                procs[idx] = start(idx)  # it died on its own; errors.log will say why
        time.sleep(0.05)
        stats = app.engine.queue_stats(QUEUE)
    elapsed = time.monotonic() - t0

    for p in procs.values():
        if p.poll() is None:
            p.send_signal(signal.SIGKILL)
            p.wait()

    return _analyse(app.engine, tasks, steps, kills, elapsed, errors_path, db, quiet) | {"kills": kills}


def _analyse(engine: Engine, tasks: int, steps: int, kills: int, elapsed: float, errors_path: str, db: str, quiet: bool) -> dict:
    conn = engine.connection
    stats = engine.queue_stats(QUEUE)
    effects = conn.execute("select task_id, step, run_id, pid from effects").fetchall()

    pids_per_run: dict[str, set] = collections.defaultdict(set)
    per_run_step: collections.Counter = collections.Counter()
    per_task_step: dict[tuple, list] = collections.defaultdict(list)
    for e in effects:
        pids_per_run[e["run_id"]].add(e["pid"])
        per_run_step[(e["run_id"], e["step"])] += 1
        per_task_step[(e["task_id"], e["step"])].append(e["run_id"])

    runs = {r["run_id"]: r for r in conn.execute(f'select run_id, state, failure_reason from "r_{QUEUE}"').fetchall()}

    def timed_out(run_id: str) -> bool:
        r = runs.get(run_id)
        if r is None or r["state"] != "failed" or r["failure_reason"] is None:
            return False
        return json.loads(r["failure_reason"]).get("name") == "$ClaimTimeout"

    shared_runs = [rid for rid, pids in pids_per_run.items() if len(pids) > 1]
    double_in_run = [k for k, n in per_run_step.items() if n > 1]
    duplicates = 0
    unexplained = []
    for key, run_ids in per_task_step.items():
        if len(run_ids) > 1:
            duplicates += len(run_ids) - 1
            earlier = run_ids[:-1]
            if not all(timed_out(rid) for rid in earlier):
                unexplained.append((key, run_ids))

    errors = open(errors_path).read() if os.path.exists(errors_path) else ""
    integrity = conn.execute("pragma integrity_check").fetchone()[0]
    wal_bytes = os.path.getsize(db + "-wal") if os.path.exists(db + "-wal") else 0
    missing = [
        (t, s) for t in (r["task_id"] for r in conn.execute(f'select task_id from "t_{QUEUE}"').fetchall())
        for s in range(steps) if (t, s) not in per_task_step
    ]

    report = {
        "tasks": tasks,
        "completed": stats["completed"],
        "kills": kills,
        "elapsed_s": round(elapsed, 1),
        "step_executions": len(effects),
        "steps_expected": tasks * steps,
        "duplicate_executions": duplicates,
        "runs_timed_out": sum(1 for r in runs.values() if timed_out(r["run_id"])),
        "runs_total": len(runs),
        "shared_runs": shared_runs,
        "double_in_run": double_in_run,
        "unexplained_duplicates": unexplained,
        "missing_steps": missing,
        "worker_errors": errors,
        "integrity": integrity,
        "wal_bytes": wal_bytes,
    }
    report["ok"] = (
        stats["completed"] == tasks
        and not shared_runs
        and not double_in_run
        and not unexplained
        and not missing
        and not errors
        and integrity == "ok"
    )
    if not quiet:
        print(
            f"{stats['completed']}/{tasks} tasks completed in {elapsed:.1f}s with {kills} kills; "
            f"{len(effects)} step executions for {tasks * steps} steps "
            f"({duplicates} re-ran after a reclaimed lease, {len(runs)} runs, "
            f"{report['runs_timed_out']} lease timeouts)"
        )
        for name in ("shared_runs", "double_in_run", "unexplained_duplicates", "missing_steps"):
            if report[name]:
                print(f"  {name}: {report[name][:5]}")
        if errors:
            print(f"  worker errors:\n{errors}")
        print(f"  integrity_check={integrity} wal={wal_bytes // 1024} KiB -> {'OK' if report['ok'] else 'FAILED'}")
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--tasks", type=int, default=200)
    ap.add_argument("--steps", type=int, default=5)
    ap.add_argument("--step-ms", type=float, default=10.0)
    ap.add_argument("--seconds", type=float, default=20.0, help="how long to keep killing workers")
    ap.add_argument("--kill-every", type=float, default=0.15)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    db = os.path.join(tempfile.mkdtemp(), "chaos.db")
    report = run(
        db, workers=args.workers, tasks=args.tasks, steps=args.steps, step_ms=args.step_ms,
        seconds=args.seconds, kill_every=args.kill_every, seed=args.seed,
    )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
