"""Throughput check: N three-step tasks drained by K worker processes.

    python examples/bench.py --tasks 2000 --workers 4
"""

import argparse
import os
import subprocess
import sys
import tempfile
import textwrap
import time

from unhinged import Unhinged

WORKER = textwrap.dedent(
    """
    import sys
    from unhinged import Unhinged
    app = Unhinged(sys.argv[1], queue_name="bench")

    @app.register_task("three-step")
    def three_step(params, ctx):
        a = ctx.step("a", lambda: params["n"] + 1)
        b = ctx.step("b", lambda: a * 2)
        c = ctx.step("c", lambda: b - 1)
        return c

    app.start_worker(worker_id=sys.argv[2], claim_timeout=30, batch_size=int(sys.argv[3]),
                     poll_interval=0.01, stop_when_idle=True)
    """
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tasks", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch", type=int, default=10)
    args = ap.parse_args()

    tmp = tempfile.mkdtemp()
    db = os.path.join(tmp, "bench.db")
    script = os.path.join(tmp, "worker.py")
    with open(script, "w") as f:
        f.write(WORKER)
    env = {**os.environ, "PYTHONPATH": os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}

    app = Unhinged(db, queue_name="bench")
    t0 = time.perf_counter()
    with app.engine.transaction():
        for i in range(args.tasks):
            app.spawn("three-step", {"n": i}, queue="bench")
    spawn_s = time.perf_counter() - t0
    print(f"spawned {args.tasks} tasks in {spawn_s:.2f}s ({args.tasks / spawn_s:,.0f}/s, one transaction)")

    t0 = time.perf_counter()
    procs = [
        subprocess.Popen([sys.executable, script, db, f"w{i}", str(args.batch)], env=env)
        for i in range(args.workers)
    ]
    for p in procs:
        p.wait()
    run_s = time.perf_counter() - t0

    stats = app.engine.queue_stats("bench")
    assert stats["completed"] == args.tasks, stats
    print(
        f"{args.workers} worker processes drained {args.tasks} tasks "
        f"({args.tasks * 3} checkpoints) in {run_s:.2f}s: {args.tasks / run_s:,.0f} tasks/s"
    )
    print(f"db size: {os.path.getsize(db) / 1024:,.0f} KiB")
    app.close()


if __name__ == "__main__":
    main()
