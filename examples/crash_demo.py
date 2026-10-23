"""Kill a worker mid-task with SIGKILL, start another, watch it resume.

    python examples/crash_demo.py

Spawns one five-step task, runs a worker subprocess that hard-crashes after
step 2, waits for the lease to expire, then runs a second worker. The second
worker skips the steps already checkpointed and finishes the rest.
"""

import os
import subprocess
import sys
import tempfile
import textwrap
import time

from unhinged import Unhinged

WORKER = textwrap.dedent(
    """
    import os, sys, time
    from unhinged import Unhinged

    db, crash_after = sys.argv[1], int(sys.argv[2])
    app = Unhinged(db, queue_name="demo")

    @app.register_task("pipeline")
    def pipeline(params, ctx):
        out = []
        for i in range(5):
            def work(i=i):
                print(f"  pid {os.getpid()} running step-{i}", flush=True)
                time.sleep(0.2)
                return i * i
            out.append(ctx.step(f"step-{i}", work))
            if i == crash_after:
                print(f"  pid {os.getpid()} kill -9 myself after step-{i}", flush=True)
                os.kill(os.getpid(), 9)
        return out

    app.start_worker(claim_timeout=2, poll_interval=0.05, stop_when_idle=True)
    """
)


def main() -> None:
    tmp = tempfile.mkdtemp()
    db = os.path.join(tmp, "crash.db")
    script = os.path.join(tmp, "worker.py")
    with open(script, "w") as f:
        f.write(WORKER)
    env = {**os.environ, "PYTHONPATH": os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}

    app = Unhinged(db, queue_name="demo")
    task = app.spawn("pipeline", None, queue="demo")
    print(f"spawned task {task['task_id']}")

    print("\n== worker 1 (will crash after step-2) ==")
    subprocess.run([sys.executable, script, db, "2"], env=env)
    print(f"worker 1 exit; task state is {app.fetch_task_result(task['task_id']).state!r}, lease held by a dead process")

    print("\n.. waiting 2s for the lease to expire ..")
    time.sleep(2.2)

    print("\n== worker 2 ==")
    while app.fetch_task_result(task["task_id"]).state != "completed":
        subprocess.run([sys.executable, script, db, "99"], env=env)

    snap = app.fetch_task_result(task["task_id"])
    print(f"\nresult: {snap.result}")
    for run in app.engine.get_runs("demo", task["task_id"]):
        reason = (run["failure_reason"] or {}).get("name")
        print(f"attempt {run['attempt']}: {run['state']:<9} claimed_by={run['claimed_by']} {reason or ''}")
    print(f"\ninspect: python -m unhinged --db {db} -q demo task {task['task_id']}")
    app.close()


if __name__ == "__main__":
    main()
