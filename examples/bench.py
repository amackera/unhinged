"""An honest benchmark for Unhinged.

    python examples/bench.py                 # everything, about two minutes
    python examples/bench.py --quick         # smaller runs
    python examples/bench.py --json out.json # also write the raw numbers

What it measures, in order:

1. Calibration. How long one fsync takes on this disk, and how many
   single-row SQLite commits a second this machine can do at each
   durability setting. Every later number is reported against these.
2. Commits per task. Counted with a trace callback, not assumed.
3. Throughput. A fixed three-step no-op task, drained by one worker
   process, at each durability setting. Burn-down mode: all tasks are
   spawned first, so the spawner never competes with the worker.
4. Worker scaling. Tasks per second against 1, 2, 4 and 8 processes, for
   no-op steps (plateaus: one writer) and for 10 ms steps (scales: the
   writer is idle most of the time).
5. Recovery. Kill -9 a worker with tasks mid-step, start another, and
   time how long until work resumes and how many steps ran twice.
6. Storage and cleanup. Bytes per completed task, and how long cleanup()
   takes.

Durability classes, which the tables label on every row:
  crash-safe  survives the process or the OS dying; may lose the last few
              commits on power loss
  power-safe  survives power loss, assuming the drive honours a flush
On macOS, synchronous=FULL is only crash-safe unless fullfsync is on; on
Linux fsync flushes the drive cache and FULL is power-safe.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import textwrap
import time

from unhinged import Engine, Unhinged

QUEUE = "bench"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IS_MAC = sys.platform == "darwin"

WORKER = textwrap.dedent(
    """
    import os, sys, time
    from unhinged import Unhinged

    db, worker_id, batch, sync, ff, step_ms, record = sys.argv[1:8]
    app = Unhinged(db, queue_name="bench", synchronous=sync, fullfsync=ff == "1")
    step_s = float(step_ms) / 1000
    conn = app.engine.connection

    @app.register_task("three-step", default_max_attempts=1000)
    def three_step(params, ctx):
        def work(i):
            if step_s:
                time.sleep(step_s)
            if record == "1":
                conn.execute("insert into effects values (?, ?, ?, ?, ?)",
                             (ctx.task_id, i, ctx.run_id, os.getpid(), time.time()))
            return params["n"] + i
        a = ctx.step("a", lambda: work(0))
        b = ctx.step("b", lambda: work(1))
        return ctx.step("c", lambda: work(2))

    app.start_worker(worker_id=worker_id, claim_timeout=int(sys.argv[8]), batch_size=int(batch),
                     poll_interval=0.005, stop_when_idle=sys.argv[9] == "1")
    """
)


# ---------------------------------------------------------------- helpers


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def fresh_db(tmp: str, name: str) -> str:
    path = os.path.join(tmp, f"{name}.db")
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(path + suffix)
        except FileNotFoundError:
            pass
    return path


def db_bytes(path: str) -> int:
    return sum(os.path.getsize(path + s) for s in ("", "-wal", "-shm") if os.path.exists(path + s))


FS_IN_MEMORY = False


def durability_class(sync: str, ff: bool) -> str:
    if FS_IN_MEMORY:
        return "none (tmpfs)"
    if sync == "normal":
        return "crash-safe"
    if IS_MAC and not ff:
        return "crash-safe"
    return "power-safe"


def label(sync: str, ff: bool) -> str:
    return f"{sync.upper()}{'+fullfsync' if ff else ''}"


def git_commit() -> str:
    try:
        return subprocess.check_output(["git", "-C", ROOT, "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def cpu_model() -> str:
    try:
        if IS_MAC:
            return subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return platform.processor() or platform.machine()


def filesystem(path: str) -> str:
    try:
        if IS_MAC:
            info = subprocess.check_output(["diskutil", "info", path], text=True)
            for line in info.splitlines():
                if "File System Personality" in line:
                    return line.split(":", 1)[1].strip()
            return "unknown"
        out = subprocess.check_output(["df", "-T", path], text=True).splitlines()[-1]
        return out.split()[1]
    except Exception:
        return "unknown"


IN_MEMORY_FS = ("tmpfs", "ramfs", "devtmpfs")


def check_filesystem(fs: str) -> None:
    if fs.lower() in IN_MEMORY_FS:
        print(
            f"\n!! {fs} is an in-memory filesystem: fsync is free and nothing here survives a reboot.\n"
            "!! Every durability label below is void. Pass --dir with a path on a real disk.\n",
            file=sys.stderr,
        )


# ---------------------------------------------------------------- 1. calibration


def calibrate(tmp: str, n: int) -> dict:
    out: dict = {"raw": {}, "sqlite": {}}
    buf = os.urandom(4096)
    modes = ["fsync"]
    if IS_MAC:
        modes.append("F_FULLFSYNC")
    else:
        modes.append("fdatasync")
    for mode in modes:
        fd = os.open(os.path.join(tmp, "raw.bin"), os.O_RDWR | os.O_CREAT, 0o644)
        lat = []
        for _ in range(n):
            os.pwrite(fd, buf, 0)
            t = time.perf_counter()
            if mode == "fsync":
                os.fsync(fd)
            elif mode == "fdatasync":
                os.fdatasync(fd)
            else:
                import fcntl

                fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
            lat.append((time.perf_counter() - t) * 1000)
        os.close(fd)
        out["raw"][mode] = {"median_ms": statistics.median(lat), "p99_ms": pct(lat, 0.99)}

    for sync, ff in (("normal", False), ("full", False), ("full", True)):
        path = fresh_db(tmp, f"cal_{sync}_{int(ff)}")
        c = sqlite3.connect(path, isolation_level=None)
        c.execute("pragma journal_mode = wal")
        c.execute(f"pragma synchronous = {sync}")
        c.execute(f"pragma fullfsync = {'on' if ff else 'off'}")
        c.execute("create table t (id integer primary key, body text)")
        lat = []
        for _ in range(n):
            t = time.perf_counter()
            c.execute("begin immediate")
            c.execute("insert into t (body) values (?)", ("x" * 200,))
            c.execute("commit")
            lat.append((time.perf_counter() - t) * 1000)
        c.close()
        out["sqlite"][label(sync, ff)] = {
            "median_ms": statistics.median(lat),
            "p99_ms": pct(lat, 0.99),
            "commits_per_s": 1000 / statistics.median(lat),
            "class": durability_class(sync, ff),
        }
    return out


# ---------------------------------------------------------------- 2. commits per task


def commits_per_task(tmp: str, tasks: int) -> dict:
    path = fresh_db(tmp, "count")
    app = Unhinged(path, queue_name=QUEUE, synchronous="normal")
    counts = {"begin": 0, "commit": 0}

    def trace(stmt: str) -> None:
        s = stmt.lstrip().lower()
        if s.startswith("begin"):
            counts["begin"] += 1
        elif s.startswith("commit"):
            counts["commit"] += 1

    app.engine.connection.set_trace_callback(trace)

    @app.register_task("three-step")
    def three_step(params, ctx):
        a = ctx.step("a", lambda: params["n"] + 1)
        b = ctx.step("b", lambda: a * 2)
        return ctx.step("c", lambda: b - 1)

    with app.engine.transaction():
        for i in range(tasks):
            app.spawn("three-step", {"n": i})
    spawn_commits = counts["commit"]
    for batch in (1, 10):
        counts["commit"] = 0
        app.start_worker(worker_id="count", claim_timeout=30, batch_size=batch, poll_interval=0.001, stop_when_idle=True)
        if batch == 1:
            per_task_b1 = counts["commit"] / tasks
            # re-spawn for the second pass
            with app.engine.transaction():
                for i in range(tasks):
                    app.spawn("three-step", {"n": i})
        else:
            per_task_b10 = counts["commit"] / tasks
    app.close()
    return {"spawn_batch_commits": spawn_commits, "per_task_batch1": per_task_b1, "per_task_batch10": per_task_b10}


# ---------------------------------------------------------------- 3/4. throughput


def spawn_all(path: str, tasks: int, sync: str, ff: bool) -> None:
    app = Unhinged(path, queue_name=QUEUE, synchronous=sync, fullfsync=ff)
    app.engine.connection.execute(
        "create table if not exists effects (task_id text, step integer, run_id text, pid integer, t real)"
    )
    with app.engine.transaction():
        for i in range(tasks):
            app.spawn("three-step", {"n": i}, queue=QUEUE, retry_strategy={"kind": "fixed", "base_seconds": 0})
    app.close()


def start_worker(path: str, idx: int, batch: int, sync: str, ff: bool, step_ms: float, record: bool,
                 claim_timeout: int = 30, stop_when_idle: bool = True, script: str = "") -> subprocess.Popen:
    env = {**os.environ, "PYTHONPATH": ROOT}
    return subprocess.Popen(
        [sys.executable, script, path, f"w{idx}", str(batch), sync, "1" if ff else "0", str(step_ms),
         "1" if record else "0", str(claim_timeout), "1" if stop_when_idle else "0"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def drain(tmp: str, script: str, tasks: int, workers: int, sync: str, ff: bool, step_ms: float = 0, batch: int = 10) -> dict:
    path = fresh_db(tmp, f"tp_{sync}_{int(ff)}_{workers}_{int(step_ms)}")
    spawn_all(path, tasks, sync, ff)
    t0 = time.perf_counter()
    procs = [start_worker(path, i, batch, sync, ff, step_ms, False, script=script) for i in range(workers)]
    for p in procs:
        p.wait()
    dt = time.perf_counter() - t0
    stats = Engine(path).queue_stats(QUEUE)
    assert stats["completed"] == tasks, stats
    return {"tasks_per_s": tasks / dt, "seconds": dt, "bytes": db_bytes(path), "path": path}


def throughput(tmp: str, script: str, tasks: int, ff_tasks: int, runs: int, cpt: float, cal: dict) -> list[dict]:
    rows = []
    for sync, ff in (("normal", False), ("full", False), ("full", True)):
        n = ff_tasks if ff else tasks
        samples = []
        for r in range(runs + 1):
            res = drain(tmp, script, n, 1, sync, ff)
            if r > 0:  # first run is warm-up
                samples.append(res["tasks_per_s"])
        med = statistics.median(samples)
        ceiling = cal["sqlite"][label(sync, ff)]["commits_per_s"]
        rows.append({
            "setting": label(sync, ff), "class": durability_class(sync, ff), "tasks": n,
            "tasks_per_s": med, "min": min(samples), "max": max(samples),
            "commits_per_s": med * cpt, "ceiling_commits_per_s": ceiling, "pct_of_ceiling": 100 * med * cpt / ceiling,
        })
    return rows


def scaling(tmp: str, script: str, tasks: int, sleep_tasks: int, worker_counts: list[int]) -> dict:
    out = {"noop": [], "sleep10ms": []}
    for w in worker_counts:
        out["noop"].append({"workers": w, "tasks_per_s": drain(tmp, script, tasks, w, "full", False)["tasks_per_s"]})
        out["sleep10ms"].append({"workers": w, "tasks_per_s": drain(tmp, script, sleep_tasks, w, "full", False, step_ms=10)["tasks_per_s"]})
    return out


# ---------------------------------------------------------------- 5. recovery


def recovery(tmp: str, script: str, tasks: int, claim_timeout: int) -> dict:
    path = fresh_db(tmp, "recovery")
    step_ms = 20
    spawn_all(path, tasks, "full", False)
    victim = start_worker(path, 0, 1, "full", False, step_ms, True, claim_timeout=claim_timeout, stop_when_idle=False, script=script)
    time.sleep(0.6)  # let it get mid-way through the first tasks
    kill_t = time.time()
    victim.send_signal(signal.SIGKILL)
    victim.wait()
    eng = Engine(path)
    steps_before = eng.connection.execute("select count(*) from effects").fetchone()[0]
    done_before = eng.queue_stats(QUEUE)["completed"]
    # The run the victim was executing when it died: its task has to wait
    # out the lease before anyone can touch it again.
    victim_run = eng.connection.execute(
        f'select run_id, task_id from "r_{QUEUE}" where state = ? order by started_at desc limit 1', ("running",)
    ).fetchone()
    rescuer = start_worker(path, 1, 1, "full", False, step_ms, True, claim_timeout=claim_timeout, stop_when_idle=True, script=script)
    rescuer.wait()
    finish_t = time.time()
    first_resume = eng.connection.execute("select min(t) from effects where t > ?", (kill_t,)).fetchone()[0]
    victim_resume = victim_reclaimed = None
    if victim_run is not None:
        victim_resume = eng.connection.execute(
            "select min(t) from effects where task_id = ? and run_id != ?", (victim_run["task_id"], victim_run["run_id"])
        ).fetchone()[0]
        failed_at = eng.get_run(QUEUE, victim_run["run_id"])["failed_at"]
        if failed_at is not None:
            victim_reclaimed = failed_at.timestamp()
    rerun = eng.connection.execute(
        "select count(*) from (select task_id, step from effects group by task_id, step having count(*) > 1)"
    ).fetchone()[0]
    timeouts = eng.connection.execute(
        f'select count(*) from "r_{QUEUE}" where state = ? and failure_reason like ?', ("failed", "%ClaimTimeout%")
    ).fetchone()[0]
    stats = eng.queue_stats(QUEUE)
    eng.close()
    return {
        "tasks": tasks, "completed": stats["completed"], "claim_timeout_s": claim_timeout,
        "steps_done_before_kill": steps_before, "tasks_done_before_kill": done_before,
        "kill_to_first_new_step_s": (first_resume - kill_t) if first_resume else None,
        "kill_to_lease_reclaimed_s": (victim_reclaimed - kill_t) if victim_reclaimed else None,
        "kill_to_victim_task_resumed_s": (victim_resume - kill_t) if victim_resume else None,
        "kill_to_all_done_s": finish_t - kill_t, "leases_reclaimed": timeouts, "steps_rerun": rerun,
    }


# ---------------------------------------------------------------- 6. storage & cleanup


def storage_and_cleanup(tmp: str, script: str, tasks: int) -> dict:
    res = drain(tmp, script, tasks, 1, "normal", False)
    path = res["path"]
    eng = Engine(path)
    eng.connection.execute("pragma wal_checkpoint(truncate)")
    size = db_bytes(path)
    eng.set_queue_policy(QUEUE, {"cleanup_ttl": 0, "cleanup_limit": tasks})
    from datetime import timedelta

    eng.set_fake_now(eng.now() + timedelta(seconds=1))
    t0 = time.perf_counter()
    report = eng.cleanup_all_queues(QUEUE)
    dt = time.perf_counter() - t0
    eng.close()
    return {"tasks": tasks, "bytes_per_task": size / tasks, "db_bytes": size, "cleanup_s": dt, "deleted": report[0]["tasks_deleted"]}


# ---------------------------------------------------------------- report


def print_report(r: dict) -> None:
    h = r["host"]
    print(f"Unhinged {h['commit']} | {h['cpu']} | {h['os']} | {h['fs']} | SQLite {h['sqlite']} | Python {h['python']}")
    print(f"pragmas: journal_mode=wal busy_timeout=5000 wal_autocheckpoint=default | worker: processes, batch_size=10, poll 5 ms")
    print()
    print("1. Calibration (median / p99)")
    for mode, v in r["calibration"]["raw"].items():
        print(f"   raw 4 KiB write + {mode:<11} {v['median_ms']:8.3f} ms / {v['p99_ms']:8.3f} ms  -> {1000 / v['median_ms']:>9,.0f}/s")
    for name, v in r["calibration"]["sqlite"].items():
        print(f"   sqlite commit {name:<15} {v['median_ms']:8.3f} ms / {v['p99_ms']:8.3f} ms  -> {v['commits_per_s']:>9,.0f} commits/s  [{v['class']}]")
    print()
    c = r["commits_per_task"]
    print(f"2. Commits per three-step task: {c['per_task_batch10']:.2f} (batch_size=10), {c['per_task_batch1']:.2f} (batch_size=1); "
          f"spawning is {c['spawn_batch_commits']} commit for the whole batch")
    print()
    print("3. Throughput, one worker process, three-step no-op task (median of runs, min-max)")
    print(f"   {'setting':<16}{'class':<12}{'tasks/s':>10}{'min-max':>16}{'commits/s':>12}{'ceiling':>10}{'% ceil':>8}")
    for row in r["throughput"]:
        print(f"   {row['setting']:<16}{row['class']:<12}{row['tasks_per_s']:>10,.0f}"
              f"{row['min']:>8,.0f}-{row['max']:<7,.0f}{row['commits_per_s']:>12,.0f}{row['ceiling_commits_per_s']:>10,.0f}{row['pct_of_ceiling']:>7.0f}%")
    print()
    print("4. Worker scaling, synchronous=FULL (tasks/s)")
    print(f"   {'workers':<10}{'no-op steps':>14}{'10 ms steps':>14}")
    for a, b in zip(r["scaling"]["noop"], r["scaling"]["sleep10ms"]):
        print(f"   {a['workers']:<10}{a['tasks_per_s']:>14,.0f}{b['tasks_per_s']:>14,.0f}")
    print()
    v = r["recovery"]
    print(f"5. Recovery: {v['tasks']} tasks of 3 x 20 ms steps, claim_timeout={v['claim_timeout_s']}s, worker killed -9 after 0.6 s")
    print(f"   done before kill: {v['tasks_done_before_kill']} tasks / {v['steps_done_before_kill']} steps")
    first, reclaimed, victim = v["kill_to_first_new_step_s"], v["kill_to_lease_reclaimed_s"], v["kill_to_victim_task_resumed_s"]
    print(f"   kill -> replacement worker's first step:   {first:.2f} s" if first is not None else "   no new step recorded")
    print(f"   kill -> interrupted task's lease reclaimed: {reclaimed:.2f} s  (lease is {v['claim_timeout_s']} s from its last checkpoint)"
          if reclaimed is not None else "   lease never reclaimed")
    print(f"   kill -> interrupted task resumed:           {victim:.2f} s  (it re-queues behind the remaining backlog)"
          if victim is not None else "   interrupted task never resumed")
    print(f"   kill -> everything done:                    {v['kill_to_all_done_s']:.2f} s")
    print(f"   leases reclaimed: {v['leases_reclaimed']}, steps that ran twice: {v['steps_rerun']}, completed: {v['completed']}/{v['tasks']}")
    print()
    s = r["storage"]
    print(f"6. Storage: {s['bytes_per_task']:,.0f} bytes per completed task ({s['db_bytes'] / 1024:,.0f} KiB for {s['tasks']:,}); "
          f"cleanup() of all {s['deleted']:,} took {s['cleanup_s']:.2f} s")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--json", help="write raw results here")
    ap.add_argument("--runs", type=int, default=3, help="measured runs per throughput row (plus one warm-up)")
    ap.add_argument("--dir", help="directory to benchmark in (default: a temp dir; on Linux /tmp is often tmpfs)")
    args = ap.parse_args()

    tasks = 1000 if args.quick else 3000
    ff_tasks = 100 if args.quick else 300
    sleep_tasks = 200 if args.quick else 400
    cal_n = 200 if args.quick else 500
    worker_counts = [1, 2, 4] if args.quick else [1, 2, 4, 8]

    tmp = tempfile.mkdtemp(prefix="unhinged-bench-", dir=args.dir)
    script = os.path.join(tmp, "worker.py")
    with open(script, "w") as f:
        f.write(WORKER)

    fs = filesystem(tmp)
    global FS_IN_MEMORY
    FS_IN_MEMORY = fs.lower() in IN_MEMORY_FS
    check_filesystem(fs)
    print(f"benchmarking in {tmp} ...", file=sys.stderr)
    r: dict = {
        "host": {
            "commit": git_commit(), "cpu": cpu_model(), "os": f"{platform.system()} {platform.release()}",
            "fs": fs, "sqlite": sqlite3.sqlite_version, "python": platform.python_version(),
            "platform": sys.platform,
        }
    }
    r["calibration"] = calibrate(tmp, cal_n)
    r["commits_per_task"] = commits_per_task(tmp, 500)
    cpt = r["commits_per_task"]["per_task_batch10"]
    r["throughput"] = throughput(tmp, script, tasks, ff_tasks, args.runs, cpt, r["calibration"])
    r["scaling"] = scaling(tmp, script, tasks, sleep_tasks, worker_counts)
    r["recovery"] = recovery(tmp, script, 60, 2)
    r["storage"] = storage_and_cleanup(tmp, script, tasks)

    print_report(r)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(r, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
