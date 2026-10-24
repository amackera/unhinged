"""The gauntlet: everything that can go wrong, in order, with a ledger at the end.

    pip install rich
    python examples/gauntlet/gauntlet.py                  # load + chaos + ledger
    python examples/gauntlet/gauntlet.py --disk-death     # + Litestream restore on "machine B"
    python examples/gauntlet/gauntlet.py --power-cut      # + hard-kill a Linux VM mid-run

Phases:

1. Load.   Thousands of three-step tasks, eight worker processes, one file.
2. Chaos.  A supervisor SIGKILLs a random worker every few hundred ms.
3. Disk.   (--disk-death) Litestream has been replicating the file. We delete
           the directory, restore from the replica on "machine B", and fresh
           workers finish the backlog there.
4. Power.  (--power-cut) Workers inside a Lima VM, database on the VM's own
           disk, every committed checkpoint acknowledged to the host over a
           pipe. We hard-stop the VM, boot it, and count acknowledged
           checkpoints the file no longer has. Run at NORMAL and at FULL.
5. Ledger. Every task completed exactly once, every re-executed step explained
           by a reclaimed lease or replica lag, integrity ok.

Every step records (task, step, run, pid, time) in a side table, which is
what the ledger is computed from.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
from dataclasses import dataclass, field

from unhinged import Engine, Unhinged

QUEUE = "gauntlet"
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

WORKER = textwrap.dedent(
    """
    import os, sys, time, traceback
    from unhinged import Unhinged

    db, worker_id, steps, step_ms, sync, ack = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4]), sys.argv[5], sys.argv[6] == "1"
    app = Unhinged(db, queue_name="gauntlet", synchronous=sync)
    conn = app.engine.connection
    out = sys.stdout

    @app.register_task("work", default_max_attempts=1000)
    def work(params, ctx):
        for i in range(steps):
            def effect(i=i):
                if step_ms:
                    time.sleep(step_ms / 1000)
                conn.execute("insert into effects values (?, ?, ?, ?, ?)",
                             (ctx.task_id, i, ctx.run_id, os.getpid(), time.time()))
                return i
            ctx.step(f"s{i}", effect)
            if ack:
                # The checkpoint for s{i} has committed by the time step() returns.
                out.write(f"ack {ctx.task_id} {i}\\n"); out.flush()
        return "ok"

    try:
        app.start_worker(worker_id=worker_id, claim_timeout=2, poll_interval=0.01, batch_size=4,
                         stop_when_idle=sys.argv[7] == "1")
    except Exception:
        sys.stderr.write(traceback.format_exc()); raise
    """
)


# ---------------------------------------------------------------- state & ui


@dataclass
class State:
    workdir: str
    db: str
    tasks: int
    steps: int
    phase: str = "starting"
    started: float = field(default_factory=time.monotonic)
    kills: int = 0
    events: list = field(default_factory=list)
    procs: dict = field(default_factory=dict)
    extra: dict = field(default_factory=dict)
    killed_pids: set = field(default_factory=set)

    quiet_ui: bool = False

    def log(self, msg: str) -> None:
        self.events.append((time.monotonic() - self.started, msg))
        if len(self.events) > 400:
            del self.events[:-400]
        if self.quiet_ui:
            from rich.console import Console

            Console(stderr=True).print(f"[dim]{self.events[-1][0]:6.1f}s[/]  {msg}")


class Dashboard:
    """A live table over the queue, refreshed from the file itself."""

    def __init__(self, state: State, enabled: bool) -> None:
        self.state = state
        self.enabled = enabled
        self.live = None
        self._last = (time.monotonic(), 0)
        self.rate = 0.0

    def __enter__(self):
        if self.enabled:
            from rich.live import Live

            self.live = Live(self.render(), refresh_per_second=5, screen=False)
            self.live.__enter__()
        return self

    def __exit__(self, *exc):
        if self.live:
            self.live.update(self.render())
            self.live.__exit__(*exc)

    def tick(self) -> None:
        if self.live:
            self.live.update(self.render())

    def render(self):
        from rich.console import Group
        from rich.panel import Panel
        from rich.table import Table

        s = self.state
        eng = Engine(s.db) if os.path.exists(s.db) else None
        stats = eng.queue_stats(QUEUE) if eng else {k: 0 for k in ("pending", "running", "sleeping", "completed", "failed", "cancelled")}
        effects = eng.connection.execute("select count(*) from effects").fetchone()[0] if eng else 0
        timeouts = eng.connection.execute(f'select count(*) from "r_{QUEUE}" where failure_reason like ?', ("%ClaimTimeout%",)).fetchone()[0] if eng else 0
        if eng:
            eng.close()
        now = time.monotonic()
        if now - self._last[0] >= 0.5:
            self.rate = (effects - self._last[1]) / (now - self._last[0])
            self._last = (now, effects)
        alive = sum(1 for p in s.procs.values() if p.poll() is None)

        t = Table.grid(padding=(0, 2))
        t.add_column(style="bold"); t.add_column()
        t.add_row("phase", f"[bold cyan]{s.phase}[/]   {now - s.started:6.1f}s")
        t.add_row("tasks", f"completed [green]{stats['completed']:,}[/] / {s.tasks:,}   pending {stats['pending']:,}   running {stats['running']:,}   sleeping {stats['sleeping']:,}   failed {stats['failed']:,}")
        t.add_row("steps", f"{effects:,} executed   {self.rate:,.0f}/s   (expected {s.tasks * s.steps:,})")
        t.add_row("workers", f"[green]{alive}[/] alive   [red]{s.kills}[/] killed   {timeouts} leases reclaimed")
        for k, v in s.extra.items():
            t.add_row(k, str(v))
        log = "\n".join(f"[dim]{ts:6.1f}s[/]  {msg}" for ts, msg in s.events[-10:])
        return Group(Panel(t, title="unhinged gauntlet", border_style="cyan"), Panel(log or "...", title="events", border_style="dim"))


# ---------------------------------------------------------------- helpers


def fresh_dir(path: str) -> str:
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    return path


def write_worker(workdir: str) -> str:
    script = os.path.join(workdir, "worker.py")
    with open(script, "w") as f:
        f.write(WORKER)
    return script


def spawn_all(db: str, tasks: int, sync: str = "full") -> None:
    app = Unhinged(db, queue_name=QUEUE, synchronous=sync)
    app.engine.connection.execute(
        "create table if not exists effects (task_id text, step integer, run_id text, pid integer, t real)"
    )
    with app.engine.transaction():
        for i in range(tasks):
            app.spawn("work", {"i": i}, queue=QUEUE, retry_strategy={"kind": "fixed", "base_seconds": 0})
    app.close()


def start_worker(script: str, db: str, idx: int, steps: int, step_ms: float, sync: str = "full",
                 ack: bool = False, stop_when_idle: bool = True, stdout=None) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, script, db, f"w{idx}", str(steps), str(step_ms), sync, "1" if ack else "0", "1" if stop_when_idle else "0"],
        env={**os.environ, "PYTHONPATH": ROOT}, stdout=stdout or subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )


def completed(db: str) -> int:
    e = Engine(db)
    try:
        return e.queue_stats(QUEUE)["completed"]
    finally:
        e.close()


# ---------------------------------------------------------------- phases


def phase_load_and_chaos(s: State, ui: Dashboard, script: str, workers: int, step_ms: float,
                         kill_every: float, stop_at_fraction: float, rng: random.Random) -> None:
    s.phase = "1 load"
    s.log(f"spawning {s.tasks:,} tasks in one transaction")
    spawn_all(s.db, s.tasks)
    s.log(f"starting {workers} worker processes on one file")
    s.procs = {i: start_worker(script, s.db, i, s.steps, step_ms) for i in range(workers)}
    warm_until = time.monotonic() + 2.0
    next_kill = None
    target = int(s.tasks * stop_at_fraction)
    while True:
        done = completed(s.db)
        if done >= target:
            break
        now = time.monotonic()
        if now >= warm_until and s.phase == "1 load":
            s.phase = "2 chaos"
            s.log(f"chaos: killing a random worker every {kill_every * 1000:.0f} ms")
            next_kill = now
        if next_kill is not None and now >= next_kill:
            idx = rng.choice(list(s.procs))
            p = s.procs[idx]
            if p.poll() is None:
                s.killed_pids.add(p.pid)
                p.send_signal(signal.SIGKILL)
                p.wait()
                s.kills += 1
                s.log(f"[red]kill -9[/] worker w{idx} (pid {p.pid})")
            s.procs[idx] = start_worker(script, s.db, idx, s.steps, step_ms)
            next_kill = now + kill_every
        for idx, p in list(s.procs.items()):
            if p.poll() is not None and p.pid not in s.killed_pids and completed(s.db) < target:
                err = p.stderr.read() if p.stderr else ""
                if err.strip():
                    s.log(f"[red]worker w{idx} died on its own:[/] {err.strip().splitlines()[-1]}")
                    s.extra["worker errors"] = s.extra.get("worker errors", 0) + 1
                s.procs[idx] = start_worker(script, s.db, idx, s.steps, step_ms)
        ui.tick()
        time.sleep(0.1)
    for p in s.procs.values():
        if p.poll() is None:
            p.send_signal(signal.SIGKILL)
            p.wait()
    s.log(f"stopped all workers at {completed(s.db):,} / {s.tasks:,} completed")


def phase_disk_death(s: State, ui: Dashboard, script: str, workers: int, step_ms: float) -> None:
    """Litestream has been replicating since before phase 1 (see main). Lose the disk, restore, finish."""
    s.phase = "3 disk death"
    ls = s.extra.pop("_litestream_proc")
    replica = s.extra.pop("_replica")
    # No grace period: the disk dies with replication in flight, as a real one would.
    before = completed(s.db)
    ls.send_signal(signal.SIGKILL)
    ls.wait(timeout=30)
    s.log(f"[red]rm -rf[/] the database directory (machine A's disk is gone, replication was mid-flight)")
    shutil.rmtree(os.path.dirname(s.db))
    machine_b = fresh_dir(os.path.join(s.workdir, "machine-b"))
    new_db = os.path.join(machine_b, "gauntlet.db")
    s.log("litestream restore on machine B")
    r = subprocess.run(["litestream", "restore", "-o", new_db, replica], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"litestream restore failed: {r.stderr.strip()}")
    s.db = new_db
    after = completed(new_db)
    s.extra["replica lag"] = f"{before - after} completions existed only on the dead disk; machine B ran them again"
    s.log(f"restored: {after:,} completed in the replica vs {before:,} on the dead disk")
    integrity = Engine(new_db).connection.execute("pragma integrity_check").fetchone()[0]
    s.log(f"integrity_check on the restored file: {integrity}")
    s.phase = "3 finishing on machine B"
    s.procs = {i: start_worker(script, new_db, i, s.steps, step_ms) for i in range(workers)}
    while completed(new_db) < s.tasks:
        ui.tick()
        time.sleep(0.1)
        for p in s.procs.values():
            if p.poll() is not None and p.returncode:
                raise RuntimeError(p.stderr.read())
    for p in s.procs.values():
        p.wait()
    s.log(f"machine B drained the rest: {completed(new_db):,} / {s.tasks:,}")


def phase_finish_local(s: State, ui: Dashboard, script: str, workers: int, step_ms: float) -> None:
    s.phase = "finishing"
    s.procs = {i: start_worker(script, s.db, i, s.steps, step_ms) for i in range(workers)}
    while completed(s.db) < s.tasks:
        ui.tick()
        time.sleep(0.1)
    for p in s.procs.values():
        p.wait()


def ledger(s: State) -> dict:
    s.phase = "5 ledger"
    eng = Engine(s.db)
    conn = eng.connection
    stats = eng.queue_stats(QUEUE)
    effects = conn.execute("select task_id, step, run_id, pid from effects").fetchall()
    runs = {r["run_id"]: r for r in conn.execute(f'select run_id, state, failure_reason from "r_{QUEUE}"').fetchall()}

    def timed_out(run_id):
        r = runs.get(run_id)
        return bool(r and r["state"] == "failed" and r["failure_reason"] and json.loads(r["failure_reason"]).get("name") == "$ClaimTimeout")

    pids_per_run = collections.defaultdict(set)
    per_run_step = collections.Counter()
    per_task_step = collections.defaultdict(list)
    for e in effects:
        pids_per_run[e["run_id"]].add(e["pid"])
        per_run_step[(e["run_id"], e["step"])] += 1
        per_task_step[(e["task_id"], e["step"])].append(e["run_id"])
    shared = [r for r, p in pids_per_run.items() if len(p) > 1]
    double = [k for k, n in per_run_step.items() if n > 1]
    rerun_lease = rerun_lag = 0
    unexplained = []
    for key, rids in per_task_step.items():
        if len(rids) > 1:
            earlier = rids[:-1]
            if all(timed_out(r) for r in earlier):
                rerun_lease += len(earlier)
            elif all(r not in runs for r in earlier):
                rerun_lag += len(earlier)  # the earlier run lived on a disk that no longer exists
            else:
                unexplained.append((key, rids))
    integrity = conn.execute("pragma integrity_check").fetchone()[0]
    report = {
        "tasks": s.tasks, "completed": stats["completed"], "kills": s.kills,
        "steps_executed": len(effects), "steps_expected": s.tasks * s.steps,
        "reran_after_lease_reclaim": rerun_lease, "reran_after_replica_lag": rerun_lag,
        "runs_timed_out": sum(1 for r in runs.values() if timed_out(r["run_id"])),
        "shared_runs": shared, "double_in_run": double, "unexplained": unexplained,
        "integrity": integrity, "elapsed_s": round(time.monotonic() - s.started, 1),
        "power_cut": s.extra.get("power cut"),
        "replica_lag": s.extra.get("replica lag"),
    }
    report["ok"] = (stats["completed"] == s.tasks and not shared and not double and not unexplained and integrity == "ok")
    eng.close()
    return report


def print_ledger(r: dict) -> None:
    from rich.console import Console
    from rich.table import Table

    c = Console()
    t = Table(title="ledger", show_header=False)
    t.add_column(style="bold"); t.add_column()
    t.add_row("tasks completed", f"{r['completed']:,} / {r['tasks']:,}")
    t.add_row("workers killed", f"{r['kills']}")
    t.add_row("leases reclaimed", f"{r['runs_timed_out']}")
    t.add_row("steps executed", f"{r['steps_executed']:,} for {r['steps_expected']:,} steps")
    t.add_row("re-ran after a reclaimed lease", f"{r['reran_after_lease_reclaim']}  (the at-least-once window)")
    if r.get("replica_lag"):
        t.add_row("lost with the disk", f"{r['replica_lag']}")
    t.add_row("a run executed by two processes", f"{len(r['shared_runs'])}")
    t.add_row("a step executed twice in one run", f"{len(r['double_in_run'])}")
    t.add_row("unexplained re-executions", f"{len(r['unexplained'])}")
    if r.get("power_cut"):
        t.add_row("power cut", r["power_cut"])
    t.add_row("integrity_check", r["integrity"])
    t.add_row("elapsed", f"{r['elapsed_s']}s")
    t.add_row("verdict", "[bold green]OK[/]" if r["ok"] else "[bold red]FAILED[/]")
    c.print(t)


# ---------------------------------------------------------------- main


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tasks", type=int, default=10000)
    ap.add_argument("--steps", type=int, default=3)
    ap.add_argument("--step-ms", type=float, default=2.0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--kill-every", type=float, default=0.25)
    ap.add_argument("--disk-death", action="store_true", help="replicate with Litestream, lose the disk, restore and finish elsewhere")
    ap.add_argument("--power-cut", action="store_true", help="hard-stop a Lima VM mid-run and count lost commits")
    ap.add_argument("--no-ui", action="store_true")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--json", help="write the ledger here")
    args = ap.parse_args()

    if args.disk_death and not shutil.which("litestream"):
        print("--disk-death needs litestream (brew install litestream)", file=sys.stderr)
        return 2

    workdir = tempfile.mkdtemp(prefix="unhinged-gauntlet-")
    machine_a = fresh_dir(os.path.join(workdir, "machine-a"))
    s = State(workdir=workdir, db=os.path.join(machine_a, "gauntlet.db"), tasks=args.tasks, steps=args.steps)
    script = write_worker(workdir)
    rng = random.Random(args.seed)
    ui_enabled = not args.no_ui and sys.stdout.isatty()
    s.quiet_ui = not ui_enabled

    if args.disk_death:
        replica = os.path.join(workdir, "replica")
        spawn_all(s.db, 0)  # create the file so litestream has something to watch
        ls = subprocess.Popen(["litestream", "replicate", s.db, f"file://{replica}"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        s.extra["_litestream_proc"] = ls
        s.extra["_replica"] = f"file://{replica}"
        s.log("litestream replicating machine A's file to a replica")

    with Dashboard(s, ui_enabled) as ui:
        phase_load_and_chaos(s, ui, script, args.workers, args.step_ms, args.kill_every,
                             stop_at_fraction=0.7 if args.disk_death else 1.0, rng=rng)
        if args.disk_death:
            phase_disk_death(s, ui, script, args.workers, args.step_ms)
        if args.power_cut:
            from power_cut import phase_power_cut  # lives next to this file

            phase_power_cut(s, ui)
        if completed(s.db) < s.tasks:
            phase_finish_local(s, ui, script, args.workers, args.step_ms)
        report = ledger(s)
        ui.tick()

    s.extra.pop("_litestream_proc", None)
    print_ledger(report)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=2, default=str)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    sys.exit(main())
