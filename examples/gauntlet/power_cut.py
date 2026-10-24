"""Phase 4 of the gauntlet: pull the plug on a Linux VM and count lost commits.

Needs Lima (brew install lima). A VM named `unhinged` is created on first use.

The database lives on the VM's own disk, not a host mount, so the host's page
cache can't rescue anything. Workers inside the VM print an acknowledgement
to the host over the SSH pipe every time a checkpoint commits. We hard-stop
the VM mid-run, boot it, and check every acknowledged checkpoint against the
file. One that is missing was acknowledged and then lost: a lost commit.

A hard stop loses the guest kernel's page cache, so this is the "OS crashed"
case. What the guest fsynced is on the virtual disk; what it didn't is gone.
That is precisely the difference between synchronous=NORMAL and FULL, so the
phase runs both.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import threading
import time

VM = "unhinged"
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HERE = os.path.dirname(os.path.abspath(__file__))

GUEST_SETUP = """
set -e
mkdir -p ~/gauntlet
python3 -c "import sqlite3, sys; print('guest python', sys.version.split()[0], 'sqlite', sqlite3.sqlite_version)"
df -T ~ | tail -1
"""

GUEST_SPAWN = """
import sys
sys.path.insert(0, "{home}/gauntlet")
from unhinged import Unhinged
app = Unhinged("{db}", queue_name="gauntlet", synchronous="{sync}")
app.engine.connection.execute("create table if not exists effects (task_id text, step integer, run_id text, pid integer, t real)")
with app.engine.transaction():
    for i in range({tasks}):
        app.spawn("work", {{"i": i}}, queue="gauntlet", retry_strategy={{"kind": "fixed", "base_seconds": 0}})
print("spawned")
"""

GUEST_CHECK = """
import json, sqlite3, sys
acks = json.load(open("{acks}"))
c = sqlite3.connect("{db}")
print("integrity", c.execute("pragma integrity_check").fetchone()[0])
have = set((t, int(n[1:])) for t, n in c.execute('select task_id, checkpoint_name from "c_gauntlet"'))
missing = [a for a in acks if (a[0], a[1]) not in have]
completed = c.execute('select count(*) from "t_gauntlet" where state = ?', ("completed",)).fetchone()[0]
print(json.dumps({{"acked": len(acks), "missing": len(missing), "checkpoints": len(have), "completed": completed}}))
"""


def sh(*args: str, check: bool = True, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(["limactl", *args], text=True, capture_output=True, check=check, **kw)


def guest(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return sh("shell", VM, "--", "bash", "-lc", cmd, check=check)


def guest_python(src: str) -> str:
    return guest(f"python3 - <<'PYEOF'\n{src}\nPYEOF").stdout


def vm_exists() -> bool:
    out = sh("list", "--json", check=False).stdout
    return any(json.loads(line).get("name") == VM for line in out.splitlines() if line.strip())


def vm_running() -> bool:
    out = sh("list", "--json", check=False).stdout
    for line in out.splitlines():
        if line.strip():
            d = json.loads(line)
            if d.get("name") == VM:
                return d.get("status") == "Running"
    return False


def ensure_vm(log) -> str:
    if not vm_exists():
        log("creating the Lima VM (downloads an Ubuntu image the first time)")
        sh("start", "--name", VM, "--tty=false", "--cpus", "4", "--memory", "4", "template:ubuntu")
    elif not vm_running():
        log("starting the Lima VM")
        sh("start", VM, "--tty=false")
    out = guest(GUEST_SETUP).stdout.strip().splitlines()
    for line in out:
        log(f"guest: {line}")
    home = guest("echo $HOME").stdout.strip()
    # Copy the package in so the guest runs this exact code.
    pkg = os.path.join(ROOT, "unhinged")
    sh("copy", "-r", pkg, f"{VM}:{home}/gauntlet/")
    guest("sync")  # or the next power cut takes our own files with it
    return home


def write_guest_worker(home: str) -> None:
    import gauntlet  # noqa: the WORKER script text lives there

    local = os.path.join(HERE, "_guest_worker.py")
    with open(local, "w") as f:
        f.write(f"import sys\nsys.path.insert(0, {home + '/gauntlet'!r})\n" + gauntlet.WORKER)
    sh("copy", local, f"{VM}:{home}/gauntlet/worker.py")
    os.unlink(local)
    guest("sync")


def run_one(sync: str, tasks: int, workers: int, run_seconds: float, home: str, log, ui) -> dict:
    db = f"{home}/gauntlet/{sync}.db"
    # The previous cut may have truncated files that were still in the page cache; put them back.
    sh("copy", "-r", os.path.join(ROOT, "unhinged"), f"{VM}:{home}/gauntlet/")
    write_guest_worker(home)
    guest(f"rm -f {shlex.quote(db)} {shlex.quote(db)}-wal {shlex.quote(db)}-shm")
    guest_python(GUEST_SPAWN.format(home=home, db=db, sync=sync, tasks=tasks))
    guest("sync")  # everything before the run is on disk; only what the run commits is under test
    log(f"[{sync}] {tasks:,} tasks spawned on the VM's disk; starting {workers} workers inside the VM")

    acks: list = []
    lock = threading.Lock()

    def pump(proc: subprocess.Popen) -> None:
        for line in proc.stdout:
            if line.startswith("ack "):
                _, task_id, step = line.split()
                with lock:
                    acks.append((task_id, int(step), time.time()))

    procs = []
    for i in range(workers):
        p = subprocess.Popen(
            ["limactl", "shell", VM, "--", "python3", f"{home}/gauntlet/worker.py", db, f"w{i}", "3", "1.0", sync, "1", "0"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1,
        )
        threading.Thread(target=pump, args=(p,), daemon=True).start()
        procs.append(p)

    t0 = time.monotonic()
    while time.monotonic() - t0 < run_seconds:
        ui.tick()
        time.sleep(0.2)
    with lock:
        n_before = len(acks)
    log(f"[{sync}] {n_before:,} checkpoints acknowledged to the host; [bold red]pulling the plug[/] (limactl stop --force)")
    cut_at = time.time()
    sh("stop", "--force", VM, check=False)
    for p in procs:
        p.wait(timeout=30)
    with lock:
        acked = [(t, s) for t, s, at in acks if at <= cut_at + 0.5]
    log(f"[{sync}] VM is off. {len(acked):,} acknowledgements in hand. Booting it again")
    sh("start", VM, "--tty=false")
    acks_path = f"{home}/gauntlet/{sync}-acks.json"
    local = os.path.join(HERE, f"_acks_{sync}.json")
    with open(local, "w") as f:
        json.dump(acked, f)
    sh("copy", local, f"{VM}:{acks_path}")
    os.unlink(local)
    out = guest_python(GUEST_CHECK.format(acks=acks_path, db=db)).strip().splitlines()
    integrity = out[0].split()[-1]
    result = json.loads(out[-1])
    result.update({"sync": sync, "integrity": integrity})
    log(f"[{sync}] integrity {integrity}; {result['missing']} of {result['acked']:,} acknowledged checkpoints are gone")
    return result


def phase_power_cut(s, ui, tasks: int = 4000, workers: int = 4, run_seconds: float = 6.0) -> None:
    s.phase = "4 power cut"
    home = ensure_vm(s.log)
    results = [run_one(sync, tasks, workers, run_seconds, home, s.log, ui) for sync in ("normal", "full")]
    summary = "; ".join(
        f"{r['sync'].upper()}: {r['missing']} of {r['acked']:,} acknowledged checkpoints lost, integrity {r['integrity']}"
        for r in results
    )
    s.extra["power cut"] = summary
    s.log(f"power cut done: {summary}")
