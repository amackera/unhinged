"""A release checklist that survives Ctrl-C and waits for approval.

    python examples/release/release.py init ./demo-project
    python examples/release/release.py run 1.4.0 --project ./demo-project
    python examples/release/release.py approve 1.4.0        # from another terminal
    python examples/release/release.py status 1.4.0

`run` builds, tests and tags the project as real subprocesses, each one a
step. Then it parks until someone approves. On approval it publishes to
dist/. Press Ctrl-C anywhere and run the same command again: steps that
finished print `cached` and are not repeated, and the release carries on
from the one that was interrupted.

The version is the idempotency key, so running `run 1.4.0` twice attaches
to the one release rather than starting a second. A failed build or test
ends the release as failed; it is not retried, because retrying a red test
suite is not what anyone wants. A crash is retried, because that is.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import textwrap
import time

from unhinged import SuspendTask, TaskCancelled, TimeoutError, Unhinged

QUEUE = "release"
DEFAULT_DB = "release.db"
CLAIM_TIMEOUT = 5  # seconds; a crashed attempt is reclaimed this fast
TERMINAL = ("completed", "failed", "cancelled")


# -- the release task --------------------------------------------------------


def shell_step(ctx, name: str, cmd: list[str], cwd: str) -> dict:
    """Run a command once. On replay, return what it did last time."""
    handle = ctx.begin_step(name)
    if handle.done:
        print(f"[{name:<7}] cached ({handle.state['seconds']}s, exit {handle.state['exit']})")
        return handle.state
    print(f"[{name:<7}] {' '.join(cmd)}", flush=True)
    t0 = time.monotonic()
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    last_beat = time.monotonic()
    while proc.poll() is None:
        time.sleep(0.1)
        if time.monotonic() - last_beat > CLAIM_TIMEOUT / 3:
            ctx.heartbeat()  # a long step keeps proving the worker is alive
            last_beat = time.monotonic()
    output = proc.stdout.read()
    result = {"exit": proc.returncode, "seconds": round(time.monotonic() - t0, 1), "output": output[-4000:]}
    print(f"[{name:<7}] exit {proc.returncode} ({result['seconds']}s)")
    return ctx.complete_step(handle, result)


def register(app: Unhinged, project: str, window: int) -> None:
    project = os.path.abspath(project)

    @app.register_task("release", default_max_attempts=20)
    def release(params, ctx):
        version = params["version"]
        tag = f"v{version}"
        for name, cmd in (
            ("build", [sys.executable, "-m", "zipfile", "-c", f"build/app-{version}.zip", "src"]),
            ("test", [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-q"]),
            ("tag", ["sh", "-c", f"git rev-parse -q --verify {tag} >/dev/null || git tag -a {tag} -m 'release {version}'"]),
        ):
            if name == "build":
                os.makedirs(os.path.join(project, "build"), exist_ok=True)
            result = shell_step(ctx, name, cmd, project)
            if result["exit"] != 0:
                print(result["output"])
                return {"version": version, "status": "failed", "step": name}

        try:
            decision = ctx.await_event(f"approve:{version}", timeout=window)
        except SuspendTask:
            # No decision yet: the run parks here, holding no lease, and this
            # process is free to exit. Re-running `run` resumes it.
            print(f"[approve] waiting: python {os.path.basename(__file__)} approve {version}", flush=True)
            raise
        except TimeoutError:
            return {"version": version, "status": "expired"}
        if not decision.get("ok"):
            return {"version": version, "status": "rejected", "by": decision.get("by")}
        print(f"[approve] approved by {decision.get('by', 'someone')}")

        def publish():
            os.makedirs(os.path.join(project, "dist"), exist_ok=True)
            src = os.path.join(project, "build", f"app-{version}.zip")
            dst = os.path.join(project, "dist", f"app-{version}.zip")
            shutil.copyfile(src, dst)
            return os.path.relpath(dst, project)

        published = ctx.step("publish", publish)
        print(f"[publish] {published}")
        return {"version": version, "status": "released", "artifact": published, "tag": tag}


# -- commands ---------------------------------------------------------------


def make_app(db: str) -> Unhinged:
    return Unhinged(db, queue_name=QUEUE)


def cmd_init(args) -> None:
    d = args.dir
    os.makedirs(os.path.join(d, "src", "app"), exist_ok=True)
    os.makedirs(os.path.join(d, "tests"), exist_ok=True)
    with open(os.path.join(d, "src", "app", "__init__.py"), "w") as f:
        f.write('def greet(name):\n    return f"hello {name}"\n')
    with open(os.path.join(d, "tests", "test_app.py"), "w") as f:
        f.write(
            textwrap.dedent(
                """
                import sys, time, unittest
                sys.path.insert(0, "src")
                from app import greet

                class SlowIntegrationTests(unittest.TestCase):
                    def _check(self, name):
                        time.sleep(0.5)  # pretend this talks to a service
                        self.assertEqual(greet(name), f"hello {name}")

                    def test_alice(self): self._check("alice")
                    def test_bob(self): self._check("bob")
                    def test_carol(self): self._check("carol")
                    def test_dave(self): self._check("dave")
                    def test_erin(self): self._check("erin")
                """
            ).lstrip()
        )
    with open(os.path.join(d, ".gitignore"), "w") as f:
        f.write("build/\ndist/\n")
    if not os.path.isdir(os.path.join(d, ".git")):
        subprocess.run(["git", "init", "-q"], cwd=d, check=True)
        # The demo repo gets its own identity so `git tag -a` works on a
        # machine with no global git config, such as a CI runner.
        subprocess.run(["git", "config", "user.name", "release demo"], cwd=d, check=True)
        subprocess.run(["git", "config", "user.email", "demo@example.com"], cwd=d, check=True)
        subprocess.run(["git", "add", "-A"], cwd=d, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "initial"], cwd=d, check=True)
    print(f"sample project in {d}: five slow tests, a git repo, nothing released")


def cmd_run(args) -> int:
    app = make_app(args.db)
    register(app, args.project, args.window)
    spawned = app.spawn(
        "release", {"version": args.version},
        idempotency_key=f"release:{args.version}",
        retry_strategy={"kind": "fixed", "base_seconds": 0},
        cancellation={"max_duration": args.window},
    )
    task_id = spawned["task_id"]
    print(f"release {args.version}: {'started' if spawned['created'] else 'resuming'} task {task_id}")
    try:
        while True:
            snap = app.fetch_task_result(task_id)
            if snap.state in TERMINAL:
                break
            if app.work_batch("release-runner", claim_timeout=CLAIM_TIMEOUT) == 0:
                time.sleep(0.25)
    except KeyboardInterrupt:
        print("\ninterrupted. Run the same command again to pick up where this left off.")
        return 130
    finally:
        app.close()
    if snap.state == "completed":
        print(f"release {args.version}: {snap.result['status']}" + (f" -> {snap.result['artifact']}" if snap.result.get("artifact") else ""))
        return 0 if snap.result["status"] == "released" else 1
    print(f"release {args.version}: {snap.state}" + (f": {snap.failure.get('message')}" if snap.failure else ""))
    return 1


def cmd_decide(args, ok: bool) -> None:
    app = make_app(args.db)
    app.emit_event(f"approve:{args.version}", {"ok": ok, "by": args.by})
    print(f"{'approved' if ok else 'rejected'} {args.version} as {args.by}; the running `run` will continue")
    app.close()


def cmd_status(args) -> None:
    app = make_app(args.db)
    row = app.engine.connection.execute(
        f'select task_id from "t_{QUEUE}" where idempotency_key = ?', (f"release:{args.version}",)
    ).fetchone()
    if row is None:
        print(f"no release {args.version}")
        return
    snap = app.fetch_task_result(row["task_id"])
    print(f"release {args.version}: {snap.state}")
    for cp in app.engine.get_task_checkpoint_states(QUEUE, row["task_id"], app.engine.get_task(QUEUE, row["task_id"])["last_attempt_run"]):
        state = cp["state"]
        detail = f"exit {state['exit']}, {state['seconds']}s" if isinstance(state, dict) and "exit" in state else json.dumps(state)[:60]
        print(f"  {cp['checkpoint_name']:<24} {detail}")
    if snap.result:
        print(f"  result: {json.dumps(snap.result)}")
    app.close()


def main(argv=None) -> int:
    try:
        sys.stdout.reconfigure(line_buffering=True)  # progress lines show up through pipes too
    except (AttributeError, ValueError):
        pass
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", default=DEFAULT_DB)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init", help="write a sample project to release")
    s.add_argument("dir")
    s.set_defaults(fn=cmd_init)

    s = sub.add_parser("run", help="run (or resume) the release for a version")
    s.add_argument("version")
    s.add_argument("--project", default=".")
    s.add_argument("--window", type=int, default=3600, help="seconds the release may stay open")
    s.set_defaults(fn=cmd_run)

    for name, ok in (("approve", True), ("reject", False)):
        s = sub.add_parser(name)
        s.add_argument("version")
        s.add_argument("--by", default=os.environ.get("USER", "someone"))
        s.set_defaults(fn=lambda a, ok=ok: cmd_decide(a, ok))

    s = sub.add_parser("status", help="show the steps so far")
    s.add_argument("version")
    s.set_defaults(fn=cmd_status)

    args = p.parse_args(argv)
    try:
        rc = args.fn(args)
    except TaskCancelled:
        print("cancelled")
        return 1
    return rc or 0


if __name__ == "__main__":
    sys.exit(main())
