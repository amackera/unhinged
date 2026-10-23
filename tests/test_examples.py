"""Run the example apps end to end with no network."""

import importlib.util
import subprocess
import json
import os
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def pipeline():
    return load("pipeline_example", EXAMPLES / "pipeline" / "pipeline.py")


@pytest.fixture
def agent():
    return load("agent_example", EXAMPLES / "agent" / "agent.py")


def test_pipeline_processes_everything_once_despite_flakiness(pipeline, tmp_path, capsys):
    db = str(tmp_path / "p.db")
    corpus, out = str(tmp_path / "corpus"), str(tmp_path / "out")
    pipeline.main(["--db", db, "generate", corpus, "--count", "30"])
    pipeline.main(["--db", db, "enqueue", corpus])
    pipeline.main(["--db", db, "enqueue", corpus])  # idempotent
    assert "enqueued 0 new tasks, 30 already queued" in capsys.readouterr().out

    import random
    from datetime import timedelta

    app = pipeline.make_app(db)
    pipeline.register(app, out, flaky=0.4)
    random.seed(7)

    def drain() -> dict:
        # Advance the engine's clock past the backoff between rounds so the
        # retries are instant for the test.
        for _ in range(50):
            app.run_until_idle(claim_timeout=30)
            stats = app.engine.queue_stats(pipeline.QUEUE)
            if stats["pending"] + stats["sleeping"] + stats["running"] == 0:
                return stats
            app.engine.set_fake_now(app.engine.now() + timedelta(seconds=60))
        raise AssertionError("queue did not drain")

    stats = drain()
    assert stats["completed"] + stats["failed"] == 30
    assert all(t["max_attempts"] == 6 for t in app.engine.list_tasks(pipeline.QUEUE, limit=100))

    # Anything that burned all six attempts can be revived by hand; the
    # checkpoints it already wrote are kept.
    for task in app.engine.list_tasks(pipeline.QUEUE, state="failed", limit=100):
        app.retry_task(task["task_id"], max_attempts=50)
    stats = drain()
    assert stats["completed"] == 30 and stats["failed"] == 0
    assert len(os.listdir(out)) == 30

    # every file was checksummed exactly once, even the ones that retried
    checksum_rows = app.engine.connection.execute(
        'select count(*) as n from "c_pipeline" where checkpoint_name = ?', ("checksum",)
    ).fetchone()["n"]
    assert checksum_rows == 30
    retried = app.engine.connection.execute('select count(*) as n from "r_pipeline" where state = ?', ("failed",)).fetchone()["n"]
    assert retried > 0  # flakiness actually happened
    app.close()

    pipeline.main(["--db", db, "status"])
    pipeline.main(["--db", db, "report"])
    out_text = capsys.readouterr().out
    assert " completed 30" in out_text
    assert "30 files" in out_text


def test_agent_parks_on_a_question_and_resumes_without_recalling_the_model(agent, tmp_path, capsys):
    db = str(tmp_path / "a.db")
    root = tmp_path / "root"
    root.mkdir()
    (root / "README.md").write_text("# demo\n")
    agent.ScriptedModel.calls = 0

    app = agent.make_app(db)
    spawned = app.spawn("agent", {"prompt": "look around"}, queue=agent.QUEUE)
    task_id = spawned["task_id"]

    # Worker 1 runs until the agent asks a question and parks.
    worker1 = agent.make_app(db)
    agent.register(worker1, str(root), agent.ScriptedModel())
    worker1.run_until_idle()
    worker1.close()
    assert app.fetch_task_result(task_id).state == "sleeping"
    assert agent.ScriptedModel.calls == 3
    event, question = agent.pending_question(app, task_id)
    assert question.startswith("Should I leave a note")

    agent.main(["--db", db, "show", task_id])
    shown = capsys.readouterr().out
    assert "model -> list_files" in shown and "waiting on: Should I" in shown

    agent.main(["--db", db, "answer", task_id, "yes please"])

    # A fresh worker (as after a crash or deploy) replays the three stored
    # turns and makes only the two new calls.
    worker2 = agent.make_app(db)
    agent.register(worker2, str(root), agent.ScriptedModel())
    worker2.run_until_idle()
    worker2.close()
    assert agent.ScriptedModel.calls == 5

    snap = app.fetch_task_result(task_id)
    assert snap.state == "completed"
    assert snap.result["turns"] == 5
    assert (root / "notes" / "summary.md").read_text().startswith("README read")

    names = [n for n, _ in agent.transcript(app, task_id)]
    assert "turn:0" in names and "tool:toolu_0" in names and "$awaitEvent:answer:%s:toolu_2" % task_id in names

    agent.main(["--db", db, "show", task_id])
    assert "human <- 'yes please'" in capsys.readouterr().out
    app.close()


def test_agent_declined_question_ends_without_side_effect(agent, tmp_path):
    db = str(tmp_path / "a.db")
    root = tmp_path / "root"
    root.mkdir()
    (root / "README.md").write_text("# demo\n")
    app = agent.make_app(db)
    agent.register(app, str(root), agent.ScriptedModel())
    task_id = app.spawn("agent", {"prompt": "look around"})["task_id"]
    app.run_until_idle()
    event, _ = agent.pending_question(app, task_id)
    app.emit_event(event, "no")
    app.run_until_idle()
    assert app.fetch_task_result(task_id).result["text"] == "Understood, no note."
    assert not (root / "notes").exists()
    app.close()


def test_agent_tool_cannot_escape_root(agent, tmp_path):
    db = str(tmp_path / "a.db")
    root = tmp_path / "root"
    root.mkdir()
    app = agent.make_app(db)

    class Escaper:
        def __call__(self, messages):
            if len(messages) == 1:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "t1", "name": "read_file", "input": {"path": "../secret"}}]}
            assert messages[-1]["content"][0]["is_error"] is True
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": messages[-1]["content"][0]["content"]}]}

    agent.register(app, str(root), Escaper())
    task_id = app.spawn("agent", {"prompt": "x"})["task_id"]
    app.run_until_idle()
    assert "outside the root" in app.fetch_task_result(task_id).result["text"]
    app.close()


def test_agent_heartbeat_extends_lease_during_model_call(agent, tmp_path):
    import time

    db = str(tmp_path / "hb.db")
    app = agent.make_app(db)
    task = app.spawn("agent", {"prompt": "x"}, queue=agent.QUEUE)
    claim = app.engine.claim_task(agent.QUEUE, "w", agent.CLAIM_TIMEOUT)[0]
    before = app.engine.get_run(agent.QUEUE, claim["run_id"])["claim_expires_at"]

    hb = agent.Heartbeat(agent.Engine(db), claim["run_id"], every=0.05)
    with hb:
        time.sleep(0.3)  # a "model call" in flight
    after = app.engine.get_run(agent.QUEUE, claim["run_id"])["claim_expires_at"]
    assert after > before
    assert not hb.cancelled

    # Cancelled mid-call: the heartbeat notices and the handler would stop.
    app.cancel_task(task["task_id"])
    hb2 = agent.Heartbeat(agent.Engine(db), claim["run_id"], every=0.05)
    with hb2:
        time.sleep(0.2)
    assert hb2.cancelled
    app.close()


RELEASE = EXAMPLES / "release" / "release.py"


def _release(db, *args, timeout=60):
    return subprocess.run(
        [sys.executable, str(RELEASE), "--db", db, *args],
        capture_output=True, text=True, timeout=timeout, env={**os.environ, "PYTHONPATH": str(EXAMPLES.parent)},
    )


def test_release_survives_interrupt_parks_and_publishes_once(tmp_path):
    import signal
    import subprocess as sp
    import time

    db = str(tmp_path / "release.db")
    proj = str(tmp_path / "proj")
    assert _release(db, "init", proj).returncode == 0
    run = ["--project", proj, "--window", "600"]

    # Run 1: interrupt while the slow tests are running.
    p = sp.Popen([sys.executable, str(RELEASE), "--db", db, "run", "1.4.0", *run],
                 stdout=sp.PIPE, stderr=sp.STDOUT, text=True, env={**os.environ, "PYTHONPATH": str(EXAMPLES.parent)})
    time.sleep(2.0)
    p.send_signal(signal.SIGINT)
    out1, _ = p.communicate(timeout=30)
    assert p.returncode == 130, out1
    assert "[build  ]" in out1 and "[test   ] " in out1 and "exit" not in out1.split("[test   ]")[1]
    assert "interrupted" in out1

    # Run 2: build is cached, tests re-run, then it parks on approval.
    p = sp.Popen([sys.executable, str(RELEASE), "--db", db, "run", "1.4.0", *run],
                 stdout=sp.PIPE, stderr=sp.STDOUT, text=True, env={**os.environ, "PYTHONPATH": str(EXAMPLES.parent)})
    deadline = time.time() + 40
    out2 = ""
    while time.time() < deadline and "[approve] waiting" not in out2:
        out2 += p.stdout.readline()
    assert "[build  ] cached" in out2, out2
    assert "[tag    ]" in out2
    assert "resuming task" in out2

    # Approve from "another terminal" while run 2 is still parked.
    r = _release(db, "approve", "1.4.0", "--by", "tester")
    assert r.returncode == 0, r.stdout + r.stderr
    rest, _ = p.communicate(timeout=30)
    out2 += rest
    assert p.returncode == 0, out2
    assert "approved by tester" in out2 and "[publish] dist/app-1.4.0.zip" in out2
    assert (tmp_path / "proj" / "dist" / "app-1.4.0.zip").exists()
    tags = sp.run(["git", "tag"], cwd=proj, capture_output=True, text=True).stdout.split()
    assert tags == ["v1.4.0"]

    # Run 3: the same version is a no-op; nothing is published twice.
    r = _release(db, "run", "1.4.0", *run)
    assert r.returncode == 0 and "resuming task" in r.stdout and "released" in r.stdout
    assert "[build  ]" not in r.stdout  # the handler never ran again

    r = _release(db, "status", "1.4.0")
    assert "completed" in r.stdout and "publish" in r.stdout and "build" in r.stdout


def test_release_failed_tests_end_the_release_without_retry(tmp_path):
    db = str(tmp_path / "release.db")
    proj = str(tmp_path / "proj")
    _release(db, "init", proj)
    (tmp_path / "proj" / "tests" / "test_app.py").write_text(
        "import unittest\nclass T(unittest.TestCase):\n    def test_red(self): self.fail('nope')\n"
    )
    r = _release(db, "run", "2.0.0", "--project", proj)
    assert r.returncode == 1
    assert "release 2.0.0: failed" in r.stdout
    assert r.stdout.count("[test   ] " + sys.executable) == 1, "a red test suite must not be retried"
    assert not (tmp_path / "proj" / "dist").exists()
