"""Several worker processes on one file, SIGKILLed at random. See examples/chaos.py."""

import importlib.util
import sys
from pathlib import Path

CHAOS = Path(__file__).resolve().parent.parent / "examples" / "chaos.py"


def test_workers_sharing_one_file_survive_random_kills(tmp_path):
    spec = importlib.util.spec_from_file_location("chaos_example", CHAOS)
    chaos = importlib.util.module_from_spec(spec)
    sys.modules["chaos_example"] = chaos
    spec.loader.exec_module(chaos)

    report = chaos.run(
        str(tmp_path / "chaos.db"),
        workers=4,
        tasks=150,
        steps=5,
        step_ms=10,
        seconds=10,
        kill_every=0.15,
        timeout=120,
        quiet=True,
    )

    assert report["completed"] == report["tasks"], report
    assert report["shared_runs"] == [], "a run was executed by two processes"
    assert report["double_in_run"] == [], "a step ran twice within one run"
    assert report["unexplained_duplicates"] == [], "a step re-ran without a reclaimed lease"
    assert report["missing_steps"] == []
    assert report["worker_errors"] == ""
    assert report["integrity"] == "ok"
    assert report["kills"] > 0, "the supervisor never killed anything; the run drained too fast to test"
    # The at-least-once window exists; what we check is that every re-run is
    # accounted for by a lease the engine timed out and reclaimed.
    assert report["duplicate_executions"] <= report["runs_timed_out"]
