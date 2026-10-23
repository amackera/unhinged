"""Engine tests ported from Absurd's ``tests/`` directory.

Each test here has a counterpart against the Postgres implementation; the
assertions are the same, only the client calls changed.
"""

from datetime import datetime, timedelta, timezone

import pytest

from unhinged import Engine, InvalidRetryStrategy, RunAlreadyFailed, TaskCancelled, UnhingedError, retry_delay_seconds

UTC = timezone.utc


# ---------------------------------------------------------------- queues


def test_create_queue_is_idempotent_and_listed(engine: Engine):
    engine.create_queue("jobs")
    engine.create_queue("jobs")
    assert engine.list_queues() == ["jobs"]
    engine.drop_queue("jobs")
    engine.drop_queue("jobs")
    assert engine.list_queues() == []


def test_queue_name_validation(engine: Engine):
    with pytest.raises(UnhingedError):
        engine.create_queue("")
    with pytest.raises(UnhingedError):
        engine.create_queue("x" * 58)
    engine.create_queue('we"ird')  # identifiers are quoted properly
    assert engine.spawn_task('we"ird', "t", {})["created"]


def test_queue_policy_roundtrip(engine: Engine):
    engine.create_queue("p")
    assert engine.get_queue_policy("p") == {"queue_name": "p", "cleanup_ttl": 2592000, "cleanup_limit": 1000}
    engine.set_queue_policy("p", {"cleanup_ttl": "2 days", "cleanup_limit": 5})
    assert engine.get_queue_policy("p")["cleanup_ttl"] == 172800
    with pytest.raises(UnhingedError):
        engine.set_queue_policy("p", {"partition_lookahead": "1 day"})


# ---------------------------------------------------------------- retry delay


def test_retry_delay_seconds():
    assert retry_delay_seconds(None, 1) == 0
    assert retry_delay_seconds({"kind": "none"}, 3) == 0
    assert retry_delay_seconds({"kind": "fixed"}, 3) == 60
    assert retry_delay_seconds({"kind": "fixed", "base_seconds": 7}, 3) == 7
    assert retry_delay_seconds({"kind": "exponential"}, 1) == 30
    assert retry_delay_seconds({"kind": "exponential"}, 3) == 120
    assert retry_delay_seconds({"kind": "exponential", "base_seconds": 1, "factor": 10}, 1000) == 86400
    assert retry_delay_seconds({"kind": "exponential", "base_seconds": 1, "factor": 10, "max_seconds": 50}, 4) == 50
    for bad in ({"kind": "weird"}, {"kind": "fixed", "base_seconds": 1e9}, {"kind": "exponential", "factor": -1}, []):
        with pytest.raises(InvalidRetryStrategy):
            retry_delay_seconds(bad, 1)


# ---------------------------------------------------------------- state transitions


def test_spawn_and_claim(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 4, 1, 10, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    spawn = engine.spawn_task("q", "hello", {"n": 1}, {"headers": {"trace": "abc"}, "max_attempts": 3})
    assert spawn["created"] and spawn["attempt"] == 1

    task = engine.get_task("q", spawn["task_id"])
    assert task["state"] == "pending"
    assert task["params"] == {"n": 1}
    assert task["enqueue_at"] == base

    claimed = engine.claim_task("q", "w1", 60)
    assert len(claimed) == 1
    c = claimed[0]
    assert c["run_id"] == spawn["run_id"]
    assert c["task_name"] == "hello"
    assert c["params"] == {"n": 1}
    assert c["headers"] == {"trace": "abc"}
    assert c["max_attempts"] == 3

    run = engine.get_run("q", c["run_id"])
    assert run["state"] == "running"
    assert run["claimed_by"] == "w1"
    assert run["claim_expires_at"] == base + timedelta(seconds=60)
    assert engine.get_task("q", spawn["task_id"])["first_started_at"] == base

    assert engine.claim_task("q", "w2") == []

    engine.complete_run("q", c["run_id"], {"ok": True})
    assert engine.get_task_result("q", spawn["task_id"]) == {
        "task_id": spawn["task_id"],
        "state": "completed",
        "result": {"ok": True},
        "failure_reason": None,
    }


def test_schedule_run_moves_task_between_running_and_sleeping(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 4, 1, 10, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    spawn = engine.spawn_task("q", "schedule-task", {"step": "start"})
    run_id = engine.claim_task("q", "worker-1", 120)[0]["run_id"]

    wake_at = base + timedelta(minutes=5)
    engine.schedule_run("q", run_id, wake_at)
    run = engine.get_run("q", run_id)
    assert run["state"] == "sleeping"
    assert run["available_at"] == wake_at
    assert run["wake_event"] is None
    assert engine.get_task("q", spawn["task_id"])["state"] == "sleeping"

    engine.set_fake_now(wake_at - timedelta(seconds=1))
    assert engine.claim_task("q", "worker-1", 120) == []

    engine.set_fake_now(wake_at)
    resumed = engine.claim_task("q", "worker-1", 120)[0]
    assert resumed["run_id"] == run_id
    assert resumed["attempt"] == 1
    assert engine.get_run("q", run_id)["started_at"] == wake_at


def test_claim_timeout_releases_run_to_new_worker(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 4, 2, 9, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    spawn = engine.spawn_task("q", "lease", {"step": "attempt"})
    run_id = engine.claim_task("q", "worker-a", 30)[0]["run_id"]

    engine.set_fake_now(base + timedelta(minutes=5))
    assert engine.get_run("q", run_id)["state"] == "running"

    second = engine.claim_task("q", "worker-b", 45)[0]
    assert second["run_id"] != run_id
    assert second["attempt"] == 2
    new_run = engine.get_run("q", second["run_id"])
    assert new_run["claimed_by"] == "worker-b"
    assert new_run["claim_expires_at"] == base + timedelta(minutes=5, seconds=45)

    expired = engine.get_run("q", run_id)
    assert expired["state"] == "failed"
    assert expired["claimed_by"] == "worker-a"
    assert expired["claim_expires_at"] == base + timedelta(seconds=30)
    assert expired["failure_reason"]["name"] == "$ClaimTimeout"
    assert expired["failure_reason"]["workerId"] == "worker-a"
    assert expired["failure_reason"]["attempt"] == 1
    assert "claimExpiredAt" in expired["failure_reason"]
    assert engine.get_task("q", spawn["task_id"])["attempts"] == 2


def test_claim_expired_sweep_is_bounded_by_qty(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 7, 2, 9, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    for i in range(5):
        engine.spawn_task("q", f"task-{i}", {"i": i})
    claimed = engine.claim_task("q", "worker-a", 1, 5)
    assert len(claimed) == 5
    ids = [c["run_id"] for c in claimed]

    engine.set_fake_now(base + timedelta(seconds=10))
    engine.claim_task("q", "sweeper", 30, 1)
    assert [engine.get_run("q", r)["state"] for r in ids].count("failed") == 1
    engine.claim_task("q", "sweeper", 30, 2)
    assert [engine.get_run("q", r)["state"] for r in ids].count("failed") == 3


def test_fail_run_without_strategy_requeues_immediately(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 4, 4, 7, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    spawn = engine.spawn_task("q", "no-strategy", {"payload": 1})
    claim = engine.claim_task("q", "worker", 60)[0]
    engine.fail_run("q", claim["run_id"], {"message": "boom"})

    first, second = engine.get_runs("q", spawn["task_id"])
    assert first["state"] == "failed"
    assert first["failure_reason"] == {"message": "boom"}
    assert first["claim_expires_at"] == base + timedelta(seconds=60)
    assert second["state"] == "pending"
    assert second["attempt"] == 2
    assert second["available_at"] == base
    task = engine.get_task("q", spawn["task_id"])
    assert task["state"] == "pending" and task["attempts"] == 2


def test_fail_run_respects_explicit_retry_at(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 4, 5, 6, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    spawn = engine.spawn_task("q", "retry-at", {"payload": 1})
    claim = engine.claim_task("q", "worker")[0]
    future = base + timedelta(minutes=15)
    engine.fail_run("q", claim["run_id"], {"message": "later"}, retry_at=future)
    _, second = engine.get_runs("q", spawn["task_id"])
    assert second["state"] == "sleeping"
    assert second["available_at"] == future
    assert engine.get_task("q", spawn["task_id"])["state"] == "sleeping"


def test_fail_run_exhausts_max_attempts(engine: Engine):
    engine.create_queue("q")
    spawn = engine.spawn_task("q", "t", {}, {"max_attempts": 2, "retry_strategy": {"kind": "fixed", "base_seconds": 0}})
    c1 = engine.claim_task("q")[0]
    engine.fail_run("q", c1["run_id"], {"message": "1"})
    c2 = engine.claim_task("q")[0]
    assert c2["attempt"] == 2
    engine.fail_run("q", c2["run_id"], {"message": "2"})
    assert len(engine.get_runs("q", spawn["task_id"])) == 2
    result = engine.get_task_result("q", spawn["task_id"])
    assert result["state"] == "failed"
    assert result["failure_reason"] == {"message": "2"}
    with pytest.raises(RunAlreadyFailed):
        engine.complete_run("q", c2["run_id"], {})
    with pytest.raises(RunAlreadyFailed):
        engine.fail_run("q", c2["run_id"], {})


# ---------------------------------------------------------------- retry & cancellation


def test_retry_flow_and_cancellation(engine: Engine):
    engine.create_queue("q")
    start = datetime(2024, 1, 1, tzinfo=UTC)
    engine.set_fake_now(start)
    spawn = engine.spawn_task(
        "q", "flaky", {"payload": 1},
        {"retry_strategy": {"kind": "fixed", "base_seconds": 30}, "max_attempts": 4, "cancellation": {"max_duration": 90}},
    )
    first = engine.claim_task("q", "worker-1")[0]
    engine.fail_run("q", first["run_id"], {"message": "boom"})
    runs = engine.get_runs("q", spawn["task_id"])
    assert [r["state"] for r in runs] == ["failed", "sleeping"]
    assert runs[1]["available_at"] == start + timedelta(seconds=30)

    engine.set_fake_now(start + timedelta(seconds=30))
    second = engine.claim_task("q", "worker-1")[0]
    assert second["attempt"] == 2

    engine.set_fake_now(start + timedelta(seconds=91))
    engine.fail_run("q", second["run_id"], {"message": "still boom"})
    task = engine.get_task("q", spawn["task_id"])
    assert task["state"] == "cancelled"
    assert task["cancelled_at"] is not None
    assert len(engine.get_runs("q", spawn["task_id"])) == 2


def test_claim_time_cancellation_preserves_terminal_run_history(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 7, 1, 10, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    spawn = engine.spawn_task(
        "q", "task", {"value": 1},
        {"retry_strategy": {"kind": "fixed", "base_seconds": 0}, "max_attempts": 3, "cancellation": {"max_duration": 1}},
    )
    run1 = engine.claim_task("q", "worker-1", 60)[0]["run_id"]
    engine.fail_run("q", run1, {"message": "retry"})
    engine.set_fake_now(base + timedelta(seconds=2))
    assert engine.claim_task("q", "worker-2") == []
    runs = engine.get_runs("q", spawn["task_id"])
    assert [r["state"] for r in runs] == ["failed", "cancelled"]
    assert engine.get_task("q", spawn["task_id"])["state"] == "cancelled"


def test_max_delay_cancels_unstarted_task(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 7, 1, 10, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    spawn = engine.spawn_task("q", "late", {}, {"cancellation": {"max_delay": 60}})
    engine.set_fake_now(base + timedelta(seconds=59))
    # Not yet; but don't claim it either or first_started_at gets set.
    engine.set_fake_now(base + timedelta(seconds=60))
    assert engine.claim_task("q") == []
    assert engine.get_task("q", spawn["task_id"])["state"] == "cancelled"


def test_manual_cancel_variants(engine: Engine):
    engine.create_queue("q")
    pending = engine.spawn_task("q", "p", {})
    engine.cancel_task("q", pending["task_id"])
    assert engine.get_task("q", pending["task_id"])["state"] == "cancelled"
    assert engine.claim_task("q") == []

    running = engine.spawn_task("q", "r", {})
    claim = engine.claim_task("q", "worker-1")[0]
    engine.cancel_task("q", running["task_id"])
    with pytest.raises(TaskCancelled):
        engine.set_task_checkpoint_state("q", running["task_id"], "step1", {"v": 1}, claim["run_id"])
    with pytest.raises(TaskCancelled):
        engine.await_event("q", running["task_id"], claim["run_id"], "wait", "ev")
    with pytest.raises(TaskCancelled):
        engine.extend_claim("q", claim["run_id"], 30)
    with pytest.raises(TaskCancelled):
        engine.complete_run("q", claim["run_id"], {"ok": True})
    with pytest.raises(TaskCancelled):
        engine.fail_run("q", claim["run_id"], {"ok": True})

    first = engine.get_task("q", running["task_id"])["cancelled_at"]
    engine.cancel_task("q", running["task_id"])
    assert engine.get_task("q", running["task_id"])["cancelled_at"] == first

    done = engine.spawn_task("q", "d", {})
    engine.complete_run("q", engine.claim_task("q")[0]["run_id"], 1)
    engine.cancel_task("q", done["task_id"])
    assert engine.get_task("q", done["task_id"])["state"] == "completed"

    with pytest.raises(UnhingedError, match="not found"):
        engine.cancel_task("q", "019a32d3-8425-7ae2-a5af-2f17a6707666")


def test_cancel_sleeping_task_removes_wait(engine: Engine):
    engine.create_queue("q")
    spawn = engine.spawn_task("q", "s", {})
    claim = engine.claim_task("q")[0]
    suspend, _ = engine.await_event("q", spawn["task_id"], claim["run_id"], "wait", "ev", 300)
    assert suspend
    assert engine.count("w", "q") == 1
    engine.cancel_task("q", spawn["task_id"])
    assert engine.get_run("q", claim["run_id"])["state"] == "cancelled"
    assert engine.count("w", "q") == 0


# ---------------------------------------------------------------- retry_task


def test_retry_task_in_place_and_spawn_new(engine: Engine):
    engine.create_queue("q")
    spawn = engine.spawn_task("q", "t", {"x": 1}, {"max_attempts": 1, "headers": {"h": 1}})
    claim = engine.claim_task("q")[0]
    with pytest.raises(UnhingedError, match="not currently failed"):
        engine.retry_task("q", spawn["task_id"])
    engine.fail_run("q", claim["run_id"], {"m": "x"})

    retried = engine.retry_task("q", spawn["task_id"])
    assert retried["task_id"] == spawn["task_id"]
    assert retried["attempt"] == 2 and retried["created"] is False
    task = engine.get_task("q", spawn["task_id"])
    assert task["state"] == "pending" and task["max_attempts"] == 2
    claim2 = engine.claim_task("q")[0]
    assert claim2["attempt"] == 2
    engine.fail_run("q", claim2["run_id"], {"m": "y"})

    with pytest.raises(UnhingedError, match="must be greater"):
        engine.retry_task("q", spawn["task_id"], {"max_attempts": 2})

    fresh = engine.retry_task("q", spawn["task_id"], {"spawn_new": True, "max_attempts": 7})
    assert fresh["created"] and fresh["task_id"] != spawn["task_id"]
    new_task = engine.get_task("q", fresh["task_id"])
    assert new_task["params"] == {"x": 1}
    assert new_task["headers"] == {"h": 1}
    assert new_task["max_attempts"] == 7


# ---------------------------------------------------------------- idempotency


def test_idempotent_spawn(engine: Engine):
    engine.create_queue("q")
    a = engine.spawn_task("q", "t", {"a": 1}, {"idempotency_key": "k1"})
    b = engine.spawn_task("q", "t", {"a": 2}, {"idempotency_key": "k1"})
    assert a["created"] and not b["created"]
    assert b["task_id"] == a["task_id"] and b["run_id"] == a["run_id"] and b["attempt"] == 1
    assert engine.count("t", "q") == 1
    c = engine.spawn_task("q", "t", {"a": 3}, {"idempotency_key": "k2"})
    assert c["created"]
    # NULL keys never collide
    engine.spawn_task("q", "t", {})
    engine.spawn_task("q", "t", {})
    assert engine.count("t", "q") == 4


# ---------------------------------------------------------------- checkpoints


def test_checkpoints_and_claim_extension(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 3, 1, tzinfo=UTC)
    engine.set_fake_now(base)
    spawn = engine.spawn_task("q", "t", {})
    claim = engine.claim_task("q", "w", 30)[0]

    assert engine.get_task_checkpoint_state("q", spawn["task_id"], "s1") is None
    engine.set_task_checkpoint_state("q", spawn["task_id"], "s1", {"v": 1}, claim["run_id"], extend_claim_by=90)
    cp = engine.get_task_checkpoint_state("q", spawn["task_id"], "s1")
    assert cp["state"] == {"v": 1}
    assert cp["status"] == "committed"
    assert cp["owner_run_id"] == claim["run_id"]
    assert engine.get_run("q", claim["run_id"])["claim_expires_at"] == base + timedelta(seconds=90)

    engine.set_task_checkpoint_state("q", spawn["task_id"], "s1", None, claim["run_id"])
    assert engine.get_task_checkpoint_state("q", spawn["task_id"], "s1")["state"] is None

    with pytest.raises(UnhingedError, match="step_name"):
        engine.set_task_checkpoint_state("q", spawn["task_id"], " ", 1, claim["run_id"])

    engine.extend_claim("q", claim["run_id"], 10)
    assert engine.get_run("q", claim["run_id"])["claim_expires_at"] == base + timedelta(seconds=10)
    with pytest.raises(UnhingedError):
        engine.extend_claim("q", claim["run_id"], 0)


def test_checkpoints_survive_retries_and_hide_later_attempts(engine: Engine):
    engine.create_queue("q")
    spawn = engine.spawn_task("q", "t", {}, {"retry_strategy": {"kind": "none"}})
    c1 = engine.claim_task("q")[0]
    engine.set_task_checkpoint_state("q", spawn["task_id"], "a", "from-1", c1["run_id"])
    engine.fail_run("q", c1["run_id"], {"m": "x"})
    c2 = engine.claim_task("q")[0]
    assert c2["attempt"] == 2
    states = engine.get_task_checkpoint_states("q", spawn["task_id"], c2["run_id"])
    assert [(s["checkpoint_name"], s["state"]) for s in states] == [("a", "from-1")]

    engine.set_task_checkpoint_state("q", spawn["task_id"], "b", "from-2", c2["run_id"])
    # An older run can't overwrite a newer run's checkpoint, and can't write at all once failed.
    with pytest.raises(RunAlreadyFailed):
        engine.set_task_checkpoint_state("q", spawn["task_id"], "b", "stale", c1["run_id"])
    assert engine.get_task_checkpoint_state("q", spawn["task_id"], "b")["state"] == "from-2"
    # Run 1 only sees attempt-1 checkpoints.
    visible_to_1 = engine.get_task_checkpoint_states("q", spawn["task_id"], c1["run_id"])
    assert [s["checkpoint_name"] for s in visible_to_1] == ["a"]

    with pytest.raises(UnhingedError, match="does not belong"):
        engine.get_task_checkpoint_states("q", "nope", c2["run_id"])


# ---------------------------------------------------------------- events


def test_await_and_emit_event_flow(engine: Engine):
    engine.create_queue("q")
    now = datetime(2024, 2, 1, 12, 0, tzinfo=UTC)
    engine.set_fake_now(now)
    spawn = engine.spawn_task("q", "waiter", {"step": 1})
    claimed = engine.claim_task("q")[0]

    suspend, payload = engine.await_event("q", spawn["task_id"], claimed["run_id"], "wait", "test-event", 60)
    assert suspend is True and payload is None
    run = engine.get_run("q", claimed["run_id"])
    assert run["state"] == "sleeping"
    assert run["wake_event"] == "test-event"
    assert run["available_at"] == now + timedelta(seconds=60)
    assert run["claimed_by"] is None

    engine.emit_event("q", "test-event", {"value": 42})
    run = engine.get_run("q", claimed["run_id"])
    assert run["state"] == "pending"
    assert run["event_payload"] == {"value": 42}
    assert engine.get_task("q", spawn["task_id"])["state"] == "pending"
    assert engine.count("w", "q") == 0

    replay = engine.claim_task("q")[0]
    assert replay["run_id"] == spawn["run_id"]
    assert replay["event_payload"] == {"value": 42}
    suspend, payload = engine.await_event("q", spawn["task_id"], replay["run_id"], "wait", "test-event", 60)
    assert suspend is False and payload == {"value": 42}
    assert engine.get_checkpoint("q", spawn["task_id"], "wait")["state"] == {"value": 42}


def test_emit_event_is_first_write_wins(engine: Engine):
    engine.create_queue("q")
    t0 = datetime(2024, 2, 2, 12, 0, tzinfo=UTC)
    engine.set_fake_now(t0)
    engine.emit_event("q", "stable", {"value": 1})
    engine.set_fake_now(t0 + timedelta(seconds=30))
    engine.emit_event("q", "stable", {"value": 2})
    row = engine.connection.execute('select payload, emitted_at from "e_q" where event_name = ?', ("stable",)).fetchone()
    assert row["payload"] == '{"value":1}'
    assert row["emitted_at"] == "2024-02-02T12:00:00.000000"

    spawn = engine.spawn_task("q", "waiter", {})
    claim = engine.claim_task("q")[0]
    suspend, payload = engine.await_event("q", spawn["task_id"], claim["run_id"], "wait", "stable", 60)
    assert suspend is False and payload == {"value": 1}


def test_emit_event_with_null_payload_still_wakes(engine: Engine):
    engine.create_queue("q")
    spawn = engine.spawn_task("q", "waiter", {})
    claim = engine.claim_task("q")[0]
    assert engine.await_event("q", spawn["task_id"], claim["run_id"], "wait", "ping")[0] is True
    engine.emit_event("q", "ping")  # payload None -> JSON null, which counts as emitted
    assert engine.get_run("q", claim["run_id"])["state"] == "pending"
    replay = engine.claim_task("q")[0]
    assert engine.await_event("q", spawn["task_id"], replay["run_id"], "wait", "ping") == (False, None)


def test_event_timeout_wakes_sleeping_run_and_does_not_loop(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 5, 1, 10, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    spawn = engine.spawn_task("q", "waiter", {})
    run_id = engine.claim_task("q")[0]["run_id"]
    assert engine.await_event("q", spawn["task_id"], run_id, "wait", "foo", 10) == (True, None)
    assert engine.count("w", "q") == 1
    assert engine.get_run("q", run_id)["available_at"] == base + timedelta(seconds=10)

    engine.set_fake_now(base + timedelta(seconds=15))
    resumed = engine.claim_task("q")[0]
    assert resumed["run_id"] == run_id
    assert resumed["wake_event"] == "foo"
    assert resumed["event_payload"] is None
    assert engine.count("w", "q") == 0

    assert engine.await_event("q", spawn["task_id"], run_id, "wait", "foo", 10) == (False, None)
    run = engine.get_run("q", run_id)
    assert run["state"] == "running" and run["wake_event"] is None
    assert engine.count("w", "q") == 0


def test_await_event_without_timeout_sleeps_forever(engine: Engine):
    engine.create_queue("q")
    spawn = engine.spawn_task("q", "waiter", {})
    run_id = engine.claim_task("q")[0]["run_id"]
    engine.await_event("q", spawn["task_id"], run_id, "wait", "never")
    engine.set_fake_now(datetime(2999, 1, 1, tzinfo=UTC))
    assert engine.claim_task("q") == []


def test_emit_wakes_many_waiters_but_not_expired_ones(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 5, 1, 10, 0, tzinfo=UTC)
    engine.set_fake_now(base)
    runs = []
    for i in range(3):
        s = engine.spawn_task("q", "w", {"i": i})
        rid = engine.claim_task("q")[0]["run_id"]
        engine.await_event("q", s["task_id"], rid, "wait", "fanout", None if i < 2 else 5)
        runs.append(rid)
    engine.set_fake_now(base + timedelta(seconds=10))
    engine.emit_event("q", "fanout", "go")
    states = [engine.get_run("q", r)["state"] for r in runs]
    assert states == ["pending", "pending", "sleeping"]
    assert engine.count("w", "q") == 0


def test_await_event_requires_running_run(engine: Engine):
    engine.create_queue("q")
    spawn = engine.spawn_task("q", "w", {})
    with pytest.raises(UnhingedError, match="must be running"):
        engine.await_event("q", spawn["task_id"], spawn["run_id"], "wait", "x")
    with pytest.raises(UnhingedError, match="event_name"):
        engine.await_event("q", spawn["task_id"], spawn["run_id"], "wait", "")


# ---------------------------------------------------------------- cleanup


def test_cleanup_tasks_and_events(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    engine.set_fake_now(base)
    done = engine.spawn_task("q", "t", {})
    claim = engine.claim_task("q")[0]
    engine.set_task_checkpoint_state("q", done["task_id"], "s", 1, claim["run_id"])
    engine.complete_run("q", claim["run_id"], "ok")

    failed = engine.spawn_task("q", "t", {}, {"max_attempts": 1})
    engine.fail_run("q", engine.claim_task("q")[0]["run_id"], {"m": "x"})

    cancelled = engine.spawn_task("q", "t", {})
    engine.cancel_task("q", cancelled["task_id"])

    live = engine.spawn_task("q", "t", {})
    engine.emit_event("q", "old")

    engine.set_fake_now(base + timedelta(days=10))
    engine.emit_event("q", "new")

    engine.set_fake_now(base + timedelta(days=20))
    assert engine.cleanup_tasks("q", 30 * 86400) == 0
    assert engine.cleanup_tasks("q", 15 * 86400, limit=2) == 2
    assert engine.cleanup_tasks("q", 15 * 86400) == 1
    assert engine.count("t", "q") == 1
    assert engine.get_task("q", live["task_id"]) is not None
    assert engine.count("c", "q") == 0 and engine.count("r", "q") == 1

    assert engine.cleanup_events("q", 15 * 86400) == 1
    assert engine.count("e", "q") == 1

    with pytest.raises(UnhingedError):
        engine.cleanup_tasks("q", -1)

    engine.set_queue_policy("q", {"cleanup_ttl": 0})
    engine.emit_event("q", "third")
    engine.set_fake_now(base + timedelta(days=21))
    report = engine.cleanup_all_queues()
    assert report == [{"queue_name": "q", "tasks_deleted": 0, "events_deleted": 2}]
    with pytest.raises(UnhingedError, match="does not exist"):
        engine.cleanup_all_queues("missing")


# ---------------------------------------------------------------- misc


def test_claim_qty_and_ordering(engine: Engine):
    engine.create_queue("q")
    base = datetime(2024, 1, 1, tzinfo=UTC)
    ids = []
    for i in range(4):
        engine.set_fake_now(base + timedelta(seconds=i))
        ids.append(engine.spawn_task("q", "t", {"i": i})["run_id"])
    engine.set_fake_now(base + timedelta(minutes=1))
    first = engine.claim_task("q", "w", 10, 3)
    assert [c["run_id"] for c in first] == ids[:3]
    assert [c["params"]["i"] for c in first] == [0, 1, 2]
    assert [c["run_id"] for c in engine.claim_task("q", "w", 10, 3)] == ids[3:]


def test_claim_with_zero_timeout_never_expires(engine: Engine):
    engine.create_queue("q")
    engine.spawn_task("q", "t", {})
    claim = engine.claim_task("q", "w", 0)[0]
    assert engine.get_run("q", claim["run_id"])["claim_expires_at"] is None
    engine.set_fake_now(datetime(2999, 1, 1, tzinfo=UTC))
    assert engine.claim_task("q") == []
    with pytest.raises(UnhingedError, match="active claim"):
        engine.extend_claim("q", claim["run_id"], 5)


def test_spawn_validation(engine: Engine):
    engine.create_queue("q")
    with pytest.raises(UnhingedError, match="task_name"):
        engine.spawn_task("q", " ", {})
    with pytest.raises(UnhingedError, match="max_attempts"):
        engine.spawn_task("q", "t", {}, {"max_attempts": 0})
    with pytest.raises(InvalidRetryStrategy):
        engine.spawn_task("q", "t", {}, {"retry_strategy": {"kind": "bogus"}})
    assert engine.get_task("q", engine.spawn_task("q", "t", None)["task_id"])["params"] is None


def test_durability_pragmas(tmp_path):
    e = Engine(str(tmp_path / "a.db"))
    assert e.connection.execute("pragma synchronous").fetchone()[0] == 2  # FULL
    assert e.connection.execute("pragma fullfsync").fetchone()[0] == 0
    e.close()
    e = Engine(str(tmp_path / "b.db"), synchronous="normal", fullfsync=True)
    assert e.connection.execute("pragma synchronous").fetchone()[0] == 1
    assert e.connection.execute("pragma fullfsync").fetchone()[0] == 1
    e.close()
    with pytest.raises(ValueError):
        Engine(str(tmp_path / "c.db"), synchronous="sometimes")
