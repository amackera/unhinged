"""``UnhingedDurability`` against a real Unhinged database.

Structure follows Pydantic AI Harness's own tests for ``AbsurdDurability``: each test enters a
real task context the way a worker does, and a replay fails the run and re-claims the task, so
step naming, encounter-order suffixes and checkpoint storage are the engine's own.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager

import pytest

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent
from pydantic_ai.exceptions import UserError
from pydantic_ai.messages import (
    AgentStreamEvent,
    FunctionToolCallEvent,
    ModelMessage,
    ModelResponse,
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.tools import RunContext
from pydantic_ai.toolsets import ExternalToolset, FunctionToolset

from unhinged import AsyncTaskContext, AsyncUnhinged, Unhinged
from unhinged.pydantic_ai import UnhingedDurability
from unhinged.sdk import _current_task_context

QUEUE = "agents"


# -- entering task contexts the way a worker does -------------------------------


@pytest.fixture
def app(tmp_path):
    a = AsyncUnhinged(str(tmp_path / "agents.db"), queue_name=QUEUE)
    try:
        yield a
    finally:
        a.close()


@asynccontextmanager
async def _entered(app: AsyncUnhinged, task: dict) -> AsyncGenerator[AsyncTaskContext, None]:
    ctx = AsyncTaskContext(app.engine, QUEUE, task, 120)
    token = _current_task_context.set(ctx)
    try:
        yield ctx
    finally:
        _current_task_context.reset(token)


@asynccontextmanager
async def running_task_context(app: AsyncUnhinged, name: str = "noop") -> AsyncGenerator[AsyncTaskContext, None]:
    """Spawn, claim and enter a task; it stays running so it can be re-entered."""
    spawned = app.spawn(name, None, queue=QUEUE, max_attempts=2, retry_strategy={"kind": "none"})
    [task] = app.claim_tasks(batch_size=1)
    assert task["task_id"] == spawned["task_id"]
    async with _entered(app, task) as ctx:
        yield ctx


@asynccontextmanager
async def reenter_running_task(app: AsyncUnhinged, task_id: str) -> AsyncGenerator[AsyncTaskContext, None]:
    """Fail the current run and enter the retry, whose checkpoints come from the file."""
    run_id = app.engine.get_task(QUEUE, task_id)["last_attempt_run"]
    app.engine.fail_run(QUEUE, run_id, {"type": "test.Replay"})
    [task] = app.claim_tasks(batch_size=1)
    assert task["task_id"] == task_id
    async with _entered(app, task) as ctx:
        yield ctx


def checkpoints(app: AsyncUnhinged, task_id: str) -> dict:
    rows = app.engine.connection.execute(
        f'select checkpoint_name, state from "c_{QUEUE}" where task_id = ? order by updated_at, checkpoint_name', (task_id,)
    ).fetchall()
    import json

    return {r["checkpoint_name"]: json.loads(r["state"]) for r in rows}


# -- models -------------------------------------------------------------------


def _make_model(counter: dict | None = None, text: str = "ok") -> FunctionModel:
    tally = counter if counter is not None else {"calls": 0}

    def fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        tally["calls"] += 1
        return ModelResponse(parts=[TextPart(content=text)])

    async def stream_fn(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        tally["calls"] += 1
        yield text

    return FunctionModel(fn, stream_function=stream_fn, model_name="fn")


def _tool_calling_model(*calls: ToolCallPart, counter: dict | None = None) -> FunctionModel:
    tally = counter if counter is not None else {"calls": 0}

    def fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        tally["calls"] += 1
        if any(isinstance(p, ToolReturnPart) for m in messages for p in m.parts):
            return ModelResponse(parts=[TextPart(content="done")])
        return ModelResponse(parts=list(calls))

    return FunctionModel(fn, model_name="fn")


# -- tests --------------------------------------------------------------------


async def test_requires_name():
    with pytest.raises(UserError, match="unique `name`"):
        Agent(_make_model(), capabilities=[UnhingedDurability()])


async def test_toolset_without_id_raises():
    with pytest.raises(UserError, match="need to have a unique `id`"):
        Agent(_make_model(), name="a", toolsets=[FunctionToolset()], capabilities=[UnhingedDurability()])


async def test_run_outside_task_is_transparent():
    counter = {"calls": 0}
    agent = Agent(_make_model(counter), name="a", capabilities=[UnhingedDurability()])
    result = await agent.run("hi")
    assert result.output == "ok" and counter["calls"] == 1


async def test_sync_task_context_is_refused(tmp_path):
    from unhinged.sdk import TaskContext

    sync_app = Unhinged(str(tmp_path / "s.db"), queue_name=QUEUE)
    spawned = sync_app.spawn("noop", None, queue=QUEUE)
    [task] = sync_app.claim_tasks()
    ctx = TaskContext(sync_app.engine, QUEUE, task, 120)
    token = _current_task_context.set(ctx)
    try:
        agent = Agent(_make_model(), name="a", capabilities=[UnhingedDurability()])
        with pytest.raises(UserError, match="async task context"):
            await agent.run("hi")
    finally:
        _current_task_context.reset(token)
        sync_app.close()


async def test_replay_serves_cached_model_response(app):
    counter = {"calls": 0}
    agent = Agent(_make_model(counter), name="crash", capabilities=[UnhingedDurability()])

    async with running_task_context(app, "crash") as ctx:
        first = await agent.run("hi")
    async with reenter_running_task(app, ctx.task_id):
        replayed = await agent.run("hi")

    assert counter["calls"] == 1
    assert replayed.output == first.output == "ok"
    assert list(checkpoints(app, ctx.task_id)) == ["crash__model.request"]


async def test_replay_does_not_rerun_function_tool(app):
    tool_calls = {"calls": 0}
    toolset = FunctionToolset[object](id="tools")

    @toolset.tool_plain
    def charge_card(amount: int) -> str:
        tool_calls["calls"] += 1
        return f"charged {amount}"

    agent = Agent(
        _tool_calling_model(ToolCallPart("charge_card", {"amount": 42})),
        name="billing",
        toolsets=[toolset],
        capabilities=[UnhingedDurability()],
    )
    async with running_task_context(app, "billing") as ctx:
        first = await agent.run("charge it")
    stored = checkpoints(app, ctx.task_id)
    assert stored["billing__function_toolset__tools.call_tool:charge_card"] == "charged 42"  # raw value
    async with reenter_running_task(app, ctx.task_id):
        replayed = await agent.run("charge it")

    assert tool_calls["calls"] == 1
    assert replayed.output == first.output == "done"


async def test_same_tool_called_twice_gets_encounter_suffix(app):
    toolset = FunctionToolset[object](id="shared")

    @toolset.tool_plain
    def echo(value: str) -> str:
        return value

    agent = Agent(
        _tool_calling_model(ToolCallPart("a_echo", {"value": "a"}), ToolCallPart("b_echo", {"value": "b"})),
        name="a",
        toolsets=[toolset.prefixed("a"), toolset.prefixed("b")],
        capabilities=[UnhingedDurability()],
    )
    async with running_task_context(app) as ctx:
        await agent.run("hi")
    stored = checkpoints(app, ctx.task_id)
    step = "a__function_toolset__shared.call_tool:echo"
    assert {k: v for k, v in stored.items() if "call_tool" in k} == {step: "a", f"{step}#2": "b"}


async def test_registered_model_selected_per_run(app):
    primary, cheap = {"calls": 0}, {"calls": 0}
    agent = Agent(
        _make_model(primary, text="primary"),
        name="a",
        capabilities=[UnhingedDurability(models={"cheap": _make_model(cheap, text="cheap")})],
    )
    async with running_task_context(app) as ctx:
        default_result = await agent.run("hi")
        cheap_result = await agent.run("hi", model="cheap")
    async with reenter_running_task(app, ctx.task_id):
        await agent.run("hi")
        replayed = await agent.run("hi", model="cheap")

    assert default_result.output == "primary"
    assert cheap_result.output == replayed.output == "cheap"
    assert primary["calls"] == cheap["calls"] == 1
    assert list(checkpoints(app, ctx.task_id)) == ["a__model.request", "a__model.request.cheap"]


async def test_runtime_toolset_rejected_but_external_allowed(app):
    late = FunctionToolset[object](id="late")
    agent: Agent[object, str] = Agent(_make_model(), name="a", capabilities=[UnhingedDurability()])
    async with running_task_context(app):
        with pytest.raises(UserError, match="cannot be added at runtime with Unhinged"):
            await agent.run("hi", toolsets=[late])
        result = await agent.run("hi", toolsets=[ExternalToolset[object](tool_defs=[])])
    assert result.output == "ok"


async def test_event_stream_handler_is_checkpointed(app):
    events: list[AgentStreamEvent] = []

    async def handler(run_ctx: RunContext[object], stream: AsyncIterable[AgentStreamEvent]) -> None:
        async for event in stream:
            events.append(event)

    async def stream_fn(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str | DeltaToolCalls]:
        if len(messages) == 1:
            yield {0: DeltaToolCall(name="greet", json_args="{}")}
        else:
            yield "done"

    toolset = FunctionToolset[object](id="tools")

    @toolset.tool_plain
    def greet() -> str:
        return "hello"

    agent = Agent(
        FunctionModel(stream_function=stream_fn, model_name="fn"),
        name="a",
        toolsets=[toolset],
        capabilities=[UnhingedDurability(event_stream_handler=handler)],
    )
    async with running_task_context(app) as ctx:
        result = await agent.run("hi")

    assert result.output == "done"
    assert any(isinstance(e, PartStartEvent | PartDeltaEvent) for e in events)
    assert any(isinstance(e, FunctionToolCallEvent) for e in events)
    assert "a__event_stream_handler" in checkpoints(app, ctx.task_id)


async def test_run_stream_inside_task_replays(app):
    counter = {"calls": 0}
    agent = Agent(_make_model(counter), name="a", capabilities=[UnhingedDurability()])
    async with running_task_context(app) as ctx:
        async with agent.run_stream("hi") as result:
            assert await result.get_output() == "ok"
    async with reenter_running_task(app, ctx.task_id):
        async with agent.run_stream("hi") as result:
            assert await result.get_output() == "ok"
    assert counter["calls"] == 1


async def test_end_to_end_through_the_worker(app):
    """The documented usage: a handler runs the agent, the worker dies, a new one resumes."""
    counter = {"calls": 0}
    tool_calls = {"calls": 0}
    toolset = FunctionToolset[object](id="tools")

    @toolset.tool_plain
    def lookup(q: str) -> str:
        tool_calls["calls"] += 1
        return f"answer to {q}"

    agent = Agent(
        _tool_calling_model(ToolCallPart("lookup", {"q": "x"}), counter=counter),
        name="analyst",
        toolsets=[toolset],
        capabilities=[UnhingedDurability()],
    )
    crashed = {"once": False}

    @app.register_task("analyse", default_max_attempts=3)
    async def analyse(params, ctx):
        result = await agent.run(params["prompt"])
        if not crashed["once"]:
            crashed["once"] = True
            raise RuntimeError("worker died after the run, before completing the task")
        return {"output": result.output}

    t = app.spawn("analyse", {"prompt": "hi"}, retry_strategy={"kind": "none"})
    await app.run_until_idle()
    snap = app.fetch_task_result(t["task_id"])
    assert snap.state == "completed" and snap.result == {"output": "done"}
    assert counter["calls"] == 2  # two model turns, made once each
    assert tool_calls["calls"] == 1
    assert list(checkpoints(app, t["task_id"])) == [
        "analyst__model.request",
        "analyst__function_toolset__tools.call_tool:lookup",
        "analyst__model.request#2",
    ]
