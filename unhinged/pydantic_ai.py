"""``UnhingedDurability``: run a Pydantic AI agent durably on Unhinged.

A port of Pydantic AI Harness's ``AbsurdDurability`` to this engine, built on
Pydantic AI's public durable-execution backend builder. Step names and
checkpoint payloads match ``AbsurdDurability``'s, so an agent moved between
the two engines resumes its in-flight work.

    from pydantic_ai import Agent
    from unhinged import AsyncUnhinged
    from unhinged.pydantic_ai import UnhingedDurability

    app = AsyncUnhinged("agents.db", queue_name="agents")
    agent = Agent("anthropic:claude-opus-5-5", name="analyst", capabilities=[UnhingedDurability()])

    @app.register_task("analyse")
    async def analyse(params, ctx):
        result = await agent.run(params["prompt"])
        return {"output": result.output}

Inside an ``AsyncUnhinged`` task handler every model request, MCP call and
function tool call is a checkpointed step, so a worker that dies mid-run
resumes from the last completed step without re-spending tokens. Outside a
task the capability does nothing.
"""

from __future__ import annotations

try:
    import pydantic_ai  # noqa: F401
except ImportError as _err:  # pragma: no cover
    raise ImportError(
        "UnhingedDurability needs pydantic-ai-slim (Python 3.11+); "
        "install it with `pip install 'unhinged[pydantic-ai]'`"
    ) from _err

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, TypeGuard

from pydantic_ai.agent import EventStreamHandler, ParallelExecutionMode
from pydantic_ai.capabilities import WrapRunHandler
from pydantic_ai.durable_exec import (
    JSON_CODEC,
    BaseDurabilityCapability,
    DurabilityEngineSpec,
    DurableOperationId,
    JournalCallableOperationBackend,
    RoleBasedOperationConfig,
    ToolsetCallToolId,
)
from pydantic_ai.exceptions import UserError
from pydantic_ai.models import Model
from pydantic_ai.run import AgentRunResult
from pydantic_ai.tools import AgentDepsT, RunContext

from .aio import AsyncTaskContext
from .sdk import TaskContext, get_current_context

ENGINE_NAME = "Unhinged"

UnhingedParallelExecutionMode = Literal["sequential", "parallel_ordered_events"]
"""Tool-call execution modes usable here. Plain ``'parallel'`` emits tool events in
completion order, which can differ on replay, so it is excluded."""


def current_async_task_context() -> AsyncTaskContext | None:
    """The active async task context, or None outside a task."""
    ctx = get_current_context()
    if ctx is None:
        return None
    if isinstance(ctx, AsyncTaskContext):
        return ctx
    if isinstance(ctx, TaskContext):
        raise UserError(
            f"{ENGINE_NAME} durability needs an async task context, but a synchronous TaskContext is "
            "active. Agent runs are async: run the agent from an AsyncUnhinged handler."
        )
    return None  # pragma: no cover


# -- checkpoint shape, identical to AbsurdDurability's --------------------------
#
# A tool-call checkpoint stores the raw return value, so the table is readable.
# A ``ToolReturn`` has no raw form, so it goes under this envelope key.
_ENVELOPE_KEY = "__pydantic_ai_harness_absurd_tool_result__"
_RAW_RESULT_KINDS = frozenset({"tool_return", "tool_content_result"})
# Control flow (ModelRetry, CallDeferred, ...) is not checkpointed; the call runs again on replay.
_CONTROL_FLOW_KINDS = frozenset(
    {"approval_required", "call_deferred", "model_retry", "validation_error", "tool_failed", "run_cancelled"}
)
_NO_CONFIG = RoleBasedOperationConfig[None](model=None, event=None, capability=None, tool=None)


def _is_str_dict(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict) and all(isinstance(k, str) for k in value)


def _is_tool_return_object(value: object) -> bool:
    return _is_str_dict(value) and value.get("kind") == "tool-return"


def _is_envelope(stored: object) -> TypeGuard[dict[str, object]]:
    if not (_is_str_dict(stored) and stored.keys() == {_ENVELOPE_KEY}):
        return False
    payload = stored[_ENVELOPE_KEY]
    return _is_str_dict(payload) and payload.get("kind") in _RAW_RESULT_KINDS and "result" in payload


def _to_checkpoint(payload: dict[str, object]) -> object:
    result = payload["result"]
    if _is_envelope(result) or (payload["kind"] == "tool_return" and _is_tool_return_object(result)):
        return {_ENVELOPE_KEY: payload}
    return result


def _from_checkpoint(stored: object) -> object:
    if _is_envelope(stored):
        return stored[_ENVELOPE_KEY]
    if _is_tool_return_object(stored):
        return {"kind": "tool_content_result", "result": stored}
    return {"kind": "tool_return", "result": stored}


class UnhingedOperationBackend(JournalCallableOperationBackend[None]):
    def __init__(self, *, agent_name: str, default_model_id: str | None) -> None:
        super().__init__(agent_name=agent_name, default_model_id=default_model_id, config=_NO_CONFIG)

    async def execute(
        self,
        *,
        operation_id: DurableOperationId,
        name: str,
        body: Callable[[], Awaitable[object]],
        cache_key: tuple[object, ...],
        config: None,
    ) -> object:
        del cache_key, config
        task_ctx = current_async_task_context()
        assert task_ctx is not None
        if not isinstance(operation_id, ToolsetCallToolId):
            return await task_ctx.step(name, body)

        handle = await task_ctx.begin_step(name)
        if handle.done:
            return _from_checkpoint(handle.state)
        payload = await body()
        assert _is_str_dict(payload)
        kind = payload.get("kind")
        if kind in _CONTROL_FLOW_KINDS:
            return payload
        assert kind in _RAW_RESULT_KINDS, kind
        return _from_checkpoint(await task_ctx.complete_step(handle, _to_checkpoint(payload)))


@dataclass(init=False)
class UnhingedDurability(BaseDurabilityCapability[AgentDepsT]):
    """Make an agent durable by checkpointing its model and tool I/O into Unhinged steps."""

    engine_spec: ClassVar = DurabilityEngineSpec(
        engine_name=ENGINE_NAME,
        durable_unit_noun="step",
        durable_container_noun="task",
        codec=JSON_CODEC,
        wrapped_toolset_kinds=frozenset({"function", "mcp"}),
        toolset_lifecycles={"function": "enter-never", "mcp": "enter-never"},
        unsupported_runtime_toolset_kinds=frozenset({"function", "mcp", "dynamic"}),
    )

    def __init__(
        self,
        *,
        models: Mapping[str, Model] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        name: str | None = None,
        parallel_execution_mode: UnhingedParallelExecutionMode = "sequential",
    ) -> None:
        """
        Args:
            models: Extra models by id for ``agent.run(model='<id>')``; the id is folded into the
                step name so a replay resolves to the same model. ``'default'`` is the agent's own.
            event_stream_handler: Model events are handled live inside the request step; each tool
                event is its own checkpointed step.
            name: Prefix for every step name. Defaults to the agent's ``name``.
            parallel_execution_mode: Applied to every run; see ``UnhingedParallelExecutionMode``.
        """
        super().__init__(models=models, event_stream_handler=event_stream_handler, name=name)
        self._parallel_execution_mode: ParallelExecutionMode = parallel_execution_mode

    @property
    def in_durable_context(self) -> bool:
        return current_async_task_context() is not None

    def get_durable_operation_backend(self) -> UnhingedOperationBackend:
        return UnhingedOperationBackend(agent_name=self.name, default_model_id=self.default_model_id)

    async def wrap_run(self, ctx: RunContext[AgentDepsT], *, handler: WrapRunHandler) -> AgentRunResult[Any]:
        agent = self.agent
        assert agent is not None
        with agent.parallel_tool_call_execution_mode(self._parallel_execution_mode):
            return await handler()


__all__ = ["UnhingedDurability", "UnhingedParallelExecutionMode", "current_async_task_context"]
