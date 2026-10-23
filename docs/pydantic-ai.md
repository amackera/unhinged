# Pydantic AI

`UnhingedDurability` makes a [Pydantic AI](https://ai.pydantic.dev) agent
run durable on Unhinged. It is a port of Pydantic AI Harness's
`AbsurdDurability` and keeps its step names and checkpoint payloads, so an
agent that moves between Absurd on Postgres and Unhinged on SQLite resumes
its in-flight work either way.

```bash
pip install 'unhinged[pydantic-ai]' 'pydantic-ai-slim[anthropic]'
```

Run the agent inside an `AsyncUnhinged` task handler. Every model request,
MCP call and function tool call is then a step; a worker that dies mid-run
resumes from the last completed step without calling the model again for the
turns it already has. Outside a task the capability does nothing.

```python
from pydantic_ai import Agent
from unhinged import AsyncUnhinged
from unhinged.pydantic_ai import UnhingedDurability

app = AsyncUnhinged("agents.db", queue_name="agents")
agent = Agent("anthropic:claude-opus-5-5", name="analyst", capabilities=[UnhingedDurability()])


@app.register_task("analyse")
async def analyse(params, ctx):
    result = await agent.run(params["prompt"])
    return {"output": result.output}


app.spawn("analyse", {"prompt": "Summarize the Q3 report."})   # producer
await app.start_worker()                                        # worker
```

`examples/pydantic_agent.py` is the runnable version, with `--model test`
for Pydantic AI's offline model.

## What is checkpointed

Step names come from the agent's `name` and each toolset's `id`, exactly as
in `AbsurdDurability`:

| step | operation |
|---|---|
| `{name}__model.request` | one model request |
| `{name}__model.request_stream` | one streamed request |
| `{name}__model.request.{model_id}` | a request to a model other than the default |
| `{name}__function_toolset__{id}.call_tool:{tool}` | a function tool call |
| `{name}__mcp_server__{id}.call_tool` | an MCP tool call |
| `{name}__event_stream_handler` | one event delivered to the handler |

A step name that recurs, a second `agent.run()` in the same task or the same
tool called twice, gets `#2`, `#3` and so on. A tool call that raises
`ModelRetry`, `ToolFailed`, `CallDeferred` or `ApprovalRequired` is not
checkpointed and runs again on replay.

## Constraints

Same as Absurd's. The agent needs a `name`, every toolset needs an `id`, and
neither should change once deployed. Tool return values are stored as JSON.
Toolsets passed per run with `run(toolsets=...)` inside a task raise a
`UserError`; register them on the agent. A synchronous `TaskContext` raises
too: agent runs are async, so the handler has to be.

A step is checkpointed after it runs, so a crash between a tool's side
effect and its checkpoint re-runs the tool. Keep tools idempotent. Model
calls are safe to repeat; they just cost tokens.

## Leases

A model call can run for minutes. Either give the worker a `claim_timeout`
longer than any call, or keep it short and heartbeat from a thread while a
call is in flight, as `examples/agent/agent.py` does. A reclaimed lease
re-runs the model call in flight; the finished ones are not touched.
