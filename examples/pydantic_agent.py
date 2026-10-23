"""A Pydantic AI agent made durable with Unhinged.

    pip install 'unhinged[pydantic-ai]' 'pydantic-ai-slim[anthropic]'
    python examples/pydantic_agent.py worker              # terminal 1
    python examples/pydantic_agent.py ask "What is 6 x 7?" # terminal 2

Every model request and tool call becomes a step. Kill the worker between
them and the next worker replays the finished ones from SQLite without
calling the model again. Pass `--model test` to use Pydantic AI's offline
TestModel instead of Claude.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from pydantic_ai import Agent

from unhinged import AsyncUnhinged
from unhinged.pydantic_ai import UnhingedDurability

QUEUE = "agents"
DB = "agents.db"


def make_agent(model: str) -> Agent:
    if model == "test":
        from pydantic_ai.models.test import TestModel

        model_obj = TestModel()
    else:
        model_obj = model
    agent = Agent(model_obj, name="calculator", capabilities=[UnhingedDurability()])

    @agent.tool_plain
    def multiply(a: float, b: float) -> float:
        """Multiply two numbers."""
        return a * b

    return agent


async def worker(model: str) -> None:
    app = AsyncUnhinged(DB, queue_name=QUEUE)
    agent = make_agent(model)

    @app.register_task("ask", default_max_attempts=5)
    async def ask(params, ctx):
        result = await agent.run(params["prompt"])
        return {"output": result.output}

    print(f"worker on {DB}, model {model}")
    await app.start_worker(claim_timeout=60, poll_interval=0.2)


async def ask(prompt: str) -> None:
    app = AsyncUnhinged(DB, queue_name=QUEUE)
    spawned = app.spawn("ask", {"prompt": prompt}, queue=QUEUE)
    print(f"task {spawned['task_id']}")
    snap = await app.await_task_result(spawned["task_id"], timeout=300)
    print(snap.result or snap.failure)
    app.close()


def main() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--model", default="anthropic:claude-opus-5-5")
    a = sub.add_parser("ask")
    a.add_argument("prompt")
    args = p.parse_args()
    if args.cmd == "worker":
        asyncio.run(worker(args.model))
    else:
        asyncio.run(ask(args.prompt))
    return 0


if __name__ == "__main__":
    sys.exit(main())
