"""A durable agent on Unhinged.

Every model call and every tool call is a checkpointed step, so the
conversation itself is the checkpoint log. Kill the worker between turns and
the next worker replays the transcript from SQLite without calling the model
again. The agent can ask a human a question and park for as long as the
answer takes.

    python examples/agent/agent.py worker --root .            # terminal 1
    python examples/agent/agent.py ask "what is this repo?"   # terminal 2
    python examples/agent/agent.py show <task_id>
    python examples/agent/agent.py answer <task_id> "yes, go ahead"

Uses Claude through the anthropic SDK (`pip install anthropic`, and sign in
with `ant auth login` or set ANTHROPIC_API_KEY). Pass `--model scripted` for a
canned model that needs no network; the tests use it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time

from unhinged import Engine, TaskCancelled, TimeoutError, Unhinged

QUEUE = "agent"
DEFAULT_DB = "agent.db"
MODEL = "claude-opus-5-5"
ANSWER_TIMEOUT = 7 * 86400
MAX_TURNS = 30
# A model call can take minutes, longer than any lease we'd want to wait out
# after a crash. So the lease is short and a thread extends it while the call
# is in flight. Kill the worker and another one picks the run up within this.
CLAIM_TIMEOUT = 30


class Heartbeat:
    """Extend a run's lease from a background thread while a long call runs."""

    def __init__(self, engine: Engine, run_id: str, every: float = CLAIM_TIMEOUT / 3) -> None:
        self.engine, self.run_id, self.every = engine, run_id, every
        self.stop = threading.Event()
        self.cancelled = False

    def _loop(self) -> None:
        while not self.stop.wait(self.every):
            try:
                self.engine.extend_claim(QUEUE, self.run_id, CLAIM_TIMEOUT)
            except TaskCancelled:
                self.cancelled = True
                return
            except Exception:
                return  # the lease will expire and the run will be reclaimed

    def __enter__(self) -> "Heartbeat":
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop.set()
        self.thread.join()

SYSTEM = (
    "You are a careful assistant working inside one directory. Use the tools to look "
    "around before answering. If you need a decision from the person, call ask_human and "
    "wait for the answer. Keep notes short."
)

TOOLS = [
    {
        "name": "list_files",
        "description": "List files and directories under a path relative to the root.",
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]},
    },
    {
        "name": "read_file",
        "description": "Read a text file relative to the root (first 20 KB).",
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]},
    },
    {
        "name": "write_note",
        "description": "Write a note into the notes/ directory. This is a side effect; it runs once.",
        "input_schema": {
            "type": "object",
            "properties": {"name": {"type": "string"}, "text": {"type": "string"}},
            "required": ["name", "text"],
        },
    },
    {
        "name": "ask_human",
        "description": "Ask the person a question and wait for their answer. May take days.",
        "input_schema": {"type": "object", "properties": {"question": {"type": "string"}}, "required": ["question"]},
    },
    {
        "name": "wait",
        "description": "Pause for a number of seconds, then continue.",
        "input_schema": {"type": "object", "properties": {"seconds": {"type": "number"}}, "required": ["seconds"]},
    },
]


# -- models ------------------------------------------------------------------


class ClaudeModel:
    def __init__(self) -> None:
        import anthropic  # optional dependency

        self.client = anthropic.Anthropic()

    def __call__(self, messages: list) -> dict:
        # fallbacks="default" re-runs a request a safety classifier declined on
        # the model Anthropic recommends, instead of handing us a refusal.
        response = self.client.beta.messages.create(
            model=MODEL,
            max_tokens=16000,
            system=SYSTEM,
            tools=TOOLS,
            messages=messages,
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
        )
        data = response.to_dict()
        return {
            "stop_reason": data["stop_reason"],
            "content": data["content"],
            "model": data.get("model"),
            "usage": {k: data["usage"].get(k) for k in ("input_tokens", "output_tokens")},
        }


class ScriptedModel:
    """A deterministic stand-in: look around, ask, write a note, finish."""

    calls = 0

    def __call__(self, messages: list) -> dict:
        ScriptedModel.calls += 1
        turn = sum(1 for m in messages if m["role"] == "assistant")
        plan = [
            ("list_files", {"path": "."}),
            ("read_file", {"path": "README.md"}),
            ("ask_human", {"question": "Should I leave a note summarising the README?"}),
            ("write_note", {"name": "summary", "text": "README read; note written by the scripted model."}),
        ]
        if turn < len(plan):
            name, args = plan[turn]
            if name == "write_note":
                answer = messages[-1]["content"][0]["content"]
                if not answer.lower().startswith("y"):
                    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Understood, no note."}]}
            block = {"type": "tool_use", "id": f"toolu_{turn}", "name": name, "input": args}
            return {"stop_reason": "tool_use", "content": [block]}
        return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Done. I left a note in notes/summary.md."}]}


def make_model(name: str):
    return ScriptedModel() if name == "scripted" else ClaudeModel()


# -- the agent ---------------------------------------------------------------


def register(app: Unhinged, root: str, model) -> None:
    root = os.path.abspath(root)
    heartbeat_engine = Engine(app.path)  # its own connection, used only from the heartbeat thread

    def safe_path(rel: str) -> str:
        path = os.path.abspath(os.path.join(root, rel))
        if path != root and not path.startswith(root + os.sep):
            raise ValueError(f"{rel!r} is outside the root")
        return path

    def run_tool(ctx, block) -> str:
        name, args = block["name"], block["input"]
        if name == "list_files":
            entries = sorted(os.listdir(safe_path(args["path"])))
            return "\n".join(entries[:200]) or "(empty)"
        if name == "read_file":
            with open(safe_path(args["path"]), encoding="utf-8", errors="replace") as f:
                return f.read(20_000)
        if name == "write_note":
            os.makedirs(os.path.join(root, "notes"), exist_ok=True)
            path = os.path.join(root, "notes", f"{os.path.basename(args['name'])}.md")
            with open(path, "w") as f:
                f.write(args["text"].rstrip() + "\n")
            return f"wrote {os.path.relpath(path, root)}"
        raise ValueError(f"unknown tool {name}")

    @app.register_task("agent", default_max_attempts=5)
    def agent(params, ctx):
        messages = [{"role": "user", "content": params["prompt"]}]
        for turn in range(MAX_TURNS):
            # The model call is a step: on replay it returns the stored response
            # and no request goes out. The stored dicts are the transcript.
            with Heartbeat(heartbeat_engine, ctx.run_id) as hb:
                response = ctx.step(f"turn:{turn}", lambda: model(messages))
            if hb.cancelled:
                raise TaskCancelled()
            messages.append({"role": "assistant", "content": response["content"]})

            if response["stop_reason"] == "refusal":
                raise RuntimeError("the model declined this request")
            if response["stop_reason"] != "tool_use":
                return {"text": "".join(b.get("text", "") for b in response["content"] if b["type"] == "text"), "turns": turn + 1}

            results = []
            for block in response["content"]:
                if block["type"] != "tool_use":
                    continue
                name, args, call_id = block["name"], block["input"], block["id"]
                if name == "ask_human":
                    ctx.step(f"question:{call_id}", lambda: args["question"])
                    try:
                        content = ctx.await_event(f"answer:{ctx.task_id}:{call_id}", timeout=ANSWER_TIMEOUT)
                    except TimeoutError:
                        content = "(no answer arrived in time)"
                elif name == "wait":
                    ctx.sleep_for(f"wait:{call_id}", float(args["seconds"]))
                    content = f"waited {args['seconds']}s"
                else:
                    try:
                        content = ctx.step(f"tool:{call_id}", lambda: run_tool(ctx, block))
                    except (ValueError, OSError) as err:
                        content = ctx.step(f"tool:{call_id}", lambda: f"Error: {err}")
                        results.append({"type": "tool_result", "tool_use_id": call_id, "content": content, "is_error": True})
                        continue
                results.append({"type": "tool_result", "tool_use_id": call_id, "content": str(content)})
            messages.append({"role": "user", "content": results})
        raise RuntimeError(f"gave up after {MAX_TURNS} turns")


# -- commands ----------------------------------------------------------------


def transcript(app: Unhinged, task_id: str) -> list:
    rows = app.engine.connection.execute(
        'select checkpoint_name, state from "c_agent" where task_id = ? order by updated_at, checkpoint_name',
        (task_id,),
    ).fetchall()
    return [(r["checkpoint_name"], json.loads(r["state"]) if r["state"] is not None else None) for r in rows]


def pending_question(app: Unhinged, task_id: str):
    row = app.engine.connection.execute('select event_name from "w_agent" where task_id = ?', (task_id,)).fetchone()
    if row is None:
        return None
    call_id = row["event_name"].rsplit(":", 1)[1]
    cp = app.engine.get_checkpoint(QUEUE, task_id, f"question:{call_id}")
    return row["event_name"], (cp or {}).get("state")


def cmd_worker(args) -> None:
    app = make_app(args.db)
    register(app, args.root, make_model(args.model))
    print(f"agent worker on {args.db}, root {os.path.abspath(args.root)}, model {args.model}")
    app.start_worker(claim_timeout=CLAIM_TIMEOUT, poll_interval=0.2, stop_when_idle=args.once)
    app.close()


def cmd_ask(args) -> None:
    app = make_app(args.db)
    spawned = app.spawn("agent", {"prompt": args.prompt}, queue=QUEUE, retry_strategy={"kind": "exponential", "base_seconds": 2})
    print(f"task {spawned['task_id']}")
    if args.wait:
        shown = set()
        while True:
            snap = app.fetch_task_result(spawned["task_id"])
            if snap.terminal:
                print(json.dumps(snap.result or snap.failure, indent=2))
                break
            q = pending_question(app, spawned["task_id"])
            if q and q[0] not in shown:
                shown.add(q[0])
                print(f"agent asks: {q[1]}\n  reply with: agent.py answer {spawned['task_id']} \"...\"")
            time.sleep(0.5)
    app.close()


def cmd_answer(args) -> None:
    app = make_app(args.db)
    q = pending_question(app, args.task_id)
    if q is None:
        print("that task is not waiting on a question", file=sys.stderr)
        sys.exit(1)
    app.emit_event(q[0], args.text)
    print("answered")
    app.close()


def cmd_show(args) -> None:
    app = make_app(args.db)
    snap = app.fetch_task_result(args.task_id)
    if snap is None:
        print("no such task", file=sys.stderr)
        sys.exit(1)
    print(f"state: {snap.state}")
    for name, state in transcript(app, args.task_id):
        if name.startswith("turn:"):
            for block in state["content"]:
                if block["type"] == "text":
                    print(f"  model: {block['text'][:300]}")
                elif block["type"] == "tool_use":
                    print(f"  model -> {block['name']}({json.dumps(block['input'])[:120]})")
        elif name.startswith("tool:"):
            print(f"  tool  <- {str(state)[:120]!r}")
        elif name.startswith("$awaitEvent:"):
            print(f"  human <- {state!r}")
        elif name.startswith("question:"):
            print(f"  model asks: {state}")
    q = pending_question(app, args.task_id)
    if q:
        print(f"waiting on: {q[1]}")
    if snap.terminal:
        print(json.dumps(snap.result or snap.failure, indent=2))
    app.close()


def make_app(db: str) -> Unhinged:
    return Unhinged(db, queue_name=QUEUE)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", default=DEFAULT_DB)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("worker", help="run the agent worker")
    s.add_argument("--root", default=".")
    s.add_argument("--model", default=os.environ.get("UNHINGED_AGENT_MODEL", "claude"), choices=["claude", "scripted"])
    s.add_argument("--once", action="store_true", help="exit when nothing is runnable")
    s.set_defaults(fn=cmd_worker)

    s = sub.add_parser("ask", help="start a run")
    s.add_argument("prompt")
    s.add_argument("--wait", action="store_true")
    s.set_defaults(fn=cmd_ask)

    s = sub.add_parser("answer", help="answer a parked question")
    s.add_argument("task_id")
    s.add_argument("text")
    s.set_defaults(fn=cmd_answer)

    s = sub.add_parser("show", help="print the transcript from the checkpoints")
    s.add_argument("task_id")
    s.set_defaults(fn=cmd_show)

    args = p.parse_args(argv)
    try:
        args.fn(args)
    except TaskCancelled:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
