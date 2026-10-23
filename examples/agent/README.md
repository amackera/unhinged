# A durable agent

An agent loop where every model call and every tool call is a step. The
checkpoints are the transcript: `show` prints the conversation straight out
of SQLite.

```bash
pip install anthropic                     # and `ant auth login`, or ANTHROPIC_API_KEY
python examples/agent/agent.py worker --root .
```

In another terminal:

```bash
python examples/agent/agent.py ask "What does this repo do? Leave me a note." --wait
```

The agent lists files, reads a few, and if it wants a decision it calls
`ask_human`. The run parks. `ask --wait` prints the question and the command to
answer it; the worker is free to do other runs meanwhile, and you can answer a
minute or a week later:

```bash
python examples/agent/agent.py answer <task_id> "yes"
python examples/agent/agent.py show <task_id>
```

Kill the worker at any point and start it again. Turns that already happened
are read back from the database, so no request goes out for them. A model
call that fails outright (a 400, a network partition longer than the SDK's
retries) fails the task, and the task's own retry replays the transcript and
makes only the one call that never finished.

The lease is 30 seconds. A model call can run longer than that, so while one
is in flight a thread extends the lease every 10 seconds. The worker holds
the run only as long as it keeps proving it's alive, and a killed worker's
run is picked up within 30 seconds, not after a ten-minute timeout sized for
the longest possible call.

`--model scripted` swaps in a canned model that lists, reads, asks, and
writes a note. It needs no network and is what the tests run.

The model is `claude-opus-5-5` with `fallbacks="default"`, which re-runs a
request on the model Anthropic recommends if a safety classifier declines
it. Drop that argument and the `betas` list if you don't want it.
