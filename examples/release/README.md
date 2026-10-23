# A release checklist that survives Ctrl-C

Build, test, tag, wait for a person to say yes, publish. Each of those is a
step. The slow ones are real subprocesses. Interrupt it anywhere and run the
same command again; the steps that finished say `cached` and don't repeat.

```
$ python examples/release/release.py init ./demo-project
sample project in ./demo-project: five slow tests, a git repo, nothing released

$ python examples/release/release.py run 1.4.0 --project ./demo-project
release 1.4.0: started task 01a16a3f-daa4-7640-977b-0c25471f8179
[build  ] python -m zipfile -c build/app-1.4.0.zip src
[build  ] exit 0 (0.1s)
[test   ] python -m unittest discover -s tests -q
^C
interrupted. Run the same command again to pick up where this left off.

$ python examples/release/release.py run 1.4.0 --project ./demo-project
release 1.4.0: resuming task 01a16a3f-daa4-7640-977b-0c25471f8179
[build  ] cached (0.1s, exit 0)
[test   ] python -m unittest discover -s tests -q
[test   ] exit 0 (2.6s)
[tag    ] sh -c git rev-parse -q --verify v1.4.0 >/dev/null || git tag -a v1.4.0 -m 'release 1.4.0'
[tag    ] exit 0 (0.1s)
[approve] waiting: python release.py approve 1.4.0
```

The run is now parked. It holds no lease and no process needs to stay up;
close the terminal if you like. From another one:

```
$ python examples/release/release.py approve 1.4.0
approved 1.4.0 as anson; the running `run` will continue
```

Back in the first terminal, or in a fresh `run` an hour later, the handler
replays from the top, finds every step already done, and does the one thing
that was left:

```
[build  ] cached (0.1s, exit 0)
[test   ] cached (2.6s, exit 0)
[tag    ] cached (0.1s, exit 0)
[approve] approved by anson
[publish] dist/app-1.4.0.zip
release 1.4.0: released -> dist/app-1.4.0.zip

$ python examples/release/release.py run 1.4.0 --project ./demo-project
release 1.4.0: resuming task 01a16a3f-daa4-7640-977b-0c25471f8179
release 1.4.0: released -> dist/app-1.4.0.zip
```

What makes it work:

- The version is the idempotency key. `run 1.4.0` twice is one release, so
  nothing can be published twice, and the last command above is a no-op.
- A long step heartbeats while its subprocess runs, so the lease is five
  seconds and a crashed attempt is reclaimed in five seconds, not after a
  timeout sized for the slowest test suite.
- A red test suite ends the release as failed, and it is not retried. A
  crash is retried. The difference is whether the handler returns a result
  or dies.
- `--window` is both the event timeout and the task's `max_duration`: a
  release nobody approves within the window expires on its own.

`status 1.4.0` prints the steps so far straight from the checkpoint table.
