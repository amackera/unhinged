# Durability

What Unhinged promises, what it doesn't, and which knob changes which.

## What survives what

Every procedure in the engine is one SQLite transaction in WAL mode, and a
step's result is committed before the step is considered done. Which
failures an acknowledged commit survives depends on `synchronous` and, on
macOS, `fullfsync`:

| setting | a killed process | an OS crash | a power cut |
|---|---|---|---|
| `synchronous="normal"` | yes | last few commits may be lost | last few commits may be lost |
| `synchronous="full"` (default), Linux | yes | yes | yes, if the drive honours a flush |
| `synchronous="full"` (default), macOS | yes | yes | no |
| `synchronous="full", fullfsync=True`, macOS | yes | yes | yes, if the drive honours a flush |

On macOS `fsync` sends the data to the drive but does not make the drive
write it; only `F_FULLFSYNC` does, and SQLite issues that only when
`PRAGMA fullfsync` is on. It costs a lot: a few hundred commits a second on
Apple silicon against tens of thousands without. On Linux `fsync` flushes
and the default is already the durable one. `BENCHMARKS.md` has the
numbers from both.

No software can tell whether a drive lies about flushing. Consumer drives
mostly don't; cheap USB enclosures sometimes do. Cloud disks are whatever
the provider says they are.

"Last few commits may be lost" means a step that finished could run again
after the machine comes back. If the step charged a card, the card is
charged twice. That is the whole reason the default is `full`.

## The window that always exists

A step's side effect happens, and then its checkpoint is committed. A
worker killed between those two moments leaves no record of the effect, so
the retry runs the step again. This is at-least-once, and no setting
closes it. Absurd on Postgres has the same window.

What you can do about it is what you'd do anywhere: make the effect
idempotent. Derive a key from `ctx.task_id` and the step name and pass it
to the API you're calling. Payment providers, email providers and most
queues accept one. A step that can't be made idempotent should be the last
thing a task does, so the window is as small as possible.

## Leases and long steps

A worker claims a run for `claim_timeout` seconds and extends it every time
it writes a checkpoint. If it dies, the lease runs out and the next
`claim_task` reclaims the run as a new attempt. So the lease is how long a
crash costs you.

A step that runs longer than the lease, a model call or a test suite, has
to keep the lease alive or it will be reclaimed and run twice while still
running. Call `ctx.heartbeat()` from the step, or from a thread while a
blocking call is in flight. The agent and release examples both do this,
which lets them use leases of thirty and five seconds instead of ten
minutes. Short lease, frequent heartbeat; that's the pattern.

`heartbeat()` raises `TaskCancelled` if someone cancelled the task, so a
long step also finds out about cancellation that way.

## Retries

A task that raises is retried according to its `retry_strategy` and
`max_attempts`. The new attempt replays the handler; completed steps
return their stored values and are not re-executed. A reclaimed lease
counts as a failed attempt too, with reason `$ClaimTimeout`.

A failure you don't want retried, a red test suite or an invalid input,
should be returned as a result rather than raised. Retrying is for
infrastructure.

## When the disk dies

A SQLite file is one disk. Unhinged does nothing about that, and neither
does Absurd for a single Postgres. The usual answer is
[Litestream](https://litestream.io), which tails the WAL and replicates it
to S3 or another machine continuously. Two things to know:

- Replication is asynchronous. A disk that dies loses whatever Litestream
  hadn't shipped yet, typically a second's worth. For steps that matter,
  that is the same at-least-once story as above.
- Litestream needs WAL mode, which Unhinged already uses. Keep
  `wal_autocheckpoint` at its default; Litestream handles checkpointing.

Keep the file on a local disk. SQLite over NFS or SMB corrupts under
concurrent writers, and the chaos test will find that out for you.

## Nothing is deleted unless you ask

Finished tasks, their runs and checkpoints stay in the file. Pass
`maintenance_interval` to `start_worker`, call `app.cleanup()` from a cron
job, or run `python -m unhinged cleanup`. Retention per queue is set with
`set_queue_policy`, 30 days and 1,000 rows per sweep by default. Deleting
3,000 finished tasks takes about 20 ms.

## When to stop using SQLite

One writer at a time. If your steps are cheaper than a commit, more
worker processes won't help; if they're real work, eight processes get
most of eight times the throughput. The file lives on one machine. When
you need workers on several machines, or more durable commits a second
than your disk can flush, move to Absurd on Postgres. The tables have the
same names and the procedures have the same signatures; that's what the
port was for.
