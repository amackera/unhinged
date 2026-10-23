# Benchmarks

Numbers from `python examples/bench.py`, which calibrates the disk before it
measures anything. Every row carries a durability class, because a tasks
per second figure without one is a vanity number:

- **crash-safe**: survives the process or the OS dying. The last few
  commits can be lost on power loss.
- **power-safe**: survives power loss, if the drive honours a flush. No
  benchmark can tell from software whether it does.

On macOS, `fsync` does not flush the drive's write cache; SQLite only
issues `F_FULLFSYNC` when `PRAGMA fullfsync` is on. So on a Mac
`synchronous=FULL` is crash-safe and `fullfsync=True` is power-safe. On
Linux, `fsync` flushes, and FULL is power-safe.

The task is the same throughout: three no-op steps, which the engine turns
into 4.1 commits when claims are batched ten at a time (one claim per
batch, three checkpoints and one completion per task) and 5 when they are
not. Spawning is one commit for the whole batch. Throughput rows are the
median of three runs after one warm-up run, burn-down mode: every task is
spawned before any worker starts.

## Apple M4, macOS 27, APFS, SQLite 3.53.1, Python 3.14

Internal NVMe. Unhinged commit `50f5415`. The bench labelled the filesystem "APFS (assumed)" on this run; it now detects it.

Calibration (median / p99):

| | latency | rate |
|---|---|---|
| raw 4 KiB write + fsync | 0.013 / 0.017 ms | 78,945/s |
| raw 4 KiB write + F_FULLFSYNC | 2.99 / 4.02 ms | 335/s |
| SQLite commit, NORMAL | 0.023 / 0.065 ms | 44,077/s |
| SQLite commit, FULL | 0.059 / 0.105 ms | 16,901/s |
| SQLite commit, FULL + fullfsync | 3.99 / 4.22 ms | 251/s |

Throughput, one worker process:

| setting | class | tasks/s | commits/s | ceiling | of ceiling |
|---|---|---|---|---|---|
| NORMAL | crash-safe | 5,002 | 20,517 | 44,077 | 47% |
| FULL | crash-safe | 3,700 | 15,177 | 16,901 | 90% |
| FULL + fullfsync | power-safe | 64 | 263 | 251 | ~100% |

The two fast rows are what a laptop gives you against a crash. The slow
row is what it gives you against a power cut. At FULL the engine runs at
90% of what the disk can commit; the gap is Python. With `fullfsync` the
engine is at the disk's limit and the only lever left is fewer commits per
task.

Worker scaling at FULL, tasks per second:

| processes | no-op steps | 10 ms steps |
|---|---|---|
| 1 | 3,714 | 24 |
| 2 | 3,273 | 46 |
| 4 | 3,079 | 91 |
| 8 | 3,335 | 160 |

SQLite has one writer. With no-op steps the writer is the whole job, so
more processes contend and gain nothing. With 10 ms steps, which is closer
to real work, the writer is idle most of the time and eight processes get
nearly seven times the throughput of one.

Recovery. Sixty tasks of three 20 ms steps, claim timeout two seconds, one
worker killed with SIGKILL 0.6 s in:

| | |
|---|---|
| done before the kill | 6 tasks, 20 steps |
| kill to a replacement worker's first step | 0.09 s |
| kill to the interrupted task's lease reclaimed | 2.02 s |
| kill to the interrupted task resumed | 4.48 s |
| steps that ran twice | 0 |

The reclaim takes exactly the lease, which is configuration. The task then
re-queues behind the remaining backlog, which is why it finishes last.
Nothing ran twice because the kill landed between steps; the at-least-once
window is a kill after a side effect and before its checkpoint commits.

Storage: 1,285 bytes per completed task on disk. Deleting 3,000 finished
tasks with `cleanup()` took 0.02 s.

## Linux, GitHub-hosted runner, ext4 on a cloud disk, SQLite 3.46.1, Python 3.14

AMD EPYC 9V45 (Azure), kernel 7.0. Unhinged commit `e61deed`. This is a
cloud VM: the disk is network-attached and its flush semantics are
whatever Azure provides. Treat it as a second data point, not as a
hardware benchmark. The first attempt ran in `/tmp`, which is tmpfs on
these runners and made fsync free; the bench now refuses to label that
durable, and this run is on the workspace disk.

Calibration (median / p99):

| | latency | rate |
|---|---|---|
| raw 4 KiB write + fsync | 0.079 / 0.289 ms | 12,588/s |
| raw 4 KiB write + fdatasync | 0.078 / 0.220 ms | 12,826/s |
| SQLite commit, NORMAL | 0.009 / 0.026 ms | 106,338/s |
| SQLite commit, FULL | 0.318 / 4.08 ms | 3,147/s |
| SQLite commit, FULL + fullfsync | 0.269 / 0.81 ms | 3,717/s |

`fullfsync` is a no-op on Linux; the two FULL rows differ only by noise,
and the 4 ms p99 on the plain FULL row is the cloud disk stalling.

Throughput, one worker process:

| setting | class | tasks/s | commits/s | ceiling | of ceiling |
|---|---|---|---|---|---|
| NORMAL | crash-safe | 1,920 | 7,875 | 106,338 | 7% |
| FULL | power-safe | 1,141 | 4,682 | 3,147 | ~150% |
| FULL + fullfsync | power-safe | 932 | 3,822 | 3,717 | ~100% |

The "of ceiling" column is approximate on this machine: the calibration
and the engine ran minutes apart on a disk whose latency moves around,
and the FULL row came out faster than its own calibration. Read it as
"the engine is at the disk's limit", not as a precise ratio.

The notable number is 1,141 tasks per second power-safe, against 64 on
the Mac. The Mac's drive takes 3 to 4 ms to honour a flush; this cloud
disk takes a fraction of a millisecond, presumably because it has a
battery-backed cache on the other end of the wire. Same engine, same
code, a twenty-fold difference that is entirely the disk.

Worker scaling at FULL, tasks per second:

| processes | no-op steps | 10 ms steps |
|---|---|---|
| 1 | 1,021 | 28 |
| 2 | 1,549 | 60 |
| 4 | 1,376 | 125 |
| 8 | 1,438 | 241 |

Recovery, same setup as the Mac run: lease reclaimed 2.00 s after the
kill, interrupted task resumed at 3.27 s behind the backlog, nothing ran
twice.

Storage: 1,285 bytes per task, identical to the Mac, as it should be.

## Reading these numbers

- Compare the `fullfsync` row, not the FULL row, with a Postgres engine
  on Linux. Postgres fsyncs the WAL on commit, which flushes the drive on
  Linux. The FULL row on a Mac is the same durability as
  `synchronous_commit=off` with the OS cache intact.
- A thousand tasks a second power-safe needs a drive that flushes fast:
  a datacenter SSD with power-loss protection, or a cloud disk with a
  battery-backed cache, as the Linux run shows. On a consumer drive the
  ceiling is a few hundred commits a second, and the only lever is fewer
  commits per task. That is the disk, not the engine.
- Cloud VMs and containers have their own flush semantics. Calibrate
  there before believing anything.

To reproduce: `python examples/bench.py --json out.json`. On Linux pass
`--dir` with a path on the disk you care about; `/tmp` is often tmpfs, and
the bench will say so and void the labels if it is.
