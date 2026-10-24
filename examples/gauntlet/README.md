# The gauntlet

Everything that can go wrong, in order, in about a minute, with a ledger at
the end that says whether anything was lost.

```bash
pip install rich
python examples/gauntlet/gauntlet.py                      # load, chaos, ledger
python examples/gauntlet/gauntlet.py --disk-death         # + lose the disk, restore from Litestream
python examples/gauntlet/gauntlet.py --os-crash           # + hard-stop a Linux VM, count lost commits
```

![the gauntlet](gauntlet.gif)

## What happens

1. **Load.** 10,000 three-step tasks spawned in one transaction. Eight worker
   processes share the one file.
2. **Chaos.** A supervisor `kill -9`s a random worker every 250 ms and starts a
   replacement. The dashboard shows leases expiring and being reclaimed.
3. **Disk death** (`--disk-death`, needs `brew install litestream`). Litestream
   has been replicating the file since before the load. With replication
   mid-flight, the database directory is deleted. The replica is restored on
   "machine B" and fresh workers finish the backlog there.
4. **OS crash** (`--os-crash`, needs `brew install lima`). Workers run inside
   an Ubuntu VM with the database on the VM's own disk, and print an
   acknowledgement to the host every time a checkpoint commits. The VM is
   hard-stopped mid-run and booted again. Every acknowledged checkpoint is
   checked against the file; one that is missing was acknowledged and then
   lost. It runs at `synchronous=NORMAL` and at `FULL`.
5. **Ledger.** Computed from a side table where every step recorded
   `(task, step, run, pid, time)`.

## A real run

Apple M4, 10,000 tasks, all phases, 62 seconds:

```
 tasks completed                   10,000 / 10,000
 workers killed                    63
 leases reclaimed                  146
 steps executed                    30,006 for 30,000 steps
 re-ran after a reclaimed lease    6  (the at-least-once window)
 lost with the disk                43 completions existed only on the dead disk;
                                   machine B ran them again
 a run executed by two processes   0
 a step executed twice in one run  0
 unexplained re-executions         0
 OS crash                          NORMAL: 130 of 11,428 acknowledged checkpoints
                                   lost, integrity ok;
                                   FULL: 0 of 7,640 acknowledged checkpoints lost,
                                   integrity ok
 integrity_check                   ok
 verdict                           OK
```

## How to read it

**The six re-runs** are the documented at-least-once window: a worker killed
after a step's side effect landed and before its checkpoint committed. Each
one traces to a run whose lease the engine timed out. That is the only way a
step runs twice, and the ledger checks that it is.

**The 43 lost with the disk** are Litestream's replication lag. Those tasks
finished on machine A in the second or so before the disk died, the replica
never heard, so machine B ran them again. Replication is asynchronous; this
is the number that tells you how asynchronous. Since the tasks are
idempotent-by-construction here, the ledger stays green. With real side
effects, this is the window you design for.

**The OS crash** is the one nobody shows. At `NORMAL`, SQLite skips the
fsync on most commits, and the guest kernel's page cache died with the VM:
130 checkpoints the worker had been told were committed were not in the
file. At `FULL`, every commit fsyncs before it returns, and zero were lost.
Same engine, same code, one pragma. `integrity_check` passed both times,
which is SQLite's promise: a crash loses recent commits at `NORMAL`, it does
not corrupt the file.

A detail worth knowing: the first time this ran, the crash also
truncated the Python files the harness had copied into the VM, because they
were still in the page cache too. The harness now runs `sync` after every
copy. Durability is not a property of the database alone.

## What it is not

A power cut. A hard VM stop loses the guest's page cache, which is the
"OS crash" column in `docs/durability.md`. The virtual disk is a file on the
host, and the host keeps running, so anything the guest fsynced is safe no
matter what a real drive would have done with the flush. Zero lost at `FULL`
here means "survives an OS crash", not "survives a power cut". Testing the
drive needs a real plug; `BENCHMARKS.md` and `docs/durability.md` cover that
boundary.
