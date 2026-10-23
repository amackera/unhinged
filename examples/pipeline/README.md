# Resumable batch pipeline

Every file in a directory is a task. Each task runs three steps: checksum the
file, analyze it, write a JSON sidecar. Each step is a checkpoint.

```bash
python examples/pipeline/pipeline.py generate ./corpus --count 200
python examples/pipeline/pipeline.py enqueue ./corpus
python examples/pipeline/pipeline.py work --concurrency 4 --flaky 0.2
```

Hit Ctrl-C while `work` is running, then run it again. Tasks that finished
stay finished. A task that was killed between `analyze` and `write-sidecar`
skips the first two steps and writes the sidecar. Nothing is checksummed twice.

`--flaky 0.2` makes the analyze step fail one time in five. Those tasks retry
with exponential backoff (half a second, then one, two, four, up to 30) and
six attempts before they are marked failed. `status` shows the counts and the
last failure reason for anything that gave up; `report` sums up what
completed.

`enqueue` uses the file path as the idempotency key, so a cron job that
enqueues a directory every hour only adds files it hasn't seen.
