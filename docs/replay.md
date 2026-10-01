# Filesystem replay

`cmd/replay` is test tooling, not part of the Collector runtime. It reproduces
recorded pod-log filesystem operations under an existing directory using ordinary
filesystem calls. It does not inject records into a parser or replace filelog's
polling.

The deliberate capture workflow supplies `testdata/capture/operations.jsonl`.
Routine tests reuse that recording; they do not start k3s or recapture. Until the
initial real capture is committed, that canonical input is not available. The
producer unit tests use explicitly synthetic data and do not stand in for it.

## Run locally

Create an empty directory, point the Collector's filelog include pattern at that
root (for example `/tmp/replay-pods/*/*/*.log`), and start the **actual Collector**
before replay. Use a separate temporary offset/queue storage directory for each
fresh test. The acceptance pipeline consumes Kafka and decodes Avro to judge the
resulting events, not filesystem notification counts.

```sh
mkdir /tmp/replay-pods
# Start the Collector with the test root/storage paths in its native YAML first.
go run ./cmd/replay -root /tmp/replay-pods \
  -recording testdata/capture/operations.jsonl -speed 10
```

`-speed 10` turns five minutes into approximately thirty seconds. `-speed 0`
disables waits; slower replay can help filelog observe short-lived rotated files.
Start a fresh replay in a new empty root; replaying over existing files fails
rather than silently replacing them. Ctrl-C/SIGTERM cancels pacing and stops the
producer. Allow filelog and Kafka ingestion to drain after the producer finishes.

## Recording format

Each JSONL entry has `at_ms` (elapsed capture milliseconds), `op`, and `path`
relative to the pod-log root. Supported operations are `mkdir` (including parents),
`create` (exclusive new file), `append` (existing file), `rename` (with destination
`to`), and `remove` (file or empty directory). `data` is standard JSON base64 for
the original bytes, preserving CRI framing, partial writes, and content exactly.
Operations are applied in recorded order; replay does not sort or reinterpret log
lines. Paths, including rename destinations and symlinks, are confined to the
root by Go's `os.Root`.

The embedding seam is `replay.Run(ctx, root, recordingReader, speed)`. It requires
Go 1.25 or later. The agreed capture format deliberately has no speculative link,
truncate, or generic filesystem-simulation operations.
