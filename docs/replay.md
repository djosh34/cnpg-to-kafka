# Filesystem replay

`cmd/replay` is test tooling, not part of the Collector runtime. It reproduces
recorded pod-log filesystem operations under an existing directory using ordinary
filesystem calls. It does not inject records into a parser or replace filelog's
polling.

The deliberate capture workflow supplies `testdata/capture/operations.jsonl`.
The committed canonical recording is from [this actual five-minute capture](https://github.com/djosh34/cnpg-to-kafka/actions/runs/36935054571).
Routine tests reuse it; they do not start k3s or recapture. The focused captured
producer test checks an observed rename, the subsequent append into the new file,
and repeatable replay. Other producer unit tests explicitly use synthetic data.

## Run locally

Create an empty directory, point the Collector's filelog include pattern at that
root (for example `/tmp/replay-pods/*/*/*.log`), and start the **actual Collector**
before replay. For this recording, use a test-only native `poll_interval: 10ms`
and `-speed 5`: the initial active files rotate after about 350 capture milliseconds,
so the default 200ms polling interval can miss initial content at accelerated speed.
Use a separate temporary offset/queue storage directory for each fresh test. The acceptance pipeline consumes Kafka and decodes Avro to judge the
resulting events, not filesystem notification counts.

```sh
mkdir /tmp/replay-pods
# Start the Collector with the test root/storage paths in its native YAML first.
go run ./cmd/replay -root /tmp/replay-pods \
  -recording testdata/capture/operations.jsonl -speed 5
```

`-speed 5` turns five minutes into approximately one minute. `-speed 0`
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
