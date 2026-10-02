# Recording of CloudNativePG pod logs

`operations.jsonl` records what happened in `/var/log/pods` on a k3s node during
five minutes of a three-instance CloudNativePG cluster. `versions.txt` lists the
versions and the workflow run it came from. Each line is one file operation:
`at_ms` since the start, `op` (`mkdir`, `create`, `append`, `rename` or
`remove`), `path`, `to` for a rename, and `data` in base64.

A client pod at `10.42.0.6` connected to the database `app` on each instance.
The log of each of `cnpg-1`, `cnpg-2` and `cnpg-3` contains:

| Role | Logins | Logouts | Failed logins |
|---|---|---|---|
| `included` | 17 | 17 | 28 |
| `excluded` | 14 | 14 | 28 |
| `unknown_role`, which does not exist | 0 | 0 | 28 |

One wrong password gives two failed-login records, because `psql` retries
without TLS. The logs also hold about 200 logins per instance by `postgres` and
`streaming_replica`, several log rotations per instance, a restart of the
`cnpg-2` container, and pods that log unrelated lines. `TestRecording` in
`cmd/collector` relies on the counts in the table.

To watch a Collector read the recording, create an empty directory
`/tmp/replay-pods` and set `include` to `/tmp/replay-pods/capture_*/*/*.log*`.
Start the Collector, then run `go run ./cmd/replay -root /tmp/replay-pods
-speed 1`. The first log files are rotated away 350 ms into the recording, so a
faster replay needs a shorter `poll_interval` than 200 ms.

To refresh the recording, start the Capture workflow in GitHub Actions and
download its `cnpg-capture` artifact. Copy `operations.jsonl` and `versions.txt`
here and update the versions in `scripts/capture/run.sh`. If the counts changed,
update the table and `TestRecording`. Then run `go test ./...`.
