# Recording of CloudNativePG pod logs

`operations.jsonl` records what happened in `/var/log/pods` on a k3s node during
five minutes of a three-instance CloudNativePG cluster. `versions.txt` lists the
versions and the workflow run it came from. Each line is one file operation:
`at_ms` since the start, `op` (`mkdir`, `create`, `append`, `rename` or
`remove`), `path`, `to` for a rename, and `data` in base64.

A client pod at `10.42.0.6` connected to the database `app` on each instance
with the method `scram-sha-256`. The log of each of `cnpg-1`, `cnpg-2` and
`cnpg-3` contains:

| Role | Logins | Logouts | Failed logins |
|---|---|---|---|
| `included` | 17 | 17 | 28 |
| `excluded` | 14 | 14 | 28 |
| `unknown_role`, which does not exist | 0 | 0 | 28 |

One wrong password gives two failed-login records, because `psql` retries
without TLS. `included` and `excluded` are two ordinary roles, and both publish
their logins and logouts. `included` has 3 more logins, which wrote enough log
lines to rotate the files.

The logs also hold about 200 logins per instance by `postgres` with `peer`, 30
by `streaming_replica` with `cert` on the primary, several log rotations per
instance, a restart of the `cnpg-2` container, and pods that log unrelated
lines. The
example `trusted_connections` hides those logins and their logouts. The
`cnpg-1-initdb` job pod logs in 5 times as `postgres` with `trust`, which no
trusted connection matches, so those 5 logins are published. Their logouts are
hidden, because `postgres` is in a trusted connection.

So the replay publishes these events, 443 in total:

| Role | Event | Count |
|---|---|---|
| `included` | `LOGIN` | 51 |
| `included` | `LOGOUT` | 51 |
| `included` | `LOGIN_FAILED` | 84 |
| `excluded` | `LOGIN` | 42 |
| `excluded` | `LOGOUT` | 42 |
| `excluded` | `LOGIN_FAILED` | 84 |
| `unknown_role` | `LOGIN_FAILED` | 84 |
| `postgres` | `LOGIN` | 5 |

Every `LOGIN` carries the identity and method from its `connection
authenticated` record: the role name and `scram-sha-256`, or `postgres` and
`trust`. Every `LOGIN_FAILED` has the method `scram-sha-256` from the matched
pg_hba rule and no CN. `TestRecording` in `cmd/collector` relies on these
counts.

To watch a Collector read the recording, create an empty directory
`/tmp/replay-pods` and set `include` to `/tmp/replay-pods/capture_*/*/*.log*`.
Start the Collector, then run `go run ./cmd/replay -root /tmp/replay-pods
-speed 1`. The first log files are rotated away 350 ms into the recording, so a
faster replay needs a shorter `poll_interval` than 200 ms.

To refresh the recording, start the Capture workflow in GitHub Actions and
download its `cnpg-capture` artifact. Copy `operations.jsonl` and `versions.txt`
here and update the versions in `scripts/capture/run.sh`. If the counts changed,
update the tables and `TestRecording`. Then run `go test ./...`.
