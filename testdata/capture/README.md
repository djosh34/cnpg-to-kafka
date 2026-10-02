# Real five-minute CNPG recording

Captured and manually inspected from [Actions run 36935054571](https://github.com/djosh34/cnpg-to-kafka/actions/runs/36935054571)
([temporary diagnostics artifact](https://github.com/djosh34/cnpg-to-kafka/actions/runs/36935054571/artifacts/11197692430)).
The deliberate bootstrap tag was `cnpg-capture-20261002-3`, before any main merge.
`operations.jsonl` and `versions.txt` are the **unchanged downloaded files**, not
invented CNPG logs. The checked-in recording is approximately 6.2 MiB; diagnostics
remain optional artifacts. Refresh instructions and JSONL format are in
[`scripts/capture/README.md`](../../scripts/capture/README.md).

## What was inspected

The agent read the setup/activity output, exact versions, final cluster status,
observed filesystem transitions, and decoded original CRI/CNPG bytes from each
instance. The recorder ran for a fixed 300 seconds after readiness (last observed
operation at 299445 ms); it did not wait for an evidence checklist. CNPG v1.30.1,
k3s v1.36.5+k3s1, and PostgreSQL 18.6 ran on the disposable amd64 Actions VM.
`cnpg-1` was primary; `cnpg-2` and `cnpg-3` were replicas. The final cluster was
healthy with three ready instances and one container restart on `cnpg-2`.

The synthetic client was **10.42.0.6**, database **app**. Original CNPG
`record.connection_from` values include a colon and client port, for example
`10.42.0.6:34964`; event `hostname` should be `10.42.0.6`, not a pod IP.
Generated roles were `included`, `excluded`, and the nonexistent `unknown_role`.
Observed source messages include `connection authorized:`, `disconnection:`,
and SQLSTATE `28P01` / `password authentication failed for user ...` on all three
instances. Authentication-progress/receipt messages also exist and are not extra
logins. Ordinary bootstrap/admin/replication records remain in the real backlog.

For **each** of the primary and both replicas, inspecting the uncompressed raw
source bytes found 17 included-role authorizations and 17 clean disconnects,
14 excluded-role authorizations and 14 clean disconnects, and 28 explicit failed
authentications for each of `included`, `excluded`, and `unknown_role`.
The client used libpq's default `sslmode=prefer`, so a bad-password invocation
produced two distinct real failures. These are observed **source** counts, not a
claim that Collector output has already passed acceptance. Tests must consume
Kafka and decode Avro, suppress excluded successful events only, and tolerate the
agreed at-least-once behavior rather than deduplicate the actual source.

Real kubelet rename/reopen rotation is present on primary and both replicas,
including rotations around 39368–39374 ms and 96397–96402 ms. Timestamped log
siblings, native gzip temporary-file renames, later removals, and writes to
reopened active files are preserved. The controlled runtime stop of `cnpg-2`
produced its new `postgres/1.log` at 147065 ms, followed by real activity/rotation.
Unrelated noise from `capture/noise` and `other/noise` is preserved to exercise
namespace/pod selection. No symlink/truncate operations were added to the agreed
mkdir/create/append/rename/remove format. Notification coalescing means recorded
read batches are not a claim about original syscall write boundaries.

Earlier deliberate attempts failed during setup: the first could not yet reach
the API; inspection of the second's k3s journal exposed Kubernetes 1.36's minimum
three-second rotation monitor interval. That native config was corrected before
this successful run. No incomplete recording was substituted into this fixture.
