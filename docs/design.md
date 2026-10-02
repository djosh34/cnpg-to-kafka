# Design

## A Collector build, not a new program

The job has four parts. Follow pod-log files through rotation, pick out the
connection records, encode them, and deliver them to Kafka without losing any
during an outage. The OpenTelemetry Collector already does the first and the
last part. Its file log receiver saves read positions, follows the kubelet's
rename-and-recreate rotation, and joins log lines that the container runtime
split. Its Kafka exporter has a queue on disk, retries, TLS and an idempotent
producer.

A standalone Go program could save the read position and the queued event in one
transaction, which the Collector cannot. It would also have to own file
discovery, rotation, checkpoints, backpressure and shutdown. The Collector's
weaker guarantee, described under [Delivery](#delivery), was accepted for that
reason.

The binary contains only what `config.yaml` uses: the file log receiver, the
avro processor from this repository, the Kafka exporter and the file storage
extension. `cmd/collector/main.go` lists them. The full contrib distribution has
hundreds of components and a dependency list to match.

## The rules live in receiver operators

The rules that decide what counts as a login, a logout or a failed login are
expressions in `config.yaml`, run by the file log receiver's operators. They are
not Go code. An operator of a cluster can then change them without a rebuild:
exclude another role, limit the pods, or count another SQLSTATE as a failed
login.

The cost is that a typo in the YAML changes what gets published. Two tests run
the real `config.yaml` to cover that, see [Tests](#tests).

Pods are selected by namespace and an optional pod-name regex, both taken from
the log file's path. Selecting by CloudNativePG cluster label would need the
Kubernetes API, a service account and RBAC rules. The path already has what the
selection needs.

The event types follow what PostgreSQL logs.

- A login is `connection authorized`, the last step of a successful connection.
  `connection received` and `connection authenticated` come before it for the
  same connection and are not events.
- A failed login is a FATAL record with an authentication SQLSTATE, or a denied
  `CONNECT` permission on the database. Other FATAL records, such as a server
  shutdown, are not.
- Excluded roles apply to logins and logouts only. A failed login as `postgres`
  is the kind of event an audit trail exists for.
- `hostname` is the client's address as PostgreSQL logged it. There is no
  reverse DNS lookup.

## Avro framing and the schema lookup

The Kafka exporter can send a log record's body as the message value unchanged,
with `encoding: raw`. The avro processor therefore replaces each record's body
with the finished message, and the exporter needs no extension.

The message is in the Confluent wire format: a zero byte, the schema ID as four
big-endian bytes, then the Avro binary. Any consumer that uses a Schema Registry
client can decode it.

The processor fetches the schema once, at startup, by subject and version. The
registry's answer is the schema that is used, and the copy in `schema/` is an
example and the test fixture. This has three consequences.

- The processor never registers a schema. Who may change a schema is a decision
  for the registry's owner.
- A registry outage after startup has no effect, because no event triggers a
  request.
- A Collector that restarts during a registry outage cannot start. Kubernetes
  retries it. A cache of the schema on disk was not built for this case.

The registry setting is a list of URLs because a registry is often reachable at
more than one address. The lookup tries each URL in order and takes the first
schema it can parse. This is a loop of a few lines around the registry client of
the Avro library, with no health state and no reordering.

The `Event` struct is written by hand. The schema has four fields, so a code
generator or a generic mapping from attributes to fields would be more code than
the struct.

## Delivery

Delivery is at least once. Read positions and the exporter's queue are in the
same file storage on the node, written with `fsync`. The exporter retries
without a time limit and waits for all in-sync replicas.

The read position and the queue are two separate writes, and so are Kafka's
acknowledgement and the removal from the queue. A crash between the two writes
of either pair publishes an event twice. Exactly-once delivery would need
deduplication across the Collector and Kafka, and consumers of an audit trail
can tolerate a duplicate.

The queue has a size limit, and a full queue blocks the receiver. The unread
logs then stay in the pod-log files. Nothing is dropped to make room. Two cases
are outside the guarantee: the node's disk is lost, or an outage lasts so long
that the kubelet deletes logs that were never read.

A log line that the container runtime split into parts is joined in memory. A
crash between the parts loses the line. This is the receiver's behaviour, and
storing the parts on disk would mean replacing the receiver's operator.

`max_concurrent_files: 1` works around a data race in the container operator of
Collector v0.162 when it parses several files at once. It costs throughput,
which connection events do not need.

[operations.md](operations.md) lists what this means for an operator.

## TLS and authentication

The example configuration uses mutual TLS with certificate files, because that
is what the project's owner runs. The code does not require it. The processor's
`tls` block and the exporter's settings are the Collector's own, and everything
they support works, including SASL for Kafka and values from environment
variables.

An earlier version rejected every setting except certificate files. That took
about 300 lines of code and tests and two patched dependencies. The Collector's
TLS settings verify certificates unless the operator switches that off, so the
check added little.

## Tests

The tests run with `go test ./...` and need no Docker, no environment variables
and no setup.

### The recording

`testdata/capture/operations.jsonl` is a recording of a real three-instance
CloudNativePG cluster on k3s. For five minutes a script logged in with correct
and wrong passwords, while the kubelet rotated the log files and one container
restarted. The recording holds every file operation in the pod-log directory
with its bytes and its time: create, append, rename, remove.

The recording holds what PostgreSQL and the kubelet wrote, including the two
failed-login records that one wrong password produces. Hand-written log lines
hold only what their author knew to write.

`internal/replay` applies the recording to a directory with ordinary file
system calls. The Collector under test reads that directory the way it reads
`/var/log/pods`, so the receiver's polling and rotation handling are part of the
test.

The capture workflow makes a new recording. It is started by hand, because it
installs k3s and runs for several minutes, and a new recording is only needed after a
CloudNativePG or Kubernetes upgrade.

### The end-to-end test

`TestRecording` runs the Collector inside the test process, with the settings of
`main` and the repository's `config.yaml`. Only addresses, paths and the poll
interval differ. It replays the recording, reads the topic with the inspector
and compares the events with the counts in
[`testdata/capture/README.md`](../testdata/capture/README.md). Halfway through,
Kafka stops for five seconds and comes back with its data, which shows that the
queue settings hold events during an outage.

`TestEventRules` uses the same setup with hand-written log lines, one per rule.

### kfake as the test broker

The broker in the tests is `kfake`, the in-process Kafka from the franz-go
project, which also wrote the client inside the Kafka exporter. It supports TLS
with client certificates, idempotent produce, and a restart on the same port
with its data. The registry is an HTTP test server that returns the schema.

An earlier version started Redpanda in Docker. That test took four minutes, and
it needed Compose, generated certificates and a setup script. What only a real
broker can show is replication and broker durability, and a single-node test
does not show those either. The Collector, the exporter and the Kafka client in
the test are the real ones.

## The image and its scan

The image is one static linux/amd64 binary on `scratch`. There is no shell and
no package manager, so the binary is all a scanner can find.

The image workflow scans the built image with Trivy and fails on a finding of
any severity, including findings without a fix. The answer to a finding is a
dependency upgrade.

`.trivyignore` has one entry, GO-2026-5932. It is a notice that the package
`golang.org/x/crypto/openpgp` is unmaintained. It has no severity and no fixed
version, and Trivy reports it for every program that uses any part of the
`golang.org/x/crypto` module. The Collector does not import `openpgp`. So that
this stays true, the workflow fails if `go list -deps ./cmd/collector` contains a
package that `.trivyignore` names. A new ignore entry has to name its package in
the same way, and is only acceptable for a finding below high severity in a
package that is not in the build.

## Releases

Every push to `main` builds the image, scans it, and pushes it as `sha-<commit>`
and `main`. Pull requests build and scan without credentials for the registry.

A release is a `v*` git tag pushed by the repository owner. The workflow does
not build on a tag. It adds the version tag to the `sha-<commit>` image that
`main` already built and scanned, and creates the GitHub release. A stable
version also moves `latest`. A tag on a commit that has no image fails.

The released image is therefore byte for byte the one that was built and scanned
on `main`.
For the same reason the binary's `--version` prints the commit it was built
from, not the release tag.
