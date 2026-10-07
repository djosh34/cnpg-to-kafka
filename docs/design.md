# Design

## A Collector build, not a new program

The job has four parts. Follow pod-log files through rotation, turn the
connection records into events, encode them, and deliver them to Kafka without
losing any during an outage. The OpenTelemetry Collector already does the first
and the last part. Its file log receiver saves read positions, follows the
kubelet's rename-and-recreate rotation, and joins log lines that the container
runtime split. Its Kafka exporter has a queue on disk, retries, TLS and an
idempotent producer.

A standalone Go program could save the read position and the queued event in one
transaction, which the Collector cannot. It would also have to own file
discovery, rotation, checkpoints, backpressure and shutdown. The Collector's
weaker guarantee, described under [Delivery](#delivery), was accepted for that
reason.

The binary contains only what `config.yaml` uses: the file log receiver, the
cnpg and avro processors from this repository, the Kafka exporter and the file
storage extension. `cmd/collector/main.go` lists them. The full contrib
distribution has hundreds of components and a dependency list to match.

The pipeline is `file_log/cnpg` to `cnpg` to `avro` to `kafka/cnpg`.

## The rules are Go code

The receiver only reads. Its one operator, `container`, parses the container
runtime's line format and sets the namespace and pod name as resource
attributes. Everything else is Go:

| Package | What it does |
|---|---|
| `internal/cnpg` | Parses one CloudNativePG JSON line into a typed `Record`. |
| `internal/event` | The `Event` type, the rules, the session table, and the hand-off through log record attributes. |
| `processor/cnpgprocessor` | Runs the rules on each log record and its config. |
| `processor/avroprocessor` | Encodes each `Event` and looks up the schema. |

An earlier version had the rules as expressions in the receiver's operators in
`config.yaml`. They could only be tested by running the whole Collector, and a
typo in the YAML changed what got published. In Go each step is a small
function with its own table test, the config is typed and checked at startup,
and the rules can join two records of one connection, which the operators could
not.

The cost is that changing a rule needs a new build. The settings that differ
between clusters are in the config: the trusted connections, the
high-privilege roles, the host name and the application name.

### Which records are events

`cnpg.Parse` drops every line that is not JSON, not `"logger": "postgres"`, or
has no `record` object. That drops the instance manager's own lines, other
containers and plain text.

The event type comes from the message and the command tag, not from the
SQLSTATE:

| Record | Event |
|---|---|
| message starts with `connection ready:` | `LOGIN` |
| `error_severity` FATAL, `command_tag` `authentication` or `startup`, SQLSTATE not `53300` | `LOGIN_FAILED` |
| message starts with `disconnection:` | `LOGOUT` |
| `connection authenticated: ...` | no event, it goes into the session table |
| anything else | no event |

`connection authorized` is not the login. PostgreSQL logs it before it checks
the database, the `CONNECT` permission, `NOLOGIN` and the connection limit, and
any of those can still end the connection with a FATAL. `connection ready` comes
only after all of them. The FATALs after authorization have the command tag
`startup`, so both tags count. `53300`, too many connections, is not counted as
a failed login and is not published.

A `disconnection` record's command tag is the backend's last status, for example
`idle` or `streaming 0/605D368` for a replica, so only the message is matched.

Every pod in the namespace is read, so the records of CloudNativePG's job pods
count too. The `initdb` job logs in as `postgres` with the method `trust`. The
example `trusted_connections` does not match that, so those logins are
published.

## The session join

PostgreSQL logs the CN and the method only on the `connection authenticated`
record, which comes before the record that makes the event. For a certificate
with the wrong CN, the tried CN is only on that record. So the cnpg processor
joins the records of one connection:

- `connection authenticated` adds an entry with the identity and the method.
  The key is the pod, then PostgreSQL's `session_id`.
- `connection ready` or any FATAL of the same session takes the entry and
  removes it. That includes a FATAL that is not published, such as `53300`.
- Every later PostgreSQL record of the pod counts each of that pod's entries
  down by one, from 1000. An entry at zero is removed and nothing is published
  for it. This removes the entries of connections whose last record never
  arrives, without a timer.

A connection setup takes a few milliseconds, so an entry normally lives for a
few records. There is one table per pod, as a field of the processor. The
processor takes one lock for the whole batch, because the Collector may call it
from several goroutines and the table has no lock of its own.

The join needs the records of one pod in log order. Three things give that:
`max_concurrent_files: 1` reads one file at a time, the
[emitter gate](#the-emitter-gate) sends each batch in read order, and the lock
handles one batch at a time.

Order is only kept within one file. On a first read, with no saved positions,
the receiver reads the files of a container in sorted order, so `0.log` comes
before its older rotated file `0.log.<time>`. A `connection ready` in `0.log`
can then come before the `connection authenticated` at the end of the rotated
file. The table also lives only in memory, so a restart between the two records
loses the entry. In these cases the `LOGIN` or `LOGIN_FAILED` has no join data:

- Its CN is null. Its method is null, except on a failed login whose record
  quotes the pg_hba rule.
- A `LOGIN` without join data is published, also for a trusted role, because its
  method and identity are unknown and cannot match a trusted connection.

There is no reorder buffer. It would be more code than the join, for a case
that only happens around a first read or a restart, and it fails toward
publishing.

A failed password login has no `connection authenticated` record. Its method
comes from the `detail` of the FATAL, which quotes the matched pg_hba rule:
field 4 of a `local` rule, or for a `host*` rule the field after the address
(after the separate netmask, if there is one). It counts only when it is a
PostgreSQL 18 method name. A role named `cert` in `host all cert all
scram-sha-256` is therefore not taken for the method.

## Trusted connections

CloudNativePG connects to its own instances all the time, and those logins
would flood the topic. `trusted_connections` hides them, and only them:

- A `LOGIN` is hidden when role, method and identity all exactly match an entry.
  All three are required.
- A `LOGOUT` has no method or identity, so it is hidden when its role is in any
  entry.
- A `LOGIN_FAILED` is always published. A failed login as `postgres` is the
  kind of event an audit trail exists for.

Matching on the method and identity is what makes hiding safe. Peer
authentication as `postgres` only works from inside the database container, and
a certificate with the subject `CN=streaming_replica` only works for whoever the
trusted CA gave it to. `postgres` with a password from the network is published.

PostgreSQL's `application_name` is not used for this. The client sets it, so any
client can call itself `cnpg-instance-manager`.

There is no default list, and the code knows no CloudNativePG role, method or
identity. The example `config.yaml` has the two entries CloudNativePG needs.

## Avro framing and the schema lookup

The cnpg processor's last step is an `Event`. It writes the event into the log
record's attributes, and the avro processor reads it back into the same `Event`
type. That round trip is the only untyped step between the two processors, and a
table test covers every field, set and empty.

The Kafka exporter can send a log record's body as the message value unchanged,
with `encoding: raw`. The avro processor therefore replaces each record's body
with the finished message, and the exporter needs no extension.

The message is in the Confluent wire format: a zero byte, the schema ID as four
big-endian bytes, then the Avro binary. Any consumer that uses a Schema Registry
client can decode it. An event that the schema cannot encode is dropped with a
warning.

The processor fetches the schema once, at startup, by subject and version. The
registry's answer is the schema that is used, and the copy in `schema/` is the
one to register and the test fixture. This has three consequences.

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

The `Event` struct is written by hand next to the schema. A code generator would
be more code than the struct.

## The emitter gate

By default the file log receiver of Collector v0.162 batches log lines and sends
a batch from two goroutines: the reader when the batch is full, and a timer
otherwise. Two batches can then reach the processor at once, and a later batch
can overtake an earlier one. The receiver also marks lines as read before the
batch is sent, so a crash can lose a batch.

The feature gate `stanza.synchronousLogEmitter` makes the receiver send each
batch from the goroutine that read it. Batches then arrive in read order, and
lines are marked as read only after they were sent. `main` turns the gate on
before the Collector starts, so a config or a flag cannot forget it, and a test
checks that it is on. The gate is alpha upstream. If a later Collector removes
it, setting it fails and the binary does not start, so an upgrade fails loudly.

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

When the pipeline returns an error, the receiver sends the same batch again.
That batch was already changed by the processors: the cnpg processor removed
the records that make no event, and the avro processor replaced the bodies with
Avro. So the cnpg processor passes a record that already carries an event in its
attributes through unchanged, without parsing it or touching the session table.
The avro processor keeps the attributes, so it encodes the same bytes again.

A log line that the container runtime split into parts is joined in memory. A
crash between the parts loses the line. This is the receiver's behaviour, and
storing the parts on disk would mean replacing the receiver's operator.

`max_concurrent_files: 1` works around a data race in the container operator of
Collector v0.162 when it parses several files at once. It also keeps one file
at a time for the session join. It costs throughput, which connection events do
not need.

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
and no setup. Every test is a [testify](https://github.com/stretchr/testify)
test. They are layered from small to large, so a broken rule shows up in the
smallest test that covers it.

1. **Steps.** Table tests for each function: `cnpg.Parse`, the event rules for
   every case in the lab and the recording, the session table, the pg_hba
   method, the client address, the timestamp, the account type, trusted
   matching, the attribute round trip, and `Event` to Avro bytes.
2. **Processors.** Log lines go into `plog.Logs` through a small helper, then
   through the cnpg processor, and through both processors with a test
   registry. One test fails the next consumer once and sends the same batch
   again, as the receiver does. No files.
3. **Kafka bytes.** Events go through the avro processor and the real Kafka
   exporter into kfake. The test reads the raw message values.
4. **End to end.** `TestPodLogs` runs the whole Collector on the sample pod logs
   in `testdata/pods`.
5. **Replay.** `TestRecording` replays the recording of a real cluster through
   the whole Collector, with a Kafka outage.

Where the tests check Kafka messages, each value must start with a zero byte and
the schema ID, and the rest must decode with the schema using plain
`avro.Unmarshal`. The Avro library's `Unmarshal` accepts a payload that ends
early, so the tests also compare the bytes with `avro.Marshal` of the expected
event.

Lines that the recording has are copied from it. Cases that it does not have, a
certificate with the wrong CN and the failures after authentication, are
written by hand in the shape that a PostgreSQL 18 lab logged them.
`internal/fixture` holds the shared lines.

### The end-to-end test

`TestPodLogs` runs the Collector inside the test process, with the factories
and the feature gate of `main` and the repository's `config.yaml`. Only paths,
addresses and the poll interval differ. It reads `testdata/pods`, which is in
the `/var/log/pods` layout, and its README lists what the files hold. The test
reads every message from the topic and compares the decoded events with the
expected list, in count and content. Each file is read in order, but the order
between files is not fixed, so the order of the list is not compared.

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

`TestRecording` uses the same setup as `TestPodLogs`. It replays the recording
20 times faster than it happened and counts the events per role, type and
method, which must equal the counts in
[`testdata/capture/README.md`](../testdata/capture/README.md). Every `LOGIN`
must carry its CN from the session table. Halfway through, Kafka stops for five
seconds and comes back with its data, which shows that the queue settings hold
events during an outage.

The capture workflow makes a new recording. It is started by hand, because it
installs k3s and runs for several minutes, and a new recording is only needed
after a CloudNativePG or Kubernetes upgrade.

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
