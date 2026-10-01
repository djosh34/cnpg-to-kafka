# Operations

## One runtime configuration

All runtime settings belong to the native [`config.yaml`](../config.yaml):
source namespace/optional pod regex, role filtering, Kafka endpoints/topic/TLS,
storage and queue limits, and registry URLs/subject/version/timeout/mTLS.
Certificate material is referenced by **file path only**. Build-time manifests,
Kubernetes mounts and test-service configuration are not additional application
configuration sources. There is no environment-override framework or hot reload;
validate the YAML and restart the Collector after configuration changes.

In the example, change the namespace prefix in
`receivers.filelog/cnpg.include` (`database_*`), the `select-pods` operator's
pod-name expression (`.*` selects all names), and the exact `excluded-roles`
list. Kafka settings are under `exporters.kafka/cnpg`: `brokers`, `logs.topic`,
and top-level `tls`. Registry startup settings are under
`processors.avro.registry`: `urls`, `subject`, `version` (`latest` or a quoted
positive decimal), `request_timeout`, and `tls`.

Native filelog/container parsing supplies pod-path metadata and handles CRI
fragments and rotation. Native JSON parsing/filtering selects the connection
categories and normalizes only the mapper's `role`, `hostname`, `eventtype` and
`database` attributes. There is no Kubernetes metadata lookup, API enrichment,
whole-record validator or reverse DNS. For the pinned v0.162 container parser,
`max_concurrent_files: 1` and `max_batches: 0` serialize native file polling to
avoid its shared timestamp-parser state race. Unlimited batches still visit all
matching files: this is a throughput tradeoff, not a narrower source selection
or a custom parser/lock. Retain it until an upstream fix is verified.

### Registry and Kafka

The registry URL list must be nonempty and ordered. Every URL must serve the
same logical registry/schema-ID space, using one configured client trust and
identity. At startup, lookup the configured subject at `latest` or a fixed
version, with a finite per-request timeout. A complete lookup includes response
decoding and Avro schema parsing: any failure advances to the next URL. First
success loads both ID and schema; all failures stop startup with diagnostics.
Kubernetes can then apply its ordinary restart/backoff behavior. Cancellation is
honored. There is no per-event lookup, schema registration, schema cache across
restarts, endpoint promotion or hidden retry router.

The checked-in schema is an example for test setup and the user's future mapper
fork, not a local-schema-equality requirement. Changing a fixed version or using
`latest` affects the next startup, not already queued events: queue bodies retain
the schema ID and binary datum with which they were encoded.

The destination topic must already exist. Native Kafka production disables
automatic topic creation and uses all-ISR acknowledgements with idempotent
production. Broker replication and `min.insync.replicas` are operator-owned;
client settings and a one-broker test cannot establish production quorum or
replication durability. A socket write is not acknowledged delivery.

TLS trust roots, client certificate and key are mounted files. Configure an
explicit CA for Kafka and registry; there is no bundled/default/system-root
fallback or disabled peer verification. The native TLS flag is
`include_system_ca_certs_pool: false`; this does not replace the required
`ca_file`. Ensure the certificates' names match the
configured endpoints. If the two services use different trust roots, provide
explicitly configured trust files as appropriate; do not enable insecure TLS to
work around a mismatch. Never commit client private keys.

## Plain DaemonSet example

[`deploy/daemonset.yaml`](../deploy/daemonset.yaml) is an example, not a deployment
performed by this project. It targets Linux amd64 nodes, mounts `/var/log/pods`
read-only and stores offsets/queued events in node-local host-backed
`/var/lib/cnpg-to-kafka`. The same host path survives replacement of the pod on
that node; it does **not** survive loss/replacement of the node or its disk.

The example uses root only to access normally root-owned pod logs and the
root-owned host state directory. It drops every capability, disables privilege
escalation, uses the runtime's default seccomp profile and a read-only container
root filesystem; it does not require a privileged container, host networking or
a host PID namespace. Where node file permissions permit it, an operator can
precreate the state directory and run as a dedicated UID instead, also adapting
Secret ownership/mode so that UID can read the certificate files (the example's
`0400` Secret files are root-readable only). Restricted Pod Security environments
may require explicit authorization for hostPath/root: do not grant extra
privileges to work around admission silently.

No Role, ClusterRole or API metadata permissions are needed. Service-account
token automount is disabled. The selected log namespace need not equal the
Collector's deployment namespace. Adapt scheduling/tolerations for the nodes
you intend to collect from; the example does not implicitly tolerate every
control-plane taint.

For an authorized non-production installation, prepare the files and replace
the image reference with an immutable reviewed release tag or digest. The
following commands illustrate preparation only; do not execute them against
production as part of this work:

```sh
kubectl create namespace observability
kubectl -n observability create configmap cnpg-to-kafka-config \
  --from-file=config.yaml=./config.yaml
kubectl -n observability create secret generic cnpg-to-kafka-tls \
  --from-file=ca.crt=/path/to/ca.crt \
  --from-file=tls.crt=/path/to/client.crt \
  --from-file=tls.key=/path/to/client.key
kubectl apply -f deploy/daemonset.yaml
kubectl -n observability rollout status daemonset/cnpg-to-kafka
kubectl -n observability logs daemonset/cnpg-to-kafka
```

Mounted paths are `/etc/cnpg-to-kafka/config.yaml` and
`/etc/cnpg-to-kafka/tls/{ca.crt,tls.crt,tls.key}`. The YAML's storage directory must
match `/var/lib/cnpg-to-kafka`, and all certificate paths must match the mounted
files. Extra distinct trust files can be keys in the same Secret. No certificate
bytes or runtime config are baked into the scratch image. The ConfigMap uses a
`subPath` mount, so replace/restart the pod after a configuration update. Restart
after replacing certificates too: the Collector loads TLS/registry state at
startup rather than promising live credential reload.

## Local real-broker acceptance

Use Linux amd64, the Go version in `go.mod`, GNU `patch`, Docker with Compose v2,
Bash, OpenSSL, curl, and jq. This uses one disposable Redpanda container and its built-in
registry with generated file-only Kafka TLS/registry mTLS identities. It never
installs k3s or refreshes the recording. Ports `19092` and `18081` must be free;
do not run competing Compose projects on those fixed ports.

From the repository root:

```sh
./scripts/prepare-go.sh
mkdir -p bin
CGO_ENABLED=0 go build -mod=vendor -p 1 -o bin/cnpg-to-kafka ./cmd/collector
bash scripts/e2e-setup.sh cnpg-e2e
CNPG_E2E=1 \
  CNPG_COLLECTOR_BIN="$PWD/bin/cnpg-to-kafka" \
  CNPG_RECORDING="$PWD/testdata/capture/operations.jsonl" \
  CNPG_DECODED_EVENTS="$PWD/decoded-events.jsonl" \
  go test -mod=vendor -p 1 -v -count=1 -run '^TestRealReplay$' -timeout=10m ./integration
```

The opt-in test starts the actual Collector before applying the canonical real
filesystem recording. Test-only native polling (`10ms`) and replay speed `5`
allow observation of early rotations; source fixtures are not injected into a
parser. The test constructs temporary native YAML/storage paths, consumes Kafka,
checks framing, and Avro-decodes the resulting records. Supplemental synthetic
edge cases are separate from the canonical recording. The native queue's focused
saturation/restart tests remain in the ordinary Go test suite.

Decoded events go both to the test console and `decoded-events.jsonl` as they
are consumed, retaining available output on failure. The PR workflow uploads
only that JSONL file, including failed runs; it does not upload TLS keys. The
existing inspector example in the README uses the same native YAML as a running
Collector; for local services, its YAML must reference `localhost:19092`,
`https://localhost:18081`, and the generated `integration/.certs` files.

Setup alone creates the disposable topic and sample schema. Repeating setup
preserves its existing CA; to discard test state, stop the services first:

```sh
docker compose -p cnpg-e2e -f integration/compose.yaml down -v
# Optional, after stopping the broker: remove generated test identities.
rm -rf integration/.certs
```

These are usage instructions, not a claim of passing acceptance or publication.

## Durability and shutdown

- On the first start with no saved offsets, existing matching files are read
  from the beginning. Later starts use upstream persisted offset behavior.
  Keep state intact during restarts and upgrades; deleting it replays source
  backlog and loses any locally queued events.
- Native file storage enables fsync for offsets and the exporter queue. Complete
  durably queued events survive transient Kafka outages and process/pod restarts
  with intact storage. Acknowledged events can be removed from the queue.
- Native receiver/exporter retries do not expire during a transient outage.
  Finite configured queue/storage limits lead to native backpressure, not
  deliberate event eviction. Monitor available host disk space and Collector
  diagnostics. Logs can still rotate away before they are read during a long
  outage; unlimited outages and permanent disk loss are outside the guarantee.
- Source checkpoints and queue insertion are separate durable operations. An
  abrupt crash can duplicate a queued event whose source offset was not saved.
  A broker acknowledgement followed by a crash before local queue deletion can
  also duplicate an event. This is at-least-once, not exactly-once delivery.
- Stock incomplete CRI-fragment state may be lost in an abrupt crash. There is no
  durable fragment-recovery subsystem. Ordinary permanent record rejection uses
  native diagnostics, not a custom dead-letter service. Kafka downtime is not a
  permanent bad-record condition.
- A clean shutdown/restart with intact state should not replay old events.
  Send SIGTERM, allow the process to exit, and retain its state directory; do not
  delete state or use SIGKILL as a normal stop procedure.
- **Shutdown during a Kafka outage can exceed the termination grace.** The native
  queue waits for its consumers before closing the Kafka producer. An in-flight
  idempotent produce may wait for a definitive broker result: cancellation could
  otherwise discard a record the broker accepted and break producer sequencing.
  Therefore a shutdown timeout is not a guarantee of prompt exit. Kubernetes may
  send SIGTERM, wait the example's 60-second grace, then use SIGKILL. That grace
  is a Kubernetes setting, not a guaranteed Collector shutdown bound.
  Keep idempotence enabled; no custom shutdown/retry workaround is required.
  Complete fsync-backed queued events remain recoverable on restart with intact
  storage, including after forced termination. The abrupt-crash duplicate and
  incomplete-CRI limits above still apply; do not discard the queue to recover
  from transient Kafka downtime.

The example queue uses `sizer: bytes` and `queue_size: 67108864` (64 MiB of
serialized requests, **not** exact disk usage), one consumer, and
`block_on_overflow: true`. Native `file_storage.max_size: 268435456` limits each
bbolt database file to 256 MiB, not the entire state directory or host disk.
There are separate offset/queue databases; leave filesystem headroom for all of
them and storage overhead. A growth write over the native limit fails rather
than evicting queued data; native receiver retries/backpressure handle that
failure. Monitor storage errors as well as queue saturation.

Tune those limits and timeouts together with node disk capacity and log retention.
The backing store is not an unlimited outage buffer. The example's CPU/memory
requests are starting points, not a production sizing promise. Use normal
Collector diagnostics (DEBUG for mapper missing-field skips), not a separate
logging or evidence system.
