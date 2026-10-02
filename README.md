# CNPG to Kafka

A minimal Linux/amd64 OpenTelemetry Collector distribution:

```text
pod log files → native container/JSON parsing and filters
              → Avro mapping → native Kafka exporter (raw bytes)
```

It emits successful session authorization (`LOGIN`), logged PostgreSQL
disconnection (`LOGOUT`), and explicit authentication/access rejection
(`LOGIN_FAILED`). Receipt/authentication-progress messages and unrelated FATAL
errors are not connection events. Primary and replica records use the same
rules.

Select one namespace and optionally a pod-name regex in the native
[`config.yaml`](config.yaml). No regex means no extra pod-name restriction.
Role exclusions are exact and case-sensitive, and suppress only successful
logins and logouts; recognizable failed logins always pass that role filter.
`hostname` means the connecting client's recorded hostname/IP, not the database
pod or node. Missing identity on a recognizable failure is an empty string;
missing database is an empty string. Extra fields are ignored, invalid JSON and
unrecognizable events are skipped, and successful login/logout records missing
necessary output fields may be skipped with DEBUG diagnostics.

Each Kafka value is one event: `0x00`, the four-byte big-endian Schema Registry
ID, then the Avro binary datum. There is no JSON or OTLP envelope. The example
schema is intentionally small; it does not claim compatibility with a private
schema. The schema fetched at startup is authoritative. The private-schema fork
boundary is [`Event`/`EventContext`](processor/avroprocessor/event.go) and the
[example schema](schema/connection-event.avsc); adapt the mapper with them.

## Build and run

Use the Go version declared in `go.mod` and GNU `patch`. The shared preparation
script generates vendor sources and applies the two [disclosed native dependency
feature cuts](patches/README.md). These are local modifications, not equivalent
upstream releases; canonical module identities/versions remain scanner-visible.

```sh
./scripts/prepare-go.sh
go test -mod=vendor ./...
mkdir -p bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -o bin/cnpg-to-kafka ./cmd/collector
./bin/cnpg-to-kafka validate --config=config.yaml
./bin/cnpg-to-kafka --config=config.yaml
```

Before running, edit `config.yaml`, supply its referenced certificate files,
and ensure the destination topic and external registry subject already exist.
Registry lookup happens once per startup; **the runtime never registers schemas
or creates topics**. No production deployment or provisioning is performed by
these commands.

See [operations and the plain DaemonSet example](docs/operations.md) for mounts,
configuration, shutdown and durability boundaries, and
[filesystem replay](docs/replay.md) for replaying the deliberate real capture.
Routine PR acceptance must replay the checked-in real recording, without
installing k3s or CNPG. Test setup alone provisions disposable services, topic
and sample schema. The acceptance workflow must print consumed, Avro-decoded
Collector events and upload `decoded-events.jsonl`, including available output
on failed end-to-end runs. The recording's deliberate capture run is linked in
[replay documentation](docs/replay.md). Passing replay-based Actions and final-image
CVE acceptance are not yet established; workflow descriptions are not completion
claims. See [local real-broker acceptance](docs/operations.md#local-real-broker-acceptance)
for the Compose/replay test commands.

## Inspect Kafka events

Use the same runtime YAML and mounted TLS files; no separate consumer config is
needed:

```sh
go run -mod=vendor ./cmd/inspect --config config.yaml --limit 10 --timeout 30s
```

The helper consumes from the earliest available records without a consumer group
or offset commits, resolves each record's schema ID, and prints decoded JSONL to
stdout (errors go to stderr). `--limit 0` follows until timeout or signal; a
positive limit not reached by the deadline exits with an error. Unlike the
running Collector's startup-only lookup, the inspector needs registry access to
resolve historical wire IDs (cached by the library). This is read-only
inspection: it does not create a topic or register a schema.

## Image boundary

The runtime image is a static amd64 binary in scratch, without a built-in CA
bundle, shell, runtime configuration or credentials. Build/test PR images do not
receive publishing credentials. Only the [gated protected-main release DAG](docs/releases.md)
publishes to GHCR, after replay acceptance succeeds and Trivy scans that frozen
commit's actual final artifact with **zero reported known CVEs at any severity,
including unfixed findings**. Scanner
failure blocks publication. This is a publish-time check, not a promise about
future or unknown vulnerabilities.
