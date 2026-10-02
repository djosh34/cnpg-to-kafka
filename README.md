# cnpg-to-kafka

cnpg-to-kafka publishes one Kafka message for each login, logout and failed
login on a [CloudNativePG](https://cloudnative-pg.io) PostgreSQL cluster. It is
for teams that run CloudNativePG on Kubernetes and want connection events in
Kafka, for example for an audit trail.

It is a build of the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
with four components. The file log receiver reads the pod logs on each node and
picks out the connection records. A processor from this repository encodes each
one as Avro with a schema from your Schema Registry. The Kafka exporter
publishes it, and the file storage extension keeps read positions and unsent
events on disk.

## An event

The value of each Kafka message is one Avro record in the Confluent wire format.
Decoded, it looks like this:

```json
{"role":"app","hostname":"10.42.0.6","eventtype":"LOGIN","context":{"database":"app"}}
```

| Field | Content |
|---|---|
| `role` | The PostgreSQL role that connected. |
| `hostname` | The client's host name or IP address as PostgreSQL logged it, without the port. |
| `eventtype` | `LOGIN`, `LOGOUT` or `LOGIN_FAILED`. |
| `context.database` | The database the client asked for. |

A `LOGIN_FAILED` event has an empty string for every field that PostgreSQL did
not log. The schema is [`schema/connection-event.avsc`](schema/connection-event.avsc).

Logins and logouts of the roles `postgres` and `streaming_replica` are not
published. CloudNativePG's instance manager connects as `postgres` about once a
second, and the replicas connect as `streaming_replica`. Their failed logins are
published. You can change the list in
[`config.yaml`](config.yaml).

## Prerequisites

- PostgreSQL must log connections. In the CloudNativePG `Cluster`, set
  `log_connections: 'on'` and `log_disconnections: 'on'` under
  `spec.postgresql.parameters`.
- The Kafka topic must exist. cnpg-to-kafka does not create it.
- The Schema Registry subject must exist and hold a schema with the fields
  above. cnpg-to-kafka reads the schema and never registers one.
- A certificate authority file for Kafka and for the registry, plus a client
  certificate and key if they ask for one. The example configuration uses
  mutual TLS. The Collector's other
  [TLS](https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configtls/README.md)
  and [Kafka authentication](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/kafkaexporter/README.md)
  settings work too.

## Configure

Copy [`config.yaml`](config.yaml) and change the namespace of your cluster, the
Kafka brokers and topic, the registry URLs and subject, and the certificate
paths. `${env:NAME}` in a value reads the environment variable `NAME`.
[docs/operations.md](docs/operations.md) explains the settings.

## Run

The image is `ghcr.io/djosh34/cnpg-to-kafka`. Use a version tag such as
`v0.2.0`. It holds one static binary for linux/amd64 and nothing else, so it has
no CA certificates and no shell. It reads `/etc/cnpg-to-kafka/config.yaml`
unless you pass `--config`.

[`deploy/daemonset.yaml`](deploy/daemonset.yaml) runs it on every node.
[docs/operations.md](docs/operations.md) has the commands.

To build and run it yourself you need the Go version named in `go.mod`:

```sh
go build -o bin/cnpg-to-kafka ./cmd/collector
bin/cnpg-to-kafka validate --config config.yaml
bin/cnpg-to-kafka --config config.yaml
```

## Inspect

`cmd/inspect` prints the events in the topic as JSON lines, starting at the
earliest offset of each partition. It takes the brokers, the topic, the registry
and the certificates from the same config file.

```sh
go run ./cmd/inspect --config config.yaml --limit 10
```

Without `--limit` it prints events until `--timeout` passes, 30 seconds by
default. It joins no consumer group and commits no offsets.

## Test

```sh
go test -v ./...
```

The tests need no Docker and no setup. One of them replays a recording of real
CloudNativePG pod logs through the Collector into an in-process Kafka and prints
every event that comes out. [docs/design.md](docs/design.md) describes it.

## More

- [docs/operations.md](docs/operations.md): deployment, tuning the event rules,
  what survives an outage, shutdown.
- [docs/design.md](docs/design.md): why it is built this way.
