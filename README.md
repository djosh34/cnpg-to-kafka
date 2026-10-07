# cnpg-to-kafka

cnpg-to-kafka publishes one Kafka message for each login, logout and failed
login on a [CloudNativePG](https://cloudnative-pg.io) PostgreSQL cluster. It is
for teams that run CloudNativePG on Kubernetes and want connection events in
Kafka, for example for an audit trail.

It is a build of the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
with five components:

1. The file log receiver reads the pod logs on each node.
2. The cnpg processor from this repository turns PostgreSQL's connection
   records into events.
3. The avro processor from this repository encodes each event as Avro, with a
   schema from your Schema Registry.
4. The Kafka exporter publishes it.
5. The file storage extension keeps read positions and unsent events on disk.

## An event

The value of each Kafka message is one Avro record in the Confluent wire format.
Decoded, it looks like this:

```json
{
  "timestamp": 1790893709982,
  "eventtype": "LOGIN",
  "account_type": "npa",
  "application_name": "payments",
  "hostdata": {"source_hostname": "db1.example.com", "source_ip": "192.0.2.1"},
  "connectiondata": {
    "role": "app",
    "database": "app",
    "cn": "app",
    "auth_method": "scram-sha-256",
    "client_address": "10.42.0.6"
  }
}
```

| Field | Content |
|---|---|
| `timestamp` | PostgreSQL's `log_time` of the record, in unix milliseconds. |
| `eventtype` | `LOGIN`, `LOGOUT` or `LOGIN_FAILED`. |
| `account_type` | `ha` when the role is in `high_privilege_roles`, otherwise `npa`. |
| `application_name` | `additional_fields.application_name` from the config. It is the same on every event, and it is not PostgreSQL's `application_name`. |
| `hostdata.source_hostname` | `source_hostname` from the config. |
| `hostdata.source_ip` | The first IP address of `source_hostname`, looked up once at startup. |
| `connectiondata.role` | The PostgreSQL role. Empty when PostgreSQL did not log one. |
| `connectiondata.database` | The database the client asked for. Empty when PostgreSQL did not log one. |
| `connectiondata.cn` | The identity PostgreSQL authenticated, as it logged it. For a certificate login that is the subject, such as `CN=app`. For a password login it is the role name. Null when it is unknown. |
| `connectiondata.auth_method` | The pg_hba method, such as `scram-sha-256`, `cert` or `peer`. Null when it is unknown. |
| `connectiondata.client_address` | The client's address without the port, or `[local]` for the unix socket. |

The schema is [`schema/connection-event.avsc`](schema/connection-event.avsc).

### When each event is made

- `LOGIN` is PostgreSQL's `connection ready` record, which it logs only when
  the whole connection setup worked.
- `LOGIN_FAILED` is a FATAL record during authentication or startup: a wrong
  password, an unknown role, a certificate with the wrong CN, a database that
  does not exist, a denied `CONNECT`, a role without `LOGIN`. "Too many
  connections" is not a failed login and is not published.
- `LOGOUT` is PostgreSQL's `disconnection` record.

PostgreSQL logs the CN and the method on an earlier record of the same
connection, `connection authenticated`. The cnpg processor remembers them for
the next records of that pod and puts them on the `LOGIN` or `LOGIN_FAILED`
that follows. [docs/design.md](docs/design.md#the-session-join) explains how.
A `LOGOUT` always has a null CN and method, because PostgreSQL does not log them
at the end of a session. A failed password login has a null CN, and its method
comes from the pg_hba rule that PostgreSQL quotes in the record.

### Which events are hidden

CloudNativePG itself connects all the time: its instance manager as `postgres`
over the unix socket every second or two, and the replicas as
`streaming_replica` with a certificate. The `trusted_connections` setting hides
these.

- A `LOGIN` is hidden when its role, method and identity all exactly match one
  entry.
- A `LOGOUT` is hidden when its role is in any entry, because a logout has no
  method or identity to match.
- A `LOGIN_FAILED` is always published.

Nothing is hidden by default, and the code knows no CloudNativePG role. The
example [`config.yaml`](config.yaml) trusts `postgres` with `peer` and identity
`postgres`, and `streaming_replica` with `cert` and identity
`CN=streaming_replica`. So `postgres` logging in with a password from the
network is published, and so is `streaming_replica` with another certificate.

A `LOGIN` whose CN and method are unknown cannot match an entry, so it is
published, also for a trusted role. That can happen right after the Collector
starts, see [docs/design.md](docs/design.md#the-session-join).

## Prerequisites

- PostgreSQL must log connections and disconnections. In the CloudNativePG
  `Cluster`, set these under `spec.postgresql.parameters`:
  ```yaml
  log_connections: 'all'
  log_disconnections: 'on'
  ```
  A list for `log_connections` also works if it has both `authentication` and
  `setup_durations`. `authentication` logs the CN and method, and
  `setup_durations` logs `connection ready`, which is the login. Without
  `setup_durations` no `LOGIN` is published. Without `authentication` every
  `LOGIN` has a null CN and method and none is hidden.
- PostgreSQL 18. Older versions do not log `connection ready`.
- The Kafka topic must exist. cnpg-to-kafka does not create it.
- The Schema Registry subject must exist and hold the schema in
  [`schema/`](schema/connection-event.avsc). cnpg-to-kafka reads the schema and
  never registers one.
- A certificate authority file for Kafka and for the registry, plus a client
  certificate and key if they ask for one. The example configuration uses
  mutual TLS. The Collector's other
  [TLS](https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configtls/README.md)
  and [Kafka authentication](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/kafkaexporter/README.md)
  settings work too.

## Configure

Copy [`config.yaml`](config.yaml) and change:

- the namespace of your cluster in the receiver's `include`
- the cnpg processor settings:
  ```yaml
  processors:
    cnpg:
      source_hostname: db1.example.com
      additional_fields:
        application_name: payments
      high_privilege_roles: [postgres, app_admin]
      trusted_connections:
        - {role: postgres, method: peer, identity: postgres}
        - {role: streaming_replica, method: cert, identity: "CN=streaming_replica"}
  ```
- the Kafka brokers and topic, the registry URLs and subject, and the
  certificate paths

`${env:NAME}` in a value reads the environment variable `NAME`.
[docs/operations.md](docs/operations.md) explains every setting.

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

The tests need no Docker and no setup. They go from single functions up to the
whole Collector, which reads sample pod logs and a replayed recording of a real
CloudNativePG cluster into an in-process Kafka.
[docs/design.md](docs/design.md#tests) describes them.

## More

- [docs/operations.md](docs/operations.md): deployment, every setting, what
  survives an outage, shutdown.
- [docs/design.md](docs/design.md): why it is built this way.
