# Operations

## Deploy

[`deploy/daemonset.yaml`](../deploy/daemonset.yaml) runs one Collector on every
linux/amd64 node. Each one reads the pod logs of its own node, so every node
that can host a PostgreSQL instance needs one. Add tolerations if those nodes
are tainted.

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
```

The pod mounts four things.

| Mount | Content |
|---|---|
| `/var/log/pods` | The node's pod logs, read-only. |
| `/var/lib/cnpg-to-kafka` | A directory on the node for read positions and the queue of unsent events. |
| `/etc/cnpg-to-kafka/config.yaml` | The configuration, from the ConfigMap. |
| `/etc/cnpg-to-kafka/tls` | The certificates, from the Secret. |

The container runs as root because the pod logs and the state directory belong
to root on the node. It drops all capabilities and has a read-only root
filesystem. It needs no Kubernetes API access, so the pod has no service account
token.

The Collector reads its configuration and certificates once, at startup. There
is no hot reload. After you change the ConfigMap or the Secret, restart the pods:

```sh
kubectl -n observability rollout restart daemonset/cnpg-to-kafka
```

The image has no CA certificates. Set `ca_file` for Kafka and for the registry,
also when a public certificate authority signed their certificates.

## Settings in config.yaml

[`config.yaml`](../config.yaml) is a complete example. These are all the
settings in it.

### Receiver `file_log/cnpg`

| Setting | Meaning |
|---|---|
| `include` | The pod logs to read, `/var/log/pods/<namespace>_<pod>_<uid>/<container>/<n>.log`. `database_*` reads every pod of the namespace `database`. Other containers and lines that are not PostgreSQL records are dropped by the cnpg processor. |
| `exclude` | Skips the kubelet's compressed rotated logs. |
| `start_at` | `beginning`, so that a new file is read from its first line. |
| `include_file_path` | Adds the file path to each record. The `container` operator takes the namespace and pod name from it. |
| `storage` | Saves read positions in `file_storage`. |
| `max_log_size` | The longest log line. A longer one is cut. |
| `max_concurrent_files` | `1`. Reads one file at a time. Collector v0.162 has a data race when it reads several, and the session join needs one file at a time. Keep it at 1. |
| `poll_interval` | How often to look for new lines. |
| `retry_on_failure` | Retries a batch that the pipeline did not accept, without a time limit. |
| `operators` | Only `container`, which parses the container runtime's line format and sets the namespace and pod name. `on_error: drop` drops a line it cannot parse. Keep it as it is. |

### Processor `cnpg`

| Setting | Meaning |
|---|---|
| `source_hostname` | Required. The host name of the database, put in `hostdata.source_hostname`. It is looked up once at startup, and the first address goes in `hostdata.source_ip`. The Collector does not start if the lookup fails. |
| `additional_fields.application_name` | Put in `application_name` on every event. |
| `high_privilege_roles` | Roles whose events have `account_type` `ha`. Every other role is `npa`. The match is exact and case-sensitive. |
| `trusted_connections` | A list of `{role, method, identity}`, or `{role, method: cert, common_name}` for a certificate login. A login is not published when it matches one entry exactly. A logout is not published when its role is in any entry. Failed logins are always published. The default is an empty list. |

Every entry needs `role` and `method`. An entry with `method: cert` also needs
`common_name` and must not have `identity`. An entry with any other method also
needs `identity` and must not have `common_name`. The Collector does not start
with any other combination, and its error names the entry.

The identity of a trusted connection is what PostgreSQL logs in
`connection authenticated: identity="..."`: the operating system user for
`peer`, the role for a password method. For `trust`, PostgreSQL logs
`user="..."` instead, and that is the identity.

For `cert`, PostgreSQL logs the certificate subject, such as
`CN=streaming_replica,OU=Databases,O=Example Corp`. A `cert` entry matches only
the common name (CN) in it, so `common_name: streaming_replica` matches that
subject whatever its OU and O are. The common name is compared after
PostgreSQL's escapes are undone, so it is the name as it was typed when the
certificate was made: `common_name: "Smith, John"` matches the logged
`CN=Smith\, John`. The match is exact and case-sensitive. A subject with no CN
or with more than one CN matches no entry, so its login is published.

To make a certificate with a non-ASCII common name, pass `-utf8` to
`openssl req`. Without it `openssl` stores the name wrongly in the certificate,
and it does not match what you typed.

### Processor `avro`

| Setting | Meaning |
|---|---|
| `registry.urls` | Required. The Schema Registry URLs, tried in order. |
| `registry.subject` | Required. The subject that holds the event schema. |
| `registry.version` | `latest` or a version number in quotes. The default is `latest`. |
| `registry.request_timeout` | Time limit for one request to one URL. The default is 10 seconds. |
| `registry.tls` | Certificates for the registry. |

### Exporter `kafka/cnpg`

| Setting | Meaning |
|---|---|
| `brokers` | The Kafka brokers. |
| `client_id` | The client ID that Kafka sees. |
| `logs.topic` | The topic. It must exist. |
| `logs.encoding` | `raw`. Sends the Avro message that the avro processor made. Keep it. |
| `tls` | Certificates for Kafka. |
| `producer.required_acks` | `all`. Waits for all in-sync replicas, and makes the producer idempotent. |
| `producer.allow_auto_topic_creation` | `false`, so a typo in the topic does not create a topic. |
| `timeout` | Time limit for one produce request. |
| `retry_on_failure` | Retries without a time limit. |
| `sending_queue` | The queue of unsent events, on `file_storage`. It holds 64 MiB of events and blocks the receiver when full. `num_consumers: 1` keeps the order. |

### Extension `file_storage`

| Setting | Meaning |
|---|---|
| `directory` | The state directory for read positions and the queue. |
| `create_directory`, `directory_permissions` | Creates it with mode 0700. |
| `fsync` | Writes to disk before it reports success. |
| `max_size` | The limit for each of its two database files. |

### Service

`service.extensions` starts `file_storage`. `service.pipelines.logs` connects
the components: `file_log/cnpg`, then `cnpg` and `avro`, then `kafka/cnpg`.
`service.telemetry.logs.level` sets the Collector's own log level, and
`metrics.level: none` turns its metrics off.

The receiver, the exporter and the extension are the upstream ones, and their
own documentation lists every setting:
[file log receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/receiver/filelogreceiver/README.md),
[Kafka exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/kafkaexporter/README.md),
[file storage](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/extension/storage/filestorage/README.md).

## Change what is published

The rules that make events are Go code in `internal/event`. The config changes
which events are published:

- To hide another connection that your platform makes, add its role, method and
  identity to `trusted_connections`, or its role, `method: cert` and common name
  for a certificate login. Its logouts are then hidden too.
- To mark another role as high-privilege, add it to `high_privilege_roles`.

A `LOGIN` with a null CN and method is published even when its role is trusted,
because it cannot match. That happens when the `connection authenticated` record
was not read before the `connection ready` record: on the first read of a node's
logs, when a rotated file is read after the new one, or after a restart of the
Collector between the two records. [design.md](design.md#the-session-join) has
the details.

To publish different fields, change the schema, the `Event` struct in
`internal/event/event.go`, its attributes in `internal/event/attributes.go`
and the rules together. The tests in `internal/event` show each rule.

## Schema Registry

At startup the processor asks the first URL for the configured version of the
subject. If the request fails, or the answer is not a schema it can parse, it
asks the next URL. All URLs must belong to the same registry, because the schema
ID from the answer goes into every message. If no URL works, the Collector exits
and Kubernetes restarts it.

The processor keeps the schema for as long as it runs. It does not contact the
registry again, so a registry outage after startup has no effect. A new schema
version takes effect at the next restart. Events that are already in the queue
keep the schema they were encoded with.

The processor never registers a schema.

## Kafka

The exporter does not create the topic. With `required_acks: all` it waits for
all in-sync replicas and produces idempotently. How many replicas that is depends
on the topic's replication factor and `min.insync.replicas`, which you set on
the broker.

## What survives an outage

Delivery is at least once. The settings in `config.yaml` that give this are the
file storage with `fsync`, the exporter's `sending_queue` on that storage, and
retries without a time limit.

- Kafka is down. Events wait in the queue on disk and go out when Kafka is back.
  The queue holds 64 MiB of events. When it is full, the receiver stops reading
  and the logs stay in the pod-log files until there is room.
- The pod restarts or the Collector is killed. It continues from the saved read
  positions and sends what is in the queue.
- The node or its disk is lost. The state directory is gone, and events that
  were only in the queue are lost.
- The outage lasts longer than the kubelet keeps log files. The kubelet deletes
  rotated logs that the receiver has not read, and those events are lost.

A crash can publish an event twice. That happens when the Collector dies after
it queued an event and before it saved the read position, or after Kafka
accepted an event and before the queue removed it. A normal stop and start
publishes nothing twice.

A crash can also lose a log line that the container runtime wrote in several
parts, if only some parts were read. The receiver keeps such parts in memory.

On the first start, with an empty state directory, the receiver reads all
existing log files from the beginning. Deleting the state directory therefore
republishes every event that is still in the logs.

`file_storage.max_size` limits each of the two database files, one for read
positions and one for the queue, to 256 MiB. Leave room for both on the node's
disk.

## Shutdown

On SIGTERM the Collector sends what is in flight and exits. During a Kafka
outage it waits for Kafka, because giving up on a request that Kafka may have
accepted would break the idempotent producer's sequence. Kubernetes kills the
pod after `terminationGracePeriodSeconds`, 60 in the example. The next pod sends
the events that are saved in the queue.

## Logs

The Collector logs to standard error. The cnpg processor logs a warning when it
drops a connection event because its `log_time` does not parse, and the avro
processor when it drops an event that the schema cannot encode. Set
`service.telemetry.logs.level` to `debug` to see what the receiver reads.

To see each event as it leaves the processors, add the debug exporter to
`config.yaml`:

```yaml
exporters:
  debug:
    verbosity: normal
  kafka/cnpg:
    ...
service:
  pipelines:
    logs:
      exporters: [kafka/cnpg, debug]
```

It writes to the Collector's log. For each batch it writes the number of events,
a line for each pod, then one line for each event: the Avro body in base64,
then the record's attributes as `key=value`. These are the event fields, which
start with `cnpg.`, and the file the line came from. `verbosity: basic` writes
only the number of events in each batch, and `detailed` writes every field of
every record over many lines. It does not show whether Kafka received an event,
and it does not show events that were already in the queue when you added it.
Remove it again when you are done, because it logs every event. Its
documentation lists every setting:
[debug exporter](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/debugexporter/README.md).
