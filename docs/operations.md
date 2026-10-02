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

| Setting | Meaning |
|---|---|
| `receivers.file_log/cnpg.include` | The pod logs to read. `database_*` selects the namespace `database`. |
| `exporters.kafka/cnpg.brokers` | The Kafka brokers. |
| `exporters.kafka/cnpg.logs.topic` | The topic. It must exist. |
| `exporters.kafka/cnpg.tls` | Certificates for Kafka. |
| `processors.avro.registry.urls` | The Schema Registry URLs, tried in order. |
| `processors.avro.registry.subject` | The subject that holds the event schema. |
| `processors.avro.registry.version` | `latest` or a version number. The default is `latest`. |
| `processors.avro.registry.request_timeout` | Time limit for one request to one URL. The default is 10 seconds. |
| `processors.avro.registry.tls` | Certificates for the registry. |
| `extensions.file_storage.directory` | The state directory. |

Only `urls` and `subject` are required for the processor. The receiver, the
exporter and the extension are the upstream ones, and their own documentation
lists every setting:
[file log receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/receiver/filelogreceiver/README.md),
[Kafka exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/kafkaexporter/README.md),
[file storage](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/extension/storage/filestorage/README.md).

## Tune the event rules

The rules that turn log lines into events are the `operators` of the receiver in
`config.yaml`. They run in order.

| Operator | What to change |
|---|---|
| `select-pods` | The regex decides which pods in the namespace count. `.*` takes all of them. `^pg-main-[0-9]+$` takes the instances of the cluster `pg-main` and skips its job pods. |
| `classify` | Decides the event type. A FATAL record with SQLSTATE `28P01` or `28000` is a failed login. So is one with `42501` and a message that starts with `permission denied for database`. A message that starts with `connection authorized:` or `replication connection authorized:` is a login. One that starts with `disconnection:` is a logout. Every other record is dropped. |
| `excluded-roles` | The roles whose logins and logouts are not published. The match is exact and case-sensitive. Failed logins are always published. |
| `role`, `database`, `client` | Copy the role, the database and the client address from the record. |
| `client-with-port` | Removes the port from the client address. |

The processor drops a login or logout that has no role or no client address,
and logs that at debug level.

`TestEventRules` in `cmd/collector/main_test.go` runs hand-written log lines
through these operators. After a change to the rules, add a line for it and run
`go test ./cmd/collector`.

To publish different fields, change the schema, the `Event` struct in
`processor/avroprocessor/event.go`, the attribute mapping in `factory.go` and
the operators together.

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

The Collector logs to standard error. Set `service.telemetry.logs.level` to
`debug` to see the lines that failed to parse and the records that the processor
dropped.
