# CNPG connection events

Vocabulary for observing CNPG connection activity and replaying its Kubernetes pod-log behavior.

## Language

**Connection event**:
A login, logout (disconnect), or failed-login event derived from CNPG logs. These are the only activity categories intended for Kafka output.

**Excluded role**:
A database role whose successful logins and logouts must not generate connection events. Failed logins are still reported, even for an excluded role.
_Avoid_: Filtered role (ambiguous about inclusion versus exclusion)

**Client host**:
The source hostname or IP address of a database connection, as reported in the CNPG connection log. This is what the sample event's `hostname` means, not the database pod or Kubernetes node.
_Avoid_: Server hostname

**Selected source**:
Pod logs in the configured namespace, optionally narrowed by a pod-name regular expression. Selection does not imply membership verified through a CNPG cluster label.

**Capture run**:
An observation of a real k3s environment containing CNPG primary and replica pods, connection activity, and unrelated pods, used to establish actual pod-log filesystem behavior.

**Replay recording**:
The reusable artifact from a capture run that describes observed pod-log content and filesystem behavior, including rotation.
_Avoid_: Guessed fixtures

**Replay producer**:
The simple Go test program that reproduces a replay recording on a real filesystem for routine tests without starting k3s or CNPG.
_Avoid_: Mock watcher
