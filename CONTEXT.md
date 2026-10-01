# CNPG connection events

Vocabulary for observing CNPG connection activity and replaying its Kubernetes pod-log behavior.

## Language

**Connection event**:
A login, logout (disconnect), or failed-login event derived from CNPG logs. These are the only activity categories intended for Kafka output.

**Excluded role**:
A database role whose activity must not generate connection events.
_Avoid_: Filtered role (ambiguous about inclusion versus exclusion)

**Capture run**:
An observation of a real k3s environment containing CNPG primary and replica pods, connection activity, and unrelated pods, used to establish actual pod-log filesystem behavior.

**Replay recording**:
The reusable artifact from a capture run that describes observed pod-log content and filesystem behavior, including rotation.
_Avoid_: Guessed fixtures

**Replay producer**:
The simple Go test program that reproduces a replay recording on a real filesystem for routine tests without starting k3s or CNPG.
_Avoid_: Mock watcher
