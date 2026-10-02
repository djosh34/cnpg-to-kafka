# Deliberate real capture

This workflow is **not** routine PR CI. It provisions a disposable Ubuntu amd64
runner with k3s, latest stable CNPG, PostgreSQL 18, one primary/two replicas,
and synthetic client/noise pods in `capture` and `other`. Never run `run.sh`
on an existing/production Kubernetes node.

Before merging the workflow, push its implementation branch and an explicit
`cnpg-capture-*` tag pointing to that ref. Afterward, `workflow_dispatch` can
refresh it manually. Ordinary branch pushes/PRs do not install a cluster.

```sh
git push origin HEAD
git tag cnpg-capture-YYYYMMDD-N
git push origin cnpg-capture-YYYYMMDD-N
# Download the named cnpg-capture artifact from that run, inspect it, then commit
# operations.jsonl and versions.txt under testdata/capture/ through normal review.
gh run download RUN_ID --repo djosh34/cnpg-to-kafka --name cnpg-capture --dir /tmp/capture-run
```

The script resolves the newest stable CNPG and newest k3s patch in its supported
Kubernetes range. PostgreSQL's `18-system-trixie` image resolves the current PG18
patch; `versions.txt` records actual server version and pulled image IDs. Native
kubelet config lowers rotation to 100Ki/three files/three-second monitoring
(the Kubernetes 1.36 minimum).
Connection/disconnection logging is enabled; direct TCP sessions cover each pod,
`included`/`excluded` roles, bad passwords and an unknown role. Ordinary PostgreSQL
LOG output causes actual kubelet rotation; the runtime stops one replica container
mid-interval so kubelet restarts it. The recorder stops at 300 seconds after
readiness, not when a checklist happens to pass. Inspect the saved output manually;
if activity or capture failed, diagnose it and rerun a new deliberate capture.

## Recording interface

`record.py` is test-only glue around native `inotifywait`, never a runtime watcher.
It reads only the dedicated synthetic namespaces' pod-log directories. It records
actual raw CRI bytes, not `kubectl logs` or reconstructed JSON. Open descriptors
retain the observed inode across rename. An initial snapshot supplies real backlog;
later reads collapse coalesced notifications into observed byte appends.

Each `operations.jsonl` line is `{at_ms,op,path,to?,data?}`:

- `at_ms`: integer milliseconds since capture start, ordered as observed.
- `path`: relative to `/var/log/pods`, with the namespace/pod/UID layout intact.
- `mkdir`, `create`, `append`, `rename`, `remove`: ordinary filesystem operations.
- `data`: standard base64 raw bytes for create/append, including CRI framing.
- `to`: relative rename destination.

Directory creation and initial file content are represented using mkdir/create;
timestamps describe observation rather than original syscall boundaries. No
symlink/truncate operations are invented. `/var/log/containers` is not the input.
`inotify.log`, `activity.log`, and `final-status.txt` are ordinary temporary debug
output; Actions artifacts may keep them but are not the durable replay source.

KISS checks during implementation: native inotify provides observation and native
kubelet/containerd provide rotation/restart. Only a tiny recorder is necessary to
retain bytes and translate those observations to the agreed Go replay interface;
there is no evidence-completeness gate, hashing/inventory system, or automated
capture-until-complete loop. Dedicated PR KISS review must challenge this again.
