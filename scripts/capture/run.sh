#!/usr/bin/env bash
# Disposable capture playground only. Never run against a production node.
set -euo pipefail
out=${1:-testdata/capture}
mkdir -p "$out"
out=$(realpath "$out")
cnpg=$(curl -fsSL https://api.github.com/repos/cloudnative-pg/cloudnative-pg/releases/latest | jq -er .tag_name)
minor=${cnpg#v}; minor=${minor%.*}
# Select the newest Kubernetes minor actually supported by this CNPG release.
supported=$(curl -fsSL "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/$cnpg/docs/src/supported_releases.md")
kubernetes=$(awk -F '|' -v version="$minor.x" '{gsub(/ /,"",$2)} $2 == version {print $6; exit}' <<< "$supported" | grep -oE '1\.[0-9]+' | sort -V | tail -1)
[[ -n $kubernetes ]]
k3s=$(curl -fsSL 'https://api.github.com/repos/k3s-io/k3s/releases?per_page=100' | jq -er --arg prefix "v$kubernetes." '[.[] | select(.prerelease == false and (.tag_name | startswith($prefix)))][0].tag_name')
curl -fsSL https://get.k3s.io -o /tmp/install-k3s.sh
# K3s supports native kubelet config drop-ins; monitor interval has no CLI flag.
sudo mkdir -p /var/lib/rancher/k3s/agent/etc/kubelet.conf.d
sudo tee /var/lib/rancher/k3s/agent/etc/kubelet.conf.d/10-capture.conf >/dev/null <<'YAML'
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
containerLogMaxSize: 100Ki
containerLogMaxFiles: 3
containerLogMonitorInterval: 3s
YAML
sudo env INSTALL_K3S_VERSION="$k3s" INSTALL_K3S_EXEC='server --disable traefik --disable servicelb --write-kubeconfig-mode 644' sh /tmp/install-k3s.sh
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
# Installation starts the service but can return before the API/node exists.
ready=false
for _ in $(seq 1 90); do
  if kubectl get --raw=/readyz >/dev/null 2>&1 && [[ -n $(kubectl get nodes -o name) ]]; then ready=true; break; fi
  sleep 2
done
if ! "$ready"; then sudo journalctl -u k3s -n 80 --no-pager; exit 1; fi
kubectl wait --for=condition=Ready node --all --timeout=180s
kubectl apply --server-side -f "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/${cnpg}/releases/cnpg-${cnpg#v}.yaml"
kubectl rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout=300s
kubectl create namespace capture
kubectl create namespace other
kubectl apply -f scripts/capture/playground.yaml
kubectl wait -n capture --for=condition=Ready cluster/cnpg --timeout=600s
kubectl wait -n capture --for=condition=Ready pod/client pod/noise --timeout=300s
kubectl wait -n other --for=condition=Ready pod/noise --timeout=300s
primary=$(kubectl get cluster cnpg -n capture -o jsonpath='{.status.currentPrimary}')
kubectl exec -n capture "$primary" -c postgres -- psql -U postgres -d app -v ON_ERROR_STOP=1 -c "CREATE ROLE included LOGIN PASSWORD 'capture-only'; CREATE ROLE excluded LOGIN PASSWORD 'capture-only';"
# Wait for the role DDL to replicate before beginning the fixed capture interval.
for pod in cnpg-1 cnpg-2 cnpg-3; do
  ready=false
  for _ in $(seq 1 30); do
    if [[ $(kubectl exec -n capture "$pod" -c postgres -- psql -U postgres -Atc "SELECT count(*) FROM pg_roles WHERE rolname IN ('included','excluded')") == 2 ]]; then ready=true; break; fi
    sleep 1
  done
  "$ready"
done
{
  printf 'CNPG: %s\nk3s: %s\n' "$cnpg" "$k3s"
  printf 'PostgreSQL: '
  kubectl exec -n capture "$primary" -c postgres -- psql -U postgres -Atc 'SHOW server_version'
  kubectl get pods -n capture -o custom-columns='POD:.metadata.name,IMAGE:.spec.containers[*].image,IMAGE_ID:.status.containerStatuses[*].imageID,IP:.status.podIP'
  printf 'Duration: 300 seconds after setup/readiness; kubelet rotation threshold: 100Ki, monitor interval: 3s\n'
  printf 'Capture source ref: %s\nCapture run: %s\n' "${GITHUB_SHA:-local}" "${GITHUB_SERVER_URL:-local}/${GITHUB_REPOSITORY:-local}/actions/runs/${GITHUB_RUN_ID:-local}"
} > "$out/versions.txt"
sudo python3 scripts/capture/record.py /var/log/pods "$out" &
recorder=$!
trap 'sudo kill "$recorder" 2>/dev/null || true; sudo chown -R "$(id -u):$(id -g)" "$out"' EXIT
for _ in $(seq 1 30); do [[ -e $out/ready ]] && break; sleep 1; done
[[ -e $out/ready ]]
read -ra ips <<< "$(kubectl get pod -n capture cnpg-1 cnpg-2 cnpg-3 -o jsonpath='{range .items[*]}{.status.podIP}{" "}{end}')"
kubectl exec -i -n capture client -- bash -s -- "${ips[@]}" < scripts/capture/activity.sh >> "$out/activity.log" 2>&1 &
activity=$!
# Stop one replica container through the real runtime, not a fabricated log rename.
sleep 145
replica=$(kubectl get pods -n capture -l cnpg.io/instanceRole=replica -o jsonpath='{.items[0].metadata.name}')
container=$(kubectl get pod -n capture "$replica" -o jsonpath='{.status.containerStatuses[?(@.name=="postgres")].containerID}')
printf 'Controlled container restart: %s %s\n' "$replica" "$container" >> "$out/activity.log"
sudo k3s crictl stop "${container#containerd://}"
wait "$recorder"
wait "$activity"
kubectl get cluster,pods -n capture > "$out/final-status.txt"
sudo chown -R "$(id -u):$(id -g)" "$out"
trap - EXIT
