#!/usr/bin/env bash
# Test provisioning only; the Collector and inspector never create these objects.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
project=${1:-cnpg-e2e}
schema=${2:-"$root/schema/connection-event.avsc"}
compose=(docker compose -p "$project" -f "$root/integration/compose.yaml")
certs="$root/integration/.certs"
# Preserve live identities on repeat setup; generating a new CA underneath a
# running broker would invalidate its already-loaded certificates.
if [[ ! -f "$certs/ca.crt" ]]; then
  "$root/scripts/e2e-certs.sh"
fi
"${compose[@]}" up -d --wait --wait-timeout 150
curl_args=(--silent --show-error --fail --max-time 5 \
  --cacert "$certs/ca.crt" --cert "$certs/client.crt" --key "$certs/client.key")
# Broker health alone does not imply the built-in registry is ready.
ready=false
for ((attempt=0; attempt<60; attempt++)); do
  if curl "${curl_args[@]}" https://localhost:18081/subjects >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  "${compose[@]}" logs redpanda
  exit 1
fi
"${compose[@]}" exec -T redpanda rpk topic create cnpg-connections \
  --partitions 1 --replicas 1 --if-not-exists
# jq quotes the complete authoritative Avro schema; no handcrafted JSON escaping.
jq -n --rawfile schema "$schema" '{schema: $schema, schemaType: "AVRO"}' |
  curl "${curl_args[@]}" -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data-binary @- https://localhost:18081/subjects/cnpg-connections-value/versions
printf '\nTest services ready: Kafka localhost:19092, registry https://localhost:18081\n'
