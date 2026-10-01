#!/usr/bin/env bash
# Generate ordinary vendor sources, then apply the disclosed native feature cuts.
set -euo pipefail
cd "$(dirname "$0")/.."

go mod vendor
# Fail closed if the canonical upstream pins drift. Never modify modules.txt.
grep -Fx '# go.opentelemetry.io/collector/config/configtls v1.68.0' vendor/modules.txt
grep -Fx '# github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka v0.162.0' vendor/modules.txt

patch --batch --forward --fuzz=0 -p1 -d vendor < patches/configtls-file-only.patch
patch --batch --forward --fuzz=0 -p1 -d vendor < patches/kafka-no-kerberos.patch
