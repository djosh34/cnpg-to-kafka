# Native dependency feature cuts

The unchanged final-image Trivy gate reported `GO-2026-5932` for
`golang.org/x/crypto v0.57.0` (UNKNOWN severity, no fixed version). The official
[advisory](https://vuln.go.dev/ID/GO-2026-5932.json) covers the unmaintained OpenPGP
packages. Being an unrelated crypto consumer is **not** a waiver for our
zero-reported-CVE contract.

Released-update checks found no supported fix: x/crypto v0.57.0 is latest stable;
master `v0.57.1-0.20260929172509-b39ff6d641ec` still contains OpenPGP.
[Upstream carve-out proposal #81227](https://github.com/golang/go/issues/81227)
is not implemented. The already-selected native configtls v1.68.0 and Kafka
v0.162.0 still import x/crypto through optional TPM and Kerberos support. Updating
franz-go alone cannot remove those independent imports.

## Exact source and changes

- [`configtls v1.68.0`](https://github.com/open-telemetry/opentelemetry-collector/tree/v0.162.0/config/configtls):
  `configtls-file-only.patch` rejects `tpm.enabled` in native validation and
  certificate loading, removes its certificate-loader branch and four TPM
  implementation files. TPM violates this distribution's file-only identity
  contract. Ordinary native CA/certificate/key loading and verification remain
  unchanged.
- [`internal/kafka v0.162.0`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.162.0/internal/kafka):
  `kafka-no-kerberos.patch` removes four Kerberos imports and its private setup
  function; configured `auth.kerberos` returns an explicit unsupported error.
  Kerberos is not a capability of this minimal TLS distribution. Native producer,
  all-ISR acknowledgement/idempotence, retry, queue and storage code is unchanged.

These are local modifications, **not equivalent upstream releases**. The
canonical modules and base versions remain pinned in go.mod/go.sum. Ordinary
`go mod vendor` retains upstream LICENSE files/notices and modules.txt. The short
preparation script checks both pins and applies the exact disclosed diffs with
zero fuzz. Do not edit modules.txt, rename modules, introduce replacement/fake
versions, change global caches, or commit generated vendor sources.

## One build/test path

Requires the pinned Go toolchain and standard GNU `patch`. Ubuntu CI provides
`patch`; the Docker build stage explicitly installs it (it is not included in
the selected Go builder image):

```sh
./scripts/prepare-go.sh
GOMAXPROCS=2 go test -mod=vendor -p 1 ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -p 1 -o cnpg-to-kafka ./cmd/collector
```

On this shared VM, run preparation/tests/builds under `/tmp/cnpg15-heavy.lock`.
Preparation regenerates vendor sources each time; do not build with module mode
or use unpatched upstream source afterward. Docker/Actions must use this same
path. Native patch rejection tests exercise both unsupported settings; normal
TLS, registry, Kafka, filesystem replay and end-to-end tests remain required.

A clean scanner exit alone is insufficient. Before completion, inspect the
Linux/amd64 runtime import graph and actual binary build information: **no
external `golang.org/x/crypto` package/module may be linked**. GOROOT's internal
`vendor/golang.org/x/crypto/...` packages are part of the Go standard library
(`Standard=true`, no external Module), remain needed for native TLS, and are
scanned honestly with the Go toolchain version. The unchanged final-image Trivy JSON
`--list-all-pkgs` inventory must still contain the original configtls v1.68.0 and
internal/kafka v0.162.0 identities and versions. Their canonical base versions
remain conservatively scanned despite these disclosed changes. Retire these
patches when a tested upstream release removes the dependency without waivers.
