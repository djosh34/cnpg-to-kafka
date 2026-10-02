# Releases

The native [`release.yml`](../.github/workflows/release.yml) DAG validates PRs and
pushes to protected `main`. Required jobs are `replay` and `build-scan`; the
main-only publisher needs both to succeed. It uses `GITHUB_TOKEN`, downloads the
successful build's artifact ID, and never rebuilds or weakens the all-severity,
including-unfixed Trivy gate. Tag pushes and manual dispatch do not publish.

Every successful main publication tags that exact scanned image as `sha-<SHA>`,
`main`, and `latest`. `VERSION` initially contains `v0.1.0`; change it in a later
PR for another release. If that Git tag does not exist, the publisher also pushes
the matching version image and uses native `gh release create --target <SHA>` to
create its tag and release. An existing Git tag skips that branch, leaving its
version tag/image alone while still updating main/latest/SHA. Git inspection
errors fail rather than being treated as a missing tag.

One non-canceling publisher concurrency group and an initial remote-main check
skip queued stale work. Once a fresh publisher starts, it finishes; this is
ordinary serialized automation, not a transaction or an absolute registry IAM
guarantee. Normal publication failures fail the job and are investigated with
narrow fixes, not speculative credential or recovery frameworks.
