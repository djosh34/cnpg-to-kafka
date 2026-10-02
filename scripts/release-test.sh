#!/usr/bin/env bash
# Five ordinary publisher cases; no real git/registry/release writes.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"
export GITHUB_REPOSITORY=djosh34/cnpg-to-kafka GITHUB_SHA=test-sha
export LOG="$tmp/commands" TAG_STATUS=2 HEAD=test-sha

git() {
  if [[ "$2" == --exit-code ]]; then return "$TAG_STATUS"; fi
  printf '%s\trefs/heads/main\n' "$HEAD"
}
docker() { printf 'docker %s\n' "$*" >> "$LOG"; }
gh() { printf 'gh %s\n' "$*" >> "$LOG"; }
export -f git docker gh

for scenario in first-release existing-version stale tag-inspection-error invalid-version; do
  export TAG_STATUS=2 HEAD=test-sha
  printf 'v0.1.0\n' > VERSION
  : > "$LOG"
  case "$scenario" in
    existing-version) export TAG_STATUS=0 ;;
    stale) export HEAD=newer-sha ;;
    tag-inspection-error) export TAG_STATUS=128 ;;
    invalid-version) printf 'v01.2.3\n' > VERSION ;;
  esac
  status=0
  bash "$root/scripts/publish-release.sh" > "$tmp/output" 2>&1 || status=$?
  case "$scenario" in
    first-release|existing-version)
      [[ "$status" == 0 ]]
      for tag in sha-test-sha main latest; do
        grep -Fxq "docker tag cnpg-to-kafka:scan ghcr.io/djosh34/cnpg-to-kafka:$tag" "$LOG"
        grep -Fxq "docker push ghcr.io/djosh34/cnpg-to-kafka:$tag" "$LOG"
      done
      if [[ "$scenario" == first-release ]]; then
        grep -Fxq 'docker push ghcr.io/djosh34/cnpg-to-kafka:v0.1.0' "$LOG"
        grep -Fq 'gh release create v0.1.0 --repo djosh34/cnpg-to-kafka --target test-sha' "$LOG"
      else
        if grep -Eq ':v0.1.0|gh release' "$LOG"; then exit 1; fi
      fi
      ;;
    stale) [[ "$status" == 0 ]]; test ! -s "$LOG" ;;
    *) [[ "$status" != 0 ]]; test ! -s "$LOG" ;;
  esac
  printf 'PASS %s\n' "$scenario"
done
printf '5 publisher cases passed; no real publication.\n'
