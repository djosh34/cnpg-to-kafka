#!/usr/bin/env bash
# Exercise the real policy scripts with native-tool responses, without writes.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"
export AUDIT="$tmp/writes" MAIN_COUNT="$tmp/main-count"
export GITHUB_REPOSITORY=djosh34/cnpg-to-kafka GITHUB_REPOSITORY_OWNER=djosh34
export GITHUB_SHA=1111111111111111111111111111111111111111
export BEFORE=2222222222222222222222222222222222222222
export GITHUB_SERVER_URL=https://github.com GITHUB_RUN_ID=123
export CONFIG=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
other=3333333333333333333333333333333333333333
export OTHER_SHA=$other

# Only these fixtures replace tools. The production scripts themselves run.
gh() {
  if [[ "$1" == release ]]; then
    printf 'gh %s\n' "$*" >> "$AUDIT"
    [[ " $* " == *' --verify-tag '* && " $* " == *" --target $GITHUB_SHA "* ]]
    return
  fi
  case "$3" in
    repos/*/commits/*/pulls)
      jq -n --arg sha "$PR_SHA" --arg repo "$PR_REPO" --arg base "$PR_BASE" --argjson merged "$PR_MERGED" \
        '[{merged_at: (if $merged then "2026-10-02" else null end), base: {repo: {full_name: $repo}, ref: $base}, merge_commit_sha: $sha}]' | jq -r "$5"
      ;;
    users/*/packages/*/versions)
      if [[ "$API_ERROR" == 1 ]]; then return 2; fi
      jq -n --arg digest "$EXISTING_IMAGE" --arg version v0.1.0 \
        '[{name: $digest, metadata: {container: {tags: (if $digest == "" then [] else [$version] end)}}}]' | jq -r "$5"
      ;;
    repos/*/releases)
      jq -n --argjson exists "$RELEASE_EXISTS" \
        '(if $exists then [{id: 1, tag_name: "v0.1.0"}] else [] end)' | jq -r "$5"
      ;;
    *) echo "Unexpected gh request: $*" >&2; return 2 ;;
  esac
}
git() {
  case "$1" in
    diff)
      [[ "$*" == "diff --quiet $BEFORE $GITHUB_SHA -- VERSION" ]] || return 2
      return "$DIFF_STATUS"
      ;;
    ls-remote)
      if [[ "$2" == --tags ]]; then
        if [[ -n "$EXISTING_TAG" ]]; then printf '%s\trefs/tags/v0.1.0\n' "$EXISTING_TAG"; fi
      else
        if [[ "$MAIN_ERROR" == 1 ]]; then return 2; fi
        local count
        count=$(< "$MAIN_COUNT")
        count=$((count + 1))
        printf '%s\n' "$count" > "$MAIN_COUNT"
        if (( count > STALE_AFTER )); then printf '%s\n' "$OTHER_SHA"; else printf '%s\n' "$GITHUB_SHA"; fi
      fi
      ;;
    tag|push) printf 'git %s\n' "$*" >> "$AUDIT" ;;
    *) echo "Unexpected git operation: $*" >&2; return 2 ;;
  esac
}
docker() {
  case "$1" in
    buildx) printf '{"config":{"digest":"sha256:%s"}}\n' "$PUBLISHED_CONFIG" ;;
    pull) : ;;
    tag|push) printf 'docker %s\n' "$*" >> "$AUDIT" ;;
    *) echo "Unexpected Docker operation: $*" >&2; return 2 ;;
  esac
}
tar() { printf '[{"Config":"blobs/sha256/%s"}]\n' "$CONFIG"; }
export -f gh git docker tar

reset_case() {
  export GITHUB_EVENT_NAME=push GITHUB_REF=refs/heads/main
  export PR_SHA=$GITHUB_SHA PR_REPO=$GITHUB_REPOSITORY PR_BASE=main PR_MERGED=true
  export REPLAY_RESULT=success SCAN_RESULT=success MERGED_PR_RESULT=success
  export DIFF_STATUS=1 EXISTING_TAG='' EXISTING_IMAGE='' RELEASE_EXISTS=false
  export STALE_AFTER=99 MAIN_ERROR=0 API_ERROR=0 PUBLISHED_CONFIG=$CONFIG
  printf 'v0.1.0\n' > VERSION
  : > "$AUDIT"
  printf '0\n' > "$MAIN_COUNT"
}
run_case() {
  local script=$1 want=$2 status=0
  bash "$root/scripts/$script" > "$tmp/output" 2>&1 || status=$?
  if [[ "$want" == pass ]]; then [[ "$status" == 0 ]]; else [[ "$status" != 0 ]]; fi
}
no_writes() { test ! -s "$AUDIT"; }
contains() { grep -Fq -- "$1" "$AUDIT"; }
absent() { ! grep -Fq -- "$1" "$AUDIT"; }
count=0
passed() { count=$((count + 1)); printf 'PASS %s\n' "$1"; }

for case_name in valid manual tag direct-push wrong-repo wrong-base wrong-sha; do
  reset_case
  want=fail
  case "$case_name" in
    valid) want=pass ;;
    manual) export GITHUB_EVENT_NAME=workflow_dispatch ;;
    tag) export GITHUB_REF=refs/tags/v0.1.0 ;;
    direct-push) export PR_MERGED=false ;;
    wrong-repo) export PR_REPO=other/repo ;;
    wrong-base) export PR_BASE=other ;;
    wrong-sha) export PR_SHA=$other ;;
  esac
  run_case verify-merged-pr.sh "$want"
  no_writes
  passed "merge gate $case_name"
done
for case_name in replay-fail scan-fail scan-skipped merge-skipped manual tag; do
  reset_case
  case "$case_name" in
    replay-fail) export REPLAY_RESULT=failure ;;
    scan-fail) export SCAN_RESULT=failure ;;
    scan-skipped) export SCAN_RESULT=skipped ;;
    merge-skipped) export MERGED_PR_RESULT=skipped ;;
    manual) export GITHUB_EVENT_NAME=workflow_dispatch ;;
    tag) export GITHUB_REF=refs/tags/v0.1.0 ;;
  esac
  run_case publish-release.sh fail
  no_writes
  passed "publisher $case_name"
done
reset_case
export STALE_AFTER=0
run_case publish-release.sh pass
no_writes
passed 'queued stale run has no writes'
reset_case
export EXISTING_TAG=$other
run_case publish-release.sh fail
no_writes
passed 'explicit reused version conflicts before any write'
reset_case
export DIFF_STATUS=0 EXISTING_TAG=$other
run_case publish-release.sh pass
contains ':main'
contains ":sha-$GITHUB_SHA"
absent ':latest'
absent ':v0.1.0'
absent 'git tag'
absent 'gh release'
passed 'unchanged version publishes only SHA/main, without failure'
reset_case
run_case publish-release.sh pass
contains "git tag v0.1.0 $GITHUB_SHA"
contains 'git push origin refs/tags/v0.1.0:refs/tags/v0.1.0'
contains 'docker tag cnpg-to-kafka:scan ghcr.io/djosh34/cnpg-to-kafka:v0.1.0'
contains ':latest'
contains 'gh release create v0.1.0'
passed 'first release creates tag/release and uses exact scanned image'
for release_exists in false true; do
  reset_case
  export EXISTING_TAG=$GITHUB_SHA EXISTING_IMAGE=sha256:published RELEASE_EXISTS=$release_exists
  run_case publish-release.sh pass
  absent 'git tag'
  absent ':v0.1.0'
  contains 'docker tag ghcr.io/djosh34/cnpg-to-kafka@sha256:published ghcr.io/djosh34/cnpg-to-kafka:latest'
  if [[ "$release_exists" == true ]]; then absent 'gh release'; else contains 'gh release create'; fi
  passed "same-SHA original-artifact resume (release exists=$release_exists)"
done
reset_case
export EXISTING_TAG=$GITHUB_SHA EXISTING_IMAGE=sha256:published PUBLISHED_CONFIG=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
run_case publish-release.sh fail
no_writes
passed 'rerun-all changed image bytes conflicts before any write'
reset_case
export STALE_AFTER=1
run_case publish-release.sh pass
contains ":sha-$GITHUB_SHA"
absent 'docker push ghcr.io/djosh34/cnpg-to-kafka:main'
absent ':latest'
passed 'stale after initial check cannot push main/latest'
reset_case
export STALE_AFTER=3
run_case publish-release.sh pass
absent 'docker push ghcr.io/djosh34/cnpg-to-kafka:latest'
passed 'stale immediately before latest cannot move it'
for case_name in registry-error diff-error main-error; do
  reset_case
  case "$case_name" in
    registry-error) export API_ERROR=1 ;;
    diff-error) export DIFF_STATUS=2 ;;
    main-error) export MAIN_ERROR=1 ;;
  esac
  run_case publish-release.sh fail
  no_writes
  passed "fail closed $case_name"
done
for version in v01.2.3 v1.02.3 v1.2.03 v1.2 v1.2.3-rc.1 v1.2.3+build; do
  reset_case
  printf '%s\n' "$version" > VERSION
  run_case publish-release.sh fail
  no_writes
  passed "noncanonical VERSION $version"
done

# Real Git range: rebase merge ends after a separate VERSION-bump commit.
command git init -q rebase
command git -C rebase config user.name test
command git -C rebase config user.email test@example.invalid
printf 'v0.0.0\n' > rebase/VERSION
command git -C rebase add VERSION
command git -C rebase -c commit.gpgsign=false commit -qm base
before=$(command git -C rebase rev-parse HEAD)
printf 'v0.1.0\n' > rebase/VERSION
command git -C rebase -c commit.gpgsign=false commit -qam bump
command git -C rebase -c commit.gpgsign=false commit --allow-empty -qm later
if command git -C rebase diff --quiet "$before" HEAD -- VERSION; then exit 1; fi
command git -C rebase diff --quiet HEAD^ HEAD -- VERSION
passed 'multi-commit rebase needs push.before, not HEAD^'

grep -Fxq '    needs: [replay, build-scan, merged-pr]' "$root/.github/workflows/release.yml"
grep -Fxq '      cancel-in-progress: false' "$root/.github/workflows/release.yml"
grep -Fq "artifact-ids: \${{ needs.build-scan.outputs.image-artifact-id }}" "$root/.github/workflows/release.yml"
if awk '/^permissions:/ { exit } { print }' "$root/.github/workflows/release.yml" |
  grep -Eq 'workflow_dispatch:|^[[:space:]]+tags:'; then exit 1; fi
test ! -e "$root/.github/workflows/image.yml"
test ! -e "$root/.github/workflows/test.yml"
passed 'one native DAG, both gates, serial publisher, exact artifact-id, no manual/tag trigger'
printf '%s policy cases passed; no real tag, registry push, or release was created.\n' "$count"
