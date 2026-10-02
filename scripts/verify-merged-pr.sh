#!/usr/bin/env bash
# Read-only gate: a real merged PR must account for this exact main push.
set -euo pipefail
[[ "$GITHUB_EVENT_NAME" == push && "$GITHUB_REF" == refs/heads/main ]]
gh api --paginate "repos/$GITHUB_REPOSITORY/commits/$GITHUB_SHA/pulls" --jq \
  'any(.[]; .merged_at != null and .base.repo.full_name == env.GITHUB_REPOSITORY and .base.ref == "main" and .merge_commit_sha == env.GITHUB_SHA)' |
  grep -Fx true
