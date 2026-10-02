#!/usr/bin/env bash
# Called only by the serialized, environment-protected publisher; never rebuild.
set -euo pipefail
[[ "$GITHUB_EVENT_NAME" == push && "$GITHUB_REF" == refs/heads/main ]]
[[ "$REPLAY_RESULT" == success && "$SCAN_RESULT" == success && "$MERGED_PR_RESULT" == success ]]
version=$(< VERSION)
[[ "$version" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]
image="ghcr.io/${GITHUB_REPOSITORY,,}"

# The whole push range includes every commit of a rebase-merged PR.
release=false
if git diff --quiet "$BEFORE" "$GITHUB_SHA" -- VERSION; then
  echo 'VERSION unchanged: leave the release tag, version image and latest alone.'
else
  status=$?
  [[ "$status" == 1 ]]
  release=true
fi

existing_tag=''
existing_image=''
if [[ "$release" == true ]]; then
  tag_ref="refs/tags/$version"
  existing_tag=$(git ls-remote --tags origin "$tag_ref" "$tag_ref^{}" |
    awk '$2 ~ /\^\{\}$/ { peeled=$1 } $2 !~ /\^\{\}$/ { direct=$1 } END { print peeled ? peeled : direct }')
  if [[ -n "$existing_tag" && "$existing_tag" != "$GITHUB_SHA" ]]; then
    echo "Refusing reused release $version: its Git tag belongs to another commit." >&2
    exit 1
  fi
  # This repository's GHCR owner is a user. Query native package versions; API
  # failure is not absence. Never overwrite an already-published version image.
  existing_image=$(gh api --paginate "users/$GITHUB_REPOSITORY_OWNER/packages/container/${GITHUB_REPOSITORY#*/}/versions" --jq \
    ".[] | select(.metadata.container.tags | index(\"$version\")) | .name")
  if [[ -n "$existing_image" ]]; then
    scanned_config=$(tar -xOf image.tar manifest.json | jq -er '.[0].Config | split("/") | last | rtrimstr(".json")')
    published_config=$(docker buildx imagetools inspect --raw "$image@$existing_image" | jq -er '.config.digest')
    if [[ "$published_config" != "sha256:$scanned_config" ]]; then
      echo "Refusing rebuilt bytes for existing release $version; retry only the failed publisher with its original artifact." >&2
      exit 1
    fi
    docker pull --platform linux/amd64 "$image@$existing_image"
  fi
fi

# One freshness check under the non-canceling global lock, before any writes.
# Once started, finish: a newer publisher cannot complete before this one.
head=$(git ls-remote origin refs/heads/main | awk '{print $1}')
if [[ "$head" != "$GITHUB_SHA" ]]; then
  echo 'Stale main run: skip publication without moving aliases.' >&2
  exit 0
fi

if [[ "$release" == true && -z "$existing_tag" ]]; then
  git tag "$version" "$GITHUB_SHA"
  git push origin "refs/tags/$version:refs/tags/$version"
fi

docker tag cnpg-to-kafka:scan "$image:sha-$GITHUB_SHA"
docker push "$image:sha-$GITHUB_SHA"
docker tag cnpg-to-kafka:scan "$image:main"
docker push "$image:main"

if [[ "$release" == true ]]; then
  if [[ -z "$existing_image" ]]; then
    docker tag cnpg-to-kafka:scan "$image:$version"
    docker push "$image:$version"
  fi
  existing_release=$(gh api --paginate "repos/$GITHUB_REPOSITORY/releases" --jq \
    ".[] | select(.tag_name == \"$version\") | .id")
  if [[ -z "$existing_release" ]]; then
    gh release create "$version" --repo "$GITHUB_REPOSITORY" --verify-tag \
      --target "$GITHUB_SHA" --title "$version" --latest \
      --notes "Scanned image from $GITHUB_SHA: $GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID"
  fi
  source=cnpg-to-kafka:scan
  if [[ -n "$existing_image" ]]; then source="$image@$existing_image"; fi
  docker tag "$source" "$image:latest"
  docker push "$image:latest"
fi
