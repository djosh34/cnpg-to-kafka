#!/usr/bin/env bash
# Protected main and the native replay/build-scan needs are the publication gate.
set -euo pipefail
image="ghcr.io/${GITHUB_REPOSITORY,,}"
version=$(< VERSION)
[[ "$version" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]
# One check inside the non-canceling global lock; finish once fresh work starts.
head=$(git ls-remote origin refs/heads/main | awk '{print $1}')
if [[ "$head" != "$GITHUB_SHA" ]]; then
  echo 'Skip queued stale publisher.'
  exit 0
fi
release=false
if git ls-remote --exit-code --refs origin "refs/tags/$version"; then
  echo "$version already exists: leave its Git tag and version image alone."
else
  status=$?
  if [[ "$status" != 2 ]]; then exit "$status"; fi
  release=true
fi
for tag in "sha-$GITHUB_SHA" main latest; do
  docker tag cnpg-to-kafka:scan "$image:$tag"
  docker push "$image:$tag"
done
if [[ "$release" == true ]]; then
  docker tag cnpg-to-kafka:scan "$image:$version"
  docker push "$image:$version"
  gh release create "$version" --repo "$GITHUB_REPOSITORY" --target "$GITHUB_SHA" \
    --title "$version" --notes "Scanned image from $GITHUB_SHA."
fi
