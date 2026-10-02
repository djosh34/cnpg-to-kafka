# Releases

[`release.yml`](../.github/workflows/release.yml) is the single validation and
publication DAG. PRs and pushes to `main` run `replay` and `build-scan` at the
frozen event SHA. PR jobs have read-only repository permissions and no publishing
credentials. Tag pushes and manual dispatch do not trigger publication. The
separate deliberate capture workflow does not publish images or releases.

Only an actual merged PR into this repository's `main`, whose merge commit is
that same event SHA, can reach the publisher. Both replay and the all-severity,
including-unfixed, fail-closed Trivy scan must pass. The publisher downloads the
image by the successful build job's artifact ID, loads it, and never rebuilds.
Artifact names include the run attempt; a publisher-only retry reuses the
successful build's exact artifact, not a same-name artifact from another attempt.
An expired or missing artifact fails rather than rebuilding or selecting another.

`VERSION` contains one canonical stable tag, initially `v0.1.0`. Change it in a
reviewed PR to release another version; no prerelease/build metadata or leading
zeros are accepted. Intent is detected across the whole push's `before..sha`
range, including multi-commit rebase merges. An ordinary merge leaving VERSION
unchanged may update the SHA and main image tags but leaves the Git release tag,
version image, GitHub release and `latest` unchanged. Reusing an existing version
for a different commit on an explicit version change fails before any writes.

A release creates its Git tag at the verified SHA using the release environment's
`RELEASE_TAG_SSH_KEY`, publishes the scanned version image, creates the GitHub
release with `--verify-tag`, and updates `latest` as that release's companion.
Protected tag creation is restricted separately from update/deletion; existing
release tags are never forced, moved or deleted. Write credentials are confined
to the gated `release` environment publisher, not PR or repository-wide jobs.

Publishers share one non-canceling concurrency group. They check remote main
once inside that lock, before any writes; queued stale runs skip. Once fresh
publication starts, it completes even if main advances, rather than abandoning
an immutable tag without its image/release/latest. The global lock prevents a
newer publisher finishing before the active one; this is bounded serialization,
not a transaction or an instantaneous guarantee that aliases follow Git HEAD.
Normal tool/network failures still fail the job, not a successful partial skip.
Retry only a failed publisher
while its SHA is still current. A matching existing Git tag resumes, but an
existing version image is never overwritten: its native registry config digest
must match the exact saved scan artifact or publication fails before any writes.
Matching published bytes are reused. Thus rerunning all jobs with a newly rebuilt
image is not permission to overwrite an already-published immutable version.
