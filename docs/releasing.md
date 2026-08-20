# Releasing

Release preparation may use an ordinary development branch, but publication is
always cut from `main` by pushing a `vX.Y.Z` tag. There is no long-lived
publication branch and no manual publication step.

## Procedure

1. Merge the release content to `main`.
2. Wait for the `CI` workflow to finish green on that exact merge commit.
3. Tag that commit and push the tag:

   ```sh
   git fetch origin main
   git tag v0.4.0 <merge-commit>
   git push origin v0.4.0
   ```

   The tag must match `vMAJOR.MINOR.PATCH`; prerelease and build-metadata
   suffixes are rejected.

## What the publication workflow verifies

`.github/workflows/publish-images.yml` runs on `push` of a `v*` tag and gates
publication on three conditions:

- The tag matches `^v[0-9]+\.[0-9]+\.[0-9]+$`, and the checked-out revision is
  the tagged commit.
- The tagged commit is an ancestor of `origin/main`, so a tag on an unmerged
  branch cannot publish.
- At least one completed `CI` workflow run for that exact commit concluded
  `success`.

The application version is the tag without its leading `v`. The current SPL
compatibility identity comes from
`scripts/read-spl-compatibility-version.mjs`; it is compiled into the server
and verified before publication. There is no caller-selectable compatibility
profile. Image creation time and
`SOURCE_DATE_EPOCH` are taken from the tagged commit, so the build is
reproducible from the tag alone.

Application `0.4.0` uses cumulative compatibility profile `0.4`, as summarized
in [`versioning.md`](versioning.md).

## What it publishes

For the tagged commit, the workflow builds and pushes both consumable images
for `linux/amd64` and `linux/arm64`:

- `ghcr.io/<owner>/open-splunk-server:<version>`
- `ghcr.io/<owner>/open-splunk-collector:<version>`

Each architecture is pushed by digest, then joined into one multi-architecture
manifest per image. The workflow inspects the published manifest and fails if
both platforms are not present. No `latest` tag is published; every release is
addressable only by its exact version.

If the gate fails, fix the underlying problem on `main`, let CI go green again,
and tag the new commit with a new version. Do not move or force-push a release
tag.
