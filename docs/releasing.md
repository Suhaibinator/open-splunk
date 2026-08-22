# Development builds and v0 release publication

Local source builds are deliberately versionless and use the `development`
source identity. Official releases use canonical `v0.MINOR.PATCH` product
versions alongside their immutable source revision.

Major version zero does not establish a persisted-state upgrade contract.
Databases, state directories, backups, cursors, collector state, and retained
artifacts remain fresh-state-only across product versions. Private entity
revisions, migration numbers, and format counters are implementation mechanics
and are not product versions.

## Reproducible development artifacts

Development work may use the artifact launchers to prove that a clean commit
produces internally consistent server and collector outputs:

```sh
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" \
make release
```

The launcher requires a clean committed revision, materializes committed files
and modes into a disposable tree, omits ignored/untracked worktree content,
uses pinned tools and fresh caches, scrubs ambient workspace/build controls,
forces backend UI mode, and verifies embedded assets and linked binary build
identity before atomic publication under `build/`.

`make oci` applies the same source-revision discipline to local server and
collector images. These local targets verify reproducibility and never
authenticate to or push to a registry.

## Official publication

Publishing a non-draft, non-prerelease GitHub Release with a canonical
`v0.MINOR.PATCH` tag triggers the complete CI gate. The tag must resolve to a
commit reachable from `main`. Only that CI job receives registry and GitHub
Release write permissions.

After every required test succeeds, CI publishes exact multi-architecture
server and collector images to GHCR, verifies both, then advances their mutable
`latest` tags. It also attaches reproducible Linux AMD64 and ARM64 archives and
one SHA-256 checksum manifest to the GitHub Release. Release binaries and OCI
labels contain both the product version and complete source revision.

## Validation

A candidate development artifact should pass, for the same source revision:

- `make docs-check`, protobuf lint/generation, and deterministic regeneration;
- `make test`, frontend lint/typecheck/tests, and `make build`;
- required ClickHouse and shipped-browser verticals; and
- the HEC, recovery, load/soak, and OCI gates appropriate to the change.

Passing those checks proves only the tested source revision. It does not create
a public compatibility or support commitment.

## Work required before v1

Before publishing a stable v1 release, the project must choose and implement:

- protobuf/HTTP/gRPC and authored-SPL evolution policy;
- database, backup, cursor, collector-state, and retained-artifact migration
  boundaries;
- deprecation, support-window, rollback, and security-response policy;
- signing/attestation policy, release notes, and operator upgrade guidance; and
- CI gates that enforce every declared promise.

Until that work is complete, use [the roadmap](roadmap.md) as the source of
truth and do not infer a v1 compatibility promise from a v0 release.
