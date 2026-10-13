# Development builds and v0 release publication

Ordinary local source builds are deliberately versionless and use the
`development` source identity. Reproducible local artifact and OCI checks also
remain product-versionless but use the complete clean source revision.
Official releases use canonical `v0.MINOR.PATCH` product versions alongside
their immutable source revision.

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
collector images. Invoke it with the exact current revision as well:

```sh
OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)" \
make oci
```

It defaults to distinct local server/collector image names tagged with that
revision on `linux/amd64`; explicit image names and `linux/arm64` are supported
through the documented environment controls. These local targets verify
reproducibility and never authenticate to or push to a registry.

## Official publication

Publishing a non-draft, non-prerelease GitHub Release with a canonical
`v0.MINOR.PATCH` tag triggers the complete CI gate. The tag must resolve to a
commit reachable from `main`. Only that CI job receives registry and GitHub
Release write permissions.

A successful publication produces exact multi-architecture server and
collector images on GHCR, verifies both, then advances their mutable `latest`
tags. It also attaches reproducible Linux AMD64 and ARM64 archives, each
containing the server, collector, and log generator, plus one SHA-256 checksum
manifest to the GitHub Release. Release binaries and OCI labels contain both
the product version and complete source revision.

The public image repositories are:

- [`ghcr.io/suhaibinator/open-splunk-server`](https://github.com/Suhaibinator/open-splunk/pkgs/container/open-splunk-server);
- [`ghcr.io/suhaibinator/open-splunk-collector`](https://github.com/Suhaibinator/open-splunk/pkgs/container/open-splunk-collector).

For a GitHub tag such as `v0.MINOR.PATCH`, both OCI tags omit the leading `v`:

```text
ghcr.io/suhaibinator/open-splunk-server:0.MINOR.PATCH
ghcr.io/suhaibinator/open-splunk-collector:0.MINOR.PATCH
```

Use the same exact version for both images. The `latest` tags are evaluation
conveniences rather than deployment pins. The upstream packages are public and
support anonymous pulls. Select a version present in both package repositories;
a GitHub Release object by itself is not evidence that a publication run
completed. Release archives and their checksum manifest are available from the
[GitHub Releases page](https://github.com/Suhaibinator/open-splunk/releases).

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


## Pinned shared dependencies

The release snapshot includes `third_party/go-common` and its per-file source
provenance, plus the compiled GoCommon core frontend archive and lockfile
integrity. Builds resolve the local Go module replacement from the committed
snapshot; they do not require the unpushed integration changes to exist in an
upstream release. Verify the Go snapshot with `node scripts/check-go-common.mjs`.
Deliberate updates use `scripts/sync-go-common.mjs` and the frontend repacking
script. Builder Go is pinned to 1.27.2 in the module, Makefile, CI and immutable
Docker image reference. Scratch runtimes include CA trust for Google HTTPS.

The pinned server image includes PostgreSQL 18 backup clients and their full
dynamic library dependencies for each target architecture. OCI builds validate
the tools in the normalized server filesystem; the native release job checks
the published image commands before running its production recovery drill.
Cross-architecture builds require registered binfmt/QEMU support because the
PostgreSQL client stage executes for the target architecture.

Every Linux CI job that executes native Go tests requires the repository Actions variable
`OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE` to contain an approved exact MinIO
server image, preferably a digest reference. `scripts/start-ci-native-services.sh`
checks this requirement before pulling images, starts disposable loopback-only
PostgreSQL 18 and MinIO services, and exports their isolated test configuration
through `GITHUB_ENV`. It extracts `pg_dump`, `pg_restore`, and their full musl
library closure from the pinned PostgreSQL 18 image. Thin host wrappers execute
that loader directly, preserving inherited pinned archive descriptors and
private fixture permissions. Backup client and server majors therefore match
without depending on runner apt.
The native release recovery drill creates its own isolated
private bucket and PostgreSQL/ClickHouse volumes; it never uses production
accounts or storage. Local operators supply the same environment variable (or
`OPEN_SPLUNK_DEPLOY_MINIO_IMAGE`). An unset image fails qualification explicitly.
A pinned source-built MinIO fixture image may be approved for this isolated test;
that choice does not change the production Compose image requirement.
