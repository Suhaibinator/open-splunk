# Development builds and publication status

Open Splunk has not declared its first official release. There is no supported
product version, release tag, upgrade path, backward-compatibility window, or
published artifact contract yet. Source revision is the only authoritative
development build identity.

Do not present current binaries, images, protobufs, routes, databases, state
directories, backups, cursors, or retained artifacts as stable across source
revisions. Private entity revisions, migration numbers, and format counters are
implementation mechanics and are not product versions.

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
collector images. Both images must come from the same commit and default to the
full immutable source revision as their tag; do not push them under a semantic
release tag or a floating `latest` tag.

## Validation

A candidate development artifact should pass, for the same source revision:

- `make docs-check`, protobuf lint/generation, and deterministic regeneration;
- `make test`, frontend lint/typecheck/tests, and `make build`;
- required ClickHouse and shipped-browser verticals; and
- the HEC, recovery, load/soak, and OCI gates appropriate to the change.

Passing those checks proves only the tested source revision. It does not create
a public compatibility or support commitment.

## Work required before the first release

Before publishing an official release, the project must choose and implement:

- product version and tag syntax plus artifact/image naming;
- protobuf/HTTP/gRPC and authored-SPL evolution policy;
- database, backup, cursor, collector-state, and retained-artifact migration
  boundaries;
- deprecation, support-window, rollback, and security-response policy;
- publication credentials, provenance/signing, release notes, and operator
  upgrade guidance; and
- CI gates that enforce every declared promise.

Until that work is complete, use [the roadmap](roadmap.md) as the source of
truth and treat current outputs as development artifacts only.
