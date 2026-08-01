# Local production-shaped deployment

The checked-in Compose deployment runs one Open Splunk server and one pinned
ClickHouse server. Two hardened one-shot containers use that same exact Open
Splunk image to provision the administrator token and apply the image's
embedded ClickHouse schema before the server starts. The image also contains
the browser UI, HTTP API, WebSocket service, collector gRPC endpoint, and both
migration sets. ClickHouse data, SQLite/control-plane state, export artifacts,
and remote collector state remain external.

The default stack intentionally does not run a collector. Collectors belong on
log-producing hosts, and the GradeThis collector cutover is a separate
deployment unit.

## Build and start

Requirements:

- Docker with Compose v2;
- Git, OpenSSL, and Node.js (for the committed snapshot materializer);
- a clean, committed repository snapshot; and
- network access, or cached copies of the digest-pinned Node and Go builders
  and the digest-pinned ClickHouse image.

Build both local OCI images from the exact current commit:

```sh
cd ..
git status --short
export OPEN_SPLUNK_APPLICATION_VERSION=0.1.0
export OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)"
make oci
```

`git status --short` must be empty. `make oci` refuses a dirty worktree, a
short or mismatched revision, an invalid semantic version, unsafe image names,
and unsupported target platforms. The Make target executes the launcher and
snapshot materializer stored in the current commit, and Docker receives only
that materialized commit rather than working-tree files. It builds the server
and collector into temporary tags and publishes neither final local tag unless
both builds and the final repository-stability check succeed. Publication is
serialized per normalized destination reference by stopped, labeled lock
containers in the effective Docker daemon. The lock therefore coordinates
independent clones and users of one daemon while naturally isolating different
Docker endpoints. A crashed publisher leaves an explicitly named stale lock
that fails closed for operator inspection. If a signal, tag failure, or final
identity check interrupts the two-tag transaction, the launcher restores both
prior tags (or removes either newly introduced tag) before releasing its locks.
It never pushes an image.

The default target is `linux/amd64`. Set
`OPEN_SPLUNK_OCI_PLATFORM=linux/arm64` when the deployment host requires an
ARM64 image. Override `OPEN_SPLUNK_SERVER_IMAGE` and
`OPEN_SPLUNK_COLLECTOR_IMAGE` only with distinct, tagged image references.
Docker Hub aliases are normalized before distinctness checks and publication
locking.

Generate deployment credentials and two CA-signed server identities, then
start the stack:

```sh
cd deploy
OPEN_SPLUNK_APPLICATION_VERSION=0.1.0 ./generate-env.sh
docker compose up --detach --wait
```

The application version passed to `generate-env.sh` must match the version
used by `make oci`. The generator records the full committed source revision,
commit timestamp, and matching local server image tag in `.env`. The production
Compose file has no source-build fallback: it uses only the prebuilt local
server image and fails if that image is absent.

The generator refuses to replace either `.env` or `.env.tls`. Both are ignored
by Git. `.env` is mode `0600`; `.env.tls` is mode `0700`. The source files that
fixed container UIDs must read through direct bind mounts are mode `0644`, but
their host parent remains owner-only. The generator atomically publishes the
environment only after every referenced file is complete, and removes the
one-use CA signing key and signing intermediates. On macOS it also removes
inherited ACLs from the generated environment and secret directory before
writing secret material.

## Endpoints and readiness

The only host-published ports are loopback application ports:

- browser/API: `https://localhost:8080`;
- collector gRPC with TLS: `127.0.0.1:4317`.

Set `OPEN_SPLUNK_SERVER_HTTP_PORT` or `OPEN_SPLUNK_SERVER_GRPC_PORT` in the
shell or `.env` to select different host ports. The bind address remains
`127.0.0.1`.

The generated CA is private, so a browser will not trust it automatically.
For a strict command-line health probe:

```sh
set -a
. ./.env
set +a
curl --fail --cacert "$OPEN_SPLUNK_SERVER_TLS_CA_FILE" \
  https://localhost:${OPEN_SPLUNK_SERVER_HTTP_PORT:-8080}/healthz
```

The expected liveness response is exactly `ok`. `/healthz` intentionally
reports only that the HTTP process is alive. Compose instead probes `/readyz`,
which performs a one-second, read-only ping through the already validated
runtime ClickHouse session. The server binary's health command uses normal
CA-chain and hostname validation, TLS 1.2 or newer, no proxy or redirects,
status `200`, and body `ok\n`. ClickHouse's own readiness executes an
authenticated `SELECT 1` over verified native TLS on port 9440. A stopped or
unreachable ClickHouse dependency, plaintext listener, or malformed mounted
identity cannot leave the production server healthy.

`docker compose ps --all` should show `server` and `clickhouse` healthy and
both `server-bootstrap` and `clickhouse-migrator` exited with code zero.

## Administrator credential

`generate-env.sh` creates a high-entropy browser administrator token. A
network-disabled, one-shot `server-bootstrap` container copies it into the
persistent `server-state` volume's image-seeded `private` directory as an
EUID-owned, mode-`0600`, single-link file. The directory itself is owned by
UID/GID `65532:65532` with mode `0700`. Publication is atomic and
no-overwrite; repeating bootstrap with the same token is idempotent, while
different or unsafe existing material fails closed.

The host-retained source path is
`OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE` in `.env`. Do not print it, place it in
a URL, commit it, or pass it as a process argument. Administrative protobuf
API requests require `Authorization: Bearer <token>`.

The current browser does not yet provide an administrator-token entry flow.
The deployed UI and unauthenticated system bootstrap are available, but fresh
index/app/token administration must currently use an API client that supplies
the bearer header. The upcoming GradeThis onboarding unit must close that UI
workflow before describing a fresh stack as administratively self-service.

## Container and secret boundaries

Both Open Splunk images are scratch runtimes with only fixed passwd/group
entries, one mode-`0555` binary, and their private state directories. They run
as numeric UID/GID `65532:65532`. The Compose server and bootstrap use a
read-only root filesystem, drop all capabilities, set
`no-new-privileges`, and mount only their required writable volumes or
read-only inputs. Bootstrap has no network namespace. The server image seeds
owner-only `private` directory entries below the state and export mountpoints.
Docker copies those entries into a newly initialized named volume, so the
fixed non-root identity writes below the volume roots without requiring the
roots themselves to be writable or changing ownership as root.

ClickHouse is pinned to:

```text
clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49
```

It runs as numeric UID/GID `101:101` with a read-only root filesystem, all
capabilities dropped, `no-new-privileges`, private data/log volumes, and a
UID-owned temporary filesystem.

ClickHouse receives four generated password values because its official
initialization path must provision and rotate the principals. That hook does
not receive source files or apply the application schema. The one-shot
`clickhouse-migrator` uses the exact same prebuilt image as the server, mounts
only the ClickHouse CA and migration-password file, validates the pinned server
version and exact migrator grant allowlist, applies `migrations.ClickHouse()`
from the binary, validates the exact release-owned physical schema, clears its
credential, and exits. It requires six consecutive fresh authenticated TLS
probes before DDL, so the official image's temporary initialization server
cannot create a false-ready race with the final server.

The long-running Open Splunk server does not receive any ClickHouse password
environment variable. It reads only the runtime and deletion credentials from
separate bounded, no-follow, stable, read-only files and starts with embedded
migrations disabled because successful completion of the one-shot migrator is
a dependency. Neither the bootstrap nor migration credential is mounted into
or passed to the long-running server. Shell ClickHouse clients consume
passwords through their environment rather than command-line arguments.

The production Compose file publishes no ClickHouse port. Its plaintext native
port 9000 remains required by the official image's initialization path and is
reachable only on the private backend network; the application is
fixed to verified TLS at `clickhouse:9440`. The
`docker-compose.integration.yaml` override publishes only 9440 and exists
solely for automated tests.

## ClickHouse principals and migrations

Initialization creates four distinct identities:

- `open_splunk_bootstrap` is the deployment administrator. Its network
  allowlist is loopback-only, so recovery runs with `docker compose exec
  clickhouse ...`; the application never receives this credential.
- `open_splunk_migrator` can create only the embedded schema and migration
  ledger, and can inspect only the metadata needed to prove their exact
  definitions. It is used only by the exact-release one-shot migrator.
- `open_splunk_runtime` can insert/select events and read the narrowly required
  index-statistics metadata.
- `open_splunk_deletion` can submit and reconcile physical data deletion. Its
  event reads are restricted to the logical index identity columns.

ClickHouse 26.3 authorizes several partition operations through the same
`ALTER DELETE` privilege. That unavoidable blast radius is why the deletion
connection is private to the fixed deletion Store path and is never shared
with ingestion, search, export, or inspection.

`clickhouse-init.sh` rejects every server version other than `26.3.17.4` and
provisions or rotates all three service principals and their exact grants. The
grants name their future database/table targets, so they can be installed
before the schema exists. The hook runs on every ClickHouse container start;
it never reads migration SQL. The image's passwordless base `default` user is
removed.

After ClickHouse becomes healthy, `clickhouse-migrator` connects through
verified native TLS and applies only the migrations embedded in its image.
The runner accepts an existing ledger only when it is an exact prefix of that
release, applies a pending suffix idempotently, records each ledger row last,
and verifies the complete ledger afterward. Before reporting success it also
requires the `open_splunk` table set to contain exactly `events` and
`schema_migrations`, and compares both complete canonical
`system.tables.create_table_query` definitions with the definitions produced by
this pinned ClickHouse release. Missing or extra objects and any change to a
column, type, default, codec, index, constraint, engine, key, TTL, or table
setting fail even when the ledger is complete. Renamed, duplicate, gapped, or
otherwise drifted history fails before DDL. An older image rejects a database
migrated by a newer image; there is no automatic down-migration or schema
rollback. The long-running server starts only after a zero exit and then
validates the exact runtime and deletion grant allowlists before opening normal
services. It has no migrator session or credential.

For an upgrade, build a new immutable server tag from the exact clean commit,
change `OPEN_SPLUNK_SERVER_IMAGE` in `.env` to that tag, and run:

```sh
docker compose up --detach --wait --no-build
```

The image change recreates the one-shot migrator before Compose replaces the
long-running server. Keep the previous image and volumes until the new stack is
healthy, but do not point an older release at a schema the newer release has
advanced: its migrator will fail closed. A same-tag local image replacement is
not an upgrade signal; use immutable tags, or deliberately add
`--force-recreate` after verifying the tag contents.

Credential rotation is coordinated. Atomically replace the migration, runtime,
and deletion password files, update all four distinct 64-character hexadecimal
password values in `.env`, then force-recreate `clickhouse`,
`clickhouse-migrator`, and `server` in one operation:

```sh
docker compose up --detach --wait --no-build --force-recreate \
  clickhouse clickhouse-migrator server
```

ClickHouse reapplies the credentials,
the one-shot migrator proves the rotated migrator identity and current ledger,
and the server cannot become healthy until its rotated runtime/deletion files
work. Do not rotate only one side of this boundary.

## State, restart, and cleanup

The stack owns four named volumes:

- `clickhouse-data`;
- `clickhouse-logs`;
- `server-state`, whose owner-only `private` child holds the administrator
  token, master key, SQLite database, WAL, and singleton lock;
- `server-exports`, whose owner-only `private` child holds bounded export
  artifacts.

`docker compose up --detach --wait` is safe to repeat. An unchanged successful
one-shot migrator remains completed; changing the release image reruns it.
Recreating the server retains its token, master key, and SQLite state.
Recreating ClickHouse retains events and reruns the idempotent principal
provisioning contract; use the coordinated rotation operation above when its
credential values change.

Stop and remove containers while retaining data:

```sh
docker compose down
```

`docker compose down --volumes` permanently deletes the deployment's
ClickHouse and Open Splunk state and is appropriate only for a disposable
stack.

Hot-copying the SQLite files is not a supported backup. Until the planned
backup/restore command exists, stop the server and retain the entire
`server-state` volume as one unit so the database and master key cannot be
separated. Back up ClickHouse through ClickHouse-supported tooling rather than
copying a live data directory.

## Collector image

`make oci` also creates `open-splunk-collector:<version>`. It is a separate
non-root scratch image because collectors run beside log sources. The image
defaults to:

```text
open-splunk-collector run -config /etc/open-splunk/collector.yaml
```

Mount a validated configuration, private writable state directory, trusted
server CA, and collector token on the log-producing host. Do not give a
collector ClickHouse credentials. The default Compose file deliberately has
no collector service; the GradeThis no-OpenTelemetry cutover will provide and
test the first concrete deployment configuration.
