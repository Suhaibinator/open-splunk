# Local production-shaped deployment

The checked-in Compose deployment runs one Open Splunk server and one pinned
ClickHouse server. Two hardened one-shot containers use that same exact Open
Splunk image to provision the administrator token and apply the image's
embedded ClickHouse schema before the server starts. The image also contains
the browser UI, HTTP API, WebSocket service, collector gRPC endpoint, and both
migration sets. ClickHouse data, SQLite/control-plane state, export artifacts,
and remote collector state remain external.

The optional HTTP Event Collector (HEC) compatibility surface is disabled by
default. When enabled, it shares the existing browser/API HTTPS listener and
does not publish a separate port 8088.

The default stack intentionally does not run a collector. Collectors belong on
log-producing hosts, and the GradeThis collector cutover is a separate
deployment unit.

## Release images

Pushing a `vX.Y.Z` semantic-version tag runs the single **Publish release
images** workflow. One successful run publishes the complete consumable image
set from the same tagged commit:

```text
ghcr.io/suhaibinator/open-splunk-server:X.Y.Z
ghcr.io/suhaibinator/open-splunk-collector:X.Y.Z
```

Both references are immutable versioned multi-platform manifests containing
Linux AMD64 and ARM64 images. The workflow does not publish a floating
`latest` tag. SemVer build metadata uses `_` in the OCI tag because `+` is not
valid there. Use the server image for the Compose stack and the collector image
on log-producing hosts; do not mix versions from different releases.

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
export OPEN_SPLUNK_EXPECTED_SPL_COMPATIBILITY_VERSION=0.2
export OPEN_SPLUNK_SOURCE_REVISION="$(git rev-parse HEAD)"
make oci
```

For the SPL v0.3 runtime release, use application `0.2.0` and expected SPL
compatibility `0.3`. The OCI build requires both identities explicitly and
rejects an unsupported or omitted expected SPL version.

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

## HTTP Event Collector feature gate

The server command receives
`-hec-enabled=${OPEN_SPLUNK_HEC_ENABLED:-false}`. To persistently enable the
complete JSON, raw, acknowledgment, and HEC health route set, add this setting
to the generated `.env`, then recreate the server:

```dotenv
OPEN_SPLUNK_HEC_ENABLED=true
```

```sh
docker compose up --detach --wait --no-build server
```

The HEC base URL is the existing browser/API origin, normally
`https://localhost:${OPEN_SPLUNK_SERVER_HTTP_PORT:-8080}`. It uses the same
generated certificate and CA. The host mapping remains loopback-only and no
plaintext listener is added. See the
[HEC deployment and operations runbook](../docs/hec-deployment.md) before
publishing the endpoint or onboarding a producer.

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

The browser provides an administrator-token entry flow at `/signin/`. It keeps
the bearer credential in the current browser tab for protected API calls and
can then open **Administration** for index and ingestion-token management.
When creating a HEC credential, choose the immutable **HTTP Event Collector
(HEC)** purpose, select allowed indexes and optional metadata defaults, and
decide at creation whether indexer acknowledgment is enabled. The HEC
plaintext token is shown once and must be moved directly into the producer's
secret store. The
[HEC runbook](../docs/hec-deployment.md#create-a-hec-token-in-administration)
contains the complete sequence.

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

ClickHouse receives six distinct generated password values because its official
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
a dependency. The bootstrap, migration, backup, and restore credentials are
never mounted into or passed to the long-running server. Shell ClickHouse
clients consume passwords through their environment rather than command-line
arguments.

The production Compose file publishes no ClickHouse port. Its plaintext native
port 9000 remains required by the official image's initialization path and is
reachable only on the private backend network; the application is
fixed to verified TLS at `clickhouse:9440`. The
`docker-compose.integration.yaml` override publishes only 9440 and exists
solely for automated tests.

## ClickHouse principals and migrations

Initialization creates six distinct identities:

- `open_splunk_bootstrap` is the deployment administrator. Its network
  allowlist is loopback-only, so direct administrative operations run with
  `docker compose exec clickhouse ...`; neither the application nor the
  recovery helpers receive this credential.
- `open_splunk_migrator` can create only the embedded schema and migration
  ledger, and can inspect only the metadata needed to prove their exact
  definitions. It is used only by the exact-release one-shot migrator.
- `open_splunk_runtime` can insert/select events and read the narrowly required
  index-statistics metadata.
- `open_splunk_deletion` can submit and reconcile physical data deletion. Its
  event reads are restricted to the logical index identity columns.
- `open_splunk_backup` can create a native backup of the release-owned
  `open_splunk` database and inspect only the schema, visibility, and mutation
  metadata needed to prove a quiescent source. Database-scoped `SHOW TABLES`
  ensures administrator-owned extras cannot be hidden from the exact physical
  schema check. Its only ordinary table
  mutation authority is `INSERT`, `SELECT`, and `TRUNCATE` on the singleton
  `recovery_archive_markers` table, used to bind one native archive and to
  synchronously clear an interrupted backup marker.
- `open_splunk_restore` can create only the canonical `open_splunk` database
  and its four exact release-owned tables, inspect only the metadata and
  bounded columns needed to validate the restored state, including scoped
  `SHOW TABLES` visibility for administrator-owned extras, write its singleton
  receipt, and consume its archive marker. Native `RESTORE` unavoidably
  requires `INSERT` on each exact destination table; the principal receives no
  raw event read, cannot truncate `events` or `schema_migrations`, and has no
  broad table target. Event reads expose only `visibility_seq`. This credential
  is used only by the one-shot recovery helper, is never mounted into the
  long-running server, and should be rotated after the restore operation.

ClickHouse 26.3 authorizes several partition operations through the same
`ALTER DELETE` privilege. That unavoidable blast radius is why the deletion
connection is private to the fixed deletion Store path and is never shared
with ingestion, search, export, or inspection.

`clickhouse-init.sh` rejects every server version other than `26.3.17.4` and
provisions or rotates all six principals. For every managed non-bootstrap
principal, it first removes all prior direct privileges and role assignments,
then reapplies the exact release allowlist. The grants name their future
database/table targets, so they can be installed before the schema exists. The
hook runs on every ClickHouse container start; it never reads migration SQL.
The image's passwordless base `default` user is removed.

After ClickHouse becomes healthy, `clickhouse-migrator` connects through
verified native TLS and applies only the migrations embedded in its image.
The runner accepts an existing ledger only when it is an exact prefix of that
release, applies a pending suffix idempotently, records each ledger row last,
and verifies the complete ledger afterward. Before reporting success it also
requires the `open_splunk` table set to contain exactly `events`,
`schema_migrations`, `recovery_archive_markers`, and `recovery_sets`, and
compares all four complete
canonical `system.tables.create_table_query` definitions with the definitions
produced by this pinned ClickHouse release. Missing or extra objects and any
change to a column, type, default, codec, index, constraint, engine, key, TTL,
or table setting fail even when the ledger is complete. Renamed, duplicate,
gapped, or otherwise drifted history fails before DDL. An older image rejects
a database migrated by a newer image; there is no automatic down-migration or
schema rollback. The long-running server starts only after a zero exit and
then validates the exact runtime and deletion grant allowlists before opening
normal services. It has no migrator session or credential.

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
deletion, backup, and restore password files, update all six distinct
64-character hexadecimal password values in `.env`, then force-recreate
`clickhouse`, `clickhouse-migrator`, and `server` in one operation:

```sh
docker compose up --detach --wait --no-build --force-recreate \
  clickhouse clickhouse-migrator server
```

ClickHouse reapplies the credentials,
the one-shot migrator proves the rotated migrator identity and current ledger,
and the server cannot become healthy until its rotated runtime/deletion files
work. Do not rotate only one side of this boundary.

## State, restart, and cleanup

The stack owns seven named volumes:

- `clickhouse-data`;
- `clickhouse-logs`;
- `clickhouse-recovery`, whose exact-ownership root holds ClickHouse-native
  deployment recovery archives;
- `server-state`, whose owner-only `private` child holds the administrator
  token, master key, SQLite database, WAL, and database-specific lock sidecar;
- `server-exports`, whose owner-only `private` child holds bounded export
  artifacts;
- `server-recovery`, whose owner-only `private` child holds canonical outer
  deployment recovery-set manifests and control-plane children; and
- `server-lock`, whose owner-only `private` child holds the empty deployment
  singleton lock shared by the long-running server and backup/restore helpers,
  including when `server-state` is rebound to a fresh restore volume.

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

## Coordinated deployment recovery

The supported recovery unit pairs the SQLite visibility/control plane with the
ClickHouse event database. The `recovery` Compose profile provides five
exact-release one-shot services:

- `deployment-backup` runs `backup-deployment-recovery-set` against a stopped
  server and a healthy, quiescent ClickHouse server;
- `deployment-marker-reconcile` clears only an explicitly repeated, exact stale
  source marker after the server and ClickHouse have been stopped and only
  ClickHouse has been restarted;
- `deployment-archive-delete` is a networkless UID-`101` archive-owner tool
  for explicit, operator-attested deletion after a failed backup;
- `deployment-verify` runs `verify-deployment-recovery-set` without a network,
  application-state mount, CA, or credential; and
- `deployment-restore` runs `restore-deployment-recovery-set` against fresh
  ClickHouse and server-state volumes. During this lifecycle the production
  `docker-compose.restore.yaml` overlay replaces ClickHouse's normal writable
  recovery mount with the same named volume mounted read-only.

Every recovery helper has a read-only root filesystem, drops all capabilities,
uses `no-new-privileges`, has no host namespace or device authority, and
disables the server image's inherited healthcheck. Backup, marker reconciliation,
and restore mount the same retained `server-lock` volume as the long-running
server and use its exact image-seeded lock path. Thus each command fails before
opening ClickHouse if the original server still owns the deployment, even when
restore uses a fresh `server-state` volume. At runtime, the configured lock parent must remain an
owner-private `0700` real directory without an extended ACL, and the lock must
remain an empty, single-link, effective-UID/GID-owned regular file with exact
mode `0600`; pathname replacement or metadata drift fails closed.

The outer recovery-set directory contains exactly `manifest.json` and an
unchanged `control-plane` backup child. The ClickHouse-native
`<recovery_set_id>.tar.zst` archive remains on the separate recovery disk. The
canonical outer manifest binds the child manifest and external archive by
exact name, size, and SHA-256, and also records the exact application release,
migration identities, ClickHouse server version, source database/table UUIDs,
maximum visibility sequence, native backup operation UUID, and the UUID of the
release-owned archive-marker table. Verification
recursively verifies the control-plane snapshot, master key, administrator
token, outer manifest, archive bytes, and ownership metadata. Renaming,
swapping, truncating, or combining members from different recovery sets fails
closed.

The recovery volumes have an exact identity contract. The image seeds
`server-recovery/private` as UID/GID `65532:65532`, mode `0700`; recovery-set
directories are mode `0700` and their files are mode `0600`. The networkless
`clickhouse-recovery-volume-bootstrap` accepts either an empty root-owned named
volume or an already prepared root, and prepares only that root as UID `101`,
GID `65532`, mode `02750`. Every native archive must remain a single-link
regular file owned by `101:65532` with mode `0640`. Do not change an archive to
the Open Splunk UID when copying it. Preserve both recovery volumes, member
names, and metadata as one off-host recovery set. Retain the separate
`server-lock` volume for the deployment lifecycle; it contains no recovery
payload, but it is the fence shared with the original server.

`generate-env.sh` creates distinct bootstrap, migration, runtime, deletion,
backup, and restore passwords. ClickHouse receives all six values so its
startup hook can provision or rotate the exact principals. Backup and marker
reconciliation receive only the read-only backup-password file and ClickHouse
CA; restore receives only its read-only restore-password file and the CA. The
verify helper receives neither, and the long-running server never receives
either recovery credential or recovery-volume mount. Keep the generated
password files in their owner-private host directory and never put a password
in a command argument.

### Create and verify a recovery set

Choose a new path below `/var/lib/open-splunk/recovery/private` by setting
`OPEN_SPLUNK_DEPLOYMENT_RECOVERY_SET_PATH` in `.env`. The default path is
`/var/lib/open-splunk/recovery/private/deployment-recovery-set`; backup refuses
to replace an existing destination or archive alias. Stop the server, then run
backup and the independent networkless verification:

```sh
docker compose stop --timeout 40 server
docker compose --profile recovery run --rm --no-deps deployment-backup
docker compose --profile recovery run --rm --no-deps deployment-verify
```

Both successful commands are silent and exit with status zero. Backup holds
the same retained deployment singleton lock as the server, rejects an active ClickHouse
mutation, proves the exact source schema and migration ledger before and after
the native backup, embeds an exact recovery-set/backup-operation marker inside
the archive, clears that marker from the live source, and rejects any
visibility or UUID identity change across the operation. A pre-publication
diagnostic or nonzero exit means that attempt is not a usable recovery set. The
normal backup helper deliberately sees
the archive volume read-only and cannot delete any retained backup. If its
diagnostic reports that exact cleanup of a newly created
`<recovery_set_id>.tar.zst` archive failed, first verify that no recovery set
was published by that attempt. Stop ClickHouse as well as the server, then
copy that canonical archive name independently into both confirmations for the
separate networkless destructive one-shot:

```sh
docker compose stop --timeout 40 clickhouse
OPEN_SPLUNK_FAILED_RECOVERY_ARCHIVE_NAME=<recovery_set_id>.tar.zst \
OPEN_SPLUNK_CONFIRMED_RECOVERY_ARCHIVE_NAME=<recovery_set_id>.tar.zst \
  docker compose --profile recovery run --rm --no-deps \
  deployment-archive-delete
```

An error explicitly reporting that outer publication already occurred is
different: do not delete that archive. Preserve the outer directory and
archive, then run `deployment-verify` independently. A zero verify retains a
usable published set; a failed verify leaves an ambiguous pair that must be
preserved for diagnosis rather than split or individually deleted.

This command deliberately cannot infer whether an archive was published. The
operator attests that the repeated exact name belongs to the failed attempt and
must be destroyed; supplying the name of a published recovery set destroys its
ClickHouse member. The tool runs as the fixed ClickHouse archive-owner UID,
mounts only the recovery volume, has no network or secret, revalidates the exact
root, file ownership, mode, link count, ACL, and pathname identity immediately
before deletion, syncs and proves final absence, and is idempotent when the
attested archive is already absent. It is supported only while ClickHouse is
stopped, because POSIX has no atomic unlink-by-descriptor operation. After
verified off-host retention, the original server may be restarted with:

```sh
docker compose up --detach --wait --no-build --no-deps clickhouse
docker compose up --detach --wait --no-build --no-deps server
```

### Reconcile an interrupted source marker

If a backup is interrupted after writing its source marker but before exact
cleanup completes, later backups fail closed rather than overwrite the retained
identity. Do not manually truncate `recovery_archive_markers`. Preserve the
failed-attempt diagnostic containing the exact recovery-set ID and backup
operation UUID.

The retained host singleton lock fences the server and recovery helper
processes, but it cannot cancel a native `BACKUP` that an earlier helper already
submitted inside ClickHouse. Stop both the server and ClickHouse, then restart
only ClickHouse before reconciliation. That restart is mandatory: it terminates
any old server-side native backup before the marker can be cleared while the
server remains fenced.

```sh
docker compose stop --timeout 40 server clickhouse
docker compose up --detach --wait --no-build --no-deps clickhouse
```

Before clearing anything, use the loopback-only bootstrap principal from inside
the ClickHouse container to inspect the confirmed operation UUID in
`system.backups` and the exact singleton marker, and inventory the retained
`clickhouse-recovery` volume. Confirm that no native backup remains active and
preserve any archive plus any published outer recovery-set directory for
independent verification. Reconciliation is not archive cleanup and cannot
decide whether those retained bytes are published or disposable.

```sh
OPEN_SPLUNK_STALE_RECOVERY_SET_ID=<recovery_set_id> \
OPEN_SPLUNK_CONFIRMED_STALE_RECOVERY_SET_ID=<recovery_set_id> \
OPEN_SPLUNK_STALE_BACKUP_OPERATION_UUID=<backup_operation_uuid> \
OPEN_SPLUNK_CONFIRMED_STALE_BACKUP_OPERATION_UUID=<backup_operation_uuid> \
  docker compose --profile recovery run --rm --no-deps \
  deployment-marker-reconcile
```

Copy both values independently into their confirmation variables. The helper
acquires the retained singleton lock before reading credentials or opening a
network connection, validates the exact backup-principal grant surface and
canonical source, requires the exact marker, clears it synchronously, and
proves absence. An already absent marker is an idempotent success only with
valid exact confirmations. A wrong, malformed, or duplicate marker fails
without mutation. The helper mounts no control state or recovery volume and
does not inspect backup status, open an archive, or delete one. After a zero
exit, either retry backup while the server remains stopped or restart the
server. Preserve the database for diagnosis instead of guessing if the two
marker identities cannot be established exactly.

### Restore a recovery set

Restore with the exact Open Splunk release recorded in the manifest. Retain
the paired `server-recovery` and `clickhouse-recovery` volumes and the existing
`server-lock` volume, but use fresh
`clickhouse-data`, `clickhouse-logs`, `server-state`, and `server-exports`
volumes. Rebind those four Compose volume keys to newly created named volumes,
using the committed `docker-compose.recovery-target.yaml` binding file, while
leaving both recovery volume keys and `server-lock` bound to the retained
deployment. Keep this binding file in every Compose command for the full
lifetime of the restored deployment; it is not a temporary restore overlay.
Keep the original data volumes intact
until the restored deployment passes validation. Do not start
`clickhouse-migrator`, `server-bootstrap`, or `server` against the fresh
volumes before restore: the final `open_splunk` database and all three
control-plane files must be absent. The restore helper itself creates and
exclusively holds the exact empty `open-splunk.db.server.lock` sidecar; that
mode-`0600`, single-link, helper-owned inode is the only additional entry
admitted in the fresh private state directory. Before opening ClickHouse, the
helper verifies the control bundle and complete destination publication prefix,
and proves that the lock pathname still names the descriptor it actually
holds. Apply `docker-compose.recovery-target.yaml` after the base file, then
apply `docker-compose.restore.yaml` for the entire restore lifecycle. The
read-only overlay intentionally does not rebind the
`clickhouse-recovery` volume; it changes only ClickHouse's mount mode, and the
restore command refuses to issue native `RESTORE` unless `system.disks`
attests the exact recovery disk path as read-only. Start only ClickHouse under
that overlay so its recovery principals are initialized, verify the retained
set again, and run restore:

```sh
docker volume create open-splunk-restored-clickhouse-data
docker volume create open-splunk-restored-clickhouse-logs
docker volume create open-splunk-restored-server-state
docker volume create open-splunk-restored-server-exports

export OPEN_SPLUNK_RECOVERY_CLICKHOUSE_DATA_VOLUME=open-splunk-restored-clickhouse-data
export OPEN_SPLUNK_RECOVERY_CLICKHOUSE_LOGS_VOLUME=open-splunk-restored-clickhouse-logs
export OPEN_SPLUNK_RECOVERY_SERVER_STATE_VOLUME=open-splunk-restored-server-state
export OPEN_SPLUNK_RECOVERY_SERVER_EXPORTS_VOLUME=open-splunk-restored-server-exports

docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \
  up --detach --wait --no-build --no-deps clickhouse
docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \
  --profile recovery run --rm --no-deps deployment-verify
docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \
  --profile recovery run --rm --no-deps deployment-restore
```

Use deployment-unique volume names, independently verify that all four volumes
are new and empty, and retain the four exported bindings in the deployment's
managed environment. `external: true` makes a typo or missing volume fail
instead of silently allocating a different target.

Restore verifies the complete set before mutation. Before issuing native
`RESTORE`, the command enumerates the bounded `open_splunk*` database namespace
and requires the canonical `open_splunk` database to be absent; every archive,
recovery, foreign reserved alias, or other prefixed database requires a
fresh ClickHouse data volume. ClickHouse restores the archive directly into
`open_splunk`. The command then rehashes and revalidates the complete recovery
set, requires the exact original manifest and archive digest, and validates the
canonical database's exact physical schema, migration ledger, server version,
visibility boundary, and lack of active mutations.

The exact archive-embedded recovery-set/backup-operation marker must be present
before receipt publication, so a same-name archive mounted into the helper but
different bytes mounted into ClickHouse cannot pass. Native `RESTORE ... AS`
intentionally creates fresh database and table UUIDs; the source UUIDs in the
outer manifest are provenance, not the expected restored UUIDs. The command
records the actual fresh UUIDs, including the marker-table UUID, in a durable
`recovery_sets` receipt bound to the recovery-set ID and outer-manifest SHA-256.
Only after that exact receipt is readable and revalidated does restore consume
the marker synchronously and prove its absence. There is no staging database,
rename, or promotion step.

ClickHouse native restore is not transactional. An unreceipted or mismatched
canonical database fails closed and requires another fresh ClickHouse data
volume. Only an exact receipted canonical database is resumable: a retry may
consume the still-exact marker after an interruption between receipt and
cleanup, or require it already absent after cleanup completed. Once the
canonical database and receipt are fully revalidated, the command closes its
restore session before restoring the control plane in the only allowed
publication order: SQLite database, master key, then administrator token. If that final
phase is interrupted, rerunning the same deployment restore revalidates the
ClickHouse receipt and resumes only an exact prefix of those three files.
Both the preflight and final control restore bind the recursively verified
child to the outer recovery-set ID and child-manifest SHA-256, so replacing it
with another same-release bundle after ClickHouse completion still fails before
any control target is mutated.
Every control-plane staging and publication boundary also revalidates that the
database-lock pathname still names the held inode; replacement, removal,
hard-linking, permission drift, or nonempty lock content fails closed.

Because ClickHouse authorization requires `INSERT` on every exact destination
table during native restore, treat the restore credential as an exposed
one-shot capability even though its table names and reads are tightly bounded.
Rotate its password after the one-shot restore before returning the deployment
to ordinary service. Revocation is not a supported substitute: the deliberate
restart initialization hook recreates the principal and its exact grants.

The base `docker-compose.yaml` deliberately keeps ClickHouse's recovery mount
writable so a later stopped-server backup can create a new native archive; the
backup and restore helper mounts remain read-only. Do not use a base-only
ClickHouse topology for this restored deployment. Retain both the fresh-volume
binding and read-only restore overlay while the restored deployment is being
validated. Before a future backup, recreate ClickHouse with the base file and
the retained recovery-target binding, dropping only the restore overlay, to
return only ClickHouse's recovery mount to its normal writable mode.

After a successful restore, use the exact-release migrator to verify the
restored schema, then start the server directly from restored state. Do not run
`server-bootstrap`, because doing so could mask a missing restored
administrator token.

```sh
docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \
  run --rm --no-deps clickhouse-migrator
docker compose -f docker-compose.yaml -f docker-compose.recovery-target.yaml -f docker-compose.restore.yaml \
  up --detach --wait --no-build --no-deps server
```

Export artifacts are deliberately outside the deployment recovery set. They
may be recreated by rerunning their source searches.

### Lower-level control-plane-only commands

Hot-copying the SQLite database, WAL, or sidecar files is not a supported
backup. The same release still provides lower-level control-plane-only
commands for maintenance that intentionally does not claim deployment
recovery. Stop the Open Splunk server before using them.

Create a bundle at a destination which does not already exist. Its existing
parent directory must be owned by the command user with mode `0700`:

```sh
open-splunk-server backup-control-plane \
  -control-db /var/lib/open-splunk/state/private/open-splunk.db \
  -master-key /var/lib/open-splunk/state/private/master.key \
  -administrator-token-file \
    /var/lib/open-splunk/state/private/administrator.token \
  -destination /path/to/new/control-plane-bundle
```

Successful creation is silent and exits with status zero. Any diagnostic or
nonzero exit means the destination must not be treated as a usable backup. A
complete bundle contains a manifest, one self-contained SQLite snapshot, the
matching master key, and the matching administrator token. It does not contain
the SQLite WAL or shared-memory sidecar.

Verify the completed bundle as a separate operation before copying or
restoring it:

```sh
open-splunk-server verify-control-plane-backup \
  -source /path/to/new/control-plane-bundle
```

Restore only into three absent paths in one fresh owner-only directory. In the
container deployment that directory is owned by UID/GID `65532:65532` with
mode `0700`; restored files are mode `0600`:

```sh
open-splunk-server restore-control-plane \
  -source /path/to/new/control-plane-bundle \
  -control-db /var/lib/open-splunk/state/private/open-splunk.db \
  -master-key /var/lib/open-splunk/state/private/master.key \
  -administrator-token-file \
    /var/lib/open-splunk/state/private/administrator.token
```

Do not restore over a current deployment or mix members from different
bundles. Restore publication has one resumable order: database, master key,
then administrator token. After an interruption, rerunning the same command
with the same bundle and targets may resume only when every existing target is
an exact match and the directory contains a prefix of that order: no members,
database only, database plus key, or all three members. A mismatched member, a
key without its database, a token without both predecessors, a SQLite sidecar,
or an unrelated directory entry fails closed. Do not repair a partial restore
by copying files manually.

> **Control-plane-only warning:** These commands contain no ClickHouse event
> data and are not deployment backup/restore commands. Use the coordinated
> recovery-set commands above whenever deployment recovery is intended. Never
> pair a control-plane-only bundle manually with an independently advanced,
> older, or different ClickHouse data set; doing so can hide acknowledged
> events, reuse visibility identities, or revive stale deletion state.

## Collector image

The same release pipeline that publishes the backend publishes the
multi-platform `ghcr.io/suhaibinator/open-splunk-collector:<version>` image
for Linux AMD64 and ARM64. Docker selects the matching architecture during a
normal pull. A local `make oci` build also creates
`open-splunk-collector:<version>` for its selected
`OPEN_SPLUNK_OCI_PLATFORM`. The non-root scratch image defaults to:

```text
open-splunk-collector run -config /etc/open-splunk/collector.yaml
```

The complete Docker runbook covers TLS compatible with this Compose stack,
network exposure, index and token prerequisites, exact mounts, UID `65532`
permissions, identity bootstrap, validation, startup, monitoring, upgrades,
token rotation, WAL recovery, disk pressure, and dead-letter handling:

[`docs/collector-deployment.md`](../docs/collector-deployment.md)

The default Compose file deliberately has no collector service. Its collector
port remains bound to host loopback, so make a deliberate firewall-restricted
listener change before deploying collectors on remote log hosts. Do not give a
collector ClickHouse or administrator credentials.
