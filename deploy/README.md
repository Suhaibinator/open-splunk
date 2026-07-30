# Local deployment

The local stack currently provides the external ClickHouse service used by the
Open Splunk server. The server remains one application binary, and collectors
remain separate processes on log-producing hosts.

## Start ClickHouse

Generate four independent 256-bit development passwords, then start the
service from this directory so Compose loads `deploy/.env`:

```sh
cd deploy
./generate-env.sh
docker compose up --detach --wait clickhouse
```

The generator refuses to overwrite an existing file and creates it under a
restrictive umask. `.env` is ignored by Git. Do not place real passwords in
`.env.example`.

Initialization creates four identities:

- `open_splunk_bootstrap` is the local deployment administrator. The
  application never receives this credential. Its config-backed network
  allowlist contains only `127.0.0.1` and `::1`, so administrative recovery
  runs through `docker compose exec clickhouse ...` inside the container; the
  published native port cannot authenticate this user.
- `open_splunk_migrator` can create only the embedded database/tables, apply
  the additive event-schema changes, and read/write the migration ledger.
- `open_splunk_runtime` can select and insert event rows and read only
  `database`, `table`, `active`, `rows`, and `bytes_on_disk` from
  `system.parts` for the index-storage estimate. Ingestion, search, export,
  isolated EXPLAIN lanes, and index statistics share this identity.
- `open_splunk_deletion` can select only `tenant_id` and `index_name`, submit
  `ALTER DELETE`, and read the two system tables needed to reconcile
  mutations. It cannot insert, migrate, drop/rename/truncate the table, or
  alter its schema.

ClickHouse 26.3 also uses `ALTER DELETE` to authorize `DROP PARTITION`,
`DROP DETACHED PARTITION`, `FORGET PARTITION`, and `APPLY DELETED MASK`.
That unavoidable blast radius is why the deletion connection is private to
the fixed deletion Store path and never shared with search, ingestion, or
inspection. Its event `SELECT` is independently column-scoped to the two
identity columns, and startup separately rejects any explicit privilege
outside the exact deletion allowlist. The pinned live contract confirms that
`ALTER MOVE PARTITION` is absent.

The server defaults to these three application usernames and reads their
passwords from `OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD`,
`OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD`, and
`OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD`. Keep
`OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD` out of the server environment when
running outside this local Compose workflow. The application removes the
migration password from its process environment as soon as connection options
have captured it, then clears and releases those privileged options as soon as
the short-lived migration session closes.

Both ClickHouse ports are published on `127.0.0.1` only:

- HTTP: `8123`
- native protocol: `9000`

Set `OPEN_SPLUNK_CLICKHOUSE_HTTP_PORT` or
`OPEN_SPLUNK_CLICKHOUSE_NATIVE_PORT` in `.env` to change a host-side port; the
bind address remains loopback. Containers in the same Compose project can use
`clickhouse:9000` directly.

Check runtime data access with the password kept in the environment file:

```sh
set -a
. ./.env
set +a
docker compose exec clickhouse clickhouse-client \
  --user open_splunk_runtime \
  --password "$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD" \
  --query "SELECT count() FROM open_splunk.events"
```

Stop containers without deleting stored events:

```sh
docker compose down
```

`docker compose down --volumes` also deletes all local ClickHouse data and is
only appropriate when a disposable development database is intended.

## Version and migrations

The image is pinned to the concrete official 26.3 LTS artifact
`clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49`.
It supports the production native `JSON` type and the GA native text index
used by the schema. Floating tags and tag-only references are not acceptable
because their behavior can change without a repository change.

`clickhouse-init.sh` first rejects every server version other than the audited
`26.3.17.4`, provisions only the migrator, runs
`../migrations/clickhouse/*.sql` lexicographically as that restricted identity,
and provisions the two long-lived principals only after the schema is
complete. The script and every migration are idempotent, so Compose runs them
on every container start. A partial first initialization therefore remains
fail-closed and is retried on restart instead of leaving a healthy-looking,
half-provisioned persistent volume. The same path rotates all four independent
credentials when the container is recreated over an existing volume. Neither
`CLICKHOUSE_DB` nor a direct `/docker-entrypoint-initdb.d` migration mount is
used, so the bootstrap principal does not own application schema creation.

The application migration runner likewise first requires the exact audited
server version, validates the migrator's complete explicit grant allowlist,
opens that short-lived identity on every server startup, applies pending files,
and closes it before opening runtime/deletion connections or constructing the
Store. Startup then validates both long-lived principals against their own
complete allowlists and refuses missing or excessive privileges. A denied read
of `system.server_settings` behaviorally proves that the ClickHouse server
configuration requires explicit grants for non-public system tables.
