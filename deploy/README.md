# Local deployment

The local stack currently provides the external ClickHouse service used by the
Open Splunk server. The server remains one application binary, and collectors
remain separate processes on log-producing hosts.

## Start ClickHouse

Generate a unique 256-bit development password, then start the service from
this directory so Compose loads `deploy/.env`:

```sh
cd deploy
./generate-env.sh
docker compose up --detach --wait clickhouse
```

The generator refuses to overwrite an existing file and creates it under a
restrictive umask. `.env` is ignored by Git. Do not place a real password in
`.env.example` or pass one on a command line that will be retained in shell
history.

Both ClickHouse ports are published on `127.0.0.1` only:

- HTTP: `8123`
- native protocol: `9000`

Set `OPEN_SPLUNK_CLICKHOUSE_HTTP_PORT` or
`OPEN_SPLUNK_CLICKHOUSE_NATIVE_PORT` in `.env` to change a host-side port; the
bind address remains loopback. Containers in the same Compose project can use
`clickhouse:9000` directly.

Check the schema with the password kept in the environment file:

```sh
set -a
. ./.env
set +a
docker compose exec clickhouse clickhouse-client \
  --user open_splunk \
  --password "$OPEN_SPLUNK_CLICKHOUSE_PASSWORD" \
  --query "DESCRIBE TABLE open_splunk.events"
```

Stop containers without deleting stored events:

```sh
docker compose down
```

`docker compose down --volumes` also deletes all local ClickHouse data and is
only appropriate when a disposable development database is intended.

## Version and migrations

The image is pinned to the concrete official 26.3 LTS patch
`clickhouse/clickhouse-server:26.3.17.4`. It supports the production native
`JSON` type and the GA native text index used by the schema. Floating `latest`
and `lts` tags are not acceptable because their schema behavior can change
without a repository change.

The official image runs `../migrations/clickhouse/*.sql` lexicographically only
when initializing an empty data volume. That is convenient for first startup,
not an upgrade mechanism. The application migration runner must apply pending
files on every server startup before serving ingestion or search traffic.
