# Docker Compose with an existing ClickHouse

The default deployment starts one Open Splunk server and connects it to an
existing ClickHouse instance over the native protocol. The server applies its
embedded ClickHouse migrations before it becomes ready; there is no separate
migration container.

Open Splunk always owns a dedicated database named `open_splunk`. Existing
databases such as `logs` are not read, migrated, or modified.

## Requirements

- Docker with Compose v2;
- an existing ClickHouse native listener reachable from the Open Splunk
  container (for example, `per-clickhouse:9000`); and
- a ClickHouse account with enough privileges to create and migrate
  `open_splunk`, read and write its tables, execute deletion mutations, and
  inspect the required `system.tables`, `system.parts`, and `system.mutations`
  rows.

The tested ClickHouse release is `26.7.5.10`. Pin that release instead of using
the mutable `clickhouse/clickhouse-server:latest` tag.

## Add Open Splunk to an existing Compose project

Copy the `server` service, volumes, and `per-obs-network` reference from
[`docker-compose.yaml`](docker-compose.yaml) into the Compose project that
already contains ClickHouse. Both services must share the same network, so the
ClickHouse service name resolves inside the Open Splunk container.

Create the environment file:

```sh
cd deploy
cp .env.example .env
```

Set these values in `.env`:

```dotenv
OPEN_SPLUNK_SERVER_IMAGE=ghcr.io/suhaibinator/open-splunk-server:0.MINOR.PATCH
OPEN_SPLUNK_CLICKHOUSE_ADDRESS=per-clickhouse:9000
OPEN_SPLUNK_CLICKHOUSE_USERNAME=clickhouse
OPEN_SPLUNK_CLICKHOUSE_PASSWORD=replace-with-the-existing-password
OPEN_SPLUNK_ADMINISTRATOR_TOKEN=replace-with-a-long-random-token
OPEN_SPLUNK_HTTP_ALLOWED_HOSTS=localhost,127.0.0.1,logs.example.internal
```

Replace `0.MINOR.PATCH` with an exact published version. Generate the
administrator token once with `openssl rand -base64 48` and retain it; it is
the browser sign-in credential.

Pull and start the server:

```sh
docker pull "$OPEN_SPLUNK_SERVER_IMAGE"
docker compose up --detach --wait server
```

Open `http://127.0.0.1:8080/signin/` and enter the configured administrator
token. On the first start, the server connects to ClickHouse's `default`
database, creates and migrates `open_splunk`, then opens its runtime
connections to `open_splunk`. Later starts apply only pending migrations.

If ClickHouse is not ready yet, the server exits without reporting readiness;
the `restart: unless-stopped` policy retries startup.

## Transport and credential model

Plaintext HTTP and plaintext native ClickHouse are supported by default,
including across a Docker bridge network. Do not expose either transport over
an untrusted network. Terminate HTTPS at a reverse proxy when browser traffic
leaves a trusted host or network.

The same ClickHouse username and password are used for schema migration,
runtime reads and writes, inspection, and index deletion. The password and
administrator token are read from the environment and removed from the server
process environment immediately after configuration. Docker still exposes
configured environment values through its container metadata, so protect
access to the Docker daemon and the `.env` file.

Verified TLS remains available for non-Compose or customized deployments via
`-http-tls-cert`, `-http-tls-key`, `-clickhouse-secure`,
`-clickhouse-ca-cert`, and `-clickhouse-server-name`.

## Persistent state

The Compose service persists the SQLite control plane, generated master key,
singleton lock, and export artifacts in named volumes. ClickHouse data remains
owned by the existing ClickHouse service. The default Compose deployment does
not configure backup or restore jobs; back up both systems using your normal
infrastructure procedures.
