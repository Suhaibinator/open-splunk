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

## Server configuration

Every normal server setting has both a CLI flag and an explicitly registered
environment variable. Built-in defaults are applied first, environment values
override them, and explicitly supplied CLI flags win over environment values.
Environment values use the same string, Boolean, integer, and duration parsers
as their CLI counterparts.

| CLI flag | Environment variable |
| --- | --- |
| `-verify-embedded-release` | `OPEN_SPLUNK_SERVER_VERIFY_EMBEDDED_RELEASE` |
| `-tenant-id` | `OPEN_SPLUNK_SERVER_TENANT_ID` |
| `-http-listen-address` | `OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS` |
| `-http-allowed-hosts` | `OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS` |
| `-http-tls-certificate-file` | `OPEN_SPLUNK_SERVER_HTTP_TLS_CERTIFICATE_FILE` |
| `-http-tls-private-key-file` | `OPEN_SPLUNK_SERVER_HTTP_TLS_PRIVATE_KEY_FILE` |
| `-http-trust-x-forwarded-proto` | `OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO` |
| `-control-database-file` | `OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE` |
| `-master-key-file` | `OPEN_SPLUNK_SERVER_MASTER_KEY_FILE` |
| `-server-lock-file` | `OPEN_SPLUNK_SERVER_LOCK_FILE` |
| `-export-artifact-directory` | `OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY` |
| `-administrator-token` | `OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN` |
| `-administrator-token-file` | `OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN_FILE` |
| `-clickhouse-address` | `OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS` |
| `-clickhouse-database` | `OPEN_SPLUNK_SERVER_CLICKHOUSE_DATABASE` |
| `-clickhouse-username` | `OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME` |
| `-clickhouse-password` | `OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD` |
| `-clickhouse-password-file` | `OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD_FILE` |
| `-clickhouse-tls-enabled` | `OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_ENABLED` |
| `-clickhouse-tls-ca-certificate-file` | `OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_CA_CERTIFICATE_FILE` |
| `-clickhouse-tls-server-name` | `OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_SERVER_NAME` |
| `-clickhouse-skip-migrations` | `OPEN_SPLUNK_SERVER_CLICKHOUSE_SKIP_MIGRATIONS` |
| `-collector-grpc-listen-address` | `OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_LISTEN_ADDRESS` |
| `-collector-grpc-plaintext-enabled` | `OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_PLAINTEXT_ENABLED` |
| `-collector-grpc-tls-certificate-file` | `OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_TLS_CERTIFICATE_FILE` |
| `-collector-grpc-tls-private-key-file` | `OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_TLS_PRIVATE_KEY_FILE` |
| `-hec-enabled` | `OPEN_SPLUNK_SERVER_HEC_ENABLED` |
| `-default-index-retention` | `OPEN_SPLUNK_SERVER_DEFAULT_INDEX_RETENTION` |
| `-search-history-maximum-age` | `OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_AGE` |
| `-search-history-maximum-entries-per-owner` | `OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_ENTRIES_PER_OWNER` |
| `-search-attempt-audit-maximum-retained-attempts` | `OPEN_SPLUNK_SERVER_SEARCH_ATTEMPT_AUDIT_MAXIMUM_RETAINED_ATTEMPTS` |

For administrator and ClickHouse credentials, select either the raw value or
the file at one configuration tier. Supplying both forms at the same tier is
an error. An explicit CLI credential form overrides both environment forms.
File flags are safer than raw CLI flags because command-line arguments may be
visible in process listings and shell history. Sensitive environment values
are removed from the server process environment immediately after parsing.

This table applies to normal server startup. Recovery and provisioning
subcommands retain their purpose-specific interfaces, and collector YAML
remains the collector daemon's configuration source.

## Add Open Splunk to an existing Compose project

Copy the `server` service, volumes, and `per-obs-network` reference from
[`docker-compose.yaml`](docker-compose.yaml) into the Compose project that
already contains ClickHouse. Both services must share the same network, so the
ClickHouse service name resolves inside the Open Splunk container.

The `image` value selects the Open Splunk artifact before Docker can pull or
start it. It is deployment configuration, not the server's runtime version.
Official images also contain their product version and source revision in the
binary and OCI metadata. When adding the service to an existing Compose file,
either replace the variable with an exact version directly:

```yaml
services:
  server:
    image: ghcr.io/suhaibinator/open-splunk-server:0.MINOR.PATCH
```

or retain the `OPEN_SPLUNK_DEPLOY_SERVER_IMAGE` variable used by the supplied Compose
file. Replace `0.MINOR.PATCH` with an exact published version in either case;
do not use `latest` as a deployment pin.

For an existing Compose project, paste the following `server` entry beneath
its existing `services:` key, merge the three volume entries beneath its
existing top-level `volumes:` key, and use the network already shared with
ClickHouse. Do not add a second `services:` or `volumes:` key.

```yaml
services:
  server:
    image: ghcr.io/suhaibinator/open-splunk-server:0.MINOR.PATCH
    restart: unless-stopped
    init: true
    user: "65532:65532"
    read_only: true
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    environment:
      TMPDIR: /tmp
      OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS: 0.0.0.0:8080
      OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS: "${OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS:?set OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS}"
      OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO: "${OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO:-false}"
      OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE: /var/lib/open-splunk/state/private/open-splunk.db
      OPEN_SPLUNK_SERVER_MASTER_KEY_FILE: /var/lib/open-splunk/state/private/master.key
      OPEN_SPLUNK_SERVER_LOCK_FILE: /var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock
      OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY: /var/lib/open-splunk/exports/private
      OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN: "${OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN:?set OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN}"
      # Set this to the existing per-clickhouse account's exact password.
      OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS: per-clickhouse:9000
      OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME: clickhouse
      OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD: "${OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD:?set OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD}"
    ports:
      - "8080:8080"
    volumes:
      - open-splunk-state:/var/lib/open-splunk/state
      - open-splunk-lock:/var/lib/open-splunk/lock
      - open-splunk-exports:/var/lib/open-splunk/exports
    tmpfs:
      - /tmp:rw,noexec,nosuid,nodev,mode=0700,uid=65532,gid=65532
    networks:
      - per-obs-network
    healthcheck:
      test:
        - CMD
        - /usr/local/bin/open-splunk-server
        - healthcheck
        - -url
        - http://127.0.0.1:8080/readyz
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 30s
    stop_grace_period: 60s
    pids_limit: 512

volumes:
  open-splunk-state:
  open-splunk-lock:
  open-splunk-exports:
```

If the existing project uses different service, account, network, or published
port names, change `per-clickhouse`, `clickhouse`, `per-obs-network`, or the
left side of `8080:8080` respectively. Keep ClickHouse's container-side native
port at `9000` unless its listener itself was reconfigured.

### Connect to the existing ClickHouse service

Use the ClickHouse **Compose service name and container port**, not the host's
published port. For example, given this existing service:

```yaml
services:
  per-clickhouse:
    image: clickhouse/clickhouse-server:26.7.5.10
    environment:
      CLICKHOUSE_USER: clickhouse
      CLICKHOUSE_PASSWORD: "${CLICKHOUSE_PASSWORD:?set CLICKHOUSE_PASSWORD}"
      CLICKHOUSE_DB: logs
    ports:
      - "8123:8123"
      - "9030:9000"
    networks:
      - per-obs-network
```

the corresponding Open Splunk settings are:

```yaml
services:
  server:
    environment:
      # This must be the password for CLICKHOUSE_USER above.
      OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS: per-clickhouse:9000
      OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME: clickhouse
      OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD: "${CLICKHOUSE_PASSWORD:?set CLICKHOUSE_PASSWORD}"
    networks:
      - per-obs-network
```

Here, `9030` is only for clients connecting through the Docker host. Containers
on `per-obs-network` connect directly to `per-clickhouse:9000`. The existing
`CLICKHOUSE_DB: logs` setting does not need to change: Open Splunk connects with
the configured account, then creates and owns a separate `open_splunk` database.
It does not read or modify `logs`.

If the existing ClickHouse password is written directly in its service instead
of read from `${CLICKHOUSE_PASSWORD}`, give
`OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD` that same exact value. Prefer placing the
shared value in the Compose `.env` file instead of duplicating it in YAML:

```dotenv
CLICKHOUSE_PASSWORD=replace-with-the-existing-clickhouse-password
```

### Configure browser access

The supplied service listens on `0.0.0.0:8080` so Docker can publish its HTTP
port. A wildcard listener requires an explicit allowlist of Host header names:

```yaml
environment:
  OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS: 0.0.0.0:8080
  OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS: "localhost,127.0.0.1,192.168.1.50,logs.example.internal"
```

List every hostname or IP address that a browser will use to open Open Splunk.
Use exact comma-separated names without URL schemes, paths, or ports. The
ClickHouse service name does not belong in this list unless browsers also use
that name to reach Open Splunk.

When a controlled reverse proxy terminates browser HTTPS and uses plaintext
HTTP for its connection to Open Splunk, opt in to its scheme assertion:

```dotenv
OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO=true
```

The proxy must replace, rather than append to, `X-Forwarded-Proto`. For nginx:

```nginx
location / {
    proxy_pass http://192.168.2.13:3016;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

Only enable this setting when clients cannot bypass the controlled proxy and
reach the plaintext Open Splunk listener directly. It accepts the forwarded
scheme globally on plaintext connections; direct TLS remains authoritative.
The setting uses the same Boolean syntax as the corresponding CLI flag and
defaults to `false` when unset.

### Generate the administrator sign-in token

`OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN` is the Open Splunk browser sign-in credential.
It is unrelated to the ClickHouse password. Generate it once and retain it:

```sh
openssl rand -base64 48
```

Paste the single-line output into `.env` without a `Bearer ` prefix:

```dotenv
OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN=replace-with-the-generated-value
```

The token must contain 32 through 512 ASCII characters from letters, digits,
`-._~+/`, with `=` permitted only as trailing base64 padding. Do not add spaces,
quotes, or line breaks to the value.

Create the environment file:

```sh
cd deploy
cp .env.example .env
```

Set these values in `.env`:

```dotenv
OPEN_SPLUNK_DEPLOY_SERVER_IMAGE=ghcr.io/suhaibinator/open-splunk-server:0.MINOR.PATCH
OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS=per-clickhouse:9000
OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME=clickhouse
OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD=replace-with-the-existing-password
OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN=replace-with-a-long-random-token
OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS=localhost,127.0.0.1,logs.example.internal
OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO=false
```

Replace `0.MINOR.PATCH` with an exact published version. Generate the
administrator token once with `openssl rand -base64 48` and retain it; it is
the browser sign-in credential.

Pull and start the server:

```sh
docker pull "$OPEN_SPLUNK_DEPLOY_SERVER_IMAGE"
docker compose up --detach --wait server
```

Open `http://127.0.0.1:8080/signin/` and enter the configured administrator
token. On the first start, the server connects to ClickHouse's `default`
database, creates and migrates `open_splunk`, then opens its runtime
connections to `open_splunk`. Later starts apply only pending migrations.

If ClickHouse is not ready yet, the server exits without reporting readiness;
the `restart: unless-stopped` policy retries startup.

## Startup errors

`restart: unless-stopped` causes a configuration error to appear repeatedly as
Docker restarts the failed process. Correct the setting, then recreate and
follow the service:

```sh
docker compose up --detach --force-recreate server
docker compose logs --follow server
```

| Error | Meaning and correction |
| --- | --- |
| `wildcard HTTP listeners require an explicit -http-allowed-hosts value` | `-http-listen-address` uses `0.0.0.0` or `[::]`, but `-http-allowed-hosts` is absent or empty. Add the exact browser-facing hostnames and IP addresses as described above. |
| `administrator token contains an invalid bearer token` | `OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN` is empty, too short, contains whitespace or unsupported characters, or includes a `Bearer ` prefix. Generate and paste a new token as described above. Do not use the ClickHouse password as this token. |
| `configure -http-trust-x-forwarded-proto from OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO` | The reverse-proxy opt-in is not a valid Boolean value. Use the same syntax accepted by the CLI Boolean flag. |
| ClickHouse connection failure at `localhost`, `9030`, or another host-published port | From the Open Splunk container, use the ClickHouse Compose service name and its container port, normally `per-clickhouse:9000`. Ensure both services share a network. |
| ClickHouse authentication failure | `OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME` and `OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD` must match the existing ClickHouse account's `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`. |

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
`-http-tls-certificate-file`, `-http-tls-private-key-file`,
`-clickhouse-tls-enabled`, `-clickhouse-tls-ca-certificate-file`, and
`-clickhouse-tls-server-name`.

## Persistent state

The Compose service persists the SQLite control plane, generated master key,
singleton lock, and export artifacts in named volumes. ClickHouse data remains
owned by the existing ClickHouse service. The default Compose deployment does
not configure backup or restore jobs; back up both systems using your normal
infrastructure procedures.
