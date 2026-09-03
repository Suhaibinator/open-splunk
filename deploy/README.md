# Docker Compose with an existing ClickHouse

The default deployment starts one Open Splunk server and connects it to an
existing ClickHouse instance over the native protocol. The server applies its
embedded ClickHouse migrations before it becomes ready; there is no separate
migration container.

Open Splunk always owns a dedicated database named `open_splunk`. Existing
databases such as `logs` are not read, migrated, or modified.

The supplied service publishes its plaintext HTTP listener on every host
interface. This is also the HEC transport when HEC is enabled. Use
`127.0.0.1:${OPEN_SPLUNK_DEPLOY_HTTP_PORT:-8080}:8080` as the port mapping for
host-local access, or configure direct HTTP TLS/a controlled TLS reverse proxy
before exposing the service to another network.

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
as their CLI counterparts. Lowercase `true` and `false`, decimal integers, and
Go duration strings such as `30m`, `24h`, or `720h` are the recommended
spellings. An empty environment value is passed to the option parser rather
than treated as unset: it is usually invalid for typed values, while string
options may subsequently apply a documented derived-default rule.

| Environment variable | CLI flag | Built-in default | Purpose | Accepted values and constraints |
| --- | --- | --- | --- | --- |
| `OPEN_SPLUNK_SERVER_VERIFY_EMBEDDED_RELEASE` | `-verify-embedded-release` | `false` | Verify the embedded release payload and exit without opening runtime storage or listeners. | Boolean. |
| `OPEN_SPLUNK_SERVER_LOG_LEVEL` | `-log-level` | `info` | Set the minimum server and router log level. | Case-insensitive `debug`, `info`, `warn`, or `error`. |
| `OPEN_SPLUNK_SERVER_LOG_FORMAT` | `-log-format` | `json` | Select the server and router log encoding written to standard error. | Case-insensitive `json` or `console`; `json` is recommended for deployed services. |
| `OPEN_SPLUNK_SERVER_TENANT_ID` | `-tenant-id` | `default` | Set the single-node tenant identity persisted with tenant-scoped data. | Non-empty UTF-8 after trimming, no control characters, at most 255 bytes. Changing an established identity creates a different tenant scope. |
| `OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS` | `-http-listen-address` | `127.0.0.1:8080` | Select the browser API, UI, and HEC listen address. | `host:port`. A wildcard host such as `0.0.0.0` or `[::]` requires an explicit allowed-host list. |
| `OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS` | `-http-allowed-hosts` | Derived from the listen host | Admit browser/API `Host` values before origin and authentication checks. | Comma-separated DNS names or IP literals, without schemes, paths, or ports; at most 32 entries. Every browser-facing name must be listed. |
| `OPEN_SPLUNK_SERVER_HTTP_TLS_CERTIFICATE_FILE` | `-http-tls-certificate-file` | Empty; HTTP | Provide the PEM certificate chain for direct HTTPS. | File path. Must be supplied together with the HTTP private-key file and form a valid matching TLS identity. |
| `OPEN_SPLUNK_SERVER_HTTP_TLS_PRIVATE_KEY_FILE` | `-http-tls-private-key-file` | Empty; HTTP | Provide the PEM private key for direct HTTPS. | File path. Must be supplied together with the HTTP certificate file and match its certificate. |
| `OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO` | `-http-trust-x-forwarded-proto` | `false` | Allow a controlled plaintext reverse proxy to assert the browser-facing scheme. | Boolean. When enabled, plaintext requests may contain exactly one `X-Forwarded-Proto` value, exactly `http` or `https`; direct TLS remains authoritative. Enable only when clients cannot bypass the proxy. |
| `OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE` | `-control-database-file` | `open-splunk.db` | Select the persistent SQLite control-plane database. | Persistent file path; empty paths and `:memory:` are rejected during lock acquisition. |
| `OPEN_SPLUNK_SERVER_MASTER_KEY_FILE` | `-master-key-file` | `<control-database-file>.key` | Select the server master key bound to the control database. | File path. A missing key is generated as 32 bytes with mode `0600`; an existing key must contain exactly 32 bytes. Preserve it with the control database. |
| `OPEN_SPLUNK_SERVER_LOCK_FILE` | `-server-lock-file` | `/tmp/open-splunk-server-open_splunk.server.lock` | Select the host-wide singleton lock that fences the canonical ClickHouse schema. | Exact absolute path. A custom path must be inside an existing private directory. |
| `OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY` | `-export-artifact-directory` | `<control-database-file>.exports` | Select the private directory for generated export artifacts. | Valid UTF-8 path without NUL bytes; must resolve to a dedicated non-root directory and must not escape through a leading `..`. |
| `OPEN_SPLUNK_SERVER_SEARCH_ARTIFACT_DIRECTORY` | `-search-artifact-directory` | `<control-database-file>.search-artifacts` | Select the private directory for durable retained-search results. | Dedicated non-root directory owned by the server user. It is exclusively locked while the server runs and stores owner-only files with checksums. |
| `OPEN_SPLUNK_SERVER_ALERT_PUBLIC_BASE_URL` | `-alert-public-base-url` | disabled | Supply absolute links in webhook payloads and permit alert enabling. | Absolute `http://` or `https://` URL reachable by webhook receivers; HTTPS is recommended outside trusted local development. Alerts may be configured while absent but cannot be enabled. |
| `OPEN_SPLUNK_SERVER_ALERT_PRIVATE_WEBHOOK_HOSTS` | `-alert-private-webhook-hosts` | empty | Permit named webhook receivers whose validated address is private, loopback, or link-local. | Comma-separated exact DNS hostnames; public HTTPS destinations need no entry. |
| `OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN` | `-administrator-token` | None; required unless a token file is used | Configure the browser administrator bearer token. | Token68 ASCII, 32–512 bytes, without a `Bearer ` prefix or whitespace; trailing `=` padding is allowed. Mutually exclusive with the token-file setting at the same tier. The environment value is removed after parsing. |
| `OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN_FILE` | `-administrator-token-file` | None; required unless a raw token is used | Read the browser administrator token from a file. | Regular file owned by the server user, exactly mode `0400` or `0600`, with one hard link. One trailing LF or CRLF is removed. Mutually exclusive with the raw token at the same tier. |
| `OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS` | `-clickhouse-address` | `127.0.0.1:9000` | Select the ClickHouse native-protocol endpoint. | Non-empty native endpoint, normally `host:port`. In Compose, use the service name and container port rather than a host-published port. |
| `OPEN_SPLUNK_SERVER_CLICKHOUSE_DATABASE` | `-clickhouse-database` | `open_splunk` | Select the application database used after migrations. | Must be exactly `open_splunk`; the embedded schema does not support another database name. |
| `OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME` | `-clickhouse-username` | `default` | Configure the ClickHouse account used for migrations and runtime operations. | Non-empty string. The account must have the privileges described under Requirements. |
| `OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD` | `-clickhouse-password` | None; required unless a password file is used | Configure the ClickHouse password. | Non-empty string. Mutually exclusive with the password-file setting at the same tier. The environment value is removed after parsing. |
| `OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD_FILE` | `-clickhouse-password-file` | None; required unless a raw password is used | Read the ClickHouse password from a file. | Regular 1–4096-byte file, owner-readable, non-executable, without group/other write permission, special bits, ACL metadata, or additional hard links. One trailing LF is removed. |
| `OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_ENABLED` | `-clickhouse-tls-enabled` | `false` | Enable verified TLS for every ClickHouse connection. | Boolean. Enabling it requires both an explicit CA certificate file and TLS server name. |
| `OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_CA_CERTIFICATE_FILE` | `-clickhouse-tls-ca-certificate-file` | Empty | Select the trust bundle for ClickHouse TLS verification. | File path containing only valid certificate PEM blocks, at most 1 MiB. Requires ClickHouse TLS and is rejected when TLS is disabled. |
| `OPEN_SPLUNK_SERVER_CLICKHOUSE_TLS_SERVER_NAME` | `-clickhouse-tls-server-name` | Empty | Select the DNS name or IP SAN verified on the ClickHouse certificate. | Valid bounded DNS name or IP address without a port or wildcard. Requires ClickHouse TLS and is rejected when TLS is disabled. |
| `OPEN_SPLUNK_SERVER_CLICKHOUSE_SKIP_MIGRATIONS` | `-clickhouse-skip-migrations` | `false` | Skip applying the embedded ClickHouse migrations at startup. | Boolean. Use only when an external process has already provisioned the exact embedded schema. |
| `OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_LISTEN_ADDRESS` | `-collector-grpc-listen-address` | Empty; listener disabled | Enable the native collector gRPC listener. | Empty or `host:port`. A configured listener requires either both collector TLS files or explicit loopback-only plaintext mode. |
| `OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_PLAINTEXT_ENABLED` | `-collector-grpc-plaintext-enabled` | `false` | Permit collector gRPC without TLS for local development. | Boolean. Valid only with a loopback listen address and cannot be combined with collector TLS files. |
| `OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_TLS_CERTIFICATE_FILE` | `-collector-grpc-tls-certificate-file` | Empty | Provide the PEM certificate for collector gRPC TLS. | File path. Required with the collector private-key file when the listener is enabled without plaintext mode. |
| `OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_TLS_PRIVATE_KEY_FILE` | `-collector-grpc-tls-private-key-file` | Empty | Provide the PEM private key for collector gRPC TLS. | File path. Required with the collector certificate file and must form a valid matching TLS identity. |
| `OPEN_SPLUNK_SERVER_HEC_ENABLED` | `-hec-enabled` | `false` | Enable the complete HTTP Event Collector route family on the HTTP listener. | Boolean. Production HEC traffic must use direct HTTPS or a reviewed TLS reverse proxy. |
| `OPEN_SPLUNK_SERVER_DEFAULT_INDEX_RETENTION` | `-default-index-retention` | `720h` (30 days) | Set retention for indexes that inherit the deployment default. | Positive Go duration using whole milliseconds; the resulting expiration must remain in the supported timestamp range. |
| `OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_AGE` | `-search-history-maximum-age` | `720h` (30 days) | Bound the age of terminal search-history entries. | Go duration from zero through 10 years; zero selects the 30-day default. |
| `OPEN_SPLUNK_SERVER_SEARCH_HISTORY_MAXIMUM_ENTRIES_PER_OWNER` | `-search-history-maximum-entries-per-owner` | `10000` | Bound terminal search-history entries per owner; pending attempts are capped independently at the same value. | Integer from zero through `1000000`; zero selects `10000`. |
| `OPEN_SPLUNK_SERVER_SEARCH_ATTEMPT_AUDIT_MAXIMUM_RETAINED_ATTEMPTS` | `-search-attempt-audit-maximum-retained-attempts` | `100000` | Bound the rolling payload-free search-attempt audit journal per tenant. | Integer zero or from `1` through `100000`; zero selects `100000`. |

The default JSON logger writes one record per line to standard error with an
ISO-8601 timestamp, level, `open-splunk-server` logger name, caller, message,
and typed context fields. The console encoder exposes the same information in a
human-readable form. The selected logger is also used by the embedded router.

For administrator and ClickHouse credentials, select either the raw value or
the file at one configuration tier. Supplying both forms at the same tier is
an error. An explicit CLI credential form overrides both environment forms.
File flags are safer than raw CLI flags because command-line arguments may be
visible in process listings and shell history. Sensitive environment values
are removed from the server process environment immediately after parsing.

This table applies to normal server startup. Recovery and provisioning
subcommands retain their purpose-specific interfaces, and collector YAML
remains the collector daemon's configuration source.

### Compose deployment variables

These variables are consumed by Compose or the development scripts. They are
not passed to `open-splunk-server` and therefore have no CLI equivalents.

| Environment variable | Default | Purpose | Accepted values and constraints |
| --- | --- | --- | --- |
| `OPEN_SPLUNK_DEPLOY_SERVER_IMAGE` | None; required by the supplied server Compose service | Select the server image for Docker to pull and run. | Exact tagged or digest-pinned OCI image reference. Published deployments should not use `latest`. |
| `OPEN_SPLUNK_DEPLOY_HTTP_PORT` | `8080` | Select the Docker host port published to container port `8080`; the development runner also uses it for its loopback listener. | Valid available TCP port on the host. |
| `OPEN_SPLUNK_DEPLOY_CLICKHOUSE_NATIVE_PORT` | `9000` | Select the loopback host port published by the development-only ClickHouse Compose service. | Valid available TCP port on the host. Server containers still connect to ClickHouse's container port `9000`. |

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

or retain the `OPEN_SPLUNK_DEPLOY_SERVER_IMAGE` variable used by the supplied
Compose file. Replace `0.MINOR.PATCH` with an exact published version in either
case; do not use `latest` as a deployment pin.

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
    proxy_pass http://127.0.0.1:8080;
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

## Health and readiness

`GET /healthz` checks only that the HTTP process can answer and returns
`200 ok`. `GET /readyz` also checks runtime readiness and ClickHouse, returning
`503 not ready` until dependencies are usable. Both are uncached plaintext
responses. The supplied container healthcheck invokes the server's restricted
loopback-only `healthcheck` subcommand against `/readyz`; use readiness, not
liveness, for rollout and traffic admission.

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
| `retained-search directory … is on nfs, which does not support atomic no-replace rename` | The state volume is a network or FUSE mount. Retained-search publication, backups, and restores depend on atomic no-replace rename, which the Linux NFS and SMB clients refuse, so the server stops instead of losing every search result. Bind the state directory to a local filesystem (ext4, xfs, btrfs). |
| `control database is on a network or FUSE filesystem; SQLite WAL mode is unsafe there` (logged at error level; the server keeps running) | The SQLite control plane is on NFS, SMB, or a FUSE bridge, where WAL mode can silently corrupt the database. Stop the server, move the state directory to a local filesystem, and start it again. |

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
singleton lock, retained-search artifacts, and export artifacts in named
volumes. ClickHouse data remains owned by the existing ClickHouse service. The
default Compose deployment does not configure backup or restore jobs; back up
both systems using your normal infrastructure procedures.

The state volume must be a local filesystem such as ext4, xfs, or btrfs.
Network and FUSE filesystems (NFS, SMB/CIFS, sshfs, and similar) are not
supported for the control database or for the retained-search and export
directories: SQLite WAL mode is unsafe without coherent shared memory and
reliable locks, and the retained-search store publishes every result with an
atomic no-replace rename that the Linux NFS and SMB clients refuse. The server
checks both at startup; it refuses to start when the retained-search directory
cannot publish, and logs an error when the control database is on a remote
mount.
