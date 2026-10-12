# Deployment and recovery

Open Splunk uses Google account sessions for browser authentication, GoCommon
organizations for membership, PostgreSQL for identity and operational metadata,
private MinIO for complete result captures and streamed exports, and ClickHouse
for ingested events. The supplied production Compose joins an existing
ClickHouse service and preserves its data and volumes.

## Requirements

- An existing ClickHouse native endpoint and account with schema migration and
  event read/write permissions for the `open_splunk` database.
- PostgreSQL and a private MinIO bucket with versioning. The supplied Compose
  adds independent `postgres-data` and `minio-data` volumes.
- A Google OAuth Web application client. Register
  `<OPEN_SPLUNK_SERVER_PUBLIC_BASE_URL>/auth/oauth/callback/` as an authorized
  redirect URI, including the trailing slash.
- An exact Open Splunk server image. Choose an exact approved MinIO server image
  from your distribution or registry for `OPEN_SPLUNK_DEPLOY_MINIO_IMAGE`.
- HTTPS ingress for remote browser access. The Compose HTTP listener is
  published on loopback; PostgreSQL and MinIO are not host-published.

## Server configuration

Flags override their environment counterparts. Sensitive values belong in a
private environment file or your deployment secret manager. Runtime startup
requires PostgreSQL, Google configuration and MinIO; there is no browser
administrator token or password sign-in route.

| Environment variable | CLI flag | Built-in default | Purpose | Accepted values and constraints |
| --- | --- | --- | --- | --- |
| `OPEN_SPLUNK_SERVER_TENANT_ID` | `-tenant-id` | `default` | Identify this deployment's physical metadata scope. | Non-empty UTF-8 identifier, at most 255 bytes, without surrounding whitespace or control characters. Preserve it across restarts and recovery; organization membership and index permissions are managed separately. |
| `OPEN_SPLUNK_SERVER_POSTGRES_DSN` | `-postgres-dsn` | required | PostgreSQL metadata and GoCommon identity database. | PostgreSQL connection string; use TLS for remote databases. |
| `OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_ID` | `-google-client-id` | required | Google OAuth Web application client. | Register the exact callback URI below. |
| `OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_SECRET` | `-google-client-secret` | required | Google OAuth client secret. | Keep outside source control. |
| `OPEN_SPLUNK_SERVER_PUBLIC_BASE_URL` | `-public-base-url` | required | Browser origin and OAuth callback base. | Absolute HTTPS URL, or HTTP localhost for development. |
| `OPEN_SPLUNK_SERVER_MINIO_ENDPOINT` | `-minio-endpoint` | required | Private MinIO service endpoint. | `host:port`; TLS is controlled separately. |
| `OPEN_SPLUNK_SERVER_MINIO_ACCESS_KEY` | `-minio-access-key` | required | Private object storage access key. | Account must access the artifact bucket. |
| `OPEN_SPLUNK_SERVER_MINIO_SECRET_KEY` | `-minio-secret-key` | required | Private object storage secret. | Keep outside source control. |
| `OPEN_SPLUNK_SERVER_MINIO_BUCKET` | `-minio-bucket` | required | Bulk captures and export artifacts. | Private bucket with versioning enabled. |
| `OPEN_SPLUNK_SERVER_MINIO_USE_TLS` | `-minio-use-tls` | true | TLS for the MinIO connection. | False only for the isolated local Compose network. |
| `OPEN_SPLUNK_SERVER_SEARCH_CAPTURE_MAXIMUM_ROWS` | `-search-capture-maximum-rows` | 100000 | Maximum rows in a complete immutable capture. | At most 10000000; exceeding a limit fails capture. |
| `OPEN_SPLUNK_SERVER_SEARCH_CAPTURE_MAXIMUM_BYTES` | `-search-capture-maximum-bytes` | 268435456 | Maximum bytes in a complete immutable capture. | At most 4294967296, and no larger than the organization budget. |
| `OPEN_SPLUNK_SERVER_SEARCH_RETENTION_ORGANIZATION_BYTES` | `-search-retention-organization-bytes` | 4294967296 | Aggregate retained-object budget per organization. | At least the capture byte limit, and no larger than the global budget. |
| `OPEN_SPLUNK_SERVER_SEARCH_RETENTION_GLOBAL_BYTES` | `-search-retention-global-bytes` | 17179869184 | Aggregate retained-object budget across organizations. | At least the organization budget. |
| `OPEN_SPLUNK_SERVER_VERIFY_EMBEDDED_RELEASE` | `-verify-embedded-release` | `false` | Verify the embedded release payload and exit without opening runtime storage or listeners. | Boolean. |
| `OPEN_SPLUNK_SERVER_LOG_LEVEL` | `-log-level` | `info` | Set the minimum server and router log level. | Case-insensitive `debug`, `info`, `warn`, or `error`. |
| `OPEN_SPLUNK_SERVER_LOG_FORMAT` | `-log-format` | `json` | Select the server and router log encoding written to standard error. | Case-insensitive `json` or `console`; `json` is recommended for deployed services. |
| `OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS` | `-http-listen-address` | `127.0.0.1:8080` | Select the browser API, UI, and HEC listen address. | `host:port`. A wildcard host such as `0.0.0.0` or `[::]` requires an explicit allowed-host list. |
| `OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS` | `-http-allowed-hosts` | Derived from the listen host | Admit browser/API `Host` values before origin and authentication checks. | Comma-separated DNS names or IP literals, without schemes, paths, or ports; at most 32 entries. Every browser-facing name must be listed. |
| `OPEN_SPLUNK_SERVER_HTTP_TLS_CERTIFICATE_FILE` | `-http-tls-certificate-file` | Empty; HTTP | Provide the PEM certificate chain for direct HTTPS. | File path. Must be supplied together with the HTTP private-key file and form a valid matching TLS identity. |
| `OPEN_SPLUNK_SERVER_HTTP_TLS_PRIVATE_KEY_FILE` | `-http-tls-private-key-file` | Empty; HTTP | Provide the PEM private key for direct HTTPS. | File path. Must be supplied together with the HTTP certificate file and match its certificate. |
| `OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO` | `-http-trust-x-forwarded-proto` | `false` | Allow a controlled plaintext reverse proxy to assert the browser-facing scheme. | Boolean. When enabled, plaintext requests may contain exactly one `X-Forwarded-Proto` value, exactly `http` or `https`; direct TLS remains authoritative. Enable only when clients cannot bypass the proxy. |
| `OPEN_SPLUNK_SERVER_LOCK_FILE` | `-server-lock-file` | `/tmp/open-splunk-server-open_splunk.server.lock` | Host guard for the fixed ClickHouse schema, alongside the PostgreSQL lease. | Private host-shared path; preserve its identity. |
| `OPEN_SPLUNK_SERVER_MASTER_KEY_FILE` | `-master-key-file` | `data/master.key` | Select the server master key bound to the PostgreSQL metadata. | File path. A missing key is generated as 32 bytes with mode `0600`; an existing key must contain exactly 32 bytes. Preserve it with the PostgreSQL metadata. |
| `OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY` | `-export-artifact-directory` | `data/export-cache` | Select the private directory for bounded temporary export staging. | Valid UTF-8 path without NUL bytes; must resolve to a dedicated non-root directory and must not escape through a leading `..`. |
| `OPEN_SPLUNK_SERVER_SEARCH_ARTIFACT_DIRECTORY` | `-search-artifact-directory` | `data/search-cache` | Select the private directory for bounded temporary search staging and cache. | Dedicated non-root directory owned by the server user; retained bulk objects reside in private MinIO. |
| `OPEN_SPLUNK_SERVER_ALERT_PUBLIC_BASE_URL` | `-alert-public-base-url` | disabled | Supply absolute links in webhook payloads and permit alert enabling. | Absolute `http://` or `https://` URL reachable by webhook receivers; HTTPS is recommended outside trusted local development. Alerts may be configured while absent but cannot be enabled. |
| `OPEN_SPLUNK_SERVER_ALERT_PRIVATE_WEBHOOK_HOSTS` | `-alert-private-webhook-hosts` | empty | Permit named webhook receivers whose validated address is private, loopback, or link-local. | Comma-separated exact DNS hostnames; public HTTPS destinations need no entry. |
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

For ClickHouse credentials, select either the raw value or
the file at one configuration tier. Supplying both forms at the same tier is
an error. An explicit CLI credential form overrides both environment forms.
File flags are safer than raw CLI flags because command-line arguments may be
visible in process listings and shell history. Sensitive environment values
are removed from the server process environment immediately after parsing.

This table applies to normal server startup. Recovery and provisioning
subcommands retain their purpose-specific interfaces, and collector YAML
remains the collector daemon's configuration source.

### Compose deployment variables

| Environment variable | Default | Purpose and constraints |
| --- | --- | --- |
| `OPEN_SPLUNK_DEPLOY_SERVER_IMAGE` | Required | Exact approved Open Splunk server image tag or digest used by production and recovery Compose. |
| `OPEN_SPLUNK_DEPLOY_HTTP_PORT` | `8080` | Host TCP port published on `127.0.0.1` for the server's container port `8080`; use an available port from `1` through `65535`. |
| `OPEN_SPLUNK_DEPLOY_CLICKHOUSE_NATIVE_PORT` | `9000` | Development Compose's loopback host TCP port for ClickHouse container port `9000`; use an available port from `1` through `65535`. Production joins an existing ClickHouse endpoint configured with `OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS`. |

`OPEN_SPLUNK_DEPLOY_SERVER_IMAGE` selects the server image. `OPEN_SPLUNK_DEPLOY_HTTP_PORT`
changes the loopback publication port. `OPEN_SPLUNK_POSTGRES_PASSWORD` initializes
the independent PostgreSQL account; Compose derives its internal server DSN.
`OPEN_SPLUNK_DEPLOY_POSTGRES_IMAGE` can select another exact compatible image.
`OPEN_SPLUNK_DEPLOY_MINIO_IMAGE` is required and must identify your approved
MinIO server image. Keep the access and secret keys aligned between the server
and MinIO service. `.env.example` lists these settings without credentials.

## Add Open Splunk to an existing Compose project

For an existing Compose project, paste this service block and its independent
volumes into the project. It joins the existing `per-obs-network`; retain the
existing ClickHouse service and volume declarations.

```yaml
services:
  server:
    image: "${OPEN_SPLUNK_DEPLOY_SERVER_IMAGE:?set OPEN_SPLUNK_DEPLOY_SERVER_IMAGE to an exact Open Splunk server image}"
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
      OPEN_SPLUNK_SERVER_LOG_LEVEL: "${OPEN_SPLUNK_SERVER_LOG_LEVEL:-info}"
      OPEN_SPLUNK_SERVER_LOG_FORMAT: "${OPEN_SPLUNK_SERVER_LOG_FORMAT:-json}"
      OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS: 0.0.0.0:8080
      OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS: "${OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS:-localhost,127.0.0.1}"
      OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO: "${OPEN_SPLUNK_SERVER_HTTP_TRUST_X_FORWARDED_PROTO:-false}"
      OPEN_SPLUNK_SERVER_MASTER_KEY_FILE: /var/lib/open-splunk/state/private/master.key
      OPEN_SPLUNK_SERVER_LOCK_FILE: /var/lib/open-splunk/lock/private/open-splunk-server-open_splunk.server.lock
      OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY: /var/lib/open-splunk/exports/private
      OPEN_SPLUNK_SERVER_ALERT_PUBLIC_BASE_URL: "${OPEN_SPLUNK_SERVER_ALERT_PUBLIC_BASE_URL:-}"
      OPEN_SPLUNK_SERVER_ALERT_PRIVATE_WEBHOOK_HOSTS: "${OPEN_SPLUNK_SERVER_ALERT_PRIVATE_WEBHOOK_HOSTS:-}"
      OPEN_SPLUNK_SERVER_POSTGRES_DSN: "postgres://open_splunk:${OPEN_SPLUNK_POSTGRES_PASSWORD:?set OPEN_SPLUNK_POSTGRES_PASSWORD}@postgres:5432/open_splunk?sslmode=disable"
      OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_ID: "${OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_ID:?set OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_ID}"
      OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_SECRET: "${OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_SECRET:?set OPEN_SPLUNK_SERVER_GOOGLE_CLIENT_SECRET}"
      OPEN_SPLUNK_SERVER_PUBLIC_BASE_URL: "${OPEN_SPLUNK_SERVER_PUBLIC_BASE_URL:?set the browser origin}"
      OPEN_SPLUNK_SERVER_MINIO_ENDPOINT: minio:9000
      OPEN_SPLUNK_SERVER_MINIO_ACCESS_KEY: "${OPEN_SPLUNK_SERVER_MINIO_ACCESS_KEY:-open-splunk}"
      OPEN_SPLUNK_SERVER_MINIO_SECRET_KEY: "${OPEN_SPLUNK_SERVER_MINIO_SECRET_KEY:?set OPEN_SPLUNK_SERVER_MINIO_SECRET_KEY}"
      OPEN_SPLUNK_SERVER_MINIO_BUCKET: "${OPEN_SPLUNK_SERVER_MINIO_BUCKET:-open-splunk-artifacts}"
      OPEN_SPLUNK_SERVER_MINIO_USE_TLS: "false"
      OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS: "${OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS:-per-clickhouse:9000}"
      OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME: "${OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME:-clickhouse}"
      OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD: "${OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD:?set OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD}"
      OPEN_SPLUNK_SERVER_HEC_ENABLED: "${OPEN_SPLUNK_SERVER_HEC_ENABLED:-false}"
    depends_on:
      postgres:
        condition: service_healthy
      minio:
        condition: service_started
    ports:
      # Google sessions authenticate browser APIs. Terminate HTTPS at ingress.
      - target: 8080
        published: "${OPEN_SPLUNK_DEPLOY_HTTP_PORT:-8080}"
        host_ip: 127.0.0.1
        protocol: tcp
    volumes:
      - server-state:/var/lib/open-splunk/state
      - server-lock:/var/lib/open-splunk/lock
      - server-exports:/var/lib/open-splunk/exports
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

  postgres:
    image: "${OPEN_SPLUNK_DEPLOY_POSTGRES_IMAGE:-postgres:18-alpine@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873}"
    restart: unless-stopped
    environment:
      POSTGRES_DB: open_splunk
      POSTGRES_USER: open_splunk
      POSTGRES_PASSWORD: "${OPEN_SPLUNK_POSTGRES_PASSWORD:?set OPEN_SPLUNK_POSTGRES_PASSWORD}"
    volumes:
      - postgres-data:/var/lib/postgresql
    networks:
      - per-obs-network
    healthcheck:
      test: [CMD-SHELL, "pg_isready -U open_splunk -d open_splunk"]
      interval: 5s
      timeout: 3s
      retries: 20

  minio:
    image: "${OPEN_SPLUNK_DEPLOY_MINIO_IMAGE:?set an exact approved MinIO server image}"
    restart: unless-stopped
    command: [server, /data]
    environment:
      MINIO_ROOT_USER: "${OPEN_SPLUNK_SERVER_MINIO_ACCESS_KEY:-open-splunk}"
      MINIO_ROOT_PASSWORD: "${OPEN_SPLUNK_SERVER_MINIO_SECRET_KEY:?set OPEN_SPLUNK_SERVER_MINIO_SECRET_KEY}"
    volumes:
      - minio-data:/data
    networks:
      - per-obs-network

volumes:
  postgres-data:
  minio-data:
  server-state:
  server-lock:
  server-exports:

```

### Connect to the existing ClickHouse service

Use the service name and its container-side native port, for example
`per-clickhouse:9000`. Do not use `localhost` from the server container or replace
an existing ClickHouse data volume. Open Splunk owns the separate `open_splunk`
database; another application database such as `logs` can remain in place.

### Configure browser access

Set the public base URL to the browser origin, register its exact Google callback
URI, and add the ingress hostname to `OPEN_SPLUNK_SERVER_HTTP_ALLOWED_HOSTS`.
Enable forwarded-protocol trust only behind a controlled HTTPS ingress that
replaces `X-Forwarded-Proto`. The scratch images include the builder's CA bundle
so Google HTTPS and trusted object-store endpoints can be verified. The server
image also includes PostgreSQL 18 `pg_dump` and `pg_restore` with their target
architecture library dependencies for native metadata backup and restore. It
does not require a runtime shell.

### Account and organization access

Any Google account can sign in. First sign-in creates a user with no
organization memberships and no index access. There is no first-user promotion
or administrator bootstrap account.

An operator with database access can deliberately assign user type 44 to an
already-registered account, for example:

```sql
UPDATE gocommon_users SET user_type = 44 WHERE email = 'operator@example.com';
```

Site administrators have access to all organizations and indexes. The
Organizations screen creates a root organization with an explicitly selected
existing owner. Parent Admins and Owners can create children. Every child member
must already belong to its parent or be admitted through GoCommon's configured
parent auto-add policy. Each organization retains an owner.

Organization roles are Viewer (1), Admin (23) and Owner (44). Index permissions
can grant or deny search, ingestion, management and grant editing to roles or
individual users. Deny wins over allow; user rules never create organization
membership. Select an organization before ordinary workspace operations.

### Local development

```sh
./deploy/generate-env.sh --development
# Edit deploy/.env.development: Google ID/secret and approved MinIO image.
make dev-clickhouse
make run
```

The environment generator creates independent ClickHouse, PostgreSQL and MinIO
credentials and refuses to overwrite an existing file. The development runner
reads assignments as data and never evaluates them as shell commands. Open
`http://localhost:8080/signin/`; configure that exact Google callback origin.
`make dev-down` stops the isolated development services without deleting volumes.

## Health and readiness

`/healthz` checks liveness and `/readyz` reports backend readiness. The server
fails startup when required identity/database/storage configuration is missing.
Use `open-splunk-server help configuration` for the executable flag reference.

## Transport and credential model

GoCommon owns Google authorization state, callback exchange and HttpOnly session
cookies. OpenSplunk sends organization context in `X-OpenSplunk-Organization-ID`
and `organization_id` on WebSockets/downloads. Backend handlers authorize the
current user and organization before reading or mutating resources. Machine
HEC/collector ingestion credentials remain separate from browser sessions.

Complete captures and export objects stay private. Capture row/byte limits and
organization/global retained-object budgets are enforced by the API and
advertised in bootstrap. Byte settings are unsigned integer counts; zero
resolves the native default. Capture progress remains distinct from the
bounded browser preview and becomes exportable only after completion. Native browser downloads
stream through the authenticated server rather than materializing an entire
artifact in JavaScript memory or handing out persistent public links. Session
and organization changes clear private browser state and restart subscriptions.

## Persistent state

Back up PostgreSQL identity and operational metadata, the private versioned
MinIO bucket, the server master key, and ClickHouse event data. Preserve the
PostgreSQL ownership lease and host guard identity across restarts. `server-state` holds keys and legacy local
staging; bulk runtime captures and exports are stored in MinIO. Never delete or
reinitialize the existing ClickHouse volume during this migration.

## Coordinated deployment recovery

Stop the server before taking a coordinated recovery set. Native commands back
up PostgreSQL identity and operational metadata, the referenced private MinIO
object versions, the master key and a native ClickHouse archive. Restore into
an empty PostgreSQL schema with the same schema identity and fresh ClickHouse
target volumes. Do not clear or replace the live ClickHouse data volume.

Provide `OPEN_SPLUNK_SERVER_POSTGRES_DSN` and the five
`OPEN_SPLUNK_SERVER_MINIO_*` settings through the operator's protected process
environment. Backup commands do not require Google credentials. DSN and object
secrets are consumed before starting PostgreSQL tools; avoid passing secrets
on the command line. Standalone binaries need PostgreSQL 18 `pg_dump` and
`pg_restore` on `PATH`; the server image includes them.

For metadata and referenced captured/export objects only:

```sh
open-splunk-server backup-control-plane \
  -master-key /var/lib/open-splunk/state/private/master.key \
  -destination /var/lib/open-splunk/recovery/private/control-plane

open-splunk-server verify-control-plane-backup \
  -source /var/lib/open-splunk/recovery/private/control-plane

# Change the protected DSN/object environment to the empty restore target first.
open-splunk-server restore-control-plane \
  -source /var/lib/open-splunk/recovery/private/control-plane \
  -master-key /var/lib/open-splunk/state/private/master.key
```

This control-plane bundle does not contain ClickHouse events. To create and
restore a complete coordinated deployment recovery set, use the TLS
ClickHouse backup/restore accounts and the archive volume:

```sh
open-splunk-server backup-deployment-recovery-set \
  -master-key /var/lib/open-splunk/state/private/master.key \
  -destination /var/lib/open-splunk/recovery/private/deployment \
  -archive-root /var/lib/open-splunk-clickhouse-backups \
  -address clickhouse:9440 -server-name clickhouse \
  -ca-cert /run/recovery/ca.crt \
  -password-file /run/recovery/backup.password

open-splunk-server verify-deployment-recovery-set \
  -source /var/lib/open-splunk/recovery/private/deployment \
  -archive-root /var/lib/open-splunk-clickhouse-backups

# Use protected restore-target environment, empty PostgreSQL and fresh CH volumes.
open-splunk-server restore-deployment-recovery-set \
  -source /var/lib/open-splunk/recovery/private/deployment \
  -archive-root /var/lib/open-splunk-clickhouse-backups \
  -master-key /var/lib/open-splunk/state/private/master.key \
  -address clickhouse:9440 -server-name clickhouse \
  -ca-cert /run/recovery/ca.crt \
  -password-file /run/recovery/restore.password
```

These paths match the recovery Compose mounts. Run its command with
`docker compose -f deploy/docker-compose.recovery.yaml run --rm recovery`
in place of `open-splunk-server` when using that topology. Destination
directories must be new and private; restore master-key paths must be absent
on the fresh target or match the resumable restore receipt. Native restore verifies object content, rebinds stored version
references and records a generation-bound receipt so an interrupted restore
can resume. It preserves the ClickHouse source UUID and visibility high-water
binding. Verify startup and authorized search/export access before switching
traffic.

The recovery Compose topology retains TLS ClickHouse account/certificate mounts
and its existing recovery/lock volumes. `prepare-config.py` prepares ClickHouse
credentials and certificates only; it does not create a browser administrator
credential. The restore overlay selects fresh state and ClickHouse targets.

### Run the disposable drill

Use the [integration guide](../integration/README.md) for executable recovery and
storage tests. Legacy file-based recovery receipts are not a substitute for
PostgreSQL and MinIO backups.
