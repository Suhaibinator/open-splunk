# Collector configuration reference

`open-splunk-collector` is YAML-first. The YAML document describes the
collector-to-server connection, durable local state, file inputs, framing, and
the ordered processor chain. CLI flags select the document and process log
level; environment variables affect configuration only when the YAML document
explicitly references them.

Start with [`configs/examples/collector-container.yaml`](../configs/examples/collector-container.yaml)
for a remote, TLS-protected collector or
[`configs/examples/collector.yaml`](../configs/examples/collector.yaml) for
loopback development. Native-ingestion provisioning, WAL operation, backup,
and recovery are covered in [Native ingestion and collector operations](ingestion.md).

## Command-line interface

The executable accepts these commands:

| Command | Flags | Purpose |
|---|---|---|
| `run` (default) | `-config PATH`, `-log-level LEVEL` | Load the configuration and run until `SIGINT` or `SIGTERM`. The default configuration path is `/etc/open-splunk/collector.yaml`; log level is `debug`, `info`, `warn`, or `error`, defaulting to `info`. |
| `validate` | `-config PATH` | Parse, expand, default, and validate YAML, print a redacted summary, and count currently matched input files. It does not contact the server. |
| `identity` | `-config PATH` | Create or read the stable collector ID in `state.directory` and print it. It does not read the ingestion token or start inputs. |
| `version` | None | Print the compiled build identity. |

Successful commands exit 0. Configuration or runtime failures exit 1, and
invalid command-line usage exits 2.

There are no CLI flags for individual YAML fields and no CLI/YAML precedence
model. `-config` chooses the YAML document; `-log-level` is process-only and is
not a YAML field.

`validate` proves that the YAML and referenced environment values parse, local
configuration constraints hold, and the input globs can be evaluated in the
current mount namespace. It deliberately does **not**:

- read or validate the token file;
- read or parse `server.tls.ca_file`;
- test ownership or permissions of those files;
- confirm that `server.address` is a usable gRPC target beyond being nonempty;
- resolve or connect to `server.address`;
- perform a TLS handshake or authenticate to the server; or
- prove that a matched source file will remain readable by the runtime user.

Run it with the same configuration, environment, user, state directory, and
source mounts as the service, then verify a real connection by looking for
`collector stream ready` and a fresh server-side collector heartbeat.

The redacted summary currently shows whether TLS is enabled, the CA path, and
the server name, but omits `server.tls.allow_insecure_remote`. Inspect the
deployed YAML separately when auditing plaintext exceptions.

## Environment substitution

Before parsing YAML, the collector replaces every `${NAME}` with the exact
value of environment variable `NAME`. Referencing an undefined variable is an
error. A defined but empty value substitutes an empty string. Write `$$` for a
literal dollar sign.

Substitution is opt-in per YAML document. The collector does not scan for or
automatically interpret `OPEN_SPLUNK_COLLECTOR_*`, and environment values do
not override fields that lack a placeholder. The names used by the supplied
examples are a documented template convention, not a runtime environment
registry.

Substitution occurs before YAML parsing and does not add quoting or escaping:

```yaml
server:
  address: "${OPEN_SPLUNK_COLLECTOR_SERVER_ADDRESS}"
  tls:
    enabled: ${DEPLOYMENT_TLS_ENABLED}
```

Quote string placeholders as shown. Leave Boolean or numeric placeholders
unquoted when the YAML field requires that type. Treat environment values as
trusted configuration: YAML-significant quotes, line breaks, or structure in a
substituted value can make the document invalid or change its meaning.

The production example uses the following complete set of template variables:

| Example variable | YAML destination | Meaning |
|---|---|---|
| `OPEN_SPLUNK_COLLECTOR_SERVER_ADDRESS` | `server.address` | Collector gRPC target in `host:port` form. |
| `OPEN_SPLUNK_COLLECTOR_TOKEN_FILE` | `server.token_file` | Path to the native ingestion token file mounted in the collector. |
| `OPEN_SPLUNK_COLLECTOR_SERVER_TLS_CA_CERTIFICATE_FILE` | `server.tls.ca_file` | PEM trust bundle used to verify the server certificate. |
| `OPEN_SPLUNK_COLLECTOR_SERVER_TLS_SERVER_NAME` | `server.tls.server_name` | DNS name or IP SAN expected in the server certificate when it differs from the dial host. |
| `OPEN_SPLUNK_COLLECTOR_STATE_DIRECTORY` | `state.directory` | Persistent collector-owned directory containing identity, WAL, checkpoints, and dead letters. |
| `OPEN_SPLUNK_COLLECTOR_INPUT_GLOB` | `inputs[0].include[0]` | File glob as seen inside the collector process or container. |
| `OPEN_SPLUNK_COLLECTOR_INPUT_INDEX` | `inputs[0].index` | Existing Open Splunk index authorized by the ingestion token. |
| `OPEN_SPLUNK_COLLECTOR_INPUT_SOURCE` | `inputs[0].source` | Stable source metadata attached to each event. |
| `OPEN_SPLUNK_COLLECTOR_INPUT_SOURCETYPE` | `inputs[0].sourcetype` | Stable sourcetype metadata attached to each event. |
| `OPEN_SPLUNK_COLLECTOR_INPUT_HOST` | `inputs[0].host` | Host represented by the mounted logs, rather than the collector container hostname. |
| `OPEN_SPLUNK_COLLECTOR_INPUT_SERVICE` | `inputs[0].fields.service` | Canonical service metadata attached to each event. |
| `OPEN_SPLUNK_COLLECTOR_INPUT_ENVIRONMENT` | `inputs[0].fields.environment` | Example static deployment-environment field. |

You may rename, remove, or add placeholders in your own YAML. If you do, your
deployment environment file must use those exact names.

## YAML parsing and scalar syntax

The configuration must be one YAML document. Unknown fields and multiple YAML
documents are rejected. Field names and enum values are case-sensitive.

Byte sizes accept a bare byte count, `B`, decimal `KB`/`MB`/`GB`/`TB`/`PB`, or
binary `KiB`/`MiB`/`GiB`/`TiB`/`PiB`. Durations use Go duration syntax such as
`250ms`, `5s`, `10m`, or `1h30m`; unitless duration numbers are rejected.

## `server` reference

| Field | Type | Default | Requirements and behavior |
|---|---|---|---|
| `server.address` | string | Required | Collector gRPC dial target, intended to be `host:port`. |
| `server.transport` | string | `grpc` | Only `grpc` is supported. |
| `server.token_file` | path | Required | Native ingestion bearer token. It is read when establishing a connection so a replacement is observed after reconnect. The file must be nonempty and at most 4 KiB; trailing CR and LF characters are ignored. |
| `server.compression` | string | Empty | Empty disables gRPC compression; `gzip` is the only supported value. |
| `server.tls.enabled` | Boolean | `false` | Enables verified TLS. Remote deployments should set this to `true`. |
| `server.tls.ca_file` | path | Empty | PEM bundle replacing the system trust roots for this connection. Valid only when TLS is enabled. It is read when a connection is created. |
| `server.tls.server_name` | string | Dial host | Overrides certificate-name verification, TLS SNI, and gRPC authority. Use it when dialing by an IP or internal alias not present in the certificate. Valid only when TLS is enabled. |
| `server.tls.allow_insecure_remote` | Boolean | `false` | Allows plaintext to a non-loopback target and therefore sends the bearer token without transport encryption. This is an explicit escape hatch for a controlled local sidecar or service mesh, not a recommended private-LAN mode. |

The collector bounds and redacts token handling, but it does not enforce the
token file's owner, mode, regular-file status, or link count. Deployment must
provide that boundary: use an owner-only file, normally mode `0600`, and a
read-only secret mount. Apply the same read-only principle to the CA bundle and
configuration file.

TLS uses a minimum of TLS 1.2 and always verifies the server certificate. There
is no `skip_verify` option. The collector does not support a client certificate
or private key, and the Open Splunk collector listener does not request client
certificates, so native mutual TLS (mTLS) is not available. Use the native
ingestion token for application authentication, or place a controlled
TLS/mTLS-capable sidecar or service mesh between the collector and server.

The server independently requires its collector listener to use TLS unless
plaintext mode is explicitly enabled on a loopback listen address. Therefore
`allow_insecure_remote: true` alone cannot create a direct plaintext LAN
connection to an Open Splunk server. The server-side listener settings are
`OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_LISTEN_ADDRESS`,
`OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_PLAINTEXT_ENABLED`,
`OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_TLS_CERTIFICATE_FILE`, and
`OPEN_SPLUNK_SERVER_COLLECTOR_GRPC_TLS_PRIVATE_KEY_FILE`; see the
[server configuration reference](../deploy/README.md#server-configuration).

## `state` reference

| Field | Type | Default | Requirements and behavior |
|---|---|---|---|
| `state.directory` | path | Required | Persistent directory holding `collector_id`, the singleton lock, WAL, checkpoints, and dead letters. It must not be shared by collector processes. |
| `state.max_queue_bytes` | byte size | `1GiB` | Maximum durable WAL size. Zero also selects the default. A full queue applies backpressure to inputs instead of discarding source data. |
| `state.dead_letter_max_bytes` | byte size | `64MiB` | Rotates the active sensitive JSONL dead-letter file before the next record would cross the bound. Zero also selects the default; the value cannot exceed 9,223,372,036,854,775,807 bytes. |
| `state.dead_letter_max_backups` | integer | `4` | Number of rotated dead-letter files retained, from 0 through 64. Explicit zero discards the prior active file at rotation. |

The state directory and dead-letter output contain operationally sensitive
data. Use a dedicated owner-only directory beneath a trusted parent, preserve
it as one unit, and back it up only while the collector is stopped.

## `inputs` reference

At least one and at most 256 inputs may be configured. Input IDs must be unique
ASCII identifiers of 1 through 128 bytes: the first character must be
alphanumeric; later characters may also contain `.`, `_`, `:`, and `-`.

| Field | Type | Default | Requirements and behavior |
|---|---|---|---|
| `id` | string | Required | Stable input identity used in checkpoints and collector registration. Renaming it creates a distinct checkpoint scope. |
| `type` | string | `file` | Only `file` is supported. |
| `include` | list of globs | Required | One or more file globs. At most 256 patterns, each 1 through 16 KiB. Patterns use Go/filepath glob syntax. |
| `exclude` | list of globs | Empty | Removes include matches. At most 256 patterns, each 1 through 16 KiB. A pattern is checked against both the complete path and basename. |
| `format` | string | `ndjson` | `ndjson` parses one JSON object per framed event; `raw` retains the framed bytes as the event body. |
| `start_at` | string | `end` | `beginning` starts a newly discovered existing file at offset zero; `end` starts it at its current EOF. Durable checkpoints and pending WAL positions take precedence after the file is known. |
| `index` | string | Required | Canonical destination index. It must exist, be enabled, and be authorized by the token at ingestion time. |
| `source` | string | Input `id` | Source metadata, valid UTF-8 without control characters, at most 4,096 bytes. |
| `sourcetype` | string | Input `format` | Sourcetype metadata, valid UTF-8 without control characters, at most 255 bytes. |
| `host` | string | OS hostname | Host metadata, valid UTF-8 without control characters, at most 255 bytes. Set it explicitly for sidecars and containers. |
| `fields` | string map | Empty | Trusted static metadata attached to every event. `service` populates canonical service metadata; other keys become dynamic string fields. At most 1,024 fields. |
| `max_event_bytes` | byte size | `1MiB` | Maximum bytes in one framed event, excluding the final line delimiter. Zero selects the default; the hard maximum is 1 MiB. Oversized records are rejected rather than truncated into valid events. |
| `poll_interval` | duration | `250ms` | File discovery and tailing cadence. Zero selects the default; a nonzero value must be at least 10 ms. |
| `multiline` | mapping | Disabled | Enables multiline framing with the fields below. |

File paths are resolved in the collector process's filesystem namespace. In a
container, use the mounted destination path, not the host path. Prefer
rename/recreate rotation and keep rotated files matched and readable until
their checkpoints have advanced.

### `inputs[].multiline`

| Field | Type | Default | Requirements and behavior |
|---|---|---|---|
| `line_start_pattern` | regular expression | Required | A physical line matching this Go regular expression starts a new logical event; following nonmatching lines continue it. |
| `max_lines` | integer | `0` | Maximum physical lines in an event, from 0 through 1,048,576. Zero is unbounded until a new start line or `max_event_bytes`. |
| `flush_after` | duration | `5s` | Flushes an incomplete event after reader inactivity. A nonzero value must be at least 10 ms. |

Lines before the first matching start line form their own event. If a multiline
event exceeds `max_event_bytes`, its continuation is discarded until the next
matching start line.

## `processors` reference

Processors are optional, shared by every input, and run in listed order. At
most 256 may be configured. Field matching is exact and case-sensitive.

| Type | Required fields | Behavior |
|---|---|---|
| `allow` | `fields` | Keeps only the named top-level dynamic fields. |
| `deny` | `fields` | Removes the named top-level dynamic fields. |
| `rename` | `from`, `to` | Renames one top-level dynamic field. An absent source is a no-op; an existing destination is replaced. Canonical metadata destinations are forbidden. |
| `redact` | `fields`; optional `replacement` | Replaces the entire value of matching fields recursively through objects and lists. Empty or omitted `replacement` defaults to `[REDACTED]`. The collector also derives a pre-WAL sanitizer from the configured redactions so matching secrets do not cross the durable queue boundary. |

Each `fields` list must be nonempty and may contain at most 1,024 names. A name
must contain 1 through 256 valid UTF-8 bytes, have no surrounding whitespace or
control characters, and identifies a field name rather than a dotted path.
Only the fields meaningful to a processor type should be set.

Static input metadata is established independently of the dynamic processor
chain. The ingestion server reapplies its mandatory validation and redaction
policy before acknowledgment and storage.

## Fixed runtime behavior

The following values are implementation defaults and are not YAML, environment,
or CLI settings:

| Behavior | Fixed value |
|---|---|
| Batch event limit | 500 events |
| Batch encoded-event size | 1 MiB |
| Batch linger | 250 ms |
| Full-queue retry | 100 ms |
| Shutdown flush grace | 5 seconds |
| Post-cancellation drain window | 10 seconds |
| WAL segment maximum | 64 MiB |

When a batch reaches any batch threshold, it is appended and fsynced to the WAL
before delivery. These fixed values may change with a release; they are listed
so operators do not mistake them for undocumented configuration fields.
