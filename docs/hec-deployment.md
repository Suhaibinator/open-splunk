# Deploy and operate HTTP Event Collector v0.1

Open Splunk can expose its bounded HTTP Event Collector (HEC) compatibility
surface on the existing browser/API listener. HEC is disabled by default. It
does not open Splunk's conventional port 8088, create another listener, or
weaken the listener's TLS policy.

This runbook applies to a server build that advertises
`SERVER_FEATURE_HEC_INGESTION`. The normative wire behavior is in the
[HEC v0.1 compatibility contract](hec-compatibility-v0.1.md). The contract
wins if this operator guide and the protocol contract ever disagree.

## Enable the complete route set

The server registers JSON, raw, acknowledgment, and health routes as one
capability. There is no supported partial mode.

For a directly launched server, add the Boolean flag:

```sh
open-splunk-server \
  -hec-enabled \
  -http-address 127.0.0.1:8080 \
  -http-tls-cert /path/to/server.crt \
  -http-tls-key /path/to/server.key
```

Production deployments must use HTTPS. Plain HTTP is accepted only by the
existing explicit loopback development mode and must not be published to a
network.

The checked-in Compose deployment passes
`OPEN_SPLUNK_HEC_ENABLED` to `-hec-enabled` and defaults it to `false`. To keep
HEC enabled across later `docker compose up` operations, add this line to the
generated `deploy/.env` before creating or recreating the server container:

```dotenv
OPEN_SPLUNK_HEC_ENABLED=true
```

Then recreate the long-running server:

```sh
cd deploy
docker compose up --detach --wait --no-build server
```

The server log includes `HEC v0.1 enabled on the existing TLS listener` after
a successful enabled startup. Changing the setting requires a server recreate;
it is not a live toggle. Set it back to `false` and recreate the server to
disable every HEC route.

## URL and TLS boundary

HEC uses the same origin and certificate as the browser and protobuf HTTP API.
For the default Compose deployment:

```sh
export OPEN_SPLUNK_HEC_URL='https://localhost:8080'
export OPEN_SPLUNK_HEC_CA='./.env.tls/ca.crt'
```

If `OPEN_SPLUNK_SERVER_HTTP_PORT` changes the host port, change the URL to
match. The generated certificate covers `localhost` and `127.0.0.1`; the URL
hostname still has to match a certificate subject alternative name. Never use
`curl -k` or disable certificate verification to work around a name, trust, or
expiry error.

The production Compose port remains bound to `127.0.0.1`. Publishing HEC to a
remote producer therefore requires an explicit, security-reviewed change to
the existing HTTPS exposure or a trusted TLS reverse proxy. Forward the
`/services/collector` namespace unchanged, keep request bodies and
`Authorization` headers private, and preserve the documented body/header
limits and timeouts. Do not add a plaintext 8088 mapping. HEC is not CORS
enabled and is not intended to be called by browser JavaScript.

The supported routes are:

| Method | Path |
| --- | --- |
| `POST` | `/services/collector` |
| `POST` | `/services/collector/event` |
| `POST` | `/services/collector/event/1.0` |
| `POST` | `/services/collector/raw` |
| `POST` | `/services/collector/raw/1.0` |
| `POST` | `/services/collector/ack` |
| `GET` | `/services/collector/health` |
| `GET` | `/services/collector/health/1.0` |

Paths are exact. In particular, a trailing slash is unsupported.

## Create a HEC token in Administration

HEC credentials are purpose-scoped ingestion tokens. An administrator bearer
token is not a HEC credential, and a native collector token cannot call HEC.

1. Open `/signin/` on the server origin and start an administrator session
   with the administrator bearer token provisioned for the deployment.
2. Open **Administration**, then **Ingestion tokens**, and choose
   **Generate token**.
3. Set **Purpose** to **HTTP Event Collector (HEC)**. Purpose is immutable.
4. Select at least one active, ingestion-enabled allowed index. A request can
   never escape this set.
5. Optionally set a default index, host, source, and sourcetype. If no default
   index is set, every JSON envelope or raw request must name an index; merely
   allowing one index does not make it a default.
6. Enable **Indexer acknowledgment** only if the producer will send a stable
   GUID channel and poll acknowledgment IDs. This choice is immutable; rotate
   the credential to change it.
7. Configure expiry, host/source constraints, and rate limits as needed, then
   generate the token.
8. Store the plaintext credential in the producer's secret manager when it is
   shown. It is displayed once and cannot be recovered from Open Splunk.

The token name, defaults, safe prefix, state, last-use time, and allowed
indexes remain visible in Administration. **Disable** immediately fences a
live token while preserving the option to **Enable** it again; both operations
use the current displayed version, and an expired or revoked token cannot be
re-enabled. Revoke and replace the token after suspected disclosure. Do not
put the plaintext token in a URL, source file, checked-in configuration,
ordinary log, or monitoring label.

## Safe curl setup

The examples below put the credential in a temporary owner-readable curl
configuration, not in shell history or curl's process arguments. Read the
one-time token without echo and do not run these commands under shell tracing:

```sh
read -r -s -p 'HEC token: ' OPEN_SPLUNK_HEC_TOKEN
printf '\n'
umask 077
OPEN_SPLUNK_HEC_CONFIG="$(mktemp "${TMPDIR:-/tmp}/open-splunk-hec.XXXXXX")"
chmod 600 "$OPEN_SPLUNK_HEC_CONFIG"
printf 'header = "Authorization: Splunk %s"\n' "$OPEN_SPLUNK_HEC_TOKEN" >"$OPEN_SPLUNK_HEC_CONFIG"
unset OPEN_SPLUNK_HEC_TOKEN
trap 'rm -f "$OPEN_SPLUNK_HEC_CONFIG"' EXIT HUP INT TERM
export OPEN_SPLUNK_HEC_URL='https://localhost:8080'
export OPEN_SPLUNK_HEC_CA='./.env.tls/ca.crt'
export OPEN_SPLUNK_HEC_INDEX='application'
```

Environment variables are a concise interactive example, not a production
secret store. A long-running producer should read its token from an
owner-readable secret file or secret manager and must keep it out of process
logs and diagnostics. Do not enable verbose curl tracing: it prints request
headers, including the credential.

Every authenticated request uses the exact, case-sensitive grammar
`Authorization: Splunk <token>`. Basic, Bearer, and query-string token
authentication are rejected.

### Send one JSON event

This example names the index explicitly, so it works whether or not the token
has a default index:

```sh
curl --silent --show-error --fail-with-body \
  --proto '=https' --tlsv1.2 \
  --cacert "$OPEN_SPLUNK_HEC_CA" \
  --connect-timeout 5 --max-time 30 \
  --config "$OPEN_SPLUNK_HEC_CONFIG" \
  --request POST \
  --url "$OPEN_SPLUNK_HEC_URL/services/collector/event" \
  --header 'Content-Type: application/json' \
  --data-binary @- <<JSON
{"index":"${OPEN_SPLUNK_HEC_INDEX:?}","event":"hello from Open Splunk","fields":{"environment":"production","attempt":1}}
JSON
```

An acknowledgment-disabled token returns:

```json
{"text":"Success","code":0}
```

The JSON endpoint also accepts multiple whitespace-separated envelope objects.
It does not accept a top-level array. The complete request is atomic: one bad
envelope rejects every envelope in that request.

### Send raw LF-delimited events

Raw ingestion always requires a channel, even when indexer acknowledgment is
disabled. Generate one GUID once per producer/channel and persist its exact
text; channel identity is case-sensitive:

```sh
export OPEN_SPLUNK_HEC_CHANNEL='01234567-89ab-cdef-0123-456789abcdef'

curl --silent --show-error --fail-with-body \
  --proto '=https' --tlsv1.2 \
  --cacert "$OPEN_SPLUNK_HEC_CA" \
  --connect-timeout 5 --max-time 30 \
  --request POST \
  --url "$OPEN_SPLUNK_HEC_URL/services/collector/raw?index=${OPEN_SPLUNK_HEC_INDEX:?}" \
  --config "$OPEN_SPLUNK_HEC_CONFIG" \
  --header "X-Splunk-Request-Channel: ${OPEN_SPLUNK_HEC_CHANNEL:?}" \
  --header 'Content-Type: text/plain; charset=utf-8' \
  --data-binary @- <<'LOGS'
first raw event
second raw event
LOGS
```

The fixed v0.1 breaker splits on LF, removes one CR immediately before LF,
skips empty segments, and emits a final nonempty unterminated segment. It does
not apply `props.conf`, multiline, regex, or timestamp extraction rules.

### Send and poll with indexer acknowledgment

For a token created with acknowledgment enabled, JSON ingestion also requires
the channel header:

```sh
curl --silent --show-error --fail-with-body \
  --proto '=https' --tlsv1.2 \
  --cacert "$OPEN_SPLUNK_HEC_CA" \
  --connect-timeout 5 --max-time 30 \
  --request POST \
  --url "$OPEN_SPLUNK_HEC_URL/services/collector/event" \
  --config "$OPEN_SPLUNK_HEC_CONFIG" \
  --header "X-Splunk-Request-Channel: ${OPEN_SPLUNK_HEC_CHANNEL:?}" \
  --header 'Content-Type: application/json' \
  --data-binary @- <<JSON
{"index":"${OPEN_SPLUNK_HEC_INDEX:?}","event":"acknowledged event"}
JSON
```

The success response contains a numeric, channel-scoped ID:

```json
{"text":"Success","code":0,"ackId":42}
```

Set the value returned by that request, without quotes, and query it on the
same token and exact channel:

```sh
export OPEN_SPLUNK_HEC_ACK_ID='42'

curl --silent --show-error --fail-with-body \
  --proto '=https' --tlsv1.2 \
  --cacert "$OPEN_SPLUNK_HEC_CA" \
  --connect-timeout 5 --max-time 30 \
  --request POST \
  --url "$OPEN_SPLUNK_HEC_URL/services/collector/ack" \
  --config "$OPEN_SPLUNK_HEC_CONFIG" \
  --header "X-Splunk-Request-Channel: ${OPEN_SPLUNK_HEC_CHANNEL:?}" \
  --header 'Content-Type: application/json' \
  --data-binary @- <<JSON
{"acks":[${OPEN_SPLUNK_HEC_ACK_ID:?}]}
JSON
```

`{"acks":{"42":true}}` proves that the staged request reached committed
ClickHouse visibility. `false` means pending, failed, unknown, expired, or
outside the token/channel scope; it does not reveal which. Querying is
non-destructive, so `true` remains true until the terminal row expires.
The setup unsets `OPEN_SPLUNK_HEC_TOKEN` immediately. Remove the temporary
configuration early with `rm -f "$OPEN_SPLUNK_HEC_CONFIG"` if testing ends
before the shell exits; the trap removes it on normal exit and common signals.

## Health probes

HEC health is separate from process liveness (`/healthz`) and general runtime
readiness (`/readyz`). It performs no write and returns only a bounded HEC
state, never queue depths or identities.

The shallow probe is unauthenticated:

```sh
curl --silent --show-error --fail-with-body \
  --proto '=https' --tlsv1.2 \
  --cacert "$OPEN_SPLUNK_HEC_CA" \
  --url "$OPEN_SPLUNK_HEC_URL/services/collector/health"
```

HTTP 200 with `{"text":"HEC is healthy","code":17}` means new HEC work can
be admitted and its durable queue is below the fail-closed capacity. To include
the acknowledgment store and reconciler in the projection:

```sh
curl --silent --show-error --fail-with-body \
  --proto '=https' --tlsv1.2 \
  --cacert "$OPEN_SPLUNK_HEC_CA" \
  --url "$OPEN_SPLUNK_HEC_URL/services/collector/health?ack=1"
```

Codes 18 through 20 use HTTP 503 for queue, acknowledgment, or combined
unavailability. Supplying the ordinary `Authorization` header additionally
checks that the credential is a usable HEC token; it does not disclose token
or index details.

## Durability, retries, and duplicates

HEC delivery is at least once:

- code 0 means the entire request is durably staged in SQLite; it does not mean
  the events are already searchable in ClickHouse;
- an ACK value of `true` proves committed search visibility for that staged
  request;
- a timeout, canceled connection, or failed response write can happen after
  staging committed, so retrying an uncertain request can create duplicates;
- resending the same body, token, or channel is a new request; HEC v0.1 has no
  client idempotency key and does not promise global exactly-once delivery;
- after receiving an `ackId`, poll `/ack` rather than resending merely because
  the value is still `false`; and
- if the client never received an `ackId`, it cannot distinguish a rolled-back
  request from a committed response loss. Retrying is valid at-least-once
  behavior, and downstream processing must tolerate a duplicate.

Retry HTTP 429, 503, network timeouts, and uncertain HTTP 500 results with
bounded exponential backoff and jitter. Honor `Retry-After` when it is present;
quota responses cap it at 3,600 seconds. Do not blindly retry deterministic
400/401/403/404/405/413/415/431 responses without changing the request or
credential. Keep producer concurrency within the limits below.

## Principal limits

The server may tighten these values before startup but cannot raise them above
the v0.1 contract ceilings:

| Resource | Ceiling/default |
| --- | ---: |
| Compressed body | 8 MiB |
| Decompressed body | 8 MiB |
| Normalized request | 8 MiB |
| Events per request | 1,000 |
| One normalized event | 1 MiB |
| Concurrent requests per token | 16 |
| Concurrent ingestion/ACK HEC requests per process | 128 |
| Concurrent HEC health requests per process | 8, reserved independently |
| Durable pending HEC requests | 64 |
| Durable pending HEC payload | 256 MiB |
| Distinct channels per token lifetime | 256 |
| ACK IDs in one query | 1,000 |
| Retained ACK rows per token | 100,000 |
| Retained request-ledger rows per token | 100,000 |
| Terminal ACK retention | 24 hours |

Per-token and per-index event/byte rate schedules also apply. Byte charge is
based on the server-computed source protobuf size before normalization or
redaction, so gzip does not reduce quota use. See
[the complete resource table](hec-compatibility-v0.1.md#resource-limits-and-backpressure)
for field, nesting, header, target, number, and durable metadata bounds.
Use the [HEC load and soak runbook](hec-load-and-soak.md) for pre-release live
transport, durable throughput, backlog-recovery, and long-soak evidence.

## Backup, restore, and recovery

HEC adds no independent backup artifact. Token profiles, channel allocation
ordinals, acknowledgment rows, quota schedules, visibility reservations, and
staged outbox payloads live in the existing SQLite control-plane recovery
state. Searchable events live in ClickHouse. Those stores remain one
coordinated recovery unit.

Follow the
[coordinated deployment recovery runbook](../deploy/README.md#coordinated-deployment-recovery):

- stop the server so no native or HEC admission can mutate the snapshot;
- back up and restore the coordinated SQLite/master-key/administrator-token
  and ClickHouse recovery set, never one side independently;
- retain the HEC plaintext credential in the producer's secret store because
  Open Splunk stores no recoverable plaintext copy; and
- after restart or restore, allow reconciliation to resolve pending staged
  requests before deciding to resend them.

Pending ACKs remain false until reconciliation proves visibility. Indexed ACKs
remain true only while their restored terminal-retention row exists. A client
must treat any ACK ID issued after the selected recovery-set snapshot as lost
external state; polling it returns false unless the discarded and restored
cryptographic ID namespaces suffer a finite-space collision. IDs are opaque,
so clients must never infer order or compute a successor. Resending may
duplicate an event that existed outside the restored point-in-time authority.

## Troubleshooting

| Symptom | Checks and action |
| --- | --- |
| 404 or a non-HEC response on a HEC path | Confirm `-hec-enabled` was active at server startup, use an exact supported path with no trailing slash, and use the documented method. |
| TLS trust/name failure | Use the deployment CA with a hostname present in the certificate. Check certificate expiry and system time. Never use `-k`. |
| 401/code 2 or 3 | Supply exactly one `Authorization: Splunk <token>` header. The scheme is case-sensitive and accepts exactly one space. |
| 403/code 1 or 4 | In Administration, check token state, expiry, immutable HEC purpose, allowed indexes, and whether at least one allowed index remains active and ingestion-enabled. Rotate a revoked or exposed credential. |
| 400/code 7 | Supply a canonical index in the request or configure a token default, and keep it in the token's allowed active indexes. Allowed scope alone is not a default. |
| 400/code 10 or 11 | Supply one canonical hyphenated GUID channel where required. Do not send both the header and query parameter; preserve exact case across ACK operations. |
| 400/code 14 | The token was created without indexer acknowledgment. Create and safely rotate to a new ACK-enabled token. |
| 400/code 16 | Remove the `token` query parameter. Query-string authentication is always disabled. |
| 413 or 415/code 6 | Check compressed/decompressed/event sizes, UTF-8, media type, charset, and that gzip contains exactly one complete member. |
| 429/code 9 | Durable token/index quota is active. Honor `Retry-After`, reduce rate or concurrency, and inspect the configured token/index schedules. |
| 429/code 26 or 27 | The durable HEC queue, per-token retained request ledger, or token channel/ACK retention capacity is exhausted. Stop retries from amplifying pressure. Resolve staging/reconciliation pressure, wait for 24-hour terminal cleanup, or rotate a token that exhausted its 256 lifetime channels. |
| 503/code 9 or health codes 18–20 | Admission, durable staging, or requested ACK health is unavailable. Back off, inspect server/ClickHouse readiness, storage pressure, and reconciliation signals. |
| 503/code 23 | Graceful shutdown has closed new admission. Retry against the restarted/healthy instance; an uncertain prior request may already be staged. |
| ACK remains false | Continue bounded polling. False also covers unknown, expired, wrong-scope, and terminal-failure IDs. Check `health?ack=1` and operator reconciliation signals; do not resend solely because indexing is not immediate. |
| Raw event count is unexpected | Raw splits only on LF, removes one preceding CR, and skips empty segments. It has no multiline or sourcetype-specific breaker. |

Public responses deliberately omit internal errors, queue depths, token names,
channels, and event contents. Diagnose persistent capacity or reconciliation
problems from protected server operational signals, not by increasing request
limits or logging credentials and payloads.

## Unsupported Splunk surfaces and deliberate differences

Only the eight exact routes listed above are HEC v0.1. Common Splunk surfaces
that are not implemented include `/services/collector/mint`,
`/services/collector/s2s`, `/services/collector/ack/1.0`, metrics-specific
collector routes, and management/token paths such as
`/services/data/inputs/http` and `/servicesNS/.../data/inputs/http/...`.
Unknown collector paths, trailing-slash variants, and implicit `HEAD` requests
fail rather than falling through to the browser application.

Other intentional differences include:

- no dedicated 8088 listener and no HEC token-management compatibility API;
- no Basic, Bearer, administrator, or query-string ingestion authentication;
- concatenated JSON objects instead of a top-level batch array;
- strict rejection of unknown or duplicate envelope members;
- bounded typed `fields` values rather than string-only coercion;
- no event-text timestamp extraction;
- a fixed raw LF/CRLF breaker with no `props.conf` or `transforms.conf`;
- exactly one gzip member;
- strict GUID channels with case-sensitive identity;
- opaque numeric `ackId` values in `1..2^53-1`, scoped to token plus channel;
- non-destructive true ACK reads retained for 24 hours;
- request-atomic staging with no valid-prefix commit; and
- at-least-once independent requests, with no client idempotency or
  exactly-once guarantee.

The [normative exclusions list](hec-compatibility-v0.1.md#intentional-deviations-and-exclusions)
defines the full compatibility boundary.
