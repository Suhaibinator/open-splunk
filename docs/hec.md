# HTTP Event Collector

Open Splunk exposes a bounded major-version-zero HTTP Event Collector (HEC)
adapter on the existing HTTPS/API listener. It does not open port 8088. This
document defines exact current release/source behavior and does not promise
cross-release compatibility.
HEC is disabled by default and is registered only as a complete JSON, raw,
acknowledgment, health, token-purpose, durable-staging, reconciliation,
recovery, metrics, and shutdown family.

> **Deployment warning:** HEC shares the server HTTP listener. The supplied
> Compose service publishes that listener from `0.0.0.0:8080` without direct
> TLS. If HEC is enabled unchanged, token-bearing HEC traffic is plaintext on
> every network that can reach the published host port. Bind the published port
> to `127.0.0.1` for host-local use, or put the complete listener behind a
> controlled TLS boundary before enabling HEC.

HEC uses the native ingestion authority described in [Ingestion](ingestion.md).
It cannot select a tenant, grant an index, write directly to ClickHouse, or
represent an HTTP client as a native collector. Public responses, logs,
metrics, and audit never expose credentials, authorization headers, channel
values, request/event bodies, token defaults, digests, or backend errors.

## Routes and response boundary

| Method | Path | Meaning |
| --- | --- | --- |
| `POST` | `/services/collector` | JSON event alias |
| `POST` | `/services/collector/event` | JSON events |
| `POST` | `/services/collector/raw` | LF-delimited raw events |
| `POST` | `/services/collector/ack` | channel-scoped acknowledgment query |
| `GET` | `/services/collector/health` | bounded HEC health |

`/services/collector` is the JSON-event alias. Paths are exact; no versioned
aliases are registered and a trailing slash is unsupported. Unknown collector
paths return bounded HEC JSON with HTTP 404/code 6 and never fall through to
the browser application. Wrong methods return HTTP 405/code 6 with
an exact `Allow` header; `HEAD` is not implicit.

Responses set `Content-Type: application/json; charset=utf-8`,
`X-Content-Type-Options: nosniff`, and `Cache-Control: no-store`. JSON member
order is fixed by the protocol fixtures. There is no BOM or trailing
newline.

JSON and ACK accept absent content type, `application/json`, or UTF-8 JSON.
JSON events additionally accept parameter-free
`application/x-www-form-urlencoded` but still decode the body as JSON. Raw
accepts absent type, UTF-8 `text/plain`, or `application/octet-stream`; raw
bytes must still be UTF-8 without NUL. Identity and one complete gzip member
are accepted. Repeated/list encodings, concatenated/truncated gzip, trailing
gzip bytes, other charsets/parameters/media, and independent compressed or
decompressed limit breaches fail.

## Authentication and tokens

Except for an unauthenticated shallow health probe, a request accepts exactly
one header in this case-sensitive grammar:

```text
Authorization: Splunk <plaintext-token>
```

Exactly one ASCII space separates scheme and credential. The credential is
bounded printable ASCII without whitespace/comma. Basic, Bearer,
administrator, repeated/comma-joined, and query-string authentication are
rejected. A `token` query parameter returns code 16 without credential lookup.

Only an active, unexpired, unrevoked HEC-purpose token with at least one active
ingestion-enabled allowed index may authenticate. Native tokens cannot call
HEC, and HEC tokens cannot open the native stream. Unknown, expired, revoked,
wrong-purpose, and unusable credentials are indistinguishable. Disabled tokens
use their fixed disabled response only after constant-shape lookup. Plaintext
is discarded before body decode, admission, logs, or metrics.

A HEC profile may define index, host, source, and sourcetype defaults. Index
scope, token constraints, and rate limits still apply. Index must come from the
envelope/request or token default; merely allowing one index does not make it a
default. Per-event precedence is:

| Metadata | Precedence | Final fallback |
| --- | --- | --- |
| index | envelope/request, token default | none; code 7 |
| host | envelope/request, token default | `hec` |
| source | envelope/request, token default | `http:hec` |
| sourcetype | envelope/request, token default, active index default | `httpevent` |
| time | envelope/request | one request receive instant |

Explicit empty/null values are invalid and do not fall through. Metadata is
bounded, valid UTF-8, control-free, not ASCII-edge-whitespace, and is not
trimmed, folded, or Unicode-normalized.

## JSON events

A JSON body is one or more whitespace-separated objects; no comma or array is
required:

```json
{"event":"one"}{"event":"two"}
```

Top-level arrays/scalars, empty bodies, BOM, invalid UTF-8/JSON, commas between
envelopes, trailing garbage, unknown envelope members, and duplicate decoded
member names are rejected. The complete request is decoded and validated before
admission; the lowest invalid zero-based event ordinal is reported and no valid
prefix commits.

Recognized members are `time`, `host`, `source`, `sourcetype`, `index`,
`event`, and `fields`. `event` is required and accepts nonempty String, object,
array, Boolean, or exact JSON number; null and empty String are blank. `_raw`
is the String bytes, deterministic compact JSON for object/array, Boolean text,
or exact authored number token. Only a String event also becomes `message`.
Nested member order and exact number lexemes are retained by deterministic
encoding; object members are not promoted to fields.

`fields` accepts unique names mapped to String, signed/unsigned 64-bit integer,
bounded exact decimal, Boolean, null, or flat arrays of those scalars. Objects,
nested arrays, reserved roots, invalid paths, overflow, and unsupported values
fail with code 15. Empty arrays/cells remain present values. There are at most
1,024 field members and 1,024 scalar array members per event.

`time` is an exact base-10 epoch-seconds number, optionally carried as a String.
It must represent an integral number of nanoseconds and fit the protobuf,
ClickHouse, event-age, and future-skew boundaries. When absent, every envelope
uses the request receive instant. HEC does not extract time from event text.

## Raw events

The fixed breaker is:

1. require UTF-8 without NUL;
2. split on LF;
3. remove exactly one CR immediately before LF;
4. skip empty segments; and
5. emit a final nonempty unterminated segment.

Each emitted line becomes both `_raw` and `message`. Raw accepts request-level
`time`, `host`, `source`, `sourcetype`, `index`, and `channel` exactly once.
Unsupported/duplicate/empty parameters fail; `fields`, timestamp extraction,
and arbitrary breakers are not supported. Raw always requires a channel.

## Channels and indexer acknowledgment

A channel is supplied once by `X-Splunk-Request-Channel` or `channel`, never
both. It is an exact case-sensitive canonical hyphenated GUID. It is scoped by
trusted tenant and token identity and is not a principal, selector, event
field, or metric label. A token retains at most 256 lifetime channels.

Indexer acknowledgment is immutable on token creation. JSON requires a channel
when ACK is enabled; a valid channel on an ACK-disabled JSON request is ignored.
Raw and `/ack` always require it.

Durable staging atomically creates a pending row and an opaque exact positive
JSON integer `ackId` in `1..2^53-1`:

```json
{"text":"Success","code":0,"ackId":42}
```

Only committed visibility for the same outbox changes `PENDING` to `INDEXED`.
Ambiguous storage remains pending until reconciliation; terminal failure stays
publicly false. IDs survive normal restart, but new allocation after restart or
restore uses a fresh cryptographic namespace. Clients must not infer sequence,
age, or adjacency.

`POST /services/collector/ack` accepts one object containing 1 through 1,000
unique positive integer IDs:

```json
{"acks":[1,2,3]}
```

It returns keys in request order. Indexed is true; pending, failed, unknown,
expired, cross-scope, and restored-away IDs are all false. Reads are
non-destructive: true remains true for the 24-hour terminal retention. Pending
rows are not expired to make capacity.

## Atomicity, durability, and retries

The request is normalized, authorized, policy-checked, and quota-checked as a
unit. One SQLite transaction charges all schedules and stages visibility,
immutable outbox payload, deterministic event IDs, and optional ACK, or it
creates none. HTTP code 0 means durable SQLite staging, not immediate
ClickHouse visibility.

Cancellation or response failure after commit does not undo admission. Reusing
the same body, token, or channel is a new at-least-once request and may create
duplicates. After receiving an ACK ID, poll it instead of resending solely
because visibility is pending. When the response outcome is unknown, retry is
valid at-least-once behavior and downstream consumers must tolerate a
duplicate.

Retry HTTP 429/503, network timeouts, and uncertain 500s with bounded
exponential backoff and jitter. Honor `Retry-After`; quota delay is 1 through
3,600 seconds. Do not retry deterministic 400/401/403/404/405/413/415/431
without changing request, route, or credential.

## Errors and limits

Base JSON is `text`, `code`, then optional `invalid-event-number` or `ackId`.
The stable code domain is:

| Code | HTTP | Meaning |
| ---: | ---: | --- |
| 0 | 200 | durable staging success |
| 1 | 403 | token disabled |
| 2–4 | 401/403 | missing/malformed authorization or unusable token |
| 5 | 400 | no event data |
| 6 | 400 | invalid format/policy; refined to 404/405/413/415/431 where applicable |
| 7 | 400 | missing/invalid/inactive/unauthorized index |
| 8 | 500 | internal failure |
| 9 | 503 | transient busy; quota uses 429 plus `Retry-After` |
| 10–11 | 400 | missing/invalid channel |
| 12–13 | 400 | missing/blank event |
| 14 | 400 | ACK disabled |
| 15 | 400 | invalid indexed fields |
| 16 | 400 | query-string authentication disabled |
| 17 | 200 | healthy |
| 18–20 | 503 | queue/ACK health unavailable |
| 21–22 | 400 | authenticated health token unusable/disabled |
| 23 | 503 | shutdown gate closed |
| 26 | 429 | durable HEC request capacity exhausted |
| 27 | 429 | channel/ACK capacity exhausted |

Codes 24 and 25 are reserved and not emitted. Authentication precedes semantic
body diagnostics. Public text never carries an internal error.

Principal hard ceilings are:

| Resource | Ceiling |
| --- | ---: |
| compressed/decompressed/normalized request | 8 MiB each |
| HEC-consumed header values, aggregate | 8 KiB |
| request target and query | 8 KiB |
| ACK request body | 64 KiB |
| events per request | 1,000 |
| normalized event | 1 MiB |
| JSON nesting / values / members | 16 / 16,384 / 4,096 |
| exact number token | 128 bytes; exponent magnitude 1,024 |
| concurrent requests per token / process | 16 / 128 |
| reserved concurrent health probes | 8 |
| pending outbox requests / payload | 64 / 256 MiB |
| retained requests per token | 100,000 |
| channels per token | 256 |
| ACK IDs per query / retained per token | 1,000 / 100,000 |
| terminal ACK retention | 24 hours |
| response | 1 MiB |

The event-age ceiling is 365 days and future skew is 5 minutes; an index may
tighten both. Native token/index schedules charge server-computed source event
bytes, so gzip never discounts quotas.

## Enablement and deployment

Enable the complete family with `-hec-enabled`. In the checked-in Compose
deployment, persist this before recreating the server:

```dotenv
OPEN_SPLUNK_SERVER_HEC_ENABLED=true
```

The HTTP server accepts plaintext whenever its TLS certificate and key are
absent; it does not restrict plaintext to loopback. The supplied Compose service
listens on `0.0.0.0:8080` and publishes `${OPEN_SPLUNK_DEPLOY_HTTP_PORT:-8080}`
on every host interface. For host-local use, change the port mapping to:

```yaml
ports:
  - "127.0.0.1:${OPEN_SPLUNK_DEPLOY_HTTP_PORT:-8080}:8080"
```

Remote HEC requires direct server HTTPS or a controlled TLS reverse proxy that
forwards `/services/collector` unchanged and preserves header/body limits and
timeouts. The browser Host/Origin policy is not a transport boundary for HEC.
Do not add plaintext 8088 or CORS.

Create an immutable HEC-purpose token in Administration, select allowed active
indexes, optional defaults/constraints/rates, and choose ACK mode. Store the
one-time token in a secret manager. Disable immediately to fence a suspected
credential; rotate/revoke after disclosure. Never put the token in a URL,
source file, ordinary log, trace, or metric.

Example request (using a protected curl config for the authorization header):

```sh
curl --silent --show-error --fail-with-body \
  --proto '=https' --tlsv1.2 --cacert "$OPEN_SPLUNK_HEC_CA" \
  --config "$OPEN_SPLUNK_HEC_CONFIG" \
  --request POST \
  --url "$OPEN_SPLUNK_HEC_URL/services/collector/event" \
  --header 'Content-Type: application/json' \
  --data-binary '{"index":"application","event":"hello"}'
```

`GET /services/collector/health` is shallow and unauthenticated. `ack=1` or
`ack=true` additionally checks the ACK store/reconciler. An ordinary auth
header optionally verifies one HEC token without exposing its details. General
process health remains `/healthz` and readiness `/readyz`.

## Backup and recovery

HEC has no standalone backup. Token profiles, channels, ACK rows, schedules,
visibility reservations, and outbox live in SQLite; searchable rows live in
ClickHouse. They are one coordinated recovery unit. The default Compose file
does not install backup or restore jobs; configure infrastructure-level backups
for both stores and never restore one side independently.

HEC persistence carries private migration and format counters. They detect
unrecognized state and are not public protocol or release versions. State from
another source revision may be rejected rather than upgraded or erased. After
restore, allow reconciliation to resolve retained pending requests. An ID
issued after the chosen snapshot is lost external state and polls false;
resending may duplicate an event that existed outside the restored authority.

## Load, soak, and slow-client gates

The always-on real TLS transport gate is:

```sh
go test ./internal/hechttp -run '^TestHandlerLive' -count=1 -v
go test -race ./internal/hechttp -run '^TestHandlerLive' -count=1
```

It covers HTTP/2 multiplexing, process/token concurrency, incomplete chunked
bodies, `100-continue` backpressure, mixed JSON/raw/gzip/ACK traffic, and
bounded heap/goroutine cleanup.

The durable shipped-process load gate exercises HEC, native collector traffic,
control-plane mutations, ClickHouse outage/backlog/reconciliation, ACK truth,
and exact event IDs:

```sh
OPEN_SPLUNK_HEC_LOAD=1 \
go test ./integration -run '^TestBackendHECDurableLoad$' \
  -count=1 -timeout=15m -v
```

Its default mixed profile offers 1,000 events/second for 30 seconds and checks
shape-specific acceptance, scheduler lag, bounded pending work, memory,
goroutines, threads, full drain, and duplicate-free event identity. The
`small-only` and `batch-only` profiles measure request-rate and batching
behavior separately; observational measurements are not product SLAs.

The production 30-second read-deadline gate holds all 16 per-token slots with
authenticated gzip/chunked TLS clients, proves bounded cleanup, then performs a
healthy ingest/ACK:

```sh
OPEN_SPLUNK_HEC_SLOW_CLIENT=1 \
go test ./integration \
  -run '^TestBackendHECSlowCompressedReadDeadline$' \
  -count=1 -timeout=15m -v
```

Long transport and shipped-process soaks require explicit operator approval, a
quiescent AC-powered no-sleep host, and a clean immutable revision. Run them
sequentially unless hosts are isolated:

```sh
OPEN_SPLUNK_HEC_SOAK=1 \
go test ./internal/hechttp \
  -run '^TestHandlerLiveTransportSoak$' -count=1 -timeout=25h -v

OPEN_SPLUNK_HEC_LOAD=1 \
OPEN_SPLUNK_HEC_LOAD_PROFILE=soak \
OPEN_SPLUNK_HEC_LOAD_DURATION=24h \
go test ./integration -run '^TestBackendHECDurableLoad$' \
  -count=1 -timeout=26h -v
```

Capture revision, machine sizing, exact environment, timestamps, classified
requests/events, queue and recovery high-water marks, and runtime resource
high-water marks. Never capture tokens, headers, channels, bodies, or fields.

## Intentional differences and exclusions

Open Splunk uses one listener; concatenated JSON objects; strict unknown and
duplicate rejection; typed bounded fields; request atomicity; fixed LF/CRLF
raw breaking; one gzip member; strict case-sensitive GUID channels; opaque
numeric ACK IDs; non-destructive 24-hour ACK truth; and independent at-least-
once requests.

Management/mint/s2s/metrics HEC routes, Basic/Bearer/query authentication,
top-level arrays, arbitrary `props.conf`/`transforms.conf`, multiline or regex
breakers, ingest-time transforms, binary raw events, timestamp extraction from
payloads, distributed ACK state, client idempotency, and exactly-once delivery
are unsupported.
