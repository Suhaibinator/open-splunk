# Open Splunk HTTP Event Collector compatibility contract v0.1

**Status:** normative implemented release-candidate contract; HEC remains
default-off and must not be enabled in a release until the complete release
gate passes

**Compatibility version:** `0.1`

**Primary compatibility target:** Splunk Enterprise 10.2 HTTP Event Collector

**Last updated:** August 10, 2026

This document fixes the externally observable HTTP Event Collector (HEC)
contract for Open Splunk v0.1. It is intentionally narrower than Splunk
Enterprise. A behavior not explicitly accepted here is unsupported and must
fail closed; an implementation must not infer support from a similar Splunk
route or from a successfully parsed prefix.

The words **must**, **must not**, **should**, and **may** are normative. Byte
counts are counts of octets. Event ordinals and `invalid-event-number` values
are zero-based. The shared fixtures under
`internal/hec/testdata/compatibility/` are executable examples of this
contract. If prose and a fixture disagree, the prose is authoritative until
both are corrected in the same change.

The public Splunk documentation establishes the common envelope, endpoint,
authentication, channel, acknowledgment, and response taxonomy. No licensed
Splunk runtime was used as a differential oracle for the ambiguous cases
below. Those cases are consequently identified as deliberate Open Splunk
choices, not claims about undocumented Splunk behavior.

## Release and security boundary

HEC is an HTTP adapter over the same durable admission, quota, redaction,
outbox, ClickHouse visibility, and index-policy authority as native ingestion.
The HTTP handler cannot select a tenant, grant an index, write directly to
ClickHouse, or represent a HEC client as a native collector.

The feature must be registered and advertised only when JSON, raw,
acknowledgment, health, token-purpose enforcement, durable staging,
reconciliation, recovery, metrics, and graceful shutdown are all configured.
The public capability waits through Phase HEC-5's administrator UI, migration,
backup/restore, security, load, and soak gates; completion of HEC-4 alone is
not enough. Partial route registration is forbidden. HTTPS is required except
for the repository's explicit loopback development mode. HEC routes are not
CORS enabled and never fall through to the browser application.

Every ordinary response is bounded JSON. Logs, metrics, audit records, and
public errors must not include credentials, authorization headers, query
strings, channel values, request bodies, raw events, fields, token defaults,
token digests, or credential-derived hashes.

## HTTP surface

The v0.1 routes are mounted on the existing Open Splunk HTTPS listener. There
is no dedicated port 8088 listener.

| Method | Path | Meaning |
| --- | --- | --- |
| `POST` | `/services/collector` | JSON event endpoint alias |
| `POST` | `/services/collector/event` | JSON event endpoint |
| `POST` | `/services/collector/event/1.0` | versioned JSON alias |
| `POST` | `/services/collector/raw` | fixed-breaker raw endpoint |
| `POST` | `/services/collector/raw/1.0` | versioned raw alias |
| `POST` | `/services/collector/ack` | channel-scoped acknowledgment query |
| `GET` | `/services/collector/health` | bounded service health |
| `GET` | `/services/collector/health/1.0` | versioned health alias |

Aliases are behaviorally identical. They do not select a different parser or
protocol version. A trailing slash is a different, unsupported path. Unknown
`/services/collector/...` paths return HTTP 404 with the code 6 response.
Known paths with the wrong method return HTTP 405 with code 6 and an exact
`Allow: POST` or `Allow: GET` header. `HEAD` is not an implicit health method.

Every response sets:

```text
Content-Type: application/json; charset=utf-8
X-Content-Type-Options: nosniff
Cache-Control: no-store
```

The server sends a correct `Content-Length` when the HTTP stack permits it.
Response JSON has no byte-order mark or trailing newline. Object member order
is fixed below because compatibility tests compare response bytes.

### Request media types and encodings

The JSON and acknowledgment endpoints accept an absent `Content-Type`,
`application/json`, or `application/json; charset=utf-8`. The JSON event
endpoint also accepts `application/x-www-form-urlencoded` with no parameters
because documented Splunk `curl -d` examples generate that media type; the
body is still decoded as HEC JSON and is never form-decoded.

The raw endpoint accepts an absent `Content-Type`, `text/plain`,
`text/plain; charset=utf-8`, or `application/octet-stream`. Despite the last
media type, the v0.1 raw body must be valid UTF-8 and must contain no NUL.
Media types and parameter names are ASCII case-insensitive. Any charset other
than UTF-8, any additional parameter, and every other media type returns HTTP
415 with code 6.

An absent `Content-Encoding` means identity. The only accepted nonidentity
value is one ASCII case-insensitive `gzip` token. Lists, repeated headers,
parameters, concatenated gzip members, truncated streams, and any bytes after
the one gzip member return code 6. Both compressed and decompressed streams
are independently hard-limited. Compression never discounts quota charge or
increases another limit.

Transfer framing is owned by the HTTP server. A malformed transfer encoding,
conflicting content lengths, a negative content length, or an HTTP-stack
framing rejection creates no HEC state. When the request reaches the HEC
handler, limit and error mappings in this document apply.

## Authentication and token isolation

Except for an unauthenticated health probe, a request accepts exactly one
header with exactly this grammar:

```text
Authorization: Splunk <plaintext-token>
```

`Splunk` is case-sensitive. Exactly one ASCII space separates it from the
credential. The credential is nonempty printable ASCII without whitespace or
comma and occupies at most the existing ingestion-token credential bound.
Leading/trailing whitespace, tabs, multiple spaces, repeated or comma-joined
headers, Basic/Bearer schemes, and browser administrator credentials are
invalid authorization.

The case-sensitive scheme is documented by the Splunk Enterprise input
endpoint reference and is not an Open Splunk deviation. Query-string token
authentication is never accepted. A `token` query parameter returns code 16
without looking up its value.

Only an active, unexpired, unrevoked token whose immutable purpose is `HEC`
may authenticate. A native token cannot call HEC, and a HEC token cannot open
a native collector stream. The token must retain at least one current active,
ingestion-enabled allowed index. Public responses do not distinguish an
unknown, expired, revoked, wrong-purpose, or otherwise unusable credential.
Disabled tokens use the documented disabled-token response only after a
constant-shape credential lookup identifies that exact record.

One complete fresh token and index-policy snapshot protects a request.
Mutable defaults, constraints, allowed indexes, and token state are
revalidated in the durable admission transaction. An update affects only a
request that has not crossed that transaction. Disable, expiry, or revocation
after commit does not retract staged data, but it prevents later ingestion and
acknowledgment queries.

The plaintext credential is discarded before decoding, admission, logging,
or metric labeling receives control.

## JSON event protocol

### Framing

A JSON event body is one or more whitespace-separated complete JSON objects.
Each top-level object is one event envelope. Concatenation needs no comma or
array wrapper, for example:

```json
{"event":"one"}{"event":"two"}
```

Only RFC 8259 whitespace (`SP`, `TAB`, `CR`, and `LF`) is permitted between
envelopes. A top-level array, scalar, comma between envelopes, trailing
non-whitespace, empty/whitespace-only body, byte-order mark, invalid UTF-8, or
invalid JSON is rejected. The decoder must retain exact number tokens and
must not route numbers through `float64`.

Duplicate object member names are rejected at every nesting level after JSON
string escape decoding. Thus `"a"` and `"\u0061"` are duplicates. Envelope,
event-object, and `fields` order must not depend on a map implementation.

The complete sequence is decoded and validated before durable admission. The
first invalid envelope by input order determines `invalid-event-number`; no
valid prefix is committed.

### Envelope

The only recognized members are:

```text
time host source sourcetype index event fields
```

Unknown members are rejected with code 6. This catches misspelled routing
metadata rather than silently indexing it elsewhere. `event` is required.
Every other member is optional, but present null is never treated as absent.

| Member | Accepted value | Null/empty/wrong type |
| --- | --- | --- |
| `event` | string, object, array, Boolean, or JSON number | missing is code 12; null or empty string is code 13 |
| `time` | exact JSON number or a string containing the same grammar | code 6 |
| `host` | canonical nonempty string | code 6 |
| `source` | canonical nonempty string | code 6 |
| `sourcetype` | canonical nonempty string | code 6 |
| `index` | canonical nonempty index name | code 7 |
| `fields` | object in the typed domain below | code 15 |

An empty object and empty array are valid event values and produce `_raw`
`{}` and `[]`. A nonempty whitespace-only event string is valid and preserved.
JSON null is the only scalar event form that is not accepted.

### Event conversion

The canonical `_raw` and optional `message` projection is:

| `event` kind | `_raw` | `message` |
| --- | --- | --- |
| string | decoded UTF-8 bytes exactly | the same string |
| object or array | deterministic compact JSON | absent |
| Boolean | `true` or `false` | absent |
| number | the exact authored JSON number token | absent |

String and raw input containing NUL is rejected with code 6. Other JSON
escapes decode normally. Objects retain authored member order recursively;
arrays retain element order. Compaction removes insignificant whitespace,
decodes member names and strings, and re-encodes them with deterministic JSON
escaping. It retains each exact valid numeric lexeme, including exponent
spelling and negative zero. This order-preserving representation is an Open
Splunk v0.1 choice; it is deterministic for one request but is not a general
RFC 8785 canonicalization promise.

Object members are not promoted into dynamic fields. Only `fields` does that.
The server assigns event ID, collected/received time, event-time source,
ingestion provenance, index time, visibility sequence, and expiration.

### Typed fields

`fields` preserves supported types rather than coercing all values to strings.
It accepts unique canonical field names mapped to:

- strings;
- exact JSON integers without a fraction or exponent, representable as signed
  or unsigned 64-bit values;
- finite exact JSON decimals (every accepted number with a fraction or
  exponent), stored without binary floating-point rounding; coefficient
  spelling and negative zero are retained, while exponent spelling is
  canonicalized for the existing `decimal/v1` transport (`E` becomes `e`, a
  leading `+` and exponent leading zeroes are removed, and zero exponent has
  no sign);
- Booleans;
- null; and
- flat arrays of those scalar forms.

Objects and nested arrays in field values are rejected. Integer overflow,
decimal overflow in the storage contract, a numeric token over 128 bytes, an
exponent outside `-1024..1024`, duplicate decoded names, reserved
canonical metadata roots, invalid dynamic-path segments, and unsupported
values return code 15. Arrays retain order and may mix supported scalar kinds.
An unadorned negative integer token uses signed 64-bit (including `-0`); an
unadorned nonnegative integer uses unsigned 64-bit. A fraction or exponent
always uses the exact-value decimal kind even when its mathematical value is
whole; only the exponent spelling normalization above changes its lexeme.
The existing per-event field-count, field-name, aggregate metadata, and typed
value limits still apply. There are at most 1,024 `fields` members and at most
1,024 scalar array elements in aggregate per event.

### Exact event time

The grammar for a numeric `time`, and for the complete contents of a string
`time`, is:

```text
-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?(0|[1-9][0-9]*))?
```

Whitespace, a leading plus, leading zeroes, hexadecimal, `NaN`, and infinity
are invalid. The exact base-10 value is epoch seconds. It must represent an
integral number of nanoseconds; precision finer than one nanosecond is
rejected rather than rounded. Negative zero maps to epoch zero. The result
must fit protobuf/ClickHouse time and the selected index's event-age and
future-skew policy.

When `time` is absent, every envelope uses the one UTC receive instant captured
at the request boundary, with event-time source
`RECEIVED_AT_FALLBACK`. HEC v0.1 never extracts a timestamp from `_raw`, the
event object, source, or sourcetype. This is an intentional Splunk deviation.

### Metadata validation and precedence

`host`, `source`, and `sourcetype` occupy 1 through 255 UTF-8 bytes, contain no
NUL or Unicode control scalar, and must not begin or end with the stable ASCII
whitespace set TAB/LF/VT/FF/CR/SPACE. Values are never trimmed, case-folded, or
Unicode-normalized. Interior whitespace and non-ASCII edge whitespace are
preserved unless rejected by the existing index policy. The same rule applies
to authored HEC token defaults.

Resolution occurs independently for each envelope in this order:

| Metadata | Precedence, highest first | Final fallback |
| --- | --- | --- |
| index | envelope, HEC token `default_index_name` | none; code 7 |
| host | envelope, HEC token `default_host` | `hec` |
| source | envelope, HEC token `default_source` | `http:hec` |
| sourcetype | envelope, HEC token `default_sourcetype`, selected active index default | `httpevent` |
| time | envelope | request receive instant |

An empty or null value is invalid and does not fall through. Index resolution
never grants authority: the selected name must be canonical, allowed by the
fresh token snapshot, active, and ingestion-enabled. Host and source
constraints run against the resolved canonical values. No request header
except the channel header supplies metadata. On JSON endpoints, `channel` is
the only supported query parameter.

## Raw event protocol

Raw v0.1 is deliberately not a `props.conf` engine. Its complete breaker is:

1. the decompressed body must be valid UTF-8 without NUL;
2. LF terminates one segment;
3. exactly one CR immediately before that LF is removed;
4. empty segments are skipped;
5. a final nonempty unterminated segment is an event; and
6. no segment continues into another HTTP request.

An empty body or a body whose segments are all empty returns code 5. A body
containing only `\r` is one nonempty event. Two CRs before LF retain the first
CR. Skipped segments do not consume an event ordinal; the remaining events are
numbered in emission order. Every emitted line has `_raw` and `message` equal
to that line.

The raw endpoint accepts exactly these query parameters once each:

```text
time host source sourcetype index channel
```

URL decoding happens before validation. An empty name, duplicate parameter,
unsupported parameter (including `fields`, `auto_extract_timestamp`, or
`token`), empty value, or malformed escape is rejected. `token` specifically
uses code 16. Metadata validation, precedence, time conversion, index
authorization, and fallbacks are identical to the JSON endpoint and apply to
every emitted event. All events share the request time and request-level raw
metadata.

Unlike the JSON endpoints, raw always requires a valid channel, even when the
token has acknowledgment disabled. This follows the documented Splunk raw
endpoint contract and gives each raw producer a bounded request identity.

## Channels

A channel may be supplied once in `X-Splunk-Request-Channel` or once in the
`channel` query parameter. HTTP header names are case-insensitive. Repeated or
comma-joined headers, duplicate query values, or both sources together return
code 11 even if their decoded strings are equal.

The value must be a canonical hyphenated GUID string:

```text
[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}
```

No UUID version or variant bit is required. Upper- and lower-case hex are both
valid, but channel identity is byte-for-byte and case-sensitive. The value is
therefore exactly 36 ASCII bytes and is within the general 128-byte storage
ceiling. Strict GUID syntax follows published Splunk guidance.

For JSON ingestion, a channel is required when the token has acknowledgment
enabled. With acknowledgment disabled, a present channel is tolerated only
after full validation and has no durable acknowledgment effect. Raw and `/ack`
always require it. A channel is scoped by trusted tenant and token record ID;
it is never a principal, tenant selector, collector ID, index selector, metric
label, or event field.

At most 256 distinct channel identities may exist for one token. A channel
record and its internal allocation ordinal are retained for the token's
lifetime; the ordinal is not a client-visible ID and conveys no polling or
ordering semantics. Channel records are removed only when their owning token
tombstone is safely reclaimed.

## Indexer acknowledgment

Acknowledgment mode is immutable for a HEC token. For an enabled token,
durable staging atomically advances a scoped internal ordinal, derives an
opaque positive acknowledgment ID for `(tenant_id, token_id, channel)`, records
a pending acknowledgment bound to the staged outbox request, and commits all
of those durable rows or none. Emitted IDs are JSON integers in
`1..9007199254740991`, so common JavaScript JSON implementations preserve them
exactly. Clients must not infer order, adjacency, age, or request count from an
ID.

The public ID namespace is keyed with fresh cryptographic process entropy and
is not a direct serialization of rollbackable SQLite state. Retained live
rows enforce scoped uniqueness; a live collision derives a different block
and retries within a fixed bound. Existing IDs and their states survive normal
restart because the durable row stores the emitted ID. New allocations after
restart or restore use a fresh namespace. This prevents deterministic replay
of a discarded snapshot branch, but—as with any finite opaque identifier—the
cross-branch non-alias property is probabilistic rather than an absolute
lifetime guarantee. Code 27 covers retained channel/ACK capacity and internal
allocation-ordinal exhaustion.

After the staging transaction commits, ingestion returns this exact member
order and a JSON integer:

```json
{"text":"Success","code":0,"ackId":42}
```

The selected wire member is `ackId` (lower-case `d`), matching the response
field named by the Splunk input endpoint reference. Some prose examples spell
`ackID` and serialize it as a string; Open Splunk does not emit those forms.

The acknowledgment state machine is closed:

```text
PENDING -> INDEXED
PENDING -> TERMINAL_FAILURE
```

Only completion of the same ClickHouse outbox and committed visibility
reservation used by search changes a row to `INDEXED`. Queueing, an attempted
send, handler completion, or an ambiguous ClickHouse result does not. An
ambiguous send remains pending until reconciliation proves the outcome. A
terminal failure remains publicly false, emits a bounded operator signal, and
makes requested ACK health unavailable until the failure is resolved or its
terminal authority safely expires.

### Acknowledgment query

`POST /services/collector/ack` requires an acknowledgment-enabled active HEC
token and matching channel. A disabled acknowledgment token returns code 14
without reading channel state. The identity body is one JSON object with
exactly one `acks` member:

```json
{"acks":[1,2,3]}
```

The array contains 1 through 1,000 unique positive JSON integers. Emitted IDs
are at most `2^53-1`; the decoder also accepts larger positive signed 64-bit
integers as unknown handles for compatibility. Strings, zero, negatives,
fractions, signed overflow, duplicates, unknown members, and an empty array
return code 6. Concatenated bodies are not accepted on this endpoint.

Success returns only the `acks` object. Its keys are canonical base-10 IDs in
request order:

```json
{"acks":{"1":true,"2":false,"3":false}}
```

`INDEXED` returns `true`. Pending, terminal failure, unknown, cross-scope, and
expired IDs all return `false`, preventing existence disclosure. Querying is
non-destructive: an indexed result remains true until terminal retention
expires. This differs intentionally from Splunk documentation that describes
deleting a true result after it is read. The non-destructive rule makes retries
and restart behavior deterministic.

Pending rows are never expired. Indexed and terminal-failure rows remain for
24 hours after their terminal transition, then bounded cleanup may delete
them. One token retains at most 100,000 acknowledgment rows. At that limit a
new acknowledged request returns code 27 before quota, request, outbox, or
visibility creation. Cleanup never deletes pending authority to admit work.

After coordinated backup/restore, a pending result remains false until
reconciliation and an indexed result remains true for its remaining retention.
New allocations continue from the restored internal ordinal under a fresh
cryptographic namespace. A post-snapshot client-held ID therefore remains
false after restore unless the finite namespace suffers a cross-branch
collision; clients must treat all such discarded-branch IDs as lost.

## Request identity, atomicity, and durability

JSON and raw requests are request-atomic:

- the entire bounded request is decoded, normalized, policy-validated, and
  authorized before admission;
- the lowest invalid event ordinal determines the response;
- any failure creates no quota charge, request row, acknowledgment, outbox,
  visibility reservation, or ClickHouse row; and
- one successful SQLite transaction charges all token/index quotas and stages
  every event, visibility reservation, immutable outbox payload, and optional
  acknowledgment.

`invalid-event-number` is added after `code` for an event-specific code 6, 7,
12, 13, or 15 response. It is omitted for request-level framing, auth, channel,
quota, capacity, shutdown, and internal failures. For malformed input after N
complete envelopes, the invalid ordinal is N. Raw ordinals count emitted
nonempty lines only.

Concretely, a top-level non-object, JSON syntax error, or trailing garbage at
the position where envelope N must begin includes ordinal N. Gzip failure,
whole-body invalid UTF-8, media/size failure, query metadata failure, and an
all-empty body are request-level and omit it. An oversized or policy-invalid
individual raw line includes that emitted line's ordinal.

HTTP success means durable SQLite staging, not ClickHouse visibility. The
acknowledgment-enabled response additionally supplies a handle for later
visibility proof. Cancellation before the transaction commits rolls back;
cancellation or a response-write failure after commit does not undo the
request, so a client may observe no response for data that is later indexed.

The server assigns a random request ID, deterministic per-request event IDs,
a per-source request sequence, and an immutable semantic digest. The request
ID, event IDs, and digest live in the immutable outbox. The sequence is
allocated in the same SQLite staging transaction and retained beside that
outbox's visibility sequence; the native-only ClickHouse `batch_sequence`
field remains the placeholder `1` for HEC rows. This association remains
stable through internal retries but is not a client idempotency contract.
Sending the same HTTP body again is a distinct at-least-once request and may
produce duplicate events. A client with an `ackId` should poll `/ack` rather
than resend merely because indexing is not yet visible.

HEC provenance is server-assigned as source kind `hec` and source ID equal to
the stable ingestion-token record ID. `collector_id` is empty. Secrets,
digests, prefixes, names, and channels are not stored as provenance or dynamic
event fields.

## Error precedence and public responses

The handler returns the first applicable category in this order:

1. unsupported path or method;
2. HTTP/header/compressed-body hard limit and media/encoding checks;
3. forbidden query-string credential;
4. missing or malformed authorization syntax;
5. invalid, disabled, expired, revoked, or wrong-purpose token;
6. required, conflicting, or malformed channel;
7. endpoint/lifecycle gate (`ACK is disabled` or shutdown);
8. empty body;
9. JSON, gzip, UTF-8, or raw framing;
10. missing or blank event;
11. metadata/time/field conversion, using the lowest event ordinal;
12. index, host, source, event, and redaction policy;
13. quota and durable capacity;
14. durable staging failure; and
15. unexpected internal failure.

Authentication precedes semantic body diagnostics. The server may reject an
absolute transport byte limit before credential verification, but it must not
return event-, index-, or field-specific information first. Within one event,
member checks use the fixed order `event`, `time`, `index`, `host`, `source`,
`sourcetype`, `fields`; policy checks use index, ordinary event validation,
host constraint, then source constraint.

Base response member order is `text`, `code`, then optional
`invalid-event-number` or `ackId`. Text is exact and never contains an internal
error. The v0.1 mapping is:

| Code | HTTP | Exact `text` | Use |
| ---: | ---: | --- | --- |
| 0 | 200 | `Success` | durable staging success |
| 1 | 403 | `Token disabled` | exact disabled ingestion token |
| 2 | 401 | `Token is required` | missing authorization |
| 3 | 401 | `Invalid authorization` | malformed authorization grammar |
| 4 | 403 | `Invalid token` | unknown, expired, revoked, wrong-purpose, or unusable token |
| 5 | 400 | `No data` | empty body or no emitted raw event |
| 6 | 400 | `Invalid data format` | syntax, conversion, policy, unsupported path/query, or generic safe rejection |
| 7 | 400 | `Incorrect index` | missing, invalid, inactive, disabled, or unauthorized index |
| 8 | 500 | `Internal server error` | non-retry-classified internal failure |
| 9 | 503 | `Server is busy` | transient concurrency, staging, or downstream unavailability |
| 10 | 400 | `Data channel is missing` | required channel absent |
| 11 | 400 | `Invalid data channel` | malformed, repeated, or conflicting channel |
| 12 | 400 | `Event field is required` | envelope lacks `event` |
| 13 | 400 | `Event field cannot be blank` | event is null or empty string |
| 14 | 400 | `ACK is disabled` | `/ack` used with acknowledgment-disabled token |
| 15 | 400 | `Error in handling indexed fields` | invalid `fields` |
| 16 | 400 | `Query string authorization is not enabled` | `token` query parameter |
| 17 | 200 | `HEC is healthy` | healthy probe |
| 18 | 503 | `HEC is unhealthy, queues are full` | staging unavailable/full |
| 19 | 503 | `HEC is unhealthy, ack service unavailable` | requested ACK health unavailable |
| 20 | 503 | `HEC is unhealthy, queues are full, ack service unavailable` | both unavailable |
| 21 | 400 | `Invalid token` | authenticated health probe has unusable token |
| 22 | 400 | `Token disabled` | authenticated health probe has disabled token |
| 23 | 503 | `Server is shutting down` | shutdown gate is closed |
| 26 | 429 | `HEC queue is at capacity and cannot process any more requests` | durable pending-work or per-token retained-request capacity |
| 27 | 429 | `HEC ACK channel is at capacity and cannot process any more requests` | channel/ACK capacity or allocation-ordinal exhaustion |

Codes 24 and 25 (approaching-capacity warnings) are reserved but not emitted
in v0.1. Open Splunk does not replace a successful code 0 response with a
warning that may cause clients to retry already-staged data.

Three standard HTTP refinements retain the same JSON category: code 6 uses
HTTP 404/405 for route/method, 413 for a compressed, decompressed, normalized,
or event-size hard limit, 415 for an unsupported media type or
`Content-Encoding` token, and 431 for consumed header values. A malformed,
truncated, concatenated, or trailing-data gzip stream is ordinary HTTP 400
code 6. Durable token/index quota pressure uses HTTP 429 with code 9 and
`Retry-After`, rather than code 9's ordinary HTTP 503. These are explicit Open
Splunk deviations. `Retry-After` is a decimal integer number of seconds,
ceiled to at least 1 and capped at 3,600.

Host/source constraint failures, invalid event time/size, redaction-policy
rejection, and other Open Splunk-only policy failures use code 6 without
identifying the field value or configured rule. A storage or ClickHouse error
never appears in `text`.

## Health

Health is a bounded operational projection and does not perform an ingestion
or ClickHouse write. Both health aliases accept no body and only the optional
query parameter `ack=true` or `ack=1`. Other `ack` values, duplicates, or
unknown parameters return code 6; `token` returns code 16.

Without `Authorization`, health is a shallow aggregate probe. With an
Authorization header, the ordinary exact grammar is enforced and the token
must be an active HEC token with at least one active allowed index. An unknown,
expired, revoked, wrong-purpose, or authority-empty token returns code 21; a
disabled token returns code 22. This explicit token-disabled health result
matches the documented health taxonomy while all other unusable credentials
remain indistinguishable.

Healthy means the server accepts new HEC work and durable staging is below its
fail-closed capacity. `ack=true|1` additionally includes acknowledgment-store
and reconciliation availability, regardless of the presented token's ACK
mode. Codes 17 through 20 are selected from queue and requested ACK health.
No health response includes tenant, token, index, channel, counts, queue depth,
ClickHouse address, schema, or reason detail.

## Administrator operational snapshot

`POST /api/v1/hec/operations/get` is the authenticated browser-administrator
projection for HEC capacity and cumulative process counters. Its protobuf
request is empty. The response includes request/event/byte totals,
authentication/decode/event-policy/rate-limit/staging failures, staging
duration, pending outbox count/bytes/oldest age, retained request capacity,
reconciliation success/retry/ambiguity, active and retained channel counts,
pending/indexed/expired acknowledgment rows, acknowledgment queries/misses,
shutdown rejections, and the fixed non-success HEC protocol-code counter
domain 1 through 27.

The snapshot is process-wide and contains no token, channel, index, request,
event, payload, address, or error-detail identity. A false retained-request
capacity signal means at least one token has reached its durable request-ledger
ceiling, but the response never identifies that token. The unauthenticated
health path does not project this token-scoped fact.

## Resource limits and backpressure

Deployment configuration may tighten a limit before startup but must not
increase these v0.1 ceilings. The effective values are fixed for one running
request.

| Resource | v0.1 ceiling/default |
| --- | ---: |
| compressed request body | 8 MiB |
| decompressed request body | 8 MiB |
| normalized admission bytes | 8 MiB |
| JSON envelopes or emitted raw events | 1,000 |
| normalized event | 1 MiB |
| one exact JSON number token | 128 bytes; exponent magnitude at most 1,024 |
| dynamic fields per event | 1,024 |
| scalar values across field arrays per event | 1,024 |
| JSON composite nesting below the envelope | 16 |
| decoded JSON values per request | 16,384 |
| decoded JSON object members per request | 4,096 |
| one dynamic field path segment | 256 bytes |
| HEC-consumed header values, aggregate | 8 KiB |
| request target (path plus query) | 8 KiB |
| channel storage bound | 128 bytes; v0.1 GUID syntax is 36 |
| distinct channels per token lifetime | 256 |
| emitted acknowledgment ID | exact positive JSON integer, at most `2^53-1` |
| acknowledgment IDs per query | 1,000 |
| retained acknowledgment rows per token | 100,000 |
| retained request rows per token | 100,000 |
| terminal acknowledgment retention | 24 hours |
| acknowledgment query body | 64 KiB |
| JSON response | 1 MiB |
| concurrent requests per token | 16 |
| concurrent ingestion/ACK HEC requests process-wide | 128 |
| concurrent HEC health requests process-wide | 8, reserved independently |
| pending HEC outbox requests process-wide | 64 |
| pending HEC outbox payload bytes process-wide | 256 MiB |
| durable outbox for one request | 16 MiB |
| durable metadata for one request | 1 MiB |

The event-age ceiling is 365 days and the future-skew ceiling is 5 minutes;
an index may tighten both. Existing field-name aggregate, typed-value,
retention, and index-name bounds also apply.

Per-token and per-index rate schedules charge each admitted event using the
server-computed protobuf-encoded size of the source event before normalization
or mandatory redaction, exactly as native ingestion does. Detached decoder byte
observations, HTTP `Content-Length`, and compressed source length are never the
accounting authority. Every applicable schedule is advanced in the same
transaction or none is. Reconnecting, changing channel, or using a new HTTP/2
stream does not bypass it.

Process concurrency is acquired before body decode. Per-token concurrency is
acquired after authentication and before semantic decode. A failed gate creates
no durable state. The durable pending-row and payload-byte capacities are
checked transactionally. ACK capacity is independent. A pending
acknowledgment is never deleted to create room.

## Shutdown, retries, and recovery

Once graceful shutdown closes admission, new ingestion and acknowledgment
queries return code 23; health returns the appropriate unavailable code.
Already-started requests may finish only within the server's bounded drain
deadline. A request that has not begun its SQLite transaction is canceled. A
committed request remains authoritative and reconciliation resumes after
restart.

HEC token profiles, channel allocation ordinals, acknowledgments, visibility
reservations, quota schedules, and outbox rows belong to one coordinated
SQLite backup/restore unit. ClickHouse reconciliation must preserve stable
payload and deduplication identity. Corrupt or future-version control/outbox
state fails startup or reconciliation closed; it is never discarded as
success.

## Intentional deviations and exclusions

Open Splunk v0.1 intentionally differs from, or narrows, documented Splunk
behavior as follows:

- it uses the existing HTTPS listener rather than a dedicated 8088 listener;
- it supports only the routes listed here and no management-port, `mint`,
  `s2s`, metrics, or token-management compatibility endpoints;
- top-level arrays are rejected; batching uses concatenated objects;
- unknown and duplicate JSON members are rejected;
- JSON numbers and event-object order are retained deterministically;
- `fields` preserves a bounded typed scalar/flat-array extension rather than
  coercing every value to a string;
- event null and empty string are blank, while `{}` and `[]` are valid;
- timestamp extraction from event text is not supported;
- index has no global fallback, and host/source do not derive from the token
  name, peer address, proxy header, or HTTP Host;
- raw uses one fixed LF/CRLF breaker, skips empty segments, and has no
  cross-request or sourcetype-specific state;
- exactly one gzip member is accepted;
- channels use strict GUID text and case-sensitive byte identity;
- ACK IDs are opaque exact positive JSON integers under `ackId`; they are not monotonic sequence numbers;
- an indexed ACK result is non-destructive and remains true for 24 hours;
- requests are atomic and never persist a valid prefix;
- independent repeated HTTP requests are at-least-once, not idempotent;
- health never accepts a token in the query string or exposes queue details;
- capacity-warning codes 24 and 25 are not emitted; and
- route, size, media, and quota failures use the documented JSON taxonomy with
  the explicit HTTP refinements above.

Also unsupported are Basic/Bearer/query authentication, arbitrary
`props.conf`/`transforms.conf`, multiline or regex breakers, ingest-time field
transforms, custom routing, binary raw events, metrics-specific HEC envelopes,
distributed acknowledgment state, exactly-once delivery, and use of channels
as identity or authority.

## Executable fixture contract

`internal/hec/testdata/compatibility/fixture.schema.json` defines a JSON Schema
for corpus documents. Fixtures contain the complete synthetic HTTP request,
clock, token/index/capacity setup, exact response bytes and headers, durable
disposition, stored event projection, and optional SPL result. They use
symbolic `{{token:<alias>}}` placeholders; fixtures never contain a usable
credential.

Implementations must run the corpus table-driven at the HTTP boundary. Binary,
gzip, and oversize inputs are described by deterministic body generators so a
multi-megabyte or secret-bearing artifact is not checked in. Fixture setup is
isolated per case. A missing expectation is not permission to ignore an
observable side effect: every case declares the complete durable disposition.

The initial corpus covers each emitted response code, every decision listed in
Phase HEC-0, success projections for JSON/raw/ACK, request atomicity, exact
time and typed fields, channel isolation, health authentication, and the main
intentional deviations. Later bug fixes must add a regression case without
weakening an existing case merely to match an implementation accident.

## Sources

Primary public compatibility references:

- [Splunk Enterprise: Format events for HTTP Event Collector](https://help.splunk.com/en/splunk-enterprise/get-started/get-data-in/10.2/get-data-with-http-event-collector/format-events-for-http-event-collector)
- [Splunk Enterprise: HTTP Event Collector REST API endpoints](https://help.splunk.com/en/splunk-enterprise/get-data-in/collect-http-event-data/http-event-collector-rest-api-endpoints)
- [Splunk Enterprise 10.4 input endpoint descriptions](https://help.splunk.com/en/splunk-enterprise/leverage-rest-apis/rest-api-reference/10.4/input-endpoints/input-endpoint-descriptions)
- [Splunk Enterprise: About HEC indexer acknowledgment](https://help.splunk.com/en/splunk-enterprise/get-data-in/get-started-with-getting-data-in/9.4/get-data-with-http-event-collector/about-http-event-collector-indexer-acknowledgment)
- [Splunk Enterprise: Troubleshoot HTTP Event Collector](https://help.splunk.com/en/splunk-enterprise/get-data-in/collect-http-event-data/troubleshoot-http-event-collector)

Repository authorities preserved by this contract:

- [HEC implementation plan](hec-compatibility-plan.md)
- [HEC deployment and operations](hec-deployment.md)
- [Protobuf v1 contracts](protobuf-v1-contracts.md)
- [Ingestion rate limits v0.1](ingestion-rate-limits-v0.1.md)
- [Ingestion token host/source constraints v0.1](ingestion-token-constraints-v0.1.md)
- [Audit events v0.1](audit-events-v0.1.md)
