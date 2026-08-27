# API contract

The current development wire package is `open_splunk`. Protobuf sources live
under `proto/open_splunk`; generated Go uses package `opensplunk`, and generated
TypeScript follows `gen/ts/open_splunk`. Major-version-zero releases do not
stabilize this namespace; it may change between v0 releases.

The `.proto` files are the field-level source of truth. This document records
the public transports and invariants. Persistent SQLite rows and ClickHouse
tables are not wire contracts.

## Browser protobuf HTTP

Every route in this table is `POST` beneath `/api` and uses
`application/x-protobuf` for successful request and response bodies. Non-2xx
responses use the bounded SRouter/go-common error shape. Unknown `/api/`
paths are API 404s and never fall through to the static application.

| Family | Relative paths |
| --- | --- |
| System | `/system/bootstrap` |
| Search jobs | `/search/jobs/create`, `/get`, `/list`, `/results`, `/fields/list`, `/field-summary`, `/timeline`, `/cancel`, `/inspect` |
| Search tools | `/search/validate`, `/search/suggestions` |
| History | `/search/history/get`, `/list`, `/delete`, `/clear` |
| Exports | `/search/exports/create`, `/get`, `/list`, `/cancel` |
| Saved searches | `/saved-searches/create`, `/get`, `/list`, `/update`, `/duplicate`, `/delete` |
| Dashboards | `/dashboards/create`, `/get`, `/list`, `/update`, `/delete`, `/panels/run` |
| Indexes | `/indexes/create`, `/get`, `/list`, `/update`, `/state/set`, `/delete`, `/stats/get`, `/fields/list` |
| Apps | `/apps/create`, `/get`, `/list`, `/update`, `/state/set`, `/delete` |
| Collectors | `/collectors/list`, `/get`, `/update`, `/state/set` |
| Ingestion tokens | `/ingestion-tokens/create`, `/get`, `/list`, `/update`, `/state/set`, `/revoke` |
| Knowledge objects | `/knowledge/objects/create`, `/get`, `/list`, `/dependencies`, `/dependents`, `/validate`, `/preview`, `/update`, `/set-state`, `/delete`, `/quarantine/prepare`, `/quarantine` |
| Lookups | `/knowledge/lookups/create`, `/get`, `/list`, `/replace`, `/state/set`, `/delete`, `/preview` |
| Audit | `/audit/events/list`, `/audit/search-attempts/list` |
| HEC administration | `/hec/operations/get` |

Bootstrap is the capability authority. It reports build identity, including the
product version when present, the source revision, and the capabilities
composed into that process. Optional families are usable only when their feature
enum is advertised. A generated message or individually registered management
route is not proof that the corresponding runtime, retained products, and
browser family are complete.

Requests are raw-body bounded before decode. Malformed protobuf and invalid
known enum values fail. Ordinary operations ignore unknown fields according to
standard protobuf decoding rules, but authority-sensitive knowledge definition
mutations and validation use bounded wire inspection and reject unknown meaning
that the server cannot safely validate. Responses are detached and validated
before serialization.

Mutable entities expose integer `version` or `state_version`. These are opaque
optimistic-concurrency revisions, not product/API versions. Updates and state
changes advance them according to the current implementation; clients only
compare values returned by the same running source contract. Mutations that
take `expected_version` fail on a stale value. Cursors, page tokens, download
grants, and idempotency values are likewise opaque.

## Collector gRPC

The native service is
`open_splunk.CollectorIngestService/Collect`, a bidirectional stream.

1. The collector uses TLS by default and sends
   `authorization: Bearer <token>` in gRPC metadata.
2. Its first frame is connection sequence 1 with `CollectorHello`; credentials
   never appear in protobuf payloads.
3. The server authenticates the bound collector and returns `CollectorReady`
   with negotiated limits.
4. The collector sends durable `EventBatch` values and heartbeats within those
   limits.
5. `BatchAck` and `BatchReject` are terminal. `RetryBatch` keeps the exact batch
   in the WAL. `Throttle` controls later sends and acknowledges nothing.
6. Reconnection reports the last contiguous acknowledgment and replays every
   later batch with unchanged sequence, IDs, events, and digest.

An acknowledgment means ClickHouse commit authority was reached. It is not a
global exactly-once promise; stable identities provide bounded retry
deduplication. Permanent per-event rejections may accompany an acknowledged
batch and are sent to the collector dead-letter output.

## Search WebSocket and downloads

`GET /api/search/ws` upgrades to a WebSocket. Each application frame is
binary protobuf: the browser sends `SearchWebSocketCommand` and receives
`SearchWebSocketEvent`. Sequences belong to the search/export target, not the
TCP connection. A reconnect supplies the last processed sequence; the server
replays retained events or sends `ResynchronizationRequired`, after which the
client reads authoritative job/results routes.

Export download is a raw `GET` response, not protobuf. The short-lived path and
bearer grant are returned by the export API. The token belongs in the
`Authorization` header, never a query string.

HEC is bounded JSON rather than protobuf and is documented in [HEC](hec.md).

## Development rules

The API is major-version-zero. Source and v0 release changes may be breaking
until the project defines its v1 compatibility policy. Within one current
source tree:

- Never renumber or reuse protobuf fields or enum numbers; reserve removed
  names and numbers. Every enum starts with `UNSPECIFIED = 0`.
- A security-sensitive field whose omission changes authority requires an
  explicit gate; ordinary unknown-field decoding is not authority.
- `TypedValue` preserves exact integers, decimals, bytes, timestamps, lists,
  objects, null, and missing. It must not be replaced by a double-based generic
  value container.
- Result rows have exactly one cell per `ResultSchema` column. Missing and
  explicit null remain distinct.
- Time intent and the resolved half-open interval remain separate messages.
- Plaintext ingestion tokens and download grants are one-time or short-lived
  secrets and must not be logged.

Regenerate and validate the complete surface with `make proto`. Generated
files are full-rebuild artifacts and must not be edited manually.
