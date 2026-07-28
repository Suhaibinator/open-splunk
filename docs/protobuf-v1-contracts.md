# Open Splunk v1 API contracts

This directory is the source of truth shared by the Go server, Go collector, and TypeScript browser application. The package name is `open_splunk.v1`; generated Go code uses package `opensplunkv1` and generated TypeScript follows the source directory hierarchy.

## Contract boundaries

- `value.proto`, `common.proto`, `event.proto`, and `result.proto` define shared wire primitives, exact dynamic values, canonical collected events, dynamic result schemas, field summaries, timelines, and visualization settings.
- `collector.proto` is the only gRPC service. It defines the collector registration and at-least-once batch-delivery stream.
- `search.proto` and `search_api.proto` separate reusable search intent and job state from SRouter request/response messages. `search_inspection_api.proto` is the administrator-only completed-job logical/physical plan inspection contract.
- `search_ws.proto` defines binary WebSocket commands and sequenced progress events. It is not a results paging API.
- `saved_search*`, `history*`, and `export*` remain separate because they have different lifecycle, persistence, and security semantics.
- `index*`, `app*`, and `collector_admin*` define control-plane entities plus SRouter operations.
- `system_api.proto` gives the static frontend one bootstrap call for server capabilities and initial app/index choices.

Persistent database rows and ClickHouse table definitions are deliberately not protobuf contracts. Converters at the service boundary keep storage migrations from becoming accidental wire changes.

## Browser routes

Every route below is `POST`, relative to `/api/v1`, and uses `application/x-protobuf` for successful request and response bodies. Non-2xx errors use the standard SRouter/go-common transport error shape. Authentication can be added by SRouter middleware without changing these messages.

| Path | Request | Response |
| --- | --- | --- |
| `/system/bootstrap` | `GetSystemBootstrapRequest` | `GetSystemBootstrapResponse` |
| `/search/jobs/create` | `CreateSearchJobRequest` | `CreateSearchJobResponse` |
| `/search/jobs/get` | `GetSearchJobRequest` | `GetSearchJobResponse` |
| `/search/jobs/list` | `ListSearchJobsRequest` | `ListSearchJobsResponse` |
| `/search/jobs/results` | `GetSearchResultsRequest` | `GetSearchResultsResponse` |
| `/search/jobs/fields/list` | `ListSearchFieldsRequest` | `ListSearchFieldsResponse` |
| `/search/jobs/field-summary` | `GetSearchFieldSummaryRequest` | `GetSearchFieldSummaryResponse` |
| `/search/jobs/timeline` | `GetSearchTimelineRequest` | `GetSearchTimelineResponse` |
| `/search/jobs/cancel` | `CancelSearchJobRequest` | `CancelSearchJobResponse` |
| `/search/jobs/inspect` | `InspectSearchJobRequest` | `InspectSearchJobResponse` |
| `/search/validate` | `ValidateSearchRequest` | `ValidateSearchResponse` |
| `/search/suggestions` | `GetSearchSuggestionsRequest` | `GetSearchSuggestionsResponse` |
| `/search/history/get` | `GetSearchHistoryEntryRequest` | `GetSearchHistoryEntryResponse` |
| `/search/history/list` | `ListSearchHistoryRequest` | `ListSearchHistoryResponse` |
| `/search/history/delete` | `DeleteSearchHistoryEntryRequest` | `DeleteSearchHistoryEntryResponse` |
| `/search/history/clear` | `ClearSearchHistoryRequest` | `ClearSearchHistoryResponse` |
| `/search/exports/create` | `CreateExportJobRequest` | `CreateExportJobResponse` |
| `/search/exports/get` | `GetExportJobRequest` | `GetExportJobResponse` |
| `/search/exports/list` | `ListExportJobsRequest` | `ListExportJobsResponse` |
| `/search/exports/cancel` | `CancelExportJobRequest` | `CancelExportJobResponse` |
| `/saved-searches/create` | `CreateSavedSearchRequest` | `CreateSavedSearchResponse` |
| `/saved-searches/get` | `GetSavedSearchRequest` | `GetSavedSearchResponse` |
| `/saved-searches/list` | `ListSavedSearchesRequest` | `ListSavedSearchesResponse` |
| `/saved-searches/update` | `UpdateSavedSearchRequest` | `UpdateSavedSearchResponse` |
| `/saved-searches/duplicate` | `DuplicateSavedSearchRequest` | `DuplicateSavedSearchResponse` |
| `/saved-searches/delete` | `DeleteSavedSearchRequest` | `DeleteSavedSearchResponse` |
| `/indexes/create` | `CreateIndexRequest` | `CreateIndexResponse` |
| `/indexes/get` | `GetIndexRequest` | `GetIndexResponse` |
| `/indexes/list` | `ListIndexesRequest` | `ListIndexesResponse` |
| `/indexes/update` | `UpdateIndexRequest` | `UpdateIndexResponse` |
| `/indexes/state/set` | `SetIndexStateRequest` | `SetIndexStateResponse` |
| `/indexes/delete` | `DeleteIndexRequest` | `DeleteIndexResponse` |
| `/indexes/stats/get` | `GetIndexStatsRequest` | `GetIndexStatsResponse` |
| `/indexes/fields/list` | `ListIndexFieldsRequest` | `ListIndexFieldsResponse` |
| `/apps/create` | `CreateAppRequest` | `CreateAppResponse` |
| `/apps/get` | `GetAppRequest` | `GetAppResponse` |
| `/apps/list` | `ListAppsRequest` | `ListAppsResponse` |
| `/apps/update` | `UpdateAppRequest` | `UpdateAppResponse` |
| `/apps/state/set` | `SetAppStateRequest` | `SetAppStateResponse` |
| `/apps/delete` | `DeleteAppRequest` | `DeleteAppResponse` |
| `/collectors/list` | `ListCollectorsRequest` | `ListCollectorsResponse` |
| `/collectors/get` | `GetCollectorRequest` | `GetCollectorResponse` |
| `/collectors/update` | `UpdateCollectorRequest` | `UpdateCollectorResponse` |
| `/collectors/state/set` | `SetCollectorEnabledRequest` | `SetCollectorEnabledResponse` |
| `/ingestion-tokens/create` | `CreateIngestionTokenRequest` | `CreateIngestionTokenResponse` |
| `/ingestion-tokens/get` | `GetIngestionTokenRequest` | `GetIngestionTokenResponse` |
| `/ingestion-tokens/list` | `ListIngestionTokensRequest` | `ListIngestionTokensResponse` |
| `/ingestion-tokens/update` | `UpdateIngestionTokenRequest` | `UpdateIngestionTokenResponse` |
| `/ingestion-tokens/revoke` | `RevokeIngestionTokenRequest` | `RevokeIngestionTokenResponse` |

### Search validation

`POST /api/v1/search/validate` accepts the same bounded `SearchDefinition`
intent used to create an ad-hoc search. The server requires nonblank,
NUL-free SPL, an explicit resolvable time range, and at least one normalized
active, search-enabled index which the caller may search. Search presentation
fields are not validation inputs. The server resolves relative times once and
passes the exact detached tenant, authorized index set, requested index set,
and half-open absolute time range through the same parse, plan, authorization,
and ClickHouse compilation path used by search-job creation.

A successfully compiled search returns HTTP 200 with `valid = true`, trimmed
`normalized_spl`, sorted unique effective `referenced_indexes`, sorted unique
logical read `referenced_fields`, and a non-unspecified
`predicted_result_kind`. Referenced fields describe inputs read by the logical
pipeline, not merely final result columns.

An SPL parse, planning, compiler, or in-query index-scope rejection also
returns HTTP 200, but with `valid = false` and one or more error
`diagnostics`. An invalid response contains no normalized SPL, referenced
indexes, referenced fields, or predicted result kind, so a rejected partial
plan cannot be mistaken for accepted analysis. Diagnostic ranges are exact
half-open ranges into the original UTF-8 SPL: byte offsets are zero-based,
while lines and Unicode-scalar columns are one-based. The shared diagnostic
projection keeps validation, retained-job/history, and WebSocket coordinates
identical.

Malformed protobuf or search definitions, invalid or unauthorized requested
index scopes, unresolvable time ranges, oversized requests, deadlines,
capacity exhaustion, unavailable dependencies, and internal failures are
transport failures rather than SPL diagnostics. They use the standard
non-2xx SRouter/go-common error response and do not expose compiler SQL,
arguments, catalog details, or other internal error text.

Validation is synchronous, cancellation-aware, and independently bounded by
the search manager's validation concurrency gate; a full gate fails fast
instead of queuing. Existing request-byte, SPL-token, scope-count,
expression-depth, logical-analysis node/depth, and compiler-work limits still
apply. Validation creates no ID or search job, changes no queue or retained
metadata, writes no history or journal record, takes no storage visibility
snapshot, and executes no ClickHouse query. Compilation may construct
generated SQL transiently to prove backend support, but neither SQL nor bound
arguments are retained, exposed, or submitted to ClickHouse.

### Administrator search inspection

`POST /api/v1/search/jobs/inspect` is an administrator-only diagnostic route
for one retained, completed search job. Its request contains only
`search_job_id`; tenant and owner scope come exclusively from the authenticated
browser principal and cannot be selected on the wire.

The response contains a bounded logical projection (ordered stages, safe field
names, exact source ranges, and final output shape), a bounded physical
projection (allowlisted ClickHouse node types, read columns, index names and
keys, and initial/selected part and granule counts), the generated SQL, raw
structured `EXPLAIN PLAN` text, and the diagnostic ClickHouse query ID.
`OPEN`, `STATIC`, and `DYNAMIC` output kinds distinguish an unknown schema, a
complete ordered schema, and a fixed prefix with a maximum number of dynamic
fields.

Generated SQL and raw EXPLAIN text are administrator-sensitive and are not
returned by ordinary search-job reads. Compiler arguments are never separate
response fields and the generated SQL remains parameterized, but ClickHouse
may render any query-bound tenant, index, or predicate value into the raw
EXPLAIN text. Treat the entire inspection response as privileged diagnostic
data. Execution rows, owner IDs, and mutable planner state are never part of
this contract. The server validates and bounds every projected collection and
string before serialization.

The export download route is a raw `GET` file response rather than protobuf. Its short-lived path and bearer capability are returned only in `ExportDownloadGrant`; the token is sent in the `Authorization` header and never placed in a query string.

The `client_request_id` fields reserve the wire contract for future durable retry handling. The current server does not support them for search jobs, exports, saved searches, indexes, or ingestion tokens: supplying the field, including an explicitly empty value, fails request validation. App administration is not implemented yet. Create requests without the field are not deduplicated, and the server does not currently advertise an idempotency-retention window. When support is added, a key will be scoped to the authenticated caller and operation, and reuse for a different canonical request will be a conflict.

Search creation always creates a job record—even parse or planning failures transition that job to `FAILED` and therefore appear in history. Search and export cancellation are idempotent, and an already-terminal job is returned unchanged.

Result cursors are scoped to one immutable search snapshot and one column selection. A page token must not be reused with another job or changed request parameters. Rows contain exactly one cell per schema column; a nonexistent field uses `MISSING_VALUE_MISSING`, while an explicitly present null uses `NULL_VALUE_NULL`.

## Collector stream

The native service is `open_splunk.v1.CollectorIngestService/Collect`, a bidirectional gRPC stream.

1. The collector opens the stream using TLS by default and sends `authorization: Bearer <token>` in gRPC metadata.
2. Its first frame has connection sequence `1` and a `CollectorHello`. Credentials never appear in protobuf payloads.
3. The server authenticates the token, negotiates protocol limits, and returns `CollectorReady`. Authentication/protocol failures that invalidate the stream use canonical gRPC status codes.
4. The collector sends durable `EventBatch` records and periodic `CollectorHeartbeat` frames while respecting negotiated in-flight and byte/event limits.
5. `BatchAck` and `BatchReject` are terminal dispositions. An ack may contain permanent per-event rejections; accepted, duplicate, and rejected counts must sum to the original event count. Rejected events go to the collector dead-letter output while the durable batch advances. `RetryBatch` is non-terminal and requires replay of the unchanged batch. `Throttle` applies to future sending without acknowledging anything.
6. On disconnect, the collector reconnects, sends its last contiguous acknowledged sequence, and replays every unacknowledged batch with the same batch ID, sequence, event IDs, and event-ID digest.

An acknowledgment means the server reached `ACK_DURABILITY_CLICKHOUSE_COMMITTED`. It does not promise global exactly-once delivery. Stable event and batch IDs make retries idempotent for the most recent 10,000 committed server visibility sequences, matching the exclusively managed ClickHouse events table's 10,000-block deduplication window. The server pauses newer sends behind an ambiguous insert until its durable outbox is reconciled, so that insert cannot age out of the window. Replaying a terminal batch older than this explicit horizon is at-least-once and may create duplicates.

## Search WebSocket

`GET /api/v1/search/ws` upgrades to a WebSocket. Every application message is one binary protobuf frame: browsers send `SearchWebSocketCommand` and decode `SearchWebSocketEvent`.

Sequences are per search/export target, not per TCP connection. A reconnecting client subscribes with its last processed sequence. The server replays newer retained events or sends `ResynchronizationRequired`; the client then calls the authoritative SRouter `get`/results endpoints. Preview rows are bounded and disposable, and may be reset or dropped without affecting durable result pages.

## Evolution rules

- Never renumber a field or reuse a deleted field/enum number; reserve both old numbers and names.
- Every enum starts with `UNSPECIFIED = 0`; handlers reject unspecified values where the operation requires a choice.
- Opaque IDs, cursors, download grants, and idempotency keys are strings. Clients must not parse their contents.
- Mutable SQLite entities expose `version`; updates and deletes require `expected_version` and fail on stale writes.
- `TimeRangeSpec` preserves relative expressions, while `ResolvedTimeRange` records the half-open absolute interval used by one execution.
- `TypedValue` preserves signed/unsigned integers, decimals, bytes, timestamps, lists, objects, explicit null, and an explicit missing-field marker. Do not replace it with `google.protobuf.Struct`, which represents every number as a double.
- Job results always include `ResultSchema`; each row's cells correspond positionally to its columns.
- Plaintext ingestion tokens and export download grants are one-time/short-lived secrets and must never be logged.

## Generation

From the repository root:

```sh
make proto-tools  # once per development environment
make proto-lint
make proto
```

Generated Go protobuf and gRPC files are written to `gen/go/open_splunk/v1`. Generated `ts-proto` messages and index files are written to `gen/ts`. Generated files are full-rebuild artifacts and must not be edited manually.

TypeScript `oneof` fields are generated as discriminated unions (`oneof=unions-value`). Frontend code must switch on `$case`; it cannot construct multiple payload variants simultaneously.
