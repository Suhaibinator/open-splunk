# Open Splunk v1 API contracts

This directory is the source of truth shared by the Go server, Go collector, and TypeScript browser application. The package name is `open_splunk.v1`; generated Go code uses package `opensplunkv1` and generated TypeScript follows the source directory hierarchy.

## Contract boundaries

- `value.proto`, `common.proto`, `event.proto`, and `result.proto` define shared wire primitives, exact dynamic values, canonical collected events, dynamic result schemas, field summaries, timelines, and visualization settings.
- `collector.proto` is the only gRPC service. It defines the collector registration and at-least-once batch-delivery stream.
- `search.proto` and `search_api.proto` separate reusable search intent and job state from SRouter request/response messages. `search_inspection_api.proto` is the administrator-only completed-job logical/physical plan inspection contract.
- `search_ws.proto` defines binary WebSocket commands and sequenced progress events. It is not a results paging API.
- `saved_search*`, `history*`, and `export*` remain separate because they have different lifecycle, persistence, and security semantics.
- `index*`, `app*`, and `collector_admin*` define control-plane entities plus SRouter operations.
- `audit*` defines the fixed, administrator-only immutable mutation-audit projection and bounded list operation. `search_attempt_audit*` defines the separately bounded, payload-free search-admission projection and list operation.
- `knowledge.proto` defines the common registry projection, authorized selectors,
  Tier-1 typed definitions, versioned dependencies, provenance, and immutable
  search snapshot. `knowledge_api.proto` reserves the protobuf CRUD,
  validation, dependency, and bounded preview messages. These contracts are
  additive but deliberately unregistered and unadvertised until the complete
  Tier-1 API and runtime family exists.
- `system_api.proto` gives the static frontend one bootstrap call for server capabilities and initial app/index choices.

Persistent database rows and ClickHouse table definitions are deliberately not protobuf contracts. Converters at the service boundary keep storage migrations from becoming accidental wire changes.

## Browser routes

Every route below is `POST`, relative to `/api/v1`, and uses `application/x-protobuf` for successful request and response bodies. Non-2xx errors use the standard SRouter/go-common transport error shape. Authentication can be added by SRouter middleware without changing these messages.

Binary protobuf version skew follows the normal ignore-unknown contract. Every
syntactically valid request may contain fields that this server version does
not recognize, including within populated known submessages. Those fields
count toward the route's raw body limit and are discarded at the HTTP
transport boundary before known-field validation or persistence. They cannot
change authentication, authorization, target selection, confirmation,
optimistic versions, update masks, quotas, stored objects, or responses.

Recognized fields keep their complete validation contract. In particular, an
unsupported numeric value in a known enum is not an unknown field and remains
invalid where the operation does not define it. A future oneof arm is absent
from an older server's known view, so an operation that requires that oneof
still rejects the request. New fields whose omission would change a
security-sensitive operation require explicit capability negotiation or a
new versioned route. Malformed or truncated protobuf remains a transport
error, and duplicate known fields retain the protobuf runtime's existing
decode semantics.

Clients may rely on known fields remaining decodable, not on unknown bytes
being preserved or echoed. The Go runtime retains unknown response bytes by
default, while the generated TypeScript decoders skip them.

Knowledge-definition mutation payloads are the deliberate exception to the
generic browser request rule above. Their raw protobuf must be inspected for
unknown fields recursively, including nested messages and unknown future
`oneof` bodies, before any generic decoder can discard those bytes. The route
rejects such a definition as invalid instead of silently accepting a meaning
an older server cannot validate. Read-only response evolution retains the
ordinary known-field decodability rule.

### Reserved knowledge-object contracts

The `SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS` enum value is reserved for the
complete Tier-1 field-knowledge family. A server must not include it in
`GetSystemBootstrapResponse.features` until CRUD, validation, authorized
resolution, immutable snapshot admission, field enrichment, inspection, and
the browser workflow are all configured. Merely generating the messages does
not create a route or advertise a capability.

Knowledge definitions deliberately repeat their indexed app, name, sharing,
and type identity in the registry projection. A future storage boundary must
require exact agreement after decoding; it must report corruption rather than
repairing one representation during a request. Definition and snapshot SHA-256
fields are raw 32-byte digests and must be length-checked.

`KnowledgeSnapshot` contains no protobuf maps, floating-point fields, or
timestamps. Unknown fields at any depth are rejected and cleared before
digesting. The digest input includes normalized binary-sorted effective
authorized indexes, the bounded shadow and closed-code warning inventory, and
aggregate query-budget charges. Wall-clock admission time belongs to job
lifecycle metadata outside the digest message.
`canonical_snapshot_bytes` is computed first as the length of deterministic
`KnowledgeSnapshot` protobuf bytes with both `snapshot_sha256` and
`canonical_snapshot_bytes` cleared. The charge is then populated, the digest
form is deterministically serialized with only `snapshot_sha256` cleared, and
the compatibility framing is hashed. This two-step definition is
non-self-referential and is pinned by golden tests.
Snapshot objects are encoded in ascending `resolution_ordinal`; each stage has
its own contiguous `stage_ordinal`. Dependencies carry bounded
`topological_depth` and are encoded by a unique contiguous
`canonical_ordinal`. Lookup asset references use a contiguous `asset_ordinal`.
These ordering rules are part of canonical snapshot hashing, not permission to
trust a client-authored snapshot. Search admission always creates the snapshot
after authentication, app and index authorization, and server-side catalog
resolution.

Executable object order is stage, normalized binary name, and stable object ID.
There is no client-authored stage order. `QUARANTINED` is a terminal
administrator-recovery state and ordinary state mutation must reject it. A
separate prepare operation performs the privileged integrity scan and returns
only a single-use, ten-minute HMAC token plus bounded metadata. The quarantine
request accepts that token and a required fresh `client_request_id`; the token
binds the raw corrupt root and its bounded ordered active-dependent closure so
recovery does not decode attacker-controlled or future definition bodies.
`DELETED` is a distinct retained terminal tombstone created only by the delete
route; it is never executable and is not accepted by create or ordinary state
mutation. The current tombstoned registry identity remains the authorization
authority, so an authorized administrator can inspect its current projection
and retained historical versions under current policy. Delete does not
physically erase immutable definition or version data.
Every mutation request uses a required 16–128 byte printable-ASCII
`client_request_id`; omission is invalid rather than non-idempotent execution.
Ordinary state mutation accepts only `ACTIVE` or `DISABLED`; `DRAFT` is an
initial create state, while quarantine and delete have dedicated routes. Field
masks on update, validate, and preview are relative to
`KnowledgeObjectDefinition`, never the containing request message.

Provenance uses a typed `oneof`: authored SPL carries only its source range, a
currently authorized object carries its stable identity and definition
location, and a currently unauthorized historical object carries only its
stage, bounded object kind, and response-local fixed redacted ordinal. The
redacted wire variant cannot retain an object ID, name, version, owner, app, or
definition location.

The route comments in `knowledge_api.proto` reserve the intended endpoint
names only. They are intentionally absent from the browser-route table below
until handlers are registered. List and dependency continuations use bounded
`PageRequest`/`PageResponse` contracts; the future signed cursor must bind all
normalized filters, ordering, caller scope, page bound, and the first-page
catalog revision. Preview accepts only a retained server-authorized search-job
identity plus a candidate definition. It never accepts raw events, physical
table names, index authority, asset paths, or SQL.

Collector display-name and enabled-state mutations return a
`CollectorAdministrationSnapshot`, not a full operational `CollectorRecord`.
The snapshot is the exact durable result of the optimistic update and contains
only fleet-owned fields. This keeps a committed security-critical disable
reportable even when unrelated runtime telemetry is unavailable or corrupt;
clients may refresh `/collectors/get` separately for a current operational
projection.

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
| `/audit/events/list` | `ListAuditEventsRequest` | `ListAuditEventsResponse` |
| `/audit/search-attempts/list` | `ListSearchAttemptAuditEventsRequest` | `ListSearchAttemptAuditEventsResponse` |
| `/ingestion-tokens/create` | `CreateIngestionTokenRequest` | `CreateIngestionTokenResponse` |
| `/ingestion-tokens/get` | `GetIngestionTokenRequest` | `GetIngestionTokenResponse` |
| `/ingestion-tokens/list` | `ListIngestionTokensRequest` | `ListIngestionTokensResponse` |
| `/ingestion-tokens/update` | `UpdateIngestionTokenRequest` | `UpdateIngestionTokenResponse` |
| `/ingestion-tokens/revoke` | `RevokeIngestionTokenRequest` | `RevokeIngestionTokenResponse` |

### Audit events

`POST /api/v1/audit/events/list` is administrator-only. Tenant and owner scope
come from the authenticated browser principal and cannot be supplied on the
wire. The first contract contains successful ingestion-token, index, app, and
saved-search mutations. Actor kind, actor role, action, and target kind are
fixed enums; the projection contains no arbitrary payload, saved-search
definition, or credential material. Administrative token, index, and app
mutations require a system or browser-administrator actor. Saved-search
mutations also accept an authenticated browser-user actor; the current trusted
single-user saved-search routes record the system actor when no browser
principal is present. App deletion reports the final archived app version
rather than a fictitious post-delete generation.

Pages are ordered by descending tenant-local sequence and capped at 200 rows.
The opaque HMAC-authenticated cursor binds tenant, exact normalized filters,
page size, total-size choice, sequence boundary, and an immutable first-page
journal high-water identity. Later appends cannot enter the traversal, and a
cursor from a database state newer than a restored snapshot fails instead of
silently traversing the wrong prefix. See
[Audit events v0.1](audit-events-v0.1.md) for storage, atomicity, capacity,
redaction, and frontend requirements.

### Search-attempt audit

`POST /api/v1/audit/search-attempts/list` is administrator-only. Tenant scope
comes from the authenticated browser principal and cannot be supplied on the
wire. Its immutable projection contains only tenant-local sequence, occurrence
time, fixed actor identity, owner ID, and search-job ID. It never exposes SPL,
index or app scope, generated SQL, results, warnings, failures, headers,
credentials, or arbitrary metadata.

Pages are ordered by descending sequence, capped at 200 rows and 2 MiB, and
support exact actor-ID and owner-ID filters. The purpose-separated authenticated
cursor binds tenant, filters, page size, total-size choice, retained-range
floor, and first-page high-water identity. Appends below the rolling ceiling
cannot enter a continuation; an eviction invalidates the continuation instead
of returning an incomplete snapshot. See
[Search-attempt audit v0.1](search-attempt-audit-v0.1.md) for atomic admission,
retention, corruption, privacy, and frontend requirements.

### Index deletion

`POST /api/v1/indexes/delete` is administrator-only. Every request supplies the
original archived optimistic version and a `confirmation_name` equal to the
canonical stored index name. `KEEP_DATA` requires the index still to be
archived at that version, completes synchronously, returns only `index_id`,
hides the catalog entry behind a permanent tombstone, and retains its
ClickHouse events.

`DELETE_DATA` is asynchronous. A fresh admission requires archived version
`N`; `N = MaxInt64` is rejected because the durable transition must create
deleting generation `N+1`. Tenant scope never comes from the protobuf request:
the server binds the operation to its trusted authenticated/configured tenant.
On success, HTTP 200 returns `index_id` and a nonempty
`deletion_operation_id` after the operation is durable. A nonblocking
postcommit wake asks the deletion coordinator to reconcile promptly, while its
periodic scan provides restart recovery and is the correctness backstop.

Exact retries using the original archived version are idempotent only while
the durable operation remains outstanding. Sequential and concurrent retries,
including after process restart and through the equivalent ID or canonical
name selector, return the same operation ID. A stale version, wrong state, or
different confirmation is rejected without starting another operation.
Terminal completion consumes the outstanding operation and tombstones the
catalog entry; after that point the same request returns `404 Not Found`.
Clients must not treat this route as an indefinite completion-status or
terminal-response replay API.

### Index statistics

`POST /api/v1/indexes/stats/get` is administrator-only and accepts the same ID
or canonical-name selector as the other index administration routes. The
server first resolves a current, non-tombstoned catalog record, then captures
the largest committed storage-visibility sequence and one UTC,
millisecond-aligned `measured_at` instant. Tenant, index ID, and canonical
index name are trusted server inputs; none can be supplied or replaced by the
browser request.

`event_count`, `earliest_event_time`, and `latest_event_time` are exact for
rows in that logical scope satisfying all three measurement boundaries:
`expires_at > measured_at`, `index_time <= measured_at`, and
`visibility_seq <= visibility_cutoff`. An empty index has count and storage
bytes zero and omits both event-time bounds. A nonempty index always returns
both bounds.

Because all logical indexes share one MergeTree, attributing compressed part
bytes exactly to one tenant/index key would require an additional expensive
scan. `storage_bytes` is therefore the overflow-checked ceiling of
`active_table_bytes * event_count / active_table_rows`. It is a proportional
estimate based on active `system.parts` metadata, and `estimates` is always
true even though the logical count and time bounds are exact. Inconsistent or
racing physical counters fail closed instead of returning a misleading
estimate.

The native ClickHouse reader issues exactly one parameterized aggregate for an
empty result and one additional active-parts aggregate for a nonempty result.
Only one native statistics operation may use the shared runtime connection at
a time; concurrent saturation fails fast rather than queueing or occupying the
sessions needed by ingestion and search. The complete operation owns a
ten-second deadline, and every query is read-only with explicit execution,
memory, read, result, thread, query-size, and subquery-depth limits. The
GORM/SQLite control plane performs only catalog selector resolution; the event
and part-statistics reads remain native ClickHouse operations.

`ListIndexesRequest.include_stats` enriches only the already-filtered,
metadata-sorted catalog page, whose endpoint maximum is 64 indexes. The
GORM/SQLite control plane remains responsible for catalog filtering, ordering,
cursor validation, and pagination. The catalog has a hard ceiling of 1,024
physical index identities, including permanently retained terminal rows. One
trigger-maintained singleton holds its physical count and global revision.
Index creation checks capacity and inserts under one immediate GORM
transaction; concurrent callers cannot both consume the last slot, tombstones
do not release capacity, and physical identity deletion/replacement is
forbidden.

The signed continuation contains the catalog revision and the final
`(name|created_at|updated_at, index_id)` key. GORM applies state and literal
text filters, the matching keyset predicate, deterministic ordering, and
`LIMIT page_size + 1` in SQLite under one read-only transaction. Exact totals
are counted only when requested. No offset or full-record catalog scan is
used. Text filtering is ASCII-case-insensitive and non-ASCII-case-sensitive
across name, display name, and description; `%` and `_` remain literal.
Create, definition update, state transition, deletion admission, and terminal
tombstone insertion all invalidate an outstanding continuation.

After that page is fixed, the server captures one committed visibility cutoff
followed by one UTC millisecond measurement instant and submits the page's
trusted tenant/index scopes to one native grouped ClickHouse query. Missing
groups become explicit empty statistics. If any group is nonempty, one
additional `system.parts` aggregate provides a common proportional-storage
sample for the whole page. The operation is therefore zero queries for an
empty page, one for an all-empty page, and two otherwise; it never issues one
query per index.

Every enriched item on one page shares the same measurement instant and
visibility cutoff. A later continuation page is measured independently because
the protobuf response carries measurement time per item rather than a
list-wide immutable statistics snapshot. The signed catalog cursor binds
`include_stats`, so plain and enriched continuations cannot be replayed across
modes. Native batching shares the single-index reader's ten-second deadline
and fail-fast one-slot gate, limits scopes/groups/results to 64, and uses
explicit read, memory, output, thread, and expanded-query-size bounds. The
bounded GORM page is selected before the administrative response permit is
acquired, so a SQLite busy wait cannot occupy response capacity. The permit
then covers defensive page validation and materialization and is released
before native work begins.

`INDEX_SORT_BY_EVENT_COUNT` and `INDEX_SORT_BY_STORAGE_BYTES` remain rejected.
Correct global statistics ordering would have to measure every filtered
catalog candidate and preserve that ordering across continuation pages despite
ingestion, retention, TTL removal, and changing part metadata. It therefore
requires an immutable bounded native-statistics snapshot in addition to the
now-enforced metadata catalog ceiling, not the page-local enrichment contract.

### Index field catalog

`POST /api/v1/indexes/fields/list` is administrator-only. On every request the
server resolves the supplied stable ID or canonical name through the
GORM/SQLite index catalog before doing any native work. Resolution admits every
current, non-tombstoned catalog record: `ACTIVE`, `ARCHIVED`, and outstanding
`DELETING` records remain inspectable regardless of whether their search access
is enabled or disabled. A completed deletion is hidden by its tombstone and
returns `404 Not Found`. The resolved ID, canonical name, and current version,
not browser-provided physical scope, bind the analysis and its cursor.

`time_range`, `time_range.earliest`, and `time_range.latest` are all required.
Bounds may still use the absolute or relative SPL time syntax represented by
`TimeRangeSpec`. The initial request resolves them once to an absolute
half-open interval `[earliest, latest)`, then captures one
`SnapshotAnalysisScope`: the largest committed visibility sequence is captured
before one clock anchor supplies both the search-start and index-time cutoffs.
The server builds an empty-AST raw-event scan with its trusted tenant and the
single resolved canonical index; callers cannot supply SPL or widen that
scope. The one native ClickHouse catalog query applies tenant/index equality,
the resolved event-time interval, `index_time <= snapshot_anchor`,
`expires_at > snapshot_anchor`, and
`visibility_seq <= visibility_cutoff`.

The query buffers and validates the complete bytewise-name-sorted catalog
before returning anything. Each `FieldProfile` has exact presence,
explicit-null, and missing-event counts plus the complete sorted set of
observed durable value types for this snapshot. Known canonical fields remain
represented with zero counts for an empty index. `selected` and `interesting`
use the same deterministic field-profile rules as completed-search catalogs.
This endpoint deliberately leaves `distinct_count` absent and
`distinct_count_is_approximate` false; it neither computes nor estimates
cardinality.

One complete catalog is limited to 10,000 profiles. ClickHouse reads one extra
ordered profile as an overflow sentinel, and the server rejects the whole
analysis instead of returning a truncated catalog. Unsupported legacy or
inconsistent field metadata likewise fails the complete request. Filtering is
an optional case-sensitive UTF-8 substring match over the validated catalog.
Pages default to 100 fields and may request at most 1,000; changing page size
between continuations is allowed. `total_size` is the exact filtered count when
`include_total_size` requests it and is otherwise omitted.

Continuation tokens are authenticated, service-instance-scoped opaque values.
They bind the caller tenant and owner, resolved index ID/name/version, original
time intent, immutable analysis snapshot, name filter, cache generation, and
paging positions. An equivalent ID/name selector may continue the same record,
but an index version change, different filter or time intent, another caller,
server restart, cache expiry, or cache eviction makes the token invalid. All
pages therefore read the exact complete in-memory result created for the first
page even if ingestion, retention, TTL removal, or relative wall-clock time
changes later.

The complete catalog is stored in the bounded field-analysis LRU and
computations for the same exact snapshot key coalesce, so a cache miss performs
one ClickHouse query and continuations perform none. The default cache admits
at most 128 entries and 64 MiB for five minutes. A shared fail-fast
field-analysis gate defaults to four concurrent native computations. The
catalog query is hard-capped at fifteen seconds, 128 MiB memory, five million
source rows, 1 GiB source bytes, two threads, and a 32 MiB result; the protobuf
response has a separate 32 MiB ceiling.

The persistence boundary is deliberate: GORM is used only to resolve the
control-plane index record, while compilation and event reads use the native
ClickHouse driver only. Existing ClickHouse migration `0003` already provides
the field metadata needed by the catalog, and the runtime event-table `SELECT`
grant already covers the scan, so this endpoint adds neither a control-plane
migration nor a ClickHouse migration or grant.

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

Export-job listing is owner-and-tenant scoped and ordered by immutable
`created_at DESC, export_job_id DESC`. Pages default to and are capped at 15
jobs. The opaque continuation token is authenticated and manager-instance
scoped; it binds the caller, canonical state and exact search-job filters,
fixed ordering, keyset boundary, and the first request's admission high-water
mark. Jobs admitted later cannot enter that traversal, even if they sort after
its current boundary. A removed boundary job does not prevent continuation.

Lifecycle state remains live between calls, and a due job is expired before
state filtering. Consequently, a job may disappear from or enter a state
filter during one traversal. An exact total is the count of currently retained
matching jobs at or below that traversal's high-water for the individual
request that computes it; it is not a frozen snapshot total. Page size and
whether to compute that total may change between continuation calls, while
changing a state or search-job filter invalidates the token. List responses are
detached bounded metadata snapshots and never contain or issue download
capabilities.

The export download route is a raw `GET` file response rather than protobuf. Its short-lived path and bearer capability are returned only in `ExportDownloadGrant`; the token is sent in the `Authorization` header and never placed in a query string.

The `client_request_id` fields reserve the wire contract for future durable retry handling. The current server does not support them for search jobs, exports, saved searches, indexes, ingestion tokens, or apps: supplying the field, including an explicitly empty value, fails request validation. Create requests without the field are not deduplicated, and the server does not currently advertise an idempotency-retention window. When support is added, a key will be scoped to the authenticated caller and operation, and reuse for a different canonical request will be a conflict.

Ingestion-token metadata is a bounded operational catalog, not permanent audit
history. The hard catalog ceiling is 1,024 physical token records and 16,384
total token-to-index scope memberships. Create returns HTTP `429` only when
normal retention compaction and reclaiming revoked tombstones cannot make
enough record and scope room; the response contains no token material. Expired
credentials remain visible and consume capacity until an administrator
explicitly revokes them. Ordinary revocation always preserves the
just-revoked current tombstone and retains the newest prior tombstones, ordered
by revocation time and token ID, within the configured bound. A later create
may reclaim even the last revoked tombstone for capacity. A physically pruned
token returns `404` from get, is absent from list totals and filters, and
invalidates a list cursor whose snapshot included it. Its former plaintext
credential remains indistinguishable from every other unauthorized credential
at the collector boundary.

`IngestionRateLimits` is shared by `IndexDefinition` and
`IngestionTokenDefinition`; returned `Index` and `IngestionToken` records expose
the same stored values. Its optional `max_events_per_second` and
`max_uncompressed_bytes_per_second` fields use zero or absence to mean
unlimited. Index and token update masks accept the whole
`ingestion_rate_limits` message or either leaf, with the usual optional-field
clear semantics. The hard ceilings, accepted-event charging, durable virtual
schedule, exact-retry precedence, and collector backpressure behavior are
normative in [Ingestion rate limits v0.1](ingestion-rate-limits-v0.1.md).

Search creation always creates a job record—even parse or planning failures transition that job to `FAILED` and therefore appear in history. An ordinary or saved-search create supplies `definition`. A `SEARCH_JOB_ORIGIN_HISTORY_RERUN` create instead supplies only `source.history_search_id`: `definition` is forbidden, and the server reconstructs trusted reusable intent from the caller's owner-scoped retained history row, reauthorizes its current app/index scope, resolves relative time again, and records the immediate source history ID on the fresh job. If current-clock resolution makes a retained mixed relative/absolute range non-executable, the server returns `409 Conflict` without admitting a job. Search and export cancellation are idempotent, and an already-terminal job is returned unchanged.

Result cursors are scoped to one immutable search snapshot and one column selection. A page token must not be reused with another job or changed request parameters. Rows contain exactly one cell per schema column; a nonexistent field uses `MISSING_VALUE_MISSING`, while an explicitly present null uses `NULL_VALUE_NULL`.

## Collector stream

The native service is `open_splunk.v1.CollectorIngestService/Collect`, a bidirectional gRPC stream.

1. The collector opens the stream using TLS by default and sends `authorization: Bearer <token>` in gRPC metadata.
2. Its first frame has connection sequence `1` and a `CollectorHello`. Credentials never appear in protobuf payloads.
3. The server authenticates the token, negotiates protocol limits, and returns `CollectorReady`. Authentication/protocol failures that invalidate the stream use canonical gRPC status codes.
4. The collector sends durable `EventBatch` records and periodic `CollectorHeartbeat` frames while respecting negotiated in-flight and byte/event limits.
5. `BatchAck` and `BatchReject` are terminal dispositions. An ack may contain permanent per-event rejections; accepted, duplicate, and rejected counts must sum to the original event count. Rejected events go to the collector dead-letter output while the durable batch advances. `RetryBatch` is non-terminal and requires replay of the unchanged batch. `Throttle` applies to future sending without acknowledging anything. A quota denial sends a `RATE_LIMITED` retry for the current batch followed by a separately sequenced `TOKEN_QUOTA` or `INDEX_QUOTA` throttle for later sends; the throttle's zero limit overrides preserve the negotiated batch limits.
6. On disconnect, the collector reconnects, sends its last contiguous acknowledged sequence, and replays every unacknowledged batch with the same batch ID, sequence, event IDs, and event-ID digest.

The control plane retains at most 256 durable collector identities per tenant,
independent of the 16-collector process-liveness ceiling. At capacity, an
authenticated Hello for a previously unseen bound `collector_id` fails with
gRPC `RESOURCE_EXHAUSTED` and records neither token use nor partial fleet
state. Existing enabled identities may reconnect at capacity; disabled
identities continue to fail as disabled rather than being misreported as a
capacity failure.

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
