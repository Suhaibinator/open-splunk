# Auditing

Open Splunk keeps successful control-plane mutations and high-volume search
admissions in separate bounded SQLite journals. Neither journal stores request
bodies, credentials, SPL, generated SQL, event payloads, definition bodies, or
free-form backend errors.

`SERVER_FEATURE_AUDIT_SEARCH` advertises the mutation journal list service.
`SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT` advertises the search-attempt list
service. Both list routes are administrator-only and derive tenant/actor scope
from the authenticated principal.

## Successful mutation journal

The immutable taxonomy covers successful mutations to ingestion tokens,
indexes, apps, saved searches, knowledge objects, and lookups. Representative
actions are:

- `ingestion_token.create`, `.update`, and `.revoke`;
- `index.create`, `.update`, `.activate`, `.archive`,
  `.delete_keep_data`, and `.delete_data`;
- `app.create`, `.update`, `.activate`, `.archive`, and `.delete`;
- `saved_search.create`, `.update`, `.duplicate`, and `.delete`;
- knowledge `create`, `update`, `scope_change`, `enable`, `disable`,
  `quarantine`, and `delete`; and
- lookup asset/definition create, replace/update, lifecycle, and delete
  actions defined by the protobuf enums.

An action is bound to its fixed target kind and actor domain. System and
browser-administrator actors may perform administrative work; a browser user
is valid only for the user-owned families permitted by the API. Unknown enum
values and invalid actor/action/target combinations fail closed.

Every row contains only tenant-local sequence, UTC microsecond occurrence
time, fixed actor kind/ID/role, fixed action/target kind, canonical target ID,
and committed target version plus the bounded redacted fields explicitly
declared by an additive family. There is no generic metadata/detail column.

Object and state revisions are opaque optimistic-concurrency counters. A
mutation records the exact committed revision returned by the same source
contract. These counters are not release or API versions, and deletion does not
fabricate another generation merely for audit. Audit projections must equal the
committed mutation revision.

The target mutation and success append use the same caller-owned SQLite/GORM
transaction and captured timestamp. If audit persistence fails, the complete
mutation rolls back. Rejected or failed mutations do not become successful
events. GORM is only the projection/transaction API and never runs
`AutoMigrate`; ClickHouse is not part of this journal.

Sequences are dense, monotonic, and tenant local; their journal position is
separate from the target object's revision counter. A tenant retains at most
100,000 ordinary successful mutation events.
Rows are not discarded merely to admit newer ordinary mutations. At capacity,
audited mutations fail with HTTP 429 while reads and existing objects remain
available. The dedicated knowledge recovery reserve and rolling rejected-
attempt journal are separate and cannot be consumed by ordinary actions.

Construction verifies the bounded journal, state counters, sequence range,
row shape, actor/action/target pairing, target version, and immutable high-water
identity. Corruption prevents the service from opening. Runtime appends/pages
perform bounded tail and high-water checks; a malformed encountered row fails
the page without returning a partial result.

`POST /api/audit/events/list` supports page sizes 1 through 200 (default
50), exact action filters, exact actor ID, exact target kind, and optional exact
total. Results are descending by sequence. Opaque HMAC-authenticated tokens
bind caller tenant, normalized filters, page choices, next boundary, and the
first page's high-water identity. Later appends cannot enter an existing
traversal; tampering, changed parameters, another key, or restore behind the
high-water invalidates it. Responses are capped at 2 MiB.

## Rejected privileged knowledge attempts

Knowledge management authenticates before body decode and records a bounded
redacted rejection for authenticated privileged operations that definitively
fail authorization, lookup, version/idempotency, definition validation,
dependency, resource, or service checks. The closed reasons are
`not_administrator`, `not_found_or_forbidden`, `version_conflict`,
`idempotency_conflict`, `invalid_definition`, `forbidden_dependency`,
`resource_limit`, and `service_unavailable`.

These records contain bounded scalar context only after that identity was
already authorized. They never contain regexes, paths, expressions, CSV,
event values, generated SQL, snapshots, request bodies, or attacker text. A
definitive rejection is exposed only after its synchronous journal append
succeeds. An indeterminate infrastructure failure or proven committed
idempotent receipt does not fabricate a rejection.

The attempt journal retains at most 100,000 rows per tenant and evicts exactly
the oldest before append. Its sequence is monotonic and is never reused. The
knowledge recovery journal has a separate lifetime reserve for one terminal
protective quarantine per physical identity; ordinary operations cannot
consume it.

## Search-attempt journal

One immutable payload-free event is committed when a search job is durably
admitted, before parsing or execution begins. A later parse/execution failure
or cancellation retains that one event. A request that never becomes a job is
not recorded. An exact retry of the same pending admission is idempotent and
cannot append twice.

Each row contains tenant-local sequence/time, fixed actor kind/ID/role,
canonical owner ID, search-job ID, and only the compact bounded knowledge
snapshot reference declared by the current protobuf contract. That reference
may carry digest/version/count evidence but never object/asset inventory,
definitions, SPL, index/app scope, SQL, results, warnings, request headers, or
credentials.

The pending search-history row and search-attempt event commit in the same
SQLite transaction. Audit failure rolls back admission. The journal has an
operator-configured tenant retention ceiling from 1 through 100,000, default
100,000. At capacity, append atomically removes the oldest retained row,
appends the next monotonically increasing sequence, and preserves a dense
retained range. Reopening with a different effective ceiling fails instead of
silently changing retention.

`POST /api/audit/search-attempts/list` is administrator-only and supports
exact actor-ID and owner-ID filters. Rows are descending by sequence; pages are
at most 200 rows and 2 MiB. Signed tokens bind caller, filters, page choices,
and the first page's retained range. Appends that do not evict that range cannot
enter the traversal; eviction invalidates continuation rather than returning
an incomplete historical snapshot.

## Operator behavior

Treat mutation-journal capacity as a service-capacity alert. Search attempts
are deliberately rolling and should be exported to an external audit system if
longer retention is required. A restore must use one coordinated recovery set.
Private format counters and high-water identities must match the running source
contract; unrecognized journals or cursors fail closed and are never rewritten.

Clients treat cursor strings as opaque, reset traversal whenever a filter or
page option changes, and distinguish invalid/expired/evicted traversal (400),
authentication/authorization (401/403), mutation capacity (429), and
unavailable/corrupt storage (503). CSV export, free-text search, live audit
streaming, raw-query audit, terminal search-outcome events, authentication
activity, and general RBAC audit remain outside the current baseline.
