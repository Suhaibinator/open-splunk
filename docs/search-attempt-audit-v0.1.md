# Search-attempt audit v0.1

This contract is the first high-volume activity-audit family. It records one
immutable, payload-free event when a search job is durably admitted, before
parsing or execution begins. Parse failures, execution failures, and later
cancellation therefore retain the same single admission event. Requests that
never become a search job are not search attempts under this contract.

Search attempts do not share the permanent mutation-audit capacity. The
mutation journal remains the durable authority for successful token, index,
app, and saved-search changes. Search volume instead uses its own per-tenant
rolling journal with a configurable retained-row ceiling, defaulting to
100,000 and never exceeding 100,000. At the ceiling, admitting one new event
atomically removes exactly the oldest retained event and appends the new one.
Sequences remain monotonic and are never reused; the retained sequence range
is dense. Operators configure the production ceiling with
`--search-attempt-audit-maximum-retained-attempts`, which accepts 1 through
100,000 and defaults to 100,000. Reopening with a different effective ceiling,
including omitting a previously configured custom value and thereby reverting
to the default, fails before the server starts rather than silently changing an
existing tenant's retention contract. Operators must repeat the custom value
on later starts.

## Durable record and atomicity

Migration `0023_search_attempt_audit.sql` is the SQLite schema authority.
GORM is used only for the SQLite control-plane projection and transaction API;
ClickHouse is not involved and GORM `AutoMigrate` is never used.

Each row contains only:

- tenant-local monotonic sequence and UTC microsecond occurrence time;
- fixed actor kind, canonical actor ID, and fixed actor role;
- canonical owner ID; and
- canonical search-job ID.

There is no SPL, normalized query, index or app scope, saved-search ID,
generated SQL, result schema or row, progress, warning, error detail, request
body, header, credential, or arbitrary metadata column. The current trusted
single-user search route records the fixed `open-splunk-server` system actor.
If a future authenticated route installs a validated browser actor, that user
or administrator is retained instead.

The search-history pending row and search-attempt audit row are inserted in
the same caller-owned GORM transaction. Audit failure rolls the pending row
back and search-job admission fails closed. An exact retry of an already
admitted pending attempt is idempotent and cannot append a second event;
conflicting or terminal ID reuse appends nothing.

## Retention and traversal

The per-tenant state stores the configured retained ceiling, first retained
sequence, next sequence, and retained count. SQLite constraints and triggers
permit only the exact oldest-row eviction required by a full journal and the
exact next-sequence insertion. Construction verifies every retained tenant
range and record before the service starts.

Administrator traversal is descending by sequence and supports exact actor-ID
and owner-ID filters. Page tokens are opaque, purpose-separated,
HMAC-authenticated, and bound to tenant, filters, page size, total-size choice,
and the first page's retained range. Appends below the ceiling cannot enter a
continuation. A rolling eviction invalidates an older continuation rather than
silently returning an incomplete snapshot. Pages are capped at 200 rows and
2 MiB.

`POST /api/v1/audit/search-attempts/list` is administrator-only. Tenant
identity comes from the authenticated browser principal and never from the
protobuf request. Bootstrap advertises
`SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT` only when the complete service is
configured.

## Frontend handoff

The generated TypeScript contract is ready for an administrator Activity view.
The frontend team should:

1. show a Search attempts surface only when bootstrap advertises
   `SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT`;
2. call `/api/v1/audit/search-attempts/list` with the authenticated protobuf
   transport and treat every page token as opaque;
3. expose exact actor-ID and owner-ID filters, resetting pagination whenever a
   filter, page size, or total-size choice changes;
4. render occurrence time, actor, owner, and search-job ID only, without
   attempting to recover query text from history or another endpoint; and
5. distinguish `400` invalid/evicted traversal, `401/403` authentication or
   authorization, and `503` unavailable or corrupt storage.

Search terminal-outcome events, raw-query audit, free-text filtering, export
activity, authentication activity, and live audit streaming remain separate
future contracts.
