# Audit events v0.1

This contract is the first durable security-audit slice. It covers successful
ingestion-token, index, and app administration mutations plus saved-search
changes. Authentication attempts, token use, searches, exports, other
saved-object families, and broader RBAC activity remain future, separately
bounded event families.

The closed action taxonomy is:

- `ingestion_token.create` at exactly version 1, then
  `ingestion_token.update` and `ingestion_token.revoke` at version 2 or later;
- `index.create` at exactly version 1; `index.update`, `index.activate`,
  `index.archive`, and `index.delete_keep_data` at version 2 or later; and
  `index.delete_data` at version 3 or later; and
- `app.create` at exactly version 1, then `app.update`, `app.activate`,
  `app.archive`, and `app.delete` at version 2 or later. App deletion records
  the final archived app version; deletion does not fabricate another object
  generation; and
- `saved_search.create` and `saved_search.duplicate` at exactly version 1,
  `saved_search.update` at version 2 or later, and `saved_search.delete` at
  version 1 or later. Duplication targets the new object, and deletion records
  the last retained version without fabricating another generation.

Each action is bound to its corresponding fixed `ingestion_token`, `index`,
`app`, or `saved_search` target kind. System and browser-administrator actors
are valid for every action. A browser user is valid only for saved-search
actions and remains invalid for token, index, and app administration.

## Durable record and atomicity

Migration `0022_audit_events.sql` is the SQLite schema authority. GORM is used
only as the control-plane projection and transaction API; it never runs
`AutoMigrate`, and ClickHouse is not involved.

Each immutable row contains exactly:

- tenant-local sequence and UTC microsecond occurrence time;
- fixed actor kind, canonical actor ID, and fixed actor role;
- fixed action and target kind; and
- canonical target ID and the committed target version.

There is no metadata, detail, payload, request, header, SQL, token prefix,
plaintext credential, or credential digest column. The migration rejects
unsupported actor/action/target combinations, invalid action/version pairs,
replacement, update, deletion, gaps, sequence reuse, and invalid tenant-state
transitions.

Each supported mutation and its audit append use the same caller-owned
GORM/SQLite transaction and the same already-captured mutation timestamp. The
audit store rejects an autocommit handle or a transaction from another
database. If audit persistence fails, the complete token, index, app, or
saved-search mutation rolls back; the server never reports a
changed-but-unaudited object. Rejected and failed mutation attempts do not
create successful-event rows.

The low-level audit append API can use the fixed system actor for trusted
internal work. Production administration adapters require an explicit
successful actor in the context and never use that fallback. The administrator
middleware installs the actor only after validating the browser principal and
removes the reusable bearer credential before the handler runs. The current
trusted single-user saved-search routes are intentionally unauthenticated, so
their mandatory production audit adapter uses the fixed system actor when no
actor is installed. If a future authenticated saved-search route installs a
browser user or administrator, the audit store preserves that explicit actor.

## Bounds and failure behavior

Sequences are dense, one-based, and local to a tenant. A tenant may retain at
most 100,000 events. Events are never discarded to admit newer events. At the
ceiling, audited token, index, app, and saved-search mutations fail closed; the
HTTP mutation surfaces map the capacity error to `429 Too Many Requests`.
Existing objects and read-only operations remain available. Operators must
treat approaching audit capacity as a service-capacity alert until a future
archival/retention contract exists.

Every hydrated row has a scalar byte-width preflight. Context-bounded store
construction verifies every persisted tenant's bounded journal count, dense
sequence, and every row's canonical tenant, timestamp, actor, action, target,
and version contract in fixed-size batches. A pre-existing interior gap or
malformed row therefore prevents the audit store from opening, blocks token
and audited saved-search construction, and prevents the production server from
starting.

After that startup invariant is established, append admission and ordinary
first and continuation pages perform only bounded tenant-state, current-tail,
and immutable high-water-row identity checks plus the requested `page_size +
1` read. Migration constraints and triggers prevent a valid runtime write from
changing or removing an older row. A runtime-corrupted tail or accounting row
still fails the next append/list operation; deeper offline corruption fails the
next construction audit, and any malformed row encountered by a page fails that
page without returning partial data. This keeps mutations and pages independent
of retained-journal length. An explicitly requested filtered exact total adds
one indexed count over the fixed 100,000-row tenant ceiling. Cursor validation
rejects a replay against a restored snapshot that is behind the signed
high-water or whose high-water row is missing or changed. The digest is not a
hash chain over the complete older prefix.

## Administrator list API

`POST /api/v1/audit/events/list` is administrator-only and is advertised as
`SERVER_FEATURE_AUDIT_SEARCH` only when the service is configured. Tenant and
owner identity come exclusively from the authenticated browser principal; the
request cannot select either.

The request supports:

- page sizes from 1 through 200 (default 50);
- exact action filters;
- one exact actor-ID filter;
- one exact target-kind filter; and
- an optional exact total captured on the first page.

Results are ordered by descending tenant-local sequence. Page tokens are
opaque, HMAC-authenticated with a stable purpose-separated key derived from the
server master key, and bound to tenant, normalized filters, page size,
`include_total_size`, the next sequence boundary, and the first page's journal
high-water identity. Appends after page one cannot enter or displace that
traversal. Tampering, another key, changed request parameters, or restoring a
database behind the cursor invalidates it. When requested, the exact total is
carried in the signed continuation and remains the first page's total.

Responses are capped at 2 MiB and contain only the fixed `AuditEvent`
projection. Actor kind, actor role, action, and target kind are protobuf enums.
The server defensively rejects invalid service rows, cross-tenant rows,
non-descending sequences, impossible action/version pairs, oversized
identities, malformed timestamps, and inconsistent page metadata.

## Frontend handoff

The generated TypeScript contract is ready for a backend Activity view. The
frontend team should:

1. show an Audit tab only when bootstrap advertises
   `SERVER_FEATURE_AUDIT_SEARCH`;
2. call `/api/v1/audit/events/list` with the existing authenticated protobuf
   transport and treat `next_page_token` as opaque;
3. expose exact action, actor-ID, and target-kind filters, resetting pagination
   whenever any filter, page size, or total-size choice changes;
4. render enum labels and target versions without interpreting target IDs or
   cursor contents;
5. distinguish `400` invalid/expired traversal, `401/403` authentication or
   authorization, `429` mutation capacity, and `503` unavailable/corrupt audit
   storage; and
6. render `saved_search.create`, `saved_search.update`,
   `saved_search.duplicate`, and `saved_search.delete` against the
   `saved_search` target kind without attempting to display a definition or
   source saved-search ID; and
7. retain the current disclosure that the Activity client is not yet wired to
   the existing backend audit route; once it is wired, make clear that v0.1
   contains successful token, index, app, and saved-search mutations only and
   must not fabricate broader unsupported activity families.

CSV/export, live streaming, free-text search, time-range filtering, and audit
families beyond successful token, index, app, and saved-search mutations are
not part of v0.1.
