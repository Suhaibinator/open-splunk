# Audit events v0.1

This contract is the first durable security-audit slice. It covers successful
ingestion-token creation, update, and revocation only. Authentication attempts,
token use, index changes, searches, exports, saved-object changes, and broader
RBAC activity remain future, separately bounded event families.

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

Token mutation and audit append use the same caller-owned GORM/SQLite
transaction and the same already-captured mutation timestamp. The audit store
rejects an autocommit handle or a transaction from another database. If audit
persistence fails, the token mutation rolls back; the server never reports a
changed-but-unaudited token. Rejected and failed mutation attempts do not create
successful-event rows.

Direct/internal stores default to the explicit `default` tenant and a fixed
system actor. The production runtime injects the configured tenant and requires
an authenticated actor in the context before randomness or mutation work. The
administrator middleware installs that actor only after validating the browser
principal and removes the reusable bearer credential before the handler runs.
An ordinary browser user cannot emit a successful token-mutation event.

## Bounds and failure behavior

Sequences are dense, one-based, and local to a tenant. A tenant may retain at
most 100,000 events. Events are never discarded to admit newer events. At the
ceiling, audited token creation, update, and revocation fail closed; the HTTP
administration surface maps the capacity error to `429 Too Many Requests`.
Existing credentials and read-only administration remain available. Operators
must treat approaching audit capacity as a service-capacity alert until a
future archival/retention contract exists.

Every hydrated row has a scalar byte-width preflight. Context-bounded store
construction verifies every persisted tenant's bounded journal count, dense
sequence, and every row's canonical tenant, timestamp, actor, action, target,
and version contract in fixed-size batches. A pre-existing interior gap or
malformed row therefore prevents the audit store, token store, and production
server from starting.

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
6. remove the current claim that the backend has no audit route, while making
   clear that v0.1 contains token mutations only and must not fabricate the
   broader activity families that remain unsupported.

CSV/export, live streaming, free-text search, time-range filtering, and audit
families beyond successful token mutations are not part of v0.1.
