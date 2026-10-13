# Architecture

Open Splunk is a single-node log ingestion, search, and analytics product. A Go
server owns the control plane, the search lifecycle, ingestion admission, and
the embedded browser application. ClickHouse owns event storage and query
execution. PostgreSQL owns identity, organization membership, index grants, bounded control-plane state and durable ingestion work. Private versioned MinIO stores complete query captures and export artifacts.
A separate Go collector tails application logs and delivers protobuf batches to
the server over an authenticated bidirectional gRPC stream.

## Components

| Component | Responsibility |
| --- | --- |
| `open-splunk-server` | HTTPS/API serving, browser assets, authentication, catalogs, search jobs, schedules, webhook alerts, native and HEC admission, audit, recovery, and reconciliation |
| `open-splunk-collector` | file discovery and tailing, canonical event construction, WAL/checkpoints, retry, pacing, and dead letters |
| Browser application | same-origin administration, search, analytics, and dashboard UI using generated protobuf codecs |
| PostgreSQL | indexes, apps, tokens, collectors, server settings, saved and scheduled searches, alerts, dashboards, jobs/history, knowledge, lookup metadata, audit journals, quotas, visibility reservations, outbox, and recovery authority |
| GoCommon | Google OAuth, HttpOnly sessions, users, organization hierarchy and membership invariants |
| Private MinIO | complete captures and streaming exports, referenced by immutable object version |
| ClickHouse | immutable event rows, expiration metadata, field metadata, and bounded SPL query execution |

The Next.js application is statically exported and embedded in the Go server.
Production browser HTTP and WebSocket traffic is same-origin. API, HEC, and
WebSocket paths are registered before the static application fallback; an
unknown protected path never becomes an HTML success.

## Authority and data flow

The server derives tenant, owner, role, app, and index authority from the
authenticated principal and current control-plane state. Request bodies may
narrow an authorized scope but cannot create one. SPL, selectors, HEC metadata,
collector labels, cursors, snapshot summaries, and retained history are never
authorization grants.

Native ingestion follows this sequence:

1. The collector reads a file, builds a canonical event, and writes the batch
   to its local WAL before sending it.
2. The server authenticates the token and collector identity, refreshes token
   and index policy, validates events, and checks token and index virtual
   schedules. Explicit collector redaction has already run before the local WAL.
3. One serializable PostgreSQL transaction reserves visibility, stores immutable
   request identity and outbox payload, charges quotas, and records terminal
   per-event rejection information.
4. The outbox writer inserts the accepted rows into ClickHouse. Stable batch,
   request, and event identities make retries idempotent within the documented
   deduplication horizon.
5. Reconciliation resolves ambiguous ClickHouse outcomes before newer source
   work advances. Only committed visibility can become searchable.

HEC uses the same policy, quota, visibility, outbox, and ClickHouse path. Its
HTTP decoder and acknowledgment store are adapters, not alternate storage or
authority systems.

Search follows this sequence:

1. The server authenticates the caller, resolves the app and permitted indexes,
   resolves one half-open event-time interval, and captures index-time and
   visibility cutoffs.
2. It parses the current authored SPL contract, resolves an immutable knowledge
   and lookup snapshot, constructs a typed logical plan, and compiles bounded
   parameterized ClickHouse SQL.
3. A job executes under explicit duration, memory, scan, group, result-row, and
   result-byte limits. Atomic operators buffer and validate the complete result
   before publication.
4. Result pages, timeline, field analysis, inspection, history, and export use
   the retained job authority. A history rerun is new admission under current
   authorization and current knowledge; historical metadata is provenance only.

Scheduled reports and alerts reuse that admission path. A scheduler persists
each occurrence claim and snapshots the saved definition, schedule period, and
retention policy before launching a job. Report runs retain result authority;
sharing is permitted only after a complete, unexpired artifact exists. An alert
evaluates only an exact result (or a provable lower-bound condition), extends
the job retention before delivery, and signs one bounded HTTPS webhook attempt.
Schedules, sharing, alert state, encrypted destinations, signing-key
generations, run history, and feature-operation audit records remain in PostgreSQL.

## Persistence boundary

Database migration numbers, entity revisions, catalog revisions, and private
serialized-format counters are implementation mechanics. They detect stale
writes and incompatible local state; they are not product versions, API
namespaces, or promises that data can move between arbitrary source revisions.
Unknown ledgers, schemas, cursors, snapshots, WAL/checkpoints, backups, and
recovery sets fail closed. The server never silently erases or rewrites
unrecognized state; development operators provision fresh state when the
source revision changes incompatibly.

PostgreSQL schema changes are explicit SQL migrations and never GORM
`AutoMigrate`. ClickHouse access on the event path uses the native driver, not
GORM. A coordinated recovery set binds the PostgreSQL authority and ClickHouse
backup to one generation so restore cannot mix branches.

Accepted ingestion is split into durable logical batches and physical
ClickHouse write groups. PostgreSQL retains each batch's identity, quota decision,
response metadata, outbox, HEC acknowledgment state, and visibility sequence.
The server seals an ordered group with a stable deduplication token before any
storage side effect, marks the complete group ambiguous before its one
synchronous insert, and commits every member atomically afterward. Startup and
recovery always resolve the oldest ambiguous group before newer ready or
ungrouped work. See [Insert coalescing](insert-coalescing.md) for the complete
protocol and limits.

## Security and bounded execution

- Secrets are accepted only at their transport boundary and removed before
  normal handling. One-time plaintexts are returned only by the operation that
  creates or rotates them, including ingestion tokens, alert signing keys, and
  short-lived export grants. Alert endpoints and signing keys are encrypted at
  rest. Secrets are never written to audit, metrics, cursors, snapshots, or
  event provenance.
- Audited control-plane mutation families use optimistic concurrency and commit
  their success audit record in the same PostgreSQL transaction.
- Unknown or malformed persisted authority fails startup or the protected
  operation; the server does not continue with a partial policy or snapshot.
- Parsers, protobuf handlers, catalogs, cursors, queues, journals, bodies,
  generated SQL, backend scans, and returned data all have hard bounds.
- Generated SQL is parameterized. Public errors redact SQL, bound values,
  event data, definition bodies, and backend exception text.

The current surface is single-node with authenticated users, organizations and per-index access rules. Distributed coordination remains separate work.


## Account and index authority

GoCommon is composed into the server through the pinned `third_party/go-common`
module snapshot. The browser uses its actual compiled core client from
`third_party/go-common-core` while retaining OpenSplunk CSS and primitives.
Google sign-in creates an ordinary user with no team memberships. GoCommon always
provisions its managed personal workspace; OpenSplunk filters organization lists
to its team type and grants no index authority through that workspace. Siteadmin type 44
is operator-assigned, with unconditional organization/index access. Ordinary
users need active organization membership before role or individual index
rules apply. Viewer, Admin and Owner roles are 1, 23 and 44. Deny wins over allow.

Organization context accompanies cookie-authenticated HTTP requests and socket
handshakes. Access is checked before query planning, retained reads, mutation,
background execution and export acquisition. The browser clears private state
on user or organization changes. MinIO object keys never confer user access;
large downloads stream through an authenticated gateway. PostgreSQL stores
metadata and manifests, while MinIO stores full captures and exports; bounded
browser previews are explicitly distinguished from complete captured results.


The PostgreSQL/GoCommon integration remains blocked on the
[granular locking refactor](roadmap.md#postgresql-integration-release-blocker).
GoCommon borrows the application pool without a transaction-start interceptor.
OpenSplunk still owns the global metadata writer lock and statement triggers;
these must be replaced with invariant-scoped locking/constraints and a consistent
order for identity lifecycle callbacks before release. Existing serial execution
checks do not establish those concurrency guarantees.
