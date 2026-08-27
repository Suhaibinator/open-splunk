# Architecture

Open Splunk is a single-node log ingestion, search, and analytics product. A Go
server owns the control plane, the search lifecycle, ingestion admission, and
the embedded browser application. ClickHouse owns event storage and query
execution. SQLite owns bounded control-plane state and durable ingestion work.
A separate Go collector tails application logs and delivers protobuf batches to
the server over an authenticated bidirectional gRPC stream.

## Components

| Component | Responsibility |
| --- | --- |
| `open-splunk-server` | HTTPS/API serving, browser assets, authentication, catalogs, search jobs, native and HEC admission, audit, recovery, and reconciliation |
| `open-splunk-collector` | file discovery and tailing, canonical event construction, WAL/checkpoints, retry, pacing, and dead letters |
| Browser application | same-origin administration, search, analytics, and dashboard UI using generated protobuf codecs |
| SQLite | indexes, apps, tokens, collectors, saved searches, dashboards, jobs/history, knowledge, lookup metadata, audit journals, quotas, visibility reservations, outbox, and recovery authority |
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
   and index policy, validates/redacts events, and checks token and index
   virtual schedules.
3. One serializable SQLite transaction reserves visibility, stores immutable
   request identity and outbox payload, charges quotas, and records terminal
   per-event rejection information.
4. The outbox writer inserts the accepted rows into ClickHouse. Stable batch,
   request, and event identities make retries idempotent within the documented
   deduplication horizon.
5. Reconciliation resolves ambiguous ClickHouse outcomes before newer source
   work advances. Only committed visibility can become searchable.

HEC uses the same policy, quota, visibility, outbox, redaction, and ClickHouse
path. Its HTTP decoder and acknowledgment store are adapters, not alternate
storage or authority systems.

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

## Persistence boundary

Database migration numbers, entity revisions, catalog revisions, and private
serialized-format counters are implementation mechanics. They detect stale
writes and incompatible local state; they are not product versions, API
namespaces, or promises that data can move between arbitrary source revisions.
Unknown ledgers, schemas, cursors, snapshots, WAL/checkpoints, backups, and
recovery sets fail closed. The server never silently erases or rewrites
unrecognized state; development operators provision fresh state when the
source revision changes incompatibly.

SQLite schema changes are explicit SQL migrations and never GORM
`AutoMigrate`. ClickHouse access on the event path uses the native driver, not
GORM. A coordinated recovery set binds the SQLite authority and ClickHouse
backup to one generation so restore cannot mix branches.

## Security and bounded execution

- Secrets are accepted only at their transport boundary, removed before normal
  handling, returned only when a token is created, and never written to audit,
  metrics, cursors, snapshots, or event provenance.
- Audited control-plane mutation families use optimistic concurrency and commit
  their success audit record in the same SQLite transaction.
- Unknown or malformed persisted authority fails startup or the protected
  operation; the server does not continue with a partial policy or snapshot.
- Parsers, protobuf handlers, catalogs, cursors, queues, journals, bodies,
  generated SQL, backend scans, and returned data all have hard bounds.
- Generated SQL is parameterized. Public errors redact SQL, bound values,
  event data, definition bodies, and backend exception text.

The current surface is intentionally single-node and trusted-single-user at
the browser boundary. The schemas are prepared for stronger role enforcement,
but distributed coordination and general multi-user RBAC are roadmap work.
