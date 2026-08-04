# Open Splunk: Product and Architecture Plan

**Status:** Initial planning draft  
**Date:** July 21, 2026  
**Working title:** “Open Splunk” is used in this document as a project name, not as a claim of affiliation or compatibility certification.

## Executive summary

Open Splunk will be a self-hosted log investigation and analytics product built around the parts of Splunk that make it unusually effective: a dense search workspace, event-first exploration, a pipe-oriented query language, fast statistical transformations, multiple logical indexes, and a lightweight collector that can be placed beside an application.

The first release is not intended to reproduce every Splunk subsystem. It should instead be a coherent vertical product that can:

1. collect structured and unstructured application logs reliably;
2. route those logs into one or more logical indexes;
3. preserve the familiar Splunk event model (`_time`, `_raw`, `index`, `host`, `source`, and `sourcetype`);
4. execute a carefully defined, useful subset of SPL against ClickHouse;
5. present the results in a search experience that feels immediately familiar to a Splunk user; and
6. fail explicitly when a command or semantic edge case is not yet supported.

The implementation will use:

- **Go** for the collector, ingestion service, SPL parser/compiler, query execution, search jobs, and administrative APIs;
- **TypeScript, React, and Next.js static export**, embedded into the Go server binary at build time, for the browser application;
- **ClickHouse** for canonical event storage and analytical query execution; and
- **SQLite** for the single-node control plane: index definitions, ingestion tokens, saved searches, dashboards, settings, and other mutable metadata.

OpenTelemetry will not be used for log collection in the target architecture. The current GradeThis OpenTelemetry pipeline is a useful source of lessons and migration data, but it will be replaced by the first-party **Open Splunk Collector** and first-party ingestion API.

Browser-to-server APIs will follow the established GradeThis pattern: SRouter routes with protobuf request and response codecs, generated Go messages on the server, and generated TypeScript messages in the client. Collector-to-server ingestion will use a separate protobuf-defined gRPC service.

## Product intent

The product promise should be narrow enough to be credible and broad enough to be genuinely useful:

> Search and analyze application logs with the SPL workflow people already know, while ClickHouse performs the storage, filtering, aggregation, and time-series work.

The first product should optimize for operational investigation:

- find a request by trace ID;
- inspect errors for one service or environment;
- pivot from an event field into a narrower search;
- aggregate error counts by route, host, or exception;
- graph latency and error volume over time;
- extract a field from JSON or text and immediately group by it;
- save a useful search and return to it; and
- keep data from several applications isolated in predictable indexes.

This implies a stronger bar than “an SPL-to-SQL demo.” The collector, index routing, time semantics, field discovery, event rendering, job cancellation, resource limits, and error messages are all part of the product.

## Scope and compatibility posture

### What “Splunk-like” means

Open Splunk should reproduce the interaction model and information density that make Splunk Search & Reporting effective:

- an app/index context;
- a prominent SPL search bar and time-range picker;
- a search job with progress, cancellation, duration, and result counts;
- a left rail for selected and interesting fields;
- a timeline histogram;
- Events, Statistics, and Visualization views;
- expandable raw events with field-value inspection;
- quick pivots such as “add to search” and “exclude from search”; and
- familiar concepts such as saved searches, reports, dashboards, and alerts.

The visual language should be original. We should not copy proprietary assets, icons, source code, exact layouts, or branding. “Familiar to a Splunk user” is a sound design objective; “indistinguishable from Splunk” creates unnecessary legal, accessibility, and maintainability risk. The project name and public compatibility claims should receive a trademark and product-counsel review before a public launch.

### What SPL-compatible means

SPL compatibility must be a written contract, not an impression. Each supported command needs documented syntax, type behavior, null behavior, ordering assumptions, and examples. Unsupported syntax should produce a precise error with a location and suggested alternative.

For example:

```text
unsupported command "transaction" at pipeline stage 3 (line 1, column 47)
```

The implementation should never silently reinterpret an unsupported SPL construct as approximately equivalent SQL.

## What the neighboring repositories tell us

The current GradeThis and go-common code provides a concrete first integration target.

### Current GradeThis log path

GradeThis currently uses a structured Zap logger from `go-common/pkg/logger`. Its production-style JSON encoder emits a stable core that includes fields such as:

- `timestamp`
- `level`
- `logger`
- `caller`
- `message`
- `stacktrace`
- `layer`
- `trace_id`

Request logs add useful fields such as `method`, `path`, `status`, `duration`, `bytes`, `ip`, and `user_agent`. Service code adds many domain-specific IDs and operation fields. Frontend telemetry is collected through the existing Faro endpoint and is eventually represented in backend JSON logs with application, browser, page, session, and trace fields.

Today, the local Docker path is roughly:

```text
GradeThis Zap JSON file
  -> OpenTelemetry Collector filelog receiver
  -> OpenTelemetry transformation/resource mapping
  -> OpenTelemetry ClickHouse exporter
  -> logs.otel_logs
  -> a narrow parsed_logs materialized view
  -> Grafana dashboards
```

The target path will be:

```text
GradeThis Zap JSON file
  -> Open Splunk Collector
  -> Open Splunk gRPC ingestion service
  -> normalized event batch
  -> ClickHouse open_splunk.events
  -> SPL search service
  -> Open Splunk search UI
```

### Lessons to preserve

- The application already emits newline-delimited structured JSON; the first collector integration does not require application logging changes.
- Trace correlation is already a first-class convention and must remain easy to search.
- Logger names and `layer` values provide useful low-cardinality dimensions.
- HTTP request logs already contain the fields needed for useful latency, status, and route investigations.
- The existing logs demonstrate why dynamic fields matter: business services add fields that cannot all be anticipated in a central table schema.

### Problems to correct

- The current collector configuration hardcodes one service name and one destination table, which does not generalize to multiple applications and indexes.
- The OpenTelemetry schema stores dynamic log attributes as strings, which weakens original JSON type fidelity for SPL comparisons and statistics.
- The current `parsed_logs` materialized view promotes only a small fixed field set.
- The current ClickHouse and collector containers use floating `latest` tags. Open Splunk should pin tested versions and make schema migrations explicit.
- Actual log samples include security-sensitive field names such as `token`, alongside user identifiers and contact data. Collection must have a centrally testable redaction and retention policy before the product is used with production logs.
- File paths in the logger profile and Docker collector mount should be reconciled during migration; the collector must report clearly when a configured input is absent or unreadable.

## Target architecture

```mermaid
flowchart LR
    subgraph Sources["Applications and hosts"]
        GT["GradeThis JSON logs"]
        GC["go-common applications"]
        TXT["Other text or JSON logs"]
    end

    subgraph Edge["Open Splunk Collector (Go)"]
        INPUT["File inputs"]
        PARSE["Framing, parsing, enrichment, redaction"]
        WAL["Durable local queue and checkpoints"]
        SEND["Protobuf/gRPC batch sender"]
        INPUT --> PARSE --> WAL --> SEND
    end

    subgraph Backend["Open Splunk server (Go)"]
        INGEST["Authenticated gRPC ingestion"]
        NORMALIZE["Validation, index routing, normalization"]
        SEARCH["SRouter protobuf API + search WebSocket"]
        SPL["SPL parser, logical plan, ClickHouse compiler"]
        ADMIN["SRouter protobuf control API"]
        INGEST --> NORMALIZE
        SEARCH --> SPL
    end

    CH[("ClickHouse events")]
    META[("Control-plane store")]
    UI["TypeScript search UI"]

    GT --> INPUT
    GC --> INPUT
    TXT --> INPUT
    SEND -->|"bidirectional gRPC stream"| INGEST
    NORMALIZE --> CH
    SPL --> CH
    ADMIN --> META
    UI -->|"protobuf HTTP + binary WebSocket"| SEARCH
    UI -->|"protobuf over HTTP"| ADMIN
```

The collector must not receive ClickHouse credentials or insert directly into ClickHouse. A server-side ingestion boundary gives us one place for token validation, index authorization, schema normalization, size limits, acknowledgments, audit logging, and future compatibility endpoints.

### Suggested monorepo shape

```text
open-splunk/
  app/                        # Root Next.js App Router source
  cmd/
    open-splunk-server/       # Go API and search service
    open-splunk-collector/    # Go edge collector
    open-splunk-loggen/       # Load and correctness test generator
  proto/
    open_splunk/v1/           # Browser APIs, shared models, collector gRPC
  gen/
    go/                       # Generated Go protobuf and gRPC code
    ts/                       # Generated ts-proto messages
  internal/
    collector/                # Inputs, framing, checkpoints, WAL, outputs
    ingest/                   # Auth, validation, normalization, batching
    indexes/                  # Index catalog and routing policy
    spl/                      # Lexer, parser, AST, semantic analysis
    plan/                     # Typed logical operators and optimizer
    clickhouse/               # SQL compiler, executor, migrations
    searchjobs/               # Lifecycle, progress fanout, cancellation, limits
    auth/                     # Users, roles, sessions, ingestion tokens
    savedobjects/             # Searches, reports, dashboards, alerts
    control/                  # SQLite connection, migrations, transactions
    export/                   # CSV/JSONL export jobs and artifacts
  migrations/
    clickhouse/
    sqlite/
  configs/
    examples/
  docs/
  out/                        # Generated Next.js export embedded by webui.go
  public/                     # Static Next.js source assets
  deploy/
    docker-compose.yaml
  next.config.ts
  package.json
  webui.go                    # Root Go package with //go:embed all:out
```

Package boundaries can evolve, but the SPL parser, logical planner, and ClickHouse SQL emitter should remain distinct packages. That separation is the architectural hinge of the project.

## Open Splunk Collector

The collector is a first-party Go daemon whose job is to turn local log sources into acknowledged, index-routed event batches without becoming part of the application’s failure path.

### Initial input support

The first production slice should support file monitoring well:

- one or more include globs;
- explicit exclude globs;
- newline-delimited JSON and raw line modes;
- configurable multiline framing;
- `start_at: beginning|end` for first discovery;
- rotation by rename/recreate;
- copy-truncate handling;
- file identity based on platform identifiers plus a content fingerprint;
- per-input `index`, `source`, `sourcetype`, `host`, and constant fields; and
- timestamp extraction with a documented fallback to collection time.

Container stdout, journald, syslog, Windows Event Log, and Kubernetes inputs are valuable later, but should not dilute the reliability work required for file collection.

Each active file poll must be transactional with respect to an observable file
generation. The collector captures a bounded private snapshot containing the
prior contiguous trailing fingerprint guard, the new raw bytes, and any framing
lookahead; frames it privately; and publishes only after an exact reread proves
every dependency byte unchanged. Cursor advancement and installation of the
complete bounded trailing guard occur before publication while one manager-wide
staged-transaction permit remains held through backpressure, bounding aggregate
staged memory. Byte mismatch or a short exact read is affirmative rewrite
evidence and starts a new generation; unrelated I/O or framing failures do not.
Read windows may grow adaptively to reach a frame boundary but remain
proportional to the configured event bound, and dense snapshots retain a fixed
event-count ceiling. A source requires two consecutive complete discovery
misses before provisional retirement; rediscovery cancels that versioned
request. Final retirement privately flushes a trailing partial frame, then
requires one last exact validation and a finite EOF probe before publishing it.
Writes through a retained descriptor after that finite boundary are outside the
cross-platform guarantee because portable file APIs cannot prove that no writer
will append in the future.

### Processing pipeline

Each input should pass through a small, ordered processor chain:

1. frame one event, including multiline assembly;
2. preserve the original bytes as `_raw` within configured size limits;
3. decode JSON when the sourcetype expects it;
4. extract canonical timestamp, severity, message, trace ID, and span ID fields;
5. attach collector, host, app, environment, source, sourcetype, and index metadata;
6. apply allow-list, deny-list, rename, and redaction rules;
7. assign a stable event ID; and
8. append the normalized envelope to the local durable queue.

JSON numbers, booleans, arrays, nulls, and nested objects should remain typed. Flattening rules must be deterministic and reversible enough that dotted SPL field access behaves predictably.

### Delivery semantics

The collector should provide **at-least-once delivery**:

- append framed events to a segmented disk-backed queue before transmission;
- batch by byte size, event count, and maximum delay;
- send protobuf batches over a long-lived gRPC stream with standard gRPC compression;
- retry transient failures with bounded exponential backoff and jitter;
- respect gRPC flow control plus explicit server throttle/retry responses;
- advance file checkpoints only after the corresponding batch is durably acknowledged;
- retain unacknowledged segments across restarts; and
- expose queue depth, oldest-event age, sent, retried, rejected, and dropped counts.

The server and ClickHouse writer should use stable batch and event IDs so identical retries can be deduplicated within a documented window. “Exactly once” should not be promised; predictable at-least-once behavior plus idempotent retry handling is the honest contract.

### Ingestion protocol

Splunk HEC—**HTTP Event Collector**—is Splunk's token-authenticated HTTP API for sending events. Its common event envelope contains fields such as `time`, `host`, `source`, `sourcetype`, `index`, `event`, and `fields`. Supporting it would let some existing Splunk-compatible producers send data to Open Splunk without using our collector.

HEC compatibility is not required for the initial release. The first-party collector and server will use a versioned, bidirectional gRPC stream that provides strong batch acknowledgments, connection-level flow control, explicit application backpressure, and typed nested fields. A later HTTP compatibility facade can expose HEC-shaped endpoints without constraining the native protocol.

The eventual design therefore has two surfaces:

- a native protobuf/gRPC service used by the first-party collector; and
- a compatibility facade shaped like Splunk HTTP Event Collector for existing tools that already emit `time`, `host`, `source`, `sourcetype`, `index`, `event`, and `fields`.

The compatibility endpoint broadens adoption without making the collector depend on OpenTelemetry, but it belongs after the native path is reliable.

The native contract should use a bidirectional stream so one connection carries registration, event batches, heartbeats, acknowledgments, permanent rejections, and server throttle instructions:

```proto
service CollectorIngestService {
  rpc Collect(stream CollectRequest) returns (stream CollectResponse);
}

message CollectRequest {
  oneof payload {
    CollectorHello hello = 1;
    EventBatch batch = 2;
    CollectorHeartbeat heartbeat = 3;
  }
}

message CollectResponse {
  oneof payload {
    CollectorReady ready = 1;
    BatchAck batch_ack = 2;
    BatchReject batch_reject = 3;
    Throttle throttle = 4;
  }
}
```

Every `EventBatch` carries a stable batch ID, collector identity, protocol version, and ordered events. The server sends `BatchAck` only after the promised ClickHouse durability point. Retryable failures leave the batch unacknowledged; permanent event validation failures return structured field-level details so the collector can move rejected events to a local dead-letter file instead of blocking its entire queue.

After immutable hard-envelope validation has succeeded and the server has computed the stable batch fingerprint, every permanent whole-batch response must be durably recorded against that identity and replayed exactly before reevaluating mutable policy. Credential and lease authorization and immutable hard-envelope checks remain stronger gates and may still deny a retry. An existing unresolved pending disposition or an identity conflict with one remains retryable until the first durable terminal outcome is known; it must not be converted into a new permanent rejection.

Arbitrary log values need a custom protobuf `oneof` rather than `google.protobuf.Struct`, because `Struct` represents every number as a double and cannot preserve all integer values exactly. The shared contract should model strings, signed and unsigned integers, doubles, booleans, nulls, bytes, lists, and nested objects explicitly.

Collector tokens travel in gRPC metadata and remain scoped to allowed indexes. TLS is the production default. Plaintext gRPC may be enabled only by an explicit local-development setting; mutual TLS can follow when deployments are no longer confined to a trusted network.

### Configuration sketch

```yaml
server:
  address: logs.example.internal:8443
  transport: grpc
  token_file: /etc/open-splunk/ingest-token
  tls:
    enabled: true
    ca_file: /etc/open-splunk/ca.pem
  compression: gzip

state:
  directory: /var/lib/open-splunk-collector
  max_queue_bytes: 10GiB

inputs:
  - id: gradethis-backend
    type: file
    include:
      - /var/log/gradethis/*.log
    exclude:
      - "*.gz"
    format: ndjson
    start_at: end
    index: gradethis
    source: gradethis-backend
    sourcetype: go:zap:json
    fields:
      service: gradethis
      environment: production

processors:
  - type: redact
    fields: [token, authorization, password, session_token]
    replacement: "[REDACTED]"
```

Environment substitution, secret-file loading, configuration validation, and a `collector validate` command should be available from the beginning. Tokens must never be printed by config dumps or diagnostics.

## Ingestion service

The Go gRPC ingestion service owns the trust boundary between collectors and storage.

Its responsibilities are:

- authenticate hashed ingestion tokens;
- authorize each token for specific indexes;
- reject unknown, disabled, or over-quota indexes;
- enforce request, batch, event, field-count, nesting, and field-name limits;
- validate timestamps and apply a policy for implausibly old or future events;
- normalize canonical metadata without discarding the original event;
- apply mandatory server-side redaction as defense in depth;
- batch ClickHouse inserts efficiently;
- acknowledge only the durability level promised by the protocol;
- publish collector heartbeats and lag information; and
- return machine-actionable partial rejection details.

The ingestion API should be independently scalable later, but the MVP can run it in the same server process as the search and administrative APIs.

The first-release rate-accounting, retry, and throttle semantics are normative
in [Ingestion rate limits v0.1](ingestion-rate-limits-v0.1.md).

The first deployment is single-user on a trusted network, so end-user authentication and RBAC are not release blockers. Collector ingestion tokens remain necessary: even on a trusted network, they prevent accidental cross-index writes and establish a protocol that can be hardened later. SQLite is sufficient for this single-node control plane and should run in WAL mode with explicit backup and migration tooling. Checked-in SQL migrations remain the sole schema authority, while explicit GORM models make control-plane keys, relationships, constraints, and bounded projections legible in Go. `AutoMigrate` is not used in production. Narrow raw SQL remains appropriate for SQLite transaction modes and conditional compare-and-swap or fencing operations that GORM cannot express safely or efficiently.

The deployment recovery contract is offline, coordinated, and release-exact.
With the server stopped, `backup-deployment-recovery-set` creates an unchanged,
independently verifiable control-plane bundle and a ClickHouse-native archive
within one quiescent operation. A strict canonical outer manifest binds the
control-plane child manifest and external archive by exact name, size, SHA-256,
release and migration identities, source ClickHouse UUID provenance, maximum
visibility sequence, backup operation UUID, and release-owned archive-marker
table UUID. The networkless
`verify-deployment-recovery-set` command recursively verifies both members and
their ownership metadata, so renaming, swapping, truncating, or combining
members from different recovery sets fails closed.

The archive root and every archive have exact descriptor-revalidated UID, GID,
mode, special-bit, single-link, and no-extended-ACL contracts. Normal backup,
verify, and restore helpers mount retained archives read-only. If a native
archive was created but outer publication later fails, the backup attempts an
exact pinned rollback; an ownership failure is recoverable only through a
separate networkless UID-`101` destructive one-shot. That tool cannot infer
publication status: with both ClickHouse and the server stopped, an operator
must establish that the archive belongs to the failed attempt and repeat its
canonical name as an explicit deletion confirmation. It mounts no state or
secret, revalidates the pinned root/file contract immediately before unlink,
syncs and proves final absence, and cannot be confused with normal backup
execution.

Once outer publication has occurred, any later close, sync, identity, or final
verification failure is explicitly classified as a published-but-ambiguous
result. Operators must preserve both members and independently verify the set;
the archive-owner deletion path is permitted only for a proven
pre-publication failed attempt.

An interrupted backup may instead retain its exact source marker. Recovery is
an explicit, confirmation-bound operation that uses the backup principal only:
it takes the deployment singleton lock before credentials or network access,
validates the canonical source and exact repeated recovery-set/operation
identity, and clears only that singleton marker synchronously. The server and
ClickHouse must both be stopped and only ClickHouse restarted before this
operation. The host lock fences server and helper processes, but cannot fence a
native `BACKUP` already executing inside ClickHouse; the restart terminates
that old server-side operation. Reconciliation mounts no recovery archive,
queries no backup status, and deletes no archive. Operators separately inspect
`system.backups`, the exact marker, and retained archive inventory before
invoking it.

`restore-deployment-recovery-set` accepts only a verified set and fresh
ClickHouse and control-plane targets. A persistent Compose binding maps the
four fresh data/state volume keys for the full restored-deployment lifetime; a
separate restore-only overlay retains the same recovery named volume but mounts
it read-only into ClickHouse;
the restore principal proves the exact configured disk name, path, and
read-only state through `system.disks` before native restore. The reserved
`open_splunk*` namespace must be absent for a fresh attempt, and ClickHouse
restores directly into the canonical `open_splunk` database. The command then
re-verifies the complete recovery set, requires the exact original manifest and
archive digest, and validates the complete release-owned schema, migration
ledger, visibility boundary, and inactive-mutation state. Both one-shot
principals have `SHOW TABLES` only on `open_splunk.*`, making
administrator-owned extra tables visible to exact source and restored-schema
validation without granting row access.
The backup writes an exact recovery-set/backup-operation marker immediately
before native `BACKUP`, so that marker is physically carried inside the
archive and is removed from the live source afterward. Restore requires that
exact marker before receipt publication, records the fresh restored UUIDs and
marker-table UUID in a durable receipt bound to the outer-manifest digest, then
revalidates the receipt, consumes the marker synchronously, and proves marker
absence before accepting the canonical database as complete. This intrinsically
binds the archive ClickHouse actually consumed to the verified outer set even
when mount aliases or filenames are adversarially substituted.
Source UUIDs remain provenance: a native `RESTORE ... AS` creates new database
and table UUIDs, and the receipt makes those restored UUIDs the retry identity.
Native restore is not transactional; an unreceipted or mismatched canonical
database requires a fresh data volume. Only an exact receipted canonical
database may resume, either consuming the still-exact marker after a
receipt-before-cleanup interruption or requiring it already absent. There is no
staging database, rename, or promotion boundary. The restore session is closed
after the canonical database and receipt are fully revalidated, before
the SQLite snapshot, master key, and administrator token are published in
their own resumable order. Before any ClickHouse connection or mutation, a
read-only control-target preflight validates the complete resumable prefix and
binds the exact empty database-lock pathname to the descriptor actually held
by the recovery process. That descriptor/path identity and its owner-only,
single-link metadata are revalidated at every control publication boundary.
Before any restore mutation, a bounded enumeration of the complete
`open_splunk*` database namespace admits only absence for a fresh restore or the
exact canonical database for a receipt-gated retry, and rejects every
foreign/archive alias or unexpected prefixed database. The server, backup,
marker-reconciliation, and restore containers also share one image-seeded
retained singleton lock volume, independent of a rebound `server-state` volume;
restore therefore cannot proceed while the original server still owns the
deployment. Its configured private directory and empty lock inode have exact
owner, mode, single-link, ACL, size, and pathname-identity checks at runtime. Both
control-plane preflight and final restore require the child recovery-set ID and
canonical child-manifest SHA-256 recorded by the outer set, preventing a
same-release child swap after ClickHouse completion. Native restore requires
the one-shot restore principal to hold `INSERT` on each exact destination table;
that unavoidable authority is confined to the four canonical table names, and
the credential is rotated after the operation. Revocation is not treated as
durable because restart initialization intentionally recreates the principal
and its exact grants.
Export artifacts remain outside the recovery set
and can be recreated from their source searches. The lower-level
`backup-control-plane`, `verify-control-plane-backup`, and
`restore-control-plane` commands remain available for control-plane-only
maintenance, but their bundles are not deployment backups and must not be
paired manually with unrelated ClickHouse state.

The ingestion-token catalog is deliberately bounded rather than an unbounded
audit log. The production default and hard structural ceiling both admit at
most 1,024 physical token records, and the catalog admits at most 16,384 total
token-to-index scope memberships. Those ceilings bound list allocation and
detect corrupt catalogs. Creation returns a capacity response only when normal
retention compaction and reclaiming revoked tombstones cannot make enough
record and scope room; expiry alone does not delete administrator-visible
metadata. An operator can explicitly revoke an obsolete or expired credential
and retry creation without ever deleting an active, disabled, or merely
expired credential. Ordinary revocation always preserves the just-revoked
current tombstone and fills the configured retention bound with the newest
prior tombstones, ordered deterministically by revocation time and token ID.
A later create that needs capacity may reclaim even the last tombstone. Each
prune deletes the victim's scope rows in the same immediate GORM transaction.
Pruned IDs disappear from administrator get/list results while their
credentials remain unauthorized. This lifecycle belongs only to the
GORM-backed SQLite control plane; ClickHouse event persistence does not use
GORM.

### Collector identity and active-instance fencing

`collector_id` is durable security identity, not client-supplied display
metadata. It participates in stored event provenance and the durable
batch-sequence/idempotency namespace. A bearer credential that can choose an
arbitrary well-formed collector ID could therefore impersonate another
collector and reserve that collector's future sequence numbers.

The native collector protocol uses the following identity contract:

- An administrator first obtains the collector's locally persisted stable
  `collector_id`, then creates an ingestion token explicitly bound to that ID.
  The server never trusts first use to choose a binding and never auto-enrolls
  an unknown identity from `CollectorHello`. The supported first-start bridge
  is `open-splunk-collector identity -config PATH`: it validates configuration
  without reading the not-yet-issued token, durably creates or reads the ID
  under the final state directory, and prints only that ID. It does not open
  inputs, checkpoints, or WAL state and does not contact the server. The state
  path and identity inode must be owned by the collector process, owner-only,
  non-symlinked, and free of external hard-link aliases; startup fails closed
  when those invariants or adjacent durable-state continuity do not hold. A
  filesystem root or the current working directory is never a valid state
  directory. Linux and macOS implement this filesystem security contract;
  other targets fail closed rather than silently weakening it.
- Every newly created native ingestion token requires exactly one canonical,
  bounded `bound_collector_id`. The binding is immutable once set. Several
  tokens may bind to the same collector during credential rotation, so the
  binding is not unique.
- The upgrade migration may represent old tokens with a `NULL` binding, but
  native gRPC authentication fails closed for them. An administrator may bind
  such a token exactly once under optimistic locking, although revoking it and
  issuing a replacement is the preferred rollout. A non-`NULL` binding can
  never be cleared or changed.
- Successful authentication returns the bound ID as trusted authorization
  state. `CollectorHello.collector_id` must match it before readiness,
  token-use recording, visibility reservation, or event insertion.
  `instance_id` remains useful operator metadata but is never security
  authority.
- Each tenant may retain at most 256 durable collector identities. This hard
  control-plane bound keeps every fleet list, substring filter, exact count,
  and sort traversal finite even though only four rows are hydrated per page.
  Reaching the bound rejects only a previously unseen identity with gRPC
  `RESOURCE_EXHAUSTED`; an existing enabled identity may still reconnect, and
  a disabled identity still reports its authoritative disabled state. The
  durable bound is independent of the smaller 16-collector process-liveness
  ceiling.

The single-node server also owns an active-stream lease keyed by
`(tenant_id, collector_id)`. Because the process lifetime lock prevents two
supported server processes from sharing one control database, the first
implementation may keep this lease registry in memory. Each accepted Hello
claims a monotonically increasing process-local generation and a random
stream ID under the server boot epoch. A fresh claim supersedes the previous
claim so a half-open connection cannot prevent reconnection. The previous
handler is canceled promptly and all post-ready heartbeat, batch, goodbye,
and deferred-cleanup actions must prove that their lease is still current.
Cleanup is conditional, so a delayed old handler cannot release a newer
lease.

Lease validation is a request-admission boundary: a request that proved it
held the current generation before a newer claim may finish, while a request
that reaches the boundary afterward is rejected without starting a durable
write. Token authorization is refreshed at every heartbeat and batch
boundary. Revocation or scope replacement that races after a boundary may
allow only that already-admitted operation to finish; the next boundary
observes the new state. The later persisted fleet implementation must combine
collector enabled-state checks, token revalidation, token-use recording, and
durable lease allocation in one SQLite transaction so administrator
disablement can offer the stronger linearizable guarantee.

Fleet state must not be inferred merely from an open gRPC socket or a
client-supplied timestamp. The server boot epoch invalidates all prior live
leases after restart, server receive time drives lifecycle display, and a
monotonic in-process deadline drives online/stale transitions. Hello and
heartbeat collections, strings, counters, and encoded sizes must be bounded
before persistence. Heartbeat storage is latest-wins and coalesced, uses a
telemetry revision separate from administrator optimistic-lock versions, and
conditions every write on the current boot epoch and lease generation.

## Indexes, apps, and tenancy

Splunk “apps” and “indexes” should not be represented by the same database entity, even if the first UI creates one of each together.

- An **index** is a logical data boundary with retention, ingestion permissions, search permissions, and default field behavior.
- An **app** is a workspace containing navigation, default index scopes, saved searches, reports, dashboards, field aliases, and later other knowledge objects.

For the initial internal deployment, an app may map one-to-one to its primary index—for example, a `gradethis` app whose default search scope is `index=gradethis`. The data model should still allow one app to search several indexes and one index to be used by several apps.

Environment is not a special storage hierarchy. Each collector explicitly chooses its destination index, so an operator may send development and production logs to different indexes when different retention or isolation is useful. Otherwise, both can share an index and carry `env` or `environment` as an ordinary searchable event field. Open Splunk should support either policy without imposing one.

Indexes should be logical partitions in a shared event table rather than one ClickHouse database or table per app. This avoids schema drift, excessive parts, cross-index query complexity, and migration duplication. Every query must be intersected with the requesting user’s allowed index set, regardless of what the SPL text contains.

Index deletion must preserve the storage-generation boundary implied by that
shared table. An administrator first archives the index, then supplies its
current optimistic version, an exact canonical-name confirmation, and a data
deletion mode. `KEEP_DATA` completes synchronously in the GORM-backed SQLite
control plane: an immutable deletion tombstone hides the archived index from
all live catalog reads while retaining its row, token/app references, and
unique name reservation. The name is not reusable because ClickHouse events
and search scopes currently identify a logical index by `tenant_id` and
`index_name`; freeing it would expose retained events through a replacement
index. Generic state administration cannot set `DELETING`, which is reserved
for the physical-deletion coordinator. The GORM-backed SQLite control plane
now admits that transition through `BeginIndexDataDeletion`: one immutable
outstanding operation snapshots the trusted tenant scope together with the
exact archived index ID, canonical name, version, and timestamp, and its insert
trigger atomically advances the index to `DELETING` at version `N+1`. Exact
same-tenant retries return the same operation, including after restart;
changing a valid tenant fails closed rather than rebinding the work.
Oldest-first discovery returns one indexed row at a time. The operation and
its deleting index are immutable until `CompleteIndexDataDeletion` consumes
the outstanding marker.

The ClickHouse Store separately provides a writer-preferring, context-aware
`WithWritesFrozen` scope that covers `Store`, `ResumeBatch`,
manual/background reconciliation, and shutdown. Its privileged drain replays
at most the visibility layer's 64 pending reservations / 256 MiB
durable-outbox bound, then separately counts every reserved row—including
live-leased rows—and succeeds only after proving the count and bytes are both
zero. The production runtime's single Store owner is part of that fence
contract; every writer for the physical events table must use it.

Physical deletion also requires a read fence; HTTP admission alone is not a
safe execution boundary because a search may remain queued and an export may
re-execute a retained job later. The compiler therefore binds the trusted
tenant/index scope and the exact positions and values of its security bind
arguments into private, tamper-evident metadata. The ordinary search executor
and its timeline, field-catalog, field-summary, field-suggestion, and export
re-execution paths validate that metadata against one detached argument
snapshot, then acquire a shared process-local lease for every index in the
scope before issuing ClickHouse work. Multi-index admission is atomic.

Each lease derives a cancellation context. Retirement permanently closes one
tenant/index key, cancels every overlapping lease with an explicit unavailable
cause, and joins every release before deletion may inspect or advance a native
mutation. The coordinator reapplies this idempotent retirement after validating
each durable operation and before reading its mutation attempt, polling native
status, freezing writers, or issuing ClickHouse mutation work. A failed or
timed-out drain retries without touching ClickHouse. Consequently a queued job
cannot silently complete with an empty result after deletion, a running job is
joined before mutation, and a retained export fails as an unavailable source
instead of publishing a changed empty artifact.

The in-memory retirement set is paired with a GORM catalog check at query
execution. Active and archived generations may remain physically readable;
`DELETING`, missing, and terminally tombstoned names are rejected before the
live lease. This durable check preserves the fence after process restart, while
the registry closes the race between the check and a concurrent transition to
`DELETING`. Compiler-only validation and `EXPLAIN` do not read event rows and do
not acquire a physical-read lease.

Native administrator index-statistics reads use the same catalog and live
lease. List enrichment batches only `ACTIVE` and `ARCHIVED` records: a visible
`DELETING` item keeps its optional statistics unset, and an all-deleting page
does no visibility-snapshot or ClickHouse work. A direct statistics request for
a deleting index fails explicitly as unavailable.

Before the first outcome-ambiguous ClickHouse `ALTER`, migration 0018 and an
explicit GORM model now persist exactly one immutable mutation attempt beneath
the outstanding deletion operation. It binds a stable correlation ID and
protocol version to the operation's admission tenant, ClickHouse
database/table, and the table's canonical nonzero UUID. Both Go validation and
SQLite triggers reject any operation/attempt tenant mismatch. Exact retries
converge on that row across concurrency and restart; any target drift fails
closed. SQLite stores only the durable intent and physical generation. Live
mutation progress remains native ClickHouse state and is never mirrored
through GORM.

The native Store can resolve the physical target only after the frozen outbox
drain, reconcile the full-request SHA-256 correlation marker against
`system.mutations`, submit at most one asynchronous heavyweight `ALTER DELETE`,
and poll pending progress without repeatedly freezing writers. Missing
mutation history is never completion evidence. A terminal candidate requires a
new frozen drain followed by one key-aligned query that both resolves the
current UUID/engine and proves no `(tenant_id, index_name)` row exists. Only the
initial `MergeTree` engine is supported. ClickHouse persistence, mutation
execution, and reconciliation remain native and do not use GORM.

Migration 0019 and the explicit GORM
`indexDataDeletionCompletionRecord` provide that terminal control-plane
transaction. `CompleteIndexDataDeletion` accepts the exact immutable mutation
attempt whose native request was just proven physically empty. One completion
insert atomically creates the ordinary catalog tombstone, deletes the matching
outstanding operation, and cascades deletion of its mutation attempt. The
retained index stays `DELETING` at version `N+1`; terminal completion does not
require an `N+2` bump, so an archived `MaxInt64-1` generation can safely finish
at SQLite's final version. The immutable completion permanently copies the
operation identity, correlation, logical index identity, archived/deleting
versions, tenant, physical database/table/UUID, protocol, and timestamps.
Exact concurrent and restart retries return that audit row through a read-only
fast path, completed protocol identities cannot be recycled, stale outstanding
reads converge to not found, and the retained row continues to preserve
foreign keys and reserve the canonical name.

This SQLite transaction is a trusted terminal boundary, not independent proof
of ClickHouse state. The runtime coordinator must invoke it inside the same
drained `WithWritesFrozen` callback in which
`AdvanceIndexDataDeletion` returns physical emptiness. If the terminal commit
fails or has an ambiguous outcome and no completion audit can be read, the
coordinator must reacquire the freeze, drain, and prove zero again rather than
reuse cached evidence.

This initial catalog belongs to one configured deployment tenant. A physical
deletion targets that tenant plus the canonical index name and deliberately
preserves rows under every other tenant key. Before supporting several tenant
catalogs in one process, indexes, tombstones, and deletion operations must
become tenant-scoped control-plane entities; the current global catalog must
not be treated as a multi-tenant deletion model.

The configured ClickHouse database/table name must remain exclusively bound to
one physical table generation from Store construction through Store shutdown.
All migrations and rename/drop/exchange/replace DDL run before the Store opens,
and three direct, role-free service principals now divide that authority. The
short-lived migrator has only the exact create/additive-schema/ledger grants
needed by the embedded files. The ordinary runtime has only `SELECT` and
`INSERT` on `open_splunk.events`. The deletion worker has column-scoped
`SELECT(tenant_id, index_name)`, `ALTER DELETE`, and the two system-table reads
needed for reconciliation. Every principal is validated through its complete
explicit `SHOW GRANTS` surface against the exact audited ClickHouse
`26.3.17.4` contract; a denied `system.server_settings` canary also proves that
non-public system-table reads require explicit grants. The migrator is
validated before any DDL, its password is removed from the application
environment after capture, and its options are cleared as soon as the
connection closes. UUID checks still detect observable drift, but ClickHouse
targets `ALTER TABLE` by name and cannot atomically fence privileged
out-of-band DDL.

The `internal/indexes` coordinator now composes these primitives with one
owned, serialized worker. It immediately recovers the oldest operation at
startup, periodically rescans SQLite so correctness never depends on a wake,
and retains the oldest pending or failing operation so younger work cannot
bypass it. The admitted operation tenant is checked against the configured
deployment tenant before even reading a mutation attempt or making a native
call. An existing attempt must then match that same operation tenant and is
polled outside the freeze. Every advancement reacquires `WithWritesFrozen`,
drains the durable outbox, and, for a new attempt, resolves and persists the
immutable physical target before the first possible `ALTER`. Only a
`PhysicallyEmpty` result with no error can invoke
`CompleteIndexDataDeletion`, and that invocation occurs inside the same frozen
callback. A failed or outcome-ambiguous terminal commit is accepted only when
the exact tenant-bound immutable audit can be read; otherwise the next cycle
must drain and prove zero again. Pending and error retries are rate-limited,
wakes are coalesced, and shutdown cancels and joins the sole worker.

The production server now wires exactly one coordinator beside exactly one
Store. Startup opens and validates the short-lived migrator, applies the
embedded files, and closes it before opening the runtime and deletion
connections. The Store owns both long-lived pools: ordinary ingestion/search
uses the runtime connection, while target resolution, mutation
submission/status, and terminal physical proof use only the private serialized
deletion connection. The coordinator then owns final Store shutdown.

Shutdown first drains later request/search/collector consumers. The deletion
runtime starts one asynchronous owner pipeline that joins the coordinator
before closing the Store, so a caller deadline also bounds a driver close that
blocks. If the graceful budget expires, the finalizer retains the Store,
visibility sequencer, and SQLite control plane behind a later unbounded join;
it never tears borrowed dependencies out from under a live worker. Default
signal behavior has already been restored, so a second termination signal
remains the operator escape hatch for a dependency that ignores cancellation.

The local deployment uses the same production contract: a digest-pinned image,
an exact pre-DDL version check, idempotent initialization on every container
start, six independent credentials, and a config-backed bootstrap
administrator limited to container loopback. A composed live test creates the
real checked-in stack, validates every principal and the migrated schema,
rotates all credentials while retaining the data volume, rejects the old
credentials, and revalidates the recovered stack.

Application connections use ClickHouse's secure native listener with TLS 1.2
or newer. Secure mode requires both an explicit bounded PEM CA bundle and an
explicit DNS name or IP SAN; it never falls back to the host trust store and
has no skip-verification mode. The trust material is parsed before the server
lock, SQLite, or either persistence connection opens. Migration, runtime,
deletion, and isolated inspection lanes receive independent TLS configurations
with normal chain and hostname verification. The checked-in local Compose
deployment generates a private development CA and server identity, exposes
the verified listener on `9440`, and retains plaintext native `9000` only for
container-local bootstrap and explicitly loopback-bound diagnostics. Its live
contract uses that CA-signed generated identity, makes Compose health execute
an authenticated query over `9440`, and rejects a wrong verification name, a
wrong trust root, plaintext on the secure listener, and legacy TLS. Generated
environment publication is shell-safe, no-overwrite, and concurrency-safe; the
one-use CA signing key is not retained.

The authenticated administrator `POST /api/v1/indexes/delete` route now admits
`DELETE_DATA`. It validates the optimistic version and deletion mode before
selector lookup, requires an exact canonical-name confirmation, and rejects an
archived `MaxInt64` generation because admission must create generation
`N+1`. A fresh request must observe the index archived at version `N`; an exact
retry may instead observe the same index deleting at `N+1`. The handler derives
`IndexDataDeletionScope` only from its trusted configured tenant, calls
`BeginIndexDataDeletion`, validates the returned immutable identity, and
returns HTTP 200 with both the index ID and deletion operation ID.

After durable admission the handler synchronously issues a nonblocking,
best-effort coordinator wake. The periodic recovery scan remains the
correctness path if that hint is coalesced or the process stops. HTTP shutdown
stops new admissions and drains every active handler, guaranteeing that each
successfully admitted deletion completes its postcommit wake before the
deletion runtime closes; the runtime rejects new wake hints once close begins.

HTTP idempotency is deliberately bounded by the outstanding operation.
Sequential, concurrent, selector-equivalent, and restart retries with the
original archived version return the same operation ID while that operation
exists. Terminal completion consumes the outstanding row and tombstones the
catalog entry, so the same HTTP request then fails selector resolution with
`404 Not Found`; this route is not a terminal operation-status or replay API.
If clients later require indefinite response replay, it must be added as an
operation-ID-backed contract without weakening the permanent catalog
tombstone.

The authenticated administrator `POST /api/v1/indexes/stats/get` route now
provides a bounded statistics view for one current catalog index selected by
ID or canonical name. The server resolves the selector through the
GORM/SQLite control plane, captures one committed visibility cutoff followed
by one UTC millisecond measurement instant, and supplies the trusted
deployment tenant plus the resolved immutable index identity to a native
ClickHouse reader. The browser cannot choose the tenant or substitute the
physical scope.

The reader returns an exact retained-and-committed event count and exact
event-time bounds using `expires_at > measured_at`,
`index_time <= measured_at`, and `visibility_seq <= visibility_cutoff`.
Empty results omit both bounds. Since all logical indexes share the event
MergeTree, compressed bytes are reported as an overflow-safe proportional
estimate from active table part bytes and rows; the response therefore always
sets `estimates`, while inconsistent part counters fail closed. Empty results
take one parameterized query and nonempty results take two. A reader-owned
ten-second operation deadline prevents clickhouse-go from widening the
fifteen-second server execution limit, and read, memory, output, concurrency,
query-size, cache, and subquery bounds apply to both queries. A single-slot,
fail-fast native gate ensures statistics can occupy at most one session in the
shared runtime pool and cannot starve ingestion or search.

Runtime access is limited to the event-table `SELECT` already used by search
plus column-scoped reads of `database`, `table`, `active`, `rows`, and
`bytes_on_disk` from `system.parts`; the exact grant surface is checked before
serving. ClickHouse statistics do not use GORM.

`POST /api/v1/indexes/list` supports page-local statistics enrichment for the
existing name/created/updated catalog sorts. The SQLite control plane admits
at most 1,024 physical index identities globally. Active, archived, deleting,
and terminal tombstoned rows all consume that permanent bound because freeing
a row would allow a canonical ClickHouse-facing name to be reused. A
trigger-maintained singleton records the physical count and a global catalog
revision; product deletion never physically removes an index row.

Creation checks the bounded count, duplicate name, and random ID and inserts
the row in one explicit GORM transaction. SQLite's immediate writer admission
serializes concurrent creators, so exactly one caller can consume the last
slot. Duplicate names, including terminal names, remain conflicts even when
the catalog is full. SQL triggers independently reject overflow, identity
replacement, physical deletion, and unbounded persisted metadata.

List requests authenticate a server-signed composite keyset before storage
work. GORM/SQLite then reads the catalog marker, at most 65 candidate rows, and
an optional exact filtered count in one read-only WAL transaction. State and
literal substring filters, deterministic name/created/updated ordering, and
the matching `(sort_key, index_id)` continuation predicate execute in SQLite;
there is no offset or full-record catalog materialization. Text filtering uses
SQLite's deterministic contract: ASCII letters are case-insensitive,
non-ASCII code points are case-sensitive, and `%`/`_` are literal. Any
definition, state, deletion-admission, or tombstone mutation advances the
global revision and makes an outstanding cursor fail closed rather than
silently duplicate or omit a row.

After SQLite fixes the metadata page, the server acquires the serialized
administrative response permit for bounded defensive validation and
materialization. It then releases that permit and captures one committed
visibility cutoff followed by one UTC millisecond measurement instant. One
native grouped event query returns every nonempty page scope; absent groups
become exact empty results. At most one shared active-parts query then supplies
the proportional storage basis for every nonempty result, preventing N+1
behavior and making estimates on the same page comparable. A SQLite busy wait
therefore cannot occupy response capacity.

The batch operation shares the single-index reader's ten-second overall
deadline and one-slot fail-fast native gate. It bounds scopes, groups, and
result rows to 64, uses a batch-only 64 KiB expanded-query limit for 64
maximum-length parameter values, and retains the existing memory, read,
result-byte, thread, cache, and subquery limits. The handler reacquires the
serialization permit before validating echoed tenant/index/snapshot/time
identity, attaching statistics, enforcing the response-size ceiling, and
transferring ownership to the protobuf codec. Empty catalog pages perform no
snapshot or ClickHouse work. Continuation pages receive independent
measurement instants, and the signed cursor binds whether statistics were
requested while preserving the legacy fingerprint for plain index, token, and
app lists.

Sorting by event count or storage bytes remains deliberately unsupported.
Global statistics ordering would require measuring every filtered catalog
candidate and retaining an immutable statistics ordering snapshot across
pages because ingestion, retention, TTL deletion, and changing `system.parts`
metadata can otherwise cause skips or duplicates. The metadata catalog is now
physically bounded, but its revision cannot freeze native event and part
statistics. That separately bounded snapshot design remains future work;
page-local statistics do not imply it.

### Administrator index field catalog

The authenticated administrator
`POST /api/v1/indexes/fields/list` route provides field discovery without
creating a search job or accepting caller-written SPL. Every request first
resolves its ID or canonical-name selector through the GORM/SQLite control
plane. The resolved stable ID, canonical name, and current version become
trusted service inputs. Selector resolution deliberately adds no
search-enabled policy. Physical execution admission then permits current
`ACTIVE` and `ARCHIVED` records, including records with disabled search access,
while outstanding `DELETING` records fail explicitly as unavailable and
terminal tombstones remain invisible. This keeps catalog identity and
lifecycle policy in GORM without making GORM responsible for event analysis.

Executable index-field requests require the `TimeRangeSpec` message and both
of its bounds. Absolute or relative expressions are resolved once to a
half-open interval before analysis admission. The server then calls
`SnapshotAnalysisScope` with only the configured tenant and resolved canonical
index: it captures the committed visibility cutoff first and then one UTC
clock anchor used for both `SearchStart` and `IndexTimeCutoff`. A server-owned
empty-AST raw-event plan is built from that immutable snapshot, so neither SPL
nor a physical tenant/index scope can be injected by the browser.

The ordinary native ClickHouse compiler emits one parameterized field-catalog
query over the untransformed event relation. Its base scan requires the trusted
tenant and index, `event_time` in `[earliest, latest)`,
`index_time <= snapshot_anchor`, `expires_at > snapshot_anchor`, and
`visibility_seq <= visibility_cutoff`. The field-catalog compiler and executor
remain the same native path used for completed-search field analysis; they do
not route ClickHouse through GORM. Materialized common subexpressions and
short-circuit evaluation are mandatory, and the query disables async insertion
and all result/query caches.

The executor buffers the complete result and validates its header, bytewise
field order, metadata version, durable value-type codes, and count invariants
before publishing a page. Presence, explicit-null, and missing counts are
exact, observed types are complete and sorted, and known canonical schema
fields still appear with zero counts when the index is empty. The catalog does
not calculate or estimate distinct counts. At most 10,000 profiles are
admitted; the compiler requests one extra ordered profile as a sentinel and an
overflow rejects the entire catalog rather than returning a misleading
truncation.

The complete immutable catalog, rather than individual pages, is held in the
bounded field-analysis LRU. Exact-key computations coalesce behind one flight;
the first miss uses one ClickHouse query, while continuations page the retained
memory without native reads. Defaults are 128 entries, 64 MiB, and a five
minute absolute TTL. A shared nonblocking computation gate defaults to four
slots and fails saturation fast. Native settings further cap a catalog query
at fifteen seconds, 128 MiB, five million source rows, 1 GiB read, two threads,
10,001 groups, and 32 MiB of result data. Query/snapshot work is completed
before acquiring the global large-response serialization permit, and the
protobuf response has its own 32 MiB limit.

Filtering is a case-sensitive UTF-8 field-name substring applied to the
validated in-memory catalog. Pages default to 100 and are capped at 1,000;
requesting `include_total_size` exposes the exact filtered total. The signed
cursor is scoped to the service instance and caller and authenticates the
resolved index ID/name/version, original time intent, snapshot fingerprint,
filter fingerprint, cache generation, result offset, scan position, and exact
total. Page size, the include-total response preference, and use of the
equivalent ID/name selector may change on a continuation; analysis-scope
changes fail. Cursors work only while that exact cache generation remains live,
so expiry, eviction, restart, index version changes, or tombstoning fail
closed. Because the cursor resumes a scan position in one immutable catalog,
all filtered continuation pages together remain linear in catalog size and
cannot drift as ingestion, retention, or TTL removal proceeds.

No schema expansion accompanies this feature. The greenfield control plane
uses its existing explicit GORM index model and therefore needs no migration.
ClickHouse migration `0003_add_field_metadata.sql` already supplies
`field_names`, `field_types`, and `field_metadata_version`, and the existing
runtime event-table `SELECT` privilege is sufficient. No ClickHouse migration
or grant is added: GORM remains control-plane-only and ClickHouse remains
native-only.

### Complete index administration capability

`SERVER_FEATURE_INDEX_ADMIN` is advertised only when the complete
index-administration family is configured: GORM-backed create, get, list,
update, state, and deletion admission; native ClickHouse single/page-batched
statistics; native index field discovery; and the durable physical-deletion
runtime. Partial embedded or test compositions remain valid but do not
advertise the broad feature, even when it was requested in static bootstrap
configuration. Duplicate requested feature values collapse to one.

This flag describes a configured API family. It is not a live ClickHouse
health signal and is not an authorization entitlement for future RBAC.
Transient dependency failures remain operation errors, while every
administrator route continues to enforce browser authentication independently.
The boundary does not change persistence ownership: GORM remains limited to
SQLite control-plane records, and ClickHouse statistics, field analysis, and
physical mutations continue through native services.

## ClickHouse event model

The storage model should preserve Splunk’s canonical event fields while retaining typed application data.

### Conceptual event

```text
event_id          stable identifier for retry handling
tenant_id         future-proof security boundary, even if initially single-tenant
index_name        logical index
_time             event time
_indextime        accepted/ingested time
host              originating host
source            originating file, stream, or producer
sourcetype        parser/semantic profile
service           promoted application/service name
level             promoted normalized severity
body              human-readable message when available
_raw              original event representation
trace_id           promoted correlation field
span_id            promoted correlation field
fields             typed dynamic JSON object
field_names        normalized paths used for field discovery
collector_id       collector identity
batch_id           delivery batch identity
```

The physical schema should use typed, promoted columns for fields that almost every search touches, plus a dynamic typed payload for the long tail. A starting point is a ClickHouse `JSON` column with static type hints for common paths and a bounded number of dynamic paths. We should benchmark that against separate typed maps before freezing the schema; observability data with extremely high field cardinality can make either design expensive when used carelessly.

The table should initially use `MergeTree`, time-based partitions, and an ordering key beginning with the security/index dimensions that nearly every search filters, followed by a coarse time bucket and event time. For example:

```text
ORDER BY (tenant_id, index_name, toStartOfHour(_time), _time, event_id)
```

This is a hypothesis, not a universal answer. It must be validated against the expected number of indexes, retention period, ingest rate, typical time range, and query corpus before production data is committed.

Additional storage principles:

- pin a tested ClickHouse release;
- use the current native text index for token searches over `_raw`/`body`, with a benchmarked tokenizer;
- retain the original event even when structured parsing succeeds;
- materialize frequently used fields only after query evidence justifies it;
- make retention configurable per index;
- avoid tiny inserts by batching in the server;
- place server-side memory, result-row, execution-time, and concurrency limits on every search; and
- expose generated SQL and ClickHouse `EXPLAIN` to administrators, not ordinary users by default.

## SPL engine

### Compiler pipeline

```text
SPL source
  -> lexer
  -> parser and source-located AST
  -> semantic analysis and schema/type resolution
  -> typed logical pipeline
  -> safe rewrites and filter pushdown
  -> ClickHouse relational plan
  -> parameterized ClickHouse SQL
  -> streamed result schema and rows
```

Directly concatenating SQL fragments while walking pipe commands will fail as soon as aliases, aggregations, window functions, or stage ordering become meaningful. The logical plan should contain backend-neutral operators such as:

```text
Scan, Filter, Project, Extend, Extract, Aggregate,
Window, Sort, Limit, Rename, TimeBucket, Output
```

Every operator should know its input and output schema. Stage boundaries can then lower to nested SQL subqueries only when ClickHouse alias visibility or aggregation semantics require them.

### SPL semantic choices that must be explicit

Some of the hardest compatibility work is not grammar; it is behavior:

- base `search` boolean precedence differs from `where`/`eval` precedence;
- free terms need a precise definition over `_raw` and token indexes;
- wildcards differ between term, field, and value positions;
- missing, null, empty, numeric, string, and multivalue fields behave differently;
- event order is undefined until a command establishes it;
- transforming commands remove event fields that are not grouped or aggregated;
- `stats`, `eventstats`, and `streamstats` have different result shapes;
- time zones and relative time modifiers affect both parsing and bucketing; and
- regex syntax and capture naming need a declared dialect.

These rules should live in a versioned **SPL compatibility specification** and executable conformance tests.

### Recommended first command set

The first useful compatibility tier should support:

**Search and projection**

- base search terms and field comparisons
- `search`
- `where`
- `fields`
- `table`
- `rename`

**Expressions and extraction**

- `eval`
- `rex`
- `spath`
- `bin`/`bucket`

**Aggregation and presentation**

- `stats`
- `chart`
- `timechart`
- `sort`
- `head`
- `tail`
- `dedup`
- `top`
- `rare`

**Initial aggregate functions**

- `count`, `dc`, `values`, `list`
- `sum`, `avg`, `min`, `max`
- `earliest`, `latest`
- `p50`, `p90`, `p95`, `p99`, and `perc<N>`

**Initial eval functions**

- `if`, `case`, `coalesce`, `isnull`, `isnotnull`
- `tonumber`, `tostring`, `round`, `ceil`, `floor`
- `lower`, `upper`, `len`, `substr`, `mvcount`, concatenation
- `match`, `like`, `replace`
- `now`, `relative_time`, `strftime`, `strptime`

`eventstats` and the deliberately bounded `streamstats` bare row-count,
exact-field occurrence-count, exact-field numeric-sum, exact-field
numeric-average, and exact-field mixed-type minimum subset are second-tier
commands included in the pre-release backend. `streamstats max(field)`,
`transaction`, subsearches, `join`, `append`, `appendpipe`, `map`, `foreach`,
data models, `tstats`, and arbitrary scripted commands remain explicitly out of
the first release.

The bounded `timechart` implementation includes bare row count, exact-field
occurrence count, integer-suffix percentile, numeric sum, and numeric average,
each with an optional one-field runtime split. Its compatibility contract pins
empty versus present-all-ineligible input, continuous fixed grids, top-ten,
`NULL`, and `OTHER` selection, and one-scan ClickHouse lowering.

The bounded `chart` implementation includes bare row count, exact-field
occurrence count, integer-suffix percentile, numeric sum, and numeric average
over one row field and one runtime column field. Its compatibility contract
pins row-domain parity with `stats BY`, occurrence-based top-ten and cells for
`count(field)`, `NULL` and `OTHER`, atomic runtime validation, and one-scan
ClickHouse lowering for every field-measure form.

### Example target searches

The first usable release should treat the following as its initial GradeThis compatibility corpus.

**Follow one request or background operation:**

```spl
index=gradethis trace_id="<trace-id>"
| sort _time
| table _time level layer logger message
```

**Inspect errors and warnings:**

```spl
index=gradethis (level=ERROR OR level=WARN)
| sort -_time
```

**Find a known error fragment in raw log text:**

```spl
index=gradethis "connection refused"
| table _time level logger message trace_id
```

**Count events by severity:**

```spl
index=gradethis
| stats count by level
| sort -count
```

**Find the most frequent errors:**

```spl
index=gradethis level=ERROR
| stats count by logger, message
| sort -count
| head 20
```

**Chart event volume by severity:**

```spl
index=gradethis
| timechart span=5m count by level
```

**Chart server errors by route:**

```spl
index=gradethis message="Request metrics" status>=500
| timechart span=5m count by path
```

**Count HTTP responses by route and status:**

```spl
index=gradethis message="Request metrics"
| stats count by path, status
| sort -count
```

**Find slow routes:**

```spl
index=gradethis message="Request metrics"
| eval duration_ms=tonumber(replace(duration, "ms$", ""))
| stats count p95(duration_ms) as p95_ms by path
| where p95_ms > 500
```

**Inspect the most common messages:**

```spl
index=gradethis
| top limit=20 message
```

## Browser API: SRouter and protobuf

Every browser-facing application endpoint will use SRouter with protobuf request and response messages, matching GradeThis:

- `.proto` files under the versioned `open_splunk.v1` package are the source of truth;
- `protoc` generates Go messages for the server and `ts-proto` TypeScript messages for the Next.js client;
- Go routes use `router.NewGenericRouteDefinition` and `codec.NewProtoCodec`;
- the frontend uses `@per/go-common-core`'s `ProtobufTransport` with generated encoders and decoders;
- successful payloads use `application/x-protobuf`; and
- generated files are reproducible in CI and are never edited by hand.

The initial search API should expose bounded request/response operations such as:

```text
POST /api/v1/search/jobs/create
POST /api/v1/search/jobs/get
POST /api/v1/search/jobs/results
POST /api/v1/search/jobs/field-summary
POST /api/v1/search/jobs/cancel
```

Index configuration, collector status, search history, and saved-object APIs follow the same SRouter/protobuf convention. The single-user release can register these routes without end-user authentication; this transport choice does not prevent adding SRouter authentication middleware later.

Search results are dynamic, so their protobuf model must carry both a schema and typed cells. A `ResultValue` `oneof` should preserve strings, integers, doubles, booleans, timestamps, nulls, bytes, lists, and objects. Result pages should include stable column definitions and bounded rows instead of embedding an untyped JSON document in protobuf.

Search progress will use a long-lived WebSocket registered as a raw SRouter route, following the `go-common/pkg/common_ws` pattern. Every application frame is binary protobuf; text frames are rejected or ignored. The HTTP routes remain authoritative for creating, canceling, inspecting, and paging a search, while the WebSocket carries timely progress and bounded previews.

```text
GET /api/v1/search/ws
```

The client sends a `SearchWebSocketCommand` with subscribe/unsubscribe payloads. A subscription identifies either a search job or an export job and includes the last sequence number the client has processed. The server sends sequenced `SearchWebSocketEvent` messages whose payload can be:

- subscription acknowledged;
- search state changed;
- progress updated, including scanned rows/bytes, matches, and elapsed time;
- result schema available;
- bounded preview rows available;
- export progress or completion;
- warning emitted;
- search completed, failed, or canceled; or
- resynchronization required.

Each search or export job retains a bounded replay buffer of recent progress events. After a reconnect, the client subscribes with `after_sequence`; the server replays what is still available. If the requested sequence has expired, it emits `resynchronization_required` and the client restores state through the corresponding SRouter `get` route. Full result sets and export artifacts never travel through the WebSocket.

The socket implementation must enforce binary-frame size limits, bounded per-connection send queues, ping/pong liveness, origin checks, and graceful shutdown. A slow browser may lose preview updates and resynchronize, but it must never block ClickHouse execution or the ingestion path.

## Search jobs and API behavior

Even a single-node product benefits from a search-job abstraction. A job should have a stable search ID, owner, normalized query, effective index scope, time range, state, progress metadata, result schema, ClickHouse query ID, warnings, timing, and expiration.

Each job also records a resolved absolute time range and `_indextime` cutoff. Re-running an operation such as export against that snapshot excludes events ingested after the original search began, making results repeatable within the job's retention window.
That repeatability contract does not permit re-execution through an index
deletion boundary: once any scoped index enters physical deletion or is
terminally tombstoned, the operation must fail explicitly as unavailable.

Recommended lifecycle:

```text
queued -> parsing -> planning -> running -> completed
                                   |-> failed
                                   |-> canceled
```

The API should support:

- creating a search with SPL and an explicit earliest/latest time range;
- publishing sequenced state, progress, warnings, schema, and bounded previews through the protobuf WebSocket;
- retrieving Events or Statistics through bounded protobuf result pages;
- canceling a job and propagating cancellation to ClickHouse;
- inspecting safe plan and timing details; and
- expiring transient results after a bounded interval.

The client creates a job through SRouter, subscribes to its progress over the WebSocket, and retrieves stable result pages through SRouter as they become available. The `get` route doubles as reconnect and diagnostic fallback, not as the normal progress mechanism. Raw-event searches must use bounded pagination and should not attempt to buffer millions of rows in the Go process or browser.

## Saved searches, search history, and results export

These are three related but distinct product concepts and should not share one overloaded record.

### Saved searches

A saved search is a reusable definition stored deliberately by the user. The first release should persist it in SQLite with:

- stable ID and optimistic version;
- name and optional description;
- owning app/workspace;
- original SPL source, never generated ClickHouse SQL;
- time-range specification, preserving relative values such as `-24h` and `now`;
- preferred result tab and selected fields;
- optional visualization settings;
- created and updated timestamps; and
- later-compatible ownership and sharing fields, even though the first deployment is single-user.

Loading a saved search restores the editor and time picker. Running it creates an ordinary search job, so it is parsed, compiled, resource-limited, and index-authorized again; saved searches are not trusted precompiled plans. The UI should support Save, Save As, Open, Rename, Duplicate, and Delete, with an unsaved-changes indicator.

Reports can later extend a saved search with richer table/visualization presentation. Scheduled searches and alerts can later reference the same saved-search ID. They should not be folded into the initial saved-search implementation.

SRouter/protobuf routes should include:

```text
POST /api/v1/saved-searches/create
POST /api/v1/saved-searches/get
POST /api/v1/saved-searches/list
POST /api/v1/saved-searches/update
POST /api/v1/saved-searches/duplicate
POST /api/v1/saved-searches/delete
```

### Search history

Search history is created automatically for every attempted job, including parse failures, cancellations, and execution failures. It records metadata rather than copying result rows:

- search ID and original SPL;
- original time-range expression plus resolved earliest/latest values;
- app and effective index scope;
- start/end times and final state;
- matched rows, scanned rows/bytes, and duration when available;
- warnings or a safe failure summary;
- compiler compatibility version; and
- whether the search originated from a saved search.

History is bounded by configurable age and row-count limits in SQLite. The UI should expose Recent Searches and a full History view with Open in Search, Run Again, Save, and Delete actions. “Open” restores the original query and time-range expression; “Run Again” creates a fresh job and resolves relative time against the current clock. History never stores generated SQL as the reusable source of truth.

### Results export

Export is server-side so large results do not need to be assembled in browser memory. The first usable release should support:

- CSV for event and statistical tables;
- JSON Lines for typed events and rows;
- explicit column selection and ordering;
- configurable row and byte limits;
- cancellation and progress reporting;
- streaming to a temporary artifact rather than buffering in Go memory; and
- automatic artifact expiration and cleanup.

An export references a completed or still-retained search job and executes against the job's resolved time range and `_indextime` cutoff. It uses the same logical plan and resource limits, records its own status, and never accepts raw ClickHouse SQL. If the source job has expired, the user must rerun the search before exporting.

Export creation, inspection, and cancellation use SRouter/protobuf:

```text
POST /api/v1/search/exports/create
POST /api/v1/search/exports/get
POST /api/v1/search/exports/cancel
```

The completed protobuf response supplies a short-lived, single-purpose download token. Downloading the actual CSV or JSONL uses a raw SRouter download route with the correct `Content-Type` and `Content-Disposition`; file bytes are the intentional exception to protobuf response serialization. CSV output must use correct RFC-style quoting and formula-injection protection for cells beginning with spreadsheet control characters.

Export progress can use the existing protobuf WebSocket by subscribing to the export ID. Full export contents never travel through WebSocket frames.

## Browser application

The browser application will be a TypeScript React client built with Next.js static export. It will use generated `ts-proto` models and the same `@per/go-common-core` protobuf transport pattern as GradeThis. The static export will not be a companion runtime directory: it will be compiled into `open-splunk-server` so production requires neither Node.js nor loose frontend files.

### Single-binary packaging

The supported release build runs in this order:

1. install the pinned frontend dependencies from the lockfile;
2. generate TypeScript protobuf models into `gen/ts`;
3. clean `out/` and run the root Next.js application with `output: "export"`;
4. verify `out/index.html` and its referenced hashed assets exist;
5. generate an asset manifest containing the UI and source revision hashes; and
6. compile the root Go asset package with `//go:embed all:out`, embedding the static UI, manifest, and database migrations into the server.

`open-splunk-server` serves the embedded filesystem using `fs.Sub`, `http.FS`, and a dedicated static handler. Hashed `/_next/static` assets receive long immutable cache headers; HTML entry points do not. Static route handling must be tested for direct navigation and browser refreshes, and API/WebSocket paths must be registered ahead of the UI fallback so the embedded frontend can never shadow SRouter routes.

The supported build target must fail when the Next.js export or generated protobufs are absent. `out/` is treated as generated input to `go build`, not hand-maintained source, and release CI rebuilds it from a clean directory to prevent stale assets. A test launches the compiled binary from an otherwise empty temporary directory and verifies the UI, SRouter API, and WebSocket handshake without access to the repository.

This produces one deployable **Open Splunk server application binary** containing:

- the Next.js UI;
- Go HTTP/SRouter APIs;
- the protobuf progress WebSocket;
- the collector gRPC ingestion server;
- protobuf descriptors/build metadata; and
- SQLite and ClickHouse migrations.

Runtime configuration, SQLite data, export artifacts, and ClickHouse data remain external writable state. ClickHouse remains a separate database process, and `open-splunk-collector` remains a separate small binary because it runs on log-producing hosts. “Single binary” therefore describes the complete Open Splunk server application, not bundling ClickHouse or remote collectors into the same executable.

### Search workspace

The first high-fidelity screen should include:

1. **Top product bar** — current app, navigation, activity/jobs, settings, and user menu.
2. **Search row** — multiline SPL editor, syntax highlighting, completion, explicit time range, and Run/Cancel action.
3. **Job strip** — state, scanned events/bytes, matched events, elapsed time, warnings, and actions.
4. **Fields rail** — Selected Fields and Interesting Fields with counts and top values.
5. **Timeline** — event histogram with drag-to-narrow time range.
6. **Result tabs** — Events, Statistics, and Visualization.
7. **Events view** — `_time`, raw text, matched-term highlighting, expansion, and typed fields.
8. **Statistics view** — virtualized table with sorting and export.
9. **Visualization view** — automatic chart suggestions for `timechart`, `chart`, and suitable `stats` results.
10. **Search actions** — Save, Save As, Open, History, and Export.

The event detail interaction is especially important. Selecting a field value should offer Include, Exclude, and New Search actions that safely modify the AST or query text rather than relying on brittle string insertion.

### Editor support

The SPL editor should eventually share the Go grammar through generated syntax metadata or a small TypeScript grammar package. The first version needs:

- command and function highlighting;
- pipe-aware completion;
- field completion based on index and time scope;
- inline parse errors with source ranges;
- command documentation on hover; and
- formatting that does not rewrite quoted strings or regexes.

## Security and governance

Security is part of the data model, not a later middleware task.

- Store ingestion tokens only as hashes; show plaintext once at creation.
- Scope tokens to explicit indexes and optional source/host constraints. The
  first-release matching, resource, rejection, and replay semantics are
  normative in [Ingestion token host/source constraints v0.1](ingestion-token-constraints-v0.1.md).
- Intersect every search with RBAC-authorized indexes in the logical plan.
- Parameterize values and quote identifiers through a single ClickHouse compiler path.
- Require authenticated TLS with explicit trust and hostname verification for non-loopback ClickHouse traffic.
- Never accept user-provided SQL as part of ordinary SPL.
- Apply query time, memory, row, byte, and concurrency budgets.
- Maintain audit events for authentication, token changes, index changes, searches, exports, and saved-object changes.
- Provide collector and server redaction, with tests for auth headers, cookies, tokens, passwords, and known application secrets.
- Make PII retention and export permissions explicit per index.
- Escape raw events in the UI; log content must never become executable HTML.
- Keep the collector’s own logs on a separate source or prevent self-collection loops.

## Testing strategy

### SPL correctness

- lexer and parser table tests;
- source-location and diagnostic golden tests;
- semantic/type-checker tests;
- AST-to-logical-plan snapshots;
- logical-plan-to-ClickHouse-SQL snapshots;
- ClickHouse integration tests with fixed event fixtures;
- a corpus of real GradeThis searches and edge cases;
- fuzzing for the lexer, parser, wildcard handling, regex handling, and identifier quoting; and
- differential tests against a licensed Splunk instance when one is available, with every intentional deviation recorded.

### Collector correctness

- append, restart, rename rotation, copy-truncate, deletion, and delayed file-creation tests;
- crash points before and after queue append, gRPC send, server insert, acknowledgment, and checkpoint;
- offline queue growth and recovery;
- corrupt queue segment behavior;
- partial batch rejection;
- gRPC reconnect, flow-control, throttle, and backpressure behavior;
- multiline and oversized-event cases;
- redaction invariants; and
- duplicate accounting under retries.

### Transport contracts

- protobuf generation is reproducible and leaves the worktree clean;
- protobuf breaking-change checks protect field numbers, reserved fields, and enum values;
- every SRouter route round-trips generated Go and TypeScript messages;
- dynamic result values preserve integer, floating-point, boolean, null, list, and object types;
- collector gRPC interoperability tests cover hello, batch acknowledgment, rejection, throttle, reconnect, and protocol-version mismatch;
- WebSocket tests cover binary-only framing, subscribe/unsubscribe, monotonic sequence numbers, replay, resynchronization, bounded queues, ping/pong, and shutdown; and
- unknown protobuf fields remain forward compatible across client/server upgrades.

### Embedded application artifact

- a clean release build regenerates protobufs and the Next.js export before `go build`;
- building fails when required UI assets or the asset manifest are missing;
- the compiled server starts from an empty working directory with no Node.js installation or frontend files;
- direct navigation and refresh work for every exported Next.js route;
- immutable caching applies only to content-hashed assets, not HTML entry points;
- embedded UI, Go server, protobuf schema, and migration versions report one consistent build revision; and
- Linux and macOS release binaries serve byte-identical frontend assets for the same source revision.

### End-to-end product tests

- collector fixture -> ingestion -> ClickHouse -> SPL -> browser result;
- browser command through SRouter protobuf -> WebSocket progress -> paged protobuf results;
- multi-index authorization and forbidden-index searches;
- query cancellation;
- saved-search persistence and search-history retention;
- CSV/JSONL export snapshot, quoting, size-limit, cancellation, and cleanup behavior;
- browser accessibility and keyboard navigation;
- large result virtualization; and
- time zone and daylight-saving boundaries.

### Performance harness

`open-splunk-loggen` should produce repeatable Zap-style, raw, nested JSON, and high-cardinality fixtures. Benchmarks should record ingest events/sec, compressed bytes/sec, queue lag, ClickHouse part count, storage ratio, rows/bytes scanned, cold and warm search latency, concurrent query degradation, and browser rendering cost.

The initial throughput target is a sustained maximum of **1,000 events per second** on one Open Splunk server and one ClickHouse server. At that sustained rate, the system receives 86.4 million events per day. At an average raw event size of 1 KiB, that is roughly 82 GiB/day or 2.4 TiB over 30 days before ClickHouse compression and index overhead. Retention, average event size, hardware, and simultaneous-search expectations must therefore be measured before setting storage-capacity and latency acceptance thresholds.

## Delivery plan

The phases below are ordered by dependency and risk, not by calendar estimate.

### Phase 0 — Contracts and skeleton

**Outcome:** the repo builds, local infrastructure starts, and the most consequential semantics are written down.

- establish the Go workspace and Next.js static-export TypeScript workspace;
- pin ClickHouse and supporting container versions;
- initialize the SQLite control plane, WAL settings, migrations, and backup contract;
- establish versioned `.proto` sources plus reproducible Go, gRPC, and `ts-proto` generation;
- establish the clean Next.js-export staging and `go:embed` build pipeline for the self-contained server binary;
- define the canonical event envelope and index model;
- define the collector bidirectional gRPC service, batch acknowledgment, rejection, throttle, and typed-value contracts;
- define the SRouter search/control APIs and protobuf WebSocket command/event contracts;
- write SPL compatibility tier 1;
- create migrations and local Docker Compose;
- add CI for Go, TypeScript, migrations, and integration tests; and
- add a small sanitized GradeThis log fixture corpus.

### Phase 1 — GradeThis vertical slice

**Outcome:** a developer can collect GradeThis logs and investigate them in a Splunk-like Events screen without OpenTelemetry.

- build the file input, JSON decoder, checkpoints, local queue, gRPC stream, and collector diagnostics;
- build gRPC ingestion token auth, index routing, validation, batching, acknowledgments, and ClickHouse insert;
- create the `gradethis` index and collector example config;
- remove the GradeThis OTel log collector from the target Compose path and add Open Splunk Collector;
- implement base search, field comparisons, boolean expressions, `fields`, `table`, `sort`, and `head`;
- implement SRouter protobuf search routes, the protobuf progress WebSocket, replay/resynchronization, and paged result messages;
- implement SQLite-backed saved-search CRUD and automatic bounded search history;
- build the search bar, time picker, job lifecycle, fields rail, timeline, and event renderer against those generated TypeScript contracts; and
- prove trace-ID, error, HTTP status, path, and duration investigations end to end.

### Phase 2 — Analytical SPL

**Outcome:** Open Splunk is useful for the common operational aggregations that justify ClickHouse.

- implement `eval`, `where`, `stats`, `chart`, `timechart`, `rename`, `dedup`, `top`, `rare`, `rex`, `spath`, and `bin`;
- add Statistics and Visualization result modes;
- add a typed result schema and chart contract;
- add server-side CSV/JSONL export jobs, WebSocket progress, expiring artifacts, and safe downloads;
- add field autocomplete and inline parser diagnostics;
- add plan inspection for administrators; and
- tune the event schema, ordering key, text index, and materialized fields against the query corpus.

### Phase 3 — Multi-app product

**Outcome:** several applications can be onboarded and operated independently.

- index/app management UI;
- per-index retention and permissions;
- token creation, revocation, and last-seen status;
- collector fleet health and lag page;
- reports and dashboards built on saved searches;
- HEC-compatible ingestion endpoints;
- expanded activity/jobs operational page;
- role-based access control and audit search; and
- onboarding flow with generated collector configuration.

### Phase 4 — Hardening and expansion

**Outcome:** the single-node product is dependable enough for sustained internal production use.

- schema and migration upgrade tests;
- backup/restore and disaster-recovery procedures;
- load shedding, fair query scheduling, and per-user concurrency;
- alerts and scheduled searches;
- `eventstats` and selected additional SPL commands;
- import/export of saved objects;
- additional collector inputs based on demonstrated need; and
- packaging, service installers, upgrade tooling, and signed releases.

Distributed ClickHouse, distributed search workers, object storage tiers, and high-availability control-plane services are intentionally outside the current plan. The interfaces should not preclude them, but the implementation should stay excellent on one server first.

## First usable-release acceptance criteria

The first usable release, covering the GradeThis vertical slice and analytical SPL phases, is complete when all of the following are true:

- GradeThis logs reach ClickHouse through the collector's protobuf/gRPC stream with no OpenTelemetry component in the log path.
- Restarting either collector or server does not lose acknowledged events.
- File rename rotation and copy-truncate are covered by automated tests.
- A collector token can ingest only into its authorized index.
- Structured numeric and boolean fields retain their types.
- `_time`, `_indextime`, `_raw`, `index`, `host`, `source`, and `sourcetype` are queryable.
- The UI can run, cancel, and inspect a time-bounded search.
- Browser commands and result pages use SRouter protobuf routes, while progress arrives as sequenced protobuf WebSocket events.
- The compiled `open-splunk-server` serves the complete UI and all application protocols from an empty working directory without Node.js or external frontend assets.
- A disconnected browser can resume progress by sequence number or resynchronize through the SRouter job routes without restarting the ClickHouse query.
- Saved searches persist SPL and relative time-range semantics in SQLite and can be reopened, edited, duplicated, and rerun.
- Every attempted search appears in bounded history with safe status and execution metadata.
- Event and statistical results can be exported to bounded CSV and JSONL artifacts without buffering the full export in browser or server memory.
- The Events view provides a timeline, field discovery, event expansion, and Include/Exclude pivots.
- The ten GradeThis compatibility searches in this document execute successfully with documented result semantics.
- Unsupported SPL produces a source-located error rather than partial results.
- Secrets matching the mandatory redaction policy do not appear in ClickHouse fixtures or rendered events.
- One end-to-end test starts the stack, writes a log line, waits for acknowledgment, searches it, and verifies the browser-visible result.

## Principal risks and mitigations

| Risk | Why it matters | Early mitigation |
| --- | --- | --- |
| SPL semantic drift | Plausible but wrong results destroy trust | Compatibility spec, query corpus, explicit errors, differential tests |
| Collector data loss or duplication | Ingestion reliability is the foundation | Disk queue, ack-based checkpoints, crash tests, stable batch IDs |
| Dynamic field explosion | Arbitrary JSON paths can damage storage and query performance | Limits, typed promoted fields, bounded JSON paths, benchmarked fallback |
| Poor ClickHouse ordering/index choices | A schema can be correct but make interactive search slow | Real query corpus, load generator, `EXPLAIN`, migration path |
| Secret and PII ingestion | Logs already contain sensitive field families | Collector/server redaction, token scopes, retention, audit, fixture scans |
| UI scope consumes backend schedule | High-fidelity search UX is a large product in itself | Build one complete Search workspace before dashboards/admin breadth |
| “Looks like Splunk” becomes literal copying | Creates legal and product-design risk | Preserve workflows, create original visual system, review naming/claims |
| Multiple indexes become physical table sprawl | Makes operations and cross-index search brittle | Shared event table with logical index catalog and enforced scopes |

## Decisions already made

- Go will own collection, ingestion, SPL planning, and query execution.
- TypeScript will own the browser experience.
- ClickHouse will store and execute searches over log events.
- The product will support multiple logical indexes and multiple app workspaces.
- The UI should be strongly familiar to Splunk users.
- Log collection will use the first-party Open Splunk Collector, not OpenTelemetry.
- The initial deployment target is a single application server and a single ClickHouse server.
- The initial deployment is single-user on a trusted network; user authentication and RBAC can follow later.
- The single-node control plane will use SQLite.
- The UI will use Next.js static export and be embedded into `open-splunk-server` with `go:embed` during the supported release build.
- The Open Splunk server application ships as one self-contained binary; ClickHouse data/state and the separately deployed collector remain external.
- The single-node ingestion target is up to 1,000 events per second.
- Collectors choose indexes explicitly; environment is an ordinary optional field rather than a required index hierarchy.
- The native collector gRPC protocol ships before Splunk HEC compatibility.
- Browser-to-server request/response APIs use SRouter with generated protobuf messages.
- Search progress and bounded previews use binary protobuf WebSocket frames with resumable sequence numbers.
- Saved searches and bounded search history are first-release SQLite control-plane features.
- CSV and JSONL results export is part of the first usable release.
- Collector-to-server ingestion uses a bidirectional protobuf/gRPC stream.
- A live Splunk instance is not available for differential testing, so documented semantics and a local compatibility corpus are the initial oracle.

## Decisions still required

1. What retention period and average event size should capacity planning use?
2. What hardware envelope—CPU cores, RAM, and local disk—is expected for the single server and ClickHouse node?
3. How many searches should the server sustain concurrently while ingesting 1,000 events/sec?
4. Must the first collector support only Linux/macOS file logs, or is Windows support required immediately?
5. Are dashboards required for the first usable release, or should they follow saved searches and export?

## Source notes

This plan was informed by the current local implementations in:

- `../gradethis/service_config/otel-collector/config.yaml`
- `../gradethis/service_config/clickhouse/init-clickhouse.sql`
- `../gradethis/docker-compose.yaml`
- `../gradethis/docs/operations/logging.md`
- `../go-common/pkg/logger/logger.go`
- `../go-common/pkg/logger/constants.go`
- `../go-common/pkg/logger/sink.go`
- `../go-common/pkg/common_api/faro.go`
- `../go-common/typescript/packages/core/src/utils.ts`
- `../gradethis/be/internal/api/dashboard_api.go`
- `../gradethis/proto/dashboard_api.proto`
- `../go-common/typescript/packages/core/src/transport.ts`
- `../go-common/pkg/common_api/chat_api.go`
- `../go-common/pkg/common_ws/connection.go`
- `../go-common/proto/chat_ws.proto`

Useful compatibility and storage references:

- [Splunk: format events for HTTP Event Collector](https://help.splunk.com/en/splunk-enterprise/get-data-in/get-started-with-getting-data-in/9.0/get-data-with-http-event-collector/format-events-for-http-event-collector)
- [Splunk: monitor files and directories](https://help.splunk.com/en/data-management/get-data-in/get-data-into-splunk-enterprise/9.0/get-data-from-files-and-directories/monitor-files-and-directories)
- [Splunk: `search` command semantics](https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.4/search-commands/search)
- [Splunk: `stats` command](https://help.splunk.com/en/splunk-cloud-platform/search/search-reference/9.2.2406/search-commands/stats)
- [Splunk: `timechart` command](https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.4/search-commands/timechart)
- [ClickHouse: full-text search GA](https://clickhouse.com/blog/full-text-search-ga-release)
- [ClickHouse: schema and ordering-key practices](https://clickhouse.com/blog/10-best-practice-tips)
- [OpenTelemetry ClickHouse exporter notes, retained only as migration context](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md)
