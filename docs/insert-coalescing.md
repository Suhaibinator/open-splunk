# Server-side ClickHouse insert coalescing

This document defines the implementation and operator contract for combining
many durable logical ingestion batches into substantially larger physical
ClickHouse inserts. It records both the runtime behavior and the acceptance
evidence required when its limits change.

## Decision summary

Open Splunk keeps the existing per-batch identity, quota decision, replay
outbox, response metadata, and visibility sequence. After admission, a
server-owned coalescer durably binds multiple pending reservations into one
ordered **write group**. One write group is sent as one synchronous ClickHouse
insert with one stable group-level deduplication token. The whole group is
marked ambiguous before the first possible ClickHouse side effect and all of
its member reservations are committed in one SQLite transaction after
ClickHouse confirms the insert.

The implementation has one physical sender and one global
ambiguous-send barrier. It gains throughput by making inserts larger, not by
allowing unordered concurrent writes. Parallel lanes are separate future work
because they require a new proof for deduplication-window and visibility
ordering.

The initial tuning envelope is:

| Control | Initial value | Role |
| --- | ---: | --- |
| target rows | 10,000 | Flush once this many rows are available. |
| hard maximum rows | 50,000 | Never form a larger write group. |
| target decoded bytes | 16 MiB | Flush large events without waiting for the row target. |
| hard maximum decoded bytes | 64 MiB | Bound driver and ClickHouse memory per send. |
| maximum member batches | 10,000 | Still reach the row target for one-event HEC requests. |
| maximum linger | 1 s | Bound added visibility latency under sparse traffic. |

These are the values returned by `clickhouse.DefaultConfig`. Code embedding
the store may override them through `clickhouse.Config`, subject to the
validation and hard ceilings below. The shipped server uses the defaults;
it does not expose insert coalescing as a command-line or environment tuning
surface. A benchmark-driven change to a default must update this document and
its load-test evidence.

## Operator contract

`clickhouse.Config` exposes `InsertTargetRows`,
`InsertTargetDecodedBytes`, `InsertMaxRows`, `InsertMaxDecodedBytes`,
`InsertMaxMembers`, and `InsertMaxLinger`. Zero values inherit the defaults in
the table above. Configuration is rejected unless:

- row and decoded-byte targets are no greater than their corresponding
  maxima;
- the row and byte maxima can contain one already-valid logical batch;
- row, byte, and member maxima do not exceed the durable schema ceilings of
  50,000 rows, 64 MiB, and 10,000 members;
- maximum linger is positive and no greater than 1 second.

The two size targets are soft. Reaching either target seals ordinary queued
work. The three maxima are hard: a group never crosses one to add another
logical batch, and replay revalidates them before ClickHouse is touched. A
valid single logical batch is never split; the validated maximum envelope is
large enough to hold it.

Maximum linger is measured from the oldest eligible reservation's durable
creation time. Low-rate traffic therefore produces smaller inserts instead of
waiting indefinitely. Added visibility latency in that case is up to 1 second,
plus ClickHouse send and SQLite finalization time. Restart does not reset the
deadline, and shutdown/freeze may force a drain earlier.

Durable admission is independently bounded at 20,000 unresolved logical
reservations, 256 MiB of aggregate outbox data, and 64 MiB of aggregate result
metadata. These are ceilings, not buffer targets. Admission returns the
existing transient capacity response when any applicable ceiling fills, such
as during a ClickHouse outage. Restoring ClickHouse drains the oldest
ambiguous group first, followed by sealed ready groups and then ungrouped work.
Do not delete SQLite rows or alter group state to clear pressure; that destroys
the replay authority. Fix storage reachability or capacity and let the single
sender reconcile it.

ClickHouse insertion remains synchronous (`async_insert=0` and
`wait_for_async_insert=1`). An insert smaller than 1,000 rows is expected under
sparse traffic, startup recovery, or a forced final drain and is not by itself
an incident. Sustained small inserts while at least 10,000 rows are continuously
eligible indicate a stuck or mistuned coalescer and require checking queue age,
group fill reasons, retries, and ClickHouse merge pressure.

## Why this work is necessary

The collector deliberately creates small durable units: at most 500 events,
1 MiB of encoded events, or 250 ms of linger. Before server-side coalescing,
the native store sent each accepted logical batch as its own synchronous
ClickHouse insert, and the background reconciler acquired and sent only one
reservation at a time. The store still explicitly sets `async_insert=0`; the
larger block now comes from the durable server-side group.

That is a good correctness baseline and a poor physical write shape. A
MergeTree insert creates one or more immutable parts, and ClickHouse must later
merge small parts. Too many small inserts spend CPU, I/O, memory, and scheduler
capacity on part creation and merging instead of ingestion and search. Current
ClickHouse guidance recommends at least 1,000 rows per synchronous insert and
ideally 10,000 to 100,000 rows. See [ClickHouse high-concurrency sizing](https://clickhouse.com/resources/engineering/high-concurrency-sizing-user-analytics)
and [ClickHouse guidance on small inserts](https://clickhouse.com/blog/common-getting-started-issues-with-clickhouse).

Increasing the collector batch size is not an adequate solution. It moves
latency and crash-loss pressure to every producer, does nothing for many small
HEC requests, and changes the collector WAL and retry unit. Enabling
ClickHouse asynchronous inserts alone is also insufficient for this repository:
the server-owned reconciler currently submits one insert and waits before
submitting the next, so it cannot reliably fill a ClickHouse-side buffer under
all traffic shapes. It would also make correctness depend more heavily on
version-specific asynchronous-insert deduplication behavior.

The required separation is:

- a **logical batch** remains the unit of identity, quota, acknowledgment,
  replay outcome, and visibility sequence;
- a **physical write group** becomes the unit of ClickHouse insertion and
  ambiguous-send recovery.

## Current contract that must survive

Admission persists a normalized replay outbox and response metadata in SQLite
before ClickHouse can be touched. Immediately before `Send`, one transaction
changes the group and all member reservations from ready/unsent to ambiguous.
A lost response is recovered by replaying the exact sealed group with its same
deduplication token. A search only captures a visibility cutoff through the
highest contiguous terminal sequence. HEC indexer acknowledgments derive their
terminal state from the same atomic member commit.

Coalescing must preserve all of the following:

1. Reusing a logical batch identity with different content is a conflict.
2. An exact retry receives the original durable result and never re-runs
   mutable policy to invent a new result.
3. No row is searchable above the captured visibility cutoff.
4. A batch cannot be reported committed before its ClickHouse insert is
   confirmed and its SQLite reservation is terminal.
5. A failure after a possible ClickHouse side effect is replayed, never
   abandoned.
6. A ClickHouse retry uses stable inserted row values and a stable
   deduplication identity.
7. Quota is charged once per logical batch, during admission, not once per
   physical replay.
8. HEC request and acknowledgment rows move to their correct terminal state
   with the corresponding logical reservation.
9. Freeze, deletion, backup, shutdown, and startup recovery cannot overlook
   accepted work merely because it is waiting in a coalescer.

## Terminology

**Reservation** means one durable logical batch in
`ingest_visibility_reservations`. It owns one visibility sequence, response
metadata, and a normalized outbox.

**Write group** means an immutable, ordered set of one or more reserved
reservations reproduced as one ClickHouse insert.

**Ready** means group membership is sealed and no ClickHouse side effect has
started.

**Ambiguous** means the group was durably marked immediately before `Send`; the
insert may or may not have reached ClickHouse.

**Terminal** means every member reservation is committed and its outbox has
been cleared. A group is not terminal one member at a time.

## Required correctness invariants

The implementation is acceptable only if these are enforced by schema,
transactions, and tests rather than comments alone.

### Membership and identity

- A pending reservation belongs to at most one non-terminal write group.
- Group members are ordered by visibility sequence. Their order is persisted
  and replayed exactly.
- A logical batch is never split across write groups. A group may
  exceed a soft target to admit one already-valid batch, but it must never
  exceed a hard bound.
- The physical rows retain their original `batch_id`, `batch_sequence`,
  `event_id`, `visibility_seq`, source, tenant, index, index time, and retention
  values.
- The group has a stable, non-secret ID used as
  `insert_deduplication_token`. Per-batch deduplication keys remain the logical
  identity authority in SQLite but are not used as the physical insert token.
- A sealed membership digest covers a format discriminator plus the ordered
  member sequence, batch key, payload digest, outbox length, and outbox digest.
  It is recomputed before every send. A mismatch fails closed without touching
  ClickHouse.

### Send and replay

- Group membership and its deduplication token are committed before preparing
  or sending ClickHouse rows.
- All members are durably changed to the group-owned ambiguous phase in one
  SQLite transaction before `prepared.Send()`.
- Once a group is ambiguous, the server may only replay that exact group. It
  may not add members, remove members, reorder rows, or mint a new token.
- An ambiguous group is retried before any newer ready group. This preserves
  the current protection against evicting an uncertain token from the
  ClickHouse deduplication window.
- A send error remains ambiguous unless the code can prove that no ClickHouse
  side effect began. The default classification is ambiguous.
- A deterministic replay/decode invariant failure blocks the affected group
  and makes reconciliation unhealthy. It must not silently discard, partially
  commit, or regenerate a batch from caller input.

### Commit and visibility

- A successful ClickHouse send is followed by one serializable SQLite
  transaction that commits every member, clears every member outbox, updates
  HEC terminal state, marks the group terminal, and advances the contiguous
  visibility cutoff once.
- No member may become committed if that transaction rolls back.
- All members use the same physical-send `committed_at` value, while keeping
  distinct logical result metadata.
- In-process native callers may be notified after commit, but notifications
  are an optimization. SQLite lookup remains the authority after timeout,
  disconnect, crash, or restart.
- It is valid for several adjacent batches to become visible at the same
  instant. It is not valid for part of a write group to become visible.

## Durable data model

The fresh-state SQLite baseline persists group ownership in the following two
tables. Their constraints are part of the v0 fresh-state contract.

### `ingest_write_groups`

Required fields:

- `write_group_id`: stable opaque identifier and ClickHouse deduplication
  token; primary key; bounded length; no secret material;
- `state`: `ready`, `ambiguous`, or `committed`;
- `attempt_id`: bounded live-process lease, empty when unowned;
- `member_count`, `row_count`, and `decoded_bytes`: positive bounded totals;
- `membership_sha256`: digest of the sealed ordered membership;
- `first_sequence` and `last_sequence`: validated member bounds;
- `created_at_unix_micro`, `sending_at_unix_micro`, and
  `committed_at_unix_micro` with state-dependent checks.

### `ingest_write_group_members`

Required fields:

- `write_group_id` foreign key;
- `ordinal`, contiguous from zero within the group;
- `visibility_sequence`, unique across active and retained groups;
- `row_count`, `decoded_bytes`, and `outbox_sha256` for bounded reconstruction
  and corruption detection.

Group formation does not decode large outbox blobs while holding the SQLite
write transaction. Bounded `stored_row_count`, `decoded_event_bytes`, and
`outbox_sha256` values are populated on each pending reservation in the
original admission transaction. They are server-derived, checked against the
outbox during replay, and cleared at terminal commit. Accepted rows are never
inferred from a client-supplied count.

Use a unique index or equivalent constraint so a reservation cannot enter two
groups. Use foreign keys that prevent pruning a reservation while retained
group membership still references it. Terminal group retention only needs to
cover transactional cleanup and diagnostics; logical idempotency retention
continues to be governed by the existing reservation/identity horizon.
Pruning must delete terminal group membership before deleting referenced
logical reservations, in the same bounded maintenance transaction.

Do not copy outbox blobs into the group table. The reservation already owns the
authoritative normalized payload. Duplicating it would double SQLite write
amplification and complicate atomic cleanup.

Because this project currently promises fresh-state schema compatibility only,
the tables and columns live in `migrations/sqlite/0001_baseline.sql` and its
schema contract tests. A future compatibility policy must introduce a real
migration instead of silently rewriting deployed state.

## Sequencer API and transaction boundaries

`visibility.WriteGroupSequencer` extends the retained one-reservation
compatibility surface with bounded `WriteGroupLimits` and a `WriteGroup`
carrying the stable ID, state, ordered reservations, row/byte totals, and
membership digest. Its group operations are:

- `FormOrAcquireWriteGroup`: seal new work or lease the oldest recoverable
  group;
- `MarkWriteGroupSending`: atomically make the group and every member
  ambiguous;
- `CommitWriteGroup`: atomically finalize the group and every member;
- `ReleaseWriteGroup`: remove only the live-process lease.

The lease belongs to the group. Do not write the same `attempt_id` into every
member reservation: the existing schema makes nonempty attempt IDs unique, and
group ownership should have one fencing record. Reservation phases may change
to group-owned ambiguous while their request attempt IDs remain empty.

`FormOrAcquireWriteGroup` runs in one serializable transaction and:

1. acquire the oldest unowned ambiguous group if one exists;
2. otherwise acquire the oldest sealed ready group;
3. otherwise select ungrouped, unleased, unsent reservations in ascending
   sequence order;
4. stop before adding a member that would cross a hard row, byte, or member
   bound;
5. seal when a soft row/byte target is met, a hard bound would be crossed, or
   the oldest eligible reservation has reached maximum linger;
6. return no work when the group is below all soft targets and its durable
   linger deadline has not arrived;
7. atomically persist the group, members, totals, digest, and attempt lease.

When step 6 returns no work, it returns the next durable linger deadline so the
worker can arm an exact timer without polling.

The linger deadline is based on the oldest reservation's durable creation
time, not an in-memory timer. A restart therefore cannot postpone a sparse
batch forever.

Group finalization updates members with a bounded prepared-statement loop
inside one transaction rather than constructing an unbounded `IN` list. The
same transaction activates the existing HEC transition logic for each member.

Compatibility wrappers for one-member reservations may remain temporarily
while tests and callers migrate, but production sending must have one owner.
Two independent reconciliation paths must not race to send the same pending
work.

## Store and coalescer behavior

`internal/clickhouse/store.go` separates durable admission from physical
completion for both protocols.

### Admission

`Stage` is the durability boundary: it persists policy-derived
metadata, quota, HEC state, the outbox, and a visibility sequence, then releases
the request attempt and wakes the coalescer.

For native ingestion, `Store` performs the same durable staging and then waits
for the reservation's terminal result. A process-local waiter registry keyed
by visibility sequence avoids polling. Its contract is:

- register without making the waiter authoritative;
- recheck SQLite after registration to close the commit-before-register race;
- wake every waiter whose reservation was committed by a group;
- remove waiters on completion or context cancellation;
- if the request context expires, leave the durable reservation intact and
  return a transient response; an exact collector retry discovers the same
  pending or terminal outcome;
- enforce a bounded waiter count tied to durable pending capacity.

The waiter also retains whether this call created the logical reservation, so
a fresh caller receives accepted counts while an exact later retry receives
duplicate counts. Grouping must not collapse those per-call response semantics.

HEC continues to return its existing staged/pending or terminal result without
waiting for ClickHouse. Its indexer acknowledgment becomes true only after the
group commit transaction changes that reservation to committed.

### Group construction and insertion

For each acquired group, the sender:

1. decode every member from its durable outbox;
2. revalidate each reservation identity and the sealed group digest;
3. rebuild rows in member order and event order;
4. apply each member's original visibility sequence;
5. verify actual totals against persisted row/byte/member totals and hard
   limits;
6. prepare one native insert using the group ID as the deduplication token;
7. append every row;
8. mark the group ambiguous in SQLite;
9. call `Send` once;
10. commit the entire group in SQLite;
11. close the prepared insert and notify native waiters.

The sender keeps `async_insert=0` and `wait_for_async_insert=1`.
Application-level coalescing gives this repository one clear
durable replay unit. ClickHouse async inserts may be benchmarked later, but
enabling two independent batching layers is out of scope.

### Empty and oversized cases

Only reservations containing at least one accepted event enter a write group;
whole-batch rejections remain SQLite-only terminal outcomes. A partially
accepted logical batch groups only its accepted rows and retains all rejection
metadata for its response.

A single valid logical batch that exceeds a soft target forms a one-member
group. No valid batch can exceed the physical hard maximum under the admitted
event and byte envelopes; configuration and invariant tests pin that
relationship.

## Backpressure and resource bounds

The durable queue permits 20,000 pending reservations: enough for one
10,000-member group in flight and another to assemble from one-event requests.
It independently caps aggregate outbox payload at 256 MiB and aggregate result
metadata at 64 MiB. The first reached ceiling applies backpressure, so actual
pending membership may be much lower for large events.

Changing those constants requires a sizing calculation and tests covering:

- maximum SQLite outbox bytes;
- maximum decoded rows and bytes for one group;
- maximum in-process row slice and driver-batch memory;
- maximum native waiters;
- time and statement count for group formation and commit;
- admission behavior while ClickHouse is unavailable.

Admission returns the existing bounded transient failure when reservation,
outbox-byte, or metadata-byte capacity is exhausted. The coalescer loads no
more than one group's decoded rows into memory and discards reconstructed
memory before retry backoff.

## Lifecycle integration

The coalescer is a server-owned lifecycle worker, not a request goroutine.

- **Startup:** recover and replay the oldest ambiguous group first, then ready
  groups, then form groups from ungrouped pending reservations.
- **Normal wake:** a successful stage sends a non-blocking wake. The worker also
  owns a timer for the nearest durable linger deadline and retry delay.
- **Shutdown:** stop new admission, force-seal eligible ungrouped work, drain
  within the existing bounded shutdown context, stop the worker, wait for its
  goroutine, then close connections. Work that cannot drain remains durable.
- **Write freeze/index deletion:** join the same write-admission gate as current
  reconciliation. An exclusive drain must prove there are no reserved
  reservations, ready groups, ambiguous groups, or live group leases before
  the callback can mutate storage.
- **Backup:** the SQLite snapshot must never represent an ambiguous group
  without its complete membership and member outboxes. Backup coordination
  must use the same freeze/drain proof already required for ClickHouse and
  control-state consistency.
- **Health:** a persistent decode, digest, schema, or finalization failure makes
  reconciliation unavailable. Sparse traffic waiting only for its linger
  deadline is healthy.

## Failure matrix

| Failure point | Durable state | Required recovery |
| --- | --- | --- |
| Before reservation commit | No accepted batch | Caller retries fresh admission. |
| After reservation commit, before grouping | Ungrouped `unsent` reservation | Coalescer includes it in a later group. |
| During group-formation transaction | Old state or complete sealed group | SQLite rollback/commit decides; never a partial membership. |
| After group seal, before ambiguous transition | `ready` group | Replay the same membership and token. |
| Ambiguous transition fails with proven rollback | `ready` group | Release lease and retry; no ClickHouse send occurred. |
| Ambiguous transition result is uncertain | Treat as `ambiguous` until reread | Do not abandon; reacquire and replay exact group. |
| During append, before ambiguous transition | `ready` group | Abort prepared batch, release lease, retry or fail closed. |
| During or after `Send` | `ambiguous` group | Replay exact rows with the same group token. |
| Send succeeds, SQLite commit fails | `ambiguous` group; rows may exist | Replay exact token, then retry atomic group commit. |
| Group commit succeeds, response/wakeup is lost | terminal members and group | Retried native/HEC lookup returns durable per-batch result. |
| Process crashes with ungrouped and grouped work | SQLite is authoritative | Resolve ambiguous, ready, then ungrouped work in order. |

Every row in this table requires at least one deterministic unit or integration
test. The uncertain-result rows require injected failures both before and after
the underlying SQLite commit where the harness can distinguish them.

## Observability

`Store.InsertCoalescingTelemetry` returns one payload-free,
fixed-cardinality snapshot. It contains no tenant, index, collector, batch,
group, channel, token, payload, or error-text labels. Its counters, gauges, and
histograms cover:

- staged logical batches and rows;
- formed groups, physical sends, successful groups, retries, and ambiguities;
- member batches, rows, decoded bytes, and distinct monthly partitions per
  group;
- group fill reason: row target, byte target, hard boundary, linger, drain, or
  recovery;
- time from reservation creation to group seal, send start, and terminal
  commit;
- native waiter count, wakeups, cancellations, and terminal lookups;
- pending ungrouped reservations, ready groups, ambiguous groups, pending
  outbox bytes, and oldest pending age;
- physical inserts per logical batch and rows per physical insert.

The opt-in load gate separately reads ClickHouse's `system.query_log` and
`system.parts` to report physical inserted rows, active parts, part growth,
delayed or rejected inserts, and merge pressure on the pinned server image.
That inspection grant exists only in the disposable integration fixture.

Logs may include aggregate counts and a bounded internal group ID only where
needed for operator correlation. They must not include payloads, secrets, HEC
channels, or source-derived identifiers.

The shipped administrator-only `POST /api/hec/operations/get` response exposes
the complete bounded `InsertCoalescingTelemetry` projection as
`insert_coalescing`. The public snapshot includes the fixed-domain fill-reason
vector; fixed-bound member, row, byte, partition, and physical-insert
histograms; fixed-bound seal/send/commit latency histograms; native waiter
counters; and the complete durable coalescer queue gauges listed above. It is
process-wide and intentionally omits tenant, index, collector, batch, group,
channel, token, payload, and error-text labels. The opt-in load gate augments
that API with disposable-fixture ClickHouse query-log, active-part, delayed or
rejected insert, and merge-pressure evidence.

## Test plan

### SQLite and sequencer tests

- schema rejects duplicate membership, noncontiguous ordinals, invalid totals,
  impossible timestamps, and illegal state transitions;
- formation selects the oldest eligible reservations and respects every soft
  and hard threshold;
- linger uses durable creation time and fires immediately after restart when
  already expired;
- ambiguous acquisition always precedes newer ready or ungrouped work;
- competing worker attempts cannot own or form overlapping groups;
- group send transition is all-or-none for members;
- group commit is all-or-none, clears all outboxes, fires all HEC terminal
  transitions, and advances cutoff to exactly the highest contiguous terminal
  sequence;
- prune cannot remove member authority needed by a pending group;
- closed/fenced sequencers reject every new group operation.

### Store unit tests

- multiple logical batches produce one `Prepare`, one `Send`, and the expected
  ordered appended rows;
- the insert uses the group token, synchronous insert settings, and existing
  JSON settings;
- every member row keeps its own batch and visibility values;
- exact replay produces identical appended values and token;
- corrupted member outbox, digest, totals, or identity prevents `Send`;
- append/mark/send/commit/close failures follow the failure matrix;
- native waiters handle commit-before-register, cancellation, multiple members,
  restart/poll fallback, and no goroutine leaks;
- partial logical acceptance returns exact rejection metadata after commit;
- HEC staged behavior and acknowledgment timing remain unchanged;
- freeze, close, and reconciler wake/timer races pass under `go test -race`.

### ClickHouse integration tests

- send at least 25 logical collector batches as one 10,000-or-more-row physical
  insert and verify every event exactly once;
- mix native and HEC reservations, multiple tenants/indexes, rejected events,
  and batches crossing monthly partitions;
- kill the server at each durable boundary, restart it, drain, and verify exact
  event IDs, terminal outcomes, HEC ACK truth, and visibility cutoff;
- force a lost ClickHouse response after storage accepts a group and prove the
  stable token prevents duplicate rows;
- hold ClickHouse unavailable until durable capacity applies backpressure,
  restore it, and prove complete ordered drain without exceeding bounds;
- exercise index deletion/write freeze while groups are ungrouped, ready,
  sending, and ambiguous;
- verify the pinned ClickHouse version and table
  `non_replicated_deduplication_window` still satisfy the replay proof.

### Load and regression gates

Extend the existing backend and HEC load suites to record physical insert
shape from ClickHouse query logs and part shape from `system.parts`. Keep the
existing duplicate, ACK, visibility, scheduler-lag, memory, goroutine, thread,
outage, and full-drain assertions.

Under a steady-state offered load that always has at least 10,000 eligible rows
before maximum linger, excluding startup, final drain, and explicit recovery
flushes:

- median physical insert size is at least 10,000 rows;
- at least 90% of physical inserts contain at least 5,000 rows;
- no physical insert exceeds configured row, byte, or member hard limits;
- physical inserts are at most one tenth of logical accepted batches over the
  measured window;
- duplicate rows remain zero by `count() = uniqExact(event_id)`;
- all accepted logical batches reach their exact terminal result;
- visibility and HEC acknowledgment assertions remain unchanged;
- peak pending bytes, process memory, goroutines, and threads remain within
  explicit test bounds;
- active-part growth and delayed-insert behavior improve relative to a captured
  pre-change baseline on the same pinned image and host profile.

The percentile criteria apply only when sufficient work exists. Sparse traffic
must flush by maximum linger even if that creates a sub-1,000-row insert. It is
impossible to guarantee both a minimum physical block size and bounded
near-real-time latency when fewer rows arrive.

### Operator verification commands

The always-on schema, sequencer, sender, failure-injection, metric, waiter, and
load-assertion unit tests run with:

```sh
go test ./internal/visibility ./internal/clickhouse ./integration
go test -race ./internal/visibility ./internal/clickhouse
```

The real ClickHouse sender and shipped-process recovery gates are opt-in
because they own Docker containers and build binaries:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/clickhouse -run '^TestStoreAgainstClickHouse$' \
  -count=1 -timeout=10m -v

OPEN_SPLUNK_BACKEND_LOAD=1 \
go test ./integration -run '^TestBackendSustainedLoad$' \
  -count=1 -timeout=12m -v

OPEN_SPLUNK_HEC_LOAD=1 \
go test ./integration -run '^TestBackendHECDurableLoad$' \
  -count=1 -timeout=15m -v

OPEN_SPLUNK_HEC_LOAD=1 \
OPEN_SPLUNK_HEC_LOAD_PROFILE=small-only \
OPEN_SPLUNK_HEC_LOAD_EVENTS_PER_SECOND=1000 \
go test ./integration -run '^TestBackendHECDurableLoad$' \
  -count=1 -timeout=15m -v
```

The HEC gate first durably stages 30 independent 1,000-event requests during a
ClickHouse outage and retains the existing recovery, exact event-ID, ACK,
resource-bound, and full-drain assertions. After recovery and an explicit
query-log quiescence barrier, a separate live 40-request phase keeps ClickHouse
available and validates the steady-state envelope against exactly 40 logical
batches and 40,000 rows. The query-log predicate accepts only finished inserts
whose `databases` and `tables` metadata identify `open_splunk.events`; active
parts, delayed/rejected inserts, and merge counters are compared with an
explicit pre-window baseline. The small-only invocation separately exercises
sustained one-event admission and bounded sparse-linger flushes.

On the acceptance run for the 1-second bounded-linger default, the live phase
formed exactly four 10,000-row physical inserts from 40 logical batches. Active
parts grew by four for the four inserts, active rows grew by exactly 40,000,
the global merged-row counter advanced by 24,557, and no merge remained active
at the final snapshot. The pinned fixture did not expose delayed-insert,
rejected-insert, or merge-time counters; the gate explicitly reports those
signals as unsupported instead of interpreting absent counters as zeros.

## Implementation slices

Implement and review this as small correctness-preserving slices:

1. **Measurement baseline:** instrument current logical/physical insert counts,
   rows, parts, and latency; capture the existing load profile.
2. **Durable group schema:** add tables, constraints, group types, formation,
   acquisition, release, ambiguous transition, atomic commit, and exhaustive
   SQLite tests without changing production sending.
3. **Grouped sender:** reconstruct and validate a group, send one physical
   block, and implement the complete failure matrix behind a disabled runtime
   switch used by tests.
4. **Unified staged admission:** make native `Store` stage-and-wait, add bounded
   waiters, and keep HEC stage semantics unchanged.
5. **Lifecycle and backpressure:** replace the one-reservation reconciler with
   the coalescer timer/wake loop and integrate freeze, drain, close, health, and
   capacity.
6. **Integration and crash tests:** add exact-once, visibility, HEC ACK,
   outage, restart, deletion, and deduplication-window gates.
7. **Performance rollout:** enable coalescing by default, tune soft defaults
   within the fixed hard envelope, and publish before/after measurements.
8. **Cleanup:** remove the old production one-reservation send path and its
   compatibility wrappers after all callers and tests use groups.

Each slice must leave one unambiguous production sender. Do not temporarily
run old and new reconcilers against the same reservation set.

## Exit criteria

This task is complete only when all of the following are true.

### Functional

- Native and HEC ingestion use the server-owned coalescer in production.
- A sustained eligible workload forms physical inserts in the thousands, with
  a 10,000-row median under the defined load gate.
- Logical batch identity, duplicate reporting, partial rejection metadata,
  quota charging, visibility sequence, collector acknowledgment, and HEC
  acknowledgment remain independently correct for every group member.
- Sparse traffic becomes searchable within maximum linger plus measured
  storage/finalization latency.

### Recovery and safety

- All failure-matrix cases have automated tests.
- Crash/restart and lost-response integration tests prove zero missing and zero
  duplicate event IDs.
- An ambiguous group blocks newer physical sends until reconciled.
- Group commit and HEC terminal transitions are one SQLite transaction.
- Durable and in-memory queues have tested hard row, byte, member, and waiter
  bounds.
- Freeze/drain proves all four states empty: ungrouped reservations, ready
  groups, ambiguous groups, and live group leases.

### Performance and operations

- The load gate meets its insert-shape criteria without relaxing any existing
  correctness or resource assertion.
- Before/after results on the same pinned ClickHouse image show fewer physical
  inserts and lower active-part growth; raw measurements are retained in test
  output or an operator-approved benchmark artifact.
- Readiness reports a stuck replay invariant as unavailable and exposes queue
  age/shape without high-cardinality or sensitive labels.
- Operators can distinguish low-load linger flushes from hard-bound,
  backpressure, retry, and recovery behavior.

### Repository quality

- Public configuration and operator documentation describe defaults, hard
  limits, latency tradeoffs, capacity math, telemetry, and recovery behavior.
- The standard verification suite passes, including Go tests, race-sensitive
  coalescer tests, ClickHouse integration, backend load, HEC load, freeze/index
  deletion, and documentation checks.
- No test is weakened, no lint suppression is added, and the old synchronous
  one-logical-batch-per-insert production path is removed.

## Explicit non-goals

- distributed ingestion or a multi-node visibility sequencer;
- parallel physical send lanes;
- changing collector WAL batch identity or HEC request identity;
- splitting one logical batch across physical groups;
- weakening `wait_for_async_insert`, returning success before SQLite commit, or
  acknowledging in-memory-only buffering;
- relying on scheduled `OPTIMIZE TABLE` to clean up excessive parts;
- changing event schema, search semantics, retention semantics, or authorization;
- claiming that every insert is large during sparse traffic, startup recovery,
  or shutdown drain.

## Decisions to confirm during the measurement slice

The architecture above is fixed; these tuning choices require evidence:

- whether a future maximum-linger reduction below 1 second can protect
  end-to-end native acknowledgment latency;
- whether decoded bytes can be measured without reconstructing rows twice;
- the exact pending-reservation count that fits the existing 256 MiB outbox
  ceiling and expected HEC request sizes;
- whether cross-month groups create enough parts to justify a bounded
  partition-count soft flush signal;
- how long terminal write-group rows should remain for diagnostics after member
  reservations commit;
- which ClickHouse query-log fields provide a stable physical inserted-row
  assertion on the pinned server image.

None of these questions permits weakening the replay, visibility, atomicity, or
resource-bound requirements above.
