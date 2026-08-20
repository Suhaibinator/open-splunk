# Database schema mechanics

Open Splunk is pre-release and supports fresh development databases for the
current source tree. SQLite and ClickHouse each have a consolidated baseline
that creates the complete current schema directly. No cross-revision upgrade,
backfill, downgrade, or adoption path is currently promised.

Migration sequence numbers, ledger digests, entity/catalog revisions, and
private row-format counters are implementation mechanics. They detect drift,
stale writes, and unrecognized local state; they are not product versions or
public compatibility levels.

## Ledger contract

Migration files are embedded into the server. The runner requires the exact
contiguous sequence declared by the current source and records each version,
filename, and SHA-256 digest. A fresh database applies the baseline; a retry
validates the same name/digest and is safe. Missing, reordered, renamed,
modified, duplicated, or unexpected rows fail as drift.

An unrecognized ledger or unledgered legacy schema is not silently adopted,
rewritten, or deleted. Provision a fresh development database or volume and
retain old state separately if forensic access is required.

SQLite applies each migration transactionally. ClickHouse DDL is not
transactional, so its baseline uses retry-safe DDL and writes the ledger row
last. GORM never runs `AutoMigrate`.

## SQLite control plane

SQLite is the authority for catalogs, optimistic-concurrency revisions,
authentication/token metadata, collectors, jobs/history/export,
knowledge/lookups, visibility, quota schedules, ingestion request/outbox state,
audit journals, cursors, and recovery metadata.

Business mutations and their audit/receipt/catalog changes share one
caller-owned transaction. The schema uses explicit foreign keys, uniqueness,
bounds, state transitions, and immutable-row triggers so malformed state fails
at the storage boundary and again during hydration.

SQLite and the server master-key/administrator-token files are one control-
plane recovery unit. Do not copy a live database independently of the
coordinated recovery procedure in [`deploy/README.md`](../deploy/README.md).

## ClickHouse event store

One `open_splunk.events` table stores all logical indexes. Tenant and index lead
the sort/primary key, followed by bounded time and stable identity components.
Monthly event-time partitions keep cardinality bounded. `event_time` preserves
nanoseconds, `index_time` records server admission, and `expires_at` stores the
index retention decision at admission. TTL removes expired rows in background
without relying on whole-part deletion across mixed index policies.

Promoted columns represent canonical event metadata. Optional promoted Strings
remain nullable so missing and empty differ. Dynamic `fields` uses the native
ClickHouse JSON type with bounded dedicated paths/types; aligned `field_names`
and `field_types` preserve canonical dotted paths and stable logical value
types, including null and extended bytes/time/duration/decimal values. The
writer derives metadata and its private format discriminator from the original
typed protobuf value rather than inferring it later from physical JSON.

Canonical SPL aliases hide physical names: `index` maps to `index_name`,
`_time` to `event_time`, `_indextime` to `index_time`, `_raw` to `raw`, and
`message` to `body`. Raw bytes use the native byte-safe insertion path; they
must not be UTF-8-repaired or base64-replaced.

## Visibility, retries, and recovery

SQLite reserves a stable positive visibility sequence, index time, and
per-index retention snapshot before ClickHouse insertion. Successful insert
marks the reservation committed; a known pre-send failure may be skipped; an
ambiguous send retains the gap and identical outbox payload until
reconciliation proves its result.

Search captures the highest contiguous committed sequence and one immutable
index-time cutoff. Every event scan applies tenant/index/time authority plus
`index_time <= cutoff`, `expires_at > cutoff`, and
`visibility_seq <= committed_cutoff`. TTL expiry therefore cannot make the
logical visibility high-water move backward.

Native collector and HEC retries reuse deterministic event identities, row
order, settings, and source-revision-specific deduplication domains.
ClickHouse's MergeTree deduplication is a bounded recent-block window, not
global exactly-once delivery. An acknowledgment is issued only after commit
authority; ambiguous outcomes keep durable work pending.

SQLite and ClickHouse form one coordinated recovery generation. A recovery
set binds their exact private schema/migration counters and digests. Restore
never mixes a control plane from one generation with ClickHouse from another.
Unknown state fails closed; recovery does not discard it as success.

## Verification

Fast schema, final-shape, ledger, digest, drift, and retry tests run in the
default suite. ClickHouse smoke tests reapply the baseline, verify typed
JSON/metadata and retry deduplication, and use the repository-pinned image:

```sh
go test ./migrations/...

OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./migrations/clickhouse -run AgainstClickHouse -count=1 -v
```

Set `OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` only for an intentional pinned-image
comparison. A schema smoke pass does not replace ingestion, query, recovery,
load, or soak gates.
