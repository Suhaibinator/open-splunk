# Database schema mechanics

OpenSplunk stores runtime control state and GoCommon identity state in one
PostgreSQL database and schema. ClickHouse continues to store event data.
MinIO stores immutable retained search snapshots and exports; PostgreSQL owns
their metadata, authorization, quotas, publication and retention state.

## Migration ownership

`postgres/` contains OpenSplunk's native, contiguous versioned migration
history, beginning with `0001_baseline.sql`. The baseline creates the final
control schema, including explicit foreign keys, checked scalar bounds,
immutable-row/state-transition guards and accounting triggers. Follow-up SQL
files add schema; the server never imports or interprets SQLite files.
`0008_export_source_identity.sql` pins each export to its source job. Reaping
may remove expired source objects while preserving the source authorization
metadata until dependent exports and object cleanup reservations are gone.

The PostgreSQL runner verifies each existing ledger row's name, version and
SHA-256 against the embedded file before applying a pending suffix. It rejects
modified, missing, gapped, future and ledgerless control schemas. Migration DDL
and ledger rows commit together under a schema-scoped advisory transaction
lock. The ledger is `open_splunk_schema_migrations`.

GoCommon independently owns its `gocommon_*` tables and
`gocommon_schema_migrations`. Startup borrows the application's PostgreSQL
pool for GoCommon initialization. Immutable named application migrations run
through `DB.ApplyAdditionalMigration` after GoCommon's schema is ready; their
separate ledger adds resource-organization foreign keys without mixing the
two schema owners' histories. OpenSplunk never calls AutoMigrate on its own
control models.

A pinned PostgreSQL session advisory lease admits one production server per
database/schema. Maintenance commands use the same lease across the entire
recovery generation. Losing that connection requires the process to stop
serving. SQL and GORM domain repositories share one application-owned pool;
the lease uses a dedicated connection so it cannot exhaust a small data pool.

## Transaction contracts

Metadata writes acquire a schema-scoped advisory transaction lock before
reading authority or capacity. Statement triggers acquire the same lock for
direct SQL writes. Mutations, audit events, idempotency receipts, catalog
revisions and quota/visibility accounting remain one transaction. Read-only
multi-query snapshots use repeatable read. Retried transactions start fresh;
a PostgreSQL error never retries a statement inside an aborted transaction.

Each index has one owning organization. Canonical app, saved search,
dashboard, knowledge and lookup authorities carry organization IDs. Global
sharing remains inside that organization. Organization membership precedes
ordinary users' index grants, while site administrators retain their universal
authority. Referential guards prevent deletion of organizations with dependent
resources.

An immutable MinIO object is published in PostgreSQL only after its upload
succeeds. Object storage and SQL cannot share a transaction, so incomplete
publication and deletion use durable reconciliation. Presigned capabilities
are issued only after authorization; source-index permissions are checked
again for retained results, downloads and long-lived consumers.

## Event visibility and recovery

PostgreSQL reserves stable visibility sequences, index times, retention
snapshots and immutable ingestion outboxes before ClickHouse insertion.
Successful insert records commit authority; ambiguous outcomes retain the
gap and exact outbox bytes until reconciliation. ClickHouse queries retain
all existing tenant/index/time/visibility predicates and deduplication rules.

Native PostgreSQL `pg_dump`/`pg_restore`, MinIO object references, the master
key and ClickHouse's existing archive form one coordinated recovery
generation. Restore requires an empty target and never silently overwrites
existing state. The former SQLite preservation utility is a separate,
disposable deliverable sourced from the previous release; production does
not contain a SQLite driver, migration history or fallback.

## Verification

Embedded migration tests pin exact file digests. Control, identity,
authorization and repository tests use isolated PostgreSQL schemas and fail
when `OPEN_SPLUNK_TEST_POSTGRES_DSN` is unavailable; they never silently skip.
Native backup tests additionally require matching `pg_dump` and `pg_restore`
clients on PATH. Database tests validate fresh initialization, restart,
concurrent startup, drift, constraint/state forgery, rollback, quota races,
lease fencing and recovery preservation.

ClickHouse migration files and their physical event contracts are unchanged.
Its integration gate remains:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./migrations/clickhouse -run AgainstClickHouse -count=1 -v
```
