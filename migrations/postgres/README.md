# PostgreSQL control-plane schema

These files port the final SQLite control-plane schema (`migrations/sqlite/0001`
through `0012`, with every migration applied) to PostgreSQL. `0001` through
`0006` hold the tables, keys, CHECK constraints, indexes and seed rows;
`0007` through `0018` port the triggers onto those tables, and `0019` pins the
`search_path` of the routines that came without it. `0020` and `0021` restore
SQLite properties the port lost: commit-ordered `durable_search_jobs` cursors,
multi-row capacity checks, and btree key sizes under the index-row limit. The
[Ordering](#ordering) table lists every file.

The SQLite files are still the live schema. These files are embedded
(`migrations.Postgres()`) and applied by the control-plane runner
(`control.ApplyPostgresMigrations`) in tests, but the server does not run them
yet. Each file's SHA-256 is pinned in `migrations/embed_test.go`; the runner
refuses a database whose ledger checksum differs, so change the schema with a
new file rather than by editing an existing one.

## Ordering

Files apply in filename order, and each file depends only on earlier ones:

| File | Contents |
|---|---|
| `0001_tenants_core_ingest.sql` | UTF-8 guard, `open_splunk` schema, `tenants`, the index catalog, ingest visibility/quota/write-group bookkeeping, ingestion tokens, HEC state, `server_key_state` |
| `0002_collectors_apps.sql` | app workspaces and the native collector fleet |
| `0003_search.sql` | saved searches, schedules, alerts, dashboards, durable search/export jobs, search history, server settings singletons |
| `0004_audit.sql` | audit journals and API mutation receipts |
| `0005_knowledge.sql` | knowledge objects, versions, projections, idempotency, recovery audit |
| `0006_lookups.sql` | lookup assets and lookup definitions |
| `0007_core_ingest_1_triggers.sql` | triggers on `indexes`, `ingestion_tokens`, `ingestion_token_hec_profiles`, `ingestion_token_indexes` |
| `0008_core_ingest_2_triggers.sql` | triggers on `index_catalog_state`, `index_data_deletion_completions`, `index_deletion_mutation_attempts`, `ingest_write_group_members` |
| `0009_core_ingest_3_triggers.sql` | triggers on `index_deletion_operations`, `index_deletion_tombstones`, `ingest_visibility_reservations`, `ingest_write_groups` |
| `0010_collectors_apps_search_1_triggers.sql` | triggers on `app_workspaces`, `collector_runtime`, `dashboards`, `search_history` |
| `0011_collectors_apps_search_2_triggers.sql` | triggers on `app_catalog_revisions`, `app_default_indexes`, `collector_catalog_revisions`, `collector_fleet`, `saved_searches` |
| `0012_knowledge_objects_1_triggers.sql` | triggers on `knowledge_objects`, `knowledge_object_dependencies`, the app and type active counters |
| `0013_knowledge_objects_2_triggers.sql` | triggers on knowledge object versions, lifecycle, ACL, dependency seals, the owner and app-type active counters |
| `0014_knowledge_catalog_1_triggers.sql` | triggers on definition blobs, mutation idempotency, list projections and selector patterns, projection tenant ledgers, recovery audit |
| `0015_knowledge_catalog_2_triggers.sql` | triggers on knowledge catalog tenants, revision heads, mutation commit authorities, list order keys, list projection seals |
| `0016_lookups_triggers.sql` | triggers on lookup assets, asset versions and stages, asset tenant ledgers, lookup definitions and their versions |
| `0017_audit_1_triggers.sql` | triggers on `audit_events`, feature-operation and search-attempt audit events, receipt and knowledge-attempt tenant state |
| `0018_audit_2_triggers.sql` | triggers on `api_mutation_receipts`, `knowledge_attempt_audit_events`, the audit, feature-operation and search-attempt tenant state |
| `0019_pin_routine_search_path.sql` | pins `search_path = open_splunk, pg_temp` on the 136 routines that `0008`, `0009`, `0012`, `0016` and `0017` created without it |
| `0020_search_job_cursor_commit_order.sql` | a statement-level lock on `durable_search_jobs` inserts, so `row_cursor` values are drawn in commit order |
| `0021_sqlite_parity_repairs.sql` | makes the version and idempotency-receipt capacities hold for multi-row INSERTs, and bounds five multi-column btree keys under the index-row limit |

`0004_audit.sql` comes before knowledge because the knowledge mutation
authorities reference `audit_events`. Inside a file, tables are ordered by
their foreign keys. A foreign key that closes a reference cycle, or that
targets a `UNIQUE INDEX` rather than a constraint, is added with
`ALTER TABLE … ADD FOREIGN KEY` at the end of its file, after every table and
index in that file exists.

Objects are created unqualified. The runner sets
`search_path = open_splunk, pg_temp` before applying a file; the files never
name `public`. The database must use
UTF-8 encoding, and `0001` raises an error otherwise.

The files are ASCII and spell bytea and regex literals with backslashes
(`'\x'::bytea`, `'[\u0001-\u001f]'`, `strpos(x, '\')`). The runner therefore
also sets `standard_conforming_strings = on` and `client_encoding = UTF8`, and
`control.OpenPostgres` pins both, with `search_path` and `TimeZone`, as
startup parameters on every pooled connection. A database or role default of
`standard_conforming_strings = off` would otherwise read `'\x'::bytea` as the
byte `x`, and a non-UTF-8 client encoding would reinterpret every byte pgx
sends. A PL/pgSQL trigger
body is parsed again by each session that fires it, so other clients of the
database should keep the default as well.

Every routine pins its own `search_path` (`SET search_path FROM CURRENT` in
its `CREATE FUNCTION`), because a trigger runs with the search_path of
whichever session fires it, and an unpinned body would resolve its
unqualified tables there, possibly to a same-named table in `public`. The pin
names `pg_temp` last: a search_path that leaves it out still searches the
firing session's temporary schema first for relations, so a temporary table
named like a control table would shadow it inside the trigger. A test fails
when a routine in `open_splunk` lacks exactly `open_splunk, pg_temp`. A file must not contain
transaction control (`BEGIN`, `COMMIT`, `ROLLBACK` and their synonyms): the
runner applies all pending files in one transaction and refuses a file that
ends it.

## Translation rules

| SQLite | PostgreSQL |
|---|---|
| `INTEGER` | `bigint` |
| `TEXT`, `TEXT COLLATE BINARY` | `text COLLATE "C"` (byte-order comparison, ASCII-only `lower()`) |
| `BLOB` | `bytea` |
| `REAL` | `double precision` |
| 0/1 `INTEGER` flags | kept as `bigint` with the same `IN (0, 1)` CHECK, so store code keeps scanning integers |
| `length(CAST(x AS BLOB))` | `octet_length(x)` |
| `length(x)` on text / blob | `char_length(x)` / `octet_length(x)` |
| `x NOT GLOB '*[^…]*'` | `x !~ '[^…]'` |
| `substr(x, 1, 1) GLOB '[…]'` | `substr(x, 1, 1) ~ '^[…]$'` |
| `NOT GLOB ('*[' \|\| char(1) \|\| '-' \|\| char(31) \|\| char(127) \|\| '-' \|\| char(159) \|\| ']*')` | `!~ '[\u0001-\u001f\u007f-\u009f]'` |
| `instr(x, 'lit')` | `strpos(x, 'lit')` |
| `instr(…, X'00') = 0`, `instr(x, char(0)) = 0` on text | dropped, because PostgreSQL text cannot hold NUL (`22021`) |
| `typeof(x) = 'blob'` on a `bytea` column | `x IS NOT NULL` (the type half is implied, the NULL half is not) |
| `x NOT LIKE '%\%'` | `strpos(x, '\') = 0`, because SQLite LIKE has no escape character and PostgreSQL's default is backslash |
| `x = x` NaN guard on `REAL` | `x <> 'NaN'::double precision`, because PostgreSQL treats NaN as equal to itself |
| `X''` | `'\x'::bytea` |
| `OLD.x + 1` in a trigger condition, where `x` may hold 9223372036854775807 | `OLD.x::numeric + 1`, because SQLite widens to REAL and still reports the trigger's message, while bigint arithmetic raises 22003 first |
| `STRICT`, `WITHOUT ROWID` | dropped |

Kept as-is: constraint names, unnamed-constraint order, `ON UPDATE` and
`ON DELETE` actions, `DEFERRABLE INITIALLY DEFERRED`, partial and expression
indexes, and `GENERATED ALWAYS AS (…) STORED` columns. The seed rows for
`ingest_visibility_state` and `index_catalog_state` are inserted too.
Identifiers are limited to 63 bytes, so
`ingestion_tokens_max_ingest_uncompressed_bytes_per_second_bounded` is renamed
`ingestion_tokens_max_ingest_uncompressed_bytes_per_sec_bounded`.

## Additions over SQLite

- `tenants`: one row per tenant. It holds the stored ClickHouse tenant key, an
  optional go-common `organization_id`, the state (`active`/`disabled`) and
  `adopted_clickhouse`. Tenants are disabled, never deleted.
- `indexes`, `saved_searches` and `ingestion_tokens` gain
  `tenant_id text COLLATE "C" NOT NULL`. Index names become unique per tenant
  (`indexes_tenant_name_key UNIQUE (tenant_id, name)`), and saved-search names
  become unique per `(tenant_id, owner_id, app_id, name)`.
- Every table with a `tenant_id` column has
  `<table>_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenants
  ON UPDATE RESTRICT ON DELETE RESTRICT`.
- References into those three tables stay inside one tenant. They gain
  `UNIQUE (tenant_id, <id>)` keys (`indexes_tenant_index_id_key`,
  `ingestion_tokens_tenant_token_key`, `saved_searches_tenant_saved_search_key`,
  plus `index_deletion_operations_tenant_operation_key`), and the SQLite
  single-column foreign keys of `app_default_indexes`, `hec_channels`,
  `hec_source_sequences`, `ingest_quota_buckets`, `ingest_rejection_buckets`,
  `index_deletion_operations`, `index_deletion_mutation_attempts`,
  `index_data_deletion_completions`, `saved_search_schedules` and
  `saved_search_schedule_runs` become `(tenant_id, <id>)` keys under their
  SQLite names (for example `hec_channels_ingestion_token_id_fkey`).
  `ingestion_token_indexes` has no `tenant_id`, so the triggers
  `ingestion_token_index_requires_same_tenant_insert` and `_update` refuse a
  token bound to another tenant's index. The tenant of an index, token or saved search is
  immutable (`indexes_tenant_is_immutable`,
  `ingestion_token_tenant_is_immutable`, `saved_search_tenant_is_immutable`),
  and the saved-search app checks in `0010` and `0011` match on
  `(tenant_id, app_id)`.
- `durable_search_jobs.row_cursor bigint GENERATED ALWAYS AS IDENTITY`
  (unique) replaces SQLite's implicit `rowid`, which the artifact listing used
  as a high-water cursor. Unlike `rowid`, it is never reused. Identity values
  are drawn at INSERT time, not at commit, so `0020` makes every transaction
  that inserts jobs take a transaction-scoped advisory lock before any value
  is drawn. Cursors therefore become visible in increasing order, as SQLite's
  single writer guaranteed: a row can never commit below a high-water mark
  that a listing has already read.

## Deviations from SQLite

- `knowledge_list_projection_description_idx` keys
  `(tenant_id, knowledge_object_id, object_version)` with the same
  `description_present = 1` predicate. SQLite keyed it on `description`
  itself, which admits 16384 bytes; a PostgreSQL btree tuple holds at most
  2704 bytes, so that key refused valid rows with SQLSTATE 54000. No query
  orders or seeks by description.
- The same limit applies to five multi-column keys whose per-column
  `char_length` bounds admit up to 1020 bytes per column: the `dashboards`
  and `saved_searches` `UNIQUE (tenant_id, owner_id, app_id, name)`,
  `search_history_owner_app_created_idx`,
  `search_history_owner_saved_created_idx` and
  `knowledge_lookup_definitions_resolution` (whose columns include
  `knowledge_lookup_definitions_private_name`'s). `0021` keeps every
  per-column bound and adds one named CHECK per key
  (`dashboards_owner_app_name_key_bytes_bounded`,
  `saved_searches_owner_app_name_key_bytes_bounded`,
  `search_history_owner_app_key_bytes_bounded`,
  `search_history_owner_saved_key_bytes_bounded`,
  `knowledge_lookup_definitions_resolution_key_bytes_bounded`) that caps the
  sum of the key's text columns at 2560 octets. A row SQLite would store
  with a larger key is refused with 23514 on that constraint instead of
  54000; every row under the cap fits the index even when its values do not
  compress. The Go validators cap each of these columns at 255 bytes, so the
  API cannot reach the cap. Keys over identifiers that have no length bound
  in SQLite either (`index_id`, `ingestion_token_id`) are unchanged.
- AFTER ROW triggers run at the end of the statement in PostgreSQL, not
  after each row as in SQLite, so a BEFORE ROW capacity check that reads a
  counter maintained by an AFTER ROW trigger sees the count from before a
  multi-row INSERT. Where the soft limit sits below the counter's column
  CHECK (`knowledge_object_versions`: 61440 of 65536,
  `knowledge_mutation_idempotency`: 16384 of 20480) `0021` makes the
  AFTER ROW increment refuse to pass the limit for the row's
  `mutation_kind`, with the BEFORE trigger's message, so a multi-row INSERT
  is refused at the same row as in SQLite. The other counter-backed limits
  equal their column CHECK, so a multi-row INSERT past them fails with
  23514.
