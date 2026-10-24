# PostgreSQL control-plane schema

These files port the final SQLite control-plane schema (`migrations/sqlite/0001`
through `0012`, with every migration applied) to PostgreSQL. This directory
holds the tables, keys, CHECK constraints, indexes and seed rows. Triggers are
ported separately and are applied after these files.

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
<<<<<<< HEAD
| `0010_collectors_apps_search_1_triggers.sql` | triggers on `app_workspaces`, `collector_runtime`, `dashboards` and `search_history` |
=======
| `0015_knowledge_catalog_2_triggers.sql` | triggers on the knowledge catalog tenants, revision heads, mutation commit authorities, list order keys and list projection seals; every function pins `search_path` |
>>>>>>> feat/pg-triggers-knowledge-catalog-2

`0004_audit.sql` comes before knowledge because the knowledge mutation
authorities reference `audit_events`. Inside a file, tables are ordered by
their foreign keys. A foreign key that closes a reference cycle, or that
targets a `UNIQUE INDEX` rather than a constraint, is added with
`ALTER TABLE … ADD FOREIGN KEY` at the end of its file, after every table and
index in that file exists.

Objects are created unqualified. The runner sets `search_path = open_splunk`
before applying a file; the files never name `public`. The database must use
UTF-8 encoding, and `0001` raises an error otherwise.

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
- `durable_search_jobs.row_cursor bigint GENERATED ALWAYS AS IDENTITY`
  (unique) replaces SQLite's implicit `rowid`, which the artifact listing used
  as a high-water cursor. Unlike `rowid`, it is never reused.
