-- open-splunk PostgreSQL control plane: three repairs where the port did not
-- keep a property of the final SQLite schema.
--
-- 1. durable_search_jobs.row_cursor is assigned in commit order.
--
--    The artifact listing (internal/searchartifacts/list.go) reads
--    MAX(row_cursor) once as a high-water mark and then serves every page
--    with row_cursor <= that mark, so a later page shows exactly the rows
--    the first page could see. Under SQLite that holds because rowids are
--    handed out by the single writer, so a row committed after the mark was
--    read always gets a rowid above it. An identity column takes its value
--    at INSERT time instead. A transaction that inserted job A (cursor 1) and
--    is still open lets an autocommit insert of job B (cursor 2) commit
--    first; a reader then takes the mark 2 without job A, and job A shows up
--    on later pages once it commits.
--
--    durable_search_jobs_insert_takes_cursor_lock is a BEFORE INSERT FOR
--    EACH STATEMENT trigger that takes a transaction-scoped advisory lock.
--    A statement trigger fires before the statement produces its first row,
--    so before the identity default calls nextval(), and the lock is
--    released only after the transaction's commit or rollback is visible.
--    Inserts into durable_search_jobs therefore take cursors one transaction
--    at a time, in commit order, exactly as SQLite's single writer did: a
--    reader that sees a row also sees every committed row with a lower
--    cursor, and every row that commits later gets a higher one. Rolled-back
--    inserts leave gaps, which the listing does not care about. Cursors are
--    still never reused, and explicit values are still refused (428C9).
--
--    Cost: concurrent job admissions queue on the lock until the holder
--    commits, across tenants, as they did on SQLite. If the holder then
--    waits on a row lock held by a transaction that is itself queued on the
--    cursor lock, PostgreSQL reports a deadlock (40P01) to one of them, and
--    WriteTx retries it. The lock key is 0x6f73000000000002, next to the
--    migration runner's 0x6f73000000000001 (postgresMigrationAdvisoryLockKey).
--    A session with session_replication_role = replica (restore, logical
--    replication) skips the trigger like every other user trigger, so its
--    inserts draw cursors without the lock.
--
-- 2. The version and idempotency-receipt capacities hold for multi-row
--    INSERTs.
--
--    knowledge_object_version_capacity_is_available and
--    knowledge_mutation_idempotency_capacity_is_available are BEFORE ROW
--    checks of knowledge_catalog_tenants.version_count / idempotency_count,
--    which the AFTER ROW triggers knowledge_object_version_after_insert and
--    knowledge_mutation_idempotency_after_insert increment. SQLite runs a
--    row's AFTER trigger before it checks the next row; PostgreSQL queues
--    AFTER ROW triggers to the end of the statement, so every row of one
--    INSERT was checked against the count from before the statement. The
--    ordinary limits (61440 versions, 16384 receipts) sit below the column
--    CHECKs (65536, 20480), so a multi-row INSERT could pass the ordinary
--    limit without any error.
--
--    The two AFTER ROW functions now increment only while the count is below
--    the limit for the row's mutation_kind, and raise the BEFORE trigger's
--    message when it is not. The queued AFTER triggers run row by row, so
--    row k sees the count after rows 1..k-1, which is what SQLite's BEFORE
--    check of row k saw: the statement is refused at the same row, with the
--    same message. A single-row INSERT is unchanged, because the BEFORE
--    check already refused a count at the limit. The Go writers insert one
--    version and one receipt per statement; this closes the gap for direct
--    SQL and any future batch writer.
--
--    The other counter-backed capacity checks
--    (knowledge_object_identity_capacity_is_available,
--    knowledge_definition_blob_capacity_is_available,
--    knowledge_object_active_capacity_insert,
--    knowledge_recovery_audit_capacity_is_available) use limits equal to
--    their counter's column CHECK, so a multi-row INSERT past them already
--    fails, with 23514 on the counter's table instead of the RAISE.
--    knowledge_recovery_audit_capacity_is_available also requires
--    NEW.sequence = recovery_audit_count + 1, so a multi-row INSERT of
--    consecutive sequences is refused where SQLite stored it, and
--    knowledge_list_projection_capacity_is_available fails closed on
--    multi-row writes (see 0014). Both refuse rather than admit, and the Go
--    writers insert those rows one per statement.
--
-- 3. Multi-column btree keys stay under PostgreSQL's index-row limit.
--
--    A btree index row holds at most 2704 bytes. The ported char_length
--    bounds admit up to 1020 bytes per 255-character column, so five keys
--    could exceed it with rows SQLite stores, and PostgreSQL refused those
--    rows with SQLSTATE 54000 ("index row size ... exceeds btree version 4
--    maximum 2704"):
--      dashboards UNIQUE (tenant_id, owner_id, app_id, name)
--      saved_searches UNIQUE (tenant_id, owner_id, app_id, name)
--      search_history_owner_app_created_idx
--      search_history_owner_saved_created_idx
--      knowledge_lookup_definitions_resolution, whose columns include those
--        of knowledge_lookup_definitions_private_name
--    Every other btree key whose columns have a length bound (a CHECK, an
--    enum, or a foreign key to a bounded column) stays below the limit.
--    Keys over identifiers that have no length bound in SQLite either
--    (index_id, ingestion_token_id) are left as they are. The sums are
--    bigint so an absurd value cannot overflow int4 before the per-column
--    CHECKs refuse it. The single-column limits are kept
--    (char_length still counts code points, as SQLite's length() did); a
--    named CHECK per key bounds the sum of its text columns' octet_length
--    at 2560 bytes. That leaves room for the index tuple header, one varlena
--    header and alignment padding per column, and the bigint key columns,
--    so every row the CHECK admits fits the index whether or not its values
--    compress. A row over it is refused with 23514 and the constraint name
--    instead of 54000. The Go validators cap each of these columns at 255
--    bytes (dashboards/validation.go, savedobjects/validation.go,
--    searchhistory/types.go, lookupcatalog and lookupdefinition), so the
--    API never comes near the bound.

-- 1. durable_search_jobs ----------------------------------------------------

CREATE FUNCTION durable_search_jobs_insert_takes_cursor_lock()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- 0x6f73000000000002; see the header.
    PERFORM pg_catalog.pg_advisory_xact_lock(8030762560531726338);
    RETURN NULL;
END
$$;

CREATE TRIGGER durable_search_jobs_insert_takes_cursor_lock
BEFORE INSERT ON durable_search_jobs
FOR EACH STATEMENT
EXECUTE FUNCTION durable_search_jobs_insert_takes_cursor_lock();

-- 2. Counter-backed capacities ----------------------------------------------

CREATE OR REPLACE FUNCTION knowledge_object_version_after_insert() RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    UPDATE knowledge_catalog_tenants
    SET version_count = version_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND version_count < CASE
          WHEN NEW.mutation_kind = 'quarantine' THEN 65536
          ELSE 61440
      END;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'knowledge object version capacity exhausted';
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION knowledge_mutation_idempotency_after_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    UPDATE knowledge_catalog_tenants
    SET idempotency_count = idempotency_count + 1
    WHERE tenant_id = NEW.tenant_id
      AND idempotency_count < CASE
          WHEN NEW.mutation_kind = 'quarantine' THEN 20480
          ELSE 16384
      END;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'knowledge mutation idempotency capacity exhausted';
    END IF;
    RETURN NULL;
END
$$;

-- 3. Btree key bounds -------------------------------------------------------

ALTER TABLE dashboards
    ADD CONSTRAINT dashboards_owner_app_name_key_bytes_bounded CHECK (
        octet_length(tenant_id)::bigint + octet_length(owner_id)
            + octet_length(app_id) + octet_length(name) <= 2560
    );

ALTER TABLE saved_searches
    ADD CONSTRAINT saved_searches_owner_app_name_key_bytes_bounded CHECK (
        octet_length(tenant_id)::bigint + octet_length(owner_id)
            + octet_length(app_id) + octet_length(name) <= 2560
    );

ALTER TABLE search_history
    ADD CONSTRAINT search_history_owner_app_key_bytes_bounded CHECK (
        octet_length(tenant_id)::bigint + octet_length(owner_id)
            + octet_length(app_id) + octet_length(search_job_id) <= 2560
    ),
    ADD CONSTRAINT search_history_owner_saved_key_bytes_bounded CHECK (
        octet_length(tenant_id)::bigint + octet_length(owner_id)
            + octet_length(saved_search_id) + octet_length(search_job_id) <= 2560
    );

ALTER TABLE knowledge_lookup_definitions
    ADD CONSTRAINT knowledge_lookup_definitions_resolution_key_bytes_bounded CHECK (
        octet_length(tenant_id)::bigint + octet_length(state) + octet_length(name)
            + octet_length(app_id) + octet_length(owner_id)
            + octet_length(lookup_id) <= 2560
    );
