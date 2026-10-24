-- open-splunk PostgreSQL control plane: two repairs where the port did not
-- keep a property of the final SQLite schema. A third, drawing
-- durable_search_jobs.row_cursor in commit order, is 0020.
--
-- 1. The version and idempotency-receipt capacities hold for multi-row
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
-- 2. Multi-column btree keys stay under PostgreSQL's index-row limit.
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

-- 1. Counter-backed capacities ----------------------------------------------

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

-- 2. Btree key bounds -------------------------------------------------------

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
