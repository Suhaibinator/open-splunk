-- open-splunk PostgreSQL control plane: triggers on indexes, ingestion_tokens,
-- ingestion_token_hec_profiles and ingestion_token_indexes (32 triggers). 29
-- are ported from the final SQLite schema (migrations/sqlite/0001..0012); see
-- migrations/postgres/README.md for the translation rules. The 3 tenant
-- guards at the end of the file have no SQLite counterpart. Applies after
-- 0001..0006.
--
-- Porting notes:
--
-- * Every trigger keeps its SQLite name, timing, events, UPDATE OF column
--   list and FOR EACH ROW granularity, and every RAISE keeps the SQLite
--   message byte for byte (SQLSTATE P0001). No Go code matches on these
--   messages; the Go stores run their own pre-checks with separate errors.
-- * A WHEN clause without a subquery stays on CREATE TRIGGER. PostgreSQL
--   forbids subqueries in trigger WHEN clauses, so EXISTS conditions move
--   into the function body.
-- * Each trigger has its own function, named after the trigger. The three
--   insert/update pairs whose SQLite bodies are identical share one function:
--   index_catalog_record_is_bounded, indexes_retention_is_millisecond_aligned
--   and ingestion_token_hec_collector_binding_is_forbidden.
-- * Functions pin search_path FROM CURRENT (the runner's open_splunk), so a
--   trigger finds its tables whatever search_path the firing session uses.
-- * instr(CAST(x AS BLOB), X'00') = 0 is dropped: PostgreSQL text cannot hold
--   NUL. length(CAST(x AS BLOB)) is octet_length(x). Each control-character
--   GLOB '*[...]*' matches the class anywhere in the value, so it becomes
--   the unanchored regex x !~ '[...]' from README.md.
-- * changes() <> 1 after the counter UPDATE becomes GET DIAGNOSTICS
--   ROW_COUNT.
--
-- Deliberate differences from SQLite:
--
-- * index_catalog_identity_collision_is_forbidden checks the name per tenant.
--   SQLite indexes had a global name UNIQUE; PostgreSQL replaced it with
--   indexes_tenant_name_key UNIQUE (tenant_id, name). A global name check
--   would forbid two tenants from both having (for example) "main". index_id
--   stays globally unique (it is the primary key).
-- * Tenant guards (no SQLite counterpart, where nothing had a tenant):
--   indexes_tenant_is_immutable and ingestion_token_tenant_is_immutable keep
--   an index or token in the tenant that created it, and
--   ingestion_token_index_requires_same_tenant refuses binding a token to
--   another tenant's index. ingestion_token_indexes has no tenant_id column
--   of its own, so a trigger stands in for the tenant-scoped foreign keys the
--   other token and index children use. With both tenants immutable, the
--   check reads rows no concurrent transaction can change.
-- * index_catalog_state still counts physical index rows across all tenants
--   (capacity 1024 for the whole server), exactly as in SQLite. A per-tenant
--   capacity needs a schema change and is out of scope here.
-- * Firing order. PostgreSQL fires same-event triggers in name order, SQLite
--   in reverse creation order. When one row breaks several triggers the
--   message can differ (for example renaming a tombstoned index reports
--   'index name is immutable' in PostgreSQL and 'tombstoned index is
--   immutable' in SQLite). The row is rejected either way, and no Go code
--   reads these messages, so no trigger is renamed.
--
-- Concurrency (load-bearing assumptions):
--
-- * index_catalog_after_index_insert/update increment the singleton
--   index_catalog_state row. The UPDATE row lock serialises writers; under
--   REPEATABLE READ or SERIALIZABLE a concurrent writer gets 40001 and must
--   retry, under READ COMMITTED the UPDATE re-reads the latest revision.
--   The capacity pre-check in index_catalog_before_index_insert reads without
--   a lock; if two READ COMMITTED inserts both see 1023 rows, the second
--   increment fails the physical_count CHECK (23514) instead of raising the
--   trigger message. The row is still refused.
-- * index_catalog_identity_collision_is_forbidden is a read-then-insert
--   check. Two concurrent inserts can both pass it; the primary key and
--   indexes_tenant_name_key then reject the second with 23505 (or SSI
--   aborts it with 40001 first).
-- * active_app_default_indexes_remain_searchable,
--   tombstoned_index_update_is_forbidden,
--   deleting_index_with_operation_* and
--   ingestion_token_hec_profile_requires_hec_purpose read other tables that a
--   concurrent transaction could change. They are exact only under
--   SERIALIZABLE, where PostgreSQL's SSI aborts the write-skew partner with
--   40001. The control-plane write path must run SERIALIZABLE with retry.
--
-- The accept/reject cases for every trigger, checked against SQLite, are in
-- internal/control/testdata/postgres_triggers/core_ingest_1.json.

-- ---------------------------------------------------------------------------
-- indexes
-- ---------------------------------------------------------------------------

CREATE FUNCTION active_app_default_indexes_remain_searchable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM app_default_indexes AS app_index
        JOIN app_workspaces AS app
          ON app.tenant_id = app_index.tenant_id
         AND app.app_id = app_index.app_id
        WHERE app_index.index_id = OLD.index_id
          AND app.state = 'active'
    ) THEN
        RAISE EXCEPTION 'active app requires searchable index';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER active_app_default_indexes_remain_searchable
BEFORE UPDATE OF state, search_enabled ON indexes
FOR EACH ROW
WHEN (NEW.state <> 'active' OR NEW.search_enabled <> 1)
EXECUTE FUNCTION active_app_default_indexes_remain_searchable();

CREATE FUNCTION deleting_index_insert_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'deleting index creation requires a deletion operation';
END;
$$;

CREATE TRIGGER deleting_index_insert_is_forbidden
BEFORE INSERT ON indexes
FOR EACH ROW
WHEN (NEW.state = 'deleting')
EXECUTE FUNCTION deleting_index_insert_is_forbidden();

CREATE FUNCTION deleting_index_with_operation_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM index_deletion_operations
        WHERE index_deletion_operations.index_id = OLD.index_id
    ) THEN
        RAISE EXCEPTION 'deleting index cannot be deleted';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER deleting_index_with_operation_delete_is_forbidden
BEFORE DELETE ON indexes
FOR EACH ROW
EXECUTE FUNCTION deleting_index_with_operation_delete_is_forbidden();

CREATE FUNCTION deleting_index_with_operation_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM index_deletion_operations
        WHERE index_deletion_operations.index_id = OLD.index_id
    ) THEN
        RAISE EXCEPTION 'deleting index is coordinator-owned';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER deleting_index_with_operation_update_is_forbidden
BEFORE UPDATE ON indexes
FOR EACH ROW
WHEN (OLD.state = 'deleting')
EXECUTE FUNCTION deleting_index_with_operation_update_is_forbidden();

CREATE FUNCTION index_catalog_after_index_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    changed bigint;
BEGIN
    UPDATE index_catalog_state
    SET revision = revision + 1,
        physical_count = physical_count + 1
    WHERE singleton_id = 1;

    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'index catalog insert accounting failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER index_catalog_after_index_insert
AFTER INSERT ON indexes
FOR EACH ROW
EXECUTE FUNCTION index_catalog_after_index_insert();

CREATE FUNCTION index_catalog_after_index_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    changed bigint;
BEGIN
    UPDATE index_catalog_state
    SET revision = revision + 1
    WHERE singleton_id = 1;

    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'index catalog update accounting failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER index_catalog_after_index_update
AFTER UPDATE ON indexes
FOR EACH ROW
EXECUTE FUNCTION index_catalog_after_index_update();

CREATE FUNCTION index_catalog_before_index_insert()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM index_catalog_state
        WHERE singleton_id = 1
          AND revision BETWEEN 1 AND 9223372036854775806
          AND physical_count BETWEEN 0 AND 1023
    ) THEN
        RAISE EXCEPTION
            'index catalog state is invalid or capacity is exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_catalog_before_index_insert
BEFORE INSERT ON indexes
FOR EACH ROW
EXECUTE FUNCTION index_catalog_before_index_insert();

CREATE FUNCTION index_catalog_before_index_update()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM index_catalog_state
        WHERE singleton_id = 1
          AND revision BETWEEN 1 AND 9223372036854775806
          AND physical_count BETWEEN 1 AND 1024
    ) THEN
        RAISE EXCEPTION 'index catalog state is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_catalog_before_index_update
BEFORE UPDATE ON indexes
FOR EACH ROW
EXECUTE FUNCTION index_catalog_before_index_update();

-- SQLite: index_id = NEW.index_id OR name = NEW.name. Names are unique per
-- tenant in PostgreSQL (indexes_tenant_name_key), so the name half is scoped
-- to NEW.tenant_id; see the header.
CREATE FUNCTION index_catalog_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM indexes
        WHERE index_id = NEW.index_id
           OR (tenant_id = NEW.tenant_id AND name = NEW.name)
    ) THEN
        RAISE EXCEPTION 'index catalog identity is already in use';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_catalog_identity_collision_is_forbidden
BEFORE INSERT ON indexes
FOR EACH ROW
EXECUTE FUNCTION index_catalog_identity_collision_is_forbidden();

CREATE FUNCTION index_catalog_index_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'index catalog identity cannot be deleted';
END;
$$;

CREATE TRIGGER index_catalog_index_delete_is_forbidden
BEFORE DELETE ON indexes
FOR EACH ROW
EXECUTE FUNCTION index_catalog_index_delete_is_forbidden();

-- Shared by index_catalog_record_insert_is_bounded and
-- index_catalog_record_update_is_bounded, whose SQLite bodies are identical.
-- The control-character classes: index_id, display_name and
-- default_sourcetype refuse U+0001-U+001F and U+007F-U+009F; description also
-- allows tab, line feed and carriage return (U+0009, U+000A, U+000D).
CREATE FUNCTION index_catalog_record_is_bounded()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT (
        octet_length(NEW.index_id) BETWEEN 1 AND 128
        AND NEW.index_id = trim(NEW.index_id)
        AND NEW.index_id !~ '[\u0001-\u001f\u007f-\u009f]'
        AND octet_length(NEW.display_name) BETWEEN 1 AND 255
        AND NEW.display_name = trim(NEW.display_name)
        AND NEW.display_name !~ '[\u0001-\u001f\u007f-\u009f]'
        AND octet_length(NEW.description) BETWEEN 0 AND 8192
        AND NEW.description
            !~ '[\u0001-\u0008\u000b-\u000c\u000e-\u001f\u007f-\u009f]'
        AND octet_length(NEW.default_sourcetype) BETWEEN 0 AND 255
        AND NEW.default_sourcetype = trim(NEW.default_sourcetype)
        AND NEW.default_sourcetype !~ '[\u0001-\u001f\u007f-\u009f]'
        AND NEW.created_at_unix_micro BETWEEN 1 AND 253402300799999999
        AND NEW.updated_at_unix_micro BETWEEN
            NEW.created_at_unix_micro AND 253402300799999999
    ) THEN
        RAISE EXCEPTION 'index catalog record is invalid or unbounded';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_catalog_record_insert_is_bounded
BEFORE INSERT ON indexes
FOR EACH ROW
EXECUTE FUNCTION index_catalog_record_is_bounded();

CREATE TRIGGER index_catalog_record_update_is_bounded
BEFORE UPDATE OF
    index_id,
    display_name,
    description,
    default_sourcetype,
    created_at_unix_micro,
    updated_at_unix_micro
ON indexes
FOR EACH ROW
EXECUTE FUNCTION index_catalog_record_is_bounded();

CREATE FUNCTION index_deleting_transition_requires_operation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM index_deletion_operations
        WHERE index_deletion_operations.index_id = OLD.index_id
          AND index_deletion_operations.index_name = OLD.name
          AND index_deletion_operations.archived_index_version = OLD.version
          -- numeric: OLD.version may be 9223372036854775807, and bigint + 1
          -- would raise 22003 instead of the message below.
          AND NEW.version = OLD.version::numeric + 1
          AND NEW.updated_at_unix_micro =
              index_deletion_operations.created_at_unix_micro
    ) THEN
        RAISE EXCEPTION
            'deleting index transition requires a matching deletion operation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER index_deleting_transition_requires_operation
BEFORE UPDATE OF state ON indexes
FOR EACH ROW
WHEN (OLD.state <> 'deleting' AND NEW.state = 'deleting')
EXECUTE FUNCTION index_deleting_transition_requires_operation();

CREATE FUNCTION indexes_id_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'index ID is immutable';
END;
$$;

CREATE TRIGGER indexes_id_is_immutable
BEFORE UPDATE OF index_id ON indexes
FOR EACH ROW
WHEN (NEW.index_id <> OLD.index_id)
EXECUTE FUNCTION indexes_id_is_immutable();

CREATE FUNCTION indexes_name_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'index name is immutable';
END;
$$;

CREATE TRIGGER indexes_name_is_immutable
BEFORE UPDATE OF name ON indexes
FOR EACH ROW
WHEN (NEW.name <> OLD.name)
EXECUTE FUNCTION indexes_name_is_immutable();

-- Shared by the _on_insert and _on_update triggers, whose SQLite bodies are
-- identical. % truncates toward zero in both engines, so a negative value
-- that is not a whole millisecond is refused here before the >= 0 CHECK.
CREATE FUNCTION indexes_retention_is_millisecond_aligned()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'index retention must use whole milliseconds';
END;
$$;

CREATE TRIGGER indexes_retention_is_millisecond_aligned_on_insert
BEFORE INSERT ON indexes
FOR EACH ROW
WHEN (NEW.retention_nanoseconds % 1000000 <> 0)
EXECUTE FUNCTION indexes_retention_is_millisecond_aligned();

CREATE TRIGGER indexes_retention_is_millisecond_aligned_on_update
BEFORE UPDATE OF retention_nanoseconds ON indexes
FOR EACH ROW
WHEN (NEW.retention_nanoseconds % 1000000 <> 0)
EXECUTE FUNCTION indexes_retention_is_millisecond_aligned();

CREATE FUNCTION tombstoned_index_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM index_deletion_tombstones
        WHERE index_deletion_tombstones.index_id = OLD.index_id
    ) THEN
        RAISE EXCEPTION 'tombstoned index cannot be deleted';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER tombstoned_index_delete_is_forbidden
BEFORE DELETE ON indexes
FOR EACH ROW
EXECUTE FUNCTION tombstoned_index_delete_is_forbidden();

CREATE FUNCTION tombstoned_index_update_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM index_deletion_tombstones
        WHERE index_deletion_tombstones.index_id = OLD.index_id
    ) THEN
        RAISE EXCEPTION 'tombstoned index is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tombstoned_index_update_is_forbidden
BEFORE UPDATE ON indexes
FOR EACH ROW
EXECUTE FUNCTION tombstoned_index_update_is_forbidden();

-- ---------------------------------------------------------------------------
-- ingestion_tokens
-- ---------------------------------------------------------------------------

CREATE FUNCTION ingestion_token_collector_binding_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'ingestion token collector binding is immutable';
END;
$$;

CREATE TRIGGER ingestion_token_collector_binding_is_immutable
BEFORE UPDATE OF bound_collector_id ON ingestion_tokens
FOR EACH ROW
WHEN (
    OLD.bound_collector_id IS NOT NULL
    AND (
        NEW.bound_collector_id IS NULL
        OR NEW.bound_collector_id <> OLD.bound_collector_id
    )
)
EXECUTE FUNCTION ingestion_token_collector_binding_is_immutable();

CREATE FUNCTION ingestion_token_collector_binding_is_required()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'ingestion token collector binding is required';
END;
$$;

CREATE TRIGGER ingestion_token_collector_binding_is_required
BEFORE INSERT ON ingestion_tokens
FOR EACH ROW
WHEN (
    NEW.purpose = 'native_collector'
    AND NEW.bound_collector_id IS NULL
)
EXECUTE FUNCTION ingestion_token_collector_binding_is_required();

CREATE FUNCTION ingestion_token_digest_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'ingestion token digest is immutable';
END;
$$;

CREATE TRIGGER ingestion_token_digest_is_immutable
BEFORE UPDATE OF token_digest ON ingestion_tokens
FOR EACH ROW
WHEN (NEW.token_digest <> OLD.token_digest)
EXECUTE FUNCTION ingestion_token_digest_is_immutable();

-- Shared by ingestion_token_hec_collector_binding_is_forbidden (INSERT) and
-- ingestion_token_hec_collector_binding_update_is_forbidden (UPDATE OF
-- bound_collector_id), whose SQLite bodies are identical.
CREATE FUNCTION ingestion_token_hec_collector_binding_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'HEC ingestion token cannot have a collector binding';
END;
$$;

CREATE TRIGGER ingestion_token_hec_collector_binding_is_forbidden
BEFORE INSERT ON ingestion_tokens
FOR EACH ROW
WHEN (
    NEW.purpose = 'hec'
    AND NEW.bound_collector_id IS NOT NULL
)
EXECUTE FUNCTION ingestion_token_hec_collector_binding_is_forbidden();

CREATE TRIGGER ingestion_token_hec_collector_binding_update_is_forbidden
BEFORE UPDATE OF bound_collector_id ON ingestion_tokens
FOR EACH ROW
WHEN (
    NEW.purpose = 'hec'
    AND NEW.bound_collector_id IS NOT NULL
)
EXECUTE FUNCTION ingestion_token_hec_collector_binding_is_forbidden();

CREATE FUNCTION ingestion_token_purpose_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'ingestion token purpose is immutable';
END;
$$;

CREATE TRIGGER ingestion_token_purpose_is_immutable
BEFORE UPDATE OF purpose ON ingestion_tokens
FOR EACH ROW
WHEN (NEW.purpose <> OLD.purpose)
EXECUTE FUNCTION ingestion_token_purpose_is_immutable();

CREATE FUNCTION revoked_ingestion_token_is_irreversible()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'revoked ingestion token cannot be reactivated';
END;
$$;

CREATE TRIGGER revoked_ingestion_token_is_irreversible
BEFORE UPDATE OF state ON ingestion_tokens
FOR EACH ROW
WHEN (OLD.state = 'revoked' AND NEW.state <> 'revoked')
EXECUTE FUNCTION revoked_ingestion_token_is_irreversible();

-- ---------------------------------------------------------------------------
-- ingestion_token_hec_profiles
-- ---------------------------------------------------------------------------

CREATE FUNCTION ingestion_token_hec_acknowledgment_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'HEC token acknowledgment mode is immutable';
END;
$$;

CREATE TRIGGER ingestion_token_hec_acknowledgment_is_immutable
BEFORE UPDATE OF indexer_acknowledgment ON ingestion_token_hec_profiles
FOR EACH ROW
WHEN (NEW.indexer_acknowledgment <> OLD.indexer_acknowledgment)
EXECUTE FUNCTION ingestion_token_hec_acknowledgment_is_immutable();

CREATE FUNCTION ingestion_token_hec_profile_owner_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'HEC token profile owner is immutable';
END;
$$;

CREATE TRIGGER ingestion_token_hec_profile_owner_is_immutable
BEFORE UPDATE OF ingestion_token_id ON ingestion_token_hec_profiles
FOR EACH ROW
WHEN (NEW.ingestion_token_id <> OLD.ingestion_token_id)
EXECUTE FUNCTION ingestion_token_hec_profile_owner_is_immutable();

CREATE FUNCTION ingestion_token_hec_profile_requires_hec_purpose()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM ingestion_tokens AS token
        WHERE token.ingestion_token_id = NEW.ingestion_token_id
          AND token.purpose = 'hec'
          AND token.bound_collector_id IS NULL
    ) THEN
        RAISE EXCEPTION 'HEC token profile requires an unbound HEC token';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ingestion_token_hec_profile_requires_hec_purpose
BEFORE INSERT ON ingestion_token_hec_profiles
FOR EACH ROW
EXECUTE FUNCTION ingestion_token_hec_profile_requires_hec_purpose();

-- ---------------------------------------------------------------------------
-- tenant guards (no SQLite counterpart)
-- ---------------------------------------------------------------------------

CREATE FUNCTION indexes_tenant_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'index tenant is immutable';
END;
$$;

CREATE TRIGGER indexes_tenant_is_immutable
BEFORE UPDATE OF tenant_id ON indexes
FOR EACH ROW
WHEN (NEW.tenant_id <> OLD.tenant_id)
EXECUTE FUNCTION indexes_tenant_is_immutable();

CREATE FUNCTION ingestion_token_tenant_is_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'ingestion token tenant is immutable';
END;
$$;

CREATE TRIGGER ingestion_token_tenant_is_immutable
BEFORE UPDATE OF tenant_id ON ingestion_tokens
FOR EACH ROW
WHEN (NEW.tenant_id <> OLD.tenant_id)
EXECUTE FUNCTION ingestion_token_tenant_is_immutable();

-- A missing token or index is left to the foreign keys (23503), so only a
-- binding whose two ends both exist in different tenants is refused here.
CREATE FUNCTION ingestion_token_index_requires_same_tenant()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM ingestion_tokens AS token
        JOIN indexes AS target_index
          ON target_index.index_id = NEW.index_id
        WHERE token.ingestion_token_id = NEW.ingestion_token_id
          AND target_index.tenant_id <> token.tenant_id
    ) THEN
        RAISE EXCEPTION 'ingestion token index belongs to another tenant';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ingestion_token_index_requires_same_tenant
BEFORE INSERT OR UPDATE OF ingestion_token_id, index_id ON ingestion_token_indexes
FOR EACH ROW
EXECUTE FUNCTION ingestion_token_index_requires_same_tenant();
