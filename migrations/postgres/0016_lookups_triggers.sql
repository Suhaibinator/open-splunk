-- open-splunk PostgreSQL control plane: lookup asset and lookup definition
-- triggers (30), ported from the final SQLite schema. Applied after
-- 0001..0006. See migrations/postgres/README.md.
--
-- Porting notes:
--
-- * Every RAISE message is byte-identical to SQLite and raised with the
--   default SQLSTATE P0001. lookupcatalog.storageConflict/storageCapacity and
--   lookupasset.mapStorageError classify errors by these substrings.
--
-- * Trigger names are the SQLite names, except one that exceeds PostgreSQL's
--   63-byte identifier limit:
--     knowledge_lookup_definition_ordinary_version_capacity_is_available
--       -> knowledge_lookup_definition_ordinary_version_capacity_available
--   The rename keeps its alphabetical position among its siblings.
--
-- * Firing order. SQLite fires same-event triggers newest-first (descending
--   sqlite_schema rowid); PostgreSQL fires them in name order. Where two
--   triggers can reject the same row and the orders disagree, the trigger
--   PostgreSQL fires first yields (returns NEW without raising) when the
--   trigger SQLite fires first would reject, so the message is the SQLite
--   one. The shared conditions live in the *_violated predicate functions
--   below. The disagreeing groups are:
--     knowledge_lookup_asset_tenant_ledgers BEFORE INSERT:
--       SQLite: initial_shape_is_exact, identity_collision_is_forbidden
--     knowledge_lookup_asset_versions BEFORE INSERT:
--       SQLite: sequence_is_exact, requires_tenant_ledger, capacity_is_available
--     knowledge_lookup_definition_versions BEFORE INSERT:
--       SQLite: version_matches_current_registry,
--               ordinary_version_capacity, version_capacity_is_available
--     knowledge_lookup_definitions BEFORE UPDATE:
--       SQLite: update_requires_active_app, transition_is_valid
--   The other groups either agree (definitions BEFORE INSERT) or hold
--   mutually exclusive conditions (the stage and identity "requires ledger"
--   versus "capacity" pairs both test ledger existence).
--
-- * WHEN clauses without subqueries stay on the trigger; PostgreSQL forbids
--   subqueries in a trigger WHEN, so those conditions move into the function
--   body with the same three-valued logic (a NULL condition does not fire).
--   SQLite `IS` / `IS NOT` become IS [NOT] DISTINCT FROM.
--
-- * Ledger accounting (the four *_accounts_after_* triggers). SQLite runs an
--   AFTER ROW trigger immediately after its row; PostgreSQL queues AFTER ROW
--   triggers until the statement ends, so a multi-row statement (for
--   example the expired-stage DELETE in lookupasset.Store.Stage and
--   PruneExpiredStages) runs every row's trigger after all of its rows are
--   applied. A literal "count = count + 1" would then hand
--   knowledge_lookup_asset_ledger_transition_is_exact a ledger that is
--   N - 1 rows behind its recount and reject the statement. Each accounting
--   trigger therefore adds the statement's not-yet-accounted delta, measured
--   as (recount - ledger) in one snapshot: +1 / +canonical_bytes for a
--   single-row statement, exactly as SQLite, and the whole statement's delta
--   on the first row of a multi-row statement, with 0 (no UPDATE) on the
--   rows after it. Load-bearing assumptions:
--     - the ledger equals its recount at every statement boundary, which
--       knowledge_lookup_asset_ledger_transition_is_exact enforces on every
--       ledger UPDATE and initial_shape_is_exact on INSERT;
--     - writers run SERIALIZABLE (control.DB.WriteTx) with retry on 40001:
--       two transactions that change the same tenant both UPDATE its single
--       ledger row, so the second fails to serialize and retries. Under READ
--       COMMITTED the UPDATE re-reads the newest ledger row and adds this
--       transaction's own delta, which stays exact as well.
--   The BEFORE capacity triggers read the ledger, as in SQLite. Inside one
--   multi-row INSERT the ledger is not yet updated for the earlier rows, so
--   an overflow there is rejected by the ledger's CHECK bounds (23514)
--   instead of the capacity message. lookupasset only inserts single rows.
--
-- * Count-based capacity triggers (definition tenant capacity, definition
--   version capacities) are predicate reads; SERIALIZABLE turns a concurrent
--   insert into the same tenant into a serialization failure.
--
-- * Functions resolve tables through search_path (open_splunk), like every
--   other object in these files.

-- Shared predicates for the yield-to-SQLite-order guards. Each returns true
-- exactly when the named trigger rejects the row.

CREATE FUNCTION knowledge_lookup_asset_ledger_initial_shape_violated(
    candidate knowledge_lookup_asset_tenant_ledgers
) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        candidate.staged_asset_count <> 0
        OR candidate.asset_identity_count <> 0
        OR candidate.published_version_count <> 0
        OR candidate.stored_content_bytes <> 0
        OR EXISTS (
            SELECT 1 FROM knowledge_lookup_asset_stages
            WHERE tenant_id = candidate.tenant_id
        )
        OR EXISTS (
            SELECT 1 FROM knowledge_lookup_assets
            WHERE tenant_id = candidate.tenant_id
        )
        OR EXISTS (
            SELECT 1 FROM knowledge_lookup_asset_versions
            WHERE tenant_id = candidate.tenant_id
        ),
        false
    )
$$;

CREATE FUNCTION knowledge_lookup_asset_version_sequence_violated(
    candidate knowledge_lookup_asset_versions
) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        NOT (
            (
                candidate.asset_version = 1
                AND EXISTS (
                    SELECT 1 FROM knowledge_lookup_assets AS asset
                    WHERE asset.tenant_id = candidate.tenant_id
                      AND asset.lookup_asset_id = candidate.lookup_asset_id
                      AND asset.current_version = 1
                      AND asset.created_at_unix_micro = candidate.created_at_unix_micro
                      AND asset.updated_at_unix_micro = candidate.created_at_unix_micro
                )
                AND NOT EXISTS (
                    SELECT 1 FROM knowledge_lookup_asset_versions AS prior
                    WHERE prior.tenant_id = candidate.tenant_id
                      AND prior.lookup_asset_id = candidate.lookup_asset_id
                )
            )
            OR (
                candidate.asset_version > 1
                AND EXISTS (
                    SELECT 1
                    FROM knowledge_lookup_assets AS asset
                    JOIN knowledge_lookup_asset_versions AS prior
                      ON prior.tenant_id = asset.tenant_id
                     AND prior.lookup_asset_id = asset.lookup_asset_id
                     AND prior.asset_version = candidate.asset_version - 1
                    WHERE asset.tenant_id = candidate.tenant_id
                      AND asset.lookup_asset_id = candidate.lookup_asset_id
                      AND asset.current_version = candidate.asset_version
                      AND asset.updated_at_unix_micro = candidate.created_at_unix_micro
                      AND prior.created_at_unix_micro <= candidate.created_at_unix_micro
                )
            )
        ),
        false
    )
$$;

CREATE FUNCTION knowledge_lookup_definition_version_registry_violated(
    candidate knowledge_lookup_definition_versions
) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        NOT (
            EXISTS (
                SELECT 1
                FROM knowledge_lookup_definitions
                WHERE tenant_id = candidate.tenant_id
                  AND lookup_id = candidate.lookup_id
                  AND current_version = candidate.definition_version
                  AND state = candidate.state
                  AND disabled_at_unix_micro IS NOT DISTINCT FROM candidate.disabled_at_unix_micro
                  AND deleted_at_unix_micro IS NOT DISTINCT FROM candidate.deleted_at_unix_micro
                  AND updated_at_unix_micro = candidate.created_at_unix_micro
            )
            AND (
                (
                    candidate.definition_version = 1
                    AND candidate.mutation_kind = 'CREATE'
                )
                OR (
                    candidate.definition_version > 1
                    AND EXISTS (
                        SELECT 1
                        FROM knowledge_lookup_definition_versions AS previous
                        WHERE previous.tenant_id = candidate.tenant_id
                          AND previous.lookup_id = candidate.lookup_id
                          AND previous.definition_version = candidate.definition_version - 1
                          AND (
                            (
                                candidate.mutation_kind = 'REPLACE'
                                AND previous.state = candidate.state
                                AND previous.disabled_at_unix_micro
                                    IS NOT DISTINCT FROM candidate.disabled_at_unix_micro
                                AND previous.deleted_at_unix_micro
                                    IS NOT DISTINCT FROM candidate.deleted_at_unix_micro
                                AND (
                                    candidate.lookup_asset_id != previous.lookup_asset_id
                                    OR candidate.asset_version != previous.asset_version
                                    OR candidate.asset_size_bytes != previous.asset_size_bytes
                                    OR candidate.asset_content_sha256 != previous.asset_content_sha256
                                    OR candidate.definition_proto != previous.definition_proto
                                    OR candidate.columns_blob != previous.columns_blob
                                )
                            )
                            OR (
                                candidate.mutation_kind = 'ENABLE'
                                AND previous.state = 'DISABLED'
                                AND candidate.state = 'ACTIVE'
                                AND candidate.lookup_asset_id = previous.lookup_asset_id
                                AND candidate.asset_version = previous.asset_version
                                AND candidate.asset_size_bytes = previous.asset_size_bytes
                                AND candidate.asset_content_sha256 = previous.asset_content_sha256
                                AND candidate.definition_proto = previous.definition_proto
                                AND candidate.columns_blob = previous.columns_blob
                            )
                            OR (
                                candidate.mutation_kind = 'DISABLE'
                                AND previous.state = 'ACTIVE'
                                AND candidate.state = 'DISABLED'
                                AND candidate.lookup_asset_id = previous.lookup_asset_id
                                AND candidate.asset_version = previous.asset_version
                                AND candidate.asset_size_bytes = previous.asset_size_bytes
                                AND candidate.asset_content_sha256 = previous.asset_content_sha256
                                AND candidate.definition_proto = previous.definition_proto
                                AND candidate.columns_blob = previous.columns_blob
                            )
                            OR (
                                candidate.mutation_kind = 'DELETE'
                                AND previous.state = 'DISABLED'
                                AND candidate.state = 'DELETED'
                                AND candidate.disabled_at_unix_micro
                                    IS NOT DISTINCT FROM previous.disabled_at_unix_micro
                                AND candidate.lookup_asset_id = previous.lookup_asset_id
                                AND candidate.asset_version = previous.asset_version
                                AND candidate.asset_size_bytes = previous.asset_size_bytes
                                AND candidate.asset_content_sha256 = previous.asset_content_sha256
                                AND candidate.definition_proto = previous.definition_proto
                                AND candidate.columns_blob = previous.columns_blob
                            )
                          )
                    )
                )
            )
        ),
        false
    )
$$;

CREATE FUNCTION knowledge_lookup_definition_update_app_violated(
    prior knowledge_lookup_definitions,
    candidate knowledge_lookup_definitions
) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        NOT EXISTS (
            SELECT 1
            FROM app_workspaces
            WHERE tenant_id = candidate.tenant_id
              AND app_id = candidate.app_id
              AND state = 'active'
        )
        AND NOT (
            prior.state = 'DISABLED'
            AND candidate.state = 'DELETED'
            AND candidate.tenant_id = prior.tenant_id
            AND candidate.lookup_id = prior.lookup_id
            AND candidate.owner_id = prior.owner_id
            AND candidate.app_id = prior.app_id
            AND candidate.name = prior.name
            AND candidate.sharing_scope = prior.sharing_scope
            AND candidate.automatic = prior.automatic
            AND candidate.created_at_unix_micro = prior.created_at_unix_micro
        ),
        false
    )
$$;

-- knowledge_lookup_definition_versions

CREATE FUNCTION knowledge_lookup_definition_ordinary_version_capacity_available()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (
        SELECT count(*)
        FROM knowledge_lookup_definition_versions
        WHERE tenant_id = NEW.tenant_id
    ) >= 4096 THEN
        -- SQLite fires the registry-authority check first.
        IF knowledge_lookup_definition_version_registry_violated(NEW) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'lookup definition ordinary version capacity is exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_definition_ordinary_version_capacity_available
BEFORE INSERT ON knowledge_lookup_definition_versions
FOR EACH ROW
WHEN (NEW.mutation_kind IN ('CREATE', 'REPLACE', 'ENABLE'))
EXECUTE FUNCTION knowledge_lookup_definition_ordinary_version_capacity_available();

CREATE FUNCTION knowledge_lookup_definition_version_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (
        SELECT count(*)
        FROM knowledge_lookup_definition_versions
        WHERE tenant_id = NEW.tenant_id
    ) >= 8192 THEN
        -- SQLite fires the registry-authority check first.
        IF knowledge_lookup_definition_version_registry_violated(NEW) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'lookup definition version capacity is exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_definition_version_capacity_is_available
BEFORE INSERT ON knowledge_lookup_definition_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_definition_version_capacity_is_available();

CREATE FUNCTION knowledge_lookup_definition_version_matches_current_registry()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF knowledge_lookup_definition_version_registry_violated(NEW) THEN
        RAISE EXCEPTION 'lookup definition version is not the current registry authority';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_definition_version_matches_current_registry
BEFORE INSERT ON knowledge_lookup_definition_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_definition_version_matches_current_registry();

CREATE FUNCTION knowledge_lookup_definition_versions_no_delete()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'lookup definition versions are retained';
END;
$$;

CREATE TRIGGER knowledge_lookup_definition_versions_no_delete
BEFORE DELETE ON knowledge_lookup_definition_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_definition_versions_no_delete();

CREATE FUNCTION knowledge_lookup_definition_versions_no_update()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'lookup definition versions are immutable';
END;
$$;

CREATE TRIGGER knowledge_lookup_definition_versions_no_update
BEFORE UPDATE ON knowledge_lookup_definition_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_definition_versions_no_update();

-- knowledge_lookup_definitions

CREATE FUNCTION knowledge_lookup_active_definition_requires_active_app_insert()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM app_workspaces
        WHERE tenant_id = NEW.tenant_id
          AND app_id = NEW.app_id
          AND state = 'active'
    ) THEN
        RAISE EXCEPTION 'lookup definition requires an active app workspace';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_active_definition_requires_active_app_insert
BEFORE INSERT ON knowledge_lookup_definitions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_active_definition_requires_active_app_insert();

CREATE FUNCTION knowledge_lookup_definition_initial_shape_is_exact()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'lookup definition initial shape is invalid';
END;
$$;

CREATE TRIGGER knowledge_lookup_definition_initial_shape_is_exact
BEFORE INSERT ON knowledge_lookup_definitions
FOR EACH ROW
WHEN (
    NEW.current_version != 1
    OR NEW.state != 'ACTIVE'
    OR NEW.updated_at_unix_micro != NEW.created_at_unix_micro
)
EXECUTE FUNCTION knowledge_lookup_definition_initial_shape_is_exact();

CREATE FUNCTION knowledge_lookup_definition_tenant_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (
        SELECT count(*)
        FROM knowledge_lookup_definitions
        WHERE tenant_id = NEW.tenant_id
    ) >= 2048 THEN
        RAISE EXCEPTION 'lookup definition tenant capacity is exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_definition_tenant_capacity_is_available
BEFORE INSERT ON knowledge_lookup_definitions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_definition_tenant_capacity_is_available();

CREATE FUNCTION knowledge_lookup_definition_transition_is_valid()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- SQLite fires the active-app check first.
    IF knowledge_lookup_definition_update_app_violated(OLD, NEW) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'lookup definition registry transition is invalid';
END;
$$;

-- numeric: OLD.current_version may be 9223372036854775807, and bigint + 1
-- would raise 22003 instead of this trigger's message.
CREATE TRIGGER knowledge_lookup_definition_transition_is_valid
BEFORE UPDATE ON knowledge_lookup_definitions
FOR EACH ROW
WHEN (NOT (
    NEW.tenant_id = OLD.tenant_id
    AND NEW.lookup_id = OLD.lookup_id
    AND NEW.owner_id = OLD.owner_id
    AND NEW.created_at_unix_micro = OLD.created_at_unix_micro
    AND NEW.current_version = OLD.current_version::numeric + 1
    AND NEW.updated_at_unix_micro > OLD.updated_at_unix_micro
    AND (
        (
            OLD.state = 'ACTIVE'
            AND NEW.state = 'ACTIVE'
        )
        OR (
            OLD.state = 'DISABLED'
            AND NEW.state = 'DISABLED'
            AND NEW.disabled_at_unix_micro IS NOT DISTINCT FROM OLD.disabled_at_unix_micro
        )
        OR (
            NEW.app_id = OLD.app_id
            AND NEW.name = OLD.name
            AND NEW.sharing_scope = OLD.sharing_scope
            AND NEW.automatic = OLD.automatic
            AND (
                (
                    OLD.state = 'ACTIVE'
                    AND NEW.state = 'DISABLED'
                    AND NEW.disabled_at_unix_micro = NEW.updated_at_unix_micro
                    AND NEW.deleted_at_unix_micro IS NULL
                )
                OR (
                    OLD.state = 'DISABLED'
                    AND NEW.state = 'ACTIVE'
                    AND NEW.disabled_at_unix_micro IS NULL
                    AND NEW.deleted_at_unix_micro IS NULL
                )
                OR (
                    OLD.state = 'DISABLED'
                    AND NEW.state = 'DELETED'
                    AND NEW.disabled_at_unix_micro IS NOT DISTINCT FROM OLD.disabled_at_unix_micro
                    AND NEW.deleted_at_unix_micro = NEW.updated_at_unix_micro
                )
            )
        )
    )
))
EXECUTE FUNCTION knowledge_lookup_definition_transition_is_valid();

CREATE FUNCTION knowledge_lookup_definition_update_requires_active_app()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF knowledge_lookup_definition_update_app_violated(OLD, NEW) THEN
        RAISE EXCEPTION 'lookup definition update requires an active app workspace';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_definition_update_requires_active_app
BEFORE UPDATE ON knowledge_lookup_definitions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_definition_update_requires_active_app();

-- knowledge_lookup_asset_versions

CREATE FUNCTION knowledge_lookup_asset_version_accounts_after_insert()
RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    version_delta bigint;
    bytes_delta bigint;
BEGIN
    -- See "Ledger accounting" above: +1 / +canonical_bytes for a single-row
    -- statement, the whole statement's delta on its first row otherwise.
    SELECT
        (
            SELECT count(*) FROM knowledge_lookup_asset_versions
            WHERE tenant_id = NEW.tenant_id
        ) - ledger.published_version_count,
        COALESCE((
            SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_stages
            WHERE tenant_id = NEW.tenant_id
        ), 0)
        + COALESCE((
            SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_versions
            WHERE tenant_id = NEW.tenant_id
        ), 0)
        - ledger.stored_content_bytes
    INTO version_delta, bytes_delta
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id;

    IF FOUND AND (version_delta <> 0 OR bytes_delta <> 0) THEN
        UPDATE knowledge_lookup_asset_tenant_ledgers
        SET published_version_count = published_version_count + version_delta,
            stored_content_bytes = stored_content_bytes + bytes_delta
        WHERE tenant_id = NEW.tenant_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_version_accounts_after_insert
AFTER INSERT ON knowledge_lookup_asset_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_version_accounts_after_insert();

CREATE FUNCTION knowledge_lookup_asset_version_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_lookup_asset_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = NEW.tenant_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM knowledge_lookup_asset_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = NEW.tenant_id
          AND ledger.published_version_count < 8192
          AND ledger.stored_content_bytes <= 2147483648 - NEW.canonical_bytes
    ) THEN
        -- SQLite fires the sequence check first.
        IF knowledge_lookup_asset_version_sequence_violated(NEW) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'lookup asset version capacity exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_version_capacity_is_available
BEFORE INSERT ON knowledge_lookup_asset_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_version_capacity_is_available();

CREATE FUNCTION knowledge_lookup_asset_version_deletes_are_forbidden()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'published lookup asset versions cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_version_deletes_are_forbidden
BEFORE DELETE ON knowledge_lookup_asset_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_version_deletes_are_forbidden();

CREATE FUNCTION knowledge_lookup_asset_version_requires_tenant_ledger()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
        WHERE tenant_id = NEW.tenant_id
    ) THEN
        -- SQLite fires the sequence check first.
        IF knowledge_lookup_asset_version_sequence_violated(NEW) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'lookup asset tenant ledger is missing';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_version_requires_tenant_ledger
BEFORE INSERT ON knowledge_lookup_asset_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_version_requires_tenant_ledger();

CREATE FUNCTION knowledge_lookup_asset_version_sequence_is_exact()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF knowledge_lookup_asset_version_sequence_violated(NEW) THEN
        RAISE EXCEPTION 'lookup asset version sequence is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_version_sequence_is_exact
BEFORE INSERT ON knowledge_lookup_asset_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_version_sequence_is_exact();

CREATE FUNCTION knowledge_lookup_asset_version_updates_are_forbidden()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'published lookup asset versions are immutable';
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_version_updates_are_forbidden
BEFORE UPDATE ON knowledge_lookup_asset_versions
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_version_updates_are_forbidden();

-- knowledge_lookup_asset_tenant_ledgers

CREATE FUNCTION knowledge_lookup_asset_ledger_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'lookup asset tenant ledger cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_ledger_delete_is_forbidden
BEFORE DELETE ON knowledge_lookup_asset_tenant_ledgers
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_ledger_delete_is_forbidden();

CREATE FUNCTION knowledge_lookup_asset_ledger_identity_collision_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
        WHERE tenant_id = NEW.tenant_id
    ) THEN
        -- SQLite fires the initial-shape check first.
        IF knowledge_lookup_asset_ledger_initial_shape_violated(NEW) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'lookup asset tenant ledger already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_ledger_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_lookup_asset_tenant_ledgers
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_ledger_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_lookup_asset_ledger_initial_shape_is_exact()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF knowledge_lookup_asset_ledger_initial_shape_violated(NEW) THEN
        RAISE EXCEPTION 'lookup asset tenant ledger must begin empty';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_ledger_initial_shape_is_exact
BEFORE INSERT ON knowledge_lookup_asset_tenant_ledgers
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_ledger_initial_shape_is_exact();

CREATE FUNCTION knowledge_lookup_asset_ledger_transition_is_exact()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id
       OR NEW.staged_asset_count <> (
           SELECT count(*) FROM knowledge_lookup_asset_stages
           WHERE tenant_id = OLD.tenant_id
       )
       OR NEW.asset_identity_count <> (
           SELECT count(*) FROM knowledge_lookup_assets
           WHERE tenant_id = OLD.tenant_id
       )
       OR NEW.published_version_count <> (
           SELECT count(*) FROM knowledge_lookup_asset_versions
           WHERE tenant_id = OLD.tenant_id
       )
       OR NEW.stored_content_bytes <> (
           COALESCE((
               SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_stages
               WHERE tenant_id = OLD.tenant_id
           ), 0)
           + COALESCE((
               SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_versions
               WHERE tenant_id = OLD.tenant_id
           ), 0)
       ) THEN
        RAISE EXCEPTION 'lookup asset tenant ledger transition is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_ledger_transition_is_exact
BEFORE UPDATE ON knowledge_lookup_asset_tenant_ledgers
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_ledger_transition_is_exact();

-- knowledge_lookup_assets

CREATE FUNCTION knowledge_lookup_asset_identity_accounts_after_insert()
RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    identity_delta bigint;
BEGIN
    -- See "Ledger accounting" above: +1 for a single-row statement, the
    -- whole statement's delta on its first row otherwise.
    SELECT
        (
            SELECT count(*) FROM knowledge_lookup_assets
            WHERE tenant_id = NEW.tenant_id
        ) - ledger.asset_identity_count
    INTO identity_delta
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id;

    IF FOUND AND identity_delta <> 0 THEN
        UPDATE knowledge_lookup_asset_tenant_ledgers
        SET asset_identity_count = asset_identity_count + identity_delta
        WHERE tenant_id = NEW.tenant_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_identity_accounts_after_insert
AFTER INSERT ON knowledge_lookup_assets
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_identity_accounts_after_insert();

CREATE FUNCTION knowledge_lookup_asset_identity_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_lookup_asset_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = NEW.tenant_id
    )
    AND (
        NEW.current_version <> 1
        OR NOT EXISTS (
            SELECT 1
            FROM knowledge_lookup_asset_tenant_ledgers AS ledger
            WHERE ledger.tenant_id = NEW.tenant_id
              AND ledger.asset_identity_count < 2048
        )
    ) THEN
        RAISE EXCEPTION 'lookup asset identity capacity is unavailable or first version is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_identity_capacity_is_available
BEFORE INSERT ON knowledge_lookup_assets
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_identity_capacity_is_available();

CREATE FUNCTION knowledge_lookup_asset_identity_delete_is_forbidden()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'lookup asset identities cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_identity_delete_is_forbidden
BEFORE DELETE ON knowledge_lookup_assets
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_identity_delete_is_forbidden();

CREATE FUNCTION knowledge_lookup_asset_identity_requires_tenant_ledger()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
        WHERE tenant_id = NEW.tenant_id
    ) THEN
        RAISE EXCEPTION 'lookup asset tenant ledger is missing';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_identity_requires_tenant_ledger
BEFORE INSERT ON knowledge_lookup_assets
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_identity_requires_tenant_ledger();

CREATE FUNCTION knowledge_lookup_asset_identity_transition_is_exact()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'lookup asset current version transition is invalid';
END;
$$;

-- numeric: OLD.current_version may be 9223372036854775807, and bigint + 1
-- would raise 22003 instead of this trigger's message.
CREATE TRIGGER knowledge_lookup_asset_identity_transition_is_exact
BEFORE UPDATE ON knowledge_lookup_assets
FOR EACH ROW
WHEN (
    NEW.tenant_id <> OLD.tenant_id
    OR NEW.lookup_asset_id <> OLD.lookup_asset_id
    OR NEW.created_at_unix_micro <> OLD.created_at_unix_micro
    OR NEW.current_version <> OLD.current_version::numeric + 1
    OR NEW.updated_at_unix_micro < OLD.updated_at_unix_micro
)
EXECUTE FUNCTION knowledge_lookup_asset_identity_transition_is_exact();

-- knowledge_lookup_asset_stages

CREATE FUNCTION knowledge_lookup_asset_stage_accounts_after_delete()
RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    staged_delta bigint;
    bytes_delta bigint;
BEGIN
    -- See "Ledger accounting" above: -1 / -canonical_bytes for a single-row
    -- statement, the whole statement's delta on its first row otherwise.
    SELECT
        (
            SELECT count(*) FROM knowledge_lookup_asset_stages
            WHERE tenant_id = OLD.tenant_id
        ) - ledger.staged_asset_count,
        COALESCE((
            SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_stages
            WHERE tenant_id = OLD.tenant_id
        ), 0)
        + COALESCE((
            SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_versions
            WHERE tenant_id = OLD.tenant_id
        ), 0)
        - ledger.stored_content_bytes
    INTO staged_delta, bytes_delta
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = OLD.tenant_id;

    IF FOUND AND (staged_delta <> 0 OR bytes_delta <> 0) THEN
        UPDATE knowledge_lookup_asset_tenant_ledgers
        SET staged_asset_count = staged_asset_count + staged_delta,
            stored_content_bytes = stored_content_bytes + bytes_delta
        WHERE tenant_id = OLD.tenant_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_stage_accounts_after_delete
AFTER DELETE ON knowledge_lookup_asset_stages
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_stage_accounts_after_delete();

CREATE FUNCTION knowledge_lookup_asset_stage_accounts_after_insert()
RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    staged_delta bigint;
    bytes_delta bigint;
BEGIN
    -- See "Ledger accounting" above: +1 / +canonical_bytes for a single-row
    -- statement, the whole statement's delta on its first row otherwise.
    SELECT
        (
            SELECT count(*) FROM knowledge_lookup_asset_stages
            WHERE tenant_id = NEW.tenant_id
        ) - ledger.staged_asset_count,
        COALESCE((
            SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_stages
            WHERE tenant_id = NEW.tenant_id
        ), 0)
        + COALESCE((
            SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_versions
            WHERE tenant_id = NEW.tenant_id
        ), 0)
        - ledger.stored_content_bytes
    INTO staged_delta, bytes_delta
    FROM knowledge_lookup_asset_tenant_ledgers AS ledger
    WHERE ledger.tenant_id = NEW.tenant_id;

    IF FOUND AND (staged_delta <> 0 OR bytes_delta <> 0) THEN
        UPDATE knowledge_lookup_asset_tenant_ledgers
        SET staged_asset_count = staged_asset_count + staged_delta,
            stored_content_bytes = stored_content_bytes + bytes_delta
        WHERE tenant_id = NEW.tenant_id;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_stage_accounts_after_insert
AFTER INSERT ON knowledge_lookup_asset_stages
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_stage_accounts_after_insert();

CREATE FUNCTION knowledge_lookup_asset_stage_capacity_is_available()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_lookup_asset_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = NEW.tenant_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM knowledge_lookup_asset_tenant_ledgers AS ledger
        WHERE ledger.tenant_id = NEW.tenant_id
          AND ledger.staged_asset_count < 64
          AND ledger.stored_content_bytes <= 2147483648 - NEW.canonical_bytes
    ) THEN
        RAISE EXCEPTION 'lookup asset staging capacity exhausted';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_stage_capacity_is_available
BEFORE INSERT ON knowledge_lookup_asset_stages
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_stage_capacity_is_available();

CREATE FUNCTION knowledge_lookup_asset_stage_requires_tenant_ledger()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
        WHERE tenant_id = NEW.tenant_id
    ) THEN
        RAISE EXCEPTION 'lookup asset tenant ledger is missing';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_stage_requires_tenant_ledger
BEFORE INSERT ON knowledge_lookup_asset_stages
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_stage_requires_tenant_ledger();

CREATE FUNCTION knowledge_lookup_asset_stage_updates_are_forbidden()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'lookup asset stages are immutable';
END;
$$;

CREATE TRIGGER knowledge_lookup_asset_stage_updates_are_forbidden
BEFORE UPDATE ON knowledge_lookup_asset_stages
FOR EACH ROW
EXECUTE FUNCTION knowledge_lookup_asset_stage_updates_are_forbidden();
