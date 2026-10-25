-- open-splunk PostgreSQL control plane, triggers for the knowledge catalog
-- (lane knowledge_catalog_2): knowledge_catalog_revision_heads,
-- knowledge_catalog_tenants, knowledge_mutation_commit_authorities,
-- knowledge_object_list_order_keys and knowledge_object_list_projection_seals.
-- Applied after 0001..0006. See migrations/postgres/README.md.
--
-- Every SQLite trigger keeps its name, its BEFORE/AFTER timing, its events,
-- its UPDATE OF column list and its FOR EACH ROW granularity. Error text is
-- byte-identical and raised with the default SQLSTATE P0001. A condition
-- without a subquery stays in the trigger's WHEN clause; a condition with a
-- subquery (which PostgreSQL does not allow in WHEN) moves into the function
-- body, guarded the same way.
--
-- Translation notes:
--   * length(blob) on bytea columns is octet_length().
--   * x IS y on nullable columns is IS NOT DISTINCT FROM.
--   * CAST(text AS BLOB) >= CAST(text AS BLOB) is a comparison under
--     COLLATE "C", which PostgreSQL implements as memcmp then length, the
--     same order as SQLite's blob comparison.
--   * randomblob(32) is sha256() over two gen_random_uuid() values (both
--     built in; pgcrypto is not required). That is 244 bits of CSPRNG input
--     spread over 32 bytes; the token is only ever compared for equality and
--     checked for a width of 32 bytes.
--
-- Name resolution. Objects are created unqualified under the runner's
-- search_path (open_splunk), and every function here carries SET search_path
-- FROM CURRENT, so its body resolves tables in open_splunk whatever the
-- calling session's search_path is. A session that qualifies table names
-- under another search_path, such as an operator's psql, still fires working
-- triggers, and no same-named object elsewhere can be picked up instead.
--
-- Firing order. SQLite fires same-event triggers newest-first; PostgreSQL
-- fires them in name order. Where two triggers on one event can both reject a
-- row, the trigger PostgreSQL reaches first also requires that the triggers
-- SQLite would have reached before it accept the row, so every input is
-- rejected with the same message as in SQLite and the set of rejected rows is
-- unchanged. No trigger is renamed. The affected events are:
--   * knowledge_catalog_tenants BEFORE INSERT: SQLite checks
--     initial_shape_is_valid before identity_collision_is_forbidden.
--   * knowledge_catalog_tenants BEFORE UPDATE: SQLite checks
--     identity_is_immutable before revision_transition_is_valid.
--   * knowledge_catalog_revision_heads BEFORE INSERT: SQLite checks
--     insert_agrees_with_tenant before identity_collision_is_forbidden.
--   * knowledge_mutation_commit_authorities BEFORE INSERT: SQLite checks
--     is_exact before collision_is_forbidden.
--   * knowledge_object_list_projection_seals BEFORE INSERT: SQLite checks
--     requires_order_key, then is_complete, then identity_collision.
-- The two AFTER INSERT triggers on knowledge_catalog_tenants also swap order
-- (SQLite provisions the lookup ledger first); they write different tables
-- and neither can fail for a new tenant, so the order is not observable.
--
-- PostgreSQL queues AFTER ROW triggers until the end of the statement, while
-- SQLite runs them after each row. The only AFTER UPDATE trigger here rotates
-- the revision head of the same tenant row, so a multi-row UPDATE produces
-- the same result.
--
-- Concurrency (SERIALIZABLE with retry). Every *_collision_is_forbidden
-- trigger is backed by a PRIMARY KEY or UNIQUE constraint on the same
-- columns, so two concurrent inserts that both pass the trigger still lose
-- with 23505 or 40001 rather than committing duplicates. The revision head
-- rotation runs inside the UPDATE of knowledge_catalog_tenants, which holds
-- the tenant row lock, and updates the single head row for that tenant
-- (PRIMARY KEY tenant_id), so concurrent revision advances serialise on the
-- tenant row and the loser retries. The exactness checks only read other
-- tables; SSI turns a concurrent change to what they read into 40001.
-- Load-bearing assumption: the writer advances knowledge_catalog_tenants
-- .catalog_revision in the same transaction before it inserts the mutation
-- commit authority, so the authority is checked against the head it rotated.
--
-- Helper predicates. Where a yielding trigger needs the full condition of a
-- larger trigger, the condition lives in one SQL function that both trigger
-- functions call, so the logic is written once:
-- knowledge_mutation_commit_authority_exact_holds and
-- knowledge_list_projection_seal_complete_holds.
--
-- Differential scenarios for every trigger (one accepted and one rejected
-- statement at least, plus each precedence case above) are in
-- internal/control/testdata/postgres_triggers/knowledge_catalog_2.json. Each
-- runs as BEGIN; setup_sql...; statement; ROLLBACK with search_path set to
-- open_splunk; a few setups use ALTER TABLE ... DISABLE TRIGGER to reach a
-- state the remaining triggers otherwise make unreachable.

-- ---------------------------------------------------------------------------
-- knowledge_catalog_tenants
-- ---------------------------------------------------------------------------

CREATE FUNCTION knowledge_catalog_tenant_identity_collision_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- SQLite checks knowledge_catalog_tenant_initial_shape_is_valid first,
    -- so a non-empty duplicate is reported as not empty.
    IF NEW.catalog_revision = 0
       AND NEW.identity_count = 0
       AND NEW.version_count = 0
       AND NEW.definition_body_bytes = 0
       AND NEW.idempotency_count = 0
       AND NEW.active_object_count = 0
       AND NEW.recovery_audit_count = 0
       AND EXISTS (
           SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = NEW.tenant_id
       ) THEN
        RAISE EXCEPTION 'knowledge catalog tenant already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_catalog_tenant_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_catalog_tenants
FOR EACH ROW
EXECUTE FUNCTION knowledge_catalog_tenant_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_catalog_tenant_initial_shape_is_valid()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge catalog tenant must begin empty';
END;
$$;

CREATE TRIGGER knowledge_catalog_tenant_initial_shape_is_valid
BEFORE INSERT ON knowledge_catalog_tenants
FOR EACH ROW
WHEN (
    NEW.catalog_revision <> 0
    OR NEW.identity_count <> 0
    OR NEW.version_count <> 0
    OR NEW.definition_body_bytes <> 0
    OR NEW.idempotency_count <> 0
    OR NEW.active_object_count <> 0
    OR NEW.recovery_audit_count <> 0
)
EXECUTE FUNCTION knowledge_catalog_tenant_initial_shape_is_valid();

CREATE FUNCTION knowledge_catalog_revision_transition_is_valid()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge catalog revision must advance by one';
END;
$$;

-- SQLite checks knowledge_catalog_tenant_identity_is_immutable first, so a
-- row whose tenant_id changes is left to that trigger. NEW.tenant_id can only
-- differ from OLD.tenant_id when tenant_id is in the SET list, because no
-- BEFORE trigger here rewrites NEW.
CREATE TRIGGER knowledge_catalog_revision_transition_is_valid
BEFORE UPDATE OF catalog_revision ON knowledge_catalog_tenants
FOR EACH ROW
WHEN (
    NEW.catalog_revision <> OLD.catalog_revision
    AND NEW.catalog_revision <> OLD.catalog_revision + 1
    AND NEW.tenant_id = OLD.tenant_id
)
EXECUTE FUNCTION knowledge_catalog_revision_transition_is_valid();

CREATE FUNCTION knowledge_catalog_tenant_identity_is_immutable()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge catalog tenant identity is immutable';
END;
$$;

CREATE TRIGGER knowledge_catalog_tenant_identity_is_immutable
BEFORE UPDATE OF tenant_id ON knowledge_catalog_tenants
FOR EACH ROW
WHEN (NEW.tenant_id <> OLD.tenant_id)
EXECUTE FUNCTION knowledge_catalog_tenant_identity_is_immutable();

CREATE FUNCTION knowledge_catalog_tenant_delete_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge catalog tenant cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_catalog_tenant_delete_is_forbidden
BEFORE DELETE ON knowledge_catalog_tenants
FOR EACH ROW
EXECUTE FUNCTION knowledge_catalog_tenant_delete_is_forbidden();

CREATE FUNCTION knowledge_catalog_tenant_creates_revision_head()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    INSERT INTO knowledge_catalog_revision_heads (
        tenant_id, catalog_revision, state_token
    ) VALUES (
        NEW.tenant_id,
        NEW.catalog_revision,
        sha256(uuid_send(gen_random_uuid()) || uuid_send(gen_random_uuid()))
    );
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_catalog_tenant_creates_revision_head
AFTER INSERT ON knowledge_catalog_tenants
FOR EACH ROW
EXECUTE FUNCTION knowledge_catalog_tenant_creates_revision_head();

CREATE FUNCTION knowledge_catalog_revision_requires_exact_head()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM knowledge_catalog_revision_heads
        WHERE tenant_id = OLD.tenant_id
          AND catalog_revision = OLD.catalog_revision
          AND octet_length(state_token) = 32
    ) THEN
        RAISE EXCEPTION 'knowledge catalog revision head is missing';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_catalog_revision_requires_exact_head
BEFORE UPDATE OF catalog_revision ON knowledge_catalog_tenants
FOR EACH ROW
WHEN (NEW.catalog_revision <> OLD.catalog_revision)
EXECUTE FUNCTION knowledge_catalog_revision_requires_exact_head();

CREATE FUNCTION knowledge_catalog_revision_rotates_state_token()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    UPDATE knowledge_catalog_revision_heads
    SET catalog_revision = NEW.catalog_revision,
        state_token = sha256(
            uuid_send(gen_random_uuid()) || uuid_send(gen_random_uuid())
        )
    WHERE tenant_id = OLD.tenant_id
      AND catalog_revision = OLD.catalog_revision;

    IF NOT EXISTS (
        SELECT 1 FROM knowledge_catalog_revision_heads
        WHERE tenant_id = NEW.tenant_id
          AND catalog_revision = NEW.catalog_revision
          AND octet_length(state_token) = 32
    ) THEN
        RAISE EXCEPTION 'knowledge catalog revision head rotation failed';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_catalog_revision_rotates_state_token
AFTER UPDATE OF catalog_revision ON knowledge_catalog_tenants
FOR EACH ROW
WHEN (NEW.catalog_revision <> OLD.catalog_revision)
EXECUTE FUNCTION knowledge_catalog_revision_rotates_state_token();

CREATE FUNCTION knowledge_catalog_tenant_provisions_lookup_asset_ledger()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    INSERT INTO knowledge_lookup_asset_tenant_ledgers (tenant_id)
    VALUES (NEW.tenant_id);
    RETURN NULL;
END;
$$;

CREATE TRIGGER knowledge_catalog_tenant_provisions_lookup_asset_ledger
AFTER INSERT ON knowledge_catalog_tenants
FOR EACH ROW
EXECUTE FUNCTION knowledge_catalog_tenant_provisions_lookup_asset_ledger();

-- ---------------------------------------------------------------------------
-- knowledge_catalog_revision_heads
-- ---------------------------------------------------------------------------

CREATE FUNCTION knowledge_catalog_revision_head_identity_collision_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- SQLite checks knowledge_catalog_revision_head_insert_agrees_with_tenant
    -- first, so a duplicate that disagrees with its tenant is reported as
    -- disagreeing.
    IF EXISTS (
        SELECT 1 FROM knowledge_catalog_revision_heads
        WHERE tenant_id = NEW.tenant_id
    ) AND EXISTS (
        SELECT 1 FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id
          AND catalog_revision = NEW.catalog_revision
    ) THEN
        RAISE EXCEPTION 'knowledge catalog revision head already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_catalog_revision_head_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_catalog_revision_heads
FOR EACH ROW
EXECUTE FUNCTION knowledge_catalog_revision_head_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_catalog_revision_head_insert_agrees_with_tenant()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM knowledge_catalog_tenants
        WHERE tenant_id = NEW.tenant_id
          AND catalog_revision = NEW.catalog_revision
    ) THEN
        RAISE EXCEPTION 'knowledge catalog revision head disagrees with tenant';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_catalog_revision_head_insert_agrees_with_tenant
BEFORE INSERT ON knowledge_catalog_revision_heads
FOR EACH ROW
EXECUTE FUNCTION knowledge_catalog_revision_head_insert_agrees_with_tenant();

CREATE FUNCTION knowledge_catalog_revision_head_transition_is_exact()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id
       OR NEW.catalog_revision <> OLD.catalog_revision + 1
       OR octet_length(NEW.state_token) <> 32
       OR NEW.state_token = OLD.state_token
       OR NOT EXISTS (
           SELECT 1 FROM knowledge_catalog_tenants
           WHERE tenant_id = OLD.tenant_id
             AND catalog_revision = NEW.catalog_revision
       ) THEN
        RAISE EXCEPTION 'knowledge catalog revision head transition is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_catalog_revision_head_transition_is_exact
BEFORE UPDATE ON knowledge_catalog_revision_heads
FOR EACH ROW
EXECUTE FUNCTION knowledge_catalog_revision_head_transition_is_exact();

CREATE FUNCTION knowledge_catalog_revision_head_delete_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge catalog revision head cannot be deleted';
END;
$$;

CREATE TRIGGER knowledge_catalog_revision_head_delete_is_forbidden
BEFORE DELETE ON knowledge_catalog_revision_heads
FOR EACH ROW
EXECUTE FUNCTION knowledge_catalog_revision_head_delete_is_forbidden();

-- ---------------------------------------------------------------------------
-- knowledge_mutation_commit_authorities
-- ---------------------------------------------------------------------------

-- The acceptance condition of knowledge_mutation_commit_authority_is_exact:
-- the authority names the tenant's current revision and head token, and the
-- current object version, its lifecycle and its successful or recovery audit
-- row all agree with it.
CREATE FUNCTION knowledge_mutation_commit_authority_exact_holds(
    candidate knowledge_mutation_commit_authorities
) RETURNS boolean LANGUAGE sql STABLE
SET search_path FROM CURRENT
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM knowledge_catalog_tenants AS tenant
        JOIN knowledge_catalog_revision_heads AS head
          ON head.tenant_id = tenant.tenant_id
         AND head.catalog_revision = tenant.catalog_revision
        JOIN knowledge_object_versions AS version
          ON version.tenant_id = tenant.tenant_id
         AND version.knowledge_object_id = candidate.knowledge_object_id
         AND version.object_version = candidate.object_version
        JOIN knowledge_object_version_lifecycle AS lifecycle
          ON lifecycle.tenant_id = version.tenant_id
         AND lifecycle.knowledge_object_id = version.knowledge_object_id
         AND lifecycle.object_version = version.object_version
         AND lifecycle.state = version.state
        JOIN knowledge_objects AS current
          ON current.tenant_id = version.tenant_id
         AND current.knowledge_object_id = version.knowledge_object_id
         AND current.current_version = version.object_version
         AND current.app_id = version.app_id
         AND current.owner_id = version.owner_id
         AND current.object_type = version.object_type
         AND current.name = version.name
         AND current.sharing_scope = version.sharing_scope
         AND current.state = version.state
         AND current.definition_digest_key = version.definition_digest_key
         AND current.updated_at_unix_micro = version.created_at_unix_micro
         AND current.disabled_at_unix_micro
             IS NOT DISTINCT FROM lifecycle.disabled_at_unix_micro
         AND current.quarantined_at_unix_micro
             IS NOT DISTINCT FROM lifecycle.quarantined_at_unix_micro
         AND current.deleted_at_unix_micro
             IS NOT DISTINCT FROM lifecycle.deleted_at_unix_micro
         AND current.quarantine_reason
             IS NOT DISTINCT FROM lifecycle.quarantine_reason
        WHERE tenant.tenant_id = candidate.tenant_id
          AND tenant.catalog_revision = candidate.catalog_revision
          AND head.state_token = candidate.catalog_state_token
          AND octet_length(head.state_token) = 32
          AND version.mutation_kind = candidate.mutation_kind
          AND version.created_at_unix_micro = candidate.occurred_at_unix_micro
          AND (
              (
                  candidate.mutation_kind <> 'quarantine'
                  AND candidate.recovery_audit_sequence IS NULL
                  AND EXISTS (
                      SELECT 1 FROM audit_events AS event
                      WHERE event.tenant_id = version.tenant_id
                        AND event.sequence = candidate.successful_audit_sequence
                        AND event.actor_kind = candidate.actor_kind
                        AND event.actor_id = candidate.actor_id
                        AND event.occurred_at_unix_micro = version.created_at_unix_micro
                        AND event.target_kind = 'knowledge_object'
                        AND event.target_id = version.knowledge_object_id
                        AND event.target_version = version.object_version
                        AND event.app_id = version.app_id
                        AND event.object_type = version.object_type
                        AND event.sharing_scope = version.sharing_scope
                        AND event.action = CASE candidate.mutation_kind
                            WHEN 'create' THEN 'knowledge.object.create'
                            WHEN 'update' THEN 'knowledge.object.update'
                            WHEN 'scope_change' THEN 'knowledge.object.scope_change'
                            WHEN 'enable' THEN 'knowledge.object.enable'
                            WHEN 'disable' THEN 'knowledge.object.disable'
                            WHEN 'delete' THEN 'knowledge.object.delete'
                        END
                  )
              )
              OR (
                  candidate.mutation_kind = 'quarantine'
                  AND candidate.successful_audit_sequence IS NULL
                  AND EXISTS (
                      SELECT 1 FROM knowledge_recovery_audit AS recovery
                      WHERE recovery.tenant_id = version.tenant_id
                        AND recovery.sequence = candidate.recovery_audit_sequence
                        AND recovery.actor_kind = candidate.actor_kind
                        AND recovery.actor_id = candidate.actor_id
                        AND recovery.occurred_at_unix_micro = version.created_at_unix_micro
                        AND recovery.knowledge_object_id = version.knowledge_object_id
                        AND recovery.object_version = version.object_version
                        AND recovery.app_id = version.app_id
                        AND recovery.object_type = version.object_type
                        AND recovery.sharing_scope = version.sharing_scope
                        AND recovery.recovery_reason = version.quarantine_reason
                  )
              )
          )
    );
$$;

CREATE FUNCTION knowledge_mutation_commit_authority_collision_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- SQLite checks knowledge_mutation_commit_authority_is_exact first, so a
    -- colliding authority that is also not exact is reported as invalid.
    IF EXISTS (
        SELECT 1
        FROM knowledge_mutation_commit_authorities
        WHERE tenant_id = NEW.tenant_id
          AND (
              catalog_revision = NEW.catalog_revision
              OR catalog_state_token = NEW.catalog_state_token
          )
    ) AND knowledge_mutation_commit_authority_exact_holds(NEW) THEN
        RAISE EXCEPTION 'knowledge mutation commit authority already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_mutation_commit_authority_collision_is_forbidden
BEFORE INSERT ON knowledge_mutation_commit_authorities
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_commit_authority_collision_is_forbidden();

CREATE FUNCTION knowledge_mutation_commit_authority_is_exact()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT knowledge_mutation_commit_authority_exact_holds(NEW) THEN
        RAISE EXCEPTION 'knowledge mutation commit authority is invalid';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_mutation_commit_authority_is_exact
BEFORE INSERT ON knowledge_mutation_commit_authorities
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_commit_authority_is_exact();

CREATE FUNCTION knowledge_mutation_commit_authority_update_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge mutation commit authority is immutable';
END;
$$;

CREATE TRIGGER knowledge_mutation_commit_authority_update_is_forbidden
BEFORE UPDATE ON knowledge_mutation_commit_authorities
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_commit_authority_update_is_forbidden();

CREATE FUNCTION knowledge_mutation_commit_authority_delete_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge mutation commit authority is retained';
END;
$$;

CREATE TRIGGER knowledge_mutation_commit_authority_delete_is_forbidden
BEFORE DELETE ON knowledge_mutation_commit_authorities
FOR EACH ROW
EXECUTE FUNCTION knowledge_mutation_commit_authority_delete_is_forbidden();

-- ---------------------------------------------------------------------------
-- knowledge_object_list_order_keys
-- ---------------------------------------------------------------------------

CREATE FUNCTION knowledge_list_order_key_agrees_with_authorities()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM knowledge_object_list_projections AS projection
        JOIN knowledge_object_versions AS version
          ON version.tenant_id = projection.tenant_id
         AND version.knowledge_object_id = projection.knowledge_object_id
         AND version.object_version = projection.object_version
        JOIN knowledge_object_versions AS creation_version
          ON creation_version.tenant_id = projection.tenant_id
         AND creation_version.knowledge_object_id = projection.knowledge_object_id
         AND creation_version.object_version = 1
        WHERE projection.tenant_id = NEW.tenant_id
          AND projection.knowledge_object_id = NEW.knowledge_object_id
          AND projection.object_version = NEW.object_version
          AND NEW.updated_at_unix_micro = version.created_at_unix_micro
          AND NEW.created_at_unix_micro = creation_version.created_at_unix_micro
    ) THEN
        RAISE EXCEPTION 'knowledge list order key disagrees with authorities';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_list_order_key_agrees_with_authorities
BEFORE INSERT ON knowledge_object_list_order_keys
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_order_key_agrees_with_authorities();

-- SQLite also checks knowledge_list_order_key_agrees_with_authorities before
-- this trigger, and PostgreSQL's name order agrees, so no yield is needed.
CREATE FUNCTION knowledge_list_order_key_identity_collision_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_list_order_keys
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge list order key already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_list_order_key_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_order_keys
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_order_key_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_list_order_key_update_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge list order key is immutable';
END;
$$;

CREATE TRIGGER knowledge_list_order_key_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_order_keys
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_order_key_update_is_forbidden();

-- Also fires for the ON DELETE CASCADE from knowledge_object_list_projections.
CREATE FUNCTION knowledge_list_order_key_sealed_delete_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM knowledge_object_list_projection_seals
        WHERE tenant_id = OLD.tenant_id
          AND knowledge_object_id = OLD.knowledge_object_id
          AND object_version = OLD.object_version
    ) THEN
        RAISE EXCEPTION 'sealed knowledge list order key cannot be deleted';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_list_order_key_sealed_delete_is_forbidden
BEFORE DELETE ON knowledge_object_list_order_keys
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_order_key_sealed_delete_is_forbidden();

-- ---------------------------------------------------------------------------
-- knowledge_object_list_projection_seals
-- ---------------------------------------------------------------------------

-- The acceptance condition of knowledge_list_projection_seal_is_complete: the
-- seal's byte counts equal the projection's, the projection's selector charge
-- is canonical, the selector rows add up to the projection's counts, and each
-- dimension's values are strictly increasing by byte order.
CREATE FUNCTION knowledge_list_projection_seal_complete_holds(
    candidate knowledge_object_list_projection_seals
) RETURNS boolean LANGUAGE sql STABLE
SET search_path FROM CURRENT
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM knowledge_object_list_projections AS projection
        WHERE projection.tenant_id = candidate.tenant_id
          AND projection.knowledge_object_id = candidate.knowledge_object_id
          AND projection.object_version = candidate.object_version
          AND projection.projection_bytes = candidate.projection_bytes
          AND projection.canonical_selector_bytes = candidate.canonical_selector_bytes
          AND projection.canonical_selector_bytes = CASE
              WHEN projection.state = 'quarantined' THEN 0
              ELSE 43
                  + 4 * (
                      projection.index_selector_count
                      + projection.host_selector_count
                      + projection.source_selector_count
                      + projection.sourcetype_selector_count
                  )
                  + projection.selector_value_bytes
          END
          AND EXISTS (
              SELECT 1
              FROM (
                  SELECT
                      COALESCE(SUM(CASE dimension WHEN 'index' THEN 1 ELSE 0 END), 0)
                          AS index_count,
                      COALESCE(SUM(CASE dimension WHEN 'host' THEN 1 ELSE 0 END), 0)
                          AS host_count,
                      COALESCE(SUM(CASE dimension WHEN 'source' THEN 1 ELSE 0 END), 0)
                          AS source_count,
                      COALESCE(SUM(CASE dimension WHEN 'sourcetype' THEN 1 ELSE 0 END), 0)
                          AS sourcetype_count,
                      COALESCE(SUM(value_bytes), 0) AS value_bytes
                  FROM knowledge_object_list_selector_patterns
                  WHERE tenant_id = candidate.tenant_id
                    AND knowledge_object_id = candidate.knowledge_object_id
                    AND object_version = candidate.object_version
              ) AS selector_aggregate
              WHERE selector_aggregate.index_count = projection.index_selector_count
                AND selector_aggregate.host_count = projection.host_selector_count
                AND selector_aggregate.source_count = projection.source_selector_count
                AND selector_aggregate.sourcetype_count = projection.sourcetype_selector_count
                AND selector_aggregate.value_bytes = projection.selector_value_bytes
          )
          AND NOT EXISTS (
              SELECT 1
              FROM knowledge_object_list_selector_patterns AS current_pattern
              JOIN knowledge_object_list_selector_patterns AS previous_pattern
                ON previous_pattern.tenant_id = current_pattern.tenant_id
               AND previous_pattern.knowledge_object_id = current_pattern.knowledge_object_id
               AND previous_pattern.object_version = current_pattern.object_version
               AND previous_pattern.dimension = current_pattern.dimension
               AND previous_pattern.ordinal = current_pattern.ordinal - 1
              WHERE current_pattern.tenant_id = candidate.tenant_id
                AND current_pattern.knowledge_object_id = candidate.knowledge_object_id
                AND current_pattern.object_version = candidate.object_version
                AND previous_pattern.value COLLATE "C"
                    >= current_pattern.value COLLATE "C"
          )
    );
$$;

CREATE FUNCTION knowledge_list_projection_seal_identity_collision_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- SQLite checks knowledge_list_projection_seal_requires_order_key and
    -- then knowledge_list_projection_seal_is_complete first, so a duplicate
    -- seal is reported as a duplicate only when both of those accept it.
    IF EXISTS (
        SELECT 1
        FROM knowledge_object_list_projection_seals
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) AND EXISTS (
        SELECT 1 FROM knowledge_object_list_order_keys
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) AND knowledge_list_projection_seal_complete_holds(NEW) THEN
        RAISE EXCEPTION 'knowledge list projection seal already exists';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_list_projection_seal_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_projection_seals
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_seal_identity_collision_is_forbidden();

CREATE FUNCTION knowledge_list_projection_seal_is_complete()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    -- SQLite checks knowledge_list_projection_seal_requires_order_key first,
    -- so a seal without an order key is reported as lacking one.
    IF EXISTS (
        SELECT 1 FROM knowledge_object_list_order_keys
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) AND NOT knowledge_list_projection_seal_complete_holds(NEW) THEN
        RAISE EXCEPTION 'knowledge list projection is incomplete';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_list_projection_seal_is_complete
BEFORE INSERT ON knowledge_object_list_projection_seals
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_seal_is_complete();

CREATE FUNCTION knowledge_list_projection_seal_requires_order_key()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM knowledge_object_list_order_keys
        WHERE tenant_id = NEW.tenant_id
          AND knowledge_object_id = NEW.knowledge_object_id
          AND object_version = NEW.object_version
    ) THEN
        RAISE EXCEPTION 'knowledge list projection lacks exact order key';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER knowledge_list_projection_seal_requires_order_key
BEFORE INSERT ON knowledge_object_list_projection_seals
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_seal_requires_order_key();

CREATE FUNCTION knowledge_list_projection_seal_update_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    RAISE EXCEPTION 'knowledge list projection seal is immutable';
END;
$$;

CREATE TRIGGER knowledge_list_projection_seal_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_projection_seals
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_seal_update_is_forbidden();

CREATE FUNCTION knowledge_list_projection_current_seal_delete_is_forbidden()
RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM knowledge_objects
        WHERE tenant_id = OLD.tenant_id
          AND knowledge_object_id = OLD.knowledge_object_id
          AND current_version = OLD.object_version
    ) THEN
        RAISE EXCEPTION 'current knowledge list projection seal cannot be deleted';
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER knowledge_list_projection_current_seal_delete_is_forbidden
BEFORE DELETE ON knowledge_object_list_projection_seals
FOR EACH ROW
EXECUTE FUNCTION knowledge_list_projection_current_seal_delete_is_forbidden();
