-- Durable catalog branch identity, immutable per-version lifecycle evidence,
-- and bounded/order-compatible list-query authorities.

-- A numeric revision alone cannot distinguish two databases which advanced
-- independently from the same backup.  The opaque state token is copied by an
-- exact backup and replaced inside the same transaction as every revision
-- increment, so equal revision numbers from divergent branches do not alias.
CREATE TABLE knowledge_catalog_revision_heads (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    catalog_revision INTEGER NOT NULL CHECK (
        catalog_revision BETWEEN 0 AND 9223372036854775806
    ),
    state_token BLOB NOT NULL CHECK (length(state_token) = 32),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

INSERT INTO knowledge_catalog_revision_heads (
    tenant_id, catalog_revision, state_token
)
SELECT tenant_id, catalog_revision, randomblob(32)
FROM knowledge_catalog_tenants;

CREATE TRIGGER knowledge_catalog_revision_head_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_catalog_revision_heads
WHEN EXISTS (
    SELECT 1 FROM knowledge_catalog_revision_heads
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head already exists');
END;

CREATE TRIGGER knowledge_catalog_revision_head_insert_agrees_with_tenant
BEFORE INSERT ON knowledge_catalog_revision_heads
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_catalog_tenants
    WHERE tenant_id = NEW.tenant_id
      AND catalog_revision = NEW.catalog_revision
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head disagrees with tenant');
END;

CREATE TRIGGER knowledge_catalog_revision_head_transition_is_exact
BEFORE UPDATE ON knowledge_catalog_revision_heads
WHEN NEW.tenant_id <> OLD.tenant_id
  OR NEW.catalog_revision <> OLD.catalog_revision + 1
  OR length(NEW.state_token) <> 32
  OR NEW.state_token = OLD.state_token
  OR NOT EXISTS (
      SELECT 1 FROM knowledge_catalog_tenants
      WHERE tenant_id = OLD.tenant_id
        AND catalog_revision = NEW.catalog_revision
  )
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head transition is invalid');
END;

CREATE TRIGGER knowledge_catalog_revision_head_delete_is_forbidden
BEFORE DELETE ON knowledge_catalog_revision_heads
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head cannot be deleted');
END;

CREATE TRIGGER knowledge_catalog_tenant_creates_revision_head
AFTER INSERT ON knowledge_catalog_tenants
BEGIN
    INSERT INTO knowledge_catalog_revision_heads (
        tenant_id, catalog_revision, state_token
    ) VALUES (NEW.tenant_id, NEW.catalog_revision, randomblob(32));
END;

CREATE TRIGGER knowledge_catalog_revision_requires_exact_head
BEFORE UPDATE OF catalog_revision ON knowledge_catalog_tenants
WHEN NEW.catalog_revision <> OLD.catalog_revision
 AND NOT EXISTS (
     SELECT 1 FROM knowledge_catalog_revision_heads
     WHERE tenant_id = OLD.tenant_id
       AND catalog_revision = OLD.catalog_revision
       AND length(state_token) = 32
 )
BEGIN
    SELECT RAISE(ABORT, 'knowledge catalog revision head is missing');
END;

CREATE TRIGGER knowledge_catalog_revision_rotates_state_token
AFTER UPDATE OF catalog_revision ON knowledge_catalog_tenants
WHEN NEW.catalog_revision <> OLD.catalog_revision
BEGIN
    UPDATE knowledge_catalog_revision_heads
    SET catalog_revision = NEW.catalog_revision,
        state_token = randomblob(32)
    WHERE tenant_id = OLD.tenant_id
      AND catalog_revision = OLD.catalog_revision;

    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM knowledge_catalog_revision_heads
        WHERE tenant_id = NEW.tenant_id
          AND catalog_revision = NEW.catalog_revision
          AND length(state_token) = 32
    ) THEN RAISE(ABORT, 'knowledge catalog revision head rotation failed') END;
END;

-- Version rows intentionally retain only mutation facts.  This companion
-- captures the lifecycle markers that were effective at each version without
-- changing any existing INSERT shape or weakening version immutability.
CREATE TABLE knowledge_object_version_lifecycle (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    state TEXT NOT NULL COLLATE BINARY CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    disabled_at_unix_micro INTEGER CHECK (
        disabled_at_unix_micro IS NULL
        OR disabled_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    quarantined_at_unix_micro INTEGER CHECK (
        quarantined_at_unix_micro IS NULL
        OR quarantined_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    deleted_at_unix_micro INTEGER CHECK (
        deleted_at_unix_micro IS NULL
        OR deleted_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    quarantine_reason TEXT COLLATE BINARY CHECK (
        quarantine_reason IS NULL
        OR quarantine_reason IN ('root_corruption', 'dependency_recovery')
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    CONSTRAINT knowledge_object_version_lifecycle_shape_is_exact CHECK (
        (
            state IN ('draft', 'active')
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'disabled'
            AND disabled_at_unix_micro IS NOT NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NULL
        )
        OR (
            state = 'quarantined'
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NOT NULL
            AND deleted_at_unix_micro IS NULL
            AND quarantine_reason IS NOT NULL
        )
        OR (
            state = 'deleted'
            AND disabled_at_unix_micro IS NULL
            AND quarantined_at_unix_micro IS NULL
            AND deleted_at_unix_micro IS NOT NULL
            AND quarantine_reason IS NULL
        )
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_versions (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

-- Reject any retained history that the runtime chronology validator would
-- reject. Migration 0024 constrained each row's local shape, but it did not
-- constrain transitions between otherwise-valid immutable rows. Upgrading one
-- of those histories and recording migration 0029 would make its object
-- unreadable with no downgrade path, so the complete upgrade must roll back.
CREATE TABLE knowledge_catalog_0029_lifecycle_upgrade_guard (
    invalid INTEGER NOT NULL CHECK (invalid = 0)
) STRICT;

-- Foreign-key enforcement can have been disabled by an older process or an
-- interrupted manual repair. Reject orphan physical authorities before any
-- backfill: tenant-driven checks and chronology joins would otherwise ignore
-- them while the lifecycle INSERT still copied every version row.
INSERT INTO knowledge_catalog_0029_lifecycle_upgrade_guard (invalid)
SELECT 1
FROM knowledge_objects AS object
WHERE NOT EXISTS (
    SELECT 1
    FROM knowledge_catalog_tenants AS tenant
    WHERE tenant.tenant_id = object.tenant_id
)
LIMIT 1;

INSERT INTO knowledge_catalog_0029_lifecycle_upgrade_guard (invalid)
SELECT 1
FROM knowledge_object_versions AS version
WHERE NOT EXISTS (
    SELECT 1
    FROM knowledge_objects AS object
    WHERE object.tenant_id = version.tenant_id
      AND object.knowledge_object_id = version.knowledge_object_id
)
LIMIT 1;

-- Stop after the first row beyond the frozen tenant version ceiling. This
-- prevents a corrupt physical history from forcing the windowed chronology
-- audit to process an unbounded number of rows before rejecting the upgrade.
INSERT INTO knowledge_catalog_0029_lifecycle_upgrade_guard (invalid)
SELECT 1
FROM knowledge_catalog_tenants AS tenant
WHERE (
    SELECT count(*)
    FROM (
        SELECT 1
        FROM knowledge_object_versions AS bounded_version
        WHERE bounded_version.tenant_id = tenant.tenant_id
        LIMIT 65537
    ) AS bounded_versions
) > 65536
LIMIT 1;

WITH history AS (
    SELECT
        version.tenant_id,
        version.knowledge_object_id,
        version.object_version,
        object.current_version,
        version.state,
        version.mutation_kind,
        version.created_at_unix_micro,
        lag(version.state) OVER (
            PARTITION BY version.tenant_id, version.knowledge_object_id
            ORDER BY version.object_version
        ) AS previous_state,
        lag(version.created_at_unix_micro) OVER (
            PARTITION BY version.tenant_id, version.knowledge_object_id
            ORDER BY version.object_version
        ) AS previous_timestamp
    FROM knowledge_object_versions AS version
    JOIN knowledge_objects AS object
      ON object.tenant_id = version.tenant_id
     AND object.knowledge_object_id = version.knowledge_object_id
), history_summary AS (
    SELECT
        history.tenant_id,
        history.knowledge_object_id,
        count(*) AS version_count,
        min(history.object_version) AS minimum_version,
        max(history.object_version) AS maximum_version,
        max(CASE WHEN history.object_version = 1
            THEN history.created_at_unix_micro END) AS creation_timestamp,
        max(CASE WHEN history.object_version = history.current_version
            THEN history.created_at_unix_micro END) AS current_timestamp,
        max(CASE WHEN history.mutation_kind = 'disable'
            THEN history.created_at_unix_micro END) AS latest_disable_timestamp,
        sum(CASE WHEN
            (history.previous_timestamp IS NOT NULL
                AND history.created_at_unix_micro < history.previous_timestamp)
            OR (history.object_version < history.current_version
                AND history.state IN ('quarantined', 'deleted'))
            OR (history.object_version = 1 AND NOT (
                history.mutation_kind = 'create'
                AND history.state IN ('draft', 'active')
            ))
            OR (history.object_version > 1 AND NOT (
                (history.mutation_kind IN ('update', 'scope_change')
                    AND history.state = history.previous_state
                    AND history.state IN ('draft', 'active', 'disabled'))
                OR (history.mutation_kind = 'enable'
                    AND history.state = 'active'
                    AND history.previous_state IN ('draft', 'disabled'))
                OR (history.mutation_kind = 'disable'
                    AND history.state = 'disabled'
                    AND history.previous_state IN ('draft', 'active'))
                OR (history.mutation_kind = 'quarantine'
                    AND history.state = 'quarantined'
                    AND history.previous_state IN ('draft', 'active', 'disabled'))
                OR (history.mutation_kind = 'delete'
                    AND history.state = 'deleted'
                    AND history.previous_state IN ('draft', 'active', 'disabled'))
            ))
            THEN 1 ELSE 0 END) AS invalid_transition_count
    FROM history
    GROUP BY history.tenant_id, history.knowledge_object_id
)
INSERT INTO knowledge_catalog_0029_lifecycle_upgrade_guard (invalid)
SELECT 1
FROM knowledge_objects AS object
LEFT JOIN history_summary AS summary
  ON summary.tenant_id = object.tenant_id
 AND summary.knowledge_object_id = object.knowledge_object_id
LEFT JOIN knowledge_object_versions AS current_version
  ON current_version.tenant_id = object.tenant_id
 AND current_version.knowledge_object_id = object.knowledge_object_id
 AND current_version.object_version = object.current_version
WHERE summary.knowledge_object_id IS NULL
   OR object.current_version > 65536
   OR summary.version_count <> object.current_version
   OR summary.minimum_version <> 1
   OR summary.maximum_version <> object.current_version
   OR summary.creation_timestamp <> object.created_at_unix_micro
   OR summary.current_timestamp <> object.updated_at_unix_micro
   OR summary.invalid_transition_count <> 0
   OR current_version.knowledge_object_id IS NULL
   OR current_version.app_id <> object.app_id
   OR current_version.owner_id <> object.owner_id
   OR current_version.object_type <> object.object_type
   OR current_version.name <> object.name
   OR current_version.sharing_scope <> object.sharing_scope
   OR current_version.state <> object.state
   OR current_version.definition_digest IS NOT object.definition_digest
   OR current_version.quarantine_reason IS NOT object.quarantine_reason
   OR (
       object.state = 'disabled'
       AND summary.latest_disable_timestamp IS NOT object.disabled_at_unix_micro
   )
   OR (
       object.state = 'quarantined'
       AND object.quarantined_at_unix_micro <> object.updated_at_unix_micro
   )
   OR (
       object.state = 'deleted'
       AND object.deleted_at_unix_micro <> object.updated_at_unix_micro
   )
LIMIT 1;

DROP TABLE knowledge_catalog_0029_lifecycle_upgrade_guard;

-- The chronology preflight above proves timestamps are nondecreasing and that
-- every disabled run begins with a disable mutation.  Carry the latest disable
-- timestamp through each object history once, rather than searching the
-- retained prefix independently for every disabled version.
WITH lifecycle_source AS (
    SELECT
        version.tenant_id,
        version.knowledge_object_id,
        version.object_version,
        version.state,
        version.created_at_unix_micro,
        version.quarantine_reason,
        max(CASE WHEN version.mutation_kind = 'disable'
            THEN version.created_at_unix_micro END) OVER (
            PARTITION BY version.tenant_id, version.knowledge_object_id
            ORDER BY version.object_version
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS effective_disabled_at_unix_micro
    FROM knowledge_object_versions AS version
)
INSERT INTO knowledge_object_version_lifecycle (
    tenant_id, knowledge_object_id, object_version,
    state,
    disabled_at_unix_micro, quarantined_at_unix_micro,
    deleted_at_unix_micro, quarantine_reason
)
SELECT
    lifecycle_source.tenant_id,
    lifecycle_source.knowledge_object_id,
    lifecycle_source.object_version,
    lifecycle_source.state,
    CASE WHEN lifecycle_source.state = 'disabled'
         THEN lifecycle_source.effective_disabled_at_unix_micro END,
    CASE WHEN lifecycle_source.state = 'quarantined'
         THEN lifecycle_source.created_at_unix_micro END,
    CASE WHEN lifecycle_source.state = 'deleted'
         THEN lifecycle_source.created_at_unix_micro END,
    CASE WHEN lifecycle_source.state = 'quarantined'
         THEN lifecycle_source.quarantine_reason END
FROM lifecycle_source;

CREATE TRIGGER knowledge_object_version_lifecycle_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_version_lifecycle
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_version_lifecycle
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version lifecycle already exists');
END;

CREATE TRIGGER knowledge_object_version_lifecycle_agrees_with_version
BEFORE INSERT ON knowledge_object_version_lifecycle
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_versions AS version
    WHERE version.tenant_id = NEW.tenant_id
      AND version.knowledge_object_id = NEW.knowledge_object_id
      AND version.object_version = NEW.object_version
      AND NEW.state = version.state
      AND (
          (
              version.state IN ('draft', 'active')
              AND NEW.disabled_at_unix_micro IS NULL
              AND NEW.quarantined_at_unix_micro IS NULL
              AND NEW.deleted_at_unix_micro IS NULL
              AND NEW.quarantine_reason IS NULL
          )
          OR (
              version.state = 'disabled'
              AND NEW.disabled_at_unix_micro IS NOT NULL
              AND NEW.disabled_at_unix_micro <= version.created_at_unix_micro
              AND NEW.disabled_at_unix_micro = (
                  SELECT disabled_version.created_at_unix_micro
                  FROM knowledge_object_versions AS disabled_version
                  WHERE disabled_version.tenant_id = version.tenant_id
                    AND disabled_version.knowledge_object_id = version.knowledge_object_id
                    AND disabled_version.object_version <= version.object_version
                    AND disabled_version.mutation_kind = 'disable'
                  ORDER BY disabled_version.object_version DESC
                  LIMIT 1
              )
              AND NEW.quarantined_at_unix_micro IS NULL
              AND NEW.deleted_at_unix_micro IS NULL
              AND NEW.quarantine_reason IS NULL
          )
          OR (
              version.state = 'quarantined'
              AND NEW.disabled_at_unix_micro IS NULL
              AND NEW.quarantined_at_unix_micro = version.created_at_unix_micro
              AND NEW.deleted_at_unix_micro IS NULL
              AND NEW.quarantine_reason = version.quarantine_reason
          )
          OR (
              version.state = 'deleted'
              AND NEW.disabled_at_unix_micro IS NULL
              AND NEW.quarantined_at_unix_micro IS NULL
              AND NEW.deleted_at_unix_micro = version.created_at_unix_micro
              AND NEW.quarantine_reason IS NULL
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version lifecycle disagrees with version');
END;

CREATE TRIGGER knowledge_object_version_lifecycle_update_is_forbidden
BEFORE UPDATE ON knowledge_object_version_lifecycle
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version lifecycle is immutable');
END;

CREATE TRIGGER knowledge_object_version_lifecycle_delete_is_forbidden
BEFORE DELETE ON knowledge_object_version_lifecycle
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version lifecycle cannot be deleted');
END;

-- Keep every new immutable history readable by the same transition matrix
-- used by migration backfill and runtime validation. The earlier schema
-- constrained each row independently but did not relate it to version N-1.
CREATE TRIGGER knowledge_object_version_transition_is_exact
BEFORE INSERT ON knowledge_object_versions
WHEN NEW.object_version > 65536
  OR (
      NEW.object_version = 1
      AND NOT (
          NEW.mutation_kind = 'create'
          AND NEW.state IN ('draft', 'active')
      )
  )
  OR (
      NEW.object_version > 1
      AND NOT EXISTS (
          SELECT 1
          FROM knowledge_object_versions AS previous
          WHERE previous.tenant_id = NEW.tenant_id
            AND previous.knowledge_object_id = NEW.knowledge_object_id
            AND previous.object_version = NEW.object_version - 1
            AND NEW.created_at_unix_micro >= previous.created_at_unix_micro
            AND (
                (
                    NEW.mutation_kind IN ('update', 'scope_change')
                    AND NEW.state = previous.state
                    AND NEW.state IN ('draft', 'active', 'disabled')
                )
                OR (
                    NEW.mutation_kind = 'enable'
                    AND NEW.state = 'active'
                    AND previous.state IN ('draft', 'disabled')
                )
                OR (
                    NEW.mutation_kind = 'disable'
                    AND NEW.state = 'disabled'
                    AND previous.state IN ('draft', 'active')
                )
                OR (
                    NEW.mutation_kind = 'quarantine'
                    AND NEW.state = 'quarantined'
                    AND previous.state IN ('draft', 'active', 'disabled')
                )
                OR (
                    NEW.mutation_kind = 'delete'
                    AND NEW.state = 'deleted'
                    AND previous.state IN ('draft', 'active', 'disabled')
                )
            )
      )
  )
BEGIN
    SELECT RAISE(ABORT, 'knowledge object version transition is invalid');
END;

CREATE TRIGGER knowledge_object_version_creates_lifecycle
AFTER INSERT ON knowledge_object_versions
BEGIN
    INSERT INTO knowledge_object_version_lifecycle (
        tenant_id, knowledge_object_id, object_version,
        state,
        disabled_at_unix_micro, quarantined_at_unix_micro,
        deleted_at_unix_micro, quarantine_reason
    ) VALUES (
        NEW.tenant_id,
        NEW.knowledge_object_id,
        NEW.object_version,
        NEW.state,
        CASE
            WHEN NEW.state = 'disabled' AND NEW.mutation_kind = 'disable'
                THEN NEW.created_at_unix_micro
            WHEN NEW.state = 'disabled' THEN (
                SELECT disabled_at_unix_micro
                FROM knowledge_object_version_lifecycle
                WHERE tenant_id = NEW.tenant_id
                  AND knowledge_object_id = NEW.knowledge_object_id
                  AND object_version = NEW.object_version - 1
            )
        END,
        CASE WHEN NEW.state = 'quarantined'
             THEN NEW.created_at_unix_micro END,
        CASE WHEN NEW.state = 'deleted'
             THEN NEW.created_at_unix_micro END,
        CASE WHEN NEW.state = 'quarantined'
             THEN NEW.quarantine_reason END
    );
END;

-- Created/updated sort keys live beside the immutable current projection.
-- List authorization is driven by bounded authorized registry branches, and
-- each registry identity must join its exact current projection before it can
-- enter the driver.  Stale, orphaned, and scope-escalated projections cannot
-- consume that bound.  Keys are derived only from the projected immutable
-- version and immutable version one.
CREATE TABLE knowledge_object_list_order_keys (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    created_at_unix_micro INTEGER NOT NULL CHECK (
        created_at_unix_micro BETWEEN 1 AND 253402300799999999
    ),
    updated_at_unix_micro INTEGER NOT NULL CHECK (
        updated_at_unix_micro BETWEEN created_at_unix_micro AND 253402300799999999
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_list_projections (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_catalog_0029_order_upgrade_guard (
    invalid INTEGER NOT NULL CHECK (invalid = 0)
) STRICT;

INSERT INTO knowledge_catalog_0029_order_upgrade_guard (invalid)
SELECT 1
WHERE EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections AS projection
    LEFT JOIN knowledge_objects AS object
      ON object.tenant_id = projection.tenant_id
     AND object.knowledge_object_id = projection.knowledge_object_id
     AND object.current_version = projection.object_version
     AND object.app_id = projection.app_id
     AND object.owner_id = projection.owner_id
     AND object.object_type = projection.object_type
     AND object.name = projection.name
     AND object.sharing_scope = projection.sharing_scope
     AND object.state = projection.state
    LEFT JOIN knowledge_object_versions AS version
      ON version.tenant_id = projection.tenant_id
     AND version.knowledge_object_id = projection.knowledge_object_id
     AND version.object_version = projection.object_version
    LEFT JOIN knowledge_object_versions AS creation_version
      ON creation_version.tenant_id = projection.tenant_id
     AND creation_version.knowledge_object_id = projection.knowledge_object_id
     AND creation_version.object_version = 1
    WHERE object.knowledge_object_id IS NULL
       OR version.knowledge_object_id IS NULL
       OR creation_version.knowledge_object_id IS NULL
       OR object.created_at_unix_micro <> creation_version.created_at_unix_micro
       OR object.updated_at_unix_micro <> version.created_at_unix_micro
);

DROP TABLE knowledge_catalog_0029_order_upgrade_guard;

INSERT INTO knowledge_object_list_order_keys (
    tenant_id, knowledge_object_id, object_version,
    created_at_unix_micro, updated_at_unix_micro
)
SELECT projection.tenant_id,
       projection.knowledge_object_id,
       projection.object_version,
       creation_version.created_at_unix_micro,
       version.created_at_unix_micro
FROM knowledge_object_list_projections AS projection
JOIN knowledge_object_versions AS version
  ON version.tenant_id = projection.tenant_id
 AND version.knowledge_object_id = projection.knowledge_object_id
 AND version.object_version = projection.object_version
JOIN knowledge_object_versions AS creation_version
  ON creation_version.tenant_id = projection.tenant_id
 AND creation_version.knowledge_object_id = projection.knowledge_object_id
 AND creation_version.object_version = 1;

CREATE INDEX knowledge_list_order_created_idx
    ON knowledge_object_list_order_keys (
        tenant_id, created_at_unix_micro,
        knowledge_object_id, object_version
    );

CREATE INDEX knowledge_list_order_updated_idx
    ON knowledge_object_list_order_keys (
        tenant_id, updated_at_unix_micro,
        knowledge_object_id, object_version
    );

CREATE INDEX knowledge_list_projection_object_type_order_idx
    ON knowledge_object_list_projections (
        tenant_id, object_type, name COLLATE BINARY,
        knowledge_object_id, object_version
    );

-- Each authorization disjunct has a leading, covering access path.  Runtime
-- builds the authorized identity set from these mutually exclusive branches
-- before applying the ordered projection driver.
CREATE INDEX knowledge_objects_authorized_global_idx
    ON knowledge_objects (tenant_id, knowledge_object_id)
    WHERE sharing_scope = 'global';

CREATE INDEX knowledge_objects_authorized_app_idx
    ON knowledge_objects (tenant_id, app_id, knowledge_object_id)
    WHERE sharing_scope = 'app';

CREATE INDEX knowledge_objects_authorized_private_idx
    ON knowledge_objects (
        tenant_id, owner_id, app_id, knowledge_object_id
    ) WHERE sharing_scope = 'private';

CREATE INDEX knowledge_list_projection_authorized_global_idx
    ON knowledge_object_list_projections (
        tenant_id, knowledge_object_id, object_version
    ) WHERE sharing_scope = 'global';

CREATE INDEX knowledge_list_projection_authorized_app_idx
    ON knowledge_object_list_projections (
        tenant_id, app_id, knowledge_object_id, object_version
    ) WHERE sharing_scope = 'app';

CREATE INDEX knowledge_list_projection_authorized_private_idx
    ON knowledge_object_list_projections (
        tenant_id, owner_id, app_id, knowledge_object_id, object_version
    ) WHERE sharing_scope = 'private';

CREATE TRIGGER knowledge_list_order_key_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_order_keys
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_order_keys
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list order key already exists');
END;

CREATE TRIGGER knowledge_list_order_key_agrees_with_authorities
BEFORE INSERT ON knowledge_object_list_order_keys
WHEN NOT EXISTS (
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
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list order key disagrees with authorities');
END;

CREATE TRIGGER knowledge_list_order_key_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_order_keys
BEGIN
    SELECT RAISE(ABORT, 'knowledge list order key is immutable');
END;

CREATE TRIGGER knowledge_list_order_key_sealed_delete_is_forbidden
BEFORE DELETE ON knowledge_object_list_order_keys
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projection_seals
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'sealed knowledge list order key cannot be deleted');
END;

CREATE TRIGGER knowledge_list_projection_creates_order_key
AFTER INSERT ON knowledge_object_list_projections
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_versions
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    INSERT INTO knowledge_object_list_order_keys (
        tenant_id, knowledge_object_id, object_version,
        created_at_unix_micro, updated_at_unix_micro
    )
    SELECT NEW.tenant_id,
           NEW.knowledge_object_id,
           NEW.object_version,
           creation_version.created_at_unix_micro,
           version.created_at_unix_micro
    FROM knowledge_object_versions AS version
    JOIN knowledge_object_versions AS creation_version
      ON creation_version.tenant_id = version.tenant_id
     AND creation_version.knowledge_object_id = version.knowledge_object_id
     AND creation_version.object_version = 1
    WHERE version.tenant_id = NEW.tenant_id
      AND version.knowledge_object_id = NEW.knowledge_object_id
      AND version.object_version = NEW.object_version;
END;

CREATE TRIGGER knowledge_object_version_creates_staged_order_key
AFTER INSERT ON knowledge_object_versions
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projections
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
 AND NOT EXISTS (
    SELECT 1 FROM knowledge_object_list_order_keys
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    INSERT INTO knowledge_object_list_order_keys (
        tenant_id, knowledge_object_id, object_version,
        created_at_unix_micro, updated_at_unix_micro
    )
    SELECT NEW.tenant_id,
           NEW.knowledge_object_id,
           NEW.object_version,
           creation_version.created_at_unix_micro,
           NEW.created_at_unix_micro
    FROM knowledge_object_versions AS creation_version
    WHERE creation_version.tenant_id = NEW.tenant_id
      AND creation_version.knowledge_object_id = NEW.knowledge_object_id
      AND creation_version.object_version = 1;
END;

CREATE TRIGGER knowledge_list_projection_seal_requires_order_key
BEFORE INSERT ON knowledge_object_list_projection_seals
WHEN NOT EXISTS (
    SELECT 1 FROM knowledge_object_list_order_keys
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection lacks exact order key');
END;
