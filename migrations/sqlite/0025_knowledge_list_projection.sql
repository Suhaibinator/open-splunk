-- Bounded, current-version list projections for knowledge management queries.
-- Duplicated registry columns are trusted linkage/filter fields needed before
-- LIMIT; definition-derived payload is only description and selector patterns.
-- A projection becomes listable only after its exact selector set is sealed.

CREATE UNIQUE INDEX knowledge_objects_current_projection_identity_idx
    ON knowledge_objects (
        tenant_id, knowledge_object_id, current_version,
        app_id, owner_id, object_type, name, sharing_scope, state
    );

CREATE TABLE knowledge_projection_tenant_ledgers (
    tenant_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    projection_bytes INTEGER NOT NULL DEFAULT 0 CHECK (
        projection_bytes BETWEEN 0 AND 268435456
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_catalog_tenants (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_object_list_projections (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    app_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    object_type TEXT NOT NULL COLLATE BINARY CHECK (
        object_type IN ('field_extraction', 'field_alias', 'calculated_field')
    ),
    name TEXT NOT NULL COLLATE BINARY,
    sharing_scope TEXT NOT NULL COLLATE BINARY CHECK (
        sharing_scope IN ('private', 'app', 'global')
    ),
    state TEXT NOT NULL COLLATE BINARY CHECK (
        state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
    ),
    description_present INTEGER NOT NULL CHECK (
        description_present IN (0, 1)
    ),
    description TEXT NOT NULL COLLATE BINARY DEFAULT '',
    index_selector_count INTEGER NOT NULL CHECK (
        index_selector_count BETWEEN 0 AND 16
    ),
    host_selector_count INTEGER NOT NULL CHECK (
        host_selector_count BETWEEN 0 AND 16
    ),
    source_selector_count INTEGER NOT NULL CHECK (
        source_selector_count BETWEEN 0 AND 16
    ),
    sourcetype_selector_count INTEGER NOT NULL CHECK (
        sourcetype_selector_count BETWEEN 0 AND 16
    ),
    selector_value_bytes INTEGER NOT NULL CHECK (
        selector_value_bytes BETWEEN 0 AND 8192
    ),
    canonical_selector_bytes INTEGER NOT NULL CHECK (
        canonical_selector_bytes BETWEEN 0 AND 8192
        AND selector_value_bytes <= canonical_selector_bytes
    ),
    -- Normative accounted bytes are exactly description bytes plus selector
    -- value bytes. Registry identity columns are already charged by KO-0.
    projection_bytes INTEGER GENERATED ALWAYS AS (
        length(CAST(description AS BLOB))
        + selector_value_bytes
    ) STORED,
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    CONSTRAINT knowledge_list_projection_description_canonical CHECK (
        length(CAST(description AS BLOB)) <= 16384
        AND instr(CAST(description AS BLOB), X'00') = 0
        AND (
            (description_present = 0 AND description = '')
            OR (
                description_present = 1
                AND length(CAST(description AS BLOB)) >= 1
                AND description = trim(description)
                AND description NOT GLOB (
                    '*[' || char(1) || '-' || char(31)
                    || char(127) || '-' || char(159) || ']*'
                )
            )
        )
    ),
    CONSTRAINT knowledge_list_projection_selector_count_bounded CHECK (
        index_selector_count
        + host_selector_count
        + source_selector_count
        + sourcetype_selector_count <= 64
    ),
    -- The canonical selector encoding has 46 bytes of domain/dimension
    -- framing, four bytes of framing per pattern, and the exact UTF-8 value
    -- bytes. Quarantined definitions are never decoded and have no encoding.
    CONSTRAINT knowledge_list_projection_selector_charge_exact CHECK (
        (
            state = 'quarantined'
            AND canonical_selector_bytes = 0
        )
        OR (
            state <> 'quarantined'
            AND canonical_selector_bytes = 46
                + 4 * (
                    index_selector_count
                    + host_selector_count
                    + source_selector_count
                    + sourcetype_selector_count
                )
                + selector_value_bytes
        )
    ),
    CONSTRAINT knowledge_list_projection_quarantine_is_bodyless CHECK (
        state <> 'quarantined'
        OR (
            description_present = 0
            AND index_selector_count = 0
            AND host_selector_count = 0
            AND source_selector_count = 0
            AND sourcetype_selector_count = 0
            AND selector_value_bytes = 0
            AND canonical_selector_bytes = 0
        )
    ),
    FOREIGN KEY (tenant_id) REFERENCES knowledge_projection_tenant_ledgers (tenant_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    FOREIGN KEY (
        tenant_id, knowledge_object_id, object_version,
        app_id, owner_id, object_type, name, sharing_scope, state
    ) REFERENCES knowledge_objects (
        tenant_id, knowledge_object_id, current_version,
        app_id, owner_id, object_type, name, sharing_scope, state
    ) ON UPDATE NO ACTION ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_object_list_selector_patterns (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    dimension TEXT NOT NULL COLLATE BINARY CHECK (
        dimension IN ('index', 'host', 'source', 'sourcetype')
    ),
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 15),
    match_kind TEXT NOT NULL COLLATE BINARY CHECK (
        match_kind IN ('exact', 'wildcard')
    ),
    value TEXT NOT NULL COLLATE BINARY,
    value_bytes INTEGER GENERATED ALWAYS AS (
        length(CAST(value AS BLOB))
    ) STORED,
    PRIMARY KEY (
        tenant_id, knowledge_object_id, object_version, dimension, ordinal
    ),
    UNIQUE (
        tenant_id, knowledge_object_id, object_version, dimension, value
    ),
    CHECK (value_bytes BETWEEN 1 AND 255),
    CHECK (instr(CAST(value AS BLOB), X'00') = 0),
    CHECK (
        value = trim(value)
        AND value NOT GLOB (
            '*[' || char(1) || '-' || char(31)
            || char(127) || '-' || char(159) || ']*'
        )
    ),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_list_projections (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE knowledge_object_list_projection_seals (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    knowledge_object_id TEXT NOT NULL COLLATE BINARY,
    object_version INTEGER NOT NULL CHECK (object_version >= 1),
    projection_bytes INTEGER NOT NULL CHECK (
        projection_bytes BETWEEN 0 AND 268435456
    ),
    canonical_selector_bytes INTEGER NOT NULL CHECK (
        canonical_selector_bytes BETWEEN 0 AND 8192
    ),
    PRIMARY KEY (tenant_id, knowledge_object_id, object_version),
    FOREIGN KEY (tenant_id, knowledge_object_id, object_version)
        REFERENCES knowledge_object_list_projections (
            tenant_id, knowledge_object_id, object_version
        ) ON UPDATE RESTRICT ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

-- KO management routes were not available before this migration. Refuse an
-- ambiguous upgrade containing manually injected/current KO rows rather than
-- silently leaving a catalog that list queries could omit after LIMIT.
CREATE TABLE knowledge_projection_upgrade_guard (
    singleton INTEGER NOT NULL CHECK (singleton = 0)
) STRICT;

CREATE TRIGGER knowledge_projection_upgrade_requires_empty_catalog
BEFORE INSERT ON knowledge_projection_upgrade_guard
BEGIN
    SELECT RAISE(ABORT, 'knowledge projection upgrade requires empty catalog');
END;

INSERT INTO knowledge_projection_upgrade_guard (singleton)
SELECT 1 FROM knowledge_objects LIMIT 1;

DROP TRIGGER knowledge_projection_upgrade_requires_empty_catalog;
DROP TABLE knowledge_projection_upgrade_guard;

-- Tenant-bounded indexes preserve binary predicate and ordering inputs.
-- Arbitrary contains matching still scans the tenant's bounded current
-- projection set; correctness never applies text filters after LIMIT.
CREATE INDEX knowledge_list_projection_filter_idx
    ON knowledge_object_list_projections (
        tenant_id, state, object_type, sharing_scope, app_id, owner_id,
        name COLLATE BINARY, knowledge_object_id, object_version
    );

CREATE INDEX knowledge_list_projection_name_idx
    ON knowledge_object_list_projections (
        tenant_id, name COLLATE BINARY, knowledge_object_id, object_version
    );

CREATE INDEX knowledge_list_projection_description_idx
    ON knowledge_object_list_projections (
        tenant_id, description COLLATE BINARY, knowledge_object_id, object_version
    ) WHERE description_present = 1;

CREATE INDEX knowledge_list_selector_value_idx
    ON knowledge_object_list_selector_patterns (
        tenant_id, dimension, match_kind, value COLLATE BINARY,
        knowledge_object_id, object_version, ordinal
    );

CREATE TRIGGER knowledge_projection_ledger_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_projection_tenant_ledgers
WHEN EXISTS (
    SELECT 1 FROM knowledge_projection_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge projection tenant ledger already exists');
END;

CREATE TRIGGER knowledge_projection_ledger_initial_shape_is_valid
BEFORE INSERT ON knowledge_projection_tenant_ledgers
WHEN NEW.projection_bytes <> 0
 OR EXISTS (
    SELECT 1 FROM knowledge_object_list_projections
    WHERE tenant_id = NEW.tenant_id
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge projection tenant ledger must begin empty');
END;

CREATE TRIGGER knowledge_projection_ledger_transition_is_exact
BEFORE UPDATE ON knowledge_projection_tenant_ledgers
WHEN NEW.tenant_id <> OLD.tenant_id
 OR NEW.projection_bytes <> COALESCE((
    SELECT sum(projection_bytes)
    FROM knowledge_object_list_projections
    WHERE tenant_id = OLD.tenant_id
), 0)
BEGIN
    SELECT RAISE(ABORT, 'knowledge projection byte ledger transition is invalid');
END;

CREATE TRIGGER knowledge_projection_ledger_delete_is_forbidden
BEFORE DELETE ON knowledge_projection_tenant_ledgers
BEGIN
    SELECT RAISE(ABORT, 'knowledge projection tenant ledger cannot be deleted');
END;

-- The 256 MiB ceiling charges both old and staged replacement projections,
-- leaving a bounded, explicit staging envelope rather than hidden temporary
-- growth. Driver-specific migration benchmarks plus concurrent publication and
-- deadline tests remain a pre-route activation gate; this schema slice cannot
-- substitute for that executable evidence.
CREATE TRIGGER knowledge_list_projection_capacity_is_available
BEFORE INSERT ON knowledge_object_list_projections
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_projection_tenant_ledgers
    WHERE tenant_id = NEW.tenant_id
      AND projection_bytes <= 268435456 - NEW.projection_bytes
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection byte capacity exhausted');
END;

CREATE TRIGGER knowledge_list_projection_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_projections
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection identity already exists');
END;

CREATE TRIGGER knowledge_list_projection_after_insert
AFTER INSERT ON knowledge_object_list_projections
BEGIN
    UPDATE knowledge_projection_tenant_ledgers
    SET projection_bytes = projection_bytes + NEW.projection_bytes
    WHERE tenant_id = NEW.tenant_id;
END;

CREATE TRIGGER knowledge_list_projection_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_projections
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection is immutable');
END;

CREATE TRIGGER knowledge_list_projection_delete_requires_unsealed_empty_row
BEFORE DELETE ON knowledge_object_list_projections
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projection_seals
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.object_version
)
 OR EXISTS (
    SELECT 1 FROM knowledge_object_list_selector_patterns
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection must be unsealed and empty before deletion');
END;

CREATE TRIGGER knowledge_list_projection_after_delete
AFTER DELETE ON knowledge_object_list_projections
BEGIN
    UPDATE knowledge_projection_tenant_ledgers
    SET projection_bytes = projection_bytes - OLD.projection_bytes
    WHERE tenant_id = OLD.tenant_id;
END;

CREATE TRIGGER knowledge_list_selector_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_selector_patterns
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_list_selector_patterns
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
      AND dimension = NEW.dimension
      AND (ordinal = NEW.ordinal OR value = NEW.value)
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector identity already exists');
END;

CREATE TRIGGER knowledge_list_selector_ordinal_is_declared
BEFORE INSERT ON knowledge_object_list_selector_patterns
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
      AND NEW.ordinal < CASE NEW.dimension
          WHEN 'index' THEN index_selector_count
          WHEN 'host' THEN host_selector_count
          WHEN 'source' THEN source_selector_count
          WHEN 'sourcetype' THEN sourcetype_selector_count
      END
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector ordinal exceeds declared count');
END;

CREATE TRIGGER knowledge_list_selector_sealed_projection_is_immutable_insert
BEFORE INSERT ON knowledge_object_list_selector_patterns
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projection_seals
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector set is sealed');
END;

CREATE TRIGGER knowledge_list_selector_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_selector_patterns
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector is immutable');
END;

CREATE TRIGGER knowledge_list_selector_sealed_projection_is_immutable_delete
BEFORE DELETE ON knowledge_object_list_selector_patterns
WHEN EXISTS (
    SELECT 1 FROM knowledge_object_list_projection_seals
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list selector set is sealed');
END;

CREATE TRIGGER knowledge_list_projection_seal_identity_collision_is_forbidden
BEFORE INSERT ON knowledge_object_list_projection_seals
WHEN EXISTS (
    SELECT 1
    FROM knowledge_object_list_projection_seals
    WHERE tenant_id = NEW.tenant_id
      AND knowledge_object_id = NEW.knowledge_object_id
      AND object_version = NEW.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection seal already exists');
END;

CREATE TRIGGER knowledge_list_projection_seal_is_complete
BEFORE INSERT ON knowledge_object_list_projection_seals
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections AS projection
    WHERE projection.tenant_id = NEW.tenant_id
      AND projection.knowledge_object_id = NEW.knowledge_object_id
      AND projection.object_version = NEW.object_version
      AND projection.projection_bytes = NEW.projection_bytes
      AND projection.canonical_selector_bytes = NEW.canonical_selector_bytes
      AND projection.canonical_selector_bytes = CASE
          WHEN projection.state = 'quarantined' THEN 0
          ELSE 46
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
              WHERE tenant_id = NEW.tenant_id
                AND knowledge_object_id = NEW.knowledge_object_id
                AND object_version = NEW.object_version
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
          WHERE current_pattern.tenant_id = NEW.tenant_id
            AND current_pattern.knowledge_object_id = NEW.knowledge_object_id
            AND current_pattern.object_version = NEW.object_version
            AND CAST(previous_pattern.value AS BLOB)
                >= CAST(current_pattern.value AS BLOB)
      )
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection is incomplete');
END;

CREATE TRIGGER knowledge_list_projection_seal_update_is_forbidden
BEFORE UPDATE ON knowledge_object_list_projection_seals
BEGIN
    SELECT RAISE(ABORT, 'knowledge list projection seal is immutable');
END;

CREATE TRIGGER knowledge_list_projection_current_seal_delete_is_forbidden
BEFORE DELETE ON knowledge_object_list_projection_seals
WHEN EXISTS (
    SELECT 1
    FROM knowledge_objects
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND current_version = OLD.object_version
)
BEGIN
    SELECT RAISE(ABORT, 'current knowledge list projection seal cannot be deleted');
END;

-- The reverse publication guard is intentionally statement-ordered: stage the
-- exact projection and seal first, then insert/update the registry row last.
-- The projection's FK back to the registry is deferred, so both sides still
-- commit atomically and neither can commit alone.
CREATE TRIGGER knowledge_object_insert_requires_sealed_list_projection
BEFORE INSERT ON knowledge_objects
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections AS projection
    JOIN knowledge_object_list_projection_seals AS seal
      ON seal.tenant_id = projection.tenant_id
     AND seal.knowledge_object_id = projection.knowledge_object_id
     AND seal.object_version = projection.object_version
     AND seal.projection_bytes = projection.projection_bytes
    WHERE projection.tenant_id = NEW.tenant_id
      AND projection.knowledge_object_id = NEW.knowledge_object_id
      AND projection.object_version = NEW.current_version
      AND projection.app_id = NEW.app_id
      AND projection.owner_id = NEW.owner_id
      AND projection.object_type = NEW.object_type
      AND projection.name = NEW.name
      AND projection.sharing_scope = NEW.sharing_scope
      AND projection.state = NEW.state
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object requires exact sealed list projection');
END;

CREATE TRIGGER knowledge_object_update_requires_sealed_list_projection
BEFORE UPDATE ON knowledge_objects
WHEN NOT EXISTS (
    SELECT 1
    FROM knowledge_object_list_projections AS projection
    JOIN knowledge_object_list_projection_seals AS seal
      ON seal.tenant_id = projection.tenant_id
     AND seal.knowledge_object_id = projection.knowledge_object_id
     AND seal.object_version = projection.object_version
     AND seal.projection_bytes = projection.projection_bytes
    WHERE projection.tenant_id = NEW.tenant_id
      AND projection.knowledge_object_id = NEW.knowledge_object_id
      AND projection.object_version = NEW.current_version
      AND projection.app_id = NEW.app_id
      AND projection.owner_id = NEW.owner_id
      AND projection.object_type = NEW.object_type
      AND projection.name = NEW.name
      AND projection.sharing_scope = NEW.sharing_scope
      AND projection.state = NEW.state
)
BEGIN
    SELECT RAISE(ABORT, 'knowledge object requires exact sealed list projection');
END;

-- Once the registry moves, remove the now-noncurrent physical projection in
-- dependency order. During the statement the old and new projections may both
-- exist, but the deferred FK and this cleanup guarantee only current survives
-- commit.
CREATE TRIGGER knowledge_object_update_removes_old_list_projection
AFTER UPDATE ON knowledge_objects
WHEN NEW.current_version <> OLD.current_version
BEGIN
    DELETE FROM knowledge_object_list_projection_seals
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.current_version;

    DELETE FROM knowledge_object_list_selector_patterns
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.current_version;

    DELETE FROM knowledge_object_list_projections
    WHERE tenant_id = OLD.tenant_id
      AND knowledge_object_id = OLD.knowledge_object_id
      AND object_version = OLD.current_version;
END;
