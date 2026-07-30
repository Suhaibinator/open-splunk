-- The index catalog is global for the initial single-deployment control
-- plane. Physical index identities are never reclaimed merely because a
-- terminal tombstone hides them: retaining the row permanently reserves the
-- canonical ClickHouse-facing name.
CREATE TABLE index_catalog_state (
    singleton_id INTEGER PRIMARY KEY NOT NULL
        CHECK (singleton_id = 1),
    revision INTEGER NOT NULL
        CHECK (revision BETWEEN 1 AND 9223372036854775807),
    physical_count INTEGER NOT NULL
        CHECK (physical_count BETWEEN 0 AND 1024)
) STRICT, WITHOUT ROWID;

INSERT INTO index_catalog_state (
    singleton_id,
    revision,
    physical_count
)
SELECT
    1,
    1,
    COUNT(*)
FROM indexes;

CREATE INDEX indexes_name_id_idx
    ON indexes (name, index_id);

CREATE INDEX indexes_created_id_idx
    ON indexes (created_at_unix_micro, index_id);

CREATE INDEX indexes_updated_id_idx
    ON indexes (updated_at_unix_micro, index_id);

CREATE TRIGGER index_catalog_state_identity_is_immutable
BEFORE UPDATE OF singleton_id ON index_catalog_state
WHEN NEW.singleton_id <> OLD.singleton_id
BEGIN
    SELECT RAISE(ABORT, 'index catalog state identity is immutable');
END;

-- Reject statement-level replacement before SQLite can apply an OR REPLACE
-- policy that bypasses DELETE triggers with recursive_triggers disabled.
CREATE TRIGGER index_catalog_state_collision_is_forbidden
BEFORE INSERT ON index_catalog_state
WHEN EXISTS (
    SELECT 1
    FROM index_catalog_state
    WHERE singleton_id = NEW.singleton_id
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog state already exists');
END;

-- Catalog triggers are the only legitimate writers. Every mutation advances
-- the revision by exactly one; only index admission may also add one physical
-- identity. This prevents a stale signed cursor revision from being made
-- current again by a reset or replacement.
CREATE TRIGGER index_catalog_state_transition_is_valid
BEFORE UPDATE OF revision, physical_count ON index_catalog_state
WHEN NOT (
    NEW.revision = OLD.revision + 1
    AND (
        NEW.physical_count = OLD.physical_count
        OR NEW.physical_count = OLD.physical_count + 1
    )
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog state transition is invalid');
END;

CREATE TRIGGER index_catalog_state_delete_is_forbidden
BEFORE DELETE ON index_catalog_state
BEGIN
    SELECT RAISE(ABORT, 'index catalog state cannot be deleted');
END;

CREATE TRIGGER index_catalog_record_insert_is_bounded
BEFORE INSERT ON indexes
WHEN NOT (
    length(CAST(NEW.index_id AS BLOB)) BETWEEN 1 AND 128
    AND instr(CAST(NEW.index_id AS BLOB), X'00') = 0
    AND NEW.index_id = trim(NEW.index_id)
    AND NEW.index_id NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.display_name AS BLOB)) BETWEEN 1 AND 255
    AND instr(CAST(NEW.display_name AS BLOB), X'00') = 0
    AND NEW.display_name = trim(NEW.display_name)
    AND NEW.display_name NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.description AS BLOB)) BETWEEN 0 AND 8192
    AND instr(CAST(NEW.description AS BLOB), X'00') = 0
    AND NEW.description NOT GLOB (
        '*[' || char(1) || '-' || char(8)
        || char(11) || '-' || char(12)
        || char(14) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.default_sourcetype AS BLOB)) BETWEEN 0 AND 255
    AND instr(CAST(NEW.default_sourcetype AS BLOB), X'00') = 0
    AND NEW.default_sourcetype = trim(NEW.default_sourcetype)
    AND NEW.default_sourcetype NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND NEW.created_at_unix_micro BETWEEN 1 AND 253402300799999999
    AND NEW.updated_at_unix_micro BETWEEN
        NEW.created_at_unix_micro AND 253402300799999999
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog record is invalid or unbounded');
END;

CREATE TRIGGER index_catalog_record_update_is_bounded
BEFORE UPDATE OF
    index_id,
    display_name,
    description,
    default_sourcetype,
    created_at_unix_micro,
    updated_at_unix_micro
ON indexes
WHEN NOT (
    length(CAST(NEW.index_id AS BLOB)) BETWEEN 1 AND 128
    AND instr(CAST(NEW.index_id AS BLOB), X'00') = 0
    AND NEW.index_id = trim(NEW.index_id)
    AND NEW.index_id NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.display_name AS BLOB)) BETWEEN 1 AND 255
    AND instr(CAST(NEW.display_name AS BLOB), X'00') = 0
    AND NEW.display_name = trim(NEW.display_name)
    AND NEW.display_name NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.description AS BLOB)) BETWEEN 0 AND 8192
    AND instr(CAST(NEW.description AS BLOB), X'00') = 0
    AND NEW.description NOT GLOB (
        '*[' || char(1) || '-' || char(8)
        || char(11) || '-' || char(12)
        || char(14) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND length(CAST(NEW.default_sourcetype AS BLOB)) BETWEEN 0 AND 255
    AND instr(CAST(NEW.default_sourcetype AS BLOB), X'00') = 0
    AND NEW.default_sourcetype = trim(NEW.default_sourcetype)
    AND NEW.default_sourcetype NOT GLOB (
        '*[' || char(1) || '-' || char(31)
        || char(127) || '-' || char(159) || ']*'
    )
    AND NEW.created_at_unix_micro BETWEEN 1 AND 253402300799999999
    AND NEW.updated_at_unix_micro BETWEEN
        NEW.created_at_unix_micro AND 253402300799999999
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog record is invalid or unbounded');
END;

CREATE TRIGGER indexes_id_is_immutable
BEFORE UPDATE OF index_id ON indexes
WHEN NEW.index_id <> OLD.index_id
BEGIN
    SELECT RAISE(ABORT, 'index ID is immutable');
END;

CREATE TRIGGER index_catalog_before_index_insert
BEFORE INSERT ON indexes
WHEN NOT EXISTS (
    SELECT 1
    FROM index_catalog_state
    WHERE singleton_id = 1
      AND revision BETWEEN 1 AND 9223372036854775806
      AND physical_count BETWEEN 0 AND 1023
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index catalog state is invalid or capacity is exhausted'
    );
END;

-- A statement-level OR REPLACE can otherwise delete a conflicting row
-- without running DELETE triggers when recursive_triggers is disabled. Index
-- identities and names are permanent, so reject both conflict namespaces
-- before SQLite applies the statement conflict policy.
CREATE TRIGGER index_catalog_identity_collision_is_forbidden
BEFORE INSERT ON indexes
WHEN EXISTS (
    SELECT 1
    FROM indexes
    WHERE index_id = NEW.index_id
       OR name = NEW.name
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog identity is already in use');
END;

CREATE TRIGGER index_catalog_after_index_insert
AFTER INSERT ON indexes
BEGIN
    UPDATE index_catalog_state
    SET revision = revision + 1,
        physical_count = physical_count + 1
    WHERE singleton_id = 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'index catalog insert accounting failed')
    END;
END;

CREATE TRIGGER index_catalog_before_index_update
BEFORE UPDATE ON indexes
WHEN NOT EXISTS (
    SELECT 1
    FROM index_catalog_state
    WHERE singleton_id = 1
      AND revision BETWEEN 1 AND 9223372036854775806
      AND physical_count BETWEEN 1 AND 1024
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog state is invalid');
END;

CREATE TRIGGER index_catalog_after_index_update
AFTER UPDATE ON indexes
BEGIN
    UPDATE index_catalog_state
    SET revision = revision + 1
    WHERE singleton_id = 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'index catalog update accounting failed')
    END;
END;

-- Product deletion always retains the indexes row so its canonical native
-- identity can never be reused. There is no supported physical row deletion.
CREATE TRIGGER index_catalog_index_delete_is_forbidden
BEFORE DELETE ON indexes
BEGIN
    SELECT RAISE(ABORT, 'index catalog identity cannot be deleted');
END;

CREATE TRIGGER index_catalog_before_tombstone_insert
BEFORE INSERT ON index_deletion_tombstones
WHEN NOT EXISTS (
    SELECT 1
    FROM index_catalog_state
    WHERE singleton_id = 1
      AND revision BETWEEN 1 AND 9223372036854775806
      AND physical_count BETWEEN 1 AND 1024
)
BEGIN
    SELECT RAISE(ABORT, 'index catalog state is invalid');
END;

CREATE TRIGGER index_catalog_after_tombstone_insert
AFTER INSERT ON index_deletion_tombstones
BEGIN
    UPDATE index_catalog_state
    SET revision = revision + 1
    WHERE singleton_id = 1;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'index catalog tombstone accounting failed')
    END;
END;
