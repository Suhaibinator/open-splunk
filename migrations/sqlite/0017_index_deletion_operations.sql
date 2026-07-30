-- DELETE_DATA begins as one immutable control-plane operation. The retained
-- index row continues to own its canonical ClickHouse-facing name; the
-- operation snapshots the trusted admission tenant and exact archived
-- generation that entered the coordinator-owned deleting state.
CREATE TABLE index_deletion_operations (
    deletion_operation_id TEXT PRIMARY KEY NOT NULL,
    index_id TEXT NOT NULL UNIQUE
        REFERENCES indexes (index_id) ON DELETE RESTRICT,
    index_name TEXT NOT NULL COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    archived_index_version INTEGER NOT NULL
        CHECK (
            archived_index_version >= 1
            AND archived_index_version < 9223372036854775807
        ),
    created_at_unix_micro INTEGER NOT NULL
        CHECK (created_at_unix_micro > 0),
    CHECK (
        length(CAST(deletion_operation_id AS BLOB)) BETWEEN 1 AND 128
        AND instr(
            CAST(deletion_operation_id AS BLOB),
            X'00'
        ) = 0
        AND substr(deletion_operation_id, 1, 1) GLOB '[A-Za-z0-9]'
        AND deletion_operation_id NOT GLOB '*[^A-Za-z0-9._:-]*'
    ),
    CONSTRAINT index_deletion_operations_tenant_id_bounded
        CHECK (
            length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(tenant_id AS BLOB), X'00') = 0
            AND tenant_id = trim(tenant_id)
            AND tenant_id NOT GLOB (
                '*['
                || char(1)
                || '-'
                || char(31)
                || char(127)
                || '-'
                || char(159)
                || ']*'
            )
        )
) STRICT;

CREATE INDEX index_deletion_operations_created_id_idx
    ON index_deletion_operations (
        created_at_unix_micro,
        deletion_operation_id
    );

CREATE TRIGGER deleting_index_insert_is_forbidden
BEFORE INSERT ON indexes
WHEN NEW.state = 'deleting'
BEGIN
    SELECT RAISE(
        ABORT,
        'deleting index creation requires a deletion operation'
    );
END;

-- The insert statement owns the archived-to-deleting transition so even a
-- direct SQL caller cannot commit only one half of operation admission.
CREATE TRIGGER index_deletion_operation_insert_is_valid
BEFORE INSERT ON index_deletion_operations
WHEN NOT EXISTS (
    SELECT 1
    FROM indexes
    WHERE indexes.index_id = NEW.index_id
      AND indexes.name = NEW.index_name
      AND indexes.version = NEW.archived_index_version
      AND indexes.state = 'archived'
      AND indexes.updated_at_unix_micro <= NEW.created_at_unix_micro
      AND NOT EXISTS (
          SELECT 1
          FROM index_deletion_tombstones
          WHERE index_deletion_tombstones.index_id = indexes.index_id
      )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion operation must match an archived index generation'
    );
END;

-- A statement-level OR REPLACE conflict policy can delete the conflicting row
-- without firing DELETE triggers when recursive_triggers is disabled. Reject
-- both operation-ID and index-ID collisions before conflict resolution so a
-- replacement can never detach an already-deleting index from its work row.
CREATE TRIGGER index_deletion_operation_identity_collision_is_forbidden
BEFORE INSERT ON index_deletion_operations
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_operations
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR index_id = NEW.index_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion operation identity is already in use'
    );
END;

CREATE TRIGGER index_deleting_transition_requires_operation
BEFORE UPDATE OF state ON indexes
WHEN OLD.state <> 'deleting'
 AND NEW.state = 'deleting'
 AND NOT EXISTS (
     SELECT 1
     FROM index_deletion_operations
     WHERE index_deletion_operations.index_id = OLD.index_id
       AND index_deletion_operations.index_name = OLD.name
       AND index_deletion_operations.archived_index_version = OLD.version
       AND NEW.version = OLD.version + 1
       AND NEW.updated_at_unix_micro =
           index_deletion_operations.created_at_unix_micro
 )
BEGIN
    SELECT RAISE(
        ABORT,
        'deleting index transition requires a matching deletion operation'
    );
END;

CREATE TRIGGER index_deletion_operation_insert_starts_deletion
AFTER INSERT ON index_deletion_operations
BEGIN
    UPDATE indexes
    SET state = 'deleting',
        version = version + 1,
        updated_at_unix_micro = NEW.created_at_unix_micro
    WHERE index_id = NEW.index_id
      AND name = NEW.index_name
      AND version = NEW.archived_index_version
      AND state = 'archived';

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(ABORT, 'index deletion operation transition failed')
    END;
END;

-- Once admitted, only the future physical-deletion coordinator may advance
-- the index and operation. A later migration can replace these guards when
-- the terminal transition is introduced.
CREATE TRIGGER deleting_index_with_operation_update_is_forbidden
BEFORE UPDATE ON indexes
WHEN OLD.state = 'deleting'
 AND EXISTS (
     SELECT 1
     FROM index_deletion_operations
     WHERE index_deletion_operations.index_id = OLD.index_id
 )
BEGIN
    SELECT RAISE(ABORT, 'deleting index is coordinator-owned');
END;

CREATE TRIGGER deleting_index_with_operation_delete_is_forbidden
BEFORE DELETE ON indexes
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_operations
    WHERE index_deletion_operations.index_id = OLD.index_id
)
BEGIN
    SELECT RAISE(ABORT, 'deleting index cannot be deleted');
END;

CREATE TRIGGER index_deletion_operation_update_is_forbidden
BEFORE UPDATE ON index_deletion_operations
BEGIN
    SELECT RAISE(ABORT, 'index deletion operation is immutable');
END;

CREATE TRIGGER index_deletion_operation_delete_is_forbidden
BEFORE DELETE ON index_deletion_operations
BEGIN
    SELECT RAISE(ABORT, 'index deletion operation cannot be deleted');
END;
