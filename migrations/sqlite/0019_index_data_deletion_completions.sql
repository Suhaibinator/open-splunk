-- A physical deletion becomes terminal through one completion-row insert.
-- The row permanently snapshots the ephemeral operation and mutation attempt;
-- its AFTER trigger creates the ordinary catalog tombstone and consumes the
-- outstanding work. The retained index stays in its already-versioned
-- DELETING generation so an admitted MaxInt64 generation can still complete.
CREATE TABLE index_data_deletion_completions (
    deletion_operation_id TEXT PRIMARY KEY NOT NULL,
    correlation_id TEXT NOT NULL UNIQUE COLLATE BINARY,
    index_id TEXT NOT NULL UNIQUE
        REFERENCES indexes (index_id) ON DELETE RESTRICT,
    index_name TEXT NOT NULL COLLATE BINARY,
    archived_index_version INTEGER NOT NULL,
    deleting_index_version INTEGER NOT NULL,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    clickhouse_database TEXT NOT NULL COLLATE BINARY,
    clickhouse_table TEXT NOT NULL COLLATE BINARY,
    clickhouse_table_uuid TEXT NOT NULL COLLATE BINARY,
    protocol_version INTEGER NOT NULL,
    operation_created_at_unix_micro INTEGER NOT NULL,
    attempt_created_at_unix_micro INTEGER NOT NULL,
    completed_at_unix_micro INTEGER NOT NULL,
    CONSTRAINT index_data_deletion_completions_operation_id_canonical
        CHECK (
            length(CAST(deletion_operation_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(deletion_operation_id AS BLOB), X'00') = 0
            AND substr(deletion_operation_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND deletion_operation_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        ),
    CONSTRAINT index_data_deletion_completions_correlation_id_canonical
        CHECK (
            length(CAST(correlation_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(correlation_id AS BLOB), X'00') = 0
            AND substr(correlation_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND correlation_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        ),
    CONSTRAINT index_data_deletion_completions_index_name_canonical
        CHECK (
            length(CAST(index_name AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(index_name AS BLOB), X'00') = 0
            AND index_name = lower(index_name)
            AND index_name NOT GLOB '*[^a-z0-9_-]*'
            AND substr(index_name, 1, 1) GLOB '[a-z0-9]'
            AND instr(index_name, 'kvstore') = 0
        ),
    CONSTRAINT index_data_deletion_completions_versions_supported
        CHECK (
            archived_index_version >= 1
            AND archived_index_version < 9223372036854775807
            AND deleting_index_version = archived_index_version + 1
        ),
    CONSTRAINT index_data_deletion_completions_tenant_id_bounded
        CHECK (
            length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        ),
    CONSTRAINT index_data_deletion_completions_database_canonical
        CHECK (
            length(CAST(clickhouse_database AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(clickhouse_database AS BLOB), X'00') = 0
            AND substr(clickhouse_database, 1, 1) GLOB '[A-Za-z_]'
            AND clickhouse_database NOT GLOB '*[^A-Za-z0-9_]*'
        ),
    CONSTRAINT index_data_deletion_completions_table_canonical
        CHECK (
            length(CAST(clickhouse_table AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(clickhouse_table AS BLOB), X'00') = 0
            AND substr(clickhouse_table, 1, 1) GLOB '[A-Za-z_]'
            AND clickhouse_table NOT GLOB '*[^A-Za-z0-9_]*'
        ),
    CONSTRAINT index_data_deletion_completions_table_uuid_canonical
        CHECK (
            length(CAST(clickhouse_table_uuid AS BLOB)) = 36
            AND instr(CAST(clickhouse_table_uuid AS BLOB), X'00') = 0
            AND clickhouse_table_uuid = lower(clickhouse_table_uuid)
            AND substr(clickhouse_table_uuid, 9, 1) = '-'
            AND substr(clickhouse_table_uuid, 14, 1) = '-'
            AND substr(clickhouse_table_uuid, 19, 1) = '-'
            AND substr(clickhouse_table_uuid, 24, 1) = '-'
            AND length(replace(clickhouse_table_uuid, '-', '')) = 32
            AND replace(clickhouse_table_uuid, '-', '')
                NOT GLOB '*[^0-9a-f]*'
            AND clickhouse_table_uuid <>
                '00000000-0000-0000-0000-000000000000'
        ),
    CONSTRAINT index_data_deletion_completions_protocol_supported
        CHECK (protocol_version = 1),
    CONSTRAINT index_data_deletion_completions_timestamps_supported
        CHECK (
            operation_created_at_unix_micro
                BETWEEN 1 AND 253402300799999999
            AND attempt_created_at_unix_micro
                BETWEEN operation_created_at_unix_micro
                    AND 253402300799999999
            AND completed_at_unix_micro
                BETWEEN attempt_created_at_unix_micro
                    AND 253402300799999999
        )
) STRICT;

-- Reject conflicts before INSERT OR REPLACE/UPSERT can implicitly remove or
-- mutate an immutable completion without running its ordinary DELETE trigger.
CREATE TRIGGER index_data_deletion_completion_identity_collision_is_forbidden
BEFORE INSERT ON index_data_deletion_completions
WHEN EXISTS (
    SELECT 1
    FROM index_data_deletion_completions
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR correlation_id = NEW.correlation_id
       OR index_id = NEW.index_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index data deletion completion identity is already in use'
    );
END;

CREATE TRIGGER index_data_deletion_completion_insert_is_valid
BEFORE INSERT ON index_data_deletion_completions
WHEN NOT EXISTS (
    SELECT 1
    FROM index_deletion_operations AS deletion_operation
    JOIN index_deletion_mutation_attempts AS mutation_attempt
      ON mutation_attempt.deletion_operation_id =
         deletion_operation.deletion_operation_id
    JOIN indexes AS target_index
      ON target_index.index_id = deletion_operation.index_id
     AND target_index.name = deletion_operation.index_name
     AND target_index.version =
         deletion_operation.archived_index_version + 1
     AND target_index.state = 'deleting'
     AND target_index.updated_at_unix_micro =
         deletion_operation.created_at_unix_micro
    WHERE deletion_operation.deletion_operation_id =
          NEW.deletion_operation_id
      AND deletion_operation.index_id = NEW.index_id
      AND deletion_operation.index_name = NEW.index_name
      AND deletion_operation.tenant_id = NEW.tenant_id
      AND deletion_operation.archived_index_version =
          NEW.archived_index_version
      AND deletion_operation.created_at_unix_micro =
          NEW.operation_created_at_unix_micro
      AND NEW.deleting_index_version =
          deletion_operation.archived_index_version + 1
      AND mutation_attempt.correlation_id = NEW.correlation_id
      AND mutation_attempt.tenant_id = NEW.tenant_id
      AND mutation_attempt.clickhouse_database =
          NEW.clickhouse_database
      AND mutation_attempt.clickhouse_table = NEW.clickhouse_table
      AND mutation_attempt.clickhouse_table_uuid =
          NEW.clickhouse_table_uuid
      AND mutation_attempt.protocol_version = NEW.protocol_version
      AND mutation_attempt.created_at_unix_micro =
          NEW.attempt_created_at_unix_micro
      AND NEW.completed_at_unix_micro >=
          mutation_attempt.created_at_unix_micro
      AND NOT EXISTS (
          SELECT 1
          FROM index_deletion_tombstones
          WHERE index_deletion_tombstones.index_id = NEW.index_id
      )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index data deletion completion must match outstanding work'
    );
END;

-- Defeat replacement of KEEP_DATA and physical-deletion tombstones alike.
CREATE TRIGGER index_deletion_tombstone_identity_collision_is_forbidden
BEFORE INSERT ON index_deletion_tombstones
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_tombstones
    WHERE index_id = NEW.index_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion tombstone identity is already in use'
    );
END;

DROP TRIGGER index_deletion_tombstone_insert_is_valid;

-- KEEP_DATA still tombstones an exact archived generation. Physical deletion
-- tombstones an exact deleting generation only through the completion row
-- whose insertion already validated the operation and mutation attempt.
CREATE TRIGGER index_deletion_tombstone_insert_is_valid
BEFORE INSERT ON index_deletion_tombstones
WHEN NOT EXISTS (
    SELECT 1
    FROM indexes AS target_index
    WHERE target_index.index_id = NEW.index_id
      AND target_index.name = NEW.name
      AND target_index.version = NEW.deleted_version
      AND (
          (
              target_index.state = 'archived'
              AND NOT EXISTS (
                  SELECT 1
                  FROM index_deletion_operations
                  WHERE index_deletion_operations.index_id =
                        target_index.index_id
              )
          )
          OR
          (
              target_index.state = 'deleting'
              AND EXISTS (
                  SELECT 1
                  FROM index_data_deletion_completions AS completion
                  WHERE completion.index_id = NEW.index_id
                    AND completion.index_name = NEW.name
                    AND completion.deleting_index_version =
                        NEW.deleted_version
                    AND completion.completed_at_unix_micro =
                        NEW.deleted_at_unix_micro
              )
          )
      )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion tombstone must match a terminal index'
    );
END;

DROP TRIGGER index_deletion_mutation_attempt_delete_is_forbidden;

CREATE TRIGGER index_deletion_mutation_attempt_delete_is_forbidden
BEFORE DELETE ON index_deletion_mutation_attempts
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_operations
    WHERE deletion_operation_id = OLD.deletion_operation_id
)
 AND NOT EXISTS (
     SELECT 1
     FROM index_data_deletion_completions
     WHERE deletion_operation_id = OLD.deletion_operation_id
       AND correlation_id = OLD.correlation_id
       AND tenant_id = OLD.tenant_id
       AND clickhouse_database = OLD.clickhouse_database
       AND clickhouse_table = OLD.clickhouse_table
       AND clickhouse_table_uuid = OLD.clickhouse_table_uuid
       AND protocol_version = OLD.protocol_version
       AND attempt_created_at_unix_micro =
           OLD.created_at_unix_micro
 )
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion mutation attempt cannot be deleted'
    );
END;

DROP TRIGGER index_deletion_operation_delete_is_forbidden;

CREATE TRIGGER index_deletion_operation_delete_is_forbidden
BEFORE DELETE ON index_deletion_operations
WHEN NOT EXISTS (
    SELECT 1
    FROM index_data_deletion_completions AS completion
    JOIN index_deletion_tombstones AS tombstone
      ON tombstone.index_id = completion.index_id
     AND tombstone.name = completion.index_name
     AND tombstone.deleted_version =
         completion.deleting_index_version
     AND tombstone.deleted_at_unix_micro =
         completion.completed_at_unix_micro
    JOIN indexes AS target_index
      ON target_index.index_id = completion.index_id
     AND target_index.name = completion.index_name
     AND target_index.version = completion.deleting_index_version
     AND target_index.state = 'deleting'
     AND target_index.updated_at_unix_micro =
         completion.operation_created_at_unix_micro
    WHERE completion.deletion_operation_id =
          OLD.deletion_operation_id
      AND completion.index_id = OLD.index_id
      AND completion.index_name = OLD.index_name
      AND completion.tenant_id = OLD.tenant_id
      AND completion.archived_index_version =
          OLD.archived_index_version
      AND completion.operation_created_at_unix_micro =
          OLD.created_at_unix_micro
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion operation cannot be deleted'
    );
END;

CREATE TRIGGER index_data_deletion_completion_finishes_operation
AFTER INSERT ON index_data_deletion_completions
BEGIN
    INSERT INTO index_deletion_tombstones (
        index_id,
        name,
        deleted_version,
        deleted_at_unix_micro
    ) VALUES (
        NEW.index_id,
        NEW.index_name,
        NEW.deleting_index_version,
        NEW.completed_at_unix_micro
    );

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(
            ABORT,
            'index data deletion completion tombstone failed'
        )
    END;

    DELETE FROM index_deletion_operations
    WHERE deletion_operation_id = NEW.deletion_operation_id
      AND index_id = NEW.index_id
      AND index_name = NEW.index_name
      AND tenant_id = NEW.tenant_id
      AND archived_index_version = NEW.archived_index_version
      AND created_at_unix_micro =
          NEW.operation_created_at_unix_micro;

    SELECT CASE
        WHEN changes() <> 1
        THEN RAISE(
            ABORT,
            'index data deletion completion operation cleanup failed'
        )
    END;

    SELECT CASE
        WHEN EXISTS (
            SELECT 1
            FROM index_deletion_mutation_attempts
            WHERE deletion_operation_id = NEW.deletion_operation_id
        )
        THEN RAISE(
            ABORT,
            'index data deletion completion attempt cleanup failed'
        )
    END;
END;

CREATE TRIGGER index_data_deletion_completion_update_is_forbidden
BEFORE UPDATE ON index_data_deletion_completions
BEGIN
    SELECT RAISE(ABORT, 'index data deletion completion is immutable');
END;

CREATE TRIGGER index_data_deletion_completion_delete_is_forbidden
BEFORE DELETE ON index_data_deletion_completions
BEGIN
    SELECT RAISE(
        ABORT,
        'index data deletion completion cannot be deleted'
    );
END;

-- Completed protocol identities can never be recycled into new outstanding
-- work, even if random generation happens to collide years later.
CREATE TRIGGER index_deletion_operation_completed_identity_is_forbidden
BEFORE INSERT ON index_deletion_operations
WHEN EXISTS (
    SELECT 1
    FROM index_data_deletion_completions
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR index_id = NEW.index_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'completed index deletion operation identity is already in use'
    );
END;

CREATE TRIGGER index_deletion_mutation_completed_identity_is_forbidden
BEFORE INSERT ON index_deletion_mutation_attempts
WHEN EXISTS (
    SELECT 1
    FROM index_data_deletion_completions
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR correlation_id = NEW.correlation_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'completed index deletion mutation identity is already in use'
    );
END;
