-- Before the first outcome-ambiguous ClickHouse ALTER, persist one immutable
-- correlation marker and the exact physical storage generation it targets.
-- Retries reuse this row; live mutation state remains authoritative in
-- system.mutations and is deliberately not copied into SQLite.
CREATE TABLE index_deletion_mutation_attempts (
    deletion_operation_id TEXT PRIMARY KEY NOT NULL
        REFERENCES index_deletion_operations (
            deletion_operation_id
        ) ON DELETE CASCADE,
    correlation_id TEXT NOT NULL UNIQUE COLLATE BINARY,
    tenant_id TEXT NOT NULL COLLATE BINARY,
    clickhouse_database TEXT NOT NULL COLLATE BINARY,
    clickhouse_table TEXT NOT NULL COLLATE BINARY,
    clickhouse_table_uuid TEXT NOT NULL COLLATE BINARY,
    protocol_version INTEGER NOT NULL,
    created_at_unix_micro INTEGER NOT NULL,
    CONSTRAINT index_deletion_mutation_attempts_correlation_id_canonical
        CHECK (
            length(CAST(correlation_id AS BLOB)) BETWEEN 1 AND 128
            AND instr(CAST(correlation_id AS BLOB), X'00') = 0
            AND substr(correlation_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND correlation_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        ),
    CONSTRAINT index_deletion_mutation_attempts_tenant_id_bounded
        CHECK (
            length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(tenant_id AS BLOB), X'00') = 0
        ),
    CONSTRAINT index_deletion_mutation_attempts_database_canonical
        CHECK (
            length(CAST(clickhouse_database AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(clickhouse_database AS BLOB), X'00') = 0
            AND substr(clickhouse_database, 1, 1) GLOB '[A-Za-z_]'
            AND clickhouse_database NOT GLOB '*[^A-Za-z0-9_]*'
        ),
    CONSTRAINT index_deletion_mutation_attempts_table_canonical
        CHECK (
            length(CAST(clickhouse_table AS BLOB)) BETWEEN 1 AND 255
            AND instr(CAST(clickhouse_table AS BLOB), X'00') = 0
            AND substr(clickhouse_table, 1, 1) GLOB '[A-Za-z_]'
            AND clickhouse_table NOT GLOB '*[^A-Za-z0-9_]*'
        ),
    CONSTRAINT index_deletion_mutation_attempts_table_uuid_canonical
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
    CONSTRAINT index_deletion_mutation_attempts_protocol_supported
        CHECK (protocol_version = 1),
    CONSTRAINT index_deletion_mutation_attempts_created_at_positive
        CHECK (
            created_at_unix_micro BETWEEN 1 AND 253402300799999999
        )
) STRICT;

-- Reject both keys before statement-level conflict handling can turn
-- INSERT OR REPLACE into an implicit delete of the durable correlation row.
CREATE TRIGGER index_deletion_mutation_attempt_identity_collision_is_forbidden
BEFORE INSERT ON index_deletion_mutation_attempts
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_mutation_attempts
    WHERE deletion_operation_id = NEW.deletion_operation_id
       OR correlation_id = NEW.correlation_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion mutation attempt identity is already in use'
    );
END;

CREATE TRIGGER index_deletion_mutation_attempt_insert_is_valid
BEFORE INSERT ON index_deletion_mutation_attempts
WHEN NOT EXISTS (
    SELECT 1
    FROM index_deletion_operations AS deletion_operation
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
      AND NEW.created_at_unix_micro >=
          deletion_operation.created_at_unix_micro
      AND NOT EXISTS (
          SELECT 1
          FROM index_deletion_tombstones
          WHERE index_deletion_tombstones.index_id =
                deletion_operation.index_id
      )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion mutation attempt requires a deleting operation'
    );
END;

CREATE TRIGGER index_deletion_mutation_attempt_update_is_forbidden
BEFORE UPDATE ON index_deletion_mutation_attempts
BEGIN
    SELECT RAISE(ABORT, 'index deletion mutation attempt is immutable');
END;

CREATE TRIGGER index_deletion_mutation_attempt_delete_is_forbidden
BEFORE DELETE ON index_deletion_mutation_attempts
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_operations
    WHERE deletion_operation_id = OLD.deletion_operation_id
)
BEGIN
    SELECT RAISE(
        ABORT,
        'index deletion mutation attempt cannot be deleted'
    );
END;
