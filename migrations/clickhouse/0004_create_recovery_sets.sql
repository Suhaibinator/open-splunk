-- Durable proof that ClickHouse was restored from the recovery set paired
-- with the control-plane bundle. The singleton slot gives retry logic one
-- release-owned receipt to compare after a successful direct canonical
-- RESTORE.

CREATE TABLE IF NOT EXISTS open_splunk.recovery_sets
(
    `slot` UInt8,
    `recovery_set_id` FixedString(32),
    `deployment_manifest_sha256` FixedString(64),
    `database_uuid` UUID,
    `schema_migrations_table_uuid` UUID,
    `events_table_uuid` UUID,
    `recovery_sets_table_uuid` UUID,
    `recovery_archive_markers_table_uuid` UUID,
    `restored_at` DateTime64(3, 'UTC'),
    CONSTRAINT slot_is_singleton CHECK `slot` = 1
)
ENGINE = MergeTree
ORDER BY (`slot`);

-- The marker is written immediately before a native deployment backup and is
-- therefore carried inside that archive. Restore requires the exact recovery
-- set and backup-operation identity before publishing a receipt, then consumes
-- the marker and proves absence before accepting the canonical restore.

CREATE TABLE IF NOT EXISTS open_splunk.recovery_archive_markers
(
    `slot` UInt8,
    `recovery_set_id` FixedString(32),
    `backup_operation_uuid` UUID,
    CONSTRAINT slot_is_singleton CHECK `slot` = 1
)
ENGINE = MergeTree
ORDER BY (`slot`);

INSERT INTO open_splunk.schema_migrations (`version`, `name`, `applied_at`)
SELECT 4, 'create_recovery_sets', now64(3)
WHERE NOT EXISTS
(
    SELECT 1
    FROM open_splunk.schema_migrations
    WHERE `version` = 4
);
