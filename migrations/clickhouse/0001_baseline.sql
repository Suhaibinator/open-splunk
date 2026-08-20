-- Fresh-state-only ClickHouse baseline for the current Open Splunk schema.
-- Databases created from earlier migration histories are intentionally unsupported.
-- Every DDL statement is restart-safe because ClickHouse DDL is not transactional.

CREATE DATABASE IF NOT EXISTS open_splunk;

CREATE TABLE IF NOT EXISTS open_splunk.schema_migrations
(
    `version` UInt32,
    `name` LowCardinality(String),
    `applied_at` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (`version`);

CREATE TABLE IF NOT EXISTS open_splunk.events
(
    `event_id` String CODEC(ZSTD(1)),
    `tenant_id` LowCardinality(String) CODEC(ZSTD(1)),
    `index_name` LowCardinality(String) CODEC(ZSTD(1)),
    `event_time` DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `index_time` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `collected_at` Nullable(DateTime64(9, 'UTC')) CODEC(ZSTD(1)),
    `event_time_source` UInt8 CODEC(T64, ZSTD(1)),
    `host` String CODEC(ZSTD(1)),
    `source` String CODEC(ZSTD(1)),
    `sourcetype` LowCardinality(String) CODEC(ZSTD(1)),
    `service` LowCardinality(Nullable(String)) CODEC(ZSTD(1)),
    `severity` UInt8 CODEC(T64, ZSTD(1)),
    `level` LowCardinality(Nullable(String)) CODEC(ZSTD(1)),
    `body` Nullable(String) CODEC(ZSTD(1)),
    `raw` String CODEC(ZSTD(1)),
    `raw_encoding` UInt8 CODEC(T64, ZSTD(1)),
    `trace_id` Nullable(String) CODEC(ZSTD(1)),
    `span_id` Nullable(String) CODEC(ZSTD(1)),
    `fields` JSON(max_dynamic_paths = 256, max_dynamic_types = 16),
    `field_names` Array(String) CODEC(ZSTD(1)),
    `field_types` Array(UInt8) CODEC(ZSTD(1)),
    `field_metadata_version` UInt8 CODEC(T64, ZSTD(1)),
    `collector_id` String CODEC(ZSTD(1)),
    `ingest_source_kind` UInt8 CODEC(T64, ZSTD(1)),
    `ingest_source_id` String CODEC(ZSTD(1)),
    `batch_id` String CODEC(ZSTD(1)),
    `batch_sequence` UInt64 CODEC(Delta, ZSTD(1)),
    `visibility_seq` UInt64 CODEC(Delta, ZSTD(1)),
    `expires_at` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),

    INDEX idx_event_id `event_id` TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_trace_id ifNull(`trace_id`, '') TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_span_id ifNull(`span_id`, '') TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_field_names `field_names` TYPE text(tokenizer = 'array'),
    INDEX idx_raw_text arrayMap(token -> lower(token), extractAll(translateUTF8(`raw`, 'ſK', 'sk'), '[A-Za-z0-9_]+')) TYPE text(tokenizer = 'array'),
    INDEX idx_visibility_seq `visibility_seq` TYPE minmax GRANULARITY 1,

    CONSTRAINT visibility_seq_is_positive CHECK `visibility_seq` > 0,
    CONSTRAINT field_metadata_version_is_supported CHECK `field_metadata_version` = 1,
    CONSTRAINT field_metadata_is_aligned CHECK
        length(`field_names`) = length(`field_types`)
        AND arrayAll(code -> code BETWEEN 1 AND 12, `field_types`),
    CONSTRAINT ingest_source_kind_is_supported CHECK `ingest_source_kind` IN (1, 2),
    CONSTRAINT ingest_source_shape_is_valid CHECK
        (`ingest_source_kind` = 1 AND notEmpty(`collector_id`) AND `ingest_source_id` = `collector_id`)
        OR (`ingest_source_kind` = 2 AND empty(`collector_id`) AND notEmpty(`ingest_source_id`))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(`event_time`)
PRIMARY KEY (`tenant_id`, `index_name`, toStartOfHour(`event_time`), `event_time`)
ORDER BY (`tenant_id`, `index_name`, toStartOfHour(`event_time`), `event_time`, `event_id`)
TTL `expires_at` DELETE
SETTINGS
    index_granularity = 8192,
    non_replicated_deduplication_window = 10000,
    write_marks_for_substreams_in_compact_parts = 1;

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
SELECT 1, 'baseline', now64(3)
WHERE NOT EXISTS
(
    SELECT 1
    FROM open_splunk.schema_migrations
    WHERE `version` = 1
);
