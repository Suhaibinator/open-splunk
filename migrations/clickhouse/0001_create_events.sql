-- Open Splunk canonical event storage.
--
-- The migration is restart-safe: every DDL statement is idempotent and the
-- ledger row is written only after the schema exists. ClickHouse does not make
-- multi-statement DDL transactional, so future migrations must follow the same
-- create/alter-first, ledger-last convention.

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
    -- Retry identity. IDs remain strings because the wire contract deliberately
    -- does not require UUID formatting.
    `event_id` String CODEC(ZSTD(1)),
    `tenant_id` LowCardinality(String) CODEC(ZSTD(1)),
    `index_name` LowCardinality(String) CODEC(ZSTD(1)),

    -- Splunk-compatible event and index times. Event time keeps protobuf's
    -- nanosecond precision; index time needs millisecond operational precision.
    `event_time` DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `index_time` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `collected_at` Nullable(DateTime64(9, 'UTC')) CODEC(ZSTD(1)),
    `event_time_source` UInt8 CODEC(T64, ZSTD(1)),

    -- Canonical dimensions and promoted fields. Nullable preserves the
    -- distinction between an absent optional field and a present empty string.
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

    -- Bounded native JSON retains long-tail integer, floating-point, boolean,
    -- array, object, string, and null values without creating unbounded physical
    -- subcolumns. Paths above the limit use ClickHouse's shared-data encoding.
    `fields` JSON(
        max_dynamic_paths = 256,
        max_dynamic_types = 16
    ),
    -- Sorted, unique, normalized dotted paths supplied by ingestion. Keeping
    -- discovery metadata separate avoids reconstructing every JSON document.
    `field_names` Array(String) CODEC(ZSTD(1)),

    `collector_id` String CODEC(ZSTD(1)),
    `batch_id` String CODEC(ZSTD(1)),
    `batch_sequence` UInt64 CODEC(Delta, ZSTD(1)),

    -- The server resolves the index retention policy when accepting an event.
    -- A physical timestamp supports different retention periods in one table.
    `expires_at` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),

    INDEX idx_event_id `event_id` TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_trace_id ifNull(`trace_id`, '') TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_span_id ifNull(`span_id`, '') TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_field_names `field_names` TYPE text(tokenizer = 'array'),
    INDEX idx_raw_text lowerUTF8(`raw`) TYPE text(tokenizer = 'splitByNonAlpha')
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

INSERT INTO open_splunk.schema_migrations (`version`, `name`, `applied_at`)
SELECT 1, 'create_events', now64(3)
WHERE NOT EXISTS
(
    SELECT 1
    FROM open_splunk.schema_migrations
    WHERE `version` = 1
);
