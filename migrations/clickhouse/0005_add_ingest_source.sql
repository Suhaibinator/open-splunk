-- Transport-neutral ingestion provenance distinguishes native collector rows
-- from HTTP Event Collector rows without inventing collector-fleet identities.
-- Existing events were written only by the native collector path, so their
-- nonempty collector_id is authoritative for the backfill defaults.

ALTER TABLE open_splunk.events
    ADD COLUMN IF NOT EXISTS `ingest_source_kind` UInt8
    DEFAULT if(empty(`collector_id`), 0, 1) CODEC(T64, ZSTD(1))
    AFTER `collector_id`;

ALTER TABLE open_splunk.events
    ADD COLUMN IF NOT EXISTS `ingest_source_id` String
    DEFAULT if(`ingest_source_kind` = 1, `collector_id`, '') CODEC(ZSTD(1))
    AFTER `ingest_source_kind`;

-- Kind zero is reserved for genuinely legacy/unknown rows exposed through the
-- additive default. ClickHouse evaluates constraints only for newly inserted
-- rows, so the supported-kind constraint rejects new unknown provenance while
-- preserving historical rows which cannot be classified truthfully.
ALTER TABLE open_splunk.events
    ADD CONSTRAINT IF NOT EXISTS ingest_source_kind_is_supported
    CHECK `ingest_source_kind` IN (1, 2);

ALTER TABLE open_splunk.events
    ADD CONSTRAINT IF NOT EXISTS ingest_source_shape_is_valid
    CHECK
        (`ingest_source_kind` = 0 AND empty(`ingest_source_id`))
        OR
        (
            `ingest_source_kind` = 1
            AND notEmpty(`collector_id`)
            AND `ingest_source_id` = `collector_id`
        )
        OR
        (
            `ingest_source_kind` = 2
            AND empty(`collector_id`)
            AND notEmpty(`ingest_source_id`)
        );

INSERT INTO open_splunk.schema_migrations (`version`, `name`, `applied_at`)
SELECT 5, 'add_ingest_source', now64(3)
WHERE NOT EXISTS
(
    SELECT 1
    FROM open_splunk.schema_migrations
    WHERE `version` = 5
);
