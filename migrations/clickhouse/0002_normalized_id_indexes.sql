-- Match the case-insensitive ID expressions used by SPL search predicates.
-- Adding metadata is restart-safe; existing parts can be materialized by an
-- administrator separately without delaying server startup.
ALTER TABLE open_splunk.events
    ADD INDEX IF NOT EXISTS idx_event_id_ci lowerUTF8(ifNull(`event_id`, ''))
    TYPE bloom_filter(0.001) GRANULARITY 1;

ALTER TABLE open_splunk.events
    ADD INDEX IF NOT EXISTS idx_trace_id_ci lowerUTF8(ifNull(`trace_id`, ''))
    TYPE bloom_filter(0.001) GRANULARITY 1;

ALTER TABLE open_splunk.events
    ADD INDEX IF NOT EXISTS idx_span_id_ci lowerUTF8(ifNull(`span_id`, ''))
    TYPE bloom_filter(0.001) GRANULARITY 1;

INSERT INTO open_splunk.schema_migrations (`version`, `name`, `applied_at`)
SELECT 2, 'normalized_id_indexes', now64(3)
WHERE NOT EXISTS
(
    SELECT 1
    FROM open_splunk.schema_migrations
    WHERE `version` = 2
);
