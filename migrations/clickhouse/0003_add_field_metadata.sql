-- Dynamic JSON physical types cannot distinguish every protobuf logical type,
-- and JSON path enumeration omits explicit nulls. Persist a versioned,
-- positionally aligned semantic type for every normalized field_names entry.

ALTER TABLE open_splunk.events
    ADD COLUMN IF NOT EXISTS `field_types` Array(UInt8) DEFAULT [] CODEC(ZSTD(1))
    AFTER `field_names`;

ALTER TABLE open_splunk.events
    ADD COLUMN IF NOT EXISTS `field_metadata_version` UInt8 DEFAULT 0 CODEC(T64, ZSTD(1))
    AFTER `field_types`;

-- Version zero identifies rows written before this metadata existed. Their
-- empty type array is intentional and must never be interpreted heuristically.
-- Version one contains only concrete protobuf leaf type codes and aligns each
-- code with the field name at the same one-based array position.
ALTER TABLE open_splunk.events
    ADD CONSTRAINT IF NOT EXISTS field_metadata_version_is_supported
    CHECK `field_metadata_version` IN (0, 1);

ALTER TABLE open_splunk.events
    ADD CONSTRAINT IF NOT EXISTS field_metadata_is_aligned
    CHECK
        (`field_metadata_version` = 0 AND empty(`field_types`))
        OR
        (
            `field_metadata_version` = 1
            AND length(`field_names`) = length(`field_types`)
            AND arrayAll(code -> code BETWEEN 1 AND 12, `field_types`)
        );

INSERT INTO open_splunk.schema_migrations (`version`, `name`, `applied_at`)
SELECT 3, 'add_field_metadata', now64(3)
WHERE NOT EXISTS
(
    SELECT 1
    FROM open_splunk.schema_migrations
    WHERE `version` = 3
);
