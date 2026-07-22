-- Immutable search snapshots need a commit-order boundary independent of
-- event/index timestamps. The single Open Splunk writer assigns one sequence
-- to every synchronously committed batch and captures max(visibility_seq)
-- when a search job starts.

ALTER TABLE open_splunk.events
    ADD COLUMN IF NOT EXISTS `visibility_seq` UInt64 DEFAULT 0 CODEC(Delta, ZSTD(1))
    AFTER `batch_sequence`;

INSERT INTO open_splunk.schema_migrations (`version`, `name`, `applied_at`)
SELECT 2, 'add_visibility_sequence', now64(3)
WHERE NOT EXISTS
(
    SELECT 1
    FROM open_splunk.schema_migrations
    WHERE `version` = 2
);
