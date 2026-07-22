-- Immutable search snapshots need a commit-order boundary independent of
-- event/index timestamps. The single Open Splunk writer assigns one sequence
-- to every synchronously committed batch and captures max(visibility_seq)
-- when a search job starts.

ALTER TABLE open_splunk.events
    ADD COLUMN IF NOT EXISTS `visibility_seq` UInt64 DEFAULT 0 CODEC(Delta, ZSTD(1))
    AFTER `batch_sequence`;

-- Existing rows read the migration default (0), but every post-upgrade writer
-- must supply a positive reservation from the durable SQLite sequencer.
ALTER TABLE open_splunk.events
    ADD CONSTRAINT IF NOT EXISTS visibility_seq_is_positive
    CHECK `visibility_seq` > 0;

ALTER TABLE open_splunk.events
    ADD INDEX IF NOT EXISTS idx_visibility_seq `visibility_seq` TYPE minmax GRANULARITY 1;

INSERT INTO open_splunk.schema_migrations (`version`, `name`, `applied_at`)
SELECT 2, 'add_visibility_sequence', now64(3)
WHERE NOT EXISTS
(
    SELECT 1
    FROM open_splunk.schema_migrations
    WHERE `version` = 2
);
