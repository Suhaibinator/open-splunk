-- Separates successive async emissions of one notification source (for
-- example an organization invitation re-sent to a newly created account).
-- AutoMigrate adds the column but never redefines an existing index.

-- +goose Up
ALTER TABLE gocommon_notification_emissions ADD COLUMN IF NOT EXISTS emission_key varchar(64) NOT NULL DEFAULT '';
DROP INDEX IF EXISTS idx_notification_emission_source;
CREATE UNIQUE INDEX idx_notification_emission_source
  ON gocommon_notification_emissions (organization_id, type, source_key, producer, emission_key);
