-- Retain the final bounded field of KnowledgeSnapshotRef in the immutable
-- search-attempt journal. Legacy five-field rows did not persist lookup
-- provenance, including for lookup-bearing snapshots, so their count remains
-- NULL/unknown. Every new tuple carries an exact count, including zero.

DROP TRIGGER search_attempt_audit_event_update_is_forbidden;
DROP TRIGGER search_attempt_audit_event_knowledge_snapshot_is_complete;

ALTER TABLE search_attempt_audit_events
ADD COLUMN knowledge_snapshot_lookup_asset_count INTEGER CHECK (
    knowledge_snapshot_lookup_asset_count IS NULL
    OR knowledge_snapshot_lookup_asset_count BETWEEN 0 AND 16
);

CREATE TRIGGER search_attempt_audit_event_knowledge_snapshot_is_complete
BEFORE INSERT ON search_attempt_audit_events
WHEN NOT (
    (
        NEW.knowledge_snapshot_sha256 IS NULL
        AND NEW.knowledge_snapshot_tenant_catalog_revision IS NULL
        AND NEW.knowledge_snapshot_tenant_catalog_state_token IS NULL
        AND NEW.knowledge_snapshot_object_count IS NULL
        AND NEW.knowledge_snapshot_compiler_compatibility_version IS NULL
        AND NEW.knowledge_snapshot_lookup_asset_count IS NULL
    )
    OR (
        NEW.knowledge_snapshot_sha256 IS NOT NULL
        AND NEW.knowledge_snapshot_tenant_catalog_revision IS NOT NULL
        AND NEW.knowledge_snapshot_tenant_catalog_state_token IS NOT NULL
        AND NEW.knowledge_snapshot_object_count IS NOT NULL
        AND NEW.knowledge_snapshot_compiler_compatibility_version = '0.1'
        AND NEW.knowledge_snapshot_lookup_asset_count IS NULL
    )
    OR (
        NEW.knowledge_snapshot_sha256 IS NOT NULL
        AND NEW.knowledge_snapshot_tenant_catalog_revision IS NOT NULL
        AND NEW.knowledge_snapshot_tenant_catalog_state_token IS NOT NULL
        AND NEW.knowledge_snapshot_object_count IS NOT NULL
        AND NEW.knowledge_snapshot_compiler_compatibility_version IS NOT NULL
        AND NEW.knowledge_snapshot_lookup_asset_count IS NOT NULL
    )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'search-attempt audit knowledge snapshot must be absent, legacy five-field, or exact'
    );
END;

CREATE TRIGGER search_attempt_audit_event_update_is_forbidden
BEFORE UPDATE ON search_attempt_audit_events
BEGIN
    SELECT RAISE(ABORT, 'search-attempt audit events cannot be updated');
END;
