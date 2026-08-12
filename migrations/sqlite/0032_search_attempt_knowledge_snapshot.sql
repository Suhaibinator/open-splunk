-- Extend the payload-free search-attempt journal with the compact identity of
-- the immutable knowledge snapshot admitted for an attempt. All five columns
-- are nullable together so rows written before knowledge-aware admission keep
-- their exact legacy meaning. A present tuple contains no definitions or
-- object inventory.

ALTER TABLE search_attempt_audit_events
ADD COLUMN knowledge_snapshot_sha256 BLOB CHECK (
    knowledge_snapshot_sha256 IS NULL
    OR (
        typeof(knowledge_snapshot_sha256) = 'blob'
        AND length(knowledge_snapshot_sha256) = 32
    )
);

ALTER TABLE search_attempt_audit_events
ADD COLUMN knowledge_snapshot_tenant_catalog_revision INTEGER CHECK (
    knowledge_snapshot_tenant_catalog_revision IS NULL
    OR knowledge_snapshot_tenant_catalog_revision
        BETWEEN 0 AND 9223372036854775806
);

ALTER TABLE search_attempt_audit_events
ADD COLUMN knowledge_snapshot_tenant_catalog_state_token BLOB CHECK (
    knowledge_snapshot_tenant_catalog_state_token IS NULL
    OR (
        typeof(knowledge_snapshot_tenant_catalog_state_token) = 'blob'
        AND length(knowledge_snapshot_tenant_catalog_state_token) = 32
    )
);

ALTER TABLE search_attempt_audit_events
ADD COLUMN knowledge_snapshot_object_count INTEGER CHECK (
    knowledge_snapshot_object_count IS NULL
    OR knowledge_snapshot_object_count BETWEEN 0 AND 256
);

ALTER TABLE search_attempt_audit_events
ADD COLUMN knowledge_snapshot_compiler_compatibility_version TEXT
    COLLATE BINARY CHECK (
        knowledge_snapshot_compiler_compatibility_version IS NULL
        OR (
            typeof(knowledge_snapshot_compiler_compatibility_version) = 'text'
            AND length(CAST(
                knowledge_snapshot_compiler_compatibility_version AS BLOB
            )) BETWEEN 1 AND 128
            AND instr(CAST(
                knowledge_snapshot_compiler_compatibility_version AS BLOB
            ), X'00') = 0
            -- Canonical boundary whitespace is exactly ASCII SPACE or
            -- TAB/LF/VT/FF/CR. The C0/C1 guard below also rejects those
            -- controls (and every other control) in the interior.
            AND substr(
                knowledge_snapshot_compiler_compatibility_version, 1, 1
            ) NOT IN (
                ' ', char(9), char(10), char(11), char(12), char(13)
            )
            AND substr(
                knowledge_snapshot_compiler_compatibility_version, -1, 1
            ) NOT IN (
                ' ', char(9), char(10), char(11), char(12), char(13)
            )
            AND knowledge_snapshot_compiler_compatibility_version NOT GLOB (
                '*[' || char(1) || '-' || char(31)
                || char(127) || '-' || char(159) || ']*'
            )
        )
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
    )
    OR (
        NEW.knowledge_snapshot_sha256 IS NOT NULL
        AND NEW.knowledge_snapshot_tenant_catalog_revision IS NOT NULL
        AND NEW.knowledge_snapshot_tenant_catalog_state_token IS NOT NULL
        AND NEW.knowledge_snapshot_object_count IS NOT NULL
        AND NEW.knowledge_snapshot_compiler_compatibility_version IS NOT NULL
    )
)
BEGIN
    SELECT RAISE(
        ABORT,
        'search-attempt audit knowledge snapshot must be wholly absent or present'
    );
END;
