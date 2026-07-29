-- Support bounded database-wide discovery of expired terminal history without
-- scanning every tenant/owner prefix.

CREATE INDEX search_history_created_idx
    ON search_history (created_at_unix_micro, search_job_id);

-- A transactional per-owner cardinality avoids scanning or OFFSET-walking a
-- potentially million-row owner history while SQLite owns the write lock.
-- Triggers keep every insert/delete path, including filtered clears,
-- crash-safe.

CREATE TABLE search_history_owner_counts (
    tenant_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    terminal_count INTEGER NOT NULL CHECK (terminal_count > 0),
    PRIMARY KEY (tenant_id, owner_id),
    CHECK (length(tenant_id) BETWEEN 1 AND 1024),
    CHECK (length(owner_id) BETWEEN 1 AND 255)
) STRICT;

INSERT INTO search_history_owner_counts (tenant_id, owner_id, terminal_count)
SELECT tenant_id, owner_id, COUNT(*)
FROM search_history
GROUP BY tenant_id, owner_id;

CREATE TRIGGER search_history_owner_count_after_insert
AFTER INSERT ON search_history
BEGIN
    INSERT INTO search_history_owner_counts (
        tenant_id,
        owner_id,
        terminal_count
    ) VALUES (
        NEW.tenant_id,
        NEW.owner_id,
        1
    )
    ON CONFLICT (tenant_id, owner_id) DO UPDATE
    SET terminal_count = terminal_count + 1;
END;

CREATE TRIGGER search_history_owner_count_after_delete
AFTER DELETE ON search_history
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM search_history_owner_counts
            WHERE tenant_id = OLD.tenant_id
              AND owner_id = OLD.owner_id
        )
        THEN RAISE(ABORT, 'search-history owner count is missing')
    END;

    DELETE FROM search_history_owner_counts
    WHERE tenant_id = OLD.tenant_id
      AND owner_id = OLD.owner_id
      AND terminal_count = 1;

    UPDATE search_history_owner_counts
    SET terminal_count = terminal_count - 1
    WHERE tenant_id = OLD.tenant_id
      AND owner_id = OLD.owner_id;
END;
