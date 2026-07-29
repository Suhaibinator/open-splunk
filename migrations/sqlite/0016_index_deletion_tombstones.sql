-- KEEP_DATA index deletion is terminal at the control-plane boundary while
-- retaining the archived index row. Keeping that row preserves token/app
-- foreign keys and permanently reserves the canonical index name, so retained
-- ClickHouse events can never become visible through a newly created index
-- with the same name.

-- Earlier builds exposed the coordinator-owned intermediate state through the
-- generic state API. Recover any such stranded rows before that path is
-- closed; the optimistic version bump makes the recovery visible to clients.
UPDATE indexes
SET state = 'archived',
    version = CASE
        WHEN version < 9223372036854775807 THEN version + 1
        ELSE version
    END,
    updated_at_unix_micro = CASE
        WHEN updated_at_unix_micro < 9223372036854775807
            THEN updated_at_unix_micro + 1
        ELSE updated_at_unix_micro
    END
WHERE state = 'deleting';

CREATE TABLE index_deletion_tombstones (
    index_id TEXT PRIMARY KEY NOT NULL
        REFERENCES indexes (index_id) ON DELETE RESTRICT,
    name TEXT NOT NULL COLLATE BINARY,
    deleted_version INTEGER NOT NULL CHECK (deleted_version >= 1),
    deleted_at_unix_micro INTEGER NOT NULL CHECK (deleted_at_unix_micro > 0)
) STRICT;

CREATE TRIGGER index_deletion_tombstone_insert_is_valid
BEFORE INSERT ON index_deletion_tombstones
WHEN NOT EXISTS (
    SELECT 1
    FROM indexes
    WHERE indexes.index_id = NEW.index_id
      AND indexes.name = NEW.name
      AND indexes.version = NEW.deleted_version
      AND indexes.state = 'archived'
)
BEGIN
    SELECT RAISE(ABORT, 'index deletion tombstone must match an archived index');
END;

CREATE TRIGGER tombstoned_index_update_is_forbidden
BEFORE UPDATE ON indexes
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_tombstones
    WHERE index_deletion_tombstones.index_id = OLD.index_id
)
BEGIN
    SELECT RAISE(ABORT, 'tombstoned index is immutable');
END;

CREATE TRIGGER tombstoned_index_delete_is_forbidden
BEFORE DELETE ON indexes
WHEN EXISTS (
    SELECT 1
    FROM index_deletion_tombstones
    WHERE index_deletion_tombstones.index_id = OLD.index_id
)
BEGIN
    SELECT RAISE(ABORT, 'tombstoned index cannot be deleted');
END;

CREATE TRIGGER index_deletion_tombstone_update_is_forbidden
BEFORE UPDATE ON index_deletion_tombstones
BEGIN
    SELECT RAISE(ABORT, 'index deletion tombstone is immutable');
END;

CREATE TRIGGER index_deletion_tombstone_delete_is_forbidden
BEFORE DELETE ON index_deletion_tombstones
BEGIN
    SELECT RAISE(ABORT, 'index deletion tombstone cannot be deleted');
END;
