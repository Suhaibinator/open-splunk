-- Reusable search intent. Generated SQL and resolved authorization are never
-- persisted here; the protobuf blob contains only SavedSearchDefinition.

CREATE TABLE saved_searches (
    saved_search_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
    version INTEGER NOT NULL CHECK (version >= 1),
    name TEXT NOT NULL COLLATE BINARY,
    app_id TEXT NOT NULL COLLATE BINARY,
    owner_id TEXT NOT NULL COLLATE BINARY,
    sharing_scope INTEGER NOT NULL CHECK (sharing_scope BETWEEN 1 AND 3),
    definition_proto BLOB NOT NULL CHECK (length(definition_proto) BETWEEN 1 AND 262144),
    created_at_unix_micro INTEGER NOT NULL,
    updated_at_unix_micro INTEGER NOT NULL,
    CHECK (length(saved_search_id) BETWEEN 1 AND 128),
    CHECK (length(name) BETWEEN 1 AND 255),
    CHECK (length(app_id) <= 255),
    CHECK (length(owner_id) <= 255),
    CHECK (updated_at_unix_micro >= created_at_unix_micro),
    UNIQUE (owner_id, app_id, name)
) STRICT;

CREATE INDEX saved_searches_app_name_id_idx
    ON saved_searches (app_id, name, saved_search_id);

CREATE INDEX saved_searches_updated_idx
    ON saved_searches (updated_at_unix_micro DESC, saved_search_id);
