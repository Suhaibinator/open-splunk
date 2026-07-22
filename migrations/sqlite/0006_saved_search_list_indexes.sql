-- Owner is the leading authorization predicate for every saved-search list.
-- Match the three unfiltered keyset sort orders so ordinary listings avoid a
-- full owner scan and temporary sort B-tree.

CREATE INDEX saved_searches_owner_name_id_idx
    ON saved_searches (owner_id, name, saved_search_id);

CREATE INDEX saved_searches_owner_created_id_idx
    ON saved_searches (owner_id, created_at_unix_micro, saved_search_id);

CREATE INDEX saved_searches_owner_updated_id_idx
    ON saved_searches (owner_id, updated_at_unix_micro, saved_search_id);
