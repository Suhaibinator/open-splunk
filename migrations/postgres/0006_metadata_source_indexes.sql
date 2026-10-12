-- Public metadata authority is filtered in SQL before totals and keyset pages.
CREATE FUNCTION os_source_index_names_valid(value JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE element JSONB; name TEXT;
BEGIN
 IF value IS NULL OR jsonb_typeof(value)<>'array' THEN RETURN FALSE; END IF;
 IF jsonb_array_length(value)>2048 THEN RETURN FALSE; END IF;
 FOR element IN SELECT jsonb_array_elements(value) LOOP
  IF jsonb_typeof(element)<>'string' THEN RETURN FALSE; END IF;
  name:=element#>>'{}';
  IF octet_length(name) NOT BETWEEN 1 AND 255 OR name!~'^[a-z0-9_*?][a-z0-9_*?-]*$' OR (position('kvstore' IN name)>0 AND name NOT IN ('kvstore_unresolved_source','kvstore_app_defaults_source')) THEN RETURN FALSE; END IF;
 END LOOP;
 RETURN TRUE;
END;
$$;
ALTER TABLE saved_searches ADD COLUMN source_index_names JSONB NOT NULL CHECK(os_source_index_names_valid(source_index_names));
ALTER TABLE dashboards ADD COLUMN source_index_names JSONB NOT NULL CHECK(os_source_index_names_valid(source_index_names));
ALTER TABLE search_history ADD COLUMN source_index_names JSONB NOT NULL CHECK(os_source_index_names_valid(source_index_names));
ALTER TABLE search_history_pending ADD COLUMN source_index_names JSONB NOT NULL CHECK(os_source_index_names_valid(source_index_names));
CREATE INDEX saved_searches_source_indexes ON saved_searches USING GIN(source_index_names);
CREATE INDEX dashboards_source_indexes ON dashboards USING GIN(source_index_names);
CREATE INDEX search_history_source_indexes ON search_history USING GIN(source_index_names);
CREATE INDEX search_history_pending_source_indexes ON search_history_pending USING GIN(source_index_names);

CREATE INDEX saved_searches_organization_name_page ON saved_searches(organization_id,name COLLATE "C",saved_search_id COLLATE "C");
CREATE INDEX saved_searches_organization_created_page ON saved_searches(organization_id,created_at_unix_micro,saved_search_id COLLATE "C");
CREATE INDEX saved_searches_organization_updated_page ON saved_searches(organization_id,updated_at_unix_micro,saved_search_id COLLATE "C");
