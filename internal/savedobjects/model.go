package savedobjects

// savedSearchRecord is the explicit GORM representation of saved_searches.
//
// Versioned SQL migrations remain the sole schema authority. In particular,
// SQLite STRICT tables, binary collations, and the app-workspace dependency
// triggers cannot be represented completely by GORM tags. Never use
// AutoMigrate for this model.
type savedSearchRecord struct {
	SavedSearchID      string `gorm:"column:saved_search_id;type:text;primaryKey;not null;check:saved_searches_id_length,length(saved_search_id) BETWEEN 1 AND 128;index:saved_searches_app_name_id_idx,priority:3;index:saved_searches_updated_idx,priority:2;index:saved_searches_owner_name_id_idx,priority:3;index:saved_searches_owner_created_id_idx,priority:3;index:saved_searches_owner_updated_id_idx,priority:3"`
	Version            int64  `gorm:"column:version;type:integer;not null;check:saved_searches_version_positive,version >= 1"`
	Name               string `gorm:"column:name;type:text;not null;check:saved_searches_name_length,length(name) BETWEEN 1 AND 255;uniqueIndex:saved_searches_owner_app_name_key,priority:3;index:saved_searches_app_name_id_idx,priority:2;index:saved_searches_owner_name_id_idx,priority:2"`
	AppID              string `gorm:"column:app_id;type:text;not null;check:saved_searches_app_id_length,length(app_id) <= 255;uniqueIndex:saved_searches_owner_app_name_key,priority:2;index:saved_searches_app_name_id_idx,priority:1"`
	OwnerID            string `gorm:"column:owner_id;type:text;not null;check:saved_searches_owner_id_length,length(owner_id) <= 255;uniqueIndex:saved_searches_owner_app_name_key,priority:1;index:saved_searches_owner_name_id_idx,priority:1;index:saved_searches_owner_created_id_idx,priority:1;index:saved_searches_owner_updated_id_idx,priority:1"`
	SharingScope       int64  `gorm:"column:sharing_scope;type:integer;not null;check:saved_searches_sharing_scope_range,sharing_scope BETWEEN 1 AND 3"`
	DefinitionProto    []byte `gorm:"column:definition_proto;type:blob;not null;check:saved_searches_definition_length,length(definition_proto) BETWEEN 1 AND 262144"`
	CreatedAtUnixMicro int64  `gorm:"column:created_at_unix_micro;type:integer;not null;index:saved_searches_owner_created_id_idx,priority:2"`
	UpdatedAtUnixMicro int64  `gorm:"column:updated_at_unix_micro;type:integer;not null;check:saved_searches_update_not_before_create,updated_at_unix_micro >= created_at_unix_micro;index:saved_searches_updated_idx,priority:1,sort:desc;index:saved_searches_owner_updated_id_idx,priority:2"`
}

func (savedSearchRecord) TableName() string {
	return "saved_searches"
}
