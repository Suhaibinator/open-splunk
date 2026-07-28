package control

// appRecord is the explicit GORM representation of app_workspaces.
//
// Versioned SQL migrations remain authoritative. In particular, the STRICT
// table checks, immutable identity trigger, catalog-revision triggers, and
// saved-search dependency triggers cannot be expressed completely by GORM
// tags. Never use AutoMigrate for this model.
type appRecord struct {
	AppID                   string   `gorm:"column:app_id;type:text;primaryKey;not null;uniqueIndex:app_workspaces_tenant_id_key,priority:2;index:app_workspaces_tenant_display_id_idx,priority:3;index:app_workspaces_tenant_created_id_idx,priority:3;index:app_workspaces_tenant_updated_id_idx,priority:3"`
	TenantID                string   `gorm:"column:tenant_id;type:text;not null;uniqueIndex:app_workspaces_tenant_slug_key,priority:1;uniqueIndex:app_workspaces_tenant_id_key,priority:1;index:app_workspaces_tenant_display_id_idx,priority:1;index:app_workspaces_tenant_created_id_idx,priority:1;index:app_workspaces_tenant_updated_id_idx,priority:1"`
	Version                 int64    `gorm:"column:version;type:integer;not null;check:app_workspaces_version_positive,version >= 1"`
	Slug                    string   `gorm:"column:slug;type:text;not null;uniqueIndex:app_workspaces_tenant_slug_key,priority:2"`
	DisplayName             string   `gorm:"column:display_name;type:text;not null;index:app_workspaces_tenant_display_id_idx,priority:2"`
	Description             string   `gorm:"column:description;type:text;not null"`
	DefaultTimeRangePresent int64    `gorm:"column:default_time_range_present;type:integer;not null;check:app_workspaces_default_time_range_present_boolean,default_time_range_present IN (0, 1)"`
	DefaultEarliest         *string  `gorm:"column:default_earliest;type:text"`
	DefaultLatest           *string  `gorm:"column:default_latest;type:text"`
	DefaultTimezone         *string  `gorm:"column:default_timezone;type:text"`
	State                   AppState `gorm:"column:state;type:text;not null"`
	CreatedAtUnixMicro      int64    `gorm:"column:created_at_unix_micro;type:integer;not null;index:app_workspaces_tenant_created_id_idx,priority:2"`
	UpdatedAtUnixMicro      int64    `gorm:"column:updated_at_unix_micro;type:integer;not null;index:app_workspaces_tenant_updated_id_idx,priority:2"`
	ArchivedAtUnixMicro     *int64   `gorm:"column:archived_at_unix_micro;type:integer"`
}

func (appRecord) TableName() string {
	return "app_workspaces"
}

// appDefaultIndexRecord represents the normalized many-to-many default-index
// membership. Names are intentionally not duplicated in this table: reads
// join indexes and order by its immutable normalized name.
type appDefaultIndexRecord struct {
	TenantID string `gorm:"column:tenant_id;type:text;primaryKey;not null;index:app_default_indexes_index_app_idx,priority:2"`
	AppID    string `gorm:"column:app_id;type:text;primaryKey;not null;index:app_default_indexes_index_app_idx,priority:3"`
	IndexID  string `gorm:"column:index_id;type:text;primaryKey;not null;index:app_default_indexes_index_app_idx,priority:1"`
}

func (appDefaultIndexRecord) TableName() string {
	return "app_default_indexes"
}

type appCatalogRevisionRecord struct {
	TenantID string `gorm:"column:tenant_id;type:text;primaryKey;not null"`
	Revision int64  `gorm:"column:revision;type:integer;not null;check:app_catalog_revisions_revision_positive,revision >= 1"`
}

func (appCatalogRevisionRecord) TableName() string {
	return "app_catalog_revisions"
}
