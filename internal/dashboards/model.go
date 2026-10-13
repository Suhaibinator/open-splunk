package dashboards

// dashboardRecord is the explicit GORM representation of dashboards. The
// versioned PostgreSQL migrations remain the sole schema authority; never use
// AutoMigrate for this model.
type dashboardRecord struct {
	OrganizationID     int64  `gorm:"column:organization_id;type:bigint;not null"`
	DashboardID        string `gorm:"column:dashboard_id;primaryKey"`
	Version            int64  `gorm:"column:version"`
	Name               string `gorm:"column:name"`
	AppID              string `gorm:"column:app_id"`
	TenantID           string `gorm:"column:tenant_id"`
	OwnerID            string `gorm:"column:owner_id"`
	SharingScope       int64  `gorm:"column:sharing_scope"`
	DefinitionProto    []byte `gorm:"column:definition_proto"`
	CreatedAtUnixMicro int64  `gorm:"column:created_at_unix_micro"`
	UpdatedAtUnixMicro int64  `gorm:"column:updated_at_unix_micro"`
	SourceIndexNames   []byte `gorm:"column:source_index_names;type:jsonb;not null"`
}

func (dashboardRecord) TableName() string { return "dashboards" }
