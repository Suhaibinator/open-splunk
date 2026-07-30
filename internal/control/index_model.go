package control

// indexRecord is the explicit GORM representation of the indexes table.
//
// Versioned SQL migrations remain the schema authority because SQLite STRICT
// tables, immutable-name triggers, and retention-alignment triggers cannot be
// represented completely by GORM tags. Keep this model's ordered columns,
// keys, and physical scalar types in sync with 0001_control_plane.sql and
// subsequent index migrations.
type indexRecord struct {
	IndexID                      string     `gorm:"column:index_id;type:text;primaryKey;not null;index:indexes_name_id_idx,priority:2;index:indexes_created_id_idx,priority:2;index:indexes_updated_id_idx,priority:2"`
	Version                      int64      `gorm:"column:version;type:integer;not null;check:indexes_version_positive,version >= 1"`
	Name                         string     `gorm:"column:name;type:text;not null;unique;index:indexes_state_name_idx,priority:2;index:indexes_name_id_idx,priority:1"`
	DisplayName                  string     `gorm:"column:display_name;type:text;not null"`
	Description                  string     `gorm:"column:description;type:text;not null"`
	RetentionNanoseconds         int64      `gorm:"column:retention_nanoseconds;type:integer;not null;check:indexes_retention_nonnegative,retention_nanoseconds >= 0"`
	IngestionEnabled             int64      `gorm:"column:ingestion_enabled;type:integer;not null;check:indexes_ingestion_enabled_boolean,ingestion_enabled IN (0, 1)"`
	SearchEnabled                int64      `gorm:"column:search_enabled;type:integer;not null;check:indexes_search_enabled_boolean,search_enabled IN (0, 1)"`
	DefaultSourcetype            string     `gorm:"column:default_sourcetype;type:text;not null"`
	MaxEventBytes                int64      `gorm:"column:max_event_bytes;type:integer;not null;check:indexes_max_event_bytes_nonnegative,max_event_bytes >= 0"`
	MaxFieldCount                int64      `gorm:"column:max_field_count;type:integer;not null;check:indexes_max_field_count_nonnegative,max_field_count >= 0"`
	MaxNestingDepth              int64      `gorm:"column:max_nesting_depth;type:integer;not null;check:indexes_max_nesting_depth_nonnegative,max_nesting_depth >= 0"`
	MaximumFutureSkewNanoseconds int64      `gorm:"column:maximum_future_skew_nanoseconds;type:integer;not null;check:indexes_future_skew_nonnegative,maximum_future_skew_nanoseconds >= 0"`
	MaximumEventAgeNanoseconds   int64      `gorm:"column:maximum_event_age_nanoseconds;type:integer;not null;check:indexes_event_age_nonnegative,maximum_event_age_nanoseconds >= 0"`
	State                        IndexState `gorm:"column:state;type:text;not null;index:indexes_state_name_idx,priority:1"`
	CreatedAtUnixMicro           int64      `gorm:"column:created_at_unix_micro;type:integer;not null;index:indexes_created_id_idx,priority:1"`
	UpdatedAtUnixMicro           int64      `gorm:"column:updated_at_unix_micro;type:integer;not null;check:indexes_update_not_before_create,updated_at_unix_micro >= created_at_unix_micro;index:indexes_updated_id_idx,priority:1"`
}

func (indexRecord) TableName() string {
	return "indexes"
}

// indexCatalogStateRecord is the singleton bound/revision marker introduced
// by 0020_index_catalog_bounds.sql. SQL triggers, not application callbacks,
// update it in the same transaction as every catalog mutation.
type indexCatalogStateRecord struct {
	SingletonID   int64 `gorm:"column:singleton_id;type:integer;primaryKey;not null;check:index_catalog_state_singleton,singleton_id = 1"`
	Revision      int64 `gorm:"column:revision;type:integer;not null;check:index_catalog_state_revision,revision BETWEEN 1 AND 9223372036854775807"`
	PhysicalCount int64 `gorm:"column:physical_count;type:integer;not null;check:index_catalog_state_count,physical_count BETWEEN 0 AND 1024"`
}

func (indexCatalogStateRecord) TableName() string {
	return "index_catalog_state"
}

// indexDeletionTombstoneRecord is the GORM representation of the terminal
// KEEP_DATA deletion marker introduced by 0016_index_deletion_tombstones.sql.
// The archived indexes row remains the source of truth for name reservation
// and existing foreign-key references.
type indexDeletionTombstoneRecord struct {
	IndexID            string `gorm:"column:index_id;type:text;primaryKey;not null"`
	Name               string `gorm:"column:name;type:text;not null"`
	DeletedVersion     int64  `gorm:"column:deleted_version;type:integer;not null;check:index_deletion_tombstones_version_positive,deleted_version >= 1"`
	DeletedAtUnixMicro int64  `gorm:"column:deleted_at_unix_micro;type:integer;not null;check:index_deletion_tombstones_timestamp_positive,deleted_at_unix_micro > 0"`
}

func (indexDeletionTombstoneRecord) TableName() string {
	return "index_deletion_tombstones"
}

// indexDeletionOperationRecord is the GORM representation of the immutable
// DELETE_DATA request admitted by 0017_index_deletion_operations.sql. The
// operation snapshots its trusted tenant and archived index generation; every
// row is a restartable outstanding-work marker.
type indexDeletionOperationRecord struct {
	DeletionOperationID  string `gorm:"column:deletion_operation_id;type:text;primaryKey;not null;index:index_deletion_operations_created_id_idx,priority:2;check:index_deletion_operations_id_byte_length,length(CAST(deletion_operation_id AS BLOB)) BETWEEN 1 AND 128"`
	IndexID              string `gorm:"column:index_id;type:text;not null;unique"`
	IndexName            string `gorm:"column:index_name;type:text;not null"`
	TenantID             string `gorm:"column:tenant_id;type:text;not null;check:index_deletion_operations_tenant_id_byte_length,length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255"`
	ArchivedIndexVersion int64  `gorm:"column:archived_index_version;type:integer;not null;check:index_deletion_operations_archived_version_supported,archived_index_version >= 1 AND archived_index_version < 9223372036854775807"`
	CreatedAtUnixMicro   int64  `gorm:"column:created_at_unix_micro;type:integer;not null;index:index_deletion_operations_created_id_idx,priority:1;check:index_deletion_operations_created_at_positive,created_at_unix_micro > 0"`
}

func (indexDeletionOperationRecord) TableName() string {
	return "index_deletion_operations"
}

// indexDeletionMutationAttemptRecord is the GORM representation of the stable
// correlation marker persisted before the first ClickHouse mutation side
// effect. The target fields bind retries to one tenant and one physical table
// UUID; live mutation progress remains in ClickHouse system tables.
type indexDeletionMutationAttemptRecord struct {
	DeletionOperationID string `gorm:"column:deletion_operation_id;type:text;primaryKey;not null"`
	CorrelationID       string `gorm:"column:correlation_id;type:text;not null;unique;check:index_deletion_mutation_attempts_correlation_id_byte_length,length(CAST(correlation_id AS BLOB)) BETWEEN 1 AND 128"`
	TenantID            string `gorm:"column:tenant_id;type:text;not null;check:index_deletion_mutation_attempts_tenant_id_byte_length,length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255"`
	ClickHouseDatabase  string `gorm:"column:clickhouse_database;type:text;not null;check:index_deletion_mutation_attempts_database_byte_length,length(CAST(clickhouse_database AS BLOB)) BETWEEN 1 AND 255"`
	ClickHouseTable     string `gorm:"column:clickhouse_table;type:text;not null;check:index_deletion_mutation_attempts_table_byte_length,length(CAST(clickhouse_table AS BLOB)) BETWEEN 1 AND 255"`
	ClickHouseTableUUID string `gorm:"column:clickhouse_table_uuid;type:text;not null"`
	ProtocolVersion     int64  `gorm:"column:protocol_version;type:integer;not null;check:index_deletion_mutation_attempts_protocol_supported,protocol_version = 1"`
	CreatedAtUnixMicro  int64  `gorm:"column:created_at_unix_micro;type:integer;not null;check:index_deletion_mutation_attempts_created_at_positive,created_at_unix_micro > 0"`
}

func (indexDeletionMutationAttemptRecord) TableName() string {
	return "index_deletion_mutation_attempts"
}

// indexDataDeletionCompletionRecord is the immutable terminal audit introduced
// by 0019_index_data_deletion_completions.sql. It copies every operation and
// mutation-attempt field needed to reconstruct the native request after the
// outstanding rows have been consumed.
type indexDataDeletionCompletionRecord struct {
	DeletionOperationID         string `gorm:"column:deletion_operation_id;type:text;primaryKey;not null"`
	CorrelationID               string `gorm:"column:correlation_id;type:text;not null;unique"`
	IndexID                     string `gorm:"column:index_id;type:text;not null;unique"`
	IndexName                   string `gorm:"column:index_name;type:text;not null"`
	ArchivedIndexVersion        int64  `gorm:"column:archived_index_version;type:integer;not null"`
	DeletingIndexVersion        int64  `gorm:"column:deleting_index_version;type:integer;not null"`
	TenantID                    string `gorm:"column:tenant_id;type:text;not null"`
	ClickHouseDatabase          string `gorm:"column:clickhouse_database;type:text;not null"`
	ClickHouseTable             string `gorm:"column:clickhouse_table;type:text;not null"`
	ClickHouseTableUUID         string `gorm:"column:clickhouse_table_uuid;type:text;not null"`
	ProtocolVersion             int64  `gorm:"column:protocol_version;type:integer;not null"`
	OperationCreatedAtUnixMicro int64  `gorm:"column:operation_created_at_unix_micro;type:integer;not null"`
	AttemptCreatedAtUnixMicro   int64  `gorm:"column:attempt_created_at_unix_micro;type:integer;not null"`
	CompletedAtUnixMicro        int64  `gorm:"column:completed_at_unix_micro;type:integer;not null"`
}

func (indexDataDeletionCompletionRecord) TableName() string {
	return "index_data_deletion_completions"
}
