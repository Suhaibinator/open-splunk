package control

// indexRecord is the explicit GORM representation of the indexes table.
//
// Versioned SQL migrations remain the schema authority because SQLite STRICT
// tables, immutable-name triggers, and retention-alignment triggers cannot be
// represented completely by GORM tags. Keep this model's ordered columns,
// keys, and physical scalar types in sync with 0001_control_plane.sql and
// subsequent index migrations.
type indexRecord struct {
	IndexID                      string     `gorm:"column:index_id;type:text;primaryKey;not null"`
	Version                      int64      `gorm:"column:version;type:integer;not null;check:indexes_version_positive,version >= 1"`
	Name                         string     `gorm:"column:name;type:text;not null;unique;index:indexes_state_name_idx,priority:2"`
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
	CreatedAtUnixMicro           int64      `gorm:"column:created_at_unix_micro;type:integer;not null"`
	UpdatedAtUnixMicro           int64      `gorm:"column:updated_at_unix_micro;type:integer;not null;check:indexes_update_not_before_create,updated_at_unix_micro >= created_at_unix_micro"`
}

func (indexRecord) TableName() string {
	return "indexes"
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
// operation snapshots the archived index generation; every row is a
// restartable outstanding-work marker.
type indexDeletionOperationRecord struct {
	DeletionOperationID  string `gorm:"column:deletion_operation_id;type:text;primaryKey;not null;index:index_deletion_operations_created_id_idx,priority:2;check:index_deletion_operations_id_byte_length,length(CAST(deletion_operation_id AS BLOB)) BETWEEN 1 AND 128"`
	IndexID              string `gorm:"column:index_id;type:text;not null;unique"`
	IndexName            string `gorm:"column:index_name;type:text;not null"`
	ArchivedIndexVersion int64  `gorm:"column:archived_index_version;type:integer;not null;check:index_deletion_operations_archived_version_supported,archived_index_version >= 1 AND archived_index_version < 9223372036854775807"`
	CreatedAtUnixMicro   int64  `gorm:"column:created_at_unix_micro;type:integer;not null;index:index_deletion_operations_created_id_idx,priority:1;check:index_deletion_operations_created_at_positive,created_at_unix_micro > 0"`
}

func (indexDeletionOperationRecord) TableName() string {
	return "index_deletion_operations"
}
