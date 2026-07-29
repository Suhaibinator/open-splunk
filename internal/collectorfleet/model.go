package collectorfleet

// These explicit GORM models mirror migrations/sqlite/0013_collector_fleet.sql.
// STRICT tables, composite foreign keys, WITHOUT ROWID storage, and triggers
// remain migration-owned and cannot be represented fully by GORM. Never call
// AutoMigrate for these models.

type fleetRecord struct {
	TenantID             string              `gorm:"column:tenant_id;type:text;primaryKey;priority:1;not null;index:collector_fleet_tenant_state_id_idx,priority:1;check:collector_fleet_tenant_id_bounded,length(CAST(tenant_id AS BLOB)) BETWEEN 1 AND 255 AND instr(tenant_id, char(0)) = 0"`
	CollectorID          string              `gorm:"column:collector_id;type:text;primaryKey;priority:2;not null;index:collector_fleet_tenant_state_id_idx,priority:3;check:collector_fleet_collector_id_canonical,length(collector_id) BETWEEN 1 AND 128 AND instr(collector_id, char(0)) = 0 AND substr(collector_id, 1, 1) GLOB '[A-Za-z0-9]' AND collector_id NOT GLOB '*[^A-Za-z0-9._:-]*'"`
	AdminVersion         int64               `gorm:"column:admin_version;type:integer;not null;check:collector_fleet_admin_version_positive,admin_version >= 1"`
	DisplayName          *string             `gorm:"column:display_name;type:text;check:collector_fleet_display_name_bounded,display_name IS NULL OR (length(CAST(display_name AS BLOB)) BETWEEN 1 AND 255 AND instr(display_name, char(0)) = 0)"`
	AdministrativeState  AdministrativeState `gorm:"column:administrative_state;type:text;not null;index:collector_fleet_tenant_state_id_idx,priority:2;check:collector_fleet_administrative_state_valid,administrative_state IN ('enabled','disabled')"`
	FirstSeenAtUnixMicro int64               `gorm:"column:first_seen_at_unix_micro;type:integer;not null;check:collector_fleet_first_seen_positive,first_seen_at_unix_micro BETWEEN 1 AND 253402300799999999"`
	UpdatedAtUnixMicro   int64               `gorm:"column:updated_at_unix_micro;type:integer;not null;check:collector_fleet_updated_at_valid,updated_at_unix_micro BETWEEN first_seen_at_unix_micro AND 253402300799999999"`
}

func (fleetRecord) TableName() string { return "collector_fleet" }

type runtimeRecord struct {
	TenantID                      string  `gorm:"column:tenant_id;type:text;primaryKey;priority:1;not null"`
	CollectorID                   string  `gorm:"column:collector_id;type:text;primaryKey;priority:2;not null"`
	TelemetryRevision             int64   `gorm:"column:telemetry_revision;type:integer;not null;check:collector_runtime_telemetry_revision_positive,telemetry_revision >= 1"`
	LeaseGeneration               int64   `gorm:"column:lease_generation;type:integer;not null;check:collector_runtime_lease_generation_positive,lease_generation >= 1"`
	BootEpoch                     *string `gorm:"column:boot_epoch;type:text;check:collector_runtime_boot_epoch_canonical,boot_epoch IS NULL OR (length(boot_epoch) BETWEEN 1 AND 128 AND instr(boot_epoch, char(0)) = 0 AND substr(boot_epoch, 1, 1) GLOB '[A-Za-z0-9]' AND boot_epoch NOT GLOB '*[^A-Za-z0-9._:-]*')"`
	StreamID                      *string `gorm:"column:stream_id;type:text;check:collector_runtime_stream_id_canonical,stream_id IS NULL OR (length(stream_id) BETWEEN 1 AND 128 AND instr(stream_id, char(0)) = 0 AND substr(stream_id, 1, 1) GLOB '[A-Za-z0-9]' AND stream_id NOT GLOB '*[^A-Za-z0-9._:-]*')"`
	ActiveInstanceID              *string `gorm:"column:active_instance_id;type:text;check:collector_runtime_instance_id_canonical,active_instance_id IS NULL OR (length(active_instance_id) BETWEEN 1 AND 128 AND instr(active_instance_id, char(0)) = 0 AND substr(active_instance_id, 1, 1) GLOB '[A-Za-z0-9]' AND active_instance_id NOT GLOB '*[^A-Za-z0-9._:-]*')"`
	ProtocolMajor                 int64   `gorm:"column:protocol_major;type:integer;not null;check:collector_runtime_protocol_major_valid,protocol_major BETWEEN 0 AND 4294967295"`
	ProtocolMinor                 int64   `gorm:"column:protocol_minor;type:integer;not null;check:collector_runtime_protocol_minor_valid,protocol_minor BETWEEN 0 AND 4294967295"`
	CollectorVersion              string  `gorm:"column:collector_version;type:text;not null;check:collector_runtime_metadata_bounded,length(CAST(collector_version AS BLOB)) <= 128 AND instr(collector_version, char(0)) = 0 AND length(CAST(hostname AS BLOB)) <= 255 AND instr(hostname, char(0)) = 0 AND length(CAST(operating_system AS BLOB)) <= 128 AND instr(operating_system, char(0)) = 0 AND length(CAST(architecture AS BLOB)) <= 128 AND instr(architecture, char(0)) = 0"`
	Hostname                      string  `gorm:"column:hostname;type:text;not null"`
	OperatingSystem               string  `gorm:"column:operating_system;type:text;not null"`
	Architecture                  string  `gorm:"column:architecture;type:text;not null;check:collector_runtime_active_lease_consistent,(boot_epoch IS NOT NULL AND stream_id IS NOT NULL AND active_instance_id IS NOT NULL AND disconnected_at_unix_micro IS NULL) OR (boot_epoch IS NULL AND stream_id IS NULL AND active_instance_id IS NULL AND disconnected_at_unix_micro IS NOT NULL)"`
	StartedAtUnixMicro            int64   `gorm:"column:started_at_unix_micro;type:integer;not null;check:collector_runtime_started_at_positive,started_at_unix_micro BETWEEN 1 AND 253402300799999999"`
	ConnectedAtUnixMicro          int64   `gorm:"column:connected_at_unix_micro;type:integer;not null;check:collector_runtime_connected_at_positive,connected_at_unix_micro BETWEEN 1 AND 253402300799999999"`
	LastSeenAtUnixMicro           int64   `gorm:"column:last_seen_at_unix_micro;type:integer;not null;check:collector_runtime_last_seen_valid,last_seen_at_unix_micro BETWEEN connected_at_unix_micro AND 253402300799999999"`
	DisconnectedAtUnixMicro       *int64  `gorm:"column:disconnected_at_unix_micro;type:integer;check:collector_runtime_disconnect_valid,disconnected_at_unix_micro IS NULL OR disconnected_at_unix_micro BETWEEN last_seen_at_unix_micro AND 253402300799999999"`
	ObservationSequence           int64   `gorm:"column:observation_sequence;type:integer;not null;check:collector_runtime_observation_sequence_nonnegative,observation_sequence >= 0"`
	ObservedAtUnixMicro           *int64  `gorm:"column:observed_at_unix_micro;type:integer;check:collector_runtime_observation_snapshot_valid,(observation_sequence = 0 AND observed_at_unix_micro IS NULL) OR (observation_sequence > 0 AND observed_at_unix_micro IS NOT NULL AND observed_at_unix_micro BETWEEN 1 AND 253402300799999999)"`
	LastAckedAtHelloSequence      *int64  `gorm:"column:last_acked_at_hello_sequence;type:integer;check:collector_runtime_hello_sequence_valid,last_acked_at_hello_sequence IS NULL OR last_acked_at_hello_sequence >= 0"`
	QueuedEvents                  int64   `gorm:"column:queued_events;type:integer;not null;check:collector_runtime_queued_events_nonnegative,queued_events >= 0"`
	QueuedBytes                   int64   `gorm:"column:queued_bytes;type:integer;not null;check:collector_runtime_queued_bytes_nonnegative,queued_bytes >= 0"`
	OldestEventAgeNanoseconds     *int64  `gorm:"column:oldest_event_age_nanoseconds;type:integer;check:collector_runtime_oldest_age_valid,oldest_event_age_nanoseconds IS NULL OR oldest_event_age_nanoseconds >= 0"`
	SentEventsTotal               int64   `gorm:"column:sent_events_total;type:integer;not null;check:collector_runtime_sent_events_nonnegative,sent_events_total >= 0"`
	AcknowledgedEventsTotal       int64   `gorm:"column:acknowledged_events_total;type:integer;not null;check:collector_runtime_acknowledged_events_nonnegative,acknowledged_events_total >= 0"`
	RetriedBatchesTotal           int64   `gorm:"column:retried_batches_total;type:integer;not null;check:collector_runtime_retried_batches_nonnegative,retried_batches_total >= 0"`
	RejectedEventsTotal           int64   `gorm:"column:rejected_events_total;type:integer;not null;check:collector_runtime_rejected_events_nonnegative,rejected_events_total >= 0"`
	DroppedEventsTotal            int64   `gorm:"column:dropped_events_total;type:integer;not null;check:collector_runtime_dropped_events_nonnegative,dropped_events_total >= 0"`
	LastSentBatchSequence         *int64  `gorm:"column:last_sent_batch_sequence;type:integer;check:collector_runtime_last_sent_sequence_valid,last_sent_batch_sequence IS NULL OR last_sent_batch_sequence >= 0"`
	LastAcknowledgedBatchSequence *int64  `gorm:"column:last_acknowledged_batch_sequence;type:integer;check:collector_runtime_last_acknowledged_sequence_valid,last_acknowledged_batch_sequence IS NULL OR last_acknowledged_batch_sequence >= 0"`
	ProcessResidentMemoryBytes    int64   `gorm:"column:process_resident_memory_bytes;type:integer;not null;check:collector_runtime_memory_nonnegative,process_resident_memory_bytes >= 0"`
	ProcessCPUPercent             float64 `gorm:"column:process_cpu_percent;type:real;not null;check:collector_runtime_cpu_valid,process_cpu_percent = process_cpu_percent AND process_cpu_percent BETWEEN 0 AND 1000000"`
}

func (runtimeRecord) TableName() string { return "collector_runtime" }

type capabilityRecord struct {
	TenantID    string `gorm:"column:tenant_id;type:text;primaryKey;priority:1;not null"`
	CollectorID string `gorm:"column:collector_id;type:text;primaryKey;priority:2;not null"`
	Capability  int64  `gorm:"column:capability;type:integer;primaryKey;priority:3;not null;check:collector_capabilities_value_valid,capability BETWEEN 1 AND 2147483647"`
}

func (capabilityRecord) TableName() string { return "collector_capabilities" }

type authorizedIndexRecord struct {
	TenantID    string `gorm:"column:tenant_id;type:text;primaryKey;priority:1;not null"`
	CollectorID string `gorm:"column:collector_id;type:text;primaryKey;priority:2;not null"`
	IndexName   string `gorm:"column:index_name;type:text;primaryKey;priority:3;not null;check:collector_authorized_indexes_name_canonical,length(index_name) BETWEEN 1 AND 255 AND index_name = lower(index_name) AND index_name NOT GLOB '*[^a-z0-9_-]*' AND substr(index_name, 1, 1) GLOB '[a-z0-9]' AND instr(index_name, 'kvstore') = 0"`
}

func (authorizedIndexRecord) TableName() string { return "collector_authorized_indexes" }

type inputRecord struct {
	TenantID    string  `gorm:"column:tenant_id;type:text;primaryKey;priority:1;not null"`
	CollectorID string  `gorm:"column:collector_id;type:text;primaryKey;priority:2;not null"`
	InputID     string  `gorm:"column:input_id;type:text;primaryKey;priority:3;not null;check:collector_inputs_id_canonical,length(input_id) BETWEEN 1 AND 128 AND instr(input_id, char(0)) = 0 AND substr(input_id, 1, 1) GLOB '[A-Za-z0-9]' AND input_id NOT GLOB '*[^A-Za-z0-9._:-]*'"`
	InputType   int64   `gorm:"column:input_type;type:integer;not null;check:collector_inputs_type_valid,input_type BETWEEN 1 AND 2147483647"`
	IndexName   string  `gorm:"column:index_name;type:text;not null;check:collector_inputs_index_name_canonical,length(index_name) BETWEEN 1 AND 255 AND index_name = lower(index_name) AND index_name NOT GLOB '*[^a-z0-9_-]*' AND substr(index_name, 1, 1) GLOB '[a-z0-9]' AND instr(index_name, 'kvstore') = 0"`
	Source      *string `gorm:"column:source;type:text;check:collector_inputs_source_bounded,source IS NULL OR (length(CAST(source AS BLOB)) BETWEEN 1 AND 4096 AND instr(source, char(0)) = 0)"`
	Sourcetype  *string `gorm:"column:sourcetype;type:text;check:collector_inputs_sourcetype_bounded,sourcetype IS NULL OR (length(CAST(sourcetype AS BLOB)) BETWEEN 1 AND 255 AND instr(sourcetype, char(0)) = 0)"`
}

func (inputRecord) TableName() string { return "collector_inputs" }

type inputHealthRecord struct {
	TenantID             string `gorm:"column:tenant_id;type:text;primaryKey;priority:1;not null"`
	CollectorID          string `gorm:"column:collector_id;type:text;primaryKey;priority:2;not null"`
	InputID              string `gorm:"column:input_id;type:text;primaryKey;priority:3;not null"`
	State                int64  `gorm:"column:state;type:integer;not null;check:collector_input_health_state_valid,state BETWEEN 1 AND 2147483647"`
	StatusMessage        string `gorm:"column:status_message;type:text;not null;check:collector_input_health_message_bounded,length(CAST(status_message AS BLOB)) <= 8192 AND instr(status_message, char(0)) = 0"`
	DiscoveredSources    int64  `gorm:"column:discovered_sources;type:integer;not null;check:collector_input_health_discovered_nonnegative,discovered_sources >= 0"`
	ActiveSources        int64  `gorm:"column:active_sources;type:integer;not null;check:collector_input_health_active_valid,active_sources BETWEEN 0 AND discovered_sources"`
	EventsReadTotal      int64  `gorm:"column:events_read_total;type:integer;not null;check:collector_input_health_events_nonnegative,events_read_total >= 0"`
	BytesReadTotal       int64  `gorm:"column:bytes_read_total;type:integer;not null;check:collector_input_health_bytes_nonnegative,bytes_read_total >= 0"`
	LastEventAtUnixMicro *int64 `gorm:"column:last_event_at_unix_micro;type:integer;check:collector_input_health_last_event_valid,last_event_at_unix_micro IS NULL OR last_event_at_unix_micro BETWEEN 1 AND 253402300799999999"`
	LastErrorAtUnixMicro *int64 `gorm:"column:last_error_at_unix_micro;type:integer;check:collector_input_health_last_error_valid,last_error_at_unix_micro IS NULL OR last_error_at_unix_micro BETWEEN 1 AND 253402300799999999"`
}

func (inputHealthRecord) TableName() string { return "collector_input_health" }
