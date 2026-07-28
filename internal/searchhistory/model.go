package searchhistory

// historyRecord is the explicit GORM representation of search_history.
//
// Versioned SQL migrations remain authoritative. In particular, SQLite STRICT
// tables, binary collations, and cross-column constraints cannot be represented
// completely by GORM tags. Never use AutoMigrate for this model.
type historyRecord struct {
	SearchJobID         string `gorm:"column:search_job_id;type:text;primaryKey;not null;check:search_history_job_id_length,length(search_job_id) BETWEEN 1 AND 256;index:search_history_owner_created_idx,priority:4,sort:desc;index:search_history_owner_finished_idx,priority:4,sort:desc;index:search_history_owner_duration_idx,priority:4,sort:desc;index:search_history_owner_matched_idx,priority:4,sort:desc;index:search_history_owner_app_created_idx,priority:5,sort:desc;index:search_history_owner_saved_created_idx,priority:5,sort:desc"`
	TenantID            string `gorm:"column:tenant_id;type:text;not null;check:search_history_tenant_id_length,length(tenant_id) BETWEEN 1 AND 1024;index:search_history_owner_created_idx,priority:1;index:search_history_owner_finished_idx,priority:1;index:search_history_owner_duration_idx,priority:1;index:search_history_owner_matched_idx,priority:1;index:search_history_owner_app_created_idx,priority:1;index:search_history_owner_saved_created_idx,priority:1"`
	OwnerID             string `gorm:"column:owner_id;type:text;not null;check:search_history_owner_id_length,length(owner_id) BETWEEN 1 AND 255;index:search_history_owner_created_idx,priority:2;index:search_history_owner_finished_idx,priority:2;index:search_history_owner_duration_idx,priority:2;index:search_history_owner_matched_idx,priority:2;index:search_history_owner_app_created_idx,priority:2;index:search_history_owner_saved_created_idx,priority:2"`
	AppID               string `gorm:"column:app_id;type:text;not null;check:search_history_app_id_length,length(app_id) <= 255;index:search_history_owner_app_created_idx,priority:3"`
	SavedSearchID       string `gorm:"column:saved_search_id;type:text;not null;check:search_history_saved_search_id_length,length(saved_search_id) <= 128;index:search_history_owner_saved_created_idx,priority:3"`
	FinalState          int64  `gorm:"column:final_state;type:integer;not null;check:search_history_final_state_terminal,final_state BETWEEN 6 AND 9"`
	SearchText          string `gorm:"column:search_text;type:text;not null;check:search_history_search_text_length,length(search_text) BETWEEN 1 AND 65536"`
	CreatedAtUnixMicro  int64  `gorm:"column:created_at_unix_micro;type:integer;not null;index:search_history_owner_created_idx,priority:3,sort:desc;index:search_history_owner_app_created_idx,priority:4,sort:desc;index:search_history_owner_saved_created_idx,priority:4,sort:desc"`
	FinishedAtUnixMicro int64  `gorm:"column:finished_at_unix_micro;type:integer;not null;check:search_history_finish_not_before_create,finished_at_unix_micro >= created_at_unix_micro;index:search_history_owner_finished_idx,priority:3,sort:desc"`
	DurationNanoseconds int64  `gorm:"column:duration_nanoseconds;type:integer;not null;check:search_history_duration_nonnegative,duration_nanoseconds >= 0;index:search_history_owner_duration_idx,priority:3,sort:desc"`
	MatchedEvents       int64  `gorm:"column:matched_events;type:integer;not null;check:search_history_matched_nonnegative,matched_events >= 0;index:search_history_owner_matched_idx,priority:3,sort:desc"`
	EntryProto          []byte `gorm:"column:entry_proto;type:blob;not null;check:search_history_entry_proto_length,length(entry_proto) BETWEEN 1 AND 524288"`
	EntrySHA256         []byte `gorm:"column:entry_sha256;type:blob;not null;check:search_history_entry_sha256_length,length(entry_sha256) = 32"`
}

func (historyRecord) TableName() string {
	return "search_history"
}

// pendingHistoryRecord is the explicit GORM representation of the durable
// pre-execution journal. The migration owns its constraints and indexes.
type pendingHistoryRecord struct {
	SearchJobID        string `gorm:"column:search_job_id;type:text;primaryKey;not null;check:search_history_pending_job_id_length,length(search_job_id) BETWEEN 1 AND 256;index:search_history_pending_owner_created_idx,priority:4"`
	TenantID           string `gorm:"column:tenant_id;type:text;not null;check:search_history_pending_tenant_id_length,length(tenant_id) BETWEEN 1 AND 1024;index:search_history_pending_owner_created_idx,priority:1"`
	OwnerID            string `gorm:"column:owner_id;type:text;not null;check:search_history_pending_owner_id_length,length(owner_id) BETWEEN 1 AND 255;index:search_history_pending_owner_created_idx,priority:2"`
	State              int64  `gorm:"column:state;type:integer;not null;check:search_history_pending_state_nonterminal,state BETWEEN 1 AND 5"`
	CreatedAtUnixMicro int64  `gorm:"column:created_at_unix_micro;type:integer;not null;index:search_history_pending_owner_created_idx,priority:3"`
	EntryProto         []byte `gorm:"column:entry_proto;type:blob;not null;check:search_history_pending_entry_proto_length,length(entry_proto) BETWEEN 1 AND 524288"`
	EntrySHA256        []byte `gorm:"column:entry_sha256;type:blob;not null;check:search_history_pending_entry_sha256_length,length(entry_sha256) = 32"`
}

func (pendingHistoryRecord) TableName() string {
	return "search_history_pending"
}
