package searchaudit

import "github.com/Suhaibinator/open-splunk/internal/audit"

// searchAttemptTenantStateRecord is the exact GORM projection of the allocation
// and rolling-retention authority in migration 0023.
type searchAttemptTenantStateRecord struct {
	TenantID                string `gorm:"column:tenant_id;type:text;primaryKey;not null"`
	FirstSequence           int64  `gorm:"column:first_sequence;type:integer;not null"`
	NextSequence            int64  `gorm:"column:next_sequence;type:integer;not null"`
	RetainedCount           int64  `gorm:"column:retained_count;type:integer;not null"`
	MaximumRetainedAttempts int64  `gorm:"column:maximum_retained_attempts;type:integer;not null"`
}

func (searchAttemptTenantStateRecord) TableName() string {
	return "search_attempt_audit_tenant_state"
}

// searchAttemptEventRecord is scalar-only and deliberately has no arbitrary
// metadata or search-content field.
type searchAttemptEventRecord struct {
	TenantID            string          `gorm:"column:tenant_id;type:text;primaryKey;not null;index:search_attempt_audit_tenant_actor_sequence_idx,priority:1;index:search_attempt_audit_tenant_owner_sequence_idx,priority:1;uniqueIndex:search_attempt_audit_tenant_job_key,priority:1"`
	Sequence            int64           `gorm:"column:sequence;type:integer;primaryKey;autoIncrement:false;not null;index:search_attempt_audit_tenant_actor_sequence_idx,priority:3,sort:desc;index:search_attempt_audit_tenant_owner_sequence_idx,priority:3,sort:desc"`
	OccurredAtUnixMicro int64           `gorm:"column:occurred_at_unix_micro;type:integer;not null"`
	ActorKind           audit.ActorKind `gorm:"column:actor_kind;type:text;not null"`
	ActorID             string          `gorm:"column:actor_id;type:text;not null;index:search_attempt_audit_tenant_actor_sequence_idx,priority:2"`
	ActorRole           audit.ActorRole `gorm:"column:actor_role;type:text;not null"`
	OwnerID             string          `gorm:"column:owner_id;type:text;not null;index:search_attempt_audit_tenant_owner_sequence_idx,priority:2"`
	SearchJobID         string          `gorm:"column:search_job_id;type:text;not null;uniqueIndex:search_attempt_audit_tenant_job_key,priority:2"`
}

func (searchAttemptEventRecord) TableName() string {
	return "search_attempt_audit_events"
}
