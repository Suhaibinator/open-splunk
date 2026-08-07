package knowledgeattemptaudit

import "github.com/Suhaibinator/open-splunk/internal/audit"

type tenantStateRecord struct {
	TenantID      string `gorm:"column:tenant_id;type:text;primaryKey;not null"`
	FirstSequence int64  `gorm:"column:first_sequence;type:integer;not null"`
	NextSequence  int64  `gorm:"column:next_sequence;type:integer;not null"`
	RetainedCount int64  `gorm:"column:retained_count;type:integer;not null"`
}

func (tenantStateRecord) TableName() string {
	return "knowledge_attempt_audit_tenant_state"
}

type eventRecord struct {
	TenantID            string          `gorm:"column:tenant_id;type:text;primaryKey;not null"`
	Sequence            int64           `gorm:"column:sequence;type:integer;primaryKey;autoIncrement:false;not null"`
	OccurredAtUnixMicro int64           `gorm:"column:occurred_at_unix_micro;type:integer;not null"`
	ActorKind           audit.ActorKind `gorm:"column:actor_kind;type:text;not null"`
	ActorID             string          `gorm:"column:actor_id;type:text;not null"`
	ActorRole           audit.ActorRole `gorm:"column:actor_role;type:text;not null"`
	Action              Action          `gorm:"column:action;type:text;not null"`
	Result              Result          `gorm:"column:result;type:text;not null"`
	Reason              Reason          `gorm:"column:reason;type:text;not null"`
	AppID               *string         `gorm:"column:app_id;type:text"`
	KnowledgeObjectID   *string         `gorm:"column:knowledge_object_id;type:text"`
	ObjectType          *ObjectType     `gorm:"column:object_type;type:text"`
	ObjectVersion       *int64          `gorm:"column:object_version;type:integer"`
	SharingScope        *SharingScope   `gorm:"column:sharing_scope;type:text"`
}

func (eventRecord) TableName() string {
	return "knowledge_attempt_audit_events"
}
