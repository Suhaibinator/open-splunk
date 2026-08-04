package audit

// auditTenantStateRecord is the explicit GORM projection of the
// audit_tenant_state allocation/capacity authority. Migration 0022 remains the
// schema authority, including transition and immutability triggers.
type auditTenantStateRecord struct {
	TenantID     string `gorm:"column:tenant_id;type:text;primaryKey;not null"`
	NextSequence int64  `gorm:"column:next_sequence;type:integer;not null;check:audit_tenant_state_next_sequence_bounded,next_sequence BETWEEN 1 AND 100001"`
	EventCount   int64  `gorm:"column:event_count;type:integer;not null;check:audit_tenant_state_event_count_bounded,event_count BETWEEN 0 AND 100000"`
}

func (auditTenantStateRecord) TableName() string { return "audit_tenant_state" }

// auditEventRecord is the exact GORM projection of one immutable audit row.
// It intentionally contains no metadata blob or credential-bearing field.
type auditEventRecord struct {
	TenantID            string     `gorm:"column:tenant_id;type:text;primaryKey;not null;index:audit_events_tenant_action_sequence_idx,priority:1;index:audit_events_tenant_actor_sequence_idx,priority:1;index:audit_events_tenant_target_sequence_idx,priority:1"`
	Sequence            int64      `gorm:"column:sequence;type:integer;primaryKey;autoIncrement:false;not null;index:audit_events_tenant_action_sequence_idx,priority:3,sort:desc;index:audit_events_tenant_actor_sequence_idx,priority:3,sort:desc;index:audit_events_tenant_target_sequence_idx,priority:3,sort:desc"`
	OccurredAtUnixMicro int64      `gorm:"column:occurred_at_unix_micro;type:integer;not null"`
	ActorKind           ActorKind  `gorm:"column:actor_kind;type:text;not null"`
	ActorID             string     `gorm:"column:actor_id;type:text;not null;index:audit_events_tenant_actor_sequence_idx,priority:2"`
	ActorRole           ActorRole  `gorm:"column:actor_role;type:text;not null"`
	Action              Action     `gorm:"column:action;type:text;not null;index:audit_events_tenant_action_sequence_idx,priority:2"`
	TargetKind          TargetKind `gorm:"column:target_kind;type:text;not null;index:audit_events_tenant_target_sequence_idx,priority:2"`
	TargetID            string     `gorm:"column:target_id;type:text;not null"`
	TargetVersion       int64      `gorm:"column:target_version;type:integer;not null"`
}

func (auditEventRecord) TableName() string { return "audit_events" }
