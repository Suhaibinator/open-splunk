package common_persistence_models

// NotificationTransactionIdentity scopes PostgreSQL transaction IDs to a
// database incarnation. Epoch is application-owned; SystemIdentifier is present
// when the database permits pg_control_system(). Never compare stamps from
// incompatible identities as PostgreSQL transaction IDs are cluster-local.
type NotificationTransactionIdentity struct {
	Epoch            uint64 `gorm:"not null;default:0"`
	SystemIdentifier string `gorm:"type:text;not null;default:''"`
}

// NotificationTransactionStamp always records the top-level transaction ID.
// XID is text in Go to retain PostgreSQL's full unsigned xid8 range.
type NotificationTransactionStamp struct {
	NotificationTransactionIdentity `gorm:"embedded"`
	XID                             string `gorm:"column:xid;type:xid8;not null;default:'0'"`
	// Ordinal orders notification operations only within this transaction.
	Ordinal uint64 `gorm:"not null;default:0"`
}

// NotificationVisibilitySnapshot is a commit-visibility fence. UpperBound is
// retained separately so bootstrap can check indexed maxima without parsing
// every potentially large snapshot. Source snapshot text can be discarded once
// audience enumeration completes.
type NotificationVisibilitySnapshot struct {
	NotificationTransactionIdentity `gorm:"embedded"`
	Snapshot                        string `gorm:"type:text;not null;default:''"`
	UpperBound                      string `gorm:"type:xid8;not null;default:'0'"`
	OwnXID                          string `gorm:"column:own_xid;type:xid8;not null;default:'0'"`
	OwnOrdinal                      uint64 `gorm:"not null;default:0"`
}

// NotificationTransactionEpoch is the singleton bootstrap record. It is not
// written by emission, acknowledgment, membership, or worker hot paths.
type NotificationTransactionEpoch struct {
	ID                   uint64 `gorm:"primaryKey;autoIncrement:false"`
	Epoch                uint64 `gorm:"not null"`
	LastSystemIdentifier string `gorm:"type:text;not null;default:''"`
	HighWaterXID         string `gorm:"column:high_water_xid;type:xid8;not null;default:'0'"`
}

func (NotificationTransactionEpoch) TableName() string {
	return "gocommon_notification_transaction_epochs"
}
