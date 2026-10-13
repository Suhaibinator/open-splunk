package common_service_models

import "time"

// AdminUsernameChangeHistoryEntry defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type AdminUsernameChangeHistoryEntry struct {
	ID           uint64
	TargetUserID uint64
	OldUsername  string
	NewUsername  string
	ActorUserID  *uint64
	Reason       string
	CreatedAt    time.Time
	TargetUser   *User
	ActorUser    *User
}
