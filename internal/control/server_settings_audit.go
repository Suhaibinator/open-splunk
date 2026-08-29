package control

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// ServerSettingsMutationAuditEvent is the secret-free projection emitted by
// a successful node-wide search-settings replacement.
type ServerSettingsMutationAuditEvent struct {
	OccurredAt time.Time
	Version    uint64
}

type ServerSettingsMutationAuditAppender interface {
	AppendServerSettingsMutationInTransaction(
		context.Context,
		*gorm.DB,
		string,
		ServerSettingsMutationAuditEvent,
	) error
}
