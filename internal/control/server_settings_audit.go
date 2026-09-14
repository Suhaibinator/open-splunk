package control

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// ServerSettingsTarget names the node-wide settings singleton a mutation
// replaced. It becomes the audit row's target_id beside the shared
// server_settings target kind, so each singleton keeps its own version line.
type ServerSettingsTarget string

const (
	// ServerSettingsTargetSearchLimits is the search resource policy.
	ServerSettingsTargetSearchLimits ServerSettingsTarget = "search-limits"
	// ServerSettingsTargetUIPalette is the instance-wide UI palette.
	ServerSettingsTargetUIPalette ServerSettingsTarget = "ui-palette"
)

// Valid reports whether the target is one of the known settings singletons.
func (target ServerSettingsTarget) Valid() bool {
	switch target {
	case ServerSettingsTargetSearchLimits, ServerSettingsTargetUIPalette:
		return true
	default:
		return false
	}
}

// ServerSettingsMutationAuditEvent is the secret-free projection emitted by
// a successful node-wide settings replacement. Old and new values are not
// recorded; the committed version plus the singleton row is the record.
type ServerSettingsMutationAuditEvent struct {
	OccurredAt time.Time
	Target     ServerSettingsTarget
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
