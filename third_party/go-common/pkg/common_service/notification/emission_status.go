package notification

import (
	"context"
	"time"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// EmissionScope narrows a status or retry lookup to what the caller may see. The
// zero value matches any emission. A positive OrganizationID restricts the lookup
// to that organization's emissions, so a caller that only knows an organization
// can neither read nor resume another tenant's work; both report ErrNotFound.
type EmissionScope struct {
	OrganizationID int64
}

// EmissionStatus is trusted backend status for one durable emission. It carries
// no payload, template content, or lease credentials. State is one of the
// persistence NotificationEmission* constants: pending, completed, cancelled,
// exhausted. LastError is a bounded classification, never callback text.
type EmissionStatus struct {
	ID                                         uint64
	Producer                                   string
	OrganizationID                             int64
	Type                                       uint32
	SourceKey                                  string
	EmissionKey                                string
	State                                      string
	SnapshotReady                              bool
	AudienceSize, Processed, Accepted, Skipped uint64
	Attempts                                   uint32
	LastError                                  string
	CreatedAt, UpdatedAt                       time.Time
	CompletedAt                                *time.Time
}

// GetEmission reports the durable state of one emission produced by Emit,
// EmitMany, EmitEvent, or EmitToOrganization in async mode. Inline emissions
// leave no durable row and report ErrNotFound.
func (h *Handler) GetEmission(ctx context.Context, id uint64, scope EmissionScope) (*EmissionStatus, error) {
	job, err := h.scopedEmission(ctx, id, scope)
	if err != nil {
		return nil, err
	}
	return &EmissionStatus{ID: job.ID, Producer: job.Producer, OrganizationID: job.OrganizationID,
		Type: job.Type, SourceKey: job.SourceKey, EmissionKey: job.EmissionKey, State: job.State, SnapshotReady: job.SnapshotReady,
		AudienceSize: job.AudienceSize, Processed: job.Processed, Accepted: job.Accepted, Skipped: job.Processed - job.Accepted,
		Attempts: job.Attempts, LastError: job.LastError, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt, CompletedAt: job.CompletedAt}, nil
}

// RetryEmission resumes an exhausted emission from its recorded progress; the
// audience snapshot and every processed recipient are preserved. It reports
// false without error for an emission in any other state and ErrNotFound for
// one that does not exist within scope.
func (h *Handler) RetryEmission(ctx context.Context, id uint64, scope EmissionScope) (bool, error) {
	if _, err := h.scopedEmission(ctx, id, scope); err != nil {
		return false, err
	}
	store := h.store
	return store.RetryNotificationEmission(ctx, id)
}

func (h *Handler) scopedEmission(ctx context.Context, id uint64, scope EmissionScope) (*models.NotificationEmission, error) {
	if !h.enabled() {
		return nil, ErrDisabled
	}
	if id == 0 || scope.OrganizationID < 0 {
		return nil, ErrInvalid
	}
	store := h.store
	job, err := store.GetNotificationEmission(ctx, id)
	if err != nil {
		return nil, err
	}
	if job == nil || (scope.OrganizationID > 0 && job.OrganizationID != scope.OrganizationID) {
		return nil, ErrNotFound
	}
	return job, nil
}
