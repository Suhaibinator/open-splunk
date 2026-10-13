// Control-plane attempt-ledger reads and aggregation (admin API): the generic attempt filter, its
// query builder, and the list/get/summarize/breakdown queries over the recorded ledger. The shared
// store type, constructor, and sentinel errors live in inference_control_plane_store.go.

package common_persistence

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"gorm.io/gorm"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// AttemptFilter carries the generic attempt-list filters. All pointer fields are optional; the
// admin service validates an operation against the code registry and applies its own allowlist to
// Metadata keys before calling the store.
type AttemptFilter struct {
	FromTime         *time.Time
	ToTime           *time.Time
	ProviderID       *string
	InferenceModelID *uint64
	Operation        *int32
	Status           *proto_models.InferenceAttemptStatus
	TokenSource      *proto_models.InferenceTokenSource
	OrganizationID   *int64
	ActorUserID      *uint64
	RequestTraceID   *string
	InvocationID     *string
	// Metadata is an allowlisted key->value map applied as a single JSON containment (@>) predicate
	// over the sanitized context. Values are matched as JSON strings.
	Metadata map[string]string
	// Query is an optional free-text substring match (case-insensitive) over invocation id, request
	// trace id, error class/message, and provider model id. LIKE wildcards in the input are escaped.
	Query  *string
	Limit  int
	Offset int
}

// AttemptSummary is the aggregate rollup over a filtered attempt set.
type AttemptSummary struct {
	AttemptCount int64
	SuccessCount int64
	FailureCount int64
	InputTokens  int64
	OutputTokens int64
	TotalTokens  int64
}

// AttemptBreakdownRow is one grouped aggregation row over (provider_id, operation, status).
type AttemptBreakdownRow struct {
	ProviderID   string
	Operation    int32
	Status       proto_models.InferenceAttemptStatus
	AttemptCount int64
	TotalTokens  int64
	InputTokens  int64
	OutputTokens int64
}

func attemptFilterFields(stage string, filter AttemptFilter) []zap.Field {
	fields := []zap.Field{
		zap.String(logkeys.Stage, stage),
		zap.Int(logkeys.Limit, filter.Limit),
		zap.Int(appLogger.KeyOffset, filter.Offset),
		zap.Int(appLogger.KeyMetadataFilterCount, len(filter.Metadata)),
	}
	if filter.OrganizationID != nil {
		fields = append(fields, zap.Int64(appLogger.KeyOrganizationID, *filter.OrganizationID))
	}
	if filter.ActorUserID != nil {
		fields = append(fields, zap.Uint64(appLogger.KeyActorUserID, *filter.ActorUserID))
	}
	if filter.InvocationID != nil {
		fields = append(fields, zap.String(appLogger.KeyInvocationID, *filter.InvocationID))
	}
	if filter.Operation != nil {
		fields = append(fields, zap.Int32(logkeys.Operation, *filter.Operation))
	}
	return fields
}

// applyAttemptFilter narrows a query by every set filter field. It returns an error only when the
// metadata containment JSON cannot be marshaled.
func applyAttemptFilter(q *gorm.DB, filter AttemptFilter) (*gorm.DB, error) {
	if filter.FromTime != nil {
		q = q.Where("created_at >= ?", *filter.FromTime)
	}
	if filter.ToTime != nil {
		q = q.Where("created_at <= ?", *filter.ToTime)
	}
	if filter.ProviderID != nil {
		q = q.Where("provider_id = ?", *filter.ProviderID)
	}
	if filter.InferenceModelID != nil {
		q = q.Where("inference_model_id = ?", *filter.InferenceModelID)
	}
	if filter.Operation != nil {
		q = q.Where("operation = ?", *filter.Operation)
	}
	if filter.Status != nil {
		q = q.Where("status = ?", *filter.Status)
	}
	if filter.TokenSource != nil {
		q = q.Where("token_source = ?", *filter.TokenSource)
	}
	if filter.OrganizationID != nil {
		q = q.Where("organization_id = ?", *filter.OrganizationID)
	}
	if filter.ActorUserID != nil {
		q = q.Where("actor_user_id = ?", *filter.ActorUserID)
	}
	if filter.RequestTraceID != nil {
		q = q.Where("request_trace_id = ?", *filter.RequestTraceID)
	}
	if filter.InvocationID != nil {
		q = q.Where("invocation_id = ?", *filter.InvocationID)
	}
	if len(filter.Metadata) > 0 {
		containment, err := json.Marshal(filter.Metadata)
		if err != nil {
			return nil, appLogger.InvariantError("inference_attempt_metadata_filter_serializable", err,
				zap.String(logkeys.Stage, "marshal_inference_attempt_metadata_filter"),
				zap.String(logkeys.Expected, "string metadata map serializes as JSON"),
				zap.String(logkeys.Actual, "serialization failed"),
				zap.Int(appLogger.KeyMetadataFilterCount, len(filter.Metadata)))
		}
		q = q.Where("sanitized_context_json @> ?", string(containment))
	}
	if filter.Query != nil && *filter.Query != "" {
		pattern := "%" + EscapeLikePattern(*filter.Query) + "%"
		q = q.Where(
			"invocation_id ILIKE ? OR request_trace_id ILIKE ? OR error_class ILIKE ? OR error_message ILIKE ? OR provider_model_id ILIKE ?",
			pattern, pattern, pattern, pattern, pattern,
		)
	}
	return q, nil
}

// ListAttempts returns a newest-first page plus a lower bound on matching rows.
// Fetching one extra row answers whether another page exists without a count scan.
// The caller owns the page bound: Limit is fetched as given (the admin sanitizer
// defaults and bounds it from inference_attempt_list_*_limit) and must be positive.
func (s *ControlPlaneStore) ListAttempts(ctx context.Context, filter AttemptFilter) ([]common_persistence_models.InferenceAttempt, int64, error) {
	if filter.Limit <= 0 {
		return nil, 0, appLogger.InvariantError("inference_attempt_list_limit_positive", ErrDatabaseError,
			attemptFilterFields("list_inference_attempts", filter)...)
	}
	base := s.session(ctx).Model(&common_persistence_models.InferenceAttempt{})
	base, err := applyAttemptFilter(base, filter)
	if err != nil {
		return nil, 0, err
	}

	limit := filter.Limit
	offset := max(filter.Offset, 0)

	var attempts []common_persistence_models.InferenceAttempt
	if err := base.
		Order("created_at DESC, id DESC").
		Limit(limit + 1).
		Offset(offset).
		Find(&attempts).Error; err != nil {
		return nil, 0, appLogger.WithErrorFields(fmt.Errorf("list attempts: %w", err),
			attemptFilterFields("list_inference_attempts", filter)...)
	}
	var total int64
	if len(attempts) > 0 {
		total = int64(offset + len(attempts))
	}
	if len(attempts) > limit {
		attempts = attempts[:limit]
	}
	if err := s.loadAttemptAttribution(ctx, attempts); err != nil {
		return nil, 0, err
	}
	return attempts, total, nil
}

// GetAttempt loads one attempt row by ledger id, returning ErrControlPlaneAttemptNotFound when it
// does not exist (matching the sentinel-error contract of the other getters).
func (s *ControlPlaneStore) GetAttempt(ctx context.Context, id uint64) (*common_persistence_models.InferenceAttempt, error) {
	var attempt common_persistence_models.InferenceAttempt
	err := s.session(ctx).Where("id = ?", id).First(&attempt).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrControlPlaneAttemptNotFound
	}
	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("get attempt: %w", err),
			zap.String(logkeys.Stage, "get_inference_attempt"),
			zap.Uint64(appLogger.KeyAttemptID, id))
	}
	attempts := []common_persistence_models.InferenceAttempt{attempt}
	if err := s.loadAttemptAttribution(ctx, attempts); err != nil {
		return nil, err
	}
	return &attempts[0], nil
}

// SummarizeAttempts aggregates a filtered attempt set into counts and token totals.
func (s *ControlPlaneStore) SummarizeAttempts(ctx context.Context, filter AttemptFilter) (*AttemptSummary, error) {
	base := s.session(ctx).Model(&common_persistence_models.InferenceAttempt{})
	base, err := applyAttemptFilter(base, filter)
	if err != nil {
		return nil, err
	}

	var summary AttemptSummary
	err = base.Select(
		"COUNT(*) AS attempt_count, " +
			fmt.Sprintf("COALESCE(SUM(CASE WHEN status = %d THEN 1 ELSE 0 END), 0) AS success_count, ", proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_SUCCEEDED) +
			fmt.Sprintf("COALESCE(SUM(CASE WHEN status = %d THEN 1 ELSE 0 END), 0) AS failure_count, ", proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_FAILED) +
			"COALESCE(SUM(input_tokens), 0) AS input_tokens, " +
			"COALESCE(SUM(output_tokens), 0) AS output_tokens, " +
			"COALESCE(SUM(total_tokens), 0) AS total_tokens",
	).Scan(&summary).Error
	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("summarize attempts: %w", err),
			attemptFilterFields("summarize_inference_attempts", filter)...)
	}
	return &summary, nil
}

// BreakdownAttempts groups a filtered attempt set by provider_id, operation, and status.
func (s *ControlPlaneStore) BreakdownAttempts(ctx context.Context, filter AttemptFilter) ([]AttemptBreakdownRow, error) {
	base := s.session(ctx).Model(&common_persistence_models.InferenceAttempt{})
	base, err := applyAttemptFilter(base, filter)
	if err != nil {
		return nil, err
	}

	var rows []AttemptBreakdownRow
	err = base.
		Select("provider_id, operation, status, COUNT(*) AS attempt_count, COALESCE(SUM(total_tokens), 0) AS total_tokens, COALESCE(SUM(input_tokens), 0) AS input_tokens, COALESCE(SUM(output_tokens), 0) AS output_tokens").
		Group("provider_id, operation, status").
		Order("provider_id ASC, operation ASC, status ASC").
		Scan(&rows).Error
	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("breakdown attempts: %w", err),
			attemptFilterFields("breakdown_inference_attempts", filter)...)
	}
	return rows, nil
}

// AggregateAttempts computes totals and breakdown from one grouped scan over
// the same snapshot, avoiding duplicated whole-range work and inconsistent sums.
func (s *ControlPlaneStore) AggregateAttempts(ctx context.Context, filter AttemptFilter) (*AttemptSummary, []AttemptBreakdownRow, error) {
	rows, err := s.BreakdownAttempts(ctx, filter)
	if err != nil {
		return nil, nil, err
	}
	summary := &AttemptSummary{}
	for _, row := range rows {
		summary.AttemptCount += row.AttemptCount
		summary.TotalTokens += row.TotalTokens
		summary.InputTokens += row.InputTokens
		summary.OutputTokens += row.OutputTokens
		switch row.Status {
		case proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_SUCCEEDED:
			summary.SuccessCount += row.AttemptCount
		case proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_FAILED:
			summary.FailureCount += row.AttemptCount
		}
	}
	return summary, rows, nil
}

// loadAttemptAttribution performs at most one query per entity type, after pagination.
// Model scopes exclude soft-deleted entities; absent entities never remove ledger rows.
func (s *ControlPlaneStore) loadAttemptAttribution(ctx context.Context, attempts []common_persistence_models.InferenceAttempt) error {
	actorIDs := make([]uint64, 0)
	orgIDs := make([]int64, 0)
	seenActors := make(map[uint64]bool)
	seenOrgs := make(map[int64]bool)
	for _, a := range attempts {
		if a.ActorUserID != nil && !seenActors[*a.ActorUserID] {
			seenActors[*a.ActorUserID] = true
			actorIDs = append(actorIDs, *a.ActorUserID)
		}
		if a.OrganizationID != nil && !seenOrgs[*a.OrganizationID] {
			seenOrgs[*a.OrganizationID] = true
			orgIDs = append(orgIDs, *a.OrganizationID)
		}
	}
	actors := make(map[uint64]*common_persistence_models.InferenceAttemptActor)
	organizations := make(map[int64]*common_persistence_models.InferenceAttemptOrganization)
	if len(actorIDs) > 0 {
		var rows []common_persistence_models.InferenceAttemptActor
		if err := s.session(ctx).Model(&common_persistence_models.User{}).
			Select("id", "username", "first_name", "last_name", "is_bot").Where("id IN ?", actorIDs).Find(&rows).Error; err != nil {
			return fmt.Errorf("load inference attempt actors: %w", err)
		}
		for i := range rows {
			actors[rows[i].ID] = &rows[i]
		}
	}
	if len(orgIDs) > 0 {
		var rows []common_persistence_models.InferenceAttemptOrganization
		if err := s.session(ctx).Model(&common_persistence_models.Organization{}).
			Select("id", "name", "slug").Where("id IN ?", orgIDs).Find(&rows).Error; err != nil {
			return fmt.Errorf("load inference attempt organizations: %w", err)
		}
		for i := range rows {
			organizations[rows[i].ID] = &rows[i]
		}
	}
	for i := range attempts {
		a := &attempts[i]
		if a.ActorUserID != nil {
			a.Actor = actors[*a.ActorUserID]
		}
		if a.OrganizationID != nil {
			a.Organization = organizations[*a.OrganizationID]
		}
	}
	return nil
}
