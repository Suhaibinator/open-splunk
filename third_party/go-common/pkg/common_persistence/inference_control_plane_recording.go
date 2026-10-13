// Control-plane attempt-ledger writes: the idempotent start-row insert and terminal-state upsert
// keyed on (invocation_id, attempt_number). Both are transaction-independent (decision 13) so an
// attempt record survives a product transaction rollback. The shared store type, constructor, and
// sentinel errors live in inference_control_plane_store.go.

package common_persistence

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm/clause"
)

func inferenceAttemptFields(stage string, attempt *common_persistence_models.InferenceAttempt) []zap.Field {
	fields := []zap.Field{zap.String(logkeys.Stage, stage)}
	if attempt == nil {
		return fields
	}
	return append(fields,
		zap.String(appLogger.KeyInvocationID, attempt.InvocationID),
		zap.Int32(appLogger.KeyAttemptNumber, attempt.AttemptNumber),
		zap.Int32(logkeys.Operation, attempt.Operation),
		zap.Uint64(appLogger.KeyInferenceModelID, attempt.InferenceModelID),
		zap.String(appLogger.KeyProvider, attempt.ProviderID),
		zap.String(appLogger.KeyProviderModelID, attempt.ProviderModelID),
	)
}

func inferenceAttemptOrganizationInvariant(stage string, attempt *common_persistence_models.InferenceAttempt, cause error) error {
	fields := append(inferenceAttemptFields(stage, attempt),
		zap.String(logkeys.Expected, "organization_id > 0"))
	switch {
	case attempt == nil:
		fields = append(fields, zap.String(logkeys.Actual, "nil attempt"))
	case attempt.OrganizationID == nil:
		fields = append(fields, zap.String(logkeys.Actual, "missing organization_id"))
	default:
		fields = append(fields, zap.Int64(logkeys.Actual, *attempt.OrganizationID))
	}
	return appLogger.InvariantError("inference_attempt_has_organization", cause, fields...)
}

// CreateAttemptStart inserts the start row for one attempt. It is idempotent on
// (invocation_id, attempt_number): a duplicate start (redelivery, retry of the observer) is a no-op
// rather than an error, so a start event can never break generation.
func (s *ControlPlaneStore) CreateAttemptStart(ctx context.Context, attempt *common_persistence_models.InferenceAttempt) error {
	if err := validateAttemptOrganization(attempt); err != nil {
		return inferenceAttemptOrganizationInvariant("create_inference_attempt_start", attempt, err)
	}
	err := s.session(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "invocation_id"}, {Name: "attempt_number"}},
			DoNothing: true,
		}).
		Create(attempt).Error
	if err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("create attempt start: %w", err),
			inferenceAttemptFields("create_inference_attempt_start", attempt)...)
	}
	return nil
}

// FinalizeAttempt writes the terminal state of one attempt. It upserts on
// (invocation_id, attempt_number): if the start row exists it updates the terminal columns; if the
// start row was never written (e.g. a lost start event) it inserts the full row. It is therefore
// idempotent under concurrent finalization and correct even when the caller's context was canceled
// before finalization (the recorder passes a detached, bounded write context). created_at is left to
// the existing row / insert default and is never overwritten. Organization attribution is also
// deliberately omitted from the update set: a finish event may fill it only by inserting a lost
// start row, but can never reassign an existing attempt to another organization.
func (s *ControlPlaneStore) FinalizeAttempt(ctx context.Context, attempt *common_persistence_models.InferenceAttempt) error {
	if err := validateAttemptOrganization(attempt); err != nil {
		return inferenceAttemptOrganizationInvariant("finalize_inference_attempt", attempt, err)
	}
	err := s.session(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "invocation_id"}, {Name: "attempt_number"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"operation", "runtime_purpose", "inference_model_id", "provider_id",
				"provider_model_id", "api", "status", "error_class", "error_message", "retryable",
				"retry_reason", "request_trace_id", "actor_user_id", "token_source",
				"input_tokens", "output_tokens", "total_tokens", "cached_tokens", "thought_tokens",
				"input_characters", "input_images", "input_videos", "input_audio_files",
				"effective_settings_json", "custom_usage_json", "sanitized_context_json", "started_at", "ended_at", "latency_ms", "first_output_at",
			}),
		}).
		Create(attempt).Error
	if err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("finalize attempt: %w", err),
			inferenceAttemptFields("finalize_inference_attempt", attempt)...)
	}
	return nil
}

func validateAttemptOrganization(attempt *common_persistence_models.InferenceAttempt) error {
	if attempt == nil || attempt.OrganizationID == nil || *attempt.OrganizationID <= 0 {
		return ErrControlPlaneAttemptOrganizationRequired
	}
	return nil
}

// SweepStaleAttempts repairs one bounded batch in its own transaction. Row locks and
// the conditional update ensure that a sweep never overwrites a recorded completion.
func (s *ControlPlaneStore) SweepStaleAttempts(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	table := common_persistence_models.TableNameInferenceAttempts
	result := s.session(ctx).Exec(fmt.Sprintf(`
 WITH stale AS (
  SELECT id FROM %s
  WHERE status = ? AND ended_at IS NULL AND deadline_at < ?
  ORDER BY deadline_at, id LIMIT ? FOR UPDATE SKIP LOCKED
 )
 UPDATE %s AS attempts SET status = ? FROM stale
 WHERE attempts.id = stale.id AND attempts.status = ? AND attempts.ended_at IS NULL`, table, table),
		proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_IN_FLIGHT, cutoff, limit,
		proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_OUTCOME_UNKNOWN,
		proto_models.InferenceAttemptStatus_INFERENCE_ATTEMPT_STATUS_IN_FLIGHT)
	if result.Error != nil {
		return 0, fmt.Errorf("sweep stale inference attempts: %w", result.Error)
	}
	return result.RowsAffected, nil
}
