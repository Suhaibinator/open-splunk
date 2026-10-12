// Control-plane model lookups and rate-limit policy persistence: inference model catalog reads
// (by local id and by the stable provider tuple) and the per-model rate-limit policy reads/upsert.
// The shared store type, constructor, and sentinel errors live in inference_control_plane_store.go.

package common_persistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetModel loads an inference model catalog row, returning ErrControlPlaneModelNotFound when absent.
func (s *ControlPlaneStore) GetModel(ctx context.Context, modelID uint64) (*common_persistence_models.InferenceModel, error) {
	var model common_persistence_models.InferenceModel
	err := s.session(ctx).Where("id = ?", modelID).First(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrControlPlaneModelNotFound
	}
	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("get model: %w", err),
			zap.String(logkeys.Stage, "get_inference_model"),
			zap.Uint64(appLogger.KeyInferenceModelID, modelID))
	}
	return &model, nil
}

// GetModelByProviderTuple resolves an inference model by its (provider, provider_model_id) tuple,
// the stable cross-environment model identity used when seeding default routes (never the local
// database id). It returns ErrControlPlaneModelNotFound when no matching model exists.
//
// The supplied provider model id is normalized before matching, because the catalog write path
// (CreateInferenceModel) stores the normalized form: a caller passing the prefixed alias its
// provider SDK uses (e.g. "googleai/gemini-3-flash-preview") must resolve the row stored as
// "gemini-3-flash-preview" rather than silently missing it.
func (s *ControlPlaneStore) GetModelByProviderTuple(ctx context.Context, provider string, providerModelID string) (*common_persistence_models.InferenceModel, error) {
	normalizedModelID, err := inference.NormalizeProviderModelID(provider, providerModelID)
	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("get model by provider tuple: %w", err),
			zap.String(logkeys.Stage, "normalize_inference_model_provider_tuple"),
			zap.String(appLogger.KeyProvider, provider),
			zap.String(appLogger.KeyProviderModelID, providerModelID))
	}
	var model common_persistence_models.InferenceModel
	err = s.session(ctx).
		Where("provider_id = ? AND provider_model_id = ?", provider, normalizedModelID).
		First(&model).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrControlPlaneModelNotFound
	}
	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("get model by provider tuple: %w", err),
			zap.String(logkeys.Stage, "get_inference_model_by_provider_tuple"),
			zap.String(appLogger.KeyProvider, provider),
			zap.String(appLogger.KeyProviderModelID, normalizedModelID))
	}
	return &model, nil
}

// GetModelRateLimitPolicy loads the model rate-limit policy, returning nil (no error) when the model
// has no configured policy.
func (s *ControlPlaneStore) GetModelRateLimitPolicy(ctx context.Context, modelID uint64) (*common_persistence_models.InferenceModelRateLimitPolicy, error) {
	var policy common_persistence_models.InferenceModelRateLimitPolicy
	err := s.session(ctx).Where("inference_model_id = ?", modelID).First(&policy).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("get model rate-limit policy: %w", err),
			zap.String(logkeys.Stage, "get_inference_model_rate_limit_policy"),
			zap.Uint64(appLogger.KeyInferenceModelID, modelID))
	}
	return &policy, nil
}

// ListModelRateLimitPolicies returns every configured model rate-limit policy ordered by model id.
func (s *ControlPlaneStore) ListModelRateLimitPolicies(ctx context.Context) ([]common_persistence_models.InferenceModelRateLimitPolicy, error) {
	var policies []common_persistence_models.InferenceModelRateLimitPolicy
	if err := s.session(ctx).
		Order("inference_model_id ASC").
		Find(&policies).Error; err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("list model rate-limit policies: %w", err),
			zap.String(logkeys.Stage, "list_inference_model_rate_limit_policies"))
	}
	return policies, nil
}

// UpsertModelRateLimitPolicy inserts or replaces a model's rate-limit policy. Updating a policy
// rebuilds the in-process bucket on next resolution (matching current behavior).
func (s *ControlPlaneStore) UpsertModelRateLimitPolicy(ctx context.Context, policy *common_persistence_models.InferenceModelRateLimitPolicy) error {
	if policy == nil {
		return appLogger.InvariantError("inference_model_rate_limit_policy_present",
			errors.New("inference model rate-limit policy is required"),
			zap.String(logkeys.Stage, "upsert_inference_model_rate_limit_policy"),
			zap.String(logkeys.Expected, "non-nil policy"),
			zap.String(logkeys.Actual, "nil policy"))
	}
	err := s.session(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "inference_model_id"}},
			UpdateAll: true,
		}).
		Create(policy).Error
	if err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("upsert model rate-limit policy: %w", err),
			zap.String(logkeys.Stage, "upsert_inference_model_rate_limit_policy"),
			zap.Uint64(appLogger.KeyInferenceModelID, policy.InferenceModelID))
	}
	return nil
}
