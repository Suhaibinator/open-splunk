package genai

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// catalogCacheTTL bounds how long the inference model list is served from
// memory. Models are effectively configuration — read on AI request paths and
// written only through rare admin operations (which also invalidate eagerly).
const catalogCacheTTL = time.Minute

const catalogLoggerName = "genai_catalog"

type ModelResolver interface {
	ResolveModel(model *common_service_models.InferenceModel) (*ResolvedModel, error)
}

// Catalog defines structured state used in the GenAI model resolution and runtime construction flow.
//
// Used in flow:
// - Catalog is read or populated by backend components in this package to move requests through this stage.
type Catalog struct {
	persistence   common_persistence.CommonPersistenceInterface
	modelResolver ModelResolver

	cacheMu        sync.RWMutex
	cachedModels   []*common_service_models.InferenceModel
	cacheExpiresAt time.Time
}

// NewCatalog performs its package-specific work in the GenAI model resolution and runtime construction flow.
func NewCatalog(persistence common_persistence.CommonPersistenceInterface, modelResolver ModelResolver) *Catalog {
	if modelResolver == nil {
		logger := zap.L().Named(catalogLoggerName)
		logger.Warn("inference model catalog initialized without runtime factory; runtime validation of models is disabled",
			zap.String(logkeys.Operation, "initialize_inference_model_catalog"),
			zap.String(logkeys.Stage, "runtime_validation_setup"),
			zap.String(logkeys.Fallback, "persist_models_without_runtime_validation"),
		)
	}
	return &Catalog{
		persistence:   persistence,
		modelResolver: modelResolver,
	}
}

// SetModelResolver performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (c *Catalog) SetModelResolver(modelResolver ModelResolver) {
	if c == nil {
		return
	}
	c.modelResolver = modelResolver
}

// ListInferenceModels performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (c *Catalog) ListInferenceModels(ctx context.Context) ([]*common_service_models.InferenceModel, error) {
	if c == nil || c.persistence == nil {
		return nil, fmt.Errorf("inference model catalog is not configured")
	}

	c.cacheMu.RLock()
	if c.cachedModels != nil && time.Now().Before(c.cacheExpiresAt) {
		cached := c.cachedModels
		c.cacheMu.RUnlock()
		return cloneInferenceModels(cached), nil
	}
	c.cacheMu.RUnlock()

	rows, err := c.persistence.ListInferenceModels(ctx)
	if err != nil {
		return nil, err
	}
	models := make([]*common_service_models.InferenceModel, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		models = append(models, inferenceModelFromPersistenceRow(row))
	}

	c.cacheMu.Lock()
	c.cachedModels = cloneInferenceModels(models)
	c.cacheExpiresAt = time.Now().Add(catalogCacheTTL)
	c.cacheMu.Unlock()

	return models, nil
}

// ListInferenceModelsPage returns a bounded persistence-backed page without
// populating or consulting the full-catalog cache used by runtime resolution.
func (c *Catalog) ListInferenceModelsPage(
	ctx context.Context,
	pagination *proto_models.PaginationRequest,
) ([]*common_service_models.InferenceModel, *proto_models.PaginationResponse, error) {
	if c == nil || c.persistence == nil {
		return nil, nil, fmt.Errorf("inference model catalog is not configured")
	}

	rows, paginationResponse, err := c.persistence.ListInferenceModelsPage(ctx, pagination)
	if err != nil {
		return nil, nil, err
	}
	models := make([]*common_service_models.InferenceModel, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		models = append(models, inferenceModelFromPersistenceRow(row))
	}
	return models, paginationResponse, nil
}

// invalidateModelCache drops the cached model list after any catalog write.
func (c *Catalog) invalidateModelCache() {
	c.cacheMu.Lock()
	c.cachedModels = nil
	c.cacheExpiresAt = time.Time{}
	c.cacheMu.Unlock()
}

// cloneInferenceModels copies the cached slice (and its elements) so callers
// can mutate results without corrupting the cache.
func cloneInferenceModels(models []*common_service_models.InferenceModel) []*common_service_models.InferenceModel {
	cloned := make([]*common_service_models.InferenceModel, 0, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		copyModel := *model
		cloned = append(cloned, &copyModel)
	}
	return cloned
}

// CreateInferenceModel performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (c *Catalog) CreateInferenceModel(ctx context.Context, model *common_service_models.InferenceModel) (*common_service_models.InferenceModel, error) {
	if c == nil || c.persistence == nil {
		return nil, fmt.Errorf("inference model catalog is not configured")
	}
	if err := c.validateInferenceModel(model, false); err != nil {
		return nil, err
	}
	row := &common_persistence_models.InferenceModel{
		ProviderID:      model.ProviderID,
		ProviderModelID: model.ProviderModelID,
		DisplayName:     model.DisplayName,
		Active:          model.Active,
	}
	if err := c.persistence.CreateInferenceModel(ctx, row); err != nil {
		return nil, err
	}
	c.invalidateModelCache()
	return inferenceModelFromPersistenceRow(row), nil
}

// UpdateInferenceModel performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (c *Catalog) UpdateInferenceModel(ctx context.Context, patch *common_service_models.InferenceModelPatch) (*common_service_models.InferenceModel, error) {
	if c == nil || c.persistence == nil {
		return nil, fmt.Errorf("inference model catalog is not configured")
	}

	model, err := c.applyInferenceModelPatch(ctx, patch)
	if err != nil {
		return nil, err
	}
	if err := c.validateInferenceModel(model, true); err != nil {
		return nil, err
	}
	row := &common_persistence_models.InferenceModel{
		ID:              model.ID,
		ProviderID:      model.ProviderID,
		ProviderModelID: model.ProviderModelID,
		DisplayName:     model.DisplayName,
		Active:          model.Active,
	}
	if err := c.persistence.UpdateInferenceModel(ctx, row); err != nil {
		return nil, err
	}
	c.invalidateModelCache()
	return model, nil
}

// DeleteInferenceModel performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (c *Catalog) DeleteInferenceModel(ctx context.Context, inferenceModelID uint64) error {
	if c == nil || c.persistence == nil {
		return fmt.Errorf("inference model catalog is not configured")
	}
	if inferenceModelID == 0 {
		return fmt.Errorf("inference model id is required")
	}
	if err := c.persistence.DeleteInferenceModel(ctx, inferenceModelID); err != nil {
		return err
	}
	c.invalidateModelCache()
	return nil
}

// SeedInferenceModelsIfEmpty performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (c *Catalog) SeedInferenceModelsIfEmpty(ctx context.Context, models []*common_service_models.InferenceModel) (bool, int64, error) {
	if c == nil || c.persistence == nil {
		return false, 0, fmt.Errorf("inference model catalog is not configured")
	}
	rows := make([]*common_persistence_models.InferenceModel, 0, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		copyModel := *model
		if err := c.validateInferenceModel(&copyModel, false); err != nil {
			return false, 0, err
		}
		rows = append(rows, &common_persistence_models.InferenceModel{
			ProviderID:      copyModel.ProviderID,
			ProviderModelID: copyModel.ProviderModelID,
			DisplayName:     copyModel.DisplayName,
			Active:          copyModel.Active,
		})
	}
	if len(rows) == 0 {
		return false, 0, fmt.Errorf("at least one inference model is required")
	}
	seeded, count, err := c.persistence.SeedInferenceModelsIfEmpty(ctx, rows)
	if seeded {
		c.invalidateModelCache()
	}
	return seeded, count, err
}

// validateInferenceModel performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (c *Catalog) validateInferenceModel(
	model *common_service_models.InferenceModel,
	requireID bool,
) error {
	if model == nil {
		return fmt.Errorf("inference model is required")
	}
	if requireID && model.ID == 0 {
		return fmt.Errorf("inference model id is required")
	}
	if model.ProviderID == "" {
		return fmt.Errorf("provider is required")
	}

	normalizedModelID, err := NormalizeProviderModelID(model.ProviderID, model.ProviderModelID)
	if err != nil {
		return err
	}
	model.ProviderModelID = normalizedModelID

	model.DisplayName = strings.TrimSpace(model.DisplayName)
	if model.DisplayName == "" {
		return fmt.Errorf("display name is required")
	}

	if c.modelResolver != nil {
		if _, err := c.modelResolver.ResolveModel(model); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("runtime validation not configured: a model resolver must be set before creating or updating inference models")
	}
	return nil
}

// applyInferenceModelPatch performs its package-specific work in the GenAI model resolution and runtime construction flow.
func (c *Catalog) applyInferenceModelPatch(ctx context.Context, patch *common_service_models.InferenceModelPatch) (*common_service_models.InferenceModel, error) {
	if patch == nil {
		return nil, fmt.Errorf("inference model patch is required")
	}
	if patch.ID == 0 {
		return nil, fmt.Errorf("inference model id is required")
	}

	if err := validateInferenceModelPatch(patch); err != nil {
		return nil, err
	}

	row, err := c.persistence.GetInferenceModelByID(ctx, patch.ID)
	if err != nil {
		return nil, err
	}
	model := inferenceModelFromPersistenceRow(row)
	if model == nil {
		return nil, fmt.Errorf("inference model %d not found", patch.ID)
	}

	for _, path := range patch.UpdateMask {
		switch path {
		case "provider_id":
			model.ProviderID = *patch.ProviderID
		case "provider_model_id":
			model.ProviderModelID = *patch.ProviderModelID
		case "display_name":
			model.DisplayName = *patch.DisplayName
		case "active":
			model.Active = *patch.Active
		}
	}

	return model, nil
}

// validateInferenceModelPatch performs its package-specific work in the GenAI model resolution and runtime construction flow.
func validateInferenceModelPatch(patch *common_service_models.InferenceModelPatch) error {
	if patch == nil {
		return fmt.Errorf("inference model patch is required")
	}
	mask, err := common_util.SanitizeInferenceModelUpdateMask(patch.UpdateMask)
	if err != nil {
		return err
	}

	for _, path := range mask.Paths {
		switch path {
		case "provider_id":
			if patch.ProviderID == nil || *patch.ProviderID == "" {
				return fmt.Errorf("provider is required")
			}
		case "provider_model_id":
			if patch.ProviderModelID == nil {
				return fmt.Errorf("provider model id is required")
			}
			trimmed := strings.TrimSpace(*patch.ProviderModelID)
			if trimmed == "" {
				return fmt.Errorf("provider model id is required")
			}
			patch.ProviderModelID = &trimmed
		case "display_name":
			if patch.DisplayName == nil {
				return fmt.Errorf("display name is required")
			}
			trimmed := strings.TrimSpace(*patch.DisplayName)
			if trimmed == "" {
				return fmt.Errorf("display name is required")
			}
			patch.DisplayName = &trimmed
		case "active":
			if patch.Active == nil {
				return fmt.Errorf("active is required")
			}
		}
	}

	patch.UpdateMask = mask.Paths
	return nil
}

// inferenceModelFromPersistenceRow performs its package-specific work in the GenAI model resolution and runtime construction flow.
func inferenceModelFromPersistenceRow(row *common_persistence_models.InferenceModel) *common_service_models.InferenceModel {
	if row == nil {
		return nil
	}
	providerModelID := strings.TrimSpace(row.ProviderModelID)
	if normalizedModelID, err := NormalizeProviderModelID(row.ProviderID, providerModelID); err == nil {
		providerModelID = normalizedModelID
	}
	return &common_service_models.InferenceModel{
		ID:              row.ID,
		ProviderID:      row.ProviderID,
		ProviderModelID: providerModelID,
		DisplayName:     row.DisplayName,
		Active:          row.Active,
	}
}

// ListProviders returns the current runtime registry's safe provider descriptors.
func (c *Catalog) ListProviders() ([]ProviderDescriptor, error) {
	if c == nil {
		return nil, fmt.Errorf("inference model catalog is not configured")
	}
	source, ok := c.modelResolver.(interface {
		ListProviders() ([]ProviderDescriptor, error)
	})
	if !ok {
		return nil, fmt.Errorf("inference provider registry is not configured")
	}
	return source.ListProviders()
}
