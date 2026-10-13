package common_persistence

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func normalizeInferenceModelForWrite(model *common_persistence_models.InferenceModel) error {
	if model == nil {
		return fmt.Errorf("inference model is required")
	}
	model.ProviderModelID = strings.TrimSpace(model.ProviderModelID)
	model.DisplayName = strings.TrimSpace(model.DisplayName)
	if model.ProviderID == "" {
		return fmt.Errorf("provider is required")
	}
	normalizedModelID, err := inference.NormalizeProviderModelID(model.ProviderID, model.ProviderModelID)
	if err != nil {
		return err
	}
	model.ProviderModelID = normalizedModelID
	return nil
}

// CreateInferenceModel executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) CreateInferenceModel(ctx context.Context, model *common_persistence_models.InferenceModel) error {
	if err := normalizeInferenceModelForWrite(model); err != nil {
		return err
	}
	db := h.getDBFromContext(ctx)
	if err := db.Create(model).Error; err != nil {
		if isUniqueViolation(err) {
			return ErrInferenceModelAlreadyExists
		}
		return appLogger.WithErrorFields(fmt.Errorf("failed to create inference model: %w", err),
			zap.String(logkeys.Stage, "create_inference_model"),
			zap.String(appLogger.KeyProvider, model.ProviderID),
			zap.String(appLogger.KeyProviderModelID, model.ProviderModelID))
	}
	return nil
}

// UpdateInferenceModel executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) UpdateInferenceModel(ctx context.Context, model *common_persistence_models.InferenceModel) error {
	if err := normalizeInferenceModelForWrite(model); err != nil {
		return err
	}
	if model.ID == 0 {
		return fmt.Errorf("inference model id is required")
	}
	db := h.getDBFromContext(ctx)
	tx := db.Model(&common_persistence_models.InferenceModel{}).
		Where("id = ?", model.ID).
		Select("provider_id", "provider_model_id", "display_name", "active", "updated_at").
		Updates(struct {
			ProviderID      string
			ProviderModelID string
			DisplayName     string
			Active          bool
			UpdatedAt       time.Time `gorm:"autoUpdateTime:false"`
		}{
			ProviderID:      model.ProviderID,
			ProviderModelID: model.ProviderModelID,
			DisplayName:     model.DisplayName,
			Active:          model.Active,
			UpdatedAt:       time.Now(),
		})
	if err := tx.Error; err != nil {
		if isUniqueViolation(err) {
			return ErrInferenceModelAlreadyExists
		}
		return appLogger.WithErrorFields(fmt.Errorf("failed to update inference model %d: %w", model.ID, err),
			zap.String(logkeys.Stage, "update_inference_model"),
			zap.Uint64(appLogger.KeyInferenceModelID, model.ID),
			zap.String(appLogger.KeyProvider, model.ProviderID),
			zap.String(appLogger.KeyProviderModelID, model.ProviderModelID))
	}
	if tx.RowsAffected == 0 {
		return ErrInferenceModelNotFound
	}
	return nil
}

// DeleteInferenceModel executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) DeleteInferenceModel(ctx context.Context, inferenceModelID uint64) error {
	if inferenceModelID == 0 {
		return fmt.Errorf("inference model id is required")
	}
	db := h.getDBFromContext(ctx)
	tx := db.Delete(&common_persistence_models.InferenceModel{}, inferenceModelID)
	if err := tx.Error; err != nil {
		if isInferenceModelRouteConstraintError(err) {
			return ErrInferenceModelInUse
		}
		return appLogger.WithErrorFields(fmt.Errorf("failed to delete inference model %d: %w", inferenceModelID, err),
			zap.String(logkeys.Stage, "delete_inference_model"),
			zap.Uint64(appLogger.KeyInferenceModelID, inferenceModelID))
	}
	if tx.RowsAffected == 0 {
		return ErrInferenceModelNotFound
	}
	return nil
}

func isInferenceModelRouteConstraintError(err error) bool {
	code, constraint := postgresErrorState(err)
	return (code == "23001" || code == "23503") && constraint == inferenceOperationRouteModelConstraint
}

// ListInferenceModels executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) ListInferenceModels(ctx context.Context) ([]*common_persistence_models.InferenceModel, error) {
	db := h.getDBFromContext(ctx)
	rows := make([]*common_persistence_models.InferenceModel, 0)
	if err := db.Order("provider_id ASC").Order("display_name ASC").Order("id ASC").Find(&rows).Error; err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("failed to list inference models: %w", err),
			zap.String(logkeys.Stage, "list_inference_models"))
	}
	return rows, nil
}

// ListInferenceModelsPage lists a bounded admin-facing page. Exact totals are
// queried only when explicitly requested; otherwise one extra row is fetched
// to determine whether a subsequent page exists.
func (h *CommonPersistenceHandler[U]) ListInferenceModelsPage(
	ctx context.Context,
	paginationRequest *proto_models.PaginationRequest,
) ([]*common_persistence_models.InferenceModel, *proto_models.PaginationResponse, error) {
	pagination := paginationRequest
	if err := requirePageLimit("list_inference_models_page", pagination); err != nil {
		return nil, nil, err
	}
	db := h.getDBFromContext(ctx)
	query := db.Model(&common_persistence_models.InferenceModel{}).
		Order("provider_id ASC").
		Order("display_name ASC").
		Order("id ASC")

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, appLogger.WithErrorFields(fmt.Errorf("failed to count inference models: %w", err),
				zap.String(logkeys.Stage, "count_inference_models_page"),
				zap.Uint32(appLogger.KeyPage, pagination.Page),
				zap.Uint32(logkeys.Limit, pagination.Limit))
		}
	}

	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	rows := make([]*common_persistence_models.InferenceModel, 0, pagination.Limit)
	if err := query.
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount)).
		Find(&rows).Error; err != nil {
		return nil, nil, appLogger.WithErrorFields(fmt.Errorf("failed to list inference models page: %w", err),
			zap.String(logkeys.Stage, "list_inference_models_page"),
			zap.Uint32(appLogger.KeyPage, pagination.Page),
			zap.Uint32(logkeys.Limit, pagination.Limit))
	}
	rows, hasNextPage := common_util.TrimPaginationProbe(rows, pagination.Limit, includeTotalCount)

	if includeTotalCount {
		return rows, common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page), nil
	}
	return rows, common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage), nil
}

// GetInferenceModelByID executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetInferenceModelByID(ctx context.Context, inferenceModelID uint64) (*common_persistence_models.InferenceModel, error) {
	if inferenceModelID == 0 {
		return nil, fmt.Errorf("inference model id is required")
	}
	db := h.getDBFromContext(ctx)
	var model common_persistence_models.InferenceModel
	if err := db.First(&model, inferenceModelID).Error; err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrInferenceModelNotFound
		}
		return nil, appLogger.WithErrorFields(fmt.Errorf("failed to get inference model: %w", err),
			zap.String(logkeys.Stage, "get_inference_model"),
			zap.Uint64(appLogger.KeyInferenceModelID, inferenceModelID))
	}
	return &model, nil
}

// GetInferenceModelByProviderModelID executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetInferenceModelByProviderModelID(
	ctx context.Context,
	provider string,
	providerModelID string,
) (*common_persistence_models.InferenceModel, error) {
	if provider == "" {
		return nil, fmt.Errorf("provider is required")
	}
	trimmedModelID := strings.TrimSpace(providerModelID)
	if trimmedModelID == "" {
		return nil, fmt.Errorf("provider model id is required")
	}
	normalizedModelID, err := inference.NormalizeProviderModelID(provider, trimmedModelID)
	if err != nil {
		return nil, err
	}
	db := h.getDBFromContext(ctx)
	var model common_persistence_models.InferenceModel
	if err := db.Where("provider_id = ? AND provider_model_id = ?", provider, normalizedModelID).First(&model).Error; err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrInferenceModelNotFound
		}
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Stage, "get_inference_model_by_provider_model_id"),
			zap.String(appLogger.KeyProvider, provider),
			zap.String(appLogger.KeyProviderModelID, normalizedModelID))
	}
	return &model, nil
}

// CountInferenceModels executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) CountInferenceModels(ctx context.Context) (int64, error) {
	db := h.getDBFromContext(ctx)
	var count int64
	if err := db.Model(&common_persistence_models.InferenceModel{}).Count(&count).Error; err != nil {
		return 0, appLogger.WithErrorFields(fmt.Errorf("failed to count inference models: %w", err),
			zap.String(logkeys.Stage, "count_inference_models"))
	}
	return count, nil
}

// SeedInferenceModelsIfEmpty executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) SeedInferenceModelsIfEmpty(ctx context.Context, models []*common_persistence_models.InferenceModel) (bool, int64, error) {
	if len(models) == 0 {
		return false, 0, fmt.Errorf("at least one inference model is required")
	}
	db := h.getDBFromContext(ctx)

	var seeded bool
	var seededCount int
	var existingCount int64
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&common_persistence_models.InferenceModel{}).Count(&existingCount).Error; err != nil {
			return appLogger.WithErrorFields(fmt.Errorf("failed to count inference models: %w", err),
				zap.String(logkeys.Stage, "count_inference_models_before_seed"))
		}
		if existingCount > 0 {
			return nil
		}

		rows := make([]*common_persistence_models.InferenceModel, 0, len(models))
		for _, model := range models {
			if model == nil {
				continue
			}
			row := *model
			if err := normalizeInferenceModelForWrite(&row); err != nil {
				return err
			}
			rows = append(rows, &row)
		}
		if len(rows) == 0 {
			return fmt.Errorf("at least one non-nil inference model is required")
		}
		if err := tx.Create(&rows).Error; err != nil {
			return appLogger.WithErrorFields(fmt.Errorf("failed to seed inference models: %w", err),
				zap.String(logkeys.Stage, "seed_inference_models"),
				zap.Int(appLogger.KeyCount, len(rows)))
		}
		seeded = true
		seededCount = len(rows)
		return nil
	})
	if err != nil {
		return false, 0, err
	}
	if seeded {
		appLogger.For(ctx, h.logger, loggerName).Info("Seeded inference model catalog",
			zap.String(logkeys.Stage, "seed_inference_models"),
			zap.String(appLogger.KeyOutcome, "seeded"),
			zap.Int(appLogger.KeyCount, seededCount))
	} else if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Skipped inference model catalog seed because rows already exist",
			zap.String(logkeys.Stage, "seed_inference_models"),
			zap.String(appLogger.KeyOutcome, "already_initialized"),
			zap.Int64(appLogger.KeyCount, existingCount))
	}
	return seeded, existingCount, nil
}
