package admin

import (
	"context"
	stderrors "errors"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ListInferenceModels returns the full inference model catalog. It requires PermissionInferenceViewing.
func (h *AdminHandler[U]) ListInferenceModels(ctx context.Context) ([]*common_service_models.InferenceModel, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionInferenceViewing, "list inference models"); err != nil {
		return nil, err
	}
	return h.inferenceModelCatalog.ListInferenceModels(ctx)
}

// ListInferenceModelsPage returns a bounded admin-facing page while leaving the
// catalog's full-list cache available to runtime model resolution.
func (h *AdminHandler[U]) ListInferenceModelsPage(
	ctx context.Context,
	pagination *proto_models.PaginationRequest,
) ([]*common_service_models.InferenceModel, *proto_models.PaginationResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionInferenceViewing, "list inference models"); err != nil {
		return nil, nil, err
	}
	return h.inferenceModelCatalog.ListInferenceModelsPage(ctx, pagination)
}

// CreateInferenceModel persists a new inference model and returns the stored record. It requires
// PermissionSystemAccess and maps catalog errors (e.g. a duplicate) to their service-layer equivalents.
func (h *AdminHandler[U]) CreateInferenceModel(ctx context.Context, model *common_service_models.InferenceModel) (*common_service_models.InferenceModel, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionSystemAccess, "create inference model"); err != nil {
		return nil, err
	}
	createdModel, err := h.inferenceModelCatalog.CreateInferenceModel(ctx, model)
	if err != nil {
		return nil, mapInferenceModelError(err)
	}
	return createdModel, nil
}

// UpdateInferenceModel applies patch to an existing model and returns the updated record. It
// requires PermissionSystemAccess and maps catalog errors (e.g. not-found) to service-layer errors.
func (h *AdminHandler[U]) UpdateInferenceModel(ctx context.Context, patch *common_service_models.InferenceModelPatch) (*common_service_models.InferenceModel, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionSystemAccess, "update inference model"); err != nil {
		return nil, err
	}
	updatedModel, err := h.inferenceModelCatalog.UpdateInferenceModel(ctx, patch)
	if err != nil {
		return nil, mapInferenceModelError(err)
	}
	return updatedModel, nil
}

// DeleteInferenceModel removes an inference model by id. It requires PermissionSystemAccess and
// maps catalog errors — notably ErrInferenceModelInUse when a route still references the model — to
// their service-layer equivalents.
func (h *AdminHandler[U]) DeleteInferenceModel(ctx context.Context, inferenceModelID uint64) error {
	if _, _, err := h.requirePermission(ctx, permission.PermissionSystemAccess, "delete inference model"); err != nil {
		return err
	}
	return mapInferenceModelError(h.inferenceModelCatalog.DeleteInferenceModel(ctx, inferenceModelID))
}

// mapInferenceModelError translates persistence catalog errors (already-exists, not-found, in-use)
// into their service-layer equivalents so callers can match on the service error set. Other errors,
// and nil, pass through unchanged.
func mapInferenceModelError(err error) error {
	switch {
	case err == nil:
		return nil
	case stderrors.Is(err, common_persistence.ErrInferenceModelAlreadyExists):
		return serviceErrors.ErrInferenceModelAlreadyExists
	case stderrors.Is(err, common_persistence.ErrInferenceModelNotFound):
		return serviceErrors.ErrInferenceModelNotFound
	case stderrors.Is(err, common_persistence.ErrInferenceModelInUse):
		return serviceErrors.ErrInferenceModelInUse
	default:
		return err
	}
}
