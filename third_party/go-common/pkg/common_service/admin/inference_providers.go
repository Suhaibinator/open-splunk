package admin

import (
	"context"

	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ListInferenceProviders exposes public metadata only and samples the live provider source.
func (h *AdminHandler[U]) ListInferenceProviders(ctx context.Context) ([]*proto_models.AdminInferenceProvider, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionInferenceViewing, "list_inference_providers"); err != nil {
		return nil, err
	}
	if h.inferenceModelCatalog == nil {
		return nil, serviceErrors.ErrInferenceControlPlaneUnavailable
	}
	descriptors, err := h.inferenceModelCatalog.ListProviders()
	if err != nil {
		return nil, err
	}
	providers := make([]*proto_models.AdminInferenceProvider, 0, len(descriptors))
	for _, descriptor := range descriptors {
		providers = append(providers, &proto_models.AdminInferenceProvider{
			Id:            descriptor.ID,
			DisplayName:   descriptor.Name,
			DefaultApi:    descriptor.DefaultAPI,
			SupportedApis: append([]proto_models.InferenceAPI(nil), descriptor.SupportedAPIs...),
		})
	}
	return providers, nil
}
