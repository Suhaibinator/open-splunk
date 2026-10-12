package admin

import (
	"context"
	stderrors "errors"
	"sort"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/genai"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// ListSelectableInferenceModels returns the models an actor may name in a request-scoped
// inference.ModelSelection: active catalog models whose provider is currently configured. It
// requires PermissionInferenceModelOverride, not inference viewing, so everyone allowed to select
// a model can see the choices. This is a picker read surface only; selection itself is authorized
// again at preparation and on every durable run drive.
//
// When req names a registered operation that currently has a route, the response also describes
// that routed default so a picker can label its "use the default" choice.
func (h *AdminHandler[U]) ListSelectableInferenceModels(ctx context.Context, req *proto_models.ListSelectableInferenceModelsRequest) (*proto_models.ListSelectableInferenceModelsResponse, error) {
	if _, _, err := h.requirePermission(ctx, permission.PermissionInferenceModelOverride, "list selectable inference models"); err != nil {
		return nil, err
	}
	if h.inferenceModelCatalog == nil {
		return nil, errors.ErrInferenceControlPlaneUnavailable
	}
	descriptors, err := h.inferenceModelCatalog.ListProviders()
	if err != nil {
		return nil, err
	}
	providers := make(map[string]genai.ProviderDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		providers[descriptor.ID] = descriptor
	}
	models, err := h.inferenceModelCatalog.ListInferenceModels(ctx)
	if err != nil {
		return nil, err
	}

	response := &proto_models.ListSelectableInferenceModelsResponse{
		Models: make([]*proto_models.SelectableInferenceModel, 0, len(models)),
	}
	for _, model := range models {
		if model == nil || !model.Active {
			continue
		}
		provider, configured := providers[model.ProviderID]
		if !configured {
			continue
		}
		displayName := provider.Name
		if displayName == "" {
			displayName = provider.ID
		}
		response.Models = append(response.Models, &proto_models.SelectableInferenceModel{
			Id:                  model.ID,
			DisplayName:         model.DisplayName,
			ProviderId:          model.ProviderID,
			ProviderDisplayName: displayName,
			ProviderModelId:     model.ProviderModelID,
			DefaultApi:          provider.DefaultAPI,
			SupportedApis:       append([]proto_models.InferenceAPI(nil), provider.SupportedAPIs...),
		})
	}
	sort.SliceStable(response.Models, func(i, j int) bool {
		a, b := response.Models[i], response.Models[j]
		if providerA, providerB := strings.ToLower(a.ProviderDisplayName), strings.ToLower(b.ProviderDisplayName); providerA != providerB {
			return providerA < providerB
		}
		return strings.ToLower(a.DisplayName) < strings.ToLower(b.DisplayName)
	})

	if req.GetOperation() == 0 || h.inferenceRoutes == nil || h.inferenceRegistry == nil {
		return response, nil
	}
	if _, registered := h.inferenceRegistry.Lookup(controlplane.Operation(req.GetOperation())); !registered {
		return response, nil
	}
	route, err := h.inferenceRoutes.GetRoute(ctx, req.GetOperation())
	if stderrors.Is(err, common_persistence.ErrControlPlaneRouteNotFound) || (err == nil && route == nil) {
		return response, nil
	}
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_selectable_inference_models_route"))
	}
	response.RoutedInferenceModelId = &route.InferenceModelID
	for _, model := range models {
		if model != nil && model.ID == route.InferenceModelID {
			displayName := model.DisplayName
			response.RoutedModelDisplayName = &displayName
			effective := h.effectiveInferenceAPI(model.ProviderID, route.API)
			response.RoutedEffectiveApi = &effective
			break
		}
	}
	return response, nil
}
