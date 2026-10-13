// Admin control-plane route administration: listing code-registered operations with their routes
// and applying validated route edits.
package admin

import (
	"context"
	stderrors "errors"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/inferenceerr"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ListInferenceRoutes returns every registered operation with its optional persisted route.
func (h *AdminHandler[U]) ListInferenceRoutes(ctx context.Context) (*proto_models.ListInferenceRoutesResponse, error) {
	store := h.inferenceRoutes
	if _, err := h.requireInferenceControlPlane(ctx, permission.PermissionInferenceViewing, "list inference routes", store); err != nil {
		return nil, err
	}

	routes, err := store.ListRoutes(ctx)
	if err != nil {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "list_inference_routes"))
	}
	routesByOperation := make(map[int32]*common_persistence_models.InferenceOperationRoute, len(routes))
	for i := range routes {
		routesByOperation[routes[i].Operation] = &routes[i]
	}

	models := newModelSnapshotCache(store)
	operations := h.inferenceRegistry.Operations()
	modelIDs := make([]uint64, 0, len(operations))
	seen := make(map[uint64]bool, len(operations))
	for _, operation := range operations {
		if route := routesByOperation[int32(operation.Operation)]; route != nil && !seen[route.InferenceModelID] {
			seen[route.InferenceModelID] = true
			modelIDs = append(modelIDs, route.InferenceModelID)
		}
	}
	models.prefetch(ctx, modelIDs)
	entries := make([]*proto_models.InferenceOperationRouteEntry, 0, len(operations))
	for _, operation := range operations {
		entry := inferenceRouteEntryToProto(operation, nil, "", "")
		if route := routesByOperation[int32(operation.Operation)]; route != nil {
			provider, providerModelID := models.snapshot(ctx, route.InferenceModelID)
			entry.Route = inferenceRouteToProto(route, provider, providerModelID, h.effectiveInferenceAPI(provider, route.API))
		}
		entries = append(entries, entry)
	}
	return &proto_models.ListInferenceRoutesResponse{Routes: entries}, nil
}

// UpdateInferenceRoute replaces the mutable route fields for one registered operation.
func (h *AdminHandler[U]) UpdateInferenceRoute(ctx context.Context, req *proto_models.UpdateInferenceRouteRequest) (*proto_models.UpdateInferenceRouteResponse, error) {
	store := h.inferenceRoutes
	adminUserID, err := h.requireInferenceControlPlane(ctx, permission.PermissionInferenceAdministration, "update inference route", store)
	if err != nil {
		return nil, err
	}
	if req == nil || req.Operation == 0 {
		return nil, errors.ErrInvalidRequest
	}
	operation, ok := h.inferenceRegistry.Lookup(controlplane.Operation(req.Operation))
	if !ok {
		return nil, errors.ErrInferenceOperationNotFound
	}

	retryPolicy := controlplane.RetryPolicy{
		TotalTimeout:   req.TotalTimeout.AsDuration(),
		MaxAttempts:    int(req.MaxAttempts),
		InitialBackoff: req.InitialBackoff.AsDuration(),
		MaxBackoff:     req.MaxBackoff.AsDuration(),
	}
	if err := controlplane.ValidateRetryPolicy(retryPolicy); err != nil {
		return nil, contextualAdminWarn(errors.ErrInvalidRequest, err,
			zap.String(logkeys.Stage, "validate_inference_route_retry_policy"),
			zap.Int32(logkeys.Operation, req.Operation))
	}
	if err := controlplane.ValidateTemperature(req.Temperature); err != nil {
		return nil, contextualAdminWarn(errors.ErrInvalidRequest, err,
			zap.String(logkeys.Stage, "validate_inference_route_temperature"),
			zap.Int32(logkeys.Operation, req.Operation))
	}

	model, err := store.GetModel(ctx, req.InferenceModelId)
	if err != nil {
		if stderrors.Is(err, common_persistence.ErrControlPlaneModelNotFound) {
			return nil, contextualAdminError(errors.ErrInferenceModelNotFound, err,
				zap.String(logkeys.Stage, "load_model_for_inference_route"),
				zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId),
				zap.Int32(logkeys.Operation, req.Operation))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "load_model_for_inference_route"),
			zap.Uint64(appLogger.KeyInferenceModelID, req.InferenceModelId),
			zap.Int32(logkeys.Operation, req.Operation))
	}
	if !model.Active {
		return nil, contextualAdminWarn(errors.ErrInvalidRequest, nil,
			zap.String(logkeys.Stage, "validate_inference_route_model"),
			zap.String(logkeys.Reason, "inactive_model"),
			zap.Int32(logkeys.Operation, req.Operation),
			zap.Uint64(appLogger.KeyInferenceModelID, model.ID))
	}

	existing, err := store.GetRoute(ctx, req.Operation)
	if err != nil && !stderrors.Is(err, common_persistence.ErrControlPlaneRouteNotFound) {
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "load_existing_inference_route"),
			zap.Int32(logkeys.Operation, req.Operation))
	}

	api := apishape.Default
	if existing != nil {
		api = existing.API
	}
	if req.Api != nil {
		api = *req.Api
	}
	if err := h.inferenceRegistry.ValidateModelRoute(ctx, model.ProviderID, model.ProviderModelID, req.Temperature, api); err != nil {
		return nil, contextualAdminWarn(errors.ErrInvalidRequest, err,
			zap.String(logkeys.Stage, "validate_inference_route_model_settings"),
			zap.Int32(logkeys.Operation, req.Operation),
			zap.Uint64(appLogger.KeyInferenceModelID, model.ID))
	}

	route := &common_persistence_models.InferenceOperationRoute{
		Operation:        req.Operation,
		API:              api,
		InferenceModelID: model.ID,
		TotalTimeout:     retryPolicy.TotalTimeout,
		MaxAttempts:      int32(retryPolicy.MaxAttempts),
		InitialBackoff:   retryPolicy.InitialBackoff,
		MaxBackoff:       retryPolicy.MaxBackoff,
		Temperature:      req.Temperature,
		UpdaterUserID:    &adminUserID,
	}
	if existing != nil {
		route.ModelListVersion = existing.ModelListVersion
	}
	if policies, supported := store.(controlplane.RoutingPolicyStore); supported {
		snapshot, err := policies.GetInferenceRoutingSnapshot(ctx, 0, []int32{req.Operation}, []uint64{model.ID})
		if err != nil {
			return nil, inferenceerr.Translate(err)
		}
		// Validate the snapshot as it will read after the write; a first route has nothing to replace.
		replaced := false
		for index := range snapshot.Routes {
			if snapshot.Routes[index].Operation == req.Operation {
				route.ModelListVersion = snapshot.Routes[index].ModelListVersion
				snapshot.Routes[index] = *route
				replaced = true
			}
		}
		if !replaced {
			snapshot.Routes = append(snapshot.Routes, *route)
		}
		if err := h.inferenceRegistry.ValidateOperationModels(ctx, controlplane.Operation(req.Operation), snapshot.Options, snapshot); err != nil {
			return nil, contextualAdminWarn(errors.ErrInvalidRequest, err, zap.String(logkeys.Stage, "validate_inference_operation_models"))
		}
	}
	// UpsertRoute assigns config_version atomically and refreshes route from the written row.
	if err := store.UpsertRoute(ctx, route); err != nil {
		// A stale model-list version or an unlisted model is a policy error, not a database one.
		if translated := inferenceerr.Translate(err); translated != err {
			return nil, contextualAdminWarn(translated, nil,
				zap.String(logkeys.Stage, "upsert_inference_route"),
				zap.Int32(logkeys.Operation, req.Operation),
				zap.Uint64(appLogger.KeyInferenceModelID, model.ID))
		}
		return nil, contextualAdminError(errors.ErrDatabaseError, err,
			zap.String(logkeys.Stage, "upsert_inference_route"),
			zap.Int32(logkeys.Operation, req.Operation),
			zap.Uint64(appLogger.KeyInferenceModelID, model.ID))
	}

	appLogger.For(ctx, h.logger, adminLoggerName).Info("Updated inference route",
		zap.Uint64(appLogger.KeyAdminUserID, adminUserID),
		zap.Int32(logkeys.Operation, req.Operation),
		zap.Uint64(appLogger.KeyInferenceModelID, model.ID))

	return &proto_models.UpdateInferenceRouteResponse{Route: inferenceRouteEntryToProto(
		operation,
		route,
		model.ProviderID,
		model.ProviderModelID,
		h.effectiveInferenceAPI(model.ProviderID, route.API),
	)}, nil
}

type modelSnapshotCache struct {
	store InferenceModelReader
	seen  map[uint64]*common_persistence_models.InferenceModel
}

// Optional capability: existing route stores retain their sequential lookup behavior.
type inferenceModelBatchReader interface {
	GetModels(context.Context, []uint64) ([]common_persistence_models.InferenceModel, error)
}

func (c *modelSnapshotCache) prefetch(ctx context.Context, modelIDs []uint64) {
	reader, ok := c.store.(inferenceModelBatchReader)
	if !ok || len(modelIDs) == 0 {
		return
	}
	models, err := reader.GetModels(ctx, modelIDs)
	if err != nil {
		// Preserve the existing best-effort per-model reads on a batch failure.
		return
	}
	for _, id := range modelIDs {
		c.seen[id] = nil
	}
	for i := range models {
		c.seen[models[i].ID] = &models[i]
	}
}

func newModelSnapshotCache(store InferenceModelReader) *modelSnapshotCache {
	return &modelSnapshotCache{store: store, seen: make(map[uint64]*common_persistence_models.InferenceModel)}
}

func (c *modelSnapshotCache) snapshot(ctx context.Context, modelID uint64) (string, string) {
	model, ok := c.seen[modelID]
	if !ok {
		loaded, err := c.store.GetModel(ctx, modelID)
		if err != nil {
			loaded = nil
		}
		c.seen[modelID] = loaded
		model = loaded
	}
	if model == nil {
		return "", ""
	}
	return model.ProviderID, model.ProviderModelID
}

func inferenceRouteEntryToProto(
	operation controlplane.RegisteredOperation,
	route *common_persistence_models.InferenceOperationRoute,
	provider string,
	providerModelID string,
	effectiveAPI ...proto_models.InferenceAPI,
) *proto_models.InferenceOperationRouteEntry {
	entry := &proto_models.InferenceOperationRouteEntry{
		Operation:    int32(operation.Operation),
		SymbolicName: operation.SymbolicName,
		DisplayName:  operation.DisplayName,
		SortOrder:    operation.SortOrder,
		Required:     operation.Required,
	}
	if route != nil {
		entry.Route = inferenceRouteToProto(route, provider, providerModelID, effectiveAPI...)
	}
	return entry
}

func inferenceRouteToProto(
	route *common_persistence_models.InferenceOperationRoute,
	provider string,
	providerModelID string,
	effectiveAPI ...proto_models.InferenceAPI,
) *proto_models.InferenceOperationRoute {
	effective := route.API
	if len(effectiveAPI) > 0 {
		effective = effectiveAPI[0]
	}
	return &proto_models.InferenceOperationRoute{
		InferenceModelId: route.InferenceModelID,
		Api:              route.API,
		EffectiveApi:     effective,
		ProviderId:       provider,
		ProviderModelId:  providerModelID,
		TotalTimeout:     durationpb.New(route.TotalTimeout),
		MaxAttempts:      route.MaxAttempts,
		InitialBackoff:   durationpb.New(route.InitialBackoff),
		MaxBackoff:       durationpb.New(route.MaxBackoff),
		Temperature:      route.Temperature,
		ConfigVersion:    route.ConfigVersion,
		UpdaterUserId:    route.UpdaterUserID,
		CreatedAt:        timestamppb.New(route.CreatedAt),
		UpdatedAt:        timestamppb.New(route.UpdatedAt),
	}
}

// Historical routes remain visible if their provider is removed or its registry cannot be read.
func (h *AdminHandler[U]) effectiveInferenceAPI(providerID string, api proto_models.InferenceAPI) proto_models.InferenceAPI {
	if api != apishape.Default || h.inferenceModelCatalog == nil {
		return api
	}
	providers, err := h.inferenceModelCatalog.ListProviders()
	if err != nil {
		return api
	}
	for _, provider := range providers {
		if provider.ID == providerID {
			return provider.DefaultAPI
		}
	}
	return api
}
