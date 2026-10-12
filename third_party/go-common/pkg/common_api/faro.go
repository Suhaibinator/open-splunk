package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
)

// FaroCollectResponse FaroCollectResponse defines data/state used by this package and is consumed as part of the backend request and service integration flow.
type FaroCollectResponse struct {
	Status string `json:"status"`
}

// RegisterFaroRoutes RegisterFaroRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterFaroRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableFaro)

	// Create Faro metrics handler. NewFaroMetricsHandler consumes the
	// MetricsHandlerInterface directly (it only needs GetRegistry/GetNamespace,
	// both on the interface), so no concrete type assertion is required. This
	// keeps Faro working for applications that inject a decorated or custom
	// MetricsHandlerInterface implementation instead of *CommonMetricsHandler.
	faroMetricsHandler := common_metrics.NewFaroMetricsHandler(h.metricsHandler, h.logger)

	// The standard JSON codec decodes the body into the typed payload, so decode
	// failures surface as a 400 (or 413 on body-cap overflow) at the router layer
	// instead of being swallowed here. The route sanitizer normalizes the payload
	// before it reaches this handler.
	faroHandler := func(r *http.Request, payload *common_metrics.FaroPayload) (*FaroCollectResponse, error) {
		if payload == nil {
			return nil, router.NewHTTPError(http.StatusBadRequest, "Empty Faro payload")
		}

		faroMetricsHandler.ProcessFaroPayload(r.Context(), payload, r.Referer())

		return &FaroCollectResponse{
			Status: "success",
		}, nil
	}
	group.Route(router.RouteConfig[*common_metrics.FaroPayload, *FaroCollectResponse]{
		Path:       "/faro/collect",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.NoAuth), // No auth required for telemetry
		Codec:      codec.NewJSONCodec[*common_metrics.FaroPayload, *FaroCollectResponse](),
		Handler:    faroHandler,
		SourceType: router.Body,
		// Normalizes label-bound values (never rejects); nil-receiver safe,
		// so a handler built without a sanitizer still works.
		Sanitizer: h.sanitizer.SanitizeFaroPayload,
		Overrides: common.RouteOverrides{
			// This route is unauthenticated, so it must not inherit the global
			// body limit sized for attachment uploads.
			MaxBodySize: h.startup.FaroMaxPayloadSize(),
		},
	})

	h.logger.Named(commonAPILoggerName).Debug("Faro telemetry collection routes registered")
}
