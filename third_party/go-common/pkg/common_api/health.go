package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
)

// RegisterHealthRoutes RegisterHealthRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterHealthRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group.Route(router.RouteConfigBase{
		Path:      pathHealth,
		Methods:   []router.HttpMethod{http.MethodGet, http.MethodOptions},
		AuthLevel: new(router.NoAuth),
		Handler:   h.HealthHandler,
		Overrides: common.RouteOverrides{
			MaxBodySize: 0,
			Timeout:     0,
		},
	})

	h.logger.Named(commonAPILoggerName).Debug("Health route registered to route group")
}

// HealthHandler HealthHandler supports the backend request flow by performing this package's local processing step.
func (h *CommonAPIHandler[U]) HealthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
