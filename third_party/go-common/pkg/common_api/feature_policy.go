package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
)

// featureGroup keeps routes installed so a disabled module can be enabled by
// the next configuration generation without rebuilding the router.
func (h *CommonAPIHandler[U]) featureGroup(group *router.RouteGroup[common_service_models.UserIdType, U], enabled func(config.APIView) bool) *router.RouteGroup[common_service_models.UserIdType, U] {
	return group.Group("/").Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !enabled(h.provider()) {
				http.NotFound(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
}
