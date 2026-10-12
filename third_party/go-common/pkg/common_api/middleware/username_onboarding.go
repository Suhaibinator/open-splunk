package middleware

import (
	"net/http"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// PathChecker returns true if the given method+path combination is allowed.
type PathChecker func(method, path string) bool

// UsernamePrefixEnforcementMiddleware restricts users with a placeholder username to allowed paths.
func UsernamePrefixEnforcementMiddleware[U common_service_models.UserLike](
	prefix string,
	isAllowedPath PathChecker,
	logger *zap.Logger,
) common.Middleware {
	normalizedPrefix := strings.TrimSpace(prefix)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if normalizedPrefix == "" || r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}

			user, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
			if !success || user == nil {
				next.ServeHTTP(w, r)
				return
			}

			if !strings.HasPrefix((*user).GetUsername(), normalizedPrefix) {
				next.ServeHTTP(w, r)
				return
			}

			if isAllowedPath != nil && isAllowedPath(r.Method, r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			appLogger.For(r.Context(), logger, commonAPILoggerName).Warn("Blocked request for user with default username",
				zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
				zap.String(logkeys.Path, r.URL.Path),
				zap.String(logkeys.Method, r.Method))

			writeForbidden(w, r, "Username update required")
		})
	}
}

// UsernamePrefixBlockMiddleware blocks all paths for users with a placeholder username.
func UsernamePrefixBlockMiddleware[U common_service_models.UserLike](
	prefix string,
	logger *zap.Logger,
) common.Middleware {
	return UsernamePrefixEnforcementMiddleware[U](prefix, nil, logger)
}
