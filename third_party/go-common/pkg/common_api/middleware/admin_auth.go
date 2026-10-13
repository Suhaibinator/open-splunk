// Package middleware provides HTTP middleware functionality for the application
package middleware

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// AdminAuthMiddlewareWithMinType requires an authenticated user meeting the minimum user type.
func AdminAuthMiddlewareWithMinType[U common_service_models.UserLike](
	minUserType proto_models.UserType,
	logger *zap.Logger,
) common.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			// Get the user from context
			user, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
			if !success || user == nil {
				appLogger.For(ctx, logger, commonAPILoggerName).Warn("Unauthenticated user attempted to access protected endpoint",
					zap.String(logkeys.Path, r.URL.Path),
					zap.String(logkeys.Method, r.Method),
					zap.String(logkeys.RemoteAddr, r.RemoteAddr),
					zap.Stringer(appLogger.KeyMinRequiredType, minUserType))

				// Return 401 Unauthorized
				http.Error(w, "Authentication required", http.StatusUnauthorized)
				return
			}

			// Check if user meets minimum type requirement
			userType := (*user).GetUserType()
			if userType < minUserType {
				appLogger.For(ctx, logger, commonAPILoggerName).Warn("User with insufficient privileges attempted to access protected endpoint",
					zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
					zap.Stringer(appLogger.KeyUserType, userType),
					zap.Stringer(appLogger.KeyMinRequiredType, minUserType),
					zap.String(logkeys.Path, r.URL.Path),
					zap.String(logkeys.Method, r.Method))

				writeForbidden(w, r, "Access denied: insufficient privileges")
				return
			}

			// Log successful access for audit trail
			appLogger.For(ctx, logger, commonAPILoggerName).Info("Protected endpoint accessed",
				zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
				zap.Stringer(appLogger.KeyUserType, userType),
				zap.Stringer(appLogger.KeyMinRequiredType, minUserType),
				zap.String(logkeys.Path, r.URL.Path),
				zap.String(logkeys.Method, r.Method))

			// User is authenticated and meets minimum type requirement, continue to the next handler
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// AdminAuthMiddlewareWithPermission requires an authenticated user holding the configured permission.
func AdminAuthMiddlewareWithPermission[U common_service_models.UserLike](
	permissionHandler permission.PermissionHandlerInterface,
	operation permission.SitePermissionOperation,
	logger *zap.Logger,
) common.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			// Get the user from context
			user, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
			if !success || user == nil {
				appLogger.For(ctx, logger, commonAPILoggerName).Warn("Unauthenticated user attempted to access protected endpoint",
					zap.String(logkeys.Path, r.URL.Path),
					zap.String(logkeys.Method, r.Method),
					zap.String(logkeys.RemoteAddr, r.RemoteAddr),
					zap.Stringer(logkeys.Operation, operation))

				// Return 401 Unauthorized
				http.Error(w, "Authentication required", http.StatusUnauthorized)
				return
			}

			if permissionHandler == nil {
				appLogger.LogInvariantViolation(appLogger.For(ctx, logger, commonAPILoggerName), "admin_authorization_has_permission_handler", nil,
					zap.String(logkeys.Path, r.URL.Path),
					zap.String(logkeys.Method, r.Method),
					zap.Stringer(logkeys.Operation, operation),
					zap.String(logkeys.Expected, "configured permission handler"),
					zap.String(logkeys.Actual, "nil permission handler"))
				http.Error(w, "Authorization unavailable", http.StatusInternalServerError)
				return
			}

			// Check if user meets permission requirement
			userType := (*user).GetUserType()
			if !permissionHandler.HasSitePermission(ctx, userType, operation) {
				appLogger.For(ctx, logger, commonAPILoggerName).Warn("User with insufficient privileges attempted to access protected endpoint",
					zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
					zap.Stringer(appLogger.KeyUserType, userType),
					zap.Stringer(logkeys.Operation, operation),
					zap.String(logkeys.Path, r.URL.Path),
					zap.String(logkeys.Method, r.Method))

				writeForbidden(w, r, "Access denied: insufficient privileges")
				return
			}

			// Log successful access for audit trail
			appLogger.For(ctx, logger, commonAPILoggerName).Info("Protected endpoint accessed",
				zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
				zap.Stringer(appLogger.KeyUserType, userType),
				zap.Stringer(logkeys.Operation, operation),
				zap.String(logkeys.Path, r.URL.Path),
				zap.String(logkeys.Method, r.Method))

			// User is authenticated and meets the permission requirement, continue to the next handler
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
