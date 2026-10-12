package permission

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

const permissionLoggerName = "common_service.permission"

// Compile-time check to ensure PermissionHandler implements PermissionHandlerInterface
var _ PermissionHandlerInterface = (*PermissionHandler)(nil)

// PermissionHandler carries state used by the permission backend flow.
type PermissionHandler struct {
	// config holds the permission configuration with operation thresholds.
	// It is stored atomically so UpdateConfig can swap it while request
	// goroutines perform permission checks concurrently.
	config atomic.Pointer[PermissionConfig]
	// configProvider is the optional authoritative reloading source. config is
	// the manual source when absent, and the validated fallback when present.
	configProvider ConfigProvider

	// logger provides structured logging for permission operations
	logger *zap.Logger
}

// ConfigProvider returns a permission policy captured from one immutable
// configuration generation. It should return a non-nil policy, which must not
// be mutated. A nil result violates the provider contract; the handler records
// the invariant and falls back to the validated startup policy (and any later
// UpdateConfig swaps) for that decision.
type ConfigProvider func() *PermissionConfig

// cfg returns the current permission configuration snapshot. Each permission
// decision loads the snapshot once so a concurrent policy swap cannot mix old
// and new thresholds within a single check. A nil provider result violates the
// provider contract, so the handler records the invariant before using its
// validated manual policy as fallback.
func (h *PermissionHandler) cfg(ctx context.Context, stage string) *PermissionConfig {
	if h.configProvider != nil {
		if snapshot := h.configProvider(); snapshot != nil {
			return snapshot
		}
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, permissionLoggerName), "permission_config_provider_non_nil",
			nil,
			zap.String(logkeys.Stage, stage),
			zap.String(logkeys.Expected, "non-nil permission policy"),
			zap.String(logkeys.Actual, "nil"),
			zap.String(logkeys.Fallback, "validated fallback permission policy"))
	}
	if fallback := h.config.Load(); fallback != nil {
		return fallback
	}

	appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, permissionLoggerName), "permission_fallback_config_available",
		nil,
		zap.String(logkeys.Stage, stage),
		zap.String(logkeys.Expected, "validated fallback permission policy"),
		zap.String(logkeys.Actual, "nil"),
		zap.String(logkeys.Fallback, "deny all permissions"))
	return nil
}

// NewPermissionHandler executes this step in the permission backend flow.
func NewPermissionHandler(
	config *PermissionConfig,
	logger *zap.Logger,
) (*PermissionHandler, error) {
	if config == nil {
		return nil, ErrPermissionConfigurationError
	}

	if logger == nil {
		// Create a no-op logger if none provided (for testing scenarios)
		logger = zap.NewNop()
	}

	// Validate the configuration before using it
	if err := config.Validate(); err != nil {
		return nil, NewPermissionErrorFromString(
			"configuration",
			proto_models.UserType_USER_TYPE_UNSPECIFIED,
			proto_models.UserType_USER_TYPE_UNSPECIFIED,
			"configuration validation failed",
			err,
		)
	}

	h := &PermissionHandler{
		logger: logger,
	}
	h.config.Store(config)
	return h, nil
}

// NewReloadingPermissionHandler constructs a permission handler whose policy is
// captured once at each public decision boundary. A non-nil provider is
// authoritative whenever it returns a policy; UpdateConfig then changes only
// the fallback used when the provider returns nil. A nil provider is ignored,
// leaving UpdateConfig as the active policy source.
func NewReloadingPermissionHandler(
	config *PermissionConfig,
	provider ConfigProvider,
	logger *zap.Logger,
) (*PermissionHandler, error) {
	handler, err := NewPermissionHandler(config, logger)
	if err != nil {
		return nil, err
	}
	handler.configProvider = provider
	return handler, nil
}

// HasSitePermission executes this step in the permission backend flow.
func (h *PermissionHandler) HasSitePermission(ctx context.Context, userType proto_models.UserType, operation SitePermissionOperation) bool {
	cfg := h.cfg(ctx, "site_permission_check")
	if cfg == nil {
		return false
	}
	return h.hasSitePermission(ctx, cfg, userType, operation)
}

// Denial reasons and grant sources recorded on permission decision events.
const (
	grantedByUserType = "user_type"
	grantedByOrgRole  = "org_role"

	reasonBelowUserTypeThreshold = "user_type_below_threshold"
	reasonNoRoleThresholdAtDepth = "no_role_threshold_at_depth"
	reasonNotMember              = "not_a_member"
	reasonRoleBelowThreshold     = "role_below_threshold"
)

func (h *PermissionHandler) hasSitePermission(ctx context.Context, cfg *PermissionConfig, userType proto_models.UserType, operation SitePermissionOperation) bool {
	requiredType, exists := cfg.requiredUserTypeForSite(operation)
	if !exists {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, permissionLoggerName), "site_permission_operation_configured",
			nil,
			zap.String(logkeys.Stage, "site_permission_check"),
			zap.Stringer(logkeys.Operation, operation),
			zap.String(logkeys.Expected, "configured permission operation"),
			zap.String(logkeys.Actual, "unknown operation"),
			zap.String(logkeys.Fallback, "deny permission"))
		return false
	}
	allowed := userType >= requiredType
	if operation == PermissionInferenceModelOverride {
		allowed = validateUserTypeThreshold("actor", userType) == nil && config.ValidateInferenceModelOverrideThreshold(requiredType) == nil && CompareUserTypes(userType, requiredType) >= 0
	}

	// Every evaluation is a Debug event and every denial an Info event. The
	// logger is derived only when one of them will be written, so the allow
	// path with debug disabled performs no logging work at all. The caller that
	// owns the operation still attaches the denial outcome so the boundary
	// emits one security record with the full subject/resource context.
	if !allowed || appLogger.DebugEnabled(h.logger) {
		if log := appLogger.For(ctx, h.logger, permissionLoggerName); log != nil {
			if ce := log.Check(zap.DebugLevel, "Site permission evaluated"); ce != nil {
				ce.Write(
					zap.Stringer(logkeys.Operation, operation),
					zap.Stringer(appLogger.KeyUserType, userType),
					zap.Stringer(appLogger.KeyRequiredType, requiredType),
					zap.Bool(appLogger.KeyHasPermission, allowed),
				)
			}
			if !allowed {
				log.Info("Site permission denied",
					zap.Stringer(logkeys.Operation, operation),
					zap.Stringer(appLogger.KeyUserType, userType),
					zap.Stringer(appLogger.KeyRequiredType, requiredType),
					zap.String(logkeys.Reason, reasonBelowUserTypeThreshold),
				)
			}
		}
	}

	return allowed
}

// HasOrgPermission executes this step in the permission backend flow.
func (h *PermissionHandler) HasOrgPermission(
	ctx context.Context,
	userType proto_models.UserType,
	role proto_models.OrganizationRole,
	operation OrganizationPermissionOperation,
	depth int,
) (bool, error) {
	cfg := h.cfg(ctx, "organization_permission_check")
	if cfg == nil {
		return false, nil
	}
	return h.hasOrgPermission(ctx, cfg, userType, role, operation, depth)
}

func (h *PermissionHandler) hasOrgPermission(
	ctx context.Context,
	cfg *PermissionConfig,
	userType proto_models.UserType,
	role proto_models.OrganizationRole,
	operation OrganizationPermissionOperation,
	depth int,
) (bool, error) {
	requiredType, exists := cfg.requiredUserTypeForOrg(operation)
	roleOnly := requiresOrganizationRole(operation)
	if !exists && !roleOnly {
		err := fmt.Errorf("%w: %s", ErrUnknownOperation, operation.String())
		return false, appLogger.InvariantError(
			"organization_permission_operation_configured",
			err,
			zap.String(logkeys.Stage, "organization_permission_check"),
			zap.Stringer(logkeys.Operation, operation),
			zap.String(logkeys.Expected, "configured permission operation"),
			zap.String(logkeys.Actual, "unknown operation"),
		)
	}

	requiredRole, hasOrgRole := cfg.requiredOrgRole(operation, depth)
	isMember := role != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
	if roleOnly {
		_, knownRole := proto_models.OrganizationRole_name[int32(role)]
		isMember = isMember && knownRole
		_, knownThreshold := proto_models.OrganizationRole_name[int32(requiredRole)]
		hasOrgRole = hasOrgRole && knownThreshold && requiredRole != proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
	}

	var (
		allowed   bool
		grantedBy string
		reason    string
	)
	switch {
	case !roleOnly && userType >= requiredType:
		allowed, grantedBy = true, grantedByUserType
	case !hasOrgRole:
		reason = reasonNoRoleThresholdAtDepth
	case !isMember:
		reason = reasonNotMember
	case role >= requiredRole:
		allowed, grantedBy = true, grantedByOrgRole
	default:
		reason = reasonRoleBelowThreshold
	}

	// Every evaluation is a Debug event and every denial an Info event. The
	// logger is derived only when one of them will be written, so the allow
	// path with debug disabled performs no logging work at all.
	if !allowed || appLogger.DebugEnabled(h.logger) {
		if log := appLogger.For(ctx, h.logger, permissionLoggerName); log != nil {
			if ce := log.Check(zap.DebugLevel, "Organization permission evaluated"); ce != nil {
				ce.Write(
					zap.Stringer(logkeys.Operation, operation),
					zap.Stringer(appLogger.KeyUserType, userType),
					zap.Stringer(appLogger.KeyRole, role),
					zap.Bool(appLogger.KeyIsMember, isMember),
					zap.Int(appLogger.KeyOrgDepth, depth),
					zap.Stringer(appLogger.KeyRequiredType, requiredType),
					zap.Stringer(appLogger.KeyRequiredRole, requiredRole),
					zap.Bool(appLogger.KeyHasPermission, allowed),
					zap.String(appLogger.KeyGrantedBy, grantedBy),
				)
			}
			if !allowed {
				log.Info("Organization permission denied",
					zap.Stringer(logkeys.Operation, operation),
					zap.Stringer(appLogger.KeyUserType, userType),
					zap.Stringer(appLogger.KeyRole, role),
					zap.Bool(appLogger.KeyIsMember, isMember),
					zap.Int(appLogger.KeyOrgDepth, depth),
					zap.Stringer(appLogger.KeyRequiredType, requiredType),
					zap.Stringer(appLogger.KeyRequiredRole, requiredRole),
					zap.String(logkeys.Reason, reason),
				)
			}
		}
	}
	return allowed, nil
}

// GetRequiredUserTypeForSite executes this step in the permission backend flow.
func (h *PermissionHandler) GetRequiredUserTypeForSite(operation SitePermissionOperation) (proto_models.UserType, error) {
	cfg := h.cfg(context.Background(), "get_required_site_user_type")
	if cfg == nil {
		return proto_models.UserType_USER_TYPE_UNSPECIFIED, ErrPermissionConfigurationError
	}
	if requiredType, exists := cfg.requiredUserTypeForSite(operation); exists {
		return requiredType, nil
	}
	return proto_models.UserType_USER_TYPE_UNSPECIFIED, fmt.Errorf("%w: %s", ErrUnknownOperation, operation.String())
}

// GetRequiredUserTypeForOrg executes this step in the permission backend flow.
func (h *PermissionHandler) GetRequiredUserTypeForOrg(operation OrganizationPermissionOperation) (proto_models.UserType, error) {
	cfg := h.cfg(context.Background(), "get_required_organization_user_type")
	if cfg == nil {
		return proto_models.UserType_USER_TYPE_UNSPECIFIED, ErrPermissionConfigurationError
	}
	if requiredType, exists := cfg.requiredUserTypeForOrg(operation); exists {
		return requiredType, nil
	}
	return proto_models.UserType_USER_TYPE_UNSPECIFIED, fmt.Errorf("%w: %s", ErrUnknownOperation, operation.String())
}

// GetRequiredOrgRole executes this step in the permission backend flow.
func (h *PermissionHandler) GetRequiredOrgRole(operation OrganizationPermissionOperation, depth int) (proto_models.OrganizationRole, error) {
	cfg := h.cfg(context.Background(), "get_required_organization_role")
	if cfg == nil {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, ErrPermissionConfigurationError
	}
	if requiredRole, exists := cfg.requiredOrgRole(operation, depth); exists {
		return requiredRole, nil
	}
	return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, fmt.Errorf("%w: %s has no org role requirement", ErrUnknownOperation, operation.String())
}

// FilterUsersBySitePermission executes this step in the permission backend flow.
func (h *PermissionHandler) FilterUsersBySitePermission(ctx context.Context, users []common_service_models.UserLike, operation SitePermissionOperation) []common_service_models.UserLike {
	if len(users) == 0 {
		return users
	}

	cfg := h.cfg(ctx, "filter_users_by_site_permission")
	if cfg == nil {
		return []common_service_models.UserLike{}
	}
	if _, exists := cfg.requiredUserTypeForSite(operation); !exists {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, permissionLoggerName), "site_permission_operation_configured",
			nil,
			zap.String(logkeys.Stage, "filter_users_by_site_permission"),
			zap.Stringer(logkeys.Operation, operation),
			zap.String(logkeys.Expected, "configured permission operation"),
			zap.String(logkeys.Actual, "unknown operation"),
			zap.String(logkeys.Fallback, "exclude all users"))
		return []common_service_models.UserLike{}
	}
	filtered := common_util.ApplyToEachFiltered(
		users,
		func(user common_service_models.UserLike) common_service_models.UserLike {
			return user
		},
		func(user common_service_models.UserLike) bool {
			return user != nil && h.hasSitePermissionInternal(cfg, user.GetUserType(), operation)
		},
		nil,
	)

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, permissionLoggerName).Debug("Filtered users by site permission",
			zap.Stringer(logkeys.Operation, operation),
			zap.Int(appLogger.KeyTotal, len(users)),
			zap.Int(appLogger.KeyCount, len(filtered)),
		)
	}

	return filtered
}

// hasSitePermissionInternal executes this step in the permission backend flow.
func (h *PermissionHandler) hasSitePermissionInternal(cfg *PermissionConfig, userType proto_models.UserType, operation SitePermissionOperation) bool {
	requiredType, exists := cfg.requiredUserTypeForSite(operation)
	if !exists {
		return false
	}

	if operation == PermissionInferenceModelOverride {
		return validateUserTypeThreshold("actor", userType) == nil && config.ValidateInferenceModelOverrideThreshold(requiredType) == nil && CompareUserTypes(userType, requiredType) >= 0
	}
	// For non-admin operations, compare by enum value
	// Higher enum values represent higher privilege in the current system
	return userType >= requiredType
}

// CompareAdminTiers executes this step in the permission backend flow.
func (h *PermissionHandler) CompareAdminTiers(userType1, userType2 proto_models.UserType) int {
	return CompareUserTypes(userType1, userType2)
}

// CheckSitePermissionWithError executes this step in the permission backend flow.
func (h *PermissionHandler) CheckSitePermissionWithError(ctx context.Context, userType proto_models.UserType, operation SitePermissionOperation) error {
	cfg := h.cfg(ctx, "check_site_permission")
	if cfg == nil {
		return ErrPermissionConfigurationError
	}
	if h.hasSitePermission(ctx, cfg, userType, operation) {
		return nil
	}

	requiredType, exists := cfg.requiredUserTypeForSite(operation)
	if !exists {
		return NewPermissionErrorWithCause(
			operation,
			userType,
			proto_models.UserType_USER_TYPE_UNSPECIFIED,
			"unknown operation",
			fmt.Errorf("%w: %s", ErrUnknownOperation, operation.String()),
		)
	}

	return NewPermissionError(
		operation,
		userType,
		requiredType,
		"insufficient permission level",
	)
}

// CheckOrgPermissionWithError executes this step in the permission backend flow.
func (h *PermissionHandler) CheckOrgPermissionWithError(ctx context.Context, userType proto_models.UserType, role proto_models.OrganizationRole, operation OrganizationPermissionOperation, depth int) error {
	cfg := h.cfg(ctx, "check_organization_permission")
	if cfg == nil {
		return ErrPermissionConfigurationError
	}
	hasPermission, err := h.hasOrgPermission(ctx, cfg, userType, role, operation, depth)
	if err != nil {
		return err
	}
	if hasPermission {
		return nil
	}

	requiredType, _ := cfg.requiredUserTypeForOrg(operation)
	return NewPermissionError(
		operation,
		userType,
		requiredType,
		"insufficient permission level",
	)
}

// UpdateConfig validates and atomically replaces the handler's default policy.
// For a handler constructed with a non-nil reloading provider, that provider
// remains authoritative and this policy is used only when it returns nil.
func (h *PermissionHandler) UpdateConfig(ctx context.Context, newConfig *PermissionConfig) error {
	if newConfig == nil {
		return ErrPermissionConfigurationError
	}

	if err := newConfig.Validate(); err != nil {
		return NewPermissionErrorFromString(
			"configuration",
			proto_models.UserType_USER_TYPE_UNSPECIFIED,
			proto_models.UserType_USER_TYPE_UNSPECIFIED,
			"new configuration validation failed",
			err,
		)
	}

	h.config.Store(newConfig)
	configurationRole := "authoritative"
	if h.configProvider != nil {
		configurationRole = "fallback"
	}
	appLogger.For(ctx, h.logger, permissionLoggerName).Info("Permission configuration updated",
		zap.Bool(appLogger.KeyProviderBacked, h.configProvider != nil),
		zap.String(appLogger.KeyConfigurationRole, configurationRole))
	return nil
}
