// Package permission provides a centralized permission management system for go-common.
// It replaces hardcoded UserType thresholds with configurable permission checks,
// allowing for dynamic permission management without code changes.
//
// The permission system is designed around operations rather than specific user types,
// making it flexible and maintainable. All permission thresholds are configurable
// via configuration, providing runtime policy updates at decision boundaries.
package permission

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// PermissionHandlerInterface defines the contract for permission management.
// It provides methods for checking permissions, comparing user types, and
// performing admin-specific operations. All methods are context-aware and
// support the established patterns used throughout go-common.
type PermissionHandlerInterface interface {
	// HasSitePermission checks if a user type has permission to perform the specified site operation.
	HasSitePermission(ctx context.Context, userType proto_models.UserType, operation SitePermissionOperation) bool

	// HasOrgPermission checks if a user has permission to perform an org-scoped
	// operation using a caller-supplied org role. This supports two-tier checks:
	HasOrgPermission(
		ctx context.Context,
		userType proto_models.UserType,
		role proto_models.OrganizationRole,
		operation OrganizationPermissionOperation,
		depth int,
	) (bool, error)

	// GetRequiredUserTypeForSite returns the minimum user type required for a site operation.
	// This is useful for error messages and UI logic that needs to display
	GetRequiredUserTypeForSite(operation SitePermissionOperation) (proto_models.UserType, error)

	// GetRequiredUserTypeForOrg returns the minimum user type required for an org operation.
	// This is useful for error messages and UI logic that needs to display
	GetRequiredUserTypeForOrg(operation OrganizationPermissionOperation) (proto_models.UserType, error)

	// GetRequiredOrgRole returns the minimum organization role required for an operation.
	GetRequiredOrgRole(operation OrganizationPermissionOperation, depth int) (proto_models.OrganizationRole, error)

	// FilterUsersBySitePermission filters a slice of users to only include those
	// who have permission for the specified site operation. This is useful for
	// bulk operations where you need to determine which users can perform
	FilterUsersBySitePermission(ctx context.Context, users []common_service_models.UserLike, operation SitePermissionOperation) []common_service_models.UserLike

	// CompareAdminTiers compares two admin user types and returns:
	//   - negative if userType1 has lower privilege than userType2
	//   - zero if they have equal privilege
	//   - positive if userType1 has higher privilege than userType2
	//
	// This is useful for operations where you need to ensure one admin
	// has sufficient privilege relative to another (e.g., an admin can only
	CompareAdminTiers(userType1, userType2 proto_models.UserType) int
}
