package organization

import (
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// startupPolicy contains only settings whose identity must remain fixed.
type startupPolicy struct {
	PersonalWorkspaceOrganizationType proto_models.OrganizationType
	PersonalWorkspaceOwnerRole        proto_models.OrganizationRole
}

// PolicyView is one coherent application generation for organization policy.
type PolicyView interface {
	config.OrganizationsView
	config.FeaturesView
	config.PermissionsView
	config.PaginationView
}

// PolicyProvider returns the current immutable organization generation.
type PolicyProvider func() PolicyView

func newStartupPolicy(view PolicyView) *startupPolicy {
	return &startupPolicy{
		PersonalWorkspaceOrganizationType: view.PersonalWorkspaceOrganizationType(),
		PersonalWorkspaceOwnerRole:        view.PersonalWorkspaceOwnerRole(),
	}
}

func getRoleAtDepth(roles []proto_models.OrganizationRole, depth int) proto_models.OrganizationRole {
	if depth < 0 || depth >= len(roles) {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
	}
	return roles[depth]
}
