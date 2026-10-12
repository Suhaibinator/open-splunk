package organization

import (
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// attributePermissionTables is one compiled generation of the attribute
// visibility policy.
type attributePermissionTables struct {
	byDepth []config.OrgRoleAttributePermissions
	global  config.UserTypeAttributePermissions
}

func compileAttributePermissionTables(view PolicyView) attributePermissionTables {
	return attributePermissionTables{
		byDepth: config.CompileOrgRoleAttributePermissionsByDepth(view.OrgUserAttributePermissions()),
		global:  config.CompileUserTypeAttributePermissions(view.GlobalUserAttributePermissions()),
	}
}

// atDepth returns the organization-role policy for depth, or nil when the
// depth has no configured policy.
func (t attributePermissionTables) atDepth(depth int) *config.OrgRoleAttributePermissions {
	if depth < 0 || depth >= len(t.byDepth) {
		return nil
	}
	return &t.byDepth[depth]
}

// unionPermissions is the most permissive combination of the viewer's
// organization role policy at depth and their global user-type policy. It
// returns nil when neither is configured, which callers treat as "no
// filtering".
func (t attributePermissionTables) unionPermissions(
	orgRole proto_models.OrganizationRole,
	userType proto_models.UserType,
	orgDepth int,
) *config.UserAttributePermissions {
	var orgPermissions *config.UserAttributePermissions
	if byRole := t.atDepth(orgDepth); byRole != nil {
		orgPermissions = byRole.GetPermissions(orgRole)
	}
	globalPermissions := t.global.GetPermissions(userType)
	if orgPermissions == nil && globalPermissions == nil {
		return nil
	}
	var union config.UserAttributePermissions
	if orgPermissions != nil {
		union = *orgPermissions
	}
	if globalPermissions != nil {
		union = union.Union(*globalPermissions)
	}
	return &union
}

// organizationCollections shares ownership-safe collection reads once per generation.
type organizationCollections struct {
	creatorRoles, associationRoles []proto_models.OrganizationRole
	creatorAutoAdd                 []bool
}

func newOrganizationCollectionsMemo() *config.GenerationMemo[PolicyView, *organizationCollections] {
	return config.NewGenerationMemo(func(view PolicyView) (*organizationCollections, error) {
		return &organizationCollections{creatorRoles: view.DefaultOrgCreatorRole(), associationRoles: view.OrganizationAutoAssociationDefaultRole(), creatorAutoAdd: view.OrgAutoAddCreatorOnChildCreate()}, nil
	}, nil)
}

func (h *OrganizationHandler[U]) collectionPolicy(view PolicyView) *organizationCollections {
	policy, _ := h.collections.Get(view)
	return policy
}

func boolAtDepth(values []bool, depth int) bool {
	return depth >= 0 && depth < len(values) && values[depth]
}
