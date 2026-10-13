package common_service

import "github.com/Suhaibinator/go-common/pkg/proto_models"

func getRoleAtDepth(roles []proto_models.OrganizationRole, depth int) proto_models.OrganizationRole {
	if depth < 0 || depth >= len(roles) {
		return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED
	}
	return roles[depth]
}
