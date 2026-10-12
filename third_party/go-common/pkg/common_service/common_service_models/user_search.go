package common_service_models

import "github.com/Suhaibinator/go-common/pkg/proto_models"

// SearchPublicUsersParams defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type SearchPublicUsersParams struct {
	Query           string
	Scope           proto_models.UserSearchScope
	OrganizationID  *uint64
	SuggestionsOnly bool
	ExcludeSelf     bool
	Pagination      *proto_models.PaginationRequest
}

// SearchPublicUsersResult defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type SearchPublicUsersResult struct {
	Users      []*User
	Pagination *proto_models.PaginationResponse
}
