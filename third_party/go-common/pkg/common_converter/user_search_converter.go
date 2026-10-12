package common_converter

import (
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// SearchPublicUsersParamsFromProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
//
// Parameters:
//   - req: Request payload consumed by this operation.
func (h *ConverterHandler[U]) SearchPublicUsersParamsFromProto(req *proto_models.SearchPublicUsersRequest) *common_service_models.SearchPublicUsersParams {
	if req == nil {
		return nil
	}

	return &common_service_models.SearchPublicUsersParams{
		Query:           req.Query,
		Scope:           req.Scope,
		OrganizationID:  req.OrganizationId,
		SuggestionsOnly: req.SuggestionsOnly,
		ExcludeSelf:     req.ExcludeSelf,
		Pagination:      req.Pagination,
	}
}

// SearchPublicUsersResultToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) SearchPublicUsersResultToProto(result *common_service_models.SearchPublicUsersResult) *proto_models.SearchPublicUsersResponse {
	if result == nil {
		return nil
	}

	identifiers := h.publicIdentifierTypes()
	users := make([]*proto_models.PublicUser, 0, len(result.Users))
	for _, user := range result.Users {
		if user == nil {
			continue
		}
		users = append(users, h.convertUserLikeToPublicProtoWithIdentifiers(user, identifiers))
	}

	return &proto_models.SearchPublicUsersResponse{
		Users:      users,
		Pagination: result.Pagination,
	}
}
