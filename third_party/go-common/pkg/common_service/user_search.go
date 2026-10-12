package common_service

import (
	"context"
	stdErrors "errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// SearchPublicUsers executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) SearchPublicUsers(
	ctx context.Context,
	params *common_service_models.SearchPublicUsersParams,
) (*common_service_models.SearchPublicUsersResult, error) {
	if params == nil {
		return nil, apperrors.ErrInvalidRequest
	}
	searchConfig := h.runtimeView()
	if searchConfig == nil || !searchConfig.EnableUserSearch() {
		return nil, apperrors.ErrUserSearchDisabled
	}

	requestingUser, hasUser := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !hasUser || requestingUser == nil {
		return nil, apperrors.ErrAuthenticationRequired
	}

	if params.Scope == proto_models.UserSearchScope_USER_SEARCH_SCOPE_UNSPECIFIED {
		return nil, apperrors.ErrInvalidRequest
	}

	if params.SuggestionsOnly && !searchConfig.UserSearchEnableSuggestions() {
		return nil, apperrors.ErrInvalidRequest
	}

	query := strings.TrimSpace(params.Query)
	if maxLength := searchConfig.UserSearchMaxQueryLength(); maxLength > 0 && len(query) > maxLength {
		return nil, apperrors.ErrInvalidRequest
	}
	emptyResult := func() *common_service_models.SearchPublicUsersResult {
		limit := uint32(0)
		page := uint32(0)
		if params.Pagination != nil {
			limit = params.Pagination.Limit
			page = params.Pagination.Page
		}
		pagination := common_util.CreatePaginationResponseWithoutTotalCount(limit, page, false)
		if common_util.ShouldIncludeTotalCount(params.Pagination) {
			pagination = common_util.CreatePaginationResponse(0, limit, page)
		}
		return &common_service_models.SearchPublicUsersResult{
			Users:      []*common_service_models.User{},
			Pagination: pagination,
		}
	}
	if !params.SuggestionsOnly && (query == "" || len(query) < searchConfig.UserSearchMinQueryLength()) {
		return emptyResult(), nil
	}

	allowEmail := searchConfig.UserSearchAllowEmailLookup()
	allowUsername := searchConfig.UserSearchAllowUsernameLookup()
	allowRealName := searchConfig.UserSearchAllowRealNameLookup()

	switch params.Scope {
	case proto_models.UserSearchScope_USER_SEARCH_SCOPE_GLOBAL:
		if (*requestingUser).GetUserType() < searchConfig.UserSearchMinUserType() {
			return nil, apperrors.ErrInsufficientPermissions
		}
	case proto_models.UserSearchScope_USER_SEARCH_SCOPE_ORGANIZATION:
		if params.OrganizationID == nil || *params.OrganizationID == 0 {
			return nil, apperrors.ErrInvalidRequest
		}

		role, isMember, err := h.organizationHandler.GetUserRoleInOrganization(
			ctx,
			(*requestingUser).GetID(),
			common_service_models.OrganizationIdType(*params.OrganizationID),
		)
		if err != nil {
			return nil, err
		}
		if !isMember {
			if searchConfig.UserSearchRequireSameOrganization() && (*requestingUser).GetUserType() < searchConfig.UserSearchMinUserType() {
				return nil, apperrors.ErrInsufficientPermissions
			}
		} else {
			orgDepth, depthErr := h.organizationHandler.GetOrganizationDepth(ctx, common_service_models.OrganizationIdType(*params.OrganizationID))
			if depthErr != nil {
				return nil, depthErr
			}
			requiredRole := getRoleAtDepth(searchConfig.UserSearchMinOrgRole(), orgDepth)
			if role < requiredRole && (*requestingUser).GetUserType() < searchConfig.UserSearchMinUserType() {
				return nil, apperrors.ErrInsufficientPermissions
			}
		}
	default:
		return nil, apperrors.ErrInvalidRequest
	}

	if err := h.checkRateLimit(ctx, rate_limiter.ActionUserSearch, fmt.Sprintf("%d", (*requestingUser).GetID())); err != nil {
		return nil, err
	}

	var excludeUserID *uint64
	if params.ExcludeSelf {
		userID := uint64((*requestingUser).GetID())
		excludeUserID = &userID
	}

	users, pagination, err := h.commonPersistenceHandler.SearchPublicUsers(
		ctx,
		query,
		params.Scope,
		params.OrganizationID,
		excludeUserID,
		allowEmail,
		allowUsername,
		allowRealName,
		params.Pagination,
		params.SuggestionsOnly,
	)
	if err != nil {
		switch {
		case stdErrors.Is(err, common_persistence.ErrDatabaseError):
			return nil, apperrors.ErrDatabaseError
		default:
			return nil, apperrors.ErrInternalError
		}
	}

	serviceUsers := make([]*common_service_models.User, 0, len(users))
	for _, user := range users {
		converted := h.converter.ConvertPersistenceToUser(user)
		if converted == nil {
			continue
		}
		serviceUsers = append(serviceUsers, converted)
	}

	return &common_service_models.SearchPublicUsersResult{
		Users:      serviceUsers,
		Pagination: pagination,
	}, nil
}
