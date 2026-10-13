package common_persistence

import (
	"context"
	"fmt"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// SearchPublicUsers executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) SearchPublicUsers(
	ctx context.Context,
	query string,
	scope proto_models.UserSearchScope,
	organizationID *uint64,
	excludeUserID *uint64,
	allowEmail bool,
	allowUsername bool,
	allowRealName bool,
	pagination *proto_models.PaginationRequest,
	suggestionsOnly bool,
) ([]*common_persistence_models.User, *proto_models.PaginationResponse, error) {
	if err := requirePageLimit("search_public_users", pagination); err != nil {
		return nil, nil, err
	}
	db := h.getDBFromContext(ctx)

	query = strings.TrimSpace(query)
	if query == "" && !suggestionsOnly {
		paginationResponse := common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, false)
		if common_util.ShouldIncludeTotalCount(pagination) {
			paginationResponse = common_util.CreatePaginationResponse(0, pagination.Limit, pagination.Page)
		}
		return []*common_persistence_models.User{}, paginationResponse, nil
	}

	usersTable := common_persistence_models.TableNameUsers
	emailVerificationTable := common_persistence_models.TableNameEmailVerifications
	userOrgTable := common_persistence_models.TableNameUserOrganizations

	searchQuery := db.Model(&common_persistence_models.User{})
	searchQuery = searchQuery.Joins(
		fmt.Sprintf("LEFT JOIN %s ON %s.user_id = %s.id", emailVerificationTable, emailVerificationTable, usersTable),
	)
	searchQuery = searchQuery.Where(fmt.Sprintf("%s.banned = ?", usersTable), false)
	searchQuery = searchQuery.Where(
		fmt.Sprintf("(%s.user_id IS NULL OR (%s.verified_at IS NOT NULL AND %s.email = %s.email))", emailVerificationTable, emailVerificationTable, emailVerificationTable, usersTable),
	)

	if excludeUserID != nil && *excludeUserID != 0 {
		searchQuery = searchQuery.Where(fmt.Sprintf("%s.id <> ?", usersTable), *excludeUserID)
	}

	if scope == proto_models.UserSearchScope_USER_SEARCH_SCOPE_ORGANIZATION && organizationID != nil && *organizationID != 0 {
		searchQuery = searchQuery.Joins(
			fmt.Sprintf("JOIN %s ON %s.user_id = %s.id", userOrgTable, userOrgTable, usersTable),
		)
		searchQuery = searchQuery.Scopes(ActiveOrganizationScope(userOrgTable, "organization_id"))
		searchQuery = searchQuery.Where(fmt.Sprintf("%s.organization_id = ?", userOrgTable), *organizationID)
	}

	if query != "" {
		searchPattern := query + "%"
		clauses := make([]string, 0, 5)
		args := make([]any, 0, 5)

		if allowUsername {
			clauses = append(clauses, fmt.Sprintf("%s.username ILIKE ?", usersTable))
			args = append(args, searchPattern)
		}
		if allowEmail {
			clauses = append(clauses, fmt.Sprintf("%s.email ILIKE ?", usersTable))
			args = append(args, searchPattern)
		}
		if allowRealName {
			clauses = append(clauses, fmt.Sprintf("%s.first_name ILIKE ?", usersTable))
			args = append(args, searchPattern)
			clauses = append(clauses, fmt.Sprintf("%s.last_name ILIKE ?", usersTable))
			args = append(args, searchPattern)
			// `||` instead of CONCAT(): the columns are NOT NULL so the result
			// is identical, and unlike CONCAT() the expression is immutable, so
			// the idx_users_full_name_trgm expression index can serve it.
			clauses = append(clauses,
				fmt.Sprintf("(%s.first_name || ' ' || %s.last_name) ILIKE ?", usersTable, usersTable),
			)
			args = append(args, searchPattern)
		}

		if len(clauses) == 0 {
			paginationResponse := common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, false)
			if common_util.ShouldIncludeTotalCount(pagination) {
				paginationResponse = common_util.CreatePaginationResponse(0, pagination.Limit, pagination.Page)
			}
			return []*common_persistence_models.User{}, paginationResponse, nil
		}

		searchQuery = searchQuery.Where("("+strings.Join(clauses, " OR ")+")", args...)
	}

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	diagnosticFields := []zap.Field{
		zap.String(logkeys.Operation, "search_public_users"),
		zap.String(appLogger.KeySearch, query),
		zap.Stringer(appLogger.KeySearchScope, scope),
		zap.Bool(appLogger.KeySuggestionsOnly, suggestionsOnly),
		zap.Uint32(appLogger.KeyPage, pagination.Page),
		zap.Uint32(logkeys.Limit, pagination.Limit),
	}
	if organizationID != nil {
		diagnosticFields = append(diagnosticFields, zap.Uint64(appLogger.KeyOrganizationID, *organizationID))
	}
	var totalCount int64
	if includeTotalCount {
		if err := searchQuery.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err, diagnosticFields...)
		}
	}

	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	searchQuery = searchQuery.Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount))
	searchQuery = searchQuery.Order(fmt.Sprintf("%s.username ASC", usersTable))

	var users []common_persistence_models.User
	if err := searchQuery.Preload(common_persistence_models.PreloadProfilePictureAttachment).Find(&users).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err, diagnosticFields...)
	}
	users, hasNextPage := common_util.TrimPaginationProbe(users, pagination.Limit, includeTotalCount)

	results := make([]*common_persistence_models.User, len(users))
	for i := range users {
		results[i] = &users[i]
	}

	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage)
	}
	return results, paginationResponse, nil
}
