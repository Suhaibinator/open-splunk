package common_persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/nyaruka/phonenumbers"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// adminListUsersSortColumn executes its operation in the persistence flow between service handlers and the database layer.
func adminListUsersSortColumn(sortBy proto_models.AdminListUsersSortBy) string {
	switch sortBy {
	case proto_models.AdminListUsersSortBy_ADMIN_LIST_USERS_SORT_BY_EMAIL:
		return "email"
	case proto_models.AdminListUsersSortBy_ADMIN_LIST_USERS_SORT_BY_USERNAME:
		return "username"
	case proto_models.AdminListUsersSortBy_ADMIN_LIST_USERS_SORT_BY_USER_TYPE:
		return "user_type"
	case proto_models.AdminListUsersSortBy_ADMIN_LIST_USERS_SORT_BY_UPDATED_AT:
		return "updated_at"
	case proto_models.AdminListUsersSortBy_ADMIN_LIST_USERS_SORT_BY_LAST_LOGIN:
		return "last_login_at"
	default:
		return "created_at"
	}
}

// adminUsernameChangeHistorySortColumn executes its operation in the persistence flow between service handlers and the database layer.
func adminUsernameChangeHistorySortColumn(sortBy proto_models.AdminListUsernameChangeHistorySortBy) string {
	switch sortBy {
	case proto_models.AdminListUsernameChangeHistorySortBy_ADMIN_LIST_USERNAME_CHANGE_HISTORY_SORT_BY_TARGET_USER_ID:
		return "target_user_id"
	case proto_models.AdminListUsernameChangeHistorySortBy_ADMIN_LIST_USERNAME_CHANGE_HISTORY_SORT_BY_ACTOR_USER_ID:
		return "actor_user_id"
	case proto_models.AdminListUsernameChangeHistorySortBy_ADMIN_LIST_USERNAME_CHANGE_HISTORY_SORT_BY_OLD_USERNAME:
		return "old_username"
	case proto_models.AdminListUsernameChangeHistorySortBy_ADMIN_LIST_USERNAME_CHANGE_HISTORY_SORT_BY_NEW_USERNAME:
		return "new_username"
	default:
		return "created_at"
	}
}

// userAuthMethodDisplayValue renders a stored auth method for the admin API's
// string list: EMAIL keeps its "password" label and every other method is its
// lowercase enum suffix (for example "phone"). Linked OAuth identities are
// listed by provider slug.
func userAuthMethodDisplayValue(provider proto_models.AuthProvider) string {
	if provider == proto_models.AuthProvider_AUTH_PROVIDER_EMAIL {
		return "password"
	}
	return strings.ToLower(strings.TrimPrefix(provider.String(), "AUTH_PROVIDER_"))
}

func preloadUnscoped(db *gorm.DB) *gorm.DB {
	return db.Unscoped()
}

// preloadLiveAttachment restores the soft-delete filter on an attachment
// preload nested under an unscoped parent. GORM copies the parent's Unscoped
// flag onto nested preloads, so "User.ProfilePictureAttachment" after
// Preload("User", preloadUnscoped) (or on an Unscoped outer query) would
// otherwise hydrate a soft-deleted attachment.
func preloadLiveAttachment(db *gorm.DB) *gorm.DB {
	return db.Where(common_persistence_models.TableNameAttachments + ".deleted_at IS NULL")
}

// analyticsBucketExpression returns a UTC-normalized timestamptz bucket. Using
// an explicit timezone keeps analytics dates stable regardless of the database
// session timezone and lets clients format the bucket without shifting it to a
// neighboring calendar day.
func analyticsBucketExpression(groupBy, column string) string {
	return fmt.Sprintf("DATE_TRUNC('%s', %s AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'", groupBy, column)
}

// buildRegistrationAuthMethodStatsQuery builds a single grouped query that
// yields, per registration time bucket, the number of users who have a live
// password (email) auth method and the number who have a linked OAuth identity
// — replacing two COUNT queries per bucket with one query per analytics
// request. The optional from/to bounds are applied to the truncated bucket date
// so partial edge buckets keep their full counts (matching the per-bucket
// semantics this replaces). groupBy must already be validated.
func buildRegistrationAuthMethodStatsQuery(
	db *gorm.DB,
	groupBy string,
	fromTime *time.Time,
	toTime *time.Time,
) *gorm.DB {
	usersTable := common_persistence_models.TableNameUsers
	bucket := analyticsBucketExpression(groupBy, usersTable+".created_at")
	boundBucket := analyticsBucketExpression(groupBy, "?::timestamptz")

	query := db.Model(&common_persistence_models.User{}).
		Select(fmt.Sprintf(
			"%s AS date, "+
				"COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM %s m WHERE m.user_id = %s.id AND m.method_type = ? AND m.enabled AND m.deleted_at IS NULL)) AS email_registrations, "+
				"COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM %s o WHERE o.user_id = %s.id)) AS oauth_registrations",
			bucket,
			common_persistence_models.TableNameUserAuthMethods, usersTable,
			common_persistence_models.TableNameOAuthLogins, usersTable,
		), proto_models.AuthProvider_AUTH_PROVIDER_EMAIL).
		Group("date")

	if fromTime != nil {
		query = query.Where(fmt.Sprintf("%s >= %s", bucket, boundBucket), *fromTime)
	}
	if toTime != nil {
		query = query.Where(fmt.Sprintf("%s <= %s", bucket, boundBucket), *toTime)
	}

	return query
}

func adminContactMessagesSortColumn(sortBy proto_models.AdminListContactMessagesSortBy) string {
	switch sortBy {
	case proto_models.AdminListContactMessagesSortBy_ADMIN_LIST_CONTACT_MESSAGES_SORT_BY_EMAIL:
		return "email"
	default:
		return "created_at"
	}
}

// adminAttachmentsSortColumn executes its operation in the persistence flow between service handlers and the database layer.
func adminAttachmentsSortColumn(sortBy proto_models.AdminListAttachmentsSortBy) string {
	switch sortBy {
	case proto_models.AdminListAttachmentsSortBy_ADMIN_LIST_ATTACHMENTS_SORT_BY_SIZE:
		return "size"
	case proto_models.AdminListAttachmentsSortBy_ADMIN_LIST_ATTACHMENTS_SORT_BY_CONTENT_TYPE:
		return "content_type"
	case proto_models.AdminListAttachmentsSortBy_ADMIN_LIST_ATTACHMENTS_SORT_BY_UPLOAD_STATUS:
		return "upload_status"
	default:
		return "created_at"
	}
}

// adminListTokensSortColumn executes its operation in the persistence flow between service handlers and the database layer.
func adminListTokensSortColumn(sortBy proto_models.AdminListTokensSortBy, tokensTable string) string {
	switch sortBy {
	case proto_models.AdminListTokensSortBy_ADMIN_LIST_TOKENS_SORT_BY_EXPIRES_AT:
		return tokensTable + ".expires_at"
	case proto_models.AdminListTokensSortBy_ADMIN_LIST_TOKENS_SORT_BY_LAST_USED_AT:
		return "COALESCE(" + tokensTable + ".used_at, " + tokensTable + ".issued_at)"
	case proto_models.AdminListTokensSortBy_ADMIN_LIST_TOKENS_SORT_BY_USER_ID:
		return tokensTable + ".user_id"
	case proto_models.AdminListTokensSortBy_ADMIN_LIST_TOKENS_SORT_BY_TOKEN_TYPE:
		return tokensTable + ".token_type"
	default:
		return tokensTable + ".issued_at"
	}
}

// adminChatChannelsSortColumn executes its operation in the persistence flow between service handlers and the database layer.
func adminChatChannelsSortColumn(sortBy proto_models.AdminListChatChannelsSortBy) string {
	switch sortBy {
	case proto_models.AdminListChatChannelsSortBy_ADMIN_LIST_CHAT_CHANNELS_SORT_BY_ID:
		return "id"
	case proto_models.AdminListChatChannelsSortBy_ADMIN_LIST_CHAT_CHANNELS_SORT_BY_NAME:
		return "name"
	case proto_models.AdminListChatChannelsSortBy_ADMIN_LIST_CHAT_CHANNELS_SORT_BY_UPDATED_AT:
		return "updated_at"
	case proto_models.AdminListChatChannelsSortBy_ADMIN_LIST_CHAT_CHANNELS_SORT_BY_LAST_MESSAGE_AT:
		return "last_message_at"
	case proto_models.AdminListChatChannelsSortBy_ADMIN_LIST_CHAT_CHANNELS_SORT_BY_CHANNEL_TYPE:
		return "channel_type"
	case proto_models.AdminListChatChannelsSortBy_ADMIN_LIST_CHAT_CHANNELS_SORT_BY_CREATED_BY_USER_ID:
		return "created_by_user_id"
	default:
		return "created_at"
	}
}

// AdminListUsers executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminListUsers(ctx context.Context, req *proto_models.AdminListUsersRequest) ([]*common_service_models.AdminUserDetails, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)
	usersTable := common_persistence_models.TableNameUsers
	verificationTable := common_persistence_models.TableNameEmailVerifications

	// Extract parameters from request
	pagination := req.GetPagination()
	if err := requirePageLimit("admin_list_users", pagination); err != nil {
		return nil, nil, err
	}
	var search string
	var userTypeFilter proto_models.UserType
	var bannedFilter *bool

	search = req.Search
	userTypeFilter = req.UserTypeFilter

	if req.BannedFilter != nil {
		bannedFilter = req.BannedFilter
	}

	// Build one filtered query and reuse it for both count and page data so
	// pagination metadata always describes the visible result set.
	query := db.Model(&common_persistence_models.User{})

	// Apply search filter
	if search != "" {
		searchPattern := "%" + search + "%"
		clauses := []string{
			usersTable + ".email ILIKE ?",
			usersTable + ".username ILIKE ?",
			usersTable + ".first_name ILIKE ?",
			usersTable + ".last_name ILIKE ?",
		}
		args := []any{searchPattern, searchPattern, searchPattern, searchPattern}
		if userID, err := strconv.ParseUint(search, 10, 64); err == nil {
			clauses = append(clauses, usersTable+".id = ?")
			args = append(args, userID)
		}
		query = query.Where("("+strings.Join(clauses, " OR ")+")", args...)
	}

	// Apply user type filter
	if userTypeFilter != proto_models.UserType_USER_TYPE_UNSPECIFIED {
		query = query.Where(usersTable+".user_type = ?", userTypeFilter)
	}

	// Apply banned filter
	if bannedFilter != nil {
		query = query.Where(usersTable+".banned = ?", *bannedFilter)
	}

	if req.IsBotFilter != nil {
		query = query.Where(usersTable+".is_bot = ?", *req.IsBotFilter)
	}

	if req.EmailVerifiedFilter != nil {
		query = query.Joins(fmt.Sprintf(
			"LEFT JOIN %s AS admin_email_verification ON admin_email_verification.user_id = %s.id",
			verificationTable,
			usersTable,
		))
		verifiedExpression := "(admin_email_verification.user_id IS NULL OR (admin_email_verification.verified_at IS NOT NULL AND admin_email_verification.email = " + usersTable + ".email))"
		if *req.EmailVerifiedFilter {
			query = query.Where(verifiedExpression)
		} else {
			query = query.Where("NOT " + verifiedExpression)
		}
	}

	if req.Filter != nil {
		if req.Filter.FromDate != nil {
			query = query.Where(usersTable+".created_at >= ?", req.Filter.FromDate.AsTime())
		}
		if req.Filter.ToDate != nil {
			query = query.Where(usersTable+".created_at <= ?", req.Filter.ToDate.AsTime())
		}
	}

	// Keep NULL login timestamps at the end in either direction and always add
	// an ID tie-breaker so offset pagination is stable when values are equal.
	sortDesc := common_util.SortDesc(req.Sort, true)
	sortDirection := "ASC"
	if sortDesc {
		sortDirection = "DESC"
	}
	sortColumn := usersTable + "." + adminListUsersSortColumn(req.GetSortBy())
	orderClause := common_util.BuildOrderClause(req.Sort, sortColumn, true)
	if req.GetSortBy() == proto_models.AdminListUsersSortBy_ADMIN_LIST_USERS_SORT_BY_LAST_LOGIN {
		orderClause += " NULLS LAST"
	}
	query = query.Order(orderClause).Order(usersTable + ".id " + sortDirection)

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "admin_list_users"),
				zap.String(logkeys.Stage, "count"))
		}
	}

	// Apply pagination
	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	query = query.Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount))

	// Execute query with ProfilePictureAttachment preload
	var users []common_persistence_models.User
	if err := query.Preload(common_persistence_models.PreloadProfilePictureAttachment).Find(&users).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_users"),
			zap.String(logkeys.Stage, "list"),
			zap.Uint32(appLogger.KeyPage, pagination.Page),
			zap.Uint32(logkeys.Limit, pagination.Limit))
	}
	users, hasNextPage := common_util.TrimPaginationProbe(users, pagination.Limit, includeTotalCount)

	// Convert to service domain models; statistics are loaded for the whole
	// page in a constant number of grouped queries rather than per user.
	userIDs := make([]uint64, len(users))
	userEmails := make(map[uint64]string, len(users))
	for i, user := range users {
		userIDs[i] = user.ID
		userEmails[user.ID] = user.Email
	}
	statsByUser := h.loadAdminUserStats(ctx, userIDs, userEmails)
	adminUsers := make([]*common_service_models.AdminUserDetails, len(users))
	for i, user := range users {
		adminUsers[i] = h.convertToAdminUserDetails(user, statsByUser[user.ID])
	}

	// Build pagination response
	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage)
	}

	return adminUsers, paginationResponse, nil
}

// AdminGetUser executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminGetUser(ctx context.Context, userID uint64) (*common_service_models.AdminUserDetails, error) {
	db := h.getDBFromContext(ctx)

	// Get the user with ProfilePictureAttachment preloaded
	var user common_persistence_models.User
	if err := db.Preload(common_persistence_models.PreloadProfilePictureAttachment).Where("id = ?", userID).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_user"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	// Convert to service domain model
	stats := h.loadAdminUserStats(ctx, []uint64{user.ID}, map[uint64]string{user.ID: user.Email})
	adminUser := h.convertToAdminUserDetails(user, stats[user.ID])
	return adminUser, nil
}

// AdminListUsernameChangeHistory executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminListUsernameChangeHistory(ctx context.Context, req *proto_models.AdminListUsernameChangeHistoryRequest) ([]*common_persistence_models.UsernameChangeHistory, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)

	pagination := req.GetPagination()
	if err := requirePageLimit("admin_list_username_change_history", pagination); err != nil {
		return nil, nil, err
	}

	query := db.Model(&common_persistence_models.UsernameChangeHistory{})

	if req.TargetUserIdFilter != nil {
		query = query.Where("target_user_id = ?", *req.TargetUserIdFilter)
	}

	if req.ActorUserIdFilter != nil {
		query = query.Where("actor_user_id = ?", *req.ActorUserIdFilter)
	}

	if usernameFilter := strings.TrimSpace(req.UsernameFilter); usernameFilter != "" {
		pattern := "%" + usernameFilter + "%"
		query = query.Where("old_username ILIKE ? OR new_username ILIKE ?", pattern, pattern)
	}

	if req.FromTime != nil {
		query = query.Where("created_at >= ?", req.FromTime.AsTime())
	}

	if req.ToTime != nil {
		query = query.Where("created_at <= ?", req.ToTime.AsTime())
	}

	orderClause := common_util.BuildOrderClause(req.Sort, adminUsernameChangeHistorySortColumn(req.GetSortBy()), true)
	query = query.Order(orderClause)

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "admin_list_username_change_history"),
				zap.String(logkeys.Stage, "count"))
		}
	}

	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	var history []common_persistence_models.UsernameChangeHistory
	if err := query.
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount)).
		Preload("TargetUser", preloadUnscoped).
		Preload("TargetUser.ProfilePictureAttachment", preloadLiveAttachment).
		Preload("ActorUser", preloadUnscoped).
		Preload("ActorUser.ProfilePictureAttachment", preloadLiveAttachment).
		Find(&history).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_username_change_history"),
			zap.String(logkeys.Stage, "list"),
			zap.Uint32(appLogger.KeyPage, pagination.Page),
			zap.Uint32(logkeys.Limit, pagination.Limit))
	}
	history, hasNextPage := common_util.TrimPaginationProbe(history, pagination.Limit, includeTotalCount)

	entries := make([]*common_persistence_models.UsernameChangeHistory, len(history))
	for i := range history {
		entries[i] = &history[i]
	}

	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage)
	}
	return entries, paginationResponse, nil
}

// AdminUpdateUserBanStatus executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminUpdateUserBanStatus(ctx context.Context, userID uint64, banned bool, reason string) error {
	db := h.getDBFromContext(ctx)

	// Update user banned status
	result := db.Model(&common_persistence_models.User{}).
		Where("id = ?", userID).
		Update("banned", banned)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "admin_update_user_ban_status"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Bool(appLogger.KeyBannedStatus, banned))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("User not found for ban-status update",
				zap.String(logkeys.Operation, "admin_update_user_ban_status"),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return ErrUserNotFound
	}

	// CRITICAL: Invalidate user cache so the ban status takes effect (deferred
	// to post-commit when the context carries the request transaction, so a
	// concurrent cache miss cannot re-cache the pre-commit row)
	h.invalidateUserCache(ctx, userID)

	// Session revocation is owned by the admin service layer (AdminHandler.BanUser),
	// which can also invalidate the auth-layer token caches; doing it here would
	// leave revoked tokens usable until the in-memory token cache expires.

	// TODO: Add audit logging here
	appLogger.For(ctx, h.logger, loggerName).Info("user ban status updated",
		zap.String(logkeys.Operation, "admin_update_user_ban_status"),
		zap.String(appLogger.KeyOutcome, "updated"),
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Bool(appLogger.KeyBannedStatus, banned),
		zap.String(logkeys.Reason, reason))

	return nil
}

// AdminUpdateUserType executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminUpdateUserType(ctx context.Context, userID uint64, newUserType proto_models.UserType, reason string) error {
	db := h.getDBFromContext(ctx)

	// Update user type
	result := db.Model(&common_persistence_models.User{}).
		Where("id = ?", userID).
		Update("user_type", newUserType)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "admin_update_user_type"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyUserType, newUserType))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("User not found for type update",
				zap.String(logkeys.Operation, "admin_update_user_type"),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return ErrUserNotFound
	}

	// CRITICAL: Invalidate user cache so the new user type takes effect
	// (deferred to post-commit when the context carries the request transaction)
	h.invalidateUserCache(ctx, userID)

	// TODO: Add audit logging here
	appLogger.For(ctx, h.logger, loggerName).Info("user type updated",
		zap.String(logkeys.Operation, "admin_update_user_type"),
		zap.String(appLogger.KeyOutcome, "updated"),
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Stringer(appLogger.KeyUserType, newUserType),
		zap.String(logkeys.Reason, reason))

	return nil
}

// AdminDeleteUser executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminDeleteUser(ctx context.Context, userID uint64, deleteAttachments bool, reason string) error {
	db := h.getDBFromContext(ctx)

	tx := db
	ownTx := false
	if _, hasTx := scontext.GetTransaction(ctx); !hasTx {
		tx = db.Begin()
		if tx.Error != nil {
			return userDeletionDatabaseError("admin_delete_user", "begin_transaction", "", userID, tx.Error)
		}
		ownTx = true
		defer tx.Rollback()
	}

	if err := lockUserForDeletion(tx, "admin_delete_user", userID); err != nil {
		return err
	}

	if err := h.deleteUserData(ctx, tx, "admin_delete_user", userID, deleteAttachments); err != nil {
		return err
	}

	if ownTx {
		if err := tx.Commit().Error; err != nil {
			return userDeletionDatabaseError("admin_delete_user", "commit_transaction", "", userID, err)
		}
	}

	// CRITICAL: Invalidate user cache so the deleted user cannot authenticate
	// (deferred to post-commit when the context carries the request transaction)
	h.invalidateUserCache(ctx, userID)

	// TODO: Add audit logging here
	appLogger.For(ctx, h.logger, loggerName).Info("user soft-deleted and identity fields tombstoned",
		zap.String(logkeys.Operation, "admin_delete_user"),
		zap.String(appLogger.KeyOutcome, "soft_deleted"),
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Bool(appLogger.KeyDeleteAttachments, deleteAttachments),
		zap.String(logkeys.Reason, reason))

	return nil
}

// DeleteUserIfUnverified soft-deletes and tombstones a user only if their email
// is still unverified, returning whether the deletion happened.
//
// The conditional delete on the email verification record is the atomicity
// guard: it blocks behind an in-flight verification commit and matches zero
// rows if the user verified first, in which case the account is left intact
// and (false, nil) is returned. When the caller's context carries a
// transaction (the unverified-user cleanup provides one so application
// callbacks share it), the guard and cascade join it and the caller owns
// commit/rollback; otherwise a local transaction is used.
//
// Returns:
//   - result1: bool reporting whether the user was deleted.
//   - result2: error value returned by DeleteUserIfUnverified.
func (h *CommonPersistenceHandler[U]) DeleteUserIfUnverified(ctx context.Context, userID uint64, deleteAttachments bool, reason string) (bool, error) {
	db := h.getDBFromContext(ctx)

	tx := db
	ownTx := false
	if _, hasTx := scontext.GetTransaction(ctx); !hasTx {
		tx = db.Begin()
		if tx.Error != nil {
			return false, userDeletionDatabaseError("delete_unverified_user", "begin_transaction", "", userID, tx.Error)
		}
		ownTx = true
		defer tx.Rollback()
	}

	// Lock before the verification guard as credential setup locks the user
	// before updating verification and token rows.
	if err := lockUserForDeletion(tx, "delete_unverified_user", userID); err != nil {
		return false, err
	}

	result := tx.Where("user_id = ? AND verified_at IS NULL", userID).
		Delete(&common_persistence_models.EmailVerification{})
	if result.Error != nil {
		return false, userDeletionDatabaseError("delete_unverified_user", "claim_unverified_email", common_persistence_models.TableNameEmailVerifications, userID, result.Error)
	}
	if result.RowsAffected == 0 {
		// The user verified (or was already deleted) since being selected as a
		// cleanup candidate; leave the account untouched.
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Unverified-user cleanup candidate no longer eligible",
				zap.String(logkeys.Operation, "delete_unverified_user"),
				zap.String(appLogger.KeyOutcome, "not_eligible"),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return false, nil
	}

	if err := h.deleteUserData(ctx, tx, "delete_unverified_user", userID, deleteAttachments); err != nil {
		return false, err
	}

	if ownTx {
		if err := tx.Commit().Error; err != nil {
			return false, userDeletionDatabaseError("delete_unverified_user", "commit_transaction", "", userID, err)
		}
	}

	// CRITICAL: Invalidate user cache so the deleted user cannot authenticate
	// (deferred to post-commit when the context carries the caller's transaction)
	h.invalidateUserCache(ctx, userID)

	appLogger.For(ctx, h.logger, loggerName).Info("unverified user soft-deleted and identity fields tombstoned",
		zap.String(logkeys.Operation, "delete_unverified_user"),
		zap.String(appLogger.KeyOutcome, "soft_deleted"),
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Bool(appLogger.KeyDeleteAttachments, deleteAttachments),
		zap.String(logkeys.Reason, reason))

	return true, nil
}

// lockUserForDeletion establishes the same user-before-children lock order as
// invitation acceptance/resend and email updates. Find preserves deletion's
// existing idempotent behavior for a missing or already-deleted user.
func lockUserForDeletion(tx *gorm.DB, operation string, userID uint64) error {
	var user common_persistence_models.User
	if err := tx.Select("id").Where("id = ?", userID).
		Clauses(clause.Locking{Strength: "UPDATE"}).Find(&user).Error; err != nil {
		return userDeletionDatabaseError(operation, "lock_user", common_persistence_models.TableNameUsers, userID, err)
	}
	return nil
}

// deleteUserData tombstones and soft-deletes a user row after deleting related
// records within the supplied transaction. Callers own the transaction lifecycle
// and must lock the user before mutating any related rows.
//
// Auth/security tokens are revoked (revoked_at set) rather than hard-deleted:
// token validation already rejects revoked and expired tokens, and the tombstoned
// user can no longer be looked up, so a retained row cannot authenticate anyone.
// Keeping the row means a late click on a verification link resolves to a
// classified "revoked" token — logged with its id and shown a friendly "expired"
// message — instead of a bare "token not found".
func (h *CommonPersistenceHandler[U]) deleteUserData(ctx context.Context, tx *gorm.DB, operation string, userID uint64, deleteAttachments bool) error {
	// If deleting attachments, delete them first
	if deleteAttachments {
		// Internal rows are protocol state owned by transcript lifecycle, not
		// ordinary user uploads. They remain until reference-aware reclamation.
		// Stored objects are kept: see TODO(object-refcounting) in
		// attachment/lifecycle_manager.go.
		if err := tx.Where("user_id = ? AND internal_only = ?", userID, false).
			Delete(&common_persistence_models.Attachment{}).Error; err != nil {
			return userDeletionDatabaseError(operation, "delete_attachments", common_persistence_models.TableNameAttachments, userID, err)
		}
	}

	if err := h.deleteUserRows(ctx, tx, operation, "delete_user_settings", common_persistence_models.TableNameUserSettings, userID, &common_persistence_models.UserSettings{}); err != nil {
		return err
	}
	if err := h.revokeUserSessionTokens(ctx, tx, operation, userID); err != nil {
		return err
	}
	if err := h.deleteUserRows(ctx, tx, operation, "delete_email_verifications", common_persistence_models.TableNameEmailVerifications, userID, &common_persistence_models.EmailVerification{}); err != nil {
		return err
	}
	if err := h.deleteUserRows(ctx, tx, operation, "delete_phone_verifications", common_persistence_models.TableNamePhoneVerifications, userID, &common_persistence_models.PhoneVerification{}); err != nil {
		return err
	}
	if err := h.deleteUserRows(ctx, tx, operation, "delete_oauth_logins", common_persistence_models.TableNameOAuthLogins, userID, &common_persistence_models.OAuthLogin{}); err != nil {
		return err
	}
	if err := h.deleteUserRows(ctx, tx, operation, "delete_user_passwords", common_persistence_models.TableNameUserPasswords, userID, &common_persistence_models.UserPassword{}); err != nil {
		return err
	}
	if err := h.deleteUserRows(ctx, tx, operation, "delete_user_auth_methods", common_persistence_models.TableNameUserAuthMethods, userID, &common_persistence_models.UserAuthMethod{}); err != nil {
		return err
	}

	if err := h.tombstoneUserIdentityFields(ctx, tx, operation, userID); err != nil {
		return err
	}

	// Finally, soft-delete the user.
	if err := tx.Where("id = ?", userID).Delete(&common_persistence_models.User{}).Error; err != nil {
		return userDeletionDatabaseError(operation, "delete_user", common_persistence_models.TableNameUsers, userID, err)
	}

	return nil
}

func (h *CommonPersistenceHandler[U]) tombstoneUserIdentityFields(ctx context.Context, tx *gorm.DB, operation string, userID uint64) error {
	tx = tx.WithContext(ctx)
	result := tx.Model(&common_persistence_models.User{}).
		Where(`
id = ?
	AND deleted_at IS NULL
	AND (
		email NOT LIKE ('deleted:' || id::text || ':%')
		OR username NOT LIKE ('deleted:' || id::text || ':%')
	)`, userID).
		Select("email", "username", "updated_at").
		UpdateColumns(struct {
			Email     clause.Expr `gorm:"type:varchar(255)"`
			Username  clause.Expr `gorm:"type:varchar(255)"`
			UpdatedAt clause.Expr `gorm:"type:timestamptz;autoUpdateTime:false"`
		}{
			Email: gorm.Expr(`
CASE
	WHEN email LIKE ('deleted:' || id::text || ':%') THEN email
	ELSE LEFT('deleted:' || id::text || ':' || email, 255)
END`),
			Username: gorm.Expr(`
CASE
	WHEN username LIKE ('deleted:' || id::text || ':%') THEN username
	ELSE LEFT('deleted:' || id::text || ':' || username, 255)
END`),
			UpdatedAt: gorm.Expr("NOW()"),
		})
	if result.Error != nil {
		return userDeletionDatabaseError(operation, "tombstone_user_identity", common_persistence_models.TableNameUsers, userID, result.Error)
	}
	return nil
}

func (h *CommonPersistenceHandler[U]) deleteUserRows(ctx context.Context, tx *gorm.DB, operation, step, table string, userID uint64, model any) error {
	tx = tx.WithContext(ctx)
	if err := tx.Where("user_id = ?", userID).Delete(model).Error; err != nil {
		return userDeletionDatabaseError(operation, step, table, userID, err)
	}
	return nil
}

// revokeUserSessionTokens soft-deletes a user's session/security tokens by
// setting revoked_at on every row that is not already effectively revoked,
// instead of removing them. See deleteUserData for why deletion retains tokens
// this way.
//
// Force semantics: a pending (future-dated) refresh revocation is pulled forward
// to now, so a user deleted mid refresh-grace-window cannot keep authenticating
// with the old token until its scheduled revoke time. Since GetValidSessionToken
// treats revoked_at > now as still valid, matching only revoked_at IS NULL here
// would leave those grace-window tokens alive.
func (h *CommonPersistenceHandler[U]) revokeUserSessionTokens(ctx context.Context, tx *gorm.DB, operation string, userID uint64) error {
	tx = tx.WithContext(ctx)
	now := time.Now().UTC()
	if err := tx.Model(&common_persistence_models.SessionToken{}).
		Where("user_id = ? AND (revoked_at IS NULL OR revoked_at > ?)", userID, now).
		Update("revoked_at", now).Error; err != nil {
		return userDeletionDatabaseError(operation, "revoke_auth_tokens", common_persistence_models.TableNameSessionTokens, userID, err)
	}
	return nil
}

func userDeletionDatabaseError(operation, step, table string, userID uint64, err error) error {
	fields := []zap.Field{
		zap.String(logkeys.Operation, operation),
		zap.String(logkeys.Stage, step),
		zap.Uint64(appLogger.KeyUserID, userID),
	}
	if table != "" {
		fields = append(fields, zap.String(appLogger.KeyTable, table))
	}
	return databaseErrorWithFields(err, fields...)
}

// adminUserStats holds the per-user statistics shown in admin user listings.
type adminUserStats struct {
	TotalLogins       int64
	ActiveSessions    int64
	AuthMethods       []string
	TotalAttachments  int64
	StorageUsedBytes  int64
	EmailVerified     bool
	InvitationPending bool
}

// loadAdminUserStats gathers statistics for a set of users with one grouped
// query per statistics source (session tokens, auth methods, attachments,
// email verification) instead of several queries per user, keeping admin list
// pages at a constant query count regardless of page size. Failures are logged
// and leave zero-value stats, matching the previous lenient per-user lookups.
// Parameters:
//   - ctx: context.Context input used by loadAdminUserStats.
//   - userIDs: IDs of the users whose statistics are loaded.
//   - userEmails: current email address for each user, used to ignore stale verification records.
//
// Returns:
//   - result1: map of user ID to that user's statistics (an entry exists for every requested ID).
func (h *CommonPersistenceHandler[U]) loadAdminUserStats(ctx context.Context, userIDs []uint64, userEmails map[uint64]string) map[uint64]*adminUserStats {
	stats := make(map[uint64]*adminUserStats, len(userIDs))
	for _, id := range userIDs {
		stats[id] = &adminUserStats{EmailVerified: true} // Default assumption when no verification row exists.
	}
	if len(userIDs) == 0 {
		return stats
	}
	db := h.getDBFromContext(ctx)

	// Session-token stats remain grouped by page. Last login is stored directly
	// on the user so it survives token cleanup and does not confuse token use or
	// sliding refresh with a successful sign-in.
	var tokenRows []struct {
		UserID         uint64
		TotalLogins    int64
		ActiveSessions int64
	}
	nowUTC := time.Now().UTC()
	if err := db.Model(&common_persistence_models.SessionToken{}).
		Select(
			"user_id, COUNT(*) AS total_logins, "+
				"COUNT(*) FILTER (WHERE expires_at > ? AND (revoked_at IS NULL OR revoked_at > ?)) AS active_sessions",
			nowUTC, nowUTC,
		).
		Where("user_id IN ? AND token_type = ?", userIDs, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION).
		Group("user_id").
		Scan(&tokenRows).Error; err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("failed to load admin user session stats",
			zap.String(logkeys.Operation, "load_admin_user_stats"),
			zap.String(logkeys.Stage, "sessions"),
			zap.Int(appLogger.KeyCount, len(userIDs)),
			zap.String(logkeys.Fallback, "return_users_without_session_stats"),
			zap.Error(err))
	}
	for _, row := range tokenRows {
		if s, ok := stats[row.UserID]; ok {
			s.TotalLogins = row.TotalLogins
			s.ActiveSessions = row.ActiveSessions
		}
	}

	var methodRows []common_persistence_models.UserAuthMethod
	if err := db.Where("user_id IN ? AND enabled = ?", userIDs, true).Find(&methodRows).Error; err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("failed to load admin user auth methods",
			zap.String(logkeys.Operation, "load_admin_user_stats"),
			zap.String(logkeys.Stage, "auth_methods"),
			zap.Int(appLogger.KeyCount, len(userIDs)),
			zap.String(logkeys.Fallback, "return_users_without_auth_method_stats"),
			zap.Error(err))
	}
	for _, row := range methodRows {
		if s, ok := stats[row.UserID]; ok {
			s.AuthMethods = append(s.AuthMethods, userAuthMethodDisplayValue(row.MethodType))
		}
	}

	var identityRows []common_persistence_models.OAuthLogin
	if err := db.Select("user_id", "provider_slug").Where("user_id IN ?", userIDs).
		Order("provider_slug").Find(&identityRows).Error; err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("failed to load admin user OAuth identities",
			zap.String(logkeys.Operation, "load_admin_user_stats"),
			zap.String(logkeys.Stage, "oauth_identities"),
			zap.Int(appLogger.KeyCount, len(userIDs)),
			zap.String(logkeys.Fallback, "return_users_without_oauth_identity_stats"),
			zap.Error(err))
	}
	for _, row := range identityRows {
		if s, ok := stats[row.UserID]; ok {
			s.AuthMethods = append(s.AuthMethods, row.ProviderSlug)
		}
	}

	// Attachment count and storage usage in one grouped aggregate.
	var attachmentRows []struct {
		UserID           uint64
		TotalAttachments int64
		StorageUsedBytes int64
	}
	if err := db.Model(&common_persistence_models.Attachment{}).
		Select("user_id, COUNT(*) AS total_attachments, COALESCE(SUM(size), 0) AS storage_used_bytes").
		Where("user_id IN ? AND upload_status = ? AND internal_only = ?", userIDs, common_persistence_models.UploadStatusVetted, false).
		Group("user_id").
		Scan(&attachmentRows).Error; err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("failed to load admin user attachment stats",
			zap.String(logkeys.Operation, "load_admin_user_stats"),
			zap.String(logkeys.Stage, "attachments"),
			zap.Int(appLogger.KeyCount, len(userIDs)),
			zap.String(logkeys.Fallback, "return_users_without_attachment_stats"),
			zap.Error(err))
	}
	for _, row := range attachmentRows {
		if s, ok := stats[row.UserID]; ok {
			s.TotalAttachments = row.TotalAttachments
			s.StorageUsedBytes = row.StorageUsedBytes
		}
	}

	var verificationRows []common_persistence_models.EmailVerification
	if err := db.Where("user_id IN ?", userIDs).Find(&verificationRows).Error; err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("failed to load admin user email verifications",
			zap.String(logkeys.Operation, "load_admin_user_stats"),
			zap.String(logkeys.Stage, "email_verification"),
			zap.Int(appLogger.KeyCount, len(userIDs)),
			zap.String(logkeys.Fallback, "retain_verified_default"),
			zap.Error(err))
	}
	for _, row := range verificationRows {
		if s, ok := stats[row.UserID]; ok {
			s.EmailVerified = row.VerifiedAt != nil && row.Email == userEmails[row.UserID]
		}
	}

	// Keep expired invitations visible for resend; password setup and email changes clear eligibility.
	var pendingIDs []uint64
	tokens := common_persistence_models.TableNameSessionTokens
	users := common_persistence_models.TableNameUsers
	passwords := common_persistence_models.TableNameUserPasswords
	if err := db.Table(tokens+" AS invitations").Distinct("invitations.user_id").
		Joins("JOIN "+users+" AS invited ON invited.id = invitations.user_id AND invited.email = invitations.bound_email").
		Where("invitations.user_id IN ? AND invitations.token_type = ? AND invitations.used_at IS NULL AND invitations.revoked_at IS NULL AND invited.is_bot = false AND invited.banned = false", userIDs, proto_models.TokenType_TOKEN_TYPE_ACCOUNT_INVITATION).
		Where("NOT EXISTS (SELECT 1 FROM "+passwords+" WHERE user_id = invited.id AND deleted_at IS NULL)").Pluck("invitations.user_id", &pendingIDs).Error; err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("failed to load pending account invitations", zap.Error(err))
	}
	for _, id := range pendingIDs {
		if s := stats[id]; s != nil {
			s.InvitationPending = true
		}
	}
	return stats
}

// convertToAdminUserDetails executes its operation in the persistence flow between service handlers and the database layer.
// Parameters:
//   - user: common_persistence_models.User input used by convertToAdminUserDetails.
//   - stats: pre-loaded statistics for the user (see loadAdminUserStats); nil yields zero-value stats.
func (h *CommonPersistenceHandler[U]) convertToAdminUserDetails(user common_persistence_models.User, stats *adminUserStats) *common_service_models.AdminUserDetails {
	// Convert basic user info to service model (maps Bot, PhoneNumber, and avatar fallback)
	serviceUser := h.convertPersistenceUserToService(user)

	if stats == nil {
		stats = &adminUserStats{EmailVerified: true}
	}
	lastLogin := time.Time{}
	if user.LastLoginAt != nil {
		lastLogin = user.LastLoginAt.UTC()
	}

	return &common_service_models.AdminUserDetails{
		User:              serviceUser,
		CreatedAt:         user.CreatedAt,
		UpdatedAt:         user.UpdatedAt,
		LastLogin:         lastLogin,
		TotalLogins:       stats.TotalLogins,
		ActiveSessions:    stats.ActiveSessions,
		AuthMethods:       stats.AuthMethods,
		TotalAttachments:  uint64(stats.TotalAttachments),
		StorageUsedBytes:  uint64(stats.StorageUsedBytes),
		EmailVerified:     stats.EmailVerified,
		InvitationPending: stats.InvitationPending,
	}
}

// convertTimeToProtoTimestamp executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) convertTimeToProtoTimestamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

// AdminListContactMessages executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminListContactMessages(ctx context.Context, pagination *proto_models.PaginationRequest, statusFilter, emailFilter string, sort *proto_models.SortRequest, sortBy proto_models.AdminListContactMessagesSortBy) ([]*proto_models.AdminContactMessage, *proto_models.PaginationResponse, error) {
	if err := requirePageLimit("admin_list_contact_messages", pagination); err != nil {
		return nil, nil, err
	}
	db := h.getDBFromContext(ctx)

	// Build the query with filters
	query := db.Model(&common_persistence_models.ContactMessage{})

	// Apply email filter
	if emailFilter != "" {
		query = query.Where("email ILIKE ?", "%"+emailFilter+"%")
	}

	// Apply status filter (values are whitelisted by the sanitizer)
	if statusFilter != "" {
		query = query.Where("status = ?", statusFilter)
	}

	// Apply sorting
	orderClause := common_util.BuildOrderClause(sort, adminContactMessagesSortColumn(sortBy), true)
	query = query.Order(orderClause)

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "admin_list_contact_messages"),
				zap.String(logkeys.Stage, "count"))
		}
	}

	// Apply pagination
	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	query = query.Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount))

	// Execute query
	var messages []common_persistence_models.ContactMessage
	if err := query.Find(&messages).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_contact_messages"),
			zap.String(logkeys.Stage, "list"),
			zap.Uint32(appLogger.KeyPage, pagination.Page),
			zap.Uint32(logkeys.Limit, pagination.Limit))
	}
	messages, hasNextPage := common_util.TrimPaginationProbe(messages, pagination.Limit, includeTotalCount)

	// Convert to admin contact messages
	adminMessages := make([]*proto_models.AdminContactMessage, len(messages))
	for i, msg := range messages {
		adminMessages[i] = h.convertToAdminContactMessage(msg)
	}

	// Build pagination response
	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage)
	}

	return adminMessages, paginationResponse, nil
}

// AdminGetContactMessageDetails executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminGetContactMessageDetails(ctx context.Context, messageID uint64) (*proto_models.AdminContactMessage, error) {
	db := h.getDBFromContext(ctx)

	var message common_persistence_models.ContactMessage
	if err := db.Where("id = ?", messageID).First(&message).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrContactMessageNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_contact_message"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}

	return h.convertToAdminContactMessage(message), nil
}

// AdminGetContactMessageByReferenceID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminGetContactMessageByReferenceID(ctx context.Context, referenceID string) (*proto_models.AdminContactMessage, error) {
	db := h.getDBFromContext(ctx)

	var message common_persistence_models.ContactMessage
	err := db.Where("reference_id = ?", referenceID).First(&message).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, ErrContactMessageNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_contact_message_by_reference_id"),
			zap.String(appLogger.KeyReferenceID, referenceID))
	}

	return h.convertToAdminContactMessage(message), nil
}

// AdminUpdateContactMessageStatus executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminUpdateContactMessageStatus(ctx context.Context, messageID uint64, status, adminNotes string) error {
	db := h.getDBFromContext(ctx)

	// Update the contact message with new status and admin notes
	result := db.Model(&common_persistence_models.ContactMessage{}).
		Where("id = ?", messageID).
		Select("status", "admin_notes", "updated_at").
		Updates(struct {
			Status     string
			AdminNotes string
			UpdatedAt  time.Time `gorm:"autoUpdateTime:false"`
		}{Status: status, AdminNotes: adminNotes, UpdatedAt: time.Now().UTC()})

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "admin_update_contact_message_status"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Status, status))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Contact message not found for status update",
				zap.String(logkeys.Operation, "admin_update_contact_message_status"),
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
		return ErrContactMessageNotFound
	}

	appLogger.For(ctx, h.logger, loggerName).Info("Contact message status updated",
		zap.String(logkeys.Operation, "admin_update_contact_message_status"),
		zap.String(appLogger.KeyOutcome, "updated"),
		zap.Uint64(appLogger.KeyMessageID, messageID),
		zap.String(logkeys.Status, status),
		zap.String(appLogger.KeyAdminNotes, adminNotes))

	return nil
}

// AdminDeleteContactMessage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminDeleteContactMessage(ctx context.Context, messageID uint64, reason string) error {
	db := h.getDBFromContext(ctx)

	result := db.Where("id = ?", messageID).Delete(&common_persistence_models.ContactMessage{})
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "admin_delete_contact_message"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Contact message not found for delete",
				zap.String(logkeys.Operation, "admin_delete_contact_message"),
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
		return ErrContactMessageNotFound
	}

	appLogger.For(ctx, h.logger, loggerName).Info("contact message deleted",
		zap.String(logkeys.Operation, "admin_delete_contact_message"),
		zap.String(appLogger.KeyOutcome, "deleted"),
		zap.Uint64(appLogger.KeyMessageID, messageID),
		zap.String(logkeys.Reason, reason))

	return nil
}

// AdminListAttachments executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminListAttachments(ctx context.Context, pagination *proto_models.PaginationRequest, userIDFilter *uint64, statusFilter, contentTypeFilter string, minSizeBytes, maxSizeBytes uint64, sort *proto_models.SortRequest, sortBy proto_models.AdminListAttachmentsSortBy) ([]*common_persistence_models.Attachment, *proto_models.PaginationResponse, error) {
	if err := requirePageLimit("admin_list_attachments", pagination); err != nil {
		return nil, nil, err
	}
	db := h.getDBFromContext(ctx)

	// Build the query with filters
	query := db.Model(&common_persistence_models.Attachment{}).Where("internal_only = ?", false)

	// Apply user ID filter
	if userIDFilter != nil {
		query = query.Where("user_id = ?", *userIDFilter)
	}

	// Apply status filter
	if statusFilter != "" {
		if status, err := common_persistence_models.ParseUploadStatus(statusFilter); err != nil {
			// Invalid status filter, skip filtering by status
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("Invalid attachment status filter ignored",
					zap.String(logkeys.Operation, "admin_list_attachments"),
					zap.String(appLogger.KeyStatusFilter, statusFilter))
			}
		} else {
			query = query.Where("upload_status = ?", status)
		}
	}

	// Apply content type filter
	if contentTypeFilter != "" {
		query = query.Where("content_type ILIKE ?", "%"+contentTypeFilter+"%")
	}

	// Apply size filters
	if minSizeBytes > 0 {
		query = query.Where("size >= ?", minSizeBytes)
	}
	if maxSizeBytes > 0 {
		query = query.Where("size <= ?", maxSizeBytes)
	}

	// Apply sorting
	orderClause := common_util.BuildOrderClause(sort, adminAttachmentsSortColumn(sortBy), true)
	query = query.Order(orderClause)

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "admin_list_attachments"),
				zap.String(logkeys.Stage, "count"))
		}
	}

	// Apply pagination
	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	query = query.Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount))

	// Execute query with owner preload for admin display.
	var attachments []common_persistence_models.Attachment
	if err := query.
		Preload(common_persistence_models.PreloadUser, preloadUnscoped).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment, preloadLiveAttachment).
		Find(&attachments).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_attachments"),
			zap.String(logkeys.Stage, "list"),
			zap.Uint32(appLogger.KeyPage, pagination.Page),
			zap.Uint32(logkeys.Limit, pagination.Limit))
	}
	attachments, hasNextPage := common_util.TrimPaginationProbe(attachments, pagination.Limit, includeTotalCount)

	out := make([]*common_persistence_models.Attachment, 0, len(attachments))
	for i := range attachments {
		out = append(out, &attachments[i])
	}

	// Build pagination response
	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage)
	}

	return out, paginationResponse, nil
}

// AdminGetAttachmentDetails executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminGetAttachmentDetails(ctx context.Context, attachmentID uint64) (*common_persistence_models.Attachment, error) {
	db := h.getDBFromContext(ctx)

	var attachment common_persistence_models.Attachment
	if err := db.
		Preload(common_persistence_models.PreloadUser, preloadUnscoped).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment, preloadLiveAttachment).
		Where("id = ? AND internal_only = ?", attachmentID, false).
		First(&attachment).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrAttachmentNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}

	return &attachment, nil
}

// AdminUpdateAttachmentStatus executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminUpdateAttachmentStatus(ctx context.Context, attachmentID uint64, status, validationDetails string) error {
	db := h.getDBFromContext(ctx)

	uploadStatus, err := common_persistence_models.ParseUploadStatus(status)
	if err != nil {
		return ErrInvalidAttachmentStatus
	}

	// Update attachment status and validation details
	result := db.Model(&common_persistence_models.Attachment{}).
		Where("id = ? AND internal_only = ?", attachmentID, false).
		Select("upload_status", "validation_details").
		Updates(common_persistence_models.Attachment{
			UploadStatus:      uploadStatus,
			ValidationDetails: validationDetails,
		})

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "admin_update_attachment_status"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(logkeys.Status, status))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Attachment not found for status update",
				zap.String(logkeys.Operation, "admin_update_attachment_status"),
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
		}
		return ErrAttachmentNotFound
	}

	appLogger.For(ctx, h.logger, loggerName).Info("attachment status updated",
		zap.String(logkeys.Operation, "admin_update_attachment_status"),
		zap.String(appLogger.KeyOutcome, "updated"),
		zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
		zap.String(logkeys.Status, status),
		zap.String(appLogger.KeyValidationDetails, validationDetails))

	return nil
}

// AdminDeleteAttachment soft-deletes a non-internal attachment row. Its stored objects are kept
// whatever deleteFiles says (TODO(object-refcounting) in attachment/lifecycle_manager.go).
func (h *CommonPersistenceHandler[U]) AdminDeleteAttachment(ctx context.Context, attachmentID uint64, deleteFiles bool, reason string) error {
	result := h.getDBFromContext(ctx).
		Where("id = ? AND internal_only = ?", attachmentID, false).
		Delete(&common_persistence_models.Attachment{})
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "admin_delete_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Attachment not found for delete",
				zap.String(logkeys.Operation, "admin_delete_attachment"),
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
		}
		return ErrAttachmentNotFound
	}

	appLogger.For(ctx, h.logger, loggerName).Info("attachment deleted",
		zap.String(logkeys.Operation, "admin_delete_attachment"),
		zap.String(appLogger.KeyOutcome, "deleted"),
		zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
		zap.Bool(appLogger.KeyDeleteFiles, deleteFiles),
		zap.String(logkeys.Reason, reason))

	return nil
}

// convertToAdminContactMessage executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) convertToAdminContactMessage(msg common_persistence_models.ContactMessage) *proto_models.AdminContactMessage {
	// Handle nullable UserID field
	var userID uint64
	if msg.UserID != nil {
		userID = *msg.UserID
	}

	return &proto_models.AdminContactMessage{
		Id:          uint64(msg.ID),
		UserId:      userID,
		Name:        msg.Name,
		Email:       msg.Email,
		Subject:     msg.Subject,
		Message:     msg.Message,
		IpAddress:   msg.IPAddress,
		UserAgent:   msg.UserAgent,
		Status:      msg.Status,
		AdminNotes:  msg.AdminNotes,
		ReferenceId: msg.ReferenceID,
		CreatedAt:   h.convertTimeToProtoTimestamp(&msg.CreatedAt),
		UpdatedAt:   h.convertTimeToProtoTimestamp(&msg.UpdatedAt),
	}
}

// convertToAdminSessionToken executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) convertToAdminSessionToken(token common_persistence_models.SessionToken) *common_service_models.AdminSessionToken {
	var serviceUser *common_service_models.User
	if token.User.ID != 0 {
		serviceUser = h.convertPersistenceUserToService(token.User)
	}

	return &common_service_models.AdminSessionToken{
		TokenID:    token.ID,
		UserID:     token.UserID,
		User:       serviceUser,
		TokenType:  token.TokenType,
		Status:     deriveAdminTokenStatus(token, time.Now().UTC()),
		CreatedAt:  token.IssuedAt,
		ExpiresAt:  token.ExpiresAt,
		LastUsedAt: token.UsedAt,
		IPAddress:  token.IPAddress,
		UserAgent:  token.UserAgent,
	}
}

// Placeholder implementations for remaining methods to satisfy the interface

// AdminGetUserAnalytics executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminGetUserAnalytics(ctx context.Context, filter *proto_models.FilterRequest, groupBy string) (*proto_models.AdminUserAnalyticsResponse, error) {
	db := h.getDBFromContext(ctx)

	// Default grouping to "day" if not specified
	if groupBy == "" {
		groupBy = "day"
	}

	// Validate grouping parameter
	switch groupBy {
	case "day", "week", "month":
		// Valid grouping
	default:
		groupBy = "day" // Default to day if invalid
	}

	// Build date range filter
	var fromTime, toTime *time.Time
	if filter != nil {
		if filter.FromDate != nil {
			t := time.Unix(filter.FromDate.Seconds, int64(filter.FromDate.Nanos))
			fromTime = &t
		}
		if filter.ToDate != nil {
			t := time.Unix(filter.ToDate.Seconds, int64(filter.ToDate.Nanos))
			toTime = &t
		}
	}
	baseQuery := db.Model(&common_persistence_models.User{})
	if fromTime != nil {
		baseQuery = baseQuery.Where("created_at >= ?", *fromTime)
	}
	if toTime != nil {
		baseQuery = baseQuery.Where("created_at <= ?", *toTime)
	}

	// Get registration stats grouped by time period
	var registrationStats []struct {
		Date               time.Time
		NewRegistrations   int64
		EmailRegistrations int64
		OAuthRegistrations int64
	}

	// Query for registration stats
	registrationQuery := baseQuery.Session(&gorm.Session{})
	registrationBucket := analyticsBucketExpression(groupBy, "created_at")
	err := registrationQuery.
		Select(fmt.Sprintf("%s as date, COUNT(*) as new_registrations", registrationBucket)).
		Group("date").
		Order("date ASC").
		Scan(&registrationStats).Error

	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_registration_analytics"),
			zap.String(logkeys.Stage, "registration_counts"))
	}

	// Email vs OAuth registration counts for every bucket in one grouped query
	// instead of two COUNT queries per bucket.
	var authMethodStats []struct {
		Date               time.Time
		EmailRegistrations int64
		OauthRegistrations int64
	}
	if err := buildRegistrationAuthMethodStatsQuery(db, groupBy, fromTime, toTime).Scan(&authMethodStats).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_registration_analytics"),
			zap.String(logkeys.Stage, "auth_method_counts"))
	}
	authStatsByBucket := make(map[int64]struct{ email, oauth int64 }, len(authMethodStats))
	for _, stat := range authMethodStats {
		authStatsByBucket[stat.Date.Unix()] = struct{ email, oauth int64 }{stat.EmailRegistrations, stat.OauthRegistrations}
	}

	// Convert registration stats to proto format
	protoRegistrationStats := make([]*proto_models.UserRegistrationStats, len(registrationStats))
	for i, stat := range registrationStats {
		counts := authStatsByBucket[stat.Date.Unix()]
		protoRegistrationStats[i] = &proto_models.UserRegistrationStats{
			Date:               &timestamppb.Timestamp{Seconds: stat.Date.Unix()},
			NewRegistrations:   uint64(stat.NewRegistrations),
			EmailRegistrations: uint64(counts.email),
			OauthRegistrations: uint64(counts.oauth),
		}
	}

	// Get activity stats (logins per time period)
	var activityStats []struct {
		Date         time.Time
		ActiveUsers  int64
		TotalLogins  int64
		UniqueLogins int64
	}

	// Query for activity stats from auth tokens
	activityQuery := db.Model(&common_persistence_models.SessionToken{}).
		Where("token_type = ?", proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION)

	if fromTime != nil {
		activityQuery = activityQuery.Where("issued_at >= ?", *fromTime)
	}
	if toTime != nil {
		activityQuery = activityQuery.Where("issued_at <= ?", *toTime)
	}

	activityBucket := analyticsBucketExpression(groupBy, "issued_at")
	err = activityQuery.
		Select(fmt.Sprintf(`
			%s as date,
			COUNT(DISTINCT user_id) as unique_logins,
			COUNT(*) as total_logins
		`, activityBucket)).
		Group("date").
		Order("date ASC").
		Scan(&activityStats).Error

	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_activity_analytics"),
			zap.String(logkeys.Stage, "activity_counts"))
	}

	// Active users are users whose authentication session overlaps a bucket,
	// including sessions issued before that bucket. The previous issued_at
	// grouping attributed a long-lived session only to its creation day and the
	// expires_at > issued_at predicate did not test whether it was active in the
	// reporting bucket.
	var activeUserRows []struct {
		Date        time.Time
		ActiveUsers int64
	}

	activeStart, activeEnd := fromTime, toTime
	if activeStart == nil && len(activityStats) > 0 {
		activeStart = &activityStats[0].Date
	}
	if activeEnd == nil && len(activityStats) > 0 {
		activeEnd = &activityStats[len(activityStats)-1].Date
	}
	if activeStart != nil && activeEnd != nil {
		bucketStep := map[string]string{"day": "1 day", "week": "1 week", "month": "1 month"}[groupBy]
		boundBucket := analyticsBucketExpression(groupBy, "?::timestamptz")
		activeUsersSQL := fmt.Sprintf(`
			SELECT bucket.date AS date, COUNT(DISTINCT tokens.user_id) AS active_users
			FROM generate_series(%s, %s, INTERVAL '%s') AS bucket(date)
			LEFT JOIN %s AS tokens
				ON tokens.token_type = ?
				AND tokens.issued_at < bucket.date + INTERVAL '%s'
				AND tokens.expires_at > bucket.date
				AND (tokens.revoked_at IS NULL OR tokens.revoked_at > bucket.date)
			GROUP BY bucket.date
			ORDER BY bucket.date ASC
		`, boundBucket, boundBucket, bucketStep, common_persistence_models.TableNameSessionTokens, bucketStep)
		if err := db.Raw(
			activeUsersSQL,
			*activeStart,
			*activeEnd,
			proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION,
		).Scan(&activeUserRows).Error; err != nil {
			return nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "admin_get_activity_analytics"),
				zap.String(logkeys.Stage, "active_user_counts"))
		}
	}
	activeUsersByBucket := make(map[int64]int64, len(activeUserRows))
	for _, row := range activeUserRows {
		activeUsersByBucket[row.Date.Unix()] = row.ActiveUsers
	}

	loginStatsByBucket := make(map[int64]struct{ total, unique int64 }, len(activityStats))
	for _, stat := range activityStats {
		loginStatsByBucket[stat.Date.Unix()] = struct{ total, unique int64 }{stat.TotalLogins, stat.UniqueLogins}
	}

	// The active-user query returns every requested bucket, including buckets
	// with no new logins. Fall back to login buckets for direct persistence calls
	// that omit date bounds.
	protoActivityStats := make([]*proto_models.UserActivityStats, 0, max(len(activeUserRows), len(activityStats)))
	if len(activeUserRows) > 0 {
		for _, row := range activeUserRows {
			logins := loginStatsByBucket[row.Date.Unix()]
			protoActivityStats = append(protoActivityStats, &proto_models.UserActivityStats{
				Date:         timestamppb.New(row.Date),
				ActiveUsers:  uint64(row.ActiveUsers),
				TotalLogins:  uint64(logins.total),
				UniqueLogins: uint64(logins.unique),
			})
		}
	} else {
		for _, stat := range activityStats {
			protoActivityStats = append(protoActivityStats, &proto_models.UserActivityStats{
				Date:         timestamppb.New(stat.Date),
				ActiveUsers:  uint64(activeUsersByBucket[stat.Date.Unix()]),
				TotalLogins:  uint64(stat.TotalLogins),
				UniqueLogins: uint64(stat.UniqueLogins),
			})
		}
	}

	// Get user type distribution
	var userTypeCounts []struct {
		UserType proto_models.UserType
		Count    int64
	}

	distributionQuery := baseQuery.Session(&gorm.Session{})
	err = distributionQuery.
		Select("user_type, COUNT(*) as count").
		Group("user_type").
		Scan(&userTypeCounts).Error

	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_user_type_distribution"))
	}

	// Convert to map for proto response
	userTypeMap := make(map[string]uint64)
	for _, count := range userTypeCounts {
		userTypeMap[count.UserType.String()] = uint64(count.Count)
	}

	return &proto_models.AdminUserAnalyticsResponse{
		RegistrationStats: protoRegistrationStats,
		ActivityStats:     protoActivityStats,
		UserTypeDistribution: &proto_models.UserTypeDistribution{
			UserTypeCounts: userTypeMap,
		},
		GroupBy: groupBy,
	}, nil
}

const adminStorageContentTypeLimit = 50

// AdminGetStorageAnalytics executes its operation in the persistence flow between service handlers and the database layer.
// Parameters:
//   - ctx: context.Context input used by AdminGetStorageAnalytics.
//   - filter: *proto_models.FilterRequest input used by AdminGetStorageAnalytics.
//   - groupBy: Effective date bucket granularity.
func (h *CommonPersistenceHandler[U]) AdminGetStorageAnalytics(
	ctx context.Context,
	filter *proto_models.FilterRequest,
	groupBy string,
) (*proto_models.AdminStorageAnalyticsResponse, error) {
	db := h.getDBFromContext(ctx)
	switch groupBy {
	case "day", "week", "month":
	default:
		groupBy = "day"
	}

	// Get total storage stats
	var totalStats struct {
		TotalBytes       sql.NullInt64
		TotalAttachments int64
	}

	baseQuery := db.Model(&common_persistence_models.Attachment{}).
		Where("upload_status = ? AND internal_only = ?", common_persistence_models.UploadStatusVetted, false)

	// Apply date filters if provided
	if filter != nil {
		if filter.FromDate != nil {
			fromTime := time.Unix(filter.FromDate.Seconds, int64(filter.FromDate.Nanos))
			baseQuery = baseQuery.Where("created_at >= ?", fromTime)
		}
		if filter.ToDate != nil {
			toTime := time.Unix(filter.ToDate.Seconds, int64(filter.ToDate.Nanos))
			baseQuery = baseQuery.Where("created_at <= ?", toTime)
		}
	}

	err := baseQuery.
		Select("COALESCE(SUM(size), 0) as total_bytes, COUNT(*) as total_attachments").
		Scan(&totalStats).Error

	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_storage_analytics"),
			zap.String(logkeys.Stage, "totals"))
	}

	var totalBytes uint64
	if totalStats.TotalBytes.Valid {
		totalBytes = uint64(totalStats.TotalBytes.Int64)
	}

	// Get storage by user type
	var storageByUserType []struct {
		UserType        proto_models.UserType
		TotalBytes      sql.NullInt64
		AttachmentCount int64
	}

	userTypeQuery := db.Model(&common_persistence_models.Attachment{}).
		Joins(fmt.Sprintf("JOIN %s ON %s.id = %s.user_id", common_persistence_models.TableNameUsers, common_persistence_models.TableNameUsers, common_persistence_models.TableNameAttachments)).
		Where(fmt.Sprintf("%s.upload_status = ? AND %s.internal_only = ?", common_persistence_models.TableNameAttachments, common_persistence_models.TableNameAttachments), common_persistence_models.UploadStatusVetted, false)

	if filter != nil {
		if filter.FromDate != nil {
			fromTime := time.Unix(filter.FromDate.Seconds, int64(filter.FromDate.Nanos))
			userTypeQuery = userTypeQuery.Where(fmt.Sprintf("%s.created_at >= ?", common_persistence_models.TableNameAttachments), fromTime)
		}
		if filter.ToDate != nil {
			toTime := time.Unix(filter.ToDate.Seconds, int64(filter.ToDate.Nanos))
			userTypeQuery = userTypeQuery.Where(fmt.Sprintf("%s.created_at <= ?", common_persistence_models.TableNameAttachments), toTime)
		}
	}

	err = userTypeQuery.
		Select(fmt.Sprintf("%s.user_type, COALESCE(SUM(%s.size), 0) as total_bytes, COUNT(%s.id) as attachment_count", common_persistence_models.TableNameUsers, common_persistence_models.TableNameAttachments, common_persistence_models.TableNameAttachments)).
		Group(fmt.Sprintf("%s.user_type", common_persistence_models.TableNameUsers)).
		Scan(&storageByUserType).Error

	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_storage_analytics"),
			zap.String(logkeys.Stage, "by_user_type"))
	}

	// Convert to proto format
	protoStorageByUserType := make([]*proto_models.StorageByUserType, len(storageByUserType))
	for i, stat := range storageByUserType {
		var bytes uint64
		if stat.TotalBytes.Valid {
			bytes = uint64(stat.TotalBytes.Int64)
		}
		protoStorageByUserType[i] = &proto_models.StorageByUserType{
			UserType:        stat.UserType,
			TotalBytes:      bytes,
			AttachmentCount: uint64(stat.AttachmentCount),
		}
	}

	// Get storage by content type
	var storageByContentType []struct {
		ContentType     string
		TotalBytes      sql.NullInt64
		AttachmentCount int64
	}

	contentTypeQuery := baseQuery.Session(&gorm.Session{})
	err = contentTypeQuery.
		Select("content_type, COALESCE(SUM(size), 0) as total_bytes, COUNT(*) as attachment_count").
		Group("content_type").
		Order("total_bytes DESC").
		Order("content_type ASC").
		Limit(adminStorageContentTypeLimit + 1).
		Scan(&storageByContentType).Error

	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_storage_analytics"),
			zap.String(logkeys.Stage, "by_content_type"))
	}

	storageByContentTypeTruncated := len(storageByContentType) > adminStorageContentTypeLimit
	if storageByContentTypeTruncated {
		storageByContentType = storageByContentType[:adminStorageContentTypeLimit]
	}

	// Convert to proto format
	protoStorageByContentType := make([]*proto_models.StorageByContentType, len(storageByContentType))
	for i, stat := range storageByContentType {
		var bytes uint64
		if stat.TotalBytes.Valid {
			bytes = uint64(stat.TotalBytes.Int64)
		}
		protoStorageByContentType[i] = &proto_models.StorageByContentType{
			ContentType:     stat.ContentType,
			TotalBytes:      bytes,
			AttachmentCount: uint64(stat.AttachmentCount),
		}
	}

	// Get storage growth stats over time (last 30 days by default)
	startDate := time.Now().UTC().AddDate(0, 0, -30)
	if filter != nil && filter.FromDate != nil {
		startDate = time.Unix(filter.FromDate.Seconds, int64(filter.FromDate.Nanos))
	}

	endDate := time.Now().UTC()
	if filter != nil && filter.ToDate != nil {
		endDate = time.Unix(filter.ToDate.Seconds, int64(filter.ToDate.Nanos))
	}

	// Calculate growth stats using the normalized bucket granularity.
	var bucketStats []struct {
		Date           time.Time
		NewBytes       sql.NullInt64
		NewAttachments int64
	}

	growthQuery := db.Model(&common_persistence_models.Attachment{}).
		Where("upload_status = ? AND internal_only = ?", common_persistence_models.UploadStatusVetted, false).
		Where("created_at >= ? AND created_at <= ?", startDate, endDate)

	err = growthQuery.
		Select(fmt.Sprintf(`
			DATE_TRUNC('%s', created_at) as date,
			COALESCE(SUM(size), 0) as new_bytes,
			COUNT(*) as new_attachments
		`, groupBy)).
		Group("date").
		Order("date ASC").
		Scan(&bucketStats).Error

	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_storage_analytics"),
			zap.String(logkeys.Stage, "growth"))
	}

	// Calculate cumulative totals
	protoGrowthStats := make([]*proto_models.StorageGrowthStats, len(bucketStats))
	var cumulativeBytes uint64 = 0

	// Get initial total before the start date
	var initialTotal sql.NullInt64
	db.Model(&common_persistence_models.Attachment{}).
		Where("upload_status = ? AND internal_only = ? AND created_at < ?", common_persistence_models.UploadStatusVetted, false, startDate).
		Select("COALESCE(SUM(size), 0)").
		Scan(&initialTotal)

	if initialTotal.Valid {
		cumulativeBytes = uint64(initialTotal.Int64)
	}

	// Build growth stats with cumulative totals
	for i, stat := range bucketStats {
		var newBytes uint64
		if stat.NewBytes.Valid {
			newBytes = uint64(stat.NewBytes.Int64)
		}
		cumulativeBytes += newBytes

		protoGrowthStats[i] = &proto_models.StorageGrowthStats{
			Date:           &timestamppb.Timestamp{Seconds: stat.Date.Unix()},
			TotalBytes:     cumulativeBytes,
			NewBytes:       newBytes,
			NewAttachments: uint64(stat.NewAttachments),
		}
	}

	return &proto_models.AdminStorageAnalyticsResponse{
		TotalStorageBytes:             totalBytes,
		TotalAttachments:              uint64(totalStats.TotalAttachments),
		StorageByUserType:             protoStorageByUserType,
		StorageByContentType:          protoStorageByContentType,
		GrowthStats:                   protoGrowthStats,
		GroupBy:                       groupBy,
		StorageByContentTypeTruncated: storageByContentTypeTruncated,
		StorageByContentTypeLimit:     adminStorageContentTypeLimit,
	}, nil
}

// normalizedAdminTokenStatusFilter executes its operation in the persistence flow between service handlers and the database layer.
func normalizedAdminTokenStatusFilter(req *proto_models.AdminListTokensRequest) proto_models.TokenStatus {
	if req == nil {
		return proto_models.TokenStatus_TOKEN_STATUS_ALL
	}

	switch req.StatusFilter {
	case proto_models.TokenStatus_TOKEN_STATUS_ALL,
		proto_models.TokenStatus_TOKEN_STATUS_ACTIVE,
		proto_models.TokenStatus_TOKEN_STATUS_EXPIRED,
		proto_models.TokenStatus_TOKEN_STATUS_REVOKED:
		return req.StatusFilter
	}

	return proto_models.TokenStatus_TOKEN_STATUS_ALL
}

// deriveAdminTokenStatus executes its operation in the persistence flow between service handlers and the database layer.
func deriveAdminTokenStatus(token common_persistence_models.SessionToken, now time.Time) proto_models.TokenStatus {
	// A future-dated revoked_at is a pending revocation (session-refresh grace window):
	// the token still authenticates until then, so only treat it as revoked once elapsed.
	if token.RevokedAt != nil && !token.RevokedAt.After(now) {
		return proto_models.TokenStatus_TOKEN_STATUS_REVOKED
	}
	if !token.ExpiresAt.After(now) {
		return proto_models.TokenStatus_TOKEN_STATUS_EXPIRED
	}
	// Single-use tokens with a consumed marker are no longer active. Surface them as
	// expired because the shared enum intentionally does not model a separate USED state.
	if token.TokenType != proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION && token.UsedAt != nil {
		return proto_models.TokenStatus_TOKEN_STATUS_EXPIRED
	}
	return proto_models.TokenStatus_TOKEN_STATUS_ACTIVE
}

// buildAdminListTokensQuery executes its operation in the persistence flow between service handlers and the database layer.
func buildAdminListTokensQuery(db *gorm.DB, req *proto_models.AdminListTokensRequest, now time.Time) *gorm.DB {
	const tokensTable = common_persistence_models.TableNameSessionTokens
	const usersTable = common_persistence_models.TableNameUsers

	query := db.Model(&common_persistence_models.SessionToken{})

	if req == nil {
		req = &proto_models.AdminListTokensRequest{}
	}

	if req.UserIdFilter != nil {
		query = query.Where(tokensTable+".user_id = ?", *req.UserIdFilter)
	}

	if req.TokenTypeFilter != proto_models.TokenType_TOKEN_TYPE_UNSPECIFIED {
		query = query.Where(tokensTable+".token_type = ?", req.TokenTypeFilter)
	}

	if req.Filter != nil {
		if req.Filter.FromDate != nil {
			query = query.Where(tokensTable+".issued_at >= ?", req.Filter.FromDate.AsTime())
		}
		if req.Filter.ToDate != nil {
			query = query.Where(tokensTable+".issued_at < ?", req.Filter.ToDate.AsTime())
		}
	}

	// Revocation semantics mirror the auth layer: a token counts as revoked only once
	// revoked_at has elapsed; a future-dated revoked_at is still live (refresh grace window).
	switch normalizedAdminTokenStatusFilter(req) {
	case proto_models.TokenStatus_TOKEN_STATUS_ACTIVE:
		query = query.Where("("+tokensTable+".revoked_at IS NULL OR "+tokensTable+".revoked_at > ?) AND "+tokensTable+".expires_at > ?", now, now)
		query = query.Where("("+tokensTable+".token_type = ? OR "+tokensTable+".used_at IS NULL)",
			proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION)
	case proto_models.TokenStatus_TOKEN_STATUS_EXPIRED:
		query = query.Where("("+tokensTable+".revoked_at IS NULL OR "+tokensTable+".revoked_at > ?)", now)
		query = query.Where("("+tokensTable+".expires_at <= ? OR ("+tokensTable+".token_type <> ? AND "+tokensTable+".used_at IS NOT NULL))",
			now, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION)
	case proto_models.TokenStatus_TOKEN_STATUS_REVOKED:
		query = query.Where(tokensTable+".revoked_at IS NOT NULL AND "+tokensTable+".revoked_at <= ?", now)
	}

	if rawTokenFilter := strings.TrimSpace(req.RawTokenFilter); rawTokenFilter != "" {
		query = query.Where(tokensTable+".token = ?", rawTokenFilter)
	}

	if req.Filter != nil {
		search := strings.TrimSpace(req.Filter.Search)
		if search != "" {
			pattern := "%" + search + "%"
			query = query.Joins("LEFT JOIN " + usersTable + " ON " + usersTable + ".id = " + tokensTable + ".user_id")

			searchQuery := db.Where(
				tokensTable+".ip_address ILIKE ? OR "+tokensTable+".user_agent ILIKE ? OR "+usersTable+".email ILIKE ? OR "+usersTable+".username ILIKE ?",
				pattern, pattern, pattern, pattern,
			)

			if numericSearch, err := strconv.ParseUint(search, 10, 64); err == nil {
				searchQuery = searchQuery.Or(tokensTable+".id = ? OR "+tokensTable+".user_id = ?", numericSearch, numericSearch)
			}

			query = query.Where(searchQuery)
		}
	}

	orderClause := common_util.BuildOrderClause(req.Sort, adminListTokensSortColumn(req.GetSortBy(), tokensTable), true)
	return query.Order(orderClause)
}

// AdminListTokens executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminListTokens(ctx context.Context, req *proto_models.AdminListTokensRequest) ([]*common_service_models.AdminSessionToken, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)

	if req == nil {
		req = &proto_models.AdminListTokensRequest{}
	}

	pagination := req.GetPagination()
	if err := requirePageLimit("admin_list_tokens", pagination); err != nil {
		return nil, nil, err
	}

	query := buildAdminListTokensQuery(db, req, time.Now().UTC())

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "admin_list_auth_tokens"),
				zap.String(logkeys.Stage, "count"))
		}
	}

	// Apply pagination
	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	query = query.Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount))

	// Execute query
	var tokens []common_persistence_models.SessionToken
	if err := query.
		Preload(common_persistence_models.PreloadUser, preloadUnscoped).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment, preloadLiveAttachment).
		Find(&tokens).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_auth_tokens"),
			zap.String(logkeys.Stage, "list"),
			zap.Uint32(appLogger.KeyPage, pagination.Page),
			zap.Uint32(logkeys.Limit, pagination.Limit))
	}
	tokens, hasNextPage := common_util.TrimPaginationProbe(tokens, pagination.Limit, includeTotalCount)

	// Convert to admin auth token format
	adminTokens := make([]*common_service_models.AdminSessionToken, len(tokens))
	for i, token := range tokens {
		adminTokens[i] = h.convertToAdminSessionToken(token)
	}

	// Build pagination response
	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage)
	}

	return adminTokens, paginationResponse, nil
}

// AdminRevokeToken executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminRevokeToken(ctx context.Context, tokenID, reason string) error {
	// TODO: Implement token revocation by ID
	return fmt.Errorf("not implemented: token revocation by ID")
}

// AdminGetSystemStats executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminGetSystemStats(ctx context.Context) (*proto_models.AdminSystemStatsResponse, error) {
	db := h.getDBFromContext(ctx)

	// Get basic user counts
	var totalUsers int64
	db.Model(&common_persistence_models.User{}).Count(&totalUsers)

	var totalContactMessages int64
	db.Model(&common_persistence_models.ContactMessage{}).Count(&totalContactMessages)

	var pendingContactMessages int64
	db.Model(&common_persistence_models.ContactMessage{}).
		Where("status = ?", common_persistence_models.ContactStatusPending).
		Count(&pendingContactMessages)

	var totalAttachments int64
	db.Model(&common_persistence_models.Attachment{}).Where("internal_only = ?", false).Count(&totalAttachments)

	// Get total storage bytes
	var totalStorageBytes sql.NullInt64
	db.Model(&common_persistence_models.Attachment{}).
		Where("upload_status = ? AND internal_only = ?", common_persistence_models.UploadStatusVetted, false).
		Select("COALESCE(SUM(size), 0)").
		Scan(&totalStorageBytes)

	var storageUsed uint64
	if totalStorageBytes.Valid {
		storageUsed = uint64(totalStorageBytes.Int64)
	}

	// Get active sessions
	var activeSessions int64
	nowUTC := time.Now().UTC()
	db.Model(&common_persistence_models.SessionToken{}).
		Where("token_type = ? AND expires_at > ? AND (revoked_at IS NULL OR revoked_at > ?)",
			proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, nowUTC, nowUTC).
		Count(&activeSessions)

	// TODO: Implement proper 30-day active users calculation
	activeUsersLast30Days := uint64(totalUsers) // Placeholder

	return &proto_models.AdminSystemStatsResponse{
		TotalUsers:             uint64(totalUsers),
		ActiveUsersLast_30Days: activeUsersLast30Days,
		TotalContactMessages:   uint64(totalContactMessages),
		PendingContactMessages: uint64(pendingContactMessages),
		TotalAttachments:       uint64(totalAttachments),
		TotalStorageBytes:      storageUsed,
		ActiveSessions:         uint64(activeSessions),
		SystemStartTime:        nil, // TODO: Track system start time
	}, nil
}

// ============================================================================
// ADMIN CHAT CHANNEL MANAGEMENT METHODS
// ============================================================================

// AdminListChatChannels executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminListChatChannels(
	ctx context.Context,
	req *proto_models.AdminListChatChannelsRequest,
) ([]*common_service_models.AdminChatChannelDetails, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)

	if req == nil {
		req = &proto_models.AdminListChatChannelsRequest{}
	}

	pagination := req.GetPagination()
	if err := requirePageLimit("admin_list_chat_channels", pagination); err != nil {
		return nil, nil, err
	}

	query := db.Model(&common_persistence_models.ChatChannel{})
	if req.GetIncludeDeleted() {
		query = query.Unscoped()
	}

	// Apply search filter
	if search := req.GetSearch(); search != "" {
		searchPattern := "%" + search + "%"
		searchQuery := db.Where("name ILIKE ? OR description ILIKE ?", searchPattern, searchPattern)
		if id, err := strconv.ParseUint(search, 10, 64); err == nil {
			searchQuery = searchQuery.Or("id = ?", id)
		}
		query = query.Where(searchQuery)
	}

	// Apply explicit ID filter
	if req.ChannelIdFilter != nil {
		query = query.Where("id = ?", req.GetChannelIdFilter())
	}

	// Apply created by filter
	if req.CreatedByUserIdFilter != nil {
		query = query.Where("created_by_user_id = ?", req.GetCreatedByUserIdFilter())
	}

	// Apply participant filter (active membership only)
	if req.ParticipantUserIdFilter != nil {
		channelTable := common_persistence_models.TableNameChatChannels
		participantTable := common_persistence_models.TableNameChatChannelParticipants
		query = query.Where(
			fmt.Sprintf(
				"EXISTS (SELECT 1 FROM %s p WHERE p.channel_id = %s.id AND p.user_id = ? AND p.left_at IS NULL)",
				participantTable,
				channelTable,
			),
			req.GetParticipantUserIdFilter(),
		)
	}

	// Apply channel type filter
	if req.GetChannelTypeFilter() != proto_models.ChannelType_CHANNEL_TYPE_UNSPECIFIED {
		query = query.Where("channel_type = ?", req.GetChannelTypeFilter())
	}

	// Apply archived filter
	if !req.GetIncludeArchived() {
		query = query.Where("is_archived = false")
	}

	// Apply sorting
	orderClause := common_util.BuildOrderClause(req.Sort, adminChatChannelsSortColumn(req.GetSortBy()), true)
	query = query.Order(orderClause)

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "admin_list_chat_channels"),
				zap.String(logkeys.Stage, "count"))
		}
	}

	// Apply pagination
	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	query = query.Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount))

	// Execute query
	var channels []common_persistence_models.ChatChannel
	if err := preloadPublicChatAvatar(query).
		Preload(common_persistence_models.PreloadCreatedByUser, preloadUnscoped).
		Preload(common_persistence_models.PreloadCreatedByUserProfilePicture, preloadLiveAttachment).
		Find(&channels).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_chat_channels"),
			zap.String(logkeys.Stage, "list"),
			zap.Uint32(appLogger.KeyPage, pagination.Page),
			zap.Uint32(logkeys.Limit, pagination.Limit))
	}
	channels, hasNextPage := common_util.TrimPaginationProbe(channels, pagination.Limit, includeTotalCount)
	for i := range channels {
		sanitizeChatChannelAvatar(&channels[i])
	}

	// Fetch active participant counts for returned channels
	counts := make(map[uint64]uint64, len(channels))
	if len(channels) > 0 {
		channelIDs := make([]uint64, 0, len(channels))
		for _, channel := range channels {
			channelIDs = append(channelIDs, channel.ID)
		}

		// countRow carries grouped participant totals while AdminListChatChannels maps
		// aggregate query results back onto each returned channel in the admin listing flow.
		type countRow struct {
			ChannelID uint64
			Count     int64
		}
		var rows []countRow
		if err := db.Model(&common_persistence_models.ChatChannelParticipant{}).
			Select("channel_id, COUNT(*) as count").
			Where("channel_id IN ? AND left_at IS NULL", channelIDs).
			Group("channel_id").
			Scan(&rows).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "admin_list_chat_channels"),
				zap.String(logkeys.Stage, "participant_counts"),
				zap.Int(appLogger.KeyCount, len(channelIDs)))
		}
		for _, row := range rows {
			counts[row.ChannelID] = uint64(row.Count)
		}
	}

	adminChannels := make([]*common_service_models.AdminChatChannelDetails, len(channels))
	for i, channel := range channels {
		var createdByUser *common_service_models.User
		if channel.CreatedByUser != nil && channel.CreatedByUser.ID != 0 {
			createdByUser = h.convertPersistenceUserToService(*channel.CreatedByUser)
		}

		adminChannels[i] = &common_service_models.AdminChatChannelDetails{
			Channel:                h.convertPersistenceChatChannelToService(channel),
			ActiveParticipantCount: counts[channel.ID],
			CreatedByUser:          createdByUser,
		}
	}

	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage)
	}
	return adminChannels, paginationResponse, nil
}

// AdminGetChatChannel executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminGetChatChannel(ctx context.Context, channelID uint64, includeDeleted bool) (*common_service_models.AdminChatChannelDetails, error) {
	db := h.getDBFromContext(ctx)

	query := db.Model(&common_persistence_models.ChatChannel{})
	if includeDeleted {
		query = query.Unscoped()
	}

	var channel common_persistence_models.ChatChannel
	if err := preloadPublicChatAvatar(query).
		Preload(common_persistence_models.PreloadCreatedByUser, preloadUnscoped).
		Preload(common_persistence_models.PreloadCreatedByUserProfilePicture, preloadLiveAttachment).
		Where("id = ?", channelID).
		First(&channel).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChatChannelNotFound
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_chat_channel"),
			zap.String(logkeys.Stage, "channel"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	sanitizeChatChannelAvatar(&channel)

	// Count active participants
	var activeCount int64
	if err := db.Model(&common_persistence_models.ChatChannelParticipant{}).
		Where("channel_id = ? AND left_at IS NULL", channelID).
		Count(&activeCount).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_get_chat_channel"),
			zap.String(logkeys.Stage, "participant_count"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}

	var createdByUser *common_service_models.User
	if channel.CreatedByUser != nil && channel.CreatedByUser.ID != 0 {
		createdByUser = h.convertPersistenceUserToService(*channel.CreatedByUser)
	}

	return &common_service_models.AdminChatChannelDetails{
		Channel:                h.convertPersistenceChatChannelToService(channel),
		ActiveParticipantCount: uint64(activeCount),
		CreatedByUser:          createdByUser,
	}, nil
}

// AdminUpdateChatChannel executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminUpdateChatChannel(ctx context.Context, req *proto_models.AdminUpdateChatChannelRequest) (*common_service_models.AdminChatChannelDetails, error) {
	if req.SupportThreads != nil && !req.GetSupportThreads() {
		var details *common_service_models.AdminChatChannelDetails
		err := h.withChatLocationTransaction(ctx, func(txCtx context.Context, db *gorm.DB) error {
			if _, err := h.lockChatLocationChannel(txCtx, req.GetChannelId()); err != nil {
				return err
			}
			var count int64
			if err := db.Model(&common_persistence_models.ChatAgentSession{}).Where("channel_id = ? AND parent_message_id <> 0", req.GetChannelId()).Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return ErrChatChannelAgentSessionThreadsRequired
			}
			var err error
			details, err = h.adminUpdateChatChannelWithDB(txCtx, db, req)
			return err
		})
		return details, err
	}

	if req.AvatarAttachmentId == nil {
		return h.adminUpdateChatChannelWithDB(ctx, h.getDBFromContext(ctx), req)
	}

	var details *common_service_models.AdminChatChannelDetails
	err := h.withChatAvatarWriteTransaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		var updateErr error
		details, updateErr = h.adminUpdateChatChannelWithDB(txCtx, tx, req)
		return updateErr
	})
	if err != nil {
		return nil, err
	}
	return details, nil
}

func (h *CommonPersistenceHandler[U]) adminUpdateChatChannelWithDB(
	ctx context.Context,
	db *gorm.DB,
	req *proto_models.AdminUpdateChatChannelRequest,
) (*common_service_models.AdminChatChannelDetails, error) {

	updates := map[string]any{}
	if req.Name != nil {
		updates["name"] = req.GetName()
	}
	if req.Description != nil {
		updates["description"] = req.GetDescription()
	}
	if req.AvatarAttachmentId != nil {
		if req.GetAvatarAttachmentId() == 0 {
			updates["avatar_attachment_id"] = nil
		} else {
			if _, err := loadPublicChatAvatarForUpdate(db, req.GetAvatarAttachmentId(), nil); err != nil {
				return nil, err
			}
			updates["avatar_attachment_id"] = req.GetAvatarAttachmentId()
		}
	}
	if req.IsArchived != nil {
		updates["is_archived"] = req.GetIsArchived()
	}

	// Feature flags change bit by bit in one expression, so a concurrent update of other bits survives.
	var set, clear common_persistence_models.ChatChannelFeatures
	for flag, value := range map[common_persistence_models.ChatChannelFeatures]*bool{
		common_persistence_models.ChatChannelSupportReplies:                req.SupportReplies,
		common_persistence_models.ChatChannelSupportThreads:                req.SupportThreads,
		common_persistence_models.ChatChannelSupportMessageEdits:           req.SupportMessageEdits,
		common_persistence_models.ChatChannelSupportMessageDeletes:         req.SupportMessageDeletes,
		common_persistence_models.ChatChannelSupportMessageReactions:       req.SupportMessageReactions,
		common_persistence_models.ChatChannelAllowMultipleReactionsPerUser: req.AllowMultipleReactionsPerUser,
		common_persistence_models.ChatChannelSupportEmbeds:                 req.SupportEmbeds,
		common_persistence_models.ChatChannelSupportAttachments:            req.SupportAttachments,
		common_persistence_models.ChatChannelAllowNativeEmbeds:             req.AllowNativeEmbeds,
		common_persistence_models.ChatChannelAllowCustomEmbeds:             req.AllowCustomEmbeds,
		common_persistence_models.ChatChannelSupportInferenceUsage:         req.SupportInferenceUsage,
	} {
		switch {
		case value == nil:
		case *value:
			set |= flag
		default:
			clear |= flag
		}
	}
	if set|clear != 0 {
		updates["features"] = gorm.Expr("(features & ~?::bigint) | ?::bigint", int64(clear), int64(set))
	}

	// Context fields
	if req.ClearContext != nil && req.GetClearContext() {
		updates["context_id"] = nil
		updates["context_display_name"] = nil
		updates["context_entity_id"] = nil
	} else {
		if req.ContextId != nil {
			updates["context_id"] = req.GetContextId()
		}
		if req.ContextDisplayName != nil {
			updates["context_display_name"] = req.GetContextDisplayName()
		}
		if req.ContextEntityId != nil {
			updates["context_entity_id"] = req.GetContextEntityId()
		}
	}

	if len(updates) == 0 {
		return h.AdminGetChatChannel(ctx, req.GetChannelId(), false)
	}

	result := db.Model(&common_persistence_models.ChatChannel{}).
		Where("id = ?", req.GetChannelId()).
		Updates(updates)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "admin_update_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, req.GetChannelId()))
	}
	if result.RowsAffected == 0 {
		return nil, ErrChatChannelNotFound
	}

	return h.AdminGetChatChannel(ctx, req.GetChannelId(), false)
}

// AdminDeleteChatChannel executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminDeleteChatChannel(ctx context.Context, channelID uint64, reason string) error {
	db := h.getDBFromContext(ctx)

	result := db.Where("id = ?", channelID).Delete(&common_persistence_models.ChatChannel{})
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "admin_delete_chat_channel"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	if result.RowsAffected == 0 {
		return ErrChatChannelNotFound
	}

	appLogger.For(ctx, h.logger, loggerName).Info("chat channel soft-deleted",
		zap.String(logkeys.Operation, "admin_delete_chat_channel"),
		zap.String(appLogger.KeyOutcome, "soft_deleted"),
		zap.Uint64(appLogger.KeyChannelID, channelID),
		zap.String(logkeys.Reason, reason))

	return nil
}

// AdminListChatChannelParticipants executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminListChatChannelParticipants(
	ctx context.Context,
	channelID uint64,
	pagination *proto_models.PaginationRequest,
	includeLeft bool,
) ([]*common_service_models.ChatChannelParticipant, *proto_models.PaginationResponse, error) {
	db := h.getDBFromContext(ctx)

	if err := requirePageLimit("admin_list_chat_channel_participants", pagination); err != nil {
		return nil, nil, err
	}

	query := db.Model(&common_persistence_models.ChatChannelParticipant{}).
		Where("channel_id = ?", channelID)
	if !includeLeft {
		query = query.Where("left_at IS NULL")
	}

	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	var totalCount int64
	if includeTotalCount {
		if err := query.Count(&totalCount).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "admin_list_chat_channel_participants"),
				zap.String(logkeys.Stage, "count"),
				zap.Uint64(appLogger.KeyChannelID, channelID))
		}
	}

	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	query = query.Offset(offset).Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount))

	var participants []common_persistence_models.ChatChannelParticipant
	if err := query.
		Preload(common_persistence_models.PreloadUser, preloadUnscoped).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment, preloadLiveAttachment).
		Order("joined_at DESC").
		Find(&participants).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_list_chat_channel_participants"),
			zap.String(logkeys.Stage, "list"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint32(appLogger.KeyPage, pagination.Page),
			zap.Uint32(logkeys.Limit, pagination.Limit))
	}
	participants, hasNextPage := common_util.TrimPaginationProbe(participants, pagination.Limit, includeTotalCount)

	out := make([]*common_service_models.ChatChannelParticipant, len(participants))
	for i, participant := range participants {
		out[i] = h.convertPersistenceChatChannelParticipantToService(participant)
	}

	var paginationResponse *proto_models.PaginationResponse
	if includeTotalCount {
		paginationResponse = common_util.CreatePaginationResponse(totalCount, pagination.Limit, pagination.Page)
	} else {
		paginationResponse = common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage)
	}
	return out, paginationResponse, nil
}

// AdminAddChatChannelParticipants executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminAddChatChannelParticipants(ctx context.Context, channelID uint64, userIDs []uint64) ([]*common_service_models.ChatChannelParticipant, error) {
	db := h.getDBFromContext(ctx)

	now := time.Now()
	participants := make([]common_persistence_models.ChatChannelParticipant, 0, len(userIDs))
	for _, id := range userIDs {
		if id == 0 {
			continue
		}
		participants = append(participants, common_persistence_models.ChatChannelParticipant{
			ChannelID: channelID,
			UserID:    id,
			Role:      proto_models.ChannelRole_CHANNEL_ROLE_MEMBER,
			JoinedAt:  now,
			LeftAt:    nil,
		})
	}

	if len(participants) == 0 {
		return []*common_service_models.ChatChannelParticipant{}, nil
	}

	// Upsert to allow re-joining with fresh membership metadata. The WHERE guard
	// ensures active members (e.g. the channel owner) are not demoted when re-added.
	conflict := buildChatParticipantRejoinOnConflict(now)
	if err := h.createStampedChatParticipants(db, participants, &conflict); err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_add_chat_participants"),
			zap.String(logkeys.Stage, "upsert"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyCount, len(userIDs)))
	}

	var outRows []common_persistence_models.ChatChannelParticipant
	if err := db.
		Preload(common_persistence_models.PreloadUser).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment).
		Where("channel_id = ? AND user_id IN ? AND left_at IS NULL", channelID, userIDs).
		Find(&outRows).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_add_chat_participants"),
			zap.String(logkeys.Stage, "load_results"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyCount, len(userIDs)))
	}

	out := make([]*common_service_models.ChatChannelParticipant, len(outRows))
	for i, participant := range outRows {
		out[i] = h.convertPersistenceChatChannelParticipantToService(participant)
	}

	return out, nil
}

// AdminRemoveChatChannelParticipant executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminRemoveChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64) error {
	if err := h.RemoveChatChannelParticipant(ctx, channelID, userID); err != nil {
		return err
	}
	return nil
}

// AdminUpdateChatChannelParticipantRole executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) AdminUpdateChatChannelParticipantRole(
	ctx context.Context,
	channelID uint64,
	userID uint64,
	role proto_models.ChannelRole,
) (*common_service_models.ChatChannelParticipant, error) {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.ChatChannelParticipant{}).
		Where("channel_id = ? AND user_id = ? AND left_at IS NULL", channelID, userID).
		Update("role", role)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "admin_update_chat_participant_role"),
			zap.String(logkeys.Stage, "update"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyRole, role))
	}
	if result.RowsAffected == 0 {
		return nil, ErrChatParticipantNotFound
	}

	var participant common_persistence_models.ChatChannelParticipant
	if err := db.
		Preload(common_persistence_models.PreloadUser).
		Preload(common_persistence_models.PreloadUserProfilePictureAttachment).
		Where("channel_id = ? AND user_id = ?", channelID, userID).
		First(&participant).Error; err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "admin_update_chat_participant_role"),
			zap.String(logkeys.Stage, "load_result"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyRole, role))
	}

	return h.convertPersistenceChatChannelParticipantToService(participant), nil
}

// convertPersistenceChatChannelToService executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) convertPersistenceChatChannelToService(channel common_persistence_models.ChatChannel) *common_service_models.ChatChannel {
	var avatarAttachmentID *uint64
	avatarPath := ""
	if channel.AvatarAttachmentID != nil &&
		channel.AvatarAttachment != nil &&
		channel.AvatarAttachment.ID == *channel.AvatarAttachmentID &&
		channel.AvatarAttachment.IsValidPublicAvatar() {
		avatarAttachmentID = channel.AvatarAttachmentID
		avatarPath = channel.AvatarAttachment.FinalObjectName
	}

	var deletedAt *time.Time
	if channel.DeletedAt.Valid {
		t := channel.DeletedAt.Time
		deletedAt = &t
	}

	return &common_service_models.ChatChannel{
		ID:                  channel.ID,
		Name:                channel.Name,
		Description:         channel.Description,
		AvatarAttachmentID:  avatarAttachmentID,
		AvatarUrl:           avatarPath,
		ChannelType:         channel.ChannelType,
		IsArchived:          channel.IsArchived,
		CreatedByUserID:     channel.CreatedByUserID,
		LastMessageAt:       channel.LastMessageAt,
		LastMessagePreview:  channel.LastMessagePreview,
		CreatedAt:           channel.CreatedAt,
		UpdatedAt:           channel.UpdatedAt,
		DeletedAt:           deletedAt,
		Features:            channel.Features,
		PublicRead:          channel.PublicRead,
		ForkRules:           channel.ForkRules,
		ForkedFromChannelID: channel.ForkedFromChannelID,
		ForkPointMessageID:  channel.ForkPointMessageID,
	}
}

// convertPersistenceChatChannelParticipantToService executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) convertPersistenceChatChannelParticipantToService(participant common_persistence_models.ChatChannelParticipant) *common_service_models.ChatChannelParticipant {
	var user *common_service_models.User
	if participant.User.ID != 0 {
		user = h.convertPersistenceUserToService(participant.User)
	}

	return &common_service_models.ChatChannelParticipant{
		ChannelID:         participant.ChannelID,
		UserID:            participant.UserID,
		Role:              participant.Role,
		NotificationMode:  participant.NotificationMode,
		IsPinned:          participant.IsPinned,
		LastReadMessageID: participant.LastReadMessageID,
		JoinedAt:          participant.JoinedAt,
		UpdatedAt:         participant.UpdatedAt,
		LeftAt:            participant.LeftAt,
		User:              user,
	}
}

// convertPersistenceUserToService executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) convertPersistenceUserToService(user common_persistence_models.User) *common_service_models.User {
	serviceUser := &common_service_models.User{
		ID:        user.ID,
		Username:  user.Username,
		AvatarUrl: "",
		Email:     user.Email,
		FirstName: user.FirstName,
		LastName:  user.LastName,
		UserType:  user.UserType,
		Banned:    user.Banned,
		Bot:       user.IsBot,
	}

	// Parse the E.164 phone number into the service model's structured type.
	if user.PhoneNumber != nil {
		if parsed, err := phonenumbers.Parse(*user.PhoneNumber, ""); err == nil {
			serviceUser.PhoneNumber = parsed
		}
	}

	// AvatarUrl is composed against the public asset base by the converter layer, so
	// it may only carry an object that actually lives in the anonymously readable
	// public bucket. A temporary or privately stored object is reachable only through
	// a backend-issued presigned URL and must not leak into this field.
	if user.ProfilePictureAttachmentID != nil &&
		user.ProfilePictureAttachment != nil &&
		user.ProfilePictureAttachment.ID == *user.ProfilePictureAttachmentID &&
		user.ProfilePictureAttachment.IsValidPublicAvatar() {
		serviceUser.AvatarUrl = user.ProfilePictureAttachment.FinalObjectName
	}

	return serviceUser
}
