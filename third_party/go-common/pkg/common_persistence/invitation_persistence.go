package common_persistence

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Invitation-specific error variables for consistent error handling.
var (
	// ErrInvitationNotFound is returned when an invitation lookup fails to find a matching invitation.
	ErrInvitationNotFound = errors.New("invitation not found")

	// ErrInvitationAlreadyExists is returned when attempting to create a duplicate pending invitation
	// for the same email and organization.
	ErrInvitationAlreadyExists = errors.New("pending invitation already exists")

	// ErrInvitationAlreadyProcessed is returned when attempting to accept or decline an invitation
	// that has already been processed.
	ErrInvitationAlreadyProcessed = errors.New("invitation has already been processed")
)

// withActiveInvitationOrganization prevents retained invitations from an
// archived organization from surfacing through ordinary invitation reads.
// Personal-workspace archival intentionally preserves invitation history, so
// visibility must be enforced by the owning organization's active state.
func withActiveInvitationOrganization(db *gorm.DB) *gorm.DB {
	invitationTable := common_persistence_models.TableNameOrganizationInvitations
	return ActiveOrganizationScope(invitationTable, "organization_id")(db)
}

// withLoadedActiveInvitationOrganization hydrates the belongs-to organization
// while applying GORM's active-row scope in the same query.
func withLoadedActiveInvitationOrganization(db *gorm.DB) *gorm.DB {
	return db.InnerJoins(common_persistence_models.PreloadOrganization)
}

// GetUserByUsername executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetUserByUsername(ctx context.Context, username string) (*common_persistence_models.User, error) {
	var user common_persistence_models.User
	db := h.getDBFromContext(ctx)

	// Username lookup is case-insensitive
	result := db.Where("LOWER(username) = LOWER(?)", username).First(&user)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_user_by_username"),
			zap.String(appLogger.KeyUsername, username))
	}
	return &user, nil
}

// CreateOrganizationInvitation creates a pending invitation, relying on the partial unique
// index on (organization_id, email, requires_acceptance) to reject concurrent duplicates.
// Expired pending rows for the same email and organization are cleared first because
// expires_at cannot participate in the index predicate.
func (h *CommonPersistenceHandler[U]) CreateOrganizationInvitation(ctx context.Context, invitation *common_persistence_models.OrganizationInvitation) error {
	// Normalize email to lowercase
	invitation.Email = strings.ToLower(invitation.Email)
	db := h.getDBFromContext(ctx)

	// Clear expired pending rows so they cannot collide with the new invitation
	// under the pending-uniqueness index.
	clearResult := db.Where("email = ? AND organization_id = ? AND accepted_at IS NULL AND declined_at IS NULL AND expires_at <= ?",
		invitation.Email, invitation.OrganizationID, time.Now()).
		Delete(&common_persistence_models.OrganizationInvitation{})
	if clearResult.Error != nil {
		return databaseErrorWithFields(clearResult.Error,
			zap.String(logkeys.Operation, "clear_expired_invitations_before_create"),
			zap.String(appLogger.KeyEmail, invitation.Email),
			zap.Int64(appLogger.KeyOrganizationID, invitation.OrganizationID))
	}

	// Fast-path duplicate check; the unique index remains the arbiter under concurrency.
	var existingCount int64
	checkResult := db.Model(&common_persistence_models.OrganizationInvitation{}).
		Where("email = ? AND organization_id = ? AND accepted_at IS NULL AND declined_at IS NULL AND expires_at > ? AND requires_acceptance = ?",
			invitation.Email, invitation.OrganizationID, time.Now(), invitation.RequiresAcceptance).
		Count(&existingCount)

	if checkResult.Error != nil {
		return databaseErrorWithFields(checkResult.Error,
			zap.String(logkeys.Operation, "check_existing_organization_invitation"),
			zap.String(appLogger.KeyEmail, invitation.Email),
			zap.Int64(appLogger.KeyOrganizationID, invitation.OrganizationID))
	}

	if existingCount > 0 {
		return ErrInvitationAlreadyExists
	}

	result := db.Create(invitation)
	if result.Error != nil {
		if isUniqueViolation(result.Error) {
			// A concurrent request created the pending invitation first.
			return ErrInvitationAlreadyExists
		}
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "create_organization_invitation"),
			zap.String(appLogger.KeyEmail, invitation.Email),
			zap.Int64(appLogger.KeyOrganizationID, invitation.OrganizationID))
	}
	return nil
}

// GetOrganizationInvitationByID executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetOrganizationInvitationByID(ctx context.Context, id uint64) (*common_persistence_models.OrganizationInvitation, error) {
	var invitation common_persistence_models.OrganizationInvitation
	db := h.getDBFromContext(ctx)

	result := withLoadedActiveInvitationOrganization(db).
		Where(common_persistence_models.TableNameOrganizationInvitations+".id = ?", id).
		First(&invitation)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrInvitationNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_organization_invitation_by_id"),
			zap.Uint64(appLogger.KeyInvitationID, id))
	}
	return &invitation, nil
}

// GetPendingInvitationByEmailAndOrg executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetPendingInvitationByEmailAndOrg(ctx context.Context, email string, organizationID int64) (*common_persistence_models.OrganizationInvitation, error) {
	var invitation common_persistence_models.OrganizationInvitation
	db := h.getDBFromContext(ctx)

	// Normalize email to lowercase for lookup
	email = strings.ToLower(email)

	result := withLoadedActiveInvitationOrganization(db).
		Where("email = ? AND organization_id = ? AND accepted_at IS NULL AND declined_at IS NULL AND expires_at > ? AND requires_acceptance = ?",
			email, organizationID, time.Now(), true).
		First(&invitation)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrInvitationNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_pending_invitation_by_email_and_organization"),
			zap.String(appLogger.KeyEmail, email),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}
	return &invitation, nil
}

// GetPendingInvitationsByEmail executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetPendingInvitationsByEmail(ctx context.Context, email string) ([]*common_persistence_models.OrganizationInvitation, error) {
	var invitations []*common_persistence_models.OrganizationInvitation
	db := h.getDBFromContext(ctx)

	// Normalize email to lowercase for lookup
	email = strings.ToLower(email)

	result := withLoadedActiveInvitationOrganization(db).
		Where("email = ? AND accepted_at IS NULL AND declined_at IS NULL AND expires_at > ? AND requires_acceptance = ?", email, time.Now(), true).
		Order(common_persistence_models.TableNameOrganizationInvitations + ".created_at DESC").
		Find(&invitations)

	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_pending_invitations_by_email"),
			zap.String(appLogger.KeyEmail, email))
	}
	return invitations, nil
}

// GetPendingInvitationsByEmailPage returns a bounded API page while preserving
// GetPendingInvitationsByEmail for registration-time processing of the complete
// pending set.
func (h *CommonPersistenceHandler[U]) GetPendingInvitationsByEmailPage(
	ctx context.Context,
	email string,
	pagination *proto_models.PaginationRequest,
) ([]*common_persistence_models.OrganizationInvitation, *proto_models.PaginationResponse, error) {
	if err := requirePageLimit("list_pending_invitation_page_by_email", pagination); err != nil {
		return nil, nil, err
	}
	includeTotalCount := common_util.ShouldIncludeTotalCount(pagination)
	email = strings.ToLower(strings.TrimSpace(email))
	now := time.Now()
	db := h.getDBFromContext(ctx)
	baseQuery := withActiveInvitationOrganization(db.Model(&common_persistence_models.OrganizationInvitation{})).
		Where(
			"email = ? AND accepted_at IS NULL AND declined_at IS NULL AND expires_at > ? AND requires_acceptance = ?",
			email,
			now,
			true,
		)

	var total int64
	if includeTotalCount {
		if err := baseQuery.Count(&total).Error; err != nil {
			return nil, nil, databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "count_pending_invitations_by_email"),
				zap.String(appLogger.KeyEmail, email))
		}
	}

	var invitations []*common_persistence_models.OrganizationInvitation
	offset := common_util.PaginationOffset(pagination.Page, pagination.Limit)
	dataQuery := withLoadedActiveInvitationOrganization(db).
		Where(
			"email = ? AND accepted_at IS NULL AND declined_at IS NULL AND expires_at > ? AND requires_acceptance = ?",
			email,
			now,
			true,
		).
		Order(common_persistence_models.TableNameOrganizationInvitations + ".created_at DESC, " +
			common_persistence_models.TableNameOrganizationInvitations + ".id DESC").
		Offset(offset).
		Limit(common_util.PaginationQueryLimit(pagination.Limit, includeTotalCount))
	if err := dataQuery.Find(&invitations).Error; err != nil {
		return nil, nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "list_pending_invitation_page_by_email"),
			zap.String(appLogger.KeyEmail, email))
	}
	invitations, hasNextPage := common_util.TrimPaginationProbe(invitations, pagination.Limit, includeTotalCount)

	if includeTotalCount {
		return invitations, common_util.CreatePaginationResponse(total, pagination.Limit, pagination.Page), nil
	}
	return invitations, common_util.CreatePaginationResponseWithoutTotalCount(pagination.Limit, pagination.Page, hasNextPage), nil
}

// GetPendingAutoAddEntriesByEmail executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetPendingAutoAddEntriesByEmail(ctx context.Context, email string) ([]*common_persistence_models.OrganizationInvitation, error) {
	var invitations []*common_persistence_models.OrganizationInvitation
	db := h.getDBFromContext(ctx)

	email = strings.ToLower(email)

	result := withLoadedActiveInvitationOrganization(db).
		Where("email = ? AND accepted_at IS NULL AND declined_at IS NULL AND expires_at > ? AND requires_acceptance = ?", email, time.Now(), false).
		Order(common_persistence_models.TableNameOrganizationInvitations + ".created_at DESC").
		Find(&invitations)

	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_pending_auto_add_entries_by_email"),
			zap.String(appLogger.KeyEmail, email))
	}

	return invitations, nil
}

// MarkOrganizationInvitationAccepted atomically marks a pending invitation as accepted.
// The conditional UPDATE guards against a concurrent accept/decline overwriting the outcome.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - id: Invitation ID to mark accepted.
//
// Returns:
//   - result: ErrInvitationNotFound when no invitation exists, ErrInvitationAlreadyProcessed
//     when another request already decided the outcome; nil on success.
func (h *CommonPersistenceHandler[U]) MarkOrganizationInvitationAccepted(ctx context.Context, id uint64) error {
	return h.markOrganizationInvitationProcessed(ctx, id, "accepted_at")
}

// MarkOrganizationInvitationDeclined atomically marks a pending invitation as declined.
// The conditional UPDATE guards against a concurrent accept/decline overwriting the outcome.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - id: Invitation ID to mark declined.
//
// Returns:
//   - result: ErrInvitationNotFound when no invitation exists, ErrInvitationAlreadyProcessed
//     when another request already decided the outcome; nil on success.
func (h *CommonPersistenceHandler[U]) MarkOrganizationInvitationDeclined(ctx context.Context, id uint64) error {
	return h.markOrganizationInvitationProcessed(ctx, id, "declined_at")
}

// markOrganizationInvitationProcessed performs the guarded conditional UPDATE shared by the
// accept and decline paths, distinguishing a missing invitation from an already-processed one.
func (h *CommonPersistenceHandler[U]) markOrganizationInvitationProcessed(ctx context.Context, id uint64, column string) error {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.OrganizationInvitation{}).
		Where("id = ? AND accepted_at IS NULL AND declined_at IS NULL", id).
		Update(column, time.Now())
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "mark_organization_invitation_processed"),
			zap.String(appLogger.KeyNewStatus, column),
			zap.Uint64(appLogger.KeyInvitationID, id))
	}
	if result.RowsAffected == 0 {
		var count int64
		checkResult := db.Model(&common_persistence_models.OrganizationInvitation{}).
			Where("id = ?", id).
			Count(&count)
		if checkResult.Error != nil {
			return databaseErrorWithFields(checkResult.Error,
				zap.String(logkeys.Operation, "recheck_organization_invitation_state"),
				zap.Uint64(appLogger.KeyInvitationID, id))
		}
		if count == 0 {
			return ErrInvitationNotFound
		}
		return ErrInvitationAlreadyProcessed
	}
	return nil
}

// DeleteExpiredInvitations executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) DeleteExpiredInvitations(ctx context.Context) error {
	db := h.getDBFromContext(ctx)

	// Delete invitations that have expired and weren't processed
	result := db.Where("expires_at < ? AND accepted_at IS NULL AND declined_at IS NULL", time.Now()).
		Delete(&common_persistence_models.OrganizationInvitation{})

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "delete_expired_invitations"))
	}

	if result.RowsAffected > 0 {
		appLogger.For(ctx, h.logger, loggerName).Info("Deleted expired invitations",
			zap.String(logkeys.Operation, "delete_expired_invitations"),
			zap.Int64(appLogger.KeyCount, result.RowsAffected))
	} else if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("No expired invitations to delete",
			zap.String(logkeys.Operation, "delete_expired_invitations"),
			zap.Int64(appLogger.KeyCount, 0))
	}

	return nil
}

// DeleteOrganizationInvitation deletes a still-pending invitation by ID. The pending guard
// prevents cancelling an invitation that a concurrent request already accepted or declined.
//
// Returns:
//   - result: ErrInvitationNotFound when no invitation exists, ErrInvitationAlreadyProcessed
//     when the invitation was already accepted or declined; nil on success.
func (h *CommonPersistenceHandler[U]) DeleteOrganizationInvitation(ctx context.Context, id uint64) error {
	db := h.getDBFromContext(ctx)

	result := db.Where("id = ? AND accepted_at IS NULL AND declined_at IS NULL", id).
		Delete(&common_persistence_models.OrganizationInvitation{})
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "delete_organization_invitation"),
			zap.Uint64(appLogger.KeyInvitationID, id))
	}
	if result.RowsAffected == 0 {
		var count int64
		checkResult := db.Model(&common_persistence_models.OrganizationInvitation{}).
			Where("id = ?", id).
			Count(&count)
		if checkResult.Error != nil {
			return databaseErrorWithFields(checkResult.Error,
				zap.String(logkeys.Operation, "recheck_organization_invitation_state"),
				zap.Uint64(appLogger.KeyInvitationID, id))
		}
		if count == 0 {
			return ErrInvitationNotFound
		}
		return ErrInvitationAlreadyProcessed
	}
	return nil
}

// GetPendingInvitationsByOrganization executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetPendingInvitationsByOrganization(ctx context.Context, organizationID int64, limit, offset int) ([]*common_persistence_models.OrganizationInvitation, int64, error) {
	var invitations []*common_persistence_models.OrganizationInvitation
	var total int64
	db := h.getDBFromContext(ctx)

	// Get total count of pending invitations
	countResult := withActiveInvitationOrganization(db.Model(&common_persistence_models.OrganizationInvitation{})).
		Where("organization_id = ? AND accepted_at IS NULL AND declined_at IS NULL AND expires_at > ? AND requires_acceptance = ?",
			organizationID, time.Now(), true).
		Count(&total)

	if countResult.Error != nil {
		return nil, 0, databaseErrorWithFields(countResult.Error,
			zap.String(logkeys.Operation, "count_pending_invitations_for_organization"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}

	// Get paginated invitations
	result := withActiveInvitationOrganization(db).
		Preload(common_persistence_models.PreloadInvitedByUser).
		Where("organization_id = ? AND accepted_at IS NULL AND declined_at IS NULL AND expires_at > ? AND requires_acceptance = ?",
			organizationID, time.Now(), true).
		Order(common_persistence_models.TableNameOrganizationInvitations + ".created_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&invitations)

	if result.Error != nil {
		return nil, 0, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_pending_invitations_for_organization"),
			zap.Int64(appLogger.KeyOrganizationID, organizationID))
	}

	return invitations, total, nil
}
