package common_persistence

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// databaseErrorWithFields preserves both the stable persistence sentinel and
// the driver/database cause while carrying diagnostic context to the owning
// logging boundary.
func databaseErrorWithFields(cause error, fields ...zap.Field) error {
	return appLogger.WithErrorFields(errors.Join(ErrDatabaseError, cause), fields...)
}

// requirePageLimit enforces the list contract: the request sanitizer defaults
// and bounds every page size from configuration, and persistence fetches that
// limit as given. A missing pagination request or a zero limit means a caller
// skipped the sanitizer, which is an invariant violation rather than something
// to paper over with a page size of our own.
func requirePageLimit(operation string, pagination *proto_models.PaginationRequest) error {
	if pagination.GetLimit() > 0 {
		return nil
	}
	return appLogger.InvariantError("pagination_limit_positive", ErrDatabaseError,
		zap.String(logkeys.Operation, operation),
		zap.Bool("pagination_present", pagination != nil),
		zap.Uint32(logkeys.Limit, pagination.GetLimit()))
}

// ============================================================================
// USER CRUD METHODS
// ============================================================================

// CreateUser executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateUser(ctx context.Context, user *common_persistence_models.User) error {
	// Persistence-layer guarantee: users.email is always stored normalized
	// (lowercase, trimmed), regardless of what upstream flows pass in. The
	// OAuth path in particular forwards provider emails verbatim.
	user.Email = common_persistence_models.NormalizeUserEmail(user.Email)
	db := h.getDBFromContext(ctx)
	result := db.Create(user)
	if result.Error != nil {
		if isUniqueViolation(result.Error) {
			// The availability check is advisory; concurrent creates can still
			// legitimately lose the unique-index race.
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("Unique constraint violation during user creation",
					zap.String(logkeys.Operation, "create_user"),
					zap.String(appLogger.KeyUsername, user.Username),
					zap.String(appLogger.KeyEmail, user.Email),
					zap.Error(result.Error))
			}
			return ErrUserAlreadyExists
		}

		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "create_user"),
			zap.String(appLogger.KeyUsername, user.Username),
			zap.String(appLogger.KeyEmail, user.Email))
	}
	return nil
}

// GetUserByID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetUserByID(ctx context.Context, id uint64) (*common_persistence_models.User, error) {
	startTime := time.Now().UTC()
	var user common_persistence_models.User
	// Use transaction from context if available
	db := h.getDBFromContext(ctx)
	// Preload ProfilePictureAttachment to include attachment data
	result := db.Preload(common_persistence_models.PreloadProfilePictureAttachment).First(&user, id)

	// Record metrics if metrics handler is available
	queryDuration := time.Since(startTime)
	if h.metricsHandler != nil {
		h.metricsHandler.RecordDBQuery("get_user_by_id", queryDuration, result.Error == nil)
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("DB query metrics recorded",
				zap.String(logkeys.Operation, "get_user_by_id"),
				appLogger.DurationMS(logkeys.DurationMS, queryDuration),
				zap.Bool(appLogger.KeySuccess, result.Error == nil))
		}
	}

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_user_by_id"),
			zap.Uint64(appLogger.KeyUserID, id))
	}

	return &user, nil
}

// GetUserByEmail executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetUserByEmail(ctx context.Context, email string) (*common_persistence_models.User, error) {
	// Stored emails are normalized on every write (and backfilled by
	// migration), so normalizing the lookup keeps the comparison exact and
	// index-friendly without a LOWER() on the column.
	email = common_persistence_models.NormalizeUserEmail(email)
	var user common_persistence_models.User
	// Use transaction from context if available
	db := h.getDBFromContext(ctx)
	// Preload ProfilePictureAttachment to include attachment data
	result := db.Preload(common_persistence_models.PreloadProfilePictureAttachment).Where("email = ?", email).First(&user)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_user_by_email"),
			zap.String(appLogger.KeyEmail, email))
	}

	return &user, nil
}

// CheckUsernameExists executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CheckUsernameExists(ctx context.Context, username string) (bool, error) {
	var count int64
	db := h.getDBFromContext(ctx)

	// Perform case-insensitive username check
	result := db.Model(&common_persistence_models.User{}).
		Where("LOWER(username) = LOWER(?)", username).
		Count(&count)

	if result.Error != nil {
		return false, appLogger.WithErrorFields(
			errors.Join(ErrUsernameAvailabilityCheckFailed, ErrDatabaseError, result.Error),
			zap.String(logkeys.Operation, "check_username_exists"),
			zap.String(appLogger.KeyUsername, username))
	}

	return count > 0, nil
}

// UpdateUser executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateUser(ctx context.Context, user *common_persistence_models.User) error {
	// Keep the normalized-email storage guarantee on update as well.
	user.Email = common_persistence_models.NormalizeUserEmail(user.Email)
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Lock the same row as invitation acceptance/resend so changing an email
		// cannot race a credential setup for the previous address.
		var current common_persistence_models.User
		if err := tx.Select("id", "email").Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, user.ID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUserNotFound
			}
			return err
		}

		// Select includes nil values so nullable fields can be cleared.
		result := tx.Model(&common_persistence_models.User{}).Where("id = ?", user.ID).
			Select("username", "email", "phone_number", "first_name", "last_name", "user_type", "banned", "bio", "profile_picture_attachment_id", "updated_at").
			Updates(user)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrUserNotFound
		}
		if common_persistence_models.NormalizeUserEmail(current.Email) != user.Email {
			// Persist revocation: changing the address back must not revive a link.
			now := time.Now().UTC()
			return tx.Model(&common_persistence_models.SessionToken{}).
				Where("user_id = ? AND token_type IN ? AND (revoked_at IS NULL OR revoked_at > ?)", user.ID, []proto_models.TokenType{proto_models.TokenType_TOKEN_TYPE_ACCOUNT_INVITATION, proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION}, now).
				Update("revoked_at", now).Error
		}
		return nil
	})
	if errors.Is(err, ErrUserNotFound) {
		return ErrUserNotFound
	}
	if err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "update_user"),
			zap.Uint64(appLogger.KeyUserID, user.ID),
			zap.String(appLogger.KeyUsername, user.Username),
			zap.String(appLogger.KeyEmail, user.Email))
	}

	// Invalidate user cache after successful update
	h.invalidateUserCache(ctx, user.ID)

	return nil
}

// UpdateUsernameWithAudit executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateUsernameWithAudit(
	ctx context.Context,
	targetUserID uint64,
	newUsername string,
	actorUserID *uint64,
	reason string,
) error {
	db := h.getDBFromContext(ctx)

	var (
		oldUsername string
		changed     bool
	)

	err := db.Transaction(func(tx *gorm.DB) error {
		var user common_persistence_models.User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", targetUserID).First(&user).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUserNotFound
			}
			return databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "load_user_for_username_update"),
				zap.Uint64(appLogger.KeyTargetUserID, targetUserID),
				zap.String(appLogger.KeyNewUsername, newUsername))
		}

		oldUsername = user.Username
		if strings.EqualFold(oldUsername, newUsername) {
			return nil
		}

		changed = true
		updateResult := tx.Model(&common_persistence_models.User{}).
			Where("id = ?", targetUserID).
			Update("username", newUsername)
		if updateResult.Error != nil {
			if isUniqueViolation(updateResult.Error) {
				return ErrUserAlreadyExists
			}
			return databaseErrorWithFields(updateResult.Error,
				zap.String(logkeys.Operation, "update_username_with_audit"),
				zap.Uint64(appLogger.KeyTargetUserID, targetUserID),
				zap.String(appLogger.KeyNewUsername, newUsername))
		}
		if updateResult.RowsAffected == 0 {
			return ErrUserNotFound
		}

		auditRecord := &common_persistence_models.UsernameChangeHistory{
			TargetUserID: targetUserID,
			OldUsername:  oldUsername,
			NewUsername:  newUsername,
			ActorUserID:  actorUserID,
			Reason:       strings.TrimSpace(reason),
		}

		if err := tx.Create(auditRecord).Error; err != nil {
			return databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "write_username_change_audit_record"),
				zap.Uint64(appLogger.KeyTargetUserID, targetUserID),
				zap.Uint64p(appLogger.KeyActorUserID, actorUserID),
				zap.String(appLogger.KeyOldUsername, oldUsername),
				zap.String(appLogger.KeyNewUsername, newUsername))
		}

		return nil
	})
	if err != nil {
		return err
	}

	if !changed {
		return nil
	}

	h.invalidateUserCache(ctx, targetUserID)
	appLogger.For(ctx, h.logger, loggerName).Info("Username updated with audit trail",
		zap.String(logkeys.Operation, "update_username_with_audit"),
		zap.Uint64(appLogger.KeyTargetUserID, targetUserID),
		zap.Uint64p(appLogger.KeyActorUserID, actorUserID),
		zap.String(appLogger.KeyOldUsername, oldUsername),
		zap.String(appLogger.KeyNewUsername, newUsername),
		zap.String(logkeys.Reason, strings.TrimSpace(reason)))

	return nil
}

// UpdateUserProfilePicture executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateUserProfilePicture(ctx context.Context, userID uint64, attachmentID *uint64) error {
	db := h.getDBFromContext(ctx)

	// Get the user first to check ownership
	var user common_persistence_models.User
	if err := db.First(&user, userID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrUserNotFound
		}
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_user_for_profile_picture_update"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint64p(appLogger.KeyAttachmentID, attachmentID))
	}

	// If setting a new attachment, verify it exists and belongs to the user
	if attachmentID != nil {
		var attachment common_persistence_models.Attachment
		if err := db.First(&attachment, *attachmentID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if appLogger.DebugEnabled(h.logger) {
					appLogger.For(ctx, h.logger, loggerName).Debug("Attachment not found for profile picture update",
						zap.String(logkeys.Operation, "verify_profile_picture_attachment"),
						zap.Uint64(appLogger.KeyUserID, userID),
						zap.Uint64(appLogger.KeyAttachmentID, *attachmentID))
				}
				return ErrAttachmentNotFound
			}
			return databaseErrorWithFields(err,
				zap.String(logkeys.Operation, "verify_profile_picture_attachment"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Uint64(appLogger.KeyAttachmentID, *attachmentID))
		}

		// Verify attachment ownership
		if attachment.UserID != userID {
			appLogger.For(ctx, h.logger, loggerName).Warn("User attempted to use an attachment they do not own as profile picture",
				zap.String(logkeys.Operation, "verify_profile_picture_attachment_ownership"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.Uint64(appLogger.KeyAttachmentID, *attachmentID),
				zap.Uint64(appLogger.KeyAttachmentUserID, attachment.UserID))
			return ErrDatabaseError // Security: user doesn't own this attachment
		}
	}

	// Update the user's profile picture attachment ID
	result := db.Model(&user).Update("profile_picture_attachment_id", attachmentID)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_user_profile_picture"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint64p(appLogger.KeyAttachmentID, attachmentID))
	}
	if result.RowsAffected == 0 {
		return appLogger.InvariantError("profile_picture_target_persists_through_update", nil,
			zap.String(logkeys.Stage, "update_user_profile_picture"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint64p(appLogger.KeyAttachmentID, attachmentID),
			zap.String(logkeys.Expected, "one updated user row"),
			zap.Int64(logkeys.Actual, result.RowsAffected))
	}

	// Invalidate user cache after successful update
	h.invalidateUserCache(ctx, userID)

	return nil
}

// ============================================================================
// USER AUTHENTICATION METHODS
// ============================================================================

// GetUserAuthMethods executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetUserAuthMethods(ctx context.Context, userID uint64) (*common_persistence_models.UserAuthMethods, error) {
	db := h.getDBFromContext(ctx)

	methods := &common_persistence_models.UserAuthMethods{
		UserID:         userID,
		HasPassword:    false,
		OAuthProviders: []common_persistence_models.OAuthLogin{},
		OTPEnabled:     false,
		OTPVerifiedAt:  nil,
	}

	// Check for password auth using new method
	userPassword, err := h.GetUserPassword(ctx, userID)
	if err != nil {
		return nil, err
	}
	methods.HasPassword = userPassword != nil
	if userPassword != nil {
		methods.OTPEnabled = userPassword.OTPEnabled
		methods.OTPVerifiedAt = userPassword.OTPVerifiedAt
	}

	// Get OAuth providers
	var oauthLogins []common_persistence_models.OAuthLogin
	err = db.Where("user_id = ?", userID).
		Order("last_login DESC").
		Find(&oauthLogins).Error

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_user_oauth_providers"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	methods.OAuthProviders = oauthLogins
	return methods, nil
}

// ============================================================================
// PASSWORD AUTHENTICATION METHODS
// ============================================================================

// CreateUserPassword executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateUserPassword(ctx context.Context, userID uint64, passwordHash, passwordSalt string) error {
	db := h.getDBFromContext(ctx)

	userPassword := &common_persistence_models.UserPassword{
		UserID:       userID,
		PasswordHash: passwordHash,
		PasswordSalt: passwordSalt,
	}

	if err := db.Create(userPassword).Error; err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "create_user_password"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	// Also create auth method record
	authMethod := newUserAuthMethodRecord(userID, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL)

	if err := db.Create(authMethod).Error; err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("failed to create password auth method after password creation",
			zap.String(logkeys.Operation, "create_password_auth_method"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Stringer(appLogger.KeyAuthMethod, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL),
			zap.String(logkeys.Fallback, "retain_password_without_auth_method_tracking"),
			zap.Error(err))
		// Don't fail - password is created, method tracking is secondary
	}

	return nil
}

// GetUserPassword executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetUserPassword(ctx context.Context, userID uint64) (*common_persistence_models.UserPassword, error) {
	db := h.getDBFromContext(ctx)

	var userPassword common_persistence_models.UserPassword
	err := db.Where("user_id = ?", userID).First(&userPassword).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil // No password set
		}
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "get_user_password"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	return &userPassword, nil
}

// UpdateUserPassword executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateUserPassword(ctx context.Context, userID uint64, passwordHash, passwordSalt string) error {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.UserPassword{}).
		Where("user_id = ?", userID).
		Select("password_hash", "password_salt", "updated_at").
		Updates(struct {
			PasswordHash string
			PasswordSalt string
			UpdatedAt    time.Time `gorm:"autoUpdateTime:false"`
		}{PasswordHash: passwordHash, PasswordSalt: passwordSalt, UpdatedAt: time.Now().UTC()})

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_user_password"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Password update matched no user password row",
				zap.String(logkeys.Operation, "update_user_password"),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return ErrUserNotFound
	}

	return nil
}

// ============================================================================
// OTP AUTHENTICATION METHODS
// ============================================================================

// UpdateUserOTPSecret executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateUserOTPSecret(ctx context.Context, userID uint64, otpSecret string) error {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.UserPassword{}).
		Where("user_id = ?", userID).
		Update("otp_secret", otpSecret)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_user_otp_secret"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("no UserPassword record found for user",
				zap.String(logkeys.Operation, "update_user_otp_secret"),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return ErrUserNotFound
	}

	return nil
}

// EnableUserOTP executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) EnableUserOTP(ctx context.Context, userID uint64, verifiedAt *time.Time) error {
	db := h.getDBFromContext(ctx)

	updates := struct {
		OTPEnabled    bool
		OTPVerifiedAt *time.Time
		UpdatedAt     time.Time `gorm:"autoUpdateTime:false"`
	}{
		OTPEnabled: true,
		UpdatedAt:  time.Now().UTC(),
	}
	columns := []string{"otp_enabled", "updated_at"}
	if verifiedAt != nil {
		updates.OTPVerifiedAt = verifiedAt
		columns = append(columns, "otp_verified_at")
	}

	result := db.Model(&common_persistence_models.UserPassword{}).
		Where("user_id = ?", userID).
		Select(columns).
		Updates(updates)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "enable_user_otp"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("no UserPassword record found for user",
				zap.String(logkeys.Operation, "enable_user_otp"),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return ErrUserNotFound
	}

	return nil
}

// DisableUserOTP executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) DisableUserOTP(ctx context.Context, userID uint64) error {
	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.UserPassword{}).
		Where("user_id = ?", userID).
		Select("otp_enabled", "otp_secret", "otp_verified_at", "updated_at").
		Updates(struct {
			OTPEnabled    bool
			OTPSecret     *string
			OTPVerifiedAt *time.Time
			UpdatedAt     time.Time `gorm:"autoUpdateTime:false"`
		}{UpdatedAt: time.Now().UTC()})

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "disable_user_otp"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("no UserPassword record found for user",
				zap.String(logkeys.Operation, "disable_user_otp"),
				zap.Uint64(appLogger.KeyUserID, userID))
		}
		return ErrUserNotFound
	}

	return nil
}

// ============================================================================
// AUTH METHOD TRACKING
// ============================================================================

func newUserAuthMethodRecord(userID uint64, provider proto_models.AuthProvider) *common_persistence_models.UserAuthMethod {
	return &common_persistence_models.UserAuthMethod{
		UserID:     userID,
		MethodType: provider,
		Enabled:    true,
	}
}

// ============================================================================
// USER DELETION UTILITY METHODS
// ============================================================================
