package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ============================================================================
// EMAIL VERIFICATION METHODS
// ============================================================================

// CreateEmailVerification executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) CreateEmailVerification(ctx context.Context, ev *common_persistence_models.EmailVerification) error {
	if ev != nil {
		ev.Email = common_persistence_models.NormalizeUserEmail(ev.Email)
	}
	db := h.getDBFromContext(ctx)
	result := db.Create(ev)
	if result.Error != nil {
		fields := []zap.Field{
			zap.Error(result.Error),
			zap.String(logkeys.Operation, "create_email_verification"),
		}
		if ev != nil {
			fields = append(fields,
				zap.Uint64(appLogger.KeyUserID, ev.UserID),
				zap.String(appLogger.KeyEmail, ev.Email))
		}
		return appLogger.WithErrorFields(ErrDatabaseError, fields...)
	}
	return nil
}

// GetEmailVerificationByUserID executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetEmailVerificationByUserID(ctx context.Context, userID uint64) (*common_persistence_models.EmailVerification, error) {
	var ev common_persistence_models.EmailVerification
	db := h.getDBFromContext(ctx)

	result := db.Where("user_id = ?", userID).First(&ev)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrEmailVerificationNotFound
		}
		return nil, appLogger.WithErrorCause(ErrDatabaseError, result.Error,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "get_email_verification"))
	}
	return &ev, nil
}

// MarkEmailVerified executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) MarkEmailVerified(ctx context.Context, userID uint64) (string, error) {
	db := h.getDBFromContext(ctx)
	var user common_persistence_models.User
	if err := db.Select("id", "email").Where("id = ?", userID).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", ErrUserNotFound
		}
		return "", appLogger.WithErrorCause(ErrDatabaseError, err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "load_user_for_email_verification"))
	}

	now := time.Now().UTC()
	email := common_persistence_models.NormalizeUserEmail(user.Email)
	ev := &common_persistence_models.EmailVerification{
		UserID:     userID,
		Email:      email,
		VerifiedAt: &now,
	}

	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"email", "verified_at"}),
	}).Create(ev)

	if result.Error != nil {
		return email, appLogger.WithErrorCause(ErrDatabaseError, result.Error,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEmail, email),
			zap.String(logkeys.Operation, "mark_email_verified"))
	}
	return email, nil
}

// DiscardedSignupCredentials reports what DiscardUnverifiedSignupCredentials removed.
type DiscardedSignupCredentials struct {
	// Discarded is true when the account was an unverified self-signup and its
	// credentials were cleared; false leaves the account untouched.
	Discarded       bool
	PasswordRemoved bool
	// RevokedAuthTokens are the authentication tokens whose caches the caller
	// must invalidate after commit. Security tokens are revoked but not returned.
	RevokedAuthTokens []string
}

// DiscardUnverifiedSignupCredentials clears credentials that nobody has proven
// ownership of before another flow verifies the account's email. It locks the
// user row, and only when the account is a self-signup (not admin-created)
// whose verification record has never been verified does it soft-delete the
// password row (with its TOTP state) and the email auth-method row, then revoke
// every outstanding token. Accounts without a verification record, and accounts
// verified at any earlier point, are left untouched.
//
// Both tables' unique indexes cover live rows only, so the discarded rows do not
// block the owner from adding a password later, and every credential read
// excludes soft-deleted rows.
func (h *CommonPersistenceHandler[U]) DiscardUnverifiedSignupCredentials(ctx context.Context, userID uint64) (DiscardedSignupCredentials, error) {
	var result DiscardedSignupCredentials
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		var user common_persistence_models.User
		if err := tx.Select("id", "admin_created").Clauses(clause.Locking{Strength: "UPDATE"}).First(&user, userID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUserNotFound
			}
			return err
		}
		if user.AdminCreated {
			return nil
		}
		var ev common_persistence_models.EmailVerification
		if err := tx.Select("verified_at").Where("user_id = ?", userID).First(&ev).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if ev.VerifiedAt != nil {
			return nil
		}
		result.Discarded = true

		deleted := tx.Where("user_id = ?", userID).Delete(&common_persistence_models.UserPassword{})
		if deleted.Error != nil {
			return deleted.Error
		}
		result.PasswordRemoved = deleted.RowsAffected > 0
		if err := tx.Where("user_id = ? AND method_type = ?", userID, proto_models.AuthProvider_AUTH_PROVIDER_EMAIL).
			Delete(&common_persistence_models.UserAuthMethod{}).Error; err != nil {
			return err
		}

		// Force semantics, as in RevokeAllSessionTokensForUserAndReturnTokens:
		// future-dated refresh revocations are pulled forward to now.
		now := time.Now().UTC()
		authTokens := []string{}
		if err := tx.Raw(
			fmt.Sprintf("UPDATE %s SET revoked_at = ? WHERE user_id = ? AND token_type = ? AND (revoked_at IS NULL OR revoked_at > ?) RETURNING token",
				common_persistence_models.TableNameSessionTokens),
			now, userID, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, now,
		).Scan(&authTokens).Error; err != nil {
			return err
		}
		result.RevokedAuthTokens = authTokens
		return tx.Model(&common_persistence_models.SessionToken{}).
			Where("user_id = ? AND token_type <> ? AND (revoked_at IS NULL OR revoked_at > ?)", userID, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, now).
			Update("revoked_at", now).Error
	})
	if errors.Is(err, ErrUserNotFound) {
		return DiscardedSignupCredentials{}, ErrUserNotFound
	}
	if err != nil {
		return DiscardedSignupCredentials{}, appLogger.WithErrorCause(ErrDatabaseError, err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "discard_unverified_signup_credentials"))
	}
	return result, nil
}

// ListUnverifiedUserIDs executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) ListUnverifiedUserIDs(ctx context.Context, cutoff time.Time, limit int) ([]uint64, error) {
	db := h.getDBFromContext(ctx)
	usersTable := common_persistence_models.TableNameUsers
	emailVerificationTable := common_persistence_models.TableNameEmailVerifications
	tokensTable := common_persistence_models.TableNameSessionTokens

	query := db.Model(&common_persistence_models.User{}).
		Select(fmt.Sprintf("%s.id", usersTable)).
		Where(fmt.Sprintf("%s.admin_created = ?", usersTable), false).
		Joins(
			fmt.Sprintf("JOIN %s ON %s.user_id = %s.id", emailVerificationTable, emailVerificationTable, usersTable),
		).
		Where(fmt.Sprintf("%s.verified_at IS NULL", emailVerificationTable)).
		Where(fmt.Sprintf("%s.created_at < ?", emailVerificationTable), cutoff).
		// Resending a verification email issues a fresh token without touching
		// the verification record, so the record's age alone would delete the
		// account while an emailed link is still live. Keep the user as long as
		// any usable verification token is outstanding.
		Where(
			fmt.Sprintf(
				"NOT EXISTS (SELECT 1 FROM %s WHERE %s.user_id = %s.id AND %s.token_type = ? AND %s.expires_at > ? AND %s.revoked_at IS NULL AND %s.used_at IS NULL)",
				tokensTable, tokensTable, usersTable, tokensTable, tokensTable, tokensTable, tokensTable,
			),
			proto_models.TokenType_TOKEN_TYPE_EMAIL_VERIFICATION, time.Now().UTC(),
		).
		Order(fmt.Sprintf("%s.created_at ASC", emailVerificationTable))

	if limit > 0 {
		query = query.Limit(limit)
	}

	var userIDs []uint64
	if err := query.Pluck(fmt.Sprintf("%s.id", usersTable), &userIDs).Error; err != nil {
		return nil, appLogger.WithErrorCause(ErrDatabaseError, err,
			zap.Time(appLogger.KeyCutoffTime, cutoff),
			zap.Int(logkeys.Limit, limit),
			zap.String(logkeys.Operation, "list_unverified_user_ids"))
	}

	return userIDs, nil
}
