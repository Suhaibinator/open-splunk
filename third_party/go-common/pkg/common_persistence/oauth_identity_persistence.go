package common_persistence

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// FindOAuthIdentity returns the identity linked to (providerSlug, subject),
// or ErrOAuthIdentityNotFound.
func (h *CommonPersistenceHandler[U]) FindOAuthIdentity(ctx context.Context, providerSlug, subject string) (*common_persistence_models.OAuthLogin, error) {
	var login common_persistence_models.OAuthLogin
	err := h.DB(ctx).Where("provider_slug = ? AND subject = ?", providerSlug, subject).Take(&login).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrOAuthIdentityNotFound
	}
	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "find_oauth_identity"),
			zap.String(appLogger.KeyProvider, providerSlug))
	}
	return &login, nil
}

// FindUserOAuthIdentity locks and returns userID's identity for providerSlug,
// or ErrOAuthIdentityNotFound. Callers hold the user's row lock first.
func (h *CommonPersistenceHandler[U]) FindUserOAuthIdentity(ctx context.Context, userID uint64, providerSlug string) (*common_persistence_models.OAuthLogin, error) {
	var login common_persistence_models.OAuthLogin
	err := h.DB(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ? AND provider_slug = ?", userID, providerSlug).Take(&login).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrOAuthIdentityNotFound
	}
	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "find_user_oauth_identity"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyProvider, providerSlug))
	}
	return &login, nil
}

// LinkOAuthIdentity links (providerSlug, subject) to userID. It returns
// ErrOAuthIdentityConflict when the user already has an identity for the
// provider or the provider account is linked to another user.
func (h *CommonPersistenceHandler[U]) LinkOAuthIdentity(ctx context.Context, userID uint64, providerSlug, subject, email string) error {
	login := &common_persistence_models.OAuthLogin{
		UserID:       userID,
		ProviderSlug: providerSlug,
		Subject:      &subject,
		Email:        email,
		LoginCount:   1,
	}
	if err := h.DB(ctx).Create(login).Error; err != nil {
		if isUniqueViolation(err) {
			return appLogger.WithErrorFields(errors.Join(ErrOAuthIdentityConflict, err),
				zap.String(logkeys.Operation, "link_oauth_identity"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyProvider, providerSlug))
		}
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "link_oauth_identity"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyProvider, providerSlug))
	}
	return nil
}

// TouchOAuthIdentity records a login through an existing identity: it bumps
// the login count and time, stores the asserted email, and fills a legacy
// NULL subject. Filling a subject another user already holds returns
// ErrOAuthIdentityConflict.
func (h *CommonPersistenceHandler[U]) TouchOAuthIdentity(ctx context.Context, identityID uint64, subject, email string) error {
	result := h.DB(ctx).Model(&common_persistence_models.OAuthLogin{}).
		Where("id = ?", identityID).
		Updates(map[string]any{
			"subject":     gorm.Expr("COALESCE(subject, ?)", subject),
			"email":       email,
			"last_login":  time.Now().UTC(),
			"login_count": gorm.Expr("login_count + 1"),
		})
	if result.Error != nil {
		if isUniqueViolation(result.Error) {
			return appLogger.WithErrorFields(errors.Join(ErrOAuthIdentityConflict, result.Error),
				zap.String(logkeys.Operation, "touch_oauth_identity"),
				zap.Uint64(appLogger.KeyOAuthIdentityID, identityID))
		}
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "touch_oauth_identity"),
			zap.Uint64(appLogger.KeyOAuthIdentityID, identityID))
	}
	if result.RowsAffected == 0 {
		return ErrOAuthIdentityNotFound
	}
	return nil
}
