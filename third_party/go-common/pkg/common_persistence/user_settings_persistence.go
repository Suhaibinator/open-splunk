package common_persistence

import (
	"context"
	"errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ============================================================================
// USER SETTINGS METHODS
// ============================================================================

// CreateUserSettings executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) CreateUserSettings(ctx context.Context, settings *common_persistence_models.UserSettings) error {
	db := h.getDBFromContext(ctx)
	// Read-created defaults must never overwrite a concurrent preference update.
	result := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "user_id"}}, DoNothing: true}).Create(settings)
	if result.Error != nil {
		fields := []zap.Field{
			zap.String(logkeys.Operation, "create_user_settings"),
		}
		if settings != nil {
			fields = append(fields, zap.Uint64(appLogger.KeyUserID, settings.UserID))
		}
		return databaseErrorWithFields(result.Error, fields...)
	}
	return nil
}

// GetUserSettingsByUserID executes this method as part of the persistence flow for transactional database operations and repository access.
func (h *CommonPersistenceHandler[U]) GetUserSettingsByUserID(ctx context.Context, userID uint64) (*common_persistence_models.UserSettings, error) {
	var settings common_persistence_models.UserSettings
	// Use transaction from context if available
	db := h.getDBFromContext(ctx)
	result := db.Where("user_id = ?", userID).First(&settings)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_user_settings_by_user_id"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	return &settings, nil
}

// UpsertUserSettings writes settings in one statement: a user without a row gets
// all of settings, and an existing row changes only columns, so concurrent
// partial updates never overwrite each other. settings receives the stored row.
// A missing or deleted user returns ErrUserNotFound; the key-share lock makes a
// concurrent deletion either wait for this write or win and leave no row.
func (h *CommonPersistenceHandler[U]) UpsertUserSettings(ctx context.Context, settings *common_persistence_models.UserSettings, columns []string) error {
	if len(columns) == 0 {
		// An empty patch still returns the row; DO UPDATE needs one assignment.
		columns = []string{"user_id"}
	}
	// Raw insert: GORM's Create replaces an explicit false with a true column default.
	result := h.getDBFromContext(ctx).Raw(`INSERT INTO ? (user_id, dark_mode, email_notifications, push_notifications,
		marketing_emails, language, chat_email_notifications, chat_push_notifications)
		SELECT id, ?, ?, ?, ?, ?, ?, ? FROM ? WHERE id = ? AND deleted_at IS NULL FOR KEY SHARE
		? RETURNING *`,
		clause.Table{Name: common_persistence_models.TableNameUserSettings},
		settings.DarkMode, settings.EmailNotifications, settings.PushNotifications,
		settings.MarketingEmails, settings.Language, settings.ChatEmailNotifications, settings.ChatPushNotifications,
		clause.Table{Name: common_persistence_models.TableNameUsers}, settings.UserID,
		clause.OnConflict{Columns: []clause.Column{{Name: "user_id"}}, DoUpdates: clause.AssignmentColumns(columns)},
	).Scan(settings)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "upsert_user_settings"),
			zap.Uint64(appLogger.KeyUserID, settings.UserID))
	}
	if result.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}
