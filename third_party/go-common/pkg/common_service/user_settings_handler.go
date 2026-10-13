package common_service

import (
	"context"
	stdErrors "errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// UserSettingsHandlerInterface defines the interface for user settings operations.
// It provides methods for retrieving, updating, and creating user preferences
// such as dark mode, notifications, and language settings.
type UserSettingsHandlerInterface interface {
	// GetUserSettings retrieves user settings by user ID.
	// If settings don't exist, default settings are created automatically.
	GetUserSettings(ctx context.Context, userID uint64) (*common_service_models.UserSettings, error)

	// UpdateUserSettings applies a partial update to a user's settings, creating
	// them with defaults for any absent fields if they don't yet exist. Only the
	// fields present (non-nil) in the update are modified.
	UpdateUserSettings(ctx context.Context, userID uint64, update *common_service_models.UserSettingsUpdate) (*common_service_models.UserSettings, error)

	// CreateDefaultSettingsForNewUser creates default settings for a newly created user.
	// This is typically called during user registration.
	CreateDefaultSettingsForNewUser(ctx context.Context, userID uint64) error

	// Stop gracefully shuts down the handler.
	Stop()
}

// UserSettingsHandler defines the common_service package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type UserSettingsHandler struct {
	persistenceHandler persistence.CommonPersistenceInterface
}

// Compile-time check to ensure UserSettingsHandler implements UserSettingsHandlerInterface
// Note: Generic types need to be instantiated for interface checking
var _ UserSettingsHandlerInterface = (*UserSettingsHandler)(nil)

// NewUserSettingsHandler executes part of the common_service package flow and provides shared helper behavior.
func NewUserSettingsHandler(persistenceHandler persistence.CommonPersistenceInterface) *UserSettingsHandler {
	return &UserSettingsHandler{
		persistenceHandler: persistenceHandler,
	}
}

// GetUserSettings executes part of the common_service service flow and coordinates work for the *UserSettingsHandler receiver.
func (h *UserSettingsHandler) GetUserSettings(ctx context.Context, userID uint64) (*common_service_models.UserSettings, error) {
	// Get user settings from persistence layer
	persistenceSettings, err := h.persistenceHandler.GetUserSettingsByUserID(ctx, userID)
	if err != nil {
		if stdErrors.Is(err, apperrors.ErrUserNotFound) || stdErrors.Is(err, persistence.ErrUserNotFound) {
			// If settings don't exist, create default settings
			// createDefaultUserSettings now uses context transaction if available
			return h.createDefaultUserSettings(ctx, userID) // Removed nil parameter
		}
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "get_user_settings"))
	}

	// Convert persistence model to service model
	settings := common_service_models.ConvertPersistenceToUserSettings(persistenceSettings)

	return settings, nil
}

// UpdateUserSettings executes part of the common_service service flow and coordinates work for the *UserSettingsHandler receiver.
//
// Parameters:
// - `ctx`: propagates request-scoped deadlines, cancellation, and trace metadata.
// - `userID`: supplies the `userID` input as `uint64` for this operation.
// - `update`: supplies the partial settings update; only non-nil fields are applied.
func (h *UserSettingsHandler) UpdateUserSettings(ctx context.Context, userID uint64, update *common_service_models.UserSettingsUpdate) (*common_service_models.UserSettings, error) {
	if update == nil {
		update = &common_service_models.UserSettingsUpdate{}
	}

	// One upsert inserts the defaults merged with the update for a user without
	// settings and otherwise writes only the supplied columns, so concurrent
	// partial updates and first writes never overwrite each other's fields.
	settings := defaultPersistenceUserSettings(userID)
	columns := applyUserSettingsUpdate(settings, update)
	if err := h.persistenceHandler.UpsertUserSettings(ctx, settings, columns); err != nil {
		if stdErrors.Is(err, persistence.ErrUserNotFound) {
			return nil, apperrors.ErrUserNotFound
		}
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "upsert_user_settings"))
	}

	// The upsert returns the stored row, so callers need no second read.
	return common_service_models.ConvertPersistenceToUserSettings(settings), nil
}

// applyUserSettingsUpdate copies the present (non-nil) fields of update onto
// the persistence settings, leaving absent fields untouched, and returns the
// columns it set.
func applyUserSettingsUpdate(dst *common_persistence_models.UserSettings, update *common_service_models.UserSettingsUpdate) []string {
	var columns []string
	if update.DarkMode != nil {
		dst.DarkMode = *update.DarkMode
		columns = append(columns, "dark_mode")
	}
	if update.EmailNotifications != nil {
		dst.EmailNotifications = *update.EmailNotifications
		columns = append(columns, "email_notifications")
	}
	if update.PushNotifications != nil {
		dst.PushNotifications = *update.PushNotifications
		columns = append(columns, "push_notifications")
	}
	if update.MarketingEmails != nil {
		dst.MarketingEmails = *update.MarketingEmails
		columns = append(columns, "marketing_emails")
	}
	if update.Language != nil {
		dst.Language = *update.Language
		columns = append(columns, "language")
	}
	if update.ChatEmailNotifications != nil {
		dst.ChatEmailNotifications = *update.ChatEmailNotifications
		columns = append(columns, "chat_email_notifications")
	}
	if update.ChatPushNotifications != nil {
		dst.ChatPushNotifications = *update.ChatPushNotifications
		columns = append(columns, "chat_push_notifications")
	}
	return columns
}

// defaultPersistenceUserSettings builds the documented default settings for a user.
func defaultPersistenceUserSettings(userID uint64) *common_persistence_models.UserSettings {
	return &common_persistence_models.UserSettings{
		UserID:                 userID,
		DarkMode:               false,
		EmailNotifications:     true,  // Default to enabled
		PushNotifications:      true,  // Default to enabled
		MarketingEmails:        false, // Default to disabled
		Language:               proto_models.Language_LANGUAGE_ENGLISH,
		ChatEmailNotifications: true, // Default to enabled
		ChatPushNotifications:  true, // Default to enabled
	}
}

// CreateDefaultSettingsForNewUser executes part of the common_service service flow and coordinates work for the *UserSettingsHandler receiver.
func (h *UserSettingsHandler) CreateDefaultSettingsForNewUser(ctx context.Context, userID uint64) error {
	// Create default settings
	persistenceSettings := defaultPersistenceUserSettings(userID)

	// CreateUserSettings uses context transaction
	err := h.persistenceHandler.CreateUserSettings(ctx, persistenceSettings)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "create_default_user_settings"))
	}

	return nil
}

// createDefaultUserSettings executes part of the common_service service flow and coordinates work for the *UserSettingsHandler receiver.
func (h *UserSettingsHandler) createDefaultUserSettings(ctx context.Context, userID uint64) (*common_service_models.UserSettings, error) {
	// Check if user exists (Read operation, uses default DB from persistence)
	_, err := h.persistenceHandler.GetUserByID(ctx, userID)
	if err != nil {
		if stdErrors.Is(err, apperrors.ErrUserNotFound) || stdErrors.Is(err, persistence.ErrUserNotFound) {
			return nil, apperrors.ErrUserNotFound
		}
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Stage, "check_user_exists"))
	}

	// Create default settings using the shared method (which now uses context transaction)
	err = h.CreateDefaultSettingsForNewUser(ctx, userID) // Removed txDB
	if err != nil {
		return nil, err // Caller's transaction (if any) will handle rollback
	}

	// Get the newly created settings (Read operation, uses default DB)
	persistenceSettings, err := h.persistenceHandler.GetUserSettingsByUserID(ctx, userID)
	if err != nil {
		return nil, appLogger.InvariantError("created_user_settings_are_readable", err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "get_user_settings"),
			zap.String(logkeys.Stage, "reload_created_settings"),
			zap.String(logkeys.Expected, "newly created settings can be loaded"),
			zap.String(logkeys.Actual, "settings lookup failed after create"))
	}

	// Convert persistence model to service model
	settings := common_service_models.ConvertPersistenceToUserSettings(persistenceSettings)

	return settings, nil
}

// Stop executes part of the common_service service flow and coordinates work for the *UserSettingsHandler receiver.
func (h *UserSettingsHandler) Stop() {
	// Currently no resources to clean up
}
