package common_service

import (
	"context"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// User Settings Operations
// These methods handle user preferences and settings management

// GetUserSettings executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetUserSettings(ctx context.Context, userID uint64) (*common_service_models.UserSettings, error) {
	settings, err := h.userSettingsHandler.GetUserSettings(ctx, userID)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "get_user_settings"))
	}
	return settings, nil
}

// UpdateUserSettings executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// Parameters:
// - `ctx`: propagates request-scoped deadlines, cancellation, and trace metadata.
// - `userID`: supplies the `userID` input as `uint64` for this operation.
// - `update`: supplies the partial settings update; only non-nil fields are applied.
func (h *CommonServiceHandler[U]) UpdateUserSettings(ctx context.Context, userID uint64, update *common_service_models.UserSettingsUpdate) (*common_service_models.UserSettings, error) {
	settings, err := h.userSettingsHandler.UpdateUserSettings(ctx, userID, update)
	if err != nil {
		return nil, err
	}

	// Record metrics on successful update
	if h.metricsHandler != nil {
		h.metricsHandler.RecordUserSettingsUpdate()
	}

	return settings, nil
}

// UpdateUserLanguagePreference executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) UpdateUserLanguagePreference(ctx context.Context, userID uint64, language proto_models.Language) error {
	// Apply only the language field via a partial update; other settings are untouched.
	_, err := h.userSettingsHandler.UpdateUserSettings(ctx, userID, &common_service_models.UserSettingsUpdate{
		Language: &language,
	})

	// Record metrics on successful update
	if err == nil && h.metricsHandler != nil {
		h.metricsHandler.RecordUserSettingsUpdate()
	}

	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "update_user_language_preference"),
			zap.Stringer(appLogger.KeyLanguage, language))
	}
	return nil
}

// RemoveUserPhoneNumber executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) RemoveUserPhoneNumber(ctx context.Context, userID uint64) error {
	// Get the user to remove phone number
	persistenceUser, err := h.commonPersistenceHandler.GetUserByID(ctx, userID)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "remove_user_phone_number"),
			zap.String(logkeys.Stage, "load_user"))
	}

	// Set phone number to nil
	persistenceUser.PhoneNumber = nil

	// Update the user in the database
	err = h.commonPersistenceHandler.UpdateUser(ctx, persistenceUser)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(logkeys.Operation, "remove_user_phone_number"),
			zap.String(logkeys.Stage, "update_user"))
	}

	// Invalidate user cache if we have an auth handler with caching
	if h.authHandler != nil {
		h.authHandler.InvalidateUserCache(common_service_models.UserIdType(userID))
	}

	// Notify the application that a core user field changed so it can resync.
	h.invokeOnUserUpdated(ctx, userID)

	appLogger.For(ctx, h.logger, loggerName).Info("Successfully removed user phone number",
		zap.Uint64(appLogger.KeyUserID, userID))

	return nil
}

// UpdateUserUsername executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) UpdateUserUsername(ctx context.Context, userID uint64, newUsername string) error {
	// Load current user first so unchanged usernames can short-circuit cleanly.
	persistenceUser, err := h.commonPersistenceHandler.GetUserByID(ctx, userID)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyNewUsername, newUsername),
			zap.String(logkeys.Stage, "load_user"))
	}

	if strings.EqualFold(persistenceUser.Username, newUsername) {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Username update is a no-op",
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyNewUsername, newUsername))
		}
		return nil
	}

	// First check if the new username is available
	available, message, err := h.CheckUsernameAvailability(ctx, newUsername)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyNewUsername, newUsername),
			zap.String(logkeys.Stage, "check_username_availability"))
	}

	if !available {
		return appLogger.WithErrorFields(errors.ErrUsernameAlreadyExists,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyNewUsername, newUsername),
			zap.String(logkeys.Reason, message))
	}

	// Store old username for logging
	oldUsername := persistenceUser.Username

	// Persist username change and immutable history entry atomically.
	actorUserID := userID
	err = h.commonPersistenceHandler.UpdateUsernameWithAudit(
		ctx,
		userID,
		newUsername,
		&actorUserID,
		"actor=self",
	)
	if err != nil {
		if err == common_persistence.ErrUserAlreadyExists {
			return errors.ErrUsernameAlreadyExists
		}
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyOldUsername, oldUsername),
			zap.String(appLogger.KeyNewUsername, newUsername),
			zap.String(logkeys.Stage, "persist_username_update"))
	}

	// Notify the application that a core user field changed so it can resync.
	h.invokeOnUserUpdated(ctx, userID)

	appLogger.For(ctx, h.logger, loggerName).Info("Successfully updated user username",
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.String(appLogger.KeyOldUsername, oldUsername),
		zap.String(appLogger.KeyNewUsername, newUsername))

	return nil
}
