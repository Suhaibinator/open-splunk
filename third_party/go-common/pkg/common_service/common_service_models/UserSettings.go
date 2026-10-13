package common_service_models

import (
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// UserSettings defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type UserSettings struct {
	// UserID is the ID of the user these settings belong to and serves as the primary key.
	UserID uint64 `json:"user_id"`

	// DarkMode indicates whether dark theme is enabled.
	DarkMode bool `json:"dark_mode"`

	// EmailNotifications indicates whether email notifications are enabled.
	EmailNotifications bool `json:"email_notifications"`

	// PushNotifications indicates whether push notifications are enabled.
	PushNotifications bool `json:"push_notifications"`

	// MarketingEmails indicates whether marketing emails are enabled.
	MarketingEmails bool `json:"marketing_emails"`

	// Language is the user's preferred language.
	Language proto_models.Language `json:"language"`

	// ChatEmailNotifications indicates whether email notifications for new chat messages are enabled.
	ChatEmailNotifications bool `json:"chat_email_notifications"`

	// ChatPushNotifications indicates whether push notifications for new chat messages are enabled.
	ChatPushNotifications bool `json:"chat_push_notifications"`
}

// UserSettingsUpdate carries a partial user settings mutation. Only non-nil
// fields are applied to the stored settings; nil fields leave the existing
// value unchanged.
type UserSettingsUpdate struct {
	// DarkMode, when non-nil, sets whether dark theme is enabled.
	DarkMode *bool

	// EmailNotifications, when non-nil, sets whether email notifications are enabled.
	EmailNotifications *bool

	// PushNotifications, when non-nil, sets whether push notifications are enabled.
	PushNotifications *bool

	// MarketingEmails, when non-nil, sets whether marketing emails are enabled.
	MarketingEmails *bool

	// Language, when non-nil, sets the user's preferred language.
	Language *proto_models.Language

	// ChatEmailNotifications, when non-nil, sets whether email notifications for new chat messages are enabled.
	ChatEmailNotifications *bool

	// ChatPushNotifications, when non-nil, sets whether push notifications for new chat messages are enabled.
	ChatPushNotifications *bool
}

// UserSettingsLike defines the minimum set of methods required for user settings objects.
// This interface allows for different settings implementations while maintaining
// a consistent API for accessing user preferences.
type UserSettingsLike interface {
	// GetDarkMode returns whether dark mode is enabled.
	GetDarkMode() bool

	// GetEmailNotifications returns whether email notifications are enabled.
	GetEmailNotifications() bool

	// GetPushNotifications returns whether push notifications are enabled.
	GetPushNotifications() bool

	// GetMarketingEmails returns whether marketing emails are enabled.
	GetMarketingEmails() bool

	// GetLanguage returns the user's preferred language.
	GetLanguage() proto_models.Language

	// GetChatEmailNotifications returns whether email notifications for new chat messages are enabled.
	GetChatEmailNotifications() bool

	// GetChatPushNotifications returns whether push notifications for new chat messages are enabled.
	GetChatPushNotifications() bool
}

// Compile-time check to ensure UserSettings implements UserSettingsLike
var _ UserSettingsLike = (*UserSettings)(nil)

// GetDarkMode returns the requested value for *UserSettings in the common_service_models backend flow.
func (us *UserSettings) GetDarkMode() bool {
	return us.DarkMode
}

// GetEmailNotifications returns the requested value for *UserSettings in the common_service_models backend flow.
func (us *UserSettings) GetEmailNotifications() bool {
	return us.EmailNotifications
}

// GetPushNotifications returns the requested value for *UserSettings in the common_service_models backend flow.
func (us *UserSettings) GetPushNotifications() bool {
	return us.PushNotifications
}

// GetMarketingEmails returns the requested value for *UserSettings in the common_service_models backend flow.
func (us *UserSettings) GetMarketingEmails() bool {
	return us.MarketingEmails
}

// GetLanguage returns the requested value for *UserSettings in the common_service_models backend flow.
func (us *UserSettings) GetLanguage() proto_models.Language {
	return us.Language
}

// GetChatEmailNotifications returns the requested value for *UserSettings in the common_service_models backend flow.
func (us *UserSettings) GetChatEmailNotifications() bool {
	return us.ChatEmailNotifications
}

// GetChatPushNotifications returns the requested value for *UserSettings in the common_service_models backend flow.
func (us *UserSettings) GetChatPushNotifications() bool {
	return us.ChatPushNotifications
}

// ConvertPersistenceToUserSettings converts input data into the target model in the common_service_models backend flow.
func ConvertPersistenceToUserSettings(settings *common_persistence_models.UserSettings) *UserSettings {
	if settings == nil {
		return nil // Return nil interface
	}

	return &UserSettings{
		UserID:                 settings.UserID,
		DarkMode:               settings.DarkMode,
		EmailNotifications:     settings.EmailNotifications,
		PushNotifications:      settings.PushNotifications,
		MarketingEmails:        settings.MarketingEmails,
		Language:               settings.Language,
		ChatEmailNotifications: settings.ChatEmailNotifications,
		ChatPushNotifications:  settings.ChatPushNotifications,
	}
}
