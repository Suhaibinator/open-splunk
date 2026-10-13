package common_converter

import (
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ConvertUserSettingsToProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
func (h *ConverterHandler[U]) ConvertUserSettingsToProto(settings *common_service_models.UserSettings) *proto_models.UserSettings {
	if settings == nil {
		return nil
	}

	return &proto_models.UserSettings{
		UserId:                 settings.UserID,
		DarkMode:               settings.DarkMode,
		EmailNotifications:     settings.EmailNotifications,
		PushNotifications:      settings.PushNotifications,
		MarketingEmails:        settings.MarketingEmails,
		Language:               settings.Language,
		ChatEmailNotifications: settings.ChatEmailNotifications,
		ChatPushNotifications:  settings.ChatPushNotifications,
	}
}
