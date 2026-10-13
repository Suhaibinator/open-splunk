package common_persistence_models

import "github.com/Suhaibinator/go-common/pkg/proto_models"

// UserSettings defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type UserSettings struct {
	UserID                 uint64                `json:"user_id" gorm:"primaryKey;type:bigint;not null"`
	User                   User                  `json:"user" gorm:"foreignKey:UserID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
	DarkMode               bool                  `json:"dark_mode" gorm:"type:boolean;not null;default:false"`
	EmailNotifications     bool                  `json:"email_notifications" gorm:"type:boolean;not null;default:true"`
	PushNotifications      bool                  `json:"push_notifications" gorm:"type:boolean;not null;default:true"`
	MarketingEmails        bool                  `json:"marketing_emails" gorm:"type:boolean;not null;default:false"`
	Language               proto_models.Language `json:"language" gorm:"type:integer;not null;default:0"`
	ChatEmailNotifications bool                  `json:"chat_email_notifications" gorm:"type:boolean;not null;default:true"`
	ChatPushNotifications  bool                  `json:"chat_push_notifications" gorm:"type:boolean;not null;default:true"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (UserSettings) TableName() string {
	return TableNameUserSettings
}
