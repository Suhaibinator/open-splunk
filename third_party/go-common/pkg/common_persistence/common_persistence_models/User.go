// Package common_persistence_models contains the database models used by the persistence layer.
// These models represent the core entities stored in the PostgreSQL database and define
// the schema through GORM struct tags.
package common_persistence_models

import (
	"strings"
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
)

// User defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type User struct {
	// AdminCreated accounts are intentionally provisioned and exempt from abandoned-registration cleanup.
	AdminCreated bool `gorm:"not null;default:false" json:"-"`
	// ID is the unique identifier for the user (auto-incrementing primary key)
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// Username is the unique username chosen by the user
	Username string `json:"username" gorm:"type:varchar(255);not null;uniqueIndex"`

	// Email is the user's email address (must be unique, used for authentication)
	Email string `json:"email" gorm:"type:varchar(255);not null;uniqueIndex"`

	// PhoneNumber is the user's phone number in E.164 international format (optional)
	PhoneNumber *string `json:"phone_number,omitempty" gorm:"type:varchar(20);index"`

	// FirstName is the user's first name
	FirstName string `json:"first_name" gorm:"type:varchar(255);not null;default:''"`

	// LastName is the user's last name
	LastName string `json:"last_name" gorm:"type:varchar(255);not null;default:''"`

	// UserType defines the user's role/permissions in the system
	UserType proto_models.UserType `json:"user_type" gorm:"type:int;not null;default:0"`

	// Banned indicates if the user account is banned/suspended
	Banned bool `json:"banned" gorm:"type:boolean;not null;default:false"`

	// IsBot indicates if this is a bot/automated account
	IsBot bool `json:"is_bot" gorm:"type:boolean;not null;default:false"`

	// Bio is the user's biography/description (optional)
	Bio *string `json:"bio,omitempty" gorm:"type:text"`

	// ProfilePictureAttachmentID references the attachment used as profile picture
	ProfilePictureAttachmentID *uint64 `json:"profile_picture_attachment_id,omitempty" gorm:"index"`

	// ProfilePictureAttachment is the loaded attachment relation (when preloaded)
	ProfilePictureAttachment *Attachment `json:"profile_picture_attachment,omitempty" gorm:"foreignKey:ProfilePictureAttachmentID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// CreatedAt timestamp when the user was created
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz"`

	// UpdatedAt timestamp when the user was last modified
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`

	// LastLoginAt is the most recent successful creation of a fresh authenticated
	// session. Session refreshes deliberately do not advance this timestamp.
	LastLoginAt *time.Time `json:"last_login_at,omitempty" gorm:"type:timestamptz;index"`

	// DeletedAt enables GORM soft delete behavior
	DeletedAt gorm.DeletedAt `json:"deleted_at" gorm:"index"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (User) TableName() string {
	return TableNameUsers
}

// NormalizeUserEmail canonicalizes an email address for storage and lookups:
// lowercased and stripped of surrounding whitespace. The persistence layer
// guarantees users.email is always stored in this form, so callers may compare
// against it with raw equality.
//
// Parameters:
//   - email: Raw email value provided by an upstream caller.
//
// Returns:
//   - result: The normalized email used in storage and comparisons.
func NormalizeUserEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// BeforeSave normalizes the email before any GORM create or struct-based
// update persists the row, so the normalization guarantee holds even for
// write paths that bypass the persistence handler methods.
//
// Parameters:
//   - tx: GORM transaction handle (unused).
//
// Returns:
//   - result: Always nil; normalization cannot fail.
func (u *User) BeforeSave(*gorm.DB) error {
	u.Email = NormalizeUserEmail(u.Email)
	return nil
}
