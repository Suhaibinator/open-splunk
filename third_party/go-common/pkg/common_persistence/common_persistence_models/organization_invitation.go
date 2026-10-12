// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the OrganizationInvitation model for tracking pending organization invitations.
package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
)

// OrganizationInvitation defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type OrganizationInvitation struct {
	// ID is the unique identifier for the invitation (auto-incrementing primary key)
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// OrganizationID references the organization the user is invited to
	OrganizationID int64 `json:"organization_id" gorm:"not null;index"`

	// Email is the email address of the invited user (used for lookup during registration)
	Email string `json:"email" gorm:"type:varchar(255);not null;index"`

	// Role defines the role the user will have upon accepting the invitation
	Role proto_models.OrganizationRole `json:"role" gorm:"type:int;not null;default:1"` // Default to MEMBER_TIER_0

	// InvitedByUserID references the user who created this invitation
	InvitedByUserID uint64 `json:"invited_by_user_id" gorm:"not null;index"`

	// CreatedAt timestamp when the invitation was created
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;not null"`

	// ExpiresAt timestamp when the invitation expires
	ExpiresAt time.Time `json:"expires_at" gorm:"type:timestamptz;not null;index"`

	// AcceptedAt timestamp when the invitation was accepted (nil if pending)
	AcceptedAt *time.Time `json:"accepted_at,omitempty" gorm:"type:timestamptz"`

	// DeclinedAt timestamp when the invitation was declined (nil if pending or accepted)
	DeclinedAt *time.Time `json:"declined_at,omitempty" gorm:"type:timestamptz"`

	// RequiresAcceptance indicates whether the recipient must accept (invite flow) or will be auto-added on registration (direct add of non-existent user)
	RequiresAcceptance bool `json:"requires_acceptance" gorm:"not null;default:true"`

	// Organization is the associated organization record (when preloaded)
	Organization Organization `json:"organization" gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// InvitedByUser is the user who created the invitation (when preloaded)
	InvitedByUser User `json:"invited_by_user" gorm:"foreignKey:InvitedByUserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

// BeforeCreate executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (oi *OrganizationInvitation) BeforeCreate(tx *gorm.DB) error {
	oi.CreatedAt = time.Now()
	return nil
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (OrganizationInvitation) TableName() string {
	return TableNameOrganizationInvitations
}

// IsExpired executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (oi *OrganizationInvitation) IsExpired() bool {
	return time.Now().After(oi.ExpiresAt)
}
