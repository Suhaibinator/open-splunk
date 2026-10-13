// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines organization-related models including organizations and user-organization relationships.
package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

// Organization defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type Organization struct {
	// ID is the unique identifier for the organization (auto-incrementing primary key)
	ID int64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// Name is the display name of the organization
	Name string `json:"name" gorm:"type:varchar(255);not null"`

	// Slug is the URL-friendly identifier for the organization (unique)
	Slug string `json:"slug" gorm:"type:varchar(255);not null;uniqueIndex"`

	// Description is an optional description of the organization
	Description *string `json:"description,omitempty" gorm:"type:text"`

	// AvatarAttachmentID references the attachment used as organization avatar/logo
	AvatarAttachmentID *uint64 `json:"avatar_attachment_id,omitempty" gorm:"index"`

	// AvatarAttachment is the loaded attachment relation (when preloaded)
	AvatarAttachment *Attachment `json:"avatar_attachment,omitempty" gorm:"foreignKey:AvatarAttachmentID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// ParentOrganizationID references the parent organization (null for root organizations)
	ParentOrganizationID *int64 `json:"parent_organization_id,omitempty" gorm:"index"`

	// ParentOrganization is the loaded parent organization relation (when preloaded)
	ParentOrganization *Organization `json:"parent_organization,omitempty" gorm:"foreignKey:ParentOrganizationID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// Lineage stores the ordered list of ancestor org IDs (root -> parent)
	Lineage pq.Int64Array `json:"lineage,omitempty" gorm:"type:bigint[];index:idx_organizations_lineage,using:gin"`

	// OrganizationType defines the organization's type/capability tier.
	OrganizationType proto_models.OrganizationType `json:"organization_type" gorm:"type:int;not null;default:12;index"`

	// PersonalOwnerUserID identifies the user whose personal workspace contains this organization.
	// It is nil for shared organizations and is retained when either side is soft-deleted.
	PersonalOwnerUserID *uint64 `json:"personal_owner_user_id,omitempty" gorm:"index:idx_organizations_personal_owner_user_id"`

	// PersonalOwnerUser is the retained owner relation. Hard deletion is restricted so ownership
	// cannot be erased by silently converting a personal organization into a shared organization.
	PersonalOwnerUser *User `json:"-" gorm:"foreignKey:PersonalOwnerUserID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:RESTRICT;"`

	// CreatedAt timestamp when the organization was created
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz"`

	// UpdatedAt timestamp when the organization was last modified
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`

	// DeletedAt enables GORM soft delete behavior
	DeletedAt gorm.DeletedAt `json:"deleted_at" gorm:"index"`

	// UserOrganizations contains the user-organization relationships (when preloaded)
	UserOrganizations []UserOrganization `json:"user_organizations,omitempty" gorm:"foreignKey:OrganizationID"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (Organization) TableName() string {
	return TableNameOrganizations
}

// UserOrganization defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type UserOrganization struct {
	// UserID references the user who is a member of the organization
	UserID uint64 `json:"user_id" gorm:"primaryKey;index"`

	// OrganizationID references the organization the user belongs to
	OrganizationID int64 `json:"organization_id" gorm:"primaryKey;index"`

	// Role defines the user's role and permissions within the organization
	Role proto_models.OrganizationRole `json:"role" gorm:"type:int;not null;default:1;index"` // Default to MEMBER_TIER_0

	// JoinedAt timestamp when the user joined the organization
	JoinedAt time.Time `json:"joined_at" gorm:"type:timestamptz;not null"`

	// UpdatedAt timestamp when the relationship was last modified (e.g., role change)
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`

	// User is the associated user record (when preloaded)
	User User `json:"user" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// Organization is the associated organization record (when preloaded)
	Organization Organization `json:"organization" gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

// BeforeCreate executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (uo *UserOrganization) BeforeCreate(tx *gorm.DB) error {
	uo.JoinedAt = time.Now()
	return nil
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (UserOrganization) TableName() string {
	return TableNameUserOrganizations
}
