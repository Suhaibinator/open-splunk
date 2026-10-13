package common_persistence_models

import "time"

// OrganizationEmailDomain defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type OrganizationEmailDomain struct {
	ID              uint64    `json:"id" gorm:"primaryKey;autoIncrement"`
	OrganizationID  uint64    `json:"organization_id" gorm:"not null;index"`
	Domain          string    `json:"domain" gorm:"type:varchar(255);not null;uniqueIndex"`
	CreatedByUserID *uint64   `json:"created_by_user_id,omitempty" gorm:"index"`
	CreatedAt       time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt       time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`

	// Organization is the associated organization (FK with CASCADE delete)
	Organization Organization `json:"-" gorm:"foreignKey:OrganizationID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// CreatedByUser is the user who created this domain mapping (FK with SET NULL on delete)
	CreatedByUser *User `json:"-" gorm:"foreignKey:CreatedByUserID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (OrganizationEmailDomain) TableName() string {
	return TableNameOrganizationEmailDomains
}
