package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// CareerApplication defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type CareerApplication struct {
	ID                 uint64                               `gorm:"primaryKey;autoIncrement;not null"`
	ReferenceID        string                               `gorm:"size:100;not null;uniqueIndex"`
	JobPostingID       uint64                               `gorm:"index;not null"`
	JobPosting         CareerJobPosting                     `gorm:"foreignKey:JobPostingID"`
	ApplicantUserID    uint64                               `gorm:"index;not null;default:0"`
	ApplicantUser      User                                 `gorm:"foreignKey:ApplicantUserID;references:ID;-:migration"`
	JobTitleSnapshot   string                               `gorm:"size:255;not null"`
	ApplicantName      string                               `gorm:"size:255;not null"`
	Email              string                               `gorm:"size:255;not null;index"`
	Phone              string                               `gorm:"size:64"`
	LocationLabel      string                               `gorm:"size:255"`
	LinkedinURL        string                               `gorm:"size:1000"`
	PortfolioURL       string                               `gorm:"size:1000"`
	CoverLetter        string                               `gorm:"type:text"`
	ResumeAttachmentID uint64                               `gorm:"index;not null"`
	ResumeAttachment   Attachment                           `gorm:"foreignKey:ResumeAttachmentID"`
	Status             proto_models.CareerApplicationStatus `gorm:"index;not null"`
	AdminNotes         string                               `gorm:"type:text"`
	CreatedAt          time.Time                            `gorm:"type:timestamptz;not null;autoCreateTime"`
	UpdatedAt          time.Time                            `gorm:"type:timestamptz;not null;autoUpdateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (CareerApplication) TableName() string {
	return TableNameCareerApplications
}
