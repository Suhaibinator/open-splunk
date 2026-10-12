package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CareerJobPosting defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type CareerJobPosting struct {
	ID             uint64 `gorm:"primaryKey;autoIncrement;not null"`
	Slug           string `gorm:"size:150;not null;uniqueIndex"`
	Title          string `gorm:"size:255;not null;index"`
	Team           string `gorm:"size:255;not null;index"`
	LocationLabel  string `gorm:"size:255;not null;index"`
	WorkplaceMode  proto_models.CareerWorkplaceMode
	EmploymentType proto_models.CareerEmploymentType
	Summary        string                              `gorm:"size:2000;not null"`
	Description    string                              `gorm:"type:text;not null"`
	Status         proto_models.CareerJobPostingStatus `gorm:"index;not null"`
	PublishedAt    *time.Time                          `gorm:"type:timestamptz;index"`
	ClosesAt       *time.Time                          `gorm:"type:timestamptz;index"`
	CreatedAt      time.Time                           `gorm:"type:timestamptz;not null;autoCreateTime"`
	UpdatedAt      time.Time                           `gorm:"type:timestamptz;not null;autoUpdateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (CareerJobPosting) TableName() string {
	return TableNameCareerJobPostings
}

// ToProto executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (j *CareerJobPosting) ToProto() *proto_models.CareerJobPosting {
	if j == nil {
		return nil
	}

	job := &proto_models.CareerJobPosting{
		Id:             j.ID,
		Slug:           j.Slug,
		Title:          j.Title,
		Team:           j.Team,
		LocationLabel:  j.LocationLabel,
		WorkplaceMode:  j.WorkplaceMode,
		EmploymentType: j.EmploymentType,
		Summary:        j.Summary,
		Description:    j.Description,
		Status:         j.Status,
		CreatedAt:      timestamppb.New(j.CreatedAt),
		UpdatedAt:      timestamppb.New(j.UpdatedAt),
	}

	if j.PublishedAt != nil {
		job.PublishedAt = timestamppb.New(*j.PublishedAt)
	}
	if j.ClosesAt != nil {
		job.ClosesAt = timestamppb.New(*j.ClosesAt)
	}

	return job
}
