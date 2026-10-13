package common_persistence_models

import (
	"time"
)

// ContactMessage defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ContactMessage struct {
	ID          uint      `gorm:"primaryKey"`
	UserID      *uint64   `gorm:"index"` // Nullable - only set if user is logged in
	Name        string    `gorm:"not null;size:255"`
	Email       string    `gorm:"not null;size:255;index"`
	Subject     string    `gorm:"not null;size:500"`
	Message     string    `gorm:"not null;type:text"`
	IPAddress   string    `gorm:"not null;size:45"` // Supports IPv6
	UserAgent   string    `gorm:"not null;type:text"`
	Status      string    `gorm:"default:'pending';size:50;index"`
	AdminNotes  string    `gorm:"type:text"`
	ReferenceID string    `gorm:"not null;size:100;index"` // Trace ID for tracking
	CreatedAt   time.Time `gorm:"type:timestamptz;not null;autoCreateTime"`
	UpdatedAt   time.Time `gorm:"type:timestamptz;not null;autoUpdateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ContactMessage) TableName() string {
	return TableNameContactMessages
}

// ContactMessageStatus constants
const (
	ContactStatusPending = "pending"
)
