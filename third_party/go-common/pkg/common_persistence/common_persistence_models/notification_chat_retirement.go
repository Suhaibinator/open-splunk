package common_persistence_models

import (
	"time"

	"github.com/lib/pq"
)

// NotificationChatRetirement is durable cleanup committed with a channel's soft
// deletion. Only pending jobs remain; each transaction advances a bounded member
// page and the final page removes the job. DueAt orders claims: a successful
// page moves the job behind its peers, and a failed page backs it off so one
// broken channel cannot hold the head of the queue. ExhaustedAt marks a job that
// failed notification_chat_retirement_max_attempts consecutive pages; it is
// retried after notification_chat_retirement_exhausted_delay or immediately
// when a new deletion re-enqueues it.
type NotificationChatRetirement struct {
	ChannelID     uint64        `gorm:"primaryKey;autoIncrement:false;index:idx_notification_chat_retirement_due,priority:2"`
	ActivityScope string        `gorm:"type:varchar(256);not null"`
	Types         pq.Int64Array `gorm:"type:bigint[];not null"`
	AfterUserID   uint64        `gorm:"not null;default:0"`
	Attempts      uint32        `gorm:"not null;default:0"`
	LastError     string        `gorm:"type:varchar(64);not null;default:''"`
	ExhaustedAt   *time.Time
	DueAt         time.Time `gorm:"not null;default:now();index:idx_notification_chat_retirement_due,priority:1"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (NotificationChatRetirement) TableName() string {
	return "gocommon_notification_chat_retirements"
}
