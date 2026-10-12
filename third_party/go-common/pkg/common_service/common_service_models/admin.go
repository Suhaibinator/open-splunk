package common_service_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// AdminUserDetails defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type AdminUserDetails struct {
	// User contains the core user information
	User *User

	// Timestamps from persistence layer
	CreatedAt time.Time // When the user account was created
	UpdatedAt time.Time // When the user account was last updated

	// Administrative statistics (computed by persistence layer)
	TotalLogins       int64    // Total number of authentication tokens created
	ActiveSessions    int64    // Number of non-expired, non-revoked auth tokens
	AuthMethods       []string // Available authentication methods ("password", "oauth_google", etc.)
	TotalAttachments  uint64   // Total number of attachments owned by user
	StorageUsedBytes  uint64   // Total storage used by user's attachments in bytes
	InvitationPending bool
	EmailVerified     bool      // Whether user's email has been verified
	LastLogin         time.Time // Timestamp of most recent authentication
}

// AdminChatChannelDetails defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type AdminChatChannelDetails struct {
	Channel                *ChatChannel
	ActiveParticipantCount uint64
	CreatedByUser          *User
}

// AdminAttachmentDetails defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type AdminAttachmentDetails struct {
	ID                  uint64
	UserID              uint64
	User                *User
	OriginalFilename    string
	ContentType         string
	SizeBytes           uint64
	UploadStatus        int64
	Visibility          proto_models.AttachmentVisibility
	FileHash            string
	ValidationDetails   string
	UploadTime          *time.Time
	ProcessingStartTime *time.Time
	ProcessingEndTime   *time.Time
	CreatedAt           time.Time
	TemporaryObjectName string
	FinalObjectName     string
}

// AdminSessionToken defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type AdminSessionToken struct {
	TokenID    uint64
	UserID     uint64
	User       *User
	TokenType  proto_models.TokenType
	Status     proto_models.TokenStatus
	CreatedAt  time.Time
	ExpiresAt  time.Time
	LastUsedAt *time.Time
	IPAddress  string
	UserAgent  string
}
