package common_persistence_models

import (
	json "encoding/json/v2"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

const ObjectTypeProfilePicture = "profile-picture"

// ObjectTypeChatAttachment is the object type of a chat message's attachments; EntityID is the
// message ID.
const ObjectTypeChatAttachment = "chat-attachment"

// ObjectTypeChatQueuedInputAttachment is the object type of a queued agent input's attachments;
// EntityID is the ChatAgentQueuedInput ID until delivery moves them to the message it creates
// (ObjectTypeChatAttachment).
const ObjectTypeChatQueuedInputAttachment = "chat-queued-input-attachment"

var profilePictureAllowedMIMETypes = [...]string{
	"image/jpeg",
	"image/jpg",
	"image/png",
	"image/gif",
	"image/webp",
	"image/bmp",
}

// ProfilePictureAllowedMIMETypes returns the established image allowlist used
// by the attachment registry and by association-time avatar validation.
func ProfilePictureAllowedMIMETypes() []string {
	result := make([]string, len(profilePictureAllowedMIMETypes))
	copy(result, profilePictureAllowedMIMETypes[:])
	return result
}

// IsProfilePictureMIMEType reports whether contentType belongs to the
// established profile-picture image allowlist.
func IsProfilePictureMIMEType(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	for _, allowed := range profilePictureAllowedMIMETypes {
		if contentType == allowed {
			return true
		}
	}
	return false
}

// UploadStatus represents the current state of an attachment in its lifecycle.
// Attachments progress through these states from upload to final storage.
type UploadStatus int

// Attachment upload status constants define the possible states.
const (
	// UploadStatusPendingUpload indicates the attachment record exists but file upload hasn't started.
	// This is the initial state when a presigned URL is generated.
	UploadStatusPendingUpload UploadStatus = iota

	// UploadStatusPendingProcessing indicates the file has been uploaded to temporary storage
	// but processing with the configured validators has not started.
	UploadStatusPendingProcessing

	// UploadStatusProcessing indicates the attachment is currently being validated and processed.
	// Processing applies configured content validators and copies to verified storage;
	// the library does not install a built-in antivirus scanner.
	UploadStatusProcessing

	// UploadStatusVetted indicates the attachment passed all validations and is available for use.
	// A verified object has been published; user-facing reads may issue its URL.
	// Internal protocol attachments remain excluded from those reads.
	UploadStatusVetted

	// UploadStatusRejected indicates the attachment failed validation or processing.
	// The validation details explain why the attachment was rejected.
	UploadStatusRejected
)

// String executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (s UploadStatus) String() string {
	switch s {
	case UploadStatusPendingUpload:
		return "pending_upload"
	case UploadStatusPendingProcessing:
		return "pending_processing"
	case UploadStatusProcessing:
		return "processing"
	case UploadStatusVetted:
		return "vetted"
	case UploadStatusRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// ParseUploadStatus converts a status string into its UploadStatus enum value.
// It returns an error for any unrecognized status.
//
// Parameters:
//   - status: the status string to convert.
//
// Returns:
//   - result: the matching UploadStatus.
//   - error: non-nil when the status is unrecognized.
func ParseUploadStatus(status string) (UploadStatus, error) {
	switch status {
	case "pending_upload":
		return UploadStatusPendingUpload, nil
	case "pending_processing":
		return UploadStatusPendingProcessing, nil
	case "processing":
		return UploadStatusProcessing, nil
	case "vetted":
		return UploadStatusVetted, nil
	case "rejected":
		return UploadStatusRejected, nil
	default:
		return 0, fmt.Errorf("invalid upload status: %s", status)
	}
}

// MarshalJSON executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (s UploadStatus) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// UnmarshalJSON executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (s *UploadStatus) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}

	parsed, err := ParseUploadStatus(str)
	if err != nil {
		return err
	}
	*s = parsed

	return nil
}

// IsPublicAttachmentVisibility reports whether a stored visibility places the
// verified object in the anonymously readable public bucket. Only an explicit
// ATTACHMENT_VISIBILITY_PUBLIC counts: ATTACHMENT_VISIBILITY_UNSPECIFIED (the
// zero value, carried by every row that predates the field) and any unknown
// value are treated as private, so the default fails closed. Every visibility
// classification — Go dispatch and SQL predicates alike — must go through this
// two-way split rather than comparing against ATTACHMENT_VISIBILITY_PRIVATE,
// which would silently misclassify legacy zero-valued rows.
func IsPublicAttachmentVisibility(v proto_models.AttachmentVisibility) bool {
	return v == proto_models.AttachmentVisibility_ATTACHMENT_VISIBILITY_PUBLIC
}

// File size constants for attachment uploads
const (
	// MaxCareerResumeSize is the maximum size for career resume uploads (10MB)
	MaxCareerResumeSize = int64(10 * 1024 * 1024)
)

// AttachmentOrderBy defines safe ordering options for attachment queries.
// This prevents SQL injection by restricting orderBy parameters to known safe values.
type AttachmentOrderBy string

const (
	// OrderByCreatedAtAsc orders by creation time, oldest first
	OrderByCreatedAtAsc AttachmentOrderBy = "created_at ASC"
	// OrderByCreatedAtDesc orders by creation time, newest first
	OrderByCreatedAtDesc AttachmentOrderBy = "created_at DESC"
	// OrderByUpdatedAtAsc orders by update time, oldest first
	OrderByUpdatedAtAsc AttachmentOrderBy = "updated_at ASC"
	// OrderByUpdatedAtDesc orders by update time, newest first
	OrderByUpdatedAtDesc AttachmentOrderBy = "updated_at DESC"
	// OrderBySizeAsc orders by file size, smallest first
	OrderBySizeAsc AttachmentOrderBy = "size ASC"
	// OrderBySizeDesc orders by file size, largest first
	OrderBySizeDesc AttachmentOrderBy = "size DESC"
	// OrderByFilenameAsc orders by original filename alphabetically
	OrderByFilenameAsc AttachmentOrderBy = "original_file_name ASC"
	// OrderByFilenameDesc orders by original filename reverse alphabetically
	OrderByFilenameDesc AttachmentOrderBy = "original_file_name DESC"
)

// AttachmentQueryOptions controls filtering, ordering, and pagination for an
// entity attachment query. Zero values return every status ordered by creation
// time from oldest to newest.
type AttachmentQueryOptions struct {
	Status  *UploadStatus
	OrderBy AttachmentOrderBy
	Limit   int
	Offset  int
}

// ParseOrderByString executes this function as part of the persistence model flow for database reads, writes, and ORM mappings.
func ParseOrderByString(orderBy string) AttachmentOrderBy {
	switch orderBy {
	case string(OrderByCreatedAtAsc):
		return OrderByCreatedAtAsc
	case string(OrderByCreatedAtDesc):
		return OrderByCreatedAtDesc
	case string(OrderByUpdatedAtAsc):
		return OrderByUpdatedAtAsc
	case string(OrderByUpdatedAtDesc):
		return OrderByUpdatedAtDesc
	case string(OrderBySizeAsc):
		return OrderBySizeAsc
	case string(OrderBySizeDesc):
		return OrderBySizeDesc
	case string(OrderByFilenameAsc):
		return OrderByFilenameAsc
	case string(OrderByFilenameDesc):
		return OrderByFilenameDesc
	default:
		// Return safe default for any invalid input
		return OrderByCreatedAtAsc
	}
}

// SQLClause returns a validated database order clause with a deterministic ID
// tie-breaker. AttachmentOrderBy values are closed over the cases accepted by
// ParseOrderByString, so caller-provided strings cannot become arbitrary SQL.
func (o AttachmentOrderBy) SQLClause() string {
	normalized := ParseOrderByString(string(o))
	direction := "ASC"
	if strings.HasSuffix(string(normalized), " DESC") {
		direction = "DESC"
	}
	return fmt.Sprintf("%s, id %s", normalized, direction)
}

// Attachment defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type Attachment struct {
	ID uint64 `gorm:"primaryKey;autoIncrement;not null"`

	// gorm.Model provides CreatedAt, UpdatedAt, DeletedAt fields
	gorm.Model

	// User Relationship

	// UserID identifies the user who uploaded this attachment
	UserID uint64 `gorm:"index"`

	// User is the associated user record (loaded via preload)
	User User `gorm:"foreignKey:UserID"`

	// File Metadata

	// OriginalFileName preserves the original name provided by the user
	OriginalFileName string `gorm:"size:255"`

	// ContentType is the MIME type of the file (e.g., "image/jpeg")
	ContentType string `gorm:"size:255"`

	// Size is the file size in bytes
	Size int64

	// Storage Locations

	// TemporaryObjectName is the MinIO path during upload/processing
	TemporaryObjectName string `gorm:"size:512"`

	// FinalObjectName stores the path using the salted hash after successful processing
	// The full path is constructed as verified/[salted-hash]
	// This uses a salted hash to prevent rainbow table attacks on stored content
	FinalObjectName string `gorm:"size:512"`

	// FileHash is the unsalted SHA-256 hash of the file content
	// This enables external integrity verification and cross-system deduplication
	// Note: For existing records, this may contain the salted hash (legacy format)
	FileHash string `gorm:"size:64;index"`

	// DeclaredFileHash is the content hash the uploader declared when the upload was created
	// (AttachmentMetadata.content_hash). It is never verified: it only names a pending upload,
	// whose FileHash is empty until processing, to the client holding the file (ClientContentHash).
	DeclaredFileHash string `gorm:"size:64"`

	// ObjectType identifies the type of object (e.g., "profile-picture", "document", "attachment")
	// This is used to organize temporary uploads and apply type-specific validation
	ObjectType string `gorm:"size:50;index"`

	// InternalOnly marks server-owned blobs that are part of an agent/provider
	// protocol rather than user content. Public attachment reads, hydration, and
	// admin attachment APIs must never return these rows.
	InternalOnly bool `gorm:"not null;default:false;index" json:"-"`

	// Visibility records where the verified object is stored and therefore how it
	// may be reached: public attachments live in the public bucket and are served
	// anonymously, everything else lives in the private bucket and is served only
	// through backend-issued presigned URLs. The stored value is the wire enum;
	// ATTACHMENT_VISIBILITY_UNSPECIFIED (0, the column default and the value on
	// every row predating the field) is treated as private, so classification
	// must go through IsPublicAttachmentVisibility rather than equality with
	// ATTACHMENT_VISIBILITY_PRIVATE. InternalOnly rows are always treated as
	// private regardless of this field. FinalObjectName keeps the same
	// verified/[salted-hash] shape in both buckets, so the pair
	// (Visibility, FinalObjectName) — not FinalObjectName alone — is the storage
	// identity that deduplication, reclamation, and locking must key on.
	Visibility proto_models.AttachmentVisibility `gorm:"type:integer;not null;default:1;index"`

	// Entity Linking

	// EntityID is the unique identifier of the entity this attachment is linked to
	// Supports any ID format: numeric (123), UUID, or custom strings
	// The entity type is implied by the ObjectType field (e.g., "profile-picture" -> user entity)
	EntityID string `gorm:"type:varchar(255);not null;index" json:"entity_id"`

	// Processing Status

	// UploadStatus tracks the current state in the attachment lifecycle
	UploadStatus UploadStatus `gorm:"type:integer;index"`

	// UploadTime records when the file upload completed
	UploadTime time.Time `gorm:"type:timestamptz;index"`

	// UploadURLExpiresAt is the latest expiry of the upload URLs issued for a pending upload; each
	// URL expires at or before it. Pending-upload cleanup counts its timeout from here, so it
	// never rejects a row while one of its URLs is still valid. On upgrade, live pending rows
	// predating this column receive rollout time plus the required historical URL-lifetime bound;
	// other legacy rows receive rollout time.
	UploadURLExpiresAt time.Time `gorm:"type:timestamptz;not null;default:now()"`

	// ProcessingStartTime tracks when validation/processing began
	ProcessingStartTime *time.Time `gorm:"type:timestamptz"`

	// ProcessingEndTime tracks when processing completed (success or failure)
	ProcessingEndTime *time.Time `gorm:"type:timestamptz"`

	// Validation Information

	// ValidationDetails contains error messages or validation results
	ValidationDetails string `gorm:"size:1024"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (Attachment) TableName() string {
	return TableNameAttachments
}

// IsPubliclyAvailable reports whether an attachment may be exposed through a
// public asset association. Temporary paths and internal protocol artifacts
// are never public, even if a caller accidentally preloads the row, and the
// row must be stored with public visibility so the object actually resides in
// the anonymously readable bucket.
func (a *Attachment) IsPubliclyAvailable() bool {
	return a != nil &&
		!a.InternalOnly &&
		a.UploadStatus == UploadStatusVetted &&
		!a.DeletedAt.Valid &&
		a.FinalObjectName != "" &&
		IsPublicAttachmentVisibility(a.Visibility)
}

// ClientContentHash is the content hash a client knows the attachment by (AttachmentMetadata and
// UploadedAttachment content_hash): its verified FileHash once processing set it, and the hash its
// uploader declared until then.
func (a *Attachment) ClientContentHash() string {
	if a.FileHash != "" {
		return a.FileHash
	}
	return a.DeclaredFileHash
}

// IsValidPublicAvatar applies both public-availability checks and the
// repository's established profile-picture object-type and MIME policy.
func (a *Attachment) IsValidPublicAvatar() bool {
	return a.IsPubliclyAvailable() &&
		a.ObjectType == ObjectTypeProfilePicture &&
		IsProfilePictureMIMEType(a.ContentType)
}
