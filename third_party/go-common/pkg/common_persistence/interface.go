// Package common_persistence provides a comprehensive persistence layer for the go-common library.
// It implements database operations using GORM with PostgreSQL and object storage using MinIO.
//
// The package follows a repository pattern with a clean interface design, supporting:
//   - User and authentication management with secure token handling
//   - Transaction management through context propagation
//   - File attachment lifecycle management with deduplication
//   - Contact message storage and retrieval
//   - MinIO object storage for binary data
//   - Metrics collection for monitoring
//
// Transaction support is provided through context propagation, allowing service layers
// to manage database transactions that automatically flow through to all persistence operations.
package common_persistence

import (
	"context"
	"io"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm" // Import gorm
)

// MinioObjectInfo defines the data structure used in the persistence flow for transactional database operations and repository access.
// It carries state across layer boundaries handled by this package.
type MinioObjectInfo struct {
	// VersionID pins immutable objects in a versioned private bucket.
	VersionID string
	// UserMetadata is detached private object metadata returned by HEAD.
	UserMetadata map[string]string
	// Size is the size of the object in bytes
	Size int64

	// ContentType is the MIME type of the object
	ContentType string

	// LastModified is the time when the object was last modified
	LastModified time.Time

	// ETag is the entity tag of the object
	ETag string

	// ContentEncoding, ContentDisposition and CacheControl are the response
	// headers stored with the object; empty when none was set.
	ContentEncoding    string
	ContentDisposition string
	CacheControl       string
}

// PutObjectOptions sets the response headers stored with an object written by
// PutObjectStream. Empty fields are omitted; an empty ContentType is stored as
// application/octet-stream. ContentEncoding only labels the bytes: the upload
// never compresses or otherwise transforms them.
type PutObjectOptions struct {
	// NoOverwrite publishes only if the object key has no current version.
	NoOverwrite        bool
	UserMetadata       map[string]string
	ContentType        string
	ContentEncoding    string // e.g. "gzip"
	ContentDisposition string // e.g. "inline", "attachment"
	CacheControl       string
}

// ObjectPrecondition pins a storage read or server-side copy to the exact
// stored version the caller previously observed. MatchETag, when non-empty, is
// the ETag returned by GetObjectInfo for the object; the operation then fails
// with ErrObjectPreconditionFailed instead of observing bytes written after
// that stat. Attachment finalization relies on this so a replayed presigned
// upload can never be hashed, validated, or promoted under an earlier
// observation. The zero value imposes no condition.
type ObjectPrecondition struct {
	MatchETag string
	VersionID string
}

// ClaimedToolCallAttemptReader is an optional optimization for durable tool executors. It returns
// the exact post-claim execution-attempt count only while leaseOwner still owns the EXECUTING row.
// Decorators around CommonPersistenceHandler should forward this capability. When it is absent,
// the service performs a lease-token-verified GetChatMessageByIDWithOptions fallback and fails
// closed if that read cannot establish an exact count; it never trusts a potentially stale scan row.
type ClaimedToolCallAttemptReader interface {
	GetClaimedToolCallExecutionAttempt(ctx context.Context, messageID uint64, leaseOwner string) (attempt uint32, owned bool, err error)
}

// OrganizationEffectiveRoleResult defines the data structure used in the persistence flow for transactional database operations and repository access.
// It carries state across layer boundaries handled by this package.
type OrganizationEffectiveRoleResult struct {
	// ExplicitRole is the user's direct membership role in the target organization.
	ExplicitRole proto_models.OrganizationRole
	// EffectiveRole is the highest role between explicit role in the target
	// organization and inherited roles from ancestors in lineage.
	EffectiveRole proto_models.OrganizationRole
	// InheritedFromOrganizationID is set when EffectiveRole is inherited from an
	// ancestor organization (nil when effective role is explicit or unspecified).
	InheritedFromOrganizationID *int64
	// EffectiveRoleDepth is the absolute depth of the organization that supplied
	// the effective role (0=root). Nil when effective role is unspecified.
	EffectiveRoleDepth *int32
}

// UserOrganizationsQuery defines the data structure used in the persistence flow for transactional database operations and repository access.
// It carries state across layer boundaries handled by this package.
type UserOrganizationsQuery struct {
	Offset                           int
	Limit                            int
	SortColumn                       proto_models.ListOrganizationsSortBy
	SortDesc                         bool
	Search                           string
	SkipExactCount                   bool
	ApplyVisibilityFilters           bool
	MinOrganizationType              proto_models.OrganizationType
	OrganizationTypeFilter           proto_models.OrganizationType
	ParentOrganizationIDFilter       *int64
	DepthFilter                      *uint32
	RequiredViewRolesByDepth         []proto_models.OrganizationRole
	OrganizationMembershipMaskFilter []*proto_models.OrganizationMembershipMaskFilter
}

// UserOrganizationWithRoleInfo defines the data structure used in the persistence flow for transactional database operations and repository access.
// It carries state across layer boundaries handled by this package.
type UserOrganizationWithRoleInfo struct {
	UserOrganization *common_persistence_models.UserOrganization
	RoleInfo         OrganizationEffectiveRoleResult
}

// AdminOrganizationsQuery defines the data structure used in the persistence flow for admin organization list queries.
type AdminOrganizationsQuery struct {
	Offset                     int
	Limit                      int
	SkipExactCount             bool
	Search                     string
	OrganizationTypeFilter     proto_models.OrganizationType
	ParentOrganizationIDFilter *int64
	DepthFilter                *uint32
	SortColumn                 string
	SortDesc                   bool
}

// AdminOrganizationParentOptionsQuery defines the bounded query used to search
// direct-parent options for the admin organization table.
type AdminOrganizationParentOptionsQuery struct {
	Offset                 int
	Limit                  int
	ParentSearch           string
	TableSearch            string
	OrganizationTypeFilter proto_models.OrganizationType
}

// AdminOrganizationFilterParentOption defines the data structure used in the persistence flow for admin organization filter metadata.
type AdminOrganizationFilterParentOption struct {
	ID   int64
	Name string
	Slug string
}

// AdminOrganizationFilterMetadata defines the data structure used in the persistence flow for admin organization filter metadata.
type AdminOrganizationFilterMetadata struct {
	MaxDepth uint32
}

// GormTransaction defines the data structure used in the persistence flow for transactional database operations and repository access.
// It carries state across layer boundaries handled by this package.
type GormTransaction struct {
	// Tx is the underlying GORM transaction instance
	Tx *gorm.DB
}

// GetDB executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransaction) GetDB() *gorm.DB {
	return gt.Tx
}

// Commit executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransaction) Commit() error {
	return gt.Tx.Commit().Error
}

// Rollback executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransaction) Rollback() error {
	return gt.Tx.Rollback().Error
}

// RollbackTo executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransaction) RollbackTo(name string) error {
	return gt.Tx.RollbackTo(name).Error
}

// SavePoint executes this method as part of the persistence flow for transactional database operations and repository access.
func (gt *GormTransaction) SavePoint(name string) error {
	return gt.Tx.SavePoint(name).Error
}

// PersonalWorkspaceReadOptions makes retained-row visibility and locking
// explicit for personal-workspace root and tree reads.
type PersonalWorkspaceReadOptions struct {
	// IncludeDeleted reads retained soft-deleted organizations.
	IncludeDeleted bool
	// ForUpdate locks the selected rows until the ambient transaction ends.
	ForUpdate bool
}

// PersonalWorkspaceOwnerLockOptions controls whether the lifecycle may lock a
// soft-deleted owner. The selected owner row is always locked FOR NO KEY
// UPDATE, which serializes lifecycle operations against each other without
// blocking foreign-key checks that reference the user row.
type PersonalWorkspaceOwnerLockOptions struct {
	IncludeDeleted bool
}

// PersonalWorkspacePersistence groups the persistence primitives whose
// invariants are specific to managed personal-workspace lifecycle operations.
// CommonPersistenceInterface embeds this contract so v1.6 consumers provide
// these capabilities at compile time.
type PersonalWorkspacePersistence interface {
	LockPersonalWorkspaceOwner(ctx context.Context, ownerUserID uint64, options PersonalWorkspaceOwnerLockOptions) error
	CreatePersonalWorkspaceRoot(ctx context.Context, organization *common_persistence_models.Organization) (created bool, err error)
	GetPersonalWorkspaceRootByOwner(ctx context.Context, ownerUserID uint64, options PersonalWorkspaceReadOptions) (*common_persistence_models.Organization, error)
	HasPersonalWorkspaceOrganizationsByOwner(ctx context.Context, ownerUserID uint64) (bool, error)
	GetPersonalWorkspaceTree(ctx context.Context, rootOrganizationID int64, ownerUserID uint64, options PersonalWorkspaceReadOptions) ([]*common_persistence_models.Organization, error)
	SetMissingPersonalWorkspaceTreeOwner(ctx context.Context, ownerUserID uint64, rootOrganizationID int64, organizationIDs []int64) error
	EnsurePersonalWorkspaceOwnerMembership(ctx context.Context, ownerUserID uint64, organizationID int64, role proto_models.OrganizationRole) (*PersonalWorkspaceMembershipEnsureResult, error)
	RepairPersonalWorkspaceOwnerMembershipRoles(ctx context.Context, ownerUserID uint64, organizationIDs []int64, role proto_models.OrganizationRole) error
	ListPersonalWorkspaceMemberships(ctx context.Context, organizationIDs []int64) ([]*common_persistence_models.UserOrganization, error)
	SoftDeletePersonalWorkspaceOrganizations(ctx context.Context, organizationIDs []int64, ownerUserID uint64) error
	RestorePersonalWorkspaceOrganizations(ctx context.Context, organizationIDs []int64, ownerUserID uint64) error
}

// PersonalWorkspaceMembershipChange reports exactly what an owner-membership
// ensure operation changed, without permitting contradictory boolean states.
type PersonalWorkspaceMembershipChange uint8

const (
	PersonalWorkspaceMembershipUnchanged PersonalWorkspaceMembershipChange = iota
	PersonalWorkspaceMembershipCreated
	PersonalWorkspaceMembershipRepaired
)

// PersonalWorkspaceMembershipEnsureResult returns the authoritative owner
// membership together with whether it was created, repaired, or already valid.
type PersonalWorkspaceMembershipEnsureResult struct {
	Membership *common_persistence_models.UserOrganization
	Change     PersonalWorkspaceMembershipChange
}

// CommonPersistenceInterface defines the contract for all persistence operations in the go-common library.
// It provides a comprehensive set of methods for database and object storage operations,
// following the repository pattern for clean separation of concerns.
//
// The interface supports:
//   - Database transactions through BeginTransaction and RunInTransaction
//   - User CRUD operations with email-based lookups
//   - Authentication token management with expiration and revocation
//   - Email verification workflows
//   - OAuth provider tracking for analytics
//   - User settings management
//   - File attachment lifecycle from upload to storage
//   - MinIO object storage operations
//   - Contact message persistence
//
// Database and object-storage operations accept context.Context to support:
//   - Request cancellation and timeouts
//   - Transaction propagation via context
//   - Distributed tracing through trace IDs
type CommonPersistenceInterface interface {
	PersonalWorkspacePersistence
	AgentStreamRecoveryPersistence
	WorkflowAgentRunRecoveryPersistence
	ChatNotificationPersistence
	ChatAgentMessagingPersistence
	ChatAgentGraphParkPersistence
	ChatAgentGraphBudgetPersistence
	ChatAgentGraphDrivePersistence

	// GetMetricsHandler returns the metrics handler for recording persistence-layer metrics.
	// This includes database connection stats, query performance, and MinIO operations.
	GetMetricsHandler() *common_metrics.CommonMetricsHandler

	// BeginTransaction starts a new database transaction.
	// Wrap the returned *gorm.DB in GormTransactionWithHooks and add it to a cloned
	// SRouter context for propagation. The owner commits or rolls back and drains hooks
	// only after a successful commit; Commit does not run hooks by itself.
	BeginTransaction(ctx context.Context) (*gorm.DB, error)

	// RunInTransaction runs fn with an isolated context carrying a new transaction,
	// or a savepoint of the ambient transaction. Errors and panics roll back fn's work
	// and discard its hooks; panics are re-raised. Successful post-commit hooks run
	// after a standalone commit or join the hook-capable ambient transaction.
	// See CommonPersistenceHandler.RunInTransaction for the full hook lifecycle.
	RunInTransaction(ctx context.Context, fn func(context.Context) error) error

	// User Management Methods

	// CreateUser creates a new user in the database.
	// The user's email must be unique. Returns ErrUserAlreadyExists if a user
	// with the same email already exists.
	CreateUser(ctx context.Context, user *common_persistence_models.User) error

	// GetUserByID retrieves a user by their unique ID.
	// Returns ErrUserNotFound if no user exists with the given ID.
	GetUserForUpdate(ctx context.Context, userID uint64) (*common_persistence_models.User, error)
	HasPendingAccountInvitation(ctx context.Context, userID uint64, email string) (bool, error)
	GetUserByID(ctx context.Context, id uint64) (*common_persistence_models.User, error)

	// GetUserByEmail retrieves a user by their email address.
	// Email lookup is case-insensitive. Returns ErrUserNotFound if no user exists
	// with the given email.
	GetUserByEmail(ctx context.Context, email string) (*common_persistence_models.User, error)

	// CheckUsernameExists checks if a username is already taken in the database.
	// Username lookup is case-insensitive. Returns true if the username exists,
	// false if it's available for use.
	CheckUsernameExists(ctx context.Context, username string) (bool, error)

	// UpdateUser updates an existing user's information.
	// The user must exist (ID must be set). It updates username, email, phone, names,
	// user type, ban status, bio, profile-picture reference, and updated_at. Identity,
	// provisioning, bot, creation/deletion, and last-login fields remain unchanged.
	// An email change also revokes prior invitation and verification links atomically.
	UpdateUser(ctx context.Context, user *common_persistence_models.User) error

	// UpdateUsernameWithAudit updates a user's username and records an immutable audit entry.
	// This operation is transactional: either both the username update and audit insert succeed,
	// or both fail. If the username is unchanged, the operation is a no-op.
	UpdateUsernameWithAudit(
		ctx context.Context,
		targetUserID uint64,
		newUsername string,
		actorUserID *uint64,
		reason string,
	) error

	// SetUserCacheInvalidator registers a callback invoked after user mutations
	// (ban, user-type change, username change, etc.) so the service layer can
	// evict the affected user from its auth cache. Implementations that wrap
	// another CommonPersistenceInterface (decorators) must forward this call to
	// the wrapped handler; embedding the interface does so automatically.
	SetUserCacheInvalidator(invalidator func(userID uint64) error)

	// Session Token Management

	// CreateSessionToken creates a new session token in the database.
	// The token must have a unique token string, user ID, type, and expiration time.
	CreateSessionToken(ctx context.Context, token *common_persistence_models.SessionToken) error

	// GetValidSessionToken retrieves a session token by its token string.
	// Only returns unexpired tokens whose revocation is absent or still future-dated,
	// and whose single-use state permits use. Missing rows return ErrSessionTokenNotFound;
	// known expired, effectively revoked, or consumed rows return ErrTokenExpired,
	// ErrTokenRevoked, or ErrTokenAlreadyUsed, respectively.
	GetValidSessionToken(ctx context.Context, token string) (*common_persistence_models.SessionToken, error)

	// ConsumeSecurityToken validates and marks used a single-use security token of the given
	// type, locking the owning user row before the token row. It requires an ambient
	// transaction and holds the user lock until that transaction ends. See the account lock
	// order in docs/backend/internal/persistence_and_schema.md.
	ConsumeSecurityToken(ctx context.Context, token string, tokenType proto_models.TokenType) (*common_persistence_models.SessionToken, error)

	// RevokeAllUserTokens revokes all tokens of a specific type for a user.
	// This is useful when a user changes their password (revoke all auth tokens)
	// or completes email verification (revoke all verification tokens).
	RevokeAllUserTokens(ctx context.Context, userID uint64, tokenType proto_models.TokenType) error

	// DeleteExpiredSessionTokens removes expired tokens from the database.
	// This should be called periodically as a cleanup task to prevent
	// the session_tokens table from growing indefinitely.
	DeleteExpiredSessionTokens(ctx context.Context) error

	// RevokeSessionToken revokes a specific session token by its token string.
	// This is used for single-session logout functionality.
	RevokeSessionToken(ctx context.Context, token string) error

	// RevokeSessionTokenAt revokes a session token effective at the given instant.
	// A future revokeAt is a pending revocation: the token keeps validating until then
	// (used by sliding-session refresh so in-flight requests survive the cookie switchover).
	RevokeSessionTokenAt(ctx context.Context, token string, revokeAt time.Time) error

	// RevokeAllSessionTokensForUser revokes all session tokens for a user.
	// This is used for "logout everywhere" functionality, forcing the user
	// to re-authenticate on all devices.
	RevokeAllSessionTokensForUser(ctx context.Context, userID uint64) error

	// RevokeAllSessionTokensForUserAndReturnTokens revokes all active authentication tokens for a user.
	// It returns the raw token strings only for server-side cache invalidation.
	RevokeAllSessionTokensForUserAndReturnTokens(ctx context.Context, userID uint64) ([]string, error)

	// Email Verification Management

	// CreateEmailVerification creates a new email verification record.
	// It tracks the issuance address, verification timestamp, and resend history.
	// Password credentials are stored separately in UserPassword.
	CreateEmailVerification(ctx context.Context, ev *common_persistence_models.EmailVerification) error

	// GetEmailVerificationByUserID retrieves email verification record by user ID.
	// Returns ErrEmailVerificationNotFound if no record exists for the user.
	GetEmailVerificationByUserID(ctx context.Context, userID uint64) (*common_persistence_models.EmailVerification, error)

	// MarkEmailVerified marks the user's current email as verified.
	// This upserts the email verification record, sets verified_at, and returns
	// the normalized email loaded as part of the update.
	MarkEmailVerified(ctx context.Context, userID uint64) (string, error)
	// DiscardUnverifiedSignupCredentials locks the user and, when the account is
	// a never-verified self-signup, soft-deletes its password/TOTP state and
	// email auth method and revokes its tokens. Call it before any flow that
	// marks the email verified on the strength of someone else's proof of
	// ownership.
	DiscardUnverifiedSignupCredentials(ctx context.Context, userID uint64) (DiscardedSignupCredentials, error)
	// ListUnverifiedUserIDs returns user IDs with unverified emails older than the cutoff.
	// This is used for background cleanup of abandoned registrations.
	ListUnverifiedUserIDs(ctx context.Context, cutoff time.Time, limit int) ([]uint64, error)

	// Phone Verification Management

	// CreatePhoneVerification creates a new phone verification record with OTP code.
	// This is used during phone number verification to track OTP status and cooldowns.
	CreatePhoneVerification(ctx context.Context, pv *common_persistence_models.PhoneVerification) error

	// GetPhoneVerificationByUserID retrieves active phone verification record by user ID.
	// Returns the most recent unverified phone verification record for the user.
	GetPhoneVerificationByUserID(ctx context.Context, userID uint64) (*common_persistence_models.PhoneVerification, error)

	// UpdatePhoneVerification updates an existing phone verification record.
	// This is used to update OTP codes, verification status, and attempt counts.
	UpdatePhoneVerification(ctx context.Context, pv *common_persistence_models.PhoneVerification) error

	// DeletePhoneVerification removes a phone verification record from the database.
	// This is typically called after successful verification or when cleaning up expired records.
	DeletePhoneVerification(ctx context.Context, id uint64) error

	// GetActiveTokensByUserID retrieves all active (non-expired, non-revoked) tokens for a user.
	// This is useful for displaying active sessions to the user or for security audits.
	GetActiveTokensByUserID(ctx context.Context, userID uint64) ([]*common_persistence_models.SessionToken, error)

	// GetActiveSessionTokensByUserID retrieves active authentication session tokens for a user.
	GetActiveSessionTokensByUserID(ctx context.Context, userID uint64) ([]*common_persistence_models.SessionToken, error)

	// ListActiveSessionTokensByUserID retrieves active authentication session tokens for a user with pagination.
	ListActiveSessionTokensByUserID(ctx context.Context, userID uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.SessionToken, *proto_models.PaginationResponse, error)

	// RevokeSessionTokenByIDForUser revokes one active authentication session owned by a user.
	// The returned token string is only for server-side cache invalidation.
	RevokeSessionTokenByIDForUser(ctx context.Context, userID uint64, sessionID uint64) (string, error)

	// RevokeOtherSessionTokensForUser revokes active authentication sessions for a user except the current token.
	// Returned token strings are only for server-side cache invalidation.
	RevokeOtherSessionTokensForUser(ctx context.Context, userID uint64, currentToken string) ([]string, error)

	// Multi-Auth Support

	// GetUserAuthMethods retrieves all authentication methods for a user.
	// This includes checking for password authentication and fetching OAuth providers.
	// Used for displaying auth methods in user settings and security checks.
	GetUserAuthMethods(ctx context.Context, userID uint64) (*common_persistence_models.UserAuthMethods, error)

	// OAuth Identities

	// FindOAuthIdentity returns the identity linked to (providerSlug, subject),
	// or ErrOAuthIdentityNotFound.
	FindOAuthIdentity(ctx context.Context, providerSlug, subject string) (*common_persistence_models.OAuthLogin, error)

	// FindUserOAuthIdentity locks and returns the user's identity for
	// providerSlug, or ErrOAuthIdentityNotFound. Callers lock the user row first.
	FindUserOAuthIdentity(ctx context.Context, userID uint64, providerSlug string) (*common_persistence_models.OAuthLogin, error)

	// LinkOAuthIdentity links (providerSlug, subject) to a user. It returns
	// ErrOAuthIdentityConflict when the user already has an identity for the
	// provider or another user holds the provider account.
	LinkOAuthIdentity(ctx context.Context, userID uint64, providerSlug, subject, email string) error

	// TouchOAuthIdentity records a login through an existing identity and fills
	// a legacy NULL subject. Filling a subject another user holds returns
	// ErrOAuthIdentityConflict; a missing row returns ErrOAuthIdentityNotFound.
	TouchOAuthIdentity(ctx context.Context, identityID uint64, subject, email string) error

	// User Settings Management

	// GetUserSettingsByUserID retrieves user settings by user ID.
	// Returns the user's preferences and configuration settings.
	// The shared implementation returns ErrUserNotFound when no settings row exists;
	// it does not create defaults on this read.
	GetUserSettingsByUserID(ctx context.Context, userID uint64) (*common_persistence_models.UserSettings, error)

	// UpsertUserSettings atomically inserts settings for a user without a row, or
	// otherwise updates only the named columns, then loads the stored row into
	// settings. The settings object must have a valid UserID field. A missing
	// user returns ErrUserNotFound.
	UpsertUserSettings(ctx context.Context, settings *common_persistence_models.UserSettings, columns []string) error

	// CreateUserSettings creates new user settings, leaving an existing row unchanged.
	// This is typically called when a new user is created to establish
	// their default preferences.
	CreateUserSettings(ctx context.Context, settings *common_persistence_models.UserSettings) error

	// Attachment Management

	// CreateAttachment creates a new attachment record in the database.
	// The attachment tracks file upload status, metadata, and storage locations.
	CreateAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error
	// CreateInternalAttachment is the privileged creation path for server-owned
	// provider state and tool media.
	CreateInternalAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error

	// GetAttachmentByID retrieves an attachment by its unique ID.
	// Returns the attachment metadata including upload status and storage paths.
	GetAttachmentByID(ctx context.Context, id uint64) (*common_persistence_models.Attachment, error)
	// GetInternalAttachmentByEntity is a privileged lookup for idempotent server-generated blobs.
	GetInternalAttachmentByEntity(ctx context.Context, objectType string, entityID string) (*common_persistence_models.Attachment, error)
	// PublishInternalAttachmentObject locks a processing internal row for the complete object write
	// and conditionally transitions it to vetted in the same database transaction. Reclamation
	// takes the same row lock, so it cannot delete the row while storage publication is in flight.
	// An object already stored under the content-addressed name is verified and reused.
	PublishInternalAttachmentObject(ctx context.Context, attachmentID uint64, objectName string, data []byte, contentType string) (*common_persistence_models.Attachment, error)
	// PublishPublicAttachmentObject serializes non-internal content-addressed publication under
	// the object's advisory lock and atomically records the vetted row metadata.
	// PublishPublicAttachmentObject promotes a validated temporary object to its
	// verified location and marks the row VETTED. source pins the temporary
	// object to the version that was validated; a mismatch fails with
	// ErrObjectPreconditionFailed and leaves the row unchanged.
	PublishPublicAttachmentObject(ctx context.Context, attachment *common_persistence_models.Attachment, source ObjectPrecondition) error
	ListInternalAttachmentReclaimCandidates(ctx context.Context, before time.Time, limit int) ([]*common_persistence_models.Attachment, error)
	TombstoneInternalAttachmentIfUnreferenced(ctx context.Context, attachmentID uint64, inactiveBefore time.Time) (bool, error)

	// UpdateAttachment updates an existing attachment's metadata.
	// This is used to track upload progress, validation results, and status changes.
	UpdateAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error
	// UpdateAttachmentIfStatus compare-and-swaps a non-internal attachment from the
	// expected lifecycle state, returning false when another worker won the race.
	UpdateAttachmentIfStatus(ctx context.Context, attachment *common_persistence_models.Attachment, expectedStatus common_persistence_models.UploadStatus) (bool, error)
	// ClaimExpiredPublicAttachments atomically rejects expired pending non-internal
	// uploads (upload_time and upload_url_expires_at both before cutoffTime) and
	// returns only rows whose temporary objects are safe to prune.
	ClaimExpiredPublicAttachments(ctx context.Context, cutoffTime, processingEndTime time.Time) ([]*common_persistence_models.Attachment, error)
	// RenewPendingUploads extends upload_url_expires_at to at least expiresAt for the given non-internal
	// attachments still waiting for their upload and returns exactly the renewed rows.
	RenewPendingUploads(ctx context.Context, attachmentIDs []uint64, expiresAt time.Time) ([]*common_persistence_models.Attachment, error)
	// RenewPendingUploadsInScope requires an ambient transaction and returns only live pending
	// uploads matching uploader/object type/entity exactly, with a non-empty temporary object.
	// The conditional UPDATE holds row locks through commit. It does not authorize the actor.
	RenewPendingUploadsInScope(ctx context.Context, attachmentIDs []uint64, userID uint64, objectType, entityID string, expiresAt time.Time) ([]*common_persistence_models.Attachment, error)

	// UpdateExpiredPendingUpload is UpdateAttachmentIfStatus from pending_upload for a row whose latest
	// upload URL expired before cutoffTime, returning false when finalization or a renewal won.
	UpdateExpiredPendingUpload(ctx context.Context, attachment *common_persistence_models.Attachment, cutoffTime time.Time) (bool, error)
	// ListSettledTemporaryObjects returns up to limit attachments, soft-deleted ones included,
	// whose temporary object is no longer needed (vetted, rejected, or deleted) but still named,
	// and whose upload URLs all expired before now.
	ListSettledTemporaryObjects(ctx context.Context, now time.Time, limit int) ([]*common_persistence_models.Attachment, error)
	// ClearTemporaryObjectName clears temporary_object_name on the row when it still equals objectName.
	ClearTemporaryObjectName(ctx context.Context, attachmentID uint64, objectName string) error

	// UpdateAttachmentObjectTypes updates the object type for multiple attachment rows.
	UpdateAttachmentObjectTypes(ctx context.Context, attachmentIDs []uint64, objectType string) error

	// GetAttachmentsByStatus retrieves attachments with a specific upload status.
	// The limit parameter controls the maximum number of results returned.
	// This is useful for batch processing of pending attachments.
	GetAttachmentsByStatus(ctx context.Context, status common_persistence_models.UploadStatus, limit int) ([]*common_persistence_models.Attachment, error)

	// DeleteAttachmentsByIDs soft-deletes the non-internal attachment records among ids.
	// Note: This does not remove the associated objects from MinIO storage.
	DeleteAttachmentsByIDs(ctx context.Context, ids []uint64) error

	// FindAttachmentByHash searches for a vetted attachment by its content hash within
	// one visibility class. This enables content deduplication by identifying identical
	// files. The visibility scope is mandatory: identical content has the same
	// FinalObjectName in both buckets, so an unscoped match could deduplicate a private
	// upload onto a public object and publish it (or the reverse). Matching is by
	// class: a public argument matches only ATTACHMENT_VISIBILITY_PUBLIC rows, any
	// other argument matches the private class (UNSPECIFIED and PRIVATE alike).
	FindAttachmentByHash(ctx context.Context, fileHash string, visibility proto_models.AttachmentVisibility) (*common_persistence_models.Attachment, error)

	// GetAttachmentsByIDs retrieves multiple attachments by their IDs without ownership validation.
	// This method should be used carefully as it doesn't enforce access control.
	GetAttachmentsByIDs(ctx context.Context, attachmentIDs []uint64) ([]*common_persistence_models.Attachment, error)

	// GetAttachmentsByIDsAndUser retrieves multiple attachments by their IDs for a specific user.
	// This method enforces ownership validation to ensure users can only access their own attachments.
	// Used for bulk operations and ownership verification.
	GetAttachmentsByIDsAndUser(ctx context.Context, attachmentIDs []uint64, userID uint64) ([]*common_persistence_models.Attachment, error)

	// GetChatMessageReferencedAttachmentIDs returns the set of ids that appear in any chat
	// message's attachment_ids array, soft-deleted messages included.
	GetChatMessageReferencedAttachmentIDs(ctx context.Context, ids []uint64) (map[uint64]bool, error)

	// GetChatHistoryReferencedAttachmentIDs returns the set of ids that appear in any chat
	// message edit-history attachment_ids array, through the ambient transaction.
	GetChatHistoryReferencedAttachmentIDs(ctx context.Context, ids []uint64) (map[uint64]bool, error)

	// CreateAttachments creates multiple attachment records in a single operation.
	// This method is used to avoid N+1 queries when creating multiple attachments.
	CreateAttachments(ctx context.Context, attachments []*common_persistence_models.Attachment) error

	// UpdateAttachmentEntityIDs updates the entity ID of the non-internal attachments among
	// attachmentIDs. Pass an empty string to clear the association.
	UpdateAttachmentEntityIDs(ctx context.Context, attachmentIDs []uint64, entityID string) error

	// UpdateUserProfilePicture updates a user's profile picture attachment reference.
	// This is typically called from entity callbacks when a profile picture is uploaded.
	// Pass nil for attachmentID to remove the current profile picture.
	UpdateUserProfilePicture(ctx context.Context, userID uint64, attachmentID *uint64) error

	// MinIO Object Storage Operations

	// GetPresignedURL generates a presigned GET URL for downloading an object.
	// The URL allows temporary access to the object without authentication.
	// The expiry parameter determines how long the URL remains valid.
	GetPresignedURL(ctx context.Context, objectName string, expiry time.Duration) (string, error)

	// PresignHostScopedDownloadURL signs a path-style GET for an object in the private
	// bucket against a public origin, entirely offline, so an edge SigV4 verifier that
	// reconstructs the canonical request from the public host accepts the URL without
	// any host rewrite. publicOrigin is "host[:port]" or "scheme://host[:port]" (scheme
	// defaults to https) and must not carry a path, query, fragment, or userinfo.
	// signingTime determines X-Amz-Date, so callers that quantize it obtain
	// byte-identical URLs within the quantum window. accessKey/secretKey select a
	// dedicated signing credential (both set, or both empty to fall back to the
	// MinIO credential); URLs signed with a dedicated pair verify only at an edge
	// holding the same secret, never at MinIO itself.
	PresignHostScopedDownloadURL(ctx context.Context, objectName string, publicOrigin string, region string, expiry time.Duration, signingTime time.Time, accessKey string, secretKey config.Secret) (string, error)

	// GeneratePresignedPostURL generates a presigned POST URL with policy constraints
	// including file size limits. This is more secure than PUT URLs as it can enforce
	// content-length ranges and other conditions. The policy expires at expiresAt.
	// Returns the URL and form data needed for the POST request.
	GeneratePresignedPostURL(ctx context.Context, objectName string, expiresAt time.Time, minSize, maxSize int64, contentType string) (url string, formData map[string]string, err error)

	// GetObjectReader returns a reader for streaming access to an object.
	// This enables memory-efficient reading of large files without loading
	// the entire content into memory. The caller must close the reader.
	// A non-zero precondition binds every read (including re-reads after a
	// seek) to the pinned version; a mismatch fails with
	// ErrObjectPreconditionFailed either at open or from a later Read.
	GetObjectReader(ctx context.Context, objectName string, precondition ObjectPrecondition) (io.ReadCloser, error)

	// PutObjectBytes stores a byte slice as an object in MinIO.
	// The contentType parameter sets the MIME type for the stored object.
	// Unlike PutObjectStream it accepts every key, including the attachment-owned
	// temp/ and verified/ prefixes, because internal attachment publication
	// writes through it.
	PutObjectBytes(ctx context.Context, objectName string, data []byte, contentType string) error

	// PutObjectStream streams reader into the private bucket with the response
	// headers in opts. The caller owns reader and closes it; its bytes are
	// uploaded unchanged. A non-negative size is the exact length of the stream
	// as uploaded (after any compression the caller applied): a shorter stream
	// fails and bytes past size are not uploaded. size -1 means unknown length
	// and uploads in object_storage_stream_part_size parts, sampled once per
	// call, which caps the object at that part size times 10,000; a longer
	// stream fails with ErrObjectStreamTooLarge and stores nothing. A reader that
	// is also an io.ReaderAt and io.Seeker is uploaded from its current offset
	// and may be read in parallel; its position afterwards is unspecified. Keys
	// under the attachment-owned temp/ and verified/ prefixes are rejected with
	// ErrReservedObjectName.
	PutObjectStream(ctx context.Context, objectName string, reader io.Reader, size int64, opts PutObjectOptions) error

	// CopyObject performs a server-side copy of an object within MinIO.
	// This is more efficient than downloading and re-uploading for large files.
	// source pins the copy to a previously observed version of sourceObject;
	// a mismatch fails with ErrObjectPreconditionFailed and writes nothing.
	CopyObject(ctx context.Context, sourceObject, destObject string, source ObjectPrecondition) error

	// CopyObjectToPublicBucket performs a server-side copy of an object from the
	// private bucket into the anonymously readable public bucket, honoring the
	// same source precondition as CopyObject.
	// Returns ErrPublicStorageDisabled when no public bucket is configured.
	CopyObjectToPublicBucket(ctx context.Context, sourceObject, destObject string, source ObjectPrecondition) error

	// RemoveObject deletes an object from MinIO storage.
	// This operation is permanent and cannot be undone.
	RemoveObject(ctx context.Context, objectName string) error

	// ObjectExists checks if an object exists in MinIO storage.
	// Returns true if the object exists, false if it doesn't.
	// Any error other than "object not found" is returned as an error.
	ObjectExists(ctx context.Context, objectName string) (bool, error)

	// PublicStorageEnabled reports whether a public bucket is configured.
	// When false, public-bucket operations fail with ErrPublicStorageDisabled
	// and attachments must remain private.
	PublicStorageEnabled() bool

	// ObjectExistsInPublicBucket checks if an object exists in the public bucket.
	// The public and private buckets are independent namespaces, so this never
	// reports objects that only exist privately.
	// Returns ErrPublicStorageDisabled when no public bucket is configured.
	ObjectExistsInPublicBucket(ctx context.Context, objectName string) (bool, error)

	// GetObjectInfo retrieves metadata about an object without downloading its content.
	// This is useful for checking file size, content type, and other properties
	// without loading the entire object into memory.
	GetObjectInfo(ctx context.Context, objectName string) (*MinioObjectInfo, error)

	// CalculateDualHashesStream computes both salted and unsalted SHA256 hashes
	// of an object's content in a single pass using streaming. The precondition
	// pins the read to a previously observed version (see GetObjectReader).
	// This is more efficient than calculating hashes separately and prevents
	// loading the entire file into memory.
	// Returns a DualHashes struct containing both hash values.
	CalculateDualHashesStream(ctx context.Context, objectName string, precondition ObjectPrecondition) (*DualHashes, error)

	// Contact Message Management

	// CreateContactMessage stores a new contact form submission.
	// The message includes user details, subject, and message content.
	CreateContactMessage(ctx context.Context, message *common_persistence_models.ContactMessage) error

	// Careers Management

	// ListCareerJobPostings retrieves public career job postings using public-facing filters.
	ListCareerJobPostings(ctx context.Context, req *proto_models.ListCareerJobPostingsRequest) ([]*common_persistence_models.CareerJobPosting, *proto_models.PaginationResponse, error)

	// GetCareerJobPostingByID retrieves a career job posting by ID.
	GetCareerJobPostingByID(ctx context.Context, id uint64) (*common_persistence_models.CareerJobPosting, error)

	// GetCareerJobPostingBySlug retrieves a career job posting by slug.
	GetCareerJobPostingBySlug(ctx context.Context, slug string) (*common_persistence_models.CareerJobPosting, error)

	// CreateCareerJobPosting stores a new career job posting.
	CreateCareerJobPosting(ctx context.Context, job *common_persistence_models.CareerJobPosting) error

	// UpdateCareerJobPosting updates an existing career job posting.
	UpdateCareerJobPosting(ctx context.Context, job *common_persistence_models.CareerJobPosting) error

	// CreateCareerApplication stores a new career application.
	CreateCareerApplication(ctx context.Context, application *common_persistence_models.CareerApplication) error

	// GetCareerApplicationByID retrieves a career application by ID.
	GetCareerApplicationByID(ctx context.Context, id uint64) (*common_persistence_models.CareerApplication, error)

	// GetCareerApplicationByReferenceID retrieves a career application by reference ID.
	GetCareerApplicationByReferenceID(ctx context.Context, referenceID string) (*common_persistence_models.CareerApplication, error)

	// ListCareerApplicationsByApplicantUserID retrieves owner-scoped career applications for self-service.
	ListCareerApplicationsByApplicantUserID(ctx context.Context, applicantUserID uint64, req *proto_models.ListMyCareerApplicationsRequest) ([]*common_persistence_models.CareerApplication, *proto_models.PaginationResponse, error)

	// GetCareerApplicationByReferenceIDAndApplicantUserID retrieves an owner-scoped application by reference ID.
	GetCareerApplicationByReferenceIDAndApplicantUserID(ctx context.Context, referenceID string, applicantUserID uint64) (*common_persistence_models.CareerApplication, error)

	// UpdateCareerApplicationStatus updates a career application's status and admin notes.
	UpdateCareerApplicationStatus(ctx context.Context, applicationID uint64, status proto_models.CareerApplicationStatus, adminNotes string) error

	// Lifecycle Management

	// Close gracefully shuts down the persistence handler. It always cancels
	// and waits for handler-owned background work, and closes the database pool
	// only when the handler opened or adopted that pool.
	Close() error

	// ============================================================================
	// NEW SIMPLIFIED MULTI-AUTH METHODS
	// ============================================================================

	// CreateUserPassword creates a password authentication record for a user.
	// This is used when a user sets up password authentication for the first time.
	CreateUserPassword(ctx context.Context, userID uint64, passwordHash, passwordSalt string) error

	// GetUserPassword retrieves the password authentication for a user.
	// Returns nil if no password is set.
	GetUserPassword(ctx context.Context, userID uint64) (*common_persistence_models.UserPassword, error)

	// UpdateUserPassword updates an existing password authentication.
	UpdateUserPassword(ctx context.Context, userID uint64, passwordHash, passwordSalt string) error

	// OTP/TOTP 2FA Methods

	// UpdateUserOTPSecret updates the OTP secret for a user (during setup).
	// This stores the secret temporarily before OTP is enabled.
	UpdateUserOTPSecret(ctx context.Context, userID uint64, otpSecret string) error

	// EnableUserOTP enables OTP for a user and sets the verification timestamp.
	// This completes the OTP setup process after successful code verification.
	EnableUserOTP(ctx context.Context, userID uint64, verifiedAt *time.Time) error

	// DisableUserOTP disables OTP for a user and clears the secret.
	// This removes OTP authentication and clears the secret from storage.
	DisableUserOTP(ctx context.Context, userID uint64) error

	// ============================================================================
	// ADMIN MANAGEMENT METHODS
	// ============================================================================

	// Admin User Management

	// AdminListUsers retrieves a paginated list of users returning service domain models.
	// Supports searching by email/username, filtering by user type and banned status,
	// and sorting by various fields. Returns detailed admin user information.
	AdminListUsers(ctx context.Context, req *proto_models.AdminListUsersRequest) ([]*common_service_models.AdminUserDetails, *proto_models.PaginationResponse, error)

	// AdminGetUser retrieves detailed statistics for a specific user as service domain model.
	// Returns login count, active sessions, storage usage, and other admin-relevant metrics.
	AdminGetUser(ctx context.Context, userID uint64) (*common_service_models.AdminUserDetails, error)

	// AdminListUsernameChangeHistory retrieves paginated username change audit entries.
	// Supports filtering by target user, actor user, username text, time range, and sort options.
	AdminListUsernameChangeHistory(ctx context.Context, req *proto_models.AdminListUsernameChangeHistoryRequest) ([]*common_persistence_models.UsernameChangeHistory, *proto_models.PaginationResponse, error)

	// AdminUpdateUserBanStatus updates a user's banned status with audit logging.
	// Records the admin action with reason for compliance and auditing purposes.
	AdminUpdateUserBanStatus(ctx context.Context, userID uint64, banned bool, reason string) error

	// AdminUpdateUserType changes a user's type/role with audit logging.
	// Records the admin action with reason for compliance and auditing purposes.
	AdminUpdateUserType(ctx context.Context, userID uint64, newUserType proto_models.UserType, reason string) error

	// AdminDeleteUser soft-deletes a user, tombstones their unique identity fields,
	// and optionally deletes their attachments. This is a destructive operation
	// that should be used carefully.
	// Records the admin action with reason for compliance and auditing purposes.
	AdminDeleteUser(ctx context.Context, userID uint64, deleteAttachments bool, reason string) error

	// DeleteUserIfUnverified soft-deletes and tombstones a user only if their
	// email is still unverified, returning whether the deletion happened. The
	// unverified check and the deletion are atomic, so a concurrently
	// verifying user is never deleted. Joins a caller-provided context
	// transaction when present; otherwise runs in its own transaction.
	DeleteUserIfUnverified(ctx context.Context, userID uint64, deleteAttachments bool, reason string) (bool, error)

	// Admin Contact Message Management

	// AdminListContactMessages retrieves a paginated list of contact messages with filtering.
	// Supports filtering by status, email, and sorting by various fields.
	AdminListContactMessages(ctx context.Context, pagination *proto_models.PaginationRequest, statusFilter, emailFilter string, sort *proto_models.SortRequest, sortBy proto_models.AdminListContactMessagesSortBy) ([]*proto_models.AdminContactMessage, *proto_models.PaginationResponse, error)

	// AdminGetContactMessageDetails retrieves detailed information about a specific contact message.
	AdminGetContactMessageDetails(ctx context.Context, messageID uint64) (*proto_models.AdminContactMessage, error)

	// AdminGetContactMessageByReferenceID retrieves a contact message by its reference ID.
	AdminGetContactMessageByReferenceID(ctx context.Context, referenceID string) (*proto_models.AdminContactMessage, error)

	// AdminUpdateContactMessageStatus updates the status and admin notes for a contact message.
	AdminUpdateContactMessageStatus(ctx context.Context, messageID uint64, status, adminNotes string) error

	// AdminDeleteContactMessage deletes a contact message with audit logging.
	AdminDeleteContactMessage(ctx context.Context, messageID uint64, reason string) error

	// Admin Careers Management

	// AdminListCareerJobPostings retrieves a paginated list of job postings with admin filters.
	AdminListCareerJobPostings(ctx context.Context, req *proto_models.AdminListCareerJobPostingsRequest) ([]*proto_models.CareerJobPosting, *proto_models.PaginationResponse, error)

	// AdminGetCareerJobPosting retrieves a specific job posting by ID.
	AdminGetCareerJobPosting(ctx context.Context, jobPostingID uint64) (*proto_models.CareerJobPosting, error)

	// AdminListCareerApplications retrieves a paginated list of applications with admin filters.
	AdminListCareerApplications(ctx context.Context, req *proto_models.AdminListCareerApplicationsRequest) ([]*common_persistence_models.CareerApplication, *proto_models.PaginationResponse, error)

	// Admin Attachment Management

	// AdminListAttachments retrieves a paginated list of attachments with filtering.
	// Supports filtering by user, status, content type, size range, and sorting.
	AdminListAttachments(ctx context.Context, pagination *proto_models.PaginationRequest, userIDFilter *uint64, statusFilter, contentTypeFilter string, minSizeBytes, maxSizeBytes uint64, sort *proto_models.SortRequest, sortBy proto_models.AdminListAttachmentsSortBy) ([]*common_persistence_models.Attachment, *proto_models.PaginationResponse, error)

	// AdminGetAttachmentDetails retrieves detailed information about a specific attachment.
	AdminGetAttachmentDetails(ctx context.Context, attachmentID uint64) (*common_persistence_models.Attachment, error)

	// AdminUpdateAttachmentStatus updates the validation status of an attachment.
	AdminUpdateAttachmentStatus(ctx context.Context, attachmentID uint64, status, validationDetails string) error

	// AdminDeleteAttachment deletes an attachment and optionally its files with audit logging.
	AdminDeleteAttachment(ctx context.Context, attachmentID uint64, deleteFiles bool, reason string) error

	// Admin Chat Channel Management

	// AdminListChatChannels retrieves a paginated list of chat channels with filtering.
	AdminListChatChannels(ctx context.Context, req *proto_models.AdminListChatChannelsRequest) ([]*common_service_models.AdminChatChannelDetails, *proto_models.PaginationResponse, error)

	// AdminGetChatChannel retrieves a specific chat channel by ID.
	AdminGetChatChannel(ctx context.Context, channelID uint64, includeDeleted bool) (*common_service_models.AdminChatChannelDetails, error)

	// AdminUpdateChatChannel updates a chat channel and its feature flags.
	AdminUpdateChatChannel(ctx context.Context, req *proto_models.AdminUpdateChatChannelRequest) (*common_service_models.AdminChatChannelDetails, error)

	// AdminDeleteChatChannel soft-deletes a chat channel.
	AdminDeleteChatChannel(ctx context.Context, channelID uint64, reason string) error

	// AdminListChatChannelParticipants retrieves a paginated list of channel participants.
	AdminListChatChannelParticipants(ctx context.Context, channelID uint64, pagination *proto_models.PaginationRequest, includeLeft bool) ([]*common_service_models.ChatChannelParticipant, *proto_models.PaginationResponse, error)

	// AdminAddChatChannelParticipants adds participants to a channel.
	AdminAddChatChannelParticipants(ctx context.Context, channelID uint64, userIDs []uint64) ([]*common_service_models.ChatChannelParticipant, error)

	// AdminRemoveChatChannelParticipant removes a participant from a channel.
	AdminRemoveChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64) error

	// AdminUpdateChatChannelParticipantRole updates a participant role in a channel.
	AdminUpdateChatChannelParticipantRole(ctx context.Context, channelID uint64, userID uint64, role proto_models.ChannelRole) (*common_service_models.ChatChannelParticipant, error)

	// Admin Analytics

	// AdminGetUserAnalytics retrieves user registration and activity analytics.
	// Supports grouping by time periods and filtering by date ranges.
	AdminGetUserAnalytics(ctx context.Context, filter *proto_models.FilterRequest, groupBy string) (*proto_models.AdminUserAnalyticsResponse, error)

	// AdminGetStorageAnalytics retrieves storage usage analytics.
	// Includes breakdowns by user type, content type, and growth over time.
	AdminGetStorageAnalytics(ctx context.Context, filter *proto_models.FilterRequest, groupBy string) (*proto_models.AdminStorageAnalyticsResponse, error)

	// Admin Token Management

	// AdminListTokens retrieves a paginated list of authentication tokens with filtering.
	// Supports filtering by user, token type, status, metadata search, raw token lookup, and sorting.
	AdminListTokens(ctx context.Context, req *proto_models.AdminListTokensRequest) ([]*common_service_models.AdminSessionToken, *proto_models.PaginationResponse, error)

	// AdminRevokeToken revokes a specific token by its ID with audit logging.
	AdminRevokeToken(ctx context.Context, tokenID, reason string) error

	// Inference Model Catalog Management

	// CreateInferenceModel creates a supported inference model row.
	CreateInferenceModel(ctx context.Context, model *common_persistence_models.InferenceModel) error

	// UpdateInferenceModel updates a supported inference model row.
	UpdateInferenceModel(ctx context.Context, model *common_persistence_models.InferenceModel) error

	// DeleteInferenceModel deletes a supported inference model row.
	DeleteInferenceModel(ctx context.Context, inferenceModelID uint64) error

	// ListInferenceModels lists all supported inference model rows.
	ListInferenceModels(ctx context.Context) ([]*common_persistence_models.InferenceModel, error)

	// ListInferenceModelsPage lists one admin-facing page without loading the
	// complete model catalog used by runtime model resolution.
	ListInferenceModelsPage(ctx context.Context, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.InferenceModel, *proto_models.PaginationResponse, error)

	// GetInferenceModelByID retrieves a supported inference model row by ID.
	GetInferenceModelByID(ctx context.Context, inferenceModelID uint64) (*common_persistence_models.InferenceModel, error)

	// GetInferenceModelByProviderModelID retrieves a supported inference model row by provider/model pair.
	GetInferenceModelByProviderModelID(ctx context.Context, provider string, providerModelID string) (*common_persistence_models.InferenceModel, error)

	// CountInferenceModels returns the total number of supported inference model rows.
	CountInferenceModels(ctx context.Context) (int64, error)

	// SeedInferenceModelsIfEmpty seeds supported inference model rows when the table is empty.
	SeedInferenceModelsIfEmpty(ctx context.Context, models []*common_persistence_models.InferenceModel) (bool, int64, error)

	// Admin System Information

	// AdminGetSystemStats retrieves comprehensive system statistics.
	// Includes user counts, storage usage, system uptime, and performance metrics.
	AdminGetSystemStats(ctx context.Context) (*proto_models.AdminSystemStatsResponse, error)

	// ============================================================================
	// ORGANIZATION MANAGEMENT METHODS
	// ============================================================================

	// Organization Management Methods

	// CreateOrganization creates a new organization in the database.
	// The organization's slug must be unique (case-insensitive). Slugs are normalized to lowercase on write.
	// Returns ErrOrganizationAlreadyExists if another organization has the same slug.
	CreateOrganization(ctx context.Context, organization *common_persistence_models.Organization) error

	// GetOrganizationByID retrieves an organization by its unique ID.
	// Returns ErrOrganizationNotFound if no organization exists with the given ID.
	GetOrganizationByID(ctx context.Context, id int64) (*common_persistence_models.Organization, error)

	// GetOrganizationBySlug retrieves an organization by its unique slug.
	// Slug lookup is case-insensitive. Returns ErrOrganizationNotFound if no organization
	// exists with the given slug.
	GetOrganizationBySlug(ctx context.Context, slug string) (*common_persistence_models.Organization, error)

	// UpdateOrganization updates an existing organization's user-editable name,
	// slug, and description. Ownership, hierarchy, type, and avatar are managed
	// through their dedicated service paths.
	UpdateOrganization(ctx context.Context, organization *common_persistence_models.Organization) error

	// DeleteOrganization performs a soft delete on an organization by its ID.
	// This sets the DeletedAt timestamp rather than removing the record.
	// Also removes all user-organization relationships for the organization.
	DeleteOrganization(ctx context.Context, id int64) error

	// Organization email domain management
	CreateOrganizationEmailDomain(ctx context.Context, domain *common_persistence_models.OrganizationEmailDomain) error
	DeleteOrganizationEmailDomain(ctx context.Context, id uint64) error
	GetOrganizationEmailDomainByDomain(ctx context.Context, domain string) (*common_persistence_models.OrganizationEmailDomain, error)
	GetOrganizationEmailDomainByID(ctx context.Context, id uint64) (*common_persistence_models.OrganizationEmailDomain, error)
	ListOrganizationEmailDomains(ctx context.Context, organizationID int64) ([]*common_persistence_models.OrganizationEmailDomain, error)
	ListOrganizationEmailDomainsPage(ctx context.Context, organizationID int64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.OrganizationEmailDomain, *proto_models.PaginationResponse, error)

	// ListOrganizations retrieves a paginated list of organizations.
	// Parameters:
	//   - offset: number of records to skip
	//   - limit: maximum number of records to return (0 for no limit)
	//   - search: optional search term to filter by name or slug
	// Results are ordered by creation time (newest first).
	ListOrganizations(ctx context.Context, offset, limit int, search string, organizationTypeFilter proto_models.OrganizationType, sortColumn string, sortDesc bool) ([]*common_persistence_models.Organization, int64, error)
	ListAdminOrganizations(ctx context.Context, query *AdminOrganizationsQuery) ([]*common_persistence_models.Organization, int64, error)
	ListAdminOrganizationParentOptions(ctx context.Context, query *AdminOrganizationParentOptionsQuery) ([]*AdminOrganizationFilterParentOption, error)
	GetAdminOrganizationFilterMetadata(ctx context.Context, search string, organizationTypeFilter proto_models.OrganizationType) (*AdminOrganizationFilterMetadata, error)
	// UpdateOrganizationType updates an organization's type.
	UpdateOrganizationType(ctx context.Context, organizationID int64, organizationType proto_models.OrganizationType) error

	// User-Organization Relationship Methods

	// AddUserToOrganization creates a new user-organization relationship.
	// Returns ErrUserAlreadyInOrganization if the user is already a member.
	AddUserToOrganization(ctx context.Context, userID uint64, organizationID int64, role proto_models.OrganizationRole) error

	// RemoveUserFromOrganizationAndDescendants removes a user from an organization and its descendants.
	// Returns ErrUserOrganizationNotFound if the relationship doesn't exist in the root organization.
	RemoveUserFromOrganizationAndDescendants(ctx context.Context, userID uint64, organizationID int64) error

	// RemoveUserFromOrganizationAndDescendantsGuardingLastOwner removes a user from an organization
	// and its descendants while atomically refusing to remove any affected organization's only
	// explicit owner. Every organization whose explicit membership row may be deleted is locked in
	// deterministic ID order and checked from explicit owner rows, so inherited ownership never
	// blocks the removal and concurrent guarded role changes cannot race the hierarchy delete. Returns
	// ErrOrganizationLastOwner when blocked and ErrUserOrganizationNotFound if the relationship
	// doesn't exist in the root organization or any descendant.
	RemoveUserFromOrganizationAndDescendantsGuardingLastOwner(ctx context.Context, userID uint64, organizationID int64, ownerRole proto_models.OrganizationRole) error

	// UpdateUserOrganizationRole updates a user's role within an organization.
	// Returns ErrUserOrganizationNotFound if the relationship doesn't exist.
	UpdateUserOrganizationRole(ctx context.Context, userID uint64, organizationID int64, role proto_models.OrganizationRole) error

	// UpdateUserOrganizationRoleGuardingLastOwner updates a user's explicit role while atomically
	// refusing a demotion below ownerRole when the user is the organization's only explicit owner.
	// Ownership is evaluated from explicit rows only, so inherited ownership never blocks the change,
	// and the check and update are serialized by a per-organization advisory lock. Returns
	// ErrOrganizationLastOwner when blocked and ErrUserOrganizationNotFound if the relationship
	// doesn't exist.
	UpdateUserOrganizationRoleGuardingLastOwner(ctx context.Context, userID uint64, organizationID int64, newRole, ownerRole proto_models.OrganizationRole) error

	// GetUserExplicitOrganizationRole retrieves a user's explicit role within a specific organization.
	// Returns ErrUserOrganizationNotFound if the relationship doesn't exist.
	GetUserExplicitOrganizationRole(ctx context.Context, userID uint64, organizationID int64) (proto_models.OrganizationRole, error)
	// GetUserEffectiveOrganizationRoleByLineage retrieves explicit and effective
	// role information for a user in an organization using the target org lineage.
	// EffectiveRole is computed as the max explicit role across lineage ancestors
	// and the target organization. Returns ErrOrganizationNotFound if the target
	// organization does not exist.
	GetUserEffectiveOrganizationRoleByLineage(ctx context.Context, userID uint64, organizationID int64) (OrganizationEffectiveRoleResult, error)
	// GetUsersEffectiveOrganizationRolesByLineage retrieves explicit and effective
	// role information for many users in one query for the target organization.
	// The returned map is keyed by user ID and includes only users with at least
	// one explicit membership in the target organization lineage.
	GetUsersEffectiveOrganizationRolesByLineage(ctx context.Context, userIDs []uint64, organizationID int64) (map[uint64]OrganizationEffectiveRoleResult, error)

	// GetUserOrganization retrieves a specific user-organization membership record.
	// Returns ErrUserOrganizationNotFound if the relationship doesn't exist.
	GetUserOrganization(ctx context.Context, userID uint64, organizationID int64) (*common_persistence_models.UserOrganization, error)

	// GetUserOrganizations retrieves organizations a user belongs to with role information.
	// Query options control filtering, sorting, and pagination; nil preserves the broad default behavior.
	GetUserOrganizations(ctx context.Context, userID uint64, query *UserOrganizationsQuery) ([]*UserOrganizationWithRoleInfo, int64, error)

	// GetOrganizationUsers retrieves all users belonging to an organization with their roles.
	// Parameters:
	//   - organizationID: the organization to get users for
	//   - roleFilter: optional role filter (use ORGANIZATION_ROLE_UNSPECIFIED for no filter)
	//   - offset: number of records to skip
	//   - limit: maximum number of records to return (0 for no limit)
	// Results are ordered by join time (most recent first).
	GetOrganizationUsers(ctx context.Context, organizationID int64, roleFilter proto_models.OrganizationRole, offset, limit int) ([]*common_persistence_models.UserOrganization, int64, error)

	// Bulk Operations

	// GetOrganizationsByIDs retrieves multiple organizations by their IDs.
	// Returns organizations in the same order as the input IDs where they exist.
	// Non-existent IDs are silently skipped.
	GetOrganizationsByIDs(ctx context.Context, organizationIDs []int64) ([]*common_persistence_models.Organization, error)
	// GetOrganizationsByParent retrieves organizations that have the given parent (or root orgs when parent is nil).
	// Returns the total count before pagination.
	GetOrganizationsByParent(ctx context.Context, parentOrganizationID *int64, offset, limit int) ([]*common_persistence_models.Organization, int64, error)
	// GetOrganizationsByParentForUser retrieves organizations by parent that the user is a member of.
	// Returns the total count before pagination.
	GetOrganizationsByParentForUser(ctx context.Context, userID uint64, parentOrganizationID *int64, offset, limit int) ([]*common_persistence_models.Organization, int64, error)
	// GetOrganizationDescendantsPage returns an ID-ordered bounded page, excluding the root.
	GetOrganizationDescendantsPage(ctx context.Context, organizationID, afterOrganizationID int64, limit int) ([]*common_persistence_models.Organization, bool, error)

	// GetOrganizationsByLineageLimited retrieves a bounded root/descendant set and reports whether more nodes exist.
	GetOrganizationsByLineageLimited(ctx context.Context, organizationID int64, maxNodes int) ([]*common_persistence_models.Organization, bool, error)
	// GetOrganizationsByLineageForUserLimited is the membership-filtered bounded hierarchy query.
	GetOrganizationsByLineageForUserLimited(ctx context.Context, userID uint64, organizationID int64, maxNodes int) ([]*common_persistence_models.Organization, bool, error)
	// GetDescendantOrganizationIDs retrieves IDs for an organization and its descendants.
	// Returns ErrOrganizationNotFound if the root organization does not exist.
	GetDescendantOrganizationIDs(ctx context.Context, organizationID int64) ([]uint64, error)

	// UpdateOrganizationAvatar updates an organization's avatar attachment reference.
	// Pass nil for attachmentID to remove the current avatar.
	UpdateOrganizationAvatar(ctx context.Context, organizationID int64, attachmentID *uint64) error

	// GetOrganizationMemberCountAtOrAboveRole returns the count of members holding the given
	// role or a higher one (e.g. all owner tiers when passed the lowest owner role).
	GetOrganizationMemberCountAtOrAboveRole(ctx context.Context, organizationID int64, minRole proto_models.OrganizationRole) (int64, error)

	// GetOrganizationMemberCount returns the total number of members in an organization.
	GetOrganizationMemberCount(ctx context.Context, organizationID int64) (int64, error)

	// GetOrganizationMemberCounts returns member counts for multiple organizations.
	// Missing IDs are omitted from the result map.
	GetOrganizationMemberCounts(ctx context.Context, organizationIDs []int64) (map[int64]int64, error)

	// ============================================================================
	// ORGANIZATION INVITATION METHODS
	// ============================================================================

	// GetUserByUsername retrieves a user by their username.
	// Username lookup is case-insensitive. Returns ErrUserNotFound if no user exists
	// with the given username.
	GetUserByUsername(ctx context.Context, username string) (*common_persistence_models.User, error)

	// SearchPublicUsers retrieves users for public search results with pagination.
	// Results exclude banned and unverified users and can be scoped to organizations.
	SearchPublicUsers(
		ctx context.Context,
		query string,
		scope proto_models.UserSearchScope,
		organizationID *uint64,
		excludeUserID *uint64,
		allowEmail bool,
		allowUsername bool,
		allowRealName bool,
		pagination *proto_models.PaginationRequest,
		suggestionsOnly bool,
	) ([]*common_persistence_models.User, *proto_models.PaginationResponse, error)

	// CreateOrganizationInvitation creates a new organization invitation.
	// Returns ErrInvitationAlreadyExists if a pending invitation already exists for
	// the same email and organization.
	CreateOrganizationInvitation(ctx context.Context, invitation *common_persistence_models.OrganizationInvitation) error

	// GetOrganizationInvitationByID retrieves an invitation by its unique ID.
	// Returns ErrInvitationNotFound if no invitation exists with the given ID.
	GetOrganizationInvitationByID(ctx context.Context, id uint64) (*common_persistence_models.OrganizationInvitation, error)
	// LoadOrganizationInvitationNotificationSources loads fresh authority in bounded batches.
	LoadOrganizationInvitationNotificationSources(ctx context.Context, invitationIDs, recipientIDs []uint64) (*OrganizationInvitationNotificationSources, error)

	// GetPendingInvitationByEmailAndOrg retrieves a pending invitation for a specific email and organization.
	// Returns ErrInvitationNotFound if no pending invitation exists.
	GetPendingInvitationByEmailAndOrg(ctx context.Context, email string, organizationID int64) (*common_persistence_models.OrganizationInvitation, error)

	// GetPendingInvitationsByEmail retrieves all pending invitations for a specific email address.
	// This is used during user registration to process pending invitations.
	// Results include the Organization relation preloaded for display.
	GetPendingInvitationsByEmail(ctx context.Context, email string) ([]*common_persistence_models.OrganizationInvitation, error)
	// GetPendingInvitationsByEmailPage retrieves a bounded page for the authenticated-user list API.
	GetPendingInvitationsByEmailPage(ctx context.Context, email string, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.OrganizationInvitation, *proto_models.PaginationResponse, error)
	// GetPendingAutoAddEntriesByEmail retrieves pending auto-add entries (requires_acceptance = false) for a specific email.
	GetPendingAutoAddEntriesByEmail(ctx context.Context, email string) ([]*common_persistence_models.OrganizationInvitation, error)

	// MarkOrganizationInvitationAccepted atomically marks a pending invitation as accepted.
	// Returns ErrInvitationNotFound when no invitation exists and ErrInvitationAlreadyProcessed
	// when the invitation was already accepted or declined.
	MarkOrganizationInvitationAccepted(ctx context.Context, id uint64) error

	// MarkOrganizationInvitationDeclined atomically marks a pending invitation as declined.
	// Returns ErrInvitationNotFound when no invitation exists and ErrInvitationAlreadyProcessed
	// when the invitation was already accepted or declined.
	MarkOrganizationInvitationDeclined(ctx context.Context, id uint64) error

	// DeleteExpiredInvitations removes expired invitations from the database.
	// This should be called periodically as a cleanup task.
	DeleteExpiredInvitations(ctx context.Context) error

	// DeleteOrganizationInvitation deletes a still-pending invitation by ID.
	// This is used to cancel pending invitations. Returns ErrInvitationAlreadyProcessed
	// when the invitation was already accepted or declined.
	DeleteOrganizationInvitation(ctx context.Context, id uint64) error

	// GetPendingInvitationsByOrganization retrieves all pending invitations for an organization.
	// This is used by org admins to view and manage outgoing invitations.
	GetPendingInvitationsByOrganization(ctx context.Context, organizationID int64, limit, offset int) ([]*common_persistence_models.OrganizationInvitation, int64, error)

	// ============================================================================
	// CHAT METHODS
	// ============================================================================

	// CreateChatChannel creates a new chat channel and participant rows (creator is added as OWNER).
	CreateChatChannel(
		ctx context.Context,
		createdByUserID uint64,
		name string,
		description *string,
		contextID *uint64,
		contextDisplayName *string,
		contextEntityID *uint64,
		channelType proto_models.ChannelType,
		participantUserIDs []uint64,
		featureFlags common_persistence_models.ChatChannelFeatureFlags,
	) (*common_persistence_models.ChatChannel, error)

	// CopyChatChannelFork copies a source channel's history up to a fork point into an empty
	// destination channel on the ambient transaction; see the implementation for the steps.
	CopyChatChannelFork(ctx context.Context, input CopyChatChannelForkInput) (*CopyChatChannelForkResult, error)

	// GetChatChannelByForkRequestKey returns the fork a user created with an idempotency key,
	// soft-deleted ones included, or ErrChatChannelNotFound.
	GetChatChannelByForkRequestKey(ctx context.Context, createdByUserID uint64, requestKey string) (*common_persistence_models.ChatChannel, error)

	// GetChatChannelByID retrieves a channel by id with participants (active only) and avatar attachment preloaded.
	GetChatChannelByID(ctx context.Context, channelID uint64) (*common_persistence_models.ChatChannel, error)

	// GetChatChannelFeatures reads only a live channel's feature bitmask, or ErrChatChannelNotFound.
	GetChatChannelFeatures(ctx context.Context, channelID uint64) (common_persistence_models.ChatChannelFeatures, error)

	// GetChatChannelAccess retrieves the data needed for channel permission checks
	// without loading the full participant roster: the channel (avatar preloaded,
	// no participants), the given user's active participant row (nil when not an
	// active member), and the active bot participant user IDs.
	GetChatChannelAccess(ctx context.Context, channelID uint64, userID uint64) (*ChatChannelAccess, error)

	// SetChatChannelOrganizationAccess writes the org-derived access a channel owns: the
	// organization it is associated with (nil = none) and the minimum effective organization
	// role that may read it without participating (ORGANIZATION_ROLE_UNSPECIFIED = no
	// org-derived read access). Both columns are written together on the ambient transaction.
	// Returns ErrChatChannelOrganizationAccessInvalid for an unusable combination and
	// ErrChatChannelNotFound when the channel does not exist.
	SetChatChannelOrganizationAccess(ctx context.Context, channelID uint64, organizationID *int64, minRoleRead proto_models.OrganizationRole) error

	// SetChatChannelPublicRead writes who beyond participants may read a channel, on the
	// ambient transaction when there is one, and returns the updated row. Returns
	// ErrChatChannelPublicReadInvalid for a level the enum does not define and
	// ErrChatChannelNotFound when the channel does not exist.
	SetChatChannelPublicRead(ctx context.Context, channelID uint64, level proto_models.ChatChannelPublicRead) (*common_persistence_models.ChatChannel, error)

	// ListChatChannelsForUser lists active channels for a given user.
	ListChatChannelsForUser(ctx context.Context, userID uint64, pagination *proto_models.PaginationRequest, includeArchived bool) ([]*common_persistence_models.ChatChannel, *proto_models.PaginationResponse, error)

	// UpdateChatChannel updates a channel's mutable fields.
	// A nil unreadNotificationsEnabled leaves the channel's unread notification policy unchanged.
	UpdateChatChannel(ctx context.Context, channelID uint64, actorUserID uint64, name *string, description *string, avatarAttachmentID *uint64, isArchived *bool, unreadNotificationsEnabled *bool) (*common_persistence_models.ChatChannel, error)

	// LeaveChatChannel sets left_at on the channel participant row.
	LeaveChatChannel(ctx context.Context, channelID uint64, userID uint64) error

	// FindChatChannelByExactActiveMembers finds an active channel whose active participants match exactly the given user IDs.
	// The member tuple must already be canonicalized (sorted + deduped + non-zero IDs).
	// Returns ErrChatChannelNotFound if no matching channel exists.
	FindChatChannelByExactActiveMembers(ctx context.Context, memberUserIDs []uint64) (*common_persistence_models.ChatChannel, error)

	// AcquireChatChannelMemberTupleLock takes a transaction-scoped advisory lock for a canonical
	// member tuple key, serializing get-or-create for the same tuple until the surrounding
	// transaction commits.
	AcquireChatChannelMemberTupleLock(ctx context.Context, key string) error

	// AddChatChannelParticipants adds (or reactivates) participants.
	AddChatChannelParticipants(ctx context.Context, channelID uint64, userIDs []uint64) ([]*common_persistence_models.ChatChannelParticipant, error)

	// RemoveChatChannelParticipant sets left_at for a participant.
	RemoveChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64) error

	// UpdateChatChannelParticipantRole updates the role for a participant.
	UpdateChatChannelParticipantRole(ctx context.Context, channelID uint64, userID uint64, role proto_models.ChannelRole) (*common_persistence_models.ChatChannelParticipant, error)

	// GetChatChannelParticipant retrieves one active membership, including its public user and avatar.
	GetChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64) (*common_persistence_models.ChatChannelParticipant, error)

	// UpdateChatChannelParticipantPreferences updates notification and pinned preferences.
	// The boolean reports whether a stored value changed; no-op updates preserve UpdatedAt.
	UpdateChatChannelParticipantPreferences(ctx context.Context, channelID uint64, userID uint64, notificationMode *proto_models.ChannelNotificationMode, isPinned *bool) (*common_persistence_models.ChatChannelParticipant, bool, error)

	// ListChatChannelParticipants lists active participants in a channel.
	ListChatChannelParticipants(ctx context.Context, channelID uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.ChatChannelParticipant, *proto_models.PaginationResponse, error)

	// ListChatChannelParticipantsBatch lists active participants for multiple channels
	// using the same pagination parameters per channel.
	ListChatChannelParticipantsBatch(ctx context.Context, channelIDs []uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.ChatChannelParticipantsBatchItem, error)

	// GetActiveChannelParticipantCount returns the count of active participants in a channel.
	GetActiveChannelParticipantCount(ctx context.Context, channelID uint64) (int64, error)

	// GetActiveChannelParticipantCountByUserIDs returns the count of active participants in a channel for the given user IDs.
	GetActiveChannelParticipantCountByUserIDs(ctx context.Context, channelID uint64, userIDs []uint64) (int64, error)

	// GetActiveChannelParticipantUserIDs returns user IDs of all active participants in a channel.
	// Active participants have left_at IS NULL and an unbanned, nondeleted user account.
	// Returns only user IDs for efficiency.
	GetActiveChannelParticipantUserIDs(ctx context.Context, channelID uint64) ([]uint64, error)

	// GetHiddenAuthorizedChannelParticipantUserIDs returns the active participants authorized to
	// receive hidden_from_users transcript rows + tool-call resolution events (bot/service users +
	// channel admins/owners). Used for the split-recipient fanout of hidden rows (Decision 7).
	GetHiddenAuthorizedChannelParticipantUserIDs(ctx context.Context, channelID uint64) ([]uint64, error)

	// SendChatMessage creates a message and associated embeds, and updates channel last_message fields.
	// Role is supplied by trusted service code; UNSPECIFIED preserves sender-based inference.
	SendChatMessage(ctx context.Context, in SendChatMessageInput) (*common_persistence_models.ChatMessage, error)

	// LockChatClientMessageID locks a sender's client message ID in a channel for the rest of the
	// transaction and returns what holds it: the queued input an earlier send left, else its message.
	// It requires a transaction (ErrChatMessageTransactionRequired).
	LockChatClientMessageID(ctx context.Context, senderID uint64, channelID uint64, clientMessageID string) (ChatClientMessageIDOwner, error)

	// CreateChatMessageEmbeds adds embeds to an existing message without editing message content.
	CreateChatMessageEmbeds(ctx context.Context, messageID uint64, embeds []CreateChatMessageEmbed) error

	// UpdateChatMessageAttachmentIDs updates the attachment ID list for a message.
	UpdateChatMessageAttachmentIDs(ctx context.Context, messageID uint64, attachmentIDs []uint64) (*common_persistence_models.ChatMessage, error)

	// GetChatMessageByID fetches a message by id with sender, embeds, and reactions preloaded,
	// using the actor-default visibility (hidden and soft-deleted rows are not found).
	GetChatMessageByID(ctx context.Context, messageID uint64) (*common_persistence_models.ChatMessage, error)

	// ListChatMessageEditHistory lists superseded versions for a message.
	ListChatMessageEditHistory(ctx context.Context, messageID uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.ChatMessageEditHistory, *proto_models.PaginationResponse, error)

	// EditChatMessage edits a message and records the prior editable state in history.
	EditChatMessage(ctx context.Context, input EditChatMessageInput) (*common_persistence_models.ChatMessage, error)

	// DeleteChatMessage soft-deletes a message and refreshes the channel's last-message
	// preview from the newest surviving visible message.
	DeleteChatMessage(ctx context.Context, channelID uint64, messageID uint64) error

	// GetChatMessageChannelID resolves only the channel a message belongs to, without loading its
	// content or associations, so a hidden get-by-ID can be authorized against that channel before
	// the hidden payload is read. Normal soft-delete scoping applies; returns ErrChatMessageNotFound
	// when the row is absent.
	GetChatMessageChannelID(ctx context.Context, messageID uint64) (uint64, error)

	// GetChatMessageByIDWithOptions fetches a message by id applying resolved visibility/deleted read options.
	GetChatMessageByIDWithOptions(ctx context.Context, messageID uint64, opts ChatMessageReadOptions) (*common_persistence_models.ChatMessage, error)

	// BatchGetChatMessagesWithOptions fetches multiple messages by id applying resolved read options.
	BatchGetChatMessagesWithOptions(ctx context.Context, channelID uint64, messageIDs []uint64, opts ChatMessageReadOptions) ([]*common_persistence_models.ChatMessage, []uint64, error)

	// ListChatMessagesWithOptions lists messages applying resolved visibility/deleted read options.
	ListChatMessagesWithOptions(ctx context.Context, channelID uint64, pagination *proto_models.PaginationRequest, parentMessageID uint64, opts ChatMessageReadOptions, sort *proto_models.SortRequest, sortBy proto_models.ListMessagesSortBy) ([]*common_persistence_models.ChatMessage, *proto_models.PaginationResponse, error)

	// GetChatMessageReplyCounts returns a map of message ID to reply count for the given message IDs.
	// This counts messages where parent_message_id equals the given message ID.
	GetChatMessageReplyCounts(ctx context.Context, channelID uint64, messageIDs []uint64) (map[uint64]uint32, error)

	// AddChatMessageReaction adds a reaction (idempotent).
	AddChatMessageReaction(ctx context.Context, messageID uint64, userID uint64, emoji string) (*common_persistence_models.ChatMessageReaction, error)

	// AddChatMessageReactionSingle adds a reaction enforcing the one-reaction-per-user policy
	// under a message row lock. Returns ErrChatReactionConflict when the user already reacted
	// with a different emoji.
	AddChatMessageReactionSingle(ctx context.Context, messageID uint64, userID uint64, emoji string) (*common_persistence_models.ChatMessageReaction, error)

	// RemoveChatMessageReaction removes a reaction.
	RemoveChatMessageReaction(ctx context.Context, messageID uint64, userID uint64, emoji string) error

	// ListChatMessageReactions lists reactions for a message.
	ListChatMessageReactions(ctx context.Context, messageID uint64, pagination *proto_models.PaginationRequest, emojiFilter *string) ([]*common_persistence_models.ChatMessageReaction, *proto_models.PaginationResponse, error)

	// --- Agentic transcript: agent sessions + config snapshots ---

	// CreateChatAgentSession inserts a new agent session row.
	CreateChatAgentSession(ctx context.Context, session *common_persistence_models.ChatAgentSession) error
	// FinalizeChatAgentSessionPrompt is called only inside the session creation transaction.
	FinalizeChatAgentSessionPrompt(ctx context.Context, sessionID uint64, prompt string, customization []byte) error

	// GetChatAgentSessionByID fetches an agent session by id.
	GetChatAgentSessionByID(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentSession, error)
	InitializeChatAgentSessionInferencePin(ctx context.Context, sessionID uint64, pin common_persistence_models.ChatAgentSessionInferencePin) (*common_persistence_models.ChatAgentSession, error)
	BlockChatAgentSessionInference(ctx context.Context, sessionID uint64, expectedPinVersion uint64, reason proto_models.InferenceBlockReason) error
	GetChatAgentSessionsByIDs(ctx context.Context, organizationID int64, sessionIDs []uint64) ([]common_persistence_models.ChatAgentSession, error)
	BatchRetargetChatAgentSessionInferencePins(ctx context.Context, organizationID int64, sessionIDs []uint64, expectedPinVersions map[uint64]uint64, pin common_persistence_models.ChatAgentSessionInferencePin, actorUserID uint64) ([]common_persistence_models.ChatAgentSession, error)
	DrainRetargetedChatAgentSessions(ctx context.Context, sessionIDs []uint64) ([]ChatAgentSessionResume, error)

	// LockChatChannelForAgentSession holds the channel lock until the required ambient transaction ends.
	LockChatChannelForAgentSession(ctx context.Context, channelID uint64) (*common_persistence_models.ChatChannel, error)
	// GetChatAgentSessionInLocation fetches the exact main-timeline or thread session, including archived sessions.
	GetChatAgentSessionInLocation(ctx context.Context, channelID uint64, parentMessageID uint64) (*common_persistence_models.ChatAgentSession, error)

	// GetChatAgentSessionByScopedKey resolves the default-scoped session for a fully scoped GetOrCreateByContext key.
	GetChatAgentSessionByScopedKey(ctx context.Context, organizationID int64, agentType string, userID uint64, contextID uint64, contextEntityID uint64) (*common_persistence_models.ChatAgentSession, error)

	// GetChatAgentSessionByCreateIdempotencyKey resolves a session-create replay key.
	GetChatAgentSessionByCreateIdempotencyKey(ctx context.Context, organizationID int64, createdByUserID uint64, userID uint64, agentType string, key string) (*common_persistence_models.ChatAgentSession, error)

	// UpdateChatAgentSessionAutoRunPolicy persists the validated automatic-run policy. Disabling posts
	// the session's queued input as ordinary messages and returns them (in a soft-deleted channel it
	// discards the queue instead).
	UpdateChatAgentSessionAutoRunPolicy(ctx context.Context, sessionID uint64, policy common_persistence_models.ChatAgentSessionAutoRunPolicy) (ChatAgentQueuedInputDelivery, error)

	// ArchiveChatAgentSession marks an agent session retired while preserving history, posting its
	// queued input as ordinary messages, which it returns (in a soft-deleted channel it discards the
	// queue instead). It clears parked_revision and joins an ambient transaction. Archiving the last
	// member holding a WAKE_POLICY_WITH_SIBLINGS group open wakes the parent for the group's newest
	// deferred ending, returning a minted parent run for the caller to start after commit.
	ArchiveChatAgentSession(ctx context.Context, sessionID uint64) (ChatAgentSessionArchive, error)
	// ArchiveReplacedChatAgentSession is ArchiveChatAgentSession for a child whose replacement joins
	// its wake group in the same transaction: it releases nothing.
	ArchiveReplacedChatAgentSession(ctx context.Context, sessionID uint64) (ChatAgentQueuedInputDelivery, error)
	// ListRetiringDelegatedChatAgentSessions recovers archived children with active runs, children
	// whose parent was archived before its cascade reached them, and archived roots of workflow-owned
	// agent types (SetChatAgentWorkflowTypesProvider) with active runs of their own.
	ListRetiringDelegatedChatAgentSessions(ctx context.Context, afterSessionID uint64, limit int) ([]*RetiringDelegatedChatAgentSession, error)
	// ListDelegatedChatAgentSessionsNeedingRetirementByParent pages unarchived children
	// and archived children with active runs; fully retired history is excluded.
	ListDelegatedChatAgentSessionsNeedingRetirementByParent(ctx context.Context, parentSessionID, afterSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error)

	// SetChatAgentSessionInitialArtifacts records the initial message/run created for a session.
	SetChatAgentSessionInitialArtifacts(ctx context.Context, sessionID uint64, initialMessageID uint64, initialRunID uint64) error

	// InsertChatAgentSessionConfigSnapshot writes an append-only config snapshot plus normalized tool rows.
	InsertChatAgentSessionConfigSnapshot(ctx context.Context, snapshot *common_persistence_models.ChatAgentSessionConfigSnapshot, toolConfigs []common_persistence_models.ChatAgentSessionToolConfig) error

	// BumpChatAgentSessionActiveConfigVersion atomically advances the active config version with optimistic concurrency.
	BumpChatAgentSessionActiveConfigVersion(ctx context.Context, sessionID uint64, expectedCurrentVersion uint64, newVersion uint64) error

	// GetChatAgentSessionConfigSnapshot fetches a config version snapshot plus its normalized tool rows.
	GetChatAgentSessionConfigSnapshot(ctx context.Context, sessionID uint64, version uint64) (*common_persistence_models.ChatAgentSessionConfigSnapshot, []common_persistence_models.ChatAgentSessionToolConfig, error)

	// GetChatAgentInteractionPolicy reads only the effective interaction policy of one config version.
	GetChatAgentInteractionPolicy(ctx context.Context, sessionID uint64, version uint64) (common_persistence_models.ChatAgentInteractionPolicy, error)

	// --- Agentic transcript: agent input queue and delivery ---

	// SetChatAgentMessageRecorder installs the recorder for delivered input and delegation
	// notices, called in their source transaction; nil records none.
	SetChatAgentMessageRecorder(recorder ChatAgentMessageRecorder)
	// LockChatChannelForSend locks, before a send reads which agent sessions it reaches, the run it
	// is tagged with (FOR KEY SHARE) and its channel (FOR NO KEY UPDATE). Requires a transaction.
	LockChatChannelForSend(ctx context.Context, channelID uint64, agentRunID uint64) error
	// LockChatAgentSessionsForSend locks, under LockChatChannelForSend's locks and before the send
	// writes, each session the send reaches (FOR UPDATE), and returns the state of each session
	// receiving it as input. Requires a transaction.
	LockChatAgentSessionsForSend(ctx context.Context, channelID uint64, locks []ChatAgentSessionSendLock) ([]ChatAgentSessionInputState, error)
	// EnqueueChatAgentInput queues a person's input for a session, writing no message, under the
	// send's locks. Requires a transaction.
	EnqueueChatAgentInput(ctx context.Context, in NewChatAgentQueuedInput) (*common_persistence_models.ChatAgentQueuedInput, error)
	// SetChatAgentQueuedInputAttachmentIDs records the attachments a send created for the input it
	// queued, under the send's locks. Requires a transaction.
	SetChatAgentQueuedInputAttachmentIDs(ctx context.Context, queuedInputID uint64, attachmentIDs []uint64) error
	// GetChatAgentQueuedInput reads one queued input with its sender
	// (ErrChatAgentQueuedInputNotQueued once it left the queue).
	GetChatAgentQueuedInput(ctx context.Context, queuedInputID uint64) (*common_persistence_models.ChatAgentQueuedInput, error)
	// ListChatAgentQueuedInputs pages a session's queue in order, each input with its sender, by
	// queued-input-ID cursor.
	ListChatAgentQueuedInputs(ctx context.Context, sessionID uint64, pagination *proto_models.PaginationRequest, limit int) ([]*common_persistence_models.ChatAgentQueuedInput, *proto_models.PaginationResponse, error)
	// LockChatAgentQueuedInput takes a queued input's admission locks in the caller's transaction and
	// reads it under them (ErrChatAgentQueuedInputNotQueued once it left the queue,
	// ErrChatChannelArchived in an archived channel). Requires a transaction.
	LockChatAgentQueuedInput(ctx context.Context, queuedInputID uint64) (*common_persistence_models.ChatAgentQueuedInput, error)
	// EditChatAgentQueuedInput replaces a queued input's content, serialized with delivery
	// (ErrChatAgentQueuedInputNotQueued when delivery or a removal took it first,
	// ErrChatChannelArchived in an archived channel).
	EditChatAgentQueuedInput(ctx context.Context, queuedInputID uint64, content ChatAgentQueuedInputContent) (*common_persistence_models.ChatAgentQueuedInput, error)
	// RemoveChatAgentQueuedInput takes a queued input out of the queue and returns it, serialized
	// with delivery (ErrChatAgentQueuedInputNotQueued when delivery or a removal took it first,
	// ErrChatChannelArchived in an archived channel).
	RemoveChatAgentQueuedInput(ctx context.Context, queuedInputID uint64) (*common_persistence_models.ChatAgentQueuedInput, error)
	// DrainChatAgentInput delivers a session's queued input into one new run when the session can
	// take one, returning the run (or nil) and the messages created. Requires a transaction.
	DrainChatAgentInput(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentRun, ChatAgentQueuedInputDelivery, error)
	// GetChatAgentInputReceipts reports what became of a human message for each agent session it
	// reached: a run it started, or the run its queued input was delivered into.
	GetChatAgentInputReceipts(ctx context.Context, messageID uint64) ([]ChatAgentInputReceipt, error)
	// ListChatAgentRunAdmittedMessageIDs lists the messages queued input became when it was delivered
	// into a run, up to throughMessageID, in delivery order.
	ListChatAgentRunAdmittedMessageIDs(ctx context.Context, runID, throughMessageID uint64) ([]uint64, error)

	// --- Agentic transcript: Stop and Resume ---

	// StopChatAgentRun cancels an active session run with STOPPED_BY_USER and suspends its session,
	// and each delegated child working for it, in one transaction, returning nil when the run is no
	// longer active.
	StopChatAgentRun(ctx context.Context, runID, stoppedByUserID uint64) (*ChatAgentRunStop, error)
	// ResumeChatAgentSession clears the suspension stopping suspendedRunID left and delivers the held
	// input, else drains the deferred delegation wake, into one run in the same transaction
	// (ErrChatAgentSessionRunActive while that run is still being cancelled).
	ResumeChatAgentSession(ctx context.Context, sessionID, suspendedRunID uint64) (*ChatAgentSessionResume, error)
	// HasChatAgentRunAdmittedInputFrom reports whether input a user queued was delivered into a run.
	HasChatAgentRunAdmittedInputFrom(ctx context.Context, runID, userID uint64) (bool, error)
	// GetChatAgentRunControlEvidence batches per-run viewer input and captured approval policy.
	GetChatAgentRunControlEvidence(ctx context.Context, runIDs []uint64, userID uint64) (map[uint64]ChatAgentRunControlEvidence, error)
	// ListSuspendedChatAgentRuns keyset-pages, for the recovery sweep, the active runs their
	// suspended session is stopped on: cancellations a parent's Stop still owes its children.
	ListSuspendedChatAgentRuns(ctx context.Context, afterRunID uint64, limit int) ([]uint64, error)
	// GetChatAgentSessionInteractionState reads a session with its active run, waiting input count
	// and policy in force.
	GetChatAgentSessionInteractionState(ctx context.Context, sessionID uint64) (*ChatAgentSessionInteractionState, error)
	// ListChatAgentSessionInteractionStates reads the interaction states of the sessions at several
	// locations of one channel in one query.
	ListChatAgentSessionInteractionStates(ctx context.Context, channelID uint64, parentMessageIDs []uint64) ([]*ChatAgentSessionInteractionState, error)

	// --- Agentic transcript: agent runs ---

	// CreateChatAgentRun inserts a new run row.
	CreateChatAgentRun(ctx context.Context, run *common_persistence_models.ChatAgentRun) error

	// CreateChatAgentRunCapturingConfig inserts a run row, capturing the session's active config version atomically.
	CreateChatAgentRunCapturingConfig(ctx context.Context, run *common_persistence_models.ChatAgentRun) error
	// CreateChatAgentRunForSend is CreateChatAgentRunCapturingConfig inside a send that holds the
	// LockChatChannelForSend and LockChatAgentSessionsForSend locks; it never locks the channel
	// again. Requires a transaction.
	CreateChatAgentRunForSend(ctx context.Context, run *common_persistence_models.ChatAgentRun) error

	// GetChatAgentRunByID fetches a run by id.
	GetChatAgentRunByID(ctx context.Context, runID uint64) (*common_persistence_models.ChatAgentRun, error)

	// CancelChatAgentRun atomically cancels the run, its outstanding tool work, and its dangling streams.
	// reason and reasonCode (caller-validated, empty stores NULL) are recorded with the transition.
	// It returns what the cancellation ended, or nil when the run was not in fromStatuses.
	CancelChatAgentRun(ctx context.Context, runID uint64, fromStatuses []proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason, reasonCode string) (*ChatAgentRunCancellation, error)

	// CompareAndSetChatAgentRunStatus conditionally transitions run status (Decision 6 state machine).
	// reason replaces the stored status reason; transitions back to RUNNING pass UNSPECIFIED.
	CompareAndSetChatAgentRunStatus(ctx context.Context, runID uint64, fromStatuses []proto_models.ChatAgentRunStatus, toStatus proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason) (bool, error)
	// ReserveChatAgentRunBudget atomically charges generation/tool work when it fits the supplied
	// ceilings and the run's graph budget pool; a pool refusal is a *GraphBudgetExhaustedError.
	ReserveChatAgentRunBudget(ctx context.Context, runID uint64, generationDelta uint64, toolCallDelta uint64, maxGenerations uint64, maxToolCalls uint64) (bool, error)
	// ReserveChatAgentRunStepAndAdmit reserves one model step's generation under the context's drive
	// lease and, with admit and a generation left for compaction, delivers the session's queued
	// input into the run as messages in the same transaction.
	ReserveChatAgentRunStepAndAdmit(ctx context.Context, runID uint64, maxGenerations uint64, maxToolCalls uint64, admit bool) (reserved bool, delivery ChatAgentQueuedInputDelivery, err error)
	// AddChatAgentRunActiveDuration adds active processing time to the run's cumulative usage and
	// to its graph budget pool.
	AddChatAgentRunActiveDuration(ctx context.Context, runID uint64, elapsed time.Duration) error

	// ClaimChatAgentRunDrive atomically acquires the cross-replica generation lease for a due run.
	ClaimChatAgentRunDrive(ctx context.Context, runID uint64, leaseOwner string, leaseDuration time.Duration) (bool, error)

	// RenewChatAgentRunDrive extends a still-live token-owned generation lease.
	RenewChatAgentRunDrive(ctx context.Context, runID uint64, leaseOwner string, leaseDuration time.Duration) (bool, error)

	// ReleaseChatAgentRunDrive clears a token-owned lease and optionally schedules retry backoff.
	// retryReason is stored only alongside a positive retryAfter and is cleared by every release.
	ReleaseChatAgentRunDrive(ctx context.Context, runID uint64, leaseOwner string, retryAfter time.Duration, retryReason proto_models.ChatAgentRunStatusReason) (bool, error)

	// AbandonChatAgentRunDrive revokes the claim of an attempt whose call ignored cancellation,
	// records its active time, and commits the run's retry or PROVIDER_TIMEOUT failure.
	AbandonChatAgentRunDrive(ctx context.Context, runID uint64, abandonment ChatAgentRunDriveAbandonment) (ChatAgentRunDriveAbandonOutcome, uint64, error)

	// ListRecoverableChatAgentRuns lists bounded due RUNNING/RETRYABLE runs without a live lease.
	ListRecoverableChatAgentRuns(ctx context.Context, limit int) ([]uint64, error)

	// ListRecoverableChatAgentRunsAfterID keyset-pages due rows without shared eligibility writes.
	ListRecoverableChatAgentRunsAfterID(ctx context.Context, afterID uint64, limit int) ([]uint64, error)

	// AssertChatAgentRunDriveLease locks and verifies the drive token carried by ctx.
	AssertChatAgentRunDriveLease(ctx context.Context, runID uint64) error

	// SealChatAgentRunStep records a fenced, validated model-step recovery boundary. It returns the
	// disposition it recorded (with steer, a final answer becomes Steered while input waits that the
	// next step can admit under maxGenerations and the graph pool's generations) and whether this
	// call wrote the seal.
	SealChatAgentRunStep(ctx context.Context, runID uint64, stepIndex uint32, disposition ChatAgentRunStepDisposition, toolCallCount uint32, maxGenerations uint64, steer bool, consumedPending uint64) (ChatAgentRunStepDisposition, bool, error)

	// ReconcileSealedChatAgentRunToolStep atomically settles the aggregate latest tool step.
	ReconcileSealedChatAgentRunToolStep(ctx context.Context, runID uint64, stepIndex uint32, resumeOnDenial bool) (ChatAgentRunToolStepResolution, error)

	// FailChatAgentRunWithNotice atomically fails a run and persists its generic visible notice.
	FailChatAgentRunWithNotice(ctx context.Context, runID uint64, reason proto_models.ChatAgentRunStatusReason) (failed bool, noticeMessageID uint64, err error)

	// FailIncompleteChatAgentRunDrive atomically fails an unsealed recovered step and any dangling stream.
	FailIncompleteChatAgentRunDrive(ctx context.Context, runID uint64) (bool, error)

	// GetChatAgentRunStepArtifactState summarizes full run-tagged transcript artifacts for recovery.
	GetChatAgentRunStepArtifactState(ctx context.Context, runID uint64) (*ChatAgentRunStepArtifactState, error)

	// ListLatestChatAgentRunsByThread returns the newest perLocation runs of each requested thread
	// location (0 = main timeline) of one channel, newest first within a location.
	ListLatestChatAgentRunsByThread(ctx context.Context, channelID uint64, parentMessageIDs []uint64, perLocation int) ([]*common_persistence_models.ChatAgentRun, error)

	// GetChatAgentRunProgress returns durable progress evidence for non-terminal runs, keyed by run id.
	GetChatAgentRunProgress(ctx context.Context, runIDs []uint64) (map[uint64]*ChatAgentRunProgress, error)

	// LockChatAgentRunForUpdate serializes aggregate decisions for one run inside the caller's
	// transaction. It must only be used with a transaction-bearing context.
	LockChatAgentRunForUpdate(ctx context.Context, runID uint64) error

	// TerminalizeChatAgentRunWithNotice moves a nonterminal run to FAILED or
	// MODEL_STEP_LIMIT_EXCEEDED with its visible notice in one transaction (the general form of
	// FailChatAgentRunWithNotice, for step-limit and work-budget stops).
	TerminalizeChatAgentRunWithNotice(ctx context.Context, runID uint64, fromStatuses []proto_models.ChatAgentRunStatus, toStatus proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason) (terminalized bool, noticeMessageID uint64, err error)

	// --- Agentic transcript: delegation (parent sessions spawning thread children) ---

	// SetChatAgentRunBudgetCeilingsProvider installs the work-budget source for runs persistence
	// mints itself (delegation wakes and drained follow-ups).
	SetChatAgentRunBudgetCeilingsProvider(provider ChatAgentRunBudgetCeilingsProvider)
	// SetChatAgentWorkflowTypesProvider installs the source of owner-driven (workflow-owned) agent
	// types: their parents are woken whatever their auto-run policy, and recovery never lists or
	// claims their runs.
	SetChatAgentWorkflowTypesProvider(provider ChatAgentWorkflowTypesProvider)
	// ListChatAgentSessionsByParent returns every session spawned by one parent, oldest first.
	ListChatAgentSessionsByParent(ctx context.Context, parentAgentSessionID uint64) ([]*common_persistence_models.ChatAgentSession, error)
	// ListChatAgentSessionsByParentAfterID returns at most limit children with an id above
	// afterSessionID, oldest first: one keyset page of a parent's children.
	ListChatAgentSessionsByParentAfterID(ctx context.Context, parentAgentSessionID, afterSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error)
	// ListActiveChatAgentSessionsByParentAfterID pages unarchived children with RUNNING/WAITING/RETRYABLE work.
	ListActiveChatAgentSessionsByParentAfterID(ctx context.Context, parentSessionID, afterSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error)
	// ListRecentChatAgentSessionsByParent returns at most limit children (active-run children
	// first, then newest first) and a lower bound on the parent's child count.
	ListRecentChatAgentSessionsByParent(ctx context.Context, parentAgentSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, uint32, error)
	// ListDelegatedThreadRootsByChannel returns at most limit delegated child thread roots of a
	// channel, active-run children first, then newest first, with how many lead with an active run
	// and whether more active children exist past the limit.
	ListDelegatedThreadRootsByChannel(ctx context.Context, channelID uint64, limit int) (DelegatedThreadRoots, error)
	// ListDelegatedThreadRootsAmong returns the given thread roots of a channel that hold a
	// delegated child session, in no particular order; the main timeline never does.
	ListDelegatedThreadRootsAmong(ctx context.Context, channelID uint64, roots []uint64) ([]uint64, error)
	// GetChatAgentSessionByIDForShare reads one session under a FOR SHARE row lock, so a concurrent
	// status change (an archive) waits for the caller's transaction. Requires a transaction.
	GetChatAgentSessionByIDForShare(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentSession, error)
	// CountRunningDelegatedChildren counts the parent's active children that still have a
	// non-terminal run.
	CountRunningDelegatedChildren(ctx context.Context, parentAgentSessionID uint64) (uint32, error)
	// CountActiveDelegatedChildrenByDriver counts, across all parents, the unarchived children with
	// a nonterminal run, split into interactive and owner-driven (workflow-owned) children.
	CountActiveDelegatedChildrenByDriver(ctx context.Context) (interactive, ownerDriven uint64, err error)
	// CountDelegatedChildren counts every child the parent has created, archived ones included.
	CountDelegatedChildren(ctx context.Context, parentAgentSessionID uint64) (uint32, error)
	// GetChatAgentSessionByParentCreateIdempotencyKey finds the parent's child created under key, or
	// ErrChatAgentSessionNotFound.
	GetChatAgentSessionByParentCreateIdempotencyKey(ctx context.Context, parentAgentSessionID uint64, key string) (*common_persistence_models.ChatAgentSession, error)
	// GetChatAgentSessionReplacing finds the child that replaced childSessionID, or
	// ErrChatAgentSessionNotFound.
	GetChatAgentSessionReplacing(ctx context.Context, childSessionID uint64) (*common_persistence_models.ChatAgentSession, error)
	// ListActiveChatAgentRunsBySession returns the session's RUNNING/WAITING/RETRYABLE runs.
	ListActiveChatAgentRunsBySession(ctx context.Context, agentSessionID uint64) ([]*common_persistence_models.ChatAgentRun, error)
	// ListActiveChatAgentRunsBySessions batches active-run reads for a bounded session page.
	ListActiveChatAgentRunsBySessions(ctx context.Context, sessionIDs []uint64) ([]*common_persistence_models.ChatAgentRun, error)
	// GetLatestBroadcastReplyByRun returns the newest broadcast text row of a run after a message
	// id, or ErrChatMessageNotFound.
	GetLatestBroadcastReplyByRun(ctx context.Context, runID uint64, afterMessageID uint64) (*common_persistence_models.ChatMessage, error)
	// ListLatestDelegationReportsByThread returns each thread's newest run-tagged broadcast text row.
	ListLatestDelegationReportsByThread(ctx context.Context, channelID uint64, rootMessageIDs []uint64) (map[uint64]*common_persistence_models.ChatMessage, error)
	// ListChatAgentRunsStartedBy returns the runs one run's ending minted: a drained human-input
	// follow-up and a delegation wake, oldest first.
	ListChatAgentRunsStartedBy(ctx context.Context, runID uint64) ([]*common_persistence_models.ChatAgentRun, error)
	GetChatAgentRunWokenBy(ctx context.Context, runID, sessionID uint64) (*common_persistence_models.ChatAgentRun, error)
	// MarkChatMessageBroadcastToChannel also shows an existing thread row on the main timeline.
	MarkChatMessageBroadcastToChannel(ctx context.Context, messageID uint64) error
	// ListChatAgentSessionsByChannel returns a channel's sessions, main timeline first and then
	// thread sessions newest first, capped at limit (the oldest thread sessions are dropped). The
	// caller samples chat_agent_sessions_by_channel_max; limit must be 1..config.MaxChatAgentSessionsByChannel.
	ListChatAgentSessionsByChannel(ctx context.Context, channelID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error)
	// CreateOrCoalesceParentWakeRun wakes, or records a pending wake for, a delegated child's
	// parent because of one broadcast trigger row.
	CreateOrCoalesceParentWakeRun(ctx context.Context, in ChatAgentParentWakeInput) (*ChatAgentSessionWakeResult, error)

	// --- Agentic transcript: inference ledger + totals ---

	// RecordChatInferenceCall appends a ledger row idempotently and bumps totals only on a real insert.
	RecordChatInferenceCall(ctx context.Context, call *common_persistence_models.ChatInferenceCall) (bool, error)

	// GetChatInferenceTotals returns the per-channel totals cache (zero-valued when no rows).
	GetChatInferenceTotals(ctx context.Context, channelID uint64) (*common_persistence_models.ChatInferenceTotals, error)

	// GetLatestChatInferenceInputTokens returns the latest positive input-token count for one run.
	GetLatestChatInferenceInputTokens(ctx context.Context, runID uint64) (int64, error)

	// GetMessageInferenceAggregate derives the usage summary for one message from linked ledger rows.
	GetMessageInferenceAggregate(ctx context.Context, messageID uint64) (*MessageInferenceAggregate, error)

	// BatchGetMessageInferenceAggregates derives usage summaries for a bounded set of message IDs.
	BatchGetMessageInferenceAggregates(ctx context.Context, channelID uint64, messageIDs []uint64) (map[uint64]*MessageInferenceAggregate, error)

	// ListChatInferenceCallsByRun returns the append-only ledger rows for one run (paginated).
	ListChatInferenceCallsByRun(ctx context.Context, runID uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.ChatInferenceCall, *proto_models.PaginationResponse, error)

	// GetChatInferenceTotalsForSessions derives totals from ledger rows attributed to a set of
	// sessions (one session, or a delegation parent and its children).
	GetChatInferenceTotalsForSessions(ctx context.Context, agentSessionIDs []uint64) (*common_persistence_models.ChatInferenceTotals, error)

	// ListChatInferenceDimensionsForSessions computes all selected dimensions in one ledger scan.
	ListChatInferenceDimensionsForSessions(ctx context.Context, agentSessionIDs []uint64, dimensions map[InferenceBreakdownDimension]*proto_models.PaginationRequest) (map[InferenceBreakdownDimension]*InferenceUsageDimension, error)

	// --- Agentic transcript: compaction + context reads ---

	// GetLatestCompaction returns the most recent compaction checkpoint for a channel/thread scope.
	GetLatestCompaction(ctx context.Context, channelID uint64, parentMessageID uint64) (*common_persistence_models.ChatMessage, error)

	// StreamChatMessagesAfter scans messages after a watermark with an unbounded id cursor.
	StreamChatMessagesAfter(ctx context.Context, channelID uint64, parentMessageID uint64, afterMessageID uint64, opts ChatMessageReadOptions, batchSize int, fn func(batch []*common_persistence_models.ChatMessage) (stop bool, err error)) error

	// CountVisibleCountingMessagesAfter counts unread-eligible messages, excluding hidden rows and the given kinds.
	CountVisibleCountingMessagesAfter(ctx context.Context, channelID uint64, parentMessageID uint64, afterMessageID uint64, excludeSenderID uint64, excludeKinds []proto_models.MessageKind) (int64, error)

	// --- Agentic transcript: tool-call approval / durable execution ---

	// ResolveToolCallApproval conditionally transitions a pending TOOL_CALL's approval/execution state.
	ResolveToolCallApproval(ctx context.Context, messageID uint64, fromStatus proto_models.ToolCallApprovalStatus, toStatus proto_models.ToolCallApprovalStatus, toExecState proto_models.ToolCallExecutionState, resolvedByUserID *uint64, reason *string, resolvedAt time.Time) (bool, error)

	// ListPendingToolCallsToExpire returns pending TOOL_CALL rows whose approval has timed out.
	ListPendingToolCallsToExpire(ctx context.Context, now time.Time, limit int) ([]*common_persistence_models.ChatMessage, error)

	// ListPendingToolCallExpiryBatch returns a lean, cursor-pageable expiry batch with its runs and
	// captured policy snapshots loaded in bulk.
	ListPendingToolCallExpiryBatch(ctx context.Context, now time.Time, after *ToolCallExpiryCursor, limit int) (*PendingToolCallExpiryBatch, error)

	// ListClaimableApprovedToolCalls returns approved TOOL_CALL rows ready to execute (incl. expired leases).
	ListClaimableApprovedToolCalls(ctx context.Context, channelID uint64, now time.Time, limit int) ([]*common_persistence_models.ChatMessage, error)

	// ClaimApprovedToolCall atomically claims an approved TOOL_CALL for execution with a lease.
	ClaimApprovedToolCall(ctx context.Context, messageID uint64, leaseOwner string, leaseExpiresAt time.Time, now time.Time) (bool, error)

	// ListClaimableToolCalls returns approved work plus retryable or stale-orphaned inline TOOL_CALL rows.
	ListClaimableToolCalls(ctx context.Context, channelID uint64, now time.Time, limit int) ([]*common_persistence_models.ChatMessage, error)

	// ClaimToolCall atomically claims approved, retryable inline, or stale-orphaned inline work with a lease.
	ClaimToolCall(ctx context.Context, messageID uint64, leaseOwner string, leaseExpiresAt time.Time, now time.Time) (bool, error)

	// ClaimFreshInlineToolCall claims a newly persisted inline TOOL_CALL before live execution.
	ClaimFreshInlineToolCall(ctx context.Context, messageID uint64, leaseOwner string, leaseExpiresAt time.Time, now time.Time) (bool, error)

	// ReleaseToolCallLease releases a claim (retryable -> reclaimable; terminal -> failed).
	ReleaseToolCallLease(ctx context.Context, messageID uint64, leaseOwner string, retryable bool, lastErr *string) (bool, error)

	// MarkInlineToolCallExecutionFailure marks a PENDING, unleased inline TOOL_CALL failure from the live loop.
	MarkInlineToolCallExecutionFailure(ctx context.Context, messageID uint64, retryable bool, lastErr *string) (bool, error)

	// MarkToolCallExecuted marks a claimed TOOL_CALL executed after its paired TOOL_RESULT is
	// persisted. Conditional on the caller still holding the execution lease (leaseOwner), so a
	// stale worker cannot mark the row executed or clear a reclaiming worker's live lease.
	MarkToolCallExecuted(ctx context.Context, messageID uint64, leaseOwner string, executedAt time.Time) (bool, error)

	// CreateTranscriptMessage inserts a validated transcript message, including STANDARD
	// assistant output, with a side row only when its kind/payload requires one.
	// It runs in one idempotent transaction and returns (messageID, created, err).
	CreateTranscriptMessage(ctx context.Context, in CreateTranscriptMessageInput) (uint64, bool, error)

	// CommitClaimedToolCallResult atomically verifies the paired TOOL_CALL's current execution
	// lease, inserts/idempotently validates its TOOL_RESULT, and marks the call EXECUTED. committed
	// is false when leaseOwner no longer owns the live claim; no result is inserted in that case.
	CommitClaimedToolCallResult(ctx context.Context, toolCallMessageID uint64, leaseOwner string, executedAt time.Time, in CreateTranscriptMessageInput) (messageID uint64, created bool, committed bool, err error)

	// CountUnresolvedToolCallsInStep counts TOOL_CALL rows in a (run, step) whose execution is not
	// finished (NOT IN EXECUTED/EXECUTION_FAILED). Used to gate whole-step resume on count==0.
	CountUnresolvedToolCallsInStep(ctx context.Context, agentRunID uint64, agentStepIndex uint32) (int, error)
	// CountRejectedArgumentSteps counts distinct malformed-output steps over the complete durable run.
	CountRejectedArgumentSteps(ctx context.Context, agentRunID uint64) (uint32, error)

	// HasDeniedOrExpiredToolCallsInStep reports whether a completed tool step contains a denial or
	// expiry. With ResumeOnDenial=false, the final sibling to finish uses this durable aggregate to
	// complete the run even when the denial was resolved before that sibling executed.
	HasDeniedOrExpiredToolCallsInStep(ctx context.Context, agentRunID uint64, agentStepIndex uint32) (bool, error)
}
