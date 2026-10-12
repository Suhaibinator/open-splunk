package attachment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/internal/hotpoll"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Constants for attachment lifecycle management
const (
	// DefaultFilename is used when filename sanitization results in empty string
	DefaultFilename = "file"

	// TempPathPrefix is the MinIO path prefix for temporary uploads
	TempPathPrefix = common_persistence.TempObjectPrefix

	// VerifiedPathPrefix is the MinIO path prefix for verified content
	VerifiedPathPrefix = common_persistence.VerifiedObjectPrefix
)

const (
	attachmentLifecycleLoggerName   = "common_service.attachment_lifecycle"
	attachmentLockManagerLoggerName = "common_service.attachment_lock_manager"

	// defaultPresignRegion is the last-resort SigV4 credential-scope region used
	// when the runtime configuration yields an empty region (a hand-built config
	// that never set PresignRegion). It matches the attachment_presign_region
	// configuration default.
	defaultPresignRegion = "us-east-1"
)

var (
	ErrValidationFailed         = errors.New("attachment validation failed")
	ErrAttachmentNotProcessable = errors.New("attachment is not in a processable state")
	// ErrAttachmentContentChanged reports that the temporary object was
	// overwritten after finalization pinned its version (a replayed presigned
	// upload). The attachment is rejected; nothing observed is promoted. It
	// wraps ErrValidationFailed so batch results classify it as a validation
	// failure rather than a transient error.
	ErrAttachmentContentChanged = fmt.Errorf("%w: uploaded content changed during validation", ErrValidationFailed)
)

// AttachmentLifecycleManager handles the complete lifecycle of file attachments from upload to storage.
// It manages the creation of upload URLs, processing of uploaded files, validation,
// deduplication, and cleanup of temporary files.
//
// The manager implements a multi-stage attachment workflow:
//  1. Create upload URL with temporary storage location
//  2. Process uploaded files with validation
//  3. Move validated files to permanent storage
//  4. Handle deduplication through content hashing
//  5. Clean up expired temporary uploads
type AttachmentLifecycleManager struct {
	// persistence provides database and MinIO operations
	persistence common_persistence.CommonPersistenceInterface

	// logger for structured logging of attachment operations
	logger *zap.Logger

	// objectHashSalt and minioPublicBucket are restart-bound resource identity.
	objectHashSalt    string
	minioPublicBucket string
	// runtimeConfigProvider supplies one coherent hot view per upload issuance or
	// cleanup sweep. Always non-nil.
	runtimeConfigProvider RuntimeConfigProvider

	// objectTypeRegistry manages registered attachment object types and their validation rules
	objectTypeRegistry *ObjectTypeRegistry

	// lockManager provides thread-safe locking for attachment processing to prevent race conditions
	lockManager *AttachmentLockManager

	// Cleanup runtime state
	cleanupCtx    context.Context
	cleanupCancel context.CancelFunc
	cleanupDone   chan struct{}
	closeOnce     sync.Once
	closeErr      error
}

// RuntimeConfigView supplies captured startup resources and hot attachment
// policy. URL issuance and cleanup sweeps read only their hot settings. The
// shared root, or a typed composition of attachment and object-storage views,
// satisfies this interface.
type RuntimeConfigView interface {
	config.AttachmentsView
	ObjectHashSalt() config.Secret
	MinioPublicBucket() string
	MinioPublicBaseURL() string
}

var _ RuntimeConfigView = config.RootView{}

// RuntimeConfigProvider returns one immutable attachment view. Providers may be
// called concurrently and must therefore be concurrency-safe; they must never
// return nil.
type RuntimeConfigProvider func() RuntimeConfigView

// UploadURLConfig contains configuration for creating upload URLs.
// This allows customization of upload behavior for different use cases
// while reusing the same core logic.
type UploadURLConfig struct {
	// ObjectType identifies the type of object being uploaded (e.g., "profile-picture", "document")
	// This is used to organize temporary uploads and can be used for type-specific validation
	ObjectType string

	// MaxSize is the maximum allowed file size in bytes (0 uses default)
	MaxSize int64

	// DefaultExpiry is the default expiration duration for the upload URL
	DefaultExpiry time.Duration

	// SanitizeFilename indicates whether to apply strict filename sanitization
	SanitizeFilename bool

	// DefaultFilename is used when sanitization results in an empty filename
	DefaultFilename string

	// AllowedMimeTypes is an optional list of allowed MIME types for this object type
	AllowedMimeTypes []string
}

// IssuedUploadURL contains a presigned upload URL and the exact policy values
// needed by callers to describe that issuance without sampling hot
// configuration a second time.
type IssuedUploadURL struct {
	Attachment *common_persistence_models.Attachment
	UploadURL  string
	FormData   map[string]string
	// ExpiresAt is the exact time the upload URL was signed to expire.
	ExpiresAt time.Time
}

// GetUploadURLExpiry returns the current upload URL expiry duration. Upload
// issuance methods capture this value together with the default maximum size so
// related policy values always come from one immutable runtime view.
//
// Returns:
//   - time.Duration: Upload URL expiration duration from manager configuration.
func (m *AttachmentLifecycleManager) GetUploadURLExpiry() time.Duration {
	return m.runtimeConfigProvider().AttachmentUploadURLExpiry()
}

// NewAttachmentLifecycleManager captures startup resources from one provider
// sample, then samples provider for each live policy operation.
func NewAttachmentLifecycleManager(
	persistence common_persistence.CommonPersistenceInterface,
	logger *zap.Logger,
	objectTypeRegistry *ObjectTypeRegistry,
	provider RuntimeConfigProvider,
) (*AttachmentLifecycleManager, error) {
	if provider == nil {
		return nil, fmt.Errorf("attachment configuration provider is required")
	}
	startup := provider()
	if startup == nil || startup == RuntimeConfigView(config.RootView{}) {
		return nil, fmt.Errorf("attachment configuration view is required")
	}
	salt := startup.ObjectHashSalt().StringValue()
	if salt == "" {
		return nil, fmt.Errorf("object hash salt cannot be empty")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	lockManager := NewAttachmentLockManager(logger, startup.AttachmentLockTTL(), startup.AttachmentLockCapacity())
	manager := &AttachmentLifecycleManager{
		persistence: persistence, logger: logger,
		objectHashSalt: salt, minioPublicBucket: startup.MinioPublicBucket(),
		runtimeConfigProvider: provider,
		objectTypeRegistry:    objectTypeRegistry, lockManager: lockManager,
	}
	manager.cleanupCtx, manager.cleanupCancel = context.WithCancel(context.Background())
	manager.cleanupDone = make(chan struct{})
	go manager.runCleanupRoutine(manager.cleanupConfig(startup).CleanupInterval)
	logger.Named(attachmentLifecycleLoggerName).Info("Started attachment lifecycle cleanup",
		appLogger.DurationSeconds(appLogger.KeyProcessingTimeoutSeconds, startup.AttachmentProcessingTimeout()),
		appLogger.DurationSeconds(appLogger.KeyPendingUploadTimeoutSeconds, startup.AttachmentPendingUploadTimeout()),
		appLogger.DurationSeconds(appLogger.KeyIntervalSeconds, manager.cleanupConfig(startup).CleanupInterval))
	return manager, nil
}

// resolveObjectTypeVisibility returns the storage visibility the registry assigns to an object type when an attachment row is created.
// Visibility is a server-side decision: unregistered or unknown object types stay private, which is also the zero value.
//
// Parameters:
//   - objectType: Object type name the attachment is being created for.
//
// Returns:
//   - proto_models.AttachmentVisibility: Configured visibility, or private when the object type is unknown.
func (m *AttachmentLifecycleManager) resolveObjectTypeVisibility(objectType string) proto_models.AttachmentVisibility {
	if m == nil || m.objectTypeRegistry == nil {
		return proto_models.AttachmentVisibility_ATTACHMENT_VISIBILITY_PRIVATE
	}
	config := m.objectTypeRegistry.Get(objectType)
	if config == nil {
		return proto_models.AttachmentVisibility_ATTACHMENT_VISIBILITY_PRIVATE
	}
	return config.Visibility
}

// CreateUploadURLWithConfig creates a new attachment record and generates a presigned upload URL
// using the provided configuration. This is the unified method that replaces the duplicate
func (m *AttachmentLifecycleManager) CreateUploadURLWithConfig(
	ctx context.Context,
	userID uint64,
	originalFileName string,
	contentType string,
	config UploadURLConfig,
) (*common_persistence_models.Attachment, string, map[string]string, error) {
	result, err := m.CreateUploadURLWithConfigResult(ctx, userID, originalFileName, contentType, config)
	if err != nil {
		return nil, "", nil, err
	}
	return result.Attachment, result.UploadURL, result.FormData, nil
}

// CreateUploadURLWithConfigResult creates an upload URL and returns the expiry
// captured for that exact issuance.
func (m *AttachmentLifecycleManager) CreateUploadURLWithConfigResult(
	ctx context.Context,
	userID uint64,
	originalFileName string,
	contentType string,
	config UploadURLConfig,
) (*IssuedUploadURL, error) {
	return m.createUploadURLWithConfig(ctx, userID, originalFileName, contentType, config, m.runtimeConfigProvider())
}

func (m *AttachmentLifecycleManager) createUploadURLWithConfig(
	ctx context.Context,
	userID uint64,
	originalFileName string,
	contentType string,
	config UploadURLConfig,
	runtime RuntimeConfigView,
) (*IssuedUploadURL, error) {
	// Apply filename sanitization if requested
	fileName := originalFileName
	if config.SanitizeFilename {
		fileName = FilenameSanitizer(originalFileName)
		if fileName == "" {
			fileName = config.DefaultFilename
			if fileName == "" {
				fileName = DefaultFilename // Ultimate fallback
			}
		}
	}

	// Note: Content type validation is now handled by streaming validators

	// Generate temporary object name with new format: temp/[object-type]/[uuid]:[filename]
	objectType := config.ObjectType
	if objectType == "" {
		objectType = "attachment" // Default object type
	}

	// Generate UUID for this upload
	uploadUUID := uuid.New().String()

	// Create the temporary object name with the new format
	tempObjectName := fmt.Sprintf("%s%s/%s:%s", TempPathPrefix, objectType, uploadUUID, fileName)

	expiry := config.DefaultExpiry
	if expiry == 0 {
		expiry = runtime.AttachmentUploadURLExpiry()
	}
	now := time.Now().UTC()
	expiresAt := now.Add(expiry)

	maxSize := config.MaxSize
	if maxSize <= 0 {
		maxSize = runtime.AttachmentDefaultMaxSize()
	}

	// Create the attachment record. Visibility is resolved from the registered
	// object type, never from the caller, and defaults to private.
	attachment := &common_persistence_models.Attachment{
		UserID:              userID,
		OriginalFileName:    fileName,
		ContentType:         contentType,
		Size:                0, // Will be updated after upload
		UploadStatus:        common_persistence_models.UploadStatusPendingUpload,
		TemporaryObjectName: tempObjectName,
		ObjectType:          objectType,
		Visibility:          m.resolveObjectTypeVisibility(objectType),
		UploadTime:          now,
		UploadURLExpiresAt:  expiresAt,
	}

	// Save the attachment record to the database
	err := m.persistence.CreateAttachment(ctx, attachment)
	if err != nil {
		return nil, contextualAttachmentError(
			fmt.Errorf("failed to create attachment record (userID=%d, fileName=%s, objectType=%s): %w", userID, fileName, objectType, err),
			zap.String(logkeys.Stage, "create_attachment_record"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyOriginalFilename, originalFileName),
			zap.String(appLogger.KeyObjectType, objectType))
	}

	// Generate presigned POST URL for client-side upload with size limits
	presignedURL, formData, err := m.persistence.GeneratePresignedPostURL(
		ctx,
		tempObjectName,
		expiresAt,
		1, // Minimum 1 byte
		maxSize,
		contentType,
	)
	if err != nil {
		return nil, contextualAttachmentError(
			fmt.Errorf("failed to generate presigned POST URL (attachmentID=%d, objectName=%s): %w", attachment.ID, tempObjectName, err),
			zap.String(logkeys.Stage, "generate_attachment_upload_url"),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(appLogger.KeyObjectName, tempObjectName))
	}

	// Rewrite the presigned URL to use the public base URL if configured
	if runtime.MinioPublicBaseURL() != "" {
		rewrittenURL, err := common_service_models.RewritePresignedURL(presignedURL, runtime.MinioPublicBaseURL())
		if err != nil {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Failed to rewrite presigned URL; using original URL",
				zap.Error(err),
				zap.String(logkeys.Stage, "rewrite_upload_url"),
				zap.String(logkeys.Fallback, "original presigned URL"),
				zap.String(appLogger.KeyOriginalURL, presignedURL),
				zap.String(appLogger.KeyPublicBaseURL, runtime.MinioPublicBaseURL()))
		} else {
			presignedURL = rewrittenURL
		}
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Created upload URL",
		zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
		zap.String(appLogger.KeyObjectName, tempObjectName),
		zap.Int64(appLogger.KeyMaxSize, maxSize),
		appLogger.DurationSeconds(appLogger.KeyExpirySeconds, expiry),
		zap.String(appLogger.KeyObjectType, objectType))

	return &IssuedUploadURL{
		Attachment: attachment,
		UploadURL:  presignedURL,
		FormData:   formData,
		ExpiresAt:  expiresAt,
	}, nil
}

// CreateUploadURLWithEntity creates a new attachment record with entity linking and generates a presigned upload URL.
// This method extends the existing upload functionality to support linking attachments to specific entities.
func (m *AttachmentLifecycleManager) CreateUploadURLWithEntity(
	ctx context.Context,
	userID uint64,
	originalFileName string,
	contentType string,
	objectType string,
	entityID string,
) (*common_persistence_models.Attachment, string, map[string]string, error) {
	result, err := m.CreateUploadURLWithEntityResult(ctx, userID, originalFileName, contentType, objectType, entityID)
	if err != nil {
		return nil, "", nil, err
	}
	return result.Attachment, result.UploadURL, result.FormData, nil
}

// CreateUploadURLWithEntityResult creates an entity-linked upload URL and
// returns the expiry captured for that exact issuance.
func (m *AttachmentLifecycleManager) CreateUploadURLWithEntityResult(
	ctx context.Context,
	userID uint64,
	originalFileName string,
	contentType string,
	objectType string,
	entityID string,
) (*IssuedUploadURL, error) {
	// Get object type configuration for validation
	config := m.objectTypeRegistry.Get(objectType)
	if config == nil {
		return nil, fmt.Errorf("unknown object type: %s", objectType)
	}
	runtime := m.runtimeConfigProvider()

	// Apply filename sanitization directly
	// Note: Content type validation now handled by streaming validators
	fileName := FilenameSanitizer(originalFileName)
	if fileName == "" {
		fileName = DefaultFilename
	}

	// Note: Content type validation is now handled by streaming validators

	// Generate UUID for this upload
	uploadUUID := uuid.New().String()

	// Create the temporary object name with the new format
	tempObjectName := fmt.Sprintf("%s%s/%s:%s", TempPathPrefix, objectType, uploadUUID, fileName)

	// Create the attachment record with entity linking. Visibility comes from the
	// resolved object type configuration and defaults to private.
	now := time.Now().UTC()
	expiresAt := now.Add(runtime.AttachmentUploadURLExpiry())
	attachment := &common_persistence_models.Attachment{
		UserID:              userID,
		OriginalFileName:    fileName,
		ContentType:         contentType,
		Size:                0, // Will be updated after upload
		UploadStatus:        common_persistence_models.UploadStatusPendingUpload,
		TemporaryObjectName: tempObjectName,
		ObjectType:          objectType,
		Visibility:          config.Visibility,
		UploadTime:          now,
		UploadURLExpiresAt:  expiresAt,
		EntityID:            entityID, // Entity ID (type is implied by ObjectType)
	}

	// Save the attachment record to the database
	err := m.persistence.CreateAttachment(ctx, attachment)
	if err != nil {
		return nil, contextualAttachmentError(
			fmt.Errorf("failed to create attachment record with entity (userID=%d, fileName=%s, objectType=%s, entityID=%s): %w", userID, fileName, objectType, entityID, err),
			zap.String(logkeys.Stage, "create_entity_attachment_record"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyOriginalFilename, originalFileName),
			zap.String(appLogger.KeyObjectType, objectType),
			zap.String(appLogger.KeyEntityID, entityID))
	}

	maxSize := config.MaxSize
	if maxSize <= 0 {
		maxSize = runtime.AttachmentDefaultMaxSize()
	}

	// Generate presigned POST URL for client-side upload with size limits
	presignedURL, formData, err := m.persistence.GeneratePresignedPostURL(
		ctx,
		tempObjectName,
		expiresAt,
		1, // Minimum 1 byte
		maxSize,
		contentType,
	)
	if err != nil {
		return nil, contextualAttachmentError(
			fmt.Errorf("failed to generate presigned POST URL for entity attachment (attachmentID=%d, objectName=%s, entityID=%s): %w", attachment.ID, tempObjectName, entityID, err),
			zap.String(logkeys.Stage, "generate_entity_attachment_upload_url"),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(appLogger.KeyObjectName, tempObjectName),
			zap.String(appLogger.KeyEntityID, entityID))
	}

	// Rewrite the presigned URL to use the public base URL if configured
	if runtime.MinioPublicBaseURL() != "" {
		rewrittenURL, err := common_service_models.RewritePresignedURL(presignedURL, runtime.MinioPublicBaseURL())
		if err != nil {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Failed to rewrite presigned URL for entity attachment; using original URL",
				zap.Error(err),
				zap.String(logkeys.Stage, "rewrite_entity_upload_url"),
				zap.String(logkeys.Fallback, "original presigned URL"),
				zap.String(appLogger.KeyOriginalURL, presignedURL),
				zap.String(appLogger.KeyPublicBaseURL, runtime.MinioPublicBaseURL()),
				zap.String(appLogger.KeyEntityID, entityID))
		} else {
			presignedURL = rewrittenURL
		}
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Created upload URL with entity linking",
		zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
		zap.String(appLogger.KeyObjectName, tempObjectName),
		zap.Int64(appLogger.KeyMaxSize, maxSize),
		appLogger.DurationSeconds(appLogger.KeyExpirySeconds, runtime.AttachmentUploadURLExpiry()),
		zap.String(appLogger.KeyObjectType, objectType),
		zap.String(appLogger.KeyEntityID, entityID))

	return &IssuedUploadURL{
		Attachment: attachment,
		UploadURL:  presignedURL,
		FormData:   formData,
		ExpiresAt:  expiresAt,
	}, nil
}

// CreateUploadURLsWithEntity creates multiple attachments linked to a specific entity in a single operation.
// This method efficiently handles bulk attachment creation by:
// 1. Creating all attachment records in a single database transaction
// 2. Generating presigned URLs in parallel for better performance
func (m *AttachmentLifecycleManager) CreateUploadURLsWithEntity(
	ctx context.Context,
	userID uint64,
	objectType string,
	entityID string,
	requests []struct {
		Filename    string
		ContentType string
		ContentHash string
	},
) ([]*common_persistence_models.Attachment, []struct {
	UploadURL string
	FormData  map[string]string
}, error) {
	issued, err := m.CreateUploadURLsWithEntityResult(ctx, userID, objectType, entityID, requests)
	if err != nil {
		return nil, nil, err
	}

	attachments := make([]*common_persistence_models.Attachment, len(issued))
	uploadResults := make([]struct {
		UploadURL string
		FormData  map[string]string
	}, len(issued))
	for i, result := range issued {
		attachments[i] = result.Attachment
		uploadResults[i] = struct {
			UploadURL string
			FormData  map[string]string
		}{
			UploadURL: result.UploadURL,
			FormData:  result.FormData,
		}
	}
	return attachments, uploadResults, nil
}

// CreateUploadURLsWithEntityResult creates multiple entity-linked upload URLs
// and returns each attachment together with the expiry captured for that exact
// batch issuance. The runtime provider is sampled once for the whole batch.
func (m *AttachmentLifecycleManager) CreateUploadURLsWithEntityResult(
	ctx context.Context,
	userID uint64,
	objectType string,
	entityID string,
	requests []struct {
		Filename    string
		ContentType string
		ContentHash string
	},
) ([]*IssuedUploadURL, error) {
	// Validate object type exists
	objectTypeConfig := m.objectTypeRegistry.Get(objectType)
	if objectTypeConfig == nil {
		return nil, fmt.Errorf("unknown object type: %s", objectType)
	}
	runtime := m.runtimeConfigProvider()
	// Every row in this batch shares one object type, so visibility is resolved once.
	visibility := m.resolveObjectTypeVisibility(objectType)

	// Create attachment records
	now := time.Now().UTC()
	expiresAt := now.Add(runtime.AttachmentUploadURLExpiry())
	attachments := make([]*common_persistence_models.Attachment, len(requests))
	for i, req := range requests {
		// Sanitize filename to prevent overflow and security issues
		sanitizedFilename := FilenameSanitizer(req.Filename)
		if sanitizedFilename == "" {
			sanitizedFilename = DefaultFilename
		}

		uniqueID := uuid.New().String()
		tempPath := fmt.Sprintf("%s%s/%s:%s", TempPathPrefix, objectType, uniqueID, sanitizedFilename)

		attachments[i] = &common_persistence_models.Attachment{
			UserID:              userID,
			TemporaryObjectName: tempPath,
			OriginalFileName:    req.Filename, // Preserve original name for display
			ContentType:         req.ContentType,
			ObjectType:          objectType,
			Visibility:          visibility,
			UploadStatus:        common_persistence_models.UploadStatusPendingUpload,
			UploadTime:          now,
			UploadURLExpiresAt:  expiresAt,
			FileHash:            "",
			DeclaredFileHash:    req.ContentHash,
			ValidationDetails:   "",
			EntityID:            entityID, // Entity ID (type is implied by ObjectType)
		}
	}

	// Bulk create all attachments in a single operation
	if err := m.persistence.CreateAttachments(ctx, attachments); err != nil {
		return nil, contextualAttachmentError(
			fmt.Errorf("failed to bulk create attachments (userID=%d, entityID=%s, count=%d): %w", userID, entityID, len(requests), err),
			zap.String(logkeys.Stage, "bulk_create_entity_attachments"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEntityID, entityID),
			zap.Int(appLogger.KeyCount, len(requests)))
	}

	// Generate presigned URLs in parallel for better performance
	type urlResult struct {
		index     int
		uploadURL string
		formData  map[string]string
		err       error
	}

	urlChan := make(chan urlResult, len(attachments))
	var wg sync.WaitGroup

	maxSize := objectTypeConfig.MaxSize
	if maxSize <= 0 {
		maxSize = runtime.AttachmentDefaultMaxSize()
	}
	for i, attachment := range attachments {
		wg.Add(1)
		go func(idx int, att *common_persistence_models.Attachment) {
			defer wg.Done()

			uploadURL, formData, err := m.presignEntityUpload(ctx, att, runtime, maxSize, expiresAt)

			urlChan <- urlResult{
				index:     idx,
				uploadURL: uploadURL,
				formData:  formData,
				err:       err,
			}
		}(i, attachment)
	}

	// Wait for all URL generations to complete
	wg.Wait()
	close(urlChan)

	// Collect results
	uploadResults := make([]*IssuedUploadURL, len(attachments))

	for result := range urlChan {
		// Validate index to prevent panic
		if err := validateAttachmentUploadResultIndex(result.index, len(attachments)); err != nil {
			return nil, err
		}

		if result.err != nil {
			return nil, contextualAttachmentError(result.err,
				zap.String(logkeys.Stage, "generate_bulk_attachment_upload_url"),
				zap.Uint64(appLogger.KeyAttachmentID, attachments[result.index].ID),
				zap.String(appLogger.KeyFilename, attachments[result.index].OriginalFileName))
		}
		uploadResults[result.index] = &IssuedUploadURL{
			Attachment: attachments[result.index],
			UploadURL:  result.uploadURL,
			FormData:   result.formData,
			ExpiresAt:  expiresAt,
		}
	}

	if appLogger.DebugEnabled(m.logger) {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Successfully created bulk attachments with upload URLs",
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyObjectType, objectType),
			zap.String(appLogger.KeyEntityID, entityID),
			zap.Int(appLogger.KeyCount, len(attachments)))
	}

	return uploadResults, nil
}

// presignEntityUpload issues the presigned POST, expiring at expiresAt, for an entity attachment's
// temporary object, rewritten onto the public base URL when one is configured (a failed rewrite
// keeps the original). The row must already record a deadline (UploadURLExpiresAt) no earlier
// than expiresAt.
func (m *AttachmentLifecycleManager) presignEntityUpload(
	ctx context.Context,
	att *common_persistence_models.Attachment,
	runtime RuntimeConfigView,
	maxSize int64,
	expiresAt time.Time,
) (string, map[string]string, error) {
	uploadURL, formData, err := m.persistence.GeneratePresignedPostURL(
		ctx,
		att.TemporaryObjectName,
		expiresAt,
		0,
		maxSize,
		att.ContentType,
	)
	if err != nil || runtime.MinioPublicBaseURL() == "" {
		return uploadURL, formData, err
	}
	rewrittenURL, rewriteErr := common_service_models.RewritePresignedURL(uploadURL, runtime.MinioPublicBaseURL())
	if rewriteErr != nil {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Failed to rewrite presigned URL in bulk operation; using original URL",
			zap.Error(rewriteErr),
			zap.String(logkeys.Stage, "rewrite_bulk_upload_url"),
			zap.String(logkeys.Fallback, "original presigned URL"),
			zap.String(appLogger.KeyOriginalURL, uploadURL),
			zap.String(appLogger.KeyPublicBaseURL, runtime.MinioPublicBaseURL()),
			zap.Uint64(appLogger.KeyAttachmentID, att.ID))
		return uploadURL, formData, nil
	}
	return rewrittenURL, formData, nil
}

// ReissueUploadURLs issues fresh presigned upload URLs for entity attachments that still wait for
// their upload, for an idempotent retry of the request that created them. Each URL targets the
// attachment's existing temporary object under the same size limit the first issuance used, and
// expires no later than the deadline its row records before signing (RenewPendingUploads), which
// pending-upload cleanup counts from, so cleanup cannot reject the row while the URL is valid.
// Attachments in any other state, or finalized or rejected meanwhile, get none. The runtime
// provider is sampled once for the batch.
func (m *AttachmentLifecycleManager) ReissueUploadURLs(ctx context.Context, attachments []*common_persistence_models.Attachment) ([]*IssuedUploadURL, error) {
	ids := make([]uint64, 0, len(attachments))
	for _, att := range attachments {
		if att != nil && att.UploadStatus == common_persistence_models.UploadStatusPendingUpload && att.TemporaryObjectName != "" {
			ids = append(ids, att.ID)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	runtime := m.runtimeConfigProvider()
	expiresAt := time.Now().UTC().Add(runtime.AttachmentUploadURLExpiry())
	renewed, err := m.persistence.RenewPendingUploads(ctx, ids, expiresAt)
	if err != nil {
		return nil, contextualAttachmentError(err, zap.String(logkeys.Stage, "renew_attachment_upload_window"))
	}
	renewedByID := make(map[uint64]*common_persistence_models.Attachment, len(renewed))
	for _, att := range renewed {
		renewedByID[att.ID] = att
	}
	issued := make([]*IssuedUploadURL, 0, len(renewed))
	for _, id := range ids {
		att, ok := renewedByID[id]
		if !ok {
			continue
		}
		maxSize := runtime.AttachmentDefaultMaxSize()
		if config := m.objectTypeRegistry.Get(att.ObjectType); config != nil && config.MaxSize > 0 {
			maxSize = config.MaxSize
		}
		uploadURL, formData, err := m.presignEntityUpload(ctx, att, runtime, maxSize, expiresAt)
		if err != nil {
			return nil, contextualAttachmentError(err,
				zap.String(logkeys.Stage, "reissue_attachment_upload_url"),
				zap.Uint64(appLogger.KeyAttachmentID, att.ID))
		}
		issued = append(issued, &IssuedUploadURL{
			Attachment: att,
			UploadURL:  uploadURL,
			FormData:   formData,
			ExpiresAt:  expiresAt,
		})
	}
	return issued, nil
}

// ProcessUploadedAttachment validates and processes an uploaded attachment using streaming validation.
// This method performs memory-efficient validation without loading entire files into memory,
func (m *AttachmentLifecycleManager) ProcessUploadedAttachment(
	ctx context.Context,
	attachmentID uint64,
	validators ...StreamingAttachmentValidator,
) error {
	// Execute core processing logic with streaming validation
	attachment, err := m.processUploadedAttachmentCore(ctx, attachmentID, validators...)
	if err != nil {
		return err
	}

	// Invoke entity-specific callback if attachment is linked to an entity
	objectTypeConfig := m.objectTypeRegistry.Get(attachment.ObjectType)
	if attachment.EntityID != "" && objectTypeConfig != nil {
		// Get the implied entity type for logging
		impliedEntityType := objectTypeConfig.ImpliedEntityType

		// Invoke entity batch callback if configured
		if objectTypeConfig.EntityBatchCallback != nil {
			if appLogger.DebugEnabled(m.logger) {
				appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Invoking entity batch callback (single attachment)",
					zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
					zap.String(appLogger.KeyObjectType, attachment.ObjectType),
					zap.String(appLogger.KeyImpliedEntityType, impliedEntityType),
					zap.String(appLogger.KeyEntityID, attachment.EntityID))
			}

			// Call batch callback with single attachment for consistency
			if err := objectTypeConfig.EntityBatchCallback(ctx, []*common_persistence_models.Attachment{attachment}, attachment.EntityID); err != nil {
				m.logEntityCallbackFailure(ctx, err,
					zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
					zap.String(appLogger.KeyObjectType, attachment.ObjectType),
					zap.String(appLogger.KeyImpliedEntityType, impliedEntityType),
					zap.String(appLogger.KeyEntityID, attachment.EntityID))
			} else if appLogger.DebugEnabled(m.logger) {
				appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Entity batch callback completed successfully",
					zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
					zap.String(appLogger.KeyObjectType, attachment.ObjectType),
					zap.String(appLogger.KeyImpliedEntityType, impliedEntityType),
					zap.String(appLogger.KeyEntityID, attachment.EntityID))
			}
		}
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Successfully processed attachment",
		zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
		zap.String(appLogger.KeyFinalObjectName, attachment.FinalObjectName))

	return nil
}

func (m *AttachmentLifecycleManager) logEntityCallbackFailure(ctx context.Context, err error, fields ...zap.Field) {
	level := zapcore.WarnLevel
	if override, ok := appLogger.ErrorLevel(err); ok {
		level = override
	}
	logFields := append(appLogger.ErrorFields(err), fields...)
	logFields = append(logFields,
		zap.String(logkeys.Stage, "entity_batch_callback"),
		zap.String(logkeys.Fallback, "continue attachment processing without entity callback"))
	appLogger.LogOperation(appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName), level, "Entity batch callback failed", "attachment_entity_batch_callback", err, logFields...)
}

// processUploadedAttachmentCore handles the core attachment processing logic using streaming validation.
func (m *AttachmentLifecycleManager) processUploadedAttachmentCore(
	ctx context.Context,
	attachmentID uint64,
	validators ...StreamingAttachmentValidator,
) (*common_persistence_models.Attachment, error) {
	// Get the attachment record
	attachment, err := m.persistence.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return nil, contextualAttachmentError(
			fmt.Errorf("failed to get attachment for processing (attachmentID=%d): %w", attachmentID, err),
			zap.String(logkeys.Stage, "load_attachment_for_processing"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}
	if attachment == nil || attachment.InternalOnly {
		return nil, ErrInternalAttachmentAccess
	}
	previousStatus := attachment.UploadStatus
	if previousStatus != common_persistence_models.UploadStatusPendingUpload &&
		previousStatus != common_persistence_models.UploadStatusPendingProcessing {
		return nil, fmt.Errorf("%w: status=%s", ErrAttachmentNotProcessable, previousStatus)
	}

	// Claim processing with a compare-and-swap. A cleanup worker can race this
	// method after the read above; only one transition may win, and a stale
	// worker must never resurrect a row already rejected by pruning.
	now := time.Now().UTC()
	attachment.UploadStatus = common_persistence_models.UploadStatusProcessing
	attachment.ProcessingStartTime = &now
	claimed, err := m.persistence.UpdateAttachmentIfStatus(ctx, attachment, previousStatus)
	if err != nil {
		return nil, contextualAttachmentError(
			fmt.Errorf("failed to update attachment status to processing (attachmentID=%d): %w", attachmentID, err),
			zap.String(logkeys.Stage, "claim_attachment_for_processing"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.Stringer(logkeys.Expected, previousStatus),
			zap.Stringer(logkeys.Actual, attachment.UploadStatus))
	}
	if !claimed {
		return nil, fmt.Errorf("attachment state changed before processing could begin (attachmentID=%d, expectedStatus=%s)", attachmentID, previousStatus)
	}

	// Get object info to check size without loading into memory. This stat is
	// also the observation every later step is pinned to (see sourceVersion).
	objInfo, err := m.persistence.GetObjectInfo(ctx, attachment.TemporaryObjectName)
	if err != nil {
		return m.rejectAttachment(ctx, attachment, now, "Failed to retrieve uploaded content info", contextualAttachmentError(
			fmt.Errorf("failed to get object info for attachment (objectName=%s): %w", attachment.TemporaryObjectName, err),
			zap.String(logkeys.Stage, "inspect_uploaded_attachment_object"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyObjectName, attachment.TemporaryObjectName)))
	}

	// Pin the exact stored version that was just observed. The client's
	// presigned POST stays valid for its whole expiry and targets this same
	// key, so a replay can overwrite the object at any point during
	// finalization. Every read below and the promotion copy carry this ETag as
	// a precondition: the store then refuses a stale observation atomically,
	// and nothing that was not hashed and validated can ever be promoted.
	if objInfo.ETag == "" {
		invariantErr := appLogger.InvariantError("uploaded_attachment_object_has_etag",
			fmt.Errorf("%w: object store returned no ETag", ErrValidationFailed),
			zap.String(logkeys.Stage, "pin_uploaded_attachment_object"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyObjectName, attachment.TemporaryObjectName),
			zap.String(logkeys.Expected, "non-empty object ETag"),
			zap.String(logkeys.Actual, "empty"))
		return m.rejectAttachment(ctx, attachment, now, "Uploaded content could not be pinned for validation",
			invariantErr)
	}
	sourceVersion := common_persistence.ObjectPrecondition{MatchETag: objInfo.ETag}

	// Update the actual size from object info
	attachment.Size = objInfo.Size

	// Calculate both salted and unsalted content hashes in a single pass
	// This is more efficient than calculating them separately
	dualHashes, err := m.persistence.CalculateDualHashesStream(ctx, attachment.TemporaryObjectName, sourceVersion)
	if err != nil {
		if errors.Is(err, common_persistence.ErrObjectPreconditionFailed) {
			return m.rejectChangedUpload(ctx, attachment, now, "hashing")
		}
		return m.rejectAttachment(ctx, attachment, now, "Failed to calculate content hash", contextualAttachmentError(
			fmt.Errorf("failed to calculate content hashes for attachment (objectName=%s): %w", attachment.TemporaryObjectName, err),
			zap.String(logkeys.Stage, "hash_uploaded_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyObjectName, attachment.TemporaryObjectName)))
	}

	// Set the unsalted hash in FileHash for external verification
	attachment.FileHash = dualHashes.Unsalted

	// Use the salted hash for storage path to prevent rainbow table attacks
	verifiedPath := fmt.Sprintf("%s%s", VerifiedPathPrefix, dualHashes.Salted)
	// Store the full verified path in FinalObjectName
	attachment.FinalObjectName = verifiedPath

	// SECURITY FIX: Always run streaming validation BEFORE checking for deduplication
	// This ensures that ALL attachments undergo security validation regardless of
	// whether identical content already exists in verified storage

	operationLogger := appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName)
	if ce := operationLogger.Check(zap.DebugLevel, "Starting streaming validation for attachment"); ce != nil {
		ce.Write(
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.Bool(appLogger.KeyHasCustomValidators, len(validators) > 0),
		)
	}

	// Get object type configuration for built-in validators
	objectTypeConfig := m.objectTypeRegistry.Get(attachment.ObjectType)
	allValidators := objectTypeValidators(objectTypeConfig, m.runtimeConfigProvider().AttachmentXMLMaxTokenBytes(), validators)

	// Run streaming validation if we have any validators
	if len(allValidators) > 0 {
		// Get reader for streaming validation
		reader, err := m.persistence.GetObjectReader(ctx, attachment.TemporaryObjectName, sourceVersion)
		if err != nil {
			if errors.Is(err, common_persistence.ErrObjectPreconditionFailed) {
				return m.rejectChangedUpload(ctx, attachment, now, "validation")
			}
			return m.rejectAttachment(ctx, attachment, now, "Failed to access file for validation", contextualAttachmentError(
				fmt.Errorf("failed to get object reader for validation (objectName=%s): %w", attachment.TemporaryObjectName, err),
				zap.String(logkeys.Stage, "open_attachment_validation_reader"),
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyObjectName, attachment.TemporaryObjectName)))
		}
		defer reader.Close()

		// Streaming validators require a seekable reader so each one can scan
		// the object from the start; reject up front if the store's reader
		// cannot seek instead of panicking mid-validation.
		seeker, ok := reader.(io.ReadSeeker)
		if !ok {
			return m.rejectAttachment(ctx, attachment, now, "File could not be validated", appLogger.InvariantError("attachment_validation_reader_is_seekable",
				fmt.Errorf("object reader for %s does not support seeking, cannot run streaming validators: %w", attachment.TemporaryObjectName, ErrValidationFailed),
				zap.String(logkeys.Stage, "prepare_attachment_validation_reader"),
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyObjectName, attachment.TemporaryObjectName),
				zap.String(logkeys.Expected, "io.ReadSeeker"),
				zap.String(logkeys.Actual, fmt.Sprintf("%T", reader))))
		}

		// Run all streaming validators
		for _, validator := range allValidators {
			// Reset reader position for each validator
			_, err := seeker.Seek(0, io.SeekStart)
			if err != nil {
				if readerHitPrecondition(reader) {
					return m.rejectChangedUpload(ctx, attachment, now, "validation")
				}
				operationLogger.Error("Failed to reset reader position",
					zap.Error(err),
					zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
					zap.String(appLogger.KeyValidator, validator.Name()))
				// Continue with validation despite seek error
			}

			isValid, validationDetails := validator.Validate(ctx, seeker, attachment.Size, attachment.ContentType)
			if !isValid {
				// Validators report only pass/fail. If the failure was actually the
				// store refusing a re-read of a changed object, classify it as a
				// replay rather than trusting the validator's wording (a security
				// validator's read-failure text would otherwise be mistaken for a
				// content security violation).
				if readerHitPrecondition(reader) {
					return m.rejectChangedUpload(ctx, attachment, now, "validation")
				}
				// Security logging: Check if this looks like a security violation
				isSecurityViolation := false
				if strings.Contains(validationDetails, "dangerous") ||
					strings.Contains(validationDetails, "spoofing") ||
					strings.Contains(validationDetails, "security") ||
					strings.Contains(validationDetails, "executable") {
					isSecurityViolation = true
					operationLogger.Warn("SECURITY: Security violation detected during validation",
						zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
						zap.Uint64(appLogger.KeyUserID, attachment.UserID),
						zap.String(appLogger.KeyDeclaredContentType, attachment.ContentType),
						zap.String(appLogger.KeyOriginalFilename, attachment.OriginalFileName),
						zap.String(appLogger.KeyObjectType, attachment.ObjectType),
						zap.String(appLogger.KeyValidator, validator.Name()),
						zap.String(appLogger.KeyValidationFailure, validationDetails))
				} else {
					// Regular validation failure logging
					operationLogger.Info("Streaming validation failed",
						zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
						zap.Uint64(appLogger.KeyUserID, attachment.UserID),
						zap.String(appLogger.KeyContentType, attachment.ContentType),
						zap.String(appLogger.KeyObjectType, attachment.ObjectType),
						zap.String(appLogger.KeyValidator, validator.Name()),
						zap.String(appLogger.KeyValidationDetails, validationDetails))
				}

				// Update status to rejected
				attachment.UploadStatus = common_persistence_models.UploadStatusRejected
				attachment.ValidationDetails = fmt.Sprintf("%s: %s", validator.Name(), validationDetails)
				attachment.ProcessingEndTime = &now
				err = m.persistence.UpdateAttachment(ctx, attachment)
				if err != nil {
					operationLogger.Error("Failed to update attachment with validation rejection",
						zap.Error(err),
						zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
				} else {
					m.cleanupTemporaryObjectAfterCommit(ctx, attachmentID, attachment.TemporaryObjectName)
				}

				// Return appropriate error based on whether this is a security violation
				if isSecurityViolation {
					// Return a wrapped error that can be checked with errors.Is()
					return attachment, fmt.Errorf("%w: %s", apperrors.ErrAttachmentSecurityViolation, validationDetails)
				}
				return attachment, fmt.Errorf("%w: %s", ErrValidationFailed, validationDetails)
			}

			if ce := operationLogger.Check(zap.DebugLevel, "Streaming validator passed"); ce != nil {
				ce.Write(
					zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
					zap.String(appLogger.KeyValidator, validator.Name()),
				)
			}
		}

		if ce := operationLogger.Check(zap.DebugLevel, "All streaming validations passed"); ce != nil {
			ce.Write(
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.Int(appLogger.KeyValidatorCount, len(allValidators)),
			)
		}
	}

	// Publish the deterministic final object and VETTED row while holding the
	// same advisory lock used by post-delete reclamation. This preserves
	// deduplication without allowing cleanup to remove an object between an
	// existence check and the row update.
	attachment.UploadStatus = common_persistence_models.UploadStatusVetted
	attachment.ProcessingEndTime = &now
	err = m.persistence.PublishPublicAttachmentObject(ctx, attachment, sourceVersion)
	if err != nil {
		if errors.Is(err, common_persistence.ErrObjectPreconditionFailed) {
			// The store refused to copy a version other than the one that was
			// hashed and validated; the row is still PROCESSING.
			return m.rejectChangedUpload(ctx, attachment, now, "promotion")
		}
		return attachment, contextualAttachmentError(
			fmt.Errorf("failed to publish vetted attachment (attachmentID=%d): %w", attachmentID, err),
			zap.String(logkeys.Stage, "publish_vetted_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}

	// Invoke post-process callback if configured for this object type
	// Note: objectTypeConfig was already retrieved during validation above
	if objectTypeConfig != nil && objectTypeConfig.PostProcessCallback != nil {
		if ce := operationLogger.Check(zap.DebugLevel, "Invoking post-process callback"); ce != nil {
			ce.Write(
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyObjectType, attachment.ObjectType),
			)
		}

		if err := objectTypeConfig.PostProcessCallback(ctx, attachment); err != nil {
			return m.rejectAttachment(ctx, attachment, now, fmt.Sprintf("Post-process callback failed: %v", err), contextualAttachmentError(
				fmt.Errorf("post-process callback failed (objectType=%s): %w", attachment.ObjectType, err),
				zap.String(logkeys.Stage, "post_process_attachment"),
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyObjectType, attachment.ObjectType)))
		}

		if ce := operationLogger.Check(zap.DebugLevel, "Post-process callback completed successfully"); ce != nil {
			ce.Write(
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyObjectType, attachment.ObjectType),
			)
		}
	}

	// TODO(object-refcounting): verified objects (verified/<salted-hash>) are
	// content-addressed and may be shared: identical uploads, internal agent
	// media and provider state, and chat-fork clones (CopyChatChannelFork keeps
	// FinalObjectName) all name the same object. No path deletes a verified
	// object until proper reference counting lands: user and admin attachment
	// deletion, internal reclamation and DeleteInternal remove rows only.
	// Orphaned objects are accepted until then. The temporary object is
	// different: its key is unique to this upload and the verified copy is
	// durable once the surrounding transaction commits, so it is released
	// post-commit (retained on rollback so a re-finalize still has its source).
	m.cleanupTemporaryObjectAfterCommit(ctx, attachmentID, attachment.TemporaryObjectName)

	operationLogger.Info("Successfully processed attachment core logic",
		zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
		zap.String(appLogger.KeyFinalObjectName, attachment.FinalObjectName))

	return attachment, nil
}

// GetAttachmentURL resolves the client-facing URL for a vetted attachment and is
// the single URL issuance path for user-visible content. It dispatches on the
// row's stored visibility because visibility selects the bucket the verified
// object lives in: a private attachment is served through a short-lived presigned
// URL against the private bucket, while a public attachment's object exists only
// in the public bucket and is served from its stable, anonymously fetchable path.
// expiry therefore applies only to the private branch; a public URL never expires.
// An expiry <= 0 selects the hot-configured AttachmentDownloadURLExpiry default.
func (m *AttachmentLifecycleManager) GetAttachmentURL(
	ctx context.Context,
	attachmentID uint64,
	expiry time.Duration,
) (string, error) {
	// Get the attachment record
	attachment, err := m.persistence.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return "", contextualAttachmentError(
			fmt.Errorf("failed to get attachment for download URL (attachmentID=%d): %w", attachmentID, err),
			zap.String(logkeys.Stage, "load_attachment_for_download_url"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}
	return m.ResolveAttachmentURL(ctx, attachment, expiry)
}

// ResolveAttachmentURL resolves the client-facing URL for an attachment row the
// caller has already loaded, applying the same validation and visibility dispatch
// as GetAttachmentURL. Callers that list attachments hold their rows already, so
// resolving from the row avoids re-reading each one by ID.
//
// Parameters:
//   - ctx: propagates request-scoped deadlines, cancellation, and trace metadata.
//   - attachment: the attachment row to expose; internal and non-vetted rows are rejected.
//   - expiry: bounds the lifetime of a presigned private URL; public URLs never
//     expire. <= 0 selects the hot-configured AttachmentDownloadURLExpiry default.
//
// Returns:
//   - result1: the client-facing URL for the attachment's verified object.
//   - err: reports any failure encountered while running this operation.
func (m *AttachmentLifecycleManager) ResolveAttachmentURL(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	expiry time.Duration,
) (string, error) {
	if attachment == nil || attachment.InternalOnly {
		return "", ErrInternalAttachmentAccess
	}

	// Check if the attachment is vetted
	if attachment.UploadStatus != common_persistence_models.UploadStatusVetted {
		err := fmt.Errorf("attachment not ready for download: status=%s", attachment.UploadStatus)
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Attempted to get download URL for non-vetted attachment",
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.Stringer(logkeys.Status, attachment.UploadStatus))
		return "", err
	}

	// InternalOnly rows never reach this point, so visibility alone selects the
	// bucket the object was published into. The runtime view is sampled once per
	// call so the default expiry, presign host, quantum, and region all come
	// from one coherent configuration generation.
	view := m.runtimeConfigProvider()
	if common_persistence_models.IsPublicAttachmentVisibility(attachment.Visibility) {
		return m.composePublicAttachmentURL(ctx, attachment, view)
	}
	effectiveExpiry := expiry
	if effectiveExpiry <= 0 {
		effectiveExpiry = view.AttachmentDownloadURLExpiry()
	}
	return m.presignPrivateAttachmentURL(ctx, attachment, effectiveExpiry, view)
}

// composePublicAttachmentURL builds the stable public URL for a vetted public
// attachment as <MinioPublicBaseURL>/<MinioPublicBucket>/<FinalObjectName>.
// MinioPublicBaseURL is the public origin fronting the MinIO server root, the same
// origin presigned private URLs are rewritten to, so the bucket segment is what
// distinguishes the two and keeps an attachment's visibility deducible from its
// URL. Both values are therefore required: a deployment that publishes public
// objects must configure them, and composing against an empty one would silently
// emit an unfetchable path instead of a URL.
//
// Parameters:
//   - ctx: propagates request-scoped deadlines, cancellation, and trace metadata.
//   - attachment: the vetted, non-internal attachment row being exposed.
//
// Returns:
//   - result1: the anonymously fetchable URL for the attachment's verified object.
//   - err: reports any failure encountered while running this operation.
func (m *AttachmentLifecycleManager) composePublicAttachmentURL(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	view RuntimeConfigView,
) (string, error) {
	publicBaseURL := view.MinioPublicBaseURL()

	if attachment.FinalObjectName == "" {
		return "", appLogger.InvariantError("public_attachment_has_verified_object",
			fmt.Errorf("failed to compose public URL for attachment (attachmentID=%d): verified object name is empty", attachment.ID),
			zap.String(logkeys.Stage, "compose_public_attachment_url"),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(logkeys.Expected, "non-empty verified object name"),
			zap.String(logkeys.Actual, "empty"))
	}
	if publicBaseURL == "" {
		return "", appLogger.InvariantError("public_attachment_base_url_configured",
			fmt.Errorf("failed to compose public URL for attachment (attachmentID=%d): public base URL is not configured", attachment.ID),
			zap.String(logkeys.Stage, "compose_public_attachment_url"),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(appLogger.KeyFinalObjectName, attachment.FinalObjectName),
			zap.String(logkeys.Expected, "non-empty public base URL"),
			zap.String(logkeys.Actual, "empty"))
	}
	if m.minioPublicBucket == "" {
		return "", appLogger.InvariantError("public_attachment_bucket_configured",
			fmt.Errorf("failed to compose public URL for attachment (attachmentID=%d): public bucket is not configured", attachment.ID),
			zap.String(logkeys.Stage, "compose_public_attachment_url"),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(appLogger.KeyFinalObjectName, attachment.FinalObjectName),
			zap.String(logkeys.Expected, "non-empty public bucket"),
			zap.String(logkeys.Actual, "empty"))
	}

	publicURL := common_service_models.ComposePublicAttachmentURL(
		attachment.FinalObjectName,
		publicBaseURL,
		m.minioPublicBucket,
	)

	if appLogger.DebugEnabled(m.logger) {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Composed public attachment URL",
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(appLogger.KeyFinalObjectName, attachment.FinalObjectName))
	}

	return publicURL, nil
}

// presignPrivateAttachmentURL generates a time-limited presigned GET URL for a
// vetted private attachment, providing secure access to the private bucket
// without exposing the object anonymously. When the sampled view configures a
// presign host, the URL is signed offline for that public origin (the canonical
// request an edge SigV4 verifier reconstructs), so no host rewrite is applied;
// otherwise the SDK presign against the internal endpoint plus host
// rewrite is used. A configured quantum floors the signing time so URLs are
// byte-identical within the quantum window.
//
// Parameters:
//   - ctx: propagates request-scoped deadlines, cancellation, and trace metadata.
//   - attachment: the vetted, non-internal attachment row being exposed.
//   - expiry: how long the issued URL remains valid; already resolved to > 0.
//   - view: the coherent runtime view sampled once by ResolveAttachmentURL.
//
// Returns:
//   - result1: the presigned download URL for the attachment's verified object.
//   - err: reports any failure encountered while running this operation.
func (m *AttachmentLifecycleManager) presignPrivateAttachmentURL(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	expiry time.Duration,
	view RuntimeConfigView,
) (string, error) {
	// Use the verified path directly from FinalObjectName (already contains "verified/" prefix)
	verifiedPath := attachment.FinalObjectName

	if host := view.AttachmentPresignHost(); host != "" {
		signingTime := time.Now().UTC()
		if q := view.AttachmentPresignQuantum(); q > 0 && q < expiry {
			// Floor the signing time to the quantum boundary so every URL issued
			// within the window is byte-identical; the expiry counts from the
			// quantum start, which is why the quantum must stay below it.
			signingTime = signingTime.Truncate(q)
		} else if q > 0 && appLogger.DebugEnabled(m.logger) {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Presign quantum is not below the effective expiry; signing unquantized",
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
				appLogger.DurationSeconds(appLogger.KeyQuantumSeconds, q),
				appLogger.DurationSeconds(appLogger.KeyExpirySeconds, expiry))
		}

		region := view.AttachmentPresignRegion()
		if region == "" {
			appLogger.LogInvariantViolation(appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName), "attachment_presign_region_non_empty",
				nil,
				zap.String(logkeys.Stage, "presign_private_attachment_url"),
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
				zap.String(logkeys.Expected, "non-empty signing region"),
				zap.String(logkeys.Actual, "empty"),
				zap.String(logkeys.Fallback, defaultPresignRegion))
			region = defaultPresignRegion
		}
		presignedURL, err := m.persistence.PresignHostScopedDownloadURL(ctx, verifiedPath, host, region, expiry, signingTime,
			view.AttachmentPresignAccessKey(), view.AttachmentPresignSecretKey())
		if err != nil {
			return "", contextualAttachmentError(
				fmt.Errorf("failed to generate host-scoped download URL for attachment (attachmentID=%d, verifiedPath=%s): %w", attachment.ID, verifiedPath, err),
				zap.String(logkeys.Stage, "generate_host_scoped_attachment_download_url"),
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
				zap.String(appLogger.KeyVerifiedPath, verifiedPath))
		}

		if appLogger.DebugEnabled(m.logger) {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Generated host-scoped download URL",
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
				zap.String(appLogger.KeyVerifiedPath, verifiedPath))
		}

		// The signature already covers the public host, so the URL must be
		// returned as signed: a host rewrite would invalidate it.
		return presignedURL, nil
	}

	// SDK path: SDK presign against the internal endpoint, then host rewrite.
	presignedURL, err := m.persistence.GetPresignedURL(ctx, verifiedPath, expiry)
	if err != nil {
		return "", contextualAttachmentError(
			fmt.Errorf("failed to generate download URL for attachment (attachmentID=%d, verifiedPath=%s): %w", attachment.ID, verifiedPath, err),
			zap.String(logkeys.Stage, "generate_attachment_download_url"),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(appLogger.KeyVerifiedPath, verifiedPath))
	}

	// Rewrite the presigned URL to use the public base URL if configured
	if view.MinioPublicBaseURL() != "" {
		rewrittenURL, err := common_service_models.RewritePresignedURL(presignedURL, view.MinioPublicBaseURL())
		if err != nil {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Failed to rewrite download URL; using original URL",
				zap.Error(err),
				zap.String(logkeys.Stage, "rewrite_download_url"),
				zap.String(logkeys.Fallback, "original presigned URL"),
				zap.String(appLogger.KeyOriginalURL, presignedURL),
				zap.String(appLogger.KeyPublicBaseURL, view.MinioPublicBaseURL()))
		} else {
			presignedURL = rewrittenURL
		}
	}

	if appLogger.DebugEnabled(m.logger) {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Generated download URL",
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(appLogger.KeyVerifiedPath, verifiedPath))
	}

	return presignedURL, nil
}

// PruneTemporaryUploads rejects non-internal uploads still pending after olderThan and deletes
// their temporary objects. The cleanup routine runs it every tick with AttachmentTemporaryUploadPruneAge.
func (m *AttachmentLifecycleManager) PruneTemporaryUploads(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoffTime := time.Now().UTC().Add(-olderThan)
	now := time.Now().UTC()
	claimedAttachments, err := m.persistence.ClaimExpiredPublicAttachments(ctx, cutoffTime, now)
	if err != nil {
		return 0, contextualAttachmentError(
			fmt.Errorf("failed to claim pending attachments for pruning (olderThan=%v, cutoffTime=%v): %w", olderThan, cutoffTime, err),
			zap.String(logkeys.Stage, "claim_pending_attachments_for_pruning"),
			appLogger.DurationSeconds(appLogger.KeyOlderThanSeconds, olderThan),
			zap.Time(appLogger.KeyCutoffTime, cutoffTime))
	}

	if len(claimedAttachments) == 0 {
		if appLogger.DebugEnabled(m.logger) {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("No pending attachments found to prune",
				zap.Time(appLogger.KeyCutoffTime, cutoffTime))
		}
		return 0, nil
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Claimed pending attachments to prune",
		zap.Int(appLogger.KeyCount, len(claimedAttachments)),
		zap.Time(appLogger.KeyCutoffTime, cutoffTime))

	// The claim update is the authority for deletion. Rows omitted because a
	// concurrent worker already moved them to PROCESSING/VETTED are never
	// included here. If the caller owns a transaction, cleanup is post-commit;
	// an unhooked external transaction safely leaks for later object-store GC.
	for _, attachment := range claimedAttachments {
		m.cleanupTemporaryObjectAfterCommit(ctx, attachment.ID, attachment.TemporaryObjectName)
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Completed pruning temporary uploads",
		zap.Int(appLogger.KeyCount, len(claimedAttachments)))

	return len(claimedAttachments), nil
}

// ReleaseSettledTemporaryObjects deletes up to limit temporary objects still named by vetted,
// rejected, or deleted attachments whose upload URLs have expired, and clears each name once
// its object is gone. It backstops the best-effort post-commit deletion, which a storage error
// or an unhooked caller transaction can skip, releases uploads whose rows were deleted before
// they settled, and deletes objects a replayed upload recreated after that deletion. It stops
// at the first storage error, so an outage costs one attempt per tick.
func (m *AttachmentLifecycleManager) ReleaseSettledTemporaryObjects(ctx context.Context, limit int) (int, error) {
	settled, err := m.persistence.ListSettledTemporaryObjects(ctx, time.Now().UTC(), limit)
	if err != nil {
		return 0, err
	}
	for released, attachment := range settled {
		if err := m.persistence.RemoveObject(ctx, attachment.TemporaryObjectName); err != nil {
			return released, err
		}
		if err := m.persistence.ClearTemporaryObjectName(ctx, attachment.ID, attachment.TemporaryObjectName); err != nil {
			return released, err
		}
	}
	return len(settled), nil
}

// deleteObjectFromTemporaryStorage removes a temporary storage object after pruning or post-processing cleanup.
//
// Parameters:
//   - ctx: Context for storage deletion and logging.
//   - objectName: Temporary object name to remove from storage.
//
// Returns:
//   - error: Storage deletion error.
func (m *AttachmentLifecycleManager) deleteObjectFromTemporaryStorage(ctx context.Context, objectName string) error {
	// Actually delete the object using the RemoveObject method
	err := m.persistence.RemoveObject(ctx, objectName)
	if err != nil {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Failed to delete temporary object",
			zap.Error(err),
			zap.String(appLogger.KeyObjectName, objectName))
		return err
	}
	return nil
}

// readerHitPrecondition reports whether a pinned object reader has recorded a
// precondition failure (the object changed after it was pinned). Readers that
// are not ObjectReadErrorReporters — unpinned reads, test doubles — never do.
func readerHitPrecondition(reader io.Reader) bool {
	reporter, ok := reader.(common_persistence.ObjectReadErrorReporter)
	return ok && errors.Is(reporter.LastReadError(), common_persistence.ErrObjectPreconditionFailed)
}

// rejectAttachment writes the REJECTED outcome for an attachment that is still
// PROCESSING, releases its temporary object once the caller's work commits,
// and returns the attachment with cause (which callers can errors.Is on). The
// rejection is durable whenever the surrounding transaction commits — the
// FinalizeAttachments batch path always does, because it folds cause into a
// per-attachment result — but a caller that lets cause abort its own
// transaction rolls the row back to its pending state and keeps the source
// object, so the next finalize re-validates whatever bytes are present then.
func (m *AttachmentLifecycleManager) rejectAttachment(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	now time.Time,
	details string,
	cause error,
) (*common_persistence_models.Attachment, error) {
	attachment.UploadStatus = common_persistence_models.UploadStatusRejected
	attachment.ValidationDetails = details
	attachment.ProcessingEndTime = &now
	if updateErr := m.persistence.UpdateAttachment(ctx, attachment); updateErr != nil {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Error("Failed to record attachment rejection",
			zap.Error(updateErr),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(appLogger.KeyValidationDetails, details))
	} else {
		m.cleanupTemporaryObjectAfterCommit(ctx, attachment.ID, attachment.TemporaryObjectName)
	}

	return attachment, fmt.Errorf("%w (attachmentID=%d)", cause, attachment.ID)
}

// rejectChangedUpload records that the pinned temporary object was overwritten
// while it was being finalized (stage names the step whose conditional read or
// copy the store refused). This only happens when the client's presigned POST
// is replayed after finalization began, so it is logged as a security event.
// Whatever is in the object now was never validated, and whatever was
// validated no longer exists, so nothing is promoted; see rejectAttachment for
// when the REJECTED row itself is durable.
func (m *AttachmentLifecycleManager) rejectChangedUpload(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	now time.Time,
	stage string,
) (*common_persistence_models.Attachment, error) {
	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("SECURITY: Uploaded content changed after finalization pinned it; rejecting attachment",
		zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
		zap.Uint64(appLogger.KeyUserID, attachment.UserID),
		zap.String(appLogger.KeyObjectType, attachment.ObjectType),
		zap.String(appLogger.KeyObjectName, attachment.TemporaryObjectName),
		zap.String(logkeys.Operation, stage))

	return m.rejectAttachment(ctx, attachment, now, "Uploaded content changed during validation",
		fmt.Errorf("%w (stage=%s)", ErrAttachmentContentChanged, stage))
}

// cleanupTemporaryObjectAfterCommit performs best-effort cleanup without blocking request completion.
func (m *AttachmentLifecycleManager) cleanupTemporaryObjectAfterCommit(
	ctx context.Context,
	attachmentID uint64,
	objectName string,
) {
	if objectName == "" {
		return
	}

	timeout := m.runtimeConfigProvider().AttachmentTemporaryObjectCleanupTimeout()
	cleanupWithTimeout := func(baseCtx context.Context) error {
		uncanceled := context.WithoutCancel(baseCtx)
		cleanupCtx, cancel := context.WithTimeout(uncanceled, timeout)
		defer cancel()
		return m.deleteObjectFromTemporaryStorage(cleanupCtx, objectName)
	}

	if common_persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		if err := cleanupWithTimeout(hookCtx); err != nil {
			return fmt.Errorf("failed to delete temporary object (attachmentID=%d, objectName=%s): %w", attachmentID, objectName, err)
		}
		return nil
	}) {
		if appLogger.DebugEnabled(m.logger) {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Registered post-commit temporary object cleanup",
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyTempObjectName, objectName))
		}
		return
	}
	if _, hasTx := scontext.GetTransaction(ctx); hasTx {
		// A transaction without post-commit hook support gives us no safe point
		// to remove the object. Retain it for later maintenance rather than
		// risking a rollback that restores a live row with missing storage.
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Deferred temporary object cleanup unavailable for transaction",
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyTempObjectName, objectName))
		return
	}

	// Fallback path when there is no request transaction/hook support in context.
	if err := cleanupWithTimeout(ctx); err != nil {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Failed to delete temporary object",
			zap.Error(err),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyTempObjectName, objectName))
	}
}

// CleanupStuckAttachments finds and marks attachments stuck in processing state as rejected.
// This prevents attachments from being stuck indefinitely due to processing failures or
func (m *AttachmentLifecycleManager) CleanupStuckAttachments(ctx context.Context, processingTimeout time.Duration) (int, error) {
	cutoffTime := time.Now().UTC().Add(-processingTimeout)

	// Find attachments stuck in processing state
	var stuckAttachments []*common_persistence_models.Attachment

	// Use a transaction for consistency
	tx, err := m.persistence.BeginTransaction(ctx)
	if err != nil {
		return 0, contextualAttachmentError(
			fmt.Errorf("failed to begin transaction for stuck attachment cleanup: %w", err),
			zap.String(logkeys.Stage, "begin_stuck_attachment_cleanup"),
			appLogger.DurationSeconds(appLogger.KeyProcessingTimeoutSeconds, processingTimeout))
	}

	// Query for attachments in processing state older than the timeout
	result := tx.Where("upload_status = ? AND processing_start_time < ? AND internal_only = ?",
		common_persistence_models.UploadStatusProcessing,
		cutoffTime,
		false).
		Find(&stuckAttachments)

	if result.Error != nil {
		tx.Rollback()
		return 0, contextualAttachmentError(
			fmt.Errorf("failed to query stuck attachments: %w", result.Error),
			zap.String(logkeys.Stage, "query_stuck_attachments"),
			appLogger.DurationSeconds(appLogger.KeyProcessingTimeoutSeconds, processingTimeout))
	}

	if len(stuckAttachments) == 0 {
		// Commit the read transaction
		if err := tx.Commit().Error; err != nil {
			return 0, contextualAttachmentError(fmt.Errorf("failed to commit read transaction: %w", err),
				zap.String(logkeys.Stage, "commit_empty_stuck_attachment_scan"))
		}

		if appLogger.DebugEnabled(m.logger) {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("No stuck attachments found",
				zap.Time(appLogger.KeyCutoffTime, cutoffTime))
		}
		return 0, nil
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Found stuck attachments to clean up",
		zap.Int(appLogger.KeyCount, len(stuckAttachments)),
		zap.Time(appLogger.KeyCutoffTime, cutoffTime))

	// Update each stuck attachment to rejected status
	cleanedCount := 0
	now := time.Now().UTC()

	for _, attachment := range stuckAttachments {
		// Update status to rejected with explanation
		attachment.UploadStatus = common_persistence_models.UploadStatusRejected
		attachment.ValidationDetails = fmt.Sprintf("Processing timeout exceeded (%v). Marked as failed by cleanup process.", processingTimeout)
		attachment.ProcessingEndTime = &now

		updateResult := tx.Model(&common_persistence_models.Attachment{}).
			Where("id = ? AND internal_only = ? AND upload_status = ?", attachment.ID, false, common_persistence_models.UploadStatusProcessing).
			Select("upload_status", "validation_details", "processing_end_time").
			Updates(common_persistence_models.Attachment{
				UploadStatus:      common_persistence_models.UploadStatusRejected,
				ValidationDetails: attachment.ValidationDetails,
				ProcessingEndTime: attachment.ProcessingEndTime,
			})
		if updateResult.Error != nil {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Error("Failed to update stuck attachment",
				zap.Error(updateResult.Error),
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
				zap.Time(appLogger.KeyProcessingStartTime, *attachment.ProcessingStartTime))
			// Continue with other attachments despite individual failures
			continue
		}
		if updateResult.RowsAffected != 1 {
			continue
		}

		cleanedCount++

		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Marked stuck attachment as rejected",
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.Uint64(appLogger.KeyUserID, attachment.UserID),
			zap.Time(appLogger.KeyProcessingStartTime, *attachment.ProcessingStartTime),
			appLogger.DurationSeconds(appLogger.KeyStuckDurationSeconds, now.Sub(*attachment.ProcessingStartTime)))
	}

	// Commit the transaction
	if err := tx.Commit().Error; err != nil {
		return cleanedCount, contextualAttachmentError(fmt.Errorf("failed to commit cleanup transaction: %w", err),
			zap.String(logkeys.Stage, "commit_stuck_attachment_cleanup"),
			zap.Int(appLogger.KeyCleanedCount, cleanedCount))
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Successfully cleaned up stuck attachments",
		zap.Int(appLogger.KeyCleanedCount, cleanedCount),
		zap.Int(appLogger.KeyTotalFound, len(stuckAttachments)))

	return cleanedCount, nil
}

// CleanupStuckPendingUploads handles attachments whose client never finalized within
// the pending-upload timeout after their latest upload URL expired
// (upload_url_expires_at), so a row is never swept while one of its URLs is valid,
// whatever upload URL expiry or timeout earlier configurations used. If the temporary
// object exists, cleanup processes it as an uploaded-but-unfinalized attachment. If
// it does not exist, cleanup rejects the abandoned row without treating the expected
// absence as an object-store failure. An attachment left pending_processing, marked
// uploaded but never claimed by processing, is processed again.
func (m *AttachmentLifecycleManager) CleanupStuckPendingUploads(ctx context.Context, pendingUploadTimeout time.Duration) (int, error) {
	cutoffTime := time.Now().UTC().Add(-pendingUploadTimeout)

	// Find attachments stuck before processing
	var stuckPendingUploads []*common_persistence_models.Attachment

	// Use a transaction for consistency
	tx, err := m.persistence.BeginTransaction(ctx)
	if err != nil {
		return 0, contextualAttachmentError(
			fmt.Errorf("failed to begin transaction for stuck pending upload cleanup: %w", err),
			zap.String(logkeys.Stage, "begin_stuck_pending_upload_cleanup"),
			appLogger.DurationSeconds(appLogger.KeyPendingUploadTimeoutSeconds, pendingUploadTimeout))
	}

	// Query for attachments not yet claimed for processing whose latest URL expired over the timeout ago
	result := tx.Where("upload_status IN ? AND upload_url_expires_at < ? AND internal_only = ?",
		[]common_persistence_models.UploadStatus{
			common_persistence_models.UploadStatusPendingUpload,
			common_persistence_models.UploadStatusPendingProcessing,
		},
		cutoffTime,
		false).
		Find(&stuckPendingUploads)

	if result.Error != nil {
		tx.Rollback()
		return 0, contextualAttachmentError(
			fmt.Errorf("failed to query stuck pending uploads: %w", result.Error),
			zap.String(logkeys.Stage, "query_stuck_pending_uploads"),
			appLogger.DurationSeconds(appLogger.KeyPendingUploadTimeoutSeconds, pendingUploadTimeout))
	}

	if len(stuckPendingUploads) == 0 {
		// Commit the read transaction
		if err := tx.Commit().Error; err != nil {
			return 0, contextualAttachmentError(fmt.Errorf("failed to commit read transaction: %w", err),
				zap.String(logkeys.Stage, "commit_empty_stuck_pending_upload_scan"))
		}

		if appLogger.DebugEnabled(m.logger) {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("No stuck pending uploads found",
				zap.Time(appLogger.KeyCutoffTime, cutoffTime))
		}
		return 0, nil
	}

	// Commit the read transaction
	if err := tx.Commit().Error; err != nil {
		return 0, contextualAttachmentError(fmt.Errorf("failed to commit read transaction: %w", err),
			zap.String(logkeys.Stage, "commit_stuck_pending_upload_scan"),
			zap.Int(appLogger.KeyCount, len(stuckPendingUploads)))
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Found stuck pending uploads to process",
		zap.Int(appLogger.KeyCount, len(stuckPendingUploads)),
		zap.Time(appLogger.KeyCutoffTime, cutoffTime))

	// Try to process each stuck pending upload
	processedCount := 0
	rejectedCount := 0

	for _, attachment := range stuckPendingUploads {
		processed, rejected := m.processStuckPendingUpload(ctx, attachment, cutoffTime)
		if processed {
			processedCount++
		}
		if rejected {
			rejectedCount++
		}
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Completed stuck pending upload cleanup",
		zap.Int(appLogger.KeyProcessedCount, processedCount),
		zap.Int(appLogger.KeyRejectedCount, rejectedCount),
		zap.Int(appLogger.KeyTotalFound, len(stuckPendingUploads)))

	return processedCount, nil
}

// processStuckPendingUpload handles one cleanup candidate while holding the same
// in-process lock used by client finalization. Database compare-and-swap updates
// remain authoritative across service instances. A pending_upload candidate first
// goes through claimStuckPendingUpload. A pending_processing candidate was marked
// uploaded, by an earlier sweep or a client finalization, but processing never
// claimed it (the claim failed or the process stopped in between), so it is
// processed again; its upload URLs can no longer be renewed.
func (m *AttachmentLifecycleManager) processStuckPendingUpload(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	cutoffTime time.Time,
) (processed, rejected bool) {
	workerID := "cleanup-" + GenerateWorkerID()
	lockResult := m.tryAcquireAttachmentLock(ctx, attachment.ID, workerID)
	if !lockResult.acquired {
		return false, false
	}
	defer m.createLockReleaser(ctx, attachment.ID, workerID)()

	// The sweep query and this operation are intentionally separate. Re-read the
	// row under the local lock and let its current state decide whether work remains.
	current, err := m.persistence.GetAttachmentByID(ctx, attachment.ID)
	if err != nil {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Error("Failed to re-read stuck pending upload",
			zap.Error(err),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID))
		return false, false
	}
	stuck := current != nil && !current.InternalOnly && current.UploadURLExpiresAt.Before(cutoffTime)
	switch {
	case stuck && current.UploadStatus == common_persistence_models.UploadStatusPendingUpload:
		marked, rejected := m.claimStuckPendingUpload(ctx, current, cutoffTime)
		if !marked {
			return false, rejected
		}
	case stuck && current.UploadStatus == common_persistence_models.UploadStatusPendingProcessing:
	default:
		if appLogger.DebugEnabled(m.logger) {
			fields := []zap.Field{zap.Uint64(appLogger.KeyAttachmentID, attachment.ID)}
			if current != nil {
				fields = append(fields, zap.Stringer(logkeys.Status, current.UploadStatus), zap.Time(appLogger.KeyExpiresAt, current.UploadURLExpiresAt))
			}
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Stuck pending upload already handled or renewed elsewhere, skipping", fields...)
		}
		return false, false
	}

	if err := m.ProcessUploadedAttachment(ctx, current.ID); err != nil {
		if errors.Is(err, ErrAttachmentNotProcessable) {
			if appLogger.DebugEnabled(m.logger) {
				appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Stuck pending upload no longer processable, skipping",
					zap.Uint64(appLogger.KeyAttachmentID, current.ID),
					zap.Error(err))
			}
			return false, false
		}

		// Core processing owns its state transition. Re-read instead of applying a
		// stale pending->rejected update that could overwrite a concurrent result.
		latest, lookupErr := m.persistence.GetAttachmentByID(ctx, current.ID)
		if lookupErr != nil {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Error("Failed to inspect attachment after cleanup processing error",
				zap.Error(lookupErr),
				zap.Uint64(appLogger.KeyAttachmentID, current.ID))
			return false, false
		}
		if latest != nil && latest.UploadStatus == common_persistence_models.UploadStatusRejected {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Uploaded but unfinalized attachment was rejected during processing",
				zap.Uint64(appLogger.KeyAttachmentID, current.ID),
				zap.Error(err))
			return false, true
		}

		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Uploaded but unfinalized attachment processing did not complete",
			zap.Uint64(appLogger.KeyAttachmentID, current.ID),
			zap.Error(err))
		return false, false
	}

	if appLogger.DebugEnabled(m.logger) {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Successfully processed uploaded but unfinalized attachment",
			zap.Uint64(appLogger.KeyAttachmentID, current.ID),
			zap.Uint64(appLogger.KeyUserID, current.UserID))
	}
	return true, false
}

// claimStuckPendingUpload takes a pending_upload candidate out of pending_upload: it rejects
// the row when no object was uploaded, and otherwise marks it pending_processing, as client
// finalization does (MarkAttachmentAsUploaded), for processing to claim. A candidate whose
// upload URLs were reissued (ReissueUploadURLs) to expire after cutoffTime is no longer stuck,
// so both transitions are conditioned on that deadline (UpdateExpiredPendingUpload): a renewal
// committed after the re-read keeps the row pending for the client's fresh URL, and once a
// transition wins no renewal can issue one.
func (m *AttachmentLifecycleManager) claimStuckPendingUpload(
	ctx context.Context,
	current *common_persistence_models.Attachment,
	cutoffTime time.Time,
) (marked, rejected bool) {
	exists, err := m.persistence.ObjectExists(ctx, current.TemporaryObjectName)
	if err != nil {
		// A storage outage or authorization failure says nothing about whether the
		// client uploaded the object. Leave the row pending so a later sweep retries.
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Error("Failed to check stuck pending upload object",
			zap.Error(err),
			zap.Uint64(appLogger.KeyAttachmentID, current.ID),
			zap.String(appLogger.KeyObjectName, current.TemporaryObjectName))
		return false, false
	}
	stuckDuration := time.Since(current.UploadTime)
	now := time.Now().UTC()
	if !exists {
		current.UploadStatus = common_persistence_models.UploadStatusRejected
		current.ValidationDetails = "Upload expired before content was received"
		current.ProcessingEndTime = &now
		updated, updateErr := m.persistence.UpdateExpiredPendingUpload(ctx, current, cutoffTime)
		if updateErr != nil {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Error("Failed to reject abandoned pending upload",
				zap.Error(updateErr),
				zap.Uint64(appLogger.KeyAttachmentID, current.ID))
			return false, false
		}
		if !updated {
			if appLogger.DebugEnabled(m.logger) {
				appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Skipped abandoned-upload rejection after concurrent state transition or renewal",
					zap.Uint64(appLogger.KeyAttachmentID, current.ID))
			}
			return false, false
		}

		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Rejected abandoned pending upload with no stored object",
			zap.Uint64(appLogger.KeyAttachmentID, current.ID),
			zap.Uint64(appLogger.KeyUserID, current.UserID),
			zap.String(appLogger.KeyObjectName, current.TemporaryObjectName),
			zap.Time(appLogger.KeyUploadTime, current.UploadTime),
			appLogger.DurationSeconds(appLogger.KeyStuckDurationSeconds, stuckDuration))
		return false, true
	}

	if appLogger.DebugEnabled(m.logger) {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Attempting to process uploaded but unfinalized attachment",
			zap.Uint64(appLogger.KeyAttachmentID, current.ID),
			zap.Uint64(appLogger.KeyUserID, current.UserID),
			zap.Time(appLogger.KeyUploadTime, current.UploadTime),
			appLogger.DurationSeconds(appLogger.KeyStuckDurationSeconds, stuckDuration))
	}

	current.UploadStatus = common_persistence_models.UploadStatusPendingProcessing
	current.UploadTime = now
	marked, err = m.persistence.UpdateExpiredPendingUpload(ctx, current, cutoffTime)
	if err != nil {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Error("Failed to mark uploaded but unfinalized attachment as uploaded",
			zap.Error(err),
			zap.Uint64(appLogger.KeyAttachmentID, current.ID))
		return false, false
	}
	if !marked {
		if appLogger.DebugEnabled(m.logger) {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Skipped processing uploaded but unfinalized attachment after concurrent state transition or renewal",
				zap.Uint64(appLogger.KeyAttachmentID, current.ID))
		}
		return false, false
	}
	return true, false
}

// runCleanupRoutine runs the background goroutine that periodically cleans up stuck attachments.
// This method runs until the cleanup context is cancelled.
func (m *AttachmentLifecycleManager) runCleanupRoutine(initialInterval time.Duration) {
	defer close(m.cleanupDone)
	logger := m.logger.Named(attachmentLifecycleLoggerName)

	hotpoll.RunWithInitial(m.cleanupCtx, initialInterval, func() time.Duration {
		return m.cleanupConfig(m.runtimeConfigProvider()).CleanupInterval
	}, func() {
		logger.Debug("Running stuck attachment cleanup")
		view := m.runtimeConfigProvider()
		cleanup := m.cleanupConfig(view)

		// Create a timeout context for the cleanup operation
		cleanupCtx, cancel := context.WithTimeout(m.cleanupCtx, 2*time.Minute)

		if cleanup.EnableAutoCleanup {
			cleanedCount, err := m.CleanupStuckAttachments(cleanupCtx, view.AttachmentProcessingTimeout())
			if err != nil {
				appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Error("Failed to cleanup stuck attachments", zap.Error(err))
			} else if cleanedCount > 0 {
				appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Info("Cleaned up stuck attachments", zap.Int(appLogger.KeyCount, cleanedCount))
			}

			processedCount, err := m.CleanupStuckPendingUploads(cleanupCtx, view.AttachmentPendingUploadTimeout())
			if err != nil {
				appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Error("Failed to cleanup stuck pending uploads", zap.Error(err))
			} else if processedCount > 0 {
				appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Info("Processed stuck pending uploads", zap.Int(appLogger.KeyCount, processedCount))
			}
		}

		if view.EnableAttachments() {
			if prunedCount, err := m.PruneTemporaryUploads(cleanupCtx, view.AttachmentTemporaryUploadPruneAge()); err != nil {
				appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Error("Failed to prune temporary uploads", zap.Error(err))
			} else if prunedCount > 0 {
				appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Info("Pruned temporary uploads", zap.Int(appLogger.KeyPrunedCount, prunedCount))
			}
			if releasedCount, err := m.ReleaseSettledTemporaryObjects(cleanupCtx, view.AttachmentTemporaryObjectSweepBatchSize()); err != nil {
				appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Error("Failed to release settled temporary objects", zap.Error(err), zap.Int(appLogger.KeyCount, releasedCount))
			} else if releasedCount > 0 {
				appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Info("Released settled temporary objects", zap.Int(appLogger.KeyCount, releasedCount))
			}
		}

		reclaimedCount, err := m.ReclaimUnreferencedInternalAttachments(cleanupCtx, view.AttachmentInternalReclaimGrace(), view.AttachmentInternalReclaimBatchSize())
		if err != nil {
			appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Error("Failed to reclaim unreferenced internal attachments", zap.Error(err))
		} else if reclaimedCount > 0 {
			appLogger.For(cleanupCtx, m.logger, attachmentLifecycleLoggerName).Info("Reclaimed unreferenced internal attachments", zap.Int(appLogger.KeyCount, reclaimedCount))
		}

		cancel()
	})
	logger.Info("Stuck attachment cleanup routine stopping")
}

type cleanupPolicy struct {
	CleanupInterval   time.Duration
	EnableAutoCleanup bool
}

// cleanupConfig reads the cleanup policy from the captured runtime view.
func (m *AttachmentLifecycleManager) cleanupConfig(view RuntimeConfigView) cleanupPolicy {
	cfg := cleanupPolicy{}
	cfg.CleanupInterval = view.AttachmentCleanupInterval()
	cfg.EnableAutoCleanup = view.AttachmentEnableAutoCleanup()
	if !view.EnableAttachments() {
		cfg.EnableAutoCleanup = false
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = time.Hour
	}
	return cfg
}

// ReclaimUnreferencedInternalAttachments soft-deletes old unreferenced internal rows under the
// same row lock transcript insertion uses. Objects are kept: see TODO(object-refcounting) in
// processUploadedAttachmentCore.
func (m *AttachmentLifecycleManager) ReclaimUnreferencedInternalAttachments(ctx context.Context, grace time.Duration, limit int) (int, error) {
	if m == nil || m.persistence == nil || grace <= 0 {
		return 0, nil
	}
	candidates, err := m.persistence.ListInternalAttachmentReclaimCandidates(ctx, time.Now().UTC().Add(-grace), limit)
	if err != nil {
		return 0, err
	}
	reclaimed := 0
	var errs []error
	for _, candidate := range candidates {
		if candidate == nil || candidate.ID == 0 || !candidate.InternalOnly {
			continue
		}
		tombstoned, err := m.persistence.TombstoneInternalAttachmentIfUnreferenced(ctx, candidate.ID, time.Now().UTC().Add(-grace))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if tombstoned {
			reclaimed++
		}
	}
	return reclaimed, errors.Join(errs...)
}

// SetupBucketLifecycle configures bucket lifecycle rules to automatically clean up temporary uploads.
// This method attempts to configure MinIO bucket lifecycle policies to automatically delete
func (m *AttachmentLifecycleManager) SetupBucketLifecycle(ctx context.Context) error {
	// Note: MinIO lifecycle management requires MinIO admin API access and proper bucket policies.
	// This implementation provides a best-effort attempt to configure lifecycle rules.
	// If it fails, the application can still rely on the manual PruneTemporaryUploads method.

	// Log the lifecycle setup attempt
	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Setting up bucket lifecycle management for temporary uploads")

	// Try to configure lifecycle rules using MinIO client extensions
	// This is a best-effort attempt - if it fails, manual cleanup via PruneTemporaryUploads is still available
	err := m.attemptMinIOLifecycleSetup(ctx)
	if err != nil {
		// Log the error but don't fail the entire setup
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Failed to configure MinIO bucket lifecycle rules, relying on manual cleanup",
			zap.Error(err))

		// Return success since manual cleanup is still available
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Bucket lifecycle management setup completed - using manual cleanup via PruneTemporaryUploads")
		return nil
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Bucket lifecycle management configured successfully - automatic cleanup enabled for temporary uploads")
	return nil
}

// attemptMinIOLifecycleSetup attempts to configure MinIO bucket lifecycle policies.
func (m *AttachmentLifecycleManager) attemptMinIOLifecycleSetup(_ context.Context) error {
	// NOTE: MinIO lifecycle configuration typically requires admin API access and specific bucket policies.
	// The standard MinIO Go SDK client doesn't directly support lifecycle configuration in all deployments.
	// This implementation attempts basic lifecycle setup but may not work in all MinIO configurations.

	// For now, we'll return an error indicating that lifecycle setup should be done manually
	// via MinIO admin console or mc (MinIO Client) commands.
	return fmt.Errorf("automatic MinIO lifecycle configuration not supported - configure manually via MinIO admin console or use mc client commands to set lifecycle policies for temporary uploads")
}

// MarkAttachmentAsUploaded transitions an uploaded attachment from `pending_upload` to `pending_processing` in the normal upload completion flow.
//
// Parameters:
//   - ctx: Context for persistence operations and logging.
//   - attachmentID: Attachment ID to transition after upload completes.
//
// Returns:
//   - error: Lookup, state-validation, or persistence error.
func (m *AttachmentLifecycleManager) MarkAttachmentAsUploaded(
	ctx context.Context,
	attachmentID uint64,
) error {
	// Get the attachment record
	attachment, err := m.persistence.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return contextualAttachmentError(
			fmt.Errorf("failed to get attachment for marking as uploaded (attachmentID=%d): %w", attachmentID, err),
			zap.String(logkeys.Stage, "load_attachment_for_mark_uploaded"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}
	if attachment == nil || attachment.InternalOnly {
		return ErrInternalAttachmentAccess
	}

	// Validate the current status
	if attachment.UploadStatus != common_persistence_models.UploadStatusPendingUpload {
		err := fmt.Errorf("attachment not in pending_upload status: status=%s", attachment.UploadStatus)
		return appLogger.WithErrorLevel(err, zapcore.WarnLevel,
			zap.String(logkeys.Stage, "mark_attachment_uploaded"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.Stringer(logkeys.Expected, common_persistence_models.UploadStatusPendingUpload),
			zap.Stringer(logkeys.Actual, attachment.UploadStatus))
	}

	// Compare-and-swap the status to pending_processing. The attachment can be
	// claimed by timeout pruning after the read above, in which case this stale
	// request must not resurrect it.
	attachment.UploadStatus = common_persistence_models.UploadStatusPendingProcessing
	attachment.UploadTime = time.Now().UTC()
	updated, err := m.persistence.UpdateAttachmentIfStatus(ctx, attachment, common_persistence_models.UploadStatusPendingUpload)
	if err != nil {
		return contextualAttachmentError(
			fmt.Errorf("failed to update attachment status to pending_processing (attachmentID=%d): %w", attachmentID, err),
			zap.String(logkeys.Stage, "mark_attachment_uploaded"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.Stringer(appLogger.KeyNewStatus, common_persistence_models.UploadStatusPendingProcessing))
	}
	if !updated {
		return fmt.Errorf("attachment state changed before it could be marked uploaded (attachmentID=%d)", attachmentID)
	}

	appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Marked attachment as uploaded",
		zap.Uint64(appLogger.KeyAttachmentID, attachmentID))

	return nil
}

// batchFetchAttachments fetches multiple attachments in a single database query to avoid N+1 problems.
func (m *AttachmentLifecycleManager) batchFetchAttachments(ctx context.Context, attachmentIDs []uint64) ([]*common_persistence_models.Attachment, error) {
	if len(attachmentIDs) == 0 {
		return []*common_persistence_models.Attachment{}, nil
	}

	// Use a transaction for consistent reads
	tx, err := m.persistence.BeginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction for batch fetch (attachmentCount=%d): %w", len(attachmentIDs), err)
	}
	defer tx.Rollback() // Safe to call even after commit

	var attachments []*common_persistence_models.Attachment
	result := tx.Where("id IN ? AND internal_only = ?", attachmentIDs, false).Find(&attachments)
	if result.Error != nil {
		return nil, fmt.Errorf("failed to fetch attachments in batch (requested=%d): %w", len(attachmentIDs), result.Error)
	}

	// Commit the read transaction
	if err := tx.Commit().Error; err != nil {
		return nil, fmt.Errorf("failed to commit read transaction for batch fetch (attachmentCount=%d): %w", len(attachments), err)
	}

	if appLogger.DebugEnabled(m.logger) {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Batch fetched attachments for finalization",
			zap.Int(appLogger.KeyRequested, len(attachmentIDs)),
			zap.Int(appLogger.KeyFound, len(attachments)))
	}

	return attachments, nil
}

// attachmentLockResult represents the result of attempting to acquire a lock on an attachment
type attachmentLockResult struct {
	acquired    bool
	lockOwner   string
	expiresAt   time.Time
	errorResult *proto_models.AttachmentResult
}

// tryAcquireAttachmentLock attempts to acquire the per-attachment processing lock used by finalization and batch processing flows.
//
// Parameters:
//   - ctx: Context for logging.
//   - attachmentID: Attachment ID to lock.
//   - workerID: Worker identifier attempting to acquire the lock.
//
// Returns:
//   - attachmentLockResult: Lock acquisition outcome, including any prebuilt error result.
func (m *AttachmentLifecycleManager) tryAcquireAttachmentLock(
	ctx context.Context,
	attachmentID uint64,
	workerID string,
) attachmentLockResult {
	if m.lockManager.TryLock(attachmentID, workerID) {
		return attachmentLockResult{acquired: true}
	}

	// Lock acquisition failed - get lock owner info
	lockOwner, expiresAt := m.lockManager.GetLockOwner(attachmentID)
	errMsg := fmt.Sprintf("Attachment is being processed by another worker (%s), expires at %v", lockOwner, expiresAt.Format(time.RFC3339))

	result := &proto_models.AttachmentResult{
		AttachmentId: attachmentID,
		Success:      false,
		Status:       proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_INVALID_STATE,
		ErrorMessage: &errMsg,
	}

	if appLogger.DebugEnabled(m.logger) {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Attachment processing skipped due to lock conflict",
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyWorkerID, workerID),
			zap.String(appLogger.KeyLockOwner, lockOwner),
			zap.Time(appLogger.KeyLockExpires, expiresAt))
	}

	return attachmentLockResult{
		acquired:    false,
		lockOwner:   lockOwner,
		expiresAt:   expiresAt,
		errorResult: result,
	}
}

// createLockReleaser builds a deferred unlock closure for a processed attachment to keep release logic consistent across branches.
//
// Parameters:
//   - ctx: Context for unlock logging.
//   - attachmentID: Attachment whose lock should be released.
//   - workerID: Worker ID expected to own the lock.
//
// Returns:
//   - func(): Closure that releases the lock at most once.
func (m *AttachmentLifecycleManager) createLockReleaser(
	ctx context.Context,
	attachmentID uint64,
	workerID string,
) func() {
	needsUnlock := true
	return func() {
		if needsUnlock {
			released := m.lockManager.Unlock(attachmentID, workerID)
			if released && appLogger.DebugEnabled(m.logger) {
				appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Lock released for processed attachment",
					zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
					zap.String(appLogger.KeyWorkerID, workerID))
			}
			needsUnlock = false
		}
	}
}

// createErrorResult builds a standardized failed attachment-processing result for finalization and batch flows.
//
// Parameters:
//   - attachmentID: Attachment ID associated with the failure.
//   - status: Processing status to report.
//   - errorMessage: Human-readable failure message.
//
// Returns:
//   - *proto_models.AttachmentResult: Failed processing result payload.
func createErrorResult(attachmentID uint64, status proto_models.AttachmentProcessingStatus, errorMessage string) *proto_models.AttachmentResult {
	return &proto_models.AttachmentResult{
		AttachmentId: attachmentID,
		Success:      false,
		Status:       status,
		ErrorMessage: &errorMessage,
	}
}

// createSuccessResult builds a standardized successful attachment-processing result for finalization and batch flows.
//
// Parameters:
//   - attachmentID: Attachment ID associated with the success.
//   - status: Processing status to report.
//
// Returns:
//   - *proto_models.AttachmentResult: Successful processing result payload.
func createSuccessResult(attachmentID uint64, status proto_models.AttachmentProcessingStatus) *proto_models.AttachmentResult {
	return &proto_models.AttachmentResult{
		AttachmentId: attachmentID,
		Success:      true,
		Status:       status,
	}
}

// attachmentStateValidationResult represents the result of validating an attachment's state
type attachmentStateValidationResult struct {
	shouldProcess     bool
	attachment        *common_persistence_models.Attachment
	result            *proto_models.AttachmentResult
	shouldAddToEntity bool
}

// validateAttachmentState evaluates whether an attachment can be finalized and performs required state transitions before work begins.
//
// Parameters:
//   - ctx: Context for logging and persistence calls.
//   - attachmentID: Attachment ID being evaluated.
//   - attachment: Attachment record loaded for processing.
//   - workerID: Worker ID handling the attachment.
//
// Returns:
//   - attachmentStateValidationResult: State decision describing whether processing should continue.
func (m *AttachmentLifecycleManager) validateAttachmentState(
	ctx context.Context,
	attachmentID uint64,
	attachment *common_persistence_models.Attachment,
	workerID string,
) attachmentStateValidationResult {
	switch attachment.UploadStatus {
	case common_persistence_models.UploadStatusPendingUpload:
		// The client called finalize after successful upload, so transition to pending_processing.
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Info("Finalize called for pending_upload attachment, marking as uploaded",
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyWorkerID, workerID))

		// Mark the attachment as uploaded to transition to pending_processing
		err := m.MarkAttachmentAsUploaded(ctx, attachmentID)
		if err != nil {
			return attachmentStateValidationResult{
				shouldProcess: false,
				result: createErrorResult(
					attachmentID,
					proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_ERROR,
					fmt.Sprintf("Failed to mark attachment as uploaded: %v", err),
				),
			}
		}

		// Re-fetch the attachment to get updated status (this is now safe due to lock)
		updatedAttachment, err := m.persistence.GetAttachmentByID(ctx, attachmentID)
		if err != nil {
			return attachmentStateValidationResult{
				shouldProcess: false,
				result: createErrorResult(
					attachmentID,
					proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_ERROR,
					fmt.Sprintf("Failed to re-fetch attachment after marking as uploaded: %v", err),
				),
			}
		}

		// Continue processing with the updated attachment
		return attachmentStateValidationResult{
			shouldProcess: true,
			attachment:    updatedAttachment,
		}

	case common_persistence_models.UploadStatusPendingProcessing:
		// This is the correct state for finalization - continue.
		return attachmentStateValidationResult{
			shouldProcess: true,
			attachment:    attachment,
		}

	case common_persistence_models.UploadStatusVetted:
		if appLogger.DebugEnabled(m.logger) {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Attachment already vetted, skipping processing",
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.Uint64(appLogger.KeyUserID, attachment.UserID))
		}

		return attachmentStateValidationResult{
			shouldProcess: false,
			result: createSuccessResult(
				attachmentID,
				proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_ALREADY_PROCESSED,
			),
			shouldAddToEntity: attachment.EntityID != "",
			attachment:        attachment,
		}

	case common_persistence_models.UploadStatusProcessing:
		return attachmentStateValidationResult{
			shouldProcess: false,
			result: createErrorResult(
				attachmentID,
				proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_INVALID_STATE,
				"Attachment is currently being processed by another worker",
			),
		}

	case common_persistence_models.UploadStatusRejected:
		return attachmentStateValidationResult{
			shouldProcess: false,
			result: createErrorResult(
				attachmentID,
				proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_INVALID_STATE,
				"Attachment was previously rejected and cannot be reprocessed",
			),
		}

	default:
		return attachmentStateValidationResult{
			shouldProcess: false,
			result: createErrorResult(
				attachmentID,
				proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_INVALID_STATE,
				fmt.Sprintf("Attachment in unknown state: %s", attachment.UploadStatus),
			),
		}
	}
}

// singleAttachmentProcessingResult represents the result of processing a single attachment
type singleAttachmentProcessingResult struct {
	result              *proto_models.AttachmentResult
	processedAttachment *common_persistence_models.Attachment
	securityViolation   bool
}

// processAttachmentWithLock orchestrates lock acquisition, state validation, core processing, and result construction for one attachment in finalization batches.
//
// Parameters:
//   - ctx: Context for logging and downstream processing.
//   - attachmentID: Attachment ID to process.
//   - attachment: Attachment record loaded for the batch.
//   - workerID: Worker identifier used for lock ownership.
//   - validators: Validators applied during core processing.
//
// Returns:
//   - singleAttachmentProcessingResult: Per-attachment processing outcome, including callback batching metadata.
func (m *AttachmentLifecycleManager) processAttachmentWithLock(
	ctx context.Context,
	attachmentID uint64,
	attachment *common_persistence_models.Attachment,
	workerID string,
	validators []StreamingAttachmentValidator,
) singleAttachmentProcessingResult {
	// Try to acquire lock for this attachment to prevent race conditions
	lockResult := m.tryAcquireAttachmentLock(ctx, attachmentID, workerID)
	if !lockResult.acquired {
		return singleAttachmentProcessingResult{
			result: lockResult.errorResult,
		}
	}

	// Create lock releaser function
	releaseLock := m.createLockReleaser(ctx, attachmentID, workerID)

	// Validate attachment state and handle state transitions
	stateValidation := m.validateAttachmentState(ctx, attachmentID, attachment, workerID)
	if !stateValidation.shouldProcess {
		var processedAttachment *common_persistence_models.Attachment
		if stateValidation.shouldAddToEntity {
			processedAttachment = stateValidation.attachment
		}
		releaseLock() // Release lock immediately like original code
		return singleAttachmentProcessingResult{
			result:              stateValidation.result,
			processedAttachment: processedAttachment,
		}
	}

	// Process the attachment WITHOUT calling entity callbacks (we'll batch them later)
	processedAttachment, err := m.processUploadedAttachmentWithoutEntityCallback(ctx, attachmentID, validators...)
	if err != nil {
		var status proto_models.AttachmentProcessingStatus
		if errors.Is(err, ErrValidationFailed) {
			status = proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_VALIDATION_FAILED
		} else {
			status = proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_ERROR
		}

		result := createErrorResult(attachmentID, status, err.Error())

		// Check if this is a security violation
		if errors.Is(err, apperrors.ErrAttachmentSecurityViolation) {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("Security violation detected during attachment finalization",
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.Error(err))
			releaseLock() // Release lock immediately like original code
			return singleAttachmentProcessingResult{
				result:            result,
				securityViolation: true,
			}
		}

		releaseLock() // Release lock immediately like original code
		return singleAttachmentProcessingResult{
			result: result,
		}
	}

	// Success case
	var attachmentForEntity *common_persistence_models.Attachment
	if processedAttachment != nil && processedAttachment.EntityID != "" {
		attachmentForEntity = processedAttachment
	}

	releaseLock() // Release lock immediately like original code
	return singleAttachmentProcessingResult{
		result: createSuccessResult(
			attachmentID,
			proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_SUCCESS,
		),
		processedAttachment: attachmentForEntity,
	}
}

// FinalizeAttachments processes multiple uploaded attachments from the client finalization path using shared locking, validation, and callback batching.
//
// Parameters:
//   - ctx: Context for logging and downstream processing.
//   - attachmentIDs: Attachment IDs referenced by the finalization request.
//   - validators: Optional streaming validators to apply while processing each attachment.
//
// Returns:
//   - []*proto_models.AttachmentResult: Result for each requested attachment in request order.
//   - error: Batch-level error such as fetch failure or detected security violation.
func (m *AttachmentLifecycleManager) FinalizeAttachments(
	ctx context.Context,
	attachmentIDs []uint64,
	validators ...StreamingAttachmentValidator,
) ([]*proto_models.AttachmentResult, error) {
	results := make([]*proto_models.AttachmentResult, 0, len(attachmentIDs))

	// Batch fetch all attachments in a single query to avoid N+1 problem
	attachments, err := m.batchFetchAttachments(ctx, attachmentIDs)
	if err != nil {
		return nil, contextualAttachmentError(
			fmt.Errorf("failed to fetch attachments for finalization (requestedCount=%d): %w", len(attachmentIDs), err),
			zap.String(logkeys.Stage, "batch_fetch_attachments_for_finalization"),
			zap.Uint64s(appLogger.KeyAttachmentIDs, attachmentIDs),
			zap.Int(appLogger.KeyCount, len(attachmentIDs)))
	}

	// Create a map for quick lookup of fetched attachments
	attachmentMap := make(map[uint64]*common_persistence_models.Attachment)
	for _, attachment := range attachments {
		attachmentMap[attachment.ID] = attachment
	}

	// Track successfully processed attachments for entity batching
	var processedAttachments []*common_persistence_models.Attachment

	// Process each requested attachment
	for _, attachmentID := range attachmentIDs {
		// Check if attachment was found in the batch fetch
		attachment, found := attachmentMap[attachmentID]
		if !found {
			result := createErrorResult(
				attachmentID,
				proto_models.AttachmentProcessingStatus_ATTACHMENT_PROCESSING_STATUS_NOT_FOUND,
				"Attachment not found",
			)
			results = append(results, result)
			continue
		}

		// Generate unique worker ID for this processing attempt
		workerID := GenerateWorkerID()

		// Process the attachment with lock management
		processingResult := m.processAttachmentWithLock(ctx, attachmentID, attachment, workerID, validators)

		// Add result to the results list
		results = append(results, processingResult.result)

		// Handle security violations - return immediately
		if processingResult.securityViolation {
			return results, apperrors.ErrAttachmentSecurityViolation
		}

		// Add processed attachment to entity batching list if available
		if processingResult.processedAttachment != nil {
			processedAttachments = append(processedAttachments, processingResult.processedAttachment)
		}
	}

	// Now handle entity callbacks in batches
	err = m.processEntityCallbacksBatch(ctx, processedAttachments)
	if err != nil {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Error("Failed to process entity callbacks in batch",
			zap.Error(err),
			zap.Int(appLogger.KeyAttachmentCount, len(processedAttachments)))
		// Don't fail the entire finalization response, just log the error.
	}

	return results, nil
}

// processUploadedAttachmentWithoutEntityCallback processes an attachment without calling entity callbacks.
func (m *AttachmentLifecycleManager) processUploadedAttachmentWithoutEntityCallback(
	ctx context.Context,
	attachmentID uint64,
	validators ...StreamingAttachmentValidator,
) (*common_persistence_models.Attachment, error) {
	// Use the core processing logic and return the processed attachment
	return m.processUploadedAttachmentCore(ctx, attachmentID, validators...)
}

// processEntityCallbacksBatch groups attachments by entity and calls callbacks in batches.
func (m *AttachmentLifecycleManager) processEntityCallbacksBatch(
	ctx context.Context,
	attachments []*common_persistence_models.Attachment,
) error {
	if len(attachments) == 0 {
		return nil
	}

	// Group attachments by entity (objectType + entityID)
	entityGroups := make(map[string][]*common_persistence_models.Attachment)

	for _, attachment := range attachments {
		if attachment.EntityID == "" {
			continue // Skip attachments without entity IDs
		}

		// Create entity key combining object type and entity ID
		entityKey := fmt.Sprintf("%s:%s", attachment.ObjectType, attachment.EntityID)
		entityGroups[entityKey] = append(entityGroups[entityKey], attachment)
	}

	if appLogger.DebugEnabled(m.logger) {
		appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Grouped attachments by entity for batch processing",
			zap.Int(appLogger.KeyAttachmentCount, len(attachments)),
			zap.Int(appLogger.KeyEntityGroups, len(entityGroups)))
	}

	// Process each entity group
	for _, entityAttachments := range entityGroups {
		if len(entityAttachments) == 0 {
			continue
		}

		// All attachments in a group have the same object type and entity ID
		objectType := entityAttachments[0].ObjectType
		entityID := entityAttachments[0].EntityID

		// Get object type configuration
		objectTypeConfig := m.objectTypeRegistry.Get(objectType)
		if objectTypeConfig == nil {
			appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Warn("No object type configuration found for entity callback",
				zap.String(logkeys.Stage, "entity_batch_callback"),
				zap.String(logkeys.Expected, "registered object type configuration"),
				zap.String(logkeys.Actual, "missing"),
				zap.String(logkeys.Fallback, "skip entity callback"),
				zap.String(appLogger.KeyObjectType, objectType),
				zap.String(appLogger.KeyEntityID, entityID),
				zap.Int(appLogger.KeyAttachmentCount, len(entityAttachments)))
			continue
		}

		// Invoke entity batch callback if configured
		if objectTypeConfig.EntityBatchCallback != nil {
			if appLogger.DebugEnabled(m.logger) {
				appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Invoking entity batch callback",
					zap.String(appLogger.KeyObjectType, objectType),
					zap.String(appLogger.KeyEntityID, entityID),
					zap.Int(appLogger.KeyAttachmentCount, len(entityAttachments)))
			}

			if err := objectTypeConfig.EntityBatchCallback(ctx, entityAttachments, entityID); err != nil {
				m.logEntityCallbackFailure(ctx, err,
					zap.String(appLogger.KeyObjectType, objectType),
					zap.String(appLogger.KeyEntityID, entityID),
					zap.Int(appLogger.KeyAttachmentCount, len(entityAttachments)))
			} else if appLogger.DebugEnabled(m.logger) {
				appLogger.For(ctx, m.logger, attachmentLifecycleLoggerName).Debug("Entity batch callback completed successfully",
					zap.String(appLogger.KeyObjectType, objectType),
					zap.String(appLogger.KeyEntityID, entityID),
					zap.Int(appLogger.KeyAttachmentCount, len(entityAttachments)))
			}
		}
	}

	return nil
}

// Close shuts down the attachment lifecycle manager and cleans up resources.
// This should be called during application shutdown to properly release all locks
func (m *AttachmentLifecycleManager) Close() error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		logger := m.logger.Named(attachmentLifecycleLoggerName)
		logger.Info("Shutting down attachment lifecycle manager")

		if m.cleanupCancel != nil {
			logger.Info("Stopping stuck attachment cleanup routine")
			m.cleanupCancel()
			select {
			case <-m.cleanupDone:
				logger.Info("Stuck attachment cleanup routine stopped")
			case <-time.After(5 * time.Second):
				logger.Warn("Timeout waiting for stuck attachment cleanup routine to stop")
			}
		}

		if m.lockManager != nil {
			m.closeErr = m.lockManager.Close()
		}
	})
	return m.closeErr
}
