package common_service

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	attachmentpkg "github.com/Suhaibinator/go-common/pkg/common_service/attachment"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Attachment Operations
// These methods handle file attachments, uploads, downloads, and background processing

// StartAttachmentWorkers executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) StartAttachmentWorkers() {
	if h.attachmentWorkerCtx != nil {
		return
	}
	logger := appLogger.For(context.Background(), h.logger, loggerName)
	// Create a cancellable context for the workers
	h.attachmentWorkerCtx, h.attachmentWorkerCancel = context.WithCancel(context.Background())

	// Start the attachment processing worker
	h.attachmentWorkerWg.Add(1)
	go h.runAttachmentProcessingWorker()

	logger.Info("Attachment workers started")
}

// runAttachmentProcessingWorker executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) runAttachmentProcessingWorker() {
	defer h.attachmentWorkerWg.Done()
	logger := appLogger.For(context.Background(), h.logger, loggerName)

	// Process attachments every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	logger.Info("Attachment processing worker started")

	for {
		select {
		case <-h.attachmentWorkerCtx.Done():
			logger.Info("Attachment processing worker stopped")
			return
		case <-ticker.C:
			if !h.attachmentWorkEnabled() {
				continue
			}
			// Process a batch of pending attachments
			ctx, cancel := context.WithTimeout(h.attachmentWorkerCtx, 25*time.Second)
			h.processPendingAttachments(ctx)
			cancel()
		}
	}
}

// processPendingAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) processPendingAttachments(ctx context.Context) {
	operationLogger := appLogger.For(ctx, h.logger, loggerName)
	operationLogger.Debug("Processing pending attachments")

	// Maximum number of attachments to process in one batch
	maxBatchSize := 10

	// Query for a batch of pending attachments
	pendingAttachments, err := h.commonPersistenceHandler.GetAttachmentsByStatus(
		ctx,
		common_persistence_models.UploadStatusPendingProcessing,
		maxBatchSize,
	)

	if err != nil {
		operationLogger.Error("Failed to query for pending attachments", zap.Error(err))
		return
	}

	if len(pendingAttachments) == 0 {
		// No pending attachments to process
		return
	}

	operationLogger.Info("Found pending attachments to process", zap.Int(appLogger.KeyCount, len(pendingAttachments)))

	// Process each attachment
	processedCount := 0
	for _, attachment := range pendingAttachments {
		// Check if we should stop
		select {
		case <-ctx.Done():
			operationLogger.Info("Pending attachment processing interrupted",
				zap.Int(appLogger.KeyProcessed, processedCount),
				zap.Int(appLogger.KeyTotal, len(pendingAttachments)))
			return
		default:
			// Continue processing
		}

		// The lifecycle core owns mandatory MIME verification on every path.
		results, err := h.attachmentLifecycleManager.FinalizeAttachments(ctx, []uint64{attachment.ID})
		if err != nil {
			operationLogger.Error("Failed to process attachment",
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
				zap.Error(err))
			continue
		}

		// Check if the specific attachment was processed successfully
		if len(results) == 0 || !results[0].Success {
			errorMsg := "Unknown error"
			if len(results) > 0 && results[0].ErrorMessage != nil {
				errorMsg = *results[0].ErrorMessage
			}
			operationLogger.Error("Attachment processing failed",
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
				zap.String(appLogger.KeyErrorMessage, errorMsg))
			continue
		}

		processedCount++

		// If the metricsHandler is available, record metrics for this operation
		if h.metricsHandler != nil {
			h.metricsHandler.RecordMinioOperation("process_attachment", 0, 0, "upload", true)
		}
	}

	operationLogger.Info("Processed pending attachments", zap.Int(appLogger.KeyProcessedCount, processedCount))
}

// FinalizeAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) FinalizeAttachments(
	ctx context.Context,
	attachmentIDs []uint64,
) ([]*proto_models.AttachmentResult, error) {

	actor, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found {
		return nil, fmt.Errorf("user not found in context")
	}
	actorId := (*actor).GetID()

	log := appLogger.For(ctx, h.logger, loggerName)
	if ce := log.Check(zap.DebugLevel, "Finalizing uploaded attachments"); ce != nil {
		ce.Write(
			zap.Uint64(appLogger.KeyUserID, actorId),
			zap.Any(appLogger.KeyAttachmentIDs, attachmentIDs),
		)
	}

	if len(attachmentIDs) == 0 {
		return nil, fmt.Errorf("no attachment IDs provided")
	}

	// Validate user owns all attachments before processing (bulk validation)
	var attachments []*common_persistence_models.Attachment
	var err error
	if h.permissionHandler.HasSitePermission(ctx, (*actor).GetUserType(), permission.PermissionAttachmentModification) {
		attachments, err = h.commonPersistenceHandler.GetAttachmentsByIDs(ctx, attachmentIDs)
	} else {
		attachments, err = h.commonPersistenceHandler.GetAttachmentsByIDsAndUser(ctx, attachmentIDs, actorId)
	}

	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("failed to validate attachment ownership: %w", err),
			zap.String(logkeys.Stage, "validate_attachment_ownership"),
			zap.Uint64(appLogger.KeyUserID, actorId),
			zap.Uint64s(appLogger.KeyAttachmentIDs, attachmentIDs))
	}

	// Check that all requested attachments were found and owned by the user
	if len(attachments) != len(attachmentIDs) {
		log.Warn("User attempted to process attachments they don't own",
			zap.Uint64(appLogger.KeyUserID, actorId),
			zap.Uint64s(appLogger.KeyAttachmentIDs, attachmentIDs),
			zap.Int(appLogger.KeyFoundCount, len(attachments)))
		return nil, apperrors.ErrUnauthorizedAttachmentAccess
	}

	// The shared lifecycle core applies mandatory and registered MIME policy.
	results, err := h.attachmentLifecycleManager.FinalizeAttachments(ctx, attachmentIDs)
	if err != nil {
		return nil, err
	}

	// Check if all attachments were processed successfully
	allSuccess := true
	for _, result := range results {
		if !result.Success {
			allSuccess = false
			break
		}
	}

	if ce := log.Check(zap.DebugLevel, "Attachment processing results"); ce != nil {
		ce.Write(
			zap.Bool(appLogger.KeyAllSuccess, allSuccess),
			zap.Int(appLogger.KeyResultsCount, len(results)),
			zap.Any(appLogger.KeyResults, results),
		)
	}

	return results, nil
}

// ValidateAttachmentOwnership executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) ValidateAttachmentOwnership(
	ctx context.Context,
	userID uint64,
	attachmentID uint64,
) (bool, error) {
	attachment, err := h.commonPersistenceHandler.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return false, appLogger.WithErrorFields(err,
			zap.String(logkeys.Stage, "validate_attachment_ownership"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}
	if attachment == nil || attachment.InternalOnly {
		return false, nil
	}

	// Check if the attachment belongs to the user
	isOwner := attachment.UserID == userID

	if !isOwner {
		appLogger.For(ctx, h.logger, loggerName).Warn("Ownership validation failed",
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint64(appLogger.KeyAttachmentOwnerID, attachment.UserID),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}

	return isOwner, nil
}

// RegisterObjectType executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) RegisterObjectType(config *attachmentpkg.ObjectTypeConfig) error {
	if err := h.validateObjectTypeVisibility(config); err != nil {
		return err
	}
	return h.objectTypeRegistry.Register(config)
}

// validateObjectTypeVisibility rejects an object type that requests public visibility while the deployment has no public bucket configured.
// The registry has no persistence reference, so this service-layer check is the single gate every registration entry point passes through.
func (h *CommonServiceHandler[U]) validateObjectTypeVisibility(config *attachmentpkg.ObjectTypeConfig) error {
	publicStorageEnabled := h.commonPersistenceHandler != nil && h.commonPersistenceHandler.PublicStorageEnabled()
	return attachmentpkg.ValidateObjectTypeVisibility(config, publicStorageEnabled)
}

// GetObjectTypeRegistry executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetObjectTypeRegistry() *attachmentpkg.ObjectTypeRegistry {
	return h.objectTypeRegistry
}

// Entity Linking Methods

// CreateProfilePictureUploadURL executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) CreateProfilePictureUploadURL(
	ctx context.Context,
	userID uint64,
	filename string,
	contentType string,
) (*AttachmentResponse, error) {
	// Entity type is implied by object type "profile-picture"
	entityID := strconv.FormatUint(userID, 10)

	// Use a default filename if none provided
	if filename == "" {
		filename = "profile.jpg"
	}

	// Create upload URL with entity pre-populated
	issued, err := h.attachmentLifecycleManager.CreateUploadURLWithEntityResult(
		ctx,
		userID,
		filename,
		contentType,       // Use provided content type, will be validated during processing
		"profile-picture", // Object type implies entity type "user"
		entityID,
	)

	if err != nil {
		return nil, err
	}
	attachment := issued.Attachment

	// Update the user profile picture to reference the new attachment
	// Note: The attachment is in "pending_upload" status at this point.
	// The file will be validated when uploaded and processed.
	err = h.commonPersistenceHandler.UpdateUserProfilePicture(ctx, userID, &attachment.ID)
	if err != nil {
		appLogger.For(ctx, h.logger, loggerName).Warn("Failed to update user profile picture reference",
			zap.String(logkeys.Stage, "update_pending_profile_picture_reference"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(logkeys.Fallback, "use_entity_callback_after_attachment_vetting"),
			zap.Error(err))
		// Don't fail the whole operation if profile picture update fails
		// The user can still upload the file and it will be set via EntityBatchCallback
	}

	return &AttachmentResponse{
		ID:       attachment.ID,
		URL:      issued.UploadURL,
		FormData: issued.FormData,
		Filename: attachment.OriginalFileName,
		Expiry:   issued.ExpiresAt,
	}, nil
}

// RemoveUserProfilePicture executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) RemoveUserProfilePicture(ctx context.Context, userID uint64) error {
	// Call persistence layer with nil to remove the profile picture
	err := h.commonPersistenceHandler.UpdateUserProfilePicture(ctx, userID, nil)
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Stage, "remove_user_profile_picture"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	appLogger.For(ctx, h.logger, loggerName).Info("Successfully removed user profile picture",
		zap.Uint64(appLogger.KeyUserID, userID))

	return nil
}

// CreateUploadURLWithEntity executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// SECURITY: entityID must be selected by the consuming service after it has authorized
// the actor to modify attachments for that entity. This helper does not perform
// entity-level authorization.
func (h *CommonServiceHandler[U]) CreateUploadURLWithEntity(
	ctx context.Context,
	userID uint64,
	objectType string,
	fileName string,
	contentType string,
	entityID string,
) (*common_persistence_models.Attachment, string, map[string]string, error) {
	// Use the singleton lifecycle manager's entity-aware method
	return h.attachmentLifecycleManager.CreateUploadURLWithEntity(
		ctx, userID, fileName, contentType, objectType, entityID)
}

// Attachment Lifecycle Management Methods
// These methods provide access to common attachment lifecycle operations without exposing the lifecycle manager directly

// CreateUploadURLWithConfig executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) CreateUploadURLWithConfig(
	ctx context.Context,
	userID uint64,
	originalFileName string,
	contentType string,
	config attachmentpkg.UploadURLConfig,
) (*common_persistence_models.Attachment, string, map[string]string, error) {
	return h.attachmentLifecycleManager.CreateUploadURLWithConfig(ctx, userID, originalFileName, contentType, config)
}

// ProcessUploadedAttachment executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) ProcessUploadedAttachment(
	ctx context.Context,
	attachmentID uint64,
	validators ...attachmentpkg.StreamingAttachmentValidator,
) error {
	return h.attachmentLifecycleManager.ProcessUploadedAttachment(ctx, attachmentID, validators...)
}

// PruneTemporaryUploads executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) PruneTemporaryUploads(ctx context.Context, olderThan time.Duration) (int, error) {
	return h.attachmentLifecycleManager.PruneTemporaryUploads(ctx, olderThan)
}

// CleanupStuckAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) CleanupStuckAttachments(ctx context.Context, processingTimeout time.Duration) (int, error) {
	return h.attachmentLifecycleManager.CleanupStuckAttachments(ctx, processingTimeout)
}

// MarkAttachmentAsUploaded executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) MarkAttachmentAsUploaded(
	ctx context.Context,
	attachmentID uint64,
) error {
	return h.attachmentLifecycleManager.MarkAttachmentAsUploaded(ctx, attachmentID)
}

// GetAttachmentURL returns the URL a client should use to fetch a vetted attachment.
// It is the single URL issuance entry point for user-visible attachments and
// dispatches on the row's stored visibility, because visibility selects the bucket
// the verified object was published into. A private attachment is served through a
// short-lived presigned URL against the private bucket; a public attachment's object
// exists only in the public bucket, so it is served from its stable, anonymously
// fetchable path composed against the configured public base URL. Non-vetted rows
// and internal protocol artifacts are rejected, matching the rest of the attachment
// read surface.
//
// Parameters:
// - `ctx`: propagates request-scoped deadlines, cancellation, and trace metadata.
// - `attachmentID`: supplies the `attachmentID` input as `uint64` for this operation.
// - `expiry`: bounds the lifetime of a presigned private URL; public URLs never expire. <= 0 selects the hot-configured AttachmentDownloadURLExpiry default.
func (h *CommonServiceHandler[U]) GetAttachmentURL(
	ctx context.Context,
	attachmentID uint64,
	expiry time.Duration,
) (string, error) {
	return h.attachmentLifecycleManager.GetAttachmentURL(ctx, attachmentID, expiry)
}

// GetUploadURLExpiry executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetUploadURLExpiry() time.Duration {
	return h.attachmentLifecycleManager.GetUploadURLExpiry()
}

// CreateEntityAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// SECURITY: entityID must be selected by the consuming service after it has authorized
// the actor to modify attachments for that entity. This helper does not perform
// entity-level authorization.
func (h *CommonServiceHandler[U]) CreateEntityAttachments(
	ctx context.Context,
	userID uint64,
	objectType string,
	entityID string,
	files []AttachmentRequest,
) ([]AttachmentResponse, error) {
	// Ensure transaction context exists for atomic operations
	if err := h.requireTransaction(ctx, "create entity attachments"); err != nil {
		return nil, err
	}

	// Prepare requests for bulk creation
	requests := make([]struct {
		Filename    string
		ContentType string
		ContentHash string
	}, len(files))
	for i, file := range files {
		requests[i] = struct {
			Filename    string
			ContentType string
			ContentHash string
		}{
			Filename:    file.Filename,
			ContentType: file.ContentType,
			ContentHash: file.ContentHash,
		}
	}

	// Use bulk creation method to avoid N+1 queries
	issued, err := h.attachmentLifecycleManager.CreateUploadURLsWithEntityResult(
		ctx, userID, objectType, entityID, requests)
	if err != nil {
		return nil, fmt.Errorf("failed to bulk create attachments: %w", err)
	}

	// Convert to response format
	responses := make([]AttachmentResponse, len(issued))
	for i, result := range issued {
		responses[i] = AttachmentResponse{
			ID:       result.Attachment.ID,
			URL:      result.UploadURL,
			FormData: result.FormData,
			Filename: result.Attachment.OriginalFileName,
			Expiry:   result.ExpiresAt,
		}
	}

	return responses, nil
}

// GetAttachmentByID executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetAttachmentByID(
	ctx context.Context,
	attachmentID uint64,
) (*common_persistence_models.Attachment, error) {
	attachment, err := h.commonPersistenceHandler.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return nil, err
	}
	if attachment == nil || attachment.InternalOnly {
		return nil, attachmentpkg.ErrInternalAttachmentAccess
	}
	return attachment, nil
}

// GetAttachmentsByIDs executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetAttachmentsByIDs(
	ctx context.Context,
	attachmentIDs []uint64,
) ([]*common_persistence_models.Attachment, error) {
	return h.commonPersistenceHandler.GetAttachmentsByIDs(ctx, attachmentIDs)
}

// GetAttachmentsByIDsAndUser executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) GetAttachmentsByIDsAndUser(
	ctx context.Context,
	userID uint64,
	attachmentIDs []uint64,
) ([]*common_persistence_models.Attachment, error) {
	return h.commonPersistenceHandler.GetAttachmentsByIDsAndUser(ctx, attachmentIDs, userID)
}

// LinkAttachmentsToEntity executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// SECURITY: this method validates attachment ownership, but it does not authorize the
// target entityID. The consuming service must verify the actor may modify attachments
// for the target entity before calling this method.
func (h *CommonServiceHandler[U]) LinkAttachmentsToEntity(
	ctx context.Context,
	userID uint64,
	attachmentIDs []uint64,
	entityID string,
) error {
	// First, validate that the user owns all the attachments
	attachments, err := h.commonPersistenceHandler.GetAttachmentsByIDsAndUser(ctx, attachmentIDs, userID)
	if err != nil {
		return fmt.Errorf("failed to validate attachment ownership: %w", err)
	}

	// Check that all requested attachments were found and owned by the user
	if len(attachments) != len(attachmentIDs) {
		return fmt.Errorf("not all attachments found or owned by user")
	}

	// Link all attachments to the entity in a single operation
	if err := h.commonPersistenceHandler.UpdateAttachmentEntityIDs(ctx, attachmentIDs, entityID); err != nil {
		return fmt.Errorf("failed to link attachments to entity: %w", err)
	}

	appLogger.For(ctx, h.logger, loggerName).Info("Linked attachments to entity",
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.String(appLogger.KeyEntityID, entityID),
		zap.Int(appLogger.KeyAttachmentCount, len(attachmentIDs)))

	return nil
}

// UnlinkAttachmentsFromEntity executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) UnlinkAttachmentsFromEntity(
	ctx context.Context,
	userID uint64,
	attachmentIDs []uint64,
) error {
	// First, validate that the user owns all the attachments
	attachments, err := h.commonPersistenceHandler.GetAttachmentsByIDsAndUser(ctx, attachmentIDs, userID)
	if err != nil {
		return fmt.Errorf("failed to validate attachment ownership: %w", err)
	}

	// Check that all requested attachments were found and owned by the user
	if len(attachments) != len(attachmentIDs) {
		return fmt.Errorf("not all attachments found or owned by user")
	}

	// Delete all attachments in a single operation since the system doesn't support orphaned attachments
	if err := h.commonPersistenceHandler.DeleteAttachmentsByIDs(ctx, attachmentIDs); err != nil {
		return fmt.Errorf("failed to delete attachments: %w", err)
	}

	appLogger.For(ctx, h.logger, loggerName).Info("Deleted attachments from entities",
		zap.Uint64(appLogger.KeyUserID, userID),
		zap.Int(appLogger.KeyAttachmentCount, len(attachmentIDs)))

	return nil
}

// PlanEntityAttachmentSynchronization executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
//
// SECURITY: entityID must be selected by the consuming service after it has authorized
// the actor to modify attachments for that entity. This helper plans and creates
// attachment changes for an already-authorized entity.
func (h *CommonServiceHandler[U]) PlanEntityAttachmentSynchronization(
	ctx context.Context,
	userID uint64,
	entityID string,
	attachmentType string,
	requestedAttachments []*proto_models.AttachmentMetadata,
	currentAttachmentIds []uint64,
) ([]AttachmentResponse, []uint64, []string, error) {
	currentAttachments, err := h.GetAttachmentsByIDs(ctx, currentAttachmentIds)
	if err != nil {
		return nil, nil, nil, appLogger.WithErrorFields(fmt.Errorf("failed to get current attachments: %w", err),
			zap.String(logkeys.Stage, "load_current_entity_attachments"),
			zap.Uint64(appLogger.KeyUserID, userID),
			zap.String(appLogger.KeyEntityID, entityID),
			zap.String(appLogger.KeyObjectType, attachmentType),
			zap.Uint64s(appLogger.KeyAttachmentIDs, currentAttachmentIds))
	}

	// Existing attachments by the hash their client knows them by, which a pending upload has too
	existingHashes := make(map[string]*common_persistence_models.Attachment)
	for _, att := range currentAttachments {
		existingHashes[att.ClientContentHash()] = att
	}

	// Find new attachments (hashes not in existing)
	var newAttachmentRequests []AttachmentRequest
	var newAttachmentHashes []string
	for _, reqAtt := range requestedAttachments {
		if _, exists := existingHashes[reqAtt.ContentHash]; !exists {
			newAttachmentRequests = append(newAttachmentRequests, AttachmentRequest{
				Filename:    reqAtt.Filename,
				ContentType: reqAtt.ContentType,
				ContentHash: reqAtt.ContentHash,
			})
			newAttachmentHashes = append(newAttachmentHashes, reqAtt.ContentHash)
		}
	}

	// Create new attachments via go-common (if any)
	var newAttachmentResponses []AttachmentResponse
	if len(newAttachmentRequests) > 0 {
		newAttachmentResponses, err = h.CreateEntityAttachments(
			ctx, userID, attachmentType, entityID, newAttachmentRequests)
		if err != nil {
			return nil, nil, nil, appLogger.WithErrorFields(fmt.Errorf("failed to create new attachments: %w", err),
				zap.String(logkeys.Stage, "create_entity_attachments"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(appLogger.KeyEntityID, entityID),
				zap.String(appLogger.KeyObjectType, attachmentType),
				zap.Int(appLogger.KeyCount, len(newAttachmentRequests)))
		}
	}

	// Handle attachment removal (attachments in existing but not in request)
	requestedHashes := make(map[string]bool)
	for _, reqAtt := range requestedAttachments {
		requestedHashes[reqAtt.ContentHash] = true
	}

	var attachmentsToRemove []uint64
	for _, existing := range currentAttachments {
		if !requestedHashes[existing.ClientContentHash()] {
			attachmentsToRemove = append(attachmentsToRemove, existing.ID)
		}
	}

	return newAttachmentResponses, attachmentsToRemove, newAttachmentHashes, nil
}

// SynchronizeEntityAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) SynchronizeEntityAttachments(
	ctx context.Context,
	userID uint64,
	entityID string,
	attachmentType string,
	requestedAttachments []*proto_models.AttachmentMetadata,
	currentAttachmentIds []uint64,
) ([]AttachmentResponse, []uint64, []string, error) {
	newAttachmentResponses, attachmentsToRemove, newAttachmentHashes, err := h.PlanEntityAttachmentSynchronization(
		ctx,
		userID,
		entityID,
		attachmentType,
		requestedAttachments,
		currentAttachmentIds,
	)
	if err != nil {
		return nil, nil, nil, err
	}

	// Remove attachments synchronously within same transaction
	if len(attachmentsToRemove) > 0 {
		if err := h.UnlinkAttachmentsFromEntity(ctx, userID, attachmentsToRemove); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to unlink attachments: %w", err)
		}
	}

	return newAttachmentResponses, attachmentsToRemove, newAttachmentHashes, nil
}

type chatAttachmentRetentionAction int

const (
	chatAttachmentRetentionActionDelete chatAttachmentRetentionAction = iota
	chatAttachmentRetentionActionKeepLive
	chatAttachmentRetentionActionArchive
)

// determineChatAttachmentRetentionAction executes part of the common_service package flow and provides shared helper behavior.
func determineChatAttachmentRetentionAction(referencedByLive, referencedByHistory bool) chatAttachmentRetentionAction {
	switch {
	case referencedByLive:
		return chatAttachmentRetentionActionKeepLive
	case referencedByHistory:
		return chatAttachmentRetentionActionArchive
	default:
		return chatAttachmentRetentionActionDelete
	}
}

// FinalizeRemovedChatAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) FinalizeRemovedChatAttachments(ctx context.Context, attachmentIDs []uint64) error {
	if len(attachmentIDs) == 0 {
		return nil
	}

	attachmentIDs = common_util.DeduplicateNonZero(attachmentIDs)
	if len(attachmentIDs) == 0 {
		return nil
	}
	live, history, err := h.removedChatAttachmentReferences(ctx, attachmentIDs)
	if err != nil {
		return err
	}
	attachmentsToArchive := make([]uint64, 0, len(attachmentIDs))
	attachmentsToDelete := make([]uint64, 0, len(attachmentIDs))
	for _, attachmentID := range attachmentIDs {
		switch determineChatAttachmentRetentionAction(live[attachmentID], history[attachmentID]) {
		case chatAttachmentRetentionActionKeepLive:
			continue
		case chatAttachmentRetentionActionArchive:
			attachmentsToArchive = append(attachmentsToArchive, attachmentID)
		case chatAttachmentRetentionActionDelete:
			attachmentsToDelete = append(attachmentsToDelete, attachmentID)
		}
	}

	if len(attachmentsToArchive) > 0 {
		if err := h.commonPersistenceHandler.UpdateAttachmentObjectTypes(ctx, attachmentsToArchive, attachmentpkg.ObjectTypeArchivedChatAttachment); err != nil {
			return fmt.Errorf("failed to archive chat attachments: %w", err)
		}
	}

	if len(attachmentsToDelete) > 0 {
		if err := h.commonPersistenceHandler.DeleteAttachmentsByIDs(ctx, attachmentsToDelete); err != nil {
			return fmt.Errorf("failed to delete unreferenced chat attachments: %w", err)
		}
	}

	return nil
}

// BuildOrderedAttachmentIDs executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) BuildOrderedAttachmentIDs(
	ctx context.Context,
	requestAttachments []*proto_models.AttachmentMetadata,
	existingAttachmentIDs []uint64,
	newAttachmentResponses []AttachmentResponse,
) ([]uint64, error) {
	if len(requestAttachments) == 0 {
		return []uint64{}, nil
	}

	// Create a map of existing attachments by content hash for lookup
	existingAttachmentMap := make(map[string]uint64)
	if len(existingAttachmentIDs) > 0 {
		// Get existing attachments to map content hash to ID
		existingAttachments, err := h.GetAttachmentsByIDs(ctx, existingAttachmentIDs)
		if err != nil {
			return nil, appLogger.WithErrorFields(err,
				zap.String(logkeys.Stage, "build_ordered_attachment_ids"),
				zap.Uint64s(appLogger.KeyAttachmentIDs, existingAttachmentIDs))
		}
		for index, att := range existingAttachments {
			if att == nil {
				return nil, appLogger.InvariantError(
					"existing_attachment_row_non_nil",
					nil,
					zap.String(logkeys.Stage, "build_ordered_attachment_ids"),
					zap.Int(appLogger.KeyAttachmentIndex, index),
					zap.Int(appLogger.KeyCount, len(existingAttachments)),
					zap.String(logkeys.Expected, "non-nil existing attachment row"),
					zap.String(logkeys.Actual, "nil"))
			}
			existingAttachmentMap[att.ClientContentHash()] = att.ID
		}
	}

	// Build new attachment IDs list based on the request order
	// This assumes the client sends attachments in the desired order
	// mixing existing (by content_hash) and new attachments
	attachmentIDs := make([]uint64, 0, len(requestAttachments))

	// Process attachments in request order
	newAttachmentIndex := 0
	for requestIndex, attachment := range requestAttachments {
		if attachment == nil {
			return nil, fmt.Errorf("attachment metadata at index %d is required", requestIndex)
		}
		// Check if this is an existing attachment (by content hash)
		if existingID, exists := existingAttachmentMap[attachment.ContentHash]; exists {
			attachmentIDs = append(attachmentIDs, existingID)
		} else {
			// This is a new attachment, use the next new attachment ID
			if newAttachmentIndex >= len(newAttachmentResponses) {
				return nil, appLogger.InvariantError(
					"new_attachment_response_available",
					nil,
					zap.String(logkeys.Stage, "build_ordered_attachment_ids"),
					zap.Int(appLogger.KeyRequestIndex, requestIndex),
					zap.Int(logkeys.Expected, newAttachmentIndex+1),
					zap.Int(logkeys.Actual, len(newAttachmentResponses)))
			}
			attachmentIDs = append(attachmentIDs, newAttachmentResponses[newAttachmentIndex].ID)
			newAttachmentIndex++
		}
	}
	if newAttachmentIndex != len(newAttachmentResponses) {
		return nil, appLogger.InvariantError(
			"new_attachment_response_count_matches_request",
			nil,
			zap.String(logkeys.Stage, "build_ordered_attachment_ids"),
			zap.Int(logkeys.Expected, newAttachmentIndex),
			zap.Int(logkeys.Actual, len(newAttachmentResponses)))
	}

	return attachmentIDs, nil
}

// CreateAttachmentUploadInfos executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) CreateAttachmentUploadInfos(
	newAttachmentResponses []AttachmentResponse,
	newAttachmentHashes []string,
) []*proto_models.AttachmentUploadInfo {
	if len(newAttachmentResponses) == 0 {
		return nil
	}

	uploadInfos := make([]*proto_models.AttachmentUploadInfo, len(newAttachmentResponses))
	for i, attachment := range newAttachmentResponses {
		uploadInfos[i] = &proto_models.AttachmentUploadInfo{
			ContentHash:  newAttachmentHashes[i], // Use hash from request
			UploadUrl:    attachment.URL,
			FormData:     attachment.FormData,
			AttachmentId: attachment.ID,
			Expiry:       timestamppb.New(attachment.Expiry),
		}
	}

	return uploadInfos
}

// CreateUploadedAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) CreateUploadedAttachments(ctx context.Context, attachmentIDs []uint64) (map[uint64]*proto_models.UploadedAttachment, error) {
	// Validate input
	if len(attachmentIDs) == 0 {
		return map[uint64]*proto_models.UploadedAttachment{}, nil
	}

	attachments, attachmentsRetrievalErr := h.GetAttachmentsByIDs(ctx, attachmentIDs)
	if attachmentsRetrievalErr != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("failed to retrieve attachments: %w", attachmentsRetrievalErr),
			zap.String(logkeys.Stage, "create_uploaded_attachment_views"),
			zap.Uint64s(appLogger.KeyAttachmentIDs, attachmentIDs))
	}

	// Create proto models for each attachment
	uploadedAttachments := make(map[uint64]*proto_models.UploadedAttachment, len(attachments))
	for _, attachment := range attachments {
		// Resolve the download URL for vetted attachments through the visibility
		// dispatch: a private attachment (the default, and what chat attachments
		// are) lives only in the private bucket and is reachable solely through a
		// presigned URL, while a public one resolves to its stable public-bucket
		// path. A row whose URL cannot be resolved keeps an empty DownloadUrl
		// rather than failing the whole batch.
		var downloadURL string
		if attachment.UploadStatus == common_persistence_models.UploadStatusVetted && h.attachmentLifecycleManager != nil {
			// Expiry 0 selects the hot-configured AttachmentDownloadURLExpiry
			// default inside the resolver.
			resolvedURL, resolveErr := h.attachmentLifecycleManager.ResolveAttachmentURL(
				ctx,
				attachment,
				0,
			)
			if resolveErr != nil {
				appLogger.For(ctx, h.logger, loggerName).Warn("Failed to resolve attachment download URL",
					zap.Error(resolveErr),
					zap.Uint64(appLogger.KeyAttachmentID, attachment.ID))
			} else {
				downloadURL = resolvedURL
			}
		}

		uploadedAttachments[attachment.ID] = &proto_models.UploadedAttachment{
			Id:           strconv.FormatUint(attachment.ID, 10),
			Filename:     attachment.OriginalFileName,
			ContentType:  attachment.ContentType,
			ContentHash:  attachment.ClientContentHash(),
			Size:         uint64(attachment.Size),
			AttachmentId: attachment.ID,
			DownloadUrl:  downloadURL,
			// The stored value is the wire enum; UNSPECIFIED reaches the client
			// as-is and is documented to be treated as private.
			Visibility: attachment.Visibility.Enum(),
		}
	}
	return uploadedAttachments, nil
}

// HydrateChatMessageAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) HydrateChatMessageAttachments(ctx context.Context, message *common_service_models.ChatMessage) error {
	if message == nil {
		return nil
	}
	return h.HydrateChatMessagesAttachments(ctx, []*common_service_models.ChatMessage{message})
}

// HydrateChatMessagesAttachments executes part of the common_service service flow and coordinates work for the *CommonServiceHandler[U] receiver.
func (h *CommonServiceHandler[U]) HydrateChatMessagesAttachments(ctx context.Context, messages []*common_service_models.ChatMessage) error {
	attachmentIDs := attachmentpkg.CollectUniqueAttachmentIDs(messages, func(message *common_service_models.ChatMessage) []uint64 {
		if message == nil {
			return nil
		}
		return message.AttachmentIDs
	})
	if len(attachmentIDs) == 0 {
		return nil
	}

	attachmentMap, err := h.CreateUploadedAttachments(ctx, attachmentIDs)
	if err != nil {
		return err
	}

	attachmentpkg.MapAttachmentsToEntities(
		messages,
		attachmentMap,
		func(message *common_service_models.ChatMessage) []uint64 {
			if message == nil {
				return nil
			}
			return message.AttachmentIDs
		},
		func(message *common_service_models.ChatMessage, attachments []*proto_models.UploadedAttachment) {
			if message == nil {
				return
			}
			message.Attachments = attachments
		},
	)
	return nil
}

// HydrateChatAgentQueuedInputsAttachments fills each queued agent input's Attachments from its
// AttachmentIDs in order, as HydrateChatMessagesAttachments does for messages, in one read.
func (h *CommonServiceHandler[U]) HydrateChatAgentQueuedInputsAttachments(ctx context.Context, inputs []*common_service_models.ChatAgentQueuedInput) error {
	attachmentIDs := attachmentpkg.CollectUniqueAttachmentIDs(inputs, func(input *common_service_models.ChatAgentQueuedInput) []uint64 {
		return input.AttachmentIDs
	})
	if len(attachmentIDs) == 0 {
		return nil
	}
	attachmentMap, err := h.CreateUploadedAttachments(ctx, attachmentIDs)
	if err != nil {
		return err
	}
	attachmentpkg.MapAttachmentsToEntities(
		inputs,
		attachmentMap,
		func(input *common_service_models.ChatAgentQueuedInput) []uint64 { return input.AttachmentIDs },
		func(input *common_service_models.ChatAgentQueuedInput, attachments []*proto_models.UploadedAttachment) {
			input.Attachments = attachments
		},
	)
	return nil
}

// attachmentWorkEnabled gates new background batches; admitted work keeps its row ownership.
func (h *CommonServiceHandler[U]) attachmentWorkEnabled() bool {
	if h.runtimeConfig != nil {
		return h.runtimeConfig().EnableAttachments()
	}
	return false
}
