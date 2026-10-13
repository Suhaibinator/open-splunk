package common_persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ============================================================================
// ATTACHMENT CRUD METHODS
// ============================================================================

// ErrInternalAttachmentMutation is returned when an ordinary attachment
// mutation attempts to create or update a server-owned protocol blob. Internal
// attachments must use the narrow privileged lifecycle methods instead.
var ErrInternalAttachmentMutation = errors.New("internal attachment requires a privileged persistence operation")

// ErrAttachmentUploadTransactionRequired reports a missing transaction for scoped renewal.
var ErrAttachmentUploadTransactionRequired = errors.New("attachment upload renewal requires an ambient transaction")

var errAttachmentPublicationStorage = errors.New("attachment publication storage operation failed")

// CreateAttachment executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error {
	if attachment == nil {
		return appLogger.InvariantError("attachment_create_requires_model", ErrDatabaseError,
			zap.String(logkeys.Operation, "create_attachment"),
			zap.String(logkeys.Expected, "non-nil attachment"),
			zap.String(logkeys.Actual, "nil attachment"))
	}
	if attachment.InternalOnly {
		return ErrInternalAttachmentMutation
	}
	return h.createAttachment(ctx, attachment)
}

// CreateInternalAttachment is the privileged creation path for server-owned
// provider state and tool-result media. Public upload creation rejects these
// rows so a generic persistence caller cannot cross the visibility boundary.
func (h *CommonPersistenceHandler[U]) CreateInternalAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error {
	if attachment == nil || !attachment.InternalOnly {
		return ErrInternalAttachmentMutation
	}
	return h.createAttachment(ctx, attachment)
}

func (h *CommonPersistenceHandler[U]) createAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error {
	db := h.getDBFromContext(ctx)
	result := db.Create(attachment)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "create_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.Uint64(appLogger.KeyUserID, attachment.UserID))
	}
	return nil
}

// GetAttachmentByID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetAttachmentByID(ctx context.Context, id uint64) (*common_persistence_models.Attachment, error) {
	var attachment common_persistence_models.Attachment
	db := h.getDBFromContext(ctx)
	result := db.Preload(common_persistence_models.PreloadUser).First(&attachment, id)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("Attachment not found",
					zap.String(logkeys.Operation, "get_attachment_by_id"),
					zap.Uint64(appLogger.KeyAttachmentID, id))
			}
			return nil, fmt.Errorf("attachment not found: %w", ErrDatabaseError)
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_attachment_by_id"),
			zap.Uint64(appLogger.KeyAttachmentID, id))
	}
	return &attachment, nil
}

// UpdateAttachment executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error {
	if attachment == nil || attachment.ID == 0 {
		return appLogger.InvariantError("attachment_update_requires_persisted_model", ErrDatabaseError,
			zap.String(logkeys.Operation, "update_attachment"),
			zap.String(logkeys.Expected, "non-nil attachment with non-zero ID"),
			zap.String(logkeys.Actual, "nil attachment or zero ID"))
	}
	if attachment.InternalOnly {
		return ErrInternalAttachmentMutation
	}

	// Save falls back to INSERT when an UPDATE matches no rows. Besides
	// resurrecting missing records, that fallback could turn an attempted
	// internal-row update into a new public row. Use an explicit guarded UPDATE
	// and enumerate mutable columns so the visibility bit can never be flipped.
	result := h.getDBFromContext(ctx).Model(&common_persistence_models.Attachment{}).
		Where("id = ? AND internal_only = ?", attachment.ID, false).
		Select(publicAttachmentUpdateColumns()).
		Updates(attachment)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_attachment"),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("No attachment found to update",
				zap.String(logkeys.Operation, "update_attachment"),
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID))
		}
		return fmt.Errorf("attachment not found: %w", ErrDatabaseError)
	}
	return nil
}

// UpdateAttachmentIfStatus performs a compare-and-swap update for a non-internal
// attachment, regardless of whether its verified object uses private or public storage. Lifecycle callers use it when moving between upload states so a
// stale worker cannot resurrect a row that a concurrent cleanup already
// rejected (or overwrite a row that another worker has begun processing).
func (h *CommonPersistenceHandler[U]) UpdateAttachmentIfStatus(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	expectedStatus common_persistence_models.UploadStatus,
) (bool, error) {
	return h.compareAndSwapAttachment(ctx, h.getDBFromContext(ctx), attachment, expectedStatus, "update_attachment_if_status")
}

// UpdateExpiredPendingUpload is UpdateAttachmentIfStatus from pending_upload for a row whose latest
// upload URL expired before cutoffTime, the pending-upload cleanup's claim. Matching
// upload_url_expires_at in the same UPDATE keeps a sweep that read the row before a renewal
// (RenewPendingUploads) committed from moving the renewed row, which then reports false.
func (h *CommonPersistenceHandler[U]) UpdateExpiredPendingUpload(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	cutoffTime time.Time,
) (bool, error) {
	return h.compareAndSwapAttachment(ctx,
		h.getDBFromContext(ctx).Where("upload_url_expires_at < ?", cutoffTime),
		attachment, common_persistence_models.UploadStatusPendingUpload, "update_expired_pending_upload")
}

// compareAndSwapAttachment writes the attachment snapshot's lifecycle columns when the row, further
// narrowed by db's conditions, is a public attachment in expectedStatus.
func (h *CommonPersistenceHandler[U]) compareAndSwapAttachment(
	ctx context.Context,
	db *gorm.DB,
	attachment *common_persistence_models.Attachment,
	expectedStatus common_persistence_models.UploadStatus,
	operation string,
) (bool, error) {
	if attachment == nil || attachment.ID == 0 {
		return false, appLogger.InvariantError("conditional_attachment_update_requires_persisted_model", ErrDatabaseError,
			zap.String(logkeys.Operation, operation),
			zap.String(logkeys.Expected, "non-nil attachment with non-zero ID"),
			zap.String(logkeys.Actual, "nil attachment or zero ID"))
	}
	if attachment.InternalOnly {
		return false, ErrInternalAttachmentMutation
	}

	result := db.Model(&common_persistence_models.Attachment{}).
		Where("id = ? AND internal_only = ? AND upload_status = ?", attachment.ID, false, expectedStatus).
		Select(publicAttachmentUpdateColumns()).
		Updates(attachment)
	if result.Error != nil {
		return false, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, operation),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.Stringer(logkeys.Expected, expectedStatus))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Conditional attachment update did not match",
				zap.String(logkeys.Operation, operation),
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
				zap.Stringer(logkeys.Expected, expectedStatus))
		}
	}
	return result.RowsAffected == 1, nil
}

// ClaimExpiredPublicAttachments atomically transitions only still-pending,
// expired non-internal uploads to REJECTED and returns exactly the rows claimed by
// this update. Callers may therefore delete temporary objects only for these
// returned rows without racing a concurrent pending -> processing transition.
// A row expires once both its upload_time and its latest upload URL expiry
// (upload_url_expires_at) are before cutoffTime, so no row is claimed while an
// upload URL issued for it is still valid.
func (h *CommonPersistenceHandler[U]) ClaimExpiredPublicAttachments(
	ctx context.Context,
	cutoffTime time.Time,
	processingEndTime time.Time,
) ([]*common_persistence_models.Attachment, error) {
	claimed := make([]*common_persistence_models.Attachment, 0)
	result := h.getDBFromContext(ctx).
		Model(&claimed).
		Clauses(clause.Returning{}).
		Where("upload_status IN ? AND upload_time < ? AND upload_url_expires_at < ? AND internal_only = ?",
			[]common_persistence_models.UploadStatus{
				common_persistence_models.UploadStatusPendingUpload,
				common_persistence_models.UploadStatusPendingProcessing,
			},
			cutoffTime,
			cutoffTime,
			false,
		).
		Select("upload_status", "validation_details", "processing_end_time").
		Updates(common_persistence_models.Attachment{
			UploadStatus:      common_persistence_models.UploadStatusRejected,
			ValidationDetails: "Upload timed out",
			ProcessingEndTime: &processingEndTime,
		})
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "claim_expired_public_attachments"),
			zap.Time(appLogger.KeyCutoffTime, cutoffTime))
	}
	return claimed, nil
}

// ListSettledTemporaryObjects returns up to limit attachments, soft-deleted ones included,
// that still name a temporary object no lifecycle step reads again: the row is vetted,
// rejected, or deleted, and every upload URL issued for it expired before now, so no
// replayed upload can recreate the object after the caller deletes it. Only id and
// temporary_object_name are loaded.
func (h *CommonPersistenceHandler[U]) ListSettledTemporaryObjects(ctx context.Context, now time.Time, limit int) ([]*common_persistence_models.Attachment, error) {
	var attachments []*common_persistence_models.Attachment
	result := h.getDBFromContext(ctx).Unscoped().
		Select("id", "temporary_object_name").
		Where("temporary_object_name <> '' AND upload_url_expires_at < ? AND (deleted_at IS NOT NULL OR upload_status IN ?)", now,
			[]common_persistence_models.UploadStatus{common_persistence_models.UploadStatusVetted, common_persistence_models.UploadStatusRejected}).
		Order("id").Limit(limit).Find(&attachments)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error, zap.String(logkeys.Operation, "list_settled_temporary_objects"))
	}
	return attachments, nil
}

// ClearTemporaryObjectName forgets a deleted temporary object so the settled-object sweep
// stops returning its row. It leaves updated_at untouched.
func (h *CommonPersistenceHandler[U]) ClearTemporaryObjectName(ctx context.Context, attachmentID uint64, objectName string) error {
	result := h.getDBFromContext(ctx).Unscoped().Model(&common_persistence_models.Attachment{}).
		Where("id = ? AND temporary_object_name = ?", attachmentID, objectName).
		UpdateColumn("temporary_object_name", "")
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "clear_temporary_object_name"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}
	return nil
}

// RenewPendingUploads records upload URLs reissued for the non-internal attachments among attachmentIDs
// that still wait for their upload: it extends each row's upload_url_expires_at to at least
// expiresAt, the expiry the caller signs the URLs with, and returns exactly the renewed rows. A row
// finalized or rejected meanwhile is left alone and absent from the result, and must get no URL.
func (h *CommonPersistenceHandler[U]) RenewPendingUploads(
	ctx context.Context,
	attachmentIDs []uint64,
	expiresAt time.Time,
) ([]*common_persistence_models.Attachment, error) {
	renewed := make([]*common_persistence_models.Attachment, 0, len(attachmentIDs))
	if len(attachmentIDs) == 0 {
		return renewed, nil
	}
	result := h.getDBFromContext(ctx).
		Model(&renewed).
		Clauses(clause.Returning{}).
		Where("id IN ? AND upload_status = ? AND internal_only = ?",
			attachmentIDs, common_persistence_models.UploadStatusPendingUpload, false).
		Update("upload_url_expires_at", gorm.Expr("GREATEST(upload_url_expires_at, ?)", expiresAt))
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "renew_pending_uploads"),
			zap.Int(appLogger.KeyCount, len(attachmentIDs)))
	}
	return renewed, nil
}

// RenewPendingUploadsInScope atomically extends only live, non-internal pending
// uploads matching the expected uploader, object type and entity ID. Empty
// entityID matches standalone rows exactly; it is never a wildcard. Unmatched or
// ineligible IDs are omitted. The UPDATE locks returned rows until the ambient
// transaction ends, serializing renewal with reparenting, finalization and cleanup.
// This checks attachment scope, not the actor's permission to modify the entity.
// Callers must roll back on error and keep signed instructions private until commit.
func (h *CommonPersistenceHandler[U]) RenewPendingUploadsInScope(
	ctx context.Context, attachmentIDs []uint64, userID uint64, objectType, entityID string, expiresAt time.Time,
) ([]*common_persistence_models.Attachment, error) {
	tx, ok := scontext.GetTransaction(ctx)
	if !ok || tx == nil || tx.GetDB() == nil {
		return nil, ErrAttachmentUploadTransactionRequired
	}
	renewed := make([]*common_persistence_models.Attachment, 0, len(attachmentIDs))
	if len(attachmentIDs) == 0 {
		return renewed, nil
	}
	result := h.getDBFromContext(ctx).Model(&renewed).Clauses(clause.Returning{}).
		Where("id IN ? AND user_id = ? AND object_type = ? AND entity_id = ? AND upload_status = ? AND internal_only = ? AND temporary_object_name <> ''",
			attachmentIDs, userID, objectType, entityID, common_persistence_models.UploadStatusPendingUpload, false).
		Update("upload_url_expires_at", gorm.Expr("GREATEST(upload_url_expires_at, ?)", expiresAt))
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "renew_pending_uploads_in_scope"), zap.Int(appLogger.KeyCount, len(attachmentIDs)))
	}
	return renewed, nil
}

// publicAttachmentUpdateColumns lists the columns a lifecycle write (UpdateAttachment,
// UpdateAttachmentIfStatus) sets from the caller's snapshot of the row. Ownership (object_type,
// entity_id) is not among them: a snapshot read before the row was re-parented, as delivery of a
// queued agent input moves its uploads to the message it creates, must not move it back. Owners
// change only through the explicit re-parenting writes. Selecting the columns also writes zero
// values and NULL processing timestamps from the typed attachment.
func publicAttachmentUpdateColumns() []string {
	return []string{
		"user_id", "original_file_name", "content_type", "size", "temporary_object_name",
		"final_object_name", "file_hash", "upload_status", "upload_time",
		"processing_start_time", "processing_end_time", "validation_details",
	}
}

// UpdateAttachmentObjectTypes executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateAttachmentObjectTypes(ctx context.Context, attachmentIDs []uint64, objectType string) error {
	if len(attachmentIDs) == 0 {
		return nil
	}

	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.Attachment{}).
		Where("id IN ? AND internal_only = ?", attachmentIDs, false).
		Update("object_type", objectType)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_attachment_object_types"),
			zap.Int(appLogger.KeyCount, len(attachmentIDs)),
			zap.String(appLogger.KeyObjectType, objectType))
	}
	if result.RowsAffected != int64(len(attachmentIDs)) {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, loggerName), "attachment_object_type_update_matches_requested_rows", nil,
			zap.String(logkeys.Operation, "update_attachment_object_types"),
			zap.Int(logkeys.Expected, len(attachmentIDs)),
			zap.Int64(logkeys.Actual, result.RowsAffected),
			zap.String(appLogger.KeyObjectType, objectType),
			zap.String(logkeys.Fallback, "retain_partial_update"))
	}

	return nil
}

// ListInternalAttachmentReclaimCandidates returns a bounded least-recently-attempted batch of
// live internal rows no message or edit history references.
func (h *CommonPersistenceHandler[U]) ListInternalAttachmentReclaimCandidates(ctx context.Context, before time.Time, limit int) ([]*common_persistence_models.Attachment, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	var attachments []*common_persistence_models.Attachment
	result := h.getDBFromContext(ctx).
		Where("internal_only = ? AND updated_at < ?", true, before).
		Where("NOT EXISTS (SELECT 1 FROM " + common_persistence_models.TableNameChatMessages + " WHERE " + common_persistence_models.TableNameAttachments + ".id = ANY(attachment_ids)) AND NOT EXISTS (SELECT 1 FROM " + common_persistence_models.TableNameChatMessageEditHistory + " WHERE " + common_persistence_models.TableNameAttachments + ".id = ANY(attachment_ids))").
		Order("updated_at ASC, id ASC").Limit(limit).Find(&attachments)
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "list_internal_attachment_reclaim_candidates"),
			zap.Time(appLogger.KeyCutoffTime, before),
			zap.Int(logkeys.Limit, limit))
	}
	return attachments, nil
}

// TombstoneInternalAttachmentIfUnreferenced serializes with CreateTranscriptMessage's FOR UPDATE
// lock and soft-deletes only when no live/history message owns the attachment. The tombstone is
// final: it keeps the row's object name for TODO(object-refcounting) in attachment/lifecycle_manager.go.
func (h *CommonPersistenceHandler[U]) TombstoneInternalAttachmentIfUnreferenced(ctx context.Context, id uint64, inactiveBefore time.Time) (bool, error) {
	db := h.getDBFromContext(ctx)
	tombstoned := false
	err := db.Transaction(func(tx *gorm.DB) error {
		var attachment common_persistence_models.Attachment
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND internal_only = ?", id, true).First(&attachment).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if attachment.DeletedAt.Valid {
			tombstoned = true
			return nil
		}
		// A StoreInternal publisher refreshes UpdatedAt while holding this same row lock. Recheck
		// the inactivity lease after acquiring the lock so a candidate fetched before publication
		// cannot reclaim the freshly published row in the gap before transcript insertion.
		if inactiveBefore.IsZero() || !attachment.UpdatedAt.Before(inactiveBefore) {
			return nil
		}
		var referenced bool
		query := "SELECT EXISTS (SELECT 1 FROM " + common_persistence_models.TableNameChatMessages +
			" WHERE ? = ANY(attachment_ids) UNION ALL SELECT 1 FROM " + common_persistence_models.TableNameChatMessageEditHistory +
			" WHERE ? = ANY(attachment_ids))"
		if err := tx.Raw(query, int64(id), int64(id)).Scan(&referenced).Error; err != nil {
			return err
		}
		if referenced {
			return nil
		}
		if err := tx.Delete(&attachment).Error; err != nil {
			return err
		}
		tombstoned = true
		return nil
	})
	if err != nil {
		return false, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "tombstone_internal_attachment_if_unreferenced"),
			zap.Uint64(appLogger.KeyAttachmentID, id),
			zap.Time(appLogger.KeyCutoffTime, inactiveBefore))
	}
	return tombstoned, nil
}

// CreateAttachments executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreateAttachments(ctx context.Context, attachments []*common_persistence_models.Attachment) error {
	if len(attachments) == 0 {
		return nil // Nothing to create
	}
	for index, attachment := range attachments {
		if attachment == nil {
			return appLogger.InvariantError("attachment_batch_create_contains_models", ErrDatabaseError,
				zap.String(logkeys.Operation, "create_attachments"),
				zap.Int(appLogger.KeyAttachmentIndex, index),
				zap.Int(appLogger.KeyCount, len(attachments)),
				zap.String(logkeys.Expected, "non-nil attachment at every index"),
				zap.String(logkeys.Actual, "nil attachment"))
		}
		if attachment.InternalOnly {
			return ErrInternalAttachmentMutation
		}
	}

	db := h.getDBFromContext(ctx)
	result := db.Create(attachments)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "create_attachments"),
			zap.Int(appLogger.KeyCount, len(attachments)))
	}

	appLogger.For(ctx, h.logger, loggerName).Info("Successfully created attachments",
		zap.String(logkeys.Operation, "create_attachments"),
		zap.String(appLogger.KeyOutcome, "created"),
		zap.Int(appLogger.KeyCount, len(attachments)))
	return nil
}

// ============================================================================
// ATTACHMENT QUERY METHODS
// ============================================================================

// GetAttachmentsByStatus executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetAttachmentsByStatus(ctx context.Context, status common_persistence_models.UploadStatus, limit int) ([]*common_persistence_models.Attachment, error) {
	var attachments []*common_persistence_models.Attachment
	db := h.getDBFromContext(ctx)

	// Default limit if not specified or invalid
	if limit <= 0 {
		limit = 50 // Default to 50 records
	}

	result := db.Where("upload_status = ? AND internal_only = ?", status, false).
		Order("updated_at ASC"). // Process oldest first
		Limit(limit).
		Preload(common_persistence_models.PreloadUser).
		Find(&attachments)

	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_attachments_by_status"),
			zap.Stringer(logkeys.Status, status),
			zap.Int(logkeys.Limit, limit))
	}

	return attachments, nil
}

// DeleteAttachmentsByIDs executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) DeleteAttachmentsByIDs(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil // Nothing to delete
	}

	db := h.getDBFromContext(ctx)
	result := db.Where("id IN ? AND internal_only = ?", ids, false).Delete(&common_persistence_models.Attachment{})
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "delete_attachments_by_ids"),
			zap.Int(appLogger.KeyCount, len(ids)))
	}

	appLogger.For(ctx, h.logger, loggerName).Info("Successfully deleted attachments",
		zap.String(logkeys.Operation, "delete_attachments_by_ids"),
		zap.String(appLogger.KeyOutcome, "deleted"),
		zap.Int(appLogger.KeyCount, len(ids)),
		zap.Int64(appLogger.KeyRows, result.RowsAffected))
	return nil
}

// FindAttachmentByHash executes its operation in the persistence flow between service handlers and the database layer.
// The lookup is scoped by visibility because the storage identity of a verified object
// is (Visibility, FinalObjectName): identical content yields the same FinalObjectName in
// both buckets, so an unscoped match could deduplicate a private upload onto a public
// row and silently publish it (or the reverse).
// Parameters:
//   - ctx: context.Context input used by FindAttachmentByHash.
//   - fileHash: string input used by FindAttachmentByHash.
//   - visibility: proto_models.AttachmentVisibility input used by FindAttachmentByHash.
//     Matching is by visibility class, not exact value: a public argument matches
//     only explicitly public rows, anything else matches the private class
//     (ATTACHMENT_VISIBILITY_UNSPECIFIED and ATTACHMENT_VISIBILITY_PRIVATE alike),
//     because legacy zero-valued rows are stored private.
func (h *CommonPersistenceHandler[U]) FindAttachmentByHash(ctx context.Context, fileHash string, visibility proto_models.AttachmentVisibility) (*common_persistence_models.Attachment, error) {
	var attachment common_persistence_models.Attachment
	db := h.getDBFromContext(ctx)

	visibilityPredicate := "visibility <> ?"
	if common_persistence_models.IsPublicAttachmentVisibility(visibility) {
		visibilityPredicate = "visibility = ?"
	}
	result := db.Where("file_hash = ? AND upload_status = ? AND internal_only = ? AND "+visibilityPredicate,
		fileHash, common_persistence_models.UploadStatusVetted, false,
		proto_models.AttachmentVisibility_ATTACHMENT_VISIBILITY_PUBLIC).
		First(&attachment)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			// No existing attachment found with this hash and visibility
			return nil, nil
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "find_attachment_by_hash"),
			zap.String(appLogger.KeyFileHash, fileHash),
			zap.Stringer(appLogger.KeyVisibility, visibility))
	}

	return &attachment, nil
}

// GetAttachmentsByIDs executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetAttachmentsByIDs(ctx context.Context, attachmentIDs []uint64) ([]*common_persistence_models.Attachment, error) {
	if len(attachmentIDs) == 0 {
		return []*common_persistence_models.Attachment{}, nil
	}

	var attachments []*common_persistence_models.Attachment
	db := h.getDBFromContext(ctx)

	result := db.Where("id IN ? AND internal_only = ?", attachmentIDs, false).
		Preload(common_persistence_models.PreloadUser).
		Find(&attachments)

	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_attachments_by_ids"),
			zap.Int(appLogger.KeyCount, len(attachmentIDs)))
	}

	return attachments, nil
}

// GetAttachmentsByIDsAndUser executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetAttachmentsByIDsAndUser(ctx context.Context, attachmentIDs []uint64, userID uint64) ([]*common_persistence_models.Attachment, error) {
	if len(attachmentIDs) == 0 {
		return []*common_persistence_models.Attachment{}, nil
	}

	var attachments []*common_persistence_models.Attachment
	db := h.getDBFromContext(ctx)

	result := db.Where("id IN ? AND user_id = ? AND internal_only = ?", attachmentIDs, userID, false).
		Preload(common_persistence_models.PreloadUser).
		Find(&attachments)

	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_attachments_by_ids_and_user"),
			zap.Int(appLogger.KeyCount, len(attachmentIDs)),
			zap.Uint64(appLogger.KeyUserID, userID))
	}

	return attachments, nil
}

// ============================================================================
// ENTITY-ATTACHMENT RELATIONSHIP METHODS
// ============================================================================

func attachmentEntityQuery(db *gorm.DB, objectType, entityID string, status *common_persistence_models.UploadStatus) *gorm.DB {
	query := db.Where("object_type = ? AND entity_id = ? AND internal_only = ?", objectType, entityID, false)
	if status != nil {
		query = query.Where("upload_status = ?", *status)
	}
	return query
}

// GetAttachmentsByEntity retrieves non-internal attachments for an object and
// entity, with optional upload-status filtering, ordering, and pagination.
func (h *CommonPersistenceHandler[U]) GetAttachmentsByEntity(ctx context.Context, objectType, entityID string, options common_persistence_models.AttachmentQueryOptions) ([]*common_persistence_models.Attachment, error) {
	var attachments []*common_persistence_models.Attachment
	db := h.getDBFromContext(ctx)

	query := attachmentEntityQuery(db, objectType, entityID, options.Status).
		Order(options.OrderBy.SQLClause())
	if options.Limit > 0 {
		query = query.Limit(options.Limit)
	}
	if options.Offset > 0 {
		query = query.Offset(options.Offset)
	}

	result := query.
		Preload(common_persistence_models.PreloadUser).
		Find(&attachments)

	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_attachments_by_entity"),
			zap.String(appLogger.KeyEntityID, entityID),
			zap.String(appLogger.KeyObjectType, objectType))
	}

	return attachments, nil
}

// GetInternalAttachmentByEntity is the privileged lookup used solely to make synchronous
// server-generated attachments idempotent. Public entity reads deliberately filter these rows.
func (h *CommonPersistenceHandler[U]) GetInternalAttachmentByEntity(
	ctx context.Context,
	objectType string,
	entityID string,
) (*common_persistence_models.Attachment, error) {
	var attachment common_persistence_models.Attachment
	result := h.getDBFromContext(ctx).
		Where("entity_id = ? AND object_type = ? AND internal_only = ?", entityID, objectType, true).
		Order("id ASC").
		First(&attachment)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if result.Error != nil {
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_internal_attachment_by_entity"),
			zap.String(appLogger.KeyEntityID, entityID),
			zap.String(appLogger.KeyObjectType, objectType))
	}
	return &attachment, nil
}

// PublishInternalAttachmentObject holds the internal row's UPDATE lock across the object write
// and the PROCESSING -> VETTED transition. This intentionally trades a short database lock for a
// strict lifecycle invariant: the reaper can never tombstone the row while
// an object write is in flight, and publication never resurrects a tombstoned row.
//
// Internal object names are content-addressed like private uploads (verified/<salted-hash>), so
// the object may already hold these bytes for another internal row, an upload or a fork clone.
// An existing object is reused, not rewritten, once its bytes are verified to equal the payload.
// No path deletes verified objects (TODO(object-refcounting) in attachment/lifecycle_manager.go),
// so the existence check needs no lock against removal.
func (h *CommonPersistenceHandler[U]) PublishInternalAttachmentObject(
	ctx context.Context,
	attachmentID uint64,
	objectName string,
	data []byte,
	contentType string,
) (*common_persistence_models.Attachment, error) {
	var published common_persistence_models.Attachment
	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND internal_only = ? AND upload_status = ? AND final_object_name = ?", attachmentID, true, common_persistence_models.UploadStatusProcessing, objectName).
			First(&published).Error; err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		if published.Size != int64(len(data)) || published.ContentType != contentType ||
			!strings.EqualFold(published.FileHash, hex.EncodeToString(digest[:])) {
			return appLogger.InvariantError("internal_attachment_publication_metadata_matches_payload", nil,
				zap.String(logkeys.Operation, "publish_internal_attachment_object"),
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyObjectName, objectName),
				zap.String(logkeys.Expected, "stored size, content type, and hash match payload"),
				zap.String(logkeys.Actual, "stored metadata differs from payload"))
		}
		exists, err := h.ObjectExists(ctx, objectName)
		if err != nil {
			return err
		}
		if !exists {
			err = h.PutObjectBytes(ctx, objectName, data, contentType)
		} else {
			err = h.verifyObjectBytes(ctx, objectName, data)
		}
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		result := tx.Model(&common_persistence_models.Attachment{}).
			Where("id = ? AND internal_only = ? AND upload_status = ? AND final_object_name = ? AND deleted_at IS NULL", attachmentID, true, common_persistence_models.UploadStatusProcessing, objectName).
			Select("upload_status", "processing_end_time", "validation_details", "updated_at").
			Updates(struct {
				UploadStatus      common_persistence_models.UploadStatus
				ProcessingEndTime *time.Time
				ValidationDetails string
				UpdatedAt         time.Time `gorm:"autoUpdateTime:false"`
			}{
				UploadStatus:      common_persistence_models.UploadStatusVetted,
				ProcessingEndTime: &now,
				ValidationDetails: "validated internal server attachment",
				UpdatedAt:         now,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		published.UploadStatus = common_persistence_models.UploadStatusVetted
		published.ProcessingEndTime = &now
		published.ValidationDetails = "validated internal server attachment"
		return nil
	})
	if err != nil {
		return nil, databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "publish_internal_attachment_object"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyObjectName, objectName),
			zap.String(appLogger.KeyContentType, contentType))
	}
	return &published, nil
}

// verifyObjectBytes fails closed unless the private object holds exactly data.
func (h *CommonPersistenceHandler[U]) verifyObjectBytes(ctx context.Context, objectName string, data []byte) error {
	reader, err := h.GetObjectReader(ctx, objectName, ObjectPrecondition{})
	if err != nil {
		return err
	}
	defer reader.Close()
	stored, err := io.ReadAll(io.LimitReader(reader, int64(len(data))+1))
	if err != nil {
		return err
	}
	if !bytes.Equal(stored, data) {
		return appLogger.InvariantError("existing_internal_attachment_object_matches_payload", nil,
			zap.String(logkeys.Operation, "publish_internal_attachment_object"),
			zap.String(appLogger.KeyObjectName, objectName),
			zap.String(logkeys.Expected, "stored bytes equal the payload their content address names"),
			zap.String(logkeys.Actual, "stored bytes differ from payload"))
	}
	return nil
}

// attachmentObjectLockKey derives the transaction-scoped advisory-lock key
// serializing publication of a verified object. It was taken against post-delete
// reclamation, which no path runs now (TODO(object-refcounting) in
// attachment/lifecycle_manager.go); it is kept for when deletion returns.
// FinalObjectName keeps the same verified/[salted-hash] shape in both buckets,
// so the storage identity is (Visibility, FinalObjectName) and the two
// visibility classes must never contend on — or interfere through — one key.
// Private objects keep the bare object name so instances still running the
// pre-visibility keying hash to the same lock during a rolling upgrade; only
// public objects carry the "public:" prefix, which cannot collide with a bare
// name because object names always start with temp/ or verified/. Callers pass
// the already-classified visibility (IsPublicAttachmentVisibility) so the
// three-valued wire enum can never fragment a lock class.
func attachmentObjectLockKey(public bool, objectName string) string {
	if !public {
		return objectName
	}
	return "public:" + objectName
}

// PublishPublicAttachmentObject serializes publication of a deterministic
// verified object name under a transaction-scoped advisory lock (see
// attachmentObjectLockKey). The lock, the row
// predicates, and the object existence/copy targets are all scoped by the
// row's visibility: the private and public buckets are independent dedup
// namespaces, so a public publish must never observe or reuse a same-named
// private object (or vice versa).
func (h *CommonPersistenceHandler[U]) PublishPublicAttachmentObject(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	source ObjectPrecondition,
) error {
	if attachment == nil || attachment.ID == 0 || attachment.InternalOnly ||
		attachment.UploadStatus != common_persistence_models.UploadStatusVetted ||
		strings.TrimSpace(attachment.TemporaryObjectName) == "" || strings.TrimSpace(attachment.FinalObjectName) == "" {
		fields := []zap.Field{
			zap.String(logkeys.Operation, "publish_public_attachment_object"),
			zap.String(logkeys.Expected, "persisted vetted public attachment with temporary and final object names"),
			zap.String(logkeys.Actual, "attachment publication preconditions are not satisfied"),
		}
		if attachment != nil {
			fields = append(fields,
				zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
				zap.Stringer(logkeys.Status, attachment.UploadStatus),
				zap.Bool(appLogger.KeyInternalOnly, attachment.InternalOnly))
		}
		return appLogger.InvariantError("public_attachment_publication_preconditions_hold", ErrDatabaseError, fields...)
	}
	publicVisibility := common_persistence_models.IsPublicAttachmentVisibility(attachment.Visibility)
	if publicVisibility && !h.PublicStorageEnabled() {
		// Fail before any lock, row, or object write: publishing a public row
		// without a public bucket would vet a row whose object is unreachable.
		return fmt.Errorf("cannot publish attachment %d with public visibility: %w", attachment.ID, ErrPublicStorageDisabled)
	}

	err := h.getDBFromContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
			attachmentObjectLockKey(publicVisibility, attachment.FinalObjectName)).Error; err != nil {
			return err
		}

		// The visibility predicate keeps the acquired lock honest: if the row's
		// stored visibility ever diverged from the caller's snapshot, publishing
		// under the snapshot's lock key would target the wrong bucket.
		var current common_persistence_models.Attachment
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND internal_only = ? AND upload_status = ? AND temporary_object_name = ? AND visibility = ?",
				attachment.ID, false, common_persistence_models.UploadStatusProcessing, attachment.TemporaryObjectName, attachment.Visibility).
			First(&current).Error; err != nil {
			return err
		}
		// The row lock orders this after any re-parenting since the caller's snapshot was read, so
		// the callbacks that follow a publication act for the current owner.
		attachment.ObjectType, attachment.EntityID = current.ObjectType, current.EntityID

		if publicVisibility {
			exists, err := h.ObjectExistsInPublicBucket(ctx, attachment.FinalObjectName)
			if err != nil {
				return fmt.Errorf("%w: %w", errAttachmentPublicationStorage, err)
			}
			if !exists {
				if err := h.CopyObjectToPublicBucket(ctx, attachment.TemporaryObjectName, attachment.FinalObjectName, source); err != nil {
					// Multi-%w wrapping keeps ErrObjectPreconditionFailed reachable for
					// the passthrough after the transaction.
					return fmt.Errorf("%w: %w", errAttachmentPublicationStorage, err)
				}
			}
		} else {
			exists, err := h.ObjectExists(ctx, attachment.FinalObjectName)
			if err != nil {
				return fmt.Errorf("%w: %w", errAttachmentPublicationStorage, err)
			}
			if !exists {
				if err := h.CopyObject(ctx, attachment.TemporaryObjectName, attachment.FinalObjectName, source); err != nil {
					// Multi-%w wrapping keeps ErrObjectPreconditionFailed reachable for
					// the passthrough after the transaction.
					return fmt.Errorf("%w: %w", errAttachmentPublicationStorage, err)
				}
			}
		}

		result := tx.Model(&common_persistence_models.Attachment{}).
			Where("id = ? AND internal_only = ? AND upload_status = ? AND temporary_object_name = ? AND visibility = ? AND deleted_at IS NULL",
				attachment.ID, false, common_persistence_models.UploadStatusProcessing, attachment.TemporaryObjectName, attachment.Visibility).
			Select("size", "file_hash", "final_object_name", "upload_status", "processing_end_time", "validation_details").
			Updates(common_persistence_models.Attachment{
				Size:              attachment.Size,
				FileHash:          attachment.FileHash,
				FinalObjectName:   attachment.FinalObjectName,
				UploadStatus:      common_persistence_models.UploadStatusVetted,
				ProcessingEndTime: attachment.ProcessingEndTime,
				ValidationDetails: attachment.ValidationDetails,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
	if err != nil {
		// A stale source precondition is a distinct, caller-handled outcome: the
		// validated version is gone and the row must be rejected, not retried as
		// a database failure.
		if errors.Is(err, ErrObjectPreconditionFailed) {
			return err
		}
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "publish_public_attachment_object"),
			zap.Uint64(appLogger.KeyAttachmentID, attachment.ID),
			zap.String(appLogger.KeyObjectName, attachment.FinalObjectName),
			zap.Bool(appLogger.KeyPublicVisibility, publicVisibility))
	}
	return nil
}

// UpdateAttachmentEntityIDs executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdateAttachmentEntityIDs(ctx context.Context, attachmentIDs []uint64, entityID string) error {
	if len(attachmentIDs) == 0 {
		return nil // Nothing to update
	}

	db := h.getDBFromContext(ctx)

	result := db.Model(&common_persistence_models.Attachment{}).
		Where("id IN ? AND internal_only = ?", attachmentIDs, false).
		Update("entity_id", entityID)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_attachment_entity_ids"),
			zap.Int(appLogger.KeyCount, len(attachmentIDs)))
	}

	logger := appLogger.For(ctx, h.logger, loggerName)
	fields := []zap.Field{
		zap.String(logkeys.Operation, "update_attachment_entity_ids"),
		zap.Int(appLogger.KeyCount, len(attachmentIDs)),
		zap.Int64(appLogger.KeyRows, result.RowsAffected),
	}
	if result.RowsAffected != int64(len(attachmentIDs)) {
		appLogger.LogInvariantViolation(logger, "attachment_entity_update_matches_requested_rows", nil,
			append(fields,
				zap.Int(logkeys.Expected, len(attachmentIDs)),
				zap.Int64(logkeys.Actual, result.RowsAffected),
				zap.String(logkeys.Fallback, "retain_partial_update"))...)
	} else {
		logger.Info("Updated attachment entity IDs", fields...)
	}

	return nil
}

// GetAttachmentCountByEntity counts non-internal attachments for an object and entity,
// optionally restricted to one upload status.
func (h *CommonPersistenceHandler[U]) GetAttachmentCountByEntity(ctx context.Context, objectType, entityID string, status *common_persistence_models.UploadStatus) (int64, error) {
	db := h.getDBFromContext(ctx)

	query := attachmentEntityQuery(db.Model(&common_persistence_models.Attachment{}), objectType, entityID, status)

	var count int64
	result := query.Count(&count)

	if result.Error != nil {
		return 0, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_attachment_count_by_entity"),
			zap.String(appLogger.KeyObjectType, objectType),
			zap.String(appLogger.KeyEntityID, entityID))
	}

	return count, nil
}

// LinkAttachmentToEntity associates a non-internal attachment with an object and entity.
func (h *CommonPersistenceHandler[U]) LinkAttachmentToEntity(ctx context.Context, attachmentID uint64, objectType, entityID string) error {
	db := h.getDBFromContext(ctx)

	updates := common_persistence_models.Attachment{
		ObjectType: objectType,
		EntityID:   entityID,
	}

	result := db.Model(&common_persistence_models.Attachment{}).
		Where("id = ? AND internal_only = ?", attachmentID, false).
		Select("object_type", "entity_id").
		Updates(updates)

	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "link_attachment_to_entity"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
			zap.String(appLogger.KeyObjectType, objectType),
			zap.String(appLogger.KeyEntityID, entityID))
	}

	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Attachment not found while linking to entity",
				zap.String(logkeys.Operation, "link_attachment_to_entity"),
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID),
				zap.String(appLogger.KeyObjectType, objectType))
		}
		return fmt.Errorf("attachment not found: %w", ErrDatabaseError)
	}

	return nil
}

// UnlinkAttachmentFromEntity clears an attachment's entity association while
// preserving its object type and lifecycle metadata.
func (h *CommonPersistenceHandler[U]) UnlinkAttachmentFromEntity(ctx context.Context, attachmentID uint64) error {
	result := h.getDBFromContext(ctx).Model(&common_persistence_models.Attachment{}).
		Where("id = ? AND internal_only = ?", attachmentID, false).
		Update("entity_id", "")
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "unlink_attachment_from_entity"),
			zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
	}
	if result.RowsAffected == 0 {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("Attachment not found while unlinking from entity",
				zap.String(logkeys.Operation, "unlink_attachment_from_entity"),
				zap.Uint64(appLogger.KeyAttachmentID, attachmentID))
		}
		return fmt.Errorf("attachment not found: %w", ErrDatabaseError)
	}
	return nil
}
