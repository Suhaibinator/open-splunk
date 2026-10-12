package attachment

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// InternalStoreAdapter exposes AttachmentLifecycleManager's private blob operations through the
// narrow optional capability consumed by transcript writers. Keeping the port generic also lets
// tool-result media use the same vetted storage path.
type InternalStoreAdapter struct {
	manager *AttachmentLifecycleManager
}

// NewInternalStoreAdapter wraps a configured lifecycle manager.
func NewInternalStoreAdapter(manager *AttachmentLifecycleManager) *InternalStoreAdapter {
	return &InternalStoreAdapter{manager: manager}
}

func (a *InternalStoreAdapter) ValidateInternal(ctx context.Context, in ports.InternalAttachmentInput) error {
	if a == nil || a.manager == nil {
		return fmt.Errorf("internal attachment store is not configured")
	}
	return a.manager.ValidateInternalAttachment(ctx, InternalAttachmentInput{
		UserID: in.UserID, ObjectType: in.ObjectType, EntityID: in.EntityID, Filename: in.Filename,
		ContentType: in.ContentType, Data: in.Data, MaxBytes: in.MaxBytes,
	})
}

func (a *InternalStoreAdapter) StoreInternal(ctx context.Context, in ports.InternalAttachmentInput) (*ports.InternalAttachmentRef, error) {
	if a == nil || a.manager == nil {
		return nil, fmt.Errorf("internal attachment store is not configured")
	}
	attachment, created, err := a.manager.StoreInternalAttachmentWithStatus(ctx, InternalAttachmentInput{
		UserID:      in.UserID,
		ObjectType:  in.ObjectType,
		EntityID:    in.EntityID,
		Filename:    in.Filename,
		ContentType: in.ContentType,
		Data:        in.Data,
		MaxBytes:    in.MaxBytes,
	})
	if err != nil {
		return nil, err
	}
	ref := internalAttachmentRef(attachment)
	if ref == nil {
		return nil, fmt.Errorf("internal attachment store returned an empty record")
	}
	ref.Created = created
	return ref, nil
}

func (a *InternalStoreAdapter) ResolveInternal(ctx context.Context, attachmentID uint64) ([]byte, *ports.InternalAttachmentRef, error) {
	if a == nil || a.manager == nil {
		return nil, nil, fmt.Errorf("internal attachment store is not configured")
	}
	data, attachment, err := a.manager.ReadInternalAttachment(ctx, attachmentID)
	if err != nil {
		return nil, nil, err
	}
	return data, internalAttachmentRef(attachment), nil
}

func (a *InternalStoreAdapter) ResolveInternalBounded(ctx context.Context, attachmentID uint64, maxBytes uint64) ([]byte, *ports.InternalAttachmentRef, error) {
	if a == nil || a.manager == nil {
		return nil, nil, fmt.Errorf("internal attachment store is not configured")
	}
	data, attachment, err := a.manager.readInternalAttachmentBounded(ctx, attachmentID, maxBytes)
	if err != nil {
		return nil, nil, err
	}
	return data, internalAttachmentRef(attachment), nil
}

func (a *InternalStoreAdapter) DeleteInternal(ctx context.Context, attachmentID uint64) error {
	if a == nil || a.manager == nil || a.manager.persistence == nil {
		return fmt.Errorf("internal attachment store is not configured")
	}
	attachment, err := a.manager.persistence.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return err
	}
	if attachment == nil || !attachment.InternalOnly {
		return ErrInternalAttachmentAccess
	}
	tombstoned, err := a.manager.persistence.TombstoneInternalAttachmentIfUnreferenced(ctx, attachmentID, time.Now().UTC().Add(time.Nanosecond))
	if err != nil {
		return err
	}
	if !tombstoned {
		return fmt.Errorf("internal attachment is still referenced")
	}
	// Only the row is soft-deleted; the object is kept (TODO(object-refcounting) in lifecycle_manager.go).
	return nil
}

// ResolveInternalURL returns a short-lived privileged URL after the manager verifies the object's
// internal-only status, size, and digest. The metadata is returned so the caller can additionally
// enforce run/tool ownership before handing the URL to a model provider.
func (a *InternalStoreAdapter) ResolveInternalURL(ctx context.Context, attachmentID uint64, maxBytes uint64) (string, *ports.InternalAttachmentRef, error) {
	if a == nil || a.manager == nil {
		return "", nil, fmt.Errorf("internal attachment store is not configured")
	}
	// A presigned URL is handed directly to an external provider, so verify the
	// actual object body—not only mutable database metadata—before minting that
	// capability. The bounded read rejects replacement/corruption and enforces
	// maxBytes before the URL can escape.
	_, attachment, err := a.manager.readInternalAttachmentBounded(ctx, attachmentID, maxBytes)
	if err != nil {
		return "", nil, err
	}
	// 0 selects the hot-configured attachment_download_url_expiry default.
	url, err := a.manager.getInternalAttachmentPresignedURLForMetadata(ctx, attachment, 0)
	if err != nil {
		return "", nil, err
	}
	return url, internalAttachmentRef(attachment), nil
}

func internalAttachmentRef(attachment *common_persistence_models.Attachment) *ports.InternalAttachmentRef {
	if attachment == nil {
		return nil
	}
	return &ports.InternalAttachmentRef{
		AttachmentID:  attachment.ID,
		OwnerUserID:   attachment.UserID,
		ObjectType:    attachment.ObjectType,
		EntityID:      attachment.EntityID,
		Filename:      attachment.OriginalFileName,
		ContentType:   attachment.ContentType,
		ContentSHA256: attachment.FileHash,
		SizeBytes:     uint64(max(attachment.Size, 0)),
	}
}

var _ ports.InternalAttachmentStore = (*InternalStoreAdapter)(nil)
var _ ports.InternalAttachmentURLStore = (*InternalStoreAdapter)(nil)
var _ ports.InternalAttachmentBoundedStore = (*InternalStoreAdapter)(nil)
