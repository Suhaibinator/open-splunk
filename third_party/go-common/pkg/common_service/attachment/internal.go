package attachment

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

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

var (
	// ErrInternalAttachmentAccess is returned when a public attachment path is
	// asked to expose a server-owned agent/provider blob.
	ErrInternalAttachmentAccess = errors.New("internal attachment is not publicly accessible")
	// ErrInvalidInternalAttachment is returned for malformed or unsafe internal
	// attachment input.
	ErrInvalidInternalAttachment = errors.New("invalid internal attachment")
)

const internalAttachmentReconcileTimeout = 5 * time.Second

// InternalAttachmentInput describes bytes produced by trusted server code. The
// bytes still pass the registered size, MIME, and streaming validators before
// becoming a vetted attachment.
type InternalAttachmentInput struct {
	UserID      uint64
	ObjectType  string
	EntityID    string
	Filename    string
	ContentType string
	Data        []byte
	// MaxBytes optionally applies a tighter caller-specific ceiling (for example
	// AgentToolDefinition.MaxResultBytes). Zero uses the object-type/default cap.
	MaxBytes int64
}

// StoreInternalAttachment synchronously validates and stores trusted
// server-produced bytes. It creates a non-readable processing row before the
// object write, then publishes the row as vetted only after storage succeeds.
func (m *AttachmentLifecycleManager) StoreInternalAttachment(
	ctx context.Context,
	in InternalAttachmentInput,
) (*common_persistence_models.Attachment, error) {
	attachment, _, err := m.StoreInternalAttachmentWithStatus(ctx, in)
	return attachment, err
}

// StoreInternalAttachmentWithStatus is StoreInternalAttachment with an idempotency signal. A
// prior attachment bound to the same server-defined entity key is reused only when all immutable
// metadata and verified bytes match; a differing retry fails closed.
func (m *AttachmentLifecycleManager) StoreInternalAttachmentWithStatus(
	ctx context.Context,
	in InternalAttachmentInput,
) (*common_persistence_models.Attachment, bool, error) {
	if err := m.ValidateInternalAttachment(ctx, in); err != nil {
		return nil, false, err
	}
	if m == nil || m.persistence == nil || m.objectTypeRegistry == nil {
		return nil, false, fmt.Errorf("%w: attachment lifecycle manager is not configured", ErrInvalidInternalAttachment)
	}
	entityID := strings.TrimSpace(in.EntityID)
	contentType := strings.TrimSpace(strings.SplitN(in.ContentType, ";", 2)[0])
	filename := FilenameSanitizer(strings.TrimSpace(in.Filename))
	if filename == "" {
		filename = DefaultFilename
	}
	plainDigest := sha256.Sum256(in.Data)
	plainDigestHex := hex.EncodeToString(plainDigest[:])
	existing, err := m.persistence.GetInternalAttachmentByEntity(ctx, in.ObjectType, entityID)
	if err != nil {
		return nil, false, fmt.Errorf("lookup internal attachment: %w", err)
	}
	if existing != nil {
		if !internalAttachmentMatchesInput(existing, in, entityID, filename, contentType, plainDigestHex) {
			return nil, false, fmt.Errorf("%w: entity key is already bound to different content", ErrInvalidInternalAttachment)
		}
		if existing.UploadStatus == common_persistence_models.UploadStatusVetted {
			return m.verifyReusableInternalAttachment(ctx, existing, in.Data)
		}
		if existing.UploadStatus != common_persistence_models.UploadStatusProcessing || strings.TrimSpace(existing.FinalObjectName) == "" {
			return nil, false, fmt.Errorf("%w: entity key is bound to an unavailable attachment", ErrInvalidInternalAttachment)
		}
	}

	created := false
	attachment := existing
	if attachment == nil {
		// Content-addressed exactly like uploads (verified/<salted-hash>): identical bytes share one
		// private-bucket object across internal rows, private uploads and fork clones.
		salted := sha256.New()
		_, _ = salted.Write([]byte(m.objectHashSalt))
		_, _ = salted.Write(in.Data)
		objectName := VerifiedPathPrefix + hex.EncodeToString(salted.Sum(nil))
		now := time.Now().UTC()
		// Visibility is deliberately left at its zero value (private): internal rows
		// are server-only and must never be stored in the public bucket.
		attachment = &common_persistence_models.Attachment{
			UserID:              in.UserID,
			OriginalFileName:    filename,
			ContentType:         contentType,
			Size:                int64(len(in.Data)),
			FinalObjectName:     objectName,
			FileHash:            plainDigestHex,
			ObjectType:          in.ObjectType,
			InternalOnly:        true,
			EntityID:            entityID,
			UploadStatus:        common_persistence_models.UploadStatusProcessing,
			UploadTime:          now,
			ProcessingStartTime: &now,
			ValidationDetails:   "validated internal server attachment; object write pending",
		}
		if createErr := m.persistence.CreateInternalAttachment(ctx, attachment); createErr != nil {
			// No object has been written yet. A timeout or connection loss is therefore safe to
			// reconcile without cleanup: either our processing row committed, a concurrent row
			// won the unique key, or this call returns while leaving no storage orphan.
			reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), internalAttachmentReconcileTimeout)
			defer cancel()
			winner, lookupErr := m.persistence.GetInternalAttachmentByEntity(reconcileCtx, in.ObjectType, entityID)
			if lookupErr != nil {
				return nil, false, errors.Join(fmt.Errorf("create internal attachment: %w", createErr), fmt.Errorf("reconcile internal attachment: %w", lookupErr))
			}
			if winner == nil {
				return nil, false, fmt.Errorf("create internal attachment: %w", createErr)
			}
			if !internalAttachmentMatchesInput(winner, in, entityID, filename, contentType, plainDigestHex) {
				return nil, false, errors.Join(fmt.Errorf("create internal attachment: %w", createErr), fmt.Errorf("%w: entity key winner does not match content", ErrInvalidInternalAttachment))
			}
			if winner.UploadStatus == common_persistence_models.UploadStatusVetted {
				return m.verifyReusableInternalAttachment(reconcileCtx, winner, in.Data)
			}
			if winner.UploadStatus != common_persistence_models.UploadStatusProcessing || strings.TrimSpace(winner.FinalObjectName) == "" {
				return nil, false, errors.Join(fmt.Errorf("create internal attachment: %w", createErr), fmt.Errorf("%w: entity key winner is unavailable", ErrInvalidInternalAttachment))
			}
			// The object name no longer identifies the writer, so only a returned ID proves the
			// winner is this call's row; an unproven row stays uncreated and is left to the reaper.
			created = attachment.ID != 0 && winner.ID == attachment.ID
			attachment = winner
		} else {
			created = true
		}
	}

	published, publishErr := m.persistence.PublishInternalAttachmentObject(
		ctx, attachment.ID, attachment.FinalObjectName, in.Data, contentType,
	)
	if publishErr != nil {
		// Publication can commit despite a caller-visible error. Never delete the object: a
		// late-visible committed row would then dangle. The persistence method holds the same row
		// lock as reclamation across object write + conditional publication, so an unsuccessful
		// attempt always leaves a processing row/object pair for retry or reaping.
		reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), internalAttachmentReconcileTimeout)
		defer cancel()
		winner, lookupErr := m.persistence.GetInternalAttachmentByEntity(reconcileCtx, in.ObjectType, entityID)
		if lookupErr != nil {
			return nil, false, errors.Join(fmt.Errorf("publish internal attachment: %w", publishErr), fmt.Errorf("reconcile internal attachment: %w", lookupErr))
		}
		if winner != nil && winner.UploadStatus == common_persistence_models.UploadStatusVetted &&
			winner.FinalObjectName == attachment.FinalObjectName &&
			internalAttachmentMatchesInput(winner, in, entityID, filename, contentType, plainDigestHex) {
			verified, _, verifyErr := m.verifyReusableInternalAttachment(reconcileCtx, winner, in.Data)
			return verified, created, verifyErr
		}
		return nil, false, fmt.Errorf("publish internal attachment: %w", publishErr)
	}
	return published, created, nil
}

func internalAttachmentMatchesInput(
	attachment *common_persistence_models.Attachment,
	in InternalAttachmentInput,
	entityID, filename, contentType, digest string,
) bool {
	return attachment != nil && attachment.ID != 0 && attachment.InternalOnly &&
		attachment.UserID == in.UserID && attachment.ObjectType == in.ObjectType &&
		attachment.EntityID == entityID && attachment.OriginalFileName == filename &&
		attachment.ContentType == contentType && attachment.Size == int64(len(in.Data)) &&
		strings.EqualFold(attachment.FileHash, digest)
}

func (m *AttachmentLifecycleManager) verifyReusableInternalAttachment(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	expected []byte,
) (*common_persistence_models.Attachment, bool, error) {
	stored, verified, err := m.ReadInternalAttachment(ctx, attachment.ID)
	if err != nil || verified == nil || !bytes.Equal(stored, expected) {
		if err == nil {
			err = ErrInternalAttachmentAccess
		}
		return nil, false, fmt.Errorf("verify existing internal attachment: %w", err)
	}
	return verified, false, nil
}

// ValidateInternalAttachment applies the complete non-mutating internal attachment policy.
func (m *AttachmentLifecycleManager) ValidateInternalAttachment(ctx context.Context, in InternalAttachmentInput) error {
	if m == nil || m.persistence == nil || m.objectTypeRegistry == nil {
		return fmt.Errorf("%w: attachment lifecycle manager is not configured", ErrInvalidInternalAttachment)
	}
	if in.ObjectType != ObjectTypeAgentProviderState && in.ObjectType != ObjectTypeAgentToolMedia {
		return fmt.Errorf("%w: unsupported internal object type %q", ErrInvalidInternalAttachment, in.ObjectType)
	}
	entityID := strings.TrimSpace(in.EntityID)
	if entityID == "" {
		return fmt.Errorf("%w: entity ID is required", ErrInvalidInternalAttachment)
	}
	if len(in.Data) == 0 {
		return fmt.Errorf("%w: content is empty", ErrInvalidInternalAttachment)
	}
	contentType := strings.TrimSpace(strings.SplitN(in.ContentType, ";", 2)[0])
	if contentType == "" {
		return fmt.Errorf("%w: content type is required", ErrInvalidInternalAttachment)
	}

	typeConfig := m.objectTypeRegistry.Get(in.ObjectType)
	if typeConfig == nil {
		return fmt.Errorf("%w: object type %q is not registered", ErrInvalidInternalAttachment, in.ObjectType)
	}
	view := m.runtimeConfigProvider()
	maxBytes := typeConfig.MaxSize
	if limit := view.AttachmentDefaultMaxSize(); limit > 0 && (maxBytes <= 0 || limit < maxBytes) {
		maxBytes = limit
	}
	if in.MaxBytes > 0 && (maxBytes <= 0 || in.MaxBytes < maxBytes) {
		maxBytes = in.MaxBytes
	}
	if maxBytes > 0 && int64(len(in.Data)) > maxBytes {
		return fmt.Errorf("%w: content size %d exceeds limit %d", ErrInvalidInternalAttachment, len(in.Data), maxBytes)
	}

	for _, validator := range objectTypeValidators(typeConfig, view.AttachmentXMLMaxTokenBytes(), nil) {
		if validator == nil {
			continue
		}
		reader := bytes.NewReader(in.Data)
		ok, details := validator.Validate(ctx, reader, int64(len(in.Data)), contentType)
		if !ok {
			return fmt.Errorf("%w: %s: %s", ErrInvalidInternalAttachment, validator.Name(), details)
		}
	}
	return nil
}

// ReadInternalAttachment returns verified server-only bytes and validates the
// persisted size and SHA-256 digest before handing them to a provider adapter.
func (m *AttachmentLifecycleManager) ReadInternalAttachment(
	ctx context.Context,
	attachmentID uint64,
) ([]byte, *common_persistence_models.Attachment, error) {
	return m.readInternalAttachmentBounded(ctx, attachmentID, ^uint64(0))
}

func (m *AttachmentLifecycleManager) readInternalAttachmentBounded(
	ctx context.Context,
	attachmentID uint64,
	maxBytes uint64,
) ([]byte, *common_persistence_models.Attachment, error) {
	attachment, err := m.getInternalAttachmentMetadata(ctx, attachmentID, maxBytes)
	if err != nil {
		return nil, nil, err
	}
	reader, err := m.persistence.GetObjectReader(ctx, attachment.FinalObjectName, common_persistence.ObjectPrecondition{})
	if err != nil {
		return nil, nil, fmt.Errorf("read internal attachment object: %w", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, attachment.Size+1))
	if err != nil {
		return nil, nil, fmt.Errorf("read internal attachment object: %w", err)
	}
	if uint64(len(data)) > maxBytes || int64(len(data)) > attachment.Size {
		return nil, nil, fmt.Errorf("%w: object exceeds remaining byte budget", ErrInternalAttachmentAccess)
	}
	if int64(len(data)) != attachment.Size {
		return nil, nil, fmt.Errorf("%w: stored size mismatch", ErrInternalAttachmentAccess)
	}
	digest := sha256.Sum256(data)
	if !strings.EqualFold(hex.EncodeToString(digest[:]), strings.TrimSpace(attachment.FileHash)) {
		return nil, nil, fmt.Errorf("%w: stored digest mismatch", ErrInternalAttachmentAccess)
	}
	return data, attachment, nil
}

func (m *AttachmentLifecycleManager) getInternalAttachmentMetadata(
	ctx context.Context,
	attachmentID uint64,
	maxBytes uint64,
) (*common_persistence_models.Attachment, error) {
	if m == nil || m.persistence == nil {
		return nil, fmt.Errorf("%w: attachment lifecycle manager is not configured", ErrInternalAttachmentAccess)
	}
	attachment, err := m.persistence.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		return nil, err
	}
	if attachment == nil || !attachment.InternalOnly ||
		(attachment.ObjectType != ObjectTypeAgentProviderState && attachment.ObjectType != ObjectTypeAgentToolMedia) {
		return nil, ErrInternalAttachmentAccess
	}
	if attachment.UploadStatus != common_persistence_models.UploadStatusVetted || strings.TrimSpace(attachment.FinalObjectName) == "" {
		return nil, fmt.Errorf("%w: attachment is not vetted", ErrInternalAttachmentAccess)
	}
	if attachment.Size <= 0 || uint64(attachment.Size) > maxBytes {
		return nil, fmt.Errorf("%w: attachment exceeds remaining byte budget", ErrInternalAttachmentAccess)
	}
	return attachment, nil
}

func (m *AttachmentLifecycleManager) getInternalAttachmentPresignedURLForMetadata(
	ctx context.Context,
	attachment *common_persistence_models.Attachment,
	expiry time.Duration,
) (string, error) {
	if attachment == nil || !attachment.InternalOnly || strings.TrimSpace(attachment.FinalObjectName) == "" {
		return "", ErrInternalAttachmentAccess
	}
	// Internal attachments live in the private bucket, so their URLs go through
	// the same visibility-aware private dispatch as user downloads: host-scoped
	// offline SigV4 when attachment_presign_host is configured (an edge verifier
	// would reject a host-rewritten SDK presign), SDK presign + rewrite
	// otherwise. An expiry <= 0 selects the hot-configured download default.
	view := m.runtimeConfigProvider()
	effectiveExpiry := expiry
	if effectiveExpiry <= 0 {
		effectiveExpiry = view.AttachmentDownloadURLExpiry()
	}
	return m.presignPrivateAttachmentURL(ctx, attachment, effectiveExpiry, view)
}
