package attachment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// ObjectReaderStore opens private-bucket objects, optionally pinned to an ETag.
// Implementations must honor ctx during opening and reads.
type ObjectReaderStore interface {
	GetObjectReader(ctx context.Context, objectName string, precondition common_persistence.ObjectPrecondition) (io.ReadCloser, error)
}

// ObjectReadOptions bounds allocation and optionally pins the object version.
// MaxBytes must be positive and less than math.MaxInt64.
type ObjectReadOptions struct {
	MaxBytes     int64
	Precondition common_persistence.ObjectPrecondition
}

// ErrObjectTooLarge reports that a stored object exceeded the caller's byte bound.
// Callers wrap it in whatever domain error their surface requires.
var ErrObjectTooLarge = errors.New("stored object exceeds maximum read size")

// AttachmentReadinessState describes a user-upload lifecycle state that can
// prevent a final, vetted read. It deliberately excludes scope, visibility,
// and object-path failures: those are authorization or data invariants and
// must not be presented as a retryable upload outcome by every caller.
type AttachmentReadinessState uint8

const (
	AttachmentReadinessUploadIncomplete AttachmentReadinessState = iota + 1
	AttachmentReadinessProcessing
	AttachmentReadinessRejected
	AttachmentReadinessMissing
)

// AttachmentReadinessError lets interactive upload-completion boundaries turn
// expected lifecycle states into actionable public responses. Persisted
// artifact readers may leave it unclassified, preserving their invariant
// semantics.
type AttachmentReadinessError struct {
	State AttachmentReadinessState
}

func (e *AttachmentReadinessError) Error() string {
	switch e.State {
	case AttachmentReadinessUploadIncomplete:
		return "attachment upload has not been completed"
	case AttachmentReadinessProcessing:
		return "attachment upload is still processing"
	case AttachmentReadinessRejected:
		return "attachment upload was rejected"
	case AttachmentReadinessMissing:
		return "attachment upload is missing"
	default:
		return "attachment upload is not ready"
	}
}

// AttachmentObjectStore is the storage surface needed to read the bytes behind an
// attachment ID: the attachment row locates the object, the object store serves it.
type AttachmentObjectStore interface {
	ObjectReaderStore
	GetAttachmentByID(ctx context.Context, attachmentID uint64) (*common_persistence_models.Attachment, error)
}

// AttachmentObjectTarget locates one attachment's stored payload.
type AttachmentObjectTarget struct {
	ObjectName  string
	ContentType string
}

// AttachmentObjectReadMode selects the only two lifecycle states in which a
// worker may read an attachment object directly. The zero value is invalid so a
// new call site cannot accidentally inherit a permissive fallback.
type AttachmentObjectReadMode uint8

const (
	AttachmentObjectReadModeFinalVetted AttachmentObjectReadMode = iota + 1
	AttachmentObjectReadModePendingTemporary
)

// AttachmentObjectExpectation binds an attachment ID supplied by a domain row
// back to the object type and entity that the caller already authorized. More
// than one entity may be listed only when the same durable artifact is
// deliberately shared across multiple authorized entities.
type AttachmentObjectExpectation struct {
	ObjectType string
	EntityIDs  []string
	ReadMode   AttachmentObjectReadMode
}

// AttachmentBatchStore looks up several attachment rows at once.
type AttachmentBatchStore interface {
	GetAttachmentsByIDs(ctx context.Context, attachmentIDs []uint64) ([]*common_persistence_models.Attachment, error)
}

// ResolveAttachmentObjectTargets maps attachment IDs to the stored objects backing
// them, so callers can read page bytes without minting download URLs.
//
// Parameters:
//   - ctx: request context for the attachment lookup.
//   - store: batch attachment lookup surface.
//   - expectations: attachment IDs mapped to the exact domain scope and lifecycle state the caller expects.
//
// Returns:
//   - map[uint64]AttachmentObjectTarget: object key and content type for every expected attachment.
//   - error: non-nil when the lookup fails.
func ResolveAttachmentObjectTargets(
	ctx context.Context,
	store AttachmentBatchStore,
	expectations map[uint64]AttachmentObjectExpectation,
) (map[uint64]AttachmentObjectTarget, error) {
	targets := make(map[uint64]AttachmentObjectTarget, len(expectations))
	if store == nil {
		return nil, fmt.Errorf("attachment batch store is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(expectations) == 0 {
		return targets, nil
	}
	attachmentIDs := make([]uint64, 0, len(expectations))
	for attachmentID, expectation := range expectations {
		if attachmentID == 0 {
			return nil, fmt.Errorf("attachment_id is required")
		}
		if err := validateAttachmentObjectExpectation(expectation); err != nil {
			return nil, fmt.Errorf("attachment %d expectation: %w", attachmentID, err)
		}
		attachmentIDs = append(attachmentIDs, attachmentID)
	}
	slices.Sort(attachmentIDs)

	attachments, err := store.GetAttachmentsByIDs(ctx, attachmentIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve attachment storage objects: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, attachment := range attachments {
		if attachment == nil {
			return nil, fmt.Errorf("attachment lookup returned a nil row")
		}
		expectation, expected := expectations[attachment.ID]
		if !expected {
			return nil, fmt.Errorf("attachment lookup returned unexpected attachment %d", attachment.ID)
		}
		if _, duplicate := targets[attachment.ID]; duplicate {
			return nil, fmt.Errorf("attachment lookup returned duplicate attachment %d", attachment.ID)
		}
		target, resolveErr := resolveValidatedAttachmentObjectTarget(attachment, expectation)
		if resolveErr != nil {
			return nil, fmt.Errorf("attachment %d is not readable: %w", attachment.ID, resolveErr)
		}
		targets[attachment.ID] = target
	}
	if len(targets) != len(expectations) {
		for attachmentID := range expectations {
			if _, found := targets[attachmentID]; !found {
				return nil, fmt.Errorf("attachment lookup did not return attachment %d: %w", attachmentID, &AttachmentReadinessError{State: AttachmentReadinessMissing})
			}
		}
	}
	return targets, nil
}

// ResolveExpectedAttachmentObjectTarget validates an already-authorized row binding.
// It does not authorize users. Expectations must come from an authorized application
// scope, never from model-supplied object keys. Internal tool media must use the
// dedicated ownership-aware media resolver instead.
func ResolveExpectedAttachmentObjectTarget(
	attachment *common_persistence_models.Attachment,
	expectation AttachmentObjectExpectation,
) (AttachmentObjectTarget, error) {
	if attachment == nil {
		return AttachmentObjectTarget{}, &AttachmentReadinessError{State: AttachmentReadinessMissing}
	}
	if err := validateAttachmentObjectExpectation(expectation); err != nil {
		return AttachmentObjectTarget{}, err
	}
	return resolveValidatedAttachmentObjectTarget(attachment, expectation)
}

// resolveValidatedAttachmentObjectTarget checks the stored binding after the
// public entrypoint has validated its expectation.
func resolveValidatedAttachmentObjectTarget(attachment *common_persistence_models.Attachment, expectation AttachmentObjectExpectation) (AttachmentObjectTarget, error) {
	if attachment == nil {
		return AttachmentObjectTarget{}, &AttachmentReadinessError{State: AttachmentReadinessMissing}
	}
	if attachment.ID == 0 {
		return AttachmentObjectTarget{}, fmt.Errorf("attachment_id is required")
	}
	if attachment.DeletedAt.Valid {
		return AttachmentObjectTarget{}, fmt.Errorf("attachment is deleted: %w", &AttachmentReadinessError{State: AttachmentReadinessMissing})
	}
	if attachment.InternalOnly {
		return AttachmentObjectTarget{}, fmt.Errorf("internal-only attachment cannot be read through an application artifact boundary")
	}
	if attachment.ObjectType != expectation.ObjectType {
		return AttachmentObjectTarget{}, fmt.Errorf("object type %q does not match expected %q", attachment.ObjectType, expectation.ObjectType)
	}
	if !slices.Contains(expectation.EntityIDs, attachment.EntityID) {
		return AttachmentObjectTarget{}, fmt.Errorf("entity %q is outside the expected attachment scope", attachment.EntityID)
	}
	if common_persistence_models.IsPublicAttachmentVisibility(attachment.Visibility) {
		return AttachmentObjectTarget{}, fmt.Errorf("visibility %s is not private", attachment.Visibility.String())
	}

	var objectName string
	switch expectation.ReadMode {
	case AttachmentObjectReadModeFinalVetted:
		if attachment.UploadStatus != common_persistence_models.UploadStatusVetted {
			switch attachment.UploadStatus {
			case common_persistence_models.UploadStatusPendingUpload:
				return AttachmentObjectTarget{}, &AttachmentReadinessError{State: AttachmentReadinessUploadIncomplete}
			case common_persistence_models.UploadStatusPendingProcessing,
				common_persistence_models.UploadStatusProcessing:
				return AttachmentObjectTarget{}, &AttachmentReadinessError{State: AttachmentReadinessProcessing}
			case common_persistence_models.UploadStatusRejected:
				return AttachmentObjectTarget{}, &AttachmentReadinessError{State: AttachmentReadinessRejected}
			}
			return AttachmentObjectTarget{}, fmt.Errorf("upload status %s is not vetted", attachment.UploadStatus)
		}
		objectName = attachment.FinalObjectName
		if !isValidLifecycleObjectName(objectName, "verified/") {
			return AttachmentObjectTarget{}, fmt.Errorf("final object path is invalid")
		}
	case AttachmentObjectReadModePendingTemporary:
		if attachment.UploadStatus != common_persistence_models.UploadStatusPendingProcessing &&
			attachment.UploadStatus != common_persistence_models.UploadStatusProcessing {
			return AttachmentObjectTarget{}, fmt.Errorf("upload status %s is not pending temporary processing", attachment.UploadStatus)
		}
		objectName = attachment.TemporaryObjectName
		if !isValidLifecycleObjectName(objectName, "temp/") {
			return AttachmentObjectTarget{}, fmt.Errorf("temporary object path is invalid")
		}
	default:
		return AttachmentObjectTarget{}, fmt.Errorf("attachment read mode %d is invalid", expectation.ReadMode)
	}

	return AttachmentObjectTarget{
		ObjectName:  objectName,
		ContentType: strings.TrimSpace(attachment.ContentType),
	}, nil
}

func validateAttachmentObjectExpectation(expectation AttachmentObjectExpectation) error {
	if expectation.ObjectType == "" || expectation.ObjectType != strings.TrimSpace(expectation.ObjectType) {
		return fmt.Errorf("expected object type is required and must be canonical")
	}
	if len(expectation.EntityIDs) == 0 {
		return fmt.Errorf("at least one expected entity_id is required")
	}
	seen := make(map[string]struct{}, len(expectation.EntityIDs))
	for _, entityID := range expectation.EntityIDs {
		if entityID == "" || entityID != strings.TrimSpace(entityID) {
			return fmt.Errorf("expected entity_id is required and must be canonical")
		}
		if _, duplicate := seen[entityID]; duplicate {
			return fmt.Errorf("expected entity_id %q is duplicated", entityID)
		}
		seen[entityID] = struct{}{}
	}
	if expectation.ReadMode != AttachmentObjectReadModeFinalVetted &&
		expectation.ReadMode != AttachmentObjectReadModePendingTemporary {
		return fmt.Errorf("attachment read mode %d is invalid", expectation.ReadMode)
	}
	return nil
}

func isValidLifecycleObjectName(objectName, requiredPrefix string) bool {
	if objectName == "" || objectName != strings.TrimSpace(objectName) || !utf8.ValidString(objectName) {
		return false
	}
	if !strings.HasPrefix(objectName, requiredPrefix) || objectName == requiredPrefix {
		return false
	}
	if path.Clean(objectName) != objectName || strings.Contains(objectName, `\`) {
		return false
	}
	return strings.IndexFunc(objectName, func(r rune) bool { return r < 0x20 || r == 0x7f }) == -1
}

// ReadObjectBytes reads at most MaxBytes+1 bytes and returns no partial payload
// on failure. The private object name must come from an authorized scope. A zero
// precondition is unconditional; a supplied ETag is forwarded unchanged.
func ReadObjectBytes(ctx context.Context, store ObjectReaderStore, objectName string, options ObjectReadOptions) (data []byte, err error) {
	if store == nil {
		return nil, fmt.Errorf("object store is required")
	}
	if objectName == "" {
		return nil, fmt.Errorf("object name is required")
	}
	if err := validateObjectReadOptions(options); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, err := store.GetObjectReader(ctx, objectName, options.Precondition)
	if err != nil {
		if reader != nil {
			_ = reader.Close()
		}
		return nil, fmt.Errorf("failed to open object %q: %w", objectName, err)
	}
	if reader == nil {
		return nil, fmt.Errorf("object store returned a nil reader")
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close object %q: %w", objectName, closeErr))
			data = nil
		}
	}()
	data, err = io.ReadAll(io.LimitReader(contextObjectReader{ctx, reader}, options.MaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read object %q: %w", objectName, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if int64(len(data)) > options.MaxBytes {
		return nil, fmt.Errorf("object %q: %w of %d bytes", objectName, ErrObjectTooLarge, options.MaxBytes)
	}
	return data, nil
}

func validateObjectReadOptions(options ObjectReadOptions) error {
	if options.MaxBytes <= 0 || options.MaxBytes == math.MaxInt64 {
		return fmt.Errorf("object read byte cap must be positive and less than MaxInt64")
	}
	return nil
}

type contextObjectReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextObjectReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}

// ReadAttachmentBytes validates an attachment's already-authorized binding,
// lifecycle and private path before reading its bytes. It returns the recorded
// content type, which image callers should validate separately with mediaimage.
// No partial bytes or content type are returned on failure.
func ReadAttachmentBytes(ctx context.Context, store AttachmentObjectStore, attachmentID uint64, expectation AttachmentObjectExpectation, options ObjectReadOptions) ([]byte, string, error) {
	if store == nil {
		return nil, "", fmt.Errorf("attachment object store is required")
	}
	if attachmentID == 0 {
		return nil, "", fmt.Errorf("attachment_id is required")
	}
	if err := validateAttachmentObjectExpectation(expectation); err != nil {
		return nil, "", err
	}
	if err := validateObjectReadOptions(options); err != nil {
		return nil, "", err
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	attachment, err := store.GetAttachmentByID(ctx, attachmentID)
	if err != nil {
		if errors.Is(err, common_persistence.ErrAttachmentNotFound) {
			err = errors.Join(err, &AttachmentReadinessError{State: AttachmentReadinessMissing})
		}
		return nil, "", fmt.Errorf("failed to load attachment %d: %w", attachmentID, err)
	}
	if attachment == nil {
		return nil, "", &AttachmentReadinessError{State: AttachmentReadinessMissing}
	}
	if attachment.ID != attachmentID {
		return nil, "", fmt.Errorf("attachment lookup returned a mismatched row for attachment %d", attachmentID)
	}
	target, err := resolveValidatedAttachmentObjectTarget(attachment, expectation)
	if err != nil {
		return nil, "", fmt.Errorf("attachment %d is not readable: %w", attachmentID, err)
	}
	data, err := ReadObjectBytes(ctx, store, target.ObjectName, options)
	if err != nil {
		return nil, "", err
	}
	return data, target.ContentType, nil
}
