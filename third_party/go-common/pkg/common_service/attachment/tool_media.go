package attachment

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"mime"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
)

const maxToolMediaAttachmentBytes = int64(ports.MaxInternalAttachmentHydrationBytes)

const previewAttachmentID = "18446744073709551615"

// ToolMediaEntityID is the deterministic ownership key for one multipart tool result. The call id
// is hashed so the persisted entity key remains bounded and delimiter-safe.
func ToolMediaEntityID(agentRunID uint64, toolCallID string, partIndex int) string {
	callDigest := sha256.Sum256([]byte(toolCallID))
	return fmt.Sprintf("run:%d:call:%s:part:%d", agentRunID, hex.EncodeToString(callDigest[:]), partIndex)
}

// ClaimedToolMediaEntityID scopes a claimed execution's pre-commit media to its fencing token.
// A stale worker may externalize before discovering it lost the lease; using an attempt-scoped key
// prevents its bytes from occupying the durable run/call/part key and poisoning the current owner.
func ClaimedToolMediaEntityID(agentRunID uint64, toolCallID, leaseOwner string, partIndex int) string {
	callDigest := sha256.Sum256([]byte(toolCallID))
	leaseDigest := sha256.Sum256([]byte(leaseOwner))
	return fmt.Sprintf("run:%d:call:%s:claim:%s:part:%d", agentRunID, hex.EncodeToString(callDigest[:]), hex.EncodeToString(leaseDigest[:]), partIndex)
}

type ToolMediaExternalizeInput struct {
	OwnerUserID uint64
	AgentRunID  uint64
	ToolCallID  string
	// LeaseOwner, when non-empty, binds newly externalized/ref-resolved media to one claimed
	// execution attempt. Persistence validates the same binding during the fenced result commit.
	LeaseOwner string
	Parts      []*proto_models.MessagePart
}

func toolMediaEntityID(in ToolMediaExternalizeInput, partIndex int) string {
	if in.LeaseOwner != "" {
		return ClaimedToolMediaEntityID(in.AgentRunID, in.ToolCallID, in.LeaseOwner, partIndex)
	}
	return ToolMediaEntityID(in.AgentRunID, in.ToolCallID, partIndex)
}

type ToolMediaExternalizeResult struct {
	Parts         []*proto_models.MessagePart
	AttachmentIDs []uint64
}

// PreviewExternalizedToolResultMedia validates every media part without touching storage and
// returns the largest reference-form descriptor StoreInternal can produce. Callers must validate
// this preview before externalization so a later descriptor rejection cannot strand an object.
func PreviewExternalizedToolResultMedia(in ToolMediaExternalizeInput) ([]*proto_models.MessagePart, error) {
	if err := ports.ValidateToolResultContentPartCount(len(in.Parts)); err != nil {
		return nil, err
	}
	parts := cloneMessageParts(in.Parts)
	var attachmentBytes uint64
	for partIndex, part := range parts {
		if part == nil || part.GetMedia() == nil {
			continue
		}
		mediaPart := part.GetMedia()
		contentType := canonicalToolMediaType(mediaPart.GetContentType())
		if contentType == "" {
			return nil, fmt.Errorf("%w: tool media part %d has invalid content type", ErrInvalidInternalAttachment, partIndex)
		}
		mediaPart.ContentType = contentType
		isInputFile := !strings.HasPrefix(contentType, "image/")
		switch source := mediaPart.GetSource().(type) {
		case *proto_models.MediaPart_InlineData:
			if len(source.InlineData) == 0 || int64(len(source.InlineData)) > maxToolMediaAttachmentBytes {
				return nil, fmt.Errorf("%w: inline tool media part %d has invalid size", ErrInvalidInternalAttachment, partIndex)
			}
			mediaPart.Name = FilenameSanitizer(toolMediaFilename(mediaPart.GetName(), contentType, partIndex))
			mediaPart.SizeBytes = uint64(len(source.InlineData))
			mediaPart.Source = &proto_models.MediaPart_AttachmentId{AttachmentId: previewAttachmentID}
		case *proto_models.MediaPart_AttachmentId:
			attachmentID, err := strconv.ParseUint(strings.TrimSpace(source.AttachmentId), 10, 64)
			if err != nil || attachmentID == 0 || mediaPart.GetSizeBytes() == 0 {
				return nil, fmt.Errorf("%w: invalid tool media attachment reference", ErrInvalidInternalAttachment)
			}
		case *proto_models.MediaPart_Url:
			validated, err := validateToolMediaExternalSource(source.Url)
			if err != nil || mediaPart.GetSizeBytes() != 0 {
				return nil, fmt.Errorf("%w: invalid tool media external source", ErrInvalidInternalAttachment)
			}
			source.Url = validated
		default:
			return nil, fmt.Errorf("%w: tool media part %d has no source", ErrInvalidInternalAttachment, partIndex)
		}
		if mediaPart.GetAttachmentId() != "" {
			if isInputFile && mediaPart.GetSizeBytes() >= ports.MaxInternalAttachmentHydrationBytes {
				return nil, fmt.Errorf("%w: non-image tool media part %d must be smaller than %d bytes", ErrInvalidInternalAttachment, partIndex, ports.MaxInternalAttachmentHydrationBytes)
			}
			var err error
			attachmentBytes, err = addToolMediaBytes(attachmentBytes, mediaPart.GetSizeBytes())
			if err != nil || attachmentBytes > ports.MaxInternalAttachmentHydrationBytes {
				return nil, fmt.Errorf("%w: combined tool media exceeds %d bytes", ErrInvalidInternalAttachment, ports.MaxInternalAttachmentHydrationBytes)
			}
		}
	}
	return parts, nil
}

// ExternalizeToolResultMedia clones a multipart result and replaces every inline media blob with
// a private, synchronously-vetted attachment reference. Existing private references are verified.
func ExternalizeToolResultMedia(
	ctx context.Context,
	store ports.InternalAttachmentBoundedStore,
	in ToolMediaExternalizeInput,
) (result *ToolMediaExternalizeResult, resultErr error) {
	if _, err := PreviewExternalizedToolResultMedia(in); err != nil {
		return nil, err
	}
	if store == nil {
		for _, part := range in.Parts {
			if part != nil && part.GetMedia() != nil && (len(part.GetMedia().GetInlineData()) > 0 || part.GetMedia().GetAttachmentId() != "") {
				return nil, fmt.Errorf("%w: internal attachment store is not configured", ErrInvalidInternalAttachment)
			}
		}
	}
	// Validate all inline blobs and resolve all existing refs before the first write. Input or
	// reference failures therefore occur before mutation; the rollback below handles a later
	// operational store failure after an earlier part was created.
	remainingBytes := ports.MaxInternalAttachmentHydrationBytes
	resolvedExisting := make(map[int]*ports.InternalAttachmentRef)
	for partIndex, part := range in.Parts {
		if part == nil || part.GetMedia() == nil {
			continue
		}
		mediaPart := part.GetMedia()
		contentType := canonicalToolMediaType(mediaPart.GetContentType())
		entityID := toolMediaEntityID(in, partIndex)
		switch source := mediaPart.GetSource().(type) {
		case *proto_models.MediaPart_InlineData:
			if err := store.ValidateInternal(ctx, ports.InternalAttachmentInput{
				UserID: in.OwnerUserID, ObjectType: ports.InternalAttachmentObjectTypeToolMedia, EntityID: entityID,
				Filename: toolMediaFilename(mediaPart.GetName(), contentType, partIndex), ContentType: contentType,
				Data: source.InlineData, MaxBytes: maxToolMediaAttachmentBytes,
			}); err != nil {
				return nil, fmt.Errorf("validate tool media part %d: %w", partIndex, err)
			}
			remainingBytes -= uint64(len(source.InlineData))
		case *proto_models.MediaPart_AttachmentId:
			attachmentID, _ := strconv.ParseUint(strings.TrimSpace(source.AttachmentId), 10, 64)
			declaredSize := mediaPart.GetSizeBytes()
			if declaredSize == 0 || declaredSize > remainingBytes {
				return nil, fmt.Errorf("%w: combined tool media exceeds %d bytes", ErrInvalidInternalAttachment, ports.MaxInternalAttachmentHydrationBytes)
			}
			_, ref, err := resolveToolMediaBytes(ctx, store, attachmentID, in.OwnerUserID, entityID, contentType, mediaPart.GetName(), declaredSize)
			if err != nil {
				return nil, err
			}
			resolvedExisting[partIndex] = ref
			remainingBytes -= declaredSize
		}
	}
	out := &ToolMediaExternalizeResult{Parts: cloneMessageParts(in.Parts)}
	createdAttachmentIDs := make([]uint64, 0)
	defer func() {
		if resultErr == nil || store == nil || len(createdAttachmentIDs) == 0 {
			return
		}
		// Storage is sequential, so a later operational failure can follow one or more successful
		// writes even though every part passed preflight. Roll back only rows this invocation reports
		// creating, and only before the caller can begin transcript persistence. DeleteInternal
		// tombstones under the transcript/reference row lock, so a concurrent transcript winner is
		// preserved; reused idempotency winners (Created=false) are never cleanup candidates.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), internalAttachmentReconcileTimeout)
		defer cancel()
		for _, createdAttachmentID := range slices.Backward(createdAttachmentIDs) {
			_ = store.DeleteInternal(cleanupCtx, createdAttachmentID)
		}
	}()
	for partIndex, part := range out.Parts {
		if part == nil || part.GetMedia() == nil {
			continue
		}
		mediaPart := part.GetMedia()
		contentType := canonicalToolMediaType(mediaPart.GetContentType())
		if contentType == "" {
			return out, fmt.Errorf("%w: tool media part %d has invalid content type", ErrInvalidInternalAttachment, partIndex)
		}
		entityID := toolMediaEntityID(in, partIndex)

		switch source := mediaPart.GetSource().(type) {
		case *proto_models.MediaPart_InlineData:
			if store == nil || len(source.InlineData) == 0 {
				return out, fmt.Errorf("%w: inline tool media part %d cannot be stored", ErrInvalidInternalAttachment, partIndex)
			}
			ref, err := store.StoreInternal(ctx, ports.InternalAttachmentInput{
				UserID:      in.OwnerUserID,
				ObjectType:  ports.InternalAttachmentObjectTypeToolMedia,
				EntityID:    entityID,
				Filename:    toolMediaFilename(mediaPart.GetName(), contentType, partIndex),
				ContentType: contentType,
				Data:        source.InlineData,
				MaxBytes:    maxToolMediaAttachmentBytes,
			})
			if ref != nil && ref.Created && ref.AttachmentID != 0 {
				createdAttachmentIDs = append(createdAttachmentIDs, ref.AttachmentID)
			}
			if err != nil {
				return out, fmt.Errorf("store tool media part %d: %w", partIndex, err)
			}
			if err := validateToolMediaRef(ref, in.OwnerUserID, entityID, contentType, ""); err != nil {
				return out, err
			}
			digest := sha256.Sum256(source.InlineData)
			if ref.SizeBytes != uint64(len(source.InlineData)) || !strings.EqualFold(ref.ContentSHA256, hex.EncodeToString(digest[:])) {
				return out, fmt.Errorf("%w: stored tool media integrity mismatch", ErrInvalidInternalAttachment)
			}
			mediaPart.ContentType = ref.ContentType
			mediaPart.Name = ref.Filename
			mediaPart.SizeBytes = ref.SizeBytes
			mediaPart.Source = &proto_models.MediaPart_AttachmentId{AttachmentId: strconv.FormatUint(ref.AttachmentID, 10)}
			out.AttachmentIDs = append(out.AttachmentIDs, ref.AttachmentID)

		case *proto_models.MediaPart_AttachmentId:
			attachmentID, err := strconv.ParseUint(strings.TrimSpace(source.AttachmentId), 10, 64)
			if err != nil || attachmentID == 0 || store == nil {
				return out, fmt.Errorf("%w: invalid tool media attachment reference", ErrInvalidInternalAttachment)
			}
			ref := resolvedExisting[partIndex]
			if ref == nil || ref.AttachmentID != attachmentID {
				return out, fmt.Errorf("%w: missing preflighted tool media reference", ErrInvalidInternalAttachment)
			}
			mediaPart.ContentType = ref.ContentType
			mediaPart.Name = ref.Filename
			mediaPart.SizeBytes = ref.SizeBytes
			out.AttachmentIDs = append(out.AttachmentIDs, attachmentID)

		case *proto_models.MediaPart_Url:
			validated, err := validateToolMediaExternalSource(source.Url)
			if err != nil {
				return out, err
			}
			source.Url = validated
		default:
			return out, fmt.Errorf("%w: tool media part %d has no source", ErrInvalidInternalAttachment, partIndex)
		}
	}
	return out, nil
}

func cloneMessageParts(parts []*proto_models.MessagePart) []*proto_models.MessagePart {
	if len(parts) == 0 {
		return nil
	}
	out := make([]*proto_models.MessagePart, len(parts))
	for i, part := range parts {
		if part != nil {
			out[i] = proto.Clone(part).(*proto_models.MessagePart)
		}
	}
	return out
}

func toolMediaFilename(filename, contentType string, partIndex int) string {
	if filename = strings.TrimSpace(filename); filename != "" {
		return filename
	}
	ext := ""
	if extensions, err := mime.ExtensionsByType(contentType); err == nil && len(extensions) > 0 {
		ext = extensions[0]
	}
	return fmt.Sprintf("tool-output-%d%s", partIndex, ext)
}

func canonicalToolMediaType(contentType string) string {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil || mediaType == "" {
		return ""
	}
	return strings.ToLower(mediaType)
}

// validateToolMediaExternalSource keeps replayable external references constrained to source
// forms understood by provider adapters. Private internal attachments use the attachment_id
// oneof instead; this URL-shaped oneof may contain a provider file ID or an absolute HTTP(S) URL.
// Inline data must use the inline_data oneof so the ordinary per-part and aggregate byte budgets
// are enforced before any decoding or storage. The executor performs that normalization for tool
// implementations; rejecting data URIs here also protects direct transcript-writer callers.
func validateToolMediaExternalSource(source string) (string, error) {
	ref := strings.TrimSpace(source)
	if hasToolMediaDataScheme(ref) {
		return "", fmt.Errorf("%w: tool media data URIs must use inline_data", ErrInvalidInternalAttachment)
	}
	if strings.HasPrefix(ref, "file-") && len(ref) > len("file-") && !strings.ContainsAny(ref, " \t\r\n") {
		return ref, nil
	}
	parsed, err := url.Parse(ref)
	if err == nil && (strings.EqualFold(parsed.Scheme, "http") || strings.EqualFold(parsed.Scheme, "https")) && parsed.Host != "" {
		return ref, nil
	}
	return "", fmt.Errorf("%w: tool media source must be a data URI, file ID, or absolute HTTP(S) URL", ErrInvalidInternalAttachment)
}

func hasToolMediaDataScheme(value string) bool {
	return len(value) >= len("data:") && strings.EqualFold(value[:len("data:")], "data:")
}

func validateToolMediaRef(ref *ports.InternalAttachmentRef, ownerUserID uint64, entityID, contentType, filename string) error {
	if ref == nil || ref.AttachmentID == 0 || ref.OwnerUserID != ownerUserID ||
		ref.ObjectType != ports.InternalAttachmentObjectTypeToolMedia || ref.EntityID != entityID ||
		canonicalToolMediaType(ref.ContentType) != canonicalToolMediaType(contentType) ||
		strings.TrimSpace(ref.Filename) == "" || strings.TrimSpace(ref.ContentSHA256) == "" || ref.SizeBytes == 0 {
		return fmt.Errorf("%w: tool media attachment metadata mismatch", ErrInvalidInternalAttachment)
	}
	if filename != "" && ref.Filename != filename {
		return fmt.Errorf("%w: tool media filename mismatch", ErrInvalidInternalAttachment)
	}
	return nil
}

func resolveToolMediaBytes(
	ctx context.Context,
	store ports.InternalAttachmentBoundedStore,
	attachmentID, ownerUserID uint64,
	entityID, contentType, filename string,
	maxBytes uint64,
) ([]byte, *ports.InternalAttachmentRef, error) {
	if maxBytes == 0 {
		return nil, nil, fmt.Errorf("%w: tool media attachment has no byte allowance", ErrInvalidInternalAttachment)
	}
	data, ref, err := store.ResolveInternalBounded(ctx, attachmentID, maxBytes)
	if err != nil {
		return nil, nil, err
	}
	if err := validateToolMediaRef(ref, ownerUserID, entityID, contentType, filename); err != nil {
		return nil, nil, err
	}
	digest := sha256.Sum256(data)
	if ref.SizeBytes != maxBytes || ref.SizeBytes != uint64(len(data)) || !strings.EqualFold(ref.ContentSHA256, hex.EncodeToString(digest[:])) {
		return nil, nil, fmt.Errorf("%w: resolved tool media integrity mismatch", ErrInvalidInternalAttachment)
	}
	return data, ref, nil
}

// ResolveToolMediaReference hydrates images as bounded, integrity-checked data URIs.
// Private object-store URLs can resolve to localhost/private networks or expire before a
// remote model fetches them. Image bytes travel only in the provider request; the durable
// transcript retains private attachment references. Non-image media may use a verified URL.
func ResolveToolMediaReference(
	ctx context.Context,
	store ports.InternalAttachmentBoundedStore,
	attachmentID, ownerUserID, agentRunID uint64,
	toolCallID string,
	partIndex int,
	contentType, filename string,
	maxBytes uint64,
) (*ports.ResolvedToolMedia, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: internal attachment store is not configured", ErrInternalAttachmentAccess)
	}
	entityID := ToolMediaEntityID(agentRunID, toolCallID, partIndex)
	if urlStore, ok := store.(ports.InternalAttachmentURLStore); ok && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "image/") {
		resolvedURL, ref, err := urlStore.ResolveInternalURL(ctx, attachmentID, maxBytes)
		if err == nil {
			if err := validateToolMediaRef(ref, ownerUserID, entityID, contentType, filename); err != nil {
				return nil, err
			}
			parsed, parseErr := url.Parse(strings.TrimSpace(resolvedURL))
			if parseErr == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" {
				return &ports.ResolvedToolMedia{Source: resolvedURL, SizeBytes: ref.SizeBytes}, nil
			}
		}
		// A provider-inaccessible or unavailable presigned URL should not discard an
		// otherwise valid tool result. The byte path below independently revalidates
		// ownership, binding, size, and digest before producing a data URI.
	}
	data, ref, err := store.ResolveInternalBounded(ctx, attachmentID, maxBytes)
	if err == nil {
		err = validateToolMediaRef(ref, ownerUserID, entityID, contentType, filename)
	}
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if ref.SizeBytes > maxBytes || ref.SizeBytes != uint64(len(data)) ||
		!strings.EqualFold(ref.ContentSHA256, hex.EncodeToString(digest[:])) {
		return nil, fmt.Errorf("%w: resolved tool media integrity mismatch", ErrInvalidInternalAttachment)
	}
	return &ports.ResolvedToolMedia{
		Source:    fmt.Sprintf("data:%s;base64,%s", ref.ContentType, base64.StdEncoding.EncodeToString(data)),
		SizeBytes: ref.SizeBytes,
	}, nil
}

func addToolMediaBytes(total, size uint64) (uint64, error) {
	if size > ^uint64(0)-total {
		return 0, fmt.Errorf("tool media byte count overflow")
	}
	return total + size, nil
}
