package ai

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/genai/partmetadata"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
)

// ChatMessageLister provides the minimal chat APIs needed to fetch context.
type ChatMessageLister interface {
	GetMessage(ctx context.Context, messageID uint64) (*common_service_models.ChatMessage, error)
	ListMessages(
		ctx context.Context,
		channelID uint64,
		pagination *proto_models.PaginationRequest,
		parentMessageID uint64,
		includeDeleted bool,
		sort *proto_models.SortRequest,
		sortBy proto_models.ListMessagesSortBy,
	) ([]*common_service_models.ChatMessage, *proto_models.PaginationResponse, error)
}

// ThreadScope controls whether context is limited to a thread.
type ThreadScope int

const (
	ThreadScopeAuto ThreadScope = iota
	ThreadScopeChannelOnly
	ThreadScopeThreadOnly
)

// ContextRequest defines a data model used by the ai backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ContextRequest struct {
	ChannelID       uint64
	AnchorMessageID uint64
	ActorUserID     uint64
}

// ContextOptions defines a data model used by the ai backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ContextOptions struct {
	// LimitMessages is the number of recent messages to read; it is required (non-zero).
	LimitMessages uint32
	MaxChars      int
	MaxImages     int

	ThreadScope ThreadScope

	IncludeParticipants  bool
	IncludeBots          bool
	IncludeEmbeds        bool
	IncludeAttachments   bool
	IncludeNonImageFiles bool
	// IncludeImageReferenceText adds a text part (image_url[/mime_type]) for image attachments.
	// This is useful when downstream tool calls need JSON arguments that can reference the image.
	IncludeImageReferenceText bool
	IncludeAnchorIfMissing    bool
	LabelParticipants         bool

	// EmbedTypeAllowlist limits which embed types are serialized via callbacks.
	// If empty or nil, any embed with a serializer is eligible.
	// Non-allowed embed types fall back to their fallback text (if provided).
	EmbedTypeAllowlist map[uint32]struct{}
}

// DefaultContextOptions returns the default configuration values in the ai backend flow.
func DefaultContextOptions() ContextOptions {
	return ContextOptions{
		ThreadScope:            ThreadScopeAuto,
		IncludeEmbeds:          true,
		IncludeAttachments:     true,
		IncludeAnchorIfMissing: true,
		LabelParticipants:      true,
	}
}

// ContextResult defines a data model used by the ai backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ContextResult struct {
	Messages     []*genkitai.Message
	RawMessages  []*common_service_models.ChatMessage
	Truncated    bool
	UsedFallback bool
}

// EmbedPartSerializerFunc turns a chat embed into one or more Genkit parts.
type EmbedPartSerializerFunc func(
	ctx context.Context,
	msg *common_service_models.ChatMessage,
	embed *common_service_models.ChatMessageEmbed,
) ([]*genkitai.Part, error)

// EmbedPartSerializerRegistry resolves part serializers for embed types.
type EmbedPartSerializerRegistry interface {
	SerializeParts(
		ctx context.Context,
		msg *common_service_models.ChatMessage,
		embed *common_service_models.ChatMessageEmbed,
	) (parts []*genkitai.Part, ok bool, err error)
}

// EmbedPartSerializerMap is a simple map-backed part serializer registry.
type EmbedPartSerializerMap map[uint32]EmbedPartSerializerFunc

// SerializeParts executes its part of the service logic for EmbedPartSerializerMap in the ai backend flow.
func (m EmbedPartSerializerMap) SerializeParts(
	ctx context.Context,
	msg *common_service_models.ChatMessage,
	embed *common_service_models.ChatMessageEmbed,
) ([]*genkitai.Part, bool, error) {
	if m == nil || msg == nil || embed == nil {
		return nil, false, nil
	}
	fn, ok := m[embed.EmbedType]
	if !ok || fn == nil {
		return nil, false, nil
	}
	parts, err := fn(ctx, msg, embed)
	return parts, true, err
}

// BuildChatContextMessages builds data needed by downstream processing in the ai backend flow.
//
// Parameters:
//   - ctx: Context that carries cancellation, deadlines, and request-scoped values.
//   - lister: Input value of type ChatMessageLister consumed by BuildChatContextMessages.
//   - req: Input value of type ContextRequest consumed by BuildChatContextMessages.
//   - opts: Option values that tune how BuildChatContextMessages performs processing.
//   - embedRegistry: Input value of type EmbedPartSerializerRegistry consumed by BuildChatContextMessages.
func BuildChatContextMessages(
	ctx context.Context,
	lister ChatMessageLister,
	req ContextRequest,
	opts ContextOptions,
	embedRegistry EmbedPartSerializerRegistry,
) (*ContextResult, error) {
	if lister == nil {
		return nil, errors.New("ai context: lister is nil")
	}
	if req.ChannelID == 0 {
		return nil, errors.New("ai context: channel id is required")
	}
	if !opts.IncludeParticipants && req.ActorUserID == 0 {
		return nil, errors.New("ai context: actor user id is required when includeParticipants is false")
	}

	// The caller owns the page size (typically sourced from its own configuration);
	// the chat read below fetches it as given, so a missing limit is rejected
	// rather than replaced with a size of our own.
	limit := opts.LimitMessages
	if limit == 0 {
		return nil, errors.New("ai context: limit messages is required")
	}

	var anchor *common_service_models.ChatMessage
	if req.AnchorMessageID != 0 {
		msg, err := lister.GetMessage(ctx, req.AnchorMessageID)
		if err == nil {
			anchor = msg
		} else if opts.ThreadScope == ThreadScopeThreadOnly {
			return nil, err
		}
	}

	parentMessageID, err := resolveParentMessageID(anchor, opts.ThreadScope)
	if err != nil {
		return nil, err
	}

	pagination := &proto_models.PaginationRequest{Limit: limit}
	sort := &proto_models.SortRequest{SortDesc: true}
	messages, _, err := lister.ListMessages(ctx, req.ChannelID, pagination, parentMessageID, false, sort, proto_models.ListMessagesSortBy_LIST_MESSAGES_SORT_BY_ID)
	if err != nil {
		return nil, err
	}

	if opts.IncludeAnchorIfMissing && anchor != nil && !containsMessage(messages, anchor.ID) {
		messages = append(messages, anchor)
	}

	slices.SortFunc(messages, func(a, b *common_service_models.ChatMessage) int {
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})

	candidates := make([]messageCandidate, 0, len(messages))
	usedFallback := false
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		if !opts.IncludeParticipants && req.ActorUserID != 0 && msg.SenderID != req.ActorUserID {
			continue
		}
		if !opts.IncludeBots && msg.Sender != nil && msg.Sender.IsBot() {
			continue
		}

		if err := validateMessageRepresentation(msg); err != nil {
			return nil, err
		}
		parts, fallbackUsed := buildMessageContent(ctx, msg, opts, embedRegistry)
		if fallbackUsed {
			usedFallback = true
		}
		if len(parts) == 0 {
			continue
		}

		role := roleOf(msg)
		candidates = append(candidates, messageCandidate{
			message: msg,
			parts:   parts,
			role:    role,
		})
	}

	selected, truncated := applyCaps(candidates, opts.MaxChars, opts.MaxImages)
	result := &ContextResult{
		Messages:     buildGenkitMessages(selected),
		RawMessages:  extractRawMessages(selected),
		Truncated:    truncated,
		UsedFallback: usedFallback,
	}

	return result, nil
}

// messageCandidate defines a data model used by the ai backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type messageCandidate struct {
	message *common_service_models.ChatMessage
	parts   []*genkitai.Part
	role    genkitai.Role
}

// resolveParentMessageID executes its part of the service logic in the ai backend flow.
func resolveParentMessageID(anchor *common_service_models.ChatMessage, scope ThreadScope) (uint64, error) {
	switch scope {
	case ThreadScopeChannelOnly:
		return 0, nil
	case ThreadScopeThreadOnly:
		if anchor == nil {
			return 0, errors.New("ai context: anchor message required for thread scope")
		}
		if anchor.ParentMessageID != nil && *anchor.ParentMessageID != 0 {
			return uint64(*anchor.ParentMessageID), nil
		}
		return uint64(anchor.ID), nil
	case ThreadScopeAuto:
		if anchor != nil && anchor.ParentMessageID != nil && *anchor.ParentMessageID != 0 {
			return uint64(*anchor.ParentMessageID), nil
		}
		return 0, nil
	default:
		return 0, errors.New("ai context: invalid thread scope")
	}
}

// buildMessageContent executes its part of the service logic in the ai backend flow.
//
// Parameters:
//   - ctx: Context that carries cancellation, deadlines, and request-scoped values.
//   - msg: Input value of type *common_service_models.ChatMessage consumed by buildMessageContent.
//   - opts: Option values that tune how buildMessageContent performs processing.
//   - embedRegistry: Input value of type EmbedPartSerializerRegistry consumed by buildMessageContent.
func buildMessageContent(
	ctx context.Context,
	msg *common_service_models.ChatMessage,
	opts ContextOptions,
	embedRegistry EmbedPartSerializerRegistry,
) ([]*genkitai.Part, bool) {
	parts := make([]*genkitai.Part, 0, 4)
	lines := make([]string, 0, 2)

	if opts.IncludeParticipants && opts.LabelParticipants {
		label := formatSenderLabel(msg)
		if label != "" {
			lines = append(lines, label+":")
		}
	}
	if strings.TrimSpace(msg.Content) != "" {
		lines = append(lines, msg.Content)
	}
	if len(lines) > 0 {
		parts = append(parts, genkitai.NewTextPart(strings.Join(lines, "\n")))
	}

	fallbackUsed := false
	if opts.IncludeEmbeds {
		for _, embed := range msg.Embeds {
			if embed == nil {
				continue
			}
			embedParts, usedFallback := serializeEmbed(ctx, msg, embed, opts, embedRegistry)
			if usedFallback {
				fallbackUsed = true
			}
			if len(embedParts) > 0 {
				parts = append(parts, embedParts...)
			}
		}
	}

	if opts.IncludeAttachments {
		for _, att := range msg.Attachments {
			if att == nil {
				continue
			}
			if isImageAttachment(att) {
				url := strings.TrimSpace(att.GetDownloadUrl())
				if url == "" {
					url = strings.TrimSpace(att.GetThumbnailUrl())
				}
				if url == "" {
					continue
				}
				mediaPart := genkitai.NewMediaPart(att.GetContentType(), url)
				mediaPart.Metadata = partmetadata.MarkTrustedHTTPSource(mediaPart.Metadata)
				parts = append(parts, mediaPart)
				if opts.IncludeImageReferenceText {
					if refText := formatImageReferenceText(att, url); refText != "" {
						parts = append(parts, genkitai.NewTextPart(refText))
					}
				}
			} else if opts.IncludeNonImageFiles {
				parts = append(parts, genkitai.NewTextPart(formatAttachmentText(att)))
			}
		}
	}

	return filterParts(parts), fallbackUsed
}

// serializeEmbed executes its part of the service logic in the ai backend flow.
//
// Parameters:
//   - ctx: Context that carries cancellation, deadlines, and request-scoped values.
//   - msg: Input value of type *common_service_models.ChatMessage consumed by serializeEmbed.
//   - embed: Input value of type *common_service_models.ChatMessageEmbed consumed by serializeEmbed.
//   - opts: Option values that tune how serializeEmbed performs processing.
//   - registry: Input value of type EmbedPartSerializerRegistry consumed by serializeEmbed.
func serializeEmbed(
	ctx context.Context,
	msg *common_service_models.ChatMessage,
	embed *common_service_models.ChatMessageEmbed,
	opts ContextOptions,
	registry EmbedPartSerializerRegistry,
) ([]*genkitai.Part, bool) {
	if msg == nil || embed == nil || !opts.IncludeEmbeds {
		return nil, false
	}

	allowed := len(opts.EmbedTypeAllowlist) == 0
	if !allowed {
		_, allowed = opts.EmbedTypeAllowlist[embed.EmbedType]
	}

	if !allowed {
		if fallback := fallbackPart(embed); fallback != nil {
			return []*genkitai.Part{fallback}, true
		}
		return nil, false
	}

	if registry != nil {
		if serializedParts, ok, err := registry.SerializeParts(ctx, msg, embed); err == nil && ok {
			filtered := filterParts(serializedParts)
			if len(filtered) > 0 {
				return filtered, false
			}
		}
	}

	if fallback := fallbackPart(embed); fallback != nil {
		return []*genkitai.Part{fallback}, true
	}
	return nil, false
}

// fallbackPart executes its part of the service logic in the ai backend flow.
func fallbackPart(embed *common_service_models.ChatMessageEmbed) *genkitai.Part {
	if embed == nil {
		return nil
	}
	text := formatEmbedText(embed.EmbedType, embed.FallbackText)
	if text == "" {
		return nil
	}
	return genkitai.NewTextPart(text)
}

// formatEmbedText executes its part of the service logic in the ai backend flow.
func formatEmbedText(embedType uint32, text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	return fmt.Sprintf("[embed:%d] %s", embedType, trimmed)
}

// isImageAttachment executes its part of the service logic in the ai backend flow.
func isImageAttachment(att *proto_models.UploadedAttachment) bool {
	if att == nil {
		return false
	}
	return strings.HasPrefix(strings.ToLower(att.GetContentType()), "image/")
}

// formatAttachmentText executes its part of the service logic in the ai backend flow.
func formatAttachmentText(att *proto_models.UploadedAttachment) string {
	if att == nil {
		return ""
	}
	name := strings.TrimSpace(att.GetFilename())
	if name == "" {
		name = "attachment"
	}
	contentType := strings.TrimSpace(att.GetContentType())
	if contentType == "" {
		return fmt.Sprintf("Attachment: %s", name)
	}
	return fmt.Sprintf("Attachment: %s (%s)", name, contentType)
}

// formatImageReferenceText executes its part of the service logic in the ai backend flow.
func formatImageReferenceText(att *proto_models.UploadedAttachment, url string) string {
	ref := strings.TrimSpace(url)
	if ref == "" {
		return ""
	}
	mimeType := ""
	if att != nil {
		mimeType = strings.TrimSpace(att.GetContentType())
	}
	if mimeType == "" {
		return fmt.Sprintf("image_url: %s", ref)
	}
	return fmt.Sprintf("image_url: %s\nmime_type: %s", ref, mimeType)
}

// formatSenderLabel executes its part of the service logic in the ai backend flow.
func formatSenderLabel(msg *common_service_models.ChatMessage) string {
	if msg == nil || msg.Sender == nil {
		if msg == nil {
			return ""
		}
		return fmt.Sprintf("user:%d", msg.SenderID)
	}
	first := strings.TrimSpace(msg.Sender.GetFirstName())
	last := strings.TrimSpace(msg.Sender.GetLastName())
	if first != "" || last != "" {
		return strings.TrimSpace(first + " " + last)
	}
	username := strings.TrimSpace(msg.Sender.GetUsername())
	if username != "" {
		return username
	}
	return fmt.Sprintf("user:%d", msg.SenderID)
}

// applyCaps executes its part of the service logic in the ai backend flow.
func applyCaps(candidates []messageCandidate, maxChars int, maxImages int) ([]messageCandidate, bool) {
	if maxChars < 0 {
		maxChars = 0
	}
	if maxImages < 0 {
		maxImages = 0
	}
	enforceChars := maxChars > 0
	enforceImages := maxImages > 0

	remainingChars := maxChars
	remainingImages := maxImages
	truncated := false

	selected := make([]messageCandidate, 0, len(candidates))
	for _, c := range slices.Backward(candidates) {

		parts, candidateTruncated := applyCapsToParts(
			c.parts,
			enforceChars,
			&remainingChars,
			enforceImages,
			&remainingImages,
		)
		if candidateTruncated {
			truncated = true
		}
		if len(parts) == 0 {
			continue
		}

		selected = append(selected, messageCandidate{
			message: c.message,
			parts:   parts,
			role:    c.role,
		})

		if enforceChars && remainingChars <= 0 && enforceImages && remainingImages <= 0 {
			break
		}
	}

	slices.Reverse(selected)
	return selected, truncated
}

// applyCapsToParts executes its part of the service logic in the ai backend flow.
func applyCapsToParts(
	parts []*genkitai.Part,
	enforceChars bool,
	remainingChars *int,
	enforceImages bool,
	remainingImages *int,
) ([]*genkitai.Part, bool) {
	if len(parts) == 0 {
		return nil, false
	}

	selected := make([]*genkitai.Part, 0, len(parts))
	truncated := false

	for _, part := range parts {
		if part == nil {
			continue
		}

		if part.IsText() {
			text := part.Text
			if enforceChars {
				if remainingChars == nil || *remainingChars <= 0 {
					truncated = true
					continue
				}
				if len(text) > *remainingChars {
					text = truncateText(text, *remainingChars)
					truncated = true
				}
				*remainingChars -= len(text)
			}
			if strings.TrimSpace(text) == "" {
				continue
			}
			clone := *part
			clone.Text = text
			selected = append(selected, &clone)
			continue
		}

		if part.IsImage() && enforceImages {
			if remainingImages == nil || *remainingImages <= 0 {
				truncated = true
				continue
			}
			*remainingImages--
		}

		selected = append(selected, part)
	}

	return filterParts(selected), truncated
}

// truncateText executes its part of the service logic in the ai backend flow.
func truncateText(text string, maxChars int) string {
	if maxChars <= 0 || len(text) <= maxChars {
		return text
	}
	if maxChars <= 3 {
		return text[:maxChars]
	}
	return text[:maxChars-3] + "..."
}

// buildGenkitMessages executes its part of the service logic in the ai backend flow.
func buildGenkitMessages(candidates []messageCandidate) []*genkitai.Message {
	out := make([]*genkitai.Message, 0, len(candidates))
	for _, c := range candidates {
		parts := filterParts(c.parts)
		if len(parts) == 0 {
			continue
		}
		switch c.role {
		case genkitai.RoleModel:
			out = append(out, genkitai.NewModelMessage(parts...))
		case genkitai.RoleSystem:
			out = append(out, genkitai.NewSystemMessage(parts...))
		default:
			out = append(out, genkitai.NewUserMessage(parts...))
		}
	}
	return out
}

// extractRawMessages executes its part of the service logic in the ai backend flow.
func extractRawMessages(candidates []messageCandidate) []*common_service_models.ChatMessage {
	out := make([]*common_service_models.ChatMessage, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.message)
	}
	return out
}

// filterParts executes its part of the service logic in the ai backend flow.
func filterParts(parts []*genkitai.Part) []*genkitai.Part {
	if len(parts) == 0 {
		return nil
	}
	out := make([]*genkitai.Part, 0, len(parts))
	for _, part := range parts {
		if part == nil {
			continue
		}
		if part.IsText() && strings.TrimSpace(part.Text) == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

// containsMessage executes its part of the service logic in the ai backend flow.
//
// Parameters:
//   - messages: Input value of type []*common_service_models.ChatMessage consumed by containsMessage.
//   - id: Identifier passed to containsMessage to locate the target record.
func containsMessage(messages []*common_service_models.ChatMessage, id uint64) bool {
	return slices.ContainsFunc(messages, func(msg *common_service_models.ChatMessage) bool {
		return msg != nil && msg.ID == id
	})
}
