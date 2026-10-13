package bot

import (
	"context"
	"strconv"
	"strings"

	attachmentpkg "github.com/Suhaibinator/go-common/pkg/common_service/attachment"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
)

func (w *agentTranscriptWriter[U]) externalizeToolResultMedia(
	result *ports.ToolResultRecord,
	leaseToken string,
) (parts []*proto_models.MessagePart, attachmentIDs []uint64, err error) {
	if result == nil || len(result.ContentParts) == 0 {
		return nil, nil, nil
	}
	if w == nil || w.d == nil || w.channelID() == 0 {
		return nil, nil, ErrTranscriptPersistenceNotConfigured
	}
	out, err := attachmentpkg.ExternalizeToolResultMedia(w.ctx, w.d.internalAttachmentStore, attachmentpkg.ToolMediaExternalizeInput{
		OwnerUserID: uint64(w.botUserID),
		AgentRunID:  result.AgentRunID,
		ToolCallID:  result.ToolCallID,
		LeaseOwner:  leaseToken,
		Parts:       result.ContentParts,
	})
	if out == nil {
		return nil, nil, err
	}
	return out.Parts, out.AttachmentIDs, err
}

// hydrateToolResultMediaForDisplay replaces validated private attachment references with
// browser-consumable sources on the realtime response view. The persisted transcript payload is
// never changed.
func (w *agentTranscriptWriter[U]) hydrateToolResultMediaForDisplay(ctx context.Context, message *common_service_models.ChatMessage) {
	if w == nil || w.d == nil || w.d.internalAttachmentStore == nil || message == nil ||
		message.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT || message.ToolCall == nil ||
		message.AgentRunID == nil || message.ToolCall.ToolCallID == nil ||
		message.ToolCall.PayloadType != proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_RESULT {
		return
	}
	var envelope proto_models.MessageModelPayload
	if err := proto.Unmarshal(message.ToolCall.ModelPayload, &envelope); err != nil {
		return
	}
	result := envelope.GetToolResult()
	if result == nil || result.GetToolCallId() == "" || result.GetToolCallId() != *message.ToolCall.ToolCallID {
		return
	}
	projected := proto.Clone(&envelope).(*proto_models.MessageModelPayload)
	changed := false
	for partIndex, part := range projected.GetToolResult().GetContentParts() {
		media := part.GetMedia()
		if media == nil || media.GetAttachmentId() == "" || media.GetSizeBytes() == 0 {
			continue
		}
		attachmentID, err := strconv.ParseUint(strings.TrimSpace(media.GetAttachmentId()), 10, 64)
		if err != nil || attachmentID == 0 {
			continue
		}
		resolved, err := attachmentpkg.ResolveToolMediaDisplayReference(ctx, w.d.internalAttachmentStore,
			attachmentID, uint64(message.SenderID), *message.AgentRunID, result.GetToolCallId(), partIndex,
			media.GetContentType(), media.GetName(), media.GetSizeBytes())
		if err != nil || resolved == nil || resolved.Source == "" || resolved.SizeBytes != media.GetSizeBytes() {
			continue
		}
		media.Source = &proto_models.MediaPart_Url{Url: resolved.Source}
		changed = true
	}
	if !changed {
		return
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(projected)
	if err != nil {
		return
	}
	toolCallView := *message.ToolCall
	toolCallView.ModelPayload = payload
	message.ToolCall = &toolCallView
}

func (w *agentTranscriptWriter[U]) ResolveToolMedia(
	ctx context.Context,
	attachmentID, ownerUserID, agentRunID uint64,
	toolCallID string,
	partIndex int,
	contentType, filename string,
	maxBytes uint64,
) (*ports.ResolvedToolMedia, error) {
	if w == nil || w.d == nil || ownerUserID == 0 {
		return nil, ErrTranscriptPersistenceNotConfigured
	}
	return attachmentpkg.ResolveToolMediaReference(
		ctx,
		w.d.internalAttachmentStore,
		attachmentID,
		ownerUserID,
		agentRunID,
		toolCallID,
		partIndex,
		contentType,
		filename,
		maxBytes,
	)
}
