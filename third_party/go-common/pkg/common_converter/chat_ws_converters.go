package common_converter

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	proto_models "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ServiceChatEventToWebSocketProto executes this method as part of the backend conversion flow between persistence, service, and transport models.
//
// It produces chat's realtime *payload* message. The generic envelope (topic, namespaced event type,
// timestamp) is built by the realtime publisher, which serializes this message into
// RealtimeEvent.payload — go-common's transport never enumerates payload types, so nothing about the
// wire frame belongs here.
//
// Exactly one oneof variant is set, chosen most-specific-first (reaction, then message stream, then
// tool-call resolution, then inference summary, then message, then channel), and the routing scalars (channel_id,
// message_id) are filled from whichever variant won.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - event: The service-layer chat event to convert.
//
// Returns:
//   - result: The chat payload message, or nil when event is nil.
func (h *ConverterHandler[U]) ServiceChatEventToWebSocketProto(
	ctx context.Context,
	event *ports.ChatEvent,
) *proto_models.ChatWebSocketEvent {
	if event == nil {
		return nil
	}

	out := &proto_models.ChatWebSocketEvent{
		Type:        event.Type,
		OccurredAt:  timestampOrNil(event.OccurredAt),
		ActorUserId: uint64(event.ActorUserID),
		Created:     event.Created,
	}

	if len(event.MemberUserIDs) > 0 {
		out.MemberUserIds = make([]uint64, 0, len(event.MemberUserIDs))
		for _, id := range event.MemberUserIDs {
			out.MemberUserIds = append(out.MemberUserIds, uint64(id))
		}
	}

	// Personal preference payloads may only be serialized for their single owner.
	// This explicit target, not the post-commit context, is the viewer.
	if event.Participant != nil {
		participant := event.Participant
		if event.Type != proto_models.ChatEventType_CHAT_EVENT_TYPE_PARTICIPANT_PREFERENCES_UPDATED ||
			!event.TargetedRecipients || len(event.MemberUserIDs) != 1 ||
			event.ActorUserID == 0 || event.ActorUserID != participant.UserID || event.MemberUserIDs[0] != participant.UserID {
			return nil
		}
		converted := h.ServiceChatChannelParticipantToProto(participant, event.MemberUserIDs[0])
		out.ChannelId = participant.ChannelID
		out.Payload = &proto_models.ChatWebSocketEvent_Participant{Participant: converted}
		return out
	}

	// Prefer the most specific payload (reaction/message/channel).
	if event.Reaction != nil {
		out.MessageId = event.Reaction.GetMessageID()
		// Reaction events usually include a minimal Message stub for routing (channel_id).
		if event.Message != nil {
			out.ChannelId = event.Message.GetChannelID()
		}
		out.Payload = &proto_models.ChatWebSocketEvent_Reaction{
			Reaction: h.ServiceChatMessageReactionToProto(ctx, event.Reaction),
		}
		return out
	}

	if event.MessageStream != nil {
		out.ChannelId = uint64(event.MessageStream.ChannelID)
		out.MessageId = uint64(event.MessageStream.MessageID)
		out.Payload = &proto_models.ChatWebSocketEvent_MessageStream{
			MessageStream: &proto_models.MessageStreamEvent{
				MessageId:       uint64(event.MessageStream.MessageID),
				ChannelId:       uint64(event.MessageStream.ChannelID),
				StreamId:        event.MessageStream.StreamID,
				Sequence:        event.MessageStream.Sequence,
				TextDelta:       event.MessageStream.TextDelta,
				AccumulatedText: event.MessageStream.AccumulatedText,
				FinalUsage:      event.MessageStream.FinalUsage,
				ParentMessageId: event.MessageStream.ParentMessageID,
			},
		}
		return out
	}

	if event.ToolCallResolution != nil {
		r := event.ToolCallResolution
		out.ChannelId = uint64(r.ChannelID)
		out.MessageId = uint64(r.MessageID)
		resolvedBy := common_util.Deref(r.ResolvedByUserID)
		reason := common_util.Deref(r.ResolutionReason)
		out.Payload = &proto_models.ChatWebSocketEvent_ToolCallResolution{
			ToolCallResolution: &proto_models.ToolCallResolutionEvent{
				MessageId:        uint64(r.MessageID),
				ChannelId:        uint64(r.ChannelID),
				ApprovalStatus:   r.ApprovalStatus,
				ExecutionState:   r.ExecutionState,
				ResolvedByUserId: resolvedBy,
				ResolvedAt:       timestampPtrOrNil(r.ResolvedAt),
				ResolutionReason: reason,
			},
		}
		return out
	}

	if event.ContextCompaction != nil {
		c := event.ContextCompaction
		out.ChannelId = c.ChannelID
		out.Payload = &proto_models.ChatWebSocketEvent_ContextCompaction{
			ContextCompaction: &proto_models.ContextCompactionEvent{
				ChannelId:       c.ChannelID,
				ParentMessageId: c.ParentMessageID,
				AgentRunId:      c.AgentRunID,
				Active:          c.Active,
			},
		}
		return out
	}

	if event.AgentRunState != nil {
		st := event.AgentRunState
		out.ChannelId = st.ChannelID
		payload := &proto_models.AgentRunStateEvent{
			ChannelId:        st.ChannelID,
			ParentMessageId:  st.ParentMessageID,
			AgentRunId:       st.AgentRunID,
			Status:           st.Status,
			StatusReason:     st.StatusReason,
			StatusReasonCode: st.StatusReasonCode,
			Activity:         st.Activity,
		}
		payload.ActivityStartedAt = timestampOrNil(st.ActivityStartedAt)
		if st.ActivityMessageID != 0 {
			activityMessageID := st.ActivityMessageID
			payload.ActivityMessageId = &activityMessageID
		}
		out.Payload = &proto_models.ChatWebSocketEvent_AgentRunState{AgentRunState: payload}
		return out
	}

	if event.AgentSessionInteraction != nil {
		interaction := event.AgentSessionInteraction
		out.ChannelId = interaction.ChannelID
		out.Payload = &proto_models.ChatWebSocketEvent_AgentSessionInteraction{
			AgentSessionInteraction: &proto_models.AgentSessionInteractionEvent{
				ChannelId:       interaction.ChannelID,
				ParentMessageId: interaction.ParentMessageID,
				AgentSessionId:  interaction.AgentSessionID,
			},
		}
		return out
	}

	if event.InferenceUsage != nil {
		out.ChannelId = event.InferenceUsage.ChannelId
		out.MessageId = event.InferenceUsage.GetMessageSummary().GetMessageId()
		out.Payload = &proto_models.ChatWebSocketEvent_InferenceUsage{InferenceUsage: event.InferenceUsage}
		return out
	}

	if event.Message != nil {
		out.ChannelId = event.Message.GetChannelID()
		out.MessageId = event.Message.GetID()
		out.Payload = &proto_models.ChatWebSocketEvent_Message{
			Message: h.ServiceChatMessageToProto(ctx, event.Message),
		}
		return out
	}

	if event.Channel != nil {
		out.ChannelId = event.Channel.GetID()
		out.Payload = &proto_models.ChatWebSocketEvent_Channel{
			Channel: h.ServiceChatChannelToProto(event.Channel),
		}
		return out
	}

	return out
}
