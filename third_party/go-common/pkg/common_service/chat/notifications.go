package chat

import (
	"context"
	"errors"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// SetNotificationSink installs the sink that records chat notifications in each source
// transaction, at startup. Persistence uses the same sink for delivered queued input and
// delegation notices, inside their source transaction.
func (h *Handler[U]) SetNotificationSink(sink ports.ChatNotificationSink) {
	h.notificationSink = sink
	var recorder persistence.ChatAgentMessageRecorder
	if sink != nil {
		recorder = sink.ChatMessageRecorded
	}
	h.persistenceHandler.SetChatAgentMessageRecorder(recorder)
}

// IsOrdinaryNotificationMessage reports whether a row is a finished human-facing
// conversation message. Emission and inbox authorization share exactly this
// part of the unread rule; each adds its own sender and enablement conditions.
func IsOrdinaryNotificationMessage(row *models.ChatMessage) bool {
	return isStandardKind(row.Kind) &&
		row.Role != proto_models.MessageRole_MESSAGE_ROLE_SYSTEM && row.Role != proto_models.MessageRole_MESSAGE_ROLE_TOOL &&
		row.StreamState != proto_models.MessageStreamState_MESSAGE_STREAM_STATE_STREAMING && row.StreamState != proto_models.MessageStreamState_MESSAGE_STREAM_STATE_FAILED
}

// IsApprovalNotificationMessage reports whether a row has the shape of an agent
// tool-call approval, whatever its current status. Emission additionally
// requires a pending call; authorization keeps resolved records readable.
func IsApprovalNotificationMessage(row *models.ChatMessage) bool {
	return row.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL && row.ToolCall != nil && row.AgentRunID != nil
}

// AuthorizeNotificationSnapshot consumes only this evaluation's freshly loaded data
// and evaluates every application permission callback as the actual recipient.
// Notification workers must not reuse a producer's actor, membership cache, or
// organization-only viewer grant; a nil snapshot denies.
func (h *Handler[U]) AuthorizeNotificationSnapshot(ctx context.Context, actor U, row *models.ChatMessage, approval bool, sources *persistence.ChatNotificationSources) (bool, error) {
	if row == nil || row.DeletedAt.Valid {
		return false, nil
	}
	ctx, channel, participant, err := h.authorizeNotificationRecipient(ctx, actor, row.ChannelID, sources, row)
	if channel == nil || err != nil {
		return false, err
	}
	if row.HiddenFromUsers != nil && *row.HiddenFromUsers {
		if _, err := h.resolveReadOptions(ctx, actor, channel, true); err != nil {
			return false, notificationPermissionError(err)
		}
	}
	if row.ParentMessageID != nil && *row.ParentMessageID != 0 {
		parent := sources.Messages[*row.ParentMessageID]
		if parent == nil || parent.ChannelID != row.ChannelID {
			return false, nil
		}
		if parent.HiddenFromUsers != nil && *parent.HiddenFromUsers {
			if _, err := h.resolveReadOptions(ctx, actor, channel, true); err != nil {
				return false, notificationPermissionError(err)
			}
		}
		parentMessage := h.converter.PersistenceChatMessageToService(parent)
		if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{Actor: actor, Channel: channel, Message: parentMessage, Operation: ports.ChatPermissionGetMessage, MessageIDs: []uint64{parent.ID}}); err != nil {
			return false, notificationPermissionError(err)
		}
	}
	if !approval {
		return row.SenderID != uint64(actor.GetID()) && IsOrdinaryNotificationMessage(row), nil
	}
	if !IsApprovalNotificationMessage(row) {
		return false, nil
	}
	run := sources.Runs[*row.AgentRunID]
	if run == nil || run.ChannelID != row.ChannelID {
		return false, nil
	}
	policy := ports.AgentApprovalPolicy{ResumeOnDenial: true}
	if run.AgentSessionID != nil && run.AgentSessionConfigVersion != nil {
		policy = h.decodeRunApprovalPolicy(ctx, run, sources.Policies[[2]uint64{*run.AgentSessionID, *run.AgentSessionConfigVersion}])
	}

	// The fresh participant row determines admin eligibility; disable the cached
	// branch in the shared resolver and evaluate its equivalent here.
	allowAdmin := policy.AllowChannelAdminsToResolve && participant.Role >= proto_models.ChannelRole_CHANNEL_ROLE_ADMIN
	policy.AllowChannelAdminsToResolve = false
	allowed, err := h.canResolveToolCall(ctx, uint64(actor.GetID()), actor.GetUserType(), run, policy)
	return allowed || allowAdmin, err
}

// Only known access denials establish that a recipient is ineligible. Unknown
// callback errors must abort emission or retry delivery, rather than silently
// discarding durable notifications during an authorization-service outage.
func notificationPermissionError(err error) error {
	for _, denied := range []error{
		serviceErrors.ErrInsufficientPermissions,
		serviceErrors.ErrInsufficientOrganizationPermissions,
		serviceErrors.ErrChatNotMember,
		serviceErrors.ErrUserNotMember,
		serviceErrors.ErrUserBanned,
		serviceErrors.ErrAuthenticationRequired,
		serviceErrors.ErrChatChannelNotFound,
		serviceErrors.ErrChatMessageNotFound,
		serviceErrors.ErrUserNotFound,
		persistence.ErrChatChannelNotFound,
		persistence.ErrChatMessageNotFound,
		persistence.ErrUserNotFound,
	} {
		if errors.Is(err, denied) {
			return nil
		}
	}
	return err
}

// AuthorizeChannelActivitySnapshot checks current channel access as the recipient
// using only this evaluation's freshly loaded rows; a nil snapshot denies.
// Source-message visibility is checked at emission; an activity entry survives
// deletion of that source and therefore never reloads a message here.
func (h *Handler[U]) AuthorizeChannelActivitySnapshot(ctx context.Context, actor U, channelID uint64, sources *persistence.ChatNotificationSources) (bool, error) {
	_, channel, _, err := h.authorizeNotificationRecipient(ctx, actor, channelID, sources, nil)
	return channel != nil && err == nil, err
}

// authorizeNotificationRecipient is the recipient gate shared by message and
// activity authorization: an active human recipient with a live membership of a
// live snapshot channel, then GetChannel and ListMessages evaluated as that
// recipient. A source message adds its identity to both and a GetMessage check;
// activity passes none. It returns the recipient-scoped context, the channel
// projection and the participant row for the caller's remaining checks; a nil
// channel means denied.
func (h *Handler[U]) authorizeNotificationRecipient(ctx context.Context, actor U, channelID uint64, sources *persistence.ChatNotificationSources, source *models.ChatMessage) (context.Context, *common_service_models.ChatChannel, *models.ChatChannelParticipant, error) {
	if sources == nil || actor.GetID() == 0 || actor.IsBot() || actor.IsBanned() || h.chatDisabled() {
		return ctx, nil, nil, nil
	}
	row, participant := sources.Channels[channelID], sources.Membership(channelID, uint64(actor.GetID()))
	if row == nil || row.DeletedAt.Valid || participant == nil || participant.LeftAt != nil {
		return ctx, nil, nil, nil
	}
	ctx = scontext.CopySRouterContext[common_service_models.UserIdType, U](ctx, ctx)
	ctx = scontext.WithUser[common_service_models.UserIdType](ctx, &actor)
	ctx = scontext.WithUserID[common_service_models.UserIdType, U](ctx, actor.GetID())
	// Every recipient in a batch is evaluated against the same snapshot rows; the converter owns
	// each projection it returns, so a callback that writes through one cannot reach the next.
	request := ports.ChatPermissionRequest[U]{Actor: actor, Channel: h.converter.PersistenceChatChannelToService(row)}
	operations := []ports.ChatPermissionOperation{ports.ChatPermissionGetChannel, ports.ChatPermissionListMessages}
	if source != nil {
		request.Message = h.converter.PersistenceChatMessageToService(source)
		if source.ParentMessageID != nil {
			request.ParentMessageID = *source.ParentMessageID
		}
		operations = append(operations, ports.ChatPermissionGetMessage)
	}
	for _, op := range operations {
		request.Operation = op
		if source != nil {
			// A fresh slice per callback, so one callback cannot rewrite the next one's input.
			request.MessageIDs = []uint64{source.ID}
		}
		if err := h.authorize(ctx, request); err != nil {
			return ctx, nil, nil, notificationPermissionError(err)
		}
	}
	return ctx, request.Channel, participant, nil
}

func (h *Handler[U]) retireChannelActivity(ctx context.Context, channelID, recipientID uint64) error {
	if h.notificationSink == nil {
		return nil
	}
	return h.notificationSink.ChatActivityRetired(ctx, channelID, recipientID)
}
