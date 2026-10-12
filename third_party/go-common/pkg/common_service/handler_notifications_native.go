package common_service

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/notificationnative"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util/notificationkey"
	"github.com/Suhaibinator/go-common/pkg/config"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
)

const NotificationTypeChatUnread = uint32(pb.NativeNotificationType_NATIVE_NOTIFICATION_TYPE_UNREAD_CHAT)
const NotificationTypeChatApprovalRequired = uint32(pb.NativeNotificationType_NATIVE_NOTIFICATION_TYPE_TOOL_APPROVAL)

// Native chat payloads share one message type; each caller's role fixes the
// shape it accepts. Unread inbox rows are channel-only, while approval rows and
// the target inside a source envelope name a message.

// parseChatChannel decodes any native chat payload that names a channel. Use it
// only where both shapes are acceptable and the caller needs no message.
func parseChatChannel(payload []byte) (*pb.ChatNotificationPayload, error) {
	p := new(pb.ChatNotificationPayload)
	if err := proto.Unmarshal(payload, p); err != nil || p.ChannelId == 0 {
		return nil, notification.ErrInvalid
	}
	return p, nil
}

// parseChatTarget decodes a message target: an approval inbox payload or the
// target carried by a source envelope. It requires message_id.
func parseChatTarget(payload []byte) (*pb.ChatNotificationPayload, error) {
	p, err := parseChatChannel(payload)
	if err != nil {
		return nil, err
	}
	if p.MessageId == 0 {
		return nil, notification.ErrInvalid
	}
	return p, nil
}

// parseChannelActivityPayload decodes an unread inbox payload. It names only the
// channel and rejects message, thread, run, and broadcast fields.
func parseChannelActivityPayload(payload []byte) (*pb.ChatNotificationPayload, error) {
	p, err := parseChatChannel(payload)
	if err != nil {
		return nil, err
	}
	if p.MessageId != 0 || p.ParentMessageId != 0 || p.AgentRunId != 0 || p.BroadcastToChannel {
		return nil, notification.ErrInvalid
	}
	return p, nil
}

// chatNotificationActivityScope serves both native chat types, whose payload
// shapes Validate has already fixed; an unread payload has no thread fields.
func chatNotificationActivityScope(_ context.Context, b []byte) (string, error) {
	p, err := parseChatChannel(b)
	if err != nil {
		return "", err
	}
	parent := p.ParentMessageId
	// A broadcast reply is also visible on the main timeline, so a channel viewer
	// must be able to suppress it. Thread viewers acknowledge both scopes, and
	// the payload keeps the thread root for navigation and source validation.
	if p.BroadcastToChannel {
		parent = 0
	}
	return notificationkey.ChatActivityScope(p.ChannelId, parent)
}

func unreadChatNotificationGroupKey(_ context.Context, b []byte) (string, error) {
	p, err := parseChannelActivityPayload(b)
	if err != nil {
		return "", err
	}
	return notificationkey.UnreadChatGroupKey(p.ChannelId)
}

func approvalNotificationGroupKey(_ context.Context, b []byte) (string, error) {
	p, err := parseChatTarget(b)
	if err != nil {
		return "", err
	}
	return notificationkey.ApprovalGroupKey(p.MessageId)
}

func (h *CommonServiceHandler[U]) registerNativeNotificationTypes() error {
	if h.notificationHandler == nil {
		return nil
	}
	if err := h.registerInvitationNotificationType(); err != nil {
		return err
	}
	for _, id := range []uint32{NotificationTypeChatUnread, NotificationTypeChatApprovalRequired} {
		approval := id == NotificationTypeChatApprovalRequired
		name := "chat_unread"
		fallback := "New activity in this channel"
		parse := parseChannelActivityPayload
		if approval {
			name = "chat_approval_required"
			fallback = "A tool call needs your approval"
			parse = parseChatTarget
		}
		typ := notification.Type{ID: id, Name: name, ReusableGroup: !approval, EmailContentUsesPayloadOnly: !approval, DefaultPolicy: config.NotificationPolicy{AlertsEnabled: true, AlertCooldown: time.Minute, EmailMode: "delayed", EmailDelay: 5 * time.Minute, GroupWhileUnread: true, CancelOnRead: true, CancelOnResolution: true},
			FanoutMode: func(context.Context) (string, error) {
				if h.runtimeConfig == nil {
					return "", nil
				}
				v := h.runtimeConfig()
				if approval {
					return v.NotificationChatApprovalRequiredFanoutMode(), nil
				}
				return v.NotificationChatUnreadFanoutMode(), nil
			},
			Validate: func(_ context.Context, b []byte) error {
				p, e := parse(b)
				if e == nil && approval && p.AgentRunId == 0 {
					return notification.ErrInvalid
				}
				return e
			},
			EvaluateSources: func(ctx context.Context, candidates []notification.SourceCandidate, options notification.SourceOptions) ([]notification.SourceDecision, error) {
				if !approval {
					return h.evaluateChannelActivitySources(ctx, candidates, options)
				}
				return h.evaluateNativeNotificationSources(ctx, candidates, options, approval)
			},
			GroupKey: unreadChatNotificationGroupKey, ActivityScope: chatNotificationActivityScope,
			DeliveryPreference: func(ctx context.Context, user uint64, b []byte) (notification.DeliveryPreference, error) {
				return h.nativeChatDeliveryPreference(ctx, user, b, approval)
			},
			RenderEmail: func(ctx context.Context, record *models.Notification) (notification.Email, error) {
				email := notification.Email{Subject: fallback, Body: fallback + ". Open your notification inbox to view it."}
				if h.notificationLinkBuilder != nil {
					payload, err := parse(record.Payload)
					if err != nil {
						return email, err
					}
					target, err := h.notificationLinkBuilder(ctx, record.UserID, record.Type, payload)
					if err != nil {
						return email, err
					}
					parsed, err := url.Parse(target)
					if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.User != nil {
						return email, notification.ErrInvalid
					}
					email.Body += "\n\n" + parsed.String()
				}
				return email, nil
			},
		}
		if approval {
			typ.DefaultPolicy.EmailDelay = time.Minute
			typ.GroupKey = approvalNotificationGroupKey
		}
		if err := h.notificationHandler.RegisterNative(notificationnative.Native(), typ); err != nil {
			return err
		}
	}
	// Chat and bot are optional modules; a disabled one has no sink to install.
	if h.chatHandler != nil {
		h.chatHandler.SetNotificationSink(h)
	}
	if h.botDispatcher != nil {
		h.botDispatcher.SetNotificationSink(h)
	}
	return h.registerNativeNotificationProducers()
}

func (h *CommonServiceHandler[U]) nativeNotificationMessage(ctx context.Context, b []byte) (*models.ChatMessage, error) {
	p, err := parseChatTarget(b)
	if err != nil {
		return nil, err
	}
	row, err := h.commonPersistenceHandler.GetChatMessageByIDWithOptions(ctx, p.MessageId, persistence.ChatMessageReadOptions{VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript})
	if errors.Is(err, persistence.ErrChatMessageNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !nativeNotificationMessageMatches(row, p) {
		return nil, nil
	}
	return row, nil
}

// nativeChatNotificationParticipant reads current preferences independently of the inbox payload.
func (h *CommonServiceHandler[U]) nativeChatNotificationParticipant(ctx context.Context, user, channelID uint64) (*models.ChatChannelParticipant, error) {
	return h.commonPersistenceHandler.GetChatNotificationParticipant(ctx, channelID, user)
}

// nativeChatDeliveryPreference samples the participant once when the current
// source evaluator cannot supply a snapshot, and before alert eligibility checks.
// Approvals honor mute, but never require a mention or the channel's
// unread-notification opt-in.
func (h *CommonServiceHandler[U]) nativeChatDeliveryPreference(ctx context.Context, user uint64, b []byte, approval bool) (notification.DeliveryPreference, error) {
	payload, err := parseChatChannel(b)
	if err != nil {
		return notification.DeliveryPreference{}, err
	}
	if !approval {
		enabled, err := h.chatUnreadNotificationsEnabled(ctx, payload.ChannelId)
		if err != nil || !enabled {
			return notification.DeliveryPreference{Suppressed: true}, err
		}
	}
	participant, err := h.nativeChatNotificationParticipant(ctx, user, payload.ChannelId)
	if err != nil {
		return notification.DeliveryPreference{Suppressed: true}, err
	}
	return nativeChatParticipantDeliveryPreference(participant, approval), nil
}

func nativeChatParticipantDeliveryPreference(participant *models.ChatChannelParticipant, approval bool) notification.DeliveryPreference {
	preference := notification.DeliveryPreference{Suppressed: participant == nil || participant.LeftAt != nil || participant.NotificationMode == pb.ChannelNotificationMode_CHANNEL_NOTIFICATION_MODE_MUTED}
	if !approval && participant != nil && participant.NotificationMode == pb.ChannelNotificationMode_CHANNEL_NOTIFICATION_MODE_MENTIONS_ONLY {
		preference.Tag = nativeChatMentionDeliveryTag
	}
	return preference
}

// Both values come from the same operation-local rows used to authorize access.
// No preference observation is retained across source evaluation boundaries.
func nativeChatSnapshotDeliveryPreference(sources *persistence.ChatNotificationSources, channelID, userID uint64, approval bool) *notification.DeliveryPreference {
	preference := nativeChatParticipantDeliveryPreference(sources.Membership(channelID, userID), approval)
	if !approval {
		channel := sources.Channels[channelID]
		preference.Suppressed = preference.Suppressed || channel == nil || !channel.UnreadNotificationsEnabled
	}
	return &preference
}

// nativeApprovalResolved loads the current source rows for one approval payload
// and applies the shared resolution predicate. Fan-out preparation calls it
// before enumerating approvers; inbox reads resolve from the batch snapshot.
func (h *CommonServiceHandler[U]) nativeApprovalResolved(ctx context.Context, b []byte) (bool, error) {
	row, err := h.nativeNotificationMessage(ctx, b)
	if err != nil {
		return false, err
	}
	now := time.Now()
	// The run is only consulted for a still-pending call, so skip its read when
	// the message alone already resolves the source.
	if nativeApprovalMessageResolved(row, now) {
		return true, nil
	}
	run, err := h.commonPersistenceHandler.GetChatAgentRunByID(ctx, *row.AgentRunID)
	if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return nativeApprovalSourceResolved(row, run, now), nil
}

// ChatMessageRecorded records the notification emission for a durable transcript
// row inside the ambient source transaction, before that transaction commits.
// It is never called for ephemeral streaming deltas.
func (h *CommonServiceHandler[U]) ChatMessageRecorded(ctx context.Context, messageID uint64) error {
	if h == nil || h.notificationHandler == nil {
		return nil
	}
	err := h.notificationHandler.WithEmissionOperation(ctx, func(tx context.Context, op *notification.EmissionOperation) error {
		row, err := h.commonPersistenceHandler.GetChatMessageByIDWithOptions(tx, messageID, persistence.ChatMessageReadOptions{VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript})
		if err != nil || row == nil {
			return err
		}
		typ := NotificationTypeChatUnread
		// Only a still-pending call emits; inbox authorization accepts the same
		// shape after resolution so existing records stay readable.
		approval := chatpkg.IsApprovalNotificationMessage(row) && row.ToolCall.ApprovalStatus == pb.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING
		if approval {
			typ = NotificationTypeChatApprovalRequired
		} else if !chatpkg.IsOrdinaryNotificationMessage(row) {
			return nil
		}
		if !approval {
			enabled, err := h.chatUnreadNotificationsEnabled(tx, row.ChannelID)
			if err != nil || !enabled {
				return err
			}
		}
		payload := &pb.ChatNotificationPayload{ChannelId: row.ChannelID, MessageId: row.ID, BroadcastToChannel: row.BroadcastToChannel}
		if row.ParentMessageID != nil {
			payload.ParentMessageId = *row.ParentMessageID
		}
		if row.AgentRunID != nil {
			payload.AgentRunId = *row.AgentRunID
		}
		b, err := marshalNativeChatNotificationSource(row, payload, approval)
		if err != nil {
			return err
		}
		fallback := "New activity in this channel"
		if approval {
			fallback = "A tool call needs your approval"
		}
		sourceTime := row.CreatedAt
		// A streamed message's source event is its terminal write, not the shell
		// created before generation. Backlog age starts when completion occurred.
		if row.StreamState == pb.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE && row.UpdatedAt.After(sourceTime) {
			sourceTime = row.UpdatedAt
		}
		_, err = op.EmitNativeEvent(tx, notificationnative.Native(), notification.EventInput{Producer: nativeChatNotificationProducer, Type: typ, Payload: b, FallbackText: fallback, SourceKey: fmt.Sprintf("message:%d", row.ID), SourceTime: sourceTime})
		return err
	})
	// The operation samples the module gate once, before reading the source.
	if errors.Is(err, notification.ErrDisabled) {
		return nil
	}
	return err
}

func (h *CommonServiceHandler[U]) ChatApprovalResolved(ctx context.Context, messageID uint64) error {
	if h == nil || h.notificationHandler == nil {
		return nil
	}
	return h.notificationHandler.ResolveSource(ctx, NotificationTypeChatApprovalRequired, fmt.Sprintf("message:%d", messageID))
}

// Cancellation owns a separate transaction so the control-plane mutation and
// dismissal of all pending approval notifications succeed or fail together.
func (h *CommonServiceHandler[U]) cancelAgentRunWithNotifications(ctx context.Context, runID uint64, cancellation ports.AgentRunCancellation) (*persistence.ChatAgentRunCancellation, []persistence.PostCommitHook, error) {
	cancel := func(ctx context.Context) (*persistence.ChatAgentRunCancellation, error) {
		return h.commonPersistenceHandler.CancelChatAgentRun(ctx, runID, persistence.ActiveChatAgentRunStatuses(), cancellation.Reason, cancellation.Code)
	}
	if h.notificationHandler == nil {
		cancelled, err := cancel(ctx)
		return cancelled, nil, err
	}
	// The caller drains the hooks itself: notification publishes must follow the cancellation of
	// every local execution lane.
	var cancelled *persistence.ChatAgentRunCancellation
	hooks, err := h.ownedTransaction(ctx, func(txCtx context.Context) error {
		var err error
		if cancelled, err = cancel(txCtx); err != nil || cancelled == nil {
			return err
		}
		return h.resolveCancelledRunNotifications(txCtx, cancelled)
	})
	if err != nil {
		return nil, nil, err
	}
	return cancelled, hooks, nil
}

// resolveCancelledRunNotifications resolves the approval-required notifications of the approvals
// a run's cancellation expired, in the cancellation's transaction. Every other approval was
// resolved, with its notification, when a person decided it.
func (h *CommonServiceHandler[U]) resolveCancelledRunNotifications(ctx context.Context, cancellation *persistence.ChatAgentRunCancellation) error {
	for _, toolCall := range cancellation.SettledToolCalls {
		if toolCall.ToolCall == nil || toolCall.ToolCall.ApprovalStatus != pb.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED {
			continue
		}
		if err := h.ChatApprovalResolved(ctx, toolCall.ID); err != nil {
			return err
		}
	}
	return nil
}

// Keep policy separate from authorization: disabling notifications does not hide inbox history.
func (h *CommonServiceHandler[U]) chatUnreadNotificationsEnabled(ctx context.Context, channelID uint64) (bool, error) {
	return h.commonPersistenceHandler.ChatUnreadNotificationsEnabled(ctx, channelID)
}

// ChatActivityRetired runs in the membership mutation's ambient transaction. A
// zero recipient queues durable bounded cleanup after channel deletion.
func (h *CommonServiceHandler[U]) ChatActivityRetired(ctx context.Context, channelID, recipientID uint64) error {
	if h.notificationHandler == nil {
		return nil
	}
	scope, err := notificationkey.ChatActivityScope(channelID, 0)
	if err != nil {
		return err
	}
	if recipientID != 0 {
		return h.notificationHandler.RetireChatActivityScopeForRecipients(ctx, scope, []persistence.NotificationRecipientType{{UserID: recipientID, Type: NotificationTypeChatUnread}, {UserID: recipientID, Type: NotificationTypeChatApprovalRequired}})
	}
	return h.notificationHandler.EnqueueChatActivityRetirement(ctx, channelID, scope, []uint32{NotificationTypeChatUnread, NotificationTypeChatApprovalRequired})
}
