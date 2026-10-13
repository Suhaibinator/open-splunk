package chat

import (
	"context"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type inferenceEventPersistence interface {
	GetChatChannelFeatures(context.Context, uint64) (common_persistence_models.ChatChannelFeatures, error)
	RecordChatInferenceCall(context.Context, *common_persistence_models.ChatInferenceCall) (bool, error)
	GetChatMessageByIDWithOptions(context.Context, uint64, persistence.ChatMessageReadOptions) (*common_persistence_models.ChatMessage, error)
	GetMessageInferenceAggregate(context.Context, uint64) (*persistence.MessageInferenceAggregate, error)
	GetChatInferenceTotals(context.Context, uint64) (*common_persistence_models.ChatInferenceTotals, error)
	GetChatInferenceTotalsForSessions(context.Context, []uint64) (*common_persistence_models.ChatInferenceTotals, error)
}

// InferenceEventConverter maps the post-commit reads an inference usage event carries.
type InferenceEventConverter interface {
	PersistenceChatInferenceTotalsToProto(uint64, *common_persistence_models.ChatInferenceTotals) *proto_models.ChatInferenceTotals
	PersistenceMessageInferenceAggregateToProto(*persistence.MessageInferenceAggregate) *proto_models.MessageInferenceSummary
}

// RecordInferenceCallWithEvent records one idempotent ledger entry and, after commit, publishes the
// channel's cumulative totals, the session's when the call has one, and the linked message's
// summary when there is a visible one, unless the channel does not show token usage. Unlinked calls
// publish totals alone. Totals reach every
// channel reader, as their REST reads do; a hidden message's summary goes in a second event to the
// hidden-authorized audience only. Both runtime and compaction writes use this seam. Delivery
// failures never turn a successful ledger write into a failure.
func RecordInferenceCallWithEvent(
	ctx context.Context, p inferenceEventPersistence, converter InferenceEventConverter,
	publisher ports.ChatEventPublisher, logger *zap.Logger,
	row *common_persistence_models.ChatInferenceCall,
) error {
	inserted, err := p.RecordChatInferenceCall(ctx, row)
	if err != nil {
		return err
	}
	if !inserted || publisher == nil || converter == nil {
		return nil
	}
	channelID := row.ChannelID
	var messageID, sessionID uint64
	if row.MessageID != nil {
		messageID = *row.MessageID
	}
	if row.AgentSessionID != nil {
		sessionID = *row.AgentSessionID
	}
	logFailure := func(ctx context.Context, stage string, err error) {
		if logger != nil {
			appLogger.For(ctx, logger, chatLoggerName).Error("failed to publish inference usage update",
				zap.String(logkeys.Stage, stage), zap.Uint64(appLogger.KeyMessageID, messageID), zap.Uint64(appLogger.KeyChannelID, channelID),
				zap.Uint64(appLogger.KeyAgentSessionID, sessionID), zap.Error(err))
		}
	}
	err = ports.PublishChatEventsAfterCommit(ctx, publisher, func(postCtx context.Context) []*ports.ChatEvent {
		features, err := p.GetChatChannelFeatures(postCtx, channelID)
		if err != nil {
			logFailure(postCtx, "read_channel_features", err)
			return nil
		}
		if !features.Has(common_persistence_models.ChatChannelSupportInferenceUsage) {
			return nil
		}
		totals, err := p.GetChatInferenceTotals(postCtx, channelID)
		if err != nil {
			logFailure(postCtx, "read_channel_inference_totals", err)
			return nil
		}
		update := &proto_models.ChatInferenceUsageUpdate{ChannelId: channelID, ChannelTotals: converter.PersistenceChatInferenceTotalsToProto(channelID, totals)}
		if sessionID != 0 {
			sessionTotals, err := p.GetChatInferenceTotalsForSessions(postCtx, []uint64{sessionID})
			if err != nil {
				logFailure(postCtx, "read_session_inference_totals", err)
				return nil
			}
			update.AgentSessionId = sessionID
			update.SessionTotals = converter.PersistenceChatInferenceTotalsToProto(channelID, sessionTotals)
		}
		newEvent := func(update *proto_models.ChatInferenceUsageUpdate, hidden bool) *ports.ChatEvent {
			return &ports.ChatEvent{Type: proto_models.ChatEventType_CHAT_EVENT_TYPE_INFERENCE_USAGE_UPDATED,
				OccurredAt: time.Now(), InferenceUsage: update, InferenceUsageHidden: hidden}
		}
		visible := newEvent(update, false)
		if messageID == 0 {
			return []*ports.ChatEvent{visible}
		}
		msg, err := p.GetChatMessageByIDWithOptions(postCtx, messageID, persistence.ChatMessageReadOptions{
			ChannelID: channelID, VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript,
		})
		if err != nil {
			logFailure(postCtx, "read_inference_message", err)
			return []*ports.ChatEvent{visible}
		}
		// Provider-state rows are opaque service protocol data, never a realtime
		// transcript audience (including bots and administrators).
		if msg == nil || msg.Kind == proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE {
			return []*ports.ChatEvent{visible}
		}
		aggregate, err := p.GetMessageInferenceAggregate(postCtx, messageID)
		if err != nil {
			logFailure(postCtx, "read_message_inference_aggregate", err)
			return []*ports.ChatEvent{visible}
		}
		summary := converter.PersistenceMessageInferenceAggregateToProto(aggregate)
		if summary == nil || summary.CallCount == 0 {
			return []*ports.ChatEvent{visible}
		}
		if msg.HiddenFromUsers == nil || !*msg.HiddenFromUsers {
			update.MessageSummary = summary
			return []*ports.ChatEvent{visible}
		}
		hiddenUpdate := proto.CloneOf(update)
		hiddenUpdate.MessageSummary = summary
		return []*ports.ChatEvent{visible, newEvent(hiddenUpdate, true)}
	})
	if err != nil {
		logFailure(ctx, "publish_inference_usage_update", err)
	}
	return nil
}
