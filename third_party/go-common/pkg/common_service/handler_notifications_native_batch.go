package common_service

import (
	"context"
	"maps"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// evaluateNativeNotificationSources retains full callback inputs but shares their
// database hydration inside this evaluation only. Eight distinct sources per
// sub-batch bound large transcript payload hydration independently of candidate
// count; application permission callbacks remain recipient-specific. Application users
// are loaded in batches and reused only within this evaluation.
func (h *CommonServiceHandler[U]) evaluateNativeNotificationSources(ctx context.Context, candidates []notification.SourceCandidate, options notification.SourceOptions, approval bool) ([]notification.SourceDecision, error) {
	return h.evaluateNativeNotificationSourcesObserved(ctx, candidates, options, approval, nil)
}

// observed is internal preparation state, copied before permission callbacks can
// yield. Materialization compares it against fresh source data under its locks.
func (h *CommonServiceHandler[U]) evaluateNativeNotificationSourcesObserved(ctx context.Context, candidates []notification.SourceCandidate, options notification.SourceOptions, approval bool, observed []persistence.ChatNotificationSourceState) ([]notification.SourceDecision, error) {
	decisions := make([]notification.SourceDecision, len(candidates))
	// A disabled chat module has no channel authorizer: nobody is authorized.
	if h.chatHandler == nil {
		return decisions, nil
	}

	parsed := make([]*pb.ChatNotificationPayload, len(candidates))
	decoded := map[string]*pb.ChatNotificationPayload{}
	for i, c := range candidates {
		key := string(c.Payload)
		p, ok := decoded[key]
		if !ok {
			var err error
			p, err = parseChatTarget(c.Payload)
			if err != nil {
				return nil, err
			}
			decoded[key] = p
		}
		parsed[i] = p
	}
	users := make(map[common_service_models.UserIdType]U)
	lookedUp := make(map[common_service_models.UserIdType]bool)
	for start := 0; start < len(candidates); {
		ids := map[uint64]bool{}
		end := start
		for end < len(candidates) && end-start < persistence.ChatNotificationCandidateBatchSize {
			id := parsed[end].MessageId
			if !ids[id] && len(ids) == persistence.ChatNotificationSourceBatchSize {
				break
			}
			ids[id] = true
			end++
		}
		requests := make([]persistence.ChatNotificationCandidate, end-start)
		for i := start; i < end; i++ {
			requests[i-start] = persistence.ChatNotificationCandidate{MessageID: parsed[i].MessageId, RecipientID: candidates[i].RecipientID}
		}
		sources, err := h.commonPersistenceHandler.LoadChatNotificationSourcesWithOptions(ctx, requests, persistence.ChatNotificationSourceOptions{IncludeApproval: approval})
		if err != nil {
			return nil, err
		}
		if sources == nil {
			return nil, appLogger.InvariantError("notification_source_snapshot_exists", notification.ErrInvalid, zap.String(logkeys.Stage, "evaluate_native_notification_sources"), zap.String(logkeys.Expected, "fresh source snapshot"), zap.String(logkeys.Actual, "nil snapshot"))
		}
		var userIDs []common_service_models.UserIdType
		for i := start; i < end; i++ {
			c := candidates[i]
			core := sources.Users[c.RecipientID]
			id := common_service_models.UserIdType(c.RecipientID)
			if nativeNotificationMessageMatches(sources.Messages[parsed[i].MessageId], parsed[i]) && core != nil && core.ID == c.RecipientID && !core.Banned && !core.DeletedAt.Valid && !lookedUp[id] {
				userIDs = append(userIDs, id)
				lookedUp[id] = true
			}
		}
		batchUsers, err := h.userCallbacks.LookupUsers(ctx, userIDs)
		if err != nil {
			return nil, err
		}
		maps.Copy(users, batchUsers)
		var views []nativeChatViewRequest
		for i := start; i < end; i++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			c := candidates[i]
			row := sources.Messages[parsed[i].MessageId]
			if !nativeNotificationMessageMatches(row, parsed[i]) {
				continue
			}
			core := sources.Users[c.RecipientID]
			if core == nil || core.ID != c.RecipientID || core.Banned || core.DeletedAt.Valid {
				continue
			}
			actor, exists := users[common_service_models.UserIdType(c.RecipientID)]
			if !exists {
				continue
			}
			if observed != nil {
				observed[i] = nativeNotificationSourceState(row, sources.Messages[parsed[i].ParentMessageId], !approval)
			}
			if options.IncludeDeliveryPreference {
				decisions[i].DeliveryPreference = nativeChatSnapshotDeliveryPreference(sources, row.ChannelID, c.RecipientID, approval)
			}
			allowed, err := h.chatHandler.AuthorizeNotificationSnapshot(ctx, actor, row, approval, sources)
			if err != nil {
				return nil, err
			}
			decisions[i].Authorized = allowed
			if allowed && options.IncludeView {
				views = append(views, nativeChatViewRequest{decision: i, recipient: c.RecipientID, channel: sources.Channels[row.ChannelID]})
			}
			if allowed && approval && options.CheckResolution {
				decisions[i].Resolved = nativeApprovalSourceResolved(row, sources.Runs[parsed[i].AgentRunId], time.Now())
			}
		}
		// A tool approval names a specific call, so its view carries no
		// latest-message preview.
		if err := h.setNativeChatNotificationViews(ctx, decisions, views, false); err != nil {
			return nil, err
		}
		start = end
	}
	return decisions, nil
}

func nativeNotificationSourceState(row, parent *models.ChatMessage, requireUnread bool) persistence.ChatNotificationSourceState {
	state := persistence.ChatNotificationSourceState{RequireUnread: requireUnread}
	if row != nil {
		state.Hidden = row.HiddenFromUsers != nil && *row.HiddenFromUsers
		if row.ParentMessageID != nil {
			state.ParentMessageID = *row.ParentMessageID
		}
	}
	if parent != nil {
		state.ParentHidden = parent.HiddenFromUsers != nil && *parent.HiddenFromUsers
	}
	return state
}
func nativeNotificationMessageMatches(row *models.ChatMessage, p *pb.ChatNotificationPayload) bool {
	if row == nil || row.ID != p.MessageId || row.ChannelID != p.ChannelId || row.DeletedAt.Valid {
		return false
	}
	var parent, run uint64
	if row.ParentMessageID != nil {
		parent = *row.ParentMessageID
	}
	if row.AgentRunID != nil {
		run = *row.AgentRunID
	}
	return parent == p.ParentMessageId && run == p.AgentRunId && row.BroadcastToChannel == p.BroadcastToChannel
}

// nativeApprovalSourceResolved is the single resolution rule for an approval
// source: the message part first, then the run's terminal state.
func nativeApprovalSourceResolved(row *models.ChatMessage, run *models.ChatAgentRun, now time.Time) bool {
	if nativeApprovalMessageResolved(row, now) {
		return true
	}
	return run == nil || run.CompletedAt != nil || run.Status == pb.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_CANCELLED || run.Status == pb.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED || run.Status == pb.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED
}

// nativeApprovalMessageResolved is the part of the rule decidable from the tool
// call alone; a false result guarantees row.AgentRunID is set.
func nativeApprovalMessageResolved(row *models.ChatMessage, now time.Time) bool {
	if row == nil || row.ToolCall == nil || row.AgentRunID == nil {
		return true
	}
	if row.ToolCall.ApprovalStatus != pb.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING {
		return true
	}
	return row.ToolCall.ApprovalExpiresAt != nil && !row.ToolCall.ApprovalExpiresAt.After(now)
}
