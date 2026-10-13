package chat

import (
	"context"
	"errors"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// Usage read service methods back the per-channel/session/run/message cost views. They are read-only
// and return zero-valued results (not errors) when no inference ledger rows exist — absence of agent
// activity is not an error. Authority requires channel read access, application authorization, and
// the channel's support_inference_usage feature.

// GetChatInferenceTotals returns the aggregate inference totals cache for a channel. Returns a
// zero-valued totals message (not an error) when the channel has no recorded inference calls.
func (h *Handler[U]) GetChatInferenceTotals(ctx context.Context, channelID uint64) (*proto_models.ChatInferenceTotals, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.usageReadAuthorized(ctx, actor, channelID); err != nil {
		return nil, err
	}

	totals, err := h.persistenceHandler.GetChatInferenceTotals(ctx, channelID)
	if err != nil {
		if errors.Is(err, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}
	// Nil totals (no rows) → a zero-valued message scoped to the channel.
	out := h.converter.PersistenceChatInferenceTotalsToProto(channelID, totals)
	return out, nil
}

// GetAgentSessionUsage returns totals scoped to an agent session plus breakdowns by run,
// purpose, provider, and model. Returns zero-valued usage when the session has no inference rows.
// With includeChildren, a delegation parent's children are folded into every dimension and the
// by_session axis lists the parent first and then each child (agent_type filled from the session);
// a child session, or a parent without children, reports itself with includes_children=false.
func (h *Handler[U]) GetAgentSessionUsage(
	ctx context.Context,
	agentSessionID uint64,
	byRunPagination *proto_models.PaginationRequest,
	byModelPagination *proto_models.PaginationRequest,
	includeChildren bool,
) (*proto_models.AgentSessionUsage, *proto_models.PaginationResponse, *proto_models.PaginationResponse, error) {
	if h.chatDisabled() {
		return nil, nil, nil, serviceErrors.ErrChatDisabled
	}
	if agentSessionID == 0 {
		return nil, nil, nil, serviceErrors.ErrInvalidRequest
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, agentSessionID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return nil, nil, nil, serviceErrors.ErrChatAgentSessionNotFound
		}
		return nil, nil, nil, serviceErrors.ErrDatabaseError
	}
	if err := h.usageReadAuthorized(ctx, actor, session.ChannelID); err != nil {
		if usageAccessDenied(err) {
			return nil, nil, nil, serviceErrors.ErrChatAgentSessionNotFound
		}
		return nil, nil, nil, err
	}

	// members is the roll-up set: the session alone, or a delegation parent followed by its
	// children oldest first. Children are read only when asked for; a child asking for its own
	// children is simply itself.
	members := []*common_persistence_models.ChatAgentSession{session}
	if includeChildren && session.IsMainTimeline() {
		children, err := h.persistenceHandler.ListChatAgentSessionsByParent(ctx, agentSessionID)
		if err != nil {
			return nil, nil, nil, mapAgentSessionLocationError(err)
		}
		members = append(members, children...)
	}
	sessionIDs := common_util.ApplyToEach(members, func(member *common_persistence_models.ChatAgentSession) uint64 { return member.ID })
	rollup := len(sessionIDs) > 1

	totals, err := h.persistenceHandler.GetChatInferenceTotalsForSessions(ctx, sessionIDs)
	if err != nil {
		if errors.Is(err, persistence.ErrDatabaseError) {
			return nil, nil, nil, serviceErrors.ErrDatabaseError
		}
		return nil, nil, nil, serviceErrors.ErrInternalError
	}

	totalsProto := h.converter.PersistenceChatInferenceTotalsToProto(session.ChannelID, totals)

	usage := &proto_models.AgentSessionUsage{
		AgentSessionId:   agentSessionID,
		ChannelId:        session.ChannelID,
		Totals:           totalsProto,
		IncludesChildren: rollup,
	}

	// The run and model dimensions are always paged: the request sanitizer
	// defaults and bounds both page sizes from configuration. A direct Go caller
	// that omits them would otherwise request an unbounded dimension.
	if byRunPagination.GetLimit() == 0 || byModelPagination.GetLimit() == 0 {
		return nil, nil, nil, appLogger.InvariantError("agent_session_usage_pages_bounded", serviceErrors.ErrInvalidRequest,
			zap.Uint64(appLogger.KeyAgentSessionID, agentSessionID),
			zap.Uint32("by_run_limit", byRunPagination.GetLimit()),
			zap.Uint32("by_model_limit", byModelPagination.GetLimit()))
	}

	// Every dimension shares one ledger aggregation, including failed/pre-message calls.
	requested := map[persistence.InferenceBreakdownDimension]*proto_models.PaginationRequest{
		persistence.InferenceBreakdownByRun:      byRunPagination,
		persistence.InferenceBreakdownByPurpose:  nil,
		persistence.InferenceBreakdownByProvider: nil,
		persistence.InferenceBreakdownByModel:    byModelPagination,
	}
	if rollup {
		requested[persistence.InferenceBreakdownBySession] = nil
	}
	dimensions, err := h.persistenceHandler.ListChatInferenceDimensionsForSessions(ctx, sessionIDs, requested)
	if err != nil {
		return nil, nil, nil, serviceErrors.ErrDatabaseError
	}
	convert := func(dim persistence.InferenceBreakdownDimension) []*proto_models.AgentSessionUsageBreakdown {
		return common_util.ApplyToEach(dimensions[dim].Rows, h.converter.PersistenceInferenceUsageBreakdownToProto)
	}
	usage.ByRun = convert(persistence.InferenceBreakdownByRun)
	usage.ByPurpose = convert(persistence.InferenceBreakdownByPurpose)
	usage.ByProvider = convert(persistence.InferenceBreakdownByProvider)
	usage.ByModel = convert(persistence.InferenceBreakdownByModel)
	if rollup {
		usage.BySession = h.sessionAxis(convert(persistence.InferenceBreakdownBySession), members)
	}
	return usage, dimensions[persistence.InferenceBreakdownByRun].Page, dimensions[persistence.InferenceBreakdownByModel].Page, nil
}

// sessionAxis renders the per-session axis of a roll-up in member order, parent first. A member
// without ledger rows still gets a zero row: the axis describes the roll-up set, not just the
// sessions that happened to spend.
func (h *Handler[U]) sessionAxis(
	rows []*proto_models.AgentSessionUsageBreakdown,
	members []*common_persistence_models.ChatAgentSession,
) []*proto_models.AgentSessionUsageBreakdown {
	bySession := make(map[uint64]*proto_models.AgentSessionUsageBreakdown, len(rows))
	for _, row := range rows {
		if row != nil && row.AgentSessionId != nil {
			bySession[*row.AgentSessionId] = row
		}
	}
	out := make([]*proto_models.AgentSessionUsageBreakdown, 0, len(members))
	for _, member := range members {
		row, ok := bySession[member.ID]
		if !ok {
			id := member.ID
			row = &proto_models.AgentSessionUsageBreakdown{AgentSessionId: &id}
		}
		agentType := member.AgentType
		row.AgentType = &agentType
		out = append(out, row)
	}
	return out
}

// ListAgentRunInferenceCalls returns the append-only inference-call ledger for one agent run,
// paginated. Returns an empty list (not an error) when the run has no calls.
func (h *Handler[U]) ListAgentRunInferenceCalls(
	ctx context.Context,
	agentRunID uint64,
	pagination *proto_models.PaginationRequest,
) ([]*proto_models.ChatInferenceCall, *proto_models.PaginationResponse, error) {
	if h.chatDisabled() {
		return nil, nil, serviceErrors.ErrChatDisabled
	}
	if agentRunID == 0 {
		return nil, nil, serviceErrors.ErrInvalidRequest
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, nil, err
	}

	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, agentRunID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
			return nil, nil, serviceErrors.ErrChatAgentRunNotFound
		}
		return nil, nil, serviceErrors.ErrDatabaseError
	}
	if err := h.usageReadAuthorized(ctx, actor, run.ChannelID); err != nil {
		if usageAccessDenied(err) {
			return nil, nil, serviceErrors.ErrChatAgentRunNotFound
		}
		return nil, nil, err
	}

	rows, page, err := h.persistenceHandler.ListChatInferenceCallsByRun(ctx, agentRunID, pagination)
	if err != nil {
		if errors.Is(err, persistence.ErrDatabaseError) {
			return nil, nil, serviceErrors.ErrDatabaseError
		}
		return nil, nil, serviceErrors.ErrInternalError
	}
	calls := common_util.ApplyToEach(rows, func(c *common_persistence_models.ChatInferenceCall) *proto_models.ChatInferenceCall {
		return h.converter.PersistenceChatInferenceCallToProto(c)
	})
	return calls, page, nil
}

// usageReadAuthorized passes the channel read gate and requires the channel to show token usage.
func (h *Handler[U]) usageReadAuthorized(ctx context.Context, actor U, channelID uint64) error {
	channel, err := h.readChannelAuthorized(ctx, actor, channelID)
	if err != nil {
		return err
	}
	if !channel.GetSupportInferenceUsage() {
		return serviceErrors.ErrChatFeatureDisabled
	}
	return nil
}

// usageAccessDenied conceals resource existence without concealing operational failures.
func usageAccessDenied(err error) bool {
	return errors.Is(err, serviceErrors.ErrChatNotMember) || errors.Is(err, serviceErrors.ErrInsufficientPermissions) || errors.Is(err, serviceErrors.ErrChatChannelNotFound)
}

// GetMessageInferenceSummary returns the derived per-message usage summary by aggregating the
// message's inference-call rows. Returns nil (not an error) when the message has no inference rows.
func (h *Handler[U]) GetMessageInferenceSummary(ctx context.Context, messageID uint64) (*proto_models.MessageInferenceSummary, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if messageID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}

	// The message must pass the read gate (participant, org reader or site admin) before exposing its usage.
	msg, err := h.persistenceHandler.GetChatMessageByID(ctx, messageID)
	if err != nil {
		return nil, mapChatMessagePersistenceError(err)
	}
	if err := h.usageReadAuthorized(ctx, actor, msg.ChannelID); err != nil {
		return nil, err
	}

	agg, err := h.persistenceHandler.GetMessageInferenceAggregate(ctx, messageID)
	if err != nil {
		if errors.Is(err, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}
	// No rows → nil summary (not an error); the frontend renders nothing.
	return h.converter.PersistenceMessageInferenceAggregateToProto(agg), nil
}

// BatchGetMessageInferenceSummaries returns derived per-message usage summaries for a bounded set of
// message IDs in one read. Messages with no inference rows are omitted from the result, as are
// hidden/deleted messages (the persistence read is visibility-filtered, matching the
// single-message path's includeHidden=false load — hidden transcript usage must not leak).
func (h *Handler[U]) BatchGetMessageInferenceSummaries(
	ctx context.Context,
	channelID uint64,
	messageIDs []uint64,
) ([]*proto_models.MessageInferenceSummary, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.usageReadAuthorized(ctx, actor, channelID); err != nil {
		return nil, err
	}

	byID, err := h.persistenceHandler.BatchGetMessageInferenceAggregates(ctx, channelID, messageIDs)
	if err != nil {
		if errors.Is(err, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}

	// Preserve the request order; omit message IDs with no rows.
	out := make([]*proto_models.MessageInferenceSummary, 0, len(messageIDs))
	for _, id := range messageIDs {
		if agg, ok := byID[id]; ok && agg != nil {
			if summary := h.converter.PersistenceMessageInferenceAggregateToProto(agg); summary != nil {
				out = append(out, summary)
			}
		}
	}
	return out, nil
}

// ResolveToolCall is implemented in handler_tool_approval.go (the approval lane).
