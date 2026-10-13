package chat

import (
	"context"
	"errors"
	"strings"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ListChannelAgentSessions lists the agent sessions bound to one channel: the main-timeline session
// first, then thread sessions newest first, delegated children included, capped at the
// chat_agent_sessions_by_channel_max sample taken once per listing (when the cap applies, the
// oldest thread sessions are dropped). Anyone who may read the
// channel may list them, including read-only
// organization readers; the application's Authorize veto applies through the channel read.
func (h *Handler[U]) ListChannelAgentSessions(ctx context.Context, channelID uint64) (*proto_models.ListChannelAgentSessionsResponse, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if channelID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := h.readChannelAuthorized(ctx, actor, channelID); err != nil {
		return nil, err
	}
	limit := h.policy().ChatAgentSessionsByChannelMax()
	sessions, err := h.persistenceHandler.ListChatAgentSessionsByChannel(ctx, channelID, limit)
	if err != nil {
		if errors.Is(err, persistence.ErrDatabaseError) {
			return nil, serviceErrors.ErrDatabaseError
		}
		return nil, serviceErrors.ErrInternalError
	}
	out := make([]*proto_models.AgentSessionSummary, 0, len(sessions))
	for _, session := range sessions {
		if session == nil {
			continue
		}
		if summary := h.AgentSessionSummary(h.converter.PersistenceChatAgentSessionToService(session)); summary != nil {
			out = append(out, summary)
		}
	}
	return &proto_models.ListChannelAgentSessionsResponse{Sessions: out, ServerTime: timestamppb.Now()}, nil
}

// AgentSessionSummary projects a session for clients with its registry display name and its
// operation's application re-target gate, from one registry lookup. An unregistered type keeps an
// empty display name (the converter falls back to the type) and an UNSPECIFIED gate, as does a
// process without inference routing.
func (h *Handler[U]) AgentSessionSummary(session *common_service_models.ChatAgentSession) *proto_models.AgentSessionSummary {
	displayName := ""
	gate := proto_models.InferenceSessionRetarget_INFERENCE_SESSION_RETARGET_UNSPECIFIED
	if session != nil && h.agentRegistry != nil {
		if def, ok := h.agentRegistry.GetAgentType(session.AgentType); ok {
			displayName = strings.TrimSpace(def.DisplayName)
			if h.retargetGates != nil {
				gate = h.retargetGates.InferenceSessionRetarget(def.Operation)
			}
		}
	}
	summary := h.converter.ServiceChatAgentSessionToSummaryProto(session, displayName)
	if summary != nil {
		summary.InferenceSessionRetarget = gate
	}
	return summary
}
