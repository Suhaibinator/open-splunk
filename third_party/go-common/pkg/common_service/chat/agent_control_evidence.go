package chat

import (
	"context"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

type agentControlEvidenceKey struct{}
type agentControlEvidence struct {
	actorID, channelID uint64
	actorUserType      proto_models.UserType
	siteApprover       bool
	channel            *pm.ChatChannel
	role               *proto_models.ChannelRole
	err                error
	runs               map[uint64]persistence.ChatAgentRunControlEvidence
}

func controlEvidence(ctx context.Context) *agentControlEvidence {
	evidence, _ := ctx.Value(agentControlEvidenceKey{}).(*agentControlEvidence)
	return evidence
}

// withAgentControlEvidence shares membership, role and site approval within this single status
// request. Negative membership never enters the cross-request cache, and application authorization
// still runs separately for every session.
func (h *Handler[U]) withAgentControlEvidence(ctx context.Context, actor U, channelID uint64, states []*persistence.ChatAgentSessionInteractionState) (context.Context, error) {
	e := &agentControlEvidence{actorID: uint64(actor.GetID()), actorUserType: actor.GetUserType(), channelID: channelID, siteApprover: h.hasSiteToolApproval(ctx, actor.GetUserType())}
	var ids []uint64
	needed := false
	for _, state := range states {
		if state.Session.IsSuspended() || (state.ActiveRun != nil && state.Policy.AllowUserStop) {
			needed = true
		}
		if state.ActiveRun != nil && state.Policy.AllowUserStop {
			ids = append(ids, state.ActiveRun.ID)
		}
	}
	if !needed {
		return ctx, nil
	}
	if !e.siteApprover || h.hasPermissionCallback() {
		channel, role, member, err := h.loadChannelAccess(ctx, channelID, actor.GetID())
		e.channel, e.role, e.err = channel, &role, err
		if err == nil && !member && !e.siteApprover {
			e.err = serviceErrors.ErrInsufficientPermissions
		}
	}
	if e.err == nil && !e.siteApprover {
		var err error
		e.runs, err = h.persistenceHandler.GetChatAgentRunControlEvidence(ctx, ids, e.actorID)
		if err != nil {
			return nil, serviceErrors.ErrDatabaseError
		}
	}
	return context.WithValue(ctx, agentControlEvidenceKey{}, e), nil
}
