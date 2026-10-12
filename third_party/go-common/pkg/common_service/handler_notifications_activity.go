package common_service

import (
	"context"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
)

// evaluateChannelActivitySources authorizes channel activity recipients against
// one bounded snapshot per sub-batch. Core eligibility and the delivery
// preference come from the same rows the chat authorizer sees.
func (h *CommonServiceHandler[U]) evaluateChannelActivitySources(ctx context.Context, candidates []notification.SourceCandidate, options notification.SourceOptions) ([]notification.SourceDecision, error) {
	decisions := make([]notification.SourceDecision, len(candidates))
	// A disabled chat module has no channel authorizer: nobody is authorized.
	if h.chatHandler == nil {
		return decisions, nil
	}
	for start := 0; start < len(candidates); start += persistence.ChatNotificationCandidateBatchSize {
		end := min(start+persistence.ChatNotificationCandidateBatchSize, len(candidates))
		requests := make([]persistence.ChatActivityCandidate, end-start)
		userIDs := make([]common_service_models.UserIdType, 0, end-start)
		seen := map[uint64]bool{}
		for i := start; i < end; i++ {
			c := candidates[i]
			p, err := parseChannelActivityPayload(c.Payload)
			if err != nil {
				return nil, err
			}
			requests[i-start] = persistence.ChatActivityCandidate{ChannelID: p.ChannelId, RecipientID: c.RecipientID}
			if !seen[c.RecipientID] {
				userIDs = append(userIDs, common_service_models.UserIdType(c.RecipientID))
				seen[c.RecipientID] = true
			}
		}
		sources, err := h.commonPersistenceHandler.LoadChatActivitySources(ctx, requests)
		if err != nil {
			return nil, err
		}
		if sources == nil {
			return nil, notification.ErrInvalid
		}
		eligible := userIDs[:0]
		for _, id := range userIDs {
			core := sources.Users[uint64(id)]
			if core != nil && core.ID == uint64(id) && !core.Banned && !core.IsBot && !core.DeletedAt.Valid {
				eligible = append(eligible, id)
			}
		}
		users, err := h.userCallbacks.LookupUsers(ctx, eligible)
		if err != nil {
			return nil, err
		}
		var views []nativeChatViewRequest
		for j, request := range requests {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			core := sources.Users[request.RecipientID]
			if core == nil || core.ID != request.RecipientID || core.Banned || core.IsBot || core.DeletedAt.Valid {
				continue
			}
			actor, exists := users[common_service_models.UserIdType(request.RecipientID)]
			if !exists {
				continue
			}
			if options.IncludeDeliveryPreference {
				decisions[start+j].DeliveryPreference = nativeChatSnapshotDeliveryPreference(sources, request.ChannelID, request.RecipientID, false)
			}
			allowed, err := h.chatHandler.AuthorizeChannelActivitySnapshot(ctx, actor, request.ChannelID, sources)
			if err != nil {
				return nil, err
			}
			decisions[start+j].Authorized = allowed
			if allowed && options.IncludeView {
				views = append(views, nativeChatViewRequest{decision: start + j, recipient: request.RecipientID, channel: sources.Channels[request.ChannelID]})
			}
		}
		if err := h.setNativeChatNotificationViews(ctx, decisions, views, true); err != nil {
			return nil, err
		}
	}
	return decisions, nil
}
