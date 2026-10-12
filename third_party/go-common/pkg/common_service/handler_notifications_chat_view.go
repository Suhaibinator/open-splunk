package common_service

import (
	"context"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
)

// nativeChatViewRequest is one authorized decision whose read asked for a view.
// channel comes from the snapshot that authorized the decision.
type nativeChatViewRequest struct {
	decision  int
	recipient uint64
	channel   *models.ChatChannel
}

// setNativeChatNotificationViews attaches read-time ChatNotificationViews to
// authorized decisions. Channel name, preview and last sender ID come from the
// authorizing channel rows. Direct-message counterparts cost one participant
// read and display names one user read, each only when its ID set is nonempty.
// withPreview (unread chat only) adds the channel's last message preview and
// sender name, both omitted when the recipient sent that message. A view with
// every field empty is not attached.
func (h *CommonServiceHandler[U]) setNativeChatNotificationViews(ctx context.Context, decisions []notification.SourceDecision, requests []nativeChatViewRequest, withPreview bool) error {
	var direct []uint64
	seenChannels := map[uint64]bool{}
	for _, request := range requests {
		if channel := request.channel; channel != nil && channel.ChannelType == pb.ChannelType_CHANNEL_TYPE_DIRECT && !seenChannels[channel.ID] {
			direct = append(direct, channel.ID)
			seenChannels[channel.ID] = true
		}
	}
	var participants map[uint64][]uint64
	if len(direct) > 0 {
		var err error
		if participants, err = h.commonPersistenceHandler.LoadChatNotificationDirectParticipants(ctx, direct); err != nil {
			return err
		}
	}

	counterparts := make([]uint64, len(requests))
	senders := make([]uint64, len(requests))
	var userIDs []uint64
	seenUsers := map[uint64]bool{}
	addUser := func(id uint64) {
		if id != 0 && !seenUsers[id] {
			userIDs = append(userIDs, id)
			seenUsers[id] = true
		}
	}
	for i, request := range requests {
		channel := request.channel
		if channel == nil {
			continue
		}
		if channel.ChannelType == pb.ChannelType_CHANNEL_TYPE_DIRECT {
			counterparts[i] = directChatCounterpart(participants[channel.ID], request.recipient)
			addUser(counterparts[i])
		}
		if withPreview && channel.LastMessageSenderID != nil && *channel.LastMessageSenderID != request.recipient {
			senders[i] = *channel.LastMessageSenderID
			addUser(senders[i])
		}
	}
	var users map[uint64]*models.User
	if len(userIDs) > 0 {
		var err error
		if users, err = h.commonPersistenceHandler.LoadChatNotificationDisplayUsers(ctx, userIDs); err != nil {
			return err
		}
	}
	// Names render exactly as the chat roster's participant users do.
	displayName := func(id uint64) string {
		user := users[id]
		if user == nil {
			return ""
		}
		public := h.converter.ConvertUserLikeToPublicProto(h.converter.ConvertPersistenceToUser(user))
		if public == nil {
			return ""
		}
		return public.DisplayPrimary
	}

	for i, request := range requests {
		channel := request.channel
		if channel == nil {
			continue
		}
		view := &pb.ChatNotificationView{CounterpartName: displayName(counterparts[i])}
		if channel.ChannelType != pb.ChannelType_CHANNEL_TYPE_DIRECT {
			view.ChannelName = channel.Name
		}
		viewerSentLast := channel.LastMessageSenderID != nil && *channel.LastMessageSenderID == request.recipient
		if withPreview && !viewerSentLast {
			if channel.LastMessagePreview != nil {
				view.LastMessagePreview = *channel.LastMessagePreview
			}
			view.LastSenderName = displayName(senders[i])
		}
		if view.ChannelName == "" && view.CounterpartName == "" && view.LastMessagePreview == "" && view.LastSenderName == "" {
			continue
		}
		encoded, err := proto.Marshal(view)
		if err != nil {
			return err
		}
		decisions[request.decision].View = encoded
	}
	return nil
}

// directChatCounterpart is the one participant other than recipient, or zero
// for a self-conversation or any membership that does not name exactly one.
func directChatCounterpart(participants []uint64, recipient uint64) uint64 {
	var counterpart uint64
	for _, id := range participants {
		if id == recipient {
			continue
		}
		if counterpart != 0 {
			return 0
		}
		counterpart = id
	}
	return counterpart
}
