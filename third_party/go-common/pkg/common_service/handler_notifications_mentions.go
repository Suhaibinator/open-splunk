package common_service

import (
	"slices"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
)

const nativeChatMentionDeliveryTag = "mention"

// The source envelope is persisted in the message transaction, before edits can
// replace its mention associations. Unread inbox payloads remain channel-only.
func marshalNativeChatNotificationSource(row *models.ChatMessage, target *pb.ChatNotificationPayload, approval bool) ([]byte, error) {
	source := &pb.ChatNotificationSourcePayload{Target: target}
	if !approval {
		for _, mention := range row.Mentions {
			if mention.UserID != 0 && mention.UserID != row.SenderID {
				source.MentionUserIds = append(source.MentionUserIds, mention.UserID)
			}
		}
		slices.Sort(source.MentionUserIds)
		source.MentionUserIds = slices.Compact(source.MentionUserIds)
	}
	return proto.Marshal(source)
}

// parseNativeChatNotificationSource decodes the producer's source envelope. Its
// target must name a message; mention IDs must be strictly ascending.
func parseNativeChatNotificationSource(b []byte) (*pb.ChatNotificationPayload, []uint64, error) {
	source := new(pb.ChatNotificationSourcePayload)
	if err := proto.Unmarshal(b, source); err != nil || source.Target == nil || source.Target.ChannelId == 0 || source.Target.MessageId == 0 {
		return nil, nil, notification.ErrInvalid
	}
	var previous uint64
	for _, id := range source.MentionUserIds {
		if id <= previous {
			return nil, nil, notification.ErrInvalid
		}
		previous = id
	}
	return source.Target, source.MentionUserIds, nil
}

// Source authorization uses the message-target payload, which is also the
// approval inbox payload. The captured mention audience only tags
// already-authorized recipients; it never widens it.
func nativeChatTargetEvent(event notification.ProducerEvent) (notification.ProducerEvent, []uint64, error) {
	target, mentions, err := parseNativeChatNotificationSource(event.Payload)
	if err != nil {
		return event, nil, err
	}
	event.Payload, err = proto.Marshal(target)
	return event, mentions, err
}
