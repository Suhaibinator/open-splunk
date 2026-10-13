package common_api

import (
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_ws"
)

// Invalidate the cache before advancing the delivery generation. A publisher that observed the
// old recipients then fails its generation check; a publisher observing the new generation can
// only load the new cache. No database work happens under the hub's lock.
type chatRealtimeInvalidator[U common_service_models.UserLike] struct {
	cache *common_ws.ChatRecipientCache
	hub   *common_ws.Hub[U]
}

func (i *chatRealtimeInvalidator[U]) InvalidateChatRealtimeAccess(change ports.ChatRealtimeAccessInvalidation) {
	i.cache.InvalidateChatRealtimeAccess(change)
	i.hub.InvalidateChatRealtimeAccess(change)
}
