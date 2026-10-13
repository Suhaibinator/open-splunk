package ports

import "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"

// RealtimeSessionCloser is implemented by the realtime transport. Revocation, ban and deletion
// paths call it after commit so the affected sockets close immediately.
type RealtimeSessionCloser interface {
	CloseSessions(tokens ...string)
	CloseUserConnections(userID common_service_models.UserIdType)
}
