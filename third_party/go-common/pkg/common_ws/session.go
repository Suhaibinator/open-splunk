package common_ws

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
)

// ErrInvalidSession rejects a connection without an absolute session expiry.
var ErrInvalidSession = errors.New("realtime session must have an expiry")

// Session is the authenticated session a socket was opened with. The socket closes at ExpiresAt,
// and CloseSessions closes it as soon as Token is revoked.
type Session struct {
	Token     string
	ExpiresAt time.Time
	// Check, when set, re-validates the session once the socket is registered: a revocation that
	// committed between authentication and registration cannot have closed it.
	Check func(context.Context) error
}

// CloseSessions closes every socket opened with one of the revoked session tokens. Revocation
// paths call it after commit, so a revoked session stops receiving events immediately.
//
// TODO(multi-instance-v2): this closes only this instance's sockets. Distribute revocations when
// multiple instances are supported.
func (h *Hub[U]) CloseSessions(tokens ...string) {
	if h == nil || len(tokens) == 0 {
		return
	}
	// Snapshot under the lock and close outside it: unregister takes the write lock.
	h.mu.RLock()
	var conns []*connection[U]
	for _, token := range tokens {
		for c := range h.connsBySession[token] {
			conns = append(conns, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range conns {
		c.close(disconnectReasonSessionInvalid)
	}
}

// CloseUserConnections closes every socket of a user, for example after a ban.
//
// TODO(multi-instance-v2): this closes only this instance's sockets.
func (h *Hub[U]) CloseUserConnections(userID common_service_models.UserIdType) {
	if h == nil {
		return
	}
	for _, c := range h.snapshotConnections([]common_service_models.UserIdType{userID}) {
		c.close(disconnectReasonSessionInvalid)
	}
}
