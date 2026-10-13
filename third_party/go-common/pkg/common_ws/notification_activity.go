package common_ws

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util/notificationkey"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const NotificationActivityCommandType = "notification.activity"
const NotificationActivityTTL = 45 * time.Second
const MaxNotificationActivityScopes = 8
const MaxNotificationActivityScopeLength = notificationkey.MaxActivityScopeLength
const notificationActivityWriteTimeout = 2 * time.Second
const notificationActivityCleanupConcurrency = 64

var ErrInvalidNotificationActivity = errors.New("invalid notification activity")

// NotificationActivityBackend is the shared presence seam. Replace stores one
// complete scope set for a server-generated connection ID until expiresAt;
// Release removes it. Implementations must make expired entries inactive even
// when Release cannot reach them (for example during a process crash).
// Methods may be called concurrently for different connections.
type NotificationActivityBackend interface {
	IsActive(userID uint64, scope string) bool
	Replace(ctx context.Context, userID uint64, connectionID string, scopes []string, expiresAt time.Time) error
	Release(ctx context.Context, userID uint64, connectionID string) error
}

type activityEntry struct {
	scopes  map[string]struct{}
	expires time.Time
}

type memoryNotificationActivityBackend struct {
	mu     sync.RWMutex
	byUser map[uint64]map[string]activityEntry
	now    func() time.Time
}

func (b *memoryNotificationActivityBackend) IsActive(userID uint64, scope string) bool {
	if b == nil || userID == 0 || scope == "" {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	now := b.now()
	for _, entry := range b.byUser[userID] {
		if now.Before(entry.expires) {
			if _, ok := entry.scopes[scope]; ok {
				return true
			}
		}
	}
	return false
}

func (b *memoryNotificationActivityBackend) Replace(ctx context.Context, userID uint64, connectionID string, scopes []string, expiresAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	next := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		next[scope] = struct{}{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	set := b.byUser[userID]
	if set == nil {
		set = make(map[string]activityEntry)
		b.byUser[userID] = set
	}
	set[connectionID] = activityEntry{scopes: next, expires: expiresAt}
	return nil
}

func (b *memoryNotificationActivityBackend) Release(ctx context.Context, userID uint64, connectionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.byUser[userID], connectionID)
	if len(b.byUser[userID]) == 0 {
		delete(b.byUser, userID)
	}
	return nil
}

// NotificationActivityStore binds a command to its authenticated socket and
// delegates accepted presence to a local or application-supplied backend. The
// hub dispatches NotificationActivityCommandType to it directly, passing the
// receiving connection, so the command never goes through the CommandRouter.
type NotificationActivityStore[U common_service_models.UserLike] struct {
	backend      NotificationActivityBackend
	now          func() time.Time
	external     bool
	cleanupSlots chan struct{}
}

func newNotificationActivityStore[U common_service_models.UserLike](backend NotificationActivityBackend) *NotificationActivityStore[U] {
	s := &NotificationActivityStore[U]{now: time.Now}
	if backend == nil {
		backend = &memoryNotificationActivityBackend{byUser: make(map[uint64]map[string]activityEntry), now: func() time.Time { return s.now() }}
	} else {
		s.external = true
		s.cleanupSlots = make(chan struct{}, notificationActivityCleanupConcurrency)
	}
	s.backend = backend
	return s
}

// IsActive implements the notification service's read-only activity query.
func (s *NotificationActivityStore[U]) IsActive(userID uint64, scope string) bool {
	return s != nil && s.backend != nil && s.backend.IsActive(userID, scope)
}

// handleCommand decodes one notification.activity payload received on c.
func (s *NotificationActivityStore[U]) handleCommand(c *connection[U], payload []byte) error {
	var activity proto_models.NotificationActivity
	if err := proto.Unmarshal(payload, &activity); err != nil {
		return ErrInvalidNotificationActivity
	}
	return s.update(c, activity.GetScopes())
}

// update replaces the activity scopes reported by the authenticated socket c.
func (s *NotificationActivityStore[U]) update(c *connection[U], scopes []string) error {
	if c == nil || c.userID == 0 || c.notificationConnectionID == "" || len(scopes) > MaxNotificationActivityScopes {
		return ErrInvalidNotificationActivity
	}
	userID := c.userID
	next := make([]string, 0, len(scopes))
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		if !notificationkey.ValidActivityScope(scope) {
			return ErrInvalidNotificationActivity
		}
		if _, exists := seen[scope]; !exists {
			next = append(next, scope)
			seen[scope] = struct{}{}
		}
	}
	if len(next) > 0 {
		c.activitySeen.Store(true)
	}
	c.activityMu.Lock()
	defer c.activityMu.Unlock()
	// Serialization with release prevents an in-flight backend write from
	// recreating activity after this connection has closed.
	select {
	case <-c.closed:
		return ErrInvalidNotificationActivity
	default:
	}
	base := c.activityCtx
	if base == nil {
		base = c.ctx
	}
	if base == nil {
		base = context.Background()
	}
	writeCtx, cancel := context.WithTimeout(base, notificationActivityWriteTimeout)
	defer cancel()
	if len(next) == 0 {
		return s.backend.Release(writeCtx, uint64(userID), c.notificationConnectionID)
	}
	return s.backend.Replace(writeCtx, uint64(userID), c.notificationConnectionID, next, s.now().Add(NotificationActivityTTL))
}

func (s *NotificationActivityStore[U]) release(c *connection[U]) {
	if s == nil || c == nil || c.notificationConnectionID == "" {
		return
	}
	if !c.activitySeen.Load() {
		return
	}
	if s.external {
		select {
		case s.cleanupSlots <- struct{}{}:
			go func() {
				defer func() { <-s.cleanupSlots }()
				s.releaseNow(c)
			}()
		default:
			if c.logger != nil {
				c.logger.Warn("notification activity cleanup saturated; expiry remains the backstop",
					zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
					zap.String(appLogger.KeyNotificationConnectionID, c.notificationConnectionID),
					zap.String(logkeys.Stage, "release"), zap.String(appLogger.KeyOutcome, "expiry_backstop"))
			}
		}
		return
	}
	s.releaseNow(c)
}

func (s *NotificationActivityStore[U]) releaseNow(c *connection[U]) {
	c.activityMu.Lock()
	defer c.activityMu.Unlock()
	base := c.ctx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(base), notificationActivityWriteTimeout)
	defer cancel()
	if err := s.backend.Release(ctx, uint64(c.userID), c.notificationConnectionID); err != nil && c.logger != nil {
		c.logger.Warn("notification activity release failed; expiry remains the backstop",
			zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
			zap.String(appLogger.KeyNotificationConnectionID, c.notificationConnectionID),
			zap.String(logkeys.Stage, "release"), zap.String(appLogger.KeyOutcome, "expiry_backstop"), zap.Error(err))
	}
}

// EnableNotificationActivity enables the server-owned command using local memory.
func (h *Hub[U]) EnableNotificationActivity() (*NotificationActivityStore[U], error) {
	return h.EnableNotificationActivityWithBackend(nil)
}

// EnableNotificationActivityWithBackend installs a shared activity backend
// before the first socket is served. A nil backend uses bounded local memory.
func (h *Hub[U]) EnableNotificationActivityWithBackend(backend NotificationActivityBackend) (*NotificationActivityStore[U], error) {
	if h == nil {
		return nil, ErrHubClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started {
		return nil, ErrHubStarted
	}
	if h.notificationActivity != nil {
		return h.notificationActivity, nil
	}
	h.notificationActivity = newNotificationActivityStore[U](backend)
	return h.notificationActivity, nil
}
