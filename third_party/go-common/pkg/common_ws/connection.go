package common_ws

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	gws "github.com/gorilla/websocket"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// closeFrameWriteTimeout bounds the best-effort going-away frame written when the hub shuts down. It
// is deliberately short: a client that is not reading must not hold up shutdown.
const closeFrameWriteTimeout = 250 * time.Millisecond

// connection defines the data structure used in the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
type connection[U common_service_models.UserLike] struct {
	// user is stored so subscribe-time topic authorizers receive the full application user, not
	// just an ID.
	user   U
	userID common_service_models.UserIdType
	ws     WSConn
	hub    *Hub[U]
	cfg    HubConfig

	ctx     context.Context
	session Session
	// notificationConnectionID is generated at accept time and never taken from a client frame.
	notificationConnectionID string
	activityMu               sync.Mutex
	activityCtx              context.Context
	activityCancel           context.CancelFunc
	activitySeen             atomic.Bool

	// logger carries the connection's trace ID, bound once at construction
	// since ctx (and therefore the trace ID) is fixed for the connection's
	// lifetime.
	logger *zap.Logger

	send chan []byte

	// topics is the set of entity topics this connection is subscribed to. It is guarded by the
	// hub's mutex, not the connection, because it is written together with the hub's topic->conns
	// index and read by unregister.
	topics map[string]struct{}

	closeOnce sync.Once
	closed    chan struct{}

	// lastPong stores the Unix timestamp of the last pong received.
	lastPong atomic.Int64
}

// newConnection builds a connection around an accepted transport: it allocates the bounded send
// queue from cfg, binds the trace ID from ctx into the connection's logger once (ctx is fixed for
// the connection's lifetime), and installs the pong/read-deadline control handlers.
//
// cfg is captured, not referenced: the connection keeps the exact settings it was accepted with —
// origins, ping, pong, read timeout, message size and command auth budget from one configuration
// generation — so a later reload governs new handshakes and never mutates this one underneath its
// running loops.
//
// It does not register the connection anywhere; the caller does that inside the hub's registration
// critical section.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - hub: The hub that owns and routes for this connection.
//   - user: The authenticated user owning the connection.
//   - ws: The websocket transport to read from and write to.
//   - cfg: Immutable settings captured once for this connection.
//
// Returns:
//   - result: The connection, ready to be registered and served.
func newConnection[U common_service_models.UserLike](ctx context.Context, hub *Hub[U], user U, ws WSConn, cfg HubConfig, session Session) *connection[U] {
	if ctx == nil {
		ctx = context.Background()
	}
	c := &connection[U]{
		user:                     user,
		userID:                   user.GetID(),
		ws:                       ws,
		hub:                      hub,
		cfg:                      cfg,
		ctx:                      ctx,
		session:                  session,
		notificationConnectionID: newNotificationConnectionID(),
		logger:                   appLogger.For(ctx, hub.logger, realtimeHubLoggerName),
		send:                     make(chan []byte, cfg.SendQueueSize),
		closed:                   make(chan struct{}),
	}
	c.activityCtx, c.activityCancel = context.WithCancel(c.ctx)
	c.lastPong.Store(time.Now().Unix())
	c.setupControlHandlers()
	return c
}

// serve executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *connection[U]) serve() {
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		c.readLoop()
	}()
	go func() {
		defer wg.Done()
		c.writeLoop()
	}()
	go func() {
		defer wg.Done()
		c.pingLoop()
	}()
	go func() {
		defer wg.Done()
		expiry := time.NewTimer(time.Until(c.session.ExpiresAt))
		defer expiry.Stop()
		select {
		case <-c.closed:
		case <-c.ctx.Done():
			c.close(disconnectReasonContextDone)
		case <-expiry.C:
			c.close(disconnectReasonSessionInvalid)
		}
	}()
	wg.Wait()
}

// enqueue executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *connection[U]) enqueue(payload []byte) error {
	if c == nil {
		return ErrNilConnection
	}
	select {
	case <-c.closed:
		return ErrHubClosed
	default:
	}
	select {
	case c.send <- payload:
		return nil
	default:
		return ErrBackpressure
	}
}

// close tears the connection down exactly once, whatever raced to trigger it.
//
// A hub shutdown gets a best-effort close frame first: without one the client sees an abnormal
// closure (1006) and cannot tell a restart from a network failure, so it backs off as if the server
// were unreachable. Every other reason is a fault or a client-initiated close where the socket is
// already gone or going, and waiting on a write there would only delay the teardown.
//
// Parameters:
//   - reason: One of the package's disconnectReason* constants, recorded as the disconnect metric's
//     label. It is deliberately a closed server-owned set, never client-derived.
func (c *connection[U]) close(reason string) {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		close(c.closed)
		if c.activityCancel != nil {
			c.activityCancel()
		}
		if c.hub != nil && c.hub.notificationActivity != nil {
			c.hub.notificationActivity.release(c)
		}
		if c.ws != nil {
			if reason == disconnectReasonHubClose {
				c.writeGoingAwayFrame()
			}
			_ = c.ws.Close()
		}
		if c.hub != nil {
			c.hub.metrics.RecordRealtimeConnectionClosed(reason)
			c.hub.unregister(c)
		}
	})
}

func newNotificationConnectionID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("cannot generate realtime connection identity: " + err.Error())
	}
	return hex.EncodeToString(raw[:])
}

// writeGoingAwayFrame tells the client the server is going away, on transports that can express it.
// It is strictly best effort and deadline-bounded: shutdown must not block on a client that has
// stopped reading, and the transport is closed immediately afterwards either way.
func (c *connection[U]) writeGoingAwayFrame() {
	writer, ok := c.ws.(CloseFrameWriter)
	if !ok {
		return
	}
	_ = writer.WriteClose(gws.CloseGoingAway, "server shutting down", time.Now().Add(closeFrameWriteTimeout))
}

// readLoop executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *connection[U]) readLoop() {
	for {
		select {
		case <-c.closed:
			return
		default:
		}

		if mr, ok := c.ws.(MessageReader); ok {
			messageType, data, err := mr.ReadMessage()
			if err != nil {
				if errors.Is(err, io.EOF) {
					c.close(disconnectReasonClientEOF)
					return
				}
				c.close(disconnectReasonReadError)
				return
			}
			c.handleClientFrame(messageType, data)
			c.resetReadDeadline()
			continue
		}

		// Fallback: drain bytes without parsing (legacy/non-message conn implementations).
		buf := make([]byte, 4096)
		_, err := c.ws.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.close(disconnectReasonClientEOF)
				return
			}
			c.close(disconnectReasonReadError)
			return
		}
		c.resetReadDeadline()
	}
}

// handleClientFrame decodes one client frame and routes it. Subscribe and unsubscribe are owned by
// the hub and handled before any router dispatch, so they keep working when no CommandRouter is
// installed — the chat-less configuration the realtime transport exists for. Notification activity
// is also hub-owned when enabled: it is bound to this socket, so the connection is passed to the
// store explicitly rather than through the router's user-scoped handler signature.
//
// Parameters:
//   - messageType: Message input used by this operation.
//   - data: Raw frame payload.
func (c *connection[U]) handleClientFrame(messageType int, data []byte) {
	if c == nil || c.hub == nil {
		return
	}

	// Commands are binary protobuf frames. Some clients may accidentally send JSON/text; log at Debug for visibility.
	if messageType == gws.TextMessage {
		if ce := c.logger.Check(zap.DebugLevel, "ignoring text websocket frame"); ce != nil {
			ce.Write(zap.Int(appLogger.KeySize, len(data)), zap.Uint64(appLogger.KeyUserID, uint64(c.userID)))
		}
		return
	}
	if messageType != gws.BinaryMessage || len(data) == 0 {
		return
	}

	var cmd proto_models.RealtimeCommand
	if err := proto.Unmarshal(data, &cmd); err != nil {
		if ce := c.logger.Check(zap.DebugLevel, "failed to unmarshal realtime command"); ce != nil {
			ce.Write(
				zap.Error(err),
				zap.Int(appLogger.KeySize, len(data)),
				zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
			)
		}
		return
	}

	switch cmd.GetType() {
	case CommandTypeSubscribe:
		c.hub.handleSubscribe(c, cmd.GetRequestId(), cmd.GetPayload())
		return
	case CommandTypeUnsubscribe:
		c.hub.handleUnsubscribe(c, cmd.GetRequestId(), cmd.GetPayload())
		return
	case NotificationActivityCommandType:
		if store := c.hub.notificationActivity; store != nil {
			if err := store.handleCommand(c, cmd.GetPayload()); err != nil {
				if ce := c.logger.Check(zap.DebugLevel, "realtime command dispatch failed"); ce != nil {
					ce.Write(
						zap.String(appLogger.KeyCommandType, cmd.GetType()),
						zap.Error(err),
						zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
					)
				}
			}
			return
		}
	}

	if c.hub.commandRouter == nil {
		if ce := c.logger.Check(zap.DebugLevel, "no realtime command router installed"); ce != nil {
			ce.Write(
				zap.String(appLogger.KeyCommandType, cmd.GetType()),
				zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
			)
		}
		return
	}

	if err := c.hub.commandRouter.Dispatch(c.ctx, c.userID, cmd.GetType(), cmd.GetRequestId(), cmd.GetPayload()); err != nil {
		if ce := c.logger.Check(zap.DebugLevel, "realtime command dispatch failed"); ce != nil {
			ce.Write(
				zap.String(appLogger.KeyCommandType, cmd.GetType()),
				zap.Error(err),
				zap.Uint64(appLogger.KeyUserID, uint64(c.userID)),
			)
		}
	}
}

// writeLoop executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *connection[U]) writeLoop() {
	for {
		select {
		case <-c.closed:
			return
		case payload := <-c.send:
			if len(payload) == 0 {
				continue
			}
			if err := c.ws.WriteBinary(payload); err != nil {
				c.close(disconnectReasonWriteError)
				return
			}
		}
	}
}

// setupControlHandlers executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *connection[U]) setupControlHandlers() {
	if c.ws == nil {
		return
	}

	cfg := c.cfg

	if cfg.MaxMessageSize > 0 {
		c.ws.SetReadLimit(cfg.MaxMessageSize)
	}

	c.ws.SetPongHandler(func(_ string) error {
		c.lastPong.Store(time.Now().Unix())
		c.resetReadDeadline()
		return nil
	})

	c.resetReadDeadline()
}

// resetReadDeadline executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *connection[U]) resetReadDeadline() {
	cfg := c.cfg
	if cfg.ReadTimeout <= 0 {
		return
	}
	if c.ws == nil {
		return
	}
	_ = c.ws.SetReadDeadline(time.Now().Add(cfg.ReadTimeout))
}

// pingLoop executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *connection[U]) pingLoop() {
	cfg := c.cfg
	if cfg.PingInterval <= 0 {
		return
	}

	if c.ws == nil {
		return
	}

	// A transport that cannot deliver pongs would fail every pong-timeout check
	// regardless of client activity; liveness for such transports falls back to
	// the read-deadline path in readLoop.
	if !c.ws.SupportsPing() {
		return
	}

	ticker := time.NewTicker(cfg.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			lastPong := time.Unix(c.lastPong.Load(), 0)
			if time.Since(lastPong) > cfg.PingInterval+cfg.PongTimeout {
				c.close(disconnectReasonPongTimeout)
				return
			}

			deadline := time.Now().Add(cfg.PongTimeout)
			if err := c.ws.WritePing(nil, deadline); err != nil {
				c.close(disconnectReasonPingFailed)
				return
			}
		}
	}
}
