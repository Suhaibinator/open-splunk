package common_ws

import (
	"errors"
	"sync"
	"time"

	gws "github.com/gorilla/websocket"
	xws "golang.org/x/net/websocket"
)

// WSConn is the minimal interface the hub needs from a websocket connection implementation.
//
// Implementations must be safe for concurrent use by a single reader goroutine and a single writer goroutine.
//
// The keepalive/deadline methods are part of this interface (not an optional capability) so that
// alternative or wrapping implementations cannot silently drop read-timeout and ping/pong
// enforcement. A decorator that embeds or wraps another WSConn must forward these calls; embedding
// the wrapped WSConn does so automatically. Transports that cannot send pings or deliver pongs
// must report that via SupportsPing so the hub falls back to read-deadline liveness instead of
// force-closing healthy connections on a pong timeout that can never be satisfied.
//
// Read-deadline liveness is reset only by client-to-server data frames: server-side pushes do not
// count, and on ping-incapable transports pong frames never arrive. Clients on such transports
// (e.g. XNetConn) that only listen must therefore send an application-level heartbeat message
// more often than the configured read timeout, or they will be treated as dead and disconnected.
type WSConn interface {
	// Read reads bytes from the websocket. The hub uses this only to detect close and drain client frames.
	Read(p []byte) (n int, err error)
	// WriteBinary writes a single binary websocket message.
	WriteBinary(payload []byte) error
	// Close closes the websocket connection.
	Close() error
	// SetReadLimit caps the size in bytes of an incoming message. Implementations that cannot enforce a limit may no-op.
	SetReadLimit(limit int64)
	// SetReadDeadline sets the absolute time after which a blocked Read returns an error, enforcing read timeouts.
	SetReadDeadline(t time.Time) error
	// SetPongHandler registers a handler invoked when a pong control frame arrives, used to track client liveness. Implementations without control-frame support may no-op, but must then return false from SupportsPing.
	SetPongHandler(h func(appData string) error)
	// WritePing writes a ping control frame with the given payload, failing if it cannot be sent before deadline.
	WritePing(data []byte, deadline time.Time) error
	// SupportsPing reports whether the transport can send ping control frames and deliver pongs to
	// the handler registered via SetPongHandler. Implementations that no-op SetPongHandler or
	// WritePing must return false; the hub then relies on read deadlines for liveness instead of
	// pong timeouts.
	SupportsPing() bool
}

// CloseFrameWriter is an optional capability a WSConn may implement to send a websocket close frame
// before the transport is torn down. It follows the SupportsPing precedent: a transport that cannot
// express control frames simply does not implement it, and the hub falls back to closing the socket.
//
// It matters on shutdown. Dropping the TCP connection without a close frame is an abnormal closure
// (1006) as far as the client is concerned, which is indistinguishable from a network failure and
// sends well-behaved clients into their full reconnect backoff; a 1001 going-away tells them the
// server is restarting and they may come straight back.
type CloseFrameWriter interface {
	// WriteClose writes a close control frame with the given code and reason, failing if it cannot be
	// sent before deadline. Callers treat it as best effort: the transport is closed either way.
	WriteClose(code int, reason string, deadline time.Time) error
}

// MessageReader is an optional capability a WSConn may implement to expose full websocket messages
// (type + payload) instead of a raw byte stream. When a WSConn implements it, the read loop parses
// client frames by type; otherwise it falls back to draining bytes via Read. This is genuinely
// optional: not every transport exposes message framing (e.g. XNetConn does not implement it).
type MessageReader interface {
	// ReadMessage reads the next websocket message, returning its type and payload.
	ReadMessage() (messageType int, p []byte, err error)
}

// XNetConn defines the data structure used in the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
type XNetConn struct {
	conn *xws.Conn
}

// NewXNetConn executes this function as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func NewXNetConn(conn *xws.Conn) *XNetConn {
	return &XNetConn{conn: conn}
}

// Read executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *XNetConn) Read(p []byte) (int, error) {
	if c == nil || c.conn == nil {
		return 0, errors.New("nil websocket conn")
	}
	return c.conn.Read(p)
}

// WriteBinary executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *XNetConn) WriteBinary(payload []byte) error {
	if c == nil || c.conn == nil {
		return errors.New("nil websocket conn")
	}
	return xws.Message.Send(c.conn, payload)
}

// Close executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *XNetConn) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// SetReadLimit executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *XNetConn) SetReadLimit(limit int64) {
	if c == nil || c.conn == nil || limit <= 0 {
		return
	}
	c.conn.MaxPayloadBytes = int(limit)
}

// SetReadDeadline executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *XNetConn) SetReadDeadline(t time.Time) error {
	if c == nil || c.conn == nil {
		return errors.New("nil websocket conn")
	}
	return c.conn.SetReadDeadline(t)
}

// SetPongHandler executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
//
// The x/net/websocket transport does not expose a control-frame handler, so this is a no-op.
func (c *XNetConn) SetPongHandler(_ func(appData string) error) {}

// WritePing executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
//
// The x/net/websocket transport does not expose ping control frames, so this is a no-op.
func (c *XNetConn) WritePing(_ []byte, _ time.Time) error { return nil }

// SupportsPing executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
//
// The x/net/websocket transport can neither send ping control frames nor deliver pongs
// (SetPongHandler and WritePing are no-ops), so pong-based liveness checks must not apply.
func (c *XNetConn) SupportsPing() bool { return false }

// GorillaConn defines the data structure used in the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
type GorillaConn struct {
	conn *gws.Conn

	// writeMu serializes all writes (including control frames) as required by gorilla/websocket.
	writeMu sync.Mutex
}

var (
	_ MessageReader    = (*GorillaConn)(nil)
	_ CloseFrameWriter = (*GorillaConn)(nil)
)

// NewGorillaConn executes this function as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func NewGorillaConn(conn *gws.Conn) *GorillaConn {
	return &GorillaConn{conn: conn}
}

// Read executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *GorillaConn) Read(p []byte) (int, error) {
	if c == nil || c.conn == nil {
		return 0, errors.New("nil websocket conn")
	}
	_, msg, err := c.conn.ReadMessage()
	if err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return len(msg), nil
	}
	return copy(p, msg), nil
}

// ReadMessage executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *GorillaConn) ReadMessage() (int, []byte, error) {
	if c == nil || c.conn == nil {
		return 0, nil, errors.New("nil websocket conn")
	}
	return c.conn.ReadMessage()
}

// WriteBinary executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *GorillaConn) WriteBinary(payload []byte) error {
	if c == nil || c.conn == nil {
		return errors.New("nil websocket conn")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteMessage(gws.BinaryMessage, payload)
}

// Close executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *GorillaConn) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	// Close the underlying net.Conn first to ensure any stuck writer unblocks before we try to serialize close.
	if nc := c.conn.UnderlyingConn(); nc != nil {
		_ = nc.Close()
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.Close()
}

// SetReadLimit executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *GorillaConn) SetReadLimit(limit int64) {
	if c == nil || c.conn == nil {
		return
	}
	c.conn.SetReadLimit(limit)
}

// SetReadDeadline executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *GorillaConn) SetReadDeadline(t time.Time) error {
	if c == nil || c.conn == nil {
		return errors.New("nil websocket conn")
	}
	return c.conn.SetReadDeadline(t)
}

// SetPongHandler executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *GorillaConn) SetPongHandler(h func(appData string) error) {
	if c == nil || c.conn == nil {
		return
	}
	c.conn.SetPongHandler(h)
}

// WritePing executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (c *GorillaConn) WritePing(data []byte, deadline time.Time) error {
	if c == nil || c.conn == nil {
		return errors.New("nil websocket conn")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteControl(gws.PingMessage, data, deadline)
}

// SupportsPing executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
//
// gorilla/websocket supports ping control frames and pong handlers, so pong-based liveness checks apply.
func (c *GorillaConn) SupportsPing() bool { return true }

// WriteClose implements CloseFrameWriter for the gorilla transport, letting the hub tell a client it
// is going away instead of leaving it to infer an abnormal closure from a dropped socket.
//
// Parameters:
//   - code: One of the websocket.Close* status codes.
//   - reason: Short human-readable reason carried in the frame.
//   - deadline: Absolute time by which the frame must be written.
//
// Returns:
//   - result: Error encountered while writing the control frame; nil on success.
func (c *GorillaConn) WriteClose(code int, reason string, deadline time.Time) error {
	if c == nil || c.conn == nil {
		return errors.New("nil websocket conn")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteControl(gws.CloseMessage, gws.FormatCloseMessage(code, reason), deadline)
}
