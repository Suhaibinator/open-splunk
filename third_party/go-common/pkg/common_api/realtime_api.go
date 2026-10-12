package common_api

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util/origin"
	"github.com/Suhaibinator/go-common/pkg/common_ws"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// ErrRealtimeNotEnabled is returned by the realtime registration hooks when the transport is
// disabled, i.e. when enable_realtime_websocket is off and no hub was installed.
var ErrRealtimeNotEnabled = errors.New("realtime websocket is not enabled")

// realtimeUpgradeBufferSize is the read/write buffer size of the built-in upgrader. Realtime frames
// are small: events are thin invalidation payloads and commands are capped by the hub's message size.
const realtimeUpgradeBufferSize = 1024

// realtimeCloseWriteTimeout bounds how long the server waits to hand a rejected connection its close
// frame before dropping it.
const realtimeCloseWriteTimeout = time.Second

// realtimeHubCloseTimeout bounds how long Stop waits for in-flight connection handlers to unwind
// before it gives up and lets shutdown continue.
const realtimeHubCloseTimeout = 5 * time.Second

// RegisterRealtimeRoutes RegisterRealtimeRoutes registers API routes and wires this capability into the backend request flow during handler setup.
//
// The realtime route has its own registration path, gated only on enable_realtime_websocket, because
// an application with no chat still needs the socket for its own topics. Registering it on the common
// API route group is what gives it cookie authentication; mounting it anywhere else loses auth
// silently and is not supported.
func (h *CommonAPIHandler[U]) RegisterRealtimeRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableRealtimeWebSocket)
	group.Route(router.RouteConfigBase{
		Path:           "/realtime/ws",
		Methods:        []router.HttpMethod{http.MethodGet},
		AuthLevel:      new(router.AuthRequired),
		DisableTimeout: true,
		Handler:        h.RealtimeWebSocket,
	})
}

// RealtimeWebSocket RealtimeWebSocket handles a backend operation in the API/request flow and delegates business logic to the service layer.
//
// It upgrades an authenticated request onto the realtime transport and serves the connection until
// it closes. The full user — not just the ID — is handed to the hub because subscribe-time topic
// authorizers receive it.
func (h *CommonAPIHandler[U]) RealtimeWebSocket(w http.ResponseWriter, r *http.Request) {
	user, found := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !found || user == nil {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Warn("Unauthenticated realtime websocket attempt",
			zap.String(logkeys.Operation, "realtime_websocket_handshake"),
			zap.String(logkeys.Path, r.URL.Path))
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if h.realtimeHub == nil {
		appLogger.LogInvariantViolation(appLogger.For(r.Context(), h.logger, commonAPILoggerName), "realtime_websocket_handler_has_hub", nil,
			zap.String(logkeys.Operation, "realtime_websocket_handshake"),
			zap.String(logkeys.Expected, "configured realtime hub"),
			zap.String(logkeys.Actual, "nil realtime hub"))
		http.Error(w, "WebSocket endpoint not implemented in go-common; enable realtime_websocket or configure a realtime hub in the consuming application", http.StatusNotImplemented)
		return
	}

	// Protocol versioning lives in the handshake, not the envelope. gorilla does not fail an upgrade
	// on a subprotocol mismatch — it omits the response header and leaves the browser to fail the
	// connection — so the check is manual and happens before Upgrade. Rejecting a stale bundle here
	// turns what would be a silently-stops-updating client into a visible failure its reconnect loop
	// can surface.
	if !requestsRealtimeSubprotocol(r) {
		// RFC 9110 requires a 426 to name the protocol the client must switch to; without it the
		// response is just an opaque error code to anything that reads the header rather than the body.
		w.Header().Set("Upgrade", "websocket")
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Realtime websocket protocol upgrade required",
			zap.String(logkeys.Operation, "realtime_websocket_handshake"),
			zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
			zap.String(logkeys.Expected, common_ws.Subprotocol))
		http.Error(w, "WebSocket subprotocol "+common_ws.Subprotocol+" is required", http.StatusUpgradeRequired)
		return
	}

	// A shutting-down hub is not a client problem: 429 would tell the client it is over its own
	// connection cap and to shed connections, when the correct signal is "this server is going away,
	// come back (or fail over) later".
	if h.realtimeHub.Closed() {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Warn("Realtime websocket rejected during shutdown",
			zap.String(logkeys.Operation, "realtime_websocket_handshake"),
			zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
			zap.String(appLogger.KeyOutcome, "service_unavailable"))
		http.Error(w, "Realtime transport is shutting down", http.StatusServiceUnavailable)
		return
	}

	// Capture one coherent realtime view for the complete handshake. The same derived configuration
	// governs origin validation and the accepted connection, so a concurrent configuration reload
	// cannot mix the two snapshots.
	handshakeConfig := h.realtimeHub.CaptureConnectionConfig()

	// Optimistic fast path: answering the cap before the handshake lets the client see a real 429.
	// The authoritative check runs inside the hub's registration critical section, where check and
	// insert share one lock acquisition.
	userID := (*user).GetID()
	if !h.realtimeHub.CanAcceptConnectionWithConfig(userID, handshakeConfig) {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Warn("Realtime websocket connection limit reached",
			zap.String(logkeys.Operation, "realtime_websocket_handshake"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyOutcome, "rate_limited"))
		http.Error(w, "Too many websocket connections for user", http.StatusTooManyRequests)
		return
	}

	upgrader := h.realtimeUpgrader
	authToken, tokenErr := extractAuthTokenFromRequest(r, h.startup.SessionCookieName())
	if tokenErr != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	// The auth middleware already validated this session; bind the socket to it so revocation
	// closes the socket and it never outlives the token's expiry.
	claims, tokenErr := h.serviceHandler.ValidateSessionToken(r.Context(), authToken)
	if tokenErr != nil || claims.UserID != uint64(userID) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	session := common_ws.Session{Token: authToken, ExpiresAt: claims.ExpiresAt, Check: func(ctx context.Context) error {
		_, err := h.serviceHandler.ValidateAuthTokenAndGetUser(ctx, authToken)
		return err
	}}
	if upgrader == nil {
		upgrader = newRealtimeUpgrader(handshakeConfig.AllowedOrigins)
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Warn("Realtime websocket upgrade failed",
			zap.String(logkeys.Operation, "realtime_websocket_upgrade"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Error(err))
		return
	}
	defer conn.Close()

	if serveErr := h.realtimeHub.ServeConnectionWithConfig(r.Context(), *user, common_ws.NewGorillaConn(conn), handshakeConfig, session); serveErr != nil {
		// The HTTP response is already committed, so a rejection after the upgrade is reported with a
		// websocket close frame instead of a status code. Losing the race for the last connection slot
		// is the expected case and is retryable by the client; a hub that is already shutting down is
		// a normal going-away, not a server fault.
		closeCode := websocket.CloseInternalServerErr
		switch {
		case errors.Is(serveErr, common_ws.ErrTooManyConns):
			closeCode = websocket.CloseTryAgainLater
		case errors.Is(serveErr, common_ws.ErrHubClosed):
			closeCode = websocket.CloseGoingAway
		}
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(closeCode, serveErr.Error()),
			time.Now().Add(realtimeCloseWriteTimeout),
		)
		fields := []zap.Field{
			zap.String(logkeys.Operation, "serve_realtime_websocket_connection"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Error(serveErr),
		}
		if errors.Is(serveErr, common_ws.ErrTooManyConns) || errors.Is(serveErr, common_ws.ErrHubClosed) {
			appLogger.For(r.Context(), h.logger, commonAPILoggerName).Warn("Realtime websocket connection rejected", fields...)
		} else {
			appLogger.For(r.Context(), h.logger, commonAPILoggerName).Error("Realtime websocket connection failed", fields...)
		}
	}
}

// CloseRealtimeHub CloseRealtimeHub handles a backend operation in the API/request flow and delegates business logic to the service layer.
//
// Parameters:
// - timeout (time.Duration): is the maximum duration allowed for graceful shutdown/cleanup logic.
func (h *CommonAPIHandler[U]) CloseRealtimeHub(timeout time.Duration) {
	if h == nil || h.realtimeHub == nil {
		return
	}
	h.realtimeHub.Close(timeout)
}

// requestsRealtimeSubprotocol reports whether the client asked for the realtime subprotocol in its
// Sec-WebSocket-Protocol header.
func requestsRealtimeSubprotocol(r *http.Request) bool {
	return slices.Contains(websocket.Subprotocols(r), common_ws.Subprotocol)
}

// newRealtimeUpgrader builds the built-in upgrader for one handshake. It always advertises the
// realtime subprotocol — gorilla only echoes a subprotocol it was told about, and a browser fails a
// connection whose requested subprotocol is not confirmed. The allowlist is captured (copied) here
// because realtime_ws_allowed_origins is hot: the upgrader must judge the handshake against the very
// generation the accepted connection is configured from.
func newRealtimeUpgrader(allowedOrigins []string) *websocket.Upgrader {
	allowedOrigins = append([]string(nil), allowedOrigins...)
	return &websocket.Upgrader{
		ReadBufferSize:  realtimeUpgradeBufferSize,
		WriteBufferSize: realtimeUpgradeBufferSize,
		Subprotocols:    []string{common_ws.Subprotocol},
		CheckOrigin: func(request *http.Request) bool {
			return realtimeOriginAllowed(request, allowedOrigins)
		},
	}
}

// realtimeOriginAllowed applies the shared frontend-origin policy to a websocket handshake: an
// absent Origin (non-browser clients) is allowed, "*" allows everything, an explicit allowlist is
// matched on normalized origins, and an empty allowlist falls back to same-host validation — the
// same safe default gorilla applies when CheckOrigin is nil.
func realtimeOriginAllowed(request *http.Request, allowedOrigins []string) bool {
	return origin.AllowsRequestOrigin(request.Header.Get("Origin"), request.Host, allowedOrigins)
}
