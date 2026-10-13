package common_ws

import (
	"context"
	"errors"
	"sync"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
)

var (
	ErrNilCommandRouter    = errors.New("nil realtime command router")
	ErrNilCommandHandler   = errors.New("nil realtime command handler")
	ErrInvalidCommandType  = errors.New("invalid realtime command type")
	ErrReservedCommandType = errors.New("realtime command type is owned by the hub")
	ErrUnsupportedCommand  = errors.New("unsupported realtime command type")
	ErrCommandAlreadyBound = errors.New("realtime command handler already registered")
)

// CommandHandler processes a client-to-server realtime command.
//
// The command's type selects the handler, so handlers receive only the correlation ID and the
// type-specific payload bytes; decoding the payload into its own proto message is the handler's job.
// Handlers should be best-effort; returning an error will typically be logged by the caller.
type CommandHandler interface {
	HandleRealtimeCommand(ctx context.Context, userID common_service_models.UserIdType, requestID string, payload []byte) error
}

// CommandHandlerFunc adapts a function into a CommandHandler.
type CommandHandlerFunc func(ctx context.Context, userID common_service_models.UserIdType, requestID string, payload []byte) error

// HandleRealtimeCommand executes this method as part of the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - userID: Identifier of the user that sent the command.
//   - requestID: Client-generated correlation ID, empty when the client did not set one.
//   - payload: Serialized command-type-specific proto message.
func (f CommandHandlerFunc) HandleRealtimeCommand(ctx context.Context, userID common_service_models.UserIdType, requestID string, payload []byte) error {
	return f(ctx, userID, requestID, payload)
}

// CommandRouter defines the data structure used in the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
//
// Command types are plain namespaced strings ("chat.typing", ...) because go-common does not
// enumerate application command types any more than it enumerates application events.
type CommandRouter struct {
	mu       sync.RWMutex
	handlers map[string]CommandHandler

	defaultHandler CommandHandler
}

// NewCommandRouter executes this function as part of the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
func NewCommandRouter() *CommandRouter {
	return &CommandRouter{
		handlers: make(map[string]CommandHandler),
	}
}

// Register binds a handler to a command type. The hub-owned subscribe, unsubscribe, and
// notification activity types are rejected: registering them would produce a handler that is
// never reached (or, for notification activity, one that shadows the server-owned command).
//
// Parameters:
//   - commandType: The RealtimeCommand type string the handler serves.
//   - handler: Handler invoked for that type.
func (r *CommandRouter) Register(commandType string, handler CommandHandler) error {
	if r == nil {
		return ErrNilCommandRouter
	}
	if handler == nil {
		return ErrNilCommandHandler
	}
	if commandType == "" {
		return ErrInvalidCommandType
	}
	if commandType == CommandTypeSubscribe || commandType == CommandTypeUnsubscribe || commandType == NotificationActivityCommandType {
		return ErrReservedCommandType
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[commandType]; exists {
		return ErrCommandAlreadyBound
	}
	r.handlers[commandType] = handler
	return nil
}

// SetDefault installs the handler used for command types with no explicit registration.
//
// Parameters:
//   - handler: Fallback handler; nil restores "unsupported command type" behavior.
func (r *CommandRouter) SetDefault(handler CommandHandler) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultHandler = handler
}

// Dispatch routes one decoded command envelope to its handler.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - userID: Identifier of the user that sent the command.
//   - commandType: The RealtimeCommand type string.
//   - requestID: Client-generated correlation ID, empty when the client did not set one.
//   - payload: Serialized command-type-specific proto message.
func (r *CommandRouter) Dispatch(
	ctx context.Context,
	userID common_service_models.UserIdType,
	commandType string,
	requestID string,
	payload []byte,
) error {
	if r == nil {
		return ErrNilCommandRouter
	}
	if userID == 0 {
		return ErrInvalidUserID
	}
	if commandType == "" {
		return ErrInvalidCommandType
	}

	r.mu.RLock()
	handler := r.handlers[commandType]
	if handler == nil {
		handler = r.defaultHandler
	}
	r.mu.RUnlock()

	if handler == nil {
		return ErrUnsupportedCommand
	}
	return handler.HandleRealtimeCommand(ctx, userID, requestID, payload)
}

// RegisteredTypes executes this method as part of the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
func (r *CommandRouter) RegisteredTypes() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.handlers))
	for commandType := range r.handlers {
		out = append(out, commandType)
	}
	return out
}
