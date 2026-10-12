package bot

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_converter"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

const botDispatcherLoggerName = "common_service.bot_dispatcher"

// Dispatcher is the agent runtime's server-side support surface. It builds the run-scoped transcript
// writer used to persist agent rows and manages the typing indicator for a directly driven agent
// run. The service handler wires it through this interface (no concrete-type assertion), so
// decorating implementations must forward every method below — embedding *AsyncDispatcher does so
// automatically.
type Dispatcher interface {
	// SetTypingEmitter wires the emitter used to publish bot typing indicators. Called by the
	// service handler during wiring; decorators must forward it.
	SetTypingEmitter(emitter ports.AgentTypingEmitter)

	// SetChatEventPublisher wires the publisher used for streamed agent event fanout. Called by the
	// service handler during wiring; decorators must forward it.
	SetChatEventPublisher(publisher ports.ChatEventPublisher)

	// NewRunTranscriptWriter builds a run-scoped ports.AgentTranscriptWriter for a directly driven
	// agent run (fresh turn or approval resume). The agent runtime binds this as its writer factory;
	// decorators must forward it.
	NewRunTranscriptWriter(
		ctx context.Context,
		channel *common_service_models.ChatChannel,
		botUserID common_service_models.UserIdType,
		triggeringUserID common_service_models.UserIdType,
	) ports.AgentTranscriptWriter

	// StartRunTypingIndicator starts the delayed/refreshed typing indicator for a run-driven agent
	// turn, returning a stop func. The agent runtime binds this as its typing starter; decorators must
	// forward it.
	StartRunTypingIndicator(
		ctx context.Context,
		channel *common_service_models.ChatChannel,
		botUserID common_service_models.UserIdType,
	) func()
}

// AsyncDispatcher provides the agent runtime's run-scoped transcript writer factory and typing
// indicator. It carries the shared persistence/publisher/converter wiring those writes and events
// flow through.
type AsyncDispatcher[U common_service_models.UserLike] struct {
	runtimeConfig            RuntimeConfigProvider
	persistenceHandler       common_persistence.CommonPersistenceInterface
	typingEmitter            ports.AgentTypingEmitter
	chatEventPublisher       ports.ChatEventPublisher
	notificationSink         ports.ChatNotificationSink
	internalAttachmentStore  ports.InternalAttachmentBoundedStore
	logger                   *zap.Logger
	streamPublishHandoffOnce sync.Once
	streamPublishHandoffs    chan struct{}
	postCommitMu             sync.Mutex
	postCommitWg             sync.WaitGroup
	postCommitStopping       bool

	// converter maps persistence transcript rows to service models for MESSAGE_SENT fanout from the
	// transcript writer. Optional; the writer skips fanout when nil.
	converter common_converter.ConverterInterface
}

// RuntimeConfigView supplies typing and post-commit policy. Each operation
// captures one immutable configuration generation.
type RuntimeConfigView interface {
	PostCommitHookTimeout() time.Duration
	BotTypingDelay() time.Duration
	BotTypingInterval() time.Duration
}

// RuntimeConfigProvider captures one immutable bot configuration generation.
type RuntimeConfigProvider func() RuntimeConfigView

var (
	_ Dispatcher        = (*AsyncDispatcher[common_service_models.User])(nil)
	_ RuntimeConfigView = config.RootView{}
)

// NewDispatcher constructs an AsyncDispatcher bound to the given persistence handler and config.
//
// Parameters:
//   - provider: Supplies current typing and post-commit policy. Nil selects fixed defaults.
//   - persistenceHandler: Persistence handler the transcript writer records through.
//   - logger: Structured logger; nil is replaced with a no-op logger.
func NewDispatcher[U common_service_models.UserLike](
	provider RuntimeConfigProvider,
	persistenceHandler common_persistence.CommonPersistenceInterface,
	logger *zap.Logger,
) *AsyncDispatcher[U] {
	if logger == nil {
		logger = zap.NewNop()
	}

	if provider == nil {
		root := config.NewRootView(config.GetDefaults())
		provider = func() RuntimeConfigView { return root }
	}

	return &AsyncDispatcher[U]{
		runtimeConfig:         provider,
		persistenceHandler:    persistenceHandler,
		logger:                logger,
		streamPublishHandoffs: make(chan struct{}, defaultMaxAssistantStreamPublishHandoffs),
	}
}

func (d *AsyncDispatcher[U]) tryAcquireStreamPublishHandoff() bool {
	if d == nil {
		return false
	}
	d.streamPublishHandoffOnce.Do(func() {
		if d.streamPublishHandoffs == nil {
			d.streamPublishHandoffs = make(chan struct{}, defaultMaxAssistantStreamPublishHandoffs)
		}
	})
	select {
	case d.streamPublishHandoffs <- struct{}{}:
		return true
	default:
		return false
	}
}

func (d *AsyncDispatcher[U]) releaseStreamPublishHandoff() {
	if d != nil && d.streamPublishHandoffs != nil {
		<-d.streamPublishHandoffs
	}
}

// SetTypingEmitter wires the emitter used to publish typing indicators.
func (d *AsyncDispatcher[U]) SetTypingEmitter(emitter ports.AgentTypingEmitter) {
	if d == nil {
		return
	}
	d.typingEmitter = emitter
}

// SetConverter wires the converter used by the transcript writer to map persistence rows to service
// models for MESSAGE_SENT fanout.
func (d *AsyncDispatcher[U]) SetConverter(converter common_converter.ConverterInterface) {
	if d == nil {
		return
	}
	d.converter = converter
}

// SetInternalAttachmentStore wires the privileged server-only provider/tool blob store.
func (d *AsyncDispatcher[U]) SetInternalAttachmentStore(store ports.InternalAttachmentBoundedStore) {
	if d != nil {
		d.internalAttachmentStore = store
	}
}

// converterMessageToService maps a persistence chat message to its service model via the wired
// converter. Returns nil when no converter is set.
func (d *AsyncDispatcher[U]) converterMessageToService(msg *common_persistence_models.ChatMessage) *common_service_models.ChatMessage {
	if d == nil || d.converter == nil {
		return nil
	}
	return d.converter.PersistenceChatMessageToService(msg)
}

// SetChatEventPublisher updates the publisher used for streamed agent event fanout.
func (d *AsyncDispatcher[U]) SetChatEventPublisher(publisher ports.ChatEventPublisher) {
	if d == nil {
		return
	}
	d.chatEventPublisher = publisher
}

// startTypingIndicator starts a delayed, periodically-refreshed typing indicator for the given
// channel/bot, returning a stop func that cancels it. It is a no-op (returning an empty stop func)
// when typing is not wired or the delay is non-positive.
func (d *AsyncDispatcher[U]) startTypingIndicator(ctx context.Context, channel *common_service_models.ChatChannel, botUserID common_service_models.UserIdType) func() {
	if d == nil || d.typingEmitter == nil || channel == nil {
		return func() {}
	}
	view := d.runtimeConfig()
	delay := view.BotTypingDelay()
	interval := view.BotTypingInterval()
	if delay <= 0 || channel.ID == 0 || botUserID == 0 {
		return func() {}
	}

	// WARNING: this is one of the three documented exceptions to post-commit publishing. A typing
	// indicator is ephemeral and only useful while the turn is still running, so it publishes on a
	// deliberately transaction-cleared context and must never be routed through
	// ports.PublishAfterCommit — deferring it to commit time would emit "typing" after the reply
	// has already landed.
	typingCtx := txClearedSRouterContext[U](ctx)
	// typingCtx is fixed for the indicator's lifetime, so bind the trace ID once
	// instead of re-wrapping on every tick.
	var typingLogger *zap.Logger
	if d.logger != nil {
		typingLogger = appLogger.For(typingCtx, d.logger, botDispatcherLoggerName)
	}
	done := make(chan struct{})
	channelID := channel.ID
	emitter := d.typingEmitter

	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		started := false
		select {
		case <-timer.C:
			if typingCtx.Err() == nil {
				if err := emitter.StartTyping(typingCtx, botUserID, channelID); err == nil {
					started = true
				} else if typingLogger != nil {
					if ce := typingLogger.Check(zap.DebugLevel, "bot typing start failed"); ce != nil {
						ce.Write(
							zap.Uint64(appLogger.KeyBotUserID, uint64(botUserID)),
							zap.Uint64(appLogger.KeyChannelID, uint64(channelID)),
							zap.Error(err),
						)
					}
				}
			}
		case <-done:
			return
		case <-ctx.Done():
			return
		}

		if interval > 0 {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
		refreshLoop:
			for {
				select {
				case <-ticker.C:
					if typingCtx.Err() != nil {
						break refreshLoop
					}
					if err := emitter.StartTyping(typingCtx, botUserID, channelID); err == nil {
						started = true
					} else if typingLogger != nil {
						if ce := typingLogger.Check(zap.DebugLevel, "bot typing refresh failed"); ce != nil {
							ce.Write(
								zap.Uint64(appLogger.KeyBotUserID, uint64(botUserID)),
								zap.Uint64(appLogger.KeyChannelID, uint64(channelID)),
								zap.Error(err),
							)
						}
					}
				case <-done:
					break refreshLoop
				case <-ctx.Done():
					break refreshLoop
				}
			}
		} else {
			select {
			case <-done:
			case <-ctx.Done():
			}
		}

		if started {
			// WARNING: the typing-stop publish is the third documented exception to post-commit
			// publishing. It clears the transaction for the same reason as the start above and must
			// never be routed through ports.PublishAfterCommit.
			stopCtx := txClearedSRouterContext[U](context.WithoutCancel(typingCtx))
			if err := emitter.StopTyping(stopCtx, botUserID, channelID); err != nil && typingLogger != nil {
				if ce := typingLogger.Check(zap.DebugLevel, "bot typing stop failed"); ce != nil {
					ce.Write(
						zap.Uint64(appLogger.KeyBotUserID, uint64(botUserID)),
						zap.Uint64(appLogger.KeyChannelID, uint64(channelID)),
						zap.Error(err),
					)
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}

// StartRunTypingIndicator starts the delayed/refreshed typing indicator for a run-driven agent turn.
func (d *AsyncDispatcher[U]) StartRunTypingIndicator(
	ctx context.Context,
	channel *common_service_models.ChatChannel,
	botUserID common_service_models.UserIdType,
) func() {
	return d.startTypingIndicator(ctx, channel, botUserID)
}

func (d *AsyncDispatcher[U]) beginPostCommitHookRun() bool {
	if d == nil {
		return false
	}
	d.postCommitMu.Lock()
	defer d.postCommitMu.Unlock()
	if d.postCommitStopping {
		return false
	}
	d.postCommitWg.Add(1)
	return true
}

// StopPostCommitHooks prevents new post-commit runners and waits for runners already admitted.
// It deliberately does not cancel their contexts: each runner keeps the PostCommitHookTimeout
// budget captured when it started.
func (d *AsyncDispatcher[U]) StopPostCommitHooks() {
	if d == nil {
		return
	}
	d.postCommitMu.Lock()
	d.postCommitStopping = true
	d.postCommitMu.Unlock()
	d.postCommitWg.Wait()
}

// runPostCommitHooks drains and runs any post-commit hooks registered on the transaction, off the
// request path, on the bounded PostCommitHookTimeout budget the PostCommitHook contract promises.
// Hook failures are logged, not returned.
func (d *AsyncDispatcher[U]) runPostCommitHooks(
	ctx context.Context,
	tx *common_persistence.GormTransactionWithHooks,
) {
	if tx == nil {
		return
	}
	hooks := tx.DrainPostCommitHooks()
	if len(hooks) == 0 {
		return
	}
	if !d.beginPostCommitHookRun() {
		return
	}

	hookCtx, cancel := context.WithTimeout(txClearedSRouterContext[U](context.WithoutCancel(ctx)), d.runtimeConfig().PostCommitHookTimeout())
	go func() {
		defer d.postCommitWg.Done()
		defer cancel()
		for i, hook := range hooks {
			if hook == nil {
				continue
			}
			d.runPostCommitHook(hookCtx, hook, i, len(hooks))
		}
	}()
}

// runPostCommitHook runs a single drained post-commit hook, containing a panic so one misbehaving
// hook cannot terminate the process or skip the hooks queued after it.
func (d *AsyncDispatcher[U]) runPostCommitHook(ctx context.Context, hook common_persistence.PostCommitHook, index, count int) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger := d.logger
			if logger == nil {
				logger = zap.NewNop()
			}
			appLogger.For(ctx, logger, botDispatcherLoggerName).Error("post-commit hook panicked; recovered",
				zap.String(logkeys.Operation, "bot_dispatch_post_commit_hook"),
				zap.String(appLogger.KeyPanicType, fmt.Sprintf("%T", recovered)),
				zap.ByteString(appLogger.KeyStacktrace, debug.Stack()),
				zap.Int(appLogger.KeyHookIndex, index),
				zap.Int(appLogger.KeyHookCount, count))
		}
	}()
	if err := hook(ctx); err != nil {
		appLogger.LogOperationWarn(appLogger.For(ctx, d.logger, botDispatcherLoggerName), "post-commit hook failed", "bot_dispatch_post_commit_hook", err,
			zap.Int(appLogger.KeyHookIndex, index),
			zap.Int(appLogger.KeyHookCount, count))
	}
}

// txClearedSRouterContext returns a copy of the SRouter context with any bound transaction cleared,
// so background work (typing, post-commit hooks) does not reuse the request's committed transaction.
func txClearedSRouterContext[U common_service_models.UserLike](baseCtx context.Context) context.Context {
	ctx := scontext.CopySRouterContext[common_service_models.UserIdType, U](baseCtx, baseCtx)
	scontext.ClearTransaction[common_service_models.UserIdType, U](ctx)
	return ctx
}
