package ports

import (
	"context"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"google.golang.org/protobuf/proto"
)

// RealtimeEvent describes a single realtime publish: the topics it fans out to, its namespaced
// event type, when it happened, and the application-defined payload message.
//
// The name is deliberately kept despite proto_models.RealtimeEvent — package qualification
// disambiguates the two, matching the ports.ChatEvent precedent. This type is the transport-neutral
// description; the transport marshals Payload into the wire envelope's bytes field.
//
// Topics of the form "user.{id}" address a user wherever they are connected; every other topic is
// an entity topic that clients must subscribe to explicitly. A given logical event is published to
// user topics OR an entity topic, never both, so no client receives it twice.
type RealtimeEvent struct {
	// Topics lists every topic this event is published on.
	Topics []string
	// EventType is the namespaced event type, e.g. "chat.message_sent".
	EventType string
	// OccurredAt is when the event happened; the transport substitutes the publish time when zero.
	OccurredAt time.Time
	// Payload is the application-defined proto message carried inside the envelope.
	Payload proto.Message
}

// RealtimePublisher fans a RealtimeEvent out to connected clients. Implementations are best-effort:
// delivery is at-most-once with no replay, and errors must never be propagated into business flows.
//
// Keeping this an interface is deliberate: a future multi-instance implementation (publish to a
// broker, each instance fans out to its local hub) slots in behind it without touching call sites.
type RealtimePublisher interface {
	Publish(ctx context.Context, event *RealtimeEvent) error
}

// PublishAfterCommit publishes event once the transaction on ctx commits, so clients never observe
// events for uncommitted state.
//
// Behavior change (round-2 decision D8): when ctx carries no transaction the event is published
// immediately instead of being silently dropped. Every pre-existing chat publish site ignored the
// hook registration's false return, so a publish outside a transaction simply vanished; the
// immediate-publish fallback fixes that.
//
// Mid-stream ephemera are the documented exception and must NOT use this helper: the bot transcript
// writer's stream publishes and the bot dispatcher's typing start/stop publish on a deliberately
// transaction-cleared context because deltas and typing indicators must not wait for a commit.
// Routing those through the post-commit hook would defer every token delta to commit time.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, tracing, and the ambient transaction.
//   - p: Publisher the event is handed to.
//   - event: Event to publish.
//
// Returns:
//   - result: Error encountered while executing this operation; nil on success or when deferred.
func PublishAfterCommit(
	ctx context.Context,
	p RealtimePublisher,
	event *RealtimeEvent,
) error {
	if p == nil || event == nil {
		return nil
	}
	return afterCommit(ctx, func(publishCtx context.Context) error {
		return p.Publish(publishCtx, event)
	})
}

// afterCommit runs publish once the transaction on ctx commits, or immediately when ctx carries no
// transaction. It is the single place the post-commit publish discipline lives: both PublishAfterCommit
// and PublishChatEventAfterCommit route through it, so the D8 fallback cannot drift between them.
func afterCommit(ctx context.Context, publish common_persistence.PostCommitHook) error {
	if common_persistence.AddPostCommitHookFromContext(ctx, publish) {
		return nil
	}
	return publish(ctx)
}
