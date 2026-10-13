package ports

import (
	"context"
	"errors"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
)

// ErrChatRealtimeUnavailable marks a transient authority lookup failure. Delivery
// is suspended and renewal may retry; it is not a definitive access denial.
var ErrChatRealtimeUnavailable = errors.New("chat realtime authority unavailable")

// ChatRealtimeCategory is the read capability required by a channel-topic event.
type ChatRealtimeCategory uint32

const (
	ChatRealtimeMessages ChatRealtimeCategory = 1 << iota
	ChatRealtimeReactions
	ChatRealtimeTyping
	ChatRealtimeChannel
	ChatRealtimeInference
	ChatRealtimeAll = ChatRealtimeMessages | ChatRealtimeReactions | ChatRealtimeTyping | ChatRealtimeChannel | ChatRealtimeInference
)

// ChatRealtimeAccessInvalidation identifies grants and participant audiences to
// discard. Zero IDs invalidate all; a user invalidation covers every channel.
type ChatRealtimeAccessInvalidation struct {
	ChannelID uint64
	UserID    common_service_models.UserIdType
	// AudiencesOnly discards cached participant audiences and leaves viewer grants live.
	AudiencesOnly bool
}

// ChatRealtimeAccessInvalidator is implemented by the realtime transport. Calls
// must synchronously suspend affected delivery before returning, except that an
// AudiencesOnly call only discards cached participant audiences.
type ChatRealtimeAccessInvalidator interface {
	InvalidateChatRealtimeAccess(ChatRealtimeAccessInvalidation)
}

// InvalidateChatRealtimeAfterCommit suspends affected access only after a successful commit.
// With no transaction the mutation must already be committed. Caller-owned transactions without
// post-commit hooks must call again after committing with a transaction-free context.
func InvalidateChatRealtimeAfterCommit(ctx context.Context, invalidator ChatRealtimeAccessInvalidator, change ChatRealtimeAccessInvalidation) {
	if invalidator == nil {
		return
	}
	hook := func(context.Context) error { invalidator.InvalidateChatRealtimeAccess(change); return nil }
	if !persistence.AddPostCommitHookFromContext(ctx, hook) {
		if tx, exists := scontext.GetTransaction(ctx); !exists || tx == nil {
			_ = hook(ctx)
		}
	}
}

// PublishChatAccessChangeAfterCommit announces a committed access change to a channel. One
// post-commit hook suspends grants and discards cached participant audiences before publication.
// The event reaches the refreshed participant audience. Bounded renewal workers restore
// authorized viewer subscriptions, whose clients refetch committed state on restoration.
//
// With no transaction the mutation must already be committed and all three steps run now. A
// caller-owned transaction without post-commit hooks publishes immediately, as
// PublishChatEventAfterCommit does, and must invalidate after committing. A nil newEvent, or one
// returning nil, only invalidates.
func PublishChatAccessChangeAfterCommit(
	ctx context.Context,
	invalidator ChatRealtimeAccessInvalidator,
	publisher ChatEventPublisher,
	change ChatRealtimeAccessInvalidation,
	newEvent func(ctx context.Context) *ChatEvent,
) error {
	announce := func(publishCtx context.Context) error {
		if publisher == nil || newEvent == nil {
			return nil
		}
		event := newEvent(publishCtx)
		if event == nil {
			return nil
		}
		event.AccessChange = true
		return publisher.PublishChatEvent(publishCtx, event)
	}
	hook := func(hookCtx context.Context) error {
		if invalidator != nil {
			change.AudiencesOnly = false
			invalidator.InvalidateChatRealtimeAccess(change)
		}
		return announce(hookCtx)
	}
	if persistence.AddPostCommitHookFromContext(ctx, hook) {
		return nil
	}
	if tx, exists := scontext.GetTransaction(ctx); exists && tx != nil {
		return announce(ctx)
	}
	return hook(ctx)
}
