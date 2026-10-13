package agentkit

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/jellydator/ttlcache/v3"
)

// ErrActorUnavailable classifies a missing or unattachable triggering actor.
// It aliases the executor-level sentinel so errors.Is works across the agentkit,
// app-executor, and durable-queue boundaries.
var ErrActorUnavailable = ports.ErrAgentToolActorUnavailable

// ActorLookup resolves the triggering user by ID so tool reads can run under that
// user's authority. It returns (nil, nil) for an unknown user; Attach converts that
// lookup miss into ErrActorUnavailable.
type ActorLookup[U any] func(ctx context.Context, userID common_service_models.UserIdType) (*U, error)

// ActorContext attaches the triggering user's identity to a read context so
// downstream domain reads (access checks) run under that user's authority. It
// caches resolved actors for a TTL to avoid a user lookup on every tool call in a
// burst.
//
// U is the app's concrete user type (e.g. its ApplicationUser). It is parameterized
// as `any` rather than common_service_models.UserLike because scontext stores the
// user as *U and the user ID is supplied explicitly by the caller — the harness
// never needs UserLike's accessors, and requiring the value type to satisfy
// UserLike would reject an app whose accessors have pointer receivers.
type ActorContext[U any] struct {
	lookup ActorLookup[U]
	cache  *ttlcache.Cache[common_service_models.UserIdType, *U]
}

// NewActorContext builds an ActorContext. lookup is required. When cacheTTL and
// cacheMaxEntries are positive, resolved actors are cached for cacheTTL; otherwise
// every attach performs a fresh lookup.
//
// Touch-on-hit is disabled so a cached identity expires cacheTTL after resolution
// even under constant use — permission or ban changes must surface within one TTL.
// The cache's background cleanup goroutine is deliberately not started: Get treats
// expired entries as misses on its own, capacity bounds memory, and ActorContext
// has no Close to stop a goroutine with.
func NewActorContext[U any](lookup ActorLookup[U], cacheTTL time.Duration, cacheMaxEntries int) *ActorContext[U] {
	var cache *ttlcache.Cache[common_service_models.UserIdType, *U]
	if cacheTTL > 0 && cacheMaxEntries > 0 {
		cache = ttlcache.New(
			ttlcache.WithTTL[common_service_models.UserIdType, *U](cacheTTL),
			ttlcache.WithCapacity[common_service_models.UserIdType, *U](uint64(cacheMaxEntries)),
			ttlcache.WithDisableTouchOnHit[common_service_models.UserIdType, *U](),
		)
	}
	return &ActorContext[U]{lookup: lookup, cache: cache}
}

// ContextWithoutActor clones ctx's SRouter context and removes both forms of
// authenticated identity. Cloning is essential because SRouter's WithUser helpers
// mutate the stored SRouterContext by pointer; clearing or replacing an actor on a
// shallow context derived with context.WithoutCancel would otherwise mutate the
// caller's request context too. Non-identity SRouter metadata is preserved.
func ContextWithoutActor[U any](ctx context.Context) context.Context {
	actorless := scontext.CopySRouterContext[common_service_models.UserIdType, U](ctx, ctx)
	return scontext.ClearUserID[common_service_models.UserIdType, U](scontext.ClearUser[common_service_models.UserIdType, U](actorless))
}

// Attach resolves userID (from cache when possible) and returns an isolated
// context carrying exactly that user's identity. The input context's ambient actor
// is removed before lookup. Nil configuration, a zero ID, or an unknown user is a
// terminal actor-unavailable error; returning the original context in any of those
// cases could silently retain a more privileged caller. A real lookup error
// propagates unchanged so the runtime may classify it as retryable.
func (a *ActorContext[U]) Attach(ctx context.Context, userID common_service_models.UserIdType) (context.Context, error) {
	actorCtx := ContextWithoutActor[U](ctx)
	if a == nil {
		return nil, fmt.Errorf("%w: actor context is not configured", ErrActorUnavailable)
	}
	if userID == 0 {
		return nil, fmt.Errorf("%w: triggering user ID is zero", ErrActorUnavailable)
	}
	if user := a.getCached(userID); user != nil {
		return withUser(actorCtx, userID, user), nil
	}
	if a.lookup == nil {
		return nil, fmt.Errorf("%w: actor lookup is not configured", ErrActorUnavailable)
	}
	user, err := a.lookup(actorCtx, userID)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, fmt.Errorf("%w: triggering user %d was not found", ErrActorUnavailable, userID)
	}
	a.putCached(userID, user)
	return withUser(actorCtx, userID, user), nil
}

func (a *ActorContext[U]) getCached(userID common_service_models.UserIdType) *U {
	if a == nil || a.cache == nil {
		return nil
	}
	item := a.cache.Get(userID)
	if item == nil {
		return nil
	}
	return item.Value()
}

func (a *ActorContext[U]) putCached(userID common_service_models.UserIdType, user *U) {
	if a == nil || a.cache == nil || user == nil {
		return
	}
	a.cache.Set(userID, user, ttlcache.DefaultTTL)
}

func withUser[U any](ctx context.Context, userID common_service_models.UserIdType, user *U) context.Context {
	ctx = scontext.WithUserID[common_service_models.UserIdType, U](ctx, userID)
	return scontext.WithUser[common_service_models.UserIdType](ctx, user)
}
