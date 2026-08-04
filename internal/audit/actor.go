package audit

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

type actorContextKey struct{}

// WithActor returns a child context carrying one validated, detached actor.
// The private key prevents callers from bypassing validation with an arbitrary
// context value. A browser principal installed here overrides the system actor
// used when no explicit actor is present.
func WithActor(ctx context.Context, actor Actor) (context.Context, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: audit actor context is nil", control.ErrInvalidArgument)
	}
	if !actor.Valid() {
		return nil, fmt.Errorf("%w: audit actor is invalid", control.ErrInvalidArgument)
	}
	return context.WithValue(ctx, actorContextKey{}, actor.detached()), nil
}

// ActorFromContext returns only an explicitly installed, validated actor. The
// append path supplies its system default separately when this returns false.
func ActorFromContext(ctx context.Context) (Actor, bool) {
	if ctx == nil {
		return Actor{}, false
	}
	actor, ok := ctx.Value(actorContextKey{}).(Actor)
	if !ok || !actor.Valid() {
		return Actor{}, false
	}
	return actor.detached(), true
}

func requireExplicitAdministrativeMutationActor(ctx context.Context) error {
	actor, explicit := ActorFromContext(ctx)
	if !explicit || !validAdministrativeMutationActor(actor) {
		return fmt.Errorf(
			"%w: audit actor cannot perform successful mutations",
			control.ErrInvalidArgument,
		)
	}
	return nil
}

func actorForAppend(ctx context.Context) Actor {
	if actor, ok := ActorFromContext(ctx); ok {
		return actor
	}
	return Actor{
		Kind: ActorKindSystem,
		ID:   defaultSystemActorID,
		Role: ActorRoleSystem,
	}
}
