package common_persistence

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
)

// RegisterNotificationPostCommitOrSuppress registers a hook when the ambient
// transaction supports it. A hook-less ambient transaction suppresses the
// best-effort invalidation, leaving clients to reconcile from the durable
// inbox after commit. False means there is no ambient transaction and the
// caller may publish immediately.
func RegisterNotificationPostCommitOrSuppress(ctx context.Context, hook PostCommitHook) bool {
	if AddPostCommitHookFromContext(ctx, hook) {
		return true
	}
	tx, ok := scontext.GetTransaction(ctx)
	return ok && tx != nil
}
