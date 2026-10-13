package common_persistence

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
)

// PostCommitHookRegistrarFromContext returns the ambient transaction when it
// can register post-commit work. Cache-only callers do not require direct
// access to the underlying database handle.
func PostCommitHookRegistrarFromContext(ctx context.Context) (PostCommitHookRegistrar, bool) {
	tx, ok := scontext.GetTransaction(ctx)
	if !ok || tx == nil {
		return nil, false
	}

	registrar, ok := tx.(PostCommitHookRegistrar)
	return registrar, ok && registrar != nil
}

// DatabasePostCommitHookRegistrarFromContext is the stricter lifecycle
// preflight: managed account mutations require both a live database handle and
// post-commit hook support.
func DatabasePostCommitHookRegistrarFromContext(ctx context.Context) (PostCommitHookRegistrar, bool) {
	tx, ok := scontext.GetTransaction(ctx)
	if !ok || tx == nil || tx.GetDB() == nil {
		return nil, false
	}

	registrar, ok := tx.(PostCommitHookRegistrar)
	return registrar, ok && registrar != nil
}

// AddPostCommitHookFromContext registers a post-commit hook on the current transaction in the persistence flow for transactional database operations and repository access.
//
// The hook is best-effort and runs on a bounded budget after the commit; see
// PostCommitHook for the full contract, including why must-happen work belongs
// in the transaction (e.g. an outbox row) rather than in a hook.
func AddPostCommitHookFromContext(ctx context.Context, hook PostCommitHook) bool {
	if hook == nil {
		return false
	}

	registrar, ok := PostCommitHookRegistrarFromContext(ctx)
	if !ok {
		return false
	}

	registrar.AddPostCommitHook(hook)
	return true
}
