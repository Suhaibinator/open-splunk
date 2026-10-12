package common_persistence

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/config"
)

// defaultPostCommitHookTimeout is the post_commit_hook_timeout default, used by
// stores built without a sampler and when a sampled value is non-positive.
var defaultPostCommitHookTimeout = config.NewRootView(config.GetDefaults()).PostCommitHookTimeout()

// postCommitHookTimeout samples the hot post_commit_hook_timeout key once per
// standalone post-commit hook drain; transactionWithHooks maps a non-positive
// value to the key's default.
func (h *CommonPersistenceHandler[U]) postCommitHookTimeout() time.Duration {
	return h.runtime().PostCommitHookTimeout()
}

type databasePoolPolicy struct{ open, idle, lifetime, idleTime int }

// applyPoolPolicy adjusts the existing pool; it never replaces connections or
// alters ownership. Serialize sampling and application to prevent an older
// concurrent sample from overwriting a newer policy.
func (h *CommonPersistenceHandler[U]) applyPoolPolicy() {
	if h.runtime == nil || h.db == nil {
		return
	}
	h.poolPolicyMu.Lock()
	defer h.poolPolicyMu.Unlock()
	v := h.runtime()
	p := databasePoolPolicy{v.DBMaxOpenConns(), v.DBMaxIdleConns(), v.DBConnMaxLifetime(), v.DBConnMaxIdleTime()}
	if h.poolPolicySet && p == h.poolPolicy {
		return
	}
	db, err := h.db.DB()
	if err != nil {
		return
	}
	db.SetMaxOpenConns(p.open)
	db.SetMaxIdleConns(p.idle)
	db.SetConnMaxLifetime(time.Duration(p.lifetime) * time.Second)
	db.SetConnMaxIdleTime(time.Duration(p.idleTime) * time.Second)
	h.poolPolicy = p
	h.poolPolicySet = true
}

// Polling also detects a disabled monitor becoming enabled without restarting
// the owned lifecycle. Short configured intervals retain their resolution.
func monitorPollInterval(interval time.Duration) time.Duration {
	if interval <= 0 || interval > time.Second {
		return time.Second
	}
	return interval
}
