package chat

import (
	"sync"
	"time"

	"github.com/Suhaibinator/go-common/pkg/config"
)

// PolicyView captures one immutable generation for all operational chat keys.
// Providers must reuse its identity only while all of these contents are unchanged.
type PolicyView interface {
	config.ChatView
	EnableAttachments() bool
	PostCommitHookTimeout() time.Duration
}

var defaultPolicy = sync.OnceValue(func() PolicyView { return config.NewRootView(config.GetDefaults()) })
