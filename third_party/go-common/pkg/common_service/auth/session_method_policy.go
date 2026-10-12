package auth

import (
	"context"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
)

// SessionAuthMethodPolicy optionally limits persisted authentication methods at
// issuance, validation and refresh. Configuring it forces session validation to
// consult the native persisted session row rather than a token identity cache.
// Nil retains the existing unrestricted behavior for other GoCommon applications.
type SessionAuthMethodPolicy func(context.Context, token.SessionAuthMethod) error

func WithSessionAuthMethodPolicy(policy SessionAuthMethodPolicy) HandlerOption {
	return func(options *handlerOptions) { options.sessionAuthMethodPolicy = policy }
}
