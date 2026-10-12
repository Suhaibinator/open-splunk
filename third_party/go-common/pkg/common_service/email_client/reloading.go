package email_client

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_util/reloading"
	"github.com/Suhaibinator/go-common/pkg/config"
	"go.uber.org/zap"
)

type clientKey struct {
	enabled     bool
	provider    config.EmailProvider
	from        string
	name        string
	credentials string
	token       string
	user        string
	endpoint    string
	postalKey   string
	resendKey   string
}

func (k clientKey) EmailEnabled() bool                  { return k.enabled }
func (k clientKey) EmailProvider() config.EmailProvider { return k.provider }
func (k clientKey) EmailFromAddress() string            { return k.from }
func (k clientKey) EmailFromName() string               { return k.name }
func (k clientKey) GmailCredentials() config.Secret     { return config.NewSecret(k.credentials) }
func (k clientKey) GmailToken() config.Secret           { return config.NewSecret(k.token) }
func (k clientKey) GmailUserID() string                 { return k.user }
func (k clientKey) PostalAPIEndpoint() string           { return k.endpoint }
func (k clientKey) PostalAPIKey() config.Secret         { return config.NewSecret(k.postalKey) }
func (k clientKey) ResendAPIKey() config.Secret         { return config.NewSecret(k.resendKey) }

type reloadingAdapter struct {
	memo     *config.StrictGenerationMemo[config.EmailView, clientKey]
	provider func() config.EmailView
	resource *reloading.Resource[clientKey, *EmailClient]
}

// CreateReloadingEmailClient samples provider on every send, including when disabled at startup.
func CreateReloadingEmailClient(provider func() config.EmailView, logger *zap.Logger) *EmailClient {
	if logger == nil {
		logger = zap.NewNop()
	}
	a := &reloadingAdapter{provider: provider, memo: config.NewStrictGenerationMemo(emailKeyFromView, func(err error) {
		logger.Named(emailLoggerName).Error("active email configuration unreadable", zap.Error(err))
	})}
	a.resource = reloading.New(func(k clientKey) (*EmailClient, error) { return CreateEmailClientFromView(k, logger) }, func(c *EmailClient) {
		if err := c.Stop(); err != nil {
			logger.Warn("failed to close retired email client", zap.Error(err))
		}
	})
	return newEmailClient(a, logger)
}
func (a *reloadingAdapter) acquire() (*EmailClient, func(), error) {
	if a.provider == nil {
		return nil, nil, config.ErrNilView
	}
	key, err := a.memo.Get(a.provider())
	if err != nil {
		return nil, nil, err
	}
	return a.resource.Acquire(key)
}
func (a *reloadingAdapter) SendEmail(ctx context.Context, to, subject, body string) error {
	c, release, err := a.acquire()
	if err != nil {
		return err
	}
	defer release()
	return c.SendEmail(ctx, to, subject, body)
}
func (a *reloadingAdapter) SendHTMLEmail(ctx context.Context, to, subject, body string) error {
	c, release, err := a.acquire()
	if err != nil {
		return err
	}
	defer release()
	return c.SendHTMLEmail(ctx, to, subject, body)
}
func (a *reloadingAdapter) Close() error { a.resource.Close(); return nil }

// emailKeyFromView materializes credentials once per immutable generation.
func emailKeyFromView(v config.EmailView) (clientKey, error) {
	if !v.EmailEnabled() {
		return clientKey{}, nil
	}
	return clientKey{
		enabled:     true,
		provider:    v.EmailProvider(),
		from:        v.EmailFromAddress(),
		name:        v.EmailFromName(),
		credentials: v.GmailCredentials().StringValue(),
		token:       v.GmailToken().StringValue(),
		user:        v.GmailUserID(),
		endpoint:    v.PostalAPIEndpoint(),
		postalKey:   v.PostalAPIKey().StringValue(),
		resendKey:   v.ResendAPIKey().StringValue(),
	}, nil
}
