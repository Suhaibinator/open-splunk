package sms_client

import (
	"context"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_util/reloading"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

type clientKey struct {
	enabled      bool
	provider     config.SMSProvider
	from         string
	sid          string
	token        string
	vonageKey    string
	vonageSecret string
}

func (k clientKey) SMSEnabled() bool                            { return k.enabled }
func (k clientKey) SMSProvider() config.SMSProvider             { return k.provider }
func (k clientKey) SMSFromNumber() string                       { return k.from }
func (k clientKey) TwilioAccountSID() config.Secret             { return config.NewSecret(k.sid) }
func (k clientKey) TwilioAuthToken() config.Secret              { return config.NewSecret(k.token) }
func (k clientKey) VonageAPIKey() config.Secret                 { return config.NewSecret(k.vonageKey) }
func (k clientKey) VonageAPISecret() config.Secret              { return config.NewSecret(k.vonageSecret) }
func (k clientKey) PhoneNumberDefaultRegion() string            { return "" }
func (k clientKey) PhoneVerificationCooldown() time.Duration    { return 0 }
func (k clientKey) PhoneVerificationOTPValidity() time.Duration { return 0 }

type reloadingAdapter struct {
	memo     *config.StrictGenerationMemo[config.SmsView, clientKey]
	provider func() config.SmsView
	resource *reloading.Resource[clientKey, *SMSClient]
}

// CreateReloadingSMSClient samples provider on every send, including when disabled at startup.
func CreateReloadingSMSClient(provider func() config.SmsView, logger *zap.Logger) *SMSClient {
	if logger == nil {
		logger = zap.NewNop()
	}
	a := &reloadingAdapter{provider: provider, memo: config.NewStrictGenerationMemo(smsKeyFromView, func(err error) {
		logger.Named(smsLoggerName).Error("active sms configuration unreadable", zap.Error(err))
	})}
	a.resource = reloading.New(func(k clientKey) (*SMSClient, error) { return CreateSMSClientFromView(k, logger) }, func(c *SMSClient) {
		if err := c.Stop(); err != nil {
			logger.Warn("failed to close retired sms client", zap.Error(err))
		}
	})
	return NewSMSClient(a, logger)
}
func (a *reloadingAdapter) acquire() (*SMSClient, func(), error) {
	if a.provider == nil {
		return nil, nil, config.ErrNilView
	}
	key, err := a.memo.Get(a.provider())
	if err != nil {
		return nil, nil, err
	}
	return a.resource.Acquire(key)
}
func (a *reloadingAdapter) SendSMS(ctx context.Context, t proto_models.MessageType, to, message string) error {
	c, release, err := a.acquire()
	if err != nil {
		return err
	}
	defer release()
	return c.SendSMS(ctx, t, to, message)
}
func (a *reloadingAdapter) Close() error { a.resource.Close(); return nil }

// smsKeyFromView materializes credentials once per immutable generation.
func smsKeyFromView(v config.SmsView) (clientKey, error) {
	if !v.SMSEnabled() {
		return clientKey{}, nil
	}
	return clientKey{
		enabled:      true,
		provider:     v.SMSProvider(),
		from:         v.SMSFromNumber(),
		sid:          v.TwilioAccountSID().StringValue(),
		token:        v.TwilioAuthToken().StringValue(),
		vonageKey:    v.VonageAPIKey().StringValue(),
		vonageSecret: v.VonageAPISecret().StringValue(),
	}, nil
}
