package auth

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

type sessionLoginAvailability struct{ email, phone, oauth bool }

func loginAvailability(view RuntimeConfigView) sessionLoginAvailability {
	return sessionLoginAvailability{email: view.EnableEmailAuth(), phone: view.SMSEnabled(), oauth: view.EnableOAuth()}
}

// validateSessionMethod uses native feature flags. Session rows hold the method;
// signed claims carry no method.
// Cached identities are fenced when login availability changes.
func (h *AuthHandler[U]) validateSessionMethod(view RuntimeConfigView, method token.SessionAuthMethod) error {
	availability := loginAvailability(view)
	switch method.Provider {
	case proto_models.AuthProvider_AUTH_PROVIDER_EMAIL:
		if availability.email && method.OAuthProviderSlug == "" {
			return nil
		}
	case proto_models.AuthProvider_AUTH_PROVIDER_PHONE:
		if availability.phone && method.OAuthProviderSlug == "" {
			return nil
		}
	case proto_models.AuthProvider_AUTH_PROVIDER_OAUTH:
		if availability.oauth && method.OAuthProviderSlug != "" {
			return nil
		}
	}
	return errors.ErrInvalidCredentials
}

// The login exchange already sampled its provider. Validate persisted OAuth
// sessions separately so issuing one during that exchange does not resample it.
func (h *AuthHandler[U]) validatePersistedSessionMethod(ctx context.Context, view RuntimeConfigView, method token.SessionAuthMethod) error {
	if err := h.validateSessionMethod(view, method); err != nil {
		return err
	}
	if method.Provider != proto_models.AuthProvider_AUTH_PROVIDER_OAUTH {
		return nil
	}
	providers, _, err := h.sampleOAuthProviders(ctx)
	if err != nil {
		return err
	}
	for _, provider := range providers {
		if provider.Slug == method.OAuthProviderSlug {
			return nil
		}
	}
	return errors.ErrInvalidCredentials
}
