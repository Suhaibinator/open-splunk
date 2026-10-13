// Package errors defines OAuth-specific error variables.
package errors

import (
	"errors"
)

// OAuth specific errors that may occur during OAuth authentication flows.
var (
	// ErrInvalidOAuthState indicates that OAuth state was not issued by this
	// server, is expired, was altered, or does not match the callback request.
	ErrInvalidOAuthState = errors.New("invalid OAuth state")

	// ErrOAuthFailed represents a failure during OAuth authentication with the provider.
	// This can occur when the OAuth code exchange fails or the provider returns an error.
	ErrOAuthFailed = errors.New("failed to authenticate with OAuth provider")

	// ErrOAuthEmailMissing indicates the OAuth provider returned no email for an
	// identity that is not linked yet; the first login links by verified email.
	ErrOAuthEmailMissing = errors.New("OAuth provider did not return email address")

	// ErrOAuthEmailNotVerified indicates the provider's email is not trusted
	// under its verification policy (or names another provider's synthetic
	// domain), so it cannot link an account.
	ErrOAuthEmailNotVerified = errors.New("OAuth provider reported email address as unverified")

	// ErrOAuthProviderUnavailable indicates that the requested provider slug is
	// not among the currently configured, valid providers.
	ErrOAuthProviderUnavailable = errors.New("OAuth provider unavailable")

	// ErrEmailDomainReserved indicates an account email on a configured OAuth
	// provider's synthetic email domain. Such an address names a provider
	// account, not a mailbox, so only that provider's login may claim it.
	ErrEmailDomainReserved = errors.New("email domain is reserved for OAuth sign-in")

	// ErrOAuthServiceUnavailable indicates the OAuth provider source or runtime
	// failed, so no provider can be served right now; it is not the caller's
	// fault.
	ErrOAuthServiceUnavailable = errors.New("OAuth sign-in temporarily unavailable")

	// ErrOAuthSubjectMissing indicates the provider identity carried no usable
	// stable subject, so it cannot be linked to an account.
	ErrOAuthSubjectMissing = errors.New("OAuth provider did not return a subject")

	// ErrOAuthIdentityConflict indicates the account already has a different
	// identity linked for this provider.
	ErrOAuthIdentityConflict = errors.New("OAuth identity conflicts with the linked account")
)
