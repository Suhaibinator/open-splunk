package token

import (
	"errors"
	"fmt"
	"time"
)

// Token validation errors
var (
	// ErrTokenInvalid indicates the token format is invalid or corrupted
	ErrTokenInvalid = errors.New("invalid token format")

	// ErrTokenExpired indicates the token has passed its expiration time
	// Storage rejection may not carry an expiry timestamp; stateless validation returns ExpiredError.
	ErrTokenExpired = errors.New("token has expired")

	// ErrTokenRevoked indicates the token has been manually revoked
	ErrTokenRevoked = errors.New("token has been revoked")

	// ErrTokenUsed indicates a single-use token has already been consumed
	ErrTokenUsed = errors.New("token has already been used")

	// ErrTokenNotFound indicates the token doesn't exist in the database
	ErrTokenNotFound = errors.New("token not found")

	// ErrTokenTypeMismatch indicates the token type doesn't match expected
	ErrTokenTypeMismatch = errors.New("token type mismatch")

	// ErrTokenGenerationFailed indicates token generation failed
	ErrTokenGenerationFailed = errors.New("failed to generate token")

	// ErrInvalidTokenVersion indicates an unsupported token version
	ErrInvalidTokenVersion = errors.New("unsupported token version")

	// ErrInvalidHMAC indicates the token signature is invalid
	ErrInvalidHMAC = errors.New("invalid token signature")

	// ErrTokenStorageFailed indicates token could not be stored in database
	ErrTokenStorageFailed = errors.New("failed to store token in database")

	// ErrTokenAlreadyUsed indicates a single-use token has already been consumed
	// (alias for ErrTokenUsed for consistency)
	ErrTokenAlreadyUsed = ErrTokenUsed
)

// ExpiredError carries state used by the token backend flow.
type ExpiredError struct {
	ExpiresAt time.Time
}

// Error executes this step in the token backend flow.
func (e ExpiredError) Error() string {
	return fmt.Sprintf("token expired at %s", e.ExpiresAt.Format(time.RFC3339))
}

// NewExpiredError executes this step in the token backend flow.
func NewExpiredError(expiresAt time.Time) error {
	return ExpiredError{ExpiresAt: expiresAt}
}
