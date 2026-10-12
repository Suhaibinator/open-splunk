package token

const (
	tokenServiceLoggerName     = "common_service.token_service"
	mockTokenServiceLoggerName = "common_service.mock_token_service"
)

// Token format versions
const (
	// TokenVersionSession is the version for session tokens
	TokenVersionSession = 4
)

// Token sizes (in bytes before encoding)
const (
	// SessionTokenSize is the total size of a session token
	// Version(1) + ExpiresAt(8) + UserID(8) + AuthMethod(1) +
	// AuthenticatedAt(8) + Random(16) + IssuedAt(8) + HMAC(16) = 66 bytes
	SessionTokenSize = 66

	// SessionTokenRandomBytes is the number of random bytes in session tokens
	SessionTokenRandomBytes = 16

	// SessionTokenHMACBytes is the number of HMAC bytes in session tokens
	SessionTokenHMACBytes = 16

	// Binary layout offsets for the current session-token format.
	sessionTokenAuthenticatedAtOffset = 18
	sessionTokenRandomOffset          = 26
	sessionTokenIssuedAtOffset        = 42
	sessionTokenHMACOffset            = SessionTokenSize - SessionTokenHMACBytes

	// SecurityTokenSize is the size of security tokens (pure random)
	SecurityTokenSize = 64
)

// Token encoding
const (
	// SessionTokenExpectedLength is the expected base64 length for session tokens
	// 66 bytes = 88 characters in base64 (no padding with RawURLEncoding)
	SessionTokenExpectedLength = 88
)
