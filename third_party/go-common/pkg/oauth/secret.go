package oauth

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/golang-jwt/jwt/v5"
)

// AppleAudience is the audience of Apple's client-secret assertion.
const AppleAudience = "https://appleid.apple.com"

// StaticSecret returns a ClientSecretSource that always yields secret.
func StaticSecret(secret config.Secret) ClientSecretSource {
	return func(context.Context, SecretInput) (string, error) {
		if secret.IsZero() {
			return "", fmt.Errorf("%w: secret is empty", ErrClientSecret)
		}
		return secret.StringValue(), nil
	}
}

// AppleClientSecret returns a ClientSecretSource that mints Apple's ES256
// client-secret JWT for every exchange. pemKey holds the PKCS#8 (or SEC 1)
// P-256 private key downloaded from Apple; the token lives for
// SecretInput.SignedSecretLifetime.
func AppleClientSecret(teamID, keyID string, pemKey config.Secret) ClientSecretSource {
	return func(_ context.Context, in SecretInput) (string, error) {
		if teamID == "" || keyID == "" {
			return "", fmt.Errorf("%w: Apple team ID and key ID are required", ErrClientSecret)
		}
		if in.SignedSecretLifetime <= 0 {
			return "", fmt.Errorf("%w: signed secret lifetime must be positive", ErrClientSecret)
		}
		key, err := jwt.ParseECPrivateKeyFromPEM(pemKey.Value())
		if err != nil {
			return "", fmt.Errorf("%w: parse Apple private key: %w", ErrClientSecret, err)
		}
		if key.Curve.Params().Name != "P-256" {
			return "", fmt.Errorf("%w: Apple private key must use P-256", ErrClientSecret)
		}
		token := jwt.NewWithClaims(jwt.SigningMethodES256, appleSecretClaims{
			Issuer:    teamID,
			Subject:   in.ClientID,
			Audience:  AppleAudience,
			IssuedAt:  in.Now.Unix(),
			ExpiresAt: in.Now.Add(in.SignedSecretLifetime).Unix(),
		})
		token.Header["kid"] = keyID
		signed, err := token.SignedString(key)
		if err != nil {
			return "", fmt.Errorf("%w: sign Apple client secret: %w", ErrClientSecret, err)
		}
		return signed, nil
	}
}

// appleSecretClaims serializes aud as a single string, as Apple documents.
type appleSecretClaims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	Audience  string `json:"aud"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

func (c appleSecretClaims) GetExpirationTime() (*jwt.NumericDate, error) {
	return jwt.NewNumericDate(time.Unix(c.ExpiresAt, 0)), nil
}
func (c appleSecretClaims) GetIssuedAt() (*jwt.NumericDate, error) {
	return jwt.NewNumericDate(time.Unix(c.IssuedAt, 0)), nil
}
func (appleSecretClaims) GetNotBefore() (*jwt.NumericDate, error) { return nil, nil }
func (c appleSecretClaims) GetIssuer() (string, error)            { return c.Issuer, nil }
func (c appleSecretClaims) GetSubject() (string, error)           { return c.Subject, nil }
func (c appleSecretClaims) GetAudience() (jwt.ClaimStrings, error) {
	return jwt.ClaimStrings{c.Audience}, nil
}
