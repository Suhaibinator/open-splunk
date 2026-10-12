package oauth

import (
	"github.com/Suhaibinator/go-common/pkg/config"
)

// Preset slugs. The legacy AuthProvider enum values map to these slugs, so an
// application that keeps existing linked identities must keep them.
const (
	SlugGoogle    = "google"
	SlugFacebook  = "facebook"
	SlugGitHub    = "github"
	SlugDiscord   = "discord"
	SlugApple     = "apple"
	SlugLinkedIn  = "linkedin"
	SlugOkta      = "okta"
	SlugMicrosoft = "microsoft"
)

// FacebookGraphVersion is the Graph API version the Facebook adapter pins.
const FacebookGraphVersion = "v26.0"

// Google returns an OIDC provider for Google accounts.
func Google(clientID string, clientSecret config.Secret, redirectURLs ...string) Provider {
	return Provider{
		Slug:         SlugGoogle,
		DisplayName:  "Google",
		IconKey:      SlugGoogle,
		ClientID:     clientID,
		ClientSecret: StaticSecret(clientSecret),
		Issuer:       "https://accounts.google.com",
		Scopes:       []string{"openid", "email", "profile"},
		RedirectURLs: redirectURLs,
		Identity:     OIDCIdentity(StandardClaims()),
	}
}

// GitHub returns an OAuth 2.0 provider for GitHub accounts. The identity is
// the numeric account ID; the email is the primary verified address.
func GitHub(clientID string, clientSecret config.Secret, redirectURLs ...string) Provider {
	return Provider{
		Slug:         SlugGitHub,
		DisplayName:  "GitHub",
		IconKey:      SlugGitHub,
		ClientID:     clientID,
		ClientSecret: StaticSecret(clientSecret),
		Endpoints: Endpoints{
			Authorization: "https://github.com/login/oauth/authorize",
			Token:         "https://github.com/login/oauth/access_token",
		},
		AuthStyle:    AuthStyleInParams,
		Scopes:       []string{"read:user", "user:email"},
		RedirectURLs: redirectURLs,
		Identity:     GitHubIdentity(),
	}
}

// Microsoft returns an OIDC provider for one Microsoft Entra tenant. tenantID
// must be the directory (tenant) GUID: the v2.0 discovery issuer names the
// GUID, so a domain name fails the issuer check. Multi-tenant ("common",
// "organizations") endpoints are unsupported for the same reason.
//
// Entra does not emit email_verified; the app registration must add the
// optional ID-token claims "email" and "xms_edov" (email domain owner
// verified), which this adapter reads as the verification claim.
func Microsoft(tenantID, clientID string, clientSecret config.Secret, redirectURLs ...string) Provider {
	mapping := StandardClaims()
	mapping.EmailVerified = "xms_edov"
	return Provider{
		Slug:         SlugMicrosoft,
		DisplayName:  "Microsoft",
		IconKey:      SlugMicrosoft,
		ClientID:     clientID,
		ClientSecret: StaticSecret(clientSecret),
		Issuer:       "https://login.microsoftonline.com/" + tenantID + "/v2.0",
		AuthStyle:    AuthStyleInParams,
		Scopes:       []string{"openid", "email", "profile"},
		RedirectURLs: redirectURLs,
		Identity:     OIDCIdentity(mapping),
	}
}

// Apple returns an OIDC provider for Sign in with Apple. clientID is the
// Services ID; the client secret is an ES256 assertion signed per exchange
// with the .p8 key identified by keyID. Apple POSTs the authorization
// response (form_post) and may encode email_verified as the string "true".
// The ID token carries no names.
func Apple(clientID, teamID, keyID string, privateKeyPEM config.Secret, redirectURLs ...string) Provider {
	return Provider{
		Slug:         SlugApple,
		DisplayName:  "Apple",
		IconKey:      SlugApple,
		ClientID:     clientID,
		ClientSecret: AppleClientSecret(teamID, keyID, privateKeyPEM),
		Issuer:       "https://appleid.apple.com",
		AuthStyle:    AuthStyleInParams,
		Scopes:       []string{"name", "email"},
		ResponseMode: ResponseModeFormPost,
		RedirectURLs: redirectURLs,
		Identity: OIDCIdentity(ClaimMapping{
			Subject:       "sub",
			Email:         "email",
			EmailVerified: "email_verified",
		}),
	}
}

// Okta returns an OIDC provider for an Okta org authorization server; domain
// is the org host, such as "example.okta.com".
func Okta(domain, clientID string, clientSecret config.Secret, redirectURLs ...string) Provider {
	return Provider{
		Slug:         SlugOkta,
		DisplayName:  "Okta",
		IconKey:      SlugOkta,
		ClientID:     clientID,
		ClientSecret: StaticSecret(clientSecret),
		Issuer:       "https://" + domain,
		Scopes:       []string{"openid", "email", "profile"},
		RedirectURLs: redirectURLs,
		Identity:     OIDCIdentity(StandardClaims()),
	}
}

// Discord returns an OAuth 2.0 provider for Discord accounts. The subject is
// the snowflake user ID and "verified" is the email verification claim.
// Applications that previously used Discord IDs as emails set
// SyntheticEmailDomain to "discordid.com".
func Discord(clientID string, clientSecret config.Secret, redirectURLs ...string) Provider {
	return Provider{
		Slug:         SlugDiscord,
		DisplayName:  "Discord",
		IconKey:      SlugDiscord,
		ClientID:     clientID,
		ClientSecret: StaticSecret(clientSecret),
		Endpoints: Endpoints{
			Authorization: "https://discord.com/oauth2/authorize",
			Token:         "https://discord.com/api/oauth2/token",
		},
		AuthStyle:    AuthStyleInParams,
		Scopes:       []string{"identify", "email"},
		RedirectURLs: redirectURLs,
		Identity: UserInfoIdentity("https://discord.com/api/users/@me", ClaimMapping{
			Subject:       "id",
			Email:         "email",
			EmailVerified: "verified",
			GivenName:     "global_name",
			Username:      "username",
		}),
	}
}

// LinkedIn returns an OIDC provider for "Sign In with LinkedIn using OpenID
// Connect".
func LinkedIn(clientID string, clientSecret config.Secret, redirectURLs ...string) Provider {
	return Provider{
		Slug:         SlugLinkedIn,
		DisplayName:  "LinkedIn",
		IconKey:      SlugLinkedIn,
		ClientID:     clientID,
		ClientSecret: StaticSecret(clientSecret),
		Issuer:       "https://www.linkedin.com/oauth",
		AuthStyle:    AuthStyleInParams,
		Scopes:       []string{"openid", "profile", "email"},
		RedirectURLs: redirectURLs,
		Identity:     OIDCIdentity(StandardClaims()),
	}
}

// Facebook returns an OAuth 2.0 provider for Facebook Login. The Graph API
// returns only confirmed emails and no verification claim, so an email
// without one is trusted. Apps with "Require App Secret" enabled need an
// appsecret_proof and must wrap UserInfoIdentity in their own resolver.
func Facebook(clientID string, clientSecret config.Secret, redirectURLs ...string) Provider {
	return Provider{
		Slug:         SlugFacebook,
		DisplayName:  "Facebook",
		IconKey:      SlugFacebook,
		ClientID:     clientID,
		ClientSecret: StaticSecret(clientSecret),
		Endpoints: Endpoints{
			Authorization: "https://www.facebook.com/" + FacebookGraphVersion + "/dialog/oauth",
			Token:         "https://graph.facebook.com/" + FacebookGraphVersion + "/oauth/access_token",
		},
		AuthStyle:    AuthStyleInParams,
		Scopes:       []string{"email", "public_profile"},
		RedirectURLs: redirectURLs,
		Identity: UserInfoIdentity("https://graph.facebook.com/"+FacebookGraphVersion+"/me?fields=id,first_name,last_name,email", ClaimMapping{
			Subject:    "id",
			Email:      "email",
			GivenName:  "first_name",
			FamilyName: "last_name",
		}),
		EmailVerification: EmailVerificationTrustWhenAbsent,
	}
}
