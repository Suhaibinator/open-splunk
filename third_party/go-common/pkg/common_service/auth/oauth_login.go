package auth

import (
	"context"
	stdErrors "errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/oauth"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// OAuthCallback completes an authorization attempt.
type OAuthCallback struct {
	Code  string
	State string
	// Binding is the browser binding cookie issued with State.
	Binding string
}

// OAuthLoginResult is the outcome of an OAuth callback. ProviderSlug is set
// as soon as the state verifies, also when the login then fails, so callers
// can label metrics with it.
type OAuthLoginResult[U common_service_models.UserLike] struct {
	User           *U
	SessionToken   string
	AccountCreated bool
	ReturnURL      string
	ProviderSlug   string
}

// OAuthProviderDescriptor is the public description of a usable provider.
type OAuthProviderDescriptor struct {
	Slug        string
	DisplayName string
	IconKey     string
	// FormPost reports that the provider answers with response_mode=form_post
	// and therefore redirects to the backend relay.
	FormPost bool
}

// WithOAuthProviders installs the application's provider source. It is sampled
// once at construction, where an error or an invalid provider fails the
// constructor, and then once per OAuth operation.
func WithOAuthProviders(source oauth.Source) HandlerOption {
	return func(options *handlerOptions) {
		options.oauthSource = source
	}
}

// sampleOAuthProviders samples the source once and returns the providers that
// are valid in that sample, plus the whole sample (whose synthetic email
// domains stay reserved even when their provider is excluded). Invalid
// providers are logged and left out; a source error fails closed.
func (h *AuthHandler[U]) sampleOAuthProviders(ctx context.Context) (valid, all []oauth.Provider, err error) {
	if h.oauthSource == nil {
		return nil, nil, nil
	}
	all, err = h.oauthSource()
	if err != nil {
		return nil, nil, appLogger.WithErrorLevel(stdErrors.Join(errors.ErrOAuthServiceUnavailable, err), zapcore.ErrorLevel,
			zap.String(logkeys.Stage, "sample_oauth_providers"))
	}
	valid, excluded := oauth.ValidProviders(all)
	h.logOAuthExclusions(ctx, excluded)
	return valid, all, nil
}

// CheckAccountEmailDomain refuses an account email on any sampled provider's
// synthetic email domain with ErrEmailDomainReserved. The whole sample counts,
// so a provider excluded as invalid keeps its domain reserved. A source
// failure fails closed with ErrOAuthServiceUnavailable.
func (h *AuthHandler[U]) CheckAccountEmailDomain(ctx context.Context, email string) error {
	_, all, err := h.sampleOAuthProviders(ctx)
	if err != nil {
		return err
	}
	normalized := common_persistence_models.NormalizeUserEmail(email)
	domain := normalized[strings.LastIndexByte(normalized, '@')+1:]
	for _, provider := range all {
		if provider.SyntheticEmailDomain != "" && strings.EqualFold(domain, provider.SyntheticEmailDomain) {
			return appLogger.WithErrorLevel(errors.ErrEmailDomainReserved, zapcore.WarnLevel,
				zap.String(appLogger.KeyEmail, email),
				zap.String(logkeys.Stage, "check_synthetic_email_domain"))
		}
	}
	return nil
}

// logOAuthExclusions warns about invalid providers only when the excluded set
// differs from the last one logged, so a persistent misconfiguration is not
// repeated on every operation. A clean sample resets it.
func (h *AuthHandler[U]) logOAuthExclusions(ctx context.Context, excluded []error) {
	messages := make([]string, len(excluded))
	for i, exclusion := range excluded {
		messages[i] = exclusion.Error()
	}
	slices.Sort(messages)
	signature := strings.Join(messages, "\n")
	h.oauthExclusionsMu.Lock()
	changed := signature != h.oauthExclusionsLogged
	h.oauthExclusionsLogged = signature
	h.oauthExclusionsMu.Unlock()
	if !changed || len(excluded) == 0 {
		return
	}
	logger := appLogger.For(ctx, h.logger, authLoggerName)
	for _, exclusion := range excluded {
		logger.Warn("Excluding invalid OAuth provider",
			zap.Error(exclusion),
			zap.String(logkeys.Stage, "sample_oauth_providers"),
			zap.String(logkeys.Fallback, "provider unavailable until the source is fixed"))
	}
}

// oauthProvider samples the source once and returns the valid provider for
// slug, plus the whole sample.
func (h *AuthHandler[U]) oauthProvider(ctx context.Context, slug string) (oauth.Provider, []oauth.Provider, error) {
	valid, all, err := h.sampleOAuthProviders(ctx)
	if err != nil {
		return oauth.Provider{}, nil, err
	}
	for _, provider := range valid {
		if provider.Slug == slug {
			return provider, all, nil
		}
	}
	return oauth.Provider{}, nil, appLogger.WithErrorLevel(errors.ErrOAuthProviderUnavailable, zapcore.WarnLevel)
}

// ListOAuthProviders describes the providers valid in one source sample. It
// performs no discovery. A source failure is logged and lists nothing.
func (h *AuthHandler[U]) ListOAuthProviders(ctx context.Context) []OAuthProviderDescriptor {
	valid, _, err := h.sampleOAuthProviders(ctx)
	if err != nil {
		appLogger.For(ctx, h.logger, authLoggerName).Error("OAuth provider source unavailable",
			zap.Error(err),
			zap.String(logkeys.Stage, "list_oauth_providers"),
			zap.String(logkeys.Fallback, "list no providers"))
		return []OAuthProviderDescriptor{}
	}
	descriptors := make([]OAuthProviderDescriptor, 0, len(valid))
	for _, provider := range valid {
		descriptors = append(descriptors, OAuthProviderDescriptor{
			Slug:        provider.Slug,
			DisplayName: provider.DisplayName,
			IconKey:     provider.IconKey,
			FormPost:    provider.ResponseMode == oauth.ResponseModeFormPost,
		})
	}
	return descriptors
}

// oauthSyntheticSubjectPunctuation is the punctuation allowed, besides ASCII
// letters and digits, in a subject used as a synthetic email local part.
const oauthSyntheticSubjectPunctuation = "._-+"

// syntheticSubjectOK reports whether subject can form "<subject>@<domain>".
func syntheticSubjectOK(subject string) bool {
	for _, r := range subject {
		if !(r < unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune(oauthSyntheticSubjectPunctuation, r))) {
			return false
		}
	}
	return subject != ""
}

// mapOAuthRuntimeError maps pkg/oauth failures onto service errors.
func mapOAuthRuntimeError(err error) error {
	switch {
	case stdErrors.Is(err, context.Canceled), stdErrors.Is(err, context.DeadlineExceeded):
		return err
	case stdErrors.Is(err, oauth.ErrSubjectMissing):
		return appLogger.WithErrorLevel(stdErrors.Join(errors.ErrOAuthSubjectMissing, err), zapcore.WarnLevel)
	case stdErrors.Is(err, oauth.ErrRedirectURINotAllowed), stdErrors.Is(err, oauth.ErrInvalidRequest):
		return appLogger.WithErrorLevel(stdErrors.Join(errors.ErrInvalidOAuthState, err), zapcore.WarnLevel)
	case stdErrors.Is(err, oauth.ErrRuntimeClosed):
		return appLogger.WithErrorLevel(stdErrors.Join(errors.ErrOAuthServiceUnavailable, err), zapcore.ErrorLevel)
	case stdErrors.Is(err, oauth.ErrInvalidProvider):
		return appLogger.WithErrorLevel(stdErrors.Join(errors.ErrOAuthProviderUnavailable, err), zapcore.ErrorLevel)
	case stdErrors.Is(err, oauth.ErrIDTokenInvalid), stdErrors.Is(err, oauth.ErrNonceMismatch),
		stdErrors.Is(err, oauth.ErrSubjectMismatch), stdErrors.Is(err, oauth.ErrExchange):
		// The browser presented something the provider or its keys reject:
		// a forged, replayed, or expired code or token.
		return appLogger.WithErrorLevel(stdErrors.Join(errors.ErrOAuthFailed, err), zapcore.WarnLevel)
	default:
		// Discovery, client secret, identity resolution, and provider API
		// failures are operational problems.
		return appLogger.WithErrorLevel(stdErrors.Join(errors.ErrOAuthFailed, err), zapcore.ErrorLevel)
	}
}

// LoginUserByOAuth completes an authorization attempt inside the caller's
// transaction. It verifies state and browser binding, exchanges the code with
// one provider sample, resolves the account by (provider, subject) first and
// by verified email otherwise, and mints an AUTH_PROVIDER_OAUTH session.
func (h *AuthHandler[U]) LoginUserByOAuth(ctx context.Context, callback OAuthCallback) (OAuthLoginResult[U], error) {
	var result OAuthLoginResult[U]
	if err := h.requireTransaction(ctx, "OAuth login"); err != nil {
		return result, err
	}
	view := h.runtimeConfigProvider()
	now := time.Now()
	state, err := h.openOAuthState(view, callback.State, now)
	if err != nil {
		return result, appLogger.WithErrorLevel(err, zapcore.WarnLevel, zap.String(logkeys.Stage, "validate_oauth_state"))
	}
	result.ProviderSlug = state.Slug
	fields := []zap.Field{zap.String(appLogger.KeyProvider, state.Slug)}
	binding, err := h.openOAuthBinding(view, callback.Binding, callback.State, now)
	if err != nil {
		return result, appLogger.WithErrorLevel(err, zapcore.WarnLevel, append(fields, zap.String(logkeys.Stage, "validate_oauth_binding"))...)
	}
	// The allowlist may have shrunk since the state was issued.
	allowlist := h.currentFrontendAllowlist()
	if allowlist == nil {
		return result, appLogger.WithErrorFields(fmt.Errorf("%w: active frontend origin configuration unavailable", errors.ErrInvalidOAuthState), fields...)
	}
	if err := allowlist.ValidateReturnURL(state.ReturnURL); err != nil {
		return result, appLogger.WithErrorLevel(appLogger.WithErrorCause(fmt.Errorf("%w: OAuth state returnUrl not allowed", errors.ErrInvalidOAuthState), err), zapcore.WarnLevel,
			append(fields, zap.String(appLogger.KeyReturnURL, state.ReturnURL), zap.String(logkeys.Stage, "validate_oauth_return_url"))...)
	}

	provider, sample, err := h.oauthProvider(ctx, state.Slug)
	if err != nil {
		return result, appLogger.WithErrorFields(err, append(fields, zap.String(logkeys.Stage, "resolve_oauth_provider"))...)
	}
	identity, err := h.oauthRuntime.Exchange(ctx, oauthSettings(view), provider, oauth.ExchangeRequest{
		Code:         callback.Code,
		RedirectURI:  state.RedirectURI,
		CodeVerifier: binding.CodeVerifier,
		Nonce:        binding.Nonce,
	})
	if err != nil {
		return result, appLogger.WithErrorFields(mapOAuthRuntimeError(err), append(fields, zap.String(logkeys.Stage, "exchange_oauth_code"))...)
	}
	if len(identity.Subject) > common_persistence_models.OAuthSubjectMaxLength {
		return result, appLogger.WithErrorLevel(fmt.Errorf("%w: subject longer than %d characters", errors.ErrOAuthSubjectMissing, common_persistence_models.OAuthSubjectMaxLength), zapcore.WarnLevel,
			append(fields, zap.String(logkeys.Stage, "check_oauth_subject"))...)
	}
	if ce := appLogger.For(ctx, h.logger, authLoggerName).Check(zap.DebugLevel, "Resolving OAuth identity"); ce != nil {
		ce.Write(append(fields, zap.String(appLogger.KeyProviderUserID, identity.Subject))...)
	}

	appUser, created, err := h.resolveOAuthAccount(ctx, view, provider, sample, identity)
	if err != nil {
		return result, appLogger.WithErrorFields(err, fields...)
	}
	sessionToken, err := h.CreateUserAuth(ctx,
		token.SessionAuthMethod{Provider: proto_models.AuthProvider_AUTH_PROVIDER_OAUTH, OAuthProviderSlug: provider.Slug},
		proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION, "", appUser.GetID())
	if err != nil {
		return result, appLogger.WithErrorFields(stdErrors.Join(errors.ErrLoginFailed, err),
			append(fields,
				zap.Uint64(appLogger.KeyUserID, uint64(appUser.GetID())),
				zap.Stringer(appLogger.KeyTokenType, proto_models.TokenType_TOKEN_TYPE_AUTHENTICATION),
				zap.String(logkeys.Stage, "mint_oauth_session"))...)
	}
	if ce := appLogger.For(ctx, h.logger, authLoggerName).Check(zap.DebugLevel, "OAuth login successful"); ce != nil {
		ce.Write(append(fields, zap.Uint64(appLogger.KeyUserID, uint64(appUser.GetID())), zap.Bool("account_created", created))...)
	}
	result.User = &appUser
	result.SessionToken = sessionToken
	result.AccountCreated = created
	result.ReturnURL = state.ReturnURL
	return result, nil
}

// resolveOAuthAccount finds or creates the account for identity. Ban and bot
// checks run on the locked user row before any write or callback.
func (h *AuthHandler[U]) resolveOAuthAccount(
	ctx context.Context,
	view RuntimeConfigView,
	provider oauth.Provider,
	sample []oauth.Provider,
	identity oauth.Identity,
) (U, bool, error) {
	var zero U
	linked, err := h.persistenceHandler.FindOAuthIdentity(ctx, provider.Slug, identity.Subject)
	switch {
	case err == nil:
		appUser, err := h.lockOAuthUser(ctx, linked.UserID, "")
		if err != nil {
			return zero, false, err
		}
		email := identity.Email
		if email == "" || len(email) > common_persistence_models.OAuthLoginEmailMaxLength {
			email = linked.Email
		}
		if err := h.persistenceHandler.TouchOAuthIdentity(ctx, linked.ID, identity.Subject, email); err != nil {
			return zero, false, oauthIdentityWriteError(err, linked.UserID, "touch_oauth_identity")
		}
		return appUser, false, nil
	case !stdErrors.Is(err, persistence.ErrOAuthIdentityNotFound):
		return zero, false, appLogger.WithErrorFields(err, zap.String(logkeys.Stage, "find_oauth_identity"))
	}

	email, err := oauthAccountEmail(provider, sample, identity)
	if err != nil {
		return zero, false, err
	}
	fields := []zap.Field{zap.String(appLogger.KeyEmail, email)}
	coreUser, err := h.persistenceHandler.GetUserByEmail(ctx, email)
	switch {
	case err == nil:
		appUser, err := h.lockOAuthUser(ctx, coreUser.ID, email)
		if err != nil {
			return zero, false, err
		}
		userID := uint64(appUser.GetID())
		existing, err := h.persistenceHandler.FindUserOAuthIdentity(ctx, userID, provider.Slug)
		newLink := false
		switch {
		case err == nil && existing.Subject != nil && *existing.Subject != identity.Subject:
			return zero, false, appLogger.WithErrorLevel(errors.ErrOAuthIdentityConflict, zapcore.WarnLevel,
				append(fields, zap.Uint64(appLogger.KeyUserID, userID), zap.String(logkeys.Stage, "link_oauth_identity"))...)
		case err == nil:
			// A legacy identity linked before subjects were recorded.
			err = h.persistenceHandler.TouchOAuthIdentity(ctx, existing.ID, identity.Subject, email)
		case stdErrors.Is(err, persistence.ErrOAuthIdentityNotFound):
			err = h.persistenceHandler.LinkOAuthIdentity(ctx, userID, provider.Slug, identity.Subject, email)
			newLink = true
		}
		if err != nil {
			return zero, false, oauthIdentityWriteError(err, userID, "link_oauth_identity")
		}
		if err := h.verifyOAuthEmail(ctx, userID, email); err != nil {
			return zero, false, err
		}
		if newLink {
			if err := h.notifyOAuthLinked(ctx, appUser.GetID(), provider.Slug); err != nil {
				return zero, false, err
			}
		}
		return appUser, false, nil
	case !stdErrors.Is(err, errors.ErrUserNotFound) && !stdErrors.Is(err, persistence.ErrUserNotFound):
		return zero, false, appLogger.WithErrorFields(err, append(fields, zap.String(logkeys.Stage, "lookup_oauth_user_by_email"))...)
	}

	appUser, err := h.createOAuthUser(ctx, view, email, identity)
	if err != nil {
		return zero, false, err
	}
	userID := uint64(appUser.GetID())
	if err := h.verifyOAuthEmail(ctx, userID, email); err != nil {
		return zero, false, err
	}
	if err := h.persistenceHandler.LinkOAuthIdentity(ctx, userID, provider.Slug, identity.Subject, email); err != nil {
		return zero, false, oauthIdentityWriteError(err, userID, "link_new_oauth_identity")
	}
	if err := h.notifyOAuthLinked(ctx, appUser.GetID(), provider.Slug); err != nil {
		return zero, false, err
	}
	return appUser, true, nil
}

func oauthIdentityWriteError(err error, userID uint64, stage string) error {
	fields := []zap.Field{zap.Uint64(appLogger.KeyUserID, userID), zap.String(logkeys.Stage, stage)}
	if stdErrors.Is(err, persistence.ErrOAuthIdentityConflict) {
		return appLogger.WithErrorLevel(stdErrors.Join(errors.ErrOAuthIdentityConflict, err), zapcore.WarnLevel, fields...)
	}
	return appLogger.WithErrorFields(stdErrors.Join(errors.ErrOAuthFailed, err), fields...)
}

// oauthAccountEmail picks the address that links or creates the account.
// A trusted provider email is used as-is. Addresses on any sampled provider's
// synthetic domain name a provider account, not a mailbox, so only the
// provider's own "<subject>@<domain>" is accepted. Without a trusted email, a
// provider with a synthetic domain falls back to that address.
func oauthAccountEmail(provider oauth.Provider, sample []oauth.Provider, identity oauth.Identity) (string, error) {
	fields := func(reason string) []zap.Field {
		return []zap.Field{
			zap.String(appLogger.KeyEmail, identity.Email),
			zap.String(logkeys.Reason, reason),
			zap.String(logkeys.Stage, "resolve_oauth_email"),
		}
	}
	email := common_persistence_models.NormalizeUserEmail(identity.Email)
	var synthetic string
	if provider.SyntheticEmailDomain != "" && syntheticSubjectOK(identity.Subject) {
		synthetic = common_persistence_models.NormalizeUserEmail(identity.Subject + "@" + provider.SyntheticEmailDomain)
	}
	if email != "" {
		_, domain, _ := strings.Cut(email, "@")
		for _, other := range sample {
			if other.SyntheticEmailDomain != "" && strings.EqualFold(domain, other.SyntheticEmailDomain) {
				if email == synthetic {
					return synthetic, nil
				}
				return "", appLogger.WithErrorLevel(errors.ErrOAuthEmailNotVerified, zapcore.WarnLevel, fields("synthetic_identifier_from_other_source")...)
			}
		}
		if provider.EmailTrusted(identity) {
			return email, nil
		}
	}
	if synthetic != "" {
		return synthetic, nil
	}
	switch {
	case email == "":
		return "", appLogger.WithErrorLevel(errors.ErrOAuthEmailMissing, zapcore.WarnLevel, fields("email_missing")...)
	case identity.EmailVerified != nil:
		return "", appLogger.WithErrorLevel(errors.ErrOAuthEmailNotVerified, zapcore.WarnLevel, fields("provider_reported_unverified")...)
	default:
		return "", appLogger.WithErrorLevel(errors.ErrOAuthEmailNotVerified, zapcore.WarnLevel, fields("verification_status_unknown")...)
	}
}

// lockOAuthUser locks the core user row and loads the application user,
// rejecting bots and banned accounts before the caller writes anything. A
// non-empty email must still be the locked account's address.
func (h *AuthHandler[U]) lockOAuthUser(ctx context.Context, userID uint64, email string) (U, error) {
	var zero U
	fields := []zap.Field{zap.Uint64(appLogger.KeyUserID, userID), zap.String(logkeys.Stage, "authorize_oauth_login")}
	locked, err := h.persistenceHandler.GetUserForUpdate(ctx, userID)
	if err != nil {
		return zero, appLogger.WithErrorFields(err, fields...)
	}
	if locked == nil || (email != "" && common_persistence_models.NormalizeUserEmail(locked.Email) != email) {
		return zero, appLogger.WithErrorLevel(errors.ErrInvalidCredentials, zapcore.WarnLevel, fields...)
	}
	if locked.IsBot {
		return zero, appLogger.WithErrorLevel(errors.ErrInvalidCredentials, zapcore.WarnLevel, fields...)
	}
	if locked.Banned {
		return zero, appLogger.WithErrorLevel(errors.ErrUserBanned, zapcore.WarnLevel, fields...)
	}
	appUser, err := h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(userID))
	if err != nil {
		return zero, appLogger.InvariantError("oauth_core_user_has_application_user", stdErrors.Join(errors.ErrUserNotFound, err),
			append(fields,
				zap.String(logkeys.Expected, "application user exists for core OAuth user"),
				zap.String(logkeys.Actual, "application user lookup failed"))...)
	}
	if appUser.IsBot() {
		return zero, appLogger.WithErrorLevel(errors.ErrInvalidCredentials, zapcore.WarnLevel, fields...)
	}
	if appUser.IsBanned() {
		return zero, appLogger.WithErrorLevel(errors.ErrUserBanned, zapcore.WarnLevel, fields...)
	}
	if uint64(appUser.GetID()) != userID {
		return zero, appLogger.InvariantError("oauth_application_user_matches_core_user", errors.ErrInternalError,
			append(fields,
				zap.String(logkeys.Expected, "application user ID equals core user ID"),
				zap.Uint64(logkeys.Actual, uint64(appUser.GetID())))...)
	}
	return appUser, nil
}

// verifyOAuthEmail records the provider's proof of the account address. A
// never-verified self-signup may hold a password chosen by someone who never
// proved they own this address, so those credentials are dropped first rather
// than activated.
func (h *AuthHandler[U]) verifyOAuthEmail(ctx context.Context, userID uint64, email string) error {
	fields := []zap.Field{zap.Uint64(appLogger.KeyUserID, userID), zap.String(appLogger.KeyEmail, email)}
	discarded, err := h.persistenceHandler.DiscardUnverifiedSignupCredentials(ctx, userID)
	if err != nil {
		return appLogger.WithErrorLevel(stdErrors.Join(errors.ErrOAuthFailed, err), zapcore.ErrorLevel,
			append(fields, zap.String(logkeys.Stage, "discard_unverified_signup_credentials"))...)
	}
	if discarded.Discarded {
		appLogger.For(ctx, h.logger, authLoggerName).Info("Discarded unverified signup credentials before OAuth email verification",
			append(fields,
				zap.Bool("password_removed", discarded.PasswordRemoved),
				zap.Int(appLogger.KeyCount, len(discarded.RevokedAuthTokens)))...)
		h.invalidateAuthTokensAfterCommit(ctx, discarded.RevokedAuthTokens)
	}
	if _, err := h.persistenceHandler.MarkEmailVerified(ctx, userID); err != nil {
		return appLogger.WithErrorLevel(stdErrors.Join(errors.ErrOAuthFailed, err), zapcore.ErrorLevel,
			append(fields, zap.String(logkeys.Stage, "persist_oauth_verified_email"))...)
	}
	if h.userCallbacks != nil && h.userCallbacks.SyncCallbacks != nil && h.userCallbacks.SyncCallbacks.OnEmailVerified != nil {
		if err := h.userCallbacks.SyncCallbacks.OnEmailVerified(ctx, common_service_models.UserIdType(userID), true); err != nil {
			return appLogger.WithErrorFields(err, append(fields, zap.String(logkeys.Stage, "email_verified_callback"))...)
		}
	}
	return nil
}

func (h *AuthHandler[U]) notifyOAuthLinked(ctx context.Context, userID common_service_models.UserIdType, slug string) error {
	if h.userCallbacks == nil || h.userCallbacks.SyncCallbacks == nil || h.userCallbacks.SyncCallbacks.OnOAuthLinked == nil {
		return nil
	}
	if err := h.userCallbacks.SyncCallbacks.OnOAuthLinked(ctx, userID, slug); err != nil {
		return appLogger.WithErrorFields(err,
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyProvider, slug),
			zap.String(logkeys.Stage, "oauth_linked_callback"))
	}
	return nil
}

// createOAuthUser creates the core and application user for a first OAuth
// sign-in under the current signup policy.
func (h *AuthHandler[U]) createOAuthUser(ctx context.Context, view RuntimeConfigView, email string, identity oauth.Identity) (U, error) {
	var zero U
	fields := []zap.Field{zap.String(appLogger.KeyEmail, email)}
	username, err := h.resolveOAuthSignupUsernameWithConfig(ctx, email, view)
	if err != nil {
		return zero, err
	}
	if h.accountLifecyclePreflight != nil {
		if err := h.accountLifecyclePreflight(ctx); err != nil {
			return zero, err
		}
	}
	created := &common_persistence_models.User{
		Username:  username,
		Email:     email,
		FirstName: identity.GivenName,
		LastName:  identity.FamilyName,
		UserType:  view.DefaultUserType(),
	}
	if err := h.persistenceHandler.CreateUser(ctx, created); err != nil {
		return zero, appLogger.WithErrorFields(err,
			append(fields, zap.String(appLogger.KeyUsername, username), zap.String(logkeys.Stage, "create_oauth_core_user"))...)
	}
	fields = append(fields, zap.Uint64(appLogger.KeyUserID, created.ID))
	var appUser U
	if h.userCallbacks.SyncCallbacks != nil && h.userCallbacks.SyncCallbacks.OnUserCreated != nil {
		appUser, err = h.userCallbacks.UserCreated(ctx, &common_service_models.User{
			ID:        created.ID,
			Username:  created.Username,
			Email:     created.Email,
			FirstName: created.FirstName,
			LastName:  created.LastName,
			UserType:  created.UserType,
			Banned:    created.Banned,
		})
		if err != nil {
			return zero, appLogger.WithErrorFields(err, append(fields, zap.String(logkeys.Stage, "create_oauth_application_user"))...)
		}
	} else {
		appUser, err = h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(created.ID))
		if err != nil {
			return zero, appLogger.WithErrorFields(err, append(fields, zap.String(logkeys.Stage, "lookup_new_oauth_application_user"))...)
		}
	}
	if appUser.IsBot() {
		return zero, errors.ErrInvalidCredentials
	}
	if uint64(appUser.GetID()) != created.ID {
		return zero, appLogger.InvariantError("oauth_application_user_matches_core_user", errors.ErrInternalError,
			append(fields,
				zap.String(logkeys.Stage, "validate_new_application_user"),
				zap.String(logkeys.Expected, "application user ID equals created core user ID"),
				zap.Uint64(logkeys.Actual, uint64(appUser.GetID())))...)
	}
	return appUser, nil
}
