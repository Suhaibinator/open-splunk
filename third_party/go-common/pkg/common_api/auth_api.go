package common_api

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	oauthStateCookieSuffix = "_oauth_state"
)

const (
	// internalHeaderPrefix marks request headers that are reserved for
	// handler-to-middleware signaling within go-common. These headers must
	// never be honored when supplied by a client, so all inbound headers
	// carrying this prefix are stripped at ingress (see stripInternalHeaders).
	internalHeaderPrefix = "X-Go-Common-Internal-"

	sessionCookieIssueRequestHeader    = internalHeaderPrefix + "Issue-Session-Cookie"
	sessionCookieClearRequestHeader    = internalHeaderPrefix + "Clear-Session-Cookie"
	oauthStateCookieIssueRequestHeader = internalHeaderPrefix + "Issue-OAuth-State-Cookie"
	oauthStateCookieClearRequestHeader = internalHeaderPrefix + "Clear-OAuth-State-Cookie"
)

// stripInternalHeaders removes any inbound request header whose name carries the
// reserved internalHeaderPrefix. These headers are an internal signaling channel
// between handlers and the session-cookie middleware; honoring client-supplied
// values would expose a session-fixation / forced-logout surface. Handlers set
// legitimate values via markIssueSessionCookie/markClearSessionCookie after this
// strip runs, so the real flow is unaffected.
func stripInternalHeaders(r *http.Request) {
	for name := range r.Header {
		if strings.HasPrefix(http.CanonicalHeaderKey(name), internalHeaderPrefix) {
			r.Header.Del(name)
		}
	}
}

func extractAuthTokenFromRequest(r *http.Request, cookieName string) (string, error) {
	cookie, err := r.Cookie(cookieName)
	if err != nil {
		return "", router.NewHTTPError(http.StatusUnauthorized, "No authorization token provided")
	}

	token := strings.TrimSpace(cookie.Value)
	if token == "" {
		return "", router.NewHTTPError(http.StatusUnauthorized, "No authorization token provided")
	}

	return token, nil
}

type recentSessionValidator interface {
	ValidateSessionToken(ctx context.Context, authToken string) (*token.SessionTokenClaims, error)
	RequireRecentAuthentication(ctx context.Context, claims *token.SessionTokenClaims, maxAge time.Duration) error
}

func validateRecentSessionFromRequest(
	ctx context.Context,
	r *http.Request,
	cookieName string,
	service recentSessionValidator,
) (*token.SessionTokenClaims, error) {
	authToken, err := extractAuthTokenFromRequest(r, cookieName)
	if err != nil {
		return nil, err
	}

	claims, err := service.ValidateSessionToken(ctx, authToken)
	if err != nil {
		switch {
		case errors.Is(err, apperrors.ErrInvalidToken), errors.Is(err, apperrors.ErrTokenExpired):
			return nil, ErrUnauthorized
		default:
			return nil, MapServiceErrorWithFields(err,
				zap.String(logkeys.Operation, "validate_recent_session"),
				zap.String(appLogger.KeyTokenType, "authentication"))
		}
	}

	if err := service.RequireRecentAuthentication(ctx, claims, 30*time.Minute); err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "require_recent_authentication"),
			zap.String(appLogger.KeyTokenType, "authentication"))
	}

	return claims, nil
}

type sessionCookieResponseWriter[U common_service_models.UserLike] struct {
	http.ResponseWriter
	handler     *CommonAPIHandler[U]
	request     *http.Request
	wroteHeader bool
}

func (w *sessionCookieResponseWriter[U]) WriteHeader(statusCode int) {
	w.applyPendingCookies()
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *sessionCookieResponseWriter[U]) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.applyPendingCookies()
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(data)
}

// Unwrap exposes the underlying writer to http.ResponseController and other
// wrappers that traverse the chain.
func (w *sessionCookieResponseWriter[U]) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// Hijack passes through to the underlying writer so WebSocket upgrades keep
// working when this wrapper sits in front of a streaming route (the session
// refresh middleware wraps every common API route, including /realtime/ws).
func (w *sessionCookieResponseWriter[U]) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

// Flush passes through to the underlying writer so streaming responses keep
// working behind the wrapper.
func (w *sessionCookieResponseWriter[U]) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		if !w.wroteHeader {
			w.applyPendingCookies()
			w.wroteHeader = true
		}
		f.Flush()
	}
}

func (w *sessionCookieResponseWriter[U]) applyPendingCookies() {
	if w.handler == nil || w.request == nil {
		return
	}
	// Clear wins over issue: a logout that races a session refresh must leave
	// the browser signed out, never with a freshly issued cookie.
	if w.request.Header.Get(sessionCookieClearRequestHeader) != "" {
		http.SetCookie(w.ResponseWriter, w.handler.newSessionCookie("", true))
		w.request.Header.Del(sessionCookieClearRequestHeader)
		w.request.Header.Del(sessionCookieIssueRequestHeader)
	} else if token := strings.TrimSpace(w.request.Header.Get(sessionCookieIssueRequestHeader)); token != "" {
		http.SetCookie(w.ResponseWriter, w.handler.newSessionCookie(token, false))
		w.request.Header.Del(sessionCookieIssueRequestHeader)
	}

	if w.request.Header.Get(oauthStateCookieClearRequestHeader) != "" {
		http.SetCookie(w.ResponseWriter, w.handler.newOAuthStateCookie("", true))
		w.request.Header.Del(oauthStateCookieClearRequestHeader)
		w.request.Header.Del(oauthStateCookieIssueRequestHeader)
	} else if state := strings.TrimSpace(w.request.Header.Get(oauthStateCookieIssueRequestHeader)); state != "" {
		http.SetCookie(w.ResponseWriter, w.handler.newOAuthStateCookie(state, false))
		w.request.Header.Del(oauthStateCookieIssueRequestHeader)
	}
}

func (h *CommonAPIHandler[U]) newSessionCookie(value string, clear bool) *http.Cookie {
	v := h.provider()
	return newCookieFromView(v, h.startup.SessionCookieName(), "/", value, v.SessionCookieTTL(), clear)
}

func (h *CommonAPIHandler[U]) oauthStateCookieName() string {
	return h.startup.SessionCookieName() + oauthStateCookieSuffix
}

// newOAuthStateCookie builds the browser binding cookie, which lives exactly
// as long as the state it binds. Its path is the OAuth route prefix, covering
// the state route that sets it and the callback that reads and clears it, so
// no other request carries it.
func (h *CommonAPIHandler[U]) newOAuthStateCookie(value string, clear bool) *http.Cookie {
	v := h.provider()
	return newCookieFromView(v, h.oauthStateCookieName(), h.oauthCookiePath(), value, v.OAuthStateTTL(), clear)
}

// oauthCookiePath is the common API prefix plus the OAuth route prefix; the
// prefix is restart-bound like the routes themselves.
func (h *CommonAPIHandler[U]) oauthCookiePath() string {
	return strings.TrimRight(h.startup.PathPrefix(), "/") + pathOAuthPrefix
}

func newCookieFromView(v config.APIView, name, path, value string, ttl time.Duration, clear bool) *http.Cookie {
	cookie := &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     path,
		Domain:   strings.TrimSpace(v.SessionCookieDomain()),
		HttpOnly: true,
		Secure:   v.SessionCookieSecure(),
		SameSite: parseSessionCookieSameSite(v.SessionCookieSameSite()),
	}
	if clear {
		cookie.MaxAge = -1
		cookie.Expires = time.Unix(0, 0).UTC()
	} else {
		cookie.MaxAge = int(ttl.Seconds())
		cookie.Expires = time.Now().UTC().Add(ttl)
	}
	return cookie
}

func (h *CommonAPIHandler[U]) sessionCookieMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip any client-supplied internal signaling headers before the
		// handler runs, so only handler-set values can drive cookie issuance.
		stripInternalHeaders(r)
		next.ServeHTTP(&sessionCookieResponseWriter[U]{
			ResponseWriter: w,
			handler:        h,
			request:        r,
		}, r)
	})
}

func markIssueSessionCookie(r *http.Request, token string) {
	if strings.TrimSpace(token) == "" {
		return
	}
	r.Header.Set(sessionCookieIssueRequestHeader, token)
}

func markClearSessionCookie(r *http.Request) {
	r.Header.Set(sessionCookieClearRequestHeader, "1")
}

func markIssueOAuthStateCookie(r *http.Request, binding string) {
	if strings.TrimSpace(binding) != "" {
		r.Header.Set(oauthStateCookieIssueRequestHeader, binding)
	}
}

func markClearOAuthStateCookie(r *http.Request) {
	r.Header.Set(oauthStateCookieClearRequestHeader, "1")
}

// RegisterAuthRoutes executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) RegisterAuthRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	log := h.logger.Named(commonAPILoggerName)
	// EnableEmailAuth and EnableOAuth are restart-bound: their routes are either
	// registered at startup or absent. SMS stays hot, so phone routes check it per request.
	emailAuth, oauthAuth := h.startup.EnableEmailAuth(), h.startup.EnableOAuth()
	if !emailAuth && !oauthAuth {
		group = h.featureGroup(group, config.APIView.SMSEnabled)
	}

	if emailAuth {
		h.addEmailAuthRoutes(group)
		h.addPasswordManagementRoutes(group)
		log.Debug("Email authentication routes registered")
	}

	h.addPhoneAuthRoutes(h.featureGroup(group, config.APIView.SMSEnabled))
	log.Debug("Phone authentication routes registered")

	if oauthAuth {
		h.addOAuthRoutes(group)
		log.Debug("OAuth authentication routes registered")
	}

	// Multi-auth routes (always enabled)
	h.addMultiAuthRoutes(group)
	log.Debug("Multi-auth routes registered")

	h.addSessionRoutes(group)
	log.Debug("Session management routes registered")

	log.Debug("Authentication routes registered to route group")
}

func (h *CommonAPIHandler[U]) addSessionRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	transactionMiddleware := h.GetTransactionMiddleware()
	group.Route(router.RouteConfig[*proto_models.LogoutRequest, *proto_models.LogoutResponse]{
		Path:        pathAuthLogout,
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.LogoutRequest, *proto_models.LogoutResponse](),
		Handler:     h.LogoutGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeLogoutRequest,
		Middlewares: []common.Middleware{transactionMiddleware, h.sessionCookieMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.ListActiveSessionsRequest, *proto_models.ListActiveSessionsResponse]{
		Path:       pathAuthSessions,
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ListActiveSessionsRequest, *proto_models.ListActiveSessionsResponse](),
		Handler:    h.ListActiveSessions,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeListActiveSessionsRequest,
	})
	group.Route(router.RouteConfig[*proto_models.RevokeSessionRequest, *proto_models.RevokeSessionResponse]{
		Path:        pathAuthRevokeSession,
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.RevokeSessionRequest, *proto_models.RevokeSessionResponse](),
		Handler:     h.RevokeSession,
		SourceType:  router.Body,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.RevokeOtherSessionsRequest, *proto_models.RevokeOtherSessionsResponse]{
		Path:        pathAuthRevokeOthers,
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.RevokeOtherSessionsRequest, *proto_models.RevokeOtherSessionsResponse](),
		Handler:     h.RevokeOtherSessions,
		SourceType:  router.Body,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
}

// addEmailAuthRoutes executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) addEmailAuthRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	// Get transaction middleware for email auth operations
	transactionMiddleware := h.GetTransactionMiddleware()
	group.Route(router.RouteConfig[*proto_models.EmailRegistrationRequest, *proto_models.EmailRegistrationResponse]{
		Path:        "/auth/email/create",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.NoAuth),
		Codec:       codec.NewProtoCodec[*proto_models.EmailRegistrationRequest, *proto_models.EmailRegistrationResponse](),
		Handler:     h.CreateAccountEmailRegister,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUserEmailRegistrationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.EmailAuthRequest, *proto_models.EmailAuthResponse]{
		Path:        "/auth/email/login",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.NoAuth),
		Codec:       codec.NewProtoCodec[*proto_models.EmailAuthRequest, *proto_models.EmailAuthResponse](),
		Handler:     h.EmailLoginGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeUserEmailAuthRequest,
		Middlewares: []common.Middleware{transactionMiddleware, h.sessionCookieMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.PasswordChangeRequest, *proto_models.PasswordChangeResponse]{
		Path:        "/auth/email/change-password",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.PasswordChangeRequest, *proto_models.PasswordChangeResponse](),
		Handler:     h.ChangePasswordGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizePasswordChangeRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.EmailAcceptInvitationRequest, *proto_models.EmailAcceptInvitationResponse]{
		Path:        "/auth/email/accept-invitation",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.NoAuth),
		Codec:       codec.NewProtoCodec[*proto_models.EmailAcceptInvitationRequest, *proto_models.EmailAcceptInvitationResponse](),
		Handler:     h.EmailAcceptInvitation,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeEmailAcceptInvitationRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.PasswordResetRequest, *proto_models.PasswordResetResponse]{
		Path:        "/auth/email/reset-password",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.NoAuth),
		Codec:       codec.NewProtoCodec[*proto_models.PasswordResetRequest, *proto_models.PasswordResetResponse](),
		Handler:     h.ResetPasswordGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizePasswordResetRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.EmailVerificationRequest, *proto_models.EmailAuthResponse]{
		Path:        "/auth/email/complete-verification",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.NoAuth),
		Codec:       codec.NewProtoCodec[*proto_models.EmailVerificationRequest, *proto_models.EmailAuthResponse](),
		Handler:     h.NewUserCompleteEmailVerificationGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeEmailVerificationRequest,
		Middlewares: []common.Middleware{transactionMiddleware, h.sessionCookieMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.PasswordResetCompleteRequest, *proto_models.EmailAuthResponse]{
		Path:        "/auth/email/complete-reset",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.NoAuth),
		Codec:       codec.NewProtoCodec[*proto_models.PasswordResetCompleteRequest, *proto_models.EmailAuthResponse](),
		Handler:     h.CompletePasswordResetGeneric,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizePasswordResetCompleteRequest,
		Middlewares: []common.Middleware{transactionMiddleware, h.sessionCookieMiddleware},
	})
}

// addPhoneAuthRoutes executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) addPhoneAuthRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group.Route(router.RouteConfig[*proto_models.SendPhoneOTPRequest, *proto_models.SendPhoneOTPResponse]{
		Path:       "/auth/phone/send-otp",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.SendPhoneOTPRequest, *proto_models.SendPhoneOTPResponse](),
		Handler:    h.SendPhoneVerificationOTP,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeSendPhoneOTPRequest,
	})
	group.Route(router.RouteConfig[*proto_models.VerifyPhoneOTPRequest, *proto_models.VerifyPhoneOTPResponse]{
		Path:       "/auth/phone/verify-otp",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.VerifyPhoneOTPRequest, *proto_models.VerifyPhoneOTPResponse](),
		Handler:    h.VerifyPhoneOTP,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeVerifyPhoneOTPRequest,
	})
	group.Route(router.RouteConfig[*proto_models.ResendPhoneOTPRequest, *proto_models.ResendPhoneOTPResponse]{
		Path:       "/auth/phone/resend-otp",
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.ResendPhoneOTPRequest, *proto_models.ResendPhoneOTPResponse](),
		Handler:    h.ResendPhoneVerificationOTP,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeResendPhoneOTPRequest,
	})
}

// CreateAccountEmailRegister executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) CreateAccountEmailRegister(r *http.Request, req *proto_models.EmailRegistrationRequest) (*proto_models.EmailRegistrationResponse, error) {
	// Input is already sanitized by SRouter sanitizer

	// Call service layer with all available registration data
	user, newUserRegistrationErr := h.serviceHandler.NewUserEmailRegistration(r.Context(), req.Email, req.Password, req.Username, req.FirstName, req.LastName, req.FrontendOrigin)
	if newUserRegistrationErr != nil {
		return nil, MapServiceErrorWithFields(newUserRegistrationErr,
			zap.String(logkeys.Operation, "register_user_with_email"),
			zap.String(appLogger.KeyEmail, req.Email),
			zap.String(appLogger.KeyUsername, req.GetUsername()),
			zap.String(appLogger.KeyAuthMethod, "email"))
	}
	if user == nil {
		return nil, MapServiceErrorWithFields(
			appLogger.InvariantError("email_registration_returns_user", nil,
				zap.String(logkeys.Stage, "register_user_with_email"),
				zap.String(logkeys.Expected, "non-nil registered user"),
				zap.String(logkeys.Actual, "nil registered user")),
			zap.String(logkeys.Operation, "register_user_with_email"),
			zap.String(appLogger.KeyEmail, req.Email),
			zap.String(appLogger.KeyUsername, req.GetUsername()))
	}

	// Log success
	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("User registered with email",
		zap.String(logkeys.Operation, "register_user_with_email"),
		zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
		zap.String(appLogger.KeyEmail, (*user).GetEmail()),
		zap.String(appLogger.KeyAuthMethod, "email"))

	return &proto_models.EmailRegistrationResponse{}, nil
}

// EmailLoginGeneric executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) EmailLoginGeneric(r *http.Request, req *proto_models.EmailAuthRequest) (*proto_models.EmailAuthResponse, error) {
	// Extract OTP code if provided
	otpCode := ""
	if req.OtpCode != nil {
		otpCode = *req.OtpCode
	}

	// Login the user with OTP support (req is already sanitized by SRouter)
	user, token, otpRequired, err := h.serviceHandler.LoginUserByEmailWithOTP(r.Context(), req.Email, req.Password, otpCode)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "login_user_with_email"),
			zap.String(appLogger.KeyEmail, req.Email),
			zap.String(appLogger.KeyAuthMethod, "email"),
			zap.Bool(appLogger.KeyOTPProvided, otpCode != ""))
	}

	// If OTP is required, return response indicating OTP needed
	if otpRequired {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Email login requires OTP",
			zap.String(logkeys.Operation, "login_user_with_email"),
			zap.String(appLogger.KeyEmail, req.Email),
			zap.String(appLogger.KeyAuthMethod, "email"),
			zap.String(appLogger.KeyOutcome, "otp_required"))
		return &proto_models.EmailAuthResponse{
			OtpRequired: true,
			Message:     new("Please provide your 6-digit authenticator code"),
		}, nil
	}
	if user == nil {
		return nil, MapServiceErrorWithFields(
			appLogger.InvariantError("successful_email_login_returns_user", nil,
				zap.String(logkeys.Stage, "login_user_with_email"),
				zap.String(logkeys.Expected, "non-nil authenticated user"),
				zap.String(logkeys.Actual, "nil authenticated user")),
			zap.String(logkeys.Operation, "login_user_with_email"),
			zap.String(appLogger.KeyEmail, req.Email),
			zap.String(appLogger.KeyAuthMethod, "email"),
			zap.Bool(appLogger.KeyOTPProvided, otpCode != ""))
	}

	// Create the successful login response
	resp := &proto_models.EmailAuthResponse{
		OtpRequired: false,
	}
	markIssueSessionCookie(r, token)

	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("User logged in with email",
		zap.String(logkeys.Operation, "login_user_with_email"),
		zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
		zap.String(appLogger.KeyEmail, (*user).GetEmail()),
		zap.String(appLogger.KeyAuthMethod, "email"),
		zap.Bool(appLogger.KeyOTPUsed, otpCode != ""))
	return resp, nil
}

// ChangePasswordGeneric executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) ChangePasswordGeneric(r *http.Request, req *proto_models.PasswordChangeRequest) (*proto_models.PasswordChangeResponse, error) {
	// Get the user pointer from the context
	user, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || user == nil { // Add checks for nil pointers
		return nil, ErrUnauthorized
	}

	// Change the password
	// We need to handle the pointer type which scontext.GetUser returns vs what the service handler expects
	err := h.serviceHandler.ChangeUserPassword(r.Context(), (*user).GetID(), req.OldPassword, req.NewPassword)
	if err != nil {
		return nil, MapChangePasswordErrorWithFields(err,
			zap.String(logkeys.Operation, "change_password"),
			zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
			zap.String(appLogger.KeyEmail, (*user).GetEmail()))
	}

	// Create the response
	resp := &proto_models.PasswordChangeResponse{}

	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Password changed",
		zap.String(logkeys.Operation, "change_password"),
		zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
		zap.String(appLogger.KeyEmail, (*user).GetEmail()))
	return resp, nil
}

// ResetPasswordGeneric executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) ResetPasswordGeneric(r *http.Request, req *proto_models.PasswordResetRequest) (*proto_models.PasswordResetResponse, error) {
	// Request password reset
	err := h.serviceHandler.ResetPasswordRequest(r.Context(), req.Email, req.FrontendOrigin)
	if err != nil {
		// An untrusted frontend origin reveals nothing about account existence, so it is
		// safe (and required — no silent fallback) to reject it outright.
		if errors.Is(err, apperrors.ErrUntrustedFrontendOrigin) {
			return nil, MapServiceErrorWithFields(err,
				zap.String(logkeys.Operation, "request_password_reset"),
				zap.String(appLogger.KeyEmail, req.Email),
				zap.String(appLogger.KeyFrontendOrigin, req.GetFrontendOrigin()))
		}
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Warn("Password reset request degraded while preserving account privacy",
			zap.String(logkeys.Operation, "request_password_reset"),
			zap.String(appLogger.KeyEmail, req.Email),
			zap.String(appLogger.KeyFrontendOrigin, req.GetFrontendOrigin()),
			zap.String(logkeys.Fallback, "return_success"),
			zap.Error(err))
		// Don't return MapServiceError here to prevent email enumeration
		// Return success even if user doesn't exist or email sending fails (as per service logic)
	}

	// Create the response (always success for security)
	resp := &proto_models.PasswordResetResponse{}

	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Password reset initiated",
		zap.String(logkeys.Operation, "request_password_reset"),
		zap.String(appLogger.KeyEmail, req.Email))
	return resp, nil
}

// REMOVED: ResendVerificationEmailGeneric handler

// NewUserCompleteEmailVerificationGeneric executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) NewUserCompleteEmailVerificationGeneric(r *http.Request, req *proto_models.EmailVerificationRequest) (*proto_models.EmailAuthResponse, error) {
	// Call service handler
	user, authToken, err := h.serviceHandler.VerifyNewUserEmail(r.Context(), req.VerificationCode)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "complete_email_verification"),
			zap.String(appLogger.KeyTokenType, "email_verification"),
			zap.String(appLogger.KeyToken, req.VerificationCode))
	}
	if user == nil {
		return nil, MapServiceErrorWithFields(
			appLogger.InvariantError("email_verification_returns_user", nil,
				zap.String(logkeys.Stage, "complete_email_verification"),
				zap.String(logkeys.Expected, "non-nil verified user"),
				zap.String(logkeys.Actual, "nil verified user")),
			zap.String(logkeys.Operation, "complete_email_verification"),
			zap.String(appLogger.KeyTokenType, "email_verification"),
			zap.String(appLogger.KeyToken, req.VerificationCode))
	}

	markIssueSessionCookie(r, authToken)
	resp := &proto_models.EmailAuthResponse{}

	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Email verification completed",
		zap.String(logkeys.Operation, "complete_email_verification"),
		zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
		zap.String(appLogger.KeyEmail, (*user).GetEmail()),
		zap.String(appLogger.KeyTokenType, "email_verification"),
		zap.Bool(appLogger.KeyAutoLogin, authToken != ""))
	return resp, nil
}

// CompletePasswordResetGeneric executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) CompletePasswordResetGeneric(r *http.Request, req *proto_models.PasswordResetCompleteRequest) (*proto_models.EmailAuthResponse, error) {
	// Call service handler with verification code and new password
	user, authToken, err := h.serviceHandler.ResetPasswordWithToken(r.Context(), req.VerificationCode, req.NewPassword)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "complete_password_reset"),
			zap.String(appLogger.KeyTokenType, "password_reset"),
			zap.String(appLogger.KeyToken, req.VerificationCode))
	}

	markIssueSessionCookie(r, authToken)
	resp := &proto_models.EmailAuthResponse{}

	if user != nil {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Password reset completed",
			zap.String(logkeys.Operation, "complete_password_reset"),
			zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
			zap.String(appLogger.KeyEmail, (*user).GetEmail()),
			zap.String(appLogger.KeyTokenType, "password_reset"),
			zap.Bool(appLogger.KeyAutoLogin, authToken != ""))
	} else {
		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Password reset completed successfully",
			zap.String(logkeys.Operation, "complete_password_reset"),
			zap.String(appLogger.KeyTokenType, "password_reset"),
			zap.Bool(appLogger.KeyAutoLogin, authToken != ""))
	}
	return resp, nil
}

// LogoutGeneric executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) LogoutGeneric(r *http.Request, req *proto_models.LogoutRequest) (*proto_models.LogoutResponse, error) {
	authToken, err := extractAuthTokenFromRequest(r, h.startup.SessionCookieName())
	if err != nil {
		return nil, err
	}

	// Get user from context to determine if we should logout everywhere
	user, success := scontext.GetUser[common_service_models.UserIdType, U](r.Context())
	if !success || user == nil {
		return nil, ErrUnauthorized
	}

	// Always drop THIS browser's session cookie, on every path including the
	// error paths below. Clearing the cookie is a response-header operation, not
	// part of the DB transaction, so a rollback of a failed revocation cannot
	// revive it. Even when server-side revocation fails, the browser must not
	// keep presenting a live token — otherwise a logout on a shared machine
	// leaves the session silently reusable by the next visitor, while the UI
	// shows logged-out.
	//
	// The original "log out everywhere" concern (clearing locally while every
	// server-side session stays valid) is still honored: on revocation failure
	// we return an error rather than reporting success, so the failure is never
	// silent.
	markClearSessionCookie(r)

	// Check if logout everywhere is requested
	if req.LogoutEverywhere {
		// Logout from all devices
		err = h.serviceHandler.LogoutUserEverywhere(r.Context(), (*user).GetID())
		if err != nil {
			return nil, router.NewHTTPErrorWithCause(http.StatusInternalServerError, "Logout failed", err).WithFields(
				zap.String(logkeys.Operation, "logout_user_everywhere"),
				zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
				zap.String(appLogger.KeyEmail, (*user).GetEmail()))
		}

		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("User logged out from all devices",
			zap.String(logkeys.Operation, "logout_user_everywhere"),
			zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
			zap.String(appLogger.KeyEmail, (*user).GetEmail()))

		return &proto_models.LogoutResponse{}, nil
	} else {
		// Logout current session only
		err = h.serviceHandler.LogoutUser(r.Context(), authToken)
		if err != nil {
			// Do not disclose whether the presented token exists.
			return nil, router.NewHTTPErrorWithCause(http.StatusInternalServerError, "Logout failed", err).WithFields(
				zap.String(logkeys.Operation, "logout_user"),
				zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
				zap.String(appLogger.KeyEmail, (*user).GetEmail()))
		}

		appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("User logged out",
			zap.String(logkeys.Operation, "logout_user"),
			zap.Uint64(appLogger.KeyUserID, uint64((*user).GetID())),
			zap.String(appLogger.KeyEmail, (*user).GetEmail()))

		return &proto_models.LogoutResponse{}, nil
	}
}

// ListActiveSessions returns active authentication sessions for the authenticated user.
func (h *CommonAPIHandler[U]) ListActiveSessions(r *http.Request, req *proto_models.ListActiveSessionsRequest) (*proto_models.ListActiveSessionsResponse, error) {
	authToken, err := extractAuthTokenFromRequest(r, h.startup.SessionCookieName())
	if err != nil {
		return nil, err
	}

	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	resp, err := h.serviceHandler.ListActiveSessions(r.Context(), userID, authToken, req.GetPagination())
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "list_active_sessions"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}
	return resp, nil
}

// RevokeSession revokes a non-current active authentication session for the authenticated user.
func (h *CommonAPIHandler[U]) RevokeSession(r *http.Request, req *proto_models.RevokeSessionRequest) (*proto_models.RevokeSessionResponse, error) {
	authToken, err := extractAuthTokenFromRequest(r, h.startup.SessionCookieName())
	if err != nil {
		return nil, err
	}

	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	resp, err := h.serviceHandler.RevokeSession(r.Context(), userID, authToken, req.GetSessionId())
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "revoke_session"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeySessionID, req.GetSessionId()))
	}
	return resp, nil
}

// RevokeOtherSessions revokes all active authentication sessions except the current one.
func (h *CommonAPIHandler[U]) RevokeOtherSessions(r *http.Request, _ *proto_models.RevokeOtherSessionsRequest) (*proto_models.RevokeOtherSessionsResponse, error) {
	authToken, err := extractAuthTokenFromRequest(r, h.startup.SessionCookieName())
	if err != nil {
		return nil, err
	}

	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found {
		return nil, ErrUnauthorized
	}

	resp, err := h.serviceHandler.RevokeOtherSessions(r.Context(), userID, authToken)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "revoke_other_sessions"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}
	return resp, nil
}

// addMultiAuthRoutes registers account routes available with any login method.
func (h *CommonAPIHandler[U]) addMultiAuthRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group.Route(router.RouteConfig[*proto_models.GetAuthMethodsResponse, *proto_models.GetAuthMethodsResponse]{
		Path:       "/auth/methods",
		Methods:    []router.HttpMethod{http.MethodGet, http.MethodOptions},
		AuthLevel:  new(router.AuthRequired),
		Codec:      codec.NewProtoCodec[*proto_models.GetAuthMethodsResponse, *proto_models.GetAuthMethodsResponse](),
		Handler:    h.HandleGetAuthMethods,
		SourceType: router.Empty,
	})
	group.Route(router.RouteConfig[*proto_models.CheckUsernameAvailabilityRequest, *proto_models.CheckUsernameAvailabilityResponse]{
		Path:       pathAuthCheckUsername,
		Methods:    []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:  new(router.NoAuth),
		Codec:      codec.NewProtoCodec[*proto_models.CheckUsernameAvailabilityRequest, *proto_models.CheckUsernameAvailabilityResponse](),
		Handler:    h.HandleCheckUsernameAvailability,
		SourceType: router.Body,
		Sanitizer:  h.sanitizer.SanitizeCheckUsernameAvailabilityRequest,
	})
}

// addPasswordManagementRoutes registers password and TOTP management, which
// require email authentication; it is called only when that is enabled at startup.
func (h *CommonAPIHandler[U]) addPasswordManagementRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	transactionMiddleware := h.GetTransactionMiddleware()
	group.Route(router.RouteConfig[*proto_models.AddPasswordRequest, *proto_models.AddPasswordResponse]{
		Path:        "/auth/add-password",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.AddPasswordRequest, *proto_models.AddPasswordResponse](),
		Handler:     h.HandleAddPassword,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeAddPasswordRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.SetupOTPRequest, *proto_models.SetupOTPResponse]{
		Path:        "/auth/otp/setup",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.SetupOTPRequest, *proto_models.SetupOTPResponse](),
		Handler:     h.HandleSetupOTP,
		SourceType:  router.Body,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.VerifyOTPSetupRequest, *proto_models.VerifyOTPSetupResponse]{
		Path:        "/auth/otp/verify-setup",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.VerifyOTPSetupRequest, *proto_models.VerifyOTPSetupResponse](),
		Handler:     h.HandleVerifyOTPSetup,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeVerifyOTPSetupRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
	group.Route(router.RouteConfig[*proto_models.DisableOTPRequest, *proto_models.DisableOTPResponse]{
		Path:        "/auth/otp/disable",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.AuthRequired),
		Codec:       codec.NewProtoCodec[*proto_models.DisableOTPRequest, *proto_models.DisableOTPResponse](),
		Handler:     h.HandleDisableOTP,
		SourceType:  router.Body,
		Middlewares: []common.Middleware{transactionMiddleware},
	})
}

// HandleAddPassword executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) HandleAddPassword(r *http.Request, req *proto_models.AddPasswordRequest) (*proto_models.AddPasswordResponse, error) {
	ctx := r.Context()
	log := appLogger.For(ctx, h.logger, commonAPILoggerName)

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		log.Warn("unauthenticated add password attempt")
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	log = log.With(zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	log.Debug("add password request received")

	_, err := validateRecentSessionFromRequest(ctx, r, h.startup.SessionCookieName(), h.serviceHandler)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "add_password_authentication"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Check passwords match
	if req.Password != req.ConfirmPassword {
		return nil, router.NewHTTPError(http.StatusBadRequest, "Passwords do not match")
	}

	// Add password through service layer
	err = h.serviceHandler.AddPasswordAuthentication(ctx, uint64(userID), req.Password)
	if err != nil {
		switch {
		case errors.Is(err, apperrors.ErrPasswordAuthAlreadyExists):
			return nil, router.NewHTTPErrorWithCause(http.StatusConflict, "Password authentication already set up", err).WithFields(
				zap.String(logkeys.Operation, "add_password_authentication"),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)))
		default:
			return nil, router.NewHTTPErrorWithCause(http.StatusInternalServerError, "Failed to add password. Please try again.", err).WithFields(
				zap.String(logkeys.Operation, "add_password_authentication"),
				zap.Uint64(appLogger.KeyUserID, uint64(userID)))
		}
	}

	log.Info("password added successfully", zap.String(logkeys.Operation, "add_password_authentication"))

	return &proto_models.AddPasswordResponse{}, nil
}

// HandleGetAuthMethods executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) HandleGetAuthMethods(r *http.Request, _ *proto_models.GetAuthMethodsResponse) (*proto_models.GetAuthMethodsResponse, error) {
	ctx := r.Context()
	log := appLogger.For(ctx, h.logger, commonAPILoggerName)

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		log.Warn("unauthenticated get auth methods attempt")
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	log = log.With(zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	log.Debug("get auth methods request received")

	// Get auth methods from service layer
	methods, err := h.serviceHandler.GetUserAuthMethods(ctx, uint64(userID))
	if err != nil {
		return nil, router.NewHTTPErrorWithCause(http.StatusInternalServerError, "Failed to retrieve authentication methods", err).WithFields(
			zap.String(logkeys.Operation, "get_user_auth_methods"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	// Convert to proto response
	response := &proto_models.GetAuthMethodsResponse{
		HasPassword:  methods.HasPassword,
		TotalMethods: uint32(methods.CountAuthMethods()),
		OtpEnabled:   methods.OTPEnabled,
	}
	if methods.OTPVerifiedAt != nil {
		response.OtpVerifiedAt = timestamppb.New(*methods.OTPVerifiedAt)
	}

	// Add OAuth provider details; display names come from the current
	// provider sample and stay empty for providers no longer configured.
	displayNames := map[string]string{}
	if len(methods.OAuthProviders) > 0 {
		for _, configured := range h.serviceHandler.ListOAuthProviders(ctx) {
			displayNames[configured.Slug] = configured.DisplayName
		}
	}
	for _, provider := range methods.OAuthProviders {
		response.OauthProviders = append(response.OauthProviders, &proto_models.OAuthProviderInfo{
			ProviderSlug: provider.ProviderSlug,
			DisplayName:  displayNames[provider.ProviderSlug],
			Email:        provider.Email,
			FirstLogin:   timestamppb.New(provider.FirstLogin),
			LastLogin:    timestamppb.New(provider.LastLogin),
			LoginCount:   uint32(provider.LoginCount),
		})
	}

	if ce := log.Check(zap.DebugLevel, "returning auth methods"); ce != nil {
		ce.Write(
			zap.String(logkeys.Operation, "get_user_auth_methods"),
			zap.Bool(appLogger.KeyHasPasswordLegacy, response.HasPassword),
			zap.Int(appLogger.KeyOAuthProviders, len(response.OauthProviders)),
		)
	}

	return response, nil
}

// HandleCheckUsernameAvailability executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) HandleCheckUsernameAvailability(r *http.Request, req *proto_models.CheckUsernameAvailabilityRequest) (*proto_models.CheckUsernameAvailabilityResponse, error) {
	ctx := r.Context()

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, commonAPILoggerName).Debug("checking username availability",
			zap.String(logkeys.Operation, "check_username_availability"),
			zap.String(appLogger.KeyUsername, req.Username))
	}

	// Call service layer to check availability
	available, message, err := h.serviceHandler.CheckUsernameAvailability(ctx, req.Username)
	if err != nil {
		return nil, router.NewHTTPErrorWithCause(http.StatusInternalServerError, "Failed to check username availability", err).WithFields(
			zap.String(logkeys.Operation, "check_username_availability"),
			zap.String(appLogger.KeyUsername, req.Username))
	}

	// Create response
	response := &proto_models.CheckUsernameAvailabilityResponse{
		Available: available,
		Message:   message,
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, commonAPILoggerName).Debug("username availability checked",
			zap.String(logkeys.Operation, "check_username_availability"),
			zap.String(appLogger.KeyUsername, req.Username),
			zap.Bool(appLogger.KeyAvailable, available))
	}

	return response, nil
}

// HandleSetupOTP executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) HandleSetupOTP(r *http.Request, req *proto_models.SetupOTPRequest) (*proto_models.SetupOTPResponse, error) {
	ctx := r.Context()
	log := appLogger.For(ctx, h.logger, commonAPILoggerName)

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		log.Warn("unauthenticated OTP setup attempt")
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	log = log.With(zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	log.Debug("OTP setup request received")

	// Validate password
	if req.CurrentPassword == "" {
		return nil, router.NewHTTPError(http.StatusBadRequest, "Current password is required to set up OTP")
	}

	// Call service layer to setup OTP
	secret, qrCodeURL, err := h.serviceHandler.SetupOTP(ctx, uint64(userID), req.CurrentPassword)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "setup_otp"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	log.Info("OTP setup initiated successfully", zap.String(logkeys.Operation, "setup_otp"))

	return &proto_models.SetupOTPResponse{
		Secret:    &secret,
		QrCodeUrl: &qrCodeURL,
	}, nil
}

// HandleVerifyOTPSetup executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) HandleVerifyOTPSetup(r *http.Request, req *proto_models.VerifyOTPSetupRequest) (*proto_models.VerifyOTPSetupResponse, error) {
	ctx := r.Context()
	log := appLogger.For(ctx, h.logger, commonAPILoggerName)

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		log.Warn("unauthenticated OTP verify setup attempt")
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	log = log.With(zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	log.Debug("OTP verify setup request received")

	// Validate OTP code
	if req.OtpCode == "" {
		return nil, router.NewHTTPError(http.StatusBadRequest, "OTP code is required")
	}
	if req.CurrentPassword == "" {
		return nil, router.NewHTTPError(http.StatusBadRequest, "Current password is required to verify OTP setup")
	}

	// Call service layer to verify OTP setup
	err := h.serviceHandler.VerifyOTPSetup(ctx, uint64(userID), req.OtpCode, req.CurrentPassword)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "verify_otp_setup"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Bool(appLogger.KeyOTPProvided, req.OtpCode != ""))
	}

	log.Info("OTP setup completed successfully", zap.String(logkeys.Operation, "verify_otp_setup"))

	return &proto_models.VerifyOTPSetupResponse{}, nil
}

// HandleDisableOTP executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) HandleDisableOTP(r *http.Request, req *proto_models.DisableOTPRequest) (*proto_models.DisableOTPResponse, error) {
	ctx := r.Context()
	log := appLogger.For(ctx, h.logger, commonAPILoggerName)

	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		log.Warn("unauthenticated OTP disable attempt")
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	log = log.With(zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	log.Debug("OTP disable request received")

	// Validate password
	if req.CurrentPassword == "" {
		return nil, router.NewHTTPError(http.StatusBadRequest, "Current password is required to disable OTP")
	}

	// Call service layer to disable OTP
	err := h.serviceHandler.DisableOTP(ctx, uint64(userID), req.CurrentPassword)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "disable_otp"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	log.Info("OTP disabled successfully", zap.String(logkeys.Operation, "disable_otp"))

	return &proto_models.DisableOTPResponse{}, nil
}

// ===================================================================
// Phone Verification Handler Methods
// ===================================================================

// SendPhoneVerificationOTP executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) SendPhoneVerificationOTP(r *http.Request, req *proto_models.SendPhoneOTPRequest) (*proto_models.SendPhoneOTPResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	// Call service layer
	err := h.serviceHandler.SendPhoneVerificationOTP(r.Context(), userID, req.PhoneNumber)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "send_phone_verification_otp"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.String(appLogger.KeyPhoneNumber, req.PhoneNumber))
	}

	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Phone verification OTP sent",
		zap.String(logkeys.Operation, "send_phone_verification_otp"),
		zap.Uint64(appLogger.KeyUserID, uint64(userID)),
		zap.String(appLogger.KeyPhoneNumber, req.PhoneNumber))

	return &proto_models.SendPhoneOTPResponse{}, nil
}

// VerifyPhoneOTP executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) VerifyPhoneOTP(r *http.Request, req *proto_models.VerifyPhoneOTPRequest) (*proto_models.VerifyPhoneOTPResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	// Call service layer
	err := h.serviceHandler.VerifyPhoneOTP(r.Context(), uint64(userID), req.OtpCode)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "verify_phone_otp"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)),
			zap.Bool(appLogger.KeyOTPProvided, req.OtpCode != ""))
	}

	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Phone verification completed",
		zap.String(logkeys.Operation, "verify_phone_otp"),
		zap.Uint64(appLogger.KeyUserID, uint64(userID)))

	return &proto_models.VerifyPhoneOTPResponse{}, nil
}

// ResendPhoneVerificationOTP executes this step in the common_api backend flow.
func (h *CommonAPIHandler[U]) ResendPhoneVerificationOTP(r *http.Request, req *proto_models.ResendPhoneOTPRequest) (*proto_models.ResendPhoneOTPResponse, error) {
	// Get authenticated user ID
	userID, found := scontext.GetUserID[common_service_models.UserIdType](r.Context())
	if !found || userID == 0 {
		return nil, router.NewHTTPError(http.StatusUnauthorized, "Authentication required")
	}

	// Call service layer
	err := h.serviceHandler.ResendPhoneVerificationOTP(r.Context(), uint64(userID))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "resend_phone_verification_otp"),
			zap.Uint64(appLogger.KeyUserID, uint64(userID)))
	}

	appLogger.For(r.Context(), h.logger, commonAPILoggerName).Info("Phone verification OTP resent",
		zap.String(logkeys.Operation, "resend_phone_verification_otp"),
		zap.Uint64(appLogger.KeyUserID, uint64(userID)))

	return &proto_models.ResendPhoneOTPResponse{}, nil
}

func (h *CommonAPIHandler[U]) EmailAcceptInvitation(r *http.Request, req *proto_models.EmailAcceptInvitationRequest) (*proto_models.EmailAcceptInvitationResponse, error) {
	response, err := h.serviceHandler.EmailAcceptInvitation(r.Context(), req)
	if err != nil {
		return nil, MapServiceError(err)
	}
	return response, nil
}

func parseSessionCookieSameSite(value string) http.SameSite {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "lax":
		return http.SameSiteLaxMode
	case "none":
		return http.SameSiteNoneMode
	case "strict", "":
		return http.SameSiteStrictMode
	default:
		return http.SameSiteStrictMode
	}
}
