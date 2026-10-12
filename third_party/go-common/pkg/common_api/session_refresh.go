package common_api

import (
	"net/http"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/common"
)

// sessionRefreshMiddleware probabilistically replaces the session cookie as it
// approaches expiry (sliding sessions). It only reads the session cookie and
// delegates the decision to the service layer, which fails open — a request is
// never broken by refresh trouble. Requests under the auth prefix are skipped:
// login/logout flows manage the session cookie themselves, and a logout must
// never race a refresh. HTTP upgrade requests are also skipped because a
// hijacked handshake bypasses the response writer and cannot reliably carry the
// replacement cookie. Security tokens (email verification, password recovery)
// can never be refreshed — they never travel in the session cookie, and both the
// stateless format validation and the DB token-type check reject them.
//
// The replacement cookie is signaled via markIssueSessionCookie and emitted by
// the sessionCookieResponseWriter installed by sessionCookieMiddleware, so this
// middleware must run inside that wrapper (GetSessionRefreshMiddleware composes
// the two).
func (h *CommonAPIHandler[U]) sessionRefreshMiddleware(next http.Handler) http.Handler {
	authPrefix := strings.TrimRight(h.startup.PathPrefix(), "/") + "/auth/"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions || strings.HasPrefix(r.URL.Path, authPrefix) || isHTTPUpgradeRequest(r) || h.serviceHandler == nil {
			next.ServeHTTP(w, r)
			return
		}

		authToken, err := extractAuthTokenFromRequest(r, h.startup.SessionCookieName())
		if err != nil {
			// No session cookie — nothing to refresh.
			next.ServeHTTP(w, r)
			return
		}

		if newToken, refreshed, _ := h.serviceHandler.MaybeRefreshSession(r.Context(), authToken); refreshed {
			markIssueSessionCookie(r, newToken)
		}
		next.ServeHTTP(w, r)
	})
}

// isHTTPUpgradeRequest reports whether the request carries a complete HTTP/1.1
// upgrade handshake. Such responses are commonly written directly to a
// hijacked connection, bypassing ResponseWriter.WriteHeader, so they cannot
// reliably deliver a replacement session cookie.
func isHTTPUpgradeRequest(r *http.Request) bool {
	if r == nil || !headerHasToken(r.Header, "Connection", "upgrade") {
		return false
	}

	for _, value := range r.Header.Values("Upgrade") {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func headerHasToken(header http.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for candidate := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(strings.TrimSpace(candidate), token) {
				return true
			}
		}
	}
	return false
}

// GetSessionRefreshMiddleware GetSessionRefreshMiddleware builds middleware logic that is executed as part of the HTTP request processing pipeline.
//
// It returns the sliding-session refresh middleware as a self-contained drop-in:
// the session-cookie response writer (which emits the Set-Cookie) wrapped around
// the refresh decision. go-common attaches it to its own route group in
// RegisterAllRoutes; consuming applications should attach the same middleware to
// their own route groups or routes so active users on application endpoints also
// keep their sessions alive. When session refresh is disabled it acts as a
// passthrough, so it is always safe to attach.
func (h *CommonAPIHandler[U]) GetSessionRefreshMiddleware() common.Middleware {
	return func(next http.Handler) http.Handler {
		return h.sessionCookieMiddleware(h.sessionRefreshMiddleware(next))
	}
}
