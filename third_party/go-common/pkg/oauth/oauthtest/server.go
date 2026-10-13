// Package oauthtest provides an in-process fake OAuth 2.0 / OpenID Connect
// provider for tests. It serves discovery, JWKS (RSA and EC keys),
// /authorize, /token (with PKCE S256, client-authentication-style, and Apple
// ES256 client-secret checks), /userinfo, and GitHub-style /user and
// /user/emails endpoints over plain HTTP on a loopback listener.
package oauthtest

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"html"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"
	json "encoding/json/v2"

	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/oauth"
	"github.com/golang-jwt/jwt/v5"
)

// RedirectURL is the callback URL the fake's provider helpers allow.
const RedirectURL = "http://localhost/oauth/callback"

// Signing algorithms the fake can sign ID tokens with.
const (
	RS256 = "RS256"
	ES256 = "ES256"
)

// AppleClient makes /token require an Apple-style ES256 client-secret
// assertion instead of a static secret.
type AppleClient struct {
	TeamID string
	KeyID  string
	Key    *ecdsa.PublicKey
}

// Config describes the fake provider's expectations and the identity it
// returns. Zero values select the documented defaults.
type Config struct {
	// ClientID defaults to "test-client".
	ClientID string
	// ClientSecret is the expected static secret; empty (without Apple)
	// expects a public client that sends only client_id in the body.
	ClientSecret string
	// AuthStyle is the client authentication style /token requires.
	AuthStyle oauth.AuthStyle
	// Apple, when set, requires an ES256 client-secret assertion.
	Apple *AppleClient

	// SigningAlg is RS256 (default) or ES256.
	SigningAlg string
	// Subject defaults to "subject-1".
	Subject string
	// Claims are added to the ID token and the default /userinfo body.
	Claims map[string]jsontext.Value

	// ID-token overrides for failure tests.
	IDTokenIssuer    string        // default: the server URL
	IDTokenAudience  string        // default: ClientID
	IDTokenSubject   string        // default: Subject
	IDTokenExpiresIn time.Duration // default: 5m; negative issues an expired token
	IDTokenNonce     *string       // default: the nonce from /authorize
	OmitIDToken      bool

	// UserInfo replaces the default /userinfo body.
	UserInfo jsontext.Value
	// GitHubUser and GitHubEmails replace the default /user and /user/emails
	// bodies.
	GitHubUser   jsontext.Value
	GitHubEmails jsontext.Value

	// Delay holds /token, /userinfo, and GitHub responses.
	Delay time.Duration
	// DiscoveryFailures makes the next N discovery requests fail with 500.
	DiscoveryFailures int
}

type pendingCode struct {
	challenge   string
	nonce       string
	redirectURI string
}

type signingKeys struct {
	rsa    *rsa.PrivateKey
	rsaKID string
	ec     *ecdsa.PrivateKey
	ecKID  string
}

// Server is a running fake provider.
type Server struct {
	// URL is the issuer and base URL, such as "http://127.0.0.1:1234".
	URL string

	httpServer *httptest.Server

	mu            sync.Mutex
	cfg           Config
	keys          signingKeys
	codes         map[string]pendingCode
	accessTokens  map[string]struct{}
	lastAuthorize url.Values
	discoveryHits int
	jwksHits      int
	tokenHits     int
}

// New starts a fake provider that is closed when the test ends.
func New(t testing.TB, cfg Config) *Server {
	t.Helper()
	if cfg.ClientID == "" {
		cfg.ClientID = "test-client"
	}
	if cfg.Subject == "" {
		cfg.Subject = "subject-1"
	}
	if cfg.SigningAlg == "" {
		cfg.SigningAlg = RS256
	}
	s := &Server{
		cfg:          cfg,
		keys:         newSigningKeys(t),
		codes:        make(map[string]pendingCode),
		accessTokens: make(map[string]struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", s.handleDiscovery)
	mux.HandleFunc("GET /jwks", s.handleJWKS)
	mux.HandleFunc("GET /authorize", s.handleAuthorize)
	mux.HandleFunc("POST /token", s.handleToken)
	mux.HandleFunc("GET /userinfo", s.handleUserInfo)
	mux.HandleFunc("GET /user", s.handleGitHubUser)
	mux.HandleFunc("GET /user/emails", s.handleGitHubEmails)
	s.httpServer = httptest.NewServer(mux)
	s.URL = s.httpServer.URL
	t.Cleanup(s.httpServer.Close)
	return s
}

// Update changes the configuration under the server's lock.
func (s *Server) Update(change func(*Config)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change(&s.cfg)
}

// Config returns a copy of the current configuration.
func (s *Server) Config() Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

// RotateKeys replaces both signing keys (with new key IDs); JWKS serves only
// the new keys afterwards.
func (s *Server) RotateKeys(t testing.TB) {
	t.Helper()
	keys := newSigningKeys(t)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = keys
}

// DiscoveryRequests counts discovery document requests.
func (s *Server) DiscoveryRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.discoveryHits
}

// JWKSRequests counts key set requests.
func (s *Server) JWKSRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jwksHits
}

// TokenRequests counts token endpoint requests.
func (s *Server) TokenRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenHits
}

// LastAuthorizeQuery returns the query of the most recent /authorize request.
func (s *Server) LastAuthorizeQuery() url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAuthorize
}

// OIDCProvider returns a discovery-based Provider for this server that
// resolves identities from the ID token's standard claims.
func (s *Server) OIDCProvider(slug string) oauth.Provider {
	cfg := s.Config()
	p := s.baseProvider(slug, cfg)
	p.Issuer = s.URL
	p.Scopes = []string{"openid", "email", "profile"}
	p.Identity = oauth.OIDCIdentity(oauth.StandardClaims())
	return p
}

// OAuth2Provider returns an explicit-endpoint Provider for this server that
// resolves identities from /userinfo.
func (s *Server) OAuth2Provider(slug string) oauth.Provider {
	cfg := s.Config()
	p := s.baseProvider(slug, cfg)
	p.Endpoints = oauth.Endpoints{Authorization: s.URL + "/authorize", Token: s.URL + "/token"}
	p.Scopes = []string{"email"}
	p.Identity = oauth.UserInfoIdentity(s.URL+"/userinfo", oauth.StandardClaims())
	return p
}

func (s *Server) baseProvider(slug string, cfg Config) oauth.Provider {
	p := oauth.Provider{
		Slug:         slug,
		DisplayName:  "Test " + slug,
		ClientID:     cfg.ClientID,
		AuthStyle:    cfg.AuthStyle,
		RedirectURLs: []string{RedirectURL},
	}
	if cfg.ClientSecret != "" {
		p.ClientSecret = oauth.StaticSecret(config.NewSecret(cfg.ClientSecret))
	}
	return p
}

// Authorize performs the browser leg against authURL and returns the code
// and state the provider sends back, from either a query redirect or a
// form_post page.
func (s *Server) Authorize(t testing.TB, authURL string) (code, state string) {
	t.Helper()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Get(authURL)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusFound:
		location, err := url.Parse(resp.Header.Get("Location"))
		if err != nil {
			t.Fatalf("authorize location: %v", err)
		}
		return location.Query().Get("code"), location.Query().Get("state")
	case http.StatusOK:
		var body bytes.Buffer
		if _, err := body.ReadFrom(resp.Body); err != nil {
			t.Fatalf("authorize body: %v", err)
		}
		fields := map[string]string{}
		for _, match := range formInput.FindAllStringSubmatch(body.String(), -1) {
			fields[html.UnescapeString(match[1])] = html.UnescapeString(match[2])
		}
		return fields["code"], fields["state"]
	default:
		t.Fatalf("authorize returned %d", resp.StatusCode)
		return "", ""
	}
}

var formInput = regexp.MustCompile(`<input type="hidden" name="([^"]*)" value="([^"]*)">`)

func (s *Server) handleDiscovery(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.discoveryHits++
	fail := s.cfg.DiscoveryFailures > 0
	if fail {
		s.cfg.DiscoveryFailures--
	}
	s.mu.Unlock()
	if fail {
		http.Error(w, "discovery unavailable", http.StatusInternalServerError)
		return
	}
	writeJSON(w, discoveryDocument{
		Issuer:           s.URL,
		Authorization:    s.URL + "/authorize",
		Token:            s.URL + "/token",
		UserInfo:         s.URL + "/userinfo",
		JWKS:             s.URL + "/jwks",
		Algorithms:       []string{RS256, ES256},
		ResponseModes:    []string{"query", "form_post"},
		ChallengeMethods: []string{"S256"},
	})
}

type discoveryDocument struct {
	Issuer           string   `json:"issuer"`
	Authorization    string   `json:"authorization_endpoint"`
	Token            string   `json:"token_endpoint"`
	UserInfo         string   `json:"userinfo_endpoint"`
	JWKS             string   `json:"jwks_uri"`
	Algorithms       []string `json:"id_token_signing_alg_values_supported"`
	ResponseModes    []string `json:"response_modes_supported"`
	ChallengeMethods []string `json:"code_challenge_methods_supported"`
}

type jwk struct {
	KeyType   string `json:"kty"`
	KeyID     string `json:"kid"`
	Use       string `json:"use"`
	Algorithm string `json:"alg"`
	N         string `json:"n,omitempty"`
	E         string `json:"e,omitempty"`
	Curve     string `json:"crv,omitempty"`
	X         string `json:"x,omitempty"`
	Y         string `json:"y,omitempty"`
}

func (s *Server) handleJWKS(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.jwksHits++
	keys := s.keys
	s.mu.Unlock()
	point, err := keys.ec.PublicKey.Bytes()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	b64 := base64.RawURLEncoding.EncodeToString
	writeJSON(w, struct {
		Keys []jwk `json:"keys"`
	}{Keys: []jwk{
		{
			KeyType: "RSA", KeyID: keys.rsaKID, Use: "sig", Algorithm: RS256,
			N: b64(keys.rsa.N.Bytes()), E: b64(big.NewInt(int64(keys.rsa.E)).Bytes()),
		},
		{
			KeyType: "EC", KeyID: keys.ecKID, Use: "sig", Algorithm: ES256,
			Curve: "P-256", X: b64(point[1:33]), Y: b64(point[33:65]),
		},
	}})
}

func (s *Server) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	s.mu.Lock()
	s.lastAuthorize = query
	clientID := s.cfg.ClientID
	s.mu.Unlock()

	switch {
	case query.Get("client_id") != clientID:
		http.Error(w, "unknown client_id", http.StatusBadRequest)
		return
	case query.Get("response_type") != "code":
		http.Error(w, "response_type must be code", http.StatusBadRequest)
		return
	case query.Get("code_challenge_method") != "S256" || query.Get("code_challenge") == "":
		http.Error(w, "S256 PKCE is required", http.StatusBadRequest)
		return
	case query.Get("redirect_uri") == "" || query.Get("state") == "":
		http.Error(w, "redirect_uri and state are required", http.StatusBadRequest)
		return
	}

	code := rand.Text()
	s.mu.Lock()
	s.codes[code] = pendingCode{
		challenge:   query.Get("code_challenge"),
		nonce:       query.Get("nonce"),
		redirectURI: query.Get("redirect_uri"),
	}
	s.mu.Unlock()

	if query.Get("response_mode") == "form_post" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<form method="post" action="%s"><input type="hidden" name="code" value="%s"><input type="hidden" name="state" value="%s"></form>`,
			html.EscapeString(query.Get("redirect_uri")), html.EscapeString(code), html.EscapeString(query.Get("state")))
		return
	}
	target, err := url.Parse(query.Get("redirect_uri"))
	if err != nil {
		http.Error(w, "malformed redirect_uri", http.StatusBadRequest)
		return
	}
	values := target.Query()
	values.Set("code", code)
	values.Set("state", query.Get("state"))
	target.RawQuery = values.Encode()
	http.Redirect(w, r, target.String(), http.StatusFound)
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.tokenHits++
	cfg := s.cfg
	keys := s.keys
	s.mu.Unlock()

	if !delay(r, cfg.Delay) {
		return
	}
	if err := r.ParseForm(); err != nil {
		tokenError(w, "invalid_request", err.Error())
		return
	}
	if reason := s.checkClientAuth(r, cfg); reason != "" {
		tokenError(w, "invalid_client", reason)
		return
	}
	if r.PostForm.Get("grant_type") != "authorization_code" {
		tokenError(w, "unsupported_grant_type", "only authorization_code is supported")
		return
	}

	s.mu.Lock()
	pending, ok := s.codes[r.PostForm.Get("code")]
	delete(s.codes, r.PostForm.Get("code"))
	s.mu.Unlock()
	switch {
	case !ok:
		tokenError(w, "invalid_grant", "unknown or used code")
		return
	case pending.redirectURI != r.PostForm.Get("redirect_uri"):
		tokenError(w, "invalid_grant", "redirect_uri mismatch")
		return
	case s256(r.PostForm.Get("code_verifier")) != pending.challenge:
		tokenError(w, "invalid_grant", "PKCE verification failed")
		return
	}

	accessToken := rand.Text()
	s.mu.Lock()
	s.accessTokens[accessToken] = struct{}{}
	s.mu.Unlock()

	response := tokenResponse{AccessToken: accessToken, TokenType: "Bearer", ExpiresIn: 3600}
	if !cfg.OmitIDToken {
		idToken, err := s.signIDToken(cfg, keys, pending.nonce)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response.IDToken = idToken
	}
	writeJSON(w, response)
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	IDToken     string `json:"id_token,omitempty"`
}

// checkClientAuth returns a failure reason, or "" when the client
// authenticated the way cfg expects.
func (s *Server) checkClientAuth(r *http.Request, cfg Config) string {
	basicUser, basicSecret, hasBasic := r.BasicAuth()
	formSecret, hasFormSecret := r.PostForm["client_secret"]

	if cfg.ClientSecret == "" && cfg.Apple == nil {
		if hasBasic || hasFormSecret {
			return "public client must not send a secret"
		}
		if r.PostForm.Get("client_id") != cfg.ClientID {
			return "public client must send client_id"
		}
		return ""
	}

	var clientID, secret string
	switch cfg.AuthStyle {
	case oauth.AuthStyleInHeader:
		if !hasBasic || hasFormSecret {
			return "expected client_secret_basic"
		}
		var err error
		if clientID, err = url.QueryUnescape(basicUser); err != nil {
			return "malformed basic user"
		}
		if secret, err = url.QueryUnescape(basicSecret); err != nil {
			return "malformed basic secret"
		}
	case oauth.AuthStyleInParams:
		if hasBasic || !hasFormSecret {
			return "expected client_secret_post"
		}
		clientID, secret = r.PostForm.Get("client_id"), formSecret[0]
	default:
		return "unsupported auth style"
	}
	if clientID != cfg.ClientID {
		return "client_id mismatch"
	}
	if cfg.Apple != nil {
		return checkAppleSecret(secret, cfg)
	}
	if secret != cfg.ClientSecret {
		return "client_secret mismatch"
	}
	return ""
}

func checkAppleSecret(secret string, cfg Config) string {
	token, err := jwt.ParseWithClaims(secret, &jwt.RegisteredClaims{},
		func(*jwt.Token) (any, error) { return cfg.Apple.Key, nil },
		jwt.WithValidMethods([]string{ES256}),
		jwt.WithIssuer(cfg.Apple.TeamID),
		jwt.WithSubject(cfg.ClientID),
		jwt.WithAudience(oauth.AppleAudience),
		jwt.WithIssuedAt(),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return "Apple client secret: " + err.Error()
	}
	if kid, _ := token.Header["kid"].(string); kid != cfg.Apple.KeyID {
		return "Apple client secret kid mismatch"
	}
	return ""
}

func (s *Server) handleUserInfo(w http.ResponseWriter, r *http.Request) {
	cfg, ok := s.authorizeBearer(w, r)
	if !ok {
		return
	}
	if cfg.UserInfo != nil {
		writeRaw(w, cfg.UserInfo)
		return
	}
	claims := make(map[string]jsontext.Value, len(cfg.Claims)+1)
	for name, value := range cfg.Claims {
		claims[name] = value
	}
	claims["sub"] = String(cfg.Subject)
	writeJSON(w, claims)
}

func (s *Server) handleGitHubUser(w http.ResponseWriter, r *http.Request) {
	cfg, ok := s.authorizeGitHub(w, r)
	if !ok {
		return
	}
	if cfg.GitHubUser != nil {
		writeRaw(w, cfg.GitHubUser)
		return
	}
	id := String(cfg.Subject)
	if strings.Trim(cfg.Subject, "0123456789") == "" {
		id = Raw(cfg.Subject)
	}
	writeJSON(w, map[string]jsontext.Value{"id": id, "login": String("octocat"), "name": String("Mona Lisa")})
}

func (s *Server) handleGitHubEmails(w http.ResponseWriter, r *http.Request) {
	cfg, ok := s.authorizeGitHub(w, r)
	if !ok {
		return
	}
	if cfg.GitHubEmails != nil {
		writeRaw(w, cfg.GitHubEmails)
		return
	}
	writeRaw(w, Raw(`[{"email":"octocat@example.com","primary":true,"verified":true,"visibility":"public"}]`))
}

func (s *Server) authorizeGitHub(w http.ResponseWriter, r *http.Request) (Config, bool) {
	if r.Header.Get("Accept") != "application/vnd.github+json" || r.Header.Get("User-Agent") == "" {
		http.Error(w, "GitHub API requires Accept: application/vnd.github+json and a User-Agent", http.StatusBadRequest)
		return Config{}, false
	}
	return s.authorizeBearer(w, r)
}

func (s *Server) authorizeBearer(w http.ResponseWriter, r *http.Request) (Config, bool) {
	s.mu.Lock()
	cfg := s.cfg
	_, known := s.accessTokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
	s.mu.Unlock()
	if !known {
		http.Error(w, "invalid access token", http.StatusUnauthorized)
		return Config{}, false
	}
	return cfg, delay(r, cfg.Delay)
}

func (s *Server) signIDToken(cfg Config, keys signingKeys, nonce string) (string, error) {
	now := time.Now()
	expiresIn := cfg.IDTokenExpiresIn
	if expiresIn == 0 {
		expiresIn = 5 * time.Minute
	}
	claims := make(map[string]jsontext.Value, len(cfg.Claims)+6)
	for name, value := range cfg.Claims {
		claims[name] = value
	}
	claims["iss"] = String(firstNonEmpty(cfg.IDTokenIssuer, s.URL))
	claims["sub"] = String(firstNonEmpty(cfg.IDTokenSubject, cfg.Subject))
	claims["aud"] = String(firstNonEmpty(cfg.IDTokenAudience, cfg.ClientID))
	claims["iat"] = Int(now.Unix())
	claims["exp"] = Int(now.Add(expiresIn).Unix())
	if cfg.IDTokenNonce != nil {
		nonce = *cfg.IDTokenNonce
	}
	if nonce != "" {
		claims["nonce"] = String(nonce)
	}
	return sign(cfg.SigningAlg, keys, claims)
}

// Sign builds a compact JWT over claims with the server's current key for alg.
func (s *Server) Sign(alg string, claims map[string]jsontext.Value) (string, error) {
	s.mu.Lock()
	keys := s.keys
	s.mu.Unlock()
	return sign(alg, keys, claims)
}

func sign(alg string, keys signingKeys, claims map[string]jsontext.Value) (string, error) {
	var method jwt.SigningMethod
	var kid string
	var key crypto.Signer
	switch alg {
	case RS256:
		method, kid, key = jwt.SigningMethodRS256, keys.rsaKID, keys.rsa
	case ES256:
		method, kid, key = jwt.SigningMethodES256, keys.ecKID, keys.ec
	default:
		return "", fmt.Errorf("oauthtest: unsupported signing algorithm %q", alg)
	}
	header, err := json.Marshal(struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		Typ string `json:"typ"`
	}{alg, kid, "JWT"})
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	b64 := base64.RawURLEncoding.EncodeToString
	signingInput := b64(header) + "." + b64(payload)
	signature, err := method.Sign(signingInput, key)
	if err != nil {
		return "", err
	}
	return signingInput + "." + b64(signature), nil
}

// NewAppleKey returns a P-256 key as Apple's PKCS#8 PEM (.p8) secret and the
// public half for Config.Apple.
func NewAppleKey(t testing.TB) (config.Secret, *ecdsa.PublicKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate Apple key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal Apple key: %v", err)
	}
	return config.NewSecret(string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))), &key.PublicKey
}

// String, Bool, Int, and Raw build claim values.
func String(s string) jsontext.Value { return mustMarshal(s) }
func Bool(b bool) jsontext.Value     { return mustMarshal(b) }
func Int(n int64) jsontext.Value     { return mustMarshal(n) }
func Raw(s string) jsontext.Value    { return jsontext.Value(s) }

func mustMarshal[T string | bool | int64](v T) jsontext.Value {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func newSigningKeys(t testing.TB) signingKeys {
	t.Helper()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate EC key: %v", err)
	}
	return signingKeys{rsa: rsaKey, rsaKID: "rsa-" + rand.Text(), ec: ecKey, ecKID: "ec-" + rand.Text()}
}

func s256(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func delay(r *http.Request, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	select {
	case <-time.After(d):
		return true
	case <-r.Context().Done():
		return false
	}
}

func tokenError(w http.ResponseWriter, code, description string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.MarshalWrite(w, struct {
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}{code, description})
}

func writeJSON[T any](w http.ResponseWriter, value T) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.MarshalWrite(w, value)
}

func writeRaw(w http.ResponseWriter, body jsontext.Value) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
