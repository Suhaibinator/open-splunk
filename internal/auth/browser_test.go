package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

const browserTestToken = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"

func TestNewBearerTokenAuthenticatorAcceptsExactBounds(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name     string
		token    string
		tenantID string
		ownerID  string
	}{
		{
			name:     "minimum",
			token:    strings.Repeat("a", MinimumBrowserBearerTokenBytes),
			tenantID: "t",
			ownerID:  "o",
		},
		{
			name:     "maximum",
			token:    strings.Repeat("z", MaximumBrowserBearerTokenBytes),
			tenantID: strings.Repeat("t", MaximumBrowserIdentityBytes),
			ownerID:  strings.Repeat("o", MaximumBrowserIdentityBytes),
		},
		{
			name:     "token68 punctuation and padding",
			token:    browserTestToken + "-._~+/==",
			tenantID: "tenant",
			ownerID:  "owner",
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			authenticator, err := NewBearerTokenAuthenticator(
				[]byte(testCase.token),
				testCase.tenantID,
				testCase.ownerID,
				BrowserRoleAdministrator,
			)
			if err != nil {
				t.Fatalf("NewBearerTokenAuthenticator(): %v", err)
			}
			principal, err := authenticator.Authenticate(
				context.Background(),
				[]byte(testCase.token),
			)
			if err != nil {
				t.Fatalf("Authenticate(): %v", err)
			}
			assertBrowserPrincipal(
				t,
				principal,
				testCase.tenantID,
				testCase.ownerID,
				BrowserRoleAdministrator,
				true,
			)
		})
	}
}

func TestValidateBrowserBearerTokenIsSyntaxOnlyAndDetailFree(t *testing.T) {
	t.Parallel()

	for _, token := range [][]byte{
		[]byte(strings.Repeat("a", MinimumBrowserBearerTokenBytes)),
		[]byte(browserTestToken),
		[]byte(strings.Repeat("z", MaximumBrowserBearerTokenBytes)),
		[]byte(browserTestToken + "-._~+/=="),
	} {
		if err := ValidateBrowserBearerToken(token); err != nil {
			t.Fatalf("ValidateBrowserBearerToken(%q): %v", token, err)
		}
	}

	for _, token := range [][]byte{
		nil,
		{},
		[]byte(strings.Repeat("a", MinimumBrowserBearerTokenBytes-1)),
		[]byte(strings.Repeat("a", MaximumBrowserBearerTokenBytes+1)),
		[]byte("Bearer " + browserTestToken),
		[]byte(browserTestToken + " "),
		[]byte("=" + browserTestToken),
		[]byte(browserTestToken + "=a"),
		append([]byte(browserTestToken), 0xff),
	} {
		err := ValidateBrowserBearerToken(token)
		if !errors.Is(err, ErrBrowserUnauthorized) {
			t.Fatalf(
				"ValidateBrowserBearerToken(%q) error = %v, want ErrBrowserUnauthorized",
				token,
				err,
			)
		}
		if err.Error() != ErrBrowserUnauthorized.Error() {
			t.Fatalf("token validation error disclosed detail: %q", err)
		}
		if len(token) != 0 && strings.Contains(err.Error(), string(token)) {
			t.Fatalf("token validation error disclosed token: %q", err)
		}
	}
}

func TestNewBearerTokenAuthenticatorRejectsInvalidSecrets(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name  string
		token []byte
	}{
		{name: "nil"},
		{name: "empty", token: []byte{}},
		{name: "short", token: []byte(strings.Repeat("a", MinimumBrowserBearerTokenBytes-1))},
		{name: "oversized", token: []byte(strings.Repeat("a", MaximumBrowserBearerTokenBytes+1))},
		{name: "authorization scheme", token: []byte("Bearer " + browserTestToken)},
		{name: "space", token: []byte(strings.Repeat("a", MinimumBrowserBearerTokenBytes) + " ")},
		{name: "control", token: []byte(strings.Repeat("a", MinimumBrowserBearerTokenBytes) + "\n")},
		{name: "non ASCII", token: []byte(strings.Repeat("a", MinimumBrowserBearerTokenBytes) + "é")},
		{name: "leading padding", token: []byte("=" + browserTestToken)},
		{name: "embedded padding", token: []byte(browserTestToken + "=a")},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			authenticator, err := NewBearerTokenAuthenticator(
				testCase.token,
				"tenant",
				"owner",
				BrowserRoleAdministrator,
			)
			if authenticator != nil {
				t.Fatalf("NewBearerTokenAuthenticator() = %#v, want nil", authenticator)
			}
			if !errors.Is(err, ErrInvalidBrowserAuthenticator) {
				t.Fatalf(
					"NewBearerTokenAuthenticator() error = %v, want ErrInvalidBrowserAuthenticator",
					err,
				)
			}
			if err.Error() != ErrInvalidBrowserAuthenticator.Error() {
				t.Fatalf("configuration error disclosed detail: %q", err)
			}
			if strings.Contains(err.Error(), string(testCase.token)) && len(testCase.token) != 0 {
				t.Fatalf("configuration error disclosed token: %q", err)
			}
		})
	}
}

func TestNewBearerTokenAuthenticatorRejectsNonCanonicalIdentityAndRole(t *testing.T) {
	t.Parallel()

	invalidUTF8 := string([]byte{0xff})
	for _, testCase := range []struct {
		name     string
		tenantID string
		ownerID  string
		role     BrowserRole
	}{
		{name: "empty tenant", ownerID: "owner", role: BrowserRoleAdministrator},
		{name: "empty owner", tenantID: "tenant", role: BrowserRoleAdministrator},
		{name: "padded tenant", tenantID: " tenant", ownerID: "owner", role: BrowserRoleAdministrator},
		{name: "padded owner", tenantID: "tenant", ownerID: "owner ", role: BrowserRoleAdministrator},
		{name: "control tenant", tenantID: "ten\nant", ownerID: "owner", role: BrowserRoleAdministrator},
		{name: "control owner", tenantID: "tenant", ownerID: "own\u007fer", role: BrowserRoleAdministrator},
		{name: "invalid UTF-8 tenant", tenantID: invalidUTF8, ownerID: "owner", role: BrowserRoleAdministrator},
		{name: "invalid UTF-8 owner", tenantID: "tenant", ownerID: invalidUTF8, role: BrowserRoleAdministrator},
		{
			name:     "oversized tenant",
			tenantID: strings.Repeat("t", MaximumBrowserIdentityBytes+1),
			ownerID:  "owner",
			role:     BrowserRoleAdministrator,
		},
		{
			name:     "oversized owner",
			tenantID: "tenant",
			ownerID:  strings.Repeat("o", MaximumBrowserIdentityBytes+1),
			role:     BrowserRoleAdministrator,
		},
		{name: "invalid role", tenantID: "tenant", ownerID: "owner", role: BrowserRoleInvalid},
		{name: "unknown role", tenantID: "tenant", ownerID: "owner", role: BrowserRole(255)},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			authenticator, err := NewBearerTokenAuthenticator(
				[]byte(browserTestToken),
				testCase.tenantID,
				testCase.ownerID,
				testCase.role,
			)
			if authenticator != nil {
				t.Fatalf("NewBearerTokenAuthenticator() = %#v, want nil", authenticator)
			}
			if !errors.Is(err, ErrInvalidBrowserAuthenticator) {
				t.Fatalf(
					"NewBearerTokenAuthenticator() error = %v, want ErrInvalidBrowserAuthenticator",
					err,
				)
			}
			for _, privateValue := range []string{testCase.tenantID, testCase.ownerID} {
				if privateValue != "" && strings.Contains(err.Error(), privateValue) {
					t.Fatalf("configuration error disclosed identity %q: %v", privateValue, err)
				}
			}
		})
	}
}

func TestBearerTokenAuthenticatorAuthenticatesAdministratorAndOrdinaryUser(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name          string
		role          BrowserRole
		administrator bool
	}{
		{name: "administrator", role: BrowserRoleAdministrator, administrator: true},
		{name: "ordinary user", role: BrowserRoleUser},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			authenticator := newBrowserTestAuthenticator(t, testCase.role)
			principal, err := authenticator.Authenticate(
				context.Background(),
				[]byte(browserTestToken),
			)
			if err != nil {
				t.Fatalf("Authenticate(): %v", err)
			}
			assertBrowserPrincipal(
				t,
				principal,
				"tenant",
				"owner",
				testCase.role,
				testCase.administrator,
			)
		})
	}

	var zero BrowserPrincipal
	if zero.Valid() || zero.IsAdministrator() || zero.Role() != BrowserRoleInvalid {
		t.Fatalf("zero BrowserPrincipal did not fail closed: %#v", zero)
	}
	if BrowserRoleInvalid.Valid() || BrowserRole(255).Valid() {
		t.Fatal("invalid browser roles reported valid")
	}
	if BrowserRoleUser.String() != "user" ||
		BrowserRoleAdministrator.String() != "administrator" ||
		BrowserRoleInvalid.String() != "invalid" ||
		BrowserRole(255).String() != "invalid" {
		t.Fatal("browser role strings are not canonical and fail-closed")
	}
}

func TestBearerTokenAuthenticatorRejectsEveryInvalidCredentialAsUnauthorized(t *testing.T) {
	t.Parallel()

	authenticator := newBrowserTestAuthenticator(t, BrowserRoleAdministrator)
	for _, testCase := range []struct {
		name  string
		token []byte
	}{
		{name: "nil"},
		{name: "empty", token: []byte{}},
		{name: "short", token: []byte("incorrect")},
		{name: "oversized", token: []byte(strings.Repeat("a", MaximumBrowserBearerTokenBytes+1))},
		{name: "wrong same length", token: []byte(strings.Repeat("x", len(browserTestToken)))},
		{name: "authorization scheme", token: []byte("Bearer " + browserTestToken)},
		{name: "malformed whitespace", token: []byte(browserTestToken + " ")},
		{name: "malformed padding", token: []byte(browserTestToken + "=x")},
		{name: "malformed binary", token: append([]byte(browserTestToken), 0xff)},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			principal, err := authenticator.Authenticate(
				context.Background(),
				testCase.token,
			)
			if principal.Valid() {
				t.Fatalf("Authenticate() principal = %#v, want invalid zero principal", principal)
			}
			if !errors.Is(err, ErrBrowserUnauthorized) {
				t.Fatalf("Authenticate() error = %v, want ErrBrowserUnauthorized", err)
			}
			if err.Error() != ErrBrowserUnauthorized.Error() {
				t.Fatalf("authentication error disclosed detail: %q", err)
			}
			if len(testCase.token) != 0 && strings.Contains(err.Error(), string(testCase.token)) {
				t.Fatalf("authentication error disclosed credential: %q", err)
			}
		})
	}
}

func TestBearerTokenAuthenticatorHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	authenticator := newBrowserTestAuthenticator(t, BrowserRoleAdministrator)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if principal, err := authenticator.Authenticate(
		canceled,
		[]byte(browserTestToken),
	); principal.Valid() || !errors.Is(err, context.Canceled) {
		t.Fatalf("Authenticate(canceled) = %#v, %v, want context.Canceled", principal, err)
	}

	expired, cancelDeadline := context.WithDeadline(
		context.Background(),
		time.Now().Add(-time.Second),
	)
	defer cancelDeadline()
	if principal, err := authenticator.Authenticate(
		expired,
		[]byte(browserTestToken),
	); principal.Valid() || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Authenticate(expired) = %#v, %v, want context.DeadlineExceeded", principal, err)
	}

	var nilContext context.Context
	if principal, err := authenticator.Authenticate(
		nilContext,
		[]byte(browserTestToken),
	); principal.Valid() || !errors.Is(err, ErrInvalidBrowserAuthenticationRequest) {
		t.Fatalf(
			"Authenticate(nil context) = %#v, %v, want ErrInvalidBrowserAuthenticationRequest",
			principal,
			err,
		)
	}
}

func TestBearerTokenAuthenticatorDetachesConfigurationAndResults(t *testing.T) {
	t.Parallel()

	token := []byte(browserTestToken)
	originalToken := append([]byte(nil), token...)
	tenantID := strings.Repeat("tenant-", 5)
	ownerID := strings.Repeat("owner-", 5)

	authenticator, err := NewBearerTokenAuthenticator(
		token,
		tenantID,
		ownerID,
		BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatalf("NewBearerTokenAuthenticator(): %v", err)
	}
	concrete := authenticator.(*bearerTokenAuthenticator)
	if concrete.tokenDigest != sha256.Sum256(originalToken) {
		t.Fatal("authenticator did not retain the exact configured token digest")
	}
	if unsafe.StringData(concrete.principal.tenantID) == unsafe.StringData(tenantID) ||
		unsafe.StringData(concrete.principal.ownerID) == unsafe.StringData(ownerID) {
		t.Fatal("authenticator retained an alias to configured identity storage")
	}

	for index := range token {
		token[index] = 'x'
	}
	first, err := authenticator.Authenticate(context.Background(), originalToken)
	if err != nil {
		t.Fatalf("Authenticate() after caller token mutation: %v", err)
	}
	second, err := authenticator.Authenticate(context.Background(), originalToken)
	if err != nil {
		t.Fatalf("Authenticate() second result: %v", err)
	}
	if unsafe.StringData(first.tenantID) == unsafe.StringData(concrete.principal.tenantID) ||
		unsafe.StringData(first.ownerID) == unsafe.StringData(concrete.principal.ownerID) ||
		unsafe.StringData(first.tenantID) == unsafe.StringData(second.tenantID) ||
		unsafe.StringData(first.ownerID) == unsafe.StringData(second.ownerID) {
		t.Fatal("Authenticate() returned identity strings aliased to retained or prior results")
	}

	firstTenant := first.TenantID()
	secondTenant := first.TenantID()
	firstOwner := first.OwnerID()
	secondOwner := first.OwnerID()
	if unsafe.StringData(firstTenant) == unsafe.StringData(first.tenantID) ||
		unsafe.StringData(firstOwner) == unsafe.StringData(first.ownerID) ||
		unsafe.StringData(firstTenant) == unsafe.StringData(secondTenant) ||
		unsafe.StringData(firstOwner) == unsafe.StringData(secondOwner) {
		t.Fatal("BrowserPrincipal accessors returned aliased identity strings")
	}
	if firstTenant != tenantID || firstOwner != ownerID {
		t.Fatalf("detached identities = (%q, %q), want (%q, %q)", firstTenant, firstOwner, tenantID, ownerID)
	}

	if principal, authenticateErr := authenticator.Authenticate(
		context.Background(),
		token,
	); principal.Valid() || !errors.Is(authenticateErr, ErrBrowserUnauthorized) {
		t.Fatalf(
			"Authenticate(mutated caller token) = %#v, %v, want ErrBrowserUnauthorized",
			principal,
			authenticateErr,
		)
	}
}

func TestBearerTokenAuthenticatorNeverFormatsCredentialMaterial(t *testing.T) {
	t.Parallel()

	authenticator := newBrowserTestAuthenticator(t, BrowserRoleAdministrator)
	concrete := authenticator.(*bearerTokenAuthenticator)
	digestHex := hex.EncodeToString(concrete.tokenDigest[:])

	jsonValue, err := json.Marshal(authenticator)
	if err != nil {
		t.Fatalf("json.Marshal(authenticator): %v", err)
	}
	renderedValues := []string{
		fmt.Sprintf("%v", authenticator),
		fmt.Sprintf("%+v", authenticator),
		fmt.Sprintf("%#v", authenticator),
		string(jsonValue),
	}
	for _, rendered := range renderedValues {
		if strings.Contains(rendered, browserTestToken) {
			t.Fatalf("formatted authenticator disclosed plaintext token: %q", rendered)
		}
		if strings.Contains(rendered, digestHex) {
			t.Fatalf("formatted authenticator disclosed credential digest: %q", rendered)
		}
	}

	privateCredential := strings.Repeat("private-credential-", 4)
	if _, err := authenticator.Authenticate(
		context.Background(),
		[]byte(privateCredential),
	); !errors.Is(err, ErrBrowserUnauthorized) ||
		strings.Contains(err.Error(), privateCredential) ||
		strings.Contains(fmt.Sprintf("%#v", err), privateCredential) {
		t.Fatalf("credential failure disclosed supplied material: %#v", err)
	}
}

func TestBearerTokenAuthenticatorIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	authenticator := newBrowserTestAuthenticator(t, BrowserRoleAdministrator)
	const workers = 64

	var waitGroup sync.WaitGroup
	errorsByWorker := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			principal, err := authenticator.Authenticate(
				context.Background(),
				[]byte(browserTestToken),
			)
			if err != nil {
				errorsByWorker <- err
				return
			}
			if !principal.IsAdministrator() ||
				principal.TenantID() != "tenant" ||
				principal.OwnerID() != "owner" {
				errorsByWorker <- errors.New("unexpected concurrent principal")
			}
		}()
	}
	waitGroup.Wait()
	close(errorsByWorker)
	for err := range errorsByWorker {
		t.Errorf("concurrent Authenticate(): %v", err)
	}
}

func TestNilBearerTokenAuthenticatorFailsClosed(t *testing.T) {
	t.Parallel()

	var authenticator *bearerTokenAuthenticator
	principal, err := authenticator.Authenticate(
		context.Background(),
		[]byte(browserTestToken),
	)
	if principal.Valid() || !errors.Is(err, ErrBrowserUnauthorized) {
		t.Fatalf("nil Authenticate() = %#v, %v, want ErrBrowserUnauthorized", principal, err)
	}
}

func newBrowserTestAuthenticator(t *testing.T, role BrowserRole) BrowserAuthenticator {
	t.Helper()

	authenticator, err := NewBearerTokenAuthenticator(
		[]byte(browserTestToken),
		"tenant",
		"owner",
		role,
	)
	if err != nil {
		t.Fatalf("NewBearerTokenAuthenticator(): %v", err)
	}
	return authenticator
}

func assertBrowserPrincipal(
	t *testing.T,
	principal BrowserPrincipal,
	tenantID string,
	ownerID string,
	role BrowserRole,
	administrator bool,
) {
	t.Helper()

	if !principal.Valid() {
		t.Fatal("BrowserPrincipal.Valid() = false, want true")
	}
	if principal.TenantID() != tenantID ||
		principal.OwnerID() != ownerID ||
		principal.Role() != role ||
		principal.IsAdministrator() != administrator {
		t.Fatalf(
			"BrowserPrincipal = (%q, %q, %s, admin=%v), want (%q, %q, %s, admin=%v)",
			principal.TenantID(),
			principal.OwnerID(),
			principal.Role(),
			principal.IsAdministrator(),
			tenantID,
			ownerID,
			role,
			administrator,
		)
	}
}
