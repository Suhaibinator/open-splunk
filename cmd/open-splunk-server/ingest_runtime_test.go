package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

func TestNormalizeRuntimeOptionsCanonicalizesAndBoundsTenantIdentity(t *testing.T) {
	t.Parallel()
	config := options{httpAddress: "127.0.0.1:8080", tenantID: " tenant ", indexRetention: time.Hour}
	if err := normalizeRuntimeOptions(&config); err != nil {
		t.Fatalf("normalizeRuntimeOptions() error = %v", err)
	}
	if config.tenantID != "tenant" {
		t.Fatalf("normalized tenant ID = %q, want tenant", config.tenantID)
	}
	for name, candidate := range map[string]options{
		"nil retention":             {httpAddress: "127.0.0.1:8080", tenantID: "tenant"},
		"sub-millisecond retention": {httpAddress: "127.0.0.1:8080", tenantID: "tenant", indexRetention: time.Nanosecond},
		"empty tenant":              {httpAddress: "127.0.0.1:8080", tenantID: " \t", indexRetention: time.Hour},
		"oversized":                 {httpAddress: "127.0.0.1:8080", tenantID: strings.Repeat("t", maximumDurableTenantIDBytes+1), indexRetention: time.Hour},
		"invalid UTF-8":             {httpAddress: "127.0.0.1:8080", tenantID: string([]byte{0xff}), indexRetention: time.Hour},
		"embedded NUL":              {httpAddress: "127.0.0.1:8080", tenantID: "tenant\x00other", indexRetention: time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			if err := normalizeRuntimeOptions(&candidate); err == nil {
				t.Fatal("normalizeRuntimeOptions unexpectedly succeeded")
			}
		})
	}
}

func TestNormalizeRuntimeOptionsRequiresLoopbackHTTPForAdministratorRoutes(t *testing.T) {
	t.Parallel()
	config := options{httpAddress: "192.0.2.10:8080", tenantID: "tenant", indexRetention: time.Hour}
	if err := normalizeRuntimeOptions(&config); err == nil {
		t.Fatal("non-loopback plaintext HTTP unexpectedly succeeded")
	}
	config.httpInsecureTrustedNetwork = true
	if err := normalizeRuntimeOptions(&config); err == nil {
		t.Fatal("trusted-network override bypassed the administrator loopback boundary")
	}

	wildcard := options{
		httpAddress: "0.0.0.0:8080", httpInsecureTrustedNetwork: true,
		tenantID: "tenant", indexRetention: time.Hour,
	}
	if err := normalizeRuntimeOptions(&wildcard); err == nil {
		t.Fatal("wildcard HTTP listener unexpectedly succeeded")
	}
	wildcard.httpAllowedHostsCSV = "logs.internal.example, 192.0.2.10"
	if err := normalizeRuntimeOptions(&wildcard); err == nil {
		t.Fatal("allowed hosts bypassed the administrator loopback boundary")
	}
}

func TestCollectorAuthorizerMapsCurrentTokenScopeWithoutAliasing(t *testing.T) {
	t.Parallel()
	indexes := []string{"audit", "main"}
	store := fakeCollectorAuthenticationStore{authentication: auth.Authentication{
		TokenID: "token-id", BoundCollectorID: "collector-id", AllowedIndexNames: indexes,
	}}
	authorization, err := (collectorAuthorizer{store: store, tenantID: "tenant"}).Authorize(context.Background(), "secret")
	if err != nil {
		t.Fatal(err)
	}
	if authorization.SubjectID != "token-id" || authorization.TenantID != "tenant" ||
		authorization.CollectorID != "collector-id" || len(authorization.AuthorizedIndexes) != 2 {
		t.Fatalf("authorization = %+v", authorization)
	}
	authorization.AuthorizedIndexes[0] = "changed"
	if indexes[0] != "audit" {
		t.Fatal("authorization aliases authentication scope")
	}
}

func TestCollectorAuthorizerRejectsMalformedOrFailedAuthentication(t *testing.T) {
	t.Parallel()
	denied := errors.New("denied")
	for name, authorizer := range map[string]collectorAuthorizer{
		"missing store":  {tenantID: "tenant"},
		"missing tenant": {store: fakeCollectorAuthenticationStore{}},
		"store error":    {store: fakeCollectorAuthenticationStore{err: denied}, tenantID: "tenant"},
		"empty binding": {
			store: fakeCollectorAuthenticationStore{authentication: auth.Authentication{
				TokenID: "id", AllowedIndexNames: []string{"main"},
			}},
			tenantID: "tenant",
		},
		"empty scope": {
			store: fakeCollectorAuthenticationStore{authentication: auth.Authentication{
				TokenID: "id", BoundCollectorID: "collector-id",
			}},
			tenantID: "tenant",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := authorizer.Authorize(context.Background(), "secret"); err == nil {
				t.Fatal("Authorize succeeded")
			}
		})
	}
}

func TestCollectorAuthorizerClassifiesOnlyCredentialFailuresAsUnauthorized(t *testing.T) {
	t.Parallel()
	authorizer := collectorAuthorizer{
		store: fakeCollectorAuthenticationStore{err: auth.ErrUnauthorized}, tenantID: "tenant",
	}
	if _, err := authorizer.Authorize(context.Background(), "bad"); !errors.Is(err, ingest.ErrUnauthorized) {
		t.Fatalf("credential error = %v, want ingest.ErrUnauthorized", err)
	}
	backendErr := errors.New("sqlite unavailable")
	authorizer.store = fakeCollectorAuthenticationStore{err: backendErr}
	if _, err := authorizer.Authorize(context.Background(), "token"); !errors.Is(err, backendErr) || errors.Is(err, ingest.ErrUnauthorized) {
		t.Fatalf("backend error classification = %v", err)
	}
}

func TestCollectorTokenUseRecorderPassesSafeAdmissionMetadata(t *testing.T) {
	t.Parallel()
	acceptedAt := time.Date(2026, 7, 28, 12, 34, 56, 123_456_000, time.UTC)
	store := &fakeCollectorTokenUseStore{}
	recorder := collectorTokenUseRecorder{store: store}

	if err := recorder.RecordCollectorTokenUse(context.Background(), "token-safe-id", acceptedAt); err != nil {
		t.Fatalf("RecordCollectorTokenUse() error = %v", err)
	}
	if store.calls != 1 || store.tokenID != "token-safe-id" || !store.acceptedAt.Equal(acceptedAt) {
		t.Fatalf(
			"recorded use = calls:%d token:%q at:%v",
			store.calls,
			store.tokenID,
			store.acceptedAt,
		)
	}
}

func TestCollectorTokenUseRecorderClassifiesOnlyInactiveTokensAsUnauthorized(t *testing.T) {
	t.Parallel()
	recorder := collectorTokenUseRecorder{}
	if err := recorder.RecordCollectorTokenUse(context.Background(), "token-id", time.Now()); err == nil ||
		errors.Is(err, ingest.ErrUnauthorized) {
		t.Fatalf("missing-store error = %v, want operational error", err)
	}

	store := &fakeCollectorTokenUseStore{}
	recorder.store = store
	for _, inactiveErr := range []error{
		auth.ErrUnauthorized,
		auth.ErrInactiveToken,
	} {
		store.err = inactiveErr
		if err := recorder.RecordCollectorTokenUse(context.Background(), "token-id", time.Now()); !errors.Is(err, ingest.ErrUnauthorized) {
			t.Fatalf("inactive-token error = %v, want ingest.ErrUnauthorized", err)
		}
	}

	backendErr := errors.New("sqlite unavailable")
	store.err = backendErr
	if err := recorder.RecordCollectorTokenUse(context.Background(), "token-id", time.Now()); !errors.Is(err, backendErr) ||
		errors.Is(err, ingest.ErrUnauthorized) {
		t.Fatalf("backend error classification = %v", err)
	}
}

func TestControlRetentionProviderRequiresOwnedActiveIngestionIndex(t *testing.T) {
	t.Parallel()
	period := 30 * 24 * time.Hour
	catalog := fakeIndexRetentionCatalog{index: control.Index{
		State: control.IndexStateActive,
		Definition: control.IndexDefinition{
			Name: "main", IngestionEnabled: true, RetentionPeriod: period,
		},
	}}
	provider := controlRetentionProvider{catalog: catalog, tenantID: "tenant"}
	got, err := provider.RetentionForIndex(context.Background(), "tenant", "main")
	if err != nil || got != period {
		t.Fatalf("RetentionForIndex = (%v, %v), want (%v, nil)", got, err, period)
	}
	if _, err := provider.RetentionForIndex(context.Background(), "other", "main"); err == nil {
		t.Fatal("cross-tenant retention lookup succeeded")
	}
	catalog.index.Definition.IngestionEnabled = false
	provider.catalog = catalog
	if _, err := provider.RetentionForIndex(context.Background(), "tenant", "main"); err == nil {
		t.Fatal("disabled index retention lookup succeeded")
	}
	catalog.index.Definition.IngestionEnabled = true
	catalog.index.Definition.RetentionPeriod = 0
	provider.catalog = catalog
	provider.defaultRetention = 7 * 24 * time.Hour
	if got, err := provider.RetentionForIndex(context.Background(), "tenant", "main"); err != nil || got != provider.defaultRetention {
		t.Fatalf("default retention lookup = (%v, %v)", got, err)
	}
	provider.defaultRetention = 0
	if _, err := provider.RetentionForIndex(context.Background(), "tenant", "main"); err == nil {
		t.Fatal("zero retention without a deployment default succeeded")
	}
}

type fakeCollectorAuthenticationStore struct {
	authentication auth.Authentication
	err            error
}

func (store fakeCollectorAuthenticationStore) Authenticate(context.Context, string) (auth.Authentication, error) {
	return store.authentication, store.err
}

type fakeCollectorTokenUseStore struct {
	tokenID    string
	acceptedAt time.Time
	calls      int
	err        error
}

func (store *fakeCollectorTokenUseStore) RecordCollectorTokenUse(
	_ context.Context,
	tokenID string,
	acceptedAt time.Time,
) error {
	store.calls++
	store.tokenID = tokenID
	store.acceptedAt = acceptedAt
	return store.err
}

type fakeIndexRetentionCatalog struct {
	index control.Index
	err   error
}

func (catalog fakeIndexRetentionCatalog) GetIndexByName(context.Context, string) (control.Index, error) {
	return catalog.index, catalog.err
}
