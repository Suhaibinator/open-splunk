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
		"nil retention": {httpAddress: "127.0.0.1:8080", tenantID: "tenant"},
		"empty tenant":  {httpAddress: "127.0.0.1:8080", tenantID: " \t", indexRetention: time.Hour},
		"oversized":     {httpAddress: "127.0.0.1:8080", tenantID: strings.Repeat("t", maximumDurableTenantIDBytes+1), indexRetention: time.Hour},
		"invalid UTF-8": {httpAddress: "127.0.0.1:8080", tenantID: string([]byte{0xff}), indexRetention: time.Hour},
		"embedded NUL":  {httpAddress: "127.0.0.1:8080", tenantID: "tenant\x00other", indexRetention: time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			if err := normalizeRuntimeOptions(&candidate); err == nil {
				t.Fatal("normalizeRuntimeOptions unexpectedly succeeded")
			}
		})
	}
}

func TestNormalizeRuntimeOptionsRequiresExplicitNonLoopbackHTTPTrust(t *testing.T) {
	t.Parallel()
	config := options{httpAddress: "192.0.2.10:8080", tenantID: "tenant", indexRetention: time.Hour}
	if err := normalizeRuntimeOptions(&config); err == nil {
		t.Fatal("non-loopback plaintext HTTP unexpectedly succeeded without explicit trust")
	}
	config.httpInsecureTrustedNetwork = true
	if err := normalizeRuntimeOptions(&config); err != nil {
		t.Fatalf("explicit trusted-network HTTP error = %v", err)
	}
	if len(config.httpAllowedHosts) != 1 || config.httpAllowedHosts[0] != "192.0.2.10" {
		t.Fatalf("derived allowed hosts = %v", config.httpAllowedHosts)
	}

	wildcard := options{
		httpAddress: "0.0.0.0:8080", httpInsecureTrustedNetwork: true,
		tenantID: "tenant", indexRetention: time.Hour,
	}
	if err := normalizeRuntimeOptions(&wildcard); err == nil {
		t.Fatal("wildcard HTTP listener unexpectedly succeeded without allowed hosts")
	}
	wildcard.httpAllowedHostsCSV = "logs.internal.example, 192.0.2.10"
	if err := normalizeRuntimeOptions(&wildcard); err != nil {
		t.Fatalf("explicit wildcard allowed hosts error = %v", err)
	}
}

func TestCollectorAuthorizerMapsCurrentTokenScopeWithoutAliasing(t *testing.T) {
	t.Parallel()
	indexes := []string{"audit", "main"}
	store := fakeCollectorAuthenticationStore{authentication: auth.Authentication{
		TokenID: "token-id", AllowedIndexNames: indexes,
	}}
	authorization, err := (collectorAuthorizer{store: store, tenantID: "tenant"}).Authorize(context.Background(), "secret")
	if err != nil {
		t.Fatal(err)
	}
	if authorization.SubjectID != "token-id" || authorization.TenantID != "tenant" || len(authorization.AuthorizedIndexes) != 2 {
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
		"empty scope":    {store: fakeCollectorAuthenticationStore{authentication: auth.Authentication{TokenID: "id"}}, tenantID: "tenant"},
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

type fakeIndexRetentionCatalog struct {
	index control.Index
	err   error
}

func (catalog fakeIndexRetentionCatalog) GetIndexByName(context.Context, string) (control.Index, error) {
	return catalog.index, catalog.err
}
