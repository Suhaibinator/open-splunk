package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
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
		"retention past horizon":    {httpAddress: "127.0.0.1:8080", tenantID: "tenant", indexRetention: 8_000_000_000 * time.Second},
		"empty tenant":              {httpAddress: "127.0.0.1:8080", tenantID: " \t", indexRetention: time.Hour},
		"oversized":                 {httpAddress: "127.0.0.1:8080", tenantID: strings.Repeat("t", maximumDurableTenantIDBytes+1), indexRetention: time.Hour},
		"invalid UTF-8":             {httpAddress: "127.0.0.1:8080", tenantID: string([]byte{0xff}), indexRetention: time.Hour},
		"embedded NUL":              {httpAddress: "127.0.0.1:8080", tenantID: "tenant\x00other", indexRetention: time.Hour},
		"embedded newline":          {httpAddress: "127.0.0.1:8080", tenantID: "tenant\nother", indexRetention: time.Hour},
		"embedded C1 control":       {httpAddress: "127.0.0.1:8080", tenantID: "tenant\u0080other", indexRetention: time.Hour},
		"trailing newline":          {httpAddress: "127.0.0.1:8080", tenantID: "tenant\n", indexRetention: time.Hour},
		"leading C1 control":        {httpAddress: "127.0.0.1:8080", tenantID: "\u0085tenant", indexRetention: time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			if err := normalizeRuntimeOptions(&candidate); err == nil {
				t.Fatal("normalizeRuntimeOptions unexpectedly succeeded")
			}
		})
	}
}

func TestNormalizeRuntimeOptionsAllowsPlaintextNonLoopbackAdministratorRoutes(t *testing.T) {
	t.Parallel()
	config := options{httpAddress: "192.0.2.10:8080", tenantID: "tenant", indexRetention: time.Hour}
	if err := normalizeRuntimeOptions(&config); err != nil {
		t.Fatalf("non-loopback plaintext HTTP rejected: %v", err)
	}
	if len(config.httpAllowedHosts) != 1 || config.httpAllowedHosts[0] != "192.0.2.10" {
		t.Fatalf("default plaintext allowed hosts = %v", config.httpAllowedHosts)
	}

	wildcard := options{
		httpAddress: "0.0.0.0:8080",
		tenantID:    "tenant", indexRetention: time.Hour,
	}
	if err := normalizeRuntimeOptions(&wildcard); err == nil {
		t.Fatal("wildcard HTTP listener unexpectedly succeeded")
	}
	wildcard.httpAllowedHostsCSV = "logs.internal.example, 192.0.2.10"
	if err := normalizeRuntimeOptions(&wildcard); err != nil {
		t.Fatalf("explicitly scoped wildcard plaintext listener rejected: %v", err)
	}

	secure := options{
		httpAddress:    "192.0.2.10:8443",
		httpTLSCert:    " server.crt ",
		httpTLSKey:     " server.key ",
		tenantID:       "tenant",
		indexRetention: time.Hour,
	}
	if err := normalizeRuntimeOptions(&secure); err != nil {
		t.Fatalf("non-loopback HTTPS rejected: %v", err)
	}
	if secure.httpTLSCert != "server.crt" || secure.httpTLSKey != "server.key" {
		t.Fatalf(
			"normalized HTTPS certificate paths = (%q, %q)",
			secure.httpTLSCert,
			secure.httpTLSKey,
		)
	}
	if len(secure.httpAllowedHosts) != 1 || secure.httpAllowedHosts[0] != "192.0.2.10" {
		t.Fatalf("default HTTPS allowed hosts = %v", secure.httpAllowedHosts)
	}

	secureWildcard := options{
		httpAddress:    "0.0.0.0:8443",
		httpTLSCert:    "server.crt",
		httpTLSKey:     "server.key",
		tenantID:       "tenant",
		indexRetention: time.Hour,
	}
	if err := normalizeRuntimeOptions(&secureWildcard); err == nil {
		t.Fatal("wildcard HTTPS listener without allowed hosts unexpectedly succeeded")
	}
	secureWildcard.httpAllowedHostsCSV = "logs.internal.example, 192.0.2.10"
	if err := normalizeRuntimeOptions(&secureWildcard); err != nil {
		t.Fatalf("explicitly scoped wildcard HTTPS listener rejected: %v", err)
	}
	if got := strings.Join(secureWildcard.httpAllowedHosts, ","); got != "logs.internal.example,192.0.2.10" {
		t.Fatalf("wildcard HTTPS allowed hosts = %q", got)
	}
}

func TestNormalizeRuntimeOptionsRejectsIncompleteHTTPTLSIdentity(t *testing.T) {
	t.Parallel()
	for name, candidate := range map[string]options{
		"certificate only": {
			httpAddress: "127.0.0.1:8443",
			httpTLSCert: "server.crt",
		},
		"key only": {
			httpAddress: "127.0.0.1:8443",
			httpTLSKey:  "server.key",
		},
	} {
		candidate.tenantID = "tenant"
		candidate.indexRetention = time.Hour
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := normalizeRuntimeOptions(&candidate); err == nil {
				t.Fatal("incomplete HTTP TLS identity unexpectedly succeeded")
			}
		})
	}
}

func TestCollectorAuthorizerMapsCurrentTokenScopeWithoutAliasing(t *testing.T) {
	t.Parallel()
	indexes := []auth.AuthorizedIndexPolicy{
		runtimeAuthorizedIndexPolicy("audit", 7*24*time.Hour),
		runtimeAuthorizedIndexPolicy("main", 30*24*time.Hour),
	}
	store := fakeCollectorAuthenticationStore{authentication: auth.Authentication{
		TokenID: "token-id", BoundCollectorID: "collector-id", AuthorizedIndexes: indexes,
		AllowedHostRegexes:   []string{`^host$`},
		AllowedSourceRegexes: []string{`^/var/log/app\.log$`},
	}}
	authorization, err := (collectorAuthorizer{store: store, tenantID: "tenant"}).Authorize(context.Background(), "secret")
	if err != nil {
		t.Fatal(err)
	}
	if authorization.SubjectID != "token-id" || authorization.TenantID != "tenant" ||
		authorization.CollectorID != "collector-id" || len(authorization.AuthorizedIndexes) != 2 ||
		len(authorization.AllowedHostRegexes) != 1 ||
		len(authorization.AllowedSourceRegexes) != 1 ||
		authorization.AllowedHostRegexes[0] != `^host$` ||
		authorization.AllowedSourceRegexes[0] != `^/var/log/app\.log$` {
		t.Fatalf("authorization = %+v", authorization)
	}
	if got := authorization.AuthorizedIndexes[0]; got.Name != "audit" ||
		got.Version != 1 || got.RetentionPeriod != 7*24*time.Hour ||
		got.DefaultSourcetype != "audit:json" || got.Limits.MaxFieldCount != 17 {
		t.Fatalf("mapped index policy = %+v", got)
	}
	authorization.AuthorizedIndexes[0].Name = "changed"
	authorization.AllowedHostRegexes[0] = "changed"
	authorization.AllowedSourceRegexes[0] = "changed"
	if indexes[0].Name != "audit" {
		t.Fatal("authorization aliases authentication scope")
	}
	if store.authentication.AllowedHostRegexes[0] != `^host$` ||
		store.authentication.AllowedSourceRegexes[0] != `^/var/log/app\.log$` {
		t.Fatal("authorization aliases authentication event constraints")
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
				TokenID: "id", AuthorizedIndexes: []auth.AuthorizedIndexPolicy{
					runtimeAuthorizedIndexPolicy("main", time.Hour),
				},
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

func TestCollectorSessionManagerAdmitsAndDetachesFreshAuthority(t *testing.T) {
	t.Parallel()
	acceptedAt := time.Date(2026, 7, 28, 12, 34, 56, 123_456_000, time.UTC)
	indexes := []auth.AuthorizedIndexPolicy{
		runtimeAuthorizedIndexPolicy("audit", 7*24*time.Hour),
		runtimeAuthorizedIndexPolicy("main", 30*24*time.Hour),
	}
	hosts := []string{`^host$`}
	sources := []string{`^/var/log/app\.log$`}
	lease := collectorfleet.Lease{
		TenantID:    "tenant-a",
		CollectorID: "collector-a",
		BootEpoch:   "boot-a",
		StreamID:    "stream-a",
		Generation:  7,
	}
	store := &fakeCollectorAdmissionRuntimeStore{
		admitResult: collectoradmission.Result{
			Authentication: auth.Authentication{
				TokenID:              "token-safe-id",
				BoundCollectorID:     "collector-a",
				AuthorizedIndexes:    indexes,
				AllowedHostRegexes:   hosts,
				AllowedSourceRegexes: sources,
			},
			Lease: lease,
		},
	}
	manager := collectorSessionManager{admission: store}
	request := ingest.CollectorSessionAdmissionRequest{
		CollectorID: "collector-a",
		BootEpoch:   "boot-a",
		StreamID:    "stream-a",
		AcceptedAt:  acceptedAt,
		Hello: collectorfleet.Hello{
			InstanceID: "instance-a",
		},
	}
	got, err := manager.Admit(context.Background(), "private-bearer", request)
	if err != nil {
		t.Fatal(err)
	}
	if store.admitBearer != "private-bearer" ||
		store.admitRequest.CollectorID != request.CollectorID ||
		!store.admitRequest.AcceptedAt.Equal(acceptedAt) {
		t.Fatalf("forwarded admission = bearer:%q request:%+v", store.admitBearer, store.admitRequest)
	}
	if got.Lease != lease ||
		got.Authorization.SubjectID != "token-safe-id" ||
		got.Authorization.TenantID != "tenant-a" ||
		got.Authorization.CollectorID != "collector-a" ||
		len(got.Authorization.AllowedHostRegexes) != 1 ||
		len(got.Authorization.AllowedSourceRegexes) != 1 ||
		got.Authorization.AllowedHostRegexes[0] != `^host$` ||
		got.Authorization.AllowedSourceRegexes[0] != `^/var/log/app\.log$` {
		t.Fatalf("admission = %+v", got)
	}
	got.Authorization.AuthorizedIndexes[0].Name = "changed"
	got.Authorization.AllowedHostRegexes[0] = "changed"
	got.Authorization.AllowedSourceRegexes[0] = "changed"
	if indexes[0].Name != "audit" {
		t.Fatal("admission authorization aliases persistence result")
	}
	if hosts[0] != `^host$` || sources[0] != `^/var/log/app\.log$` {
		t.Fatal("admission event constraints alias persistence result")
	}
}

func TestCollectorSessionManagerPreservesInvalidEventAuthorityForDurableReplay(t *testing.T) {
	t.Parallel()

	lease := collectorfleet.Lease{
		TenantID:    "tenant-a",
		CollectorID: "collector-a",
		BootEpoch:   "boot-a",
		StreamID:    "stream-a",
		Generation:  7,
	}
	hosts := []string{`(`}
	sources := []string{`^/var/log/app\.log$`}
	store := &fakeCollectorAdmissionRuntimeStore{
		authorizeAuthentication: auth.Authentication{
			TokenID:              "token-safe-id",
			BoundCollectorID:     "collector-a",
			AllowedHostRegexes:   hosts,
			AllowedSourceRegexes: sources,
		},
		authorizeErr: auth.ErrInvalidEventAuthority,
	}
	manager := collectorSessionManager{admission: store}
	checkedAt := time.Date(2026, 7, 28, 12, 45, 0, 0, time.UTC)
	got, err := manager.AuthorizeLease(
		context.Background(),
		"private-bearer",
		lease,
		checkedAt,
	)
	if !errors.Is(err, ingest.ErrInvalidEventAuthority) {
		t.Fatalf("AuthorizeLease() error = %v, want ErrInvalidEventAuthority", err)
	}
	if got.SubjectID != "token-safe-id" || got.TenantID != "tenant-a" ||
		got.CollectorID != "collector-a" || len(got.AllowedHostRegexes) != 1 ||
		len(got.AllowedSourceRegexes) != 1 || got.AllowedHostRegexes[0] != `(` ||
		got.AllowedSourceRegexes[0] != `^/var/log/app\.log$` {
		t.Fatalf("deferred event authority = %+v", got)
	}
	if store.authorizeBearer != "private-bearer" ||
		store.authorizeLease != lease || !store.authorizeCheckedAt.Equal(checkedAt) {
		t.Fatalf("forwarded lease authorization = %+v", store)
	}
	got.AllowedHostRegexes[0] = "changed"
	got.AllowedSourceRegexes[0] = "changed"
	if hosts[0] != `(` || sources[0] != `^/var/log/app\.log$` {
		t.Fatal("deferred event authority aliases persistence result")
	}
}

func TestCollectorSessionManagerMapsOnlyKnownBoundaryErrors(t *testing.T) {
	t.Parallel()
	backendErr := errors.New("sqlite unavailable")
	tests := []struct {
		name          string
		err           error
		want          error
		knownBoundary bool
	}{
		{
			name:          "credential",
			err:           auth.ErrUnauthorized,
			want:          ingest.ErrUnauthorized,
			knownBoundary: true,
		},
		{
			name:          "inactive",
			err:           auth.ErrInactiveToken,
			want:          ingest.ErrUnauthorized,
			knownBoundary: true,
		},
		{
			name:          "invalid event authority",
			err:           auth.ErrInvalidEventAuthority,
			want:          ingest.ErrInvalidEventAuthority,
			knownBoundary: true,
		},
		{
			name:          "lease",
			err:           collectoradmission.ErrLeaseNotCurrent,
			want:          ingest.ErrCollectorLeaseNotCurrent,
			knownBoundary: true,
		},
		{
			name:          "heartbeat lease",
			err:           collectorfleet.ErrHeartbeatLeaseNotActive,
			want:          ingest.ErrCollectorLeaseNotCurrent,
			knownBoundary: true,
		},
		{
			name:          "disabled",
			err:           collectorfleet.ErrCollectorDisabled,
			want:          ingest.ErrCollectorDisabled,
			knownBoundary: true,
		},
		{
			name:          "invalid",
			err:           control.ErrInvalidArgument,
			want:          ingest.ErrInvalidCollectorSnapshot,
			knownBoundary: true,
		},
		{
			name:          "capacity",
			err:           control.ErrCapacityExceeded,
			want:          ingest.ErrCollectorSessionCapacity,
			knownBoundary: true,
		},
		{name: "backend", err: backendErr, want: backendErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapped := mapCollectorSessionError(test.err)
			if !errors.Is(mapped, test.want) {
				t.Fatalf("mapped error = %v, want %v", mapped, test.want)
			}
			if test.knownBoundary && errors.Is(mapped, backendErr) {
				t.Fatalf("known error mapped as backend failure: %v", mapped)
			}
		})
	}
}

func TestCollectorSessionManagerDelegatesHeartbeatLifecycle(t *testing.T) {
	t.Parallel()
	lease := collectorfleet.Lease{
		TenantID:    "tenant-a",
		CollectorID: "collector-a",
		BootEpoch:   "boot-a",
		StreamID:    "stream-a",
		Generation:  3,
	}
	heartbeat := collectorfleet.Heartbeat{ObservationSequence: 7}
	runtime := &fakeCollectorHeartbeatRuntime{offerAccepted: true}
	manager := collectorSessionManager{heartbeats: runtime}

	if err := manager.Activate(lease); err != nil {
		t.Fatalf("Activate(): %v", err)
	}
	accepted, err := manager.RecordHeartbeat(
		context.Background(),
		lease,
		heartbeat,
	)
	if err != nil || !accepted {
		t.Fatalf("RecordHeartbeat() = (%t, %v), want (true, nil)", accepted, err)
	}
	if runtime.activatedLease != lease ||
		runtime.offeredLease != lease ||
		runtime.offeredHeartbeat.ObservationSequence !=
			heartbeat.ObservationSequence {
		t.Fatalf("heartbeat runtime calls = %#v", runtime)
	}
}

func TestCollectorSessionManagerAlwaysDurablyDisconnectsAfterReleaseFailure(
	t *testing.T,
) {
	t.Parallel()
	releaseErr := errors.New("release failed")
	disconnectErr := errors.New("durable disconnect failed")
	lease := collectorfleet.Lease{
		TenantID:    "tenant-a",
		CollectorID: "collector-a",
		BootEpoch:   "boot-a",
		StreamID:    "stream-a",
		Generation:  9,
	}
	receivedAt := time.Date(2026, 7, 28, 13, 0, 0, 0, time.UTC)
	calls := make([]string, 0, 2)
	runtime := &fakeCollectorHeartbeatRuntime{
		releaseErr: releaseErr,
		calls:      &calls,
	}
	fleet := &fakeCollectorFleetRuntimeStore{
		disconnectApplied: true,
		disconnectErr:     disconnectErr,
		calls:             &calls,
	}
	manager := collectorSessionManager{
		fleet:      fleet,
		heartbeats: runtime,
	}

	applied, err := manager.Disconnect(
		context.Background(),
		lease,
		receivedAt,
	)
	if !applied ||
		!errors.Is(err, releaseErr) ||
		!errors.Is(err, disconnectErr) {
		t.Fatalf(
			"Disconnect() = (%t, %v), want applied and joined errors",
			applied,
			err,
		)
	}
	if len(calls) != 2 || calls[0] != "release" || calls[1] != "disconnect" {
		t.Fatalf("disconnect call order = %v, want [release disconnect]", calls)
	}
	if runtime.releasedLease != lease ||
		fleet.disconnectedLease != lease ||
		!fleet.disconnectedAt.Equal(receivedAt) {
		t.Fatalf(
			"disconnect arguments = runtime:%#v fleet:%#v",
			runtime,
			fleet,
		)
	}
}

func TestCollectorSessionManagerMissingHeartbeatRuntimeStillDisconnectsDurably(
	t *testing.T,
) {
	t.Parallel()
	fleet := &fakeCollectorFleetRuntimeStore{disconnectApplied: true}
	manager := collectorSessionManager{fleet: fleet}
	applied, err := manager.Disconnect(
		context.Background(),
		collectorfleet.Lease{
			TenantID:    "tenant-a",
			CollectorID: "collector-a",
			BootEpoch:   "boot-a",
			StreamID:    "stream-a",
			Generation:  1,
		},
		time.Now().UTC(),
	)
	if !applied || err == nil || fleet.disconnectCalls != 1 {
		t.Fatalf(
			"Disconnect() = (%t, %v), durable calls = %d",
			applied,
			err,
			fleet.disconnectCalls,
		)
	}
}

func TestCollectorSessionManagerReservesParentContextForDurableDisconnect(
	t *testing.T,
) {
	t.Parallel()
	runtime := &fakeCollectorHeartbeatRuntime{
		waitForReleaseContext: true,
	}
	fleet := &fakeCollectorFleetRuntimeStore{disconnectApplied: true}
	manager := collectorSessionManager{
		fleet:                   fleet,
		heartbeats:              runtime,
		heartbeatReleaseTimeout: 5 * time.Millisecond,
	}
	parentContext, cancelParent := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancelParent()
	parentDeadline, parentHasDeadline := parentContext.Deadline()
	if !parentHasDeadline {
		t.Fatal("parent disconnect context has no deadline")
	}

	applied, err := manager.Disconnect(
		parentContext,
		collectorfleet.Lease{
			TenantID:    "tenant-a",
			CollectorID: "collector-a",
			BootEpoch:   "boot-a",
			StreamID:    "stream-a",
			Generation:  1,
		},
		time.Now().UTC(),
	)
	if !applied || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf(
			"Disconnect() = (%t, %v), want applied/deadline",
			applied,
			err,
		)
	}
	if fleet.disconnectCalls != 1 || fleet.disconnectContextErr != nil {
		t.Fatalf(
			"durable disconnect = calls:%d context:%v",
			fleet.disconnectCalls,
			fleet.disconnectContextErr,
		)
	}
	if !runtime.releaseHasDeadline ||
		!fleet.disconnectHasDeadline ||
		!runtime.releaseDeadline.Before(fleet.disconnectDeadline) ||
		!fleet.disconnectDeadline.Equal(parentDeadline) {
		t.Fatalf(
			"disconnect deadlines = release:%v/%t durable:%v/%t parent:%v",
			runtime.releaseDeadline,
			runtime.releaseHasDeadline,
			fleet.disconnectDeadline,
			fleet.disconnectHasDeadline,
			parentDeadline,
		)
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
	catalog.index.Definition.RetentionPeriod = 8_000_000_000 * time.Second
	provider.catalog = catalog
	if _, err := provider.RetentionForIndex(context.Background(), "tenant", "main"); err == nil {
		t.Fatal("retention past the storage horizon succeeded")
	}
	catalog.index.Definition.RetentionPeriod = period
	provider.catalog = catalog
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

func runtimeAuthorizedIndexPolicy(
	name string,
	retention time.Duration,
) auth.AuthorizedIndexPolicy {
	return auth.AuthorizedIndexPolicy{
		Name:              name,
		Version:           1,
		RetentionPeriod:   retention,
		DefaultSourcetype: name + ":json",
		Limits: control.IndexLimits{
			MaxEventBytes:     4096,
			MaxFieldCount:     17,
			MaxNestingDepth:   4,
			MaximumFutureSkew: time.Minute,
			MaximumEventAge:   24 * time.Hour,
		},
	}
}

func (store fakeCollectorAuthenticationStore) Authenticate(context.Context, string) (auth.Authentication, error) {
	return store.authentication, store.err
}

type fakeCollectorAdmissionRuntimeStore struct {
	admitBearer             string
	admitRequest            collectoradmission.Request
	admitResult             collectoradmission.Result
	admitErr                error
	authorizeBearer         string
	authorizeLease          collectorfleet.Lease
	authorizeCheckedAt      time.Time
	authorizeAuthentication auth.Authentication
	authorizeErr            error
}

func (store *fakeCollectorAdmissionRuntimeStore) Admit(
	_ context.Context,
	bearer string,
	request collectoradmission.Request,
) (collectoradmission.Result, error) {
	store.admitBearer = bearer
	store.admitRequest = request
	return store.admitResult, store.admitErr
}

func (store *fakeCollectorAdmissionRuntimeStore) AuthorizeLease(
	_ context.Context,
	bearer string,
	lease collectorfleet.Lease,
	checkedAt time.Time,
) (auth.Authentication, error) {
	store.authorizeBearer = bearer
	store.authorizeLease = lease
	store.authorizeCheckedAt = checkedAt
	if store.authorizeAuthentication.TokenID == "" && store.authorizeErr == nil {
		return auth.Authentication{}, errors.New("unexpected lease authorization")
	}
	return store.authorizeAuthentication, store.authorizeErr
}

type fakeCollectorHeartbeatRuntime struct {
	activatedLease        collectorfleet.Lease
	activateErr           error
	offeredLease          collectorfleet.Lease
	offeredHeartbeat      collectorfleet.Heartbeat
	offerAccepted         bool
	offerErr              error
	releasedLease         collectorfleet.Lease
	releaseErr            error
	waitForReleaseContext bool
	releaseDeadline       time.Time
	releaseHasDeadline    bool
	calls                 *[]string
}

func (runtime *fakeCollectorHeartbeatRuntime) Activate(
	lease collectorfleet.Lease,
) error {
	runtime.activatedLease = lease
	return runtime.activateErr
}

func (runtime *fakeCollectorHeartbeatRuntime) Offer(
	_ context.Context,
	lease collectorfleet.Lease,
	heartbeat collectorfleet.Heartbeat,
) (bool, error) {
	runtime.offeredLease = lease
	runtime.offeredHeartbeat = heartbeat
	return runtime.offerAccepted, runtime.offerErr
}

func (runtime *fakeCollectorHeartbeatRuntime) Release(
	ctx context.Context,
	lease collectorfleet.Lease,
) error {
	if runtime.calls != nil {
		*runtime.calls = append(*runtime.calls, "release")
	}
	runtime.releasedLease = lease
	runtime.releaseDeadline, runtime.releaseHasDeadline = ctx.Deadline()
	if runtime.waitForReleaseContext {
		<-ctx.Done()
		return ctx.Err()
	}
	return runtime.releaseErr
}

type fakeCollectorFleetRuntimeStore struct {
	disconnectApplied     bool
	disconnectErr         error
	disconnectedLease     collectorfleet.Lease
	disconnectedAt        time.Time
	disconnectCalls       int
	disconnectContextErr  error
	disconnectDeadline    time.Time
	disconnectHasDeadline bool
	calls                 *[]string
}

func (store *fakeCollectorFleetRuntimeStore) Disconnect(
	ctx context.Context,
	lease collectorfleet.Lease,
	receivedAt time.Time,
) (bool, error) {
	if store.calls != nil {
		*store.calls = append(*store.calls, "disconnect")
	}
	store.disconnectCalls++
	store.disconnectContextErr = ctx.Err()
	store.disconnectDeadline, store.disconnectHasDeadline = ctx.Deadline()
	store.disconnectedLease = lease
	store.disconnectedAt = receivedAt
	return store.disconnectApplied, store.disconnectErr
}

type fakeIndexRetentionCatalog struct {
	index control.Index
	err   error
}

func (catalog fakeIndexRetentionCatalog) GetIndexByName(context.Context, string) (control.Index, error) {
	return catalog.index, catalog.err
}
