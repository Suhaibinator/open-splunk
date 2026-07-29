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

func TestCollectorSessionManagerAdmitsAndDetachesFreshAuthority(t *testing.T) {
	t.Parallel()
	acceptedAt := time.Date(2026, 7, 28, 12, 34, 56, 123_456_000, time.UTC)
	indexes := []string{"audit", "main"}
	lease := collectorfleet.Lease{
		Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
		CollectorID: "collector-a",
		BootEpoch:   "boot-a",
		StreamID:    "stream-a",
		Generation:  7,
	}
	store := &fakeCollectorAdmissionRuntimeStore{
		admitResult: collectoradmission.Result{
			Authentication: auth.Authentication{
				TokenID:           "token-safe-id",
				BoundCollectorID:  "collector-a",
				AllowedIndexNames: indexes,
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
		got.Authorization.CollectorID != "collector-a" {
		t.Fatalf("admission = %+v", got)
	}
	got.Authorization.AuthorizedIndexes[0] = "changed"
	if indexes[0] != "audit" {
		t.Fatal("admission authorization aliases persistence result")
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
		Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
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
		Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
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
			Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
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
			Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
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

type fakeCollectorAdmissionRuntimeStore struct {
	admitBearer  string
	admitRequest collectoradmission.Request
	admitResult  collectoradmission.Result
	admitErr     error
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

func (*fakeCollectorAdmissionRuntimeStore) AuthorizeLease(
	context.Context,
	string,
	collectorfleet.Lease,
	time.Time,
) (auth.Authentication, error) {
	return auth.Authentication{}, errors.New("unexpected lease authorization")
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
