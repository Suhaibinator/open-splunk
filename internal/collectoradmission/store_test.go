package collectoradmission

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const testCollectorID = "123e4567-e89b-12d3-a456-426614174000"

var testDigestKey = []byte("0123456789abcdef0123456789abcdef")

type admissionFixture struct {
	database *control.DB
	tokens   *auth.Store
	fleet    *collectorfleet.Store
	store    *Store
}

func openAdmissionFixture(t *testing.T, indexNames ...string) admissionFixture {
	t.Helper()
	ctx := context.Background()
	database, err := control.Open(
		ctx,
		filepath.Join(t.TempDir(), "control.sqlite"),
	)
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("control DB Close(): %v", err)
		}
	})
	for _, name := range indexNames {
		if _, err := database.CreateIndex(ctx, control.IndexDefinition{
			Name:             name,
			DisplayName:      name,
			IngestionEnabled: true,
			SearchEnabled:    true,
		}); err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
	}
	tokens, err := auth.NewStore(database, testDigestKey)
	if err != nil {
		t.Fatalf("auth.NewStore(): %v", err)
	}
	fleet, err := collectorfleet.New(database)
	if err != nil {
		t.Fatalf("collectorfleet.New(): %v", err)
	}
	store, err := New(database, tokens, "tenant-a")
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return admissionFixture{
		database: database,
		tokens:   tokens,
		fleet:    fleet,
		store:    store,
	}
}

func issueToken(
	t *testing.T,
	fixture admissionFixture,
	name string,
	collectorID string,
	indexNames ...string,
) auth.IssuedCollectorToken {
	t.Helper()
	issued, err := fixture.tokens.CreateCollectorToken(
		context.Background(),
		auth.CreateCollectorTokenRequest{
			Name:              name,
			BoundCollectorID:  collectorID,
			AllowedIndexNames: indexNames,
		},
	)
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	return issued
}

func admissionRequest(
	collectorID string,
	streamID string,
	acceptedAt time.Time,
	inputIndex string,
) Request {
	inputs := []collectorfleet.InputRegistration(nil)
	if inputIndex != "" {
		inputs = []collectorfleet.InputRegistration{{
			InputID:   "input-1",
			InputType: 1,
			IndexName: inputIndex,
		}}
	}
	return Request{
		CollectorID: collectorID,
		BootEpoch:   "server-boot-1",
		StreamID:    streamID,
		AcceptedAt:  acceptedAt,
		Hello: collectorfleet.Hello{
			InstanceID:        "instance-1",
			ProtocolMajor:     1,
			ProtocolMinor:     0,
			CollectorVersion:  "1.2.3",
			Hostname:          "collector.example",
			OperatingSystem:   "linux",
			Architecture:      "amd64",
			StartedAt:         acceptedAt.Add(-time.Minute),
			Capabilities:      []uint32{2, 1, 2},
			AuthorizedIndexes: []string{"caller-controlled-stale-scope"},
			Inputs:            inputs,
		},
	}
}

func TestAdmitCommitsFreshCredentialScopeTokenUseAndDurableLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "audit", "main", "stale")
	issued := issueToken(
		t,
		fixture,
		"collector",
		testCollectorID,
		"main",
		"audit",
	)
	acceptedAt := issued.Token.CreatedAt.Add(time.Minute)

	result, err := fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(testCollectorID, "stream-1", acceptedAt, "main"),
	)
	if err != nil {
		t.Fatalf("Admit(): %v", err)
	}
	if result.Authentication.TokenID != issued.Token.ID ||
		result.Authentication.BoundCollectorID != testCollectorID ||
		!slices.Equal(
			result.Authentication.AllowedIndexNames,
			[]string{"audit", "main"},
		) {
		t.Fatalf("fresh authentication = %#v", result.Authentication)
	}
	if result.Lease.TenantID != "tenant-a" ||
		result.Lease.CollectorID != testCollectorID ||
		result.Lease.BootEpoch != "server-boot-1" ||
		result.Lease.StreamID != "stream-1" ||
		result.Lease.Generation != 1 {
		t.Fatalf("lease = %#v", result.Lease)
	}
	if result.Collector.ActiveLease == nil ||
		result.Collector.ActiveLease.StreamID != "stream-1" ||
		!slices.Equal(result.Collector.AuthorizedIndexes, []string{"audit", "main"}) {
		t.Fatalf("collector = %#v", result.Collector)
	}
	token, err := fixture.tokens.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if !token.LastUsedAt.Equal(acceptedAt) ||
		token.Version != issued.Token.Version ||
		!token.UpdatedAt.Equal(issued.Token.UpdatedAt) {
		t.Fatalf("token after admission = %#v", token)
	}
	persisted, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
	)
	if err != nil {
		t.Fatalf("fleet Get(): %v", err)
	}
	if !slices.Equal(persisted.AuthorizedIndexes, []string{"audit", "main"}) ||
		slices.Contains(
			persisted.AuthorizedIndexes,
			"caller-controlled-stale-scope",
		) {
		t.Fatalf("persisted authorized indexes = %v", persisted.AuthorizedIndexes)
	}
}

func TestAdmitCapacityRollsBackTokenUseAndNewCollectorIdentity(
	t *testing.T,
) {
	fixture := openAdmissionFixture(t, "main")
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)
	for ordinal := 0; ordinal < collectorfleet.MaximumDurableCollectorsPerTenant; ordinal++ {
		collectorID := fmt.Sprintf(
			"collector-admission-capacity-%03d",
			ordinal,
		)
		request := admissionRequest(
			collectorID,
			"stream-"+collectorID,
			now.Add(time.Duration(ordinal)*time.Microsecond),
			"",
		)
		_, _, err := fixture.fleet.Claim(ctx, collectorfleet.ClaimRequest{
			Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
			CollectorID: request.CollectorID,
			BootEpoch:   request.BootEpoch,
			StreamID:    request.StreamID,
			ReceivedAt:  request.AcceptedAt,
			Hello: collectorfleet.Hello{
				InstanceID:        "instance-" + collectorID,
				ProtocolMajor:     request.Hello.ProtocolMajor,
				ProtocolMinor:     request.Hello.ProtocolMinor,
				CollectorVersion:  request.Hello.CollectorVersion,
				Hostname:          collectorID + ".example",
				OperatingSystem:   request.Hello.OperatingSystem,
				Architecture:      request.Hello.Architecture,
				StartedAt:         request.Hello.StartedAt,
				AuthorizedIndexes: []string{"main"},
			},
		})
		if err != nil {
			t.Fatalf("seed durable collector %d: %v", ordinal, err)
		}
	}

	const rejectedCollectorID = "collector-admission-over-capacity"
	issued := issueToken(
		t,
		fixture,
		"over-capacity",
		rejectedCollectorID,
		"main",
	)
	acceptedAt := issued.Token.CreatedAt.Add(time.Minute)
	_, err := fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(
			rejectedCollectorID,
			"stream-over-capacity",
			acceptedAt,
			"main",
		),
	)
	if !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf(
			"Admit(over capacity) error = %v, want ErrCapacityExceeded",
			err,
		)
	}
	assertTokenUnused(t, fixture.tokens, issued.Token.ID)
	if _, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		rejectedCollectorID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"fleet Get(rejected identity) error = %v, want ErrNotFound",
			err,
		)
	}
}

func TestAdmitRejectsBindingMismatchWithoutPartialWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	acceptedAt := issued.Token.CreatedAt.Add(time.Minute)
	otherCollector := "123e4567-e89b-12d3-a456-426614174999"

	_, err := fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(otherCollector, "stream-1", acceptedAt, "main"),
	)
	if !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("Admit() error = %v, want ErrUnauthorized", err)
	}
	assertTokenUnused(t, fixture.tokens, issued.Token.ID)
	if _, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		otherCollector,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("fleet Get() error = %v, want ErrNotFound", err)
	}
}

func TestAdmitDisabledFleetRollsBackTokenUseAndLeaseChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	firstAcceptedAt := issued.Token.CreatedAt.Add(time.Minute)
	first, err := fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(
			testCollectorID,
			"stream-1",
			firstAcceptedAt,
			"main",
		),
	)
	if err != nil {
		t.Fatalf("first Admit(): %v", err)
	}
	if _, err := fixture.fleet.UpdateAdministration(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
		1,
		collectorfleet.Administration{
			State: collectorfleet.AdministrativeStateDisabled,
		},
		firstAcceptedAt.Add(time.Minute),
	); err != nil {
		t.Fatalf("UpdateAdministration(disable): %v", err)
	}

	_, err = fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(
			testCollectorID,
			"stream-2",
			firstAcceptedAt.Add(2*time.Minute),
			"main",
		),
	)
	if !errors.Is(err, collectorfleet.ErrCollectorDisabled) {
		t.Fatalf("disabled Admit() error = %v, want ErrCollectorDisabled", err)
	}
	token, err := fixture.tokens.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if !token.LastUsedAt.Equal(firstAcceptedAt) {
		t.Fatalf(
			"last use after rejected admission = %v, want %v",
			token.LastUsedAt,
			firstAcceptedAt,
		)
	}
	persisted, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
	)
	if err != nil {
		t.Fatalf("fleet Get(): %v", err)
	}
	if persisted.AdministrativeState !=
		collectorfleet.AdministrativeStateDisabled ||
		persisted.ActiveLease != nil ||
		persisted.LeaseGeneration != first.Lease.Generation {
		t.Fatalf("fleet after rejected admission = %#v", persisted)
	}
}

func TestAdmitRejectsInputOutsideFreshScopeAndRollsBack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "audit", "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	acceptedAt := issued.Token.CreatedAt.Add(time.Minute)

	_, err := fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(testCollectorID, "stream-1", acceptedAt, "audit"),
	)
	if !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Admit() error = %v, want ErrInvalidArgument", err)
	}
	assertTokenUnused(t, fixture.tokens, issued.Token.ID)
	if _, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("fleet Get() error = %v, want ErrNotFound", err)
	}
}

func TestAdmitInactiveCredentialsLeaveFleetAndTokenUseUnchanged(t *testing.T) {
	t.Parallel()
	for name, prepare := range map[string]func(
		t *testing.T,
		fixture admissionFixture,
	) (auth.IssuedCollectorToken, time.Time){
		"revoked": func(
			t *testing.T,
			fixture admissionFixture,
		) (auth.IssuedCollectorToken, time.Time) {
			issued := issueToken(
				t,
				fixture,
				"revoked collector",
				testCollectorID,
				"main",
			)
			if _, err := fixture.tokens.RevokeCollectorToken(
				context.Background(),
				issued.Token.ID,
				issued.Token.Version,
			); err != nil {
				t.Fatalf("RevokeCollectorToken(): %v", err)
			}
			return issued, issued.Token.CreatedAt.Add(time.Minute)
		},
		"expired": func(
			t *testing.T,
			fixture admissionFixture,
		) (auth.IssuedCollectorToken, time.Time) {
			expiresAt := time.Now().UTC().Add(time.Hour)
			issued, err := fixture.tokens.CreateCollectorToken(
				context.Background(),
				auth.CreateCollectorTokenRequest{
					Name:              "expired collector",
					BoundCollectorID:  testCollectorID,
					AllowedIndexNames: []string{"main"},
					ExpiresAt:         expiresAt,
				},
			)
			if err != nil {
				t.Fatalf("CreateCollectorToken(): %v", err)
			}
			return issued, issued.Token.ExpiresAt.Add(time.Microsecond)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			fixture := openAdmissionFixture(t, "main")
			issued, acceptedAt := prepare(t, fixture)
			_, err := fixture.store.Admit(
				ctx,
				issued.Secret.Plaintext(),
				admissionRequest(
					testCollectorID,
					"stream-1",
					acceptedAt,
					"main",
				),
			)
			if !errors.Is(err, auth.ErrUnauthorized) {
				t.Fatalf("Admit() error = %v, want ErrUnauthorized", err)
			}
			assertTokenUnused(t, fixture.tokens, issued.Token.ID)
			if _, err := fixture.fleet.Get(
				ctx,
				collectorfleet.Scope{TenantID: "tenant-a"},
				testCollectorID,
			); !errors.Is(err, control.ErrNotFound) {
				t.Fatalf("fleet Get() error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestConcurrentRotatedCredentialsAllocateMonotonicDurableLeases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	firstToken := issueToken(
		t,
		fixture,
		"collector first",
		testCollectorID,
		"main",
	)
	secondToken := issueToken(
		t,
		fixture,
		"collector replacement",
		testCollectorID,
		"main",
	)
	acceptedAt := firstToken.Token.CreatedAt
	if secondToken.Token.CreatedAt.After(acceptedAt) {
		acceptedAt = secondToken.Token.CreatedAt
	}
	acceptedAt = acceptedAt.Add(time.Minute)

	type outcome struct {
		result Result
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for index, issued := range []auth.IssuedCollectorToken{
		firstToken,
		secondToken,
	} {
		index := index
		issued := issued
		go func() {
			<-start
			result, err := fixture.store.Admit(
				ctx,
				issued.Secret.Plaintext(),
				admissionRequest(
					testCollectorID,
					fmt.Sprintf("stream-%d", index+1),
					acceptedAt,
					"main",
				),
			)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	first := <-outcomes
	second := <-outcomes
	for index, current := range []outcome{first, second} {
		if current.err != nil {
			t.Fatalf("concurrent Admit(%d): %v", index, current.err)
		}
	}
	generations := []uint64{
		first.result.Lease.Generation,
		second.result.Lease.Generation,
	}
	slices.Sort(generations)
	if !slices.Equal(generations, []uint64{1, 2}) {
		t.Fatalf("lease generations = %v, want [1 2]", generations)
	}
	persisted, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
	)
	if err != nil {
		t.Fatalf("fleet Get(): %v", err)
	}
	if persisted.ActiveLease == nil ||
		persisted.ActiveLease.Generation != 2 {
		t.Fatalf("final active lease = %#v", persisted.ActiveLease)
	}
	for _, issued := range []auth.IssuedCollectorToken{
		firstToken,
		secondToken,
	} {
		token, err := fixture.tokens.GetCollectorToken(ctx, issued.Token.ID)
		if err != nil {
			t.Fatalf("GetCollectorToken(%s): %v", issued.Token.ID, err)
		}
		if !token.LastUsedAt.Equal(acceptedAt) {
			t.Fatalf(
				"token %s last use = %v, want %v",
				issued.Token.ID,
				token.LastUsedAt,
				acceptedAt,
			)
		}
	}
}

func TestAdmissionClockRollbackKeepsTokenAndFleetTimesMonotonic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	newerAcceptedAt := issued.Token.CreatedAt.Add(2 * time.Minute)
	if _, err := fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(
			testCollectorID,
			"stream-1",
			newerAcceptedAt,
			"main",
		),
	); err != nil {
		t.Fatalf("newer Admit(): %v", err)
	}
	olderAcceptedAt := newerAcceptedAt.Add(-time.Minute)
	second, err := fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(
			testCollectorID,
			"stream-2",
			olderAcceptedAt,
			"main",
		),
	)
	if err != nil {
		t.Fatalf("clock-rollback Admit(): %v", err)
	}
	if second.Lease.Generation != 2 ||
		!second.Collector.ConnectedAt.Equal(newerAcceptedAt) ||
		!second.Collector.LastSeenAt.Equal(newerAcceptedAt) {
		t.Fatalf("collector after clock rollback = %#v", second.Collector)
	}
	token, err := fixture.tokens.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if !token.LastUsedAt.Equal(newerAcceptedAt) {
		t.Fatalf(
			"last use after clock rollback = %v, want %v",
			token.LastUsedAt,
			newerAcceptedAt,
		)
	}
}

func TestConcurrentAdmissionAndDisableHaveNoPartialOutcome(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	firstAcceptedAt := issued.Token.CreatedAt.Add(time.Minute)
	if _, err := fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(
			testCollectorID,
			"stream-1",
			firstAcceptedAt,
			"main",
		),
	); err != nil {
		t.Fatalf("first Admit(): %v", err)
	}
	if _, err := fixture.fleet.UpdateAdministration(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
		1,
		collectorfleet.Administration{
			State: collectorfleet.AdministrativeStateDisabled,
		},
		firstAcceptedAt.Add(time.Minute),
	); err != nil {
		t.Fatalf("first disable: %v", err)
	}
	if _, err := fixture.fleet.UpdateAdministration(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
		2,
		collectorfleet.Administration{
			State: collectorfleet.AdministrativeStateEnabled,
		},
		firstAcceptedAt.Add(2*time.Minute),
	); err != nil {
		t.Fatalf("re-enable: %v", err)
	}

	secondAcceptedAt := firstAcceptedAt.Add(3 * time.Minute)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	var admitErr error
	var disableErr error
	go func() {
		defer wait.Done()
		<-start
		_, admitErr = fixture.store.Admit(
			ctx,
			issued.Secret.Plaintext(),
			admissionRequest(
				testCollectorID,
				"stream-2",
				secondAcceptedAt,
				"main",
			),
		)
	}()
	go func() {
		defer wait.Done()
		<-start
		_, disableErr = fixture.fleet.UpdateAdministration(
			ctx,
			collectorfleet.Scope{TenantID: "tenant-a"},
			testCollectorID,
			3,
			collectorfleet.Administration{
				State: collectorfleet.AdministrativeStateDisabled,
			},
			secondAcceptedAt.Add(time.Minute),
		)
	}()
	close(start)
	wait.Wait()

	if disableErr != nil {
		t.Fatalf("concurrent disable: %v", disableErr)
	}
	if admitErr != nil &&
		!errors.Is(admitErr, collectorfleet.ErrCollectorDisabled) {
		t.Fatalf("concurrent Admit() error = %v", admitErr)
	}
	persisted, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
	)
	if err != nil {
		t.Fatalf("fleet Get(): %v", err)
	}
	if persisted.AdministrativeState !=
		collectorfleet.AdministrativeStateDisabled ||
		persisted.ActiveLease != nil {
		t.Fatalf("final fleet state = %#v", persisted)
	}
	token, err := fixture.tokens.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	wantLastUse := firstAcceptedAt
	if admitErr == nil {
		wantLastUse = secondAcceptedAt
	}
	if !token.LastUsedAt.Equal(wantLastUse) {
		t.Fatalf("last use = %v, want %v", token.LastUsedAt, wantLastUse)
	}
}

func TestConcurrentAdmissionAndRevocationAreLinearizable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	acceptedAt := issued.Token.CreatedAt.Add(time.Minute)

	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	var admission Result
	var admitErr error
	var revokeErr error
	go func() {
		defer wait.Done()
		<-start
		admission, admitErr = fixture.store.Admit(
			ctx,
			issued.Secret.Plaintext(),
			admissionRequest(
				testCollectorID,
				"stream-1",
				acceptedAt,
				"main",
			),
		)
	}()
	go func() {
		defer wait.Done()
		<-start
		_, revokeErr = fixture.tokens.RevokeCollectorToken(
			ctx,
			issued.Token.ID,
			issued.Token.Version,
		)
	}()
	close(start)
	wait.Wait()

	if revokeErr != nil {
		t.Fatalf("RevokeCollectorToken(): %v", revokeErr)
	}
	if admitErr != nil && !errors.Is(admitErr, auth.ErrUnauthorized) {
		t.Fatalf("Admit() error = %v, want success or ErrUnauthorized", admitErr)
	}
	token, err := fixture.tokens.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if token.State != auth.CollectorTokenStateRevoked {
		t.Fatalf("final token state = %s, want revoked", token.State)
	}
	if admitErr == nil {
		if admission.Lease.Generation != 1 ||
			!token.LastUsedAt.Equal(acceptedAt) {
			t.Fatalf(
				"admission-first result = (%#v, last use %v)",
				admission,
				token.LastUsedAt,
			)
		}
		persisted, err := fixture.fleet.Get(
			ctx,
			collectorfleet.Scope{TenantID: "tenant-a"},
			testCollectorID,
		)
		if err != nil || persisted.ActiveLease == nil {
			t.Fatalf(
				"admission-first fleet = (%#v, %v)",
				persisted,
				err,
			)
		}
		return
	}
	if !token.LastUsedAt.IsZero() {
		t.Fatalf("revocation-first last use = %v, want zero", token.LastUsedAt)
	}
	if _, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("revocation-first fleet error = %v, want ErrNotFound", err)
	}
}

func TestConcurrentAdmissionAndIndexDisableUseOneScopeOrdering(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	index, err := fixture.database.GetIndexByName(ctx, "main")
	if err != nil {
		t.Fatalf("GetIndexByName(main): %v", err)
	}
	disabledDefinition := index.Definition
	disabledDefinition.IngestionEnabled = false
	acceptedAt := issued.Token.CreatedAt.Add(time.Minute)

	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	var admission Result
	var admitErr error
	var updateErr error
	go func() {
		defer wait.Done()
		<-start
		admission, admitErr = fixture.store.Admit(
			ctx,
			issued.Secret.Plaintext(),
			admissionRequest(
				testCollectorID,
				"stream-1",
				acceptedAt,
				"main",
			),
		)
	}()
	go func() {
		defer wait.Done()
		<-start
		_, updateErr = fixture.database.UpdateIndex(
			ctx,
			index.ID,
			index.Version,
			disabledDefinition,
		)
	}()
	close(start)
	wait.Wait()

	if updateErr != nil {
		t.Fatalf("UpdateIndex(disable ingestion): %v", updateErr)
	}
	if admitErr != nil && !errors.Is(admitErr, auth.ErrUnauthorized) {
		t.Fatalf("Admit() error = %v, want success or ErrUnauthorized", admitErr)
	}
	token, err := fixture.tokens.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if admitErr == nil {
		if admission.Lease.Generation != 1 ||
			!token.LastUsedAt.Equal(acceptedAt) {
			t.Fatalf(
				"admission-first result = (%#v, last use %v)",
				admission,
				token.LastUsedAt,
			)
		}
		return
	}
	if !token.LastUsedAt.IsZero() {
		t.Fatalf("index-disable-first last use = %v, want zero", token.LastUsedAt)
	}
	if _, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("index-disable-first fleet error = %v, want ErrNotFound", err)
	}
}

func TestAdmitRejectsCorruptTotalScopeCardinalityWithoutUnboundedProjection(
	t *testing.T,
) {
	for name, ingestionEnabled := range map[string]bool{
		"active scopes":   true,
		"inactive scopes": false,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fixture := openAdmissionFixture(t, "main")
			issued := issueToken(
				t,
				fixture,
				"collector",
				testCollectorID,
				"main",
			)

			indexIDs := make([]string, 0, 256)
			for index := 0; index < 256; index++ {
				created, err := fixture.database.CreateIndex(
					ctx,
					control.IndexDefinition{
						Name:             fmt.Sprintf("scope_%03d", index),
						IngestionEnabled: ingestionEnabled,
						SearchEnabled:    true,
					},
				)
				if err != nil {
					t.Fatalf("CreateIndex(scope_%03d): %v", index, err)
				}
				indexIDs = append(indexIDs, created.ID)
			}
			tx, err := fixture.database.SQLDB().BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("BeginTx(): %v", err)
			}
			for _, indexID := range indexIDs {
				if _, err := tx.ExecContext(
					ctx,
					`INSERT INTO ingestion_token_indexes (
						ingestion_token_id,
						index_id
					) VALUES (?, ?)`,
					issued.Token.ID,
					indexID,
				); err != nil {
					_ = tx.Rollback()
					t.Fatalf("insert corrupt scope: %v", err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit corrupt scopes: %v", err)
			}

			acceptedAt := issued.Token.CreatedAt.Add(time.Minute)
			_, err = fixture.store.Admit(
				ctx,
				issued.Secret.Plaintext(),
				admissionRequest(
					testCollectorID,
					"stream-1",
					acceptedAt,
					"main",
				),
			)
			if err == nil ||
				!strings.Contains(err.Error(), "scope count exceeds") ||
				strings.Contains(err.Error(), issued.Secret.Plaintext()) {
				t.Fatalf("Admit() error = %v", err)
			}
			assertPersistedTokenUnused(
				t,
				fixture.database,
				issued.Token.ID,
			)
			if _, err := fixture.fleet.Get(
				ctx,
				collectorfleet.Scope{TenantID: "tenant-a"},
				testCollectorID,
			); !errors.Is(err, control.ErrNotFound) {
				t.Fatalf("fleet Get() error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestAdmitRejectsOversizedPersistedTokenIDBeforeProjection(t *testing.T) {
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	oversizedTokenID := strings.Repeat("x", 1<<20)

	connection, err := fixture.database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("SQLDB().Conn(): %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA foreign_keys = OFF`,
	); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		`UPDATE ingestion_token_indexes
		 SET ingestion_token_id = ?
		 WHERE ingestion_token_id = ?`,
		oversizedTokenID,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("corrupt token membership ID: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		`UPDATE ingestion_tokens
		 SET ingestion_token_id = ?
		 WHERE ingestion_token_id = ?`,
		oversizedTokenID,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("corrupt token ID: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA foreign_keys = ON`,
	); err != nil {
		t.Fatalf("restore foreign keys: %v", err)
	}

	acceptedAt := issued.Token.CreatedAt.Add(time.Minute)
	_, err = fixture.store.Admit(
		ctx,
		issued.Secret.Plaintext(),
		admissionRequest(testCollectorID, "stream-1", acceptedAt, "main"),
	)
	if !errors.Is(err, auth.ErrUnauthorized) ||
		strings.Contains(err.Error(), issued.Secret.Plaintext()) {
		t.Fatalf("Admit() error = %v, want sanitized ErrUnauthorized", err)
	}
	var lastUsedAt *int64
	if err := fixture.database.SQLDB().QueryRowContext(
		ctx,
		`SELECT last_used_at_unix_micro
		 FROM ingestion_tokens
		 WHERE ingestion_token_id = ?`,
		oversizedTokenID,
	).Scan(&lastUsedAt); err != nil {
		t.Fatalf("read corrupt token use: %v", err)
	}
	if lastUsedAt != nil {
		t.Fatalf("rejected corrupt token last use = %d, want NULL", *lastUsedAt)
	}
	if _, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("fleet Get() error = %v, want ErrNotFound", err)
	}
}

func TestTransactionScopedAdmissionHelpersRejectRootDatabaseHandle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	acceptedAt := issued.Token.CreatedAt.Add(time.Minute)
	if _, err := fixture.tokens.
		RevalidateAndRecordCollectorUseInTransaction(
			ctx,
			fixture.database.GORMDB(),
			issued.Secret.Plaintext(),
			acceptedAt,
		); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("token helper error = %v, want ErrInvalidArgument", err)
	}
	request := admissionRequest(
		testCollectorID,
		"stream-1",
		acceptedAt,
		"main",
	)
	prepared, err := collectorfleet.PrepareClaim(
		collectorfleet.ClaimRequest{
			Scope:       collectorfleet.Scope{TenantID: "tenant-a"},
			CollectorID: request.CollectorID,
			BootEpoch:   request.BootEpoch,
			StreamID:    request.StreamID,
			ReceivedAt:  request.AcceptedAt,
			Hello:       request.Hello,
		},
	)
	if err != nil {
		t.Fatalf("PrepareClaim(): %v", err)
	}
	if _, _, err := fixture.fleet.ClaimPreparedInTransaction(
		ctx,
		fixture.database.GORMDB(),
		prepared,
		[]string{"main"},
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("fleet helper error = %v, want ErrInvalidArgument", err)
	}
	assertTokenUnused(t, fixture.tokens, issued.Token.ID)
}

func TestCanceledAdmissionWaitingForWriterLeavesNoPartialState(t *testing.T) {
	ctx := context.Background()
	fixture := openAdmissionFixture(t, "main")
	issued := issueToken(t, fixture, "collector", testCollectorID, "main")
	blocker := fixture.database.GORMDB().WithContext(ctx).Begin()
	if blocker.Error != nil {
		t.Fatalf("begin blocking transaction: %v", blocker.Error)
	}
	blockerFinished := false
	defer func() {
		if !blockerFinished {
			_ = blocker.Rollback().Error
		}
	}()

	admissionContext, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := fixture.store.Admit(
			admissionContext,
			issued.Secret.Plaintext(),
			admissionRequest(
				testCollectorID,
				"stream-1",
				issued.Token.CreatedAt.Add(time.Minute),
				"main",
			),
		)
		result <- err
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := blocker.Rollback().Error; err != nil {
		t.Fatalf("roll back blocking transaction: %v", err)
	}
	blockerFinished = true
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Admit() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled Admit() did not stop after SQLite writer released")
	}
	assertTokenUnused(t, fixture.tokens, issued.Token.ID)
	if _, err := fixture.fleet.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		testCollectorID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("fleet Get() error = %v, want ErrNotFound", err)
	}
}

func TestNewRejectsInvalidDependenciesAndTenantAliases(t *testing.T) {
	t.Parallel()
	fixture := openAdmissionFixture(t, "main")
	for name, test := range map[string]struct {
		database *control.DB
		tokens   *auth.Store
		tenantID string
	}{
		"nil database": {
			tokens: fixture.tokens, tenantID: "tenant-a",
		},
		"nil token store": {
			database: fixture.database, tenantID: "tenant-a",
		},
		"empty tenant": {
			database: fixture.database, tokens: fixture.tokens,
		},
		"padded tenant": {
			database: fixture.database,
			tokens:   fixture.tokens,
			tenantID: " tenant-a ",
		},
		"tenant with NUL": {
			database: fixture.database,
			tokens:   fixture.tokens,
			tenantID: "tenant\x00a",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := New(
				test.database,
				test.tokens,
				test.tenantID,
			); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("New() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func assertTokenUnused(
	t *testing.T,
	tokens *auth.Store,
	tokenID string,
) {
	t.Helper()
	token, err := tokens.GetCollectorToken(context.Background(), tokenID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	if !token.LastUsedAt.IsZero() {
		t.Fatalf("token last use = %v, want zero", token.LastUsedAt)
	}
}

func assertPersistedTokenUnused(
	t *testing.T,
	database *control.DB,
	tokenID string,
) {
	t.Helper()
	var row struct {
		LastUsedAtUnixMicro *int64 `gorm:"column:last_used_at_unix_micro"`
	}
	query := database.GORMDB().
		Table("ingestion_tokens").
		Select("last_used_at_unix_micro").
		Where("ingestion_token_id = ?", tokenID).
		Take(&row)
	if query.Error != nil {
		t.Fatalf("read persisted collector token last use: %v", query.Error)
	}
	if row.LastUsedAtUnixMicro != nil {
		t.Fatalf(
			"persisted token last use = %d, want NULL",
			*row.LastUsedAtUnixMicro,
		)
	}
}
