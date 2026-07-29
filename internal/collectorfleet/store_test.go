package collectorfleet

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func openTestStore(t *testing.T) (*control.DB, *Store) {
	t.Helper()
	database, err := control.Open(
		context.Background(),
		filepath.Join(t.TempDir(), "control.sqlite"),
	)
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	store, err := New(database)
	if err != nil {
		_ = database.Close()
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("control DB Close(): %v", err)
		}
	})
	return database, store
}

func testClaim(receivedAt time.Time) ClaimRequest {
	lastAcknowledged := uint64(7)
	source := "/var/log/app.json"
	sourcetype := "app:json"
	return ClaimRequest{
		Scope:       Scope{TenantID: "tenant-a"},
		CollectorID: "123e4567-e89b-12d3-a456-426614174000",
		BootEpoch:   "server-boot-1",
		StreamID:    "stream-1",
		ReceivedAt:  receivedAt,
		Hello: Hello{
			InstanceID:                    "instance-1",
			ProtocolMajor:                 1,
			ProtocolMinor:                 0,
			CollectorVersion:              "1.2.3",
			Hostname:                      "collector.example",
			OperatingSystem:               "linux",
			Architecture:                  "amd64",
			StartedAt:                     receivedAt.Add(-time.Minute),
			Capabilities:                  []uint32{6, 1, 1, 2},
			AuthorizedIndexes:             []string{"main", "audit", "main"},
			LastAcknowledgedBatchSequence: &lastAcknowledged,
			Inputs: []InputRegistration{
				{
					InputID:    "input-app",
					InputType:  1,
					IndexName:  "main",
					Source:     &source,
					Sourcetype: &sourcetype,
				},
				{
					InputID:   "input-audit",
					InputType: 1,
					IndexName: "audit",
				},
			},
		},
	}
}

func testHeartbeat(receivedAt time.Time, sequence uint64) Heartbeat {
	oldest := 30 * time.Second
	lastSent := uint64(11)
	lastAcknowledged := uint64(10)
	lastEvent := receivedAt.Add(-time.Second)
	return Heartbeat{
		ObservationSequence: sequence,
		ObservedAt:          receivedAt.Add(-500 * time.Millisecond),
		ReceivedAt:          receivedAt,
		Queue: QueueTelemetry{
			QueuedEvents:            4,
			QueuedBytes:             4096,
			OldestEventAge:          &oldest,
			SentEventsTotal:         100,
			AcknowledgedEventsTotal: 90,
			RetriedBatchesTotal:     3,
			RejectedEventsTotal:     2,
			DroppedEventsTotal:      1,
		},
		Inputs: []InputHealth{
			{
				InputID:           "input-app",
				State:             2,
				StatusMessage:     "healthy",
				DiscoveredSources: 2,
				ActiveSources:     1,
				EventsReadTotal:   100,
				BytesReadTotal:    20_000,
				LastEventAt:       &lastEvent,
			},
		},
		LastSentBatchSequence:         &lastSent,
		LastAcknowledgedBatchSequence: &lastAcknowledged,
		ProcessResidentMemoryBytes:    64 << 20,
		ProcessCPUPercent:             12.5,
	}
}

func TestClaimCreatesTenantScopedFleetRecordAndSupersedesLease(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	receivedAt := time.Date(2026, 7, 28, 18, 0, 0, 123_456_789, time.UTC)
	first, firstLease, err := store.Claim(ctx, testClaim(receivedAt))
	if err != nil {
		t.Fatalf("Claim(first): %v", err)
	}
	if first.Version != 1 ||
		first.TelemetryRevision != 1 ||
		first.LeaseGeneration != 1 ||
		first.AdministrativeState != AdministrativeStateEnabled {
		t.Fatalf("first collector = %#v", first)
	}
	if first.ActiveLease == nil ||
		first.ActiveLease.BootEpoch != "server-boot-1" ||
		first.ActiveLease.StreamID != "stream-1" ||
		first.ActiveLease.InstanceID != "instance-1" {
		t.Fatalf("first active lease = %#v", first.ActiveLease)
	}
	if !first.FirstSeenAt.Equal(receivedAt.Truncate(time.Microsecond)) ||
		!first.ConnectedAt.Equal(receivedAt.Truncate(time.Microsecond)) ||
		!first.LastSeenAt.Equal(receivedAt.Truncate(time.Microsecond)) {
		t.Fatalf("server receive times were not canonicalized: %#v", first)
	}
	if !slices.Equal(first.Capabilities, []uint32{1, 2, 6}) ||
		!slices.Equal(first.AuthorizedIndexes, []string{"audit", "main"}) {
		t.Fatalf(
			"normalized capabilities/indexes = %v/%v",
			first.Capabilities,
			first.AuthorizedIndexes,
		)
	}
	if len(first.Inputs) != 2 ||
		first.Inputs[0].InputID != "input-app" ||
		first.Inputs[1].InputID != "input-audit" {
		t.Fatalf("normalized inputs = %#v", first.Inputs)
	}

	if _, err := store.Get(ctx, Scope{TenantID: "tenant-b"}, first.CollectorID); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(cross tenant) error = %v, want ErrNotFound", err)
	}
	got, err := store.Get(ctx, Scope{TenantID: "tenant-a"}, first.CollectorID)
	if err != nil {
		t.Fatalf("Get(): %v", err)
	}
	if got.ActiveLease == nil || got.ActiveLease.Generation != firstLease.Generation {
		t.Fatalf("Get() = %#v", got)
	}

	secondRequest := testClaim(receivedAt.Add(time.Minute))
	secondRequest.BootEpoch = "server-boot-2"
	secondRequest.StreamID = "stream-2"
	secondRequest.Hello.InstanceID = "instance-2"
	secondRequest.Hello.Hostname = "replacement.example"
	secondRequest.Hello.Capabilities = []uint32{4}
	secondRequest.Hello.AuthorizedIndexes = []string{"main"}
	secondRequest.Hello.Inputs = secondRequest.Hello.Inputs[:1]
	second, secondLease, err := store.Claim(ctx, secondRequest)
	if err != nil {
		t.Fatalf("Claim(second): %v", err)
	}
	if second.Version != first.Version ||
		second.TelemetryRevision != first.TelemetryRevision+1 ||
		second.LeaseGeneration != firstLease.Generation+1 ||
		secondLease.Generation != second.LeaseGeneration {
		t.Fatalf("superseding claim = %#v lease=%#v", second, secondLease)
	}
	if second.Hostname != "replacement.example" ||
		!slices.Equal(second.Capabilities, []uint32{4}) ||
		!slices.Equal(second.AuthorizedIndexes, []string{"main"}) ||
		len(second.Inputs) != 1 {
		t.Fatalf("superseding hello snapshot = %#v", second)
	}
	applied, err := store.RecordHeartbeat(
		ctx,
		firstLease,
		testHeartbeat(receivedAt.Add(2*time.Minute), 1),
	)
	if err != nil || applied {
		t.Fatalf("RecordHeartbeat(stale lease) = %t, %v, want false/nil", applied, err)
	}
}

func TestTrustedTenantScopeRejectsPaddedAlias(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 28, 18, 30, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	padded := Scope{TenantID: " tenant-a "}
	if _, err := store.Get(ctx, padded, lease.CollectorID); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Get(padded tenant) error = %v, want ErrInvalidArgument", err)
	}
	paddedLease := lease
	paddedLease.Scope = padded
	if applied, err := store.RecordHeartbeat(
		ctx,
		paddedLease,
		testHeartbeat(connectedAt.Add(time.Minute), 1),
	); applied || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("RecordHeartbeat(padded tenant) = %t, %v", applied, err)
	}
	if _, err := store.UpdateAdministration(
		ctx,
		padded,
		lease.CollectorID,
		collector.Version,
		Administration{State: AdministrativeStateDisabled},
		connectedAt.Add(time.Minute),
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("UpdateAdministration(padded tenant) error = %v, want ErrInvalidArgument", err)
	}
	got, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != collector.Version || got.AdministrativeState != AdministrativeStateEnabled {
		t.Fatalf("padded tenant mutated canonical tenant: %#v", got)
	}
}

func TestHeartbeatIsGenerationConditionalLatestWinsAndDetached(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 28, 19, 0, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatalf("Claim(): %v", err)
	}
	heartbeat := testHeartbeat(connectedAt.Add(time.Minute), 2)
	applied, err := store.RecordHeartbeat(ctx, lease, heartbeat)
	if err != nil || !applied {
		t.Fatalf("RecordHeartbeat(new): %t, %v", applied, err)
	}
	heartbeat.Inputs[0].StatusMessage = "caller mutation"
	*heartbeat.Queue.OldestEventAge = time.Hour

	got, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatalf("Get(): %v", err)
	}
	if got.Version != 1 || got.TelemetryRevision != 2 ||
		got.ObservationSequence != 2 ||
		got.Queue.QueuedEvents != 4 ||
		got.Queue.OldestEventAge == nil ||
		*got.Queue.OldestEventAge != 30*time.Second ||
		len(got.InputHealth) != 1 ||
		got.InputHealth[0].StatusMessage != "healthy" {
		t.Fatalf("heartbeat snapshot = %#v", got)
	}

	for label, candidate := range map[string]Heartbeat{
		"same":  testHeartbeat(connectedAt.Add(2*time.Minute), 2),
		"older": testHeartbeat(connectedAt.Add(3*time.Minute), 1),
	} {
		candidateApplied, candidateErr := store.RecordHeartbeat(ctx, lease, candidate)
		if candidateErr != nil || candidateApplied {
			t.Fatalf(
				"RecordHeartbeat(%s) = %t, %v, want false/nil",
				label,
				candidateApplied,
				candidateErr,
			)
		}
	}
	for label, staleLease := range map[string]Lease{
		"boot":       withLeaseBoot(lease, "wrong-boot"),
		"generation": withLeaseGeneration(lease, lease.Generation+1),
		"stream":     withLeaseStream(lease, "wrong-stream"),
		"tenant":     withLeaseTenant(lease, "tenant-b"),
	} {
		staleApplied, staleErr := store.RecordHeartbeat(
			ctx,
			staleLease,
			testHeartbeat(connectedAt.Add(4*time.Minute), 3),
		)
		if staleErr != nil || staleApplied {
			t.Fatalf(
				"RecordHeartbeat(stale %s) = %t, %v, want false/nil",
				label,
				staleApplied,
				staleErr,
			)
		}
	}

	// A backwards wall-clock adjustment does not move lifecycle time backward;
	// the stream-local observation sequence, not wall time, determines latest.
	rolledBack := testHeartbeat(connectedAt.Add(-time.Hour), 3)
	applied, err = store.RecordHeartbeat(ctx, lease, rolledBack)
	if err != nil || !applied {
		t.Fatalf("RecordHeartbeat(clock rollback): %t, %v", applied, err)
	}
	got, err = store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.LastSeenAt.Equal(connectedAt.Add(time.Minute)) ||
		got.ObservationSequence != 3 ||
		!got.ObservedAt.Equal(rolledBack.ObservedAt.Truncate(time.Microsecond)) {
		t.Fatalf("clock-rollback snapshot = %#v", got)
	}
}

func TestHeartbeatRejectsUnknownInputAndOverflowWithoutPartialMutation(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 28, 20, 0, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	for label, mutate := range map[string]func(*Heartbeat){
		"unknown input": func(heartbeat *Heartbeat) {
			heartbeat.Inputs[0].InputID = "unknown"
		},
		"counter overflow": func(heartbeat *Heartbeat) {
			heartbeat.Queue.QueuedEvents = math.MaxUint64
		},
		"non-finite CPU": func(heartbeat *Heartbeat) {
			heartbeat.ProcessCPUPercent = math.Inf(1)
		},
		"duplicate input": func(heartbeat *Heartbeat) {
			heartbeat.Inputs = append(heartbeat.Inputs, heartbeat.Inputs[0])
		},
		"observed time above public range": func(heartbeat *Heartbeat) {
			heartbeat.ObservedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		},
		"receive time above public range": func(heartbeat *Heartbeat) {
			heartbeat.ReceivedAt = time.UnixMicro(math.MaxInt64)
		},
		"input event time above public range": func(heartbeat *Heartbeat) {
			value := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			heartbeat.Inputs[0].LastEventAt = &value
		},
		"input error time above public range": func(heartbeat *Heartbeat) {
			value := time.UnixMicro(math.MaxInt64)
			heartbeat.Inputs[0].LastErrorAt = &value
		},
	} {
		t.Run(label, func(t *testing.T) {
			heartbeat := testHeartbeat(connectedAt.Add(time.Minute), 1)
			mutate(&heartbeat)
			if applied, err := store.RecordHeartbeat(ctx, lease, heartbeat); applied ||
				!errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("RecordHeartbeat() = %t, %v, want false/ErrInvalidArgument", applied, err)
			}
			got, getErr := store.Get(ctx, lease.Scope, lease.CollectorID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if got.TelemetryRevision != 1 || got.ObservationSequence != 0 || len(got.InputHealth) != 0 {
				t.Fatalf("rejected heartbeat partially mutated state: %#v", got)
			}
		})
	}
}

func TestAdministrativeCASDisableInvalidatesLeaseAndRequiresFreshClaim(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 28, 21, 0, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	displayName := "Production collector"
	if _, err := store.UpdateAdministration(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version+1,
		Administration{State: AdministrativeStateDisabled, DisplayName: &displayName},
		connectedAt.Add(time.Minute),
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("UpdateAdministration(stale) error = %v, want ErrVersionConflict", err)
	}
	disabled, err := store.UpdateAdministration(
		ctx,
		lease.Scope,
		lease.CollectorID,
		collector.Version,
		Administration{State: AdministrativeStateDisabled, DisplayName: &displayName},
		connectedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("UpdateAdministration(disable): %v", err)
	}
	if disabled.Version != 2 ||
		disabled.AdministrativeState != AdministrativeStateDisabled ||
		disabled.DisplayName == nil ||
		*disabled.DisplayName != displayName {
		t.Fatalf("disabled collector = %#v", disabled)
	}
	persistedDisabled, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if persistedDisabled.TelemetryRevision != 2 ||
		persistedDisabled.ActiveLease != nil ||
		persistedDisabled.DisconnectedAt == nil {
		t.Fatalf("disabled collector runtime = %#v", persistedDisabled)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		lease,
		testHeartbeat(connectedAt.Add(2*time.Minute), 1),
	); err != nil || applied {
		t.Fatalf("RecordHeartbeat(disabled lease) = %t, %v", applied, err)
	}
	if _, _, err := store.Claim(ctx, testClaim(connectedAt.Add(2*time.Minute))); !errors.Is(err, ErrCollectorDisabled) {
		t.Fatalf("Claim(disabled) error = %v, want ErrCollectorDisabled", err)
	}

	enabled, err := store.UpdateAdministration(
		ctx,
		lease.Scope,
		lease.CollectorID,
		disabled.Version,
		Administration{State: AdministrativeStateEnabled, DisplayName: &displayName},
		connectedAt.Add(3*time.Minute),
	)
	if err != nil {
		t.Fatalf("UpdateAdministration(enable): %v", err)
	}
	if enabled.Version != 3 ||
		enabled.AdministrativeState != AdministrativeStateEnabled {
		t.Fatalf("enabled collector before fresh claim = %#v", enabled)
	}
	persistedEnabled, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if persistedEnabled.ActiveLease != nil {
		t.Fatalf("enabled collector revived stale lease: %#v", persistedEnabled)
	}
	freshRequest := testClaim(connectedAt.Add(4 * time.Minute))
	freshRequest.StreamID = "stream-fresh"
	fresh, freshLease, err := store.Claim(ctx, freshRequest)
	if err != nil {
		t.Fatalf("Claim(after enable): %v", err)
	}
	if fresh.LeaseGeneration != lease.Generation+1 ||
		freshLease.Generation != fresh.LeaseGeneration {
		t.Fatalf("fresh lease = %#v collector=%#v", freshLease, fresh)
	}
}

func TestAdministrativeDisableWinsAtTelemetryRevisionCapacity(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 28, 21, 30, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	preservedDisplayName := "preserved at capacity"
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_fleet
		SET admin_version = ?, display_name = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		int64(math.MaxInt64),
		preservedDisplayName,
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("saturate administrator version: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_runtime
		SET telemetry_revision = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		int64(math.MaxInt64),
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("saturate telemetry revision: %v", err)
	}

	rejectedDisplayName := "must not replace terminal metadata"
	disabled, err := store.UpdateAdministration(
		ctx,
		lease.Scope,
		lease.CollectorID,
		uint64(math.MaxInt64),
		Administration{
			State:       AdministrativeStateDisabled,
			DisplayName: &rejectedDisplayName,
		},
		connectedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatalf("UpdateAdministration(disable at capacity): %v", err)
	}
	if disabled.Version != uint64(math.MaxInt64) ||
		disabled.AdministrativeState != AdministrativeStateDisabled ||
		disabled.DisplayName == nil ||
		*disabled.DisplayName != preservedDisplayName {
		t.Fatalf("disabled collector at capacity = %#v", disabled)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		lease,
		testHeartbeat(connectedAt.Add(2*time.Minute), 1),
	); err != nil || applied {
		t.Fatalf("RecordHeartbeat(disabled saturated lease) = %t, %v", applied, err)
	}
	if _, err := store.UpdateAdministration(
		ctx,
		lease.Scope,
		lease.CollectorID,
		uint64(math.MaxInt64),
		Administration{State: AdministrativeStateEnabled},
		connectedAt.Add(3*time.Minute),
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("UpdateAdministration(after terminal disable) error = %v, want ErrCapacityExceeded", err)
	}
	persisted, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.AdministrativeState != AdministrativeStateDisabled ||
		persisted.TelemetryRevision != uint64(math.MaxInt64) ||
		persisted.ActiveLease != nil ||
		persisted.DisconnectedAt == nil {
		t.Fatalf("terminal disable did not remain durable: %#v", persisted)
	}
}

func TestTelemetryRevisionReservesTerminalDisconnect(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 28, 21, 45, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_runtime
		SET telemetry_revision = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		int64(math.MaxInt64-1),
		lease.TenantID,
		lease.CollectorID,
	); err != nil {
		t.Fatalf("reserve terminal telemetry revision: %v", err)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		lease,
		testHeartbeat(connectedAt.Add(time.Minute), 1),
	); applied || !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("RecordHeartbeat(at terminal reserve) = %t, %v, want false/ErrCapacityExceeded", applied, err)
	}
	if applied, err := store.Disconnect(
		ctx,
		lease,
		connectedAt.Add(2*time.Minute),
	); err != nil || !applied {
		t.Fatalf("Disconnect(at terminal reserve) = %t, %v, want true/nil", applied, err)
	}
	got, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TelemetryRevision != uint64(math.MaxInt64) ||
		got.ActiveLease != nil ||
		got.DisconnectedAt == nil {
		t.Fatalf("terminal disconnect = %#v", got)
	}
}

func TestDisconnectIsConditionalAndDoesNotReleaseNewerLease(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 28, 22, 0, 0, 0, time.UTC)
	_, oldLease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	replacement := testClaim(connectedAt.Add(time.Minute))
	replacement.StreamID = "stream-new"
	_, newLease, err := store.Claim(ctx, replacement)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := store.Disconnect(
		ctx,
		oldLease,
		connectedAt.Add(2*time.Minute),
	); err != nil || applied {
		t.Fatalf("Disconnect(old lease) = %t, %v", applied, err)
	}
	got, err := store.Get(ctx, newLease.Scope, newLease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveLease == nil || got.ActiveLease.StreamID != newLease.StreamID {
		t.Fatalf("old cleanup released new lease: %#v", got)
	}
	if applied, err := store.Disconnect(
		ctx,
		newLease,
		connectedAt.Add(3*time.Minute),
	); err != nil || !applied {
		t.Fatalf("Disconnect(current lease) = %t, %v", applied, err)
	}
	got, err = store.Get(ctx, newLease.Scope, newLease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveLease != nil ||
		got.DisconnectedAt == nil ||
		got.TelemetryRevision != 3 {
		t.Fatalf("disconnected collector = %#v", got)
	}
}

func TestConcurrentClaimsAllocateUniqueMonotonicGenerations(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	baseTime := time.Date(2026, 7, 28, 23, 0, 0, 0, time.UTC)
	const contenders = 16
	start := make(chan struct{})
	leases := make(chan Lease, contenders)
	errs := make(chan error, contenders)
	var workers sync.WaitGroup
	for contender := range contenders {
		workers.Add(1)
		go func() {
			defer workers.Done()
			request := testClaim(baseTime.Add(time.Duration(contender) * time.Second))
			request.StreamID = fmt.Sprintf("stream-%d", contender)
			request.Hello.InstanceID = fmt.Sprintf("instance-%d", contender)
			<-start
			_, lease, err := store.Claim(ctx, request)
			if err != nil {
				errs <- err
				return
			}
			leases <- lease
		}()
	}
	close(start)
	workers.Wait()
	close(leases)
	close(errs)
	for err := range errs {
		t.Fatalf("Claim(concurrent): %v", err)
	}
	byGeneration := make(map[uint64]Lease, contenders)
	for lease := range leases {
		byGeneration[lease.Generation] = lease
	}
	if len(byGeneration) != contenders {
		t.Fatalf("unique generations = %d, want %d: %#v", len(byGeneration), contenders, byGeneration)
	}
	for generation := uint64(1); generation <= contenders; generation++ {
		if _, exists := byGeneration[generation]; !exists {
			t.Fatalf("missing generation %d: %#v", generation, byGeneration)
		}
	}
	got, err := store.Get(ctx, Scope{TenantID: "tenant-a"}, testClaim(baseTime).CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	winner := byGeneration[contenders]
	if got.ActiveLease == nil ||
		got.ActiveLease.Generation != contenders ||
		got.ActiveLease.StreamID != winner.StreamID {
		t.Fatalf("active concurrent winner = %#v, want %#v", got.ActiveLease, winner)
	}
	for generation, lease := range byGeneration {
		applied, err := store.RecordHeartbeat(
			ctx,
			lease,
			testHeartbeat(baseTime.Add(time.Hour), 1),
		)
		if err != nil {
			t.Fatalf("RecordHeartbeat(generation %d): %v", generation, err)
		}
		if applied != (generation == contenders) {
			t.Fatalf("generation %d applied = %t", generation, applied)
		}
	}
}

func TestConcurrentHeartbeatsPersistHighestObservationSequence(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	const observations = 32
	start := make(chan struct{})
	errs := make(chan error, observations)
	var workers sync.WaitGroup
	for sequence := uint64(1); sequence <= observations; sequence++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			heartbeat := testHeartbeat(
				connectedAt.Add(time.Duration(sequence)*time.Second),
				sequence,
			)
			heartbeat.Queue.QueuedEvents = sequence
			<-start
			_, err := store.RecordHeartbeat(ctx, lease, heartbeat)
			errs <- err
		}()
	}
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("RecordHeartbeat(concurrent): %v", err)
		}
	}
	got, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ObservationSequence != observations ||
		got.Queue.QueuedEvents != observations ||
		!got.LastSeenAt.Equal(connectedAt.Add(observations*time.Second)) {
		t.Fatalf("latest concurrent heartbeat = %#v", got)
	}
	if got.Version != 1 {
		t.Fatalf("heartbeats changed admin CAS version: %d", got.Version)
	}
}

func TestConcurrentAdministrationUpdatesHaveOneCASWinner(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 29, 0, 30, 0, 0, time.UTC)
	collector, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	const contenders = 8
	start := make(chan struct{})
	results := make(chan AdministrationSnapshot, contenders)
	errs := make(chan error, contenders)
	var workers sync.WaitGroup
	for contender := range contenders {
		workers.Add(1)
		go func() {
			defer workers.Done()
			displayName := fmt.Sprintf("collector-%d", contender)
			<-start
			result, updateErr := store.UpdateAdministration(
				ctx,
				lease.Scope,
				lease.CollectorID,
				collector.Version,
				Administration{
					State:       AdministrativeStateEnabled,
					DisplayName: &displayName,
				},
				connectedAt.Add(time.Minute),
			)
			if updateErr != nil {
				errs <- updateErr
				return
			}
			results <- result
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errs)

	var winners []AdministrationSnapshot
	for result := range results {
		winners = append(winners, result)
	}
	if len(winners) != 1 {
		t.Fatalf("successful administration updates = %d, want 1", len(winners))
	}
	conflicts := 0
	for updateErr := range errs {
		if !errors.Is(updateErr, control.ErrVersionConflict) {
			t.Fatalf("losing administration error = %v, want ErrVersionConflict", updateErr)
		}
		conflicts++
	}
	if conflicts != contenders-1 {
		t.Fatalf("administration conflicts = %d, want %d", conflicts, contenders-1)
	}
	got, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 2 ||
		got.DisplayName == nil ||
		winners[0].DisplayName == nil ||
		*got.DisplayName != *winners[0].DisplayName ||
		got.TelemetryRevision != 1 {
		t.Fatalf("persisted administration = %#v, winner = %#v", got, winners[0])
	}
}

func TestFleetStateSurvivesDatabaseReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	database, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(database)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	connectedAt := time.Date(2026, 7, 29, 1, 0, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		lease,
		testHeartbeat(connectedAt.Add(time.Minute), 1),
	); err != nil || !applied {
		_ = database.Close()
		t.Fatalf("RecordHeartbeat(): %t, %v", applied, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedStore, err := New(reopened)
	if err != nil {
		t.Fatal(err)
	}
	bootStartedAt := connectedAt.Add(2 * time.Minute)
	invalidated, err := reopenedStore.InvalidatePriorBootLeases(
		ctx,
		"server-boot-2",
		bootStartedAt,
	)
	if err != nil {
		t.Fatalf("InvalidatePriorBootLeases(): %v", err)
	}
	if invalidated != 1 {
		t.Fatalf("invalidated prior-boot leases = %d, want 1", invalidated)
	}
	got, err := reopenedStore.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveLease != nil ||
		got.DisconnectedAt == nil ||
		!got.DisconnectedAt.Equal(bootStartedAt) ||
		got.TelemetryRevision != 3 ||
		got.ObservationSequence != 1 ||
		got.Version != 1 ||
		got.LeaseGeneration != lease.Generation ||
		len(got.InputHealth) != 1 {
		t.Fatalf("boot-invalidated reopened collector = %#v", got)
	}
	if applied, err := reopenedStore.RecordHeartbeat(
		ctx,
		lease,
		testHeartbeat(connectedAt.Add(3*time.Minute), 2),
	); err != nil || applied {
		t.Fatalf("RecordHeartbeat(prior boot) = %t, %v, want false/nil", applied, err)
	}
	if repeated, err := reopenedStore.InvalidatePriorBootLeases(
		ctx,
		"server-boot-2",
		bootStartedAt.Add(time.Minute),
	); err != nil || repeated != 0 {
		t.Fatalf("repeated boot invalidation = %d, %v, want 0/nil", repeated, err)
	}
	freshClaim := testClaim(connectedAt.Add(4 * time.Minute))
	freshClaim.BootEpoch = "server-boot-2"
	freshClaim.StreamID = "stream-after-restart"
	fresh, freshLease, err := reopenedStore.Claim(ctx, freshClaim)
	if err != nil {
		t.Fatalf("Claim(current boot): %v", err)
	}
	if fresh.ActiveLease == nil ||
		fresh.ActiveLease.BootEpoch != "server-boot-2" ||
		freshLease.Generation != lease.Generation+1 {
		t.Fatalf("fresh post-restart claim = %#v lease=%#v", fresh, freshLease)
	}
}

func TestPriorBootInvalidationIsGlobalIdempotentAndPreservesCurrentBoot(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	baseTime := time.Date(2026, 7, 29, 1, 30, 0, 0, time.UTC)

	oldA := testClaim(baseTime)
	oldA.Scope = Scope{TenantID: "tenant-a"}
	_, oldALease, err := store.Claim(ctx, oldA)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := store.RecordHeartbeat(
		ctx,
		oldALease,
		testHeartbeat(baseTime.Add(10*time.Minute), 1),
	); err != nil || !applied {
		t.Fatalf("RecordHeartbeat(old tenant A) = %t, %v", applied, err)
	}

	oldB := testClaim(baseTime.Add(time.Minute))
	oldB.Scope = Scope{TenantID: "tenant-b"}
	oldB.BootEpoch = "server-boot-old-b"
	oldB.StreamID = "stream-old-b"
	_, oldBLease, err := store.Claim(ctx, oldB)
	if err != nil {
		t.Fatal(err)
	}

	current := testClaim(baseTime.Add(2 * time.Minute))
	current.Scope = Scope{TenantID: "tenant-c"}
	current.BootEpoch = "server-boot-current"
	current.StreamID = "stream-current"
	_, currentLease, err := store.Claim(ctx, current)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE collector_runtime
		SET telemetry_revision = ?
		WHERE tenant_id = ? AND collector_id = ?`,
		int64(math.MaxInt64),
		oldBLease.TenantID,
		oldBLease.CollectorID,
	); err != nil {
		t.Fatalf("saturate old tenant B telemetry: %v", err)
	}
	invalidatedAt := baseTime.Add(5 * time.Minute)
	invalidated, err := store.InvalidatePriorBootLeases(
		ctx,
		"server-boot-current",
		invalidatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if invalidated != 2 {
		t.Fatalf("invalidated leases = %d, want 2", invalidated)
	}

	gotA, err := store.Get(ctx, oldALease.Scope, oldALease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if gotA.ActiveLease != nil ||
		gotA.DisconnectedAt == nil ||
		!gotA.DisconnectedAt.Equal(baseTime.Add(10*time.Minute)) ||
		gotA.Version != 1 ||
		gotA.LeaseGeneration != oldALease.Generation {
		t.Fatalf("invalidated tenant A collector = %#v", gotA)
	}
	gotB, err := store.Get(ctx, oldBLease.Scope, oldBLease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if gotB.ActiveLease != nil ||
		gotB.TelemetryRevision != uint64(math.MaxInt64) ||
		gotB.Version != 1 {
		t.Fatalf("invalidated saturated tenant B collector = %#v", gotB)
	}
	gotCurrent, err := store.Get(ctx, currentLease.Scope, currentLease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if gotCurrent.ActiveLease == nil ||
		gotCurrent.ActiveLease.BootEpoch != "server-boot-current" {
		t.Fatalf("current-boot collector was invalidated: %#v", gotCurrent)
	}
	if repeated, err := store.InvalidatePriorBootLeases(
		ctx,
		"server-boot-current",
		invalidatedAt.Add(time.Minute),
	); err != nil || repeated != 0 {
		t.Fatalf("repeated invalidation = %d, %v, want 0/nil", repeated, err)
	}
}

func TestPriorBootInvalidationLinearizesWithCurrentBootClaim(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t)
	ctx := context.Background()
	baseTime := time.Date(2026, 7, 29, 1, 40, 0, 0, time.UTC)
	if _, _, err := store.Claim(ctx, testClaim(baseTime)); err != nil {
		t.Fatal(err)
	}
	currentClaim := testClaim(baseTime.Add(time.Minute))
	currentClaim.BootEpoch = "server-boot-current"
	currentClaim.StreamID = "stream-current"
	currentClaim.Hello.InstanceID = "instance-current"

	start := make(chan struct{})
	claimResult := make(chan Lease, 1)
	errs := make(chan error, 2)
	go func() {
		<-start
		_, lease, err := store.Claim(ctx, currentClaim)
		if err == nil {
			claimResult <- lease
		}
		errs <- err
	}()
	go func() {
		<-start
		_, err := store.InvalidatePriorBootLeases(
			ctx,
			"server-boot-current",
			baseTime.Add(2*time.Minute),
		)
		errs <- err
	}()
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent boot transition: %v", err)
		}
	}
	lease := <-claimResult
	got, err := store.Get(ctx, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveLease == nil ||
		got.ActiveLease.BootEpoch != "server-boot-current" ||
		got.ActiveLease.StreamID != "stream-current" {
		t.Fatalf("current boot claim lost to startup invalidation: %#v", got)
	}
}

func TestGetUsesWALReadTransactionAlongsideWriter(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	ctx := context.Background()
	connectedAt := time.Date(2026, 7, 29, 1, 45, 0, 0, time.UTC)
	_, lease, err := store.Claim(ctx, testClaim(connectedAt))
	if err != nil {
		t.Fatal(err)
	}
	writer := database.GORMDB().WithContext(ctx).Begin()
	if writer.Error != nil {
		t.Fatalf("begin writer: %v", writer.Error)
	}
	defer writer.Rollback()

	readContext, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	got, err := store.Get(readContext, lease.Scope, lease.CollectorID)
	if err != nil {
		t.Fatalf("Get() alongside reserved writer: %v", err)
	}
	if got.ActiveLease == nil || got.ActiveLease.StreamID != lease.StreamID {
		t.Fatalf("concurrent WAL read = %#v", got)
	}
}

func TestStoreValidatesContextAndBounds(t *testing.T) {
	t.Parallel()

	database, store := openTestStore(t)
	if _, err := New(nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("New(nil) error = %v, want ErrInvalidArgument", err)
	}
	if _, err := New(&control.DB{}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("New(empty DB) error = %v, want ErrInvalidArgument", err)
	}
	if database.GORMDB() == nil {
		t.Fatal("test database unexpectedly lacks GORM")
	}
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 2, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*ClaimRequest)
	}{
		{name: "tenant", mutate: func(request *ClaimRequest) {
			request.TenantID = ""
		}},
		{name: "padded tenant", mutate: func(request *ClaimRequest) {
			request.TenantID = " tenant-a "
		}},
		{name: "collector ID", mutate: func(request *ClaimRequest) {
			request.CollectorID = "/invalid"
		}},
		{name: "boot epoch", mutate: func(request *ClaimRequest) {
			request.BootEpoch = ""
		}},
		{name: "stream ID", mutate: func(request *ClaimRequest) {
			request.StreamID = ""
		}},
		{name: "instance ID", mutate: func(request *ClaimRequest) {
			request.Hello.InstanceID = ""
		}},
		{name: "receive time", mutate: func(request *ClaimRequest) {
			request.ReceivedAt = time.Time{}
		}},
		{name: "started time", mutate: func(request *ClaimRequest) {
			request.Hello.StartedAt = time.Time{}
		}},
		{name: "receive time above public range", mutate: func(request *ClaimRequest) {
			request.ReceivedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
		{name: "started time above public range", mutate: func(request *ClaimRequest) {
			request.Hello.StartedAt = time.UnixMicro(math.MaxInt64)
		}},
		{name: "too many capabilities", mutate: func(request *ClaimRequest) {
			request.Hello.Capabilities = make([]uint32, maximumCapabilities+1)
			for index := range request.Hello.Capabilities {
				request.Hello.Capabilities[index] = uint32(index + 1)
			}
		}},
		{name: "unauthorized input index", mutate: func(request *ClaimRequest) {
			request.Hello.Inputs[0].IndexName = "other"
		}},
		{name: "duplicate input ID", mutate: func(request *ClaimRequest) {
			request.Hello.Inputs = append(request.Hello.Inputs, request.Hello.Inputs[0])
		}},
		{name: "oversized aggregate", mutate: func(request *ClaimRequest) {
			request.Hello.Inputs[0].Source = stringPointer(
				strings.Repeat("x", maximumSourceBytes+1),
			)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := testClaim(now)
			test.mutate(&request)
			if _, _, err := store.Claim(ctx, request); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("Claim() error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := store.Claim(canceled, testClaim(now)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Claim(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := store.Get(canceled, Scope{TenantID: "tenant-a"}, "collector"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get(canceled) error = %v, want context.Canceled", err)
	}
}

func withLeaseBoot(lease Lease, boot string) Lease {
	lease.BootEpoch = boot
	return lease
}

func withLeaseGeneration(lease Lease, generation uint64) Lease {
	lease.Generation = generation
	return lease
}

func withLeaseStream(lease Lease, stream string) Lease {
	lease.StreamID = stream
	return lease
}

func withLeaseTenant(lease Lease, tenant string) Lease {
	lease.TenantID = tenant
	return lease
}

func stringPointer(value string) *string {
	return &value
}
