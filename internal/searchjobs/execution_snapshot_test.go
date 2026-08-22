package searchjobs

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

var (
	retainedKnowledgeAuthorityDigestsSink RetainedKnowledgeAuthorityDigests
	retainedKnowledgeAuthorityErrorSink   error
)

func TestCompletedExecutionSnapshotForReturnsDetachedExecutionMetadata(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 22, 8, 9, 10, 11, time.FixedZone("west", -7*60*60))
	clock := &fakeClock{now: now}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("ready")})
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			return 91, nil
		}),
		RetentionTTL:    time.Hour,
		CleanupInterval: -1,
		Now:             clock.Now,
		NewID:           sequenceIDs("execution-snapshot"),
	})
	earliest := time.Date(2026, time.July, 20, 1, 2, 3, 4, time.FixedZone("east", 2*60*60))
	latest := time.Date(2026, time.July, 21, 5, 6, 7, 8, time.FixedZone("east", 2*60*60))
	request := CreateRequest{
		SPL:               " index=alpha | table message ",
		OwnerID:           "snapshot-owner",
		TenantID:          "snapshot-tenant",
		AuthorizedIndexes: []string{"beta", "alpha"},
		RequestedIndexes:  []string{"alpha"},
		TimeRange:         mustAbsoluteTimeRange(earliest, latest),
	}
	job, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	completed := waitForState(t, manager, job.ID, StateCompleted)
	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}

	snapshot, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor() error = %v", err)
	}
	legacyAuthority, err := snapshot.ValidateRetainedKnowledgeAuthority()
	if err != nil || legacyAuthority != (RetainedKnowledgeAuthorityDigests{}) {
		t.Fatalf(
			"ValidateRetainedKnowledgeAuthority(sealed legacy) = (%#v, %v), want absent",
			legacyAuthority,
			err,
		)
	}
	legacyPrelude, legacyPreludePresent, openPreludeErr := snapshot.OpenRetainedKnowledgePrelude()
	if openPreludeErr != nil || legacyPreludePresent || !legacyPrelude.IsZero() {
		t.Fatalf(
			"OpenRetainedKnowledgePrelude(sealed legacy) = (%#v, %t, %v)",
			legacyPrelude,
			legacyPreludePresent,
			openPreludeErr,
		)
	}
	want := ExecutionSnapshot{
		ID:               completed.ID,
		OwnerID:          request.OwnerID,
		TenantID:         request.TenantID,
		SPL:              request.SPL,
		EffectiveIndexes: []string{"alpha"},
		Earliest:         earliest.UTC(),
		Latest:           latest.UTC(),
		SearchStart:      completed.CreatedAt,
		SearchTimezone:   "UTC",
		IndexTimeCutoff:  now.UTC(),
		VisibilityCutoff: 91,
		FinishedAt:       now.UTC(),
		ExpiresAt:        now.UTC().Add(time.Hour),

		knowledgeAuthoritySeal: snapshot.knowledgeAuthoritySeal}
	if !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("CompletedExecutionSnapshotFor() = %#v, want %#v", snapshot, want)
	}

	snapshot.EffectiveIndexes[0] = "mutated"
	fresh, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fresh.EffectiveIndexes, []string{"alpha"}) {
		t.Fatalf("stored effective indexes changed through returned snapshot: %v", fresh.EffectiveIndexes)
	}
	assertLeaseCounts(t, manager, job.ID, 0, 0)
}

func TestOpenRetainedKnowledgeExecutionValidatesAppAndDetaches(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			_ clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			return sink.SetSchema(messageSchema())
		}),
		KnowledgeResolver: resolver,
		CleanupInterval:   -1,
		NewID:             sequenceIDs("retained-knowledge-execution"),
	})
	request := validRequest()
	request.AppID = appID
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	snapshot, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		completed.ID,
	)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(): %v", err)
	}
	authority, err := snapshot.ValidateRetainedKnowledgeAuthority()
	if err != nil || !authority.Present ||
		authority.SnapshotDigest != snapshot.KnowledgeSnapshot.Digest() {
		t.Fatalf("ValidateRetainedKnowledgeAuthority() = (%#v, %v)", authority, err)
	}
	if snapshot.CompiledQuery == nil {
		t.Fatal("enabled retained snapshot compiled query is nil")
	}
	compiledDigest, ok := snapshot.CompiledQuery.ExecutionAuthorityDigest()
	if !ok || authority.CompiledDigest != compiledDigest {
		t.Fatalf(
			"validated compiled digest = (%x, %t), want %x",
			authority.CompiledDigest,
			ok,
			compiledDigest,
		)
	}
	sameSnapshot, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		completed.ID,
	)
	if err != nil || !snapshot.Equal(sameSnapshot) || !sameSnapshot.Equal(snapshot) {
		t.Fatalf("fresh snapshot equality = (%t/%t, %v), want true/true", snapshot.Equal(sameSnapshot), sameSnapshot.Equal(snapshot), err)
	}
	detachedAuthority := authority
	detachedAuthority.SnapshotDigest[0] ^= 0xff
	detachedAuthority.CompiledDigest[0] ^= 0xff
	freshAuthority, err := snapshot.ValidateRetainedKnowledgeAuthority()
	if err != nil || freshAuthority != authority ||
		!snapshot.Equal(sameSnapshot) || !sameSnapshot.Equal(snapshot) {
		t.Fatalf(
			"fixed digest result was not detached: fresh=%#v original=%#v equality=%t/%t err=%v",
			freshAuthority,
			authority,
			snapshot.Equal(sameSnapshot),
			sameSnapshot.Equal(snapshot),
			err,
		)
	}

	retained, err := snapshot.OpenRetainedKnowledgeExecution()
	if err != nil || retained == nil || retained.KnowledgeSummary == nil ||
		len(retained.CompiledQuery.OutputFields) == 0 || retained.KnowledgePrelude.IsZero() ||
		!retained.KnowledgePrelude.IsEmpty() {
		t.Fatalf("OpenRetainedKnowledgeExecution() = (%#v, %v)", retained, err)
	}
	wantPrelude := snapshot.KnowledgeSnapshot.Prelude()
	if wantPrelude.IsZero() || !retained.KnowledgePrelude.Equal(wantPrelude) {
		t.Fatal("opened knowledge execution prelude disagrees with retained snapshot")
	}
	wantFields := slices.Clone(retained.CompiledQuery.OutputFields)
	wantDigest := bytes.Clone(retained.KnowledgeSummary.GetRef().GetSnapshotSha256())
	retained.CompiledQuery.OutputFields[0] = "mutated"
	retained.KnowledgeSummary.Ref.SnapshotSha256[0] ^= 0xff
	retained.KnowledgePrelude = knowledgeprogram.Program{}

	fresh, err := snapshot.OpenRetainedKnowledgeExecution()
	if err != nil || fresh == nil || fresh.KnowledgeSummary == nil ||
		!fresh.KnowledgePrelude.Equal(wantPrelude) {
		t.Fatalf("OpenRetainedKnowledgeExecution(fresh) = (%#v, %v)", fresh, err)
	}
	if !slices.Equal(fresh.CompiledQuery.OutputFields, wantFields) ||
		!bytes.Equal(fresh.KnowledgeSummary.GetRef().GetSnapshotSha256(), wantDigest) {
		t.Fatal("opened knowledge execution aliases retained authority")
	}

	drifted := snapshot
	drifted.AppID = "app_bbbbbbbbbbbbbbbbbbbbbB"
	if invalid, validateErr := drifted.ValidateRetainedKnowledgeAuthority(); invalid != (RetainedKnowledgeAuthorityDigests{}) ||
		!errors.Is(validateErr, ErrResultsUnavailable) {
		t.Fatalf(
			"ValidateRetainedKnowledgeAuthority(AppID drift) = (%#v, %v), want zero/unavailable",
			invalid,
			validateErr,
		)
	}
	if opened, openErr := drifted.OpenRetainedKnowledgeExecution(); opened != nil || !errors.Is(openErr, ErrResultsUnavailable) {
		t.Fatalf("OpenRetainedKnowledgeExecution(AppID drift) = (%#v, %v)", opened, openErr)
	}

	prelude, present, err := snapshot.OpenRetainedKnowledgePrelude()
	if err != nil || !present || prelude.IsZero() || !prelude.IsEmpty() ||
		!prelude.Equal(wantPrelude) {
		t.Fatalf(
			"OpenRetainedKnowledgePrelude() = (zero:%t empty:%t present:%t, %v)",
			prelude.IsZero(),
			prelude.IsEmpty(),
			present,
			err,
		)
	}
	freshPrelude, present, err := snapshot.OpenRetainedKnowledgePrelude()
	if err != nil || !present || !freshPrelude.Equal(wantPrelude) {
		t.Fatalf("OpenRetainedKnowledgePrelude(fresh) = (%#v, %t, %v)", freshPrelude, present, err)
	}

	unsignedLegacy := ExecutionSnapshot{
		ID:       "unsigned-legacy",
		TenantID: "legacy-tenant",
		SPL:      "index=legacy",
	}
	if invalid, validateErr := unsignedLegacy.ValidateRetainedKnowledgeAuthority(); invalid != (RetainedKnowledgeAuthorityDigests{}) ||
		!errors.Is(validateErr, ErrResultsUnavailable) {
		t.Fatalf(
			"ValidateRetainedKnowledgeAuthority(unsigned legacy) = (%#v, %v), want zero/unavailable",
			invalid,
			validateErr,
		)
	}
	legacyPrelude, present, err := unsignedLegacy.OpenRetainedKnowledgePrelude()
	if !legacyPrelude.IsZero() || present || !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf(
			"OpenRetainedKnowledgePrelude(unsigned legacy) = (%#v, %t, %v), want unavailable",
			legacyPrelude,
			present,
			err,
		)
	}

	reconstructed := ExecutionSnapshot{
		ID:               snapshot.ID,
		OwnerID:          snapshot.OwnerID,
		TenantID:         snapshot.TenantID,
		AppID:            snapshot.AppID,
		SPL:              snapshot.SPL,
		EffectiveIndexes: slices.Clone(snapshot.EffectiveIndexes),
		Earliest:         snapshot.Earliest,
		Latest:           snapshot.Latest,
		SearchStart:      snapshot.SearchStart,
		SearchTimezone:   snapshot.SearchTimezone,
		IndexTimeCutoff:  snapshot.IndexTimeCutoff,
		VisibilityCutoff: snapshot.VisibilityCutoff,
		FinishedAt:       snapshot.FinishedAt,
		ExpiresAt:        snapshot.ExpiresAt,
	}
	reconstructedPrelude, present, err := reconstructed.OpenRetainedKnowledgePrelude()
	if !reconstructedPrelude.IsZero() || present || !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf(
			"OpenRetainedKnowledgePrelude(reconstructed downgrade) = (%#v, %t, %v), want unavailable",
			reconstructedPrelude,
			present,
			err,
		)
	}

	for _, test := range []struct {
		name   string
		mutate func(*ExecutionSnapshot)
	}{
		{
			name: "compiled only",
			mutate: func(value *ExecutionSnapshot) {
				value.KnowledgeSnapshot = ExecutionSnapshot{}.KnowledgeSnapshot
			},
		},
		{
			name: "snapshot only",
			mutate: func(value *ExecutionSnapshot) {
				value.CompiledQuery = nil
			},
		},
		{name: "both nonzero"},
	} {
		t.Run("unsigned "+test.name, func(t *testing.T) {
			unsigned := snapshot
			unsigned.knowledgeAuthoritySeal = knowledgeExecutionAuthoritySeal{}
			if test.mutate != nil {
				test.mutate(&unsigned)
			}
			if invalid, validateErr := unsigned.ValidateRetainedKnowledgeAuthority(); invalid != (RetainedKnowledgeAuthorityDigests{}) ||
				!errors.Is(validateErr, ErrResultsUnavailable) {
				t.Fatalf(
					"ValidateRetainedKnowledgeAuthority() = (%#v, %v), want zero/unavailable",
					invalid,
					validateErr,
				)
			}
			opened, openedPresent, openErr := unsigned.OpenRetainedKnowledgePrelude()
			if !opened.IsZero() || openedPresent || !errors.Is(openErr, ErrResultsUnavailable) {
				t.Fatalf(
					"OpenRetainedKnowledgePrelude() = (%#v, %t, %v), want unavailable",
					opened,
					openedPresent,
					openErr,
				)
			}
		})
	}

	stripped := snapshot
	stripped.KnowledgeSnapshot = ExecutionSnapshot{}.KnowledgeSnapshot
	stripped.CompiledQuery = nil
	if invalid, validateErr := stripped.ValidateRetainedKnowledgeAuthority(); invalid != (RetainedKnowledgeAuthorityDigests{}) ||
		!errors.Is(validateErr, ErrResultsUnavailable) {
		t.Fatalf(
			"ValidateRetainedKnowledgeAuthority(stripped) = (%#v, %v), want zero/unavailable",
			invalid,
			validateErr,
		)
	}
	strippedPrelude, present, err := stripped.OpenRetainedKnowledgePrelude()
	if !strippedPrelude.IsZero() || present || !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf(
			"OpenRetainedKnowledgePrelude(stripped sealed authority) = (%#v, %t, %v), want unavailable",
			strippedPrelude,
			present,
			err,
		)
	}
}

func TestValidateRetainedKnowledgeAuthorityHasAbsoluteAllocationBound(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			_ context.Context,
			_ clickhouse.CompiledQuery,
			sink ResultSink,
		) error {
			return sink.SetSchema(messageSchema())
		}),
		KnowledgeResolver: resolver,
		CleanupInterval:   -1,
		NewID:             sequenceIDs("retained-authority-allocation"),
	})
	request := validRequest()
	request.AppID = appID
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	snapshot, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		created.ID,
	)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(): %v", err)
	}
	validate := func() {
		retainedKnowledgeAuthorityDigestsSink,
			retainedKnowledgeAuthorityErrorSink =
			snapshot.ValidateRetainedKnowledgeAuthority()
	}
	validate()
	if retainedKnowledgeAuthorityErrorSink != nil ||
		!retainedKnowledgeAuthorityDigestsSink.Present {
		t.Fatalf(
			"ValidateRetainedKnowledgeAuthority() = (%#v, %v)",
			retainedKnowledgeAuthorityDigestsSink,
			retainedKnowledgeAuthorityErrorSink,
		)
	}
	allocs := testing.AllocsPerRun(1_000, validate)
	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			validate()
		}
	})
	// Ed25519 verification has a fixed implementation cost in this toolchain;
	// keep a conservative absolute ceiling far below even the smallest retained
	// snapshot payload, independently of snapshot encoding or object count.
	if allocs > 256 || result.AllocsPerOp() > 256 ||
		result.AllocedBytesPerOp() > 16<<10 {
		t.Fatalf(
			"ValidateRetainedKnowledgeAuthority allocation bound = %.2f allocs/run, benchmark %d allocs %d bytes/op; want <=256 and <=16 KiB",
			allocs,
			result.AllocsPerOp(),
			result.AllocedBytesPerOp(),
		)
	}
}

func TestExecutionSnapshotEqualCoversEveryFieldAndIndexOrder(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, time.July, 28, 1, 2, 3, 4, time.UTC)
	base := ExecutionSnapshot{
		ID:               "job",
		OwnerID:          "owner",
		TenantID:         "tenant",
		SPL:              "index=alpha",
		EffectiveIndexes: []string{"alpha", "beta"},
		Earliest:         baseTime,
		Latest:           baseTime.Add(time.Hour),
		SearchStart:      baseTime.Add(2 * time.Hour),
		SearchTimezone:   "UTC",
		IndexTimeCutoff:  baseTime.Add(3 * time.Hour),
		VisibilityCutoff: 41,
		FinishedAt:       baseTime.Add(4 * time.Hour),
		ExpiresAt:        baseTime.Add(5 * time.Hour),
	}
	sealManager := &Manager{knowledgeExecutionSigner: deriveKnowledgeExecutionSigningKey(
		[]byte("execution-snapshot-equality-test-key-at-least-32-bytes"),
		"execution-snapshot-equality",
		"epoch",
	)}
	if _, ok := sealManager.sealExecutionSnapshot(
		&base,
		executionResultMetadata{
			jobID:      base.ID,
			generation: 1,
			schema: Schema{Columns: []Column{{
				Name: "message",
				Kind: ValueKindString,
			}}},
		},
	); !ok {
		t.Fatal("seal equality fixture")
	}
	if !base.Equal(base) {
		t.Fatal("snapshot is not equal to itself")
	}

	tests := []struct {
		name   string
		mutate func(*ExecutionSnapshot)
	}{
		{name: "ID", mutate: func(snapshot *ExecutionSnapshot) { snapshot.ID += "-changed" }},
		{name: "owner ID", mutate: func(snapshot *ExecutionSnapshot) { snapshot.OwnerID += "-changed" }},
		{name: "tenant ID", mutate: func(snapshot *ExecutionSnapshot) { snapshot.TenantID += "-changed" }},
		{name: "app ID", mutate: func(snapshot *ExecutionSnapshot) { snapshot.AppID = "changed-app" }},
		{name: "SPL", mutate: func(snapshot *ExecutionSnapshot) { snapshot.SPL += " | head 1" }},
		{name: "effective index order", mutate: func(snapshot *ExecutionSnapshot) {
			snapshot.EffectiveIndexes = []string{"beta", "alpha"}
		}},
		{name: "earliest", mutate: func(snapshot *ExecutionSnapshot) {
			snapshot.Earliest = snapshot.Earliest.Add(time.Nanosecond)
		}},
		{name: "latest", mutate: func(snapshot *ExecutionSnapshot) {
			snapshot.Latest = snapshot.Latest.Add(time.Nanosecond)
		}},
		{name: "search start", mutate: func(snapshot *ExecutionSnapshot) {
			snapshot.SearchStart = snapshot.SearchStart.Add(time.Nanosecond)
		}},
		{name: "search timezone", mutate: func(snapshot *ExecutionSnapshot) {
			snapshot.SearchTimezone = "America/Los_Angeles"
		}},
		{name: "index-time cutoff", mutate: func(snapshot *ExecutionSnapshot) {
			snapshot.IndexTimeCutoff = snapshot.IndexTimeCutoff.Add(time.Nanosecond)
		}},
		{name: "visibility cutoff", mutate: func(snapshot *ExecutionSnapshot) {
			snapshot.VisibilityCutoff++
		}},
		{name: "finished at", mutate: func(snapshot *ExecutionSnapshot) {
			snapshot.FinishedAt = snapshot.FinishedAt.Add(time.Nanosecond)
		}},
		{name: "expires at", mutate: func(snapshot *ExecutionSnapshot) {
			snapshot.ExpiresAt = snapshot.ExpiresAt.Add(time.Nanosecond)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			test.mutate(&changed)
			if base.Equal(changed) || changed.Equal(base) {
				t.Fatal("field mutation remained equal")
			}
		})
	}
}

func TestCompletedExecutionSnapshotForContextScopeAndLifecycleErrors(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
			}
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return nil
		}),
		MaxConcurrent:   1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("execution-lifecycle"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateRunning)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	var nilContext context.Context

	if _, err := manager.CompletedExecutionSnapshotFor(nilContext, access, job.ID); err == nil {
		t.Fatal("CompletedExecutionSnapshotFor(nil context) error = nil")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.CompletedExecutionSnapshotFor(canceled, access, job.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("CompletedExecutionSnapshotFor(canceled context) = %v, want context.Canceled", err)
	}
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CompletedExecutionSnapshotFor(missing) = %v, want ErrNotFound", err)
	}
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), AccessScope{TenantID: "other", OwnerID: "owner"}, job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CompletedExecutionSnapshotFor(cross tenant) = %v, want ErrNotFound", err)
	}
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "other"}, job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CompletedExecutionSnapshotFor(cross owner) = %v, want ErrNotFound", err)
	}
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, job.ID); !errors.Is(err, ErrResultsNotReady) {
		t.Fatalf("CompletedExecutionSnapshotFor(active) = %v, want ErrResultsNotReady", err)
	}

	if err := manager.Cancel(job.ID); err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCanceled)
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, job.ID); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("CompletedExecutionSnapshotFor(canceled) = %v, want ErrResultsUnavailable", err)
	}
	close(release)

	failedManager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			return errors.New("untrusted executor detail")
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("execution-failed"),
	})
	failed, err := failedManager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, failedManager, failed.ID, StateFailed)
	if _, err := failedManager.CompletedExecutionSnapshotFor(context.Background(), access, failed.ID); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("CompletedExecutionSnapshotFor(failed) = %v, want ErrResultsUnavailable", err)
	}

	closedManager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("execution-closed"),
	})
	closedJob, err := closedManager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, closedManager, closedJob.ID, StateCompleted)
	if err := closedManager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := closedManager.CompletedExecutionSnapshotFor(context.Background(), access, closedJob.ID); !errors.Is(err, ErrClosed) {
		t.Fatalf("CompletedExecutionSnapshotFor(closed manager) = %v, want ErrClosed", err)
	}
}

func TestCompletedExecutionSnapshotForExpiresAtExactDeadlineWithoutNewLease(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 22, 10, 0, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("pinned")})
		}),
		MaxResultLeases:       1,
		MaxResultLeasesPerJob: 1,
		RetentionTTL:          10 * time.Second,
		CleanupInterval:       -1,
		Now:                   clock.Now,
		NewID:                 sequenceIDs("execution-expiry"),
	})
	completed := createCompletedMessageJob(t, manager)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	lease, err := manager.AcquireResultsFor(context.Background(), access, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	assertLeaseCounts(t, manager, completed.ID, 1, 1)
	if _, err := manager.AcquireResultsFor(context.Background(), access, completed.ID); !errors.Is(err, ErrCapacity) {
		t.Fatalf("second AcquireResultsFor() = %v, want ErrCapacity", err)
	}
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, completed.ID); err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(with saturated lease capacity) = %v", err)
	}
	assertLeaseCounts(t, manager, completed.ID, 1, 1)

	clock.Add(10*time.Second - time.Nanosecond)
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, completed.ID); err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(before expiry) = %v", err)
	}
	clock.Add(time.Nanosecond)
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, completed.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("CompletedExecutionSnapshotFor(at expiry) = %v, want ErrExpired", err)
	}
	expired, err := manager.Get(completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.State != StateExpired {
		t.Fatalf("job state after exact-deadline read = %v, want expired", expired.State)
	}
	assertLeaseCounts(t, manager, completed.ID, 1, 1)
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok || row.Ordinal != 0 {
		t.Fatalf("pinned lease Next after snapshot-triggered expiry = (%#v, %v, %v)", row, ok, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	assertLeaseCounts(t, manager, completed.ID, 0, 0)
}

func TestCompletedExecutionSnapshotForConcurrentTombstoneCleanup(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 22, 11, 0, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		RetentionTTL:     time.Second,
		ExpiredRetention: time.Nanosecond,
		CleanupInterval:  -1,
		Now:              clock.Now,
		NewID:            sequenceIDs("execution-cleanup"),
	})
	completed := createCompletedMessageJob(t, manager)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	clock.Add(time.Second)
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, completed.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("CompletedExecutionSnapshotFor(at expiry) = %v, want ErrExpired", err)
	}
	clock.Add(time.Nanosecond)

	const readers = 8
	var wait sync.WaitGroup
	wait.Add(readers + 1)
	for range readers {
		go func() {
			defer wait.Done()
			for range 100 {
				_, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, completed.ID)
				if !errors.Is(err, ErrExpired) && !errors.Is(err, ErrNotFound) {
					t.Errorf("concurrent snapshot error = %v, want ErrExpired or ErrNotFound", err)
					return
				}
			}
		}()
	}
	go func() {
		defer wait.Done()
		for range 100 {
			manager.Cleanup()
		}
	}()
	wait.Wait()
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, completed.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CompletedExecutionSnapshotFor(after cleanup) = %v, want ErrNotFound", err)
	}
}
