package searchjobs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
)

func TestAcquireExecutionForLegacyAtomicallyPinsResultGenerationAcrossExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.August, 8, 13, 0, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("pinned")})
		}),
		Now:              clock.Now,
		NewID:            sequenceIDs("atomic-legacy"),
		RetentionTTL:     time.Minute,
		ExpiredRetention: time.Minute,
		CleanupInterval:  -1,
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	access := AccessScope{TenantID: completed.TenantID, OwnerID: completed.OwnerID}
	lease, snapshot, err := manager.AcquireExecutionFor(context.Background(), access, completed.ID)
	if err != nil {
		t.Fatalf("AcquireExecutionFor(): %v", err)
	}
	defer func() { _ = lease.Close() }()
	if snapshot.CompiledQuery != nil || !snapshot.KnowledgeSnapshot.IsZero() {
		t.Fatalf("legacy execution carried knowledge authority: %#v", snapshot)
	}
	if lease.Generation() == 0 || lease.RowCount() != 1 || !lease.RowCountExact() ||
		!reflect.DeepEqual(lease.Schema(), messageSchema()) {
		t.Fatalf("atomic result authority generation=%d rows=%d exact=%t schema=%#v",
			lease.Generation(), lease.RowCount(), lease.RowCountExact(), lease.Schema())
	}
	metadata, ok := snapshot.ValidatedResultLease(lease)
	if !ok || metadata.Generation != lease.Generation() ||
		metadata.RowCount != lease.RowCount() || !metadata.RowCountExact ||
		metadata.ResultsTruncated != lease.ResultsTruncated() ||
		!reflect.DeepEqual(metadata.Schema, messageSchema()) {
		t.Fatalf("validated result metadata = (%#v, %t)", metadata, ok)
	}

	clock.Add(time.Minute)
	if changed := manager.Cleanup(); changed != 1 {
		t.Fatalf("Cleanup(at expiry) changed = %d, want 1", changed)
	}
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, completed.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("CompletedExecutionSnapshotFor(expired) = %v, want ErrExpired", err)
	}
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok || row.Ordinal != 0 {
		t.Fatalf("pinned lease Next() = (%#v, %t, %v)", row, ok, err)
	}
	value, ok := row.Values[0].String()
	if !ok || value != "pinned" {
		t.Fatalf("pinned row value = (%q, %t)", value, ok)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	assertExecutionLeasePermits(t, manager, completed.ID, 0, 0)
}

func TestAcquireResultsForDoesNotMintOrRetainExecutionAuthority(t *testing.T) {
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("ordinary")})
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("ordinary-result-lease"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	lease, err := manager.AcquireResultsFor(
		context.Background(),
		AccessScope{TenantID: completed.TenantID, OwnerID: completed.OwnerID},
		completed.ID,
	)
	if err != nil {
		t.Fatalf("AcquireResultsFor(): %v", err)
	}
	defer func() { _ = lease.Close() }()

	concrete, ok := lease.(*resultLease)
	if !ok {
		t.Fatalf("AcquireResultsFor() returned %T, want *resultLease", lease)
	}
	if concrete.resultAuthority != nil {
		t.Fatal("ordinary result lease retained execution-only result authority")
	}
	if _, ok := concrete.sealedExecutionResultLease(); ok {
		t.Fatal("ordinary result lease exposed execution-only attestation")
	}
}

func TestManagerExecutionAuthorityPostflightRejectsSharedMutationButContainsValueReassignment(t *testing.T) {
	for _, test := range []struct {
		name      string
		mutate    func(*clickhouse.CompiledQuery)
		wantState State
	}{
		{
			name: "non-read argument mutation",
			mutate: func(query *clickhouse.CompiledQuery) {
				for index, argument := range query.Args {
					if value, ok := argument.(string); ok && value == "authority-filter" {
						query.Args[index] = "tampered-filter"
						return
					}
				}
			},
			wantState: StateFailed,
		},
		{
			name: "by-value SQL reassignment",
			mutate: func(query *clickhouse.CompiledQuery) {
				query.SQL = "SELECT reassigned only in callee value"
			},
			wantState: StateCompleted,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
					if !slices.Equal(query.OutputFields, []string{"message"}) {
						return ErrInvalidResult
					}
					test.mutate(&query)
					if test.wantState == StateFailed {
						if _, _, ok := query.ReadScope(); !ok {
							t.Fatal("non-read mutation invalidated read scope instead of the full execution seal")
						}
					}
					return sink.SetSchema(messageSchema())
				}),
				CleanupInterval: -1,
				NewID:           sequenceIDs("execution-postflight"),
			})
			request := validRequest()
			request.SPL = `index=main message="authority-filter" | table message`
			created, err := manager.Create(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			terminal := waitForState(t, manager, created.ID, test.wantState)
			if test.wantState == StateFailed {
				if terminal.Failure == nil || terminal.Failure.Code != FailureInternal || terminal.Schema != nil {
					t.Fatalf("mutated execution authority published results: %#v", terminal)
				}
			} else if terminal.Schema == nil {
				t.Fatalf("contained value reassignment did not complete: %#v", terminal)
			}
		})
	}
}

func TestKnowledgeExecutionJointSealRejectsEveryMutationDowngradeAndAuthoritySwap(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t, "tenant")
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		KnowledgeResolver: resolver,
		CleanupInterval:   -1,
		NewID:             sequenceIDs("joint-seal"),
	})
	request := validRequest()
	request.AppID = appID
	request.SPL = `index=main message="first" | table message`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}
	lease, base, err := manager.AcquireExecutionFor(context.Background(), access, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	if !base.ValidKnowledgeAuthority() || !base.ValidFor(lease) || !base.Equal(base) {
		t.Fatal("manager-minted knowledge execution did not validate")
	}

	secondRequest := request
	secondRequest.SPL = `index=main message="second" | table message`
	second, err := manager.Create(context.Background(), secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, second.ID, StateCompleted)
	secondSnapshot, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	alternateAuthority, err := knowledgesnapshot.Prepare(knowledgesnapshot.Input{
		TenantID:                   base.TenantID,
		PrincipalID:                base.OwnerID,
		AppID:                      base.AppID,
		TenantCatalogRevision:      2,
		TenantCatalogStateToken:    bytes.Repeat([]byte{0x72}, sha256.Size),
		EffectiveAuthorizedIndexes: slices.Clone(base.EffectiveIndexes),
	})
	if err != nil {
		t.Fatal(err)
	}
	alternateSnapshot, err := alternateAuthority.Finalize(*base.CompiledQuery)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*ExecutionSnapshot)
	}{
		{name: "ID", mutate: func(value *ExecutionSnapshot) { value.ID += "-changed" }},
		{name: "owner", mutate: func(value *ExecutionSnapshot) { value.OwnerID += "-changed" }},
		{name: "tenant", mutate: func(value *ExecutionSnapshot) { value.TenantID += "-changed" }},
		{name: "app", mutate: func(value *ExecutionSnapshot) { value.AppID += "-changed" }},
		{name: "SPL", mutate: func(value *ExecutionSnapshot) { value.SPL += " | head 1" }},
		{name: "indexes", mutate: func(value *ExecutionSnapshot) { value.EffectiveIndexes[0] = "other" }},
		{name: "earliest", mutate: func(value *ExecutionSnapshot) { value.Earliest = value.Earliest.Add(time.Nanosecond) }},
		{name: "latest", mutate: func(value *ExecutionSnapshot) { value.Latest = value.Latest.Add(time.Nanosecond) }},
		{name: "search start", mutate: func(value *ExecutionSnapshot) { value.SearchStart = value.SearchStart.Add(time.Nanosecond) }},
		{name: "timezone", mutate: func(value *ExecutionSnapshot) { value.SearchTimezone = "America/Los_Angeles" }},
		{name: "index cutoff", mutate: func(value *ExecutionSnapshot) { value.IndexTimeCutoff = value.IndexTimeCutoff.Add(time.Nanosecond) }},
		{name: "visibility", mutate: func(value *ExecutionSnapshot) { value.VisibilityCutoff++ }},
		{name: "finished", mutate: func(value *ExecutionSnapshot) { value.FinishedAt = value.FinishedAt.Add(time.Nanosecond) }},
		{name: "expires", mutate: func(value *ExecutionSnapshot) { value.ExpiresAt = value.ExpiresAt.Add(time.Nanosecond) }},
		{name: "compiled", mutate: func(value *ExecutionSnapshot) { value.CompiledQuery.SQL += " -- changed" }},
		{name: "same-scope compiled swap", mutate: func(value *ExecutionSnapshot) {
			cloned, ok := secondSnapshot.CompiledQuery.CloneForExecution()
			if !ok {
				t.Fatal("clone alternate compiled query")
			}
			value.CompiledQuery = &cloned
		}},
		{name: "same-scope snapshot swap", mutate: func(value *ExecutionSnapshot) { value.KnowledgeSnapshot = alternateSnapshot }},
		{name: "strip compiled", mutate: func(value *ExecutionSnapshot) { value.CompiledQuery = nil }},
		{name: "strip snapshot", mutate: func(value *ExecutionSnapshot) { value.KnowledgeSnapshot = knowledgesnapshot.Snapshot{} }},
		{name: "strip pair downgrade", mutate: func(value *ExecutionSnapshot) {
			value.CompiledQuery = nil
			value.KnowledgeSnapshot = knowledgesnapshot.Snapshot{}
		}},
		{name: "constructed legacy downgrade", mutate: func(value *ExecutionSnapshot) {
			value.CompiledQuery = nil
			value.KnowledgeSnapshot = knowledgesnapshot.Snapshot{}
			value.knowledgeAuthoritySeal = knowledgeExecutionAuthoritySeal{}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			changed.EffectiveIndexes = slices.Clone(base.EffectiveIndexes)
			if base.CompiledQuery != nil {
				cloned, ok := base.CompiledQuery.CloneForExecution()
				if !ok {
					t.Fatal("clone base compiled query")
				}
				changed.CompiledQuery = &cloned
			}
			test.mutate(&changed)
			if changed.ValidKnowledgeAuthority() || changed.ValidFor(lease) || changed.Equal(base) {
				t.Fatal("mutated or downgraded authority validated")
			}
		})
	}

	fresh, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, created.ID)
	if err != nil || !base.Equal(fresh) {
		t.Fatalf("manager reacquisition changed joint seal: equal=%t err=%v", base.Equal(fresh), err)
	}
	manager.mu.RLock()
	entry := manager.jobs[created.ID]
	manager.mu.RUnlock()
	entry.mu.Lock()
	entry.job.AppID = "mismatched-app"
	entry.mu.Unlock()
	if _, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, created.ID); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("CompletedExecutionSnapshotFor(AppID mismatch) = %v, want ErrResultsUnavailable", err)
	}
}

func TestExecutionResultPinPrivateAttestationRejectsClosedAndCrossManagerSwap(t *testing.T) {
	now := time.Date(2026, time.August, 8, 15, 0, 0, 0, time.UTC)
	newManager := func() *Manager {
		return newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				return sink.SetSchema(messageSchema())
			}),
			Snapshotter:     snapshotterFunc(func(context.Context) (uint64, error) { return 77, nil }),
			CleanupInterval: -1,
			Now:             func() time.Time { return now },
			NewID:           func() string { return "same-job" },
			CursorKey:       []byte("same-cross-manager-cursor-key-at-least-32-bytes"),
		})
	}
	leftManager := newManager()
	rightManager := newManager()
	request := validRequest()
	for _, manager := range []*Manager{leftManager, rightManager} {
		created, err := manager.Create(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, created.ID, StateCompleted)
	}
	access := AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}
	leftLease, left, err := leftManager.AcquireExecutionFor(context.Background(), access, "same-job")
	if err != nil {
		t.Fatal(err)
	}
	rightLease, right, err := rightManager.AcquireExecutionFor(context.Background(), access, "same-job")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rightLease.Close() }()
	if leftLease.Generation() != rightLease.Generation() ||
		!reflect.DeepEqual(leftLease.Schema(), rightLease.Schema()) ||
		leftLease.RowCount() != rightLease.RowCount() {
		t.Fatal("cross-manager fixture does not share public result metadata")
	}
	if !left.ValidFor(leftLease) || !right.ValidFor(rightLease) ||
		left.ValidFor(rightLease) || right.ValidFor(leftLease) {
		t.Fatal("private result pin attestation admitted a cross-manager swap")
	}
	if err := leftLease.Close(); err != nil {
		t.Fatal(err)
	}
	if left.ValidFor(leftLease) {
		t.Fatal("closed result pin retained execution authority")
	}
}

func TestAcquireExecutionForCancellationAfterSnapshotCloneUnwindsBeforePin(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t, "tenant")
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		KnowledgeResolver: resolver,
		CleanupInterval:   -1,
		NewID:             sequenceIDs("atomic-cancel"),
	})
	request := validRequest()
	request.AppID = appID
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	ctx := newCancelOnNthErrContext(3)
	lease, snapshot, err := manager.AcquireExecutionFor(
		ctx,
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		created.ID,
	)
	if !errors.Is(err, context.Canceled) || lease != nil || !reflect.DeepEqual(snapshot, ExecutionSnapshot{}) {
		t.Fatalf("AcquireExecutionFor(cancel during clone) = (%#v, %#v, %v)", lease, snapshot, err)
	}
	assertExecutionLeasePermits(t, manager, created.ID, 0, 0)
}

func TestAcquireExecutionForTamperedCompiledAuthorityFailsBeforePin(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t, "tenant")
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		KnowledgeResolver: resolver,
		CleanupInterval:   -1,
		NewID:             sequenceIDs("atomic-tamper"),
	})
	request := validRequest()
	request.AppID = appID
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	manager.mu.RLock()
	entry := manager.jobs[created.ID]
	manager.mu.RUnlock()
	entry.mu.Lock()
	entry.preparedCompiled.SQL += " -- tampered"
	entry.mu.Unlock()

	lease, snapshot, err := manager.AcquireExecutionFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		created.ID,
	)
	if !errors.Is(err, ErrResultsUnavailable) || lease != nil || !reflect.DeepEqual(snapshot, ExecutionSnapshot{}) {
		t.Fatalf("AcquireExecutionFor(tampered authority) = (%#v, %#v, %v)", lease, snapshot, err)
	}
	assertExecutionLeasePermits(t, manager, created.ID, 0, 0)
}

func TestAcquireExecutionForScopeContextAndResultCoherenceFailuresDoNotPin(t *testing.T) {
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("atomic-failures"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	access := AccessScope{TenantID: created.TenantID, OwnerID: created.OwnerID}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name   string
		ctx    context.Context
		access AccessScope
		want   error
	}{
		{name: "nil context", ctx: nil, access: access},
		{name: "canceled context", ctx: canceled, access: access, want: context.Canceled},
		{name: "cross owner", ctx: context.Background(), access: AccessScope{TenantID: access.TenantID, OwnerID: "other"}, want: ErrNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			lease, snapshot, err := manager.AcquireExecutionFor(test.ctx, test.access, created.ID)
			if err == nil || test.want != nil && !errors.Is(err, test.want) || lease != nil || !reflect.DeepEqual(snapshot, ExecutionSnapshot{}) {
				t.Fatalf("AcquireExecutionFor() = (%#v, %#v, %v), want %v", lease, snapshot, err, test.want)
			}
			assertExecutionLeasePermits(t, manager, created.ID, 0, 0)
		})
	}

	manager.mu.RLock()
	entry := manager.jobs[created.ID]
	manager.mu.RUnlock()
	entry.mu.Lock()
	entry.resultGeneration = 0
	entry.mu.Unlock()
	lease, snapshot, err := manager.AcquireExecutionFor(context.Background(), access, created.ID)
	if !errors.Is(err, ErrResultsUnavailable) || lease != nil || !reflect.DeepEqual(snapshot, ExecutionSnapshot{}) {
		t.Fatalf("AcquireExecutionFor(incoherent result) = (%#v, %#v, %v)", lease, snapshot, err)
	}
	assertExecutionLeasePermits(t, manager, created.ID, 0, 0)
}

type cancelOnNthErrContext struct {
	cancelAt int32
	calls    atomic.Int32
	done     chan struct{}
	once     sync.Once
}

func newCancelOnNthErrContext(cancelAt int32) *cancelOnNthErrContext {
	return &cancelOnNthErrContext{cancelAt: cancelAt, done: make(chan struct{})}
}

func (ctx *cancelOnNthErrContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *cancelOnNthErrContext) Done() <-chan struct{}       { return ctx.done }
func (ctx *cancelOnNthErrContext) Value(any) any               { return nil }
func (ctx *cancelOnNthErrContext) Err() error {
	if ctx.calls.Add(1) < ctx.cancelAt {
		return nil
	}
	ctx.once.Do(func() { close(ctx.done) })
	return context.Canceled
}

func assertExecutionLeasePermits(
	t *testing.T,
	manager *Manager,
	id string,
	wantJobPins int,
	wantManagerPins int,
) {
	t.Helper()
	manager.mu.RLock()
	entry := manager.jobs[id]
	manager.mu.RUnlock()
	if entry == nil {
		t.Fatalf("job %q is not retained", id)
	}
	entry.mu.RLock()
	jobPins := entry.resultPins
	entry.mu.RUnlock()
	manager.budgetMu.Lock()
	managerPins := manager.activeResultLeases
	manager.budgetMu.Unlock()
	if jobPins != wantJobPins || managerPins != wantManagerPins {
		t.Fatalf("result lease permits = job:%d manager:%d, want %d/%d", jobPins, managerPins, wantJobPins, wantManagerPins)
	}
}
