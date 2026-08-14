package searchjobs

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
)

type knowledgeResolverFunc func(
	context.Context,
	knowledgecatalog.ResolutionScope,
) (knowledgecatalog.Resolution, error)

func (resolver knowledgeResolverFunc) Resolve(
	ctx context.Context,
	scope knowledgecatalog.ResolutionScope,
) (knowledgecatalog.Resolution, error) {
	return resolver(ctx, scope)
}

type nilKnowledgeResolver struct{}

func (*nilKnowledgeResolver) Resolve(
	context.Context,
	knowledgecatalog.ResolutionScope,
) (knowledgecatalog.Resolution, error) {
	panic("typed-nil knowledge resolver was called")
}

func TestKnowledgeAdmissionSealsEmptyAuthorityBeforeJournalAndDetaches(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t, "tenant")
	request := validRequest()
	request.AppID = appID

	var resolverCalls atomic.Int32
	var resolvedScope knowledgecatalog.ResolutionScope
	var eventMu sync.Mutex
	var events []string
	record := func(event string) {
		eventMu.Lock()
		events = append(events, event)
		eventMu.Unlock()
	}
	counted := knowledgeResolverFunc(func(ctx context.Context, scope knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
		record("resolve")
		resolverCalls.Add(1)
		resolvedScope = scope
		return resolver.Resolve(ctx, scope)
	})
	executed := make(chan clickhouse.CompiledQuery, 1)
	release := make(chan struct{})
	admitted := make(chan Job, 1)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, compiled clickhouse.CompiledQuery, sink ResultSink) error {
			executed <- compiled
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			record("snapshot")
			return 73, nil
		}),
		Journal: jobJournalFunc{admit: func(_ context.Context, job Job) error {
			record("journal")
			admitted <- job
			return nil
		}},
		KnowledgeResolver: counted,
		MaxConcurrent:     1,
		CleanupInterval:   -1,
		NewID: func() string {
			record("id")
			return "knowledge-empty-1"
		},
		Now: func() time.Time {
			return time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
		},
	})

	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(configured empty authority): %v", err)
	}
	if resolverCalls.Load() != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolverCalls.Load())
	}
	eventMu.Lock()
	gotEvents := slices.Clone(events)
	eventMu.Unlock()
	if !slices.Equal(gotEvents, []string{"snapshot", "resolve", "id", "journal"}) {
		t.Fatalf("configured admission order = %v", gotEvents)
	}
	if resolvedScope.TenantID != request.TenantID || resolvedScope.PrincipalID != request.OwnerID ||
		resolvedScope.AppID != appID || !slices.Equal(resolvedScope.EffectiveAuthorizedIndexes, []string{"main"}) {
		t.Fatalf("resolution scope = %#v", resolvedScope)
	}
	assertEmptyKnowledgeSummary(t, created.KnowledgeSnapshot)
	if !slices.Equal(created.EffectiveIndexes, []string{"main"}) {
		t.Fatalf("created effective indexes = %v", created.EffectiveIndexes)
	}
	journalSnapshot := <-admitted
	assertEmptyKnowledgeSummary(t, journalSnapshot.KnowledgeSnapshot)
	if journalSnapshot.ID != created.ID || journalSnapshot.State != StateQueued {
		t.Fatalf("journal admission = %#v", journalSnapshot)
	}

	created.KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff
	created.KnowledgeSnapshot.Ref.TenantCatalogStateToken[0] ^= 0xff
	stored, err := manager.Get(created.ID)
	if err != nil {
		t.Fatalf("Get(): %v", err)
	}
	if reflect.DeepEqual(created.KnowledgeSnapshot, stored.KnowledgeSnapshot) {
		t.Fatal("caller mutation reached retained knowledge summary")
	}
	assertEmptyKnowledgeSummary(t, stored.KnowledgeSnapshot)

	select {
	case compiled := <-executed:
		if compiled.SQL == "" || len(compiled.Args) == 0 {
			t.Fatalf("executed compiled query = %#v", compiled)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("prepared query did not execute")
	}
	close(release)
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if !slices.Equal(completedStateHistory(t, manager, created.ID), []State{
		StateQueued, StateParsing, StatePlanning, StateRunning, StateCompleted,
	}) {
		t.Fatalf("prepared state history = %v", completedStateHistory(t, manager, created.ID))
	}
	listPage, err := manager.ListPageFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		JobListRequest{PageSize: 1},
	)
	if err != nil || len(listPage.Jobs) != 1 {
		t.Fatalf("ListPageFor(knowledge) = (%#v, %v)", listPage, err)
	}
	assertEmptyKnowledgeSummary(t, listPage.Jobs[0].KnowledgeSnapshot)
	listPage.Jobs[0].KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff
	freshPage, err := manager.ListPageFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		JobListRequest{PageSize: 1},
	)
	if err != nil || len(freshPage.Jobs) != 1 {
		t.Fatalf("ListPageFor(knowledge fresh) = (%#v, %v)", freshPage, err)
	}
	assertEmptyKnowledgeSummary(t, freshPage.Jobs[0].KnowledgeSnapshot)
	execution, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		completed.ID,
	)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(): %v", err)
	}
	if execution.KnowledgeSnapshot.IsZero() ||
		!reflect.DeepEqual(execution.KnowledgeSnapshot.Reference(), completed.KnowledgeSnapshot.Ref) {
		t.Fatalf("execution knowledge snapshot = %#v", execution.KnowledgeSnapshot.Reference())
	}
	if prelude := execution.KnowledgeSnapshot.Prelude(); prelude.IsZero() || !prelude.IsEmpty() || prelude.ObjectCount() != 0 {
		t.Fatalf("execution knowledge prelude = zero:%t empty:%t objects:%d", prelude.IsZero(), prelude.IsEmpty(), prelude.ObjectCount())
	}
	if execution.CompiledQuery == nil {
		t.Fatal("completed configured execution omitted its sealed compiled query")
	}
	lease, acquired, err := manager.AcquireExecutionFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		completed.ID,
	)
	if err != nil {
		t.Fatalf("AcquireExecutionFor(): %v", err)
	}
	if !execution.Equal(acquired) || lease.Generation() == 0 || !reflect.DeepEqual(lease.Schema(), messageSchema()) {
		t.Fatalf("atomic execution lease = snapshotEqual:%t generation:%d schema:%#v", execution.Equal(acquired), lease.Generation(), lease.Schema())
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("execution lease Close(): %v", err)
	}
	fresh, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		completed.ID,
	)
	if err != nil || !execution.Equal(fresh) {
		t.Fatalf("detached execution snapshot equality = %t, error=%v", execution.Equal(fresh), err)
	}
	fresh.CompiledQuery.SQL += " -- mutated"
	if execution.Equal(fresh) {
		t.Fatal("execution equality ignored compiled-query authority")
	}
	fresh, err = manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		completed.ID,
	)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(after mutation): %v", err)
	}
	fresh.KnowledgeSnapshot = fresh.KnowledgeSnapshot.Clone()
	if !execution.Equal(fresh) {
		t.Fatal("equal cloned knowledge authority compared unequal")
	}
	fresh.KnowledgeSnapshot = knowledgesnapshot.Snapshot{}
	if execution.Equal(fresh) {
		t.Fatal("execution equality ignored knowledge snapshot authority")
	}
}

func TestKnowledgeAdmissionReservesAndSealsAddInfoSIDBeforeResolution(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t, "tenant")
	request := validRequest()
	request.AppID = appID
	request.SPL = `index=main | addinfo | table info_sid`

	const jobID = "knowledge-addinfo-immutable-1"
	var (
		idCalls       atomic.Int32
		resolverCalls atomic.Int32
		eventMu       sync.Mutex
		events        []string
	)
	record := func(event string) {
		eventMu.Lock()
		events = append(events, event)
		eventMu.Unlock()
	}
	counted := knowledgeResolverFunc(func(
		ctx context.Context,
		scope knowledgecatalog.ResolutionScope,
	) (knowledgecatalog.Resolution, error) {
		resolverCalls.Add(1)
		record("resolve")
		return resolver.Resolve(ctx, scope)
	})
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, _ ResultSink) error {
			<-ctx.Done()
			return ctx.Err()
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			record("snapshot")
			return 73, nil
		}),
		KnowledgeResolver: counted,
		MaxConcurrent:     1,
		CleanupInterval:   -1,
		NewID: func() string {
			idCalls.Add(1)
			record("id")
			return jobID
		},
		Now: func() time.Time {
			return time.Date(2026, time.August, 12, 12, 0, 0, 123_000_000, time.UTC)
		},
	})

	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(addinfo knowledge search): %v", err)
	}
	if created.ID != jobID || idCalls.Load() != 1 || resolverCalls.Load() != 1 {
		t.Fatalf(
			"addinfo authority = id %q/id calls %d/resolver calls %d",
			created.ID,
			idCalls.Load(),
			resolverCalls.Load(),
		)
	}
	eventMu.Lock()
	gotEvents := slices.Clone(events)
	eventMu.Unlock()
	if !slices.Equal(gotEvents, []string{"snapshot", "id", "resolve"}) {
		t.Fatalf("addinfo admission order = %v, want snapshot, ID reservation, resolution", gotEvents)
	}

	manager.mu.RLock()
	entry := manager.jobs[jobID]
	manager.mu.RUnlock()
	if entry == nil {
		t.Fatal("created addinfo job is not retained")
	}
	entry.mu.RLock()
	if entry.preparedCompiled == nil {
		entry.mu.RUnlock()
		t.Fatal("addinfo knowledge admission did not retain a compiled authority")
	}
	compiled, ok := entry.preparedCompiled.CloneForExecution()
	entry.mu.RUnlock()
	if !ok || !compiled.HasValidExecutionSeal() {
		t.Fatal("addinfo knowledge admission retained an unsealed compiler result")
	}
	if strings.Contains(compiled.SQL, jobID) {
		t.Fatal("immutable addinfo SID was interpolated into generated SQL")
	}
	jobIDArguments := 0
	for _, argument := range compiled.Args {
		if value, ok := argument.(string); ok && value == jobID {
			jobIDArguments++
		}
	}
	if jobIDArguments != 1 {
		t.Fatalf("immutable addinfo SID occurs %d times in bound args, want once: %#v", jobIDArguments, compiled.Args)
	}
}

func TestKnowledgePreparedWorkerNeverReparsesReplansOrReresolves(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t, "tenant")
	var resolverCalls atomic.Int32
	counted := knowledgeResolverFunc(func(ctx context.Context, scope knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
		resolverCalls.Add(1)
		return resolver.Resolve(ctx, scope)
	})

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondExecuted := make(chan clickhouse.CompiledQuery, 1)
	var executions atomic.Int32
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, compiled clickhouse.CompiledQuery, sink ResultSink) error {
			call := executions.Add(1)
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			if call == 1 {
				close(firstStarted)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-releaseFirst:
					return nil
				}
			}
			observed, ok := compiled.CloneForExecution()
			if !ok {
				return errors.New("worker received an invalid compiled query")
			}
			secondExecuted <- observed
			return nil
		}),
		KnowledgeResolver: counted,
		MaxConcurrent:     1,
		CleanupInterval:   -1,
		NewID:             sequenceIDs("prepared-once"),
	})

	legacy, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("Create(legacy blocker): %v", err)
	}
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("legacy blocker did not start")
	}
	request := validRequest()
	request.AppID = appID
	prepared, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(prepared): %v", err)
	}
	manager.mu.RLock()
	entry := manager.jobs[prepared.ID]
	manager.mu.RUnlock()
	entry.mu.Lock()
	if entry.preparedCompiled == nil {
		entry.mu.Unlock()
		t.Fatal("prepared query was not retained while queued")
	}
	want, ok := entry.preparedCompiled.CloneForExecution()
	if !ok {
		entry.mu.Unlock()
		t.Fatal("retained prepared query cannot be cloned")
	}
	// If the worker consults either mutable planning input, this request fails
	// or executes a different scope after the blocking legacy job releases.
	entry.job.SPL = "| definitely_not_a_command"
	entry.authorizedIndexes = []string{"attacker"}
	entry.job.RequestedIndexes = []string{"attacker"}
	entry.mu.Unlock()
	close(releaseFirst)
	waitForState(t, manager, legacy.ID, StateCompleted)

	var got clickhouse.CompiledQuery
	select {
	case got = <-secondExecuted:
	case <-time.After(3 * time.Second):
		t.Fatal("prepared query did not execute")
	}
	if got.SQL != want.SQL || !reflect.DeepEqual(got.Args, want.Args) ||
		!slices.Equal(got.OutputFields, want.OutputFields) {
		t.Fatalf("worker query diverged from sealed admission\ngot:  %#v\nwant: %#v", got, want)
	}
	if resolverCalls.Load() != 1 {
		t.Fatalf("resolver calls = %d, want exactly 1", resolverCalls.Load())
	}
	waitForState(t, manager, prepared.ID, StateCompleted)
	execution, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		prepared.ID,
	)
	if err != nil || execution.CompiledQuery == nil || !want.EqualForExecution(*execution.CompiledQuery) {
		t.Fatalf("retained compiler authority changed after executor mutation: snapshot=%#v err=%v", execution.CompiledQuery, err)
	}
	entry.mu.RLock()
	retainedCompiled := entry.preparedCompiled
	claimed := entry.preparedExecutionClaimed
	entry.mu.RUnlock()
	if retainedCompiled == nil || !claimed {
		t.Fatal("worker did not retain the sealed original after claiming its sole execution clone")
	}
}

func TestKnowledgeAdmissionLegacyAndAppLessParity(t *testing.T) {
	var calls atomic.Int32
	resolver := knowledgeResolverFunc(func(context.Context, knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
		calls.Add(1)
		return knowledgecatalog.Resolution{}, errors.New("must not resolve")
	})
	var typedNil *nilKnowledgeResolver
	for _, test := range []struct {
		name     string
		resolver KnowledgeResolver
		enabled  bool
	}{
		{name: "configured resolver with empty app", resolver: resolver, enabled: true},
		{name: "typed nil resolver", resolver: typedNil},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
					return errors.New("legacy async execution reached")
				}),
				KnowledgeResolver: test.resolver,
				CleanupInterval:   -1,
				NewID:             sequenceIDs("legacy-parity"),
			})
			if manager.KnowledgeAdmissionEnabled() != test.enabled {
				t.Fatalf("KnowledgeAdmissionEnabled() = %t", manager.KnowledgeAdmissionEnabled())
			}
			if manager.KnowledgeExecutionEnabled() != test.enabled {
				t.Fatalf("KnowledgeExecutionEnabled() = %t", manager.KnowledgeExecutionEnabled())
			}
			created, err := manager.Create(context.Background(), withSPL(validRequest(), "| unsupported_legacy_command"))
			if err != nil {
				t.Fatalf("legacy Create() became synchronous: %v", err)
			}
			if created.KnowledgeSnapshot != nil || len(created.EffectiveIndexes) != 0 {
				t.Fatalf("legacy queued snapshot changed = %#v", created)
			}
			waitForState(t, manager, created.ID, StateFailed)
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("app-less resolver calls = %d, want 0", calls.Load())
	}
}

func TestKnowledgeAdmissionFailuresAreSynchronousSafeAndSideEffectFree(t *testing.T) {
	privateCause := errors.New("sqlite path=/secret/catalog.db")
	var resolverCalls atomic.Int32
	resolver := knowledgeResolverFunc(func(context.Context, knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
		resolverCalls.Add(1)
		return knowledgecatalog.Resolution{}, privateCause
	})
	for _, test := range []struct {
		name          string
		spl           string
		want          error
		wantResolvers int32
	}{
		{name: "malformed SPL", spl: "index=(", want: ErrInvalidSPL},
		{name: "unsupported SPL", spl: "| definitely_not_a_command", want: ErrUnsupportedSPL},
		{name: "resolver unavailable", spl: validRequest().SPL, want: ErrKnowledgeUnavailable, wantResolvers: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var ids atomic.Int32
			var journals atomic.Int32
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
					t.Fatal("failed admission executed")
					return nil
				}),
				KnowledgeResolver: resolver,
				Journal: jobJournalFunc{admit: func(context.Context, Job) error {
					journals.Add(1)
					return nil
				}},
				CleanupInterval: -1,
				NewID: func() string {
					ids.Add(1)
					return "unexpected-id"
				},
			})
			request := validRequest()
			request.AppID = "app_aaaaaaaaaaaaaaaaaaaaaA"
			request.SPL = test.spl
			_, err := manager.Create(context.Background(), request)
			if !errors.Is(err, test.want) {
				t.Fatalf("Create() error = %v, want %v", err, test.want)
			}
			if strings.Contains(err.Error(), "secret") || ids.Load() != 0 || journals.Load() != 0 {
				t.Fatalf("failed admission leaked/committed err=%v ids=%d journals=%d", err, ids.Load(), journals.Load())
			}
			assertEmptyManagerAdmissionState(t, manager)
		})
	}
	if resolverCalls.Load() != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolverCalls.Load())
	}
}

func TestKnowledgeAdmissionPlanningGateAndMemoryAreBounded(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t, "tenant")
	request := validRequest()
	request.AppID = appID
	var ids atomic.Int32
	manager := newTestManager(t, Config{
		Executor:          executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		KnowledgeResolver: resolver,
		MaxConcurrent:     1,
		CleanupInterval:   -1,
		NewID: func() string {
			ids.Add(1)
			return "gate-id"
		},
	})
	manager.validationGate <- struct{}{}
	_, err := manager.Create(context.Background(), request)
	<-manager.validationGate
	if !errors.Is(err, ErrCapacity) || ids.Load() != 0 {
		t.Fatalf("gate-saturated Create() = %v, IDs=%d, want ErrCapacity and no ID", err, ids.Load())
	}
	assertEmptyManagerAdmissionState(t, manager)

	base, err := retainedJobMetadataReservation("metadata-id", request)
	if err != nil {
		t.Fatalf("base metadata reservation: %v", err)
	}
	limit := base + 8<<10
	limited := newTestManager(t, Config{
		Executor:          executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		KnowledgeResolver: resolver,
		MaxMetadataBytes:  limit,
		CleanupInterval:   -1,
		NewID:             func() string { return "metadata-id" },
	})
	_, err = limited.Create(context.Background(), request)
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("undercharged configured admission = %v, want ErrCapacity (limit=%d base=%d)", err, limit, base)
	}
	assertEmptyManagerAdmissionState(t, limited)
}

func TestKnowledgeAdmissionRejectsResolverScopeDivergence(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t, "tenant")
	divergent := knowledgeResolverFunc(func(ctx context.Context, scope knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
		// A resolver is a dependency boundary. Mutating its input and resolving a
		// different authority must not mutate the manager's expected cross-check.
		scope.EffectiveAuthorizedIndexes[0] = "other"
		return resolver.Resolve(ctx, scope)
	})
	var ids atomic.Int32
	manager := newTestManager(t, Config{
		Executor:          executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		KnowledgeResolver: divergent,
		CleanupInterval:   -1,
		NewID: func() string {
			ids.Add(1)
			return "scope-divergence"
		},
	})
	request := validRequest()
	request.AppID = appID
	_, err := manager.Create(context.Background(), request)
	if !errors.Is(err, ErrKnowledgeUnavailable) || ids.Load() != 0 {
		t.Fatalf("Create(divergent resolver) = %v, IDs=%d, want fail-closed pre-ID", err, ids.Load())
	}
	assertEmptyManagerAdmissionState(t, manager)
}

func TestKnowledgeAdmissionCancellationAndCloseInterruptResolver(t *testing.T) {
	for _, test := range []struct {
		name string
		stop func(context.CancelFunc, *Manager) error
		want error
	}{
		{name: "caller cancellation", stop: func(cancel context.CancelFunc, _ *Manager) error {
			cancel()
			return context.Canceled
		}, want: context.Canceled},
		{name: "manager close", stop: func(_ context.CancelFunc, manager *Manager) error {
			return manager.Close()
		}, want: ErrClosed},
	} {
		t.Run(test.name, func(t *testing.T) {
			entered := make(chan struct{})
			resolver := knowledgeResolverFunc(func(ctx context.Context, _ knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
				close(entered)
				<-ctx.Done()
				return knowledgecatalog.Resolution{}, ctx.Err()
			})
			manager := newTestManager(t, Config{
				Executor:          executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
				KnowledgeResolver: resolver,
				CleanupInterval:   -1,
			})
			request := validRequest()
			request.AppID = "app_aaaaaaaaaaaaaaaaaaaaaA"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := manager.Create(ctx, request)
				result <- err
			}()
			<-entered
			stopDone := make(chan error, 1)
			go func() { stopDone <- test.stop(cancel, manager) }()
			err := <-result
			if !errors.Is(err, test.want) {
				t.Fatalf("Create() error = %v, want %v", err, test.want)
			}
			if stopErr := <-stopDone; stopErr != nil && errors.Is(test.want, ErrClosed) {
				t.Fatalf("Close(): %v", stopErr)
			}
			assertEmptyManagerAdmissionState(t, manager)
		})
	}
}

func newEmptyKnowledgeResolver(t *testing.T, tenantID string) (KnowledgeResolver, string) {
	t.Helper()
	database, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("control DB Close(): %v", err)
		}
	})
	appID := "app_aaaaaaaaaaaaaaaaaaaaaA"
	appCatalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: testCursorKey,
		IDGenerator: func() (string, error) {
			return appID, nil
		},
	})
	if err != nil {
		t.Fatalf("NewAppCatalog(): %v", err)
	}
	if _, err := appCatalog.CreateApp(context.Background(), control.AppAccessScope{TenantID: tenantID}, control.AppDefinition{
		Slug: "search-app", DisplayName: "Search App",
	}); err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	store, err := knowledgecatalog.New(database, knowledgecatalog.Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("knowledgecatalog.New(): %v", err)
	}
	resolver, err := knowledgecatalog.NewResolver(store, knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatalf("knowledgecatalog.NewResolver(): %v", err)
	}
	// The production resolver deliberately owns a 250 ms deadline. Under a
	// fully parallel race package, SQLite instrumentation can consume that
	// complete budget even for this empty fixture. These manager tests exercise
	// admission semantics rather than the resolver deadline (which has its own
	// concrete timeout suite), so retry only a resolver-owned deadline while the
	// caller remains live. Every successful response still comes from the real
	// migrated Store/Resolver boundary.
	return knowledgeResolverFunc(func(
		ctx context.Context,
		scope knowledgecatalog.ResolutionScope,
	) (knowledgecatalog.Resolution, error) {
		retryContext, cancelRetry := context.WithTimeout(ctx, 30*time.Second)
		defer cancelRetry()
		for {
			resolution, resolveErr := resolver.Resolve(retryContext, scope)
			if resolveErr == nil || retryContext.Err() != nil ||
				!errors.Is(resolveErr, context.DeadlineExceeded) {
				return resolution, resolveErr
			}
		}
	}), appID
}

func assertEmptyKnowledgeSummary(t *testing.T, summary *opensplunkv1.KnowledgeSnapshotSummary) {
	t.Helper()
	if summary == nil || summary.GetRef() == nil || summary.GetRef().GetObjectCount() != 0 {
		t.Fatalf("knowledge summary = %#v, want present empty authority", summary)
	}
}

func completedStateHistory(t *testing.T, manager *Manager, id string) []State {
	t.Helper()
	manager.mu.RLock()
	entry := manager.jobs[id]
	manager.mu.RUnlock()
	if entry == nil {
		t.Fatalf("job %q is not retained", id)
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	return slices.Clone(entry.history)
}

func assertEmptyManagerAdmissionState(t *testing.T, manager *Manager) {
	t.Helper()
	manager.mu.RLock()
	jobs := len(manager.jobs)
	pending := manager.pendingAdmissions
	activeOperations := manager.activeOperations
	reservedIDs := len(manager.reservedIDs)
	queued := manager.queueCount
	manager.mu.RUnlock()
	manager.budgetMu.Lock()
	metadata := manager.metadataBytes
	manager.budgetMu.Unlock()
	if jobs != 0 || pending != 0 || activeOperations != 0 || reservedIDs != 0 || queued != 0 || metadata != 0 {
		t.Fatalf("failed admission leaked jobs=%d pending=%d active=%d IDs=%d queued=%d metadata=%d", jobs, pending, activeOperations, reservedIDs, queued, metadata)
	}
}

var _ KnowledgeResolver = knowledgeResolverFunc(nil)
var _ KnowledgeResolver = (*nilKnowledgeResolver)(nil)
