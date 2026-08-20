package export

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"google.golang.org/protobuf/proto"
)

func TestReexecutionSourceUsesExactRetainedKnowledgeExecutionAndPinsItsLifetime(t *testing.T) {
	t.Parallel()
	searches, schema, access, execution, retained := newKnowledgeReexecutionFixture(t)

	var calls atomic.Int32
	var captured clickhouse.CompiledQuery
	executor := reexecutionTestExecutor(func(_ context.Context, compiled clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		calls.Add(1)
		captured = compiled
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		return sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(200)})
	})
	source := newReexecutionTestSource(t, searches, executor, func(config *ReexecutionSourceConfig) {
		// The retained knowledge path must not consult this compiler. This
		// configuration deterministically rejects any attempted legacy rebuild.
		config.Compiler = clickhouse.Compiler{Database: "not-valid"}
	})

	lease, err := source.AcquireResultsFor(context.Background(), access, execution.ID)
	if err != nil {
		t.Fatalf("AcquireResultsFor(knowledge): %v", err)
	}
	if calls.Load() != 0 || searches.lastPinClosed() {
		t.Fatalf("lazy acquisition = executor calls %d, pin closed %t", calls.Load(), searches.lastPinClosed())
	}
	provider, ok := lease.(knowledgeSnapshotResultLease)
	if !ok {
		t.Fatal("knowledge re-execution lease omitted admission provenance")
	}
	firstSummary, err := provider.knowledgeSnapshotSummary()
	if err != nil || firstSummary == nil || !reflect.DeepEqual(firstSummary.GetRef(), execution.KnowledgeSnapshot.Reference()) {
		t.Fatalf("knowledge summary = %#v, error = %v", firstSummary, err)
	}
	firstSummary.Ref.SnapshotSha256[0] ^= 0xff
	secondSummary, err := provider.knowledgeSnapshotSummary()
	if err != nil || bytes.Equal(firstSummary.GetRef().GetSnapshotSha256(), secondSummary.GetRef().GetSnapshotSha256()) {
		t.Fatalf("knowledge summary was not detached: %#v, %v", secondSummary, err)
	}

	// Mutating the dependency-owned snapshot after admission cannot alter the
	// executable clone already held by the lease.
	searches.mu.Lock()
	searches.execution.CompiledQuery.SQL += " -- dependency mutation"
	searches.mu.Unlock()
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok || len(row.Values) != 1 {
		t.Fatalf("Next() = (%#v, %t, %v)", row, ok, err)
	}
	if _, ok, err := lease.Next(context.Background()); err != nil || ok {
		t.Fatalf("Next(end) = (%t, %v)", ok, err)
	}
	if calls.Load() != 1 || !captured.EqualForExecution(retained) {
		t.Fatalf("executor received exact retained query = %t, calls = %d", captured.EqualForExecution(retained), calls.Load())
	}
	if searches.lastPinClosed() {
		t.Fatal("source pin closed before lease Close")
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
	if !searches.lastPinClosed() {
		t.Fatal("underlying pin remained open")
	}
}

func TestReexecutionSourceRejectsIncompleteOrTamperedKnowledgeAuthorityAndClosesPin(t *testing.T) {
	t.Parallel()
	_, _, _, base, retained := newKnowledgeReexecutionFixture(t)
	principalMismatchAuthority, err := knowledgesnapshot.Prepare(knowledgesnapshot.Input{
		TenantID:                   base.TenantID,
		PrincipalID:                "different-owner",
		AppID:                      "app-1",
		TenantCatalogRevision:      1,
		TenantCatalogStateToken:    bytes.Repeat([]byte{0x4a}, sha256.Size),
		EffectiveAuthorizedIndexes: slices.Clone(base.EffectiveIndexes),
	})
	if err != nil {
		t.Fatal(err)
	}
	principalMismatchSnapshot, err := principalMismatchAuthority.Finalize(retained)
	if err != nil {
		t.Fatal(err)
	}
	chargedExecution := base
	chargedExecution.SPL = `index=main | rex field=status "(?<captured>[0-9]+)" | table status`
	chargedLogical, err := searchsnapshot.BuildPlan(searchjobs.Job{
		ID:               chargedExecution.ID,
		OwnerID:          chargedExecution.OwnerID,
		TenantID:         chargedExecution.TenantID,
		AppID:            chargedExecution.AppID,
		SPL:              chargedExecution.SPL,
		EffectiveIndexes: slices.Clone(chargedExecution.EffectiveIndexes),
		Earliest:         chargedExecution.Earliest,
		Latest:           chargedExecution.Latest,
		CreatedAt:        chargedExecution.SearchStart,
		TimeRange: searchtime.Intent{
			Timezone: chargedExecution.SearchTimezone,
		},
		IndexTimeCutoff:  chargedExecution.IndexTimeCutoff,
		VisibilityCutoff: chargedExecution.VisibilityCutoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	chargedCompiled, err := (clickhouse.Compiler{Database: "open_splunk", Table: "events"}).Compile(chargedLogical)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*searchjobs.ExecutionSnapshot)
	}{
		{name: "snapshot without compiled query", mutate: func(value *searchjobs.ExecutionSnapshot) {
			value.CompiledQuery = nil
		}},
		{name: "compiled query without snapshot", mutate: func(value *searchjobs.ExecutionSnapshot) {
			value.KnowledgeSnapshot = knowledgesnapshot.Snapshot{}
		}},
		{name: "tampered compiled query", mutate: func(value *searchjobs.ExecutionSnapshot) {
			value.CompiledQuery.SQL += " -- tampered"
		}},
		{name: "compiled scope mismatch", mutate: func(value *searchjobs.ExecutionSnapshot) {
			value.EffectiveIndexes = []string{"other"}
		}},
		{name: "snapshot app mismatch", mutate: func(value *searchjobs.ExecutionSnapshot) {
			value.AppID = "app_bbbbbbbbbbbbbbbbbbbbbB"
		}},
		{name: "snapshot principal mismatch", mutate: func(value *searchjobs.ExecutionSnapshot) {
			value.KnowledgeSnapshot = principalMismatchSnapshot
		}},
		{name: "snapshot compiler charges mismatch", mutate: func(value *searchjobs.ExecutionSnapshot) {
			value.CompiledQuery = &chargedCompiled
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			searches, _, access := newReexecutionTestSearches()
			execution := base
			compiled, ok := retained.CloneForExecution()
			if !ok {
				t.Fatal("fixture compiled query is invalid")
			}
			execution.CompiledQuery = &compiled
			execution.KnowledgeSnapshot = base.KnowledgeSnapshot.Clone()
			test.mutate(&execution)
			searches.execution = &execution
			source := newReexecutionTestSource(t, searches, reexecutionTestExecutor(func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
				t.Fatal("invalid authority reached executor")
				return nil
			}), nil)
			lease, err := source.AcquireResultsFor(context.Background(), access, execution.ID)
			if lease != nil || !errors.Is(err, searchjobs.ErrResultsUnavailable) {
				t.Fatalf("AcquireResultsFor() = (%v, %v), want nil/ErrResultsUnavailable", lease, err)
			}
			if !searches.lastPinClosed() {
				t.Fatal("failed admission pin remained open")
			}
		})
	}
}

func TestReexecutionLeaseRejectsExecutorMutationOfDetachedCompiledQuery(t *testing.T) {
	t.Parallel()
	searches, schema, access, execution, retained := newKnowledgeReexecutionFixture(t)
	source := newReexecutionTestSource(t, searches, reexecutionTestExecutor(func(
		_ context.Context,
		compiled clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		if len(compiled.Args) == 0 {
			t.Fatal("fixture compiled query has no arguments")
		}
		compiled.Args[0] = "mutated-tenant"
		return sink.SetSchema(schema)
	}), nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || !errors.Is(nextErr, searchjobs.ErrInvalidResult) {
		t.Fatalf("Next(mutated executor query) = (ok=%t, err=%v)", ok, nextErr)
	}
	concrete := lease.(*reexecutionLease)
	if !concrete.compiled.EqualForExecution(retained) {
		t.Fatal("executor mutation reached lease-retained compiled authority")
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if !searches.lastPinClosed() {
		t.Fatal("pin remained open")
	}
}

func TestReexecutionLeaseCancellationAndConcurrentCloseReleasePinOnce(t *testing.T) {
	t.Parallel()
	searches, schema, access, execution, _ := newKnowledgeReexecutionFixture(t)
	started := make(chan struct{})
	executor := reexecutionTestExecutor(func(ctx context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	ctx, cancel := context.WithCancel(context.Background())
	lease, err := source.AcquireResultsFor(ctx, access, execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	nextDone := make(chan error, 1)
	go func() {
		_, _, nextErr := lease.Next(context.Background())
		nextDone <- nextErr
	}()
	<-started
	cancel()

	var waits sync.WaitGroup
	for range 8 {
		waits.Go(func() {
			_ = lease.Close()
		})
	}
	waits.Wait()
	if nextErr := <-nextDone; !errors.Is(nextErr, context.Canceled) && !errors.Is(nextErr, searchjobs.ErrResultLeaseClosed) {
		t.Fatalf("Next after cancellation = %v", nextErr)
	}
	if !searches.lastPinClosed() {
		t.Fatal("canceled underlying pin remained open")
	}
}

func TestReexecutionSourceAtomicPinKeepsExpiredSearchTombstoneUntilClose(t *testing.T) {
	t.Parallel()
	clock := &integrationClock{now: time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)}
	access := searchjobs.AccessScope{TenantID: "tenant-expiry", OwnerID: "owner-expiry"}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "status", Kind: searchjobs.ValueKindSigned}}}
	searchManager, err := searchjobs.New(searchjobs.Config{
		Executor: integrationSearchExecutor(func(_ context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			return sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(200)})
		}),
		Snapshotter:      integrationSnapshotter(func(context.Context) (uint64, error) { return 19, nil }),
		MaxConcurrent:    1,
		RetentionTTL:     time.Second,
		ExpiredRetention: time.Second,
		CleanupInterval:  -1,
		Now:              clock.Now,
		NewID:            func() string { return "search-expiry" },
		CursorKey:        []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = searchManager.Close() })
	rangeIntent, err := searchtime.NewAbsoluteRange(clock.Now().Add(-time.Hour), clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	created, err := searchManager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               "index=main | table status",
		OwnerID:           access.OwnerID,
		TenantID:          access.TenantID,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         rangeIntent,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForIntegrationSearchState(t, searchManager, access, created.ID, searchjobs.StateCompleted)

	started := make(chan struct{})
	reexecution, err := NewReexecutionSource(ReexecutionSourceConfig{
		Searches: searchManager,
		Executor: reexecutionTestExecutor(func(ctx context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}),
		Compiler: clickhouse.Compiler{Database: "open_splunk", Table: "events"},
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := reexecution.AcquireResultsFor(context.Background(), access, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	nextDone := make(chan error, 1)
	go func() {
		_, _, nextErr := lease.Next(context.Background())
		nextDone <- nextErr
	}()
	<-started

	clock.Advance(2 * time.Second)
	if changed := searchManager.Cleanup(); changed == 0 {
		t.Fatal("Cleanup did not publish source expiry")
	}
	clock.Advance(2 * time.Second)
	_ = searchManager.Cleanup()
	if expired, err := searchManager.GetFor(access, created.ID); err != nil || expired.State != searchjobs.StateExpired {
		t.Fatalf("pinned expired tombstone = (%#v, %v)", expired, err)
	}

	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if nextErr := <-nextDone; !errors.Is(nextErr, context.Canceled) && !errors.Is(nextErr, searchjobs.ErrResultLeaseClosed) {
		t.Fatalf("Next after Close = %v", nextErr)
	}
	_ = searchManager.Cleanup()
	if _, err := searchManager.GetFor(access, created.ID); !errors.Is(err, searchjobs.ErrNotFound) {
		t.Fatalf("GetFor(after pin release) = %v, want ErrNotFound", err)
	}
}

func TestExportManagerRetainsAndDetachesKnowledgeSummaryAcrossLifecycle(t *testing.T) {
	t.Parallel()
	summary := validExportKnowledgeSummary()
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	base := &exportTestSource{datasets: map[string]exportTestDataset{
		"complete": {
			schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}},
			rows:   []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue("value")}}},
		},
		"cancel": {
			schema:      searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}},
			nextGate:    gate,
			nextStarted: started,
		},
	}}
	source := &knowledgeSummaryTestSource{ResultSource: base, summary: summary}
	manager := newExportTestManager(t, source, func(config *Config) { config.MaxWorkers = 1 })

	completedCreate, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "complete", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	assertExportKnowledgeSummary(t, completedCreate.KnowledgeSnapshot)
	completedCreate.KnowledgeSnapshot.Objects[0].GetAuthorizedObject().Name = "mutated"
	completedCreate.KnowledgeSnapshot.LookupAssets[0].Asset.ContentSha256[0] ^= 0xff
	completed := waitExportState(t, manager, testAccess, completedCreate.ID, StateCompleted)
	assertExportKnowledgeSummary(t, completed.KnowledgeSnapshot)
	page, err := manager.List(context.Background(), testAccess, ListRequest{PageSize: 15})
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("List() = (%#v, %v)", page, err)
	}
	assertExportKnowledgeSummary(t, page.Jobs[0].KnowledgeSnapshot)
	page.Jobs[0].KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff
	fresh, err := manager.Get(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertExportKnowledgeSummary(t, fresh.KnowledgeSnapshot)

	canceledCreate, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "cancel", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel export did not start")
	}
	canceled, err := manager.Cancel(context.Background(), testAccess, canceledCreate.ID)
	if err != nil || canceled.State != StateCanceled {
		t.Fatalf("Cancel() = (%#v, %v)", canceled, err)
	}
	assertExportKnowledgeSummary(t, canceled.KnowledgeSnapshot)
	close(gate)
}

func TestLegacyExportMetadataExactFitPreservesNilSummaryAdmission(t *testing.T) {
	t.Parallel()
	baseDirectory := t.TempDir()
	access := searchjobs.AccessScope{TenantID: "t", OwnerID: "o"}
	if summaryBytes, err := knowledgeSnapshotMetadataBytes(nil); err != nil || summaryBytes != 0 {
		t.Fatalf("nil summary metadata = (%d, %v), want 0/nil", summaryBytes, err)
	}
	source := &exportTestSource{datasets: map[string]exportTestDataset{
		"x": {schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}}},
	}}
	manager, err := New(Config{
		Source:          source,
		ArtifactDir:     baseDirectory,
		CleanupInterval: -1,
		NewID:           func() string { return "legacy-exact" },
	})
	if err != nil {
		t.Fatalf("New(exact legacy metadata): %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	exactMetadata, err := requestedMetadataBytes(manager.artifactDir, access, "x", []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	// Pin the test's internal budget to the actual manager-owned session path so
	// it exercises exact-fit admission rather than MkdirTemp suffix length.
	manager.budgetMu.Lock()
	manager.maxTotalMetadata = exactMetadata
	manager.budgetMu.Unlock()
	created, err := manager.Create(context.Background(), access, CreateRequest{
		SearchJobID: "x",
		Format:      FormatCSV,
		Columns:     []string{"x"},
	})
	if err != nil {
		t.Fatalf("Create(exact legacy metadata): %v", err)
	}
	completed := waitExportState(t, manager, access, created.ID, StateCompleted)
	if completed.KnowledgeSnapshot != nil {
		t.Fatalf("legacy export invented summary = %#v", completed.KnowledgeSnapshot)
	}
	manager.budgetMu.Lock()
	retained := manager.totalMetadata
	manager.budgetMu.Unlock()
	if retained != exactMetadata {
		t.Fatalf("legacy retained metadata = %d, want exact %d", retained, exactMetadata)
	}
}

func TestKnowledgeSummaryMetadataExpansionIsAtomicAccountedAndReclaimed(t *testing.T) {
	t.Parallel()
	clock := &exportTestClock{now: time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)}
	summary := validExportKnowledgeSummary()
	base := &exportTestSource{datasets: map[string]exportTestDataset{
		"knowledge": {
			schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}}},
			rows:   []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue("value")}}},
		},
	}}
	source := &knowledgeSummaryTestSource{ResultSource: base, summary: summary}
	manager := newExportTestManager(t, source, func(config *Config) {
		config.Now = clock.Now
		config.ArtifactTTL = time.Minute
		config.ExpiredRetention = time.Minute
	})
	legacyMetadata, err := resolvedMetadataBytes(
		manager.artifactDir,
		testAccess,
		"knowledge",
		[]searchjobs.Column{{Name: "x", Kind: searchjobs.ValueKindString}},
	)
	if err != nil {
		t.Fatal(err)
	}
	summaryMetadata, err := knowledgeSnapshotMetadataBytes(summary)
	if err != nil || summaryMetadata == 0 {
		t.Fatalf("summary metadata = (%d, %v)", summaryMetadata, err)
	}
	exactMetadata, ok := checkedAddUint64(legacyMetadata, summaryMetadata)
	if !ok {
		t.Fatal("fixture metadata overflow")
	}
	manager.maxTotalMetadata = exactMetadata - 1
	if job, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "knowledge",
		Format:      FormatCSV,
		Columns:     []string{"x"},
	}); !errors.Is(err, ErrCapacity) || job.ID != "" {
		t.Fatalf("Create(one byte below summary expansion) = (%#v, %v)", job, err)
	}
	manager.mu.RLock()
	jobs, reservations := len(manager.jobs), manager.reservations
	manager.mu.RUnlock()
	manager.budgetMu.Lock()
	retainedAfterFailure := manager.totalMetadata
	manager.budgetMu.Unlock()
	if jobs != 0 || reservations != 0 || retainedAfterFailure != 0 || base.closedLeases() != 1 {
		t.Fatalf(
			"failed expansion leaked jobs=%d reservations=%d metadata=%d closedLeases=%d",
			jobs,
			reservations,
			retainedAfterFailure,
			base.closedLeases(),
		)
	}

	manager.maxTotalMetadata = exactMetadata
	created, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "knowledge",
		Format:      FormatCSV,
		Columns:     []string{"x"},
	})
	if err != nil {
		t.Fatalf("Create(exact summary expansion): %v", err)
	}
	completed := waitExportState(t, manager, testAccess, created.ID, StateCompleted)
	assertExportKnowledgeSummary(t, completed.KnowledgeSnapshot)
	manager.budgetMu.Lock()
	retainedAtCompletion := manager.totalMetadata
	manager.budgetMu.Unlock()
	if retainedAtCompletion != exactMetadata {
		t.Fatalf("knowledge retained metadata = %d, want %d", retainedAtCompletion, exactMetadata)
	}
	// The source-owned summary is not the retained accounting authority.
	summary.Ref.SnapshotSha256[0] ^= 0xff
	fresh, err := manager.Get(context.Background(), testAccess, completed.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertExportKnowledgeSummary(t, fresh.KnowledgeSnapshot)

	clock.Advance(time.Minute)
	_ = manager.Cleanup(context.Background())
	clock.Advance(time.Minute)
	_ = manager.Cleanup(context.Background())
	if _, err := manager.Get(context.Background(), testAccess, completed.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(after summary tombstone cleanup) = %v, want ErrNotFound", err)
	}
	manager.budgetMu.Lock()
	retainedAfterCleanup := manager.totalMetadata
	manager.budgetMu.Unlock()
	if retainedAfterCleanup != 0 {
		t.Fatalf("summary metadata after cleanup = %d, want 0", retainedAfterCleanup)
	}
}

func TestKnowledgeSummaryMetadataChargeUsesValidatedDetachedBounds(t *testing.T) {
	t.Parallel()
	if charge, err := knowledgeSnapshotMetadataBytes(nil); err != nil || charge != 0 {
		t.Fatalf("nil summary charge = (%d, %v)", charge, err)
	}
	summary := validExportKnowledgeSummary()
	charge, err := knowledgeSnapshotMetadataBytes(summary)
	if err != nil {
		t.Fatal(err)
	}
	want := metadataKnowledgeSummaryFixed + 2*uint64(proto.Size(summary)) + metadataKnowledgeSummaryPerObject
	if charge != want || charge > metadataKnowledgeSummaryMaximum {
		t.Fatalf("summary charge = %d, want %d within %d", charge, want, metadataKnowledgeSummaryMaximum)
	}
	detached, err := knowledgesnapshot.CloneSummary(summary)
	if err != nil {
		t.Fatal(err)
	}
	summary.Objects[0].GetAuthorizedObject().Name = "mutated-source"
	detachedCharge, err := knowledgeSnapshotMetadataBytes(detached)
	if err != nil || detachedCharge != charge || detached.Objects[0].GetAuthorizedObject().GetName() != "extract-one" {
		t.Fatalf("detached summary charge = (%d, %v), summary=%#v", detachedCharge, err, detached)
	}

	maximum := validExportKnowledgeSummary()
	maximum.Ref.ObjectCount = knowledgesnapshot.MaximumSummaryObjects
	maximum.Objects = make([]*opensplunk.KnowledgeSnapshotObjectSummary, knowledgesnapshot.MaximumSummaryObjects)
	for ordinal := range maximum.Objects {
		maximum.Objects[ordinal] = &opensplunk.KnowledgeSnapshotObjectSummary{
			ResolutionOrdinal: uint32(ordinal),
			ObjectType:        opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			Stage:             opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
			Disclosure: &opensplunk.KnowledgeSnapshotObjectSummary_AuthorizedObject{
				AuthorizedObject: &opensplunk.KnowledgeSnapshotAuthorizedObjectSummary{
					KnowledgeObjectId: fmt.Sprintf("object-%d", ordinal),
					Version:           uint64(ordinal + 1),
					Name:              fmt.Sprintf("extract-%d", ordinal),
				},
			},
		}
	}
	maximumCharge, err := knowledgeSnapshotMetadataBytes(maximum)
	if err != nil || maximumCharge == 0 || maximumCharge > metadataKnowledgeSummaryMaximum {
		t.Fatalf("maximum summary charge = (%d, %v), envelope=%d", maximumCharge, err, metadataKnowledgeSummaryMaximum)
	}
	tooMany := proto.Clone(maximum).(*opensplunk.KnowledgeSnapshotSummary)
	tooMany.Ref.ObjectCount++
	tooMany.Objects = append(tooMany.Objects, proto.Clone(tooMany.Objects[0]).(*opensplunk.KnowledgeSnapshotObjectSummary))
	if charge, err := knowledgeSnapshotMetadataBytes(tooMany); err == nil || charge != 0 {
		t.Fatalf("oversized repeated summary charge = (%d, %v)", charge, err)
	}
}

type knowledgeSummaryTestSource struct {
	ResultSource
	summary *opensplunk.KnowledgeSnapshotSummary
}

func (source *knowledgeSummaryTestSource) AcquireResultsFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.ResultLease, error) {
	lease, err := source.ResultSource.AcquireResultsFor(ctx, access, id)
	if err != nil {
		return nil, err
	}
	return &knowledgeSummaryTestLease{ResultLease: lease, summary: source.summary}, nil
}

type knowledgeSummaryTestLease struct {
	searchjobs.ResultLease
	summary *opensplunk.KnowledgeSnapshotSummary
}

//nolint:unparam // Both results are required by knowledgeSnapshotSummaryProvider.
func (lease *knowledgeSummaryTestLease) knowledgeSnapshotSummary() (*opensplunk.KnowledgeSnapshotSummary, error) {
	cloned, _ := proto.Clone(lease.summary).(*opensplunk.KnowledgeSnapshotSummary)
	return cloned, nil
}

func newKnowledgeReexecutionFixture(
	t *testing.T,
) (*reexecutionTestSearches, searchjobs.Schema, searchjobs.AccessScope, searchjobs.ExecutionSnapshot, clickhouse.CompiledQuery) {
	t.Helper()
	searches, schema, access := newReexecutionTestSearches()
	const appID = "app_aaaaaaaaaaaaaaaaaaaaaA"
	cursorKey := []byte("export-knowledge-fixture-cursor-key-at-least-32-bytes")
	database, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: cursorKey,
		IDGenerator: func() (string, error) {
			return appID, nil
		},
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog(): %v", err)
	}
	if _, err := apps.CreateApp(
		context.Background(),
		control.AppAccessScope{TenantID: access.TenantID},
		control.AppDefinition{Slug: "export-app", DisplayName: "Export App"},
	); err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	store, err := knowledgecatalog.New(database, knowledgecatalog.Options{CursorKey: cursorKey})
	if err != nil {
		t.Fatalf("knowledgecatalog.New(): %v", err)
	}
	resolver, err := store.NewResolver(knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatalf("NewResolver(): %v", err)
	}
	searches.resolver = resolver
	searches.appID = appID
	searches.mu.Lock()
	err = searches.startManagerLocked()
	searches.mu.Unlock()
	if err != nil {
		t.Fatalf("start knowledge search manager: %v", err)
	}
	t.Cleanup(func() { _ = searches.manager.Close() })
	managerPin, execution, err := searches.manager.AcquireExecutionFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatalf("AcquireExecutionFor(): %v", err)
	}
	if err := managerPin.Close(); err != nil {
		t.Fatalf("Close fixture pin: %v", err)
	}
	if !execution.ValidKnowledgeAuthority() || execution.KnowledgeSnapshot.IsZero() || execution.CompiledQuery == nil {
		t.Fatal("manager did not mint a valid knowledge execution")
	}
	compiled, ok := execution.CompiledQuery.CloneForExecution()
	if !ok {
		t.Fatal("clone manager compiled execution")
	}
	searches.execution = &execution
	return searches, schema, access, execution, compiled
}

func validExportKnowledgeSummary() *opensplunk.KnowledgeSnapshotSummary {
	return &opensplunk.KnowledgeSnapshotSummary{
		Ref: &opensplunk.KnowledgeSnapshotRef{
			SnapshotSha256:          bytes.Repeat([]byte{0x11}, sha256.Size),
			TenantCatalogRevision:   7,
			TenantCatalogStateToken: bytes.Repeat([]byte{0x22}, sha256.Size),
			ObjectCount:             1,
			LookupAssetCount:        1,
		},
		Objects: []*opensplunk.KnowledgeSnapshotObjectSummary{{
			ResolutionOrdinal: 0,
			ObjectType:        opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			Stage:             opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
			Disclosure: &opensplunk.KnowledgeSnapshotObjectSummary_AuthorizedObject{
				AuthorizedObject: &opensplunk.KnowledgeSnapshotAuthorizedObjectSummary{
					KnowledgeObjectId: "object-1",
					Version:           3,
					Name:              "extract-one",
				},
			},
		}},
		LookupAssets: []*opensplunk.KnowledgeSnapshotLookupAsset{{
			AssetOrdinal:  0,
			LookupId:      "lookup-export",
			LookupVersion: 12,
			Asset: &opensplunk.KnowledgeLookupAssetVersionReference{
				LookupAssetId: "asset-export",
				Version:       9,
				SizeBytes:     256,
				ContentSha256: bytes.Repeat([]byte{0x33}, sha256.Size),
			},
		}},
	}
}

func assertExportKnowledgeSummary(t *testing.T, summary *opensplunk.KnowledgeSnapshotSummary) {
	t.Helper()
	if err := knowledgesnapshot.ValidateSummary(summary); err != nil {
		t.Fatalf("knowledge summary invalid: %v", err)
	}
	if got := summary.GetObjects()[0].GetAuthorizedObject().GetName(); got != "extract-one" {
		t.Fatalf("knowledge object name = %q", got)
	}
	if summary.GetRef().GetLookupAssetCount() != 1 ||
		len(summary.GetLookupAssets()) != 1 ||
		summary.GetLookupAssets()[0].GetLookupId() != "lookup-export" ||
		summary.GetLookupAssets()[0].GetLookupVersion() != 12 ||
		summary.GetLookupAssets()[0].GetAsset().GetLookupAssetId() != "asset-export" ||
		summary.GetLookupAssets()[0].GetAsset().GetVersion() != 9 {
		t.Fatalf("knowledge lookup provenance = %#v", summary.GetLookupAssets())
	}
}
