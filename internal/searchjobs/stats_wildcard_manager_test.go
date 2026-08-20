package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type statsWildcardTestExecutor struct {
	mu                      sync.Mutex
	inventoryCalls          int
	executeCalls            int
	inventoryFields         []string
	inventoryDelay          time.Duration
	lastExecutionTimeBudget time.Duration
}

func (executor *statsWildcardTestExecutor) ExecuteStatsWildcardInventory(
	_ context.Context,
	query clickhouse.CompiledStatsWildcardInventory,
) (plan.StatsWildcardExpansion, error) {
	executor.mu.Lock()
	executor.inventoryCalls++
	delay := executor.inventoryDelay
	executor.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	request := query.Request()
	matches := make([]plan.StatsWildcardInventoryMatch, 0, request.MaximumPairs())
	fields := executor.inventoryFields
	if len(fields) == 0 {
		fields = []string{"bytes", "delay"}
	}
	for _, pattern := range request.Patterns() {
		for _, field := range fields {
			if spl.MatchStatsFieldGlob(pattern.Pattern, field) {
				matches = append(matches, plan.StatsWildcardInventoryMatch{
					Ordinal: pattern.Ordinal,
					Field:   field,
				})
			}
		}
	}
	return plan.ValidateStatsWildcardInventory(request, matches)
}

func (executor *statsWildcardTestExecutor) Execute(
	ctx context.Context,
	query clickhouse.CompiledQuery,
	sink ResultSink,
) error {
	executor.mu.Lock()
	executor.executeCalls++
	if deadline, ok := ctx.Deadline(); ok {
		executor.lastExecutionTimeBudget = time.Until(deadline)
	}
	executor.mu.Unlock()
	columns := make([]Column, len(query.OutputFields))
	for index, name := range query.OutputFields {
		columns[index] = Column{Name: name, Kind: ValueKindDouble, Nullable: true}
	}
	return sink.SetSchema(Schema{Columns: columns})
}

func (executor *statsWildcardTestExecutor) calls() (int, int) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.inventoryCalls, executor.executeCalls
}

func (executor *statsWildcardTestExecutor) executionTimeBudget() time.Duration {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.lastExecutionTimeBudget
}

func TestManagerExecutesAndSealsOpenSchemaStatsWildcardExpansion(t *testing.T) {
	executor := &statsWildcardTestExecutor{}
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
		NewID:           sequenceIDs("stats-wildcard-open-schema"),
	})
	request := validRequest()
	request.SPL = `index=main | stats sum(*)`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.Schema == nil || len(completed.Schema.Columns) != 2 {
		t.Fatalf("completed schema = %#v, want two expanded stats columns", completed.Schema)
	}
	inventoryCalls, executeCalls := executor.calls()
	if inventoryCalls != 1 || executeCalls != 1 {
		t.Fatalf("executor calls = inventory %d search %d, want 1/1", inventoryCalls, executeCalls)
	}
	snapshot, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		created.ID,
	)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(): %v", err)
	}
	expansion, present, err := snapshot.OpenRetainedStatsWildcardExpansion()
	if err != nil || !present || expansion.IsZero() {
		t.Fatalf("OpenRetainedStatsWildcardExpansion() = (zero=%t, present=%t, %v)", expansion.IsZero(), present, err)
	}
	if _, ok := expansion.AuthorityDigest(); !ok {
		t.Fatal("retained wildcard expansion has no authority digest")
	}
	fresh, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		created.ID,
	)
	if err != nil || !snapshot.Equal(fresh) {
		t.Fatalf("fresh execution snapshot equality = %t, err=%v", snapshot.Equal(fresh), err)
	}
	stripped := snapshot
	stripped.StatsWildcardExpansion = plan.StatsWildcardExpansion{}
	if stripped.ValidKnowledgeAuthority() {
		t.Fatal("stripping wildcard replay evidence preserved execution authority")
	}
	if _, _, openErr := stripped.OpenRetainedStatsWildcardExpansion(); !errors.Is(openErr, ErrResultsUnavailable) {
		t.Fatalf("OpenRetainedStatsWildcardExpansion(stripped) = %v, want unavailable", openErr)
	}
}

func TestValidateDefersOpenSchemaStatsWildcardInventoryExecution(t *testing.T) {
	executor := &statsWildcardTestExecutor{}
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
	})
	rangeStart := time.Date(2026, time.August, 11, 8, 0, 0, 0, time.UTC)
	result, err := manager.Validate(context.Background(), ValidateRequest{
		SPL:               `index=allowed | stats sum(*)`,
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"allowed"},
		RequestedIndexes:  []string{"allowed"},
		TimeRange:         mustAbsoluteTimeRange(rangeStart, rangeStart.Add(time.Hour)),
	})
	if err != nil || !result.Valid || len(result.Diagnostics) != 0 {
		t.Fatalf("Validate(raw wildcard) = (%#v, %v), want valid deferred query", result, err)
	}
	inventoryCalls, executeCalls := executor.calls()
	if inventoryCalls != 0 || executeCalls != 0 {
		t.Fatalf("Validate executed storage: inventory %d search %d", inventoryCalls, executeCalls)
	}
}

func TestManagerFailsClosedWhenStatsWildcardInventoryCapabilityIsAbsent(t *testing.T) {
	var executeCalls int
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, _ ResultSink) error {
			executeCalls++
			return nil
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("stats-wildcard-no-capability"),
	})
	request := validRequest()
	request.SPL = `index=main | stats sum(*)`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	failed := waitForState(t, manager, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureUnsupportedSPL {
		t.Fatalf("failed job = %#v, want unsupported SPL", failed.Failure)
	}
	if executeCalls != 0 {
		t.Fatalf("ordinary executor calls = %d, want zero", executeCalls)
	}
}

func TestStatsWildcardExpansionMetadataIsReleasedByCleanup(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.August, 11, 9, 0, 0, 0, time.UTC)}
	executor := &statsWildcardTestExecutor{}
	manager := newTestManager(t, Config{
		Executor:         executor,
		RetentionTTL:     time.Second,
		ExpiredRetention: time.Nanosecond,
		CleanupInterval:  -1,
		Now:              clock.Now,
		NewID:            sequenceIDs("stats-wildcard-metadata"),
	})
	request := validRequest()
	request.SPL = `index=main | stats sum(*)`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	waitForState(t, manager, created.ID, StateCompleted)

	manager.budgetMu.Lock()
	metadataBeforeCleanup := manager.metadataBytes
	manager.budgetMu.Unlock()
	if metadataBeforeCleanup == 0 {
		t.Fatal("completed wildcard execution retained no metadata")
	}

	clock.Add(time.Second)
	if changed := manager.Cleanup(); changed != 1 {
		t.Fatalf("expiration cleanup changed %d jobs, want 1", changed)
	}
	clock.Add(time.Nanosecond)
	if changed := manager.Cleanup(); changed != 1 {
		t.Fatalf("deletion cleanup changed %d jobs, want 1", changed)
	}
	manager.budgetMu.Lock()
	metadataAfterCleanup, retainedAfterCleanup := manager.metadataBytes, manager.retainedBytes
	manager.budgetMu.Unlock()
	if metadataAfterCleanup != 0 || retainedAfterCleanup != 0 {
		t.Fatalf(
			"cleanup retained metadata=%d result bytes=%d, want 0/0",
			metadataAfterCleanup,
			retainedAfterCleanup,
		)
	}
}

func TestStatsWildcardExpansionPrivateAuthorityParticipatesInMetadataCapacity(t *testing.T) {
	resolver, appID := newEmptyKnowledgeResolver(t)
	request := validRequest()
	request.AppID = appID
	primaryIndex := "primary_" + strings.Repeat("x", 40)
	request.SPL = `index=` + primaryIndex + ` | eval padding="` + strings.Repeat("x", 15<<10) + `" | stats sum(*)`
	request.RequestedIndexes = []string{primaryIndex}
	request.AuthorizedIndexes = []string{primaryIndex}
	for index := range 127 {
		request.AuthorizedIndexes = append(
			request.AuthorizedIndexes,
			fmt.Sprintf("scope_index_%03d_%s", index, strings.Repeat("x", 32)),
		)
	}

	const id = "stats-wildcard-metadata-capacity"
	probe := newTestManager(t, Config{
		Executor:          &statsWildcardTestExecutor{},
		KnowledgeResolver: resolver,
		CleanupInterval:   -1,
		NewID:             func() string { return id },
	})
	prepared, err := probe.prepareKnowledgeAdmission(
		context.Background(),
		request,
		0,
		time.Date(2026, time.August, 11, 9, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("prepareKnowledgeAdmission(): %v", err)
	}
	wildcardBytes, ok := prepared.wildcardExpansion.RetainedBytes()
	if !ok || wildcardBytes == 0 {
		t.Fatalf("wildcard expansion RetainedBytes() = (%d, %t)", wildcardBytes, ok)
	}
	baselineRequest := validRequest()
	baselineRequest.AppID = appID
	baselineRequest.SPL = `index=main | stats sum(*)`
	baseline, err := probe.prepareKnowledgeAdmission(
		context.Background(),
		baselineRequest,
		0,
		time.Date(2026, time.August, 11, 9, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("prepareKnowledgeAdmission(baseline): %v", err)
	}
	baselineWildcardBytes, ok := baseline.wildcardExpansion.RetainedBytes()
	if !ok {
		t.Fatal("baseline wildcard expansion has no retained-byte authority")
	}
	// The longer authored source is retained once; the effective index name is
	// retained independently in both authorized and requested private scope
	// slices. Fixed structures, patterns, and matches are otherwise identical.
	minimumPrivateIncrease := uint64(
		len(request.SPL) - len(baselineRequest.SPL) +
			2*(len(primaryIndex)-len("main")),
	)
	if wildcardBytes < baselineWildcardBytes+minimumPrivateIncrease {
		t.Fatalf(
			"wildcard authority charge = %d, baseline %d, want private source/scope increase >= %d",
			wildcardBytes,
			baselineWildcardBytes,
			minimumPrivateIncrease,
		)
	}

	baseBytes, err := retainedJobMetadataReservation(id, request)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.metadataBytes < wildcardBytes {
		t.Fatalf("base/prepared metadata = %d/%d: %v", baseBytes, prepared.metadataBytes, err)
	}
	withoutWildcard, err := checkedAdd(baseBytes, prepared.metadataBytes-wildcardBytes)
	if err != nil {
		t.Fatal(err)
	}
	withWildcard, err := checkedAdd(withoutWildcard, wildcardBytes)
	if err != nil || withWildcard <= withoutWildcard {
		t.Fatalf("metadata totals without/with wildcard = %d/%d: %v", withoutWildcard, withWildcard, err)
	}

	executor := &statsWildcardTestExecutor{}
	var journalCalls atomic.Int32
	limited := newTestManager(t, Config{
		Executor:          executor,
		KnowledgeResolver: resolver,
		MaxMetadataBytes:  withoutWildcard,
		CleanupInterval:   -1,
		NewID:             func() string { return id },
		Journal: jobJournalFunc{admit: func(context.Context, Job) error {
			journalCalls.Add(1)
			return nil
		}},
	})
	created, err := limited.Create(context.Background(), request)
	if !errors.Is(err, ErrCapacity) || created.ID != "" {
		t.Fatalf("Create(metadata excludes wildcard authority) = (%#v, %v)", created, err)
	}
	inventoryCalls, executeCalls := executor.calls()
	if inventoryCalls != 1 || executeCalls != 0 || journalCalls.Load() != 0 {
		t.Fatalf(
			"capacity side effects = inventory %d search %d journal %d, want 1/0/0",
			inventoryCalls,
			executeCalls,
			journalCalls.Load(),
		)
	}
	assertEmptyManagerAdmissionState(t, limited)
}

func TestKnowledgeAdmissionDiscoversGeneratedStatsWildcardField(t *testing.T) {
	resolver, appID := newCalculatedFieldKnowledgeResolver(
		t,
		"tenant",
		"knowledge_latency",
	)
	executor := &statsWildcardTestExecutor{inventoryFields: []string{"knowledge_latency"}}
	manager := newTestManager(t, Config{
		Executor:          executor,
		KnowledgeResolver: resolver,
		CleanupInterval:   -1,
		NewID:             sequenceIDs("stats-wildcard-knowledge"),
	})
	request := validRequest()
	request.AppID = appID
	request.SPL = `index=main | eval keep=if(status=200, 1, 0) | stats sum(knowledge_*) | where 'sum(knowledge_latency)'>0`
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.Schema == nil || len(completed.Schema.Columns) != 1 ||
		completed.Schema.Columns[0].Name != "sum(knowledge_latency)" {
		t.Fatalf("completed schema = %#v, want expanded generated field", completed.Schema)
	}
	if created.KnowledgeSnapshot == nil || created.KnowledgeSnapshot.GetRef().GetObjectCount() != 1 {
		t.Fatalf("created knowledge snapshot = %#v, want one generated object", created.KnowledgeSnapshot)
	}
	inventoryCalls, executeCalls := executor.calls()
	if inventoryCalls != 1 || executeCalls != 1 {
		t.Fatalf("executor calls = inventory %d search %d, want 1/1", inventoryCalls, executeCalls)
	}
	snapshot, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID},
		created.ID,
	)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(): %v", err)
	}
	if _, present, openErr := snapshot.OpenRetainedStatsWildcardExpansion(); openErr != nil || !present {
		t.Fatalf("OpenRetainedStatsWildcardExpansion() = (present=%t, %v)", present, openErr)
	}
	retainedKnowledge, err := snapshot.OpenRetainedKnowledgeExecution()
	if err != nil || retainedKnowledge == nil || retainedKnowledge.KnowledgePrelude.ObjectCount() != 1 {
		t.Fatalf("OpenRetainedKnowledgeExecution() = (%#v, %v)", retainedKnowledge, err)
	}
}

func newCalculatedFieldKnowledgeResolver(
	t *testing.T,
	tenantID string,
	destination string,
) (KnowledgeResolver, string) {
	t.Helper()
	ctx := context.Background()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("control DB Close(): %v", err)
		}
	})
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name:             "main",
		DisplayName:      "main",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
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
	if _, err := appCatalog.CreateApp(ctx, control.AppAccessScope{TenantID: tenantID}, control.AppDefinition{
		Slug: "search-app", DisplayName: "Search App",
	}); err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("audit.NewStore(): %v", err)
	}
	writer, err := knowledgecatalog.NewWriter(database, auditStore, knowledgecatalog.WriterOptions{
		IDGenerator: func() (string, error) {
			return "ko-stats-wildcard-calculated", nil
		},
	})
	if err != nil {
		t.Fatalf("knowledgecatalog.NewWriter(): %v", err)
	}
	actorContext, err := audit.WithActor(ctx, audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "stats-wildcard-test-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(): %v", err)
	}
	definition := &opensplunk.KnowledgeObjectDefinition{
		AppId:        appID,
		Name:         "stats-wildcard-calculated",
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		Selector: &opensplunk.KnowledgeSelector{
			IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "main"}},
		},
		Body: &opensplunk.KnowledgeObjectDefinition_CalculatedField{
			CalculatedField: &opensplunk.CalculatedFieldDefinition{
				DestinationField:  destination,
				Expression:        "1",
				OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			},
		},
	}
	if _, err := writer.Create(actorContext, knowledgecatalog.WriteScope{
		TenantID: tenantID, OwnerID: "owner", WritableAppIDs: []string{appID},
	}, &opensplunk.CreateKnowledgeObjectRequest{
		Definition:      definition,
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		ClientRequestId: "stats-wildcard-calculated-create-0001",
	}); err != nil {
		t.Fatalf("Writer.Create(calculated field): %v", err)
	}
	store, err := knowledgecatalog.New(database, knowledgecatalog.Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("knowledgecatalog.New(): %v", err)
	}
	resolver, err := knowledgecatalog.NewResolver(store, knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatalf("knowledgecatalog.NewResolver(): %v", err)
	}
	return resolver, appID
}

func TestStatsWildcardInventoryAndSearchShareOneRuntimeBudget(t *testing.T) {
	const (
		maximumRuntime = 500 * time.Millisecond
		inventoryDelay = 120 * time.Millisecond
	)

	t.Run("legacy", func(t *testing.T) {
		executor := &statsWildcardTestExecutor{inventoryDelay: inventoryDelay}
		manager := newTestManager(t, Config{
			Executor:        executor,
			MaxRuntime:      maximumRuntime,
			CleanupInterval: -1,
			NewID:           sequenceIDs("stats-wildcard-runtime-legacy"),
		})
		request := validRequest()
		request.SPL = `index=main | stats sum(*)`
		created, err := manager.Create(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, created.ID, StateCompleted)
		assertReducedStatsWildcardRuntimeBudget(
			t,
			executor.executionTimeBudget(),
			maximumRuntime,
			inventoryDelay,
		)
	})

	t.Run("knowledge", func(t *testing.T) {
		resolver, appID := newCalculatedFieldKnowledgeResolver(
			t,
			"tenant",
			"knowledge_latency",
		)
		executor := &statsWildcardTestExecutor{
			inventoryDelay:  inventoryDelay,
			inventoryFields: []string{"knowledge_latency"},
		}
		manager := newTestManager(t, Config{
			Executor:          executor,
			KnowledgeResolver: resolver,
			MaxRuntime:        maximumRuntime,
			CleanupInterval:   -1,
			NewID:             sequenceIDs("stats-wildcard-runtime-knowledge"),
		})
		request := validRequest()
		request.AppID = appID
		request.SPL = `index=main | stats sum(knowledge_*)`
		created, err := manager.Create(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, created.ID, StateCompleted)
		assertReducedStatsWildcardRuntimeBudget(
			t,
			executor.executionTimeBudget(),
			maximumRuntime,
			inventoryDelay,
		)
	})

	t.Run("ordinary search retains full budget", func(t *testing.T) {
		executor := &statsWildcardTestExecutor{}
		manager := newTestManager(t, Config{
			Executor:        executor,
			MaxRuntime:      maximumRuntime,
			CleanupInterval: -1,
			NewID:           sequenceIDs("stats-wildcard-runtime-ordinary"),
		})
		request := validRequest()
		request.SPL = `index=main | stats sum(bytes)`
		created, err := manager.Create(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, created.ID, StateCompleted)
		budget := executor.executionTimeBudget()
		if budget <= maximumRuntime-inventoryDelay/2 || budget > maximumRuntime {
			t.Fatalf("ordinary execution budget = %s, want essentially full %s", budget, maximumRuntime)
		}
	})
}

func TestExhaustedStatsWildcardRuntimeBudgetPreventsFinalExecutionAndAdmission(t *testing.T) {
	const (
		maximumRuntime = 30 * time.Millisecond
		inventoryDelay = 70 * time.Millisecond
	)

	t.Run("legacy", func(t *testing.T) {
		executor := &statsWildcardTestExecutor{inventoryDelay: inventoryDelay}
		manager := newTestManager(t, Config{
			Executor:        executor,
			MaxRuntime:      maximumRuntime,
			CleanupInterval: -1,
			NewID:           sequenceIDs("stats-wildcard-runtime-exhausted"),
		})
		request := validRequest()
		request.SPL = `index=main | stats sum(*)`
		created, err := manager.Create(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		failed := waitForState(t, manager, created.ID, StateFailed)
		if failed.Failure == nil || failed.Failure.Code != FailureTimeout {
			t.Fatalf("failed job = %#v, want timeout", failed.Failure)
		}
		inventoryCalls, executeCalls := executor.calls()
		if inventoryCalls != 1 || executeCalls != 0 {
			t.Fatalf("executor calls = inventory %d search %d, want 1/0", inventoryCalls, executeCalls)
		}
	})

	t.Run("knowledge pre-journal", func(t *testing.T) {
		resolver, appID := newCalculatedFieldKnowledgeResolver(
			t,
			"tenant",
			"knowledge_latency",
		)
		executor := &statsWildcardTestExecutor{
			inventoryDelay:  inventoryDelay,
			inventoryFields: []string{"knowledge_latency"},
		}
		var idCalls atomic.Int32
		var journalCalls atomic.Int32
		manager := newTestManager(t, Config{
			Executor:          executor,
			KnowledgeResolver: resolver,
			MaxRuntime:        maximumRuntime,
			CleanupInterval:   -1,
			NewID: func() string {
				idCalls.Add(1)
				return "stats-wildcard-runtime-knowledge-exhausted"
			},
			Journal: jobJournalFunc{admit: func(context.Context, Job) error {
				journalCalls.Add(1)
				return nil
			}},
		})
		request := validRequest()
		request.AppID = appID
		request.SPL = `index=main | stats sum(knowledge_*)`
		if created, err := manager.Create(context.Background(), request); !errors.Is(err, ErrKnowledgeUnavailable) || created.ID != "" {
			t.Fatalf("Create(exhausted knowledge inventory) = (%#v, %v)", created, err)
		}
		inventoryCalls, executeCalls := executor.calls()
		if inventoryCalls != 1 || executeCalls != 0 || idCalls.Load() != 0 || journalCalls.Load() != 0 {
			t.Fatalf(
				"side effects = inventory %d search %d ids %d journal %d, want 1/0/0/0",
				inventoryCalls,
				executeCalls,
				idCalls.Load(),
				journalCalls.Load(),
			)
		}
	})
}

func assertReducedStatsWildcardRuntimeBudget(
	t *testing.T,
	budget time.Duration,
	maximum time.Duration,
	inventoryDelay time.Duration,
) {
	t.Helper()
	if budget <= 0 || budget >= maximum-inventoryDelay/2 {
		t.Fatalf(
			"final execution budget = %s, want positive budget reduced from %s by inventory duration near %s",
			budget,
			maximum,
			inventoryDelay,
		)
	}
}
