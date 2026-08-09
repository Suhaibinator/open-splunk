package searchinspection

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"google.golang.org/protobuf/proto"
)

func TestInspectKnowledgeExecutionUsesExactRetainedCompilerAuthority(t *testing.T) {
	snapshot := retainedKnowledgeInspectionSnapshot(t)
	wantCompiled, ok := snapshot.CompiledQuery.CloneForExecution()
	if !ok {
		t.Fatal("fixture retained query has no valid execution seal")
	}
	wantSummary := snapshot.KnowledgeSnapshot.Summary()
	if err := knowledgesnapshot.ValidateSummary(wantSummary); err != nil {
		t.Fatalf("fixture knowledge summary: %v", err)
	}
	retained, retainedErr := snapshot.OpenRetainedKnowledgeExecution()
	if retainedErr != nil || retained == nil {
		authority := snapshot.KnowledgeSnapshot.Proto()
		evidence, evidenceOK := snapshot.CompiledQuery.KnowledgeSnapshotEvidence()
		t.Fatalf(
			"fixture retained authority contract failed: authority=%#v evidence=%#v evidenceOK=%t indexes=%v error=%v",
			authority,
			evidence,
			evidenceOK,
			snapshot.EffectiveIndexes,
			retainedErr,
		)
	}
	normalized := normalizedRequest{
		access: searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID},
		jobID:  snapshot.ID,
	}
	if !validSnapshotContract(snapshot, normalized) {
		t.Fatalf("fixture execution snapshot contract failed: %#v", snapshot)
	}
	if !validGeneratedSQL(wantCompiled) {
		t.Fatal("fixture retained compiler output failed SQL seal validation")
	}
	logical, err := searchsnapshot.BuildExecutionPlan(snapshot)
	if err != nil {
		t.Fatalf("fixture BuildExecutionPlan(): %v", err)
	}
	retainedPrelude, retainedPreludePresent := logical.KnowledgePrelude()
	if !retainedPreludePresent ||
		!retainedPrelude.Equal(snapshot.KnowledgeSnapshot.Prelude()) {
		t.Fatal("fixture BuildExecutionPlan omitted or changed the retained knowledge prelude")
	}
	if _, err := projectLogicalPlan(context.Background(), logical, snapshot.SPL); err != nil {
		t.Fatalf("fixture projectLogicalPlan(): %v", err)
	}
	if !snapshot.Equal(snapshot) {
		t.Fatal("fixture execution snapshot is not equal to itself")
	}

	searches := &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{
		snapshot,
		snapshot,
	}}
	compiler := &inspectionCompiler{err: errors.New("mutable compiler is unavailable")}
	explainer := &inspectionExplainer{
		result: inspectionExplainResult("open-splunk-explain-retained-knowledge"),
	}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches: searches, Compiler: compiler, Explainer: explainer,
	})

	result, err := service.Inspect(
		context.Background(),
		searchjobs.AccessScope{
			TenantID: snapshot.TenantID,
			OwnerID:  snapshot.OwnerID,
		},
		Request{SearchJobID: snapshot.ID},
	)
	if err != nil {
		t.Fatalf(
			"Inspect(retained knowledge execution): %v (snapshots=%d compiler=%d explainer=%d explainedEqual=%t)",
			err,
			searches.callCount(),
			compiler.callCount(),
			explainer.callCount(),
			wantCompiled.EqualForExecution(explainer.lastQuery()),
		)
	}
	if compiler.callCount() != 0 {
		t.Fatalf("compiler calls = %d, want zero", compiler.callCount())
	}
	if searches.callCount() != 2 || explainer.callCount() != 1 {
		t.Fatalf(
			"snapshot/explainer calls = %d/%d, want 2/1",
			searches.callCount(),
			explainer.callCount(),
		)
	}
	explained := explainer.lastQuery()
	if !wantCompiled.EqualForExecution(explained) {
		t.Fatalf("Explainer did not receive exact retained execution\nwant=%#v\ngot=%#v", wantCompiled, explained)
	}
	if result.GeneratedSQL != wantCompiled.SQL ||
		!proto.Equal(result.KnowledgeSnapshot, wantSummary) {
		t.Fatalf("inspection result authority = %#v", result.KnowledgeSnapshot)
	}
	if err := ValidateResult(result); err != nil {
		t.Fatalf("ValidateResult(retained knowledge execution): %v", err)
	}
	if len(result.Plan.Stages) == 0 {
		t.Fatal("retained execution omitted detached logical projection")
	}

	// The result is detached from both the opaque snapshot and later calls.
	result.KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff
	if proto.Equal(result.KnowledgeSnapshot, snapshot.KnowledgeSnapshot.Summary()) {
		t.Fatal("result knowledge summary aliases retained snapshot authority")
	}
	again, err := service.Inspect(
		context.Background(),
		searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID},
		Request{SearchJobID: snapshot.ID},
	)
	if err != nil || !proto.Equal(again.KnowledgeSnapshot, wantSummary) {
		t.Fatalf("second inspection summary = (%#v, %v)", again.KnowledgeSnapshot, err)
	}
}

func TestInspectKnowledgeExecutionRejectsIncompleteOrTamperedAuthority(t *testing.T) {
	knowledge := retainedKnowledgeInspectionSnapshot(t)
	legacy := validInspectionSnapshot()
	tamperedCompiled := *knowledge.CompiledQuery
	tamperedCompiled.SQL += " -- tampered"

	tests := []struct {
		name     string
		snapshot searchjobs.ExecutionSnapshot
	}{
		{
			name: "legacy snapshot with invented retained compiler",
			snapshot: func() searchjobs.ExecutionSnapshot {
				legacy.CompiledQuery = knowledge.CompiledQuery
				return legacy
			}(),
		},
		{
			name: "knowledge snapshot without retained compiler",
			snapshot: func() searchjobs.ExecutionSnapshot {
				value := knowledge
				value.CompiledQuery = nil
				return value
			}(),
		},
		{
			name: "knowledge snapshot with tampered compiler seal",
			snapshot: func() searchjobs.ExecutionSnapshot {
				value := knowledge
				value.CompiledQuery = &tamperedCompiled
				return value
			}(),
		},
		{
			name: "knowledge authority disagrees with execution scope",
			snapshot: func() searchjobs.ExecutionSnapshot {
				value := knowledge
				value.EffectiveIndexes = []string{"other"}
				return value
			}(),
		},
		{
			name: "knowledge authority disagrees with execution app",
			snapshot: func() searchjobs.ExecutionSnapshot {
				value := knowledge
				value.AppID = "app_bbbbbbbbbbbbbbbbbbbbbB"
				return value
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compiler := &inspectionCompiler{}
			explainer := &inspectionExplainer{}
			service := newInspectionTestService(t, inspectionTestConfig{
				Searches:  &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{test.snapshot}},
				Compiler:  compiler,
				Explainer: explainer,
			})
			result, err := service.Inspect(
				context.Background(),
				searchjobs.AccessScope{
					TenantID: test.snapshot.TenantID,
					OwnerID:  test.snapshot.OwnerID,
				},
				Request{SearchJobID: test.snapshot.ID},
			)
			if !errors.Is(err, ErrInspectionFailed) {
				t.Fatalf("Inspect() error = %v, want ErrInspectionFailed", err)
			}
			assertZeroInspection(t, result)
			if compiler.callCount() != 0 || explainer.callCount() != 0 {
				t.Fatalf(
					"invalid authority reached compiler/explainer = %d/%d",
					compiler.callCount(),
					explainer.callCount(),
				)
			}
		})
	}
}

func TestInspectKnowledgePostflightMismatchSuppressesOutput(t *testing.T) {
	snapshot := retainedKnowledgeInspectionSnapshot(t)
	changed := snapshot
	changedCompiled := *snapshot.CompiledQuery
	changedCompiled.SQL += " -- changed after explain"
	changed.CompiledQuery = &changedCompiled

	compiler := &inspectionCompiler{}
	explainer := &inspectionExplainer{
		result: inspectionExplainResult("open-splunk-explain-knowledge-postflight-mismatch"),
	}
	service := newInspectionTestService(t, inspectionTestConfig{
		Searches: &inspectionSearches{snapshots: []searchjobs.ExecutionSnapshot{
			snapshot,
			changed,
		}},
		Compiler:  compiler,
		Explainer: explainer,
	})
	result, err := service.Inspect(
		context.Background(),
		searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID},
		Request{SearchJobID: snapshot.ID},
	)
	if !errors.Is(err, ErrInspectionFailed) {
		t.Fatalf("Inspect() error = %v, want ErrInspectionFailed", err)
	}
	assertZeroInspection(t, result)
	if compiler.callCount() != 0 || explainer.callCount() != 1 {
		t.Fatalf(
			"compiler/explainer calls = %d/%d, want 0/1",
			compiler.callCount(),
			explainer.callCount(),
		)
	}
}

func retainedKnowledgeInspectionSnapshot(t *testing.T) searchjobs.ExecutionSnapshot {
	t.Helper()
	const (
		tenantID = "inspection-knowledge-tenant"
		ownerID  = "inspection-knowledge-owner"
		appID    = "app_aaaaaaaaaaaaaaaaaaaaaA"
		jobID    = "inspection-knowledge-job"
	)
	cursorKey := []byte("inspection-knowledge-cursor-key-at-least-32-bytes")
	database, err := control.Open(
		context.Background(),
		filepath.Join(t.TempDir(), "control.db"),
	)
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	databaseClosed := false
	t.Cleanup(func() {
		if !databaseClosed {
			if closeErr := database.Close(); closeErr != nil {
				t.Errorf("close control database: %v", closeErr)
			}
		}
	})
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
		control.AppAccessScope{TenantID: tenantID},
		control.AppDefinition{Slug: "inspection-app", DisplayName: "Inspection App"},
	); err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	store, err := knowledgecatalog.New(
		database,
		knowledgecatalog.Options{CursorKey: cursorKey},
	)
	if err != nil {
		t.Fatalf("knowledgecatalog.New(): %v", err)
	}
	resolver, err := store.NewResolver(knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatalf("knowledgecatalog.NewResolver(): %v", err)
	}

	now := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: inspectionManagerExecutorFunc(func(
			_ context.Context,
			query clickhouse.CompiledQuery,
			sink searchjobs.ResultSink,
		) error {
			if !slices.Equal(query.OutputFields, []string{"message"}) {
				return searchjobs.ErrInvalidResult
			}
			return sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
				Name: "message", Kind: searchjobs.ValueKindString,
			}}})
		}),
		Snapshotter: inspectionSnapshotterFunc(func(context.Context) (uint64, error) {
			return 91, nil
		}),
		KnowledgeResolver: resolver,
		RetentionTTL:      time.Hour,
		CleanupInterval:   -1,
		Now:               func() time.Time { return now },
		NewID:             func() string { return jobID },
	})
	if err != nil {
		t.Fatalf("searchjobs.New(): %v", err)
	}
	t.Cleanup(func() {
		if closeErr := manager.Close(); closeErr != nil {
			t.Errorf("close search manager: %v", closeErr)
		}
	})
	resolvedRange, err := searchtime.NewAbsoluteRange(
		now.Add(-2*time.Hour),
		now.Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("searchtime.NewAbsoluteRange(): %v", err)
	}
	created, err := manager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               "index=main | table message",
		OwnerID:           ownerID,
		TenantID:          tenantID,
		AppID:             appID,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         resolvedRange,
	})
	if err != nil {
		t.Fatalf("Manager.Create(): %v", err)
	}
	completed := waitForInspectionJob(t, manager, created.ID)
	if completed.State != searchjobs.StateCompleted {
		t.Fatalf("knowledge job state = %v, want completed", completed.State)
	}
	snapshot, err := manager.CompletedExecutionSnapshotFor(
		context.Background(),
		searchjobs.AccessScope{TenantID: tenantID, OwnerID: ownerID},
		created.ID,
	)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(): %v", err)
	}
	if snapshot.KnowledgeSnapshot.IsZero() || snapshot.CompiledQuery == nil {
		t.Fatalf("configured execution authority = %#v", snapshot)
	}

	// Inspection receives only the retained authority. Closing the underlying
	// catalog proves subsequent mutable catalog/resolver availability cannot
	// influence the diagnostic execution.
	if err := database.Close(); err != nil {
		t.Fatalf("close catalog before inspection: %v", err)
	}
	databaseClosed = true
	return snapshot
}
