package searchsnapshot

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type statsWildcardReplayExecutor struct{}

func (statsWildcardReplayExecutor) ExecuteStatsWildcardInventory(
	_ context.Context,
	compiled clickhouse.CompiledStatsWildcardInventory,
) (plan.StatsWildcardExpansion, error) {
	request := compiled.Request()
	matches := make([]plan.StatsWildcardInventoryMatch, 0, len(request.Patterns()))
	for _, pattern := range request.Patterns() {
		matches = append(matches, plan.StatsWildcardInventoryMatch{
			Ordinal: pattern.Ordinal,
			Field:   "bytes",
		})
	}
	return plan.ValidateStatsWildcardInventory(request, matches)
}

func (statsWildcardReplayExecutor) Execute(
	_ context.Context,
	compiled clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	columns := make([]searchjobs.Column, len(compiled.OutputFields))
	for index, field := range compiled.OutputFields {
		columns[index] = searchjobs.Column{
			Name: field, Kind: searchjobs.ValueKindDouble, Nullable: true,
		}
	}
	return sink.SetSchema(searchjobs.Schema{Columns: columns})
}

type statsWildcardReplaySnapshotter struct{}

func (statsWildcardReplaySnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return 41, nil
}

func TestBuildExecutionPlanRejectsChangedKnowledgeForRetainedStatsWildcardInventory(t *testing.T) {
	ctx := context.Background()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	const (
		tenantID = "tenant-stats-wildcard-replay"
		ownerID  = "owner-stats-wildcard-replay"
		appID    = "app_aaaaaaaaaaaaaaaaaaaaaA"
	)
	appCatalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: []byte("stats-wildcard-replay-app-cursor-key-0001"),
		IDGenerator: func() (string, error) {
			return appID, nil
		},
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog(): %v", err)
	}
	if _, err := appCatalog.CreateApp(
		ctx,
		control.AppAccessScope{TenantID: tenantID},
		control.AppDefinition{Slug: "stats-replay", DisplayName: "Stats Replay"},
	); err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	store, err := knowledgecatalog.New(database, knowledgecatalog.Options{
		CursorKey: []byte("stats-wildcard-replay-catalog-key-000001"),
	})
	if err != nil {
		t.Fatalf("knowledgecatalog.New(): %v", err)
	}
	resolver, err := store.NewResolver(knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatalf("knowledgecatalog.NewResolver(): %v", err)
	}
	now := time.Date(2026, time.August, 11, 10, 0, 0, 0, time.UTC)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:          statsWildcardReplayExecutor{},
		Snapshotter:       statsWildcardReplaySnapshotter{},
		KnowledgeResolver: resolver,
		CleanupInterval:   -1,
		Now:               func() time.Time { return now },
		NewID:             func() string { return "stats-wildcard-replay-job" },
		CursorKey:         []byte("stats-wildcard-replay-search-key-0000001"),
	})
	if err != nil {
		t.Fatalf("searchjobs.New(): %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	timeRange, err := searchtime.NewAbsoluteRange(now.Add(-time.Hour), now)
	if err != nil {
		t.Fatalf("searchtime.NewAbsoluteRange(): %v", err)
	}
	created, err := manager.Create(ctx, searchjobs.CreateRequest{
		SPL:               `index=main | stats sum(*)`,
		OwnerID:           ownerID,
		TenantID:          tenantID,
		AppID:             appID,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         timeRange,
	})
	if err != nil {
		t.Fatalf("Manager.Create(): %v", err)
	}
	access := searchjobs.AccessScope{TenantID: tenantID, OwnerID: ownerID}
	deadline := time.Now().Add(3 * time.Second)
	for {
		job, getErr := manager.GetFor(access, created.ID)
		if getErr != nil {
			t.Fatalf("Manager.GetFor(): %v", getErr)
		}
		if job.State == searchjobs.StateCompleted {
			break
		}
		if job.State.Terminal() {
			t.Fatalf("wildcard replay fixture reached %s: %#v", job.State, job.Failure)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for wildcard replay fixture")
		}
		time.Sleep(time.Millisecond)
	}
	snapshot, err := manager.CompletedExecutionSnapshotFor(ctx, access, created.ID)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(): %v", err)
	}
	retained, err := snapshot.OpenRetainedKnowledgeExecution()
	if err != nil || retained == nil || retained.KnowledgePrelude.IsZero() {
		t.Fatalf("OpenRetainedKnowledgeExecution() = (%#v, %v)", retained, err)
	}
	if _, err := BuildExecutionPlan(snapshot); err != nil {
		t.Fatalf("BuildExecutionPlan(retained expansion): %v", err)
	}
	if _, err := BuildExecutionPlanWithKnowledgePrelude(
		snapshot,
		retained.KnowledgePrelude,
	); err != nil {
		t.Fatalf("BuildExecutionPlanWithKnowledgePrelude(exact): %v", err)
	}

	candidate := statsWildcardReplayCandidateProgram(t, appID, ownerID)
	if candidate.Equal(retained.KnowledgePrelude) {
		t.Fatal("changed candidate unexpectedly equals retained knowledge prelude")
	}
	if logical, err := BuildExecutionPlanWithKnowledgePrelude(snapshot, candidate); err == nil || logical != nil {
		t.Fatalf(
			"BuildExecutionPlanWithKnowledgePrelude(changed) = (%#v, %v), want stale-inventory failure",
			logical,
			err,
		)
	}
}

func statsWildcardReplayCandidateProgram(
	t *testing.T,
	appID string,
	ownerID string,
) knowledgeprogram.Program {
	t.Helper()
	definition := &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        appID,
		Name:         "candidate-alias",
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField:       "bytes",
				DestinationField:  "candidate_bytes",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			},
		},
	}
	normalized, err := knowledgedefinition.Normalize(definition)
	if err != nil {
		t.Fatalf("knowledgedefinition.Normalize(): %v", err)
	}
	program, err := knowledgeprogram.Prepare(knowledgeprogram.Input{Objects: []*opensplunkv1.KnowledgeSnapshotObject{{
		ResolutionOrdinal: 0,
		Stage:             opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
		StageOrdinal:      0,
		KnowledgeObjectId: "ko-stats-wildcard-replay-candidate",
		Version:           1,
		ObjectType:        normalized.ObjectType,
		Name:              normalized.Name,
		AppId:             normalized.AppID,
		OwnerId:           ownerID,
		SharingScope:      normalized.SharingScope,
		Definition:        normalized.Definition,
		DefinitionSha256:  append([]byte(nil), normalized.Digest[:]...),
	}}})
	if err != nil {
		t.Fatalf("knowledgeprogram.Prepare(): %v", err)
	}
	return program
}
