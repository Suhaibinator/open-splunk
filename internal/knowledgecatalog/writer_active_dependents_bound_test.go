package knowledgecatalog

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const activeDependentVisitFunction = "ko_test_active_dependent_visit_v1"

var activeDependentVisitProbes sync.Map

func TestActiveDependentLookupPlanIsBoundedCurrentFirst(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	assertActiveDependentLookupPlan(t, database)
}

func assertActiveDependentLookupPlan(t *testing.T, database *control.DB) {
	t.Helper()
	details := explainPostgreSQLQueryPlan(
		t,
		database.SQLDB(),
		activeDependentLookupSQL,
		[]any{"ko-plan-target", testTenant},
	)
	joined := strings.Join(details, "\n")
	for _, required := range []string{
		"on dependent table knowledge_objects using knowledge_objects_active_tenant_idx",
		"source_object_version = dependent.current_version",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("active-dependent plan lacks %q:\n%s", required, joined)
		}
	}
	if dependentIndex, dependencyIndex := indexContaining(details, "on dependent table"), indexContaining(details, "on dependency table"); dependentIndex < 0 || dependencyIndex <= dependentIndex {
		t.Fatalf("active-dependent plan is not current-registry first:\n%s", joined)
	}
	exactDependencyProbe := false
	for _, detail := range details {
		if !strings.Contains(detail, "on dependency table knowledge_object_dependencies using ") {
			continue
		}
		if !strings.HasPrefix(detail, "Index Scan") && !strings.HasPrefix(detail, "Index Only Scan") {
			t.Fatalf("dependency probe is not indexed:\n%s", joined)
		}
		for _, key := range []string{"tenant_id = dependent.tenant_id", "source_object_id = dependent.knowledge_object_id", "source_object_version = dependent.current_version", "target_kind =", "target_object_id ="} {
			if !strings.Contains(detail, key) {
				t.Fatalf("current dependency point probe lacks exact key %s:\n%s", key, joined)
			}
		}
		exactDependencyProbe = true
	}
	if !exactDependencyProbe || strings.Count(joined, "Limit") != 2 {
		t.Fatalf("current registry/dependency plan lacks two bounded drivers:\n%s", joined)
	}
	for _, forbidden := range []string{"Seq Scan on dependency", "Sort"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("active-dependent plan contains forbidden %q:\n%s", forbidden, joined)
		}
	}

}

func TestActiveDependentLookupIgnoresLargeValidHistoricalEdgeSet(t *testing.T) {
	database, store := newCatalogTestStore(t)
	const (
		targetID    = "ko-bounded-dependent-history-target"
		sourceCount = 32
		edgesPerOld = 128
	)
	insertActiveDependentHistoryFixtures(t, database, targetID, sourceCount, edgesPerOld)
	poisonActiveDependentPlannerStatistics(t, database)
	assertActiveDependentLookupPlan(t, database)

	var retainedEdges int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM knowledge_object_dependencies
		WHERE tenant_id = $1 AND target_kind = 'object' AND target_object_id = $2`,
		testTenant, targetID,
	).Scan(&retainedEdges); err != nil {
		t.Fatalf("count retained historical edges: %v", err)
	}
	if retainedEdges != sourceCount*edgesPerOld {
		t.Fatalf("retained historical edges = %d, want %d", retainedEdges, sourceCount*edgesPerOld)
	}

	// The retained graph is valid under the public structural limits: every old
	// draft root has 128 direct immutable targets (129 nodes total), while each
	// current draft version has deliberately moved on to an empty graph.
	historicalVersion := uint64(1)
	if _, err := store.Get(
		context.Background(),
		testReadScope(),
		"ko-bounded-dependent-history-source-00",
		&historicalVersion,
	); err != nil {
		t.Fatalf("Get(valid retained dependency history): %v", err)
	}

	const token = "active-dependent-history-probe"
	counter := &pgVisitCounter{}
	activeDependentVisitProbes.Store(token, counter)
	preparePostgresVisit(t, database, activeDependentVisitFunction, token, counter, false)
	t.Cleanup(func() { activeDependentVisitProbes.Delete(token) })
	instrumented := strings.Replace(
		activeDependentLookupSQL,
		"AND dependency.target_kind = 'object'",
		"AND "+activeDependentVisitFunction+"(?, dependency.source_object_version) = 1\n\t  AND dependency.target_kind = 'object'",
		1,
	)
	if instrumented == activeDependentLookupSQL {
		t.Fatal("instrument active-dependent lookup SQL")
	}
	var records []struct {
		Found int64 `gorm:"column:found"`
	}
	if err := database.GORMDB().Raw(instrumented, token, targetID, testTenant).Scan(&records).Error; err != nil {
		t.Fatalf("execute instrumented active-dependent lookup: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("historical-only active-dependent lookup returned %#v", records)
	}
	if visits := counter.Load(); visits != 0 {
		t.Fatalf("historical-only active-dependent dependency visits = %d, want 0", visits)
	}
	if blocked, err := hasActiveDependents(database.GORMDB(), testTenant, targetID); err != nil || blocked {
		t.Fatalf("hasActiveDependents(historical only) = %t, %v, want false", blocked, err)
	}

	disableHistoricalOnlyActiveTarget(t, database, targetID, edgesPerOld)
}

func TestActiveDependentLookupAndTriggerBlockExactCurrentActiveEdge(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	targetID := "ko-bounded-current-dependent-target"
	insertFixtureObject(t, database, fixtureObject{
		id: targetID,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp, "bounded-current-dependent-target", SharingScopePrivate,
				nil, "bounded-current-target-*", dependencyFixtureInputField,
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-bounded-current-dependent-source",
		versions: []fixtureVersion{{
			definition: dependencyAliasDefinition(
				testApp, "bounded-current-dependent-source", SharingScopePrivate,
				nil, "bounded-current-source-*", dependencyFixtureInputField, "bounded_alias",
			),
			state: StateActive, mutation: "create", timestamp: 20,
			dependencies: []fixtureDependency{{
				targetObjectID: targetID,
				targetVersion:  1,
			}},
		}},
	})

	if blocked, err := hasActiveDependents(database.GORMDB(), testTenant, targetID); err != nil || !blocked {
		t.Fatalf("hasActiveDependents(current active edge) = %t, %v, want true", blocked, err)
	}
	assertDirectActiveTargetTransitionBlocked(t, database, targetID)

	var triggerSQL string
	if err := database.SQLDB().QueryRowContext(t.Context(), `SELECT pg_get_triggerdef(t.oid)||';'||pg_get_functiondef(t.tgfoid) FROM pg_trigger t JOIN pg_class r ON r.oid=t.tgrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=current_schema() AND NOT t.tgisinternal AND t.tgname='knowledge_active_dependency_target_transition_is_blocked'`).Scan(&triggerSQL); err != nil {
		t.Fatalf("read active-dependent trigger SQL: %v", err)
	}
	for _, required := range []string{
		"knowledge_objects",
		"knowledge_object_dependencies",
		"LATERAL",
		"dependency.source_object_version = dependent.current_version",
		"dependency.target_kind = 'object'",
	} {
		if !pgtest.ContainsCheck(triggerSQL, required) {
			t.Fatalf("active-dependent trigger lacks %q:\n%s", required, triggerSQL)
		}
	}
}

func poisonActiveDependentPlannerStatistics(t *testing.T, database *control.DB) {
	t.Helper()
	if _, err := database.SQLDB().ExecContext(t.Context(), `ANALYZE`); err != nil {
		t.Fatalf("analyze active-dependent fixtures: %v", err)
	}
	// Poison the actual PostgreSQL index cardinalities inside this isolated
	// schema. The current-registry driver must remain bounded despite hostile
	// inverse-target estimates; no process-global planner settings are changed.
	if _, err := database.SQLDB().ExecContext(t.Context(), `UPDATE pg_class r SET reltuples=1000000000 WHERE r.relnamespace=current_schema()::regnamespace AND r.relname='knowledge_object_dependencies_source_target_idx'`); err != nil {
		t.Fatalf("poison source-index statistics: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), `UPDATE pg_class r SET reltuples=1 WHERE r.relnamespace=current_schema()::regnamespace AND r.relname='knowledge_object_dependencies_target_idx'`); err != nil {
		t.Fatalf("poison inverse-index statistics: %v", err)
	}

}

func insertActiveDependentHistoryFixtures(
	t *testing.T,
	database *control.DB,
	targetID string,
	sourceCount, edgesPerOld int,
) {
	t.Helper()
	targetVersions := make([]fixtureVersion, edgesPerOld)
	for index := range targetVersions {
		mutation := "update"
		if index == 0 {
			mutation = "create"
		}
		targetVersions[index] = fixtureVersion{
			definition: dependencyExtractionDefinition(
				testApp,
				"bounded-dependent-history-target",
				SharingScopePrivate,
				nil,
				fmt.Sprintf("bounded-history-target-v%03d-*", index+1),
				dependencyFixtureInputField,
			),
			state: StateActive, mutation: mutation, timestamp: int64(100 + index),
		}
	}
	insertFixtureObject(t, database, fixtureObject{id: targetID, versions: targetVersions})

	dependencies := make([]fixtureDependency, edgesPerOld)
	for index := range dependencies {
		dependencies[index] = fixtureDependency{targetObjectID: targetID, targetVersion: int64(index + 1)}
	}
	for source := range sourceCount {
		name := fmt.Sprintf("bounded-dependent-history-source-%02d", source)
		insertFixtureObject(t, database, fixtureObject{
			id: "ko-" + name,
			versions: []fixtureVersion{
				{
					definition: dependencyAliasDefinition(
						testApp, name, SharingScopePrivate, nil,
						name+"-old-*", dependencyFixtureInputField, "bounded_alias",
					),
					state: StateDraft, mutation: "create", timestamp: int64(1_000 + source*2),
					dependencies: dependencies,
				},
				{
					definition: dependencyAliasDefinition(
						testApp, name, SharingScopePrivate, nil,
						name+"-current-*", dependencyFixtureInputField, "bounded_alias",
					),
					state: StateDraft, mutation: "update", timestamp: int64(1_001 + source*2),
				},
			},
		})
	}
}

func disableHistoricalOnlyActiveTarget(
	t *testing.T,
	database *control.DB,
	targetID string,
	expectedVersion uint64,
) {
	t.Helper()
	auditStore, err := audit.NewStore(database, audit.StoreOptions{})
	if err != nil {
		t.Fatalf("audit.NewStore(historical active-dependent fixture): %v", err)
	}
	writer, err := NewWriter(database, auditStore, WriterOptions{
		Clock:                func() time.Time { return time.UnixMicro(10_000).UTC() },
		IDGenerator:          func() (string, error) { return "unused-bounded-dependent-id", nil },
		IdempotencyRetention: minimumIdempotencyRetention,
	})
	if err != nil {
		t.Fatalf("NewWriter(historical active-dependent fixture): %v", err)
	}
	ctx, err := audit.WithActor(context.Background(), audit.Actor{
		Kind: audit.ActorKindBrowser, ID: testOwner, Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(historical active-dependent fixture): %v", err)
	}
	response, err := writer.SetState(ctx, WriteScope{
		TenantID: testTenant, OwnerID: testOwner, WritableAppIDs: []string{testApp}, OrganizationID: 1,
	}, &opensplunk.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: targetID,
		ExpectedVersion:   expectedVersion,
		State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		ClientRequestId:   "historical-dependent-disable-0001",
	})
	if err != nil {
		t.Fatalf("SetState(target with historical-only dependents): %v", err)
	}
	if response.GetKnowledgeObject().GetVersion() != expectedVersion+1 ||
		response.GetKnowledgeObject().GetState() != opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED {
		t.Fatalf("SetState(target with historical-only dependents) = %#v", response)
	}
}

func assertDirectActiveTargetTransitionBlocked(
	t *testing.T,
	database *control.DB,
	targetID string,
) {
	t.Helper()
	tx, err := pgmeta.BeginSQL(t.Context(), database.SQLDB(), nil)
	if err != nil {
		t.Fatalf("begin direct active-target transition: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(t.Context(), `UPDATE knowledge_objects
		SET current_version = current_version + 1,
		    state = 'disabled',
		    updated_at_unix_micro = updated_at_unix_micro + 1,
		    disabled_at_unix_micro = updated_at_unix_micro + 1
		WHERE tenant_id = $1 AND knowledge_object_id = $2`, testTenant, targetID)
	if err == nil || !strings.Contains(err.Error(), "active dependents") {
		t.Fatalf("direct active-target transition error = %v, want active dependents", err)
	}
}
