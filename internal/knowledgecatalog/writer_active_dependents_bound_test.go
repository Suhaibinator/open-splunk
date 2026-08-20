package knowledgecatalog

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	moderncsqlite "modernc.org/sqlite"
)

const activeDependentVisitFunction = "ko_test_active_dependent_visit_v1"

var (
	activeDependentVisitRegisterOnce sync.Once
	activeDependentVisitRegisterErr  error
	activeDependentVisitProbes       sync.Map
)

func registerActiveDependentVisitFunction(t *testing.T) {
	t.Helper()
	activeDependentVisitRegisterOnce.Do(func() {
		activeDependentVisitRegisterErr = moderncsqlite.RegisterScalarFunction(
			activeDependentVisitFunction,
			2,
			func(_ *moderncsqlite.FunctionContext, arguments []driver.Value) (driver.Value, error) {
				if len(arguments) != 2 {
					return nil, fmt.Errorf("active-dependent visit arguments = %d", len(arguments))
				}
				token, ok := arguments[0].(string)
				if !ok || token == "" {
					return nil, fmt.Errorf("active-dependent visit token has type %T", arguments[0])
				}
				value, found := activeDependentVisitProbes.Load(token)
				if !found {
					return nil, fmt.Errorf("active-dependent visit token is unknown")
				}
				counter, ok := value.(*atomic.Int64)
				if !ok || counter == nil {
					return nil, fmt.Errorf("active-dependent visit counter is invalid")
				}
				counter.Add(1)
				return int64(1), nil
			},
		)
	})
	if activeDependentVisitRegisterErr != nil {
		t.Fatalf("register active-dependent visit function: %v", activeDependentVisitRegisterErr)
	}
}

func TestActiveDependentLookupPlanIsBoundedCurrentFirst(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	assertActiveDependentLookupPlan(t, database)
}

func assertActiveDependentLookupPlan(t *testing.T, database *control.DB) {
	t.Helper()
	details := explainSQLiteQueryPlan(
		t,
		database.SQLDB(),
		activeDependentLookupSQL,
		[]any{testTenant, "ko-plan-target"},
	)
	joined := strings.Join(details, "\n")
	for _, required := range []string{
		"dependent USING INDEX knowledge_objects_resolution_idx",
		"dependency USING COVERING INDEX knowledge_object_dependencies_source_target_idx",
		"tenant_id=? AND source_object_id=? AND source_object_version=?",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("active-dependent plan lacks %q:\n%s", required, joined)
		}
	}
	if dependentIndex, dependencyIndex := indexContaining(details, "dependent USING"), indexContaining(details, "dependency USING"); dependentIndex < 0 || dependencyIndex <= dependentIndex {
		t.Fatalf("active-dependent plan is not current-registry first:\n%s", joined)
	}
	for _, forbidden := range []string{
		"knowledge_object_dependencies_target_idx",
		"sqlite_autoindex_knowledge_object_dependencies",
		"SCAN dependency",
		"USE TEMP B-TREE",
	} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("active-dependent plan contains forbidden %q:\n%s", forbidden, joined)
		}
	}
}

func TestActiveDependentLookupIgnoresLargeValidHistoricalEdgeSet(t *testing.T) {
	registerActiveDependentVisitFunction(t)
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
		WHERE tenant_id = ? AND target_kind = 'object' AND target_object_id = ?`,
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
	counter := &atomic.Int64{}
	activeDependentVisitProbes.Store(token, counter)
	t.Cleanup(func() { activeDependentVisitProbes.Delete(token) })
	instrumented := strings.Replace(
		activeDependentLookupSQL,
		"\t  AND dependency.target_kind = 'object'",
		"\t  AND "+activeDependentVisitFunction+"(?, dependency.source_object_version) = 1\n\t  AND dependency.target_kind = 'object'",
		1,
	)
	if instrumented == activeDependentLookupSQL {
		t.Fatal("instrument active-dependent lookup SQL")
	}
	var records []struct {
		Found int64 `gorm:"column:found"`
	}
	if err := database.GORMDB().Raw(instrumented, testTenant, token, targetID).Scan(&records).Error; err != nil {
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
	if err := database.SQLDB().QueryRowContext(t.Context(), `SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'knowledge_active_dependency_target_transition_is_blocked'`).Scan(&triggerSQL); err != nil {
		t.Fatalf("read active-dependent trigger SQL: %v", err)
	}
	for _, required := range []string{
		"INDEXED BY knowledge_objects_resolution_idx",
		"CROSS JOIN knowledge_object_dependencies",
		"INDEXED BY knowledge_object_dependencies_source_target_idx",
		"dependency.source_object_version = dependent.current_version",
		"dependency.target_kind = 'object'",
	} {
		if !strings.Contains(triggerSQL, required) {
			t.Fatalf("active-dependent trigger lacks %q:\n%s", required, triggerSQL)
		}
	}
}

func poisonActiveDependentPlannerStatistics(t *testing.T, database *control.DB) {
	t.Helper()
	if _, err := database.SQLDB().ExecContext(t.Context(), `ANALYZE`); err != nil {
		t.Fatalf("analyze active-dependent fixtures: %v", err)
	}
	// Lie that the forced source index is maximally unselective and the inverse
	// target index is maximally selective. INDEXED BY must keep both the writer
	// and trigger access path bounded even if persisted statistics are hostile.
	if _, err := database.SQLDB().ExecContext(t.Context(), `UPDATE sqlite_stat1
		SET stat = '1000000000 1000000000 1000000000 1000000000 1000000000 1000000000'
		WHERE idx = 'knowledge_object_dependencies_source_target_idx'`); err != nil {
		t.Fatalf("poison source-index statistics: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), `UPDATE sqlite_stat1
		SET stat = '1 1 1 1 1 1 1'
		WHERE idx = 'knowledge_object_dependencies_target_idx'`); err != nil {
		t.Fatalf("poison inverse-index statistics: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), `ANALYZE sqlite_schema`); err != nil {
		t.Fatalf("reload hostile active-dependent statistics: %v", err)
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
		TenantID: testTenant, OwnerID: testOwner, WritableAppIDs: []string{testApp},
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
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin direct active-target transition: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(t.Context(), `UPDATE knowledge_objects
		SET current_version = current_version + 1,
		    state = 'disabled',
		    updated_at_unix_micro = updated_at_unix_micro + 1,
		    disabled_at_unix_micro = updated_at_unix_micro + 1
		WHERE tenant_id = ? AND knowledge_object_id = ?`, testTenant, targetID)
	if err == nil || !strings.Contains(err.Error(), "active dependents") {
		t.Fatalf("direct active-target transition error = %v, want active dependents", err)
	}
}
