package knowledgecatalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"gorm.io/gorm"
)

const projectionVisitFunction = "ko_test_projection_visit"

var (
	projectionVisitCounters sync.Map
	projectionVisitToken    atomic.Uint64
)

// newProjectionVisitCounter supplies a public-driver-only progress probe to
// package tests. The native SQL function is VOLATILE so PostgreSQL records
// physical candidate evaluations instead of constant-folding the probe.
func newProjectionVisitCounter(t *testing.T, database *control.DB) (string, *pgVisitCounter) {
	t.Helper()
	token := fmt.Sprintf("projection-visit-%d", projectionVisitToken.Add(1))
	counter := &pgVisitCounter{}
	projectionVisitCounters.Store(token, counter)
	preparePostgresVisit(t, database, projectionVisitFunction, token, counter, false)
	t.Cleanup(func() { projectionVisitCounters.Delete(token) })
	return token, counter
}

func TestListFullQueryPlanIsAuthorizationDrivenAndOnlySortsAuthorizedRows(t *testing.T) {
	database, _ := newCatalogTestStore(t)

	for _, sortBy := range []SortBy{
		SortByName,
		SortByCreatedAt,
		SortByUpdatedAt,
		SortByObjectType,
	} {
		for _, direction := range []SortDirection{SortAscending, SortDescending} {
			name := string(sortBy) + "/" + string(direction)
			t.Run(name, func(t *testing.T) {
				query, arguments := compiledOrderedListProjectionQuery(t, database, sortBy, direction)
				details := explainPostgreSQLQueryPlan(t, database.SQLDB(), query, arguments)
				joined := strings.Join(details, "\n")
				for _, required := range []string{"knowledge_objects_authorized_global_idx", "knowledge_objects_authorized_app_idx", "knowledge_objects_authorized_private_idx", "Limit", "Append", "on projection", "Sort"} {
					if !strings.Contains(joined, required) {
						t.Fatalf("native List plan lacks %q:\n%s\nSQL:%s", required, joined, query)
					}
				}
				if strings.Contains(joined, "Seq Scan on projection") || strings.Contains(joined, "knowledge_list_projection_name_idx") || strings.Contains(joined, "knowledge_list_order_") {
					t.Fatalf("native List drives from hidden tenant order rows:\n%s", joined)
				}
				if strings.Count(joined, "Sort") != 1 {
					t.Fatalf("native List must sort only its bounded authorized rows:\n%s", joined)
				}
				if limitIndex, sortIndex := indexContaining(details, "Limit"), indexContaining(details, "Sort"); limitIndex < 0 || sortIndex <= limitIndex {
					t.Fatalf("native List sort precedes bounded authorization:\n%s", joined)
				}
			})
		}
	}
}

func TestListOrderedProjectionQueryExecutesOnce(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertQueryProgressFixtures(t, database)

	for _, sortBy := range []SortBy{
		SortByName,
		SortByCreatedAt,
		SortByUpdatedAt,
		SortByObjectType,
	} {
		for _, direction := range []SortDirection{SortAscending, SortDescending} {
			t.Run(string(sortBy)+"/"+string(direction), func(t *testing.T) {
				var orderedProjectionQueries atomic.Int64
				callbackName := "test:single-ordered-projection-" + string(sortBy) + "-" + string(direction)
				if err := database.GORMDB().Callback().Row().After("gorm:row").Register(
					callbackName,
					func(tx *gorm.DB) {
						sqlText := tx.Statement.SQL.String()
						if strings.Contains(sqlText, "AS authorized_projection") &&
							strings.Contains(sqlText, "ORDER BY") {
							orderedProjectionQueries.Add(1)
						}
					},
				); err != nil {
					t.Fatalf("register query counter: %v", err)
				}
				_, listErr := store.List(context.Background(), testReadScope(), ListRequest{
					PageSize: 1, SortBy: sortBy, SortDirection: direction,
				})
				if err := database.GORMDB().Callback().Row().Remove(callbackName); err != nil {
					t.Fatalf("remove query counter: %v", err)
				}
				if listErr != nil {
					t.Fatalf("List: %v", listErr)
				}
				if got := orderedProjectionQueries.Load(); got != 1 {
					t.Fatalf("ordered projection executions = %d, want exactly 1", got)
				}
			})
		}
	}
}

func TestListFullQueryVisitsOnlyAuthorizedProjectionRows(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	insertQueryProgressFixtures(t, database)

	type queryCase struct {
		sortBy    SortBy
		direction SortDirection
		baseline  []byte
	}
	cases := make([]queryCase, 0, 8)
	for _, sortBy := range []SortBy{
		SortByName,
		SortByCreatedAt,
		SortByUpdatedAt,
		SortByObjectType,
	} {
		for _, direction := range []SortDirection{SortAscending, SortDescending} {
			token, visits := newProjectionVisitCounter(t, database)
			query, arguments := compiledCountedOrderedListProjectionQuery(
				t, database, sortBy, direction, token,
			)
			baseline := executeProjectionQueryJSON(t, database, query, arguments)
			if got := visits.Load(); got != 1 {
				t.Fatalf("baseline authorized projection visits for %s/%s = %d, want 1", sortBy, direction, got)
			}
			cases = append(cases, queryCase{
				sortBy: sortBy, direction: direction,
				baseline: baseline,
			})
		}
	}

	const hiddenRows = 4096
	cloneHiddenQueryProgressFixtures(t, database, hiddenRows)
	for _, test := range cases {
		t.Run(string(test.sortBy)+"/"+string(test.direction), func(t *testing.T) {
			token, visits := newProjectionVisitCounter(t, database)
			query, arguments := compiledCountedOrderedListProjectionQuery(
				t, database, test.sortBy, test.direction, token,
			)
			got := executeProjectionQueryJSON(t, database, query, arguments)
			if count := visits.Load(); count != 1 {
				t.Fatalf(
					"authorized projection visits with %d hidden rows = %d, want 1",
					hiddenRows, count,
				)
			}
			if string(got) != string(test.baseline) {
				t.Fatalf("hidden rows changed ordered projection result:\nbaseline: %s\nafter: %s", test.baseline, got)
			}
		})
	}
}

func compiledCountedOrderedListProjectionQuery(
	t *testing.T,
	database *control.DB,
	sortBy SortBy,
	direction SortDirection,
	token string,
) (string, []any) {
	t.Helper()
	normalized, err := normalizeListRequest(testReadScope(), ListRequest{
		PageSize: 1, SortBy: sortBy, SortDirection: direction,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := applyListFilters(baseProjectionQuery(database.GORMDB()), normalized)
	query = query.Where(
		projectionVisitFunction+"(?, projection.knowledge_object_id) = 1",
		token,
	)
	query = applyListCursor(query, normalized, listCursor{})
	query = applyListOrder(query, normalized)
	result := query.Session(&gorm.Session{DryRun: true}).Limit(2).Find(&[]projectionReadRecord{})
	if result.Error != nil {
		t.Fatalf("compile counted ordered List query: %v", result.Error)
	}
	arguments := make([]any, len(result.Statement.Vars))
	copy(arguments, result.Statement.Vars)
	return result.Statement.SQL.String(), arguments
}

func executeProjectionQueryJSON(
	t *testing.T,
	database *control.DB,
	query string,
	arguments []any,
) []byte {
	t.Helper()
	var records []projectionReadRecord
	if err := database.GORMDB().Raw(query, arguments...).Scan(&records).Error; err != nil {
		t.Fatalf("execute counted ordered List query: %v\n%s", err, query)
	}
	encoded, err := json.Marshal(records)
	if err != nil {
		t.Fatalf("encode counted ordered List result: %v", err)
	}
	return encoded
}

func compiledOrderedListProjectionQuery(
	t *testing.T,
	database *control.DB,
	sortBy SortBy,
	direction SortDirection,
) (string, []any) {
	t.Helper()
	normalized, err := normalizeListRequest(testReadScope(), ListRequest{
		PageSize: 1, SortBy: sortBy, SortDirection: direction,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := applyListFilters(baseProjectionQuery(database.GORMDB()), normalized)
	query = applyListCursor(query, normalized, listCursor{})
	query = applyListOrder(query, normalized)
	result := query.Session(&gorm.Session{DryRun: true}).Limit(2).Find(&[]projectionReadRecord{})
	if result.Error != nil {
		t.Fatalf("compile ordered List query: %v", result.Error)
	}
	if result.Statement.SQL.Len() == 0 {
		t.Fatal("compiled ordered List query is empty")
	}
	arguments := make([]any, len(result.Statement.Vars))
	copy(arguments, result.Statement.Vars)
	return result.Statement.SQL.String(), arguments
}

type nativeExplainNode struct {
	NodeType     string              `json:"Node Type"`
	RelationName string              `json:"Relation Name"`
	Alias        string              `json:"Alias"`
	IndexName    string              `json:"Index Name"`
	IndexCond    string              `json:"Index Cond"`
	Filter       string              `json:"Filter"`
	Plans        []nativeExplainNode `json:"Plans"`
}

func explainPostgreSQLQueryPlan(t *testing.T, database *sql.DB, query string, arguments []any) []string {
	t.Helper()
	connection, err := database.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	// Empty fixture statistics favor sequential scans. Verify that the complete
	// bounded query can use native indexes; progress tests exercise actual work.
	if _, err := connection.ExecContext(t.Context(), "SET enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := connection.ExecContext(context.Background(), "RESET enable_seqscan"); err != nil {
			t.Errorf("reset planner: %v", err)
		}
	}()
	if _, err := connection.ExecContext(t.Context(), "SET enable_bitmapscan = off"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := connection.ExecContext(context.Background(), "RESET enable_bitmapscan"); err != nil {
			t.Errorf("reset planner: %v", err)
		}
	}()
	var encoded []byte
	if err := connection.QueryRowContext(t.Context(), "EXPLAIN (FORMAT JSON, COSTS OFF) "+pgtest.Parameters(query), arguments...).Scan(&encoded); err != nil {
		t.Fatalf("native explain: %v\n%s", err, query)
	}
	var roots []struct {
		Plan nativeExplainNode `json:"Plan"`
	}
	if err := json.Unmarshal(encoded, &roots); err != nil || len(roots) != 1 {
		t.Fatalf("native explain document: %s %v", encoded, err)
	}
	var details []string
	var visit func(nativeExplainNode)
	visit = func(node nativeExplainNode) {
		for _, child := range node.Plans {
			visit(child)
		}
		detail := node.NodeType
		if node.Alias != "" {
			detail += " on " + node.Alias
		}
		if node.RelationName != "" {
			detail += " table " + node.RelationName
		}
		if node.IndexName != "" {
			detail += " using " + node.IndexName
		}
		if node.IndexCond != "" {
			detail += " condition " + node.IndexCond
		}
		if node.Filter != "" {
			detail += " filter " + node.Filter
		}
		details = append(details, detail)
	}
	visit(roots[0].Plan)
	return details
}

func indexContaining(values []string, substring string) int {
	for index, value := range values {
		if strings.Contains(value, substring) {
			return index
		}
	}
	return -1
}

func insertQueryProgressFixtures(t *testing.T, database *control.DB) {
	t.Helper()
	definition := func(description *string) *fixtureVersion {
		return &fixtureVersion{
			definition: aliasDefinition(testApp, "progress-tie", SharingScopePrivate, description, ""),
			state:      StateDraft, mutation: "create", timestamp: 10,
		}
	}
	visible := definition(nil)
	hiddenDescription := "hidden progress definition"
	hidden := definition(&hiddenDescription)
	insertFixtureObject(t, database, fixtureObject{
		id: "mmm-progress-visible", owner: testOwner, versions: []fixtureVersion{*visible},
	})
	insertFixtureObject(t, database, fixtureObject{
		id: "mmm-progress-hidden-source", owner: "owner-hidden", versions: []fixtureVersion{*hidden},
	})
}

func cloneHiddenQueryProgressFixtures(t *testing.T, database *control.DB, count int) {
	t.Helper()
	tx, err := pgmeta.BeginSQL(context.Background(), database.SQLDB(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `CREATE TEMP TABLE query_progress_hidden_ids (
		knowledge_object_id TEXT PRIMARY KEY
	)`); err != nil {
		t.Fatal(err)
	}
	stagedIDs := make([]string, 0, count)
	for start := 0; start < count; start += 250 {
		end := min(start+250, count)
		var statement strings.Builder
		statement.WriteString(`INSERT INTO query_progress_hidden_ids (knowledge_object_id) VALUES `)
		arguments := make([]any, 0, end-start)
		for index := start; index < end; index++ {
			if index != start {
				statement.WriteByte(',')
			}
			statement.WriteString("(?)")
			prefix := "aaa"
			if index >= count/2 {
				prefix = "zzz"
			}
			identity := fmt.Sprintf("%s-progress-hidden-%05d", prefix, index)
			arguments = append(arguments, identity)
			stagedIDs = append(stagedIDs, identity)
		}
		if _, err := tx.ExecContext(t.Context(), pgtest.Parameters(statement.String()), arguments...); err != nil {
			t.Fatalf("stage hidden identities [%d,%d): %v", start, end, err)
		}
	}
	copyAuthority := func(label, statement string) {
		t.Helper()
		// Native AFTER ROW accounting observes every row inserted by a statement.
		// Clone one guarded statement per identity so each physical journal and
		// projection ledger transition sees the exact current table contents.
		query := pgtest.Parameters(statement)
		for _, identity := range stagedIDs {
			if _, err := tx.ExecContext(t.Context(), query, testTenant, "mmm-progress-hidden-source", identity); err != nil {
				t.Fatalf("clone hidden %s: %v", label, err)
			}
		}
	}
	copyAuthority("versions", `INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		dependency_count, mutation_kind, quarantine_reason, created_at_unix_micro
	)
	SELECT source.tenant_id, staged.knowledge_object_id, source.object_version,
	       source.app_id, source.owner_id, source.object_type, source.name,
	       source.sharing_scope, source.state, source.definition_digest,
	       source.dependency_count, source.mutation_kind,
	       source.quarantine_reason, source.created_at_unix_micro
	FROM query_progress_hidden_ids AS staged
	JOIN knowledge_object_versions AS source
	  ON source.tenant_id = ? AND source.knowledge_object_id = ?
	 AND source.object_version = 1 AND staged.knowledge_object_id = ?`)
	copyAuthority("dependency seals", `INSERT INTO knowledge_object_dependency_seals (
		tenant_id, knowledge_object_id, object_version, dependency_count
	)
	SELECT source.tenant_id, staged.knowledge_object_id,
	       source.object_version, source.dependency_count
	FROM query_progress_hidden_ids AS staged
	JOIN knowledge_object_dependency_seals AS source
	  ON source.tenant_id = ? AND source.knowledge_object_id = ?
	 AND source.object_version = 1 AND staged.knowledge_object_id = ?`)
	copyAuthority("projections", `INSERT INTO knowledge_object_list_projections (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id,
		object_type, name, sharing_scope, state, description_present,
		description, index_selector_count, host_selector_count,
		source_selector_count, sourcetype_selector_count,
		selector_value_bytes, canonical_selector_bytes
	)
	SELECT source.tenant_id, staged.knowledge_object_id, source.object_version,
	       source.app_id, source.owner_id, source.object_type, source.name,
	       source.sharing_scope, source.state, source.description_present,
	       source.description, source.index_selector_count,
	       source.host_selector_count, source.source_selector_count,
	       source.sourcetype_selector_count, source.selector_value_bytes,
	       source.canonical_selector_bytes
	FROM query_progress_hidden_ids AS staged
	JOIN knowledge_object_list_projections AS source
	  ON source.tenant_id = ? AND source.knowledge_object_id = ?
	 AND source.object_version = 1 AND staged.knowledge_object_id = ?`)
	copyAuthority("projection seals", `INSERT INTO knowledge_object_list_projection_seals (
		tenant_id, knowledge_object_id, object_version,
		projection_bytes, canonical_selector_bytes
	)
	SELECT source.tenant_id, staged.knowledge_object_id, source.object_version,
	       source.projection_bytes, source.canonical_selector_bytes
	FROM query_progress_hidden_ids AS staged
	JOIN knowledge_object_list_projection_seals AS source
	  ON source.tenant_id = ? AND source.knowledge_object_id = ?
	 AND source.object_version = 1 AND staged.knowledge_object_id = ?`)
	copyAuthority("registries", `INSERT INTO knowledge_objects (organization_id,
		tenant_id, knowledge_object_id, current_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro,
		deleted_at_unix_micro, quarantine_reason
	)
	SELECT 1, source.tenant_id, staged.knowledge_object_id, source.current_version,
	       source.app_id, source.owner_id, source.object_type, source.name,
	       source.sharing_scope, source.state, source.definition_digest,
	       source.created_at_unix_micro, source.updated_at_unix_micro,
	       source.disabled_at_unix_micro, source.quarantined_at_unix_micro,
	       source.deleted_at_unix_micro, source.quarantine_reason
	FROM query_progress_hidden_ids AS staged
	JOIN knowledge_objects AS source
	  ON source.tenant_id = ? AND source.knowledge_object_id = ? AND staged.knowledge_object_id = ?`)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %d hidden query-progress fixtures: %v", count, err)
	}
}

func nativeBoundLimit(query string, args []any, want int) bool {
	if len(args) == 0 || !strings.Contains(query, fmt.Sprintf("LIMIT $%d", len(args))) {
		return false
	}
	switch value := args[len(args)-1].(type) {
	case int:
		return value == want
	case int64:
		return value == int64(want)
	case int32:
		return value == int32(want)
	case uint32:
		return value == uint32(want)
	}
	return false
}
