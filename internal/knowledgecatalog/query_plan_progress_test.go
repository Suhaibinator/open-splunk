package knowledgecatalog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
	moderncsqlite "modernc.org/sqlite"
)

const projectionVisitFunction = "ko_test_projection_visit"

var (
	projectionVisitCounters sync.Map
	projectionVisitToken    atomic.Uint64
)

func init() {
	moderncsqlite.MustRegisterScalarFunction(
		projectionVisitFunction,
		2,
		func(_ *moderncsqlite.FunctionContext, arguments []driver.Value) (driver.Value, error) {
			if len(arguments) != 2 {
				return nil, fmt.Errorf("%s received %d arguments, want 2", projectionVisitFunction, len(arguments))
			}
			token, ok := arguments[0].(string)
			if !ok || token == "" {
				return nil, fmt.Errorf("%s received an invalid counter token", projectionVisitFunction)
			}
			counterValue, ok := projectionVisitCounters.Load(token)
			if !ok {
				return nil, fmt.Errorf("%s received an unknown counter token", projectionVisitFunction)
			}
			counter, ok := counterValue.(*atomic.Int64)
			if !ok || counter == nil {
				return nil, fmt.Errorf("%s counter authority is invalid", projectionVisitFunction)
			}
			counter.Add(1)
			return int64(1), nil
		},
	)
}

// newProjectionVisitCounter supplies a public-driver-only progress probe to
// package tests. The SQL function is intentionally non-deterministic so SQLite
// cannot constant-fold or move it away from the candidate projection row.
func newProjectionVisitCounter(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	token := fmt.Sprintf("projection-visit-%d", projectionVisitToken.Add(1))
	counter := &atomic.Int64{}
	projectionVisitCounters.Store(token, counter)
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
				details := explainSQLiteQueryPlan(t, database.SQLDB(), query, arguments)
				joined := strings.Join(details, "\n")
				for _, required := range []string{
					"CO-ROUTINE authorized_projection",
					"knowledge_objects_authorized_global_idx",
					"knowledge_objects_authorized_app_idx",
					"knowledge_objects_authorized_private_idx",
					"SCAN authorized_projection",
					"SEARCH projection USING PRIMARY KEY",
					"USE TEMP B-TREE FOR ORDER BY",
				} {
					if !strings.Contains(joined, required) {
						t.Fatalf("full List plan lacks %q:\n%s\nSQL:\n%s", required, joined, query)
					}
				}
				if strings.Contains(joined, "SCAN projection") ||
					strings.Contains(joined, "SEARCH projection USING INDEX knowledge_list_projection_name_idx") ||
					strings.Contains(joined, "SEARCH ordering USING COVERING INDEX knowledge_list_order_") {
					t.Fatalf("full List plan can drive from hidden tenant order rows:\n%s", joined)
				}
				if strings.Count(joined, "USE TEMP B-TREE FOR ORDER BY") != 1 {
					t.Fatalf("full List plan sorter count is not exactly one:\n%s", joined)
				}
				scanIndex := indexContaining(details, "SCAN authorized_projection")
				sortIndex := indexContaining(details, "USE TEMP B-TREE FOR ORDER BY")
				if scanIndex < 0 || sortIndex <= scanIndex {
					t.Fatalf("List sorter is not downstream of its authorized driver:\n%s", joined)
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
			token, visits := newProjectionVisitCounter(t)
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
			token, visits := newProjectionVisitCounter(t)
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
		projectionVisitFunction+"(?, projection.knowledge_object_id)",
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

func explainSQLiteQueryPlan(
	t *testing.T,
	database *sql.DB,
	query string,
	arguments []any,
) []string {
	t.Helper()
	rows, err := database.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, arguments...)
	if err != nil {
		t.Fatalf("explain SQLite query: %v\n%s", err, query)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan SQLite query plan: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read SQLite query plan: %v", err)
	}
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
	tx, err := database.SQLDB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `CREATE TEMP TABLE query_progress_hidden_ids (
		knowledge_object_id TEXT PRIMARY KEY
	)`); err != nil {
		t.Fatal(err)
	}
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
			arguments = append(arguments, fmt.Sprintf("%s-progress-hidden-%05d", prefix, index))
		}
		if _, err := tx.ExecContext(t.Context(), statement.String(), arguments...); err != nil {
			t.Fatalf("stage hidden identities [%d,%d): %v", start, end, err)
		}
	}
	copyAuthority := func(label, statement string) {
		t.Helper()
		if _, err := tx.ExecContext(t.Context(), statement, testTenant, "mmm-progress-hidden-source"); err != nil {
			t.Fatalf("clone hidden %s: %v", label, err)
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
	 AND source.object_version = 1`)
	copyAuthority("dependency seals", `INSERT INTO knowledge_object_dependency_seals (
		tenant_id, knowledge_object_id, object_version, dependency_count
	)
	SELECT source.tenant_id, staged.knowledge_object_id,
	       source.object_version, source.dependency_count
	FROM query_progress_hidden_ids AS staged
	JOIN knowledge_object_dependency_seals AS source
	  ON source.tenant_id = ? AND source.knowledge_object_id = ?
	 AND source.object_version = 1`)
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
	 AND source.object_version = 1`)
	copyAuthority("projection seals", `INSERT INTO knowledge_object_list_projection_seals (
		tenant_id, knowledge_object_id, object_version,
		projection_bytes, canonical_selector_bytes
	)
	SELECT source.tenant_id, staged.knowledge_object_id, source.object_version,
	       source.projection_bytes, source.canonical_selector_bytes
	FROM query_progress_hidden_ids AS staged
	JOIN knowledge_object_list_projection_seals AS source
	  ON source.tenant_id = ? AND source.knowledge_object_id = ?
	 AND source.object_version = 1`)
	copyAuthority("registries", `INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro,
		deleted_at_unix_micro, quarantine_reason
	)
	SELECT source.tenant_id, staged.knowledge_object_id, source.current_version,
	       source.app_id, source.owner_id, source.object_type, source.name,
	       source.sharing_scope, source.state, source.definition_digest,
	       source.created_at_unix_micro, source.updated_at_unix_micro,
	       source.disabled_at_unix_micro, source.quarantined_at_unix_micro,
	       source.deleted_at_unix_micro, source.quarantine_reason
	FROM query_progress_hidden_ids AS staged
	JOIN knowledge_objects AS source
	  ON source.tenant_id = ? AND source.knowledge_object_id = ?`)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %d hidden query-progress fixtures: %v", count, err)
	}
}
