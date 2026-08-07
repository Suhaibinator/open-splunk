package knowledgecatalog

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
	"modernc.org/sqlite"
)

const boundedPreflightVisitFunction = "ko_test_bounded_preflight_visit_v1"

type boundedPreflightProbe struct {
	visits   atomic.Int64
	cancelAt int64
	cancel   context.CancelFunc
}

var (
	boundedPreflightRegisterOnce sync.Once
	boundedPreflightRegisterErr  error
	boundedPreflightProbes       sync.Map
)

func registerBoundedPreflightVisitFunction(t *testing.T) {
	t.Helper()
	boundedPreflightRegisterOnce.Do(func() {
		boundedPreflightRegisterErr = sqlite.RegisterScalarFunction(
			boundedPreflightVisitFunction,
			2,
			func(_ *sqlite.FunctionContext, arguments []driver.Value) (driver.Value, error) {
				if len(arguments) != 2 {
					return nil, fmt.Errorf("bounded preflight visit arguments = %d", len(arguments))
				}
				token, ok := arguments[0].(string)
				if !ok {
					return nil, fmt.Errorf("bounded preflight visit token has type %T", arguments[0])
				}
				value, found := boundedPreflightProbes.Load(token)
				if !found {
					return nil, fmt.Errorf("bounded preflight visit token is unknown")
				}
				probe := value.(*boundedPreflightProbe)
				visits := probe.visits.Add(1)
				if probe.cancel != nil && visits == probe.cancelAt {
					probe.cancel()
				}
				return int64(1), nil
			},
		)
	})
	if boundedPreflightRegisterErr != nil {
		t.Fatalf("register bounded preflight visit function: %v", boundedPreflightRegisterErr)
	}
}

func TestIntegrationSelectorPreflightsBoundHostilePhysicalRowsBeforeCanonicalSort(t *testing.T) {
	registerBoundedPreflightVisitFunction(t)
	database, store := newCatalogTestStore(t)
	for _, fixture := range []struct {
		id    string
		owner string
		name  string
	}{
		{id: "ko-bounded-selector-visible", owner: testOwner, name: "bounded-selector-visible"},
		{id: "ko-bounded-selector-hidden", owner: "owner-hidden", name: "bounded-selector-hidden"},
	} {
		description := fixture.name
		insertFixtureObject(t, database, fixtureObject{
			id: fixture.id, owner: fixture.owner,
			versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, fixture.name, SharingScopePrivate, &description, fixture.name+"-*"),
				state:      StateActive, mutation: "create", timestamp: 10,
			}},
		})
	}
	insertHostileSelectorRows(t, database, "ko-bounded-selector-hidden", 4096)

	// An unauthorized hostile selector set is neither scanned nor disclosed by
	// the public catalog operations.
	page, err := store.List(context.Background(), testReadScope(), ListRequest{PageSize: 10})
	if err != nil || len(page.Objects) != 1 || page.Objects[0].KnowledgeObjectID != "ko-bounded-selector-visible" {
		t.Fatalf("List(with hidden hostile selectors) = %#v, %v", page, err)
	}
	if _, err := store.Get(context.Background(), testReadScope(), "ko-bounded-selector-hidden", nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(hidden hostile selector object) error = %v, want NotFound", err)
	}

	assertBoundedSelectorPreflightProgress(t, database, []string{"ko-bounded-selector-hidden"}, false)
	assertBoundedSelectorPreflightProgress(t, database, []string{
		"ko-bounded-selector-hidden",
		"ko-bounded-selector-visible",
	}, true)
	if _, err := readSelectorRecords(
		database.GORMDB(), testTenant, "ko-bounded-selector-hidden", 1,
	); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("single selector hostile cardinality error = %v, want ErrCorrupt", err)
	}
	if _, err := readCurrentSelectorRecordsBatch(
		database.GORMDB(), testTenant, []string{"ko-bounded-selector-hidden"},
		maximumSelectorPatterns, maximumSelectorAggregateValueBytes,
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("batch selector hostile cardinality error = %v, want CapacityExceeded", err)
	}
	assertCatalogConnectionReusable(t, database)
}

func TestIntegrationSelectorPreflightCancellationIsPromptAndReleasesConnection(t *testing.T) {
	registerBoundedPreflightVisitFunction(t)
	database, _ := newCatalogTestStore(t)
	description := "selector cancellation"
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-selector-cancel", owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "selector-cancel", SharingScopePrivate, &description, "cancel-*"),
			state:      StateActive, mutation: "create", timestamp: 10,
		}},
	})
	insertHostileSelectorRows(t, database, "ko-selector-cancel", 4096)

	preCanceledContext, preCancel := context.WithCancel(context.Background())
	preCancel()
	preCanceledProbe := &boundedPreflightProbe{}
	const preCanceledToken = "selector-pre-canceled"
	boundedPreflightProbes.Store(preCanceledToken, preCanceledProbe)
	defer boundedPreflightProbes.Delete(preCanceledToken)
	var preCanceledRows []int64
	preCanceledErr := selectorRecordPreflightQuery(
		database.GORMDB().WithContext(preCanceledContext), testTenant, "ko-selector-cancel", 1,
	).Where(
		boundedPreflightVisitFunction+"(?, ordinal) = 1", preCanceledToken,
	).Select("ordinal").Limit(maximumSelectorPatterns + 1).Find(&preCanceledRows).Error
	if !errors.Is(preCanceledErr, context.Canceled) {
		t.Fatalf("pre-canceled selector preflight error = %v, want context.Canceled", preCanceledErr)
	}
	if got := preCanceledProbe.visits.Load(); got != 0 {
		t.Fatalf("pre-canceled selector visits = %d, want 0", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	probe := &boundedPreflightProbe{cancelAt: 8, cancel: cancel}
	token := "selector-cancel"
	boundedPreflightProbes.Store(token, probe)
	defer boundedPreflightProbes.Delete(token)
	var rows []int64
	err := selectorRecordPreflightQuery(
		database.GORMDB().WithContext(ctx), testTenant, "ko-selector-cancel", 1,
	).Where(
		boundedPreflightVisitFunction+"(?, ordinal) = 1", token,
	).Select("ordinal").Limit(maximumSelectorPatterns + 1).Find(&rows).Error
	cancel()
	// The physically bounded query may finish its remaining <=57 rows before
	// the driver's interrupt goroutine wins the cancellation race. Both outcomes
	// are correct; unbounded continuation is not.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-query canceled selector preflight error = %v", err)
	}
	if got := probe.visits.Load(); got < probe.cancelAt || got > maximumSelectorPatterns+1 {
		t.Fatalf("canceled selector visits = %d, want [%d,%d]", got, probe.cancelAt, maximumSelectorPatterns+1)
	}
	assertCatalogConnectionReusable(t, database)
}

func TestIntegrationDependencyPhysicalPreflightBoundsRowsBeforeGroupedValidation(t *testing.T) {
	registerBoundedPreflightVisitFunction(t)
	database, _ := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-bounded-dependency-target", owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp, "bounded-dependency-target", SharingScopePrivate, nil,
				"bounded-*", dependencyFixtureInputField,
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-bounded-dependency-source", owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyAliasDefinition(
				testApp, "bounded-dependency-source", SharingScopePrivate, nil,
				"bounded-*", dependencyFixtureInputField, "bounded_alias",
			),
			state: StateActive, mutation: "create", timestamp: 11,
			dependencies: []fixtureDependency{{
				targetObjectID: "ko-bounded-dependency-target", targetVersion: 1,
			}},
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-z-bounded-dependency-clean", owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp, "z-bounded-dependency-clean", SharingScopePrivate, nil,
				"bounded-*", "clean_input",
			),
			state: StateActive, mutation: "create", timestamp: 12,
		}},
	})
	insertHostileDependencyRows(t, database, "ko-bounded-dependency-source", 4096)

	const maximumRows = int64(64)
	objectIDs := []string{"ko-bounded-dependency-source", "ko-z-bounded-dependency-clean"}
	token := "dependency-progress"
	probe := &boundedPreflightProbe{}
	boundedPreflightProbes.Store(token, probe)
	defer boundedPreflightProbes.Delete(token)
	query := currentDependencyPhysicalPreflightQuery(
		database.GORMDB(), testTenant, objectIDs,
	).Where(
		boundedPreflightVisitFunction+"(?, coalesce(dependency.ordinal, -1)) = 1", token,
	).Select("dependency.ordinal").Limit(int(maximumRows) + len(objectIDs) + 1)
	assertPreflightHasNoTemporaryOrderSort(t, database, query)
	var ordinals []int64
	if err := query.Find(&ordinals).Error; err != nil {
		t.Fatalf("execute dependency physical progress query: %v", err)
	}
	wantVisits := maximumRows + int64(len(objectIDs)) + 1
	if got := probe.visits.Load(); got != wantVisits {
		t.Fatalf("dependency physical visits = %d, want %d", got, wantVisits)
	}

	versions, err := readCurrentVersionRecordsBatch(
		database.GORMDB(), testTenant, objectIDs,
	)
	if err != nil {
		t.Fatalf("read hostile source version: %v", err)
	}
	var groupedQueries atomic.Int64
	const callbackName = "test:dependency-physical-preflight-before-group"
	if err := database.GORMDB().Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "GROUP BY source.knowledge_object_id") {
			groupedQueries.Add(1)
		}
	}); err != nil {
		t.Fatalf("register dependency grouped-query observer: %v", err)
	}
	_, err = validateCurrentDependenciesBatch(
		database.GORMDB(), testTenant, objectIDs, versions, maximumRows,
	)
	if removeErr := database.GORMDB().Callback().Query().Remove(callbackName); removeErr != nil {
		t.Fatalf("remove dependency grouped-query observer: %v", removeErr)
	}
	if !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("hostile dependency cardinality error = %v, want CapacityExceeded", err)
	}
	if got := groupedQueries.Load(); got != 0 {
		t.Fatalf("grouped dependency queries before physical rejection = %d, want 0", got)
	}

	// Width-only physical preflight must reject every persisted identity and
	// fixed literal before the grouped phase can process attacker-sized TEXT.
	dropTableTriggers(t, database, "knowledge_object_dependencies")
	connection, connectionErr := database.SQLDB().Conn(context.Background())
	if connectionErr != nil {
		t.Fatalf("acquire wide-dependency connection: %v", connectionErr)
	}
	t.Cleanup(func() {
		_, _ = connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = OFF`)
		_, _ = connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`)
		_ = connection.Close()
	})
	if _, connectionErr = connection.ExecContext(
		context.Background(),
		`PRAGMA ignore_check_constraints = ON`,
	); connectionErr != nil {
		t.Fatalf("disable dependency width checks: %v", connectionErr)
	}
	if _, connectionErr = connection.ExecContext(
		context.Background(),
		`PRAGMA foreign_keys = OFF`,
	); connectionErr != nil {
		t.Fatalf("disable dependency foreign keys: %v", connectionErr)
	}
	assertWideLiteralRejected := func(column, restore string) {
		t.Helper()
		if _, err := connection.ExecContext(context.Background(), `
			UPDATE knowledge_object_dependencies
			SET `+column+` = ? || CAST(zeroblob(?) AS TEXT)
			WHERE tenant_id = ? AND source_object_id = ?
			  AND source_object_version = 1 AND ordinal = 0
		`, "SECRET-DEPENDENCY-LITERAL", 1<<20, testTenant, "ko-bounded-dependency-source"); err != nil {
			t.Fatalf("inject oversized dependency %s: %v", column, err)
		}
		groupedQueries.Store(0)
		wideCallback := callbackName + "-" + column
		if err := database.GORMDB().Callback().Query().After("gorm:query").Register(wideCallback, func(tx *gorm.DB) {
			if strings.Contains(tx.Statement.SQL.String(), "GROUP BY source.knowledge_object_id") {
				groupedQueries.Add(1)
			}
		}); err != nil {
			t.Fatalf("register %s grouped-query observer: %v", column, err)
		}
		_, err := validateCurrentDependenciesBatch(
			database.GORMDB(), testTenant, objectIDs, versions, 8192,
		)
		if removeErr := database.GORMDB().Callback().Query().Remove(wideCallback); removeErr != nil {
			t.Fatalf("remove %s grouped-query observer: %v", column, removeErr)
		}
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("oversized dependency %s error = %v, want ErrCorrupt", column, err)
		}
		if got := groupedQueries.Load(); got != 0 {
			t.Fatalf("grouped queries after oversized dependency %s = %d, want 0", column, got)
		}
		if _, err := connection.ExecContext(context.Background(), `
			UPDATE knowledge_object_dependencies SET `+column+` = ?
			WHERE tenant_id = ? AND source_object_id = ?
			  AND source_object_version = 1 AND ordinal = 0
		`, restore, testTenant, "ko-bounded-dependency-source"); err != nil {
			t.Fatalf("restore dependency %s: %v", column, err)
		}
	}
	assertWideLiteralRejected("target_kind", "object")
	assertWideLiteralRejected("target_object_id", "ko-bounded-dependency-target")
	assertWideLiteralRejected("dependency_role", "field_input")
	if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatalf("restore dependency width checks: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("restore dependency foreign keys: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close wide-dependency connection: %v", err)
	}
	assertCatalogConnectionReusable(t, database)
}

func assertBoundedSelectorPreflightProgress(
	t *testing.T,
	database *control.DB,
	objectIDs []string,
	batch bool,
) {
	t.Helper()
	token := fmt.Sprintf("selector-progress-%t", batch)
	probe := &boundedPreflightProbe{}
	boundedPreflightProbes.Store(token, probe)
	defer boundedPreflightProbes.Delete(token)
	var query *gorm.DB
	if batch {
		query = currentSelectorRecordPreflightQuery(database.GORMDB(), testTenant, objectIDs).
			Where(boundedPreflightVisitFunction+"(?, selector.ordinal) = 1", token).
			Select("selector.ordinal")
	} else {
		if len(objectIDs) != 1 {
			t.Fatalf("single selector progress IDs = %d, want 1", len(objectIDs))
		}
		query = selectorRecordPreflightQuery(database.GORMDB(), testTenant, objectIDs[0], 1).
			Where(boundedPreflightVisitFunction+"(?, ordinal) = 1", token).
			Select("ordinal")
	}
	query = query.Limit(maximumSelectorPatterns + 1)
	assertPreflightHasNoTemporaryOrderSort(t, database, query)
	var ordinals []int64
	if err := query.Find(&ordinals).Error; err != nil {
		t.Fatalf("execute selector progress query: %v", err)
	}
	if got := probe.visits.Load(); got != maximumSelectorPatterns+1 {
		t.Fatalf("selector batch=%t visits = %d, want %d", batch, got, maximumSelectorPatterns+1)
	}
}

func assertPreflightHasNoTemporaryOrderSort(t *testing.T, database *control.DB, query *gorm.DB) {
	t.Helper()
	destination := []struct{}{}
	compiled := query.Session(&gorm.Session{DryRun: true}).Find(&destination)
	if compiled.Error != nil {
		t.Fatalf("compile bounded preflight query: %v", compiled.Error)
	}
	rows, err := database.SQLDB().Query(
		"EXPLAIN QUERY PLAN "+compiled.Statement.SQL.String(),
		compiled.Statement.Vars...,
	)
	if err != nil {
		t.Fatalf("explain bounded preflight query: %v\n%s", err, compiled.Statement.SQL.String())
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan bounded preflight plan: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read bounded preflight plan: %v", err)
	}
	joined := strings.Join(details, "\n")
	if strings.Contains(joined, "USE TEMP B-TREE FOR ORDER BY") {
		t.Fatalf("bounded preflight plan sorts before LIMIT:\n%s", joined)
	}
	if !strings.Contains(joined, "USING PRIMARY KEY") {
		t.Fatalf("bounded preflight plan does not use a primary key:\n%s", joined)
	}
}

func insertHostileSelectorRows(t *testing.T, database *control.DB, objectID string, count int) {
	t.Helper()
	for _, trigger := range []string{
		"knowledge_list_selector_identity_collision_is_forbidden",
		"knowledge_list_selector_ordinal_is_declared",
		"knowledge_list_selector_sealed_projection_is_immutable_insert",
	} {
		dropTrigger(t, database, trigger)
	}
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire hostile selector connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("ignore hostile selector checks: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
			t.Errorf("restore hostile selector checks: %v", err)
		}
	}()
	if _, err := connection.ExecContext(context.Background(), `
		WITH RECURSIVE hostile(n) AS (
			SELECT 0
			UNION ALL
			SELECT n + 1 FROM hostile WHERE n + 1 < ?
		)
		INSERT INTO knowledge_object_list_selector_patterns (
			tenant_id, knowledge_object_id, object_version, dimension,
			ordinal, match_kind, value
		)
		SELECT ?, ?, 1, 'host', 1000 + n, 'exact', printf('hostile-selector-%05d', n)
		FROM hostile
	`, count, testTenant, objectID); err != nil {
		t.Fatalf("insert %d hostile selector rows: %v", count, err)
	}
}

func insertHostileDependencyRows(t *testing.T, database *control.DB, sourceID string, count int) {
	t.Helper()
	for _, trigger := range []string{
		"knowledge_dependency_ordinal_is_declared",
		"knowledge_dependency_sealed_version_is_immutable",
		"knowledge_dependency_identity_collision_is_forbidden",
	} {
		dropTrigger(t, database, trigger)
	}
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire hostile dependency connection: %v", err)
	}
	defer connection.Close()
	for _, pragma := range []string{
		"PRAGMA foreign_keys = OFF",
		"PRAGMA ignore_check_constraints = ON",
	} {
		if _, err := connection.ExecContext(context.Background(), pragma); err != nil {
			t.Fatalf("configure hostile dependency connection: %v", err)
		}
	}
	defer func() {
		for _, pragma := range []string{
			"PRAGMA ignore_check_constraints = OFF",
			"PRAGMA foreign_keys = ON",
		} {
			if _, err := connection.ExecContext(context.Background(), pragma); err != nil {
				t.Errorf("restore hostile dependency connection: %v", err)
			}
		}
	}()
	if _, err := connection.ExecContext(context.Background(), `
		WITH RECURSIVE hostile(n) AS (
			SELECT 0
			UNION ALL
			SELECT n + 1 FROM hostile WHERE n + 1 < ?
		)
		INSERT INTO knowledge_object_dependencies (
			tenant_id, source_object_id, source_object_version, ordinal,
			target_kind, target_object_id, target_object_version, dependency_role
		)
		SELECT ?, ?, 1, 10000 + n, 'object',
			printf('ko-hostile-target-%05d', n), 1, 'field_input'
		FROM hostile
	`, count, testTenant, sourceID); err != nil {
		t.Fatalf("insert %d hostile dependency rows: %v", count, err)
	}
}

func assertCatalogConnectionReusable(t *testing.T, database *control.DB) {
	t.Helper()
	var one int
	if err := database.SQLDB().QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("catalog connection after bounded rejection = %d, %v", one, err)
	}
}
