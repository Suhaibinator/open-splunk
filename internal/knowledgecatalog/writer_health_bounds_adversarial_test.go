package knowledgecatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestWriterActiveCounterPreflightsUseBoundedTenantPrefixes(t *testing.T) {
	registerBoundedPreflightVisitFunction(t)
	tests := []struct {
		name string
		kind activeCounterPreflightKind
	}{
		{name: "app", kind: activeAppCounterPreflight},
		{name: "owner", kind: activeOwnerCounterPreflight},
		{name: "type", kind: activeTypeCounterPreflight},
		{name: "app-type", kind: activeAppTypeCounterPreflight},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, _ := newCatalogTestStore(t)
			spec := activeCounterSpec(test.kind)
			dropTableTriggers(t, database, spec.table)
			insertWriterActiveCounterRange(t, database, test.kind, 0, spec.maximumRows)

			query := activeCounterPreflightQuery(database.GORMDB(), testTenant, spec)
			assertWriterHealthPreflightPlan(t, database, query, "counter")
			var exact []activeCounterPreflightRecord
			if err := query.Find(&exact).Error; err != nil {
				t.Fatalf("read exact %s counter boundary: %v", test.name, err)
			}
			if len(exact) != spec.maximumRows {
				t.Fatalf("exact %s counter rows = %d, want %d", test.name, len(exact), spec.maximumRows)
			}
			if err := validateActiveCounterPreflight(test.kind, spec, exact); err != nil {
				t.Fatalf("validate exact %s counter boundary: %v", test.name, err)
			}

			insertWriterActiveCounterRange(t, database, test.kind, spec.maximumRows, spec.maximumRows+1)
			token := "writer-counter-bound-" + test.name
			probe := &boundedPreflightProbe{}
			boundedPreflightProbes.Store(token, probe)
			defer boundedPreflightProbes.Delete(token)
			query = activeCounterPreflightQuery(database.GORMDB(), testTenant, spec).Where(
				boundedPreflightVisitFunction+"(?, counter."+spec.countColumn+") = 1", token,
			)
			var over []activeCounterPreflightRecord
			if err := query.Find(&over).Error; err != nil {
				t.Fatalf("read over-bound %s counter prefix: %v", test.name, err)
			}
			if got, want := probe.visits.Load(), int64(spec.maximumRows+1); got != want {
				t.Fatalf("over-bound %s counter visits = %d, want %d", test.name, got, want)
			}
			if err := validateActiveCounterPreflight(test.kind, spec, over); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("over-bound %s counter error = %v, want ErrCorrupt", test.name, err)
			}

			grouped := observeWriterHealthGroupedQueries(t, database)
			if err := validateActiveCounterHealth(database.GORMDB(), testTenant); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("validate over-bound %s counter health error = %v, want ErrCorrupt", test.name, err)
			}
			if got := grouped.Load(); got != 0 {
				t.Fatalf("over-bound %s counter reached %d grouped queries, want 0", test.name, got)
			}
			assertCatalogConnectionReusable(t, database)
		})
	}
}

func TestWriterActiveCounterPreflightRejectsKeysAndScalarsBeforeGrouping(t *testing.T) {
	const wideSentinel = "SECRET-WRITER-COUNTER-"
	tests := []struct {
		name       string
		kind       activeCounterPreflightKind
		firstKey   string
		secondKey  string
		counter    int64
		wantNoLeak bool
	}{
		{name: "app-wide", kind: activeAppCounterPreflight, firstKey: wideSentinel + strings.Repeat("a", 1<<20), wantNoLeak: true},
		{name: "owner-wide", kind: activeOwnerCounterPreflight, firstKey: wideSentinel + strings.Repeat("o", 1<<20), wantNoLeak: true},
		{name: "type-wide", kind: activeTypeCounterPreflight, firstKey: wideSentinel + strings.Repeat("t", 1<<20), wantNoLeak: true},
		{name: "app-type-wide-app", kind: activeAppTypeCounterPreflight, firstKey: wideSentinel + strings.Repeat("a", 1<<20), secondKey: string(ObjectTypeFieldAlias), wantNoLeak: true},
		{name: "app-type-wide-type", kind: activeAppTypeCounterPreflight, firstKey: writerHealthCanonicalAppID(), secondKey: wideSentinel + strings.Repeat("t", 1<<20), wantNoLeak: true},
		{name: "app-semantic", kind: activeAppCounterPreflight, firstKey: "app_not-canonical", counter: 0},
		{name: "owner-semantic", kind: activeOwnerCounterPreflight, firstKey: " owner-not-canonical", counter: 0},
		{name: "type-semantic", kind: activeTypeCounterPreflight, firstKey: "future_type", counter: 0},
		{name: "app-type-semantic", kind: activeAppTypeCounterPreflight, firstKey: writerHealthCanonicalAppID(), secondKey: "future_type", counter: 0},
		{name: "app-negative", kind: activeAppCounterPreflight, firstKey: writerHealthCanonicalAppID(), counter: -1},
		{name: "owner-overflow", kind: activeOwnerCounterPreflight, firstKey: "owner-health", counter: 513},
		{name: "type-overflow", kind: activeTypeCounterPreflight, firstKey: string(ObjectTypeFieldAlias), counter: 2049},
		{name: "app-type-overflow", kind: activeAppTypeCounterPreflight, firstKey: writerHealthCanonicalAppID(), secondKey: string(ObjectTypeFieldAlias), counter: 513},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, _ := newCatalogTestStore(t)
			spec := activeCounterSpec(test.kind)
			dropTableTriggers(t, database, spec.table)
			insertWriterActiveCounterRecord(t, database, test.kind, test.firstKey, test.secondKey, test.counter)
			grouped := observeWriterHealthGroupedQueries(t, database)
			err := validateActiveCounterHealth(database.GORMDB(), testTenant)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("invalid %s counter error = %v, want ErrCorrupt", test.name, err)
			}
			if test.wantNoLeak && strings.Contains(err.Error(), wideSentinel) {
				t.Fatalf("invalid %s counter error leaked hostile key: %v", test.name, err)
			}
			if got := grouped.Load(); got != 0 {
				t.Fatalf("invalid %s counter reached %d grouped queries, want 0", test.name, got)
			}
			assertCatalogConnectionReusable(t, database)
		})
	}
}

func TestWriterActiveCounterPreflightCancellationReleasesConnection(t *testing.T) {
	registerBoundedPreflightVisitFunction(t)
	database, _ := newCatalogTestStore(t)
	spec := activeCounterSpec(activeOwnerCounterPreflight)
	dropTableTriggers(t, database, spec.table)
	insertWriterActiveCounterRange(t, database, activeOwnerCounterPreflight, 0, spec.maximumRows+1)

	preCanceled, cancelPreCanceled := context.WithCancel(context.Background())
	cancelPreCanceled()
	grouped := observeWriterHealthGroupedQueries(t, database)
	if err := validateActiveCounterHealth(database.GORMDB().WithContext(preCanceled), testTenant); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled active counter health error = %v, want context.Canceled", err)
	}
	if got := grouped.Load(); got != 0 {
		t.Fatalf("pre-canceled active counter health reached %d grouped queries, want 0", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	probe := &boundedPreflightProbe{cancelAt: 8, cancel: cancel}
	const token = "writer-owner-counter-cancel"
	boundedPreflightProbes.Store(token, probe)
	defer boundedPreflightProbes.Delete(token)
	query := activeCounterPreflightQuery(database.GORMDB().WithContext(ctx), testTenant, spec).Where(
		boundedPreflightVisitFunction+"(?, counter."+spec.countColumn+") = 1", token,
	)
	var records []activeCounterPreflightRecord
	err := query.Find(&records).Error
	cancel()
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-query canceled active counter preflight error = %v", err)
	}
	if got := probe.visits.Load(); got < probe.cancelAt || got > int64(spec.maximumRows+1) {
		t.Fatalf("mid-query canceled active counter visits = %d, want [%d,%d]", got, probe.cancelAt, spec.maximumRows+1)
	}
	assertCatalogConnectionReusable(t, database)
}

func TestWriterDefinitionBlobCardinalityRejectsForgedOrphanBeforeByteSum(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-writer-health-orphan", owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "writer-health-orphan", SharingScopePrivate, nil, "health-*"),
			state:      StateDraft, mutation: "create", timestamp: 10,
		}},
	})
	dropTableTriggers(t, database, "knowledge_definition_blobs")
	insertWriterHealthDefinitionBlob(t, database, maximumVersionsPerTenant+2)
	if err := database.GORMDB().Exec(`UPDATE knowledge_catalog_tenants
		SET version_count = 2,
		    definition_body_bytes = (
			SELECT coalesce(sum(definition_bytes), 0)
			FROM knowledge_definition_blobs WHERE tenant_id = ?
		)
		WHERE tenant_id = ?`, testTenant, testTenant).Error; err != nil {
		t.Fatalf("forge orphan definition ledger: %v", err)
	}

	sums := observeWriterHealthDefinitionSums(t, database)
	_, _, err := readMutationTenantHealth(database.GORMDB(), testTenant)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("forged orphan definition health error = %v, want ErrCorrupt", err)
	}
	if got := sums.Load(); got != 0 {
		t.Fatalf("forged orphan definition health executed %d byte sums before cardinality rejection, want 0", got)
	}
	assertCatalogConnectionReusable(t, database)
}

func TestWriterProjectionCardinalityRejectsMissingZeroChargeProjection(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-writer-health-zero-projection", owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(
				testApp,
				"writer-health-zero-projection",
				SharingScopePrivate,
				nil,
				"",
			),
			state: StateDraft, mutation: "create", timestamp: 10,
		}},
	})
	assertWriterPhysicalHealthProjectionPlan(t, database)

	var projectionCount, projectionBytes int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*), coalesce(sum(projection_bytes), 0)
		FROM knowledge_object_list_projections
		WHERE tenant_id = ?`, testTenant,
	).Scan(&projectionCount, &projectionBytes); err != nil {
		t.Fatalf("read zero-charge projection fixture: %v", err)
	}
	if projectionCount != 1 || projectionBytes != 0 {
		t.Fatalf(
			"zero-charge projection fixture = count %d, bytes %d; want 1/0",
			projectionCount,
			projectionBytes,
		)
	}
	if _, _, err := readMutationTenantHealth(database.GORMDB(), testTenant); err != nil {
		t.Fatalf("read healthy zero-charge projection tenant: %v", err)
	}

	dropTrigger(t, database, "knowledge_list_projection_current_seal_delete_is_forbidden")
	for _, statement := range []string{
		`DELETE FROM knowledge_object_list_projection_seals
		 WHERE tenant_id = ? AND knowledge_object_id = 'ko-writer-health-zero-projection'`,
		`DELETE FROM knowledge_object_list_projections
		 WHERE tenant_id = ? AND knowledge_object_id = 'ko-writer-health-zero-projection'`,
	} {
		result, err := database.SQLDB().ExecContext(t.Context(), statement, testTenant)
		if err != nil {
			t.Fatalf("remove zero-charge projection authority: %v", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			t.Fatalf("removed zero-charge projection rows = %d, %v; want 1", affected, err)
		}
	}

	_, _, err := readMutationTenantHealth(database.GORMDB(), testTenant)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing zero-charge projection health error = %v, want ErrCorrupt", err)
	}
	assertCatalogConnectionReusable(t, database)
}

func TestWriterDefinitionBlobCardinalityExactAndOverCapacityBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds the exact 65,536-row immutable version/blob boundary")
	}
	registerBoundedPreflightVisitFunction(t)
	database, _ := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-writer-health-definition-boundary", owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "writer-health-definition-boundary", SharingScopePrivate, nil, "health-boundary-*"),
			state:      StateDraft, mutation: "create", timestamp: 10,
		}},
	})
	dropTableTriggers(t, database, "knowledge_definition_blobs")
	dropTableTriggers(t, database, "knowledge_object_versions")
	seedWriterHealthDefinitionBoundary(t, database)
	assertWriterDefinitionCardinalityPlan(t, database)

	sums := observeWriterHealthDefinitionSums(t, database)
	health, _, err := readMutationTenantHealth(database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read exact definition cardinality boundary: %v", err)
	}
	if health.VersionCount != maximumVersionsPerTenant {
		t.Fatalf("exact definition boundary version ledger = %d, want %d", health.VersionCount, maximumVersionsPerTenant)
	}
	if got := sums.Load(); got != 1 {
		t.Fatalf("exact definition boundary byte sums = %d, want 1", got)
	}

	for _, total := range []int{maximumVersionsPerTenant + 1, maximumVersionsPerTenant + 2} {
		insertWriterHealthDefinitionBlob(t, database, total)
		if err := database.GORMDB().Exec(`UPDATE knowledge_catalog_tenants
			SET definition_body_bytes = (
				SELECT coalesce(sum(definition_bytes), 0)
				FROM knowledge_definition_blobs WHERE tenant_id = ?
			)
			WHERE tenant_id = ?`, testTenant, testTenant).Error; err != nil {
			t.Fatalf("forge %d-row definition ledger: %v", total, err)
		}
		sums.Store(0)
		_, _, err := readMutationTenantHealth(database.GORMDB(), testTenant)
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("read %d-row definition boundary error = %v, want ErrCorrupt", total, err)
		}
		if got := sums.Load(); got != 0 {
			t.Fatalf("%d-row definition boundary executed %d byte sums before cardinality rejection, want 0", total, got)
		}
	}
	assertWriterDefinitionCardinalityProgressAndCancellation(t, database)
	assertCatalogConnectionReusable(t, database)
}

func assertWriterDefinitionCardinalityProgressAndCancellation(t *testing.T, database *control.DB) {
	t.Helper()
	const query = `SELECT count(*) FROM (
		SELECT 1 FROM knowledge_definition_blobs
		WHERE tenant_id = ?
		  AND ` + boundedPreflightVisitFunction + `(?, definition_bytes) = 1
		LIMIT 65537
	)`
	const progressToken = "writer-definition-cardinality-progress"
	progress := &boundedPreflightProbe{}
	boundedPreflightProbes.Store(progressToken, progress)
	defer boundedPreflightProbes.Delete(progressToken)
	var count int64
	if err := database.GORMDB().Raw(query, testTenant, progressToken).Scan(&count).Error; err != nil {
		t.Fatalf("read definition cardinality progress prefix: %v", err)
	}
	if count != maximumVersionsPerTenant+1 || progress.visits.Load() != maximumVersionsPerTenant+1 {
		t.Fatalf(
			"definition cardinality progress = count %d visits %d, want %d/%d",
			count, progress.visits.Load(), maximumVersionsPerTenant+1, maximumVersionsPerTenant+1,
		)
	}

	preCanceled, cancelPreCanceled := context.WithCancel(context.Background())
	cancelPreCanceled()
	if _, _, err := readMutationTenantHealth(database.GORMDB().WithContext(preCanceled), testTenant); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled definition health error = %v, want context.Canceled", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	canceled := &boundedPreflightProbe{cancelAt: 8, cancel: cancel}
	const cancelToken = "writer-definition-cardinality-cancel"
	boundedPreflightProbes.Store(cancelToken, canceled)
	defer boundedPreflightProbes.Delete(cancelToken)
	count = 0
	err := database.GORMDB().WithContext(ctx).Raw(query, testTenant, cancelToken).Scan(&count).Error
	cancel()
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-query canceled definition cardinality error = %v", err)
	}
	if got := canceled.visits.Load(); got < canceled.cancelAt || got > maximumVersionsPerTenant+1 {
		t.Fatalf(
			"mid-query canceled definition cardinality visits = %d, want [%d,%d]",
			got, canceled.cancelAt, maximumVersionsPerTenant+1,
		)
	}
}

func observeWriterHealthGroupedQueries(t *testing.T, database *control.DB) *atomic.Int64 {
	t.Helper()
	var queries atomic.Int64
	name := fmt.Sprintf("test:writer-health-grouped:%p", &queries)
	if err := database.GORMDB().Callback().Row().After("gorm:row").Register(name, func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "GROUP BY") {
			queries.Add(1)
		}
	}); err != nil {
		t.Fatalf("register writer health grouped observer: %v", err)
	}
	t.Cleanup(func() {
		if err := database.GORMDB().Callback().Row().Remove(name); err != nil {
			t.Errorf("remove writer health grouped observer: %v", err)
		}
	})
	return &queries
}

func observeWriterHealthDefinitionSums(t *testing.T, database *control.DB) *atomic.Int64 {
	t.Helper()
	var queries atomic.Int64
	name := fmt.Sprintf("test:writer-health-definition-sum:%p", &queries)
	if err := database.GORMDB().Callback().Row().After("gorm:row").Register(name, func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "sum(definition_bytes)") {
			queries.Add(1)
		}
	}); err != nil {
		t.Fatalf("register writer health definition-sum observer: %v", err)
	}
	t.Cleanup(func() {
		if err := database.GORMDB().Callback().Row().Remove(name); err != nil {
			t.Errorf("remove writer health definition-sum observer: %v", err)
		}
	})
	return &queries
}

func assertWriterHealthPreflightPlan(t *testing.T, database *control.DB, query *gorm.DB, alias string) {
	t.Helper()
	compiled := query.Session(&gorm.Session{DryRun: true}).Find(&[]activeCounterPreflightRecord{})
	if compiled.Error != nil {
		t.Fatalf("compile writer health preflight: %v", compiled.Error)
	}
	details := writerHealthExplainDetails(t, database, compiled.Statement.SQL.String(), compiled.Statement.Vars...)
	joined := strings.Join(details, "\n")
	if strings.Contains(joined, "USE TEMP B-TREE") {
		t.Fatalf("writer health preflight uses a temporary sort/group:\n%s", joined)
	}
	if !strings.Contains(joined, "SEARCH "+alias+" USING PRIMARY KEY (tenant_id=?)") {
		t.Fatalf("writer health preflight does not use the exact tenant primary-key prefix:\n%s", joined)
	}
}

func assertWriterDefinitionCardinalityPlan(t *testing.T, database *control.DB) {
	t.Helper()
	details := writerHealthExplainDetails(
		t, database, mutationDefinitionCardinalitySQL, testTenant, testTenant,
	)
	joined := strings.Join(details, "\n")
	if strings.Contains(joined, "USE TEMP B-TREE") {
		t.Fatalf("definition cardinality preflight uses a temporary sort/group:\n%s", joined)
	}
	for table, plan := range map[string]string{
		"knowledge_object_versions":  `SEARCH knowledge_object_versions USING COVERING INDEX sqlite_autoindex_knowledge_object_versions_3 (tenant_id=?)`,
		"knowledge_definition_blobs": `SEARCH knowledge_definition_blobs USING PRIMARY KEY (tenant_id=?)`,
	} {
		if !strings.Contains(joined, plan) {
			t.Fatalf("definition cardinality preflight does not use %s exact tenant-prefix index:\n%s", table, joined)
		}
	}
}

func assertWriterPhysicalHealthProjectionPlan(t *testing.T, database *control.DB) {
	t.Helper()
	details := writerHealthExplainDetails(
		t,
		database,
		mutationPhysicalHealthSQL,
		testTenant,
		testTenant,
		testTenant,
		testTenant,
		testTenant,
		testTenant,
		testTenant,
	)
	joined := strings.Join(details, "\n")
	if strings.Contains(joined, "USE TEMP B-TREE") {
		t.Fatalf("physical projection health query uses a temporary sort/group:\n%s", joined)
	}
	const boundedProjectionPrefix = "knowledge_object_list_projections WHERE tenant_id = ? LIMIT 8193"
	if got := strings.Count(mutationPhysicalHealthSQL, boundedProjectionPrefix); got != 2 {
		t.Fatalf("physical projection health bounded prefixes = %d, want 2", got)
	}
	projectionAccesses := 0
	for _, detail := range details {
		if !strings.Contains(detail, "knowledge_object_list_projections") {
			continue
		}
		projectionAccesses++
		if !strings.Contains(detail, "(tenant_id=?)") || strings.HasPrefix(detail, "SCAN ") {
			t.Fatalf("physical projection health lacks exact tenant-prefix access:\n%s", joined)
		}
	}
	if projectionAccesses != 2 {
		t.Fatalf("physical projection health accesses = %d, want 2:\n%s", projectionAccesses, joined)
	}
}

func writerHealthExplainDetails(t *testing.T, database *control.DB, query string, arguments ...any) []string {
	t.Helper()
	return explainSQLiteQueryPlan(t, database.SQLDB(), query, arguments)
}

func insertWriterActiveCounterRange(
	t *testing.T,
	database *control.DB,
	kind activeCounterPreflightKind,
	start, end int,
) {
	t.Helper()
	if start < 0 || end <= start {
		t.Fatalf("invalid active counter range [%d,%d)", start, end)
	}
	spec := activeCounterSpec(kind)
	columns := "tenant_id, " + spec.firstKeyColumn
	values := "?, " + writerHealthCounterKeyExpression(kind, false)
	if spec.secondKeyColumn != "" {
		columns += ", " + spec.secondKeyColumn
		values += ", " + writerHealthCounterKeyExpression(kind, true)
	}
	columns += ", " + spec.countColumn
	values += ", 0"
	withWriterHealthCorruptionConnection(t, database, func(connection *sql.Conn) {
		// #nosec G202 -- table, columns, and expressions come only from the closed activeCounterPreflightKind mapping.
		if _, err := connection.ExecContext(context.Background(), `WITH RECURSIVE identities(n) AS (
			SELECT ?
			UNION ALL SELECT n + 1 FROM identities WHERE n + 1 < ?
		)
		INSERT INTO `+spec.table+` (`+columns+`)
		SELECT `+values+` FROM identities`, start, end, testTenant); err != nil {
			t.Fatalf("insert active %d counter range [%d,%d): %v", kind, start, end, err)
		}
	})
}

func insertWriterActiveCounterRecord(
	t *testing.T,
	database *control.DB,
	kind activeCounterPreflightKind,
	firstKey, secondKey string,
	counter int64,
) {
	t.Helper()
	spec := activeCounterSpec(kind)
	columns := "tenant_id, " + spec.firstKeyColumn
	arguments := []any{testTenant, firstKey}
	placeholders := "?, ?"
	if spec.secondKeyColumn != "" {
		columns += ", " + spec.secondKeyColumn
		arguments = append(arguments, secondKey)
		placeholders += ", ?"
	}
	columns += ", " + spec.countColumn
	arguments = append(arguments, counter)
	placeholders += ", ?"
	withWriterHealthCorruptionConnection(t, database, func(connection *sql.Conn) {
		// #nosec G202 -- table and column names come only from the closed activeCounterPreflightKind mapping.
		if _, err := connection.ExecContext(
			context.Background(),
			"INSERT INTO "+spec.table+" ("+columns+") VALUES ("+placeholders+")",
			arguments...,
		); err != nil {
			t.Fatalf("insert invalid active %d counter: %v", kind, err)
		}
	})
}

func writerHealthCounterKeyExpression(kind activeCounterPreflightKind, second bool) string {
	if second {
		return `CASE n % 3
			WHEN 0 THEN 'field_extraction'
			WHEN 1 THEN 'field_alias'
			ELSE 'calculated_field' END`
	}
	switch kind {
	case activeAppCounterPreflight:
		return `printf('app_%021dA', n)`
	case activeOwnerCounterPreflight:
		return `printf('owner-health-%05d', n)`
	case activeTypeCounterPreflight:
		return `CASE n
			WHEN 0 THEN 'field_extraction'
			WHEN 1 THEN 'field_alias'
			WHEN 2 THEN 'calculated_field'
			ELSE 'future_type' END`
	case activeAppTypeCounterPreflight:
		return `printf('app_%021dA', n / 3)`
	default:
		panic("unknown writer health counter kind")
	}
}

func writerHealthCanonicalAppID() string {
	return fmt.Sprintf("app_%021dA", 0)
}

func withWriterHealthCorruptionConnection(
	t *testing.T,
	database *control.DB,
	work func(*sql.Conn),
) {
	t.Helper()
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire writer health corruption connection: %v", err)
	}
	defer func() {
		for _, pragma := range []string{"PRAGMA ignore_check_constraints = OFF", "PRAGMA foreign_keys = ON"} {
			if _, err := connection.ExecContext(context.Background(), pragma); err != nil {
				t.Errorf("restore writer health corruption connection: %v", err)
			}
		}
		if err := connection.Close(); err != nil {
			t.Errorf("close writer health corruption connection: %v", err)
		}
	}()
	for _, pragma := range []string{"PRAGMA foreign_keys = OFF", "PRAGMA ignore_check_constraints = ON"} {
		if _, err := connection.ExecContext(context.Background(), pragma); err != nil {
			t.Fatalf("configure writer health corruption connection: %v", err)
		}
	}
	work(connection)
}

func seedWriterHealthDefinitionBoundary(t *testing.T, database *control.DB) {
	t.Helper()
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire definition boundary connection: %v", err)
	}
	defer func() { _ = connection.Close() }()
	tx, err := connection.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin definition boundary transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(context.Background(), `WITH RECURSIVE versions(n) AS (
		SELECT 2
		UNION ALL SELECT n + 1 FROM versions WHERE n < ?
	)
	INSERT INTO knowledge_definition_blobs (
		tenant_id, definition_digest, definition_proto, definition_bytes, created_at_unix_micro
	)
	SELECT ?, CAST(printf('health-definition-%014d', n) AS BLOB), X'01', 1, 100 + n
	FROM versions`, maximumVersionsPerTenant, testTenant); err != nil {
		t.Fatalf("seed exact definition blob boundary: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `WITH RECURSIVE versions(n) AS (
		SELECT 2
		UNION ALL SELECT n + 1 FROM versions WHERE n < ?
	)
	INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version, app_id, owner_id,
		object_type, name, sharing_scope, state, definition_digest,
		dependency_count, mutation_kind, quarantine_reason, created_at_unix_micro
	)
	SELECT ?, 'ko-writer-health-definition-boundary', n, ?, ?,
		'field_alias', 'writer-health-definition-boundary', 'private', 'draft',
		CAST(printf('health-definition-%014d', n) AS BLOB),
		0, 'update', NULL, 100 + n
	FROM versions`, maximumVersionsPerTenant, testTenant, testApp, testOwner); err != nil {
		t.Fatalf("seed exact immutable version boundary: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE knowledge_catalog_tenants
		SET version_count = (
			SELECT count(*) FROM knowledge_object_versions WHERE tenant_id = ?
		), definition_body_bytes = (
			SELECT coalesce(sum(definition_bytes), 0)
			FROM knowledge_definition_blobs WHERE tenant_id = ?
		)
		WHERE tenant_id = ?`, testTenant, testTenant, testTenant); err != nil {
		t.Fatalf("set exact definition boundary ledger: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact definition boundary: %v", err)
	}
}

func insertWriterHealthDefinitionBlob(t *testing.T, database *control.DB, ordinal int) {
	t.Helper()
	if err := database.GORMDB().Exec(`INSERT INTO knowledge_definition_blobs (
		tenant_id, definition_digest, definition_proto, definition_bytes, created_at_unix_micro
	) VALUES (?, CAST(printf('health-definition-%014d', ?) AS BLOB), X'01', 1, ?)`,
		testTenant, ordinal, 100+ordinal,
	).Error; err != nil {
		t.Fatalf("insert writer health definition blob %d: %v", ordinal, err)
	}
}
