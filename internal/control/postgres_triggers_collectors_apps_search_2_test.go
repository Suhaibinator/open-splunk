package control_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// Lane collectors_apps_search_2 ports the SQLite triggers on
// app_catalog_revisions, app_default_indexes, collector_catalog_revisions,
// collector_fleet and saved_searches
// (migrations/postgres/0011_collectors_apps_search_2_triggers.sql). Every
// identifier here carries the cas2 prefix because each trigger lane adds its
// own file to package control_test.

const cas2TriggerFixture = "testdata/postgres_triggers/collectors_apps_search_2.json"

// cas2Lane is this lane's name in depends_on_lanes and in its migration file
// name (NNNN_<lane>_triggers.sql).
const cas2Lane = "collectors_apps_search_2"

// cas2TriggerTables lists every trigger the lane ports with the table it is
// attached to. The catalog test requires exactly these, and the fixture must
// exercise each with at least one rejected and one accepted statement.
var cas2TriggerTables = map[string]string{
	"app_catalog_revision_delete_is_forbidden":                       "app_catalog_revisions",
	"app_catalog_revision_identity_collision_is_forbidden":           "app_catalog_revisions",
	"app_catalog_revision_initial_shape_is_exact":                    "app_catalog_revisions",
	"app_catalog_revision_provisions_knowledge_catalog_after_insert": "app_catalog_revisions",
	"app_catalog_revision_tenant_is_immutable":                       "app_catalog_revisions",
	"app_catalog_revision_transition_is_exact":                       "app_catalog_revisions",
	"app_default_indexes_require_searchable_index_insert":            "app_default_indexes",
	"app_default_indexes_require_searchable_index_update":            "app_default_indexes",
	"canonical_saved_search_app_exists_insert":                       "saved_searches",
	"canonical_saved_search_app_exists_update":                       "saved_searches",
	"collector_catalog_revision_after_fleet_delete":                  "collector_fleet",
	"collector_catalog_revision_after_fleet_insert":                  "collector_fleet",
	"collector_catalog_revision_after_fleet_update":                  "collector_fleet",
	"collector_catalog_revision_before_fleet_insert":                 "collector_fleet",
	"collector_catalog_revision_marker_is_undeletable":               "collector_catalog_revisions",
	"collector_fleet_admin_version_is_monotonic":                     "collector_fleet",
	"collector_fleet_identity_is_immutable":                          "collector_fleet",
	// A tenant guard with no SQLite counterpart.
	"saved_search_tenant_is_immutable": "saved_searches",
}

// cas2TriggerScenario is one fixture entry. The setup statements and the
// statement run in one transaction with search_path = open_splunk, which is
// always rolled back, so scenarios cannot observe one another.
type cas2TriggerScenario struct {
	Name string `json:"name"`
	// DependsOnLanes names other trigger lanes whose triggers the setup or
	// the expected outcome relies on. Such a scenario runs with every
	// embedded trigger enabled, in
	// TestPostgresTriggersCollectorsAppsSearch2CrossLane, once every named
	// lane's migration is embedded, and must then behave exactly as written.
	// A scenario without it is lane-local: it runs with every trigger outside
	// this lane disabled, so its setup can plant states that other lanes'
	// triggers would refuse or rewrite, exactly as the SQLite differential
	// run that produced it did.
	DependsOnLanes []string `json:"depends_on_lanes,omitempty"`
	SetupSQL       []string `json:"setup_sql"`
	Statement      string   `json:"statement"`
	// Expect is "reject" (the statement fails with SQLSTATE P0001 and exactly
	// Message) or "accept" (it succeeds and Message is empty).
	Expect  string `json:"expect"`
	Message string `json:"message"`
	// CheckSQL, when set, runs after an accepted statement in the same
	// transaction and must return one row of one text column equal to
	// CheckResult, so accepted statements also prove the rows they leave.
	CheckSQL    string `json:"check_sql,omitempty"`
	CheckResult string `json:"check_result,omitempty"`
}

func (scenario cas2TriggerScenario) trigger() string {
	trigger, _, _ := strings.Cut(scenario.Name, "/")
	return trigger
}

func loadCAS2TriggerScenarios(t *testing.T) []cas2TriggerScenario {
	t.Helper()
	contents, err := os.ReadFile(filepath.FromSlash(cas2TriggerFixture))
	if err != nil {
		t.Fatalf("read trigger fixture: %v", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(contents)))
	decoder.DisallowUnknownFields()
	var scenarios []cas2TriggerScenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode trigger fixture: %v", err)
	}
	if len(scenarios) == 0 {
		t.Fatal("trigger fixture has no scenarios")
	}
	seen := make(map[string]bool, len(scenarios))
	for _, scenario := range scenarios {
		if seen[scenario.Name] {
			t.Fatalf("trigger fixture repeats scenario %q", scenario.Name)
		}
		seen[scenario.Name] = true
		if _, ok := cas2TriggerTables[scenario.trigger()]; !ok {
			t.Fatalf("scenario %q does not name one of this lane's triggers", scenario.Name)
		}
		if strings.TrimSpace(scenario.Statement) == "" {
			t.Fatalf("scenario %q has no statement", scenario.Name)
		}
		switch scenario.Expect {
		case "reject":
			if scenario.Message == "" || scenario.CheckSQL != "" {
				t.Fatalf("rejected scenario %q needs a message and no check", scenario.Name)
			}
		case "accept":
			if scenario.Message != "" {
				t.Fatalf("accepted scenario %q has a message", scenario.Name)
			}
			if (scenario.CheckSQL == "") != (scenario.CheckResult == "") {
				t.Fatalf("scenario %q needs both check_sql and check_result, or neither", scenario.Name)
			}
		default:
			t.Fatalf("scenario %q has expect %q, want reject or accept", scenario.Name, scenario.Expect)
		}
		if err := checkPostgresTriggerLaneDependencies(cas2Lane, scenario.DependsOnLanes); err != nil {
			t.Fatalf("scenario %q: %v", scenario.Name, err)
		}
	}
	return scenarios
}

// cas2EmbeddedLanes returns the trigger lanes whose migration files the
// shipped PostgreSQL set embeds.
func cas2EmbeddedLanes(t *testing.T) map[string]bool {
	t.Helper()
	lanes := postgresEmbeddedTriggerLanes(t)
	if !lanes[cas2Lane] {
		t.Fatalf("embedded PostgreSQL migrations do not include the %s trigger file", cas2Lane)
	}
	return lanes
}

// TestPostgresTriggersCollectorsAppsSearch2 runs every lane-local fixture
// scenario against the shipped migrations with only this lane's triggers
// enabled.
func TestPostgresTriggersCollectorsAppsSearch2(t *testing.T) {
	t.Parallel()

	scenarios := loadCAS2TriggerScenarios(t)
	db := testsupport.NewPostgresControlDB(t)
	ran := 0
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) > 0 {
			continue
		}
		ran++
		t.Run(scenario.Name, func(t *testing.T) {
			runCAS2TriggerScenario(t, db, scenario)
		})
	}
	if ran == 0 {
		t.Fatal("no lane-local trigger scenarios ran")
	}
}

// TestPostgresTriggersCollectorsAppsSearch2CrossLane runs the scenarios whose
// setup or outcome needs another lane's triggers, with every embedded trigger
// enabled. Every lane is merged, so every such scenario must name only
// embedded lanes and pass exactly; there is no skip and no waiting list, so a
// lane that behaves differently, or a misspelled lane, fails here.
func TestPostgresTriggersCollectorsAppsSearch2CrossLane(t *testing.T) {
	t.Parallel()

	scenarios := loadCAS2TriggerScenarios(t)
	embedded := cas2EmbeddedLanes(t)
	var runnable []cas2TriggerScenario
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) == 0 {
			continue
		}
		requirePostgresTriggerLanesEmbedded(t, embedded, scenario.DependsOnLanes)
		runnable = append(runnable, scenario)
	}
	if len(runnable) == 0 {
		t.Fatal("trigger fixture has no cross-lane scenarios")
	}
	db := testsupport.NewPostgresControlDB(t)
	for _, scenario := range runnable {
		t.Run(scenario.Name, func(t *testing.T) {
			runCAS2TriggerScenario(t, db, scenario)
		})
	}
}

func runCAS2TriggerScenario(t *testing.T, db *control.PostgresDB, scenario cas2TriggerScenario) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("rollback: %v", err)
		}
	}()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path = open_splunk`); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	if len(scenario.DependsOnLanes) == 0 {
		isolateCAS2Triggers(ctx, t, tx)
	}
	for index, statement := range scenario.SetupSQL {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("setup statement %d failed: %v\n%s", index+1, err, statement)
		}
	}
	_, err = tx.ExecContext(ctx, scenario.Statement)
	if scenario.Expect == "reject" {
		assertCAS2Raise(t, err, scenario.Message)
		return
	}
	if err != nil {
		t.Fatalf("statement rejected, want accepted: %v\n%s", err, scenario.Statement)
	}
	if scenario.CheckSQL == "" {
		return
	}
	var got sql.NullString
	if err := tx.QueryRowContext(ctx, scenario.CheckSQL).Scan(&got); err != nil {
		t.Fatalf("check query failed: %v\n%s", err, scenario.CheckSQL)
	}
	if !got.Valid || got.String != scenario.CheckResult {
		t.Fatalf("check %s\n got %q (valid=%t), want %q", scenario.CheckSQL, got.String, got.Valid, scenario.CheckResult)
	}
}

// isolateCAS2Triggers disables, for the rest of tx only, every user trigger
// in open_splunk that this lane does not own. Foreign keys are internal
// triggers and stay enforced. The ALTERs roll back with tx, and they lock
// only the scenario's own database.
func isolateCAS2Triggers(ctx context.Context, t *testing.T, tx *sql.Tx) {
	t.Helper()
	names := slices.Sorted(maps.Keys(cas2TriggerTables))
	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('open_splunk_test.cas2_lane_triggers', $1, true)`,
		strings.Join(names, ","),
	); err != nil {
		t.Fatalf("record lane triggers: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DO $$
		DECLARE
			target record;
		BEGIN
			FOR target IN
				SELECT relation.relname, trigger.tgname
				FROM pg_catalog.pg_trigger AS trigger
				JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				WHERE namespace.nspname = 'open_splunk'
				  AND NOT trigger.tgisinternal
				  AND trigger.tgname <> ALL (string_to_array(
				      current_setting('open_splunk_test.cas2_lane_triggers'), ','))
			LOOP
				EXECUTE format('ALTER TABLE open_splunk.%I DISABLE TRIGGER %I',
					target.relname, target.tgname);
			END LOOP;
		END
		$$`); err != nil {
		t.Fatalf("disable other lanes' triggers: %v", err)
	}
}

// assertCAS2Raise requires a PL/pgSQL RAISE (P0001) carrying exactly message,
// because Go callers match these texts.
func assertCAS2Raise(t *testing.T, err error, message string) {
	t.Helper()
	if err == nil {
		t.Fatalf("statement accepted, want rejection %q", message)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("rejection is not a PostgreSQL error: %v", err)
	}
	if pgErr.Code != "P0001" || pgErr.Message != message {
		t.Fatalf("rejection = %s %q, want P0001 %q", pgErr.Code, pgErr.Message, message)
	}
	if got, ok := control.RaiseMessage(err); !ok || got != message || !control.IsRaise(err) {
		t.Fatalf("control.RaiseMessage(%v) = %q, %t", err, got, ok)
	}
}

// TestPostgresTriggersCollectorsAppsSearch2Catalog checks that the migration
// installs exactly this lane's triggers on their tables, enabled, and that
// the fixture rejects and accepts at least one statement through each.
func TestPostgresTriggersCollectorsAppsSearch2Catalog(t *testing.T) {
	t.Parallel()

	scenarios := loadCAS2TriggerScenarios(t)
	rejects := make(map[string]int)
	accepts := make(map[string]int)
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) > 0 {
			continue
		}
		if scenario.Expect == "reject" {
			rejects[scenario.trigger()]++
		} else {
			accepts[scenario.trigger()]++
		}
	}
	for trigger := range cas2TriggerTables {
		if rejects[trigger] == 0 || accepts[trigger] == 0 {
			t.Errorf("trigger %s has %d rejected and %d accepted lane-local scenarios, want at least one of each",
				trigger, rejects[trigger], accepts[trigger])
		}
	}

	db := testsupport.NewPostgresControlDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	names := slices.Sorted(maps.Keys(cas2TriggerTables))
	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT trigger.tgname, relation.relname, trigger.tgenabled
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = 'open_splunk'
		  AND NOT trigger.tgisinternal
		  AND trigger.tgname = ANY ($1)
		ORDER BY trigger.tgname`, names)
	if err != nil {
		t.Fatalf("list triggers: %v", err)
	}
	defer func() { _ = rows.Close() }()
	found := make(map[string]bool)
	for rows.Next() {
		var name, table, enabled string
		if err := rows.Scan(&name, &table, &enabled); err != nil {
			t.Fatalf("scan trigger: %v", err)
		}
		if found[name] {
			t.Errorf("trigger %s is defined on more than one table", name)
		}
		found[name] = true
		if want := cas2TriggerTables[name]; table != want {
			t.Errorf("trigger %s is on %s, want %s", name, table, want)
		}
		if enabled != "O" {
			t.Errorf("trigger %s has tgenabled %q, want O (origin and local)", name, enabled)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("list triggers: %v", err)
	}
	for _, name := range names {
		if !found[name] {
			t.Errorf("trigger %s is not installed", name)
		}
	}
}

// The tests below port the SQLite collector fleet trigger tests in
// internal/collectorfleet/migration_test.go that assert this lane's messages
// together with the rows a rejected statement must leave unchanged. They
// commit their setup, like those tests, so every check reads what a later
// transaction would see. The collector_fleet and collector_catalog_revisions
// tables belong to this lane only, so the tests run with every embedded
// trigger enabled.

const cas2CollectorInsert = `
	INSERT INTO collector_fleet (
		tenant_id, collector_id, admin_version, display_name,
		administrative_state, first_seen_at_unix_micro, updated_at_unix_micro
	) VALUES ($1, $2, 1, NULL, 'enabled', 1, 1)`

// cas2CollectorCapacity is the per-tenant fleet bound the triggers enforce
// (collectorfleet.MaximumDurableCollectorsPerTenant).
const cas2CollectorCapacity = 256

type cas2Marker struct {
	revision, fleetCount, runtimeCount int64
}

func newCAS2CollectorTenant(ctx context.Context, t *testing.T, db *sql.DB, tenantID string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
		VALUES ($1, 1, 1)`, tenantID); err != nil {
		t.Fatalf("insert tenant %s: %v", tenantID, err)
	}
}

func readCAS2Marker(ctx context.Context, t *testing.T, db *sql.DB, tenantID string) (cas2Marker, bool) {
	t.Helper()
	var marker cas2Marker
	err := db.QueryRowContext(ctx, `
		SELECT revision, fleet_count, runtime_count
		FROM collector_catalog_revisions
		WHERE tenant_id = $1`, tenantID).Scan(&marker.revision, &marker.fleetCount, &marker.runtimeCount)
	if errors.Is(err, sql.ErrNoRows) {
		return cas2Marker{}, false
	}
	if err != nil {
		t.Fatalf("read collector catalog revision of %s: %v", tenantID, err)
	}
	return marker, true
}

func assertCAS2Marker(ctx context.Context, t *testing.T, db *sql.DB, tenantID string, want cas2Marker) {
	t.Helper()
	got, ok := readCAS2Marker(ctx, t, db, tenantID)
	if !ok {
		t.Fatalf("collector catalog revision of %s is missing, want %+v", tenantID, want)
	}
	if got != want {
		t.Fatalf("collector catalog revision of %s = %+v, want %+v", tenantID, got, want)
	}
}

func countCAS2Collectors(ctx context.Context, t *testing.T, db *sql.DB, tenantID string) int64 {
	t.Helper()
	var count int64
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM collector_fleet WHERE tenant_id = $1`, tenantID,
	).Scan(&count); err != nil {
		t.Fatalf("count collectors of %s: %v", tenantID, err)
	}
	return count
}

// TestPostgresTriggersCollectorsAppsSearch2CollectorCapacity ports
// TestCollectorFleetMigrationDefendsDurableCollectorCapacity: a tenant holding
// the maximum number of collectors rejects one more, and the rejected insert
// leaves the marker and the fleet as they were. The fleet is filled by one
// multi-row INSERT, whose AFTER ROW events PostgreSQL queues to the end of
// the statement, so the counter must still come out exact.
func TestPostgresTriggersCollectorsAppsSearch2CollectorCapacity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	const tenantID = "tenant-capacity"
	newCAS2CollectorTenant(ctx, t, db, tenantID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_fleet (
			tenant_id, collector_id, admin_version, display_name,
			administrative_state, first_seen_at_unix_micro, updated_at_unix_micro
		)
		SELECT $1, 'collector-' || ordinal, 1, NULL, 'enabled', 1, 1
		FROM generate_series(1, $2::bigint) AS ordinal`,
		tenantID, cas2CollectorCapacity,
	); err != nil {
		t.Fatalf("fill the fleet to capacity in one statement: %v", err)
	}
	full := cas2Marker{revision: cas2CollectorCapacity, fleetCount: cas2CollectorCapacity}
	assertCAS2Marker(ctx, t, db, tenantID, full)

	_, err := db.ExecContext(ctx, cas2CollectorInsert, tenantID, "collector-over-capacity")
	assertCAS2Raise(t, err, "collector fleet capacity exhausted")
	assertCAS2Marker(ctx, t, db, tenantID, full)
	if got := countCAS2Collectors(ctx, t, db, tenantID); got != cas2CollectorCapacity {
		t.Fatalf("collectors after the rejected insert = %d, want %d", got, cas2CollectorCapacity)
	}

	// Removing one collector frees exactly one slot.
	if _, err := db.ExecContext(ctx,
		`DELETE FROM collector_fleet WHERE tenant_id = $1 AND collector_id = 'collector-1'`, tenantID,
	); err != nil {
		t.Fatalf("delete one collector: %v", err)
	}
	if _, err := db.ExecContext(ctx, cas2CollectorInsert, tenantID, "collector-over-capacity"); err != nil {
		t.Fatalf("insert into the freed slot: %v", err)
	}
	assertCAS2Marker(ctx, t, db, tenantID, cas2Marker{
		revision:   cas2CollectorCapacity + 2,
		fleetCount: cas2CollectorCapacity,
	})
}

// TestPostgresTriggersCollectorsAppsSearch2RejectedFleetMutationsAreAtomic
// ports the fleet halves of the SQLite tests for the revision marker guard,
// revision overflow, count underflow, a missing marker, and the identity and
// administrator-version guards: each rejected statement raises its exact
// message and leaves both the marker and the collector row unchanged.
func TestPostgresTriggersCollectorsAppsSearch2RejectedFleetMutationsAreAtomic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	const tenantID = "tenant-atomic"
	newCAS2CollectorTenant(ctx, t, db, tenantID)
	if _, err := db.ExecContext(ctx, cas2CollectorInsert, tenantID, "collector-a"); err != nil {
		t.Fatalf("insert collector: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE collector_fleet SET admin_version = 3
		WHERE tenant_id = $1 AND collector_id = 'collector-a'`, tenantID); err != nil {
		t.Fatalf("advance administrator version: %v", err)
	}
	assertCAS2Marker(ctx, t, db, tenantID, cas2Marker{revision: 2, fleetCount: 1})

	type collectorRow struct {
		adminVersion int64
		displayName  sql.NullString
	}
	readCollector := func() collectorRow {
		t.Helper()
		var row collectorRow
		if err := db.QueryRowContext(ctx, `
			SELECT admin_version, display_name FROM collector_fleet
			WHERE tenant_id = $1 AND collector_id = 'collector-a'`, tenantID,
		).Scan(&row.adminVersion, &row.displayName); err != nil {
			t.Fatalf("read collector-a: %v", err)
		}
		return row
	}
	exec := func(statement string, args ...any) error {
		t.Helper()
		_, err := db.ExecContext(ctx, statement, args...)
		return err
	}
	mustExec := func(statement string, args ...any) {
		t.Helper()
		if err := exec(statement, args...); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	unchanged := collectorRow{adminVersion: 3}
	assertUnchanged := func(want cas2Marker) {
		t.Helper()
		assertCAS2Marker(ctx, t, db, tenantID, want)
		if got := readCollector(); got != unchanged {
			t.Fatalf("collector-a after a rejected statement = %+v, want %+v", got, unchanged)
		}
	}
	healthy := cas2Marker{revision: 2, fleetCount: 1}

	t.Run("marker delete", func(t *testing.T) {
		assertCAS2Raise(t, exec(`DELETE FROM collector_catalog_revisions WHERE tenant_id = $1`, tenantID),
			"collector catalog fleet/runtime revision marker cannot be deleted")
		assertUnchanged(healthy)
	})
	t.Run("identity change", func(t *testing.T) {
		assertCAS2Raise(t, exec(`
			UPDATE collector_fleet SET collector_id = 'collector-b'
			WHERE tenant_id = $1 AND collector_id = 'collector-a'`, tenantID),
			"collector fleet identity is immutable")
		assertUnchanged(healthy)
	})
	t.Run("administrator version rollback", func(t *testing.T) {
		assertCAS2Raise(t, exec(`
			UPDATE collector_fleet SET admin_version = 2, display_name = 'must-roll-back'
			WHERE tenant_id = $1 AND collector_id = 'collector-a'`, tenantID),
			"collector administrator version cannot move backward")
		assertUnchanged(healthy)
	})
	t.Run("revision overflow", func(t *testing.T) {
		mustExec(`UPDATE collector_catalog_revisions SET revision = $2 WHERE tenant_id = $1`,
			tenantID, int64(math.MaxInt64))
		exhausted := cas2Marker{revision: math.MaxInt64, fleetCount: 1}
		assertCAS2Raise(t, exec(`
			UPDATE collector_fleet SET display_name = 'must-roll-back'
			WHERE tenant_id = $1 AND collector_id = 'collector-a'`, tenantID),
			"collector catalog revision exhausted")
		assertUnchanged(exhausted)
		assertCAS2Raise(t, exec(cas2CollectorInsert, tenantID, "collector-b"),
			"collector catalog revision exhausted")
		assertUnchanged(exhausted)
		if got := countCAS2Collectors(ctx, t, db, tenantID); got != 1 {
			t.Fatalf("collectors after the rejected insert = %d, want 1", got)
		}
		mustExec(`UPDATE collector_catalog_revisions SET revision = 2 WHERE tenant_id = $1`, tenantID)
	})
	t.Run("fleet count underflow", func(t *testing.T) {
		mustExec(`UPDATE collector_catalog_revisions SET fleet_count = 0 WHERE tenant_id = $1`, tenantID)
		assertCAS2Raise(t, exec(`
			DELETE FROM collector_fleet WHERE tenant_id = $1 AND collector_id = 'collector-a'`, tenantID),
			"collector catalog fleet count underflow")
		assertUnchanged(cas2Marker{revision: 2})
		mustExec(`UPDATE collector_catalog_revisions SET fleet_count = 1 WHERE tenant_id = $1`, tenantID)
	})
	t.Run("missing marker", func(t *testing.T) {
		// Only a writer that bypasses the delete guard can lose the marker;
		// reproduce that the way the SQLite test drops its trigger.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		for _, statement := range []string{
			`ALTER TABLE collector_catalog_revisions DISABLE TRIGGER collector_catalog_revision_marker_is_undeletable`,
			`DELETE FROM collector_catalog_revisions WHERE tenant_id = 'tenant-atomic'`,
			`ALTER TABLE collector_catalog_revisions ENABLE TRIGGER collector_catalog_revision_marker_is_undeletable`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				_ = tx.Rollback()
				t.Fatalf("force marker loss: %v\n%s", err, statement)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit marker loss: %v", err)
		}
		const missing = "collector catalog fleet/runtime revision is missing"
		for _, statement := range []string{
			`UPDATE collector_fleet SET display_name = 'must-roll-back'
			 WHERE tenant_id = $1 AND collector_id = 'collector-a'`,
			`DELETE FROM collector_fleet WHERE tenant_id = $1 AND collector_id = 'collector-a'`,
		} {
			assertCAS2Raise(t, exec(statement, tenantID), missing)
			if got := readCollector(); got != unchanged {
				t.Fatalf("collector-a after a rejected statement = %+v, want %+v", got, unchanged)
			}
		}
		assertCAS2Raise(t, exec(cas2CollectorInsert, tenantID, "collector-b"), missing)
		if _, ok := readCAS2Marker(ctx, t, db, tenantID); ok {
			t.Fatal("a rejected statement recreated the lost marker")
		}
		if got := countCAS2Collectors(ctx, t, db, tenantID); got != 1 {
			t.Fatalf("collectors after the rejected insert = %d, want 1", got)
		}
	})
}

// TestPostgresTriggersCollectorsAppsSearch2ConcurrentFleetInserts checks the
// concurrency contract in the migration header: two transactions inserting
// collectors for one tenant serialize on its marker row. Under READ
// COMMITTED the second waits and then counts the first; under SERIALIZABLE,
// which WriteTx uses, it fails with a retryable 40001 and its retry counts
// exactly. Both the first collector of a tenant (which creates the marker
// through the unique index) and a later one (which locks the existing marker)
// are covered.
func TestPostgresTriggersCollectorsAppsSearch2ConcurrentFleetInserts(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	for _, test := range []struct {
		name      string
		tenantID  string
		isolation sql.IsolationLevel
		existing  bool
	}{
		{name: "read committed first collector", tenantID: "race-rc-first", isolation: sql.LevelReadCommitted},
		{name: "read committed existing marker", tenantID: "race-rc-existing", isolation: sql.LevelReadCommitted, existing: true},
		{name: "serializable first collector", tenantID: "race-ser-first", isolation: sql.LevelSerializable},
		{name: "serializable existing marker", tenantID: "race-ser-existing", isolation: sql.LevelSerializable, existing: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			newCAS2CollectorTenant(ctx, t, db, test.tenantID)
			var before cas2Marker
			if test.existing {
				if _, err := db.ExecContext(ctx, cas2CollectorInsert, test.tenantID, "collector-0"); err != nil {
					t.Fatalf("insert the existing collector: %v", err)
				}
				before = cas2Marker{revision: 1, fleetCount: 1}
			}
			options := &sql.TxOptions{Isolation: test.isolation}
			first, err := db.BeginTx(ctx, options)
			if err != nil {
				t.Fatalf("begin first: %v", err)
			}
			defer func() { _ = first.Rollback() }()
			second, err := db.BeginTx(ctx, options)
			if err != nil {
				t.Fatalf("begin second: %v", err)
			}
			defer func() { _ = second.Rollback() }()
			// The second transaction's first statement takes its snapshot
			// before the first commits.
			var secondPID int64
			if err := second.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&secondPID); err != nil {
				t.Fatalf("read second backend: %v", err)
			}
			if _, err := first.ExecContext(ctx, cas2CollectorInsert, test.tenantID, "collector-a"); err != nil {
				t.Fatalf("first insert: %v", err)
			}
			secondDone := make(chan error, 1)
			go func() {
				_, err := second.ExecContext(ctx, cas2CollectorInsert, test.tenantID, "collector-b")
				secondDone <- err
			}()
			waitForCAS2LockWait(ctx, t, db, secondPID, secondDone)
			if err := first.Commit(); err != nil {
				t.Fatalf("commit first: %v", err)
			}
			want := cas2Marker{revision: before.revision + 2, fleetCount: before.fleetCount + 2}
			secondErr := <-secondDone
			if test.isolation == sql.LevelReadCommitted {
				if secondErr != nil {
					t.Fatalf("second insert under READ COMMITTED: %v", secondErr)
				}
				if err := second.Commit(); err != nil {
					t.Fatalf("commit second: %v", err)
				}
				assertCAS2Marker(ctx, t, db, test.tenantID, want)
				return
			}
			if !control.IsRetryable(secondErr) {
				t.Fatalf("second insert under SERIALIZABLE = %v, want a retryable serialization failure", secondErr)
			}
			if code, _ := control.PostgresErrorCode(secondErr); code != "40001" {
				t.Fatalf("second insert SQLSTATE = %q, want 40001", code)
			}
			_ = second.Rollback()
			assertCAS2Marker(ctx, t, db, test.tenantID,
				cas2Marker{revision: before.revision + 1, fleetCount: before.fleetCount + 1})
			retry, err := db.BeginTx(ctx, options)
			if err != nil {
				t.Fatalf("begin retry: %v", err)
			}
			defer func() { _ = retry.Rollback() }()
			if _, err := retry.ExecContext(ctx, cas2CollectorInsert, test.tenantID, "collector-b"); err != nil {
				t.Fatalf("retried insert: %v", err)
			}
			if err := retry.Commit(); err != nil {
				t.Fatalf("commit retry: %v", err)
			}
			assertCAS2Marker(ctx, t, db, test.tenantID, want)
		})
	}
}

// waitForCAS2LockWait returns once backend pid is blocked on a lock, which
// proves the second writer reached the marker before the first committed.
func waitForCAS2LockWait(ctx context.Context, t *testing.T, db *sql.DB, pid int64, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var waitEventType sql.NullString
		if err := db.QueryRowContext(ctx,
			`SELECT wait_event_type FROM pg_catalog.pg_stat_activity WHERE pid = $1`, pid,
		).Scan(&waitEventType); err != nil {
			t.Fatalf("read backend %d state: %v", pid, err)
		}
		if waitEventType.String == "Lock" {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("second insert finished before the first committed (err = %v), want it to wait", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("backend %d never waited on a lock (wait_event_type %q)", pid, waitEventType.String)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
