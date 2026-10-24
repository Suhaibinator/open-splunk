package control_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
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
}

// cas2TriggerScenario is one fixture entry. The setup statements and the
// statement run in one transaction with search_path = open_splunk, which is
// always rolled back, so scenarios cannot observe one another.
type cas2TriggerScenario struct {
	Name string `json:"name"`
	// DependsOnLanes names other trigger lanes whose triggers the setup or
	// the expected outcome relies on. Such a scenario runs in
	// TestPostgresTriggersCollectorsAppsSearch2CrossLane once every named
	// lane's migration is embedded, and must then behave exactly as written.
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
		for _, lane := range scenario.DependsOnLanes {
			if lane == cas2Lane || !cas2LaneName.MatchString(lane) {
				t.Fatalf("scenario %q depends on invalid lane %q", scenario.Name, lane)
			}
		}
	}
	return scenarios
}

var cas2LaneName = regexp.MustCompile(`^[a-z0-9]+(?:_[a-z0-9]+)*$`)

var cas2TriggerMigration = regexp.MustCompile(`^[0-9]{4}_([a-z0-9_]+)_triggers\.sql$`)

// cas2EmbeddedLanes returns the trigger lanes whose migration files the
// shipped PostgreSQL set embeds.
func cas2EmbeddedLanes(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	lanes := make(map[string]bool)
	for _, entry := range entries {
		if matches := cas2TriggerMigration.FindStringSubmatch(entry.Name()); matches != nil {
			lanes[matches[1]] = true
		}
	}
	if !lanes[cas2Lane] {
		t.Fatalf("embedded PostgreSQL migrations do not include the %s trigger file", cas2Lane)
	}
	return lanes
}

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
// setup or outcome needs another lane's triggers. A scenario whose lanes are
// not all embedded yet is skipped by name with the lanes it is waiting for;
// once they are embedded it must pass exactly, so a lane that is merged but
// behaves differently fails here instead of being skipped.
func TestPostgresTriggersCollectorsAppsSearch2CrossLane(t *testing.T) {
	t.Parallel()

	scenarios := loadCAS2TriggerScenarios(t)
	embedded := cas2EmbeddedLanes(t)
	missing := make(map[string][]string)
	runnable := 0
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) == 0 {
			continue
		}
		for _, lane := range scenario.DependsOnLanes {
			if !embedded[lane] {
				missing[scenario.Name] = append(missing[scenario.Name], lane)
			}
		}
		if len(missing[scenario.Name]) == 0 {
			runnable++
		}
	}
	var db *control.PostgresDB
	if runnable > 0 {
		db = testsupport.NewPostgresControlDB(t)
	}
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) == 0 {
			continue
		}
		t.Run(scenario.Name, func(t *testing.T) {
			if lanes := missing[scenario.Name]; len(lanes) > 0 {
				t.Skipf("needs the triggers of lane(s) %s, which are not embedded yet",
					strings.Join(lanes, ", "))
			}
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
	if !control.IsRaise(err) {
		t.Fatalf("control.IsRaise(%v) = false", err)
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
		if scenario.Expect == "reject" {
			rejects[scenario.trigger()]++
		} else {
			accepts[scenario.trigger()]++
		}
	}
	for trigger := range cas2TriggerTables {
		if rejects[trigger] == 0 || accepts[trigger] == 0 {
			t.Errorf("trigger %s has %d rejected and %d accepted scenarios, want at least one of each",
				trigger, rejects[trigger], accepts[trigger])
		}
	}

	db := testsupport.NewPostgresControlDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	names := make([]string, 0, len(cas2TriggerTables))
	for trigger := range cas2TriggerTables {
		names = append(names, trigger)
	}
	slices.Sort(names)
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
