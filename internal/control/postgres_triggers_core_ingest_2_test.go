package control_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"modernc.org/sqlite"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// The core_ingest_2 trigger lane ports the SQLite triggers on
// index_catalog_state, index_data_deletion_completions,
// index_deletion_mutation_attempts and ingest_write_group_members to
// migrations/postgres/0008_core_ingest_2_triggers.sql. Its differential
// fixture holds, per scenario, a PostgreSQL and a SQLite spelling of the same
// setup and statement and the outcome both engines must produce. These tests
// run every scenario against the shipped PostgreSQL migrations and against
// the shipped SQLite migrations, so the port and the fixture cannot drift
// from the schema the server still runs.
const (
	coreIngest2Lane            = "core_ingest_2"
	coreIngest2Migration       = "0008_core_ingest_2_triggers.sql"
	coreIngest2FixturePath     = "testdata/postgres_triggers/core_ingest_2.json"
	coreIngest2TriggerCount    = 19
	coreIngest2ExpectAccept    = "accept"
	coreIngest2ExpectReject    = "reject"
	coreIngest2SQLiteRaiseCode = 1811 // SQLITE_CONSTRAINT_TRIGGER, the code RAISE(ABORT, ...) reports.
)

var (
	coreIngest2CreateTrigger = regexp.MustCompile(`(?m)^CREATE TRIGGER ([a-z0-9_]+)$`)
	coreIngest2LaneKey       = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// coreIngest2Scenario is one fixture entry. SetupSQL and Statement are
// PostgreSQL and assume search_path = open_splunk; the SQLite fields spell
// the same state for the SQLite schema, which has no tenants table and
// writes blob literals differently. PostconditionSQL, when present, is one
// query, valid in both engines, that must return true after an accepted
// statement. DependsOnLanes names other trigger lanes whose PostgreSQL
// triggers the scenario needs; such a scenario runs only in
// TestPostgresCoreIngest2CrossLaneTriggerScenarios.
type coreIngest2Scenario struct {
	Name             string   `json:"name"`
	Trigger          string   `json:"trigger"`
	SetupSQL         []string `json:"setup_sql"`
	SQLiteSetupSQL   []string `json:"sqlite_setup_sql"`
	Statement        string   `json:"statement"`
	SQLiteStatement  string   `json:"sqlite_statement"`
	Expect           string   `json:"expect"`
	Message          string   `json:"message"`
	Note             string   `json:"note,omitempty"`
	PostconditionSQL string   `json:"postcondition_sql,omitempty"`
	DependsOnLanes   []string `json:"depends_on_lanes,omitempty"`
}

func loadCoreIngest2Scenarios(t *testing.T) []coreIngest2Scenario {
	t.Helper()
	contents, err := os.ReadFile(filepath.FromSlash(coreIngest2FixturePath))
	if err != nil {
		t.Fatalf("read %s: %v", coreIngest2FixturePath, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var scenarios []coreIngest2Scenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode %s: %v", coreIngest2FixturePath, err)
	}
	if decoder.More() {
		t.Fatalf("%s holds more than one JSON value", coreIngest2FixturePath)
	}
	return scenarios
}

// coreIngest2MigrationTriggers returns the trigger names the lane's shipped
// migration creates, in file order.
func coreIngest2MigrationTriggers(t *testing.T) []string {
	t.Helper()
	contents, err := fs.ReadFile(migrations.Postgres(), coreIngest2Migration)
	if err != nil {
		t.Fatalf("read embedded %s: %v", coreIngest2Migration, err)
	}
	var names []string
	for _, match := range coreIngest2CreateTrigger.FindAllSubmatch(contents, -1) {
		names = append(names, string(match[1]))
	}
	return names
}

// coreIngest2ShippedLanes reports which trigger lanes the embedded
// PostgreSQL set carries, keyed by lane name (NNNN_<lane>_triggers.sql).
func coreIngest2ShippedLanes(t *testing.T) map[string]bool {
	t.Helper()
	names, err := fs.Glob(migrations.Postgres(), "*_triggers.sql")
	if err != nil {
		t.Fatalf("list embedded PostgreSQL trigger migrations: %v", err)
	}
	lanes := make(map[string]bool, len(names))
	for _, name := range names {
		if len(name) > len("0000_") {
			lanes[strings.TrimSuffix(name[len("0000_"):], "_triggers.sql")] = true
		}
	}
	return lanes
}

func TestPostgresCoreIngest2TriggerFixtureCoversEveryTrigger(t *testing.T) {
	t.Parallel()
	scenarios := loadCoreIngest2Scenarios(t)
	triggers := coreIngest2MigrationTriggers(t)
	if len(triggers) != coreIngest2TriggerCount {
		t.Fatalf("%s creates %d triggers, want %d: %v",
			coreIngest2Migration, len(triggers), coreIngest2TriggerCount, triggers)
	}
	type outcomes struct{ accepted, rejected int }
	coverage := make(map[string]*outcomes, len(triggers))
	for _, name := range triggers {
		if coverage[name] != nil {
			t.Fatalf("%s creates trigger %s twice", coreIngest2Migration, name)
		}
		coverage[name] = &outcomes{}
	}

	seen := make(map[string]bool, len(scenarios))
	for index, scenario := range scenarios {
		label := fmt.Sprintf("scenario %d (%s)", index, scenario.Name)
		switch {
		case scenario.Name == "":
			t.Fatalf("%s has no name", label)
		case seen[scenario.Name]:
			t.Fatalf("%s repeats a scenario name", label)
		case strings.TrimSpace(scenario.Statement) == "" || strings.TrimSpace(scenario.SQLiteStatement) == "":
			t.Fatalf("%s needs both a PostgreSQL and a SQLite statement", label)
		case coverage[scenario.Trigger] == nil:
			t.Fatalf("%s names trigger %q, which %s does not create", label, scenario.Trigger, coreIngest2Migration)
		}
		seen[scenario.Name] = true
		for _, lane := range scenario.DependsOnLanes {
			if !coreIngest2LaneKey.MatchString(lane) || lane == coreIngest2Lane {
				t.Fatalf("%s depends on invalid lane %q", label, lane)
			}
		}
		switch scenario.Expect {
		case coreIngest2ExpectAccept:
			if scenario.Message != "" {
				t.Fatalf("%s is accepted but carries message %q", label, scenario.Message)
			}
			coverage[scenario.Trigger].accepted++
		case coreIngest2ExpectReject:
			if scenario.Message == "" {
				t.Fatalf("%s is rejected without an expected message", label)
			}
			if scenario.PostconditionSQL != "" {
				t.Fatalf("%s is rejected; a postcondition only applies to accepted statements", label)
			}
			coverage[scenario.Trigger].rejected++
		default:
			t.Fatalf("%s expects %q, want %q or %q", label, scenario.Expect, coreIngest2ExpectAccept, coreIngest2ExpectReject)
		}
	}
	for _, name := range triggers {
		if got := coverage[name]; got.accepted == 0 || got.rejected == 0 {
			t.Errorf("trigger %s has %d accepted and %d rejected scenarios, want at least one of each",
				name, got.accepted, got.rejected)
		}
	}
}

func TestPostgresCoreIngest2MigrationKeepsTheSQLiteTriggers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	sqliteDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open SQLite control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteDB.Close(); err != nil {
			t.Errorf("close SQLite control plane: %v", err)
		}
	})
	rows, err := sqliteDB.SQLDB().QueryContext(ctx, `
		SELECT name FROM sqlite_schema
		WHERE type = 'trigger' AND tbl_name IN (
			'index_catalog_state',
			'index_data_deletion_completions',
			'index_deletion_mutation_attempts',
			'ingest_write_group_members'
		)
		ORDER BY name`)
	if err != nil {
		t.Fatalf("list SQLite triggers: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close SQLite trigger rows: %v", err)
		}
	}()
	var sqliteTriggers []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan SQLite trigger: %v", err)
		}
		sqliteTriggers = append(sqliteTriggers, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate SQLite triggers: %v", err)
	}
	postgresTriggers := coreIngest2MigrationTriggers(t)
	sort.Strings(postgresTriggers)
	if !slices.Equal(sqliteTriggers, postgresTriggers) {
		t.Fatalf("SQLite triggers on the lane's tables = %v, PostgreSQL port creates %v", sqliteTriggers, postgresTriggers)
	}

	db := testsupport.NewPostgresControlDB(t)
	applied := make(map[string]bool, len(postgresTriggers))
	triggerRows, err := db.SQLDB().QueryContext(ctx, `
		SELECT tg.tgname::text
		FROM pg_catalog.pg_trigger AS tg
		JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = 'open_splunk' AND NOT tg.tgisinternal AND tg.tgenabled = 'O'
		  AND c.relname IN (
			'index_catalog_state',
			'index_data_deletion_completions',
			'index_deletion_mutation_attempts',
			'ingest_write_group_members'
		  )`)
	if err != nil {
		t.Fatalf("list PostgreSQL triggers: %v", err)
	}
	defer func() {
		if err := triggerRows.Close(); err != nil {
			t.Errorf("close PostgreSQL trigger rows: %v", err)
		}
	}()
	for triggerRows.Next() {
		var name string
		if err := triggerRows.Scan(&name); err != nil {
			t.Fatalf("scan PostgreSQL trigger: %v", err)
		}
		applied[name] = true
	}
	if err := triggerRows.Err(); err != nil {
		t.Fatalf("iterate PostgreSQL triggers: %v", err)
	}
	for _, name := range postgresTriggers {
		if !applied[name] {
			t.Errorf("migrated PostgreSQL database lacks enabled trigger %s", name)
		}
	}
	if len(applied) != len(postgresTriggers) {
		t.Errorf("migrated PostgreSQL database has %d enabled triggers on the lane's tables, want %d",
			len(applied), len(postgresTriggers))
	}
}

// TestPostgresCoreIngest2TriggerScenarios runs every scenario that needs only
// this lane's triggers. Each runs on one connection inside a SERIALIZABLE
// transaction, the isolation the control-plane writers use, that is rolled
// back afterwards, so scenarios cannot see each other's rows or test-only
// DDL. Some setups disable a trigger or set session_replication_role, which
// the superuser test role is allowed to do.
func TestPostgresCoreIngest2TriggerScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadCoreIngest2Scenarios(t)
	db := testsupport.NewPostgresControlDB(t)
	ran := 0
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) != 0 {
			continue
		}
		ran++
		t.Run(scenario.Name, func(t *testing.T) {
			runCoreIngest2PostgresScenario(t, db.SQLDB(), scenario)
		})
	}
	if ran == 0 {
		t.Fatal("no lane-local scenario ran")
	}
}

// TestPostgresCoreIngest2CrossLaneTriggerScenarios runs the scenarios that
// need another lane's PostgreSQL triggers. A scenario whose lanes are all in
// the embedded set must produce its fixture outcome; one whose lanes are not
// yet merged is reported as skipped with the lanes it waits for, never
// passed. Once every lane is merged nothing can skip.
func TestPostgresCoreIngest2CrossLaneTriggerScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadCoreIngest2Scenarios(t)
	shipped := coreIngest2ShippedLanes(t)
	if !shipped[coreIngest2Lane] {
		t.Fatalf("embedded PostgreSQL migrations lack %s", coreIngest2Migration)
	}
	var crossLane []coreIngest2Scenario
	runnable := false
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) == 0 {
			continue
		}
		crossLane = append(crossLane, scenario)
		if coreIngest2MissingLanes(scenario, shipped) == nil {
			runnable = true
		}
	}
	var db *control.PostgresDB
	if runnable {
		db = testsupport.NewPostgresControlDB(t)
	}
	for _, scenario := range crossLane {
		t.Run(scenario.Name, func(t *testing.T) {
			if missing := coreIngest2MissingLanes(scenario, shipped); missing != nil {
				t.Skipf("needs the PostgreSQL triggers of lanes %v, which the embedded migrations do not carry yet", missing)
			}
			runCoreIngest2PostgresScenario(t, db.SQLDB(), scenario)
		})
	}
	t.Logf("%d of %d scenarios depend on other trigger lanes", len(crossLane), len(scenarios))
}

func coreIngest2MissingLanes(scenario coreIngest2Scenario, shipped map[string]bool) []string {
	var missing []string
	for _, lane := range scenario.DependsOnLanes {
		if !shipped[lane] {
			missing = append(missing, lane)
		}
	}
	return missing
}

// TestCoreIngest2TriggerFixtureMatchesSQLite runs the SQLite spelling of
// every scenario against the shipped SQLite schema, which carries all of its
// triggers, and requires the fixture outcome there too. A scenario that
// passes on PostgreSQL but not here would mean the fixture, not the port,
// defines the behaviour.
func TestCoreIngest2TriggerFixtureMatchesSQLite(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	scenarios := loadCoreIngest2Scenarios(t)
	db, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open SQLite control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close SQLite control plane: %v", err)
		}
	})
	for _, scenario := range scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			runCoreIngest2SQLiteScenario(t, db.SQLDB(), scenario)
		})
	}
}

func runCoreIngest2PostgresScenario(t *testing.T, db *sql.DB, scenario coreIngest2Scenario) {
	t.Helper()
	ctx := t.Context()
	runCoreIngest2Scenario(ctx, t, db, scenario, coreIngest2Engine{
		name: "PostgreSQL",
		options: &sql.TxOptions{
			Isolation: sql.LevelSerializable,
		},
		prepare:   []string{`SET LOCAL search_path TO open_splunk`},
		setup:     scenario.SetupSQL,
		statement: scenario.Statement,
		message: func(err error) (string, bool) {
			return control.RaiseMessage(err)
		},
		describe: func(err error) string {
			code, _ := control.PostgresErrorCode(err)
			return fmt.Sprintf("SQLSTATE %q: %v", code, err)
		},
	})
}

func runCoreIngest2SQLiteScenario(t *testing.T, db *sql.DB, scenario coreIngest2Scenario) {
	t.Helper()
	ctx := t.Context()
	runCoreIngest2Scenario(ctx, t, db, scenario, coreIngest2Engine{
		name:      "SQLite",
		setup:     scenario.SQLiteSetupSQL,
		statement: scenario.SQLiteStatement,
		message:   coreIngest2SQLiteRaiseMessage,
		describe: func(err error) string {
			var sqliteErr *sqlite.Error
			if errors.As(err, &sqliteErr) {
				return fmt.Sprintf("SQLite code %d: %v", sqliteErr.Code(), err)
			}
			return err.Error()
		},
	})
}

// coreIngest2SQLiteRaiseMessage returns the RAISE(ABORT, ...) text of a
// SQLite trigger failure, which the driver reports as
// "constraint failed: <text> (1811)".
func coreIngest2SQLiteRaiseMessage(err error) (string, bool) {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code() != coreIngest2SQLiteRaiseCode {
		return "", false
	}
	text := sqliteErr.Error()
	suffix := fmt.Sprintf(" (%d)", coreIngest2SQLiteRaiseCode)
	const prefix = "constraint failed: "
	if !strings.HasPrefix(text, prefix) || !strings.HasSuffix(text, suffix) {
		return "", false
	}
	return text[len(prefix) : len(text)-len(suffix)], true
}

type coreIngest2Engine struct {
	name      string
	options   *sql.TxOptions
	prepare   []string
	setup     []string
	statement string
	message   func(error) (string, bool)
	describe  func(error) string
}

func runCoreIngest2Scenario(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	scenario coreIngest2Scenario,
	engine coreIngest2Engine,
) {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("%s: acquire connection: %v", engine.name, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("%s: release connection: %v", engine.name, err)
		}
	}()
	tx, err := conn.BeginTx(ctx, engine.options)
	if err != nil {
		t.Fatalf("%s: begin scenario transaction: %v", engine.name, err)
	}
	// Every scenario is rolled back, including its test-only DDL.
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("%s: roll back scenario: %v", engine.name, err)
		}
	}()
	for _, statement := range engine.prepare {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %s: %v", engine.name, statement, err)
		}
	}
	for index, statement := range engine.setup {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: setup step %d failed (%s)\n%s", engine.name, index, engine.describe(err), statement)
		}
	}
	_, err = tx.ExecContext(ctx, engine.statement)
	switch scenario.Expect {
	case coreIngest2ExpectAccept:
		if err != nil {
			t.Fatalf("%s rejected a statement trigger %s must accept (%s)\n%s",
				engine.name, scenario.Trigger, engine.describe(err), engine.statement)
		}
		if scenario.PostconditionSQL == "" {
			return
		}
		var holds bool
		if err := tx.QueryRowContext(ctx, scenario.PostconditionSQL).Scan(&holds); err != nil {
			t.Fatalf("%s: postcondition query failed (%s)\n%s", engine.name, engine.describe(err), scenario.PostconditionSQL)
		}
		if !holds {
			t.Fatalf("%s: postcondition is false after the accepted statement\n%s", engine.name, scenario.PostconditionSQL)
		}
	case coreIngest2ExpectReject:
		if err == nil {
			t.Fatalf("%s accepted a statement trigger %s must reject with %q\n%s",
				engine.name, scenario.Trigger, scenario.Message, engine.statement)
		}
		message, raised := engine.message(err)
		if !raised {
			t.Fatalf("%s rejected the statement without a trigger raise (%s), want %q",
				engine.name, engine.describe(err), scenario.Message)
		}
		if message != scenario.Message {
			t.Fatalf("%s trigger message = %q, want %q", engine.name, message, scenario.Message)
		}
	default:
		t.Fatalf("scenario expects %q", scenario.Expect)
	}
}
