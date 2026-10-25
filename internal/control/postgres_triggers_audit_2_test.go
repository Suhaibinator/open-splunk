package control_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// Lane audit_2 ports the SQLite triggers on api_mutation_receipts,
// audit_tenant_state, feature_operation_audit_tenant_state,
// knowledge_attempt_audit_events and search_attempt_audit_tenant_state. The
// differential fixture holds scenarios that were run against both engines
// and produced the same outcome; these tests replay its PostgreSQL side
// against the embedded migrations.
const (
	audit2TriggerMigration = "0018_audit_2_triggers.sql"
	audit2TriggerFixture   = "testdata/postgres_triggers/audit_2.json"
	audit2TriggerCount     = 22
)

// audit2CrossLaneScenariosEnabled turns on the fixture scenarios that list
// depends_on_lanes. They need the companion triggers of lane audit_1 (the
// event tables and the knowledge-attempt and receipt tenant state), so they
// cannot run until that lane is merged. Whoever merges it sets this to true;
// until then TestPostgresAudit2TriggerScenariosAcrossLanes fails as soon as
// the lane's migration is present, so the scenarios are never left off by
// accident.
const audit2CrossLaneScenariosEnabled = true

// audit2LaneMigrations names the trigger lanes that fixture scenarios may
// depend on, by the fixed migration file each lane ships.
var audit2LaneMigrations = map[string]string{
	"audit_1": "0017_audit_1_triggers.sql",
}

type audit2TriggerScenario struct {
	Name            string   `json:"name"`
	Trigger         string   `json:"trigger"`
	SetupSQL        []string `json:"setup_sql"`
	Statement       string   `json:"statement"`
	Expect          string   `json:"expect"`
	Message         string   `json:"message"`
	CheckSQL        string   `json:"check_sql"`
	CheckResult     *string  `json:"check_result"`
	DependsOnLanes  []string `json:"depends_on_lanes"`
	Note            string   `json:"note"`
	SQLiteSetupSQL  []string `json:"sqlite_setup_sql"`
	SQLiteStatement string   `json:"sqlite_statement"`
	SQLiteExpect    string   `json:"sqlite_expect"`
	SQLiteMessage   string   `json:"sqlite_message"`
}

func loadAudit2TriggerScenarios(t *testing.T) []audit2TriggerScenario {
	t.Helper()
	contents, err := os.ReadFile(filepath.FromSlash(audit2TriggerFixture))
	if err != nil {
		t.Fatalf("read %s: %v", audit2TriggerFixture, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var scenarios []audit2TriggerScenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode %s: %v", audit2TriggerFixture, err)
	}
	if decoder.More() {
		t.Fatalf("%s has trailing data after the scenario list", audit2TriggerFixture)
	}
	if len(scenarios) == 0 {
		t.Fatalf("%s has no scenarios", audit2TriggerFixture)
	}
	return scenarios
}

var (
	audit2CreateTriggerPattern = regexp.MustCompile(`(?m)^CREATE TRIGGER ([a-z_][a-z0-9_]*)\s`)
	audit2RaisePattern         = regexp.MustCompile(`RAISE EXCEPTION\s+'([^']*)'`)
	audit2MigrationVersion     = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)
)

// audit2TriggerSource returns the lane's migration as embedded in the
// server, failing when the file is missing or shipped under another version.
func audit2TriggerSource(t *testing.T) string {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	var lane []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), "_audit_2_triggers.sql") || strings.HasPrefix(entry.Name(), "0018_") {
			lane = append(lane, entry.Name())
		}
	}
	if !slices.Equal(lane, []string{audit2TriggerMigration}) {
		t.Fatalf("embedded audit_2 trigger migrations = %v, want exactly [%s]", lane, audit2TriggerMigration)
	}
	contents, err := fs.ReadFile(migrations.Postgres(), audit2TriggerMigration)
	if err != nil {
		t.Fatalf("read %s: %v", audit2TriggerMigration, err)
	}
	return string(contents)
}

func audit2TriggerNames(t *testing.T, source string) []string {
	t.Helper()
	var names []string
	for _, match := range audit2CreateTriggerPattern.FindAllStringSubmatch(source, -1) {
		if slices.Contains(names, match[1]) {
			t.Fatalf("%s creates trigger %s twice", audit2TriggerMigration, match[1])
		}
		names = append(names, match[1])
	}
	if len(names) != audit2TriggerCount {
		t.Fatalf("%s creates %d triggers, want the %d of lane audit_2", audit2TriggerMigration, len(names), audit2TriggerCount)
	}
	sort.Strings(names)
	return names
}

// audit2RaisedMessages returns every message the lane's functions raise.
// Each RAISE must carry a literal, so a rejected scenario can match it
// exactly.
func audit2RaisedMessages(t *testing.T, source string) map[string]bool {
	t.Helper()
	matches := audit2RaisePattern.FindAllStringSubmatch(source, -1)
	if count := strings.Count(source, "RAISE EXCEPTION"); count != len(matches) {
		t.Fatalf("%s has %d RAISE EXCEPTION statements but %d raise a literal message", audit2TriggerMigration, count, len(matches))
	}
	messages := make(map[string]bool, len(matches))
	for _, match := range matches {
		messages[match[1]] = true
	}
	return messages
}

// audit2MissingLanes reports which of lanes are not yet in the embedded
// migration set. A lane counts as present only under its fixed file name: a
// lane file at another version, or another file at the lane's version,
// fails the test instead of being mistaken for an absent lane.
func audit2MissingLanes(t *testing.T, lanes []string) []string {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	var missing []string
	for _, lane := range lanes {
		file, known := audit2LaneMigrations[lane]
		if !known {
			t.Fatalf("fixture depends on unknown trigger lane %q", lane)
		}
		version, _, _ := strings.Cut(file, "_")
		present := false
		for _, entry := range entries {
			name := entry.Name()
			switch {
			case name == file:
				present = true
			case strings.HasPrefix(name, version+"_") || strings.HasSuffix(name, "_"+lane+"_triggers.sql"):
				t.Fatalf("lane %s must ship as %s, but the embedded set has %s", lane, file, name)
			}
		}
		if !present {
			missing = append(missing, lane)
		}
	}
	return missing
}

// TestPostgresAudit2TriggerFixtureCoversEveryTrigger checks the fixture
// against the migration file: every trigger the file creates has an
// accepted and a rejected scenario that needs no other lane, every message
// it can raise is the expected message of such a rejected scenario, and
// every scenario is well formed.
func TestPostgresAudit2TriggerFixtureCoversEveryTrigger(t *testing.T) {
	t.Parallel()
	source := audit2TriggerSource(t)
	triggers := audit2TriggerNames(t, source)
	messages := audit2RaisedMessages(t, source)
	scenarios := loadAudit2TriggerScenarios(t)

	names := make(map[string]bool, len(scenarios))
	accepted := make(map[string]int)
	rejected := make(map[string]int)
	raised := make(map[string]bool)
	crossLane := 0
	for _, scenario := range scenarios {
		if scenario.Name == "" || names[scenario.Name] {
			t.Fatalf("scenario name %q is empty or repeated", scenario.Name)
		}
		names[scenario.Name] = true
		if !slices.Contains(triggers, scenario.Trigger) {
			t.Errorf("scenario %s targets trigger %q, which %s does not create", scenario.Name, scenario.Trigger, audit2TriggerMigration)
		}
		if strings.TrimSpace(scenario.Statement) == "" {
			t.Errorf("scenario %s has no statement", scenario.Name)
		}
		if (scenario.CheckSQL == "") != (scenario.CheckResult == nil) {
			t.Errorf("scenario %s must give check_sql and check_result together", scenario.Name)
		}
		if scenario.SQLiteExpect != "" && scenario.SQLiteExpect != "accept" && scenario.SQLiteExpect != "reject" {
			t.Errorf("scenario %s has sqlite_expect %q, want accept or reject", scenario.Name, scenario.SQLiteExpect)
		}
		if len(scenario.DependsOnLanes) > 0 {
			crossLane++
			// Resolving the lanes validates their names and versions.
			audit2MissingLanes(t, scenario.DependsOnLanes)
		}
		switch scenario.Expect {
		case "accept":
			if scenario.Message != "" {
				t.Errorf("accepted scenario %s carries message %q", scenario.Name, scenario.Message)
			}
			if len(scenario.DependsOnLanes) == 0 {
				accepted[scenario.Trigger]++
			}
		case "reject":
			if scenario.Message == "" {
				t.Errorf("rejected scenario %s has no expected message", scenario.Name)
			}
			if scenario.CheckSQL != "" {
				t.Errorf("rejected scenario %s has a check_sql, which a failed statement cannot reach", scenario.Name)
			}
			if len(scenario.DependsOnLanes) == 0 {
				rejected[scenario.Trigger]++
				raised[scenario.Message] = true
			}
		default:
			t.Errorf("scenario %s expects %q, want accept or reject", scenario.Name, scenario.Expect)
		}
	}
	for _, trigger := range triggers {
		if accepted[trigger] == 0 || rejected[trigger] == 0 {
			t.Errorf("trigger %s has %d accepted and %d rejected single-lane scenarios, want at least one of each",
				trigger, accepted[trigger], rejected[trigger])
		}
	}
	for message := range messages {
		if !raised[message] {
			t.Errorf("no single-lane rejected scenario expects %q", message)
		}
	}
	if crossLane == 0 {
		t.Error("the fixture has no cross-lane scenarios; drop audit2CrossLaneScenariosEnabled if that is intended")
	}
}

// TestPostgresAudit2TriggerScenarios replays every fixture scenario that
// needs only this lane and the table migrations.
func TestPostgresAudit2TriggerScenarios(t *testing.T) {
	t.Parallel()
	source := audit2TriggerSource(t)
	messages := audit2RaisedMessages(t, source)
	scenarios := loadAudit2TriggerScenarios(t)
	db := newAudit2TriggerDB(t, source)
	ran := 0
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) > 0 {
			continue
		}
		ran++
		t.Run(scenario.Name, func(t *testing.T) {
			runAudit2TriggerScenario(t, db, messages, scenario)
		})
	}
	if ran == 0 {
		t.Fatal("the fixture has no single-lane scenarios")
	}
}

// TestPostgresAudit2TriggerScenariosAcrossLanes replays the fixture
// scenarios whose outcome also depends on another trigger lane: the
// rolling windows, receipt accounting and audit appends whose state
// transitions cross between audit_1 and audit_2. It never skips. While
// audit2CrossLaneScenariosEnabled is false it fails once every lane the
// scenarios need is embedded, so the merge that brings the lane in must
// turn them on; once enabled, a missing lane or any difference fails.
func TestPostgresAudit2TriggerScenariosAcrossLanes(t *testing.T) {
	t.Parallel()
	source := audit2TriggerSource(t)
	messages := audit2RaisedMessages(t, source)
	var dependent []audit2TriggerScenario
	for _, scenario := range loadAudit2TriggerScenarios(t) {
		if len(scenario.DependsOnLanes) > 0 {
			dependent = append(dependent, scenario)
		}
	}
	if len(dependent) == 0 {
		t.Fatal("the fixture has no cross-lane scenarios")
	}
	var pending []string
	for _, scenario := range dependent {
		missing := audit2MissingLanes(t, scenario.DependsOnLanes)
		switch {
		case audit2CrossLaneScenariosEnabled && len(missing) > 0:
			t.Errorf("scenario %s needs trigger lanes %v, which are not in migrations/postgres", scenario.Name, missing)
		case !audit2CrossLaneScenariosEnabled && len(missing) == 0:
			t.Errorf("scenario %s can run now that lanes %v are merged; set audit2CrossLaneScenariosEnabled = true",
				scenario.Name, scenario.DependsOnLanes)
		case !audit2CrossLaneScenariosEnabled:
			pending = append(pending, scenario.Name)
		}
	}
	if t.Failed() {
		return
	}
	if !audit2CrossLaneScenariosEnabled {
		t.Logf("%d cross-lane scenarios wait for lanes %v: %v", len(pending), slices.Sorted(maps.Keys(audit2LaneMigrations)), pending)
		return
	}
	db := newAudit2TriggerDB(t, source)
	for _, scenario := range dependent {
		t.Run(scenario.Name, func(t *testing.T) {
			runAudit2TriggerScenario(t, db, messages, scenario)
		})
	}
}

// TestPostgresAudit2TriggerScenariosRollBack checks the harness property the
// fixture relies on: a scenario's rows and its ALTER TABLE ... DISABLE
// TRIGGER setup do not outlive its transaction.
func TestPostgresAudit2TriggerScenariosRollBack(t *testing.T) {
	t.Parallel()
	source := audit2TriggerSource(t)
	messages := audit2RaisedMessages(t, source)
	db := newAudit2TriggerDB(t, source)
	runAudit2TriggerScenario(t, db, messages, audit2TriggerScenario{
		Name:    "disable_then_write",
		Trigger: "audit_tenant_state_delete_is_forbidden",
		SetupSQL: []string{
			`INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro) VALUES ('t1', 1, 1)`,
			`INSERT INTO audit_tenant_state (tenant_id, next_sequence, event_count) VALUES ('t1', 1, 0)`,
			`ALTER TABLE audit_tenant_state DISABLE TRIGGER USER`,
		},
		Statement: `DELETE FROM audit_tenant_state WHERE tenant_id = 't1'`,
		Expect:    "accept",
	})
	assertAudit2TriggersEnabled(t, db, source)
	var tenants int
	if err := db.SQLDB().QueryRowContext(t.Context(),
		`SELECT count(*) FROM `+control.PostgresSchema+`.tenants`,
	).Scan(&tenants); err != nil {
		t.Fatalf("count tenants: %v", err)
	}
	if tenants != 0 {
		t.Fatalf("a rolled-back scenario left %d tenants behind", tenants)
	}
	runAudit2TriggerScenario(t, db, messages, audit2TriggerScenario{
		Name:    "guard_is_back",
		Trigger: "audit_tenant_state_delete_is_forbidden",
		SetupSQL: []string{
			`INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro) VALUES ('t1', 1, 1)`,
			`INSERT INTO audit_tenant_state (tenant_id, next_sequence, event_count) VALUES ('t1', 1, 0)`,
		},
		Statement: `DELETE FROM audit_tenant_state WHERE tenant_id = 't1'`,
		Expect:    "reject",
		Message:   "audit tenant state cannot be deleted",
	})
}

// audit2MigrationSet returns the embedded PostgreSQL migrations, or, while
// other trigger lanes are not merged yet, the same set with each missing
// version below audit_2's filled by a no-op file so the runner's contiguity
// rule holds. Every real file is used unchanged. Once all lanes are merged
// the set has no gaps and the embedded migrations are used as they ship.
func audit2MigrationSet(t *testing.T) (fs.FS, bool) {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	present := make(map[int]bool)
	highest := 0
	for _, entry := range entries {
		matches := audit2MigrationVersion.FindStringSubmatch(entry.Name())
		if matches == nil {
			continue
		}
		version, err := strconv.Atoi(matches[1])
		if err != nil {
			t.Fatalf("parse migration version of %s: %v", entry.Name(), err)
		}
		present[version] = true
		highest = max(highest, version)
	}
	if !present[18] {
		t.Fatalf("embedded PostgreSQL migrations lack version 0018 (%s)", audit2TriggerMigration)
	}
	gaps := 0
	for version := 1; version <= highest; version++ {
		if !present[version] {
			gaps++
		}
	}
	if gaps == 0 {
		return migrations.Postgres(), true
	}
	set := fstest.MapFS{}
	for _, entry := range entries {
		contents, err := fs.ReadFile(migrations.Postgres(), entry.Name())
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		set[entry.Name()] = &fstest.MapFile{Data: contents}
	}
	for version := 1; version <= highest; version++ {
		if !present[version] {
			if version <= 6 {
				t.Fatalf("table migration %04d is missing from the embedded set", version)
			}
			set[fmt.Sprintf("%04d_absent_trigger_lane.sql", version)] = &fstest.MapFile{
				Data: []byte("-- Stands in for a trigger lane that is not merged on this branch.\nSELECT 1;\n"),
			}
		}
	}
	return set, false
}

func newAudit2TriggerDB(t *testing.T, source string) *control.PostgresDB {
	t.Helper()
	set, shipped := audit2MigrationSet(t)
	var db *control.PostgresDB
	if shipped {
		db = testsupport.NewPostgresControlDB(t)
	} else {
		db = testsupport.NewPostgresControlDBWithMigrations(t, set)
	}
	assertAudit2TriggersEnabled(t, db, source)
	return db
}

func assertAudit2TriggersEnabled(t *testing.T, db *control.PostgresDB, source string) {
	t.Helper()
	want := audit2TriggerNames(t, source)
	rows, err := db.SQLDB().QueryContext(t.Context(), `
		SELECT tg.tgname
		FROM pg_catalog.pg_trigger AS tg
		JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND NOT tg.tgisinternal AND tg.tgenabled = 'O'
		  AND tg.tgname = ANY ($2)
		ORDER BY tg.tgname COLLATE "C"`, control.PostgresSchema, want)
	if err != nil {
		t.Fatalf("list audit_2 triggers: %v", err)
	}
	got := scanAudit2Strings(t, rows)
	if !slices.Equal(got, want) {
		t.Fatalf("enabled audit_2 triggers in the migrated database = %v, want %v", got, want)
	}
}

// runAudit2TriggerScenario runs setup and statement in one transaction that
// is always rolled back, so scenarios share a database without seeing each
// other's rows or disabled triggers.
func runAudit2TriggerScenario(
	t *testing.T,
	db *control.PostgresDB,
	raised map[string]bool,
	scenario audit2TriggerScenario,
) {
	t.Helper()
	ctx := t.Context()
	tx, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("roll back scenario %s: %v", scenario.Name, err)
		}
	}()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path = `+control.PostgresSchema); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	for index, statement := range scenario.SetupSQL {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("setup statement %d failed: %s\n%s", index+1, describeAudit2Error(err), statement)
		}
	}
	_, err = tx.ExecContext(ctx, scenario.Statement)
	switch scenario.Expect {
	case "accept":
		if err != nil {
			t.Fatalf("statement was rejected, want accepted: %s\n%s", describeAudit2Error(err), scenario.Statement)
		}
		if scenario.CheckResult == nil {
			return
		}
		rows, err := tx.QueryContext(ctx, scenario.CheckSQL)
		if err != nil {
			t.Fatalf("check query failed: %s\n%s", describeAudit2Error(err), scenario.CheckSQL)
		}
		if got := strings.Join(scanAudit2Rows(t, rows), ";"); got != *scenario.CheckResult {
			t.Fatalf("check result = %q, want %q\n%s", got, *scenario.CheckResult, scenario.CheckSQL)
		}
	case "reject":
		if err == nil {
			t.Fatalf("statement was accepted, want rejected with %q\n%s", scenario.Message, scenario.Statement)
		}
		pgErr, ok := errors.AsType[*pgconn.PgError](err)
		if !ok {
			t.Fatalf("statement failed outside PostgreSQL: %v", err)
		}
		if pgErr.Message != scenario.Message {
			t.Fatalf("statement failed with %s, want %q", describeAudit2Error(err), scenario.Message)
		}
		// A message this lane raises must come from the RAISE itself, not
		// from an engine error that happens to read the same.
		if message, isRaise := control.RaiseMessage(err); raised[scenario.Message] && (!isRaise || message != scenario.Message) {
			t.Fatalf("statement failed with %s, want RAISE %q (SQLSTATE P0001)", describeAudit2Error(err), scenario.Message)
		}
	default:
		t.Fatalf("scenario expects %q, want accept or reject", scenario.Expect)
	}
}

func describeAudit2Error(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return fmt.Sprintf("SQLSTATE %s %q (%s)", pgErr.Code, pgErr.Message, pgErr.Where)
	}
	return err.Error()
}

func scanAudit2Strings(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	defer func() { _ = rows.Close() }()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan: %v", err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return values
}

// scanAudit2Rows renders rows the way the fixture records them, which is the
// sqlite3 shell's list mode: columns joined by '|', NULL as an empty string.
func scanAudit2Rows(t *testing.T, rows *sql.Rows) []string {
	t.Helper()
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	rendered := []string{}
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		fields := make([]string, len(values))
		for index, value := range values {
			switch typed := value.(type) {
			case nil:
				fields[index] = ""
			case int64:
				fields[index] = strconv.FormatInt(typed, 10)
			case string:
				fields[index] = typed
			case []byte:
				fields[index] = string(typed)
			default:
				t.Fatalf("column %s has unsupported type %T", columns[index], value)
			}
		}
		rendered = append(rendered, strings.Join(fields, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return rendered
}
