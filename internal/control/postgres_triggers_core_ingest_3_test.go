package control_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// The core_ingest_3 trigger lane guards index_deletion_operations,
// index_deletion_tombstones, ingest_visibility_reservations and
// ingest_write_groups. Its scenarios come from the differential fixture,
// which records the outcome SQLite's final schema gives for each one, plus a
// few PostgreSQL-only scenarios below that reach paths SQL alone cannot.
const (
	coreIngest3TriggerMigration = "0009_core_ingest_3_triggers.sql"
	coreIngest3TriggerFixture   = "testdata/postgres_triggers/core_ingest_3.json"

	coreIngest3ExpectAccept = "accept"
	coreIngest3ExpectReject = "reject"
)

// coreIngest3LaneMarkers names, for every other trigger lane a scenario may
// depend on, one trigger that only that lane's migration creates. A lane is
// present when its migration is embedded; the marker proves the migration
// really installed it.
var coreIngest3LaneMarkers = map[string]string{
	// Index catalog counting on indexes: physical_count must reach 1 before
	// index_catalog_before_tombstone_insert accepts a tombstone.
	"core_ingest_1": "index_catalog_after_index_insert",
	// Completion side effects: the completion deletes the operation and its
	// attempts, which index_deletion_operation_delete_is_forbidden must allow.
	"core_ingest_2": "index_data_deletion_completion_finishes_operation",
}

// coreIngest3PendingLanes lists the dependency lanes whose migrations this
// branch does not ship yet; the scenarios needing them wait. When a lane's
// migration is merged, TestPostgresCoreIngest3CrossLaneTriggerScenarios
// fails until the lane is removed here, which enables its scenarios.
var coreIngest3PendingLanes = []string{"core_ingest_1", "core_ingest_2"}

var (
	coreIngest3CreateTrigger = regexp.MustCompile(`(?s)CREATE TRIGGER ([a-z_][a-z0-9_]*)\s+(?:BEFORE|AFTER)\b.*?\bON\s+([a-z_][a-z0-9_]*)`)
	coreIngest3RaiseMessage  = regexp.MustCompile(`RAISE EXCEPTION\s+'([^']*)'`)
	coreIngest3SQLiteOnly    = regexp.MustCompile(`(?i)\bX''|\bINSERT OR\b|\bPRAGMA\b`)
	coreIngest3LineComment   = regexp.MustCompile(`(?m)--.*$`)
)

type coreIngest3TriggerScenario struct {
	Name            string   `json:"name"`
	Trigger         string   `json:"trigger"`
	SetupSQL        []string `json:"setup_sql"`
	Statement       string   `json:"statement"`
	Expect          string   `json:"expect"`
	Message         string   `json:"message"`
	SQLiteSetupSQL  []string `json:"sqlite_setup_sql"`
	SQLiteStatement string   `json:"sqlite_statement"`
	// VerifySQL runs after the statement: after an accepted one it sees the
	// statement's effects, and after a rejected one it runs once the
	// statement has been rolled back to a savepoint, so it checks atomicity.
	VerifySQL      string   `json:"verify_sql"`
	VerifyRows     []string `json:"verify_rows"`
	DependsOnLanes []string `json:"depends_on_lanes"`
}

// TestPostgresCoreIngest3TriggerFixtureCoversTheLane checks the fixture
// itself, without a database: every scenario is well formed and targets a
// trigger the lane creates, every trigger is exercised, every RAISE message
// in the lane is asserted by a rejection, and every dependency is a lane this
// file knows how to detect, so no scenario can be skipped by a typo.
func TestPostgresCoreIngest3TriggerFixtureCoversTheLane(t *testing.T) {
	t.Parallel()

	triggers := coreIngest3LaneTriggers(t)
	messages := coreIngest3LaneMessages(t)
	scenarios := coreIngest3AllScenarios(t)

	names := make(map[string]bool, len(scenarios))
	exercised := make(map[string]int, len(triggers))
	asserted := make(map[string]int, len(messages))
	for _, scenario := range scenarios {
		if names[scenario.Name] {
			t.Fatalf("scenario %q is defined twice", scenario.Name)
		}
		names[scenario.Name] = true
		if _, ok := triggers[scenario.Trigger]; !ok {
			t.Fatalf("scenario %q targets trigger %q, which %s does not create",
				scenario.Name, scenario.Trigger, coreIngest3TriggerMigration)
		}
		exercised[scenario.Trigger]++
		if scenario.Expect == coreIngest3ExpectReject {
			asserted[scenario.Message]++
		}
		for _, lane := range scenario.DependsOnLanes {
			if _, known := coreIngest3LaneMarkers[lane]; !known {
				t.Fatalf("scenario %q depends on unknown lane %q", scenario.Name, lane)
			}
		}
	}
	for trigger := range triggers {
		if exercised[trigger] == 0 {
			t.Errorf("trigger %s has no scenario", trigger)
		}
	}
	for message := range messages {
		if asserted[message] == 0 {
			t.Errorf("RAISE message %q is never asserted by a rejected scenario", message)
		}
	}
}

// TestPostgresCoreIngest3TriggersAreInstalled checks that the shipped
// migrations install exactly the lane's triggers, each on the table its
// CREATE TRIGGER names.
func TestPostgresCoreIngest3TriggersAreInstalled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)

	want := coreIngest3LaneTriggers(t)
	for trigger, table := range want {
		var installedOn string
		err := db.SQLDB().QueryRowContext(ctx, `
			SELECT c.relname
			FROM pg_catalog.pg_trigger AS tg
			JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
			JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND tg.tgname = $2 AND NOT tg.tgisinternal`,
			control.PostgresSchema, trigger,
		).Scan(&installedOn)
		if err != nil {
			t.Fatalf("look up trigger %s: %v", trigger, err)
		}
		if installedOn != table {
			t.Fatalf("trigger %s is on %s, want %s", trigger, installedOn, table)
		}
	}
}

// TestPostgresCoreIngest3TriggerScenarios runs every scenario that needs no
// other trigger lane. Each runs in its own SERIALIZABLE transaction that is
// always rolled back, so the scenarios share one database.
func TestPostgresCoreIngest3TriggerScenarios(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)
	present := coreIngest3PresentLanes(ctx, t, db)
	before := coreIngest3StateSnapshot(ctx, t, db)

	ran := 0
	for _, scenario := range coreIngest3AllScenarios(t) {
		if len(scenario.DependsOnLanes) != 0 {
			continue
		}
		ran++
		t.Run(scenario.Name, func(t *testing.T) {
			runCoreIngest3TriggerScenario(ctx, t, db, scenario)
		})
	}
	if ran == 0 {
		t.Fatal("no lane-independent scenarios ran")
	}
	if after := coreIngest3StateSnapshot(ctx, t, db); !slices.Equal(after, before) {
		t.Fatalf("rolled-back scenarios left state behind: before %v, after %v", before, after)
	}
	t.Logf("ran %d lane-independent scenarios; other lanes present: %v", ran, present)
}

// TestPostgresCoreIngest3CrossLaneTriggerScenarios runs the scenarios whose
// expected outcome also depends on another lane's triggers, as recorded by
// the fixture's depends_on_lanes. A scenario runs once none of its lanes is
// in coreIngest3PendingLanes, and then fails on any difference from the
// SQLite outcome. The pending list must agree with the embedded migrations
// in both directions, so nothing is ever skipped silently: a pending lane
// whose migration has landed fails the test until the integrator enables its
// scenarios, and an enabled lane whose migration is missing fails it too.
func TestPostgresCoreIngest3CrossLaneTriggerScenarios(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)
	present := coreIngest3PresentLanes(ctx, t, db)
	before := coreIngest3StateSnapshot(ctx, t, db)

	scenarios := coreIngest3AllScenarios(t)
	waiting := make(map[string]int, len(coreIngest3PendingLanes))
	for _, scenario := range scenarios {
		for _, lane := range scenario.DependsOnLanes {
			if slices.Contains(coreIngest3PendingLanes, lane) {
				waiting[lane]++
			}
		}
	}
	for _, lane := range coreIngest3PendingLanes {
		if _, known := coreIngest3LaneMarkers[lane]; !known {
			t.Fatalf("pending lane %q is not a dependency lane this test can detect", lane)
		}
		if present[lane] {
			t.Fatalf("trigger lane %s is now embedded, so the %d scenarios that need it can run: "+
				"remove %q from coreIngest3PendingLanes", lane, waiting[lane], lane)
		}
	}
	for lane := range coreIngest3LaneMarkers {
		if !present[lane] && !slices.Contains(coreIngest3PendingLanes, lane) {
			t.Fatalf("trigger lane %s is enabled in this test but its migration is not embedded; "+
				"add it to coreIngest3PendingLanes until it is", lane)
		}
	}

	ran, pending := 0, 0
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) == 0 {
			continue
		}
		if slices.ContainsFunc(scenario.DependsOnLanes, func(lane string) bool {
			return slices.Contains(coreIngest3PendingLanes, lane)
		}) {
			pending++
			continue
		}
		ran++
		t.Run(scenario.Name, func(t *testing.T) {
			runCoreIngest3TriggerScenario(ctx, t, db, scenario)
		})
	}
	if ran+pending == 0 {
		t.Fatal("no scenario depends on another trigger lane; drop the lane markers")
	}
	if after := coreIngest3StateSnapshot(ctx, t, db); !slices.Equal(after, before) {
		t.Fatalf("rolled-back scenarios left state behind: before %v, after %v", before, after)
	}
	t.Logf("ran %d cross-lane scenarios; %d wait for pending lanes %v", ran, pending, coreIngest3PendingLanes)
}

func runCoreIngest3TriggerScenario(
	ctx context.Context,
	t *testing.T,
	db *control.PostgresDB,
	scenario coreIngest3TriggerScenario,
) {
	t.Helper()
	tx, err := db.SQLDB().BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("begin scenario transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("roll back scenario transaction: %v", err)
		}
	}()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path = `+control.PostgresSchema); err != nil {
		t.Fatalf("set scenario search_path: %v", err)
	}
	for index, statement := range scenario.SetupSQL {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("setup statement %d failed: %s\n%s", index+1, describeCoreIngest3Error(err), statement)
		}
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT core_ingest_3_statement`); err != nil {
		t.Fatalf("create statement savepoint: %v", err)
	}

	_, err = tx.ExecContext(ctx, scenario.Statement)
	switch scenario.Expect {
	case coreIngest3ExpectAccept:
		if err != nil {
			t.Fatalf("statement rejected with %s, want it accepted\n%s",
				describeCoreIngest3Error(err), scenario.Statement)
		}
	case coreIngest3ExpectReject:
		if err == nil {
			t.Fatalf("statement accepted, want RAISE %q\n%s", scenario.Message, scenario.Statement)
		}
		message, raised := control.RaiseMessage(err)
		if !raised {
			t.Fatalf("statement failed with %s, want RAISE (P0001) %q\n%s",
				describeCoreIngest3Error(err), scenario.Message, scenario.Statement)
		}
		if message != scenario.Message {
			t.Fatalf("statement raised %q, want %q\n%s", message, scenario.Message, scenario.Statement)
		}
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT core_ingest_3_statement`); err != nil {
			t.Fatalf("roll back rejected statement: %v", err)
		}
	default:
		t.Fatalf("scenario expects %q", scenario.Expect)
	}

	if scenario.VerifySQL == "" {
		return
	}
	got, err := queryCoreIngest3Rows(ctx, tx, scenario.VerifySQL)
	if err != nil {
		t.Fatalf("verify query failed: %s\n%s", describeCoreIngest3Error(err), scenario.VerifySQL)
	}
	if !slices.Equal(got, scenario.VerifyRows) {
		t.Fatalf("verify rows = %q, want %q\n%s", got, scenario.VerifyRows, scenario.VerifySQL)
	}
}

// queryCoreIngest3Rows renders each row as its columns joined by "|", with
// NULL as the empty string, which is how the fixture records them.
func queryCoreIngest3Rows(ctx context.Context, tx *sql.Tx, query string) (result []string, err error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]sql.NullString, len(columns))
	targets := make([]any, len(columns))
	for index := range values {
		targets[index] = &values[index]
	}
	for rows.Next() {
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		fields := make([]string, len(values))
		for index, value := range values {
			fields[index] = value.String
		}
		result = append(result, strings.Join(fields, "|"))
	}
	return result, rows.Err()
}

func describeCoreIngest3Error(err error) string {
	if code, ok := control.PostgresErrorCode(err); ok {
		return fmt.Sprintf("SQLSTATE %s: %v", code, err)
	}
	return err.Error()
}

// coreIngest3PresentLanes reports which dependency lanes are applied. A lane
// whose migration is embedded must have installed its marker trigger, and a
// marker trigger must not appear without its migration: either mismatch
// means the detection itself is stale and fails the test.
func coreIngest3PresentLanes(ctx context.Context, t *testing.T, db *control.PostgresDB) map[string]bool {
	t.Helper()
	files, err := fs.Glob(migrations.Postgres(), "*.sql")
	if err != nil {
		t.Fatalf("list PostgreSQL migrations: %v", err)
	}
	present := make(map[string]bool, len(coreIngest3LaneMarkers))
	for lane, marker := range coreIngest3LaneMarkers {
		pattern := regexp.MustCompile(`^\d{4}_` + regexp.QuoteMeta(lane) + `_triggers\.sql$`)
		embedded := slices.ContainsFunc(files, pattern.MatchString)
		var installed bool
		if err := db.SQLDB().QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_catalog.pg_trigger AS tg
				JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
				JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
				WHERE n.nspname = $1 AND tg.tgname = $2 AND NOT tg.tgisinternal
			)`,
			control.PostgresSchema, marker,
		).Scan(&installed); err != nil {
			t.Fatalf("look up %s marker trigger %s: %v", lane, marker, err)
		}
		if embedded != installed {
			t.Fatalf("lane %s: migration embedded = %v but marker trigger %s installed = %v",
				lane, embedded, marker, installed)
		}
		present[lane] = installed
	}
	return present
}

// coreIngest3StateSnapshot counts the rows every scenario may touch, so a
// test can prove its rolled-back scenarios left nothing behind.
func coreIngest3StateSnapshot(ctx context.Context, t *testing.T, db *control.PostgresDB) []string {
	t.Helper()
	var snapshot []string
	for _, query := range []string{
		`SELECT count(*) FROM tenants`,
		`SELECT count(*) FROM indexes`,
		`SELECT count(*) FROM index_deletion_operations`,
		`SELECT count(*) FROM index_deletion_tombstones`,
		`SELECT count(*) FROM index_data_deletion_completions`,
		`SELECT count(*) FROM ingest_visibility_reservations`,
		`SELECT count(*) FROM ingest_write_groups`,
		`SELECT count(*) FROM hec_requests`,
		`SELECT revision || '/' || physical_count FROM index_catalog_state`,
	} {
		var value string
		if err := db.SQLDB().QueryRowContext(ctx, query).Scan(&value); err != nil {
			t.Fatalf("snapshot %q: %v", query, err)
		}
		snapshot = append(snapshot, value)
	}
	return snapshot
}

// coreIngest3LaneTriggers maps each trigger the lane migration creates to
// its table.
func coreIngest3LaneTriggers(t *testing.T) map[string]string {
	t.Helper()
	contents := coreIngest3LaneMigration(t)
	triggers := make(map[string]string)
	for _, match := range coreIngest3CreateTrigger.FindAllSubmatch(contents, -1) {
		name, table := string(match[1]), string(match[2])
		if _, duplicate := triggers[name]; duplicate {
			t.Fatalf("%s creates trigger %s twice", coreIngest3TriggerMigration, name)
		}
		triggers[name] = table
	}
	if got := bytes.Count(contents, []byte("CREATE TRIGGER ")); got != len(triggers) || got == 0 {
		t.Fatalf("%s has %d CREATE TRIGGER statements, parsed %d", coreIngest3TriggerMigration, got, len(triggers))
	}
	return triggers
}

// coreIngest3LaneMessages returns every message the lane can RAISE.
func coreIngest3LaneMessages(t *testing.T) map[string]bool {
	t.Helper()
	contents := coreIngest3LaneMigration(t)
	messages := make(map[string]bool)
	for _, match := range coreIngest3RaiseMessage.FindAllSubmatch(contents, -1) {
		messages[string(match[1])] = true
	}
	if got := bytes.Count(contents, []byte("RAISE EXCEPTION")); got == 0 ||
		got != len(coreIngest3RaiseMessage.FindAll(contents, -1)) {
		t.Fatalf("%s has %d RAISE EXCEPTION statements that are not all single-quoted literals",
			coreIngest3TriggerMigration, got)
	}
	return messages
}

// coreIngest3LaneMigration returns the lane migration without its line
// comments, which quote SQLite syntax. The file has no "--" inside a string.
func coreIngest3LaneMigration(t *testing.T) []byte {
	t.Helper()
	contents, err := fs.ReadFile(migrations.Postgres(), coreIngest3TriggerMigration)
	if err != nil {
		t.Fatalf("read embedded %s: %v", coreIngest3TriggerMigration, err)
	}
	return coreIngest3LineComment.ReplaceAll(contents, nil)
}

// coreIngest3AllScenarios returns the fixture scenarios followed by the
// PostgreSQL-only ones, each validated.
func coreIngest3AllScenarios(t *testing.T) []coreIngest3TriggerScenario {
	t.Helper()
	fixture := loadCoreIngest3Fixture(t)
	scenarios := append(slices.Clone(fixture), coreIngest3PostgresScenarios(t, fixture)...)
	for _, scenario := range scenarios {
		validateCoreIngest3Scenario(t, scenario)
	}
	return scenarios
}

func loadCoreIngest3Fixture(t *testing.T) []coreIngest3TriggerScenario {
	t.Helper()
	contents, err := os.ReadFile(coreIngest3TriggerFixture)
	if err != nil {
		t.Fatalf("read %s: %v", coreIngest3TriggerFixture, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var scenarios []coreIngest3TriggerScenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode %s: %v", coreIngest3TriggerFixture, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		t.Fatalf("%s holds data after its scenario list", coreIngest3TriggerFixture)
	}
	if len(scenarios) == 0 {
		t.Fatalf("%s has no scenarios", coreIngest3TriggerFixture)
	}
	for _, scenario := range scenarios {
		// The fixture is differential: every scenario also records the SQLite
		// form it was checked against.
		if scenario.SQLiteStatement == "" || scenario.SQLiteSetupSQL == nil {
			t.Fatalf("fixture scenario %q has no SQLite form", scenario.Name)
		}
	}
	return scenarios
}

func validateCoreIngest3Scenario(t *testing.T, scenario coreIngest3TriggerScenario) {
	t.Helper()
	switch {
	case strings.TrimSpace(scenario.Name) == "":
		t.Fatalf("scenario %+v has no name", scenario)
	case strings.TrimSpace(scenario.Trigger) == "":
		t.Fatalf("scenario %q names no trigger", scenario.Name)
	case strings.TrimSpace(scenario.Statement) == "":
		t.Fatalf("scenario %q has no statement", scenario.Name)
	case scenario.Expect == coreIngest3ExpectAccept && scenario.Message != "":
		t.Fatalf("accepted scenario %q carries message %q", scenario.Name, scenario.Message)
	case scenario.Expect == coreIngest3ExpectReject && scenario.Message == "":
		t.Fatalf("rejected scenario %q has no message", scenario.Name)
	case scenario.Expect != coreIngest3ExpectAccept && scenario.Expect != coreIngest3ExpectReject:
		t.Fatalf("scenario %q expects %q", scenario.Name, scenario.Expect)
	case (scenario.VerifySQL == "") != (scenario.VerifyRows == nil):
		t.Fatalf("scenario %q must give verify_sql and verify_rows together", scenario.Name)
	}
	for _, statement := range append(slices.Clone(scenario.SetupSQL), scenario.Statement, scenario.VerifySQL) {
		if coreIngest3SQLiteOnly.MatchString(statement) {
			t.Fatalf("scenario %q runs SQLite-only SQL on PostgreSQL: %s", scenario.Name, statement)
		}
	}
	lanes := slices.Clone(scenario.DependsOnLanes)
	sort.Strings(lanes)
	if len(slices.Compact(lanes)) != len(scenario.DependsOnLanes) {
		t.Fatalf("scenario %q lists a dependency lane twice", scenario.Name)
	}
}

// coreIngest3PostgresScenarios are scenarios with no SQLite form. Some reach
// defensive error paths no plain statement can, by installing a test trigger
// inside the scenario's transaction (PostgreSQL DDL is transactional, so the
// rollback removes it); the others port SQLite store tests that assert these
// triggers' behavior without matching the message, and PostgreSQL-only
// statement forms such as ON CONFLICT. Each derives its setup from a fixture
// scenario so the two cannot drift.
func coreIngest3PostgresScenarios(
	t *testing.T,
	fixture []coreIngest3TriggerScenario,
) []coreIngest3TriggerScenario {
	t.Helper()
	base := func(name string) coreIngest3TriggerScenario {
		t.Helper()
		index := slices.IndexFunc(fixture, func(scenario coreIngest3TriggerScenario) bool {
			return scenario.Name == name
		})
		if index < 0 {
			t.Fatalf("PostgreSQL scenario base %q is not in %s", name, coreIngest3TriggerFixture)
		}
		return fixture[index]
	}
	derive := func(
		from string,
		name string,
		trigger string,
		extraSetup []string,
		statement string,
		expect string,
		message string,
		verifySQL string,
		verifyRows ...string,
	) coreIngest3TriggerScenario {
		t.Helper()
		source := base(from)
		scenario := coreIngest3TriggerScenario{
			Name:           name,
			Trigger:        trigger,
			SetupSQL:       append(slices.Clone(source.SetupSQL), extraSetup...),
			Statement:      statement,
			Expect:         expect,
			Message:        message,
			DependsOnLanes: slices.Clone(source.DependsOnLanes),
			VerifySQL:      verifySQL,
		}
		if verifySQL != "" {
			scenario.VerifyRows = append([]string{}, verifyRows...)
		}
		return scenario
	}

	const (
		operationInsert        = `INSERT INTO index_deletion_operations (deletion_operation_id, index_id, index_name, tenant_id, archived_index_version, created_at_unix_micro) VALUES ('op-1', 'idx-1', 'main1', 'default', 2, 3000)`
		indexAndOperationCount = `SELECT index_id, state, version, updated_at_unix_micro, (SELECT count(*) FROM index_deletion_operations) FROM indexes ORDER BY index_id`
		tombstoneInsert        = `INSERT INTO index_deletion_tombstones (index_id, name, deleted_version, deleted_at_unix_micro) VALUES ('idx-1', 'main1', 2, 2500)`
		tombstoneAccounting    = `SELECT (SELECT count(*) FROM index_deletion_tombstones), revision, physical_count FROM index_catalog_state`
		hecRequestStates       = `SELECT request_id, state, CASE WHEN terminal_at_unix_micro IS NULL THEN 'null' WHEN terminal_at_unix_micro = 5000 THEN '5000' WHEN terminal_at_unix_micro > 1000000000000000 THEN 'clock' ELSE 'other' END FROM hec_requests ORDER BY request_sequence`
	)
	// The test triggers sort before every shipped trigger name, so on
	// PostgreSQL they fire first among the BEFORE triggers of their event.
	suppressDeletingTransition := []string{
		`CREATE FUNCTION core_ingest_3_test_skip_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$`,
		`CREATE TRIGGER aa_core_ingest_3_test_skip_deleting BEFORE UPDATE OF state ON indexes FOR EACH ROW WHEN (NEW.state = 'deleting') EXECUTE FUNCTION core_ingest_3_test_skip_row()`,
	}
	failDeletingTransition := []string{
		`CREATE FUNCTION core_ingest_3_test_fail_deleting() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected deletion transition failure'; END $$`,
		`CREATE TRIGGER core_ingest_3_test_fail_deleting AFTER UPDATE OF state ON indexes FOR EACH ROW WHEN (NEW.state = 'deleting') EXECUTE FUNCTION core_ingest_3_test_fail_deleting()`,
	}
	suppressCatalogUpdate := []string{
		`CREATE FUNCTION core_ingest_3_test_skip_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$`,
		`CREATE TRIGGER aa_core_ingest_3_test_skip_catalog BEFORE UPDATE ON index_catalog_state FOR EACH ROW EXECUTE FUNCTION core_ingest_3_test_skip_row()`,
	}

	return []coreIngest3TriggerScenario{
		// The compare-and-set update of the archived index changes no row, so
		// the operation insert is refused and undone with it.
		derive("operation_insert_on_archived_index_starts_deletion",
			"postgres_operation_insert_whose_transition_changes_no_row_is_rejected",
			"index_deletion_operation_insert_starts_deletion",
			suppressDeletingTransition, operationInsert,
			coreIngest3ExpectReject, "index deletion operation transition failed",
			indexAndOperationCount, "idx-1|archived|2|2000|0"),
		// Port of TestBeginIndexDataDeletionRollsBackWhenTransitionFails: a
		// failure inside the index transition aborts the operation insert.
		derive("operation_insert_on_archived_index_starts_deletion",
			"postgres_operation_insert_rolls_back_when_transition_fails",
			"index_deletion_operation_insert_starts_deletion",
			failDeletingTransition, operationInsert,
			coreIngest3ExpectReject, "injected deletion transition failure",
			indexAndOperationCount, "idx-1|archived|2|2000|0"),
		// The catalog revision bump changes no row, so the tombstone is
		// refused and the catalog is left as it was.
		derive("tombstone_insert_for_archived_index_is_accepted",
			"postgres_tombstone_insert_whose_revision_bump_changes_no_row_is_rejected",
			"index_catalog_after_tombstone_insert",
			suppressCatalogUpdate, tombstoneInsert,
			coreIngest3ExpectReject, "index catalog tombstone accounting failed",
			tombstoneAccounting, "0|3|1"),
		// SQLite's INSERT OR REPLACE / OR IGNORE become ON CONFLICT here; the
		// identity triggers fire before the conflict is examined, so neither
		// form can replace or silently drop a duplicate.
		derive("tombstone_insert_duplicate_is_rejected",
			"postgres_tombstone_upsert_do_update_is_rejected",
			"index_deletion_tombstone_identity_collision_is_forbidden",
			nil, tombstoneInsert+` ON CONFLICT (index_id) DO UPDATE SET deleted_at_unix_micro = EXCLUDED.deleted_at_unix_micro + 1`,
			coreIngest3ExpectReject, "index deletion tombstone identity is already in use",
			`SELECT index_id, deleted_at_unix_micro FROM index_deletion_tombstones ORDER BY index_id`, "idx-1|2500"),
		derive("tombstone_insert_duplicate_is_rejected",
			"postgres_tombstone_upsert_do_nothing_is_rejected",
			"index_deletion_tombstone_identity_collision_is_forbidden",
			nil, tombstoneInsert+` ON CONFLICT DO NOTHING`,
			coreIngest3ExpectReject, "index deletion tombstone identity is already in use",
			`SELECT index_id, deleted_at_unix_micro FROM index_deletion_tombstones ORDER BY index_id`, "idx-1|2500"),
		derive("operation_insert_reusing_operation_id_is_rejected",
			"postgres_operation_upsert_do_nothing_is_rejected",
			"index_deletion_operation_identity_collision_is_forbidden",
			nil, base("operation_insert_reusing_operation_id_is_rejected").Statement+` ON CONFLICT DO NOTHING`,
			coreIngest3ExpectReject, "index deletion operation identity is already in use",
			indexAndOperationCount, "idx-1|deleting|3|3000|1", "idx-2|archived|2|2000|1"),
		derive("operation_insert_reusing_operation_id_is_rejected",
			"postgres_operation_upsert_do_update_is_rejected",
			"index_deletion_operation_identity_collision_is_forbidden",
			nil, base("operation_insert_reusing_operation_id_is_rejected").Statement+
				` ON CONFLICT (deletion_operation_id) DO UPDATE SET index_id = EXCLUDED.index_id`,
			coreIngest3ExpectReject, "index deletion operation identity is already in use",
			indexAndOperationCount, "idx-1|deleting|3|3000|1", "idx-2|archived|2|2000|1"),
		// Ports of the direct-mutation cases in
		// TestIndexDeletionOperationSQLGuards: renaming the
		// operation or moving it to another tenant is refused by the trigger
		// before the tenant foreign key is examined.
		derive("operation_update_is_rejected",
			"postgres_operation_index_name_update_is_rejected",
			"index_deletion_operation_update_is_forbidden",
			nil, `UPDATE index_deletion_operations SET index_name = 'changed' WHERE deletion_operation_id = 'op-1'`,
			coreIngest3ExpectReject, "index deletion operation is immutable",
			`SELECT deletion_operation_id, index_name, tenant_id FROM index_deletion_operations ORDER BY deletion_operation_id`,
			"op-1|main1|default"),
		derive("operation_update_is_rejected",
			"postgres_operation_tenant_update_is_rejected",
			"index_deletion_operation_update_is_forbidden",
			nil, `UPDATE index_deletion_operations SET tenant_id = 'other' WHERE deletion_operation_id = 'op-1'`,
			coreIngest3ExpectReject, "index deletion operation is immutable",
			`SELECT deletion_operation_id, index_name, tenant_id FROM index_deletion_operations ORDER BY deletion_operation_id`,
			"op-1|main1|default"),
		// Port of TestSQLiteWriteGroupCommitTransitionsAllHECAcknowledgments:
		// one statement committing several reservations finishes every
		// pending HEC request bound to them.
		derive("reservation_commit_marks_hec_request_indexed",
			"postgres_multi_row_commit_marks_every_hec_request_indexed",
			"hec_request_visibility_committed",
			nil, `UPDATE ingest_visibility_reservations SET state = 'committed', phase = 'final', outbox = '\x'::bytea, committed_at_unix_micro = 5000 WHERE sequence IN (1, 2)`,
			coreIngest3ExpectAccept, "",
			hecRequestStates, "req-1|indexed|5000", "req-2|indexed|5000"),
		derive("reservation_abandon_marks_hec_request_failed_at_clock_time",
			"postgres_multi_row_abandon_marks_every_hec_request_failed",
			"hec_request_visibility_failed",
			nil, `UPDATE ingest_visibility_reservations SET state = 'abandoned', phase = 'final', outbox = '\x'::bytea WHERE sequence IN (1, 2)`,
			coreIngest3ExpectAccept, "",
			hecRequestStates, "req-1|terminal_failure|clock", "req-2|terminal_failure|clock"),
		// A rejected commit leaves every HEC request pending: the AFTER
		// trigger's update is undone with the statement.
		derive("reservation_commit_marks_hec_request_indexed",
			"postgres_rejected_multi_row_commit_leaves_hec_requests_pending",
			"ingest_visibility_reservation_accounting_update_is_valid",
			nil, `UPDATE ingest_visibility_reservations SET state = 'committed', phase = 'final', outbox = '\x'::bytea, committed_at_unix_micro = 5000, stored_row_count = CASE WHEN sequence = 2 THEN 0 ELSE stored_row_count END WHERE sequence IN (1, 2)`,
			coreIngest3ExpectReject, "invalid ingest visibility reservation accounting",
			hecRequestStates, "req-1|pending|null", "req-2|pending|null"),
	}
}
