package control_test

import (
	"bytes"
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
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// Lane audit_1 ports the SQLite triggers on search_attempt_audit_events,
// feature_operation_audit_events, knowledge_attempt_audit_tenant_state,
// audit_events and api_mutation_receipt_tenant_state. The differential
// fixture holds scenarios that were run against both engines and produced the
// same outcome; these tests replay its PostgreSQL side against the shipped
// migrations.
const (
	audit1TriggerMigration = "0017_audit_1_triggers.sql"
	audit1TriggerFixture   = "testdata/postgres_triggers/audit_1.json"
)

// audit1LaneMigrations names the trigger lanes that fixture scenarios may
// depend on, by the fixed migration file each lane ships. A scenario that
// lists a lane runs only once that file is part of the embedded set.
var audit1LaneMigrations = map[string]string{
	"audit_2": "0018_audit_2_triggers.sql",
}

type audit1TriggerScenario struct {
	Name            string   `json:"name"`
	Trigger         string   `json:"trigger"`
	SetupSQL        []string `json:"setup_sql"`
	Statement       string   `json:"statement"`
	Expect          string   `json:"expect"`
	Message         string   `json:"message"`
	SQLiteSetupSQL  []string `json:"sqlite_setup_sql"`
	SQLiteStatement string   `json:"sqlite_statement"`
	VerifySQL       string   `json:"verify_sql"`
	VerifyRows      []string `json:"verify_rows"`
	DependsOnLanes  []string `json:"depends_on_lanes"`
	Note            string   `json:"note"`
}

func loadAudit1TriggerScenarios(t *testing.T) []audit1TriggerScenario {
	t.Helper()
	contents, err := os.ReadFile(filepath.FromSlash(audit1TriggerFixture))
	if err != nil {
		t.Fatalf("read %s: %v", audit1TriggerFixture, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var scenarios []audit1TriggerScenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode %s: %v", audit1TriggerFixture, err)
	}
	if decoder.More() {
		t.Fatalf("%s has trailing data after the scenario list", audit1TriggerFixture)
	}
	return scenarios
}

var (
	audit1CreateTriggerPattern = regexp.MustCompile(`(?m)^CREATE TRIGGER ([a-z_][a-z0-9_]*)\s`)
	audit1RaisePattern         = regexp.MustCompile(`RAISE EXCEPTION\s+'([^']*)'`)
)

// audit1TriggerSource returns the lane's migration as embedded in the server,
// failing when the lane's file is missing or shipped under another version.
func audit1TriggerSource(t *testing.T) string {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	var lane []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), "_audit_1_triggers.sql") {
			lane = append(lane, entry.Name())
		}
	}
	if !slices.Equal(lane, []string{audit1TriggerMigration}) {
		t.Fatalf("embedded audit_1 trigger migrations = %v, want [%s]", lane, audit1TriggerMigration)
	}
	contents, err := fs.ReadFile(migrations.Postgres(), audit1TriggerMigration)
	if err != nil {
		t.Fatalf("read %s: %v", audit1TriggerMigration, err)
	}
	return string(contents)
}

// audit1MissingLanes reports which of lanes are not yet in the embedded
// migration set. A lane is present only under its fixed file name: a lane
// file at another version, or another file at the lane's version, fails the
// test instead of being mistaken for an absent lane.
func audit1MissingLanes(t *testing.T, lanes []string) []string {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	names := make(map[string]bool, len(entries))
	for _, entry := range entries {
		names[entry.Name()] = true
	}
	var missing []string
	for _, lane := range lanes {
		file, known := audit1LaneMigrations[lane]
		if !known {
			t.Fatalf("fixture depends on unknown trigger lane %q", lane)
		}
		if names[file] {
			continue
		}
		versionPrefix, _, _ := strings.Cut(file, "_")
		for name := range names {
			if strings.HasPrefix(name, versionPrefix+"_") || strings.HasSuffix(name, "_"+lane+"_triggers.sql") {
				t.Fatalf("lane %s must ship as %s, but the embedded set has %s", lane, file, name)
			}
		}
		missing = append(missing, lane)
	}
	return missing
}

// TestPostgresAudit1TriggerFixtureCoversEveryTrigger checks the fixture
// against the migration file itself: every trigger the file creates has an
// accepted and a rejected scenario, and every message it can raise is the
// expected message of some rejected scenario.
func TestPostgresAudit1TriggerFixtureCoversEveryTrigger(t *testing.T) {
	t.Parallel()
	source := audit1TriggerSource(t)
	scenarios := loadAudit1TriggerScenarios(t)

	triggers := make(map[string]bool)
	for _, match := range audit1CreateTriggerPattern.FindAllStringSubmatch(source, -1) {
		if triggers[match[1]] {
			t.Fatalf("%s creates trigger %s twice", audit1TriggerMigration, match[1])
		}
		triggers[match[1]] = true
	}
	if len(triggers) != 25 {
		t.Fatalf("%s creates %d triggers, want the 25 of lane audit_1", audit1TriggerMigration, len(triggers))
	}
	messages := make(map[string]bool)
	for _, match := range audit1RaisePattern.FindAllStringSubmatch(source, -1) {
		messages[match[1]] = true
	}
	if count := strings.Count(source, "RAISE EXCEPTION"); count != len(audit1RaisePattern.FindAllString(source, -1)) {
		t.Fatalf("%s has %d RAISE EXCEPTION statements but only some raise a literal message", audit1TriggerMigration, count)
	}

	names := make(map[string]bool, len(scenarios))
	accepted := make(map[string]int)
	rejected := make(map[string]int)
	raised := make(map[string]bool)
	for _, scenario := range scenarios {
		if scenario.Name == "" || names[scenario.Name] {
			t.Fatalf("scenario name %q is empty or repeated", scenario.Name)
		}
		names[scenario.Name] = true
		if !triggers[scenario.Trigger] {
			t.Errorf("scenario %s targets trigger %q, which %s does not create", scenario.Name, scenario.Trigger, audit1TriggerMigration)
		}
		if strings.TrimSpace(scenario.Statement) == "" {
			t.Errorf("scenario %s has no statement", scenario.Name)
		}
		if (scenario.VerifySQL == "") != (scenario.VerifyRows == nil) {
			t.Errorf("scenario %s must give verify_sql and verify_rows together", scenario.Name)
		}
		switch scenario.Expect {
		case "accept":
			accepted[scenario.Trigger]++
			if scenario.Message != "" {
				t.Errorf("accepted scenario %s carries message %q", scenario.Name, scenario.Message)
			}
		case "reject":
			rejected[scenario.Trigger]++
			if !messages[scenario.Message] {
				t.Errorf("rejected scenario %s expects %q, which %s never raises", scenario.Name, scenario.Message, audit1TriggerMigration)
			}
			raised[scenario.Message] = true
		default:
			t.Errorf("scenario %s expects %q, want accept or reject", scenario.Name, scenario.Expect)
		}
	}
	for trigger := range triggers {
		if accepted[trigger] == 0 || rejected[trigger] == 0 {
			t.Errorf("trigger %s has %d accepted and %d rejected scenarios, want at least one of each",
				trigger, accepted[trigger], rejected[trigger])
		}
	}
	for message := range messages {
		if !raised[message] {
			t.Errorf("no rejected scenario expects %q", message)
		}
	}
}

// TestPostgresAudit1TriggerScenarios replays every fixture scenario that
// needs only this lane and the table migrations.
func TestPostgresAudit1TriggerScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadAudit1TriggerScenarios(t)
	db := newAudit1TriggerDB(t)
	ran := 0
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) > 0 {
			continue
		}
		ran++
		t.Run(scenario.Name, func(t *testing.T) {
			runAudit1TriggerScenario(t, db, scenario)
		})
	}
	if ran == 0 {
		t.Fatal("the fixture has no single-lane scenarios")
	}
}

// TestPostgresAudit1TriggerScenariosAcrossLanes replays the fixture
// scenarios whose outcome also depends on another trigger lane, such as the
// knowledge-attempt event and receipt accounting of lane audit_2. A scenario
// is skipped, by name, only while a lane it lists is absent from the embedded
// migrations. Once the lane is merged the scenario runs and any difference
// fails the test.
func TestPostgresAudit1TriggerScenariosAcrossLanes(t *testing.T) {
	t.Parallel()
	scenarios := loadAudit1TriggerScenarios(t)
	var dependent []audit1TriggerScenario
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) > 0 {
			dependent = append(dependent, scenario)
		}
	}
	if len(dependent) == 0 {
		t.Fatal("the fixture has no cross-lane scenarios; drop this test if that is intended")
	}
	missing := make(map[string][]string, len(dependent))
	var db *control.PostgresDB
	for _, scenario := range dependent {
		missing[scenario.Name] = audit1MissingLanes(t, scenario.DependsOnLanes)
		if len(missing[scenario.Name]) == 0 && db == nil {
			db = newAudit1TriggerDB(t)
		}
	}
	for _, scenario := range dependent {
		t.Run(scenario.Name, func(t *testing.T) {
			if lanes := missing[scenario.Name]; len(lanes) > 0 {
				t.Skipf("needs trigger lanes %v, which are not in migrations/postgres yet", lanes)
			}
			runAudit1TriggerScenario(t, db, scenario)
		})
	}
}

// TestPostgresAudit1TriggerStoreScenarios covers trigger behaviour the
// SQLite store tests rely on but the differential fixture does not
// exercise: ON CONFLICT DO UPDATE as PostgreSQL's counterpart of the
// INSERT OR REPLACE attacks, the sequence ceiling, the receipt store's
// high-water statement, and the documented one-event-per-INSERT rule.
func TestPostgresAudit1TriggerStoreScenarios(t *testing.T) {
	t.Parallel()
	db := newAudit1TriggerDB(t)
	for _, scenario := range audit1StoreScenarios() {
		t.Run(scenario.Name, func(t *testing.T) {
			runAudit1TriggerScenario(t, db, scenario)
		})
	}
}

func newAudit1TriggerDB(t *testing.T) *control.PostgresDB {
	t.Helper()
	source := audit1TriggerSource(t)
	db := testsupport.NewPostgresControlDB(t)
	var want []string
	for _, match := range audit1CreateTriggerPattern.FindAllStringSubmatch(source, -1) {
		want = append(want, match[1])
	}
	sort.Strings(want)
	rows, err := db.SQLDB().QueryContext(t.Context(), `
		SELECT tg.tgname
		FROM pg_catalog.pg_trigger AS tg
		JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND NOT tg.tgisinternal AND tg.tgenabled = 'O'
		  AND tg.tgname = ANY ($2)
		ORDER BY tg.tgname COLLATE "C"`, control.PostgresSchema, want)
	if err != nil {
		t.Fatalf("list audit_1 triggers: %v", err)
	}
	got := scanAudit1Strings(t, rows)
	if !slices.Equal(got, want) {
		t.Fatalf("enabled audit_1 triggers in the migrated database = %v, want %v", got, want)
	}
	return db
}

// runAudit1TriggerScenario runs setup and statement in one transaction that
// is always rolled back, so scenarios share a database without seeing each
// other's rows or dropped triggers.
func runAudit1TriggerScenario(t *testing.T, db *control.PostgresDB, scenario audit1TriggerScenario) {
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
			t.Fatalf("setup statement %d failed: %v\n%s", index+1, err, statement)
		}
	}
	_, err = tx.ExecContext(ctx, scenario.Statement)
	switch scenario.Expect {
	case "accept":
		if err != nil {
			t.Fatalf("statement was rejected, want accepted: %v\n%s", describeAudit1Error(err), scenario.Statement)
		}
		if scenario.VerifySQL == "" {
			return
		}
		rows, err := tx.QueryContext(ctx, scenario.VerifySQL)
		if err != nil {
			t.Fatalf("verify query failed: %v\n%s", err, scenario.VerifySQL)
		}
		got := scanAudit1Rows(t, rows)
		if !slices.Equal(got, scenario.VerifyRows) {
			t.Fatalf("verify rows = %q, want %q\n%s", got, scenario.VerifyRows, scenario.VerifySQL)
		}
	case "reject":
		if err == nil {
			t.Fatalf("statement was accepted, want rejected with %q\n%s", scenario.Message, scenario.Statement)
		}
		message, raised := control.RaiseMessage(err)
		if !raised || message != scenario.Message {
			t.Fatalf("statement failed with %s, want RAISE %q", describeAudit1Error(err), scenario.Message)
		}
	default:
		t.Fatalf("scenario expects %q, want accept or reject", scenario.Expect)
	}
}

func describeAudit1Error(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Sprintf("SQLSTATE %s %q (%s)", pgErr.Code, pgErr.Message, pgErr.Where)
	}
	return err.Error()
}

func scanAudit1Strings(t *testing.T, rows *sql.Rows) []string {
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

// scanAudit1Rows renders rows the way the fixture records them, which is the
// sqlite3 shell's list mode: columns joined by '|', NULL as an empty string.
func scanAudit1Rows(t *testing.T, rows *sql.Rows) []string {
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

const (
	audit1TenantsSQL = `INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
		VALUES ('t1', 1, 1), ('t2', 1, 1)`
	audit1SearchEventColumns = `INSERT INTO search_attempt_audit_events (
		tenant_id, sequence, occurred_at_unix_micro, actor_kind, actor_id, actor_role, owner_id,
		search_job_id, knowledge_snapshot_sha256, knowledge_snapshot_tenant_catalog_revision,
		knowledge_snapshot_tenant_catalog_state_token, knowledge_snapshot_object_count,
		knowledge_snapshot_lookup_asset_count) VALUES `
	audit1FeatureEventColumns = `INSERT INTO feature_operation_audit_events (
		tenant_id, sequence, occurred_at_unix_micro, feature, operation, outcome, items, bytes) VALUES `
	audit1AuditEventColumns = `INSERT INTO audit_events (
		tenant_id, sequence, occurred_at_unix_micro, actor_kind, actor_id, actor_role, action,
		target_kind, target_id, target_version, app_id, object_type, sharing_scope) VALUES `
	audit1KnowledgeStateColumns = `INSERT INTO knowledge_attempt_audit_tenant_state (
		tenant_id, first_sequence, next_sequence, retained_count) VALUES `
)

func audit1SearchEvent(sequence int64, job string) string {
	return fmt.Sprintf(`('t1', %d, %d, 'browser', 'alice', 'user', 'alice', '%s', NULL, NULL, NULL, NULL, NULL)`,
		sequence, 1000+sequence%1000, job)
}

func audit1AuditEvent(sequence int64, targetVersion int64) string {
	return fmt.Sprintf(`('t1', %d, %d, 'system', 'system', 'system', 'index.create', 'index', 'idx-1', %d, NULL, NULL, NULL)`,
		sequence, 1000+sequence, targetVersion)
}

// audit1StoreScenarios are PostgreSQL-only scenarios. Each note names the
// SQLite test whose behaviour it carries over.
func audit1StoreScenarios() []audit1TriggerScenario {
	const maxSequence = "9223372036854775807"
	return []audit1TriggerScenario{
		{
			Name:    "knowledge_state_on_conflict_update_with_new_shape_is_rejected",
			Trigger: "knowledge_attempt_audit_state_initial_shape_is_valid",
			SetupSQL: []string{
				audit1TenantsSQL,
				audit1KnowledgeStateColumns + `('t1', 1, 1, 0)`,
			},
			Statement: audit1KnowledgeStateColumns + `('t1', 1, 2, 1)
				ON CONFLICT (tenant_id) DO UPDATE
				SET next_sequence = EXCLUDED.next_sequence, retained_count = EXCLUDED.retained_count`,
			Expect:  "reject",
			Message: "knowledge-attempt audit tenant state must begin empty",
			Note:    "knowledgeattemptaudit TestORReplaceCannotBypassImmutabilityWithRecursiveTriggersOffAndOn: INSERT OR REPLACE of the state row.",
		},
		{
			Name:    "knowledge_state_on_conflict_update_with_empty_shape_is_rejected",
			Trigger: "knowledge_attempt_audit_state_identity_collision_is_forbidden",
			SetupSQL: []string{
				audit1TenantsSQL,
				audit1KnowledgeStateColumns + `('t1', 1, 1, 0)`,
			},
			Statement: audit1KnowledgeStateColumns + `('t1', 1, 1, 0)
				ON CONFLICT (tenant_id) DO UPDATE SET retained_count = EXCLUDED.retained_count`,
			Expect:  "reject",
			Message: "knowledge-attempt audit tenant state already exists",
			Note:    "BEFORE INSERT triggers run before ON CONFLICT DO UPDATE, so the update arm is never reached.",
		},
		{
			Name:    "knowledge_state_transition_at_sequence_ceiling_is_rejected_without_overflow",
			Trigger: "knowledge_attempt_audit_state_transition_is_valid",
			SetupSQL: []string{
				audit1TenantsSQL,
				audit1KnowledgeStateColumns + `('t1', 1, 1, 0)`,
				`ALTER TABLE knowledge_attempt_audit_tenant_state DISABLE TRIGGER USER`,
				`UPDATE knowledge_attempt_audit_tenant_state
					SET first_sequence = ` + maxSequence + `, next_sequence = ` + maxSequence + `
					WHERE tenant_id = 't1'`,
				`ALTER TABLE knowledge_attempt_audit_tenant_state ENABLE TRIGGER USER`,
			},
			Statement: `UPDATE knowledge_attempt_audit_tenant_state
				SET retained_count = retained_count WHERE tenant_id = 't1'`,
			Expect:  "reject",
			Message: "knowledge-attempt audit tenant state transition is invalid",
			Note:    "knowledgeattemptaudit TestSequenceExhaustionFailsClosedWithoutWrapping: an exhausted state fails with the trigger message, not numeric overflow 22003.",
		},
		{
			Name:    "search_append_at_sequence_ceiling_is_rejected",
			Trigger: "search_attempt_audit_event_insert_requires_current_state",
			SetupSQL: []string{
				audit1TenantsSQL,
				`ALTER TABLE search_attempt_audit_tenant_state DISABLE TRIGGER USER`,
				`INSERT INTO search_attempt_audit_tenant_state (
					tenant_id, first_sequence, next_sequence, retained_count, maximum_retained_attempts)
					VALUES ('t1', ` + maxSequence + `, ` + maxSequence + `, 0, 2)`,
				`ALTER TABLE search_attempt_audit_tenant_state ENABLE TRIGGER USER`,
			},
			Statement: audit1SearchEventColumns + `('t1', ` + maxSequence + `, 1001, 'browser', 'alice', 'user', 'alice', 'job-1', NULL, NULL, NULL, NULL, NULL)`,
			Expect:    "reject",
			Message:   "search-attempt audit tenant state is invalid or sequence is exhausted",
			Note:      "The last bigint sequence is never appended, so next_sequence + 1 cannot overflow.",
		},
		{
			Name:    "feature_append_at_sequence_ceiling_is_rejected",
			Trigger: "feature_operation_audit_event_insert_requires_current_state",
			SetupSQL: []string{
				audit1TenantsSQL,
				`ALTER TABLE feature_operation_audit_tenant_state DISABLE TRIGGER USER`,
				`INSERT INTO feature_operation_audit_tenant_state (
					tenant_id, first_sequence, next_sequence, retained_count, last_occurred_at_unix_micro)
					VALUES ('t1', ` + maxSequence + `, ` + maxSequence + `, 0, NULL)`,
				`ALTER TABLE feature_operation_audit_tenant_state ENABLE TRIGGER USER`,
			},
			Statement: audit1FeatureEventColumns + `('t1', ` + maxSequence + `, 1001, 1, 2, 3, 4, 5)`,
			Expect:    "reject",
			Message:   "feature-operation audit tenant state is invalid or sequence is exhausted",
			Note:      "The last bigint sequence is never appended, so next_sequence + 1 cannot overflow.",
		},
		{
			Name:    "search_event_on_conflict_update_is_rejected",
			Trigger: "search_attempt_audit_event_insert_requires_current_state",
			SetupSQL: []string{
				audit1TenantsSQL,
				`INSERT INTO search_attempt_audit_tenant_state (
					tenant_id, first_sequence, next_sequence, retained_count, maximum_retained_attempts)
					VALUES ('t1', 1, 1, 0, 2)`,
				audit1SearchEventColumns + audit1SearchEvent(1, "job-1"),
			},
			Statement: audit1SearchEventColumns + audit1SearchEvent(1, "job-forged") + `
				ON CONFLICT (tenant_id, sequence) DO UPDATE SET search_job_id = EXCLUDED.search_job_id`,
			Expect:  "reject",
			Message: "search-attempt audit tenant state is invalid or sequence is exhausted",
			Note:    "searchaudit TestStartupIntegrityRejectsGapAndForgedRows forges rows only after dropping the update guard; an upsert cannot.",
		},
		{
			Name:    "audit_event_on_conflict_update_is_rejected",
			Trigger: "audit_event_insert_requires_current_tenant_state",
			SetupSQL: []string{
				audit1TenantsSQL,
				`INSERT INTO audit_tenant_state (tenant_id, next_sequence, event_count) VALUES ('t1', 1, 0)`,
				audit1AuditEventColumns + audit1AuditEvent(1, 1),
			},
			Statement: audit1AuditEventColumns + audit1AuditEvent(1, 2) + `
				ON CONFLICT (tenant_id, sequence) DO UPDATE SET target_version = EXCLUDED.target_version`,
			Expect:  "reject",
			Message: "audit tenant state is invalid or capacity is exhausted",
			Note:    "audit TestAuditRowsAndStateAreImmutable: INSERT OR REPLACE of a committed event.",
		},
		{
			Name:    "audit_multi_row_insert_is_rejected_at_second_row",
			Trigger: "audit_event_insert_requires_current_tenant_state",
			SetupSQL: []string{
				audit1TenantsSQL,
				`INSERT INTO audit_tenant_state (tenant_id, next_sequence, event_count) VALUES ('t1', 1, 0)`,
			},
			Statement: audit1AuditEventColumns + audit1AuditEvent(1, 1) + `, ` + audit1AuditEvent(2, 1),
			Expect:    "reject",
			Message:   "audit tenant state is invalid or capacity is exhausted",
			Note:      "Documented divergence: PostgreSQL defers AFTER ROW accounting to the end of the statement, so the stores append one event per INSERT.",
		},
		{
			Name:    "search_multi_row_insert_is_rejected_at_second_row",
			Trigger: "search_attempt_audit_event_insert_requires_current_state",
			SetupSQL: []string{
				audit1TenantsSQL,
				`INSERT INTO search_attempt_audit_tenant_state (
					tenant_id, first_sequence, next_sequence, retained_count, maximum_retained_attempts)
					VALUES ('t1', 1, 1, 0, 2)`,
			},
			Statement: audit1SearchEventColumns + audit1SearchEvent(1, "job-1") + `, ` + audit1SearchEvent(2, "job-2"),
			Expect:    "reject",
			Message:   "search-attempt audit tenant state is invalid or sequence is exhausted",
			Note:      "Documented divergence: PostgreSQL defers AFTER ROW accounting to the end of the statement, so the stores append one event per INSERT.",
		},
		{
			Name:    "audit_sequential_inserts_in_one_transaction_are_accepted",
			Trigger: "audit_event_advances_tenant_state",
			SetupSQL: []string{
				audit1TenantsSQL,
				`INSERT INTO audit_tenant_state (tenant_id, next_sequence, event_count) VALUES ('t1', 1, 0)`,
				audit1AuditEventColumns + audit1AuditEvent(1, 1),
				audit1AuditEventColumns + audit1AuditEvent(2, 1),
			},
			Statement:  audit1AuditEventColumns + audit1AuditEvent(3, 1),
			Expect:     "accept",
			VerifySQL:  `SELECT next_sequence, event_count FROM audit_tenant_state WHERE tenant_id = 't1'`,
			VerifyRows: []string{"4|3"},
			Note:       "audit TestConcurrentAppendAllocatesDenseUniqueTenantSequences: one event per statement keeps the journal dense.",
		},
		{
			Name:    "receipt_state_greatest_high_water_behind_clock_is_accepted",
			Trigger: "api_mutation_receipt_tenant_state_transition_is_valid",
			SetupSQL: []string{
				audit1TenantsSQL,
				`INSERT INTO api_mutation_receipt_tenant_state (
					tenant_id, receipt_count, encoded_bytes, high_water_unix_micro)
					VALUES ('t1', 0, 0, 1000) ON CONFLICT (tenant_id) DO NOTHING`,
			},
			Statement: `UPDATE api_mutation_receipt_tenant_state
				SET high_water_unix_micro = GREATEST(high_water_unix_micro, 500)
				WHERE tenant_id = 't1'`,
			Expect:     "accept",
			VerifySQL:  `SELECT receipt_count, encoded_bytes, high_water_unix_micro FROM api_mutation_receipt_tenant_state WHERE tenant_id = 't1'`,
			VerifyRows: []string{"0|0|1000"},
			Note:       "requestidempotency receipt.go: the PostgreSQL form of MAX(high_water_unix_micro, ?) when the clock is behind the high water.",
		},
	}
}
