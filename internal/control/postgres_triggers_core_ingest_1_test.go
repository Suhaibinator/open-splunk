package control_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// The core_ingest_1 trigger lane ports the SQLite triggers on indexes,
// ingestion_tokens and ingestion_token_hec_profiles to
// migrations/postgres/0007_core_ingest_1_triggers.sql, which also holds four
// tenant-guard triggers with no SQLite counterpart (two on
// ingestion_token_indexes, one per event).
// Its scenarios live in testdata/postgres_triggers/core_ingest_1.json; each
// ported one was checked against SQLite when the lane was ported, and these
// tests hold PostgreSQL to them.

const coreIngest1FixturePath = "testdata/postgres_triggers/core_ingest_1.json"

// coreIngest1Tables are the tables whose triggers this lane owns. Trigger
// lanes partition tables, so every user trigger on them belongs here.
var coreIngest1Tables = []string{"indexes", "ingestion_token_hec_profiles", "ingestion_token_indexes", "ingestion_tokens"}

// coreIngest1KnownLanes are the trigger lanes a scenario may depend on. A
// lane is present when the embedded migrations hold NNNN_<lane>_triggers.sql.
var coreIngest1KnownLanes = []string{
	"audit_1",
	"audit_2",
	"collectors_apps_search_1",
	"collectors_apps_search_2",
	"core_ingest_2",
	"core_ingest_3",
	"knowledge_catalog_1",
	"knowledge_catalog_2",
	"knowledge_objects_1",
	"knowledge_objects_2",
	"lookups",
}

// coreIngest1Scenario is one fixture entry. setup_sql runs inside the
// scenario transaction with search_path = open_splunk, then statement runs
// and must be accepted or rejected as expect says. A rejection is a P0001
// RAISE with exactly message, unless sqlstate names another SQLSTATE (with
// constraint naming the violated constraint). An accepted statement is
// followed by check_sql, whose single text value must equal check_result.
// Everything is rolled back afterwards.
//
// depends_on_lanes lists trigger lanes the outcome needs installed; those
// scenarios run in TestPostgresCoreIngest1CrossLaneTriggerScenarios. The
// sqlite_* fields record the SQLite side of the differential check (setup
// that drops triggers instead of disabling them, and the deliberate
// differences) and are not used here.
type coreIngest1Scenario struct {
	Name           string   `json:"name"`
	Trigger        string   `json:"trigger"`
	SetupSQL       []string `json:"setup_sql"`
	Statement      string   `json:"statement"`
	Expect         string   `json:"expect"`
	Message        string   `json:"message"`
	SQLState       string   `json:"sqlstate,omitempty"`
	Constraint     string   `json:"constraint,omitempty"`
	CheckSQL       string   `json:"check_sql,omitempty"`
	CheckResult    string   `json:"check_result,omitempty"`
	DependsOnLanes []string `json:"depends_on_lanes,omitempty"`
	SQLiteSetupSQL []string `json:"sqlite_setup_sql,omitempty"`
	SQLiteExpect   string   `json:"sqlite_expect,omitempty"`
	SQLiteMessage  string   `json:"sqlite_message,omitempty"`
	Note           string   `json:"note,omitempty"`
}

func loadCoreIngest1Scenarios(t *testing.T) []coreIngest1Scenario {
	t.Helper()
	contents, err := os.ReadFile(coreIngest1FixturePath)
	if err != nil {
		t.Fatalf("read %s: %v", coreIngest1FixturePath, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var scenarios []coreIngest1Scenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode %s: %v", coreIngest1FixturePath, err)
	}
	if decoder.More() {
		t.Fatalf("%s holds more than one JSON value", coreIngest1FixturePath)
	}
	return scenarios
}

// validateCoreIngest1Scenario reports a malformed scenario, so a typo in the
// fixture fails instead of quietly asserting less.
func validateCoreIngest1Scenario(scenario coreIngest1Scenario) error {
	var problems []string
	if strings.TrimSpace(scenario.Name) == "" {
		problems = append(problems, "no name")
	}
	if strings.TrimSpace(scenario.Statement) == "" {
		problems = append(problems, "no statement")
	}
	switch scenario.Expect {
	case "accept":
		if scenario.Message != "" || scenario.SQLState != "" || scenario.Constraint != "" {
			problems = append(problems, "an accepted statement carries a rejection message, SQLSTATE or constraint")
		}
	case "reject":
		if scenario.CheckSQL != "" {
			problems = append(problems, "a rejected statement has check_sql")
		}
		if scenario.SQLState == "" && scenario.Message == "" {
			problems = append(problems, "a rejected RAISE has no message")
		}
		if scenario.SQLState != "" && scenario.Constraint == "" {
			problems = append(problems, "a non-RAISE rejection names no constraint")
		}
	default:
		problems = append(problems, "expect is "+scenario.Expect+", want accept or reject")
	}
	if (scenario.CheckSQL == "") != (scenario.CheckResult == "") {
		problems = append(problems, "check_sql and check_result must be given together")
	}
	for _, lane := range scenario.DependsOnLanes {
		if !slices.Contains(coreIngest1KnownLanes, lane) {
			problems = append(problems, "depends on unknown lane "+lane)
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(problems, "; "))
}

// coreIngest1PresentLanes returns the trigger lanes whose migration file is
// embedded in migrationFS.
func coreIngest1PresentLanes(t *testing.T, migrationFS fs.FS) map[string]bool {
	t.Helper()
	names, err := fs.Glob(migrationFS, "*.sql")
	if err != nil {
		t.Fatalf("list PostgreSQL migrations: %v", err)
	}
	present := make(map[string]bool)
	pattern := regexp.MustCompile(`^\d{4}_([a-z0-9_]+)_triggers\.sql$`)
	for _, name := range names {
		if matches := pattern.FindStringSubmatch(name); matches != nil {
			present[matches[1]] = true
		}
	}
	return present
}

// runCoreIngest1Scenario executes one scenario in its own transaction on a
// dedicated connection and always rolls it back.
func runCoreIngest1Scenario(ctx context.Context, t *testing.T, db *sql.DB, scenario coreIngest1Scenario) {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire connection: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("release connection: %v", err)
		}
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin scenario transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("roll back scenario: %v", err)
		}
	}()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO `+control.PostgresSchema); err != nil {
		t.Fatalf("pin scenario search_path: %v", err)
	}
	for index, statement := range scenario.SetupSQL {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("setup statement %d failed: %v\n%s", index+1, err, statement)
		}
	}
	_, statementErr := tx.ExecContext(ctx, scenario.Statement)
	if scenario.Expect == "accept" {
		if statementErr != nil {
			t.Fatalf("statement was rejected, want accepted: %v\n%s", statementErr, scenario.Statement)
		}
		if scenario.CheckSQL == "" {
			return
		}
		var got sql.NullString
		if err := tx.QueryRowContext(ctx, scenario.CheckSQL).Scan(&got); err != nil {
			t.Fatalf("check query failed: %v\n%s", err, scenario.CheckSQL)
		}
		if !got.Valid || got.String != scenario.CheckResult {
			t.Fatalf("check %s = %q (valid %t), want %q", scenario.CheckSQL, got.String, got.Valid, scenario.CheckResult)
		}
		return
	}
	if statementErr == nil {
		t.Fatalf("statement was accepted, want rejected with %q\n%s", scenario.Message, scenario.Statement)
	}
	if scenario.SQLState != "" {
		code, _ := control.PostgresErrorCode(statementErr)
		constraint, _ := control.PostgresConstraintName(statementErr)
		if code != scenario.SQLState || constraint != scenario.Constraint {
			t.Fatalf("rejection = SQLSTATE %q constraint %q (%v), want SQLSTATE %q constraint %q",
				code, constraint, statementErr, scenario.SQLState, scenario.Constraint)
		}
		return
	}
	message, raised := control.RaiseMessage(statementErr)
	if !raised {
		t.Fatalf("rejection is not a P0001 RAISE: %v; want %q", statementErr, scenario.Message)
	}
	if message != scenario.Message {
		t.Fatalf("RAISE message = %q, want %q", message, scenario.Message)
	}
}

func runCoreIngest1Scenarios(t *testing.T, scenarios []coreIngest1Scenario) {
	t.Helper()
	db := testsupport.NewPostgresControlDB(t)
	names := make(map[string]bool, len(scenarios))
	for _, scenario := range scenarios {
		if err := validateCoreIngest1Scenario(scenario); err != nil {
			t.Fatalf("scenario %q is malformed: %v", scenario.Name, err)
		}
		if names[scenario.Name] {
			t.Fatalf("scenario name %q is used twice", scenario.Name)
		}
		names[scenario.Name] = true
		// The scenarios share one database and run one after another:
		// several disable triggers, which takes an exclusive table lock.
		t.Run(scenario.Name, func(t *testing.T) {
			runCoreIngest1Scenario(t.Context(), t, db.SQLDB(), scenario)
		})
	}
}

// TestPostgresCoreIngest1TriggerScenarios runs every fixture scenario that
// needs no other trigger lane.
func TestPostgresCoreIngest1TriggerScenarios(t *testing.T) {
	t.Parallel()
	var standalone []coreIngest1Scenario
	for _, scenario := range loadCoreIngest1Scenarios(t) {
		if len(scenario.DependsOnLanes) == 0 {
			standalone = append(standalone, scenario)
		}
	}
	if len(standalone) == 0 {
		t.Fatal("the fixture holds no standalone scenarios")
	}
	runCoreIngest1Scenarios(t, standalone)
}

// TestPostgresCoreIngest1CrossLaneTriggerScenarios runs the scenarios whose
// outcome needs another lane's triggers, such as the core_ingest_3 trigger
// that moves an index to 'deleting' when its deletion operation is inserted.
// A scenario runs as soon as every lane it names is embedded, and then any
// difference fails the test. The test never skips: until a lane lands, its
// scenarios are listed in the test log, by name, with the missing lanes, and
// the scenarios whose lanes are all present still run.
func TestPostgresCoreIngest1CrossLaneTriggerScenarios(t *testing.T) {
	t.Parallel()
	present := coreIngest1PresentLanes(t, migrations.Postgres())
	if !present["core_ingest_1"] {
		t.Fatal("the embedded PostgreSQL migrations hold no NNNN_core_ingest_1_triggers.sql")
	}
	var runnable []coreIngest1Scenario
	var waiting []string
	for _, scenario := range loadCoreIngest1Scenarios(t) {
		if len(scenario.DependsOnLanes) == 0 {
			continue
		}
		var missing []string
		for _, lane := range scenario.DependsOnLanes {
			if !present[lane] {
				missing = append(missing, lane)
			}
		}
		if len(missing) > 0 {
			waiting = append(waiting, scenario.Name+" (needs "+strings.Join(missing, ", ")+")")
			continue
		}
		runnable = append(runnable, scenario)
	}
	if len(runnable) == 0 && len(waiting) == 0 {
		t.Fatal("the fixture holds no cross-lane scenarios")
	}
	if len(runnable) > 0 {
		runCoreIngest1Scenarios(t, runnable)
	}
	if len(waiting) > 0 {
		t.Logf("%d cross-lane scenarios wait for trigger lanes that are not embedded yet: %s",
			len(waiting), strings.Join(waiting, "; "))
	}
}

// TestPostgresCoreIngest1TriggerCatalogMatchesFixture checks that the
// database holds exactly the lane's triggers, enabled, with functions pinned
// to the control schema, and that the fixture accepts and rejects at least
// one statement for each of them.
func TestPostgresCoreIngest1TriggerCatalogMatchesFixture(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)
	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT trigger.tgname, trigger.tgenabled::text, coalesce(array_to_string(function.proconfig, ','), '')
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		JOIN pg_catalog.pg_proc AS function ON function.oid = trigger.tgfoid
		WHERE namespace.nspname = $1
		  AND class.relname = ANY ($2)
		  AND NOT trigger.tgisinternal
		ORDER BY trigger.tgname`,
		control.PostgresSchema, coreIngest1Tables)
	if err != nil {
		t.Fatalf("list lane triggers: %v", err)
	}
	var installed []string
	for rows.Next() {
		var name, enabled, config string
		if err := rows.Scan(&name, &enabled, &config); err != nil {
			t.Fatalf("scan lane trigger: %v", err)
		}
		if enabled != "O" {
			t.Errorf("trigger %s is enabled as %q, want O (origin and local)", name, enabled)
		}
		if config != "search_path="+control.PostgresSearchPath {
			t.Errorf("trigger %s function config = %q, want search_path=%s", name, config, control.PostgresSearchPath)
		}
		installed = append(installed, name)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("read lane triggers: %v", err)
	}
	// 29 ported from SQLite plus the 4 tenant-guard triggers.
	const wantTriggers = 33
	if len(installed) != wantTriggers {
		t.Fatalf("lane tables carry %d user triggers, want %d: %v", len(installed), wantTriggers, installed)
	}

	accepted := make(map[string]int)
	rejected := make(map[string]int)
	for _, scenario := range loadCoreIngest1Scenarios(t) {
		if !slices.Contains(installed, scenario.Trigger) {
			t.Errorf("scenario %q targets trigger %q, which is not installed", scenario.Name, scenario.Trigger)
		}
		if scenario.Expect == "accept" {
			accepted[scenario.Trigger]++
		} else {
			rejected[scenario.Trigger]++
		}
	}
	sort.Strings(installed)
	for _, name := range installed {
		if accepted[name] == 0 || rejected[name] == 0 {
			t.Errorf("trigger %s has %d accepted and %d rejected scenarios, want at least one of each",
				name, accepted[name], rejected[name])
		}
	}
}

// TestPostgresCoreIngest1TriggersIgnoreTheSessionSearchPath fires the lane's
// cross-table triggers from a session whose search_path is public and holds
// decoy tables with the same names. The functions pin search_path to the
// control schema, so they must read and write the open_splunk tables only.
func TestPostgresCoreIngest1TriggersIgnoreTheSessionSearchPath(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)
	tx, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("roll back: %v", err)
		}
	}()
	exec := func(statement string) {
		t.Helper()
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	exec(`SET LOCAL search_path TO public`)
	exec(`CREATE TABLE public.index_catalog_state (singleton_id bigint, revision bigint, physical_count bigint)`)
	exec(`INSERT INTO public.index_catalog_state VALUES (1, 500, 0)`)
	exec(`CREATE TABLE public.index_deletion_tombstones (index_id text)`)
	exec(`INSERT INTO public.index_deletion_tombstones VALUES ('idx_main')`)
	exec(`INSERT INTO open_splunk.tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
		VALUES ('default', 1, 1)`)
	exec(`INSERT INTO open_splunk.indexes (index_id, tenant_id, version, name, display_name, description,
			retention_nanoseconds, ingestion_enabled, search_enabled, default_sourcetype, state,
			created_at_unix_micro, updated_at_unix_micro)
		VALUES ('idx_main', 'default', 1, 'main', 'Main', '', 0, 1, 1, '', 'active', 1000, 1000)`)
	// A decoy tombstone must not freeze the real index.
	exec(`UPDATE open_splunk.indexes SET description = 'changed' WHERE index_id = 'idx_main'`)

	var controlState, decoy string
	if err := tx.QueryRowContext(ctx, `
		SELECT revision || ',' || physical_count FROM open_splunk.index_catalog_state WHERE singleton_id = 1`).
		Scan(&controlState); err != nil {
		t.Fatalf("read control catalog state: %v", err)
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT revision || ',' || physical_count FROM public.index_catalog_state WHERE singleton_id = 1`).
		Scan(&decoy); err != nil {
		t.Fatalf("read decoy catalog state: %v", err)
	}
	if controlState != "3,1" || decoy != "500,0" {
		t.Fatalf("catalog state after insert and update = %q (decoy %q), want 3,1 (decoy untouched 500,0)", controlState, decoy)
	}

	// A real tombstone does freeze it, through the pinned search_path.
	exec(`SAVEPOINT tombstone`)
	exec(`UPDATE open_splunk.indexes SET state = 'archived' WHERE index_id = 'idx_main'`)
	exec(`ALTER TABLE open_splunk.index_deletion_tombstones DISABLE TRIGGER USER`)
	exec(`INSERT INTO open_splunk.index_deletion_tombstones (index_id, name, deleted_version, deleted_at_unix_micro)
		VALUES ('idx_main', 'main', 1, 3000)`)
	exec(`ALTER TABLE open_splunk.index_deletion_tombstones ENABLE TRIGGER USER`)
	_, err = tx.ExecContext(ctx, `UPDATE open_splunk.indexes SET description = 'again' WHERE index_id = 'idx_main'`)
	if message, raised := control.RaiseMessage(err); !raised || message != "tombstoned index is immutable" {
		t.Fatalf("update of a tombstoned index under search_path=public = %v, want RAISE %q",
			err, "tombstoned index is immutable")
	}
	exec(`ROLLBACK TO SAVEPOINT tombstone`)
}

// coreIngest1SQLiteParityScenarios port what the SQLite store tests rely on
// these tables to do when written directly, for the cases the fixture does
// not already cover. Each names the SQLite test it mirrors.
var coreIngest1SQLiteParityScenarios = []coreIngest1Scenario{
	{
		// internal/auth/hec_token_test.go: "delete revoked HEC tombstone with
		// cascaded profile". No trigger guards token deletion, and the
		// profile and its default-index scope cascade away with the token.
		Name:    "revoked_hec_token_delete_cascades_profile",
		Trigger: "ingestion_token_hec_profile_requires_hec_purpose",
		SetupSQL: []string{
			coreIngest1TenantSQL,
			coreIngest1ActiveIndexSQL,
			coreIngest1HECTokenSQL("revoked", "2000"),
			`INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id) VALUES ('tok_hec', 'idx_main')`,
			`INSERT INTO ingestion_token_hec_profiles (ingestion_token_id, default_index_id, indexer_acknowledgment)
				VALUES ('tok_hec', 'idx_main', 1)`,
		},
		Statement: `DELETE FROM ingestion_tokens WHERE ingestion_token_id = 'tok_hec'`,
		Expect:    "accept",
		CheckSQL: `SELECT (SELECT count(*) FROM ingestion_token_hec_profiles) || ',' ||
			(SELECT count(*) FROM ingestion_token_indexes) || ',' || (SELECT count(*) FROM ingestion_tokens)`,
		CheckResult: "0,0,0",
	},
	{
		// internal/auth/hec_token_test.go: "remove HEC profile for corruption
		// test". Deleting a profile directly is not guarded by a trigger.
		Name:    "hec_profile_direct_delete_accepted",
		Trigger: "ingestion_token_hec_profile_owner_is_immutable",
		SetupSQL: []string{
			coreIngest1TenantSQL,
			coreIngest1HECTokenSQL("active", "NULL"),
			`INSERT INTO ingestion_token_hec_profiles (ingestion_token_id, indexer_acknowledgment) VALUES ('tok_hec', 0)`,
		},
		Statement:   `DELETE FROM ingestion_token_hec_profiles WHERE ingestion_token_id = 'tok_hec'`,
		Expect:      "accept",
		CheckSQL:    `SELECT count(*)::text FROM ingestion_token_hec_profiles`,
		CheckResult: "0",
	},
	{
		// internal/auth/hec_token_test.go: direct "out-of-scope default"
		// mutation. The composite foreign key refuses it, not a trigger.
		Name:    "hec_profile_out_of_scope_default_index_rejected",
		Trigger: "ingestion_token_hec_acknowledgment_is_immutable",
		SetupSQL: []string{
			coreIngest1TenantSQL,
			coreIngest1HECTokenSQL("active", "NULL"),
			`INSERT INTO ingestion_token_hec_profiles (ingestion_token_id, indexer_acknowledgment) VALUES ('tok_hec', 1)`,
		},
		Statement: `UPDATE ingestion_token_hec_profiles SET default_index_id = 'missing-index'
			WHERE ingestion_token_id = 'tok_hec'`,
		Expect:     "reject",
		SQLState:   "23503",
		Constraint: "ingestion_token_hec_profiles_default_index_membership",
	},
	{
		// internal/visibility/hec_acknowledgment_test.go moves a token's
		// expiry directly; no immutability trigger covers that column.
		Name:    "token_expiry_update_accepted",
		Trigger: "revoked_ingestion_token_is_irreversible",
		SetupSQL: []string{
			coreIngest1TenantSQL,
			coreIngest1HECTokenSQL("active", "NULL"),
		},
		Statement:   `UPDATE ingestion_tokens SET expires_at_unix_micro = 5000 WHERE ingestion_token_id = 'tok_hec'`,
		Expect:      "accept",
		CheckSQL:    `SELECT expires_at_unix_micro::text FROM ingestion_tokens WHERE ingestion_token_id = 'tok_hec'`,
		CheckResult: "5000",
	},
	{
		// internal/auth/hec_token_test.go disables a HEC token directly.
		Name:    "active_token_disable_accepted",
		Trigger: "revoked_ingestion_token_is_irreversible",
		SetupSQL: []string{
			coreIngest1TenantSQL,
			coreIngest1HECTokenSQL("active", "NULL"),
		},
		Statement:   `UPDATE ingestion_tokens SET state = 'disabled' WHERE ingestion_token_id = 'tok_hec'`,
		Expect:      "accept",
		CheckSQL:    `SELECT state FROM ingestion_tokens WHERE ingestion_token_id = 'tok_hec'`,
		CheckResult: "disabled",
	},
	{
		// internal/auth/index_policy_authentication_test.go "default
		// sourcetype" drops index_catalog_record_update_is_bounded to plant
		// a padded sourcetype, so the trigger must refuse it.
		Name:    "index_update_padded_default_sourcetype_rejected",
		Trigger: "index_catalog_record_update_is_bounded",
		SetupSQL: []string{
			coreIngest1TenantSQL,
			coreIngest1ActiveIndexSQL,
		},
		Statement: `UPDATE indexes SET default_sourcetype = ' surrounding-space ' WHERE name = 'main'`,
		Expect:    "reject",
		Message:   "index catalog record is invalid or unbounded",
	},
	{
		// internal/auth/index_policy_authentication_test.go "unaligned
		// retention" drops indexes_retention_is_millisecond_aligned_on_update
		// to plant 1ms + 1ns, so the trigger must refuse it.
		Name:    "index_update_millisecond_plus_nanosecond_retention_rejected",
		Trigger: "indexes_retention_is_millisecond_aligned_on_update",
		SetupSQL: []string{
			coreIngest1TenantSQL,
			coreIngest1ActiveIndexSQL,
		},
		Statement: `UPDATE indexes SET retention_nanoseconds = 1000001 WHERE name = 'main'`,
		Expect:    "reject",
		Message:   "index retention must use whole milliseconds",
	},
	{
		// internal/auth/index_policy_authentication_test.go updates policy
		// columns of an index directly; the catalog revision still advances.
		Name:    "index_policy_update_accepted_and_counted",
		Trigger: "index_catalog_after_index_update",
		SetupSQL: []string{
			coreIngest1TenantSQL,
			coreIngest1ActiveIndexSQL,
		},
		Statement:   `UPDATE indexes SET max_event_bytes = 1024, retention_nanoseconds = 86400000000000 WHERE name = 'main'`,
		Expect:      "accept",
		CheckSQL:    `SELECT revision || ',' || physical_count FROM index_catalog_state WHERE singleton_id = 1`,
		CheckResult: "3,1",
	},
}

const coreIngest1TenantSQL = `INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
	VALUES ('default', 1, 1)`

const coreIngest1ActiveIndexSQL = `INSERT INTO indexes (index_id, tenant_id, version, name, display_name,
		description, retention_nanoseconds, ingestion_enabled, search_enabled, default_sourcetype, state,
		created_at_unix_micro, updated_at_unix_micro)
	VALUES ('idx_main', 'default', 1, 'main', 'Main', '', 0, 1, 1, '', 'active', 1000, 1000)`

// coreIngest1HECTokenSQL inserts the unbound HEC token tok_hec in state;
// revokedAt is a SQL literal (NULL unless state is revoked).
func coreIngest1HECTokenSQL(state, revokedAt string) string {
	return `INSERT INTO ingestion_tokens (ingestion_token_id, tenant_id, version, name, token_prefix,
			token_digest, state, created_at_unix_micro, updated_at_unix_micro, revoked_at_unix_micro,
			bound_collector_id, purpose)
		VALUES ('tok_hec', 'default', 1, 'Token tok_hec', 'osk_abcd1234',
			decode('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'hex'), '` + state + `',
			1000, ` + coreIngest1RevisionTime(revokedAt) + `, ` + revokedAt + `, NULL, 'hec')`
}

// coreIngest1RevisionTime keeps updated_at at or after revoked_at.
func coreIngest1RevisionTime(revokedAt string) string {
	if revokedAt == "NULL" {
		return "1000"
	}
	return revokedAt
}

// TestPostgresCoreIngest1TriggersMatchSQLiteStoreTests runs the SQLite
// parity scenarios above.
func TestPostgresCoreIngest1TriggersMatchSQLiteStoreTests(t *testing.T) {
	t.Parallel()
	runCoreIngest1Scenarios(t, coreIngest1SQLiteParityScenarios)
}
