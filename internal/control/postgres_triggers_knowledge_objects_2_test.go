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
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// The knowledge_objects_2 trigger lane ports the SQLite triggers on
// knowledge object versions, version lifecycles, dependency seals, the
// app/type and owner active counters and object ACLs to
// migrations/postgres/0013_knowledge_objects_2_triggers.sql. Its scenarios
// live in testdata/postgres_triggers/knowledge_objects_2.json; each one was
// checked against SQLite when the lane was ported, and these tests hold
// PostgreSQL to them.

const (
	knowledgeObjects2FixturePath = "testdata/postgres_triggers/knowledge_objects_2.json"
	knowledgeObjects2Lane        = "knowledge_objects_2"
	knowledgeObjects2Triggers    = 26
	knowledgeObjects2Capacity    = "knowledge object version capacity exhausted"
	knowledgeObjects2OrdinaryCap = 61440
)

// knowledgeObjects2Tables are the tables whose triggers this lane owns.
// Trigger lanes partition tables, so every user trigger on them is the
// lane's.
var knowledgeObjects2Tables = []string{
	"knowledge_app_type_active_counters",
	"knowledge_object_acl",
	"knowledge_object_dependency_seals",
	"knowledge_object_version_lifecycle",
	"knowledge_object_versions",
	"knowledge_owner_active_counters",
}

// knowledgeObjects2KnownLanes are the trigger lanes a scenario may depend
// on. A lane is present when the migrations hold NNNN_<lane>_triggers.sql.
var knowledgeObjects2KnownLanes = []string{
	"audit_1",
	"audit_2",
	"collectors_apps_search_1",
	"collectors_apps_search_2",
	"core_ingest_1",
	"core_ingest_2",
	"core_ingest_3",
	"knowledge_catalog_1",
	"knowledge_catalog_2",
	"knowledge_objects_1",
	"lookups",
}

// knowledgeObjects2Scenario is one fixture entry. setup_sql runs inside the
// scenario transaction with search_path = open_splunk, then statement runs
// and must be accepted or rejected as expect says. A rejection is a P0001
// RAISE with exactly message. An accepted statement may be followed by
// check_sql, whose single value, as text, must equal check_result.
// Everything is rolled back afterwards, so deferred foreign keys are never
// checked and setups may drop triggers.
//
// Scenarios whose AFTER trigger writes a row prove it with a probe: the
// statement inserts a duplicate counter row, and is rejected by the counter
// identity trigger, only if the expected row exists.
//
// depends_on_lanes lists trigger lanes the outcome needs installed. Such a
// scenario runs, and must hold, as soon as every lane it names is present;
// until then it is listed in the test log, never skipped silently.
type knowledgeObjects2Scenario struct {
	Name           string   `json:"name"`
	Trigger        string   `json:"trigger"`
	SetupSQL       []string `json:"setup_sql"`
	Statement      string   `json:"statement"`
	Expect         string   `json:"expect"`
	Message        string   `json:"message"`
	CheckSQL       string   `json:"check_sql,omitempty"`
	CheckResult    string   `json:"check_result,omitempty"`
	DependsOnLanes []string `json:"depends_on_lanes,omitempty"`
}

func loadKnowledgeObjects2Scenarios(t *testing.T) []knowledgeObjects2Scenario {
	t.Helper()
	contents, err := os.ReadFile(knowledgeObjects2FixturePath)
	if err != nil {
		t.Fatalf("read %s: %v", knowledgeObjects2FixturePath, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var scenarios []knowledgeObjects2Scenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode %s: %v", knowledgeObjects2FixturePath, err)
	}
	if decoder.More() {
		t.Fatalf("%s holds more than one JSON value", knowledgeObjects2FixturePath)
	}
	return scenarios
}

// validateKnowledgeObjects2Scenario reports a malformed scenario, so a typo
// in the fixture fails instead of quietly asserting less.
func validateKnowledgeObjects2Scenario(scenario knowledgeObjects2Scenario) error {
	var problems []string
	if strings.TrimSpace(scenario.Name) == "" {
		problems = append(problems, "no name")
	}
	if strings.TrimSpace(scenario.Trigger) == "" {
		problems = append(problems, "no target trigger")
	}
	if strings.TrimSpace(scenario.Statement) == "" {
		problems = append(problems, "no statement")
	}
	switch scenario.Expect {
	case "accept":
		if scenario.Message != "" {
			problems = append(problems, "an accepted statement carries a rejection message")
		}
	case "reject":
		if scenario.Message == "" {
			problems = append(problems, "a rejected statement has no message")
		}
		if scenario.CheckSQL != "" {
			problems = append(problems, "a rejected statement has check_sql")
		}
	default:
		problems = append(problems, "expect is "+strconv.Quote(scenario.Expect)+", want accept or reject")
	}
	if (scenario.CheckSQL == "") != (scenario.CheckResult == "") {
		problems = append(problems, "check_sql and check_result must be given together")
	}
	for _, lane := range scenario.DependsOnLanes {
		if !slices.Contains(knowledgeObjects2KnownLanes, lane) {
			problems = append(problems, "depends on unknown lane "+lane)
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(problems, "; "))
}

var knowledgeObjects2MigrationName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// knowledgeObjects2MigrationSet returns the migrations the lane tests run
// against: the embedded set itself once its versions are contiguous. While
// trigger lanes numbered before this one are still unmerged, the runner
// refuses the embedded set's version gap, so the same files are renumbered
// in order to close it. Their contents, and so every trigger, are unchanged.
func knowledgeObjects2MigrationSet(t *testing.T) (fs.FS, bool) {
	t.Helper()
	shipped := migrations.Postgres()
	names, err := fs.Glob(shipped, "*.sql")
	if err != nil {
		t.Fatalf("list PostgreSQL migrations: %v", err)
	}
	sort.Strings(names)
	contiguous := true
	renumbered := fstest.MapFS{}
	for index, name := range names {
		matches := knowledgeObjects2MigrationName.FindStringSubmatch(name)
		if matches == nil {
			t.Fatalf("PostgreSQL migration %q is not NNNN_name.sql", name)
		}
		if matches[1] != fmt.Sprintf("%04d", index+1) {
			contiguous = false
		}
		contents, err := fs.ReadFile(shipped, name)
		if err != nil {
			t.Fatalf("read PostgreSQL migration %q: %v", name, err)
		}
		renumbered[fmt.Sprintf("%04d_%s.sql", index+1, matches[2])] = &fstest.MapFile{Data: contents}
	}
	if contiguous {
		return shipped, false
	}
	return renumbered, true
}

// newKnowledgeObjects2ControlDB returns a fresh control database migrated
// with knowledgeObjects2MigrationSet.
func newKnowledgeObjects2ControlDB(t *testing.T) *control.PostgresDB {
	t.Helper()
	migrationFS, renumbered := knowledgeObjects2MigrationSet(t)
	if !renumbered {
		return testsupport.NewPostgresControlDB(t)
	}
	return testsupport.NewPostgresControlDBWithMigrations(t, migrationFS)
}

// knowledgeObjects2PresentLanes returns the trigger lanes whose migration
// file the lane tests apply.
func knowledgeObjects2PresentLanes(t *testing.T) map[string]bool {
	t.Helper()
	migrationFS, _ := knowledgeObjects2MigrationSet(t)
	names, err := fs.Glob(migrationFS, "*.sql")
	if err != nil {
		t.Fatalf("list PostgreSQL migrations: %v", err)
	}
	pattern := regexp.MustCompile(`^\d{4}_([a-z0-9_]+)_triggers\.sql$`)
	present := make(map[string]bool)
	for _, name := range names {
		if matches := pattern.FindStringSubmatch(name); matches != nil {
			present[matches[1]] = true
		}
	}
	return present
}

// runKnowledgeObjects2Scenario executes one scenario in its own transaction
// on a dedicated connection and always rolls it back.
func runKnowledgeObjects2Scenario(ctx context.Context, t *testing.T, db *sql.DB, scenario knowledgeObjects2Scenario) {
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
	message, raised := control.RaiseMessage(statementErr)
	if !raised {
		t.Fatalf("rejection is not a P0001 RAISE: %v; want %q", statementErr, scenario.Message)
	}
	if message != scenario.Message {
		t.Fatalf("RAISE message = %q, want %q", message, scenario.Message)
	}
}

// runKnowledgeObjects2Scenarios validates scenarios and runs every one whose
// lanes are present, one after another on one database: several drop a
// trigger, which holds an exclusive table lock until rollback. Scenarios
// waiting for absent lanes are named in the log, never skipped silently.
func runKnowledgeObjects2Scenarios(t *testing.T, scenarios []knowledgeObjects2Scenario) {
	t.Helper()
	if len(scenarios) == 0 {
		t.Fatal("no scenarios to run")
	}
	present := knowledgeObjects2PresentLanes(t)
	if !present[knowledgeObjects2Lane] {
		t.Fatalf("the PostgreSQL migrations hold no NNNN_%s_triggers.sql", knowledgeObjects2Lane)
	}
	names := make(map[string]bool, len(scenarios))
	var runnable []knowledgeObjects2Scenario
	var waiting []string
	for _, scenario := range scenarios {
		if err := validateKnowledgeObjects2Scenario(scenario); err != nil {
			t.Fatalf("scenario %q is malformed: %v", scenario.Name, err)
		}
		if names[scenario.Name] {
			t.Fatalf("scenario name %q is used twice", scenario.Name)
		}
		names[scenario.Name] = true
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
	if len(waiting) > 0 {
		t.Logf("%d scenarios wait for trigger lanes that are not merged yet and did not run: %s",
			len(waiting), strings.Join(waiting, "; "))
	}
	db := newKnowledgeObjects2ControlDB(t)
	for _, scenario := range runnable {
		t.Run(scenario.Name, func(t *testing.T) {
			runKnowledgeObjects2Scenario(t.Context(), t, db.SQLDB(), scenario)
		})
	}
}

// TestPostgresKnowledgeObjects2TriggerScenarios runs every fixture
// scenario.
func TestPostgresKnowledgeObjects2TriggerScenarios(t *testing.T) {
	t.Parallel()
	runKnowledgeObjects2Scenarios(t, loadKnowledgeObjects2Scenarios(t))
}

// TestPostgresKnowledgeObjects2TriggerCatalogMatchesFixture checks that the
// lane's tables carry exactly the lane's triggers, enabled, with functions
// pinned to the control schema, and that the fixture and the SQLite parity
// scenarios accept and reject at least one statement for each of them.
func TestPostgresKnowledgeObjects2TriggerCatalogMatchesFixture(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := newKnowledgeObjects2ControlDB(t)
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
		control.PostgresSchema, knowledgeObjects2Tables)
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
		if config != "search_path="+control.PostgresSchema {
			t.Errorf("trigger %s function config = %q, want search_path=%s", name, config, control.PostgresSchema)
		}
		installed = append(installed, name)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("read lane triggers: %v", err)
	}
	if len(installed) != knowledgeObjects2Triggers {
		t.Fatalf("lane tables carry %d user triggers, want %d: %v", len(installed), knowledgeObjects2Triggers, installed)
	}

	accepted := make(map[string]int)
	rejected := make(map[string]int)
	scenarios := append(loadKnowledgeObjects2Scenarios(t), knowledgeObjects2SQLiteParityScenarios...)
	for _, scenario := range scenarios {
		if !slices.Contains(installed, scenario.Trigger) {
			t.Errorf("scenario %q targets trigger %q, which is not installed", scenario.Name, scenario.Trigger)
		}
		if scenario.Expect == "accept" {
			accepted[scenario.Trigger]++
		} else {
			rejected[scenario.Trigger]++
		}
	}
	for _, name := range installed {
		if accepted[name] == 0 || rejected[name] == 0 {
			t.Errorf("trigger %s has %d accepted and %d rejected scenarios, want at least one of each",
				name, accepted[name], rejected[name])
		}
	}
}

// TestPostgresKnowledgeObjects2TriggersIgnoreTheSessionSearchPath fires the
// lane's cross-table triggers from a session whose search_path names a
// schema of decoy tables with the control tables' names. Each decoy would
// change the outcome if a trigger read it: the decoy tenant is at capacity
// and the decoy dependency would make a zero-count seal incomplete. The
// functions pin search_path, so they read and write the control tables only.
func TestPostgresKnowledgeObjects2TriggersIgnoreTheSessionSearchPath(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := newKnowledgeObjects2ControlDB(t)
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
	exec(`SET LOCAL search_path TO ` + control.PostgresSchema)
	for _, statement := range knowledgeObjects2BaseSetup {
		exec(statement)
	}
	exec(`CREATE SCHEMA knowledge_objects_2_decoy`)
	exec(`SET LOCAL search_path TO knowledge_objects_2_decoy`)
	exec(`CREATE TABLE knowledge_catalog_tenants (tenant_id text, version_count bigint)`)
	exec(`INSERT INTO knowledge_catalog_tenants VALUES ('t1', 65536)`)
	exec(`CREATE TABLE knowledge_object_dependencies (
		tenant_id text, source_object_id text, source_object_version bigint)`)
	exec(`INSERT INTO knowledge_object_dependencies VALUES ('t1', 'ko1', 1)`)
	exec(`CREATE TABLE knowledge_object_version_lifecycle (tenant_id text, knowledge_object_id text)`)

	exec(`INSERT INTO ` + control.PostgresSchema + `.knowledge_object_versions (tenant_id, knowledge_object_id,
			object_version, app_id, owner_id, object_type, name, sharing_scope, state, definition_digest,
			dependency_count, mutation_kind, quarantine_reason, created_at_unix_micro)
		VALUES ('t1', 'ko1', 1, 'app_AAAAAAAAAAAAAAAAAAAAAA', 'u1', 'field_alias', 'n1', 'app', 'active',
			'\x1111111111111111111111111111111111111111111111111111111111111111'::bytea, 0, 'create', NULL, 100)`)
	exec(`INSERT INTO ` + control.PostgresSchema + `.knowledge_object_dependency_seals
			(tenant_id, knowledge_object_id, object_version, dependency_count)
		VALUES ('t1', 'ko1', 1, 0)`)

	var stored string
	if err := tx.QueryRowContext(ctx, `
		SELECT (SELECT version_count FROM `+control.PostgresSchema+`.knowledge_catalog_tenants WHERE tenant_id = 't1')
			|| ',' || (SELECT state FROM `+control.PostgresSchema+`.knowledge_object_version_lifecycle
				WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1)`).
		Scan(&stored); err != nil {
		t.Fatalf("read control tables: %v", err)
	}
	var decoy string
	if err := tx.QueryRowContext(ctx, `
		SELECT (SELECT version_count FROM knowledge_catalog_tenants) || ','
			|| (SELECT count(*) FROM knowledge_object_version_lifecycle)`).Scan(&decoy); err != nil {
		t.Fatalf("read decoy tables: %v", err)
	}
	if stored != "1,active" || decoy != "65536,0" {
		t.Fatalf("control version count and lifecycle = %q (decoy %q), want 1,active (decoy untouched 65536,0)",
			stored, decoy)
	}
}

// TestPostgresKnowledgeObjects2ConcurrentVersionsRespectCapacity commits two
// versions concurrently for a tenant one version below the ordinary limit.
// Both pass the capacity check against their snapshots, so the counter
// update in knowledge_object_version_after_insert is what serializes them:
// the second writer's update of the same catalog row fails with 40001,
// WriteTx retries it, and the retry sees the limit and is refused. Under
// READ COMMITTED both would commit and overshoot the limit.
func TestPostgresKnowledgeObjects2ConcurrentVersionsRespectCapacity(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := newKnowledgeObjects2ControlDB(t)
	seedKnowledgeObjects2CapacityTenant(ctx, t, db.SQLDB(), knowledgeObjects2OrdinaryCap-3)

	barrier := make(chan struct{})
	var arrived atomic.Int32
	arrive := func() error {
		if arrived.Add(1) == 2 {
			close(barrier)
		}
		select {
		case <-barrier:
			return nil
		case <-time.After(30 * time.Second):
			return errors.New("the other writer never reached the barrier")
		}
	}
	objects := []string{"ko_a", "ko_b"}
	attempts := make([]int, len(objects))
	results := make([]error, len(objects))
	var writers sync.WaitGroup
	for index, objectID := range objects {
		writers.Go(func() {
			results[index] = db.WriteTx(ctx, func(tx *sql.Tx) error {
				attempts[index]++
				var count int64
				readErr := tx.QueryRowContext(ctx,
					`SELECT version_count FROM knowledge_catalog_tenants WHERE tenant_id = 't1'`).Scan(&count)
				if attempts[index] == 1 {
					// Both snapshots are taken before either writer inserts.
					if err := arrive(); err != nil {
						return errors.Join(readErr, err)
					}
				}
				if readErr != nil {
					return readErr
				}
				if _, err := tx.ExecContext(ctx, knowledgeObjects2NamedVersionSQL(
					objectID, objectID, 2, "active", "2", "update", 200)); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `INSERT INTO knowledge_object_dependency_seals
					(tenant_id, knowledge_object_id, object_version, dependency_count) VALUES ('t1', $1, 2, 0)`,
					objectID)
				return err
			})
		})
	}
	writers.Wait()

	var committed, refused []int
	for index, err := range results {
		if err == nil {
			committed = append(committed, index)
			continue
		}
		if message, raised := control.RaiseMessage(err); !raised || message != knowledgeObjects2Capacity {
			t.Fatalf("writer %s failed with %v, want nil or RAISE %q", objects[index], err, knowledgeObjects2Capacity)
		}
		refused = append(refused, index)
	}
	if len(committed) != 1 || len(refused) != 1 {
		t.Fatalf("committed writers %v and refused writers %v, want exactly one of each", committed, refused)
	}
	if attempts[refused[0]] < 2 {
		t.Fatalf("refused writer ran %d attempts, want a serialization retry before the refusal", attempts[refused[0]])
	}
	var state string
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT (SELECT version_count FROM knowledge_catalog_tenants WHERE tenant_id = 't1') || ','
			|| (SELECT count(*) FROM knowledge_object_versions WHERE tenant_id = 't1')`).Scan(&state); err != nil {
		t.Fatalf("read committed capacity: %v", err)
	}
	if want := strconv.Itoa(knowledgeObjects2OrdinaryCap) + ",3"; state != want {
		t.Fatalf("version count and stored versions = %q, want %q", state, want)
	}
}

// seedKnowledgeObjects2CapacityTenant commits a tenant whose catalog counts
// versionCount versions plus ko_a and ko_b at version 1. The catalog-tenant
// and registry triggers belong to other lanes: they are switched off while
// the counter is planted and the registry rows are written, so the seed does
// not depend on them, and are on again before the writers run.
func seedKnowledgeObjects2CapacityTenant(ctx context.Context, t *testing.T, db *sql.DB, versionCount int) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back seed: %v", err)
		}
	}()
	statements := slices.Clone(knowledgeObjects2BaseSetup)
	statements = append(statements,
		`ALTER TABLE knowledge_catalog_tenants DISABLE TRIGGER USER`,
		`UPDATE knowledge_catalog_tenants SET version_count = `+strconv.Itoa(versionCount)+` WHERE tenant_id = 't1'`,
		`ALTER TABLE knowledge_catalog_tenants ENABLE TRIGGER USER`,
		`ALTER TABLE knowledge_objects DISABLE TRIGGER USER`,
	)
	for _, objectID := range []string{"ko_a", "ko_b"} {
		statements = append(statements,
			`INSERT INTO knowledge_objects (tenant_id, knowledge_object_id, current_version, app_id, owner_id,
				object_type, name, sharing_scope, state, definition_digest, created_at_unix_micro,
				updated_at_unix_micro)
			VALUES ('t1', '`+objectID+`', 1, 'app_AAAAAAAAAAAAAAAAAAAAAA', 'u1', 'field_alias', '`+objectID+`',
				'app', 'active', `+knowledgeObjects2Digest("1")+`, 100, 100)`,
			knowledgeObjects2NamedVersionSQL(objectID, objectID, 1, "active", "1", "create", 100),
			`INSERT INTO knowledge_object_dependency_seals (tenant_id, knowledge_object_id, object_version,
				dependency_count) VALUES ('t1', '`+objectID+`', 1, 0)`,
		)
	}
	statements = append(statements,
		// Every referenced row exists now; checking the deferred foreign
		// keys here clears the pending events ALTER TABLE refuses to run
		// with.
		`SET CONSTRAINTS ALL IMMEDIATE`,
		`ALTER TABLE knowledge_objects ENABLE TRIGGER USER`,
	)
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed capacity tenant: %v\n%s", err, statement)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit capacity tenant: %v", err)
	}
	var count int64
	if err := db.QueryRowContext(ctx,
		`SELECT version_count FROM knowledge_catalog_tenants WHERE tenant_id = 't1'`).Scan(&count); err != nil {
		t.Fatalf("read seeded version count: %v", err)
	}
	if count != int64(versionCount+2) {
		t.Fatalf("seeded version count = %d, want %d", count, versionCount+2)
	}
}

// knowledgeObjects2BaseSetup is the tenant, catalog row, apps and definition
// blobs every fixture scenario starts from.
var knowledgeObjects2BaseSetup = []string{
	`INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro) VALUES ('t1', 1, 1)`,
	`INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('t1')`,
	`INSERT INTO app_workspaces (app_id, tenant_id, version, slug, display_name, default_time_range_present,
		state, created_at_unix_micro, updated_at_unix_micro)
	VALUES ('app_AAAAAAAAAAAAAAAAAAAAAA', 't1', 1, 'search', 'Search', 0, 'active', 1, 1)`,
	`INSERT INTO app_workspaces (app_id, tenant_id, version, slug, display_name, default_time_range_present,
		state, created_at_unix_micro, updated_at_unix_micro)
	VALUES ('app_BBBBBBBBBBBBBBBBBBBBBA', 't1', 1, 'other', 'Other', 0, 'active', 1, 1)`,
	knowledgeObjects2BlobSQL("1"),
	knowledgeObjects2BlobSQL("2"),
	knowledgeObjects2BlobSQL("3"),
}

// knowledgeObjects2Digest is a bytea literal of 32 bytes, each 0xNN for the
// hex digit nibble.
func knowledgeObjects2Digest(nibble string) string {
	return `'\x` + strings.Repeat(nibble, 64) + `'::bytea`
}

func knowledgeObjects2BlobSQL(nibble string) string {
	return `INSERT INTO knowledge_definition_blobs (tenant_id, definition_digest, definition_proto,
		definition_bytes, created_at_unix_micro)
	VALUES ('t1', ` + knowledgeObjects2Digest(nibble) + `, '\x01'::bytea, 1, 1)`
}

// knowledgeObjects2VersionSQL inserts a field_alias version named n1 of
// objectID, owned by u1 in the first app, with no dependencies.
func knowledgeObjects2VersionSQL(
	objectID string,
	version int,
	state, digestNibble string,
	mutation string,
	createdAt int,
) string {
	return knowledgeObjects2NamedVersionSQL(objectID, "n1", version, state, digestNibble, mutation, createdAt)
}

// knowledgeObjects2NamedVersionSQL is knowledgeObjects2VersionSQL for an
// object called name.
func knowledgeObjects2NamedVersionSQL(
	objectID, name string,
	version int,
	state, digestNibble string,
	mutation string,
	createdAt int,
) string {
	return `INSERT INTO knowledge_object_versions (tenant_id, knowledge_object_id, object_version, app_id,
		owner_id, object_type, name, sharing_scope, state, definition_digest, dependency_count, mutation_kind,
		quarantine_reason, created_at_unix_micro)
	VALUES ('t1', '` + objectID + `', ` + strconv.Itoa(version) + `, 'app_AAAAAAAAAAAAAAAAAAAAAA', 'u1',
		'field_alias', '` + name + `', 'app', '` + state + `', ` + knowledgeObjects2Digest(digestNibble) + `, 0, '` +
		mutation + `', NULL, ` + strconv.Itoa(createdAt) + `)`
}

func knowledgeObjects2Setup(statements ...string) []string {
	return append(slices.Clone(knowledgeObjects2BaseSetup), statements...)
}

// knowledgeObjects2SQLiteParityScenarios port what the SQLite knowledge
// catalog tests rely on these triggers for when they corrupt stored rows
// directly: dropping exactly the named trigger makes the write possible, so
// that trigger is the only guard on it. Each names the SQLite test it
// mirrors; the matching rejection with the trigger in place is in the
// fixture.
var knowledgeObjects2SQLiteParityScenarios = []knowledgeObjects2Scenario{
	{
		// internal/knowledgecatalog/remediation_test.go "version time" and
		// error_context_test.go rewrite a stored version after dropping
		// knowledge_object_version_update_is_forbidden.
		Name:    "version_time_rewrite_after_dropping_update_trigger_is_accepted",
		Trigger: "knowledge_object_version_update_is_forbidden",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
			knowledgeObjects2VersionSQL("ko1", 2, "active", "2", "update", 200),
			`DROP TRIGGER knowledge_object_version_update_is_forbidden ON knowledge_object_versions`,
		),
		Statement: `UPDATE knowledge_object_versions SET created_at_unix_micro = 199
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 2`,
		Expect: "accept",
		CheckSQL: `SELECT created_at_unix_micro FROM knowledge_object_versions
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 2`,
		CheckResult: "199",
	},
	{
		// internal/knowledgecatalog/privacy_nondisclosure_contract_test.go
		// "current version identity" and
		// writer_replay_latest_authority_adversarial_test.go rewrite a
		// version's owner after dropping the same trigger.
		Name:    "version_owner_rewrite_after_dropping_update_trigger_is_accepted",
		Trigger: "knowledge_object_version_update_is_forbidden",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
			`DROP TRIGGER knowledge_object_version_update_is_forbidden ON knowledge_object_versions`,
		),
		Statement: `UPDATE knowledge_object_versions SET owner_id = 'owner-b'
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
		Expect: "accept",
		CheckSQL: `SELECT owner_id FROM knowledge_object_versions
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
		CheckResult: "owner-b",
	},
	{
		// internal/knowledgecatalog/writer_replay_latest_authority_adversarial_test.go
		// stages a hidden latest version with another owner after dropping
		// knowledge_object_version_writer_semantics_are_exact; the other
		// version triggers still accept it.
		Name:    "hidden_latest_version_after_dropping_writer_trigger_is_accepted",
		Trigger: "knowledge_object_version_writer_semantics_are_exact",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
			`DROP TRIGGER knowledge_object_version_writer_semantics_are_exact ON knowledge_object_versions`,
		),
		Statement: `INSERT INTO knowledge_object_versions (tenant_id, knowledge_object_id, object_version,
				app_id, owner_id, object_type, name, sharing_scope, state, definition_digest, dependency_count,
				mutation_kind, quarantine_reason, created_at_unix_micro)
			SELECT tenant_id, knowledge_object_id, 2, app_id, 'owner-b', object_type, name, sharing_scope, state,
				definition_digest, dependency_count, 'update', NULL, created_at_unix_micro + 1
			FROM knowledge_object_versions
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
		Expect: "accept",
		CheckSQL: `SELECT owner_id || ',' || created_at_unix_micro FROM knowledge_object_versions
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 2`,
		CheckResult: "owner-b,101",
	},
	{
		// The same staged version with the writer trigger in place: the owner
		// may not change on an update.
		Name:    "hidden_latest_version_with_writer_trigger_is_rejected",
		Trigger: "knowledge_object_version_writer_semantics_are_exact",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
		),
		Statement: `INSERT INTO knowledge_object_versions (tenant_id, knowledge_object_id, object_version,
				app_id, owner_id, object_type, name, sharing_scope, state, definition_digest, dependency_count,
				mutation_kind, quarantine_reason, created_at_unix_micro)
			SELECT tenant_id, knowledge_object_id, 2, app_id, 'owner-b', object_type, name, sharing_scope, state,
				definition_digest, dependency_count, 'update', NULL, created_at_unix_micro + 1
			FROM knowledge_object_versions
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
		Expect:  "reject",
		Message: "knowledge object version writer semantics are invalid",
	},
	{
		// internal/knowledgecatalog/remediation_test.go "seal count" rewrites
		// a seal after dropping knowledge_dependency_seal_update_is_forbidden.
		Name:    "seal_count_rewrite_after_dropping_update_trigger_is_accepted",
		Trigger: "knowledge_dependency_seal_update_is_forbidden",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
			`INSERT INTO knowledge_object_dependency_seals (tenant_id, knowledge_object_id, object_version,
				dependency_count) VALUES ('t1', 'ko1', 1, 0)`,
			`DROP TRIGGER knowledge_dependency_seal_update_is_forbidden ON knowledge_object_dependency_seals`,
		),
		Statement: `UPDATE knowledge_object_dependency_seals SET dependency_count = 1
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
		Expect: "accept",
		CheckSQL: `SELECT dependency_count FROM knowledge_object_dependency_seals
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
		CheckResult: "1",
	},
	{
		// internal/knowledgecatalog/version_revision_properties_test.go
		// deletes a historical seal after dropping
		// knowledge_dependency_seal_delete_is_forbidden.
		Name:    "seal_delete_after_dropping_delete_trigger_is_accepted",
		Trigger: "knowledge_dependency_seal_delete_is_forbidden",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
			`INSERT INTO knowledge_object_dependency_seals (tenant_id, knowledge_object_id, object_version,
				dependency_count) VALUES ('t1', 'ko1', 1, 0)`,
			`DROP TRIGGER knowledge_dependency_seal_delete_is_forbidden ON knowledge_object_dependency_seals`,
		),
		Statement: `DELETE FROM knowledge_object_dependency_seals
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
		Expect:      "accept",
		CheckSQL:    `SELECT count(*) FROM knowledge_object_dependency_seals`,
		CheckResult: "0",
	},
	{
		// internal/knowledgecatalog/integration_dependency_semantics_adversarial_test.go
		// rewrites a sealed dependency set: with the version update and seal
		// delete triggers dropped, the version's count changes, the seal is
		// replaced, and the new seal is complete against the new set.
		Name:    "semantic_dependency_set_rewrite_reseals_completely",
		Trigger: "knowledge_dependency_seal_is_complete",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko2", 1, "active", "1", "create", 100),
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
			`INSERT INTO knowledge_object_dependency_seals (tenant_id, knowledge_object_id, object_version,
				dependency_count) VALUES ('t1', 'ko1', 1, 0)`,
			`DROP TRIGGER knowledge_object_version_update_is_forbidden ON knowledge_object_versions`,
			`DROP TRIGGER knowledge_dependency_seal_delete_is_forbidden ON knowledge_object_dependency_seals`,
			// The seal goes first: it references the version's count, and
			// ON UPDATE RESTRICT is checked immediately even when deferred.
			`DELETE FROM knowledge_object_dependency_seals
				WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
			`UPDATE knowledge_object_versions SET dependency_count = 1
				WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
			`INSERT INTO knowledge_object_dependencies (tenant_id, source_object_id, source_object_version, ordinal,
				target_kind, target_object_id, target_object_version, dependency_role)
			VALUES ('t1', 'ko1', 1, 0, 'object', 'ko2', 1, 'field_input')`,
		),
		Statement: `INSERT INTO knowledge_object_dependency_seals (tenant_id, knowledge_object_id, object_version,
				dependency_count) VALUES ('t1', 'ko1', 1, 1)`,
		Expect: "accept",
		CheckSQL: `SELECT dependency_count FROM knowledge_object_dependency_seals
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
		CheckResult: "1",
	},
	{
		// internal/knowledgecatalog/writer_reclaim_authority_adversarial_test.go
		// "immutable lifecycle marker" and remediation_test.go "lifecycle
		// marker" move a lifecycle timestamp after dropping
		// knowledge_object_version_lifecycle_update_is_forbidden.
		Name:    "lifecycle_marker_rewrite_after_dropping_update_trigger_is_accepted",
		Trigger: "knowledge_object_version_lifecycle_update_is_forbidden",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
			knowledgeObjects2VersionSQL("ko1", 2, "disabled", "1", "disable", 200),
			`DROP TRIGGER knowledge_object_version_lifecycle_update_is_forbidden
				ON knowledge_object_version_lifecycle`,
		),
		Statement: `UPDATE knowledge_object_version_lifecycle SET disabled_at_unix_micro = 150
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 2`,
		Expect: "accept",
		CheckSQL: `SELECT state || ',' || disabled_at_unix_micro FROM knowledge_object_version_lifecycle
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 2`,
		CheckResult: "disabled,150",
	},
	{
		// The same lifecycle rewrite with the trigger in place.
		Name:    "lifecycle_marker_rewrite_is_rejected",
		Trigger: "knowledge_object_version_lifecycle_update_is_forbidden",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
			knowledgeObjects2VersionSQL("ko1", 2, "disabled", "1", "disable", 200),
		),
		Statement: `UPDATE knowledge_object_version_lifecycle SET disabled_at_unix_micro = 150
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 2`,
		Expect:  "reject",
		Message: "knowledge object version lifecycle is immutable",
	},
	{
		// internal/knowledgecatalog/integration_lifecycle_chronology_adversarial_test.go
		// "missing" deletes a lifecycle row after dropping the lifecycle
		// table's triggers.
		Name:    "lifecycle_delete_after_dropping_delete_trigger_is_accepted",
		Trigger: "knowledge_object_version_lifecycle_delete_is_forbidden",
		SetupSQL: knowledgeObjects2Setup(
			knowledgeObjects2VersionSQL("ko1", 1, "active", "1", "create", 100),
			`DROP TRIGGER knowledge_object_version_lifecycle_delete_is_forbidden
				ON knowledge_object_version_lifecycle`,
		),
		Statement: `DELETE FROM knowledge_object_version_lifecycle
			WHERE tenant_id = 't1' AND knowledge_object_id = 'ko1' AND object_version = 1`,
		Expect:      "accept",
		CheckSQL:    `SELECT count(*) FROM knowledge_object_version_lifecycle`,
		CheckResult: "0",
	},
}

// TestPostgresKnowledgeObjects2TriggersMatchSQLiteStoreTests runs the SQLite
// parity scenarios above.
func TestPostgresKnowledgeObjects2TriggersMatchSQLiteStoreTests(t *testing.T) {
	t.Parallel()
	runKnowledgeObjects2Scenarios(t, knowledgeObjects2SQLiteParityScenarios)
}
