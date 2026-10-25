package control_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// The knowledge_objects_1 trigger lane ports the SQLite triggers on the
// knowledge-object registry, its dependency edges and the per-app / per-type
// active counters to migrations/postgres/0012_knowledge_objects_1_triggers.sql.
// Its fixture lists scenarios in PostgreSQL dialect; each runs as BEGIN; SET
// LOCAL search_path; setup_sql; statement; ROLLBACK. Every identifier in this
// file carries the lane name so the twelve trigger-lane test files can share
// package control_test.
const (
	knowledgeObjects1Lane          = "knowledge_objects_1"
	knowledgeObjects1MigrationFile = "0012_knowledge_objects_1_triggers.sql"
	knowledgeObjects1FixturePath   = "testdata/postgres_triggers/knowledge_objects_1.json"
)

// knowledgeObjects1Tables are the tables whose triggers this lane owns. No
// other lane installs a trigger on them, so the installed set must equal the
// set the migration file creates.
var knowledgeObjects1Tables = []string{
	"knowledge_app_active_counters",
	"knowledge_object_dependencies",
	"knowledge_objects",
	"knowledge_type_active_counters",
}

// knowledgeObjects1StateQueries summarize every row the lane's triggers write
// or remove. An accepted scenario pins each summary after its statement, so a
// trigger that silently stops maintaining a counter, or removes the wrong
// projection rows, fails on its own scenario. Each query returns one text row.
var knowledgeObjects1StateQueries = map[string]string{
	"counters": `SELECT concat_ws(' | ',
		'tenants ' || COALESCE((SELECT string_agg(
			tenant_id || ' identities=' || identity_count || ' active=' || active_object_count,
			',' ORDER BY tenant_id) FROM knowledge_catalog_tenants), '-'),
		'app ' || COALESCE((SELECT string_agg(
			tenant_id || '/' || app_id || '=' || active_object_count,
			',' ORDER BY tenant_id, app_id) FROM knowledge_app_active_counters), '-'),
		'type ' || COALESCE((SELECT string_agg(
			tenant_id || '/' || object_type || '=' || active_object_count,
			',' ORDER BY tenant_id, object_type) FROM knowledge_type_active_counters), '-'),
		'app_type ' || COALESCE((SELECT string_agg(
			tenant_id || '/' || app_id || '/' || object_type || '=' || active_object_count,
			',' ORDER BY tenant_id, app_id, object_type) FROM knowledge_app_type_active_counters), '-'),
		'owner ' || COALESCE((SELECT string_agg(
			tenant_id || '/' || owner_id || '=' || active_private_object_count,
			',' ORDER BY tenant_id, owner_id) FROM knowledge_owner_active_counters), '-'))`,
	"registry": `SELECT COALESCE(string_agg(
		tenant_id || '/' || knowledge_object_id || '@' || current_version || ' ' || state
			|| ' ' || app_id || ' ' || owner_id || ' ' || object_type || ' ' || sharing_scope
			|| ' ' || name,
		',' ORDER BY tenant_id, knowledge_object_id), '-') FROM knowledge_objects`,
	"projections": `SELECT concat_ws(' | ',
		'projections ' || COALESCE((SELECT string_agg(
			tenant_id || '/' || knowledge_object_id || '@' || object_version || ' ' || state,
			',' ORDER BY tenant_id, knowledge_object_id, object_version)
			FROM knowledge_object_list_projections), '-'),
		'seals ' || COALESCE((SELECT string_agg(
			tenant_id || '/' || knowledge_object_id || '@' || object_version,
			',' ORDER BY tenant_id, knowledge_object_id, object_version)
			FROM knowledge_object_list_projection_seals), '-'),
		'selectors ' || COALESCE((SELECT string_agg(
			tenant_id || '/' || knowledge_object_id || '@' || object_version || '/' || dimension
				|| '#' || ordinal,
			',' ORDER BY tenant_id, knowledge_object_id, object_version, dimension, ordinal)
			FROM knowledge_object_list_selector_patterns), '-'))`,
	"dependencies": `SELECT COALESCE(string_agg(
		tenant_id || '/' || source_object_id || '@' || source_object_version || '#' || ordinal
			|| '->' || target_object_id || '@' || target_object_version,
		',' ORDER BY tenant_id, source_object_id, source_object_version, ordinal), '-')
		FROM knowledge_object_dependencies`,
}

type knowledgeObjects1Scenario struct {
	Name string `json:"name"`
	// Trigger is the trigger the scenario exercises. A reject scenario either
	// fails on that trigger's own message or on a message whose cause is that
	// trigger's side effect; disabling it must change the outcome.
	Trigger string `json:"trigger"`
	// UnchangedWithoutTrigger marks a reject scenario that proves Trigger did
	// not act: a BEFORE trigger whose WHEN did not match, or an AFTER trigger
	// that never ran because the statement was rejected first. Disabling
	// Trigger must then leave the rejection exactly as it is.
	UnchangedWithoutTrigger bool `json:"unchanged_without_trigger,omitempty"`
	// DependsOnLanes names other trigger lanes whose triggers the scenario's
	// expected outcome requires. Every scenario runs with every shipped
	// trigger enabled, so this only documents the dependency; the lanes must
	// be in the migration ledger.
	DependsOnLanes []string          `json:"depends_on_lanes,omitempty"`
	SetupSQL       []string          `json:"setup_sql"`
	Statement      string            `json:"statement"`
	Expect         string            `json:"expect"`
	Message        string            `json:"message"`
	State          map[string]string `json:"state,omitempty"`
}

func loadKnowledgeObjects1Scenarios(t *testing.T) []knowledgeObjects1Scenario {
	t.Helper()
	contents, err := os.ReadFile(filepath.FromSlash(knowledgeObjects1FixturePath))
	if err != nil {
		t.Fatalf("read %s: %v", knowledgeObjects1FixturePath, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	// A field this harness does not understand would otherwise be ignored,
	// and with it whatever the fixture author meant it to enforce.
	decoder.DisallowUnknownFields()
	var scenarios []knowledgeObjects1Scenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode %s: %v", knowledgeObjects1FixturePath, err)
	}
	if decoder.More() {
		t.Fatalf("%s holds more than one JSON value", knowledgeObjects1FixturePath)
	}
	return scenarios
}

var knowledgeObjects1CreateTrigger = regexp.MustCompile(`(?m)^CREATE TRIGGER ([a-z_][a-z0-9_]*)\s+(?:BEFORE|AFTER)\b[^;]*?\bON ([a-z_][a-z0-9_]*)\b`)

// knowledgeObjects1FileTriggers returns trigger name -> table for every
// CREATE TRIGGER in the shipped lane migration.
func knowledgeObjects1FileTriggers(t *testing.T) map[string]string {
	t.Helper()
	contents, err := fs.ReadFile(migrations.Postgres(), knowledgeObjects1MigrationFile)
	if err != nil {
		t.Fatalf("read embedded %s: %v", knowledgeObjects1MigrationFile, err)
	}
	triggers := make(map[string]string)
	for _, match := range knowledgeObjects1CreateTrigger.FindAllStringSubmatch(string(contents), -1) {
		if _, duplicate := triggers[match[1]]; duplicate {
			t.Fatalf("%s creates trigger %s twice", knowledgeObjects1MigrationFile, match[1])
		}
		triggers[match[1]] = match[2]
	}
	if got := strings.Count(string(contents), "\nCREATE TRIGGER "); got != len(triggers) {
		t.Fatalf("%s has %d CREATE TRIGGER statements, parsed %d", knowledgeObjects1MigrationFile, got, len(triggers))
	}
	return triggers
}

// TestPostgresKnowledgeObjects1TriggerFixtureIsWellFormed checks the fixture
// against the migration file: names are unique, every scenario names a
// trigger the file creates, every trigger has an accepted and a rejected
// scenario, and accepted scenarios pin every state summary.
func TestPostgresKnowledgeObjects1TriggerFixtureIsWellFormed(t *testing.T) {
	t.Parallel()
	scenarios := loadKnowledgeObjects1Scenarios(t)
	triggers := knowledgeObjects1FileTriggers(t)
	if len(triggers) != 30 {
		t.Fatalf("%s creates %d triggers, want the 30 SQLite triggers of the lane", knowledgeObjects1MigrationFile, len(triggers))
	}
	for name, table := range triggers {
		if !slices.Contains(knowledgeObjects1Tables, table) {
			t.Errorf("trigger %s is on %s, outside the lane's tables %v", name, table, knowledgeObjects1Tables)
		}
	}

	seen := make(map[string]bool, len(scenarios))
	accepts := make(map[string]int)
	rejects := make(map[string]int)
	for _, scenario := range scenarios {
		if scenario.Name == "" || seen[scenario.Name] {
			t.Errorf("scenario name %q is empty or repeated", scenario.Name)
		}
		seen[scenario.Name] = true
		if _, ok := triggers[scenario.Trigger]; !ok {
			t.Errorf("scenario %q names trigger %q, which %s does not create", scenario.Name, scenario.Trigger, knowledgeObjects1MigrationFile)
		}
		if strings.TrimSpace(scenario.Statement) == "" {
			t.Errorf("scenario %q has no statement", scenario.Name)
		}
		if err := checkPostgresTriggerLaneDependencies(knowledgeObjects1Lane, scenario.DependsOnLanes); err != nil {
			t.Errorf("scenario %q: %v", scenario.Name, err)
		}
		switch scenario.Expect {
		case "accept":
			accepts[scenario.Trigger]++
			if scenario.Message != "" {
				t.Errorf("accepted scenario %q carries message %q", scenario.Name, scenario.Message)
			}
			if scenario.UnchangedWithoutTrigger {
				t.Errorf("accepted scenario %q sets unchanged_without_trigger, which only applies to rejects", scenario.Name)
			}
			if len(scenario.State) != len(knowledgeObjects1StateQueries) {
				t.Errorf("accepted scenario %q pins %d state summaries, want %d", scenario.Name, len(scenario.State), len(knowledgeObjects1StateQueries))
			}
			for key := range scenario.State {
				if _, ok := knowledgeObjects1StateQueries[key]; !ok {
					t.Errorf("accepted scenario %q pins unknown state summary %q", scenario.Name, key)
				}
			}
		case "reject":
			rejects[scenario.Trigger]++
			if scenario.Message == "" {
				t.Errorf("rejected scenario %q has no message", scenario.Name)
			}
			if len(scenario.State) != 0 {
				t.Errorf("rejected scenario %q pins state, but a rejected statement leaves none", scenario.Name)
			}
		default:
			t.Errorf("scenario %q expects %q, want accept or reject", scenario.Name, scenario.Expect)
		}
	}
	for name := range triggers {
		if accepts[name] == 0 || rejects[name] == 0 {
			t.Errorf("trigger %s has %d accepted and %d rejected scenarios, want at least one of each", name, accepts[name], rejects[name])
		}
	}
}

// TestPostgresKnowledgeObjects1TriggersAreInstalled checks that the migrated
// database carries exactly the lane's triggers on the lane's tables, enabled,
// row-level, and on the table the file names.
func TestPostgresKnowledgeObjects1TriggersAreInstalled(t *testing.T) {
	t.Parallel()
	want := knowledgeObjects1FileTriggers(t)
	db := testsupport.NewPostgresControlDB(t)
	rows, err := db.SQLDB().QueryContext(t.Context(), `
		SELECT trigger.tgname, class.relname, trigger.tgenabled::text,
		       (trigger.tgtype & 1) = 1 AS row_level
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'open_splunk'
		  AND class.relname = ANY($1)
		  AND NOT trigger.tgisinternal
		ORDER BY trigger.tgname`, knowledgeObjects1Tables)
	if err != nil {
		t.Fatalf("list installed triggers: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := make(map[string]string)
	for rows.Next() {
		var name, table, enabled string
		var rowLevel bool
		if err := rows.Scan(&name, &table, &enabled, &rowLevel); err != nil {
			t.Fatalf("scan installed trigger: %v", err)
		}
		if enabled != "O" || !rowLevel {
			t.Errorf("trigger %s on %s has tgenabled=%q row_level=%t, want an enabled FOR EACH ROW trigger", name, table, enabled, rowLevel)
		}
		got[name] = table
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate installed triggers: %v", err)
	}
	for name, table := range want {
		if got[name] != table {
			t.Errorf("trigger %s is installed on %q, want %q", name, got[name], table)
		}
	}
	for name, table := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("unexpected trigger %s on lane table %s", name, table)
		}
	}
}

// TestPostgresKnowledgeObjects1TriggerScenarios runs every fixture scenario
// with every shipped trigger enabled. Every trigger lane is merged, so a
// scenario that depends on another lane runs here too; a lane it names that
// the migration ledger lacks fails the test instead of leaving the scenario
// pending, so no expectation can hide behind a misspelled or dropped lane.
func TestPostgresKnowledgeObjects1TriggerScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadKnowledgeObjects1Scenarios(t)
	if len(scenarios) == 0 {
		t.Fatal("the fixture holds no scenarios")
	}
	db := testsupport.NewPostgresControlDB(t)
	installed := knowledgeObjects1InstalledLanes(t, db.SQLDB())
	for _, scenario := range scenarios {
		requirePostgresTriggerLanesEmbedded(t, installed, scenario.DependsOnLanes)
		t.Run(scenario.Name, func(t *testing.T) {
			runKnowledgeObjects1Scenario(t, db.SQLDB(), scenario, "")
		})
	}
}

// TestPostgresKnowledgeObjects1ActiveDependentCheckFollowsCurrentEdges is the
// PostgreSQL counterpart of the trigger-SQL assertion in
// internal/knowledgecatalog/writer_active_dependents_bound_test.go. The
// writer's hasActiveDependents query and this trigger must agree on what an
// active dependent is: an active registry row whose current version (not an
// older one) declares an object edge to the target. The SQLite INDEXED BY
// hints have no PostgreSQL equivalent; the predicates they guarded must stay.
// The trigger must also sort first among the BEFORE UPDATE triggers on
// knowledge_objects, because PostgreSQL fires same-event triggers in name
// order and the writer maps a blocked direct transition by this message.
func TestPostgresKnowledgeObjects1ActiveDependentCheckFollowsCurrentEdges(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	ctx := t.Context()
	const trigger = "knowledge_active_dependency_target_transition_is_blocked"
	var definition, function string
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT pg_catalog.pg_get_triggerdef(trigger.oid), pg_catalog.pg_get_functiondef(trigger.tgfoid)
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'open_splunk'
		  AND class.relname = 'knowledge_objects'
		  AND trigger.tgname = $1`, trigger).Scan(&definition, &function); err != nil {
		t.Fatalf("read %s: %v", trigger, err)
	}
	for _, required := range []string{
		"BEFORE UPDATE OF state ON open_splunk.knowledge_objects FOR EACH ROW",
		"(old.state = 'active'::text)",
		"'disabled'::text, 'quarantined'::text, 'deleted'::text",
	} {
		if !strings.Contains(definition, required) {
			t.Errorf("trigger definition lacks %q:\n%s", required, definition)
		}
	}
	for _, required := range []string{
		"CROSS JOIN knowledge_object_dependencies AS dependency",
		"dependent.state = 'active'",
		"dependency.source_object_id = dependent.knowledge_object_id",
		"dependency.source_object_version = dependent.current_version",
		"dependency.target_kind = 'object'",
		"dependency.target_object_id = OLD.knowledge_object_id",
		"RAISE EXCEPTION 'active knowledge dependency has active dependents'",
	} {
		if !strings.Contains(function, required) {
			t.Errorf("trigger function lacks %q:\n%s", required, function)
		}
	}

	var first string
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT trigger.tgname
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'open_splunk'
		  AND class.relname = 'knowledge_objects'
		  AND NOT trigger.tgisinternal
		  AND (trigger.tgtype & 2) = 2
		  AND (trigger.tgtype & 16) = 16
		ORDER BY trigger.tgname COLLATE "C"
		LIMIT 1`).Scan(&first); err != nil {
		t.Fatalf("read first BEFORE UPDATE trigger: %v", err)
	}
	if first != trigger {
		t.Fatalf("first BEFORE UPDATE trigger on knowledge_objects = %s, want %s", first, trigger)
	}
}

// TestPostgresKnowledgeObjects1RejectsNeedTheirTrigger disables each reject
// scenario's trigger and reruns it. The statement must then no longer fail
// with the scenario's message, proving the scenario exercises the trigger it
// names rather than passing on another trigger or a constraint. A scenario
// marked unchanged_without_trigger must instead fail exactly as before,
// proving the trigger did not act.
func TestPostgresKnowledgeObjects1RejectsNeedTheirTrigger(t *testing.T) {
	t.Parallel()
	scenarios := loadKnowledgeObjects1Scenarios(t)
	db := testsupport.NewPostgresControlDB(t)
	installed := knowledgeObjects1InstalledLanes(t, db.SQLDB())
	for _, scenario := range scenarios {
		if scenario.Expect != "reject" {
			continue
		}
		requirePostgresTriggerLanesEmbedded(t, installed, scenario.DependsOnLanes)
		t.Run(scenario.Name, func(t *testing.T) {
			err := execKnowledgeObjects1Scenario(t, db.SQLDB(), scenario, scenario.Trigger)
			message, ok := control.RaiseMessage(err)
			unchanged := ok && message == scenario.Message
			switch {
			case scenario.UnchangedWithoutTrigger && !unchanged:
				t.Fatalf("with trigger %s disabled the statement error = %v, want the same %q", scenario.Trigger, err, scenario.Message)
			case !scenario.UnchangedWithoutTrigger && unchanged:
				t.Fatalf("with trigger %s disabled the statement still fails with %q", scenario.Trigger, message)
			}
		})
	}
}

var knowledgeObjects1LaneMigrationName = regexp.MustCompile(`^[0-9]{4}_([a-z][a-z0-9_]*)_triggers\.sql$`)

// knowledgeObjects1InstalledLanes reads the trigger lanes from the migration
// ledger, where each lane is one NNNN_<lane>_triggers.sql file.
func knowledgeObjects1InstalledLanes(t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT name FROM open_splunk.schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("read migration ledger: %v", err)
	}
	defer func() { _ = rows.Close() }()
	lanes := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan migration ledger: %v", err)
		}
		if match := knowledgeObjects1LaneMigrationName.FindStringSubmatch(name); match != nil {
			lanes[match[1]] = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate migration ledger: %v", err)
	}
	if !lanes[knowledgeObjects1Lane] {
		t.Fatalf("migration ledger lacks %s", knowledgeObjects1MigrationFile)
	}
	return lanes
}

// runKnowledgeObjects1Scenario runs one scenario and checks its outcome: an
// accepted statement must succeed and leave every pinned state summary; a
// rejected one must raise exactly the scenario's message as P0001.
func runKnowledgeObjects1Scenario(t *testing.T, db *sql.DB, scenario knowledgeObjects1Scenario, disabledTrigger string) {
	t.Helper()
	err := execKnowledgeObjects1Scenario(t, db, scenario, disabledTrigger)
	switch scenario.Expect {
	case "accept":
		if err != nil {
			t.Fatalf("statement error = %v, want success", err)
		}
	case "reject":
		message, ok := control.RaiseMessage(err)
		if !ok {
			t.Fatalf("statement error = %v, want P0001 %q", err, scenario.Message)
		}
		if message != scenario.Message {
			t.Fatalf("statement message = %q, want %q", message, scenario.Message)
		}
	default:
		t.Fatalf("scenario expects %q", scenario.Expect)
	}
}

// execKnowledgeObjects1Scenario returns the statement's error. Setup failures
// and, for an accepted statement, state mismatches fail the test directly.
// The transaction always rolls back, so scenarios share one database.
func execKnowledgeObjects1Scenario(
	t *testing.T,
	db *sql.DB,
	scenario knowledgeObjects1Scenario,
	disabledTrigger string,
) error {
	t.Helper()
	ctx := t.Context()
	// SERIALIZABLE matches control.PostgresDB.WriteTx, the only isolation the
	// check-then-write triggers are sound under.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("begin scenario transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back scenario transaction: %v", err)
		}
	}()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path = open_splunk`); err != nil {
		t.Fatalf("set scenario search_path: %v", err)
	}
	if disabledTrigger != "" {
		disableKnowledgeObjects1Trigger(ctx, t, tx, disabledTrigger)
	}
	for index, statement := range scenario.SetupSQL {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("setup statement %d failed: %v\n%s", index, err, statement)
		}
	}
	if _, err := tx.ExecContext(ctx, scenario.Statement); err != nil {
		return err
	}
	if scenario.Expect != "accept" || disabledTrigger != "" {
		return nil
	}
	keys := make([]string, 0, len(scenario.State))
	for key := range scenario.State {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		query, ok := knowledgeObjects1StateQueries[key]
		if !ok {
			t.Fatalf("unknown state summary %q", key)
		}
		var got string
		if err := tx.QueryRowContext(ctx, query).Scan(&got); err != nil {
			t.Fatalf("read %s state: %v", key, err)
		}
		if got != scenario.State[key] {
			t.Errorf("%s state after statement:\n got  %q\n want %q", key, got, scenario.State[key])
		}
	}
	return nil
}

// disableKnowledgeObjects1Trigger disables one lane trigger inside tx; the
// rollback restores it. PostgreSQL builds the DDL so identifiers stay quoted.
func disableKnowledgeObjects1Trigger(ctx context.Context, t *testing.T, tx *sql.Tx, trigger string) {
	t.Helper()
	var statement string
	if err := tx.QueryRowContext(ctx, `
		SELECT format('ALTER TABLE open_splunk.%I DISABLE TRIGGER %I', class.relname, trigger.tgname)
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = 'open_splunk'
		  AND class.relname = ANY($1)
		  AND trigger.tgname = $2
		  AND NOT trigger.tgisinternal`, knowledgeObjects1Tables, trigger).Scan(&statement); err != nil {
		t.Fatalf("find trigger %s: %v", trigger, err)
	}
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		t.Fatalf("disable trigger %s: %v", trigger, err)
	}
}
