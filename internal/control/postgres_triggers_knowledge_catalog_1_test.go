package control_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// The knowledge_catalog_1 trigger lane ports the SQLite triggers on the
// knowledge catalog's definition blobs, mutation idempotency receipts, list
// projections and selectors, projection ledgers and recovery audit to
// migrations/postgres/0014_knowledge_catalog_1_triggers.sql. Its scenarios
// live in testdata/postgres_triggers/knowledge_catalog_1.json; each one was
// run against SQLite and PostgreSQL when the lane was ported, and these tests
// hold PostgreSQL to the recorded outcomes.

const knowledgeCatalog1FixturePath = "testdata/postgres_triggers/knowledge_catalog_1.json"

// knowledgeCatalog1KnownLanes are the trigger lanes a scenario may depend on.
// A lane is present when the embedded migrations hold
// NNNN_<lane>_triggers.sql.
var knowledgeCatalog1KnownLanes = []string{
	"audit_1",
	"audit_2",
	"collectors_apps_search_1",
	"collectors_apps_search_2",
	"core_ingest_1",
	"core_ingest_2",
	"core_ingest_3",
	"knowledge_catalog_2",
	"knowledge_objects_1",
	"knowledge_objects_2",
	"lookups",
}

// knowledgeCatalog1Trigger is one trigger the lane installs. Each trigger
// runs a function of the same name, FOR EACH ROW, on one event.
type knowledgeCatalog1Trigger struct {
	name   string
	table  string
	timing string
	event  string
}

// knowledgeCatalog1Triggers lists every trigger in 0014. Trigger lanes
// partition tables, so these are all of the user triggers on the lane's six
// tables.
var knowledgeCatalog1Triggers = []knowledgeCatalog1Trigger{
	{"knowledge_definition_blob_after_insert", "knowledge_definition_blobs", "AFTER", "INSERT"},
	{"knowledge_definition_blob_capacity_is_available", "knowledge_definition_blobs", "BEFORE", "INSERT"},
	{"knowledge_definition_blob_delete_is_forbidden", "knowledge_definition_blobs", "BEFORE", "DELETE"},
	{"knowledge_definition_blob_identity_collision_is_forbidden", "knowledge_definition_blobs", "BEFORE", "INSERT"},
	{"knowledge_definition_blob_update_is_forbidden", "knowledge_definition_blobs", "BEFORE", "UPDATE"},
	{"knowledge_list_projection_after_delete", "knowledge_object_list_projections", "AFTER", "DELETE"},
	{"knowledge_list_projection_after_insert", "knowledge_object_list_projections", "AFTER", "INSERT"},
	{"knowledge_list_projection_capacity_is_available", "knowledge_object_list_projections", "BEFORE", "INSERT"},
	{"knowledge_list_projection_creates_order_key", "knowledge_object_list_projections", "AFTER", "INSERT"},
	{"knowledge_list_projection_delete_requires_unsealed_empty_row", "knowledge_object_list_projections", "BEFORE", "DELETE"},
	{"knowledge_list_projection_identity_collision_is_forbidden", "knowledge_object_list_projections", "BEFORE", "INSERT"},
	{"knowledge_list_projection_update_is_forbidden", "knowledge_object_list_projections", "BEFORE", "UPDATE"},
	{"knowledge_list_selector_identity_collision_is_forbidden", "knowledge_object_list_selector_patterns", "BEFORE", "INSERT"},
	{"knowledge_list_selector_ordinal_is_declared", "knowledge_object_list_selector_patterns", "BEFORE", "INSERT"},
	{"knowledge_list_selector_sealed_projection_is_immutable_delete", "knowledge_object_list_selector_patterns", "BEFORE", "DELETE"},
	{"knowledge_list_selector_sealed_projection_is_immutable_insert", "knowledge_object_list_selector_patterns", "BEFORE", "INSERT"},
	{"knowledge_list_selector_update_is_forbidden", "knowledge_object_list_selector_patterns", "BEFORE", "UPDATE"},
	{"knowledge_mutation_idempotency_after_delete", "knowledge_mutation_idempotency", "AFTER", "DELETE"},
	{"knowledge_mutation_idempotency_after_insert", "knowledge_mutation_idempotency", "AFTER", "INSERT"},
	{"knowledge_mutation_idempotency_capacity_is_available", "knowledge_mutation_idempotency", "BEFORE", "INSERT"},
	{"knowledge_mutation_idempotency_delete_in_retention_is_forbidden", "knowledge_mutation_idempotency", "BEFORE", "DELETE"},
	{"knowledge_mutation_idempotency_identity_collision_is_forbidden", "knowledge_mutation_idempotency", "BEFORE", "INSERT"},
	{"knowledge_mutation_idempotency_matches_audit_authority", "knowledge_mutation_idempotency", "BEFORE", "INSERT"},
	{"knowledge_mutation_idempotency_matches_commit_authority", "knowledge_mutation_idempotency", "BEFORE", "INSERT"},
	{"knowledge_mutation_idempotency_update_is_forbidden", "knowledge_mutation_idempotency", "BEFORE", "UPDATE"},
	{"knowledge_projection_ledger_delete_is_forbidden", "knowledge_projection_tenant_ledgers", "BEFORE", "DELETE"},
	{"knowledge_projection_ledger_identity_collision_is_forbidden", "knowledge_projection_tenant_ledgers", "BEFORE", "INSERT"},
	{"knowledge_projection_ledger_initial_shape_is_valid", "knowledge_projection_tenant_ledgers", "BEFORE", "INSERT"},
	{"knowledge_projection_ledger_transition_is_exact", "knowledge_projection_tenant_ledgers", "BEFORE", "UPDATE"},
	{"knowledge_recovery_audit_after_insert", "knowledge_recovery_audit", "AFTER", "INSERT"},
	{"knowledge_recovery_audit_capacity_is_available", "knowledge_recovery_audit", "BEFORE", "INSERT"},
	{"knowledge_recovery_audit_delete_is_forbidden", "knowledge_recovery_audit", "BEFORE", "DELETE"},
	{"knowledge_recovery_audit_identity_collision_is_forbidden", "knowledge_recovery_audit", "BEFORE", "INSERT"},
	{"knowledge_recovery_audit_matches_terminal_version", "knowledge_recovery_audit", "BEFORE", "INSERT"},
	{"knowledge_recovery_audit_update_is_forbidden", "knowledge_recovery_audit", "BEFORE", "UPDATE"},
}

// knowledgeCatalog1Scenario is one fixture entry. setup_sql runs inside the
// scenario transaction with search_path = open_splunk and every constraint
// IMMEDIATE, then statement runs and must be accepted or rejected as expect
// says. A rejection is a P0001 RAISE whose message equals message exactly,
// unless sqlstate names another SQLSTATE; then constraint names the violated
// constraint and message is PostgreSQL's full primary message. An accepted
// statement may be followed by check_sql, whose single text value must equal
// check_result. Everything is rolled back afterwards.
//
// depends_on_lanes lists other trigger lanes the outcome needs installed.
// Every scenario runs with every shipped trigger enabled, so those lanes only
// have to be embedded.
type knowledgeCatalog1Scenario struct {
	Name           string   `json:"name"`
	SetupSQL       []string `json:"setup_sql"`
	Statement      string   `json:"statement"`
	Expect         string   `json:"expect"`
	Message        string   `json:"message"`
	SQLState       string   `json:"sqlstate,omitempty"`
	Constraint     string   `json:"constraint,omitempty"`
	CheckSQL       string   `json:"check_sql,omitempty"`
	CheckResult    string   `json:"check_result,omitempty"`
	DependsOnLanes []string `json:"depends_on_lanes,omitempty"`
}

func loadKnowledgeCatalog1Scenarios(t *testing.T) []knowledgeCatalog1Scenario {
	t.Helper()
	contents, err := os.ReadFile(knowledgeCatalog1FixturePath)
	if err != nil {
		t.Fatalf("read %s: %v", knowledgeCatalog1FixturePath, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var scenarios []knowledgeCatalog1Scenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode %s: %v", knowledgeCatalog1FixturePath, err)
	}
	if decoder.More() {
		t.Fatalf("%s holds more than one JSON value", knowledgeCatalog1FixturePath)
	}
	if len(scenarios) == 0 {
		t.Fatalf("%s holds no scenarios", knowledgeCatalog1FixturePath)
	}
	return scenarios
}

// validateKnowledgeCatalog1Scenario reports a malformed scenario, so a typo in
// the fixture fails instead of quietly asserting less.
func validateKnowledgeCatalog1Scenario(scenario knowledgeCatalog1Scenario) error {
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
		if scenario.Message == "" {
			problems = append(problems, "a rejection has no message")
		}
		if scenario.CheckSQL != "" {
			problems = append(problems, "a rejected statement has check_sql")
		}
		if (scenario.SQLState == "") != (scenario.Constraint == "") {
			problems = append(problems, "sqlstate and constraint must be given together")
		}
		if scenario.SQLState == "P0001" {
			problems = append(problems, "a RAISE rejection must leave sqlstate empty")
		}
	default:
		problems = append(problems, "expect is "+scenario.Expect+", want accept or reject")
	}
	if (scenario.CheckSQL == "") != (scenario.CheckResult == "") {
		problems = append(problems, "check_sql and check_result must be given together")
	}
	for _, lane := range scenario.DependsOnLanes {
		if !slices.Contains(knowledgeCatalog1KnownLanes, lane) {
			problems = append(problems, "depends on unknown lane "+lane)
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(problems, "; "))
}

// runKnowledgeCatalog1Scenario executes one scenario in its own transaction on
// a dedicated connection and always rolls it back.
func runKnowledgeCatalog1Scenario(ctx context.Context, t *testing.T, db *sql.DB, scenario knowledgeCatalog1Scenario) {
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
	// SQLite checks foreign keys per statement in autocommit; checking the
	// deferred ones immediately keeps a scenario that rolls back from hiding
	// a violation PostgreSQL would only report at COMMIT.
	for _, statement := range []string{
		`SET LOCAL search_path TO ` + control.PostgresSchema,
		`SET CONSTRAINTS ALL IMMEDIATE`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
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
	var pgErr *pgconn.PgError
	if !errors.As(statementErr, &pgErr) {
		t.Fatalf("rejection is not a PostgreSQL server error: %v; want %q", statementErr, scenario.Message)
	}
	if scenario.SQLState == "" {
		message, raised := control.RaiseMessage(statementErr)
		if !raised {
			t.Fatalf("rejection is SQLSTATE %s (%v), want a P0001 RAISE %q", pgErr.Code, statementErr, scenario.Message)
		}
		if message != scenario.Message {
			t.Fatalf("RAISE message = %q, want %q", message, scenario.Message)
		}
		return
	}
	if pgErr.Code != scenario.SQLState || pgErr.ConstraintName != scenario.Constraint || pgErr.Message != scenario.Message {
		t.Fatalf("rejection = SQLSTATE %q constraint %q message %q, want SQLSTATE %q constraint %q message %q",
			pgErr.Code, pgErr.ConstraintName, pgErr.Message, scenario.SQLState, scenario.Constraint, scenario.Message)
	}
}

func runKnowledgeCatalog1Scenarios(t *testing.T, scenarios []knowledgeCatalog1Scenario) {
	t.Helper()
	db := testsupport.NewPostgresControlDB(t)
	names := make(map[string]bool, len(scenarios))
	for _, scenario := range scenarios {
		if err := validateKnowledgeCatalog1Scenario(scenario); err != nil {
			t.Fatalf("scenario %q is malformed: %v", scenario.Name, err)
		}
		if names[scenario.Name] {
			t.Fatalf("scenario name %q is used twice", scenario.Name)
		}
		names[scenario.Name] = true
		// The scenarios share one database and run one after another:
		// several disable a trigger, which takes an exclusive table lock.
		t.Run(scenario.Name, func(t *testing.T) {
			runKnowledgeCatalog1Scenario(t.Context(), t, db.SQLDB(), scenario)
		})
	}
}

// TestPostgresKnowledgeCatalog1TriggerScenarios runs every fixture scenario
// with every shipped trigger enabled. Every trigger lane is merged, so a
// scenario that depends on another lane runs here too; a lane it names that
// the embedded migrations lack fails the test instead of leaving the
// scenario pending.
func TestPostgresKnowledgeCatalog1TriggerScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadKnowledgeCatalog1Scenarios(t)
	if len(scenarios) == 0 {
		t.Fatal("the fixture holds no scenarios")
	}
	embedded := postgresEmbeddedTriggerLanes(t)
	requirePostgresTriggerLanesEmbedded(t, embedded, []string{"knowledge_catalog_1"})
	for _, scenario := range scenarios {
		requirePostgresTriggerLanesEmbedded(t, embedded, scenario.DependsOnLanes)
	}
	runKnowledgeCatalog1Scenarios(t, scenarios)
}

// TestPostgresKnowledgeCatalog1TriggerCatalogMatchesFixture checks that the
// lane's tables carry exactly the triggers 0014 declares, enabled, with the
// declared timing and event, each running its own function with search_path
// pinned to the control schema, and that every message those functions raise
// is the expected rejection of at least one fixture scenario.
func TestPostgresKnowledgeCatalog1TriggerCatalogMatchesFixture(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)

	tables := make([]string, 0, len(knowledgeCatalog1Triggers))
	want := make(map[string]knowledgeCatalog1Trigger, len(knowledgeCatalog1Triggers))
	for _, trigger := range knowledgeCatalog1Triggers {
		if !slices.Contains(tables, trigger.table) {
			tables = append(tables, trigger.table)
		}
		want[trigger.name] = trigger
	}
	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT trigger.tgname,
		       class.relname,
		       CASE WHEN trigger.tgtype & 2 <> 0 THEN 'BEFORE' ELSE 'AFTER' END,
		       concat_ws(',',
		           CASE WHEN trigger.tgtype & 4 <> 0 THEN 'INSERT' END,
		           CASE WHEN trigger.tgtype & 8 <> 0 THEN 'DELETE' END,
		           CASE WHEN trigger.tgtype & 16 <> 0 THEN 'UPDATE' END,
		           CASE WHEN trigger.tgtype & 32 <> 0 THEN 'TRUNCATE' END),
		       trigger.tgtype & 1 <> 0,
		       trigger.tgtype & 64 <> 0,
		       trigger.tgenabled::text,
		       trigger.tgqual IS NOT NULL,
		       trigger.tgnargs,
		       cardinality(trigger.tgattr::int2[]),
		       function.proname,
		       function_namespace.nspname,
		       coalesce(array_to_string(function.proconfig, ','), '')
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		JOIN pg_catalog.pg_proc AS function ON function.oid = trigger.tgfoid
		JOIN pg_catalog.pg_namespace AS function_namespace ON function_namespace.oid = function.pronamespace
		WHERE namespace.nspname = $1
		  AND class.relname = ANY ($2)
		  AND NOT trigger.tgisinternal
		ORDER BY trigger.tgname`,
		control.PostgresSchema, tables)
	if err != nil {
		t.Fatalf("list lane triggers: %v", err)
	}
	var installed []string
	for rows.Next() {
		var (
			name, table, timing, events, enabled, function, functionSchema, config string
			row, insteadOf, hasWhen                                                bool
			arguments, columns                                                     int
		)
		if err := rows.Scan(&name, &table, &timing, &events, &row, &insteadOf, &enabled, &hasWhen,
			&arguments, &columns, &function, &functionSchema, &config); err != nil {
			t.Fatalf("scan lane trigger: %v", err)
		}
		installed = append(installed, name)
		expected, known := want[name]
		if !known {
			t.Errorf("trigger %s on %s is not declared by the lane", name, table)
			continue
		}
		if table != expected.table || timing != expected.timing || events != expected.event {
			t.Errorf("trigger %s = %s %s ON %s, want %s %s ON %s",
				name, timing, events, table, expected.timing, expected.event, expected.table)
		}
		if !row || insteadOf || hasWhen || arguments != 0 || columns != 0 {
			t.Errorf("trigger %s: row %t, instead of %t, WHEN %t, %d arguments, %d UPDATE OF columns; "+
				"want a plain FOR EACH ROW trigger", name, row, insteadOf, hasWhen, arguments, columns)
		}
		if enabled != "O" {
			t.Errorf("trigger %s is enabled as %q, want O (origin and local)", name, enabled)
		}
		if function != name || functionSchema != control.PostgresSchema {
			t.Errorf("trigger %s runs %s.%s, want %s.%s", name, functionSchema, function, control.PostgresSchema, name)
		}
		if config != "search_path="+control.PostgresSearchPath {
			t.Errorf("trigger %s function config = %q, want search_path=%s", name, config, control.PostgresSearchPath)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("read lane triggers: %v", err)
	}
	for name := range want {
		if !slices.Contains(installed, name) {
			t.Errorf("declared trigger %s is not installed", name)
		}
	}

	raised := knowledgeCatalog1RaisedMessages(ctx, t, db.SQLDB())
	rejected := make(map[string]int)
	for _, scenario := range loadKnowledgeCatalog1Scenarios(t) {
		if scenario.Expect != "reject" || scenario.SQLState != "" {
			continue
		}
		if !slices.Contains(raised, scenario.Message) {
			t.Errorf("scenario %q expects RAISE %q, which no lane function raises", scenario.Name, scenario.Message)
		}
		rejected[scenario.Message]++
	}
	for _, message := range raised {
		if rejected[message] == 0 {
			t.Errorf("no fixture scenario is rejected with %q", message)
		}
	}
}

// knowledgeCatalog1RaisedMessages returns the sorted, distinct RAISE
// EXCEPTION messages in the bodies of the lane's trigger functions.
func knowledgeCatalog1RaisedMessages(ctx context.Context, t *testing.T, db *sql.DB) []string {
	t.Helper()
	functions := make([]string, 0, len(knowledgeCatalog1Triggers))
	for _, trigger := range knowledgeCatalog1Triggers {
		functions = append(functions, trigger.name)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT match[1]
		FROM pg_catalog.pg_proc AS function
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
		CROSS JOIN LATERAL regexp_matches(function.prosrc, 'RAISE EXCEPTION ''([^'']*)''', 'g') AS match
		WHERE namespace.nspname = $1 AND function.proname = ANY ($2)`,
		control.PostgresSchema, functions)
	if err != nil {
		t.Fatalf("list raised messages: %v", err)
	}
	var messages []string
	for rows.Next() {
		var message string
		if err := rows.Scan(&message); err != nil {
			t.Fatalf("scan raised message: %v", err)
		}
		messages = append(messages, message)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("read raised messages: %v", err)
	}
	// The lane raises 27 distinct messages; fewer means the pattern stopped
	// matching the function bodies and the coverage check would pass vacuously.
	if len(messages) != 27 {
		t.Fatalf("lane functions raise %d distinct messages, want 27: %v", len(messages), messages)
	}
	sort.Strings(messages)
	return messages
}

// TestPostgresKnowledgeCatalog1TriggersIgnoreTheSessionSearchPath fires the
// lane's capacity, collision and counter triggers from a session whose
// search_path is public and holds decoy tables with the same names. The
// functions pin search_path to the control schema, so they must read and
// write the open_splunk tables only.
func TestPostgresKnowledgeCatalog1TriggersIgnoreTheSessionSearchPath(t *testing.T) {
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
	// A full decoy counter would refuse the insert, and a decoy blob with the
	// same digest would report a collision, if the triggers read public.
	exec(`CREATE TABLE public.knowledge_catalog_tenants (tenant_id text, definition_body_bytes bigint)`)
	exec(`INSERT INTO public.knowledge_catalog_tenants VALUES ('t1', 536870912)`)
	exec(`CREATE TABLE public.knowledge_definition_blobs (tenant_id text, definition_digest bytea)`)
	exec(`INSERT INTO public.knowledge_definition_blobs
		VALUES ('t1', decode('0101010101010101010101010101010101010101010101010101010101010101', 'hex'))`)
	exec(`SET LOCAL session_replication_role = replica`)
	exec(`INSERT INTO open_splunk.tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro) VALUES ('t1', 1, 1)`)
	exec(`INSERT INTO open_splunk.knowledge_catalog_tenants (tenant_id) VALUES ('t1')`)
	exec(`SET LOCAL session_replication_role = origin`)
	exec(`INSERT INTO open_splunk.knowledge_definition_blobs
			(tenant_id, definition_digest, definition_proto, definition_bytes, created_at_unix_micro)
		VALUES ('t1', decode('0101010101010101010101010101010101010101010101010101010101010101', 'hex'),
			decode('5a5a5a5a5a5a5a5a', 'hex'), 8, 1000)`)

	var controlBytes, decoyBytes int64
	if err := tx.QueryRowContext(ctx, `
		SELECT definition_body_bytes FROM open_splunk.knowledge_catalog_tenants WHERE tenant_id = 't1'`).
		Scan(&controlBytes); err != nil {
		t.Fatalf("read control body bytes: %v", err)
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT definition_body_bytes FROM public.knowledge_catalog_tenants WHERE tenant_id = 't1'`).
		Scan(&decoyBytes); err != nil {
		t.Fatalf("read decoy body bytes: %v", err)
	}
	if controlBytes != 8 || decoyBytes != 536870912 {
		t.Fatalf("body bytes after insert = %d (decoy %d), want 8 (decoy untouched 536870912)", controlBytes, decoyBytes)
	}

	// The real blob now exists, so a second insert of its digest collides
	// through the pinned search_path.
	_, err = tx.ExecContext(ctx, `INSERT INTO open_splunk.knowledge_definition_blobs
			(tenant_id, definition_digest, definition_proto, definition_bytes, created_at_unix_micro)
		VALUES ('t1', decode('0101010101010101010101010101010101010101010101010101010101010101', 'hex'),
			decode('5a5a', 'hex'), 2, 2000)`)
	if message, raised := control.RaiseMessage(err); !raised || message != "knowledge definition blob already exists" {
		t.Fatalf("duplicate blob insert under search_path=public = %v, want RAISE %q",
			err, "knowledge definition blob already exists")
	}
}

// knowledgeCatalog1ParityScenario ports a direct write that a SQLite test in
// internal/knowledgecatalog issues against a lane table. Those tests drop one
// trigger by name to plant corrupt rows, so each write must be refused while
// the trigger is installed and, once only that trigger is disabled, accepted
// with no other side effect. Where the SQLite test also turns foreign keys
// off, the write must instead fail on the foreign key named by
// guardedByForeignKey, so a port of that test knows it must suspend the key
// as well. The scenario reuses the setup of the fixture scenario named base.
type knowledgeCatalog1ParityScenario struct {
	// mirrors names the SQLite tests that issue this write.
	mirrors string
	base    string
	// disable is the trigger the SQLite tests drop before the write.
	disable     string
	table       string
	name        string
	statement   string
	message     string
	checkSQL    string
	checkResult string
	// guardedByForeignKey names PostgreSQL's generated name for the foreign
	// key that refuses the write once the trigger is disabled, when the
	// SQLite test needs foreign keys off as well.
	guardedByForeignKey string
}

// knowledgeCatalog1ReceiptAuthorityForeignKey is PostgreSQL's generated name
// for the unnamed key from knowledge_mutation_idempotency to
// knowledge_mutation_commit_authorities in 0005.
const knowledgeCatalog1ReceiptAuthorityForeignKey = "knowledge_mutation_idempotenc_tenant_id_committed_catalog__fkey"

var knowledgeCatalog1ParityScenarios = []knowledgeCatalog1ParityScenario{
	{
		mirrors: "store_test.go TestQuarantineNeverReadsHistoricalDefinition, resolver_test.go, " +
			"graph_integration_test.go, remediation_test.go, integration_batch_hydration_adversarial_test.go, " +
			"integration_dependency_policy_adversarial_test.go",
		base:      "definition_blob_update_rejected",
		disable:   "knowledge_definition_blob_update_is_forbidden",
		table:     "knowledge_definition_blobs",
		name:      "definition_blob_body_corruption",
		statement: `UPDATE knowledge_definition_blobs SET definition_proto = '\x00'::bytea, definition_bytes = 1 WHERE tenant_id = 't1'`,
		message:   "knowledge definition blob is immutable",
		// No trigger re-charges the tenant for a rewritten body.
		checkSQL: `SELECT b.definition_bytes || ',' || encode(b.definition_proto, 'hex') || ',' || c.definition_body_bytes
			FROM knowledge_definition_blobs AS b JOIN knowledge_catalog_tenants AS c USING (tenant_id)`,
		checkResult: "1,00,8",
	},
	{
		mirrors:   "writer_replay_authorization_order_adversarial_test.go removeWriterReplayDefinitionBlobs",
		base:      "definition_blob_delete_rejected",
		disable:   "knowledge_definition_blob_delete_is_forbidden",
		table:     "knowledge_definition_blobs",
		name:      "definition_blob_removal",
		statement: `DELETE FROM knowledge_definition_blobs WHERE tenant_id = 't1'`,
		message:   "knowledge definition blob cannot be deleted",
		// No trigger refunds the tenant's body bytes on delete.
		checkSQL: `SELECT (SELECT count(*) FROM knowledge_definition_blobs) || ',' ||
			(SELECT definition_body_bytes FROM knowledge_catalog_tenants WHERE tenant_id = 't1')`,
		checkResult: "0,8",
	},
	{
		mirrors:   "store_test.go TestVisibleCorruptionFailsClosedWithoutDisclosingHiddenObject, remediation_test.go",
		base:      "list_projection_update_rejected",
		disable:   "knowledge_list_projection_update_is_forbidden",
		table:     "knowledge_object_list_projections",
		name:      "list_projection_description_tamper",
		statement: `UPDATE knowledge_object_list_projections SET description = 'tampered' WHERE tenant_id = 't1'`,
		message:   "knowledge list projection is immutable",
		// The generated charge follows the new description; the ledger keeps
		// the charge recorded at insert, so readers can detect the drift.
		checkSQL: `SELECT p.projection_bytes || ',' || l.projection_bytes
			FROM knowledge_object_list_projections AS p JOIN knowledge_projection_tenant_ledgers AS l USING (tenant_id)`,
		checkResult: "8,3",
	},
	{
		mirrors:   "privacy_nondisclosure_contract_test.go, integration_batch_hydration_adversarial_test.go",
		base:      "list_projection_update_rejected",
		disable:   "knowledge_list_projection_update_is_forbidden",
		table:     "knowledge_object_list_projections",
		name:      "list_projection_owner_tamper",
		statement: `UPDATE knowledge_object_list_projections SET owner_id = 'owner-b' WHERE tenant_id = 't1'`,
		message:   "knowledge list projection is immutable",
		// The SQLite tests turn foreign keys off too: the owner is part of the
		// projection's key into its object version.
		guardedByForeignKey: "knowledge_object_list_projec_tenant_id_knowledge_object_i_fkey1",
	},
	{
		mirrors:     "integration_batch_hydration_adversarial_test.go, remediation_test.go",
		base:        "list_selector_update_rejected",
		disable:     "knowledge_list_selector_update_is_forbidden",
		table:       "knowledge_object_list_selector_patterns",
		name:        "list_selector_value_tamper",
		statement:   `UPDATE knowledge_object_list_selector_patterns SET value = 'xxxxxx' WHERE tenant_id = 't1'`,
		message:     "knowledge list selector is immutable",
		checkSQL:    `SELECT value FROM knowledge_object_list_selector_patterns`,
		checkResult: "xxxxxx",
	},
	{
		mirrors:   "error_context_test.go, writer_replay_authorization_order_adversarial_test.go",
		base:      "mutation_idempotency_update_rejected",
		disable:   "knowledge_mutation_idempotency_update_is_forbidden",
		table:     "knowledge_mutation_idempotency",
		name:      "mutation_idempotency_outcome_tamper",
		statement: `UPDATE knowledge_mutation_idempotency SET outcome_proto = '\x00'::bytea WHERE tenant_id = 't1'`,
		message:   "knowledge mutation idempotency outcome is immutable",
		// Rewriting a receipt neither adds nor releases receipt capacity.
		checkSQL: `SELECT encode(i.outcome_proto, 'hex') || ',' || c.idempotency_count
			FROM knowledge_mutation_idempotency AS i JOIN knowledge_catalog_tenants AS c USING (tenant_id)`,
		checkResult: "00,1",
	},
	{
		mirrors:   "writer_replay_definition_binding_adversarial_test.go, writer_replay_authorization_order_adversarial_test.go",
		base:      "mutation_idempotency_update_rejected",
		disable:   "knowledge_mutation_idempotency_update_is_forbidden",
		table:     "knowledge_mutation_idempotency",
		name:      "mutation_idempotency_request_digest_tamper",
		statement: `UPDATE knowledge_mutation_idempotency SET request_digest = '\x` + strings.Repeat("00", 32) + `'::bytea WHERE tenant_id = 't1'`,
		message:   "knowledge mutation idempotency outcome is immutable",
		// The SQLite test turns foreign keys off: the digest is part of the
		// receipt's key into its commit authority.
		guardedByForeignKey: knowledgeCatalog1ReceiptAuthorityForeignKey,
	},
	{
		mirrors:   "writer_capacity_integration_test.go reclaim-prefix corruption",
		base:      "mutation_idempotency_update_rejected",
		disable:   "knowledge_mutation_idempotency_update_is_forbidden",
		table:     "knowledge_mutation_idempotency",
		name:      "mutation_idempotency_state_token_tamper",
		statement: `UPDATE knowledge_mutation_idempotency SET committed_catalog_state_token = '\x` + strings.Repeat("5a", 32) + `'::bytea WHERE tenant_id = 't1'`,
		message:   "knowledge mutation idempotency outcome is immutable",
		// The SQLite test turns foreign keys off: the state token is part of
		// the receipt's key into its commit authority.
		guardedByForeignKey: knowledgeCatalog1ReceiptAuthorityForeignKey,
	},
	{
		mirrors: "writer_capacity_integration_test.go shortened retention fence and seedCapacityReceiptCopies",
		base:    "mutation_idempotency_update_rejected",
		disable: "knowledge_mutation_idempotency_update_is_forbidden",
		table:   "knowledge_mutation_idempotency",
		name:    "mutation_idempotency_retention_tamper",
		statement: `UPDATE knowledge_mutation_idempotency
			SET retention_anchor_unix_micro = 1000, retain_until_unix_micro = 691200001000 WHERE tenant_id = 't1'`,
		message:     "knowledge mutation idempotency outcome is immutable",
		checkSQL:    `SELECT retain_until_unix_micro::text FROM knowledge_mutation_idempotency`,
		checkResult: "691200001000",
	},
}

// TestPostgresKnowledgeCatalog1TriggersMatchSQLiteStoreTests runs the parity
// writes above, each once with the lane's triggers installed (rejected) and
// once with only the trigger the SQLite test drops disabled (accepted).
func TestPostgresKnowledgeCatalog1TriggersMatchSQLiteStoreTests(t *testing.T) {
	t.Parallel()
	bases := make(map[string]knowledgeCatalog1Scenario)
	for _, scenario := range loadKnowledgeCatalog1Scenarios(t) {
		bases[scenario.Name] = scenario
	}
	scenarios := make([]knowledgeCatalog1Scenario, 0, 2*len(knowledgeCatalog1ParityScenarios))
	for _, parity := range knowledgeCatalog1ParityScenarios {
		base, ok := bases[parity.base]
		if !ok {
			t.Fatalf("parity scenario %s reuses unknown fixture scenario %s", parity.name, parity.base)
		}
		if !slices.ContainsFunc(knowledgeCatalog1Triggers, func(trigger knowledgeCatalog1Trigger) bool {
			return trigger.name == parity.disable && trigger.table == parity.table
		}) {
			t.Fatalf("parity scenario %s disables %s, which is not a lane trigger on %s",
				parity.name, parity.disable, parity.table)
		}
		if parity.mirrors == "" {
			t.Fatalf("parity scenario %s names no SQLite test", parity.name)
		}
		if (parity.guardedByForeignKey == "") == (parity.checkSQL == "") {
			t.Fatalf("parity scenario %s needs exactly one of a foreign key and a check query", parity.name)
		}
		guarded := knowledgeCatalog1Scenario{
			Name:      parity.name + "_rejected",
			SetupSQL:  slices.Clone(base.SetupSQL),
			Statement: parity.statement,
			Expect:    "reject",
			Message:   parity.message,
		}
		unguarded := knowledgeCatalog1Scenario{
			Name: parity.name + "_with_guard_disabled_accepted",
			SetupSQL: append(slices.Clone(base.SetupSQL),
				`ALTER TABLE `+parity.table+` DISABLE TRIGGER `+parity.disable),
			Statement:   parity.statement,
			Expect:      "accept",
			CheckSQL:    parity.checkSQL,
			CheckResult: parity.checkResult,
		}
		if parity.guardedByForeignKey != "" {
			unguarded.Name = parity.name + "_with_guard_disabled_rejected_by_foreign_key"
			unguarded.Expect = "reject"
			unguarded.SQLState = "23503"
			unguarded.Constraint = parity.guardedByForeignKey
			unguarded.Message = `insert or update on table "` + parity.table +
				`" violates foreign key constraint "` + parity.guardedByForeignKey + `"`
		}
		scenarios = append(scenarios, guarded, unguarded)
	}
	runKnowledgeCatalog1Scenarios(t, scenarios)
}
