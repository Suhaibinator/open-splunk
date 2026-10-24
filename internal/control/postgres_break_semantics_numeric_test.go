package control_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// These tests try to break the PostgreSQL port where its numeric and
// trigger semantics differ from SQLite's: int64 ceilings, 0/1 flags,
// generated columns, partial unique indexes, deferred foreign keys, UPDATE OF
// column filters, row-skipping BEFORE triggers (SQLite's RAISE(IGNORE)), and
// the trigger clock.
//
// The data-driven half lives in testdata/postgres_break/semantics_numeric.json.
// Every scenario there was also run against the SQLite schema built from
// migrations/sqlite (triggers dropped for seeding and recreated, the same
// subsets kept), and sqlite_expect / sqlite_message record that outcome.

const semNumFixturePath = "testdata/postgres_break/semantics_numeric.json"

// semNumScenario is one fixture entry.
//
// seed_sql runs with session_replication_role = replica, so neither user
// triggers nor foreign keys fire; CHECK and unique constraints still do.
// statements then run with every trigger and foreign key active, except that
// statements_without_triggers keeps the replica role, disable_triggers_on
// disables the user triggers of the listed tables, and keep_only_triggers
// leaves only the named user triggers of a table enabled. Every statement but
// the last must succeed. check_deferred runs SET CONSTRAINTS ALL IMMEDIATE
// afterwards, which reports a deferred foreign key exactly as COMMIT would.
//
// A rejection happens at reject_at ("statement", the default, or "deferred")
// and carries sqlstate (default P0001) and, when given, constraint and the
// exact RAISE message. An accepted scenario's check_sql must return
// check_result as its single text value. Everything is rolled back.
type semNumScenario struct {
	Name                      string              `json:"name"`
	Area                      string              `json:"area"`
	Note                      string              `json:"note,omitempty"`
	PGOnly                    bool                `json:"pg_only,omitempty"`
	SeedSQL                   []string            `json:"seed_sql"`
	StatementsWithoutTriggers bool                `json:"statements_without_triggers,omitempty"`
	DisableTriggersOn         []string            `json:"disable_triggers_on,omitempty"`
	KeepOnlyTriggers          map[string][]string `json:"keep_only_triggers,omitempty"`
	Statements                []string            `json:"statements"`
	CheckDeferred             bool                `json:"check_deferred,omitempty"`
	Expect                    string              `json:"expect"`
	RejectAt                  string              `json:"reject_at,omitempty"`
	SQLState                  string              `json:"sqlstate,omitempty"`
	Constraint                string              `json:"constraint,omitempty"`
	Message                   string              `json:"message,omitempty"`
	CheckSQL                  string              `json:"check_sql,omitempty"`
	CheckResult               string              `json:"check_result,omitempty"`

	// The SQLite half of the differential run; not executed here.
	SQLiteSeedSQL          []string            `json:"sqlite_seed_sql,omitempty"`
	SQLiteKeepOnlyTriggers map[string][]string `json:"sqlite_keep_only_triggers,omitempty"`
	SQLiteCheckSQL         string              `json:"sqlite_check_sql,omitempty"`
	SQLiteExpect           string              `json:"sqlite_expect,omitempty"`
	SQLiteMessage          string              `json:"sqlite_message,omitempty"`
}

var semNumAreas = []string{
	"cascade", "ceiling", "clock", "deferred_fk", "generated", "partial_unique", "update_of",
}

var semNumIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func loadSemNumScenarios(t *testing.T) []semNumScenario {
	t.Helper()
	contents, err := os.ReadFile(semNumFixturePath)
	if err != nil {
		t.Fatalf("read %s: %v", semNumFixturePath, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var scenarios []semNumScenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode %s: %v", semNumFixturePath, err)
	}
	if decoder.More() {
		t.Fatalf("%s holds more than one JSON value", semNumFixturePath)
	}
	return scenarios
}

// validateSemNumScenario rejects a malformed scenario, so a typo fails the
// suite instead of quietly asserting less.
func validateSemNumScenario(scenario semNumScenario) error {
	var problems []string
	if !strings.HasPrefix(scenario.Name, scenario.Area+"/") || !slices.Contains(semNumAreas, scenario.Area) {
		problems = append(problems, "name must start with a known area")
	}
	if len(scenario.Statements) == 0 {
		problems = append(problems, "no statements")
	}
	tables := append([]string(nil), scenario.DisableTriggersOn...)
	for table, names := range scenario.KeepOnlyTriggers {
		tables = append(tables, table)
		tables = append(tables, names...)
	}
	for _, name := range tables {
		if !semNumIdentifier.MatchString(name) {
			problems = append(problems, "unsafe identifier "+name)
		}
	}
	if scenario.StatementsWithoutTriggers && (len(scenario.DisableTriggersOn) > 0 || len(scenario.KeepOnlyTriggers) > 0) {
		problems = append(problems, "statements_without_triggers already disables every trigger")
	}
	switch scenario.Expect {
	case "accept":
		if scenario.RejectAt != "" || scenario.SQLState != "" || scenario.Constraint != "" || scenario.Message != "" {
			problems = append(problems, "an accepted scenario carries rejection details")
		}
		if (scenario.CheckSQL == "") != (scenario.CheckResult == "") {
			problems = append(problems, "check_sql and check_result go together")
		}
	case "reject":
		if scenario.CheckSQL != "" {
			problems = append(problems, "a rejected scenario has check_sql")
		}
		if scenario.RejectAt != "" && scenario.RejectAt != "statement" && scenario.RejectAt != "deferred" {
			problems = append(problems, "reject_at must be statement or deferred")
		}
		if scenario.RejectAt == "deferred" && !scenario.CheckDeferred {
			problems = append(problems, "a deferred rejection needs check_deferred")
		}
		if scenario.Constraint != "" && scenario.SQLState == "" {
			problems = append(problems, "a constraint needs its SQLSTATE")
		}
	default:
		problems = append(problems, "expect must be accept or reject")
	}
	// The differential record must agree with the PostgreSQL expectation:
	// a divergence is either fixed or documented with pg_only.
	if scenario.PGOnly {
		if scenario.SQLiteExpect != "" {
			problems = append(problems, "a pg_only scenario records a SQLite outcome")
		}
	} else {
		if scenario.SQLiteExpect != scenario.Expect {
			problems = append(problems, fmt.Sprintf("SQLite outcome %q differs from %q", scenario.SQLiteExpect, scenario.Expect))
		}
		if scenario.Expect == "reject" && scenario.Message != "" && scenario.SQLiteMessage != scenario.Message {
			problems = append(problems, fmt.Sprintf("SQLite message %q differs from %q", scenario.SQLiteMessage, scenario.Message))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(problems, "; "))
}

func TestPostgresBreakSemanticsNumericScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadSemNumScenarios(t)
	if len(scenarios) < 30 {
		t.Fatalf("%s holds %d scenarios, want at least 30", semNumFixturePath, len(scenarios))
	}
	db := testsupport.NewPostgresControlDB(t)
	seen := make(map[string]bool, len(scenarios))
	perArea := make(map[string]int)
	for _, scenario := range scenarios {
		if err := validateSemNumScenario(scenario); err != nil {
			t.Fatalf("scenario %q is malformed: %v", scenario.Name, err)
		}
		if seen[scenario.Name] {
			t.Fatalf("scenario %q appears twice", scenario.Name)
		}
		seen[scenario.Name] = true
		perArea[scenario.Area]++
		// One database, one scenario at a time: ALTER TABLE ... DISABLE
		// TRIGGER takes an exclusive lock until the rollback.
		t.Run(scenario.Name, func(t *testing.T) {
			runSemNumScenario(t.Context(), t, db.SQLDB(), scenario)
		})
	}
	for _, area := range semNumAreas {
		if perArea[area] == 0 {
			t.Errorf("area %s has no scenario", area)
		}
	}
}

func runSemNumScenario(ctx context.Context, t *testing.T, db *sql.DB, scenario semNumScenario) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin scenario transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("roll back scenario: %v", err)
		}
	}()
	mustExec := func(label, statement string) {
		t.Helper()
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s failed: %v\n%s", label, err, statement)
		}
	}
	mustExec("enter replica role", `SET LOCAL session_replication_role = replica`)
	for index, statement := range scenario.SeedSQL {
		mustExec(fmt.Sprintf("seed statement %d", index+1), statement)
	}
	if !scenario.StatementsWithoutTriggers {
		mustExec("leave replica role", `SET LOCAL session_replication_role = origin`)
	}
	for _, table := range scenario.DisableTriggersOn {
		mustExec("disable triggers", `ALTER TABLE `+table+` DISABLE TRIGGER USER`)
	}
	tables := make([]string, 0, len(scenario.KeepOnlyTriggers))
	for table := range scenario.KeepOnlyTriggers {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		mustExec("disable triggers", `ALTER TABLE `+table+` DISABLE TRIGGER USER`)
		for _, name := range scenario.KeepOnlyTriggers[table] {
			mustExec("re-enable trigger", `ALTER TABLE `+table+` ENABLE TRIGGER `+name)
		}
	}

	last := len(scenario.Statements) - 1
	for index, statement := range scenario.Statements[:last] {
		mustExec(fmt.Sprintf("statement %d", index+1), statement)
	}
	rejectedAt := ""
	_, outcome := tx.ExecContext(ctx, scenario.Statements[last])
	if outcome != nil {
		rejectedAt = "statement"
	} else if scenario.CheckDeferred {
		if _, outcome = tx.ExecContext(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); outcome != nil {
			rejectedAt = "deferred"
		}
	}

	if scenario.Expect == "accept" {
		if outcome != nil {
			t.Fatalf("rejected at %s, want accepted: %v", rejectedAt, outcome)
		}
		if scenario.CheckSQL == "" {
			return
		}
		var got sql.NullString
		if err := tx.QueryRowContext(ctx, scenario.CheckSQL).Scan(&got); err != nil {
			t.Fatalf("check query failed: %v\n%s", err, scenario.CheckSQL)
		}
		if !got.Valid || got.String != scenario.CheckResult {
			t.Fatalf("check = %q (valid %t), want %q", got.String, got.Valid, scenario.CheckResult)
		}
		return
	}
	if outcome == nil {
		t.Fatalf("accepted, want rejected (%s %s)", scenario.SQLState, scenario.Message)
	}
	wantAt := scenario.RejectAt
	if wantAt == "" {
		wantAt = "statement"
	}
	if rejectedAt != wantAt {
		t.Fatalf("rejected at %s, want at %s: %v", rejectedAt, wantAt, outcome)
	}
	code, ok := control.PostgresErrorCode(outcome)
	if !ok {
		t.Fatalf("rejection carries no SQLSTATE: %v", outcome)
	}
	wantCode := scenario.SQLState
	if wantCode == "" && scenario.Message != "" {
		wantCode = "P0001"
	}
	if wantCode != "" && code != wantCode {
		t.Fatalf("SQLSTATE = %s, want %s: %v", code, wantCode, outcome)
	}
	if wantCode == "" && code == "22003" {
		// An unpinned rejection may be any integrity or RAISE error, never
		// an arithmetic overflow escaping a trigger that SQLite reports.
		t.Fatalf("rejection is a bigint overflow (22003): %v", outcome)
	}
	if scenario.Constraint != "" {
		if constraint, _ := control.PostgresConstraintName(outcome); constraint != scenario.Constraint {
			t.Fatalf("constraint = %q, want %q: %v", constraint, scenario.Constraint, outcome)
		}
	}
	if scenario.Message != "" {
		if message, _ := control.RaiseMessage(outcome); message != scenario.Message {
			t.Fatalf("RAISE message = %q, want %q", message, scenario.Message)
		}
	}
}

// semNumTrigger is one user trigger of the open_splunk schema.
type semNumTrigger struct {
	table, name, function, source    string
	before, row, insert, update, del bool
	updateOf                         []string
	hasWhen                          bool
}

func readSemNumTriggers(ctx context.Context, t *testing.T, db *sql.DB) []semNumTrigger {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT c.relname::text, tg.tgname::text, p.proname::text, p.prosrc,
		       (tg.tgtype & 2) <> 0, (tg.tgtype & 1) <> 0,
		       (tg.tgtype & 4) <> 0, (tg.tgtype & 16) <> 0, (tg.tgtype & 8) <> 0,
		       (tg.tgtype & 64) <> 0,
		       coalesce((
		           SELECT string_agg(a.attname::text, ',' ORDER BY a.attname)
		           FROM unnest(tg.tgattr::int2[]) AS k (attnum)
		           JOIN pg_catalog.pg_attribute AS a ON a.attrelid = tg.tgrelid AND a.attnum = k.attnum
		       ), ''),
		       tg.tgqual IS NOT NULL
		FROM pg_catalog.pg_trigger AS tg
		JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		JOIN pg_catalog.pg_proc AS p ON p.oid = tg.tgfoid
		WHERE n.nspname = $1 AND NOT tg.tgisinternal
		ORDER BY 1, 2`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list PostgreSQL triggers: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close trigger rows: %v", err)
		}
	}()
	var triggers []semNumTrigger
	for rows.Next() {
		var trigger semNumTrigger
		var instead bool
		var updateOf string
		if err := rows.Scan(&trigger.table, &trigger.name, &trigger.function, &trigger.source,
			&trigger.before, &trigger.row, &trigger.insert, &trigger.update, &trigger.del,
			&instead, &updateOf, &trigger.hasWhen); err != nil {
			t.Fatalf("scan trigger: %v", err)
		}
		if instead {
			t.Errorf("trigger %s on %s is INSTEAD OF; the schema has no views", trigger.name, trigger.table)
		}
		if updateOf != "" {
			trigger.updateOf = strings.Split(updateOf, ",")
		}
		triggers = append(triggers, trigger)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate triggers: %v", err)
	}
	if len(triggers) == 0 {
		t.Fatal("the migrated schema has no triggers")
	}
	return triggers
}

var (
	semNumLineComment  = regexp.MustCompile(`--[^\n]*`)
	semNumStringLit    = regexp.MustCompile(`'(?:[^']|'')*'`)
	semNumReturn       = regexp.MustCompile(`(?i)\bRETURN\s+([A-Za-z_]+)`)
	semNumTerminalStmt = regexp.MustCompile(`(?is)(?:\bRETURN\s+(?:NEW|OLD)|\bRAISE\s+EXCEPTION\b[^;]*);\s*END;?\s*$`)
)

// semNumCode strips comments and string literals from a PL/pgSQL body so
// keyword searches see only code.
func semNumCode(source string) string {
	return semNumStringLit.ReplaceAllString(semNumLineComment.ReplaceAllString(source, ""), "''")
}

// SQLite's RAISE(IGNORE) silently skips the row; its PostgreSQL equivalent is
// a BEFORE ROW trigger returning NULL. A BEFORE UPDATE trigger that returns
// OLD silently reverts the update, and one returning NEW from a DELETE
// skips the delete. SQLite's schema uses RAISE(IGNORE) nowhere, so no
// PostgreSQL trigger may do any of these, and none may fall off its end
// (2F005) on a path the scenarios do not reach.
func TestPostgresBreakBeforeRowTriggersNeverSkipOrRevertTheRow(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	sqliteTriggers := readSemNumSQLiteTriggers(ctx, t)
	for name, trigger := range sqliteTriggers {
		if regexp.MustCompile(`(?i)RAISE\s*\(\s*IGNORE`).MatchString(trigger.sql) {
			t.Errorf("SQLite trigger %s uses RAISE(IGNORE); the port assumes none does", name)
		}
	}

	db := testsupport.NewPostgresControlDB(t)
	checked := 0
	for _, trigger := range readSemNumTriggers(ctx, t, db.SQLDB()) {
		if !trigger.before || !trigger.row {
			continue
		}
		checked++
		label := trigger.name + " on " + trigger.table
		events := 0
		for _, fires := range []bool{trigger.insert, trigger.update, trigger.del} {
			if fires {
				events++
			}
		}
		if events != 1 {
			t.Errorf("%s fires on %d events; a shared body must branch on TG_OP", label, events)
			continue
		}
		want := "NEW"
		if trigger.del {
			want = "OLD"
		}
		code := semNumCode(trigger.source)
		for _, match := range semNumReturn.FindAllStringSubmatch(code, -1) {
			if !strings.EqualFold(match[1], want) {
				t.Errorf("%s (%s) has RETURN %s, want only RETURN %s", label, trigger.function, match[1], want)
			}
		}
		if !semNumTerminalStmt.MatchString(code) {
			t.Errorf("%s (%s) can reach END without RETURN or RAISE", label, trigger.function)
		}
	}
	if checked < 100 {
		t.Fatalf("checked %d BEFORE ROW triggers, want the full schema", checked)
	}
}

// semNumSQLiteTrigger is a trigger of the SQLite schema built by control.Open.
type semNumSQLiteTrigger struct {
	table, timing, event string
	updateOf             []string
	sql                  string
}

var semNumSQLiteSignature = regexp.MustCompile(
	`(?is)^CREATE\s+TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?"?\w+"?\s+(BEFORE|AFTER|INSTEAD\s+OF)?\s*(INSERT|DELETE|UPDATE)(?:\s+OF\s+(.*?))?\s+ON\s+"?(\w+)"?`)

func readSemNumSQLiteTriggers(ctx context.Context, t *testing.T) map[string]semNumSQLiteTrigger {
	t.Helper()
	sqliteDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open SQLite control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteDB.Close(); err != nil {
			t.Errorf("close SQLite control plane: %v", err)
		}
	})
	rows, err := sqliteDB.SQLDB().QueryContext(ctx, `SELECT name, sql FROM sqlite_schema WHERE type = 'trigger'`)
	if err != nil {
		t.Fatalf("list SQLite triggers: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close SQLite trigger rows: %v", err)
		}
	}()
	triggers := make(map[string]semNumSQLiteTrigger)
	for rows.Next() {
		var name, statement string
		if err := rows.Scan(&name, &statement); err != nil {
			t.Fatalf("scan SQLite trigger: %v", err)
		}
		match := semNumSQLiteSignature.FindStringSubmatch(strings.TrimSpace(statement))
		if match == nil {
			t.Fatalf("cannot parse SQLite trigger %s", name)
		}
		trigger := semNumSQLiteTrigger{
			table:  strings.ToLower(match[4]),
			timing: strings.ToUpper(match[1]),
			event:  strings.ToUpper(match[2]),
			sql:    statement,
		}
		if trigger.timing == "" {
			trigger.timing = "BEFORE"
		}
		if match[3] != "" {
			for column := range strings.SplitSeq(match[3], ",") {
				trigger.updateOf = append(trigger.updateOf, strings.ToLower(strings.Trim(strings.TrimSpace(column), `"`)))
			}
			sort.Strings(trigger.updateOf)
		}
		triggers[name] = trigger
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate SQLite triggers: %v", err)
	}
	return triggers
}

// semNumRenamedTriggers maps SQLite trigger names longer than PostgreSQL's
// 63-byte identifier limit to their ported names.
var semNumRenamedTriggers = map[string]string{
	"knowledge_app_type_active_counter_identity_collision_is_forbidden":   "knowledge_app_type_active_counter_identity_collision_is_forbidd",
	"knowledge_lookup_definition_ordinary_version_capacity_is_available":  "knowledge_lookup_definition_ordinary_version_capacity_available",
	"knowledge_mutation_idempotency_delete_before_retention_is_forbidden": "knowledge_mutation_idempotency_delete_in_retention_is_forbidden",
	"knowledge_object_version_lifecycle_identity_collision_is_forbidden":  "knowledge_object_version_lifecycle_identity_collision_is_forbid",
}

// semNumPostgresOnlyTrigger is a trigger SQLite never had: the tenant guards
// of 0007 and 0011, which keep rows from referencing or moving into another
// tenant where SQLite had no tenant to compare.
type semNumPostgresOnlyTrigger struct {
	table, event string
	updateOf     []string
}

var semNumPostgresOnlyTriggers = map[string]semNumPostgresOnlyTrigger{
	"indexes_tenant_is_immutable":                       {table: "indexes", event: "UPDATE", updateOf: []string{"tenant_id"}},
	"ingestion_token_index_requires_same_tenant_insert": {table: "ingestion_token_indexes", event: "INSERT"},
	"ingestion_token_index_requires_same_tenant_update": {table: "ingestion_token_indexes", event: "UPDATE", updateOf: []string{"index_id", "ingestion_token_id"}},
	"ingestion_token_tenant_is_immutable":               {table: "ingestion_tokens", event: "UPDATE", updateOf: []string{"tenant_id"}},
	"saved_search_tenant_is_immutable":                  {table: "saved_searches", event: "UPDATE", updateOf: []string{"tenant_id"}},
}

// semNumStatementTriggers are the documented FOR EACH STATEMENT ports
// (0018_audit_2_triggers.sql); every other trigger stays FOR EACH ROW like
// SQLite's.
var semNumStatementTriggers = []string{"api_mutation_receipt_after_delete", "api_mutation_receipt_after_insert"}

// An UPDATE OF column list decides whether a trigger fires at all, so a port
// that drops, adds or misspells one column silently changes which updates are
// checked. Every trigger must keep SQLite's table, timing, event and exact
// UPDATE OF list.
func TestPostgresBreakTriggerSignaturesMatchSQLite(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	sqliteTriggers := readSemNumSQLiteTriggers(ctx, t)
	db := testsupport.NewPostgresControlDB(t)
	postgresTriggers := make(map[string]semNumTrigger)
	for _, trigger := range readSemNumTriggers(ctx, t, db.SQLDB()) {
		postgresTriggers[trigger.name] = trigger
	}
	if want := len(sqliteTriggers) + len(semNumPostgresOnlyTriggers); len(postgresTriggers) != want {
		t.Errorf("PostgreSQL has %d triggers, want SQLite's %d plus %d PostgreSQL-only",
			len(postgresTriggers), len(sqliteTriggers), len(semNumPostgresOnlyTriggers))
	}
	for name, want := range semNumPostgresOnlyTriggers {
		if _, ok := sqliteTriggers[name]; ok {
			t.Errorf("%s is listed as PostgreSQL-only, but SQLite has it", name)
		}
		got, ok := postgresTriggers[name]
		if !ok {
			t.Errorf("PostgreSQL-only trigger %s is missing", name)
			continue
		}
		event := "DELETE"
		switch {
		case got.insert:
			event = "INSERT"
		case got.update:
			event = "UPDATE"
		}
		if got.table != want.table || !got.before || !got.row || event != want.event ||
			!slices.Equal(got.updateOf, want.updateOf) {
			t.Errorf("%s is not BEFORE %s OF %v ON %s FOR EACH ROW", name, want.event, want.updateOf, want.table)
		}
	}
	withUpdateOf := 0
	for sqliteName, want := range sqliteTriggers {
		name := sqliteName
		if renamed, ok := semNumRenamedTriggers[sqliteName]; ok {
			name = renamed
		}
		got, ok := postgresTriggers[name]
		if !ok {
			t.Errorf("SQLite trigger %s has no PostgreSQL port named %s", sqliteName, name)
			continue
		}
		timing := "AFTER"
		if got.before {
			timing = "BEFORE"
		}
		event := "DELETE"
		switch {
		case got.insert:
			event = "INSERT"
		case got.update:
			event = "UPDATE"
		}
		if got.table != want.table || timing != want.timing || event != want.event {
			t.Errorf("%s is %s %s ON %s, SQLite has %s %s ON %s",
				name, timing, event, got.table, want.timing, want.event, want.table)
		}
		if !slices.Equal(got.updateOf, want.updateOf) {
			t.Errorf("%s fires on UPDATE OF %v, SQLite on UPDATE OF %v", name, got.updateOf, want.updateOf)
		}
		if len(want.updateOf) > 0 {
			withUpdateOf++
		}
		if got.row == slices.Contains(semNumStatementTriggers, name) {
			t.Errorf("%s row-level = %t; only %v are statement-level", name, got.row, semNumStatementTriggers)
		}
	}
	if withUpdateOf < 40 {
		t.Fatalf("compared %d UPDATE OF triggers, want the full schema", withUpdateOf)
	}
}

// PostgreSQL computes STORED generated columns after BEFORE ROW triggers, so a
// BEFORE INSERT or UPDATE trigger sees NULL where SQLite saw the value. No such
// trigger may read NEW.<generated column> or hand NEW to a helper, and no
// UPDATE OF list may name a generated column: it can never be a SET target,
// so the trigger would never fire.
func TestPostgresBreakTriggersNeverReadUncomputedGeneratedColumns(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)
	generated := make(map[string][]string)
	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT c.relname::text, a.attname::text
		FROM pg_catalog.pg_attribute AS a
		JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND a.attgenerated = 's' AND NOT a.attisdropped
		ORDER BY 1, 2`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list generated columns: %v", err)
	}
	count := 0
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			t.Fatalf("scan generated column: %v", err)
		}
		generated[table] = append(generated[table], column)
		count++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("iterate generated columns: %v", err)
	}
	if count != 7 {
		t.Fatalf("the schema has %d generated columns, want the 7 the port notes list: %v", count, generated)
	}
	wholeRow := regexp.MustCompile(`(?i)[(,]\s*NEW\s*[,)]`)
	for _, trigger := range readSemNumTriggers(ctx, t, db.SQLDB()) {
		columns := generated[trigger.table]
		if len(columns) == 0 {
			continue
		}
		label := trigger.name + " on " + trigger.table
		for _, column := range trigger.updateOf {
			if slices.Contains(columns, column) {
				t.Errorf("%s lists generated column %s in UPDATE OF", label, column)
			}
		}
		if !trigger.before || trigger.del {
			continue
		}
		code := semNumCode(trigger.source)
		for _, column := range columns {
			if regexp.MustCompile(`(?i)\bNEW\s*\.\s*` + column + `\b`).MatchString(code) {
				t.Errorf("%s reads NEW.%s before PostgreSQL computes it", label, column)
			}
		}
		if wholeRow.MatchString(code) {
			t.Errorf("%s passes NEW, whose generated columns are still NULL, to a helper", label)
		}
	}
}

// SQLite flags are INTEGER columns with CHECK (x IN (0, 1)). The port keeps
// them bigint so store code scans int64; a boolean flag would make
// "flag = 1" a type error (42883). tenants.adopted_clickhouse is the one
// deliberate boolean.
func TestPostgresBreakFlagColumnsStayZeroOneIntegers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	sqliteFlags := make(map[string]bool)
	sqliteDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open SQLite control plane: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteDB.Close(); err != nil {
			t.Errorf("close SQLite control plane: %v", err)
		}
	})
	rows, err := sqliteDB.SQLDB().QueryContext(ctx, `SELECT name, sql FROM sqlite_schema WHERE type = 'table'`)
	if err != nil {
		t.Fatalf("list SQLite tables: %v", err)
	}
	flagCheck := regexp.MustCompile(`\b(\w+)\s+IN\s*\(\s*0\s*,\s*1\s*\)`)
	for rows.Next() {
		var table, statement string
		if err := rows.Scan(&table, &statement); err != nil {
			t.Fatalf("scan SQLite table: %v", err)
		}
		for _, match := range flagCheck.FindAllStringSubmatch(statement, -1) {
			sqliteFlags[table+"."+match[1]] = true
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("iterate SQLite tables: %v", err)
	}

	db := testsupport.NewPostgresControlDB(t)
	postgresFlags := make(map[string]bool)
	flagRows, err := db.SQLDB().QueryContext(ctx, `
		SELECT c.relname::text, a.attname::text, format_type(a.atttypid, a.atttypmod)
		FROM pg_catalog.pg_constraint AS k
		JOIN pg_catalog.pg_class AS c ON c.oid = k.conrelid
		JOIN pg_catalog.pg_attribute AS a ON a.attrelid = k.conrelid AND a.attnum = ANY (k.conkey)
		WHERE k.contype = 'c' AND k.connamespace = $1::regnamespace
		  AND pg_get_constraintdef(k.oid) ~ ('\m' || a.attname || '\M = ANY \(ARRAY\[\(0\)::bigint, \(1\)::bigint\]\)')`,
		control.PostgresSchema)
	if err != nil {
		t.Fatalf("list PostgreSQL flag columns: %v", err)
	}
	for flagRows.Next() {
		var table, column, columnType string
		if err := flagRows.Scan(&table, &column, &columnType); err != nil {
			t.Fatalf("scan flag column: %v", err)
		}
		if columnType != "bigint" {
			t.Errorf("flag %s.%s is %s, want bigint", table, column, columnType)
		}
		postgresFlags[table+"."+column] = true
	}
	if err := errors.Join(flagRows.Err(), flagRows.Close()); err != nil {
		t.Fatalf("iterate flag columns: %v", err)
	}
	if len(sqliteFlags) < 10 || !maps2Equal(sqliteFlags, postgresFlags) {
		t.Fatalf("0/1 flag columns: SQLite %v, PostgreSQL %v", sortedKeys(sqliteFlags), sortedKeys(postgresFlags))
	}

	var booleans []string
	boolRows, err := db.SQLDB().QueryContext(ctx, `
		SELECT table_name::text || '.' || column_name::text
		FROM information_schema.columns
		WHERE table_schema = $1 AND data_type = 'boolean'
		ORDER BY 1`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list boolean columns: %v", err)
	}
	for boolRows.Next() {
		var column string
		if err := boolRows.Scan(&column); err != nil {
			t.Fatalf("scan boolean column: %v", err)
		}
		booleans = append(booleans, column)
	}
	if err := errors.Join(boolRows.Err(), boolRows.Close()); err != nil {
		t.Fatalf("iterate boolean columns: %v", err)
	}
	if !slices.Equal(booleans, []string{"tenants.adopted_clickhouse"}) {
		t.Fatalf("boolean columns = %v, want only tenants.adopted_clickhouse", booleans)
	}
}

func maps2Equal(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if !right[key] {
			return false
		}
	}
	return true
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Store code ported from SQLite may still bind Go bools to 0/1 columns or
// int64s to the boolean tenants column. Neither may be coerced silently into
// a stored value; each must fail before anything is written.
func TestPostgresBreakBooleanBindingsFailInsteadOfCoercing(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro, adopted_clickhouse)
			VALUES ('flags', 1, 1, $1)`, true)
		return err
	})
	if err != nil {
		t.Fatalf("insert tenant with a Go bool: %v", err)
	}

	type probe struct {
		name      string
		statement string
		arg       any
	}
	probes := []probe{
		{"Go bool into a 0/1 flag", `UPDATE indexes SET search_enabled = $1 WHERE index_id = 'none'`, true},
		{"Go bool compared with a 0/1 flag", `SELECT count(*) FROM indexes WHERE search_enabled = $1`, false},
		{"text 'true' into a 0/1 flag", `UPDATE indexes SET search_enabled = $1 WHERE index_id = 'none'`, "true"},
		{"Go int64 into the boolean column", `UPDATE tenants SET adopted_clickhouse = $1 WHERE tenant_id = 'flags'`, int64(1)},
	}
	for _, probe := range probes {
		_, err := db.SQLDB().ExecContext(ctx, probe.statement, probe.arg)
		if err == nil {
			t.Errorf("%s was accepted, want an encoding or type error", probe.name)
			continue
		}
		// The driver refuses to encode (no SQLSTATE), or the server refuses
		// the value or the operator. Anything else means the probe broke.
		if code, fromServer := control.PostgresErrorCode(err); fromServer && !slices.Contains([]string{"22P02", "42804", "42883"}, code) {
			t.Errorf("%s failed with SQLSTATE %s (%v), want an encoding or type error", probe.name, code, err)
		}
	}
	for _, statement := range []string{
		`SELECT count(*) FROM indexes WHERE search_enabled = true`,
		`UPDATE indexes SET search_enabled = true WHERE index_id = 'none'`,
		`UPDATE tenants SET adopted_clickhouse = 1 WHERE tenant_id = 'flags'`,
	} {
		_, err := db.SQLDB().ExecContext(ctx, statement)
		code, _ := control.PostgresErrorCode(err)
		if code != "42883" && code != "42804" {
			t.Errorf("%s: error %v, want SQLSTATE 42883 or 42804", statement, err)
		}
	}

	// GORM passes a Go bool straight to the driver; on a bigint flag the
	// write must fail rather than store something other than 0/1.
	result := db.GORMDB().WithContext(ctx).Table("indexes").Where("index_id = ?", "none").
		Updates(map[string]any{"search_enabled": true})
	if result.Error == nil {
		t.Error("GORM wrote a Go bool into a 0/1 flag")
	}

	var adopted bool
	if err := db.SQLDB().QueryRowContext(ctx,
		`SELECT adopted_clickhouse FROM tenants WHERE tenant_id = 'flags'`).Scan(&adopted); err != nil {
		t.Fatalf("read adopted_clickhouse: %v", err)
	}
	if !adopted {
		t.Fatal("adopted_clickhouse = false after storing true; a probe changed it")
	}
}

// SQLite rolls back only the failed statement and keeps the transaction
// open; PostgreSQL aborts the whole transaction. Store code that swallows a
// constraint error and carries on must not see its earlier writes committed.
func TestPostgresBreakSwallowedStatementErrorAbortsTheWriteTx(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := newPostgresFixtureDB(t)

	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'kept?')`); err != nil {
			return err
		}
		// The error is ignored deliberately, as a ported "insert or
		// ignore" might.
		_, ignored := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (2, 'forbidden')`)
		if !control.IsCheckViolation(ignored) {
			return fmt.Errorf("probe insert = %w, want a CHECK violation", ignored)
		}
		return nil
	})
	if !errors.Is(err, pgx.ErrTxCommitRollback) {
		t.Fatalf("WriteTx() = %v, want pgx.ErrTxCommitRollback", err)
	}
	if got := countProbes(t, db); got != 0 {
		t.Fatalf("probes = %d after an aborted transaction, want 0", got)
	}

	gormErr := db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Exec(`INSERT INTO probes (id, label) VALUES (3, 'kept?')`).Error; err != nil {
			return err
		}
		_ = tx.Exec(`INSERT INTO probes (id, label) VALUES (4, 'forbidden')`)
		return nil
	})
	if !errors.Is(gormErr, pgx.ErrTxCommitRollback) {
		t.Fatalf("WriteGORMTx() = %v, want pgx.ErrTxCommitRollback", gormErr)
	}
	if got := countProbes(t, db); got != 0 {
		t.Fatalf("probes = %d after an aborted GORM transaction, want 0", got)
	}

	// A savepoint is the supported way to keep SQLite's per-statement
	// rollback.
	err = db.WriteTx(ctx, func(tx *sql.Tx) error {
		for _, statement := range []string{
			`INSERT INTO probes (id, label) VALUES (5, 'kept')`,
			`SAVEPOINT optional_write`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (6, 'forbidden')`); !control.IsCheckViolation(err) {
			return fmt.Errorf("probe insert = %w, want a CHECK violation", err)
		}
		_, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT optional_write`)
		return err
	})
	if err != nil {
		t.Fatalf("WriteTx() with a savepoint = %v", err)
	}
	if got := countProbes(t, db); got != 1 {
		t.Fatalf("probes = %d after the savepoint rollback, want 1", got)
	}
}

// The fixture mirrors the knowledge_objects / knowledge_object_versions
// cycle: a head names its current version through a deferred key.
const semNumDeferredBaseline = `
CREATE TABLE versions (
	head_id bigint NOT NULL,
	version bigint NOT NULL,
	PRIMARY KEY (head_id, version)
);

CREATE TABLE heads (
	id bigint PRIMARY KEY,
	current_version bigint NOT NULL,
	CONSTRAINT heads_current_version_fk FOREIGN KEY (id, current_version)
		REFERENCES versions (head_id, version) DEFERRABLE INITIALLY DEFERRED
);
`

func newSemNumDeferredDB(t *testing.T) *control.PostgresDB {
	t.Helper()
	return testsupport.NewPostgresControlDBWithMigrations(t, fstest.MapFS{
		"0001_baseline.sql": {Data: []byte(semNumDeferredBaseline)},
	})
}

// A deferred foreign key is checked at COMMIT, after the callback returned
// nil. WriteTx must report it as a foreign key violation naming the
// constraint, must not retry it, and must leave nothing behind; the cycle it
// exists for must still commit when both rows arrive in either order.
func TestPostgresBreakDeferredForeignKeyFailsAtCommitWithoutRetry(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := newSemNumDeferredDB(t)

	attempts := 0
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempts++
		_, err := tx.ExecContext(ctx, `INSERT INTO heads (id, current_version) VALUES (1, 1)`)
		return err
	})
	if !control.IsForeignKeyViolation(err) {
		t.Fatalf("WriteTx() = %v, want a foreign key violation at commit", err)
	}
	if constraint, _ := control.PostgresConstraintName(err); constraint != "heads_current_version_fk" {
		t.Fatalf("constraint = %q, want heads_current_version_fk", constraint)
	}
	if attempts != 1 {
		t.Fatalf("WriteTx ran %d attempts for a non-retryable commit failure", attempts)
	}

	gormErr := db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
		return tx.Exec(`INSERT INTO heads (id, current_version) VALUES (2, 1)`).Error
	})
	if !control.IsForeignKeyViolation(gormErr) {
		t.Fatalf("WriteGORMTx() = %v, want a foreign key violation at commit", gormErr)
	}

	for _, order := range [][]string{
		{`INSERT INTO heads (id, current_version) VALUES (3, 1)`, `INSERT INTO versions (head_id, version) VALUES (3, 1)`},
		{`INSERT INTO versions (head_id, version) VALUES (4, 1)`, `INSERT INTO heads (id, current_version) VALUES (4, 1)`},
	} {
		if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			for _, statement := range order {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("WriteTx(%v) = %v", order, err)
		}
	}
	var heads int
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM heads`).Scan(&heads); err != nil {
		t.Fatalf("count heads: %v", err)
	}
	if heads != 2 {
		t.Fatalf("heads = %d, want the 2 complete cycles", heads)
	}
}

var (
	semNumClockExpression = regexp.MustCompile(`(?i)clock_timestamp\s*\(\s*\)`)
	semNumExactClock      = regexp.MustCompile(`\(extract\(epoch FROM clock_timestamp\(\)\) \* 1000000\)::bigint`)
	semNumFrozenClock     = regexp.MustCompile(`(?i)\b(now\s*\(|current_timestamp|current_date|current_time|localtimestamp|localtime|transaction_timestamp|statement_timestamp|timeofday|unixepoch)`)
)

// The SQLite triggers read unixepoch('subsec'), which advances between
// statements. The port must use clock_timestamp() in exactly one
// microsecond-preserving form, never a transaction-start clock (now(),
// CURRENT_TIMESTAMP) and never anything TimeZone-dependent; and that form
// must agree with Go's clock under any session TimeZone.
func TestPostgresBreakTriggerClockIsMicrosecondWallTime(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)
	// Every function, not only trigger functions: helpers run inside the
	// triggers that call them.
	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT p.proname::text, p.prosrc
		FROM pg_catalog.pg_proc AS p
		WHERE p.pronamespace = $1::regnamespace
		ORDER BY 1`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list functions: %v", err)
	}
	functions, clockFunctions := 0, 0
	for rows.Next() {
		var name, source string
		if err := rows.Scan(&name, &source); err != nil {
			t.Fatalf("scan function: %v", err)
		}
		functions++
		code := semNumCode(source)
		if match := semNumFrozenClock.FindString(code); match != "" {
			t.Errorf("%s reads %s; trigger clocks must be clock_timestamp()", name, match)
		}
		uses := len(semNumClockExpression.FindAllString(code, -1))
		if uses == 0 {
			continue
		}
		clockFunctions++
		if exact := len(semNumExactClock.FindAllString(code, -1)); exact != uses {
			t.Errorf("%s reads clock_timestamp() %d times but only %d in the exact microsecond form", name, uses, exact)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("iterate functions: %v", err)
	}
	if functions < 200 || clockFunctions != 2 {
		t.Fatalf("%d of %d functions read the clock, want the 2 unixepoch('subsec') ports", clockFunctions, functions)
	}

	conn, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire connection: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("release connection: %v", err)
		}
	}()
	for _, zone := range []string{"UTC", "Pacific/Kiritimati", "America/St_Johns", "Asia/Kathmandu"} {
		if _, err := conn.ExecContext(ctx, `SELECT set_config('TimeZone', $1, false)`, zone); err != nil {
			t.Fatalf("set TimeZone %s: %v", zone, err)
		}
		before := time.Now().UnixMicro()
		var first, second int64
		if err := conn.QueryRowContext(ctx, `
			SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint,
			       (SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint FROM pg_sleep(0.002))`).
			Scan(&first, &second); err != nil {
			t.Fatalf("read trigger clock in %s: %v", zone, err)
		}
		after := time.Now().UnixMicro()
		// The database runs on this host in tests; allow for scheduling
		// only, so a seconds or milliseconds unit cannot pass.
		const slack = int64(2 * time.Second / time.Microsecond)
		if first < before-slack || first > after+slack {
			t.Errorf("clock in %s = %d, Go saw %d..%d", zone, first, before, after)
		}
		if second-first < 2000 {
			t.Errorf("clock in %s advanced %dus across a 2ms sleep in one statement; it is not wall time", zone, second-first)
		}
	}
	if _, err := conn.ExecContext(ctx, `SELECT set_config('TimeZone', 'UTC', false)`); err != nil {
		t.Fatalf("restore TimeZone: %v", err)
	}
}

// durable_search_jobs.row_cursor replaces SQLite's rowid as a listing
// high-water mark. It must never be reused after a delete, must refuse to
// wrap at the bigint ceiling, and cannot be written by callers.
func TestPostgresBreakRowCursorIdentityNeverReusesOrWraps(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)
	const insertJob = `
		INSERT INTO durable_search_jobs (id, tenant_id, owner_id, state, visibility, retention_class,
			lifetime_ns, job_payload, created_at_us, version)
		VALUES ($1, 't1', 'owner', 1, 1, 1, 1, '\x0a'::bytea, 1, 1)
		RETURNING row_cursor`
	insert := func(id string) (int64, error) {
		var cursor int64
		err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
				return err
			}
			return tx.QueryRowContext(ctx, insertJob, id).Scan(&cursor)
		})
		return cursor, err
	}
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro) VALUES ('t1', 1, 1)`)
		return err
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}

	first, err := insert("job-1")
	if err != nil {
		t.Fatalf("insert first job: %v", err)
	}
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM durable_search_jobs WHERE id = 'job-1'`)
		return err
	}); err != nil {
		t.Fatalf("delete first job: %v", err)
	}
	second, err := insert("job-2")
	if err != nil {
		t.Fatalf("insert second job: %v", err)
	}
	if second <= first {
		t.Fatalf("row_cursor %d after deleting %d; a cursor was reused", second, first)
	}

	_, err = db.SQLDB().ExecContext(ctx, `
		INSERT INTO durable_search_jobs (id, tenant_id, owner_id, state, visibility, retention_class,
			lifetime_ns, job_payload, created_at_us, version, row_cursor)
		VALUES ('job-x', 't1', 'owner', 1, 1, 1, 1, '\x0a'::bytea, 1, 1, 1)`)
	if code, _ := control.PostgresErrorCode(err); code != "428C9" {
		t.Fatalf("explicit row_cursor = %v, want SQLSTATE 428C9", err)
	}

	if _, err := db.SQLDB().ExecContext(ctx,
		`SELECT setval(pg_get_serial_sequence('durable_search_jobs', 'row_cursor'), 9223372036854775806)`); err != nil {
		t.Fatalf("advance row_cursor sequence: %v", err)
	}
	last, err := insert("job-max")
	if err != nil {
		t.Fatalf("insert at the ceiling: %v", err)
	}
	if last != 9223372036854775807 {
		t.Fatalf("row_cursor at the ceiling = %d, want 9223372036854775807", last)
	}
	_, err = insert("job-wrap")
	if code, _ := control.PostgresErrorCode(err); code != "2200H" {
		t.Fatalf("insert past the ceiling = %v, want SQLSTATE 2200H (sequence exhausted)", err)
	}
	var jobs int
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM durable_search_jobs`).Scan(&jobs); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if jobs != 2 {
		t.Fatalf("durable_search_jobs = %d rows, want job-2 and job-max", jobs)
	}
}
