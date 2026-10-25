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
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// Firing order across every trigger lane. SQLite fires the triggers of one
// table and event newest-created first; PostgreSQL fires them in name order
// (byte order, so RI_ConstraintTrigger_* foreign-key triggers come before
// every lowercase name). Each lane ported its own tables, so this file looks
// at the whole schema at once:
//
//   - testdata/postgres_break_firing_order/inventory.json pins every
//     same-table, same-timing, same-event group of two or more triggers with
//     its PostgreSQL order and the order the final SQLite schema fires it in.
//     Triggers with no SQLite counterpart (the tenant guards) are listed in
//     postgres_only and left out of the SQLite order.
//     A new trigger, a rename or a trigger moved between lanes changes which
//     check reports first, so it fails here until the inventory is reviewed.
//   - testdata/postgres_break_firing_order/scenarios.json holds rows that
//     break several triggers at once, FK cascades into triggered tables,
//     AFTER-trigger cascades and multi-row statements. Each one was run on
//     PostgreSQL and on a copy of the final SQLite schema (the sqlite_*
//     fields); these tests hold PostgreSQL to its recorded outcome and check
//     that every difference from SQLite is explained.
//
// Accept/reject matched SQLite in every scenario. Messages differ only where
// the inventory shows the two engines disagree on order (or, for multi-row
// statements, where AFTER ROW triggers are queued to the end of the
// statement), and no Go code matches any of those messages.

const (
	breakFiringOrderInventoryPath = "testdata/postgres_break_firing_order/inventory.json"
	breakFiringOrderScenariosPath = "testdata/postgres_break_firing_order/scenarios.json"
)

// firingOrderGroup is one inventory entry: the triggers PostgreSQL fires for
// one table, timing, level and event, in firing order. PostgresOnly names the
// triggers SQLite never had; SQLiteOrder lists the others in SQLite's order.
type firingOrderGroup struct {
	Table         string   `json:"table"`
	Timing        string   `json:"timing"`
	Level         string   `json:"level"`
	Event         string   `json:"event"`
	Migration     string   `json:"migration"`
	PostgresOrder []string `json:"postgres_order"`
	SQLiteOrder   []string `json:"sqlite_order"`
	PostgresOnly  []string `json:"postgres_only,omitempty"`
}

func (group firingOrderGroup) key() string {
	return strings.Join([]string{group.Table, group.Timing, group.Level, group.Event}, " ")
}

// firesInSQLiteOrder reports whether PostgreSQL runs exactly SQLite's
// triggers in SQLite's order. A group holding a PostgreSQL-only trigger never
// does: that trigger can report first where SQLite reported another check.
func (group firingOrderGroup) firesInSQLiteOrder() bool {
	return len(group.PostgresOnly) == 0 && slices.Equal(group.SQLiteOrder, group.PostgresOrder)
}

// firingOrderScenario is one scenario. setup_sql runs inside a transaction
// with search_path = open_splunk, then statement must be accepted or
// rejected as expect says: a P0001 RAISE with exactly message, or the
// SQLSTATE and constraint given. An accepted statement is followed by
// check_sql, whose rows (columns joined by "|", rows by newline) must equal
// check_result. Everything is rolled back afterwards.
//
// group names the inventory group ("table TIMING EVENT", row level) the
// statement exercises. sqlite_expect and sqlite_message record what the
// final SQLite schema did with the same setup and statement. consumer names
// Go code that classifies the message; such a scenario must report the
// SQLite message.
type firingOrderScenario struct {
	Name          string   `json:"name"`
	Group         string   `json:"group,omitempty"`
	SetupSQL      []string `json:"setup_sql"`
	Statement     string   `json:"statement"`
	Expect        string   `json:"expect"`
	Message       string   `json:"message,omitempty"`
	SQLState      string   `json:"sqlstate,omitempty"`
	Constraint    string   `json:"constraint,omitempty"`
	CheckSQL      string   `json:"check_sql,omitempty"`
	CheckResult   string   `json:"check_result,omitempty"`
	SQLiteExpect  string   `json:"sqlite_expect"`
	SQLiteMessage string   `json:"sqlite_message,omitempty"`
	Consumer      string   `json:"consumer,omitempty"`
	Note          string   `json:"note,omitempty"`
}

func decodeFiringOrderFixture(t *testing.T, path string, into any) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if decoder.More() {
		t.Fatalf("%s holds more than one JSON value", path)
	}
}

func loadFiringOrderInventory(t *testing.T) []firingOrderGroup {
	t.Helper()
	var groups []firingOrderGroup
	decodeFiringOrderFixture(t, breakFiringOrderInventoryPath, &groups)
	if len(groups) == 0 {
		t.Fatalf("%s holds no groups", breakFiringOrderInventoryPath)
	}
	return groups
}

func loadFiringOrderScenarios(t *testing.T) []firingOrderScenario {
	t.Helper()
	var scenarios []firingOrderScenario
	decodeFiringOrderFixture(t, breakFiringOrderScenariosPath, &scenarios)
	if len(scenarios) == 0 {
		t.Fatalf("%s holds no scenarios", breakFiringOrderScenariosPath)
	}
	return scenarios
}

// installedFiringOrderGroups reads every group of two or more user triggers
// from the catalog, each in the order PostgreSQL fires it.
func installedFiringOrderGroups(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `
		SELECT class.relname,
		       CASE
		           WHEN trigger.tgtype & 2 <> 0 THEN 'BEFORE'
		           WHEN trigger.tgtype & 64 <> 0 THEN 'INSTEAD OF'
		           ELSE 'AFTER'
		       END,
		       CASE WHEN trigger.tgtype & 1 <> 0 THEN 'ROW' ELSE 'STATEMENT' END,
		       event.name,
		       trigger.tgname
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		CROSS JOIN (VALUES ('INSERT', 4), ('DELETE', 8), ('UPDATE', 16), ('TRUNCATE', 32)) AS event (name, bit)
		WHERE namespace.nspname = $1
		  AND NOT trigger.tgisinternal
		  AND trigger.tgtype & event.bit <> 0
		ORDER BY 1, 2, 3, 4, trigger.tgname COLLATE "C"`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list triggers: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close trigger rows: %v", err)
		}
	}()
	all := make(map[string][]string)
	for rows.Next() {
		var group firingOrderGroup
		var name string
		if err := rows.Scan(&group.Table, &group.Timing, &group.Level, &group.Event, &name); err != nil {
			t.Fatalf("scan trigger: %v", err)
		}
		all[group.key()] = append(all[group.key()], name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate triggers: %v", err)
	}
	groups := make(map[string][]string)
	for key, names := range all {
		if len(names) > 1 {
			groups[key] = names
		}
	}
	return groups
}

// migrationTriggerFiles maps every trigger name to the embedded migration
// that creates it.
func migrationTriggerFiles(t *testing.T) (map[string]string, map[string]string) {
	t.Helper()
	migrationFS := migrations.Postgres()
	names, err := fs.Glob(migrationFS, "*.sql")
	if err != nil {
		t.Fatalf("list PostgreSQL migrations: %v", err)
	}
	create := regexp.MustCompile(`(?m)^CREATE (?:CONSTRAINT )?TRIGGER\s+(\w+)`)
	files := make(map[string]string)
	sources := make(map[string]string)
	for _, name := range names {
		contents, err := fs.ReadFile(migrationFS, name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		sources[name] = string(contents)
		for _, match := range create.FindAllStringSubmatch(string(contents), -1) {
			if previous, ok := files[match[1]]; ok {
				t.Fatalf("trigger %s is created by both %s and %s", match[1], previous, name)
			}
			files[match[1]] = name
		}
	}
	return files, sources
}

// TestPostgresBreakFiringOrderInventoryIsConsistent checks the inventory on
// its own: firing orders are in byte order, the SQLite order names the same
// triggers, each group comes from one lane migration, and a lane whose
// group fires in a different order than SQLite documents firing order.
func TestPostgresBreakFiringOrderInventoryIsConsistent(t *testing.T) {
	t.Parallel()
	files, sources := migrationTriggerFiles(t)
	seen := make(map[string]bool)
	disagreeing := 0
	for _, group := range loadFiringOrderInventory(t) {
		key := group.key()
		if seen[key] {
			t.Errorf("inventory lists %s twice", key)
		}
		seen[key] = true
		if len(group.PostgresOrder) < 2 {
			t.Errorf("%s: a group needs at least two triggers, got %v", key, group.PostgresOrder)
		}
		for index := 1; index < len(group.PostgresOrder); index++ {
			if group.PostgresOrder[index-1] >= group.PostgresOrder[index] {
				t.Errorf("%s: postgres_order %v is not strictly in byte order, which is how PostgreSQL fires triggers", key, group.PostgresOrder)
				break
			}
		}
		for _, name := range group.PostgresOnly {
			if slices.Contains(group.SQLiteOrder, name) {
				t.Errorf("%s: %s is listed in both postgres_only and sqlite_order", key, name)
			}
		}
		sorted := slices.Sorted(slices.Values(slices.Concat(group.SQLiteOrder, group.PostgresOnly)))
		if !slices.Equal(sorted, group.PostgresOrder) {
			t.Errorf("%s: sqlite_order %v with postgres_only %v names different triggers than postgres_order %v",
				key, group.SQLiteOrder, group.PostgresOnly, group.PostgresOrder)
		}
		for _, name := range group.PostgresOrder {
			if files[name] != group.Migration {
				t.Errorf("%s: trigger %s is created by %q, the inventory says %q", key, name, files[name], group.Migration)
			}
		}
		if !group.firesInSQLiteOrder() {
			disagreeing++
			if !strings.Contains(strings.ToLower(sources[group.Migration]), "firing order") {
				t.Errorf("%s fires in a different order than SQLite, but %s does not document firing order", key, group.Migration)
			}
		}
	}
	if disagreeing == 0 {
		t.Error("no inventory group fires in a different order than SQLite; the sqlite_order fields were probably overwritten")
	}
}

// TestPostgresBreakFiringOrderInventoryMatchesCatalog checks that the
// migrated schema holds exactly the inventoried multi-trigger groups.
func TestPostgresBreakFiringOrderInventoryMatchesCatalog(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	installed := installedFiringOrderGroups(t, db.SQLDB())
	want := make(map[string][]string)
	for _, group := range loadFiringOrderInventory(t) {
		want[group.key()] = group.PostgresOrder
	}
	for key, names := range installed {
		expected, ok := want[key]
		switch {
		case !ok:
			t.Errorf("%s has %d triggers %v but is not in %s; add it with its SQLite order", key, len(names), names, breakFiringOrderInventoryPath)
		case !slices.Equal(expected, names):
			t.Errorf("%s fires %v, inventory says %v", key, names, expected)
		}
	}
	for key := range want {
		if _, ok := installed[key]; !ok {
			t.Errorf("%s is inventoried but the schema no longer has two or more such triggers", key)
		}
	}
}

// reviewedBeforeTriggerWrites lists the BEFORE triggers that write to a
// table. Such a write runs before every later-sorted BEFORE trigger of the
// row, and before or after other checks in a different order than in SQLite,
// so each one is reviewed for order dependence:
//
//   - knowledge_object_active_counters_before_update decrements the counters
//     of the OLD app, type, owner and tenant. The only same-event reader,
//     knowledge_object_active_capacity_update, reads NEW keys and only when
//     the key changed (or the row was not active, when the decrement does
//     not run), so the two never touch the same counter row.
var reviewedBeforeTriggerWrites = []string{
	"knowledge_objects.knowledge_object_active_counters_before_update",
}

// TestPostgresBreakFiringOrderBeforeTriggerShapes checks two properties the
// firing-order analysis relies on: no BEFORE ROW trigger returns NULL (which
// would silently skip the row and every later trigger, where the SQLite
// trigger aborted), and the only BEFORE triggers that write are the reviewed
// ones.
func TestPostgresBreakFiringOrderBeforeTriggerShapes(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	rows, err := db.SQLDB().QueryContext(t.Context(), `
		SELECT class.relname, trigger.tgname, trigger.tgtype & 1 <> 0, function.prosrc
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
		JOIN pg_catalog.pg_proc AS function ON function.oid = trigger.tgfoid
		WHERE namespace.nspname = $1
		  AND NOT trigger.tgisinternal
		  AND trigger.tgtype & 2 <> 0
		ORDER BY 1, 2`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list BEFORE triggers: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close trigger rows: %v", err)
		}
	}()
	comment := regexp.MustCompile(`--[^\n]*`)
	returnsNull := regexp.MustCompile(`(?i)\breturn\s+null\b`)
	writes := regexp.MustCompile(`(?i)\b(insert\s+into|update\s+\w+\s+set|delete\s+from)\b`)
	var writers []string
	count := 0
	for rows.Next() {
		var table, name, source string
		var rowLevel bool
		if err := rows.Scan(&table, &name, &rowLevel, &source); err != nil {
			t.Fatalf("scan trigger: %v", err)
		}
		count++
		source = comment.ReplaceAllString(source, "")
		if rowLevel && returnsNull.MatchString(source) {
			t.Errorf("BEFORE ROW trigger %s.%s returns NULL, which skips the row instead of rejecting it", table, name)
		}
		if writes.MatchString(source) {
			writers = append(writers, table+"."+name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate triggers: %v", err)
	}
	if count == 0 {
		t.Fatal("the schema has no BEFORE triggers")
	}
	if !slices.Equal(writers, reviewedBeforeTriggerWrites) {
		t.Errorf("BEFORE triggers that write = %v, reviewed = %v", writers, reviewedBeforeTriggerWrites)
	}
}

// validateFiringOrderScenario reports a malformed scenario, and a difference
// from SQLite that the scenario does not explain.
func validateFiringOrderScenario(scenario firingOrderScenario, inventory map[string]firingOrderGroup) error {
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
			problems = append(problems, "an accepted statement carries a rejection")
		}
	case "reject":
		if scenario.CheckSQL != "" {
			problems = append(problems, "a rejected statement has check_sql")
		}
		if (scenario.Message == "") == (scenario.SQLState == "") {
			problems = append(problems, "a rejection needs exactly one of message and sqlstate")
		}
		if scenario.SQLState != "" && scenario.Constraint == "" {
			problems = append(problems, "a non-RAISE rejection names no constraint")
		}
	default:
		problems = append(problems, "expect is "+scenario.Expect+", want accept or reject")
	}
	if scenario.CheckSQL == "" && scenario.CheckResult != "" {
		problems = append(problems, "check_result without check_sql")
	}
	switch scenario.SQLiteExpect {
	case "accept":
		if scenario.SQLiteMessage != "" {
			problems = append(problems, "an accepted SQLite statement carries sqlite_message")
		}
	case "reject":
		if scenario.SQLiteMessage == "" {
			problems = append(problems, "a SQLite rejection has no sqlite_message")
		}
	default:
		problems = append(problems, "sqlite_expect is "+scenario.SQLiteExpect+", want accept or reject")
	}
	var group firingOrderGroup
	if scenario.Group != "" {
		fields := strings.Fields(scenario.Group)
		found := false
		if len(fields) == 3 {
			group, found = inventory[strings.Join([]string{fields[0], fields[1], "ROW", fields[2]}, " ")]
		}
		if !found {
			problems = append(problems, "group "+scenario.Group+" is not an inventoried row-level group")
		}
	}
	if scenario.Expect != scenario.SQLiteExpect {
		problems = append(problems, "PostgreSQL and SQLite disagree on accept/reject")
	}
	if scenario.Expect == "reject" && scenario.SQLiteExpect == "reject" && scenario.Message != scenario.SQLiteMessage {
		if scenario.Note == "" {
			problems = append(problems, "the message differs from SQLite and no note explains why")
		}
		if scenario.Message != "" && (scenario.Group == "" || group.firesInSQLiteOrder()) {
			problems = append(problems, "the RAISE message differs from SQLite in a group that fires in SQLite order")
		}
		if scenario.Consumer != "" {
			problems = append(problems, "Go code classifies this message ("+scenario.Consumer+") but PostgreSQL reports a different one")
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(problems, "; "))
}

// queryFiringOrderCheck renders every row of query as the scenario fixture
// records it.
func queryFiringOrderCheck(ctx context.Context, tx *sql.Tx, query string) (string, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	var lines []string
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return "", err
		}
		fields := make([]string, len(values))
		for index, value := range values {
			fields[index] = value.String
		}
		lines = append(lines, strings.Join(fields, "|"))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(lines, "\n"), nil
}

// runFiringOrderScenario executes one scenario in its own transaction on a
// dedicated connection and always rolls it back.
func runFiringOrderScenario(ctx context.Context, t *testing.T, db *sql.DB, scenario firingOrderScenario) {
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
	if _, err := tx.ExecContext(ctx, `SELECT set_config('search_path', $1, true)`, control.PostgresSchema); err != nil {
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
		got, err := queryFiringOrderCheck(ctx, tx, scenario.CheckSQL)
		if err != nil {
			t.Fatalf("check query failed: %v\n%s", err, scenario.CheckSQL)
		}
		if got != scenario.CheckResult {
			t.Fatalf("check %s = %q, want %q", scenario.CheckSQL, got, scenario.CheckResult)
		}
		return
	}
	if statementErr == nil {
		t.Fatalf("statement was accepted, want rejected\n%s", scenario.Statement)
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
		t.Fatalf("RAISE message = %q, want %q (SQLite reports %q)", message, scenario.Message, scenario.SQLiteMessage)
	}
}

// TestPostgresBreakFiringOrderScenariosAreExplained checks every scenario's
// shape and that each difference from SQLite is one firing order or row
// timing explains, without a database.
func TestPostgresBreakFiringOrderScenariosAreExplained(t *testing.T) {
	t.Parallel()
	inventory := make(map[string]firingOrderGroup)
	for _, group := range loadFiringOrderInventory(t) {
		inventory[group.key()] = group
	}
	names := make(map[string]bool)
	differing, consumers := 0, 0
	for _, scenario := range loadFiringOrderScenarios(t) {
		if names[scenario.Name] {
			t.Errorf("scenario name %q is used twice", scenario.Name)
		}
		names[scenario.Name] = true
		if err := validateFiringOrderScenario(scenario, inventory); err != nil {
			t.Errorf("scenario %q: %v", scenario.Name, err)
		}
		if scenario.Message != scenario.SQLiteMessage {
			differing++
		}
		if scenario.Consumer != "" {
			consumers++
		}
	}
	if differing == 0 || consumers == 0 {
		t.Errorf("the fixture has %d scenarios that differ from SQLite and %d Go-consumed ones; want some of each", differing, consumers)
	}
}

// TestPostgresBreakFiringOrderScenarios runs every scenario against the
// fully migrated schema. The scenarios share one database and run one after
// another: several disable triggers, which takes an exclusive table lock.
func TestPostgresBreakFiringOrderScenarios(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	for _, scenario := range loadFiringOrderScenarios(t) {
		t.Run(scenario.Name, func(t *testing.T) {
			runFiringOrderScenario(t.Context(), t, db.SQLDB(), scenario)
		})
	}
}

// TestPostgresBreakFiringOrderValidatorRejectsUnexplainedDifferences keeps
// the explanation rules honest: each one must reject the scenario it exists
// for.
func TestPostgresBreakFiringOrderValidatorRejectsUnexplainedDifferences(t *testing.T) {
	t.Parallel()
	inventory := map[string]firingOrderGroup{
		"t BEFORE ROW INSERT": {Table: "t", Timing: "BEFORE", Level: "ROW", Event: "INSERT", PostgresOrder: []string{"a", "b"}, SQLiteOrder: []string{"b", "a"}},
		"u BEFORE ROW INSERT": {Table: "u", Timing: "BEFORE", Level: "ROW", Event: "INSERT", PostgresOrder: []string{"a", "b"}, SQLiteOrder: []string{"a", "b"}},
		"w BEFORE ROW INSERT": {Table: "w", Timing: "BEFORE", Level: "ROW", Event: "INSERT", PostgresOrder: []string{"a", "b"}, SQLiteOrder: []string{"b"}, PostgresOnly: []string{"a"}},
	}
	base := firingOrderScenario{
		Name: "probe", Group: "t BEFORE INSERT", Statement: "SELECT 1", Expect: "reject",
		Message: "a fired", SQLiteExpect: "reject", SQLiteMessage: "b fired", Note: "a sorts first",
	}
	if err := validateFiringOrderScenario(base, inventory); err != nil {
		t.Fatalf("explained difference rejected: %v", err)
	}
	postgresOnly := base
	postgresOnly.Group = "w BEFORE INSERT"
	if err := validateFiringOrderScenario(postgresOnly, inventory); err != nil {
		t.Fatalf("difference explained by a PostgreSQL-only trigger rejected: %v", err)
	}
	cases := map[string]func(*firingOrderScenario){
		"no note":               func(scenario *firingOrderScenario) { scenario.Note = "" },
		"order agrees":          func(scenario *firingOrderScenario) { scenario.Group = "u BEFORE INSERT" },
		"no group":              func(scenario *firingOrderScenario) { scenario.Group = "" },
		"unknown group":         func(scenario *firingOrderScenario) { scenario.Group = "v BEFORE INSERT" },
		"consumed message":      func(scenario *firingOrderScenario) { scenario.Consumer = "some Go code" },
		"accept/reject differs": func(scenario *firingOrderScenario) { scenario.SQLiteExpect, scenario.SQLiteMessage = "accept", "" },
		"missing sqlite result": func(scenario *firingOrderScenario) { scenario.SQLiteExpect = "" },
		"both message and code": func(scenario *firingOrderScenario) { scenario.SQLState = "23514" },
	}
	for name, mutate := range cases {
		scenario := base
		mutate(&scenario)
		if err := validateFiringOrderScenario(scenario, inventory); err == nil {
			t.Errorf("%s: validator accepted %+v", name, scenario)
		}
	}
}
