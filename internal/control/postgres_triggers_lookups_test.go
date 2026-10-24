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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// The lookup trigger lane ports the SQLite triggers on the knowledge_lookup_*
// tables. Its fixture holds scenarios replayed against both engines when the
// port was written; these tests replay the PostgreSQL side against the
// shipped migration set and add the checks SQLite cannot express.
const (
	lookupTriggerFixturePath = "testdata/postgres_triggers/lookups.json"
	lookupTriggerMigration   = "0016_lookups_triggers.sql"
	lookupTriggerLane        = "lookups"
	postgresRaiseSQLState    = "P0001"
)

// postgresTriggerLanes names every trigger lane. A lane is installed when the
// embedded set holds NNNN_<lane>_triggers.sql.
var postgresTriggerLanes = []string{
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
	"knowledge_objects_2",
	"lookups",
}

// lookupTriggerRenames maps SQLite trigger names that exceed PostgreSQL's
// 63-byte identifier limit to their ported names.
var lookupTriggerRenames = map[string]string{
	"knowledge_lookup_definition_ordinary_version_capacity_is_available": "knowledge_lookup_definition_ordinary_version_capacity_available",
}

// lookupTriggerScenario is one fixture entry. Every statement in SetupSQL and
// Statement runs inside one transaction that is always rolled back.
type lookupTriggerScenario struct {
	Name      string   `json:"name"`
	SetupSQL  []string `json:"setup_sql"`
	Statement string   `json:"statement"`
	Expect    string   `json:"expect"`
	Message   string   `json:"message"`
	// DependsOnLanes lists other trigger lanes whose triggers the scenario
	// needs. Such scenarios run in TestPostgresLookupTriggerCrossLaneScenarios.
	DependsOnLanes []string `json:"depends_on_lanes,omitempty"`
}

func loadLookupTriggerScenarios(t *testing.T) []lookupTriggerScenario {
	t.Helper()
	contents, err := os.ReadFile(filepath.FromSlash(lookupTriggerFixturePath))
	if err != nil {
		t.Fatalf("read lookup trigger fixture: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	// An unknown field would be a scenario property this runner silently
	// ignores, so the fixture and the runner must agree on every field.
	decoder.DisallowUnknownFields()
	var scenarios []lookupTriggerScenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode lookup trigger fixture: %v", err)
	}
	if len(scenarios) == 0 {
		t.Fatal("lookup trigger fixture has no scenarios")
	}
	seen := make(map[string]bool, len(scenarios))
	for index, scenario := range scenarios {
		switch {
		case strings.TrimSpace(scenario.Name) == "":
			t.Fatalf("lookup trigger scenario %d has no name", index)
		case seen[scenario.Name]:
			t.Fatalf("lookup trigger scenario %q is duplicated", scenario.Name)
		case strings.TrimSpace(scenario.Statement) == "":
			t.Fatalf("lookup trigger scenario %q has no statement", scenario.Name)
		case scenario.Expect == "accept" && scenario.Message != "":
			t.Fatalf("lookup trigger scenario %q accepts but names message %q", scenario.Name, scenario.Message)
		case scenario.Expect == "reject" && scenario.Message == "":
			t.Fatalf("lookup trigger scenario %q rejects without a message", scenario.Name)
		case scenario.Expect != "accept" && scenario.Expect != "reject":
			t.Fatalf("lookup trigger scenario %q expects %q, want accept or reject", scenario.Name, scenario.Expect)
		}
		for _, lane := range scenario.DependsOnLanes {
			if lane == lookupTriggerLane || !slices.Contains(postgresTriggerLanes, lane) {
				t.Fatalf("lookup trigger scenario %q depends on unknown lane %q", scenario.Name, lane)
			}
		}
		seen[scenario.Name] = true
	}
	return scenarios
}

func lookupTriggerScenarioNamed(t *testing.T, scenarios []lookupTriggerScenario, name string) lookupTriggerScenario {
	t.Helper()
	for _, scenario := range scenarios {
		if scenario.Name == name {
			return scenario
		}
	}
	t.Fatalf("lookup trigger fixture has no scenario %q", name)
	return lookupTriggerScenario{}
}

// installedPostgresTriggerLanes reports which trigger lanes the embedded
// PostgreSQL migration set carries.
func installedPostgresTriggerLanes(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	installed := make(map[string]bool)
	for _, lane := range postgresTriggerLanes {
		pattern := regexp.MustCompile(`^[0-9]{4}_` + regexp.QuoteMeta(lane) + `_triggers\.sql$`)
		for _, entry := range entries {
			if pattern.MatchString(entry.Name()) {
				installed[lane] = true
			}
		}
	}
	if !installed[lookupTriggerLane] {
		t.Fatalf("embedded PostgreSQL migrations do not carry %s", lookupTriggerMigration)
	}
	return installed
}

// inLookupTriggerTx runs setup and then fn inside one transaction pinned to
// the open_splunk schema, and always rolls it back.
func inLookupTriggerTx(
	t *testing.T,
	db *control.PostgresDB,
	setup []string,
	fn func(ctx context.Context, tx *sql.Tx),
) {
	t.Helper()
	ctx := t.Context()
	tx, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin scenario transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back scenario transaction: %v", err)
		}
	}()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO `+control.PostgresSchema); err != nil {
		t.Fatalf("pin scenario search_path: %v", err)
	}
	for index, statement := range setup {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("setup statement %d failed: %v\n%s", index, err, statement)
		}
	}
	fn(ctx, tx)
}

// runLookupTriggerScenario replays one fixture scenario. An accepted
// statement must also leave every tenant ledger equal to its recount, which
// is what the accounting triggers exist to maintain.
func runLookupTriggerScenario(t *testing.T, db *control.PostgresDB, scenario lookupTriggerScenario) {
	t.Helper()
	inLookupTriggerTx(t, db, scenario.SetupSQL, func(ctx context.Context, tx *sql.Tx) {
		_, err := tx.ExecContext(ctx, scenario.Statement)
		if scenario.Expect == "reject" {
			requireLookupTriggerRejection(t, err, scenario.Message)
			return
		}
		if err != nil {
			t.Fatalf("statement rejected, want accept: %v", err)
		}
		requireLookupLedgersMatchRecount(ctx, t, tx)
	})
}

func requireLookupTriggerRejection(t *testing.T, err error, message string) {
	t.Helper()
	if err == nil {
		t.Fatalf("statement accepted, want rejection %q", message)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("statement error = %v, want a PostgreSQL RAISE %q", err, message)
	}
	if pgErr.Code != postgresRaiseSQLState || pgErr.Message != message {
		t.Fatalf("statement rejected with SQLSTATE %s %q, want SQLSTATE %s %q",
			pgErr.Code, pgErr.Message, postgresRaiseSQLState, message)
	}
	if got, ok := control.RaiseMessage(err); !ok || got != message {
		t.Fatalf("control.RaiseMessage() = %q, %v; want %q, true", got, ok, message)
	}
}

// requireLookupLedgersMatchRecount fails when any tenant ledger differs from
// the rows it accounts for.
func requireLookupLedgersMatchRecount(ctx context.Context, t *testing.T, queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
},
) {
	t.Helper()
	rows, err := queryer.QueryContext(ctx, `
		SELECT ledger.tenant_id,
			ledger.staged_asset_count, recount.stages,
			ledger.asset_identity_count, recount.identities,
			ledger.published_version_count, recount.versions,
			ledger.stored_content_bytes, recount.bytes
		FROM knowledge_lookup_asset_tenant_ledgers AS ledger
		CROSS JOIN LATERAL (
			SELECT
				(SELECT count(*) FROM knowledge_lookup_asset_stages
					WHERE tenant_id = ledger.tenant_id) AS stages,
				(SELECT count(*) FROM knowledge_lookup_assets
					WHERE tenant_id = ledger.tenant_id) AS identities,
				(SELECT count(*) FROM knowledge_lookup_asset_versions
					WHERE tenant_id = ledger.tenant_id) AS versions,
				COALESCE((SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_stages
					WHERE tenant_id = ledger.tenant_id), 0)
				+ COALESCE((SELECT sum(canonical_bytes) FROM knowledge_lookup_asset_versions
					WHERE tenant_id = ledger.tenant_id), 0) AS bytes
		) AS recount
		WHERE ledger.staged_asset_count <> recount.stages
			OR ledger.asset_identity_count <> recount.identities
			OR ledger.published_version_count <> recount.versions
			OR ledger.stored_content_bytes <> recount.bytes
		ORDER BY ledger.tenant_id`)
	if err != nil {
		t.Fatalf("recount lookup ledgers: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var mismatches []string
	for rows.Next() {
		var tenant string
		var counts [8]int64
		if err := rows.Scan(&tenant, &counts[0], &counts[1], &counts[2], &counts[3],
			&counts[4], &counts[5], &counts[6], &counts[7]); err != nil {
			t.Fatalf("scan lookup ledger recount: %v", err)
		}
		mismatches = append(mismatches, fmt.Sprintf(
			"%s: stages %d/%d identities %d/%d versions %d/%d bytes %d/%d (ledger/recount)",
			tenant, counts[0], counts[1], counts[2], counts[3], counts[4], counts[5], counts[6], counts[7]))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read lookup ledger recount: %v", err)
	}
	if len(mismatches) > 0 {
		t.Fatalf("lookup ledgers differ from their recount:\n%s", strings.Join(mismatches, "\n"))
	}
}

// TestPostgresLookupTriggerScenarios replays every fixture scenario that needs
// only this lane's triggers. Each runs on its own database so the boundary
// scenarios, which seed thousands of rows, can run in parallel.
func TestPostgresLookupTriggerScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadLookupTriggerScenarios(t)
	installedPostgresTriggerLanes(t)
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) > 0 {
			// Asserted by TestPostgresLookupTriggerCrossLaneScenarios.
			continue
		}
		t.Run(scenario.Name, func(t *testing.T) {
			t.Parallel()
			runLookupTriggerScenario(t, testsupport.NewPostgresControlDB(t), scenario)
		})
	}
}

// TestPostgresLookupTriggerCrossLaneScenarios replays the scenarios that need
// another lane's triggers. It runs each one as soon as every lane it names is
// in the embedded migration set, and then fails on any difference; until then
// it reports which lanes are missing.
func TestPostgresLookupTriggerCrossLaneScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadLookupTriggerScenarios(t)
	installed := installedPostgresTriggerLanes(t)
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) == 0 {
			continue
		}
		t.Run(scenario.Name, func(t *testing.T) {
			t.Parallel()
			var missing []string
			for _, lane := range scenario.DependsOnLanes {
				if !installed[lane] {
					missing = append(missing, lane)
				}
			}
			if len(missing) > 0 {
				t.Skipf("needs trigger lanes %v, which the embedded PostgreSQL migrations do not carry yet", missing)
			}
			runLookupTriggerScenario(t, testsupport.NewPostgresControlDB(t), scenario)
		})
	}
}

// lookupPlantedScenarios reach trigger branches that no statement can reach
// while every trigger is enforcing, so SQLite could not replay them. Each
// starts from a fixture scenario's setup, plants the unreachable state with
// triggers and foreign keys off, and then runs Statement with them back on.
var lookupPlantedScenarios = []struct {
	name      string
	base      string
	plant     []string
	statement string
	message   string
}{
	{
		// With every trigger on, a missing ledger means no identity exists,
		// so the sequence check rejects first.
		name: "asset_version_requires_tenant_ledger_rejects_missing_ledger",
		base: "asset_version_requires_tenant_ledger_accepts_existing_ledger",
		plant: []string{
			`DELETE FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = 'tenant-a'`,
		},
		message: "lookup asset tenant ledger is missing",
	},
	{
		// With every trigger on, 8192 versions leave every definition
		// DELETED, so the registry check rejects first.
		name: "definition_version_capacity_rejects_disable_at_eight_thousand_one_hundred_ninety_two",
		base: "definition_version_matches_registry_accepts_disable",
		plant: []string{
			`INSERT INTO knowledge_lookup_definition_versions (
				tenant_id, lookup_id, definition_version, lookup_asset_id, asset_version,
				asset_size_bytes, asset_content_sha256, definition_proto, columns_blob,
				mutation_kind, state, disabled_at_unix_micro, deleted_at_unix_micro,
				created_at_unix_micro
			)
			SELECT version.tenant_id, 'filler-' || filler, 1, version.lookup_asset_id,
				version.asset_version, version.asset_size_bytes, version.asset_content_sha256,
				version.definition_proto, version.columns_blob, 'CREATE', 'ACTIVE', NULL, NULL,
				version.created_at_unix_micro
			FROM knowledge_lookup_definition_versions AS version
			CROSS JOIN generate_series(1, 8191) AS filler
			WHERE version.tenant_id = 'tenant-a' AND version.lookup_id = 'lookup-1'
				AND version.definition_version = 1`,
		},
		message: "lookup definition version capacity is exhausted",
	},
}

// TestPostgresLookupTriggerPlantedStates covers the two trigger messages the
// fixture can only reach through an earlier trigger.
func TestPostgresLookupTriggerPlantedStates(t *testing.T) {
	t.Parallel()
	scenarios := loadLookupTriggerScenarios(t)
	for _, planted := range lookupPlantedScenarios {
		base := lookupTriggerScenarioNamed(t, scenarios, planted.base)
		t.Run(planted.name, func(t *testing.T) {
			t.Parallel()
			db := testsupport.NewPostgresControlDB(t)
			// The base statement is accepted on the unplanted state, so the
			// rejection below comes from the planted state alone.
			runLookupTriggerScenario(t, db, base)
			inLookupTriggerTx(t, db, base.SetupSQL, func(ctx context.Context, tx *sql.Tx) {
				plantWithoutTriggers(ctx, t, tx, planted.plant)
				statement := planted.statement
				if statement == "" {
					statement = base.Statement
				}
				_, err := tx.ExecContext(ctx, statement)
				requireLookupTriggerRejection(t, err, planted.message)
			})
		})
	}
}

// plantWithoutTriggers runs statements with every trigger, including the
// foreign-key triggers, disabled for the current transaction only.
func plantWithoutTriggers(ctx context.Context, t *testing.T, tx *sql.Tx, statements []string) {
	t.Helper()
	if _, err := tx.ExecContext(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatalf("disable triggers for planting: %v", err)
	}
	for index, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("plant statement %d failed: %v\n%s", index, err, statement)
		}
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL session_replication_role = origin`); err != nil {
		t.Fatalf("re-enable triggers after planting: %v", err)
	}
}

var postgresRaiseMessagePattern = regexp.MustCompile(`RAISE EXCEPTION '((?:[^']|'')*)'`)

// TestPostgresLookupTriggerMessagesAreAllAsserted ties the fixture to the
// migration: every message the lane can raise is asserted by a rejection,
// and every asserted message is one the lane raises.
func TestPostgresLookupTriggerMessagesAreAllAsserted(t *testing.T) {
	t.Parallel()
	contents, err := fs.ReadFile(migrations.Postgres(), lookupTriggerMigration)
	if err != nil {
		t.Fatalf("read %s: %v", lookupTriggerMigration, err)
	}
	raised := make(map[string]bool)
	for _, match := range postgresRaiseMessagePattern.FindAllStringSubmatch(string(contents), -1) {
		raised[strings.ReplaceAll(match[1], "''", "'")] = true
	}
	if len(raised) == 0 {
		t.Fatalf("%s raises no messages", lookupTriggerMigration)
	}
	asserted := make(map[string]bool)
	for _, scenario := range loadLookupTriggerScenarios(t) {
		if scenario.Expect == "reject" {
			asserted[scenario.Message] = true
		}
	}
	for _, planted := range lookupPlantedScenarios {
		asserted[planted.message] = true
	}
	var unasserted, unknown []string
	for message := range raised {
		if !asserted[message] {
			unasserted = append(unasserted, message)
		}
	}
	for message := range asserted {
		if !raised[message] {
			unknown = append(unknown, message)
		}
	}
	slices.Sort(unasserted)
	slices.Sort(unknown)
	if len(unasserted) > 0 || len(unknown) > 0 {
		t.Fatalf("lookup trigger messages without a rejection scenario: %q; scenario messages the lane never raises: %q",
			unasserted, unknown)
	}
}

type lookupTriggerShape struct {
	table    string
	timing   string
	event    string
	ofColumn string
}

var triggerHeaderPattern = regexp.MustCompile(
	`(?is)^\s*CREATE\s+TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?\s+(BEFORE|AFTER)\s+` +
		`(INSERT|DELETE|UPDATE)(?:\s+OF\s+(.+?))?\s+ON\s+(?:\w+\.)?"?(\w+)"?\s`)

func parseTriggerHeader(t *testing.T, definition string) (string, lookupTriggerShape) {
	t.Helper()
	match := triggerHeaderPattern.FindStringSubmatch(definition)
	if match == nil {
		t.Fatalf("cannot parse trigger definition:\n%s", definition)
	}
	var columns []string
	for column := range strings.SplitSeq(match[4], ",") {
		if column = strings.TrimSpace(column); column != "" {
			columns = append(columns, strings.ToLower(column))
		}
	}
	return match[1], lookupTriggerShape{
		table:    strings.ToLower(match[5]),
		timing:   strings.ToUpper(match[2]),
		event:    strings.ToUpper(match[3]),
		ofColumn: strings.Join(columns, ","),
	}
}

// TestPostgresLookupTriggersMatchTheSQLiteInventory checks that every SQLite
// trigger on a knowledge_lookup_* table has a PostgreSQL row trigger with the
// same name, table, timing, event and UPDATE OF columns, and no more.
func TestPostgresLookupTriggersMatchTheSQLiteInventory(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	sqliteDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("open SQLite control database: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteDB.Close(); err != nil {
			t.Errorf("close SQLite control database: %v", err)
		}
	})
	want := readTriggerInventory(ctx, t, sqliteDB.SQLDB(), `
		SELECT name, sql FROM sqlite_schema
		WHERE type = 'trigger' AND substr(tbl_name, 1, 17) = 'knowledge_lookup_'`)
	for sqliteName, portedName := range lookupTriggerRenames {
		shape, ok := want[sqliteName]
		if !ok {
			t.Fatalf("SQLite has no trigger %q to rename", sqliteName)
		}
		delete(want, sqliteName)
		want[portedName] = shape
	}

	postgresDB := testsupport.NewPostgresControlDB(t)
	got := readTriggerInventory(ctx, t, postgresDB.SQLDB(), `
		SELECT trigger.tgname, pg_get_triggerdef(trigger.oid)
		FROM pg_trigger AS trigger
		JOIN pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE NOT trigger.tgisinternal
			AND namespace.nspname = 'open_splunk'
			AND substr(class.relname, 1, 17) = 'knowledge_lookup_'
			AND trigger.tgtype & 1 = 1`)
	var statementTriggers int
	if err := postgresDB.SQLDB().QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_trigger AS trigger
		JOIN pg_class AS class ON class.oid = trigger.tgrelid
		JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE NOT trigger.tgisinternal
			AND namespace.nspname = 'open_splunk'
			AND substr(class.relname, 1, 17) = 'knowledge_lookup_'
			AND trigger.tgtype & 1 = 0`).Scan(&statementTriggers); err != nil {
		t.Fatalf("count statement-level lookup triggers: %v", err)
	}
	if statementTriggers != 0 {
		t.Fatalf("PostgreSQL has %d statement-level lookup triggers; SQLite triggers are row-level", statementTriggers)
	}
	if len(want) != 30 {
		t.Fatalf("SQLite lookup trigger inventory has %d triggers, want the 30 this lane ports", len(want))
	}
	for name, shape := range want {
		if gotShape, ok := got[name]; !ok {
			t.Errorf("PostgreSQL is missing lookup trigger %s %+v", name, shape)
		} else if gotShape != shape {
			t.Errorf("lookup trigger %s = %+v, want %+v", name, gotShape, shape)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("PostgreSQL has lookup trigger %s that SQLite does not", name)
		}
	}
}

func readTriggerInventory(ctx context.Context, t *testing.T, db *sql.DB, query string) map[string]lookupTriggerShape {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("list triggers: %v", err)
	}
	defer func() { _ = rows.Close() }()
	inventory := make(map[string]lookupTriggerShape)
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			t.Fatalf("scan trigger: %v", err)
		}
		parsedName, shape := parseTriggerHeader(t, definition)
		if parsedName != name {
			t.Fatalf("trigger %q has definition naming %q", name, parsedName)
		}
		inventory[name] = shape
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read triggers: %v", err)
	}
	return inventory
}

// TestPostgresLookupAssetPointerAdvanceNeedsItsVersion is the PostgreSQL
// counterpart of lookupasset's unpaired pointer check: the identity trigger
// accepts a +1 advance, and the deferred foreign key refuses it at commit.
func TestPostgresLookupAssetPointerAdvanceNeedsItsVersion(t *testing.T) {
	t.Parallel()
	base := lookupTriggerScenarioNamed(t, loadLookupTriggerScenarios(t),
		"asset_version_requires_tenant_ledger_accepts_existing_ledger")
	db := testsupport.NewPostgresControlDB(t)
	inLookupTriggerTx(t, db, append(slices.Clone(base.SetupSQL), base.Statement), func(ctx context.Context, tx *sql.Tx) {
		if _, err := tx.ExecContext(ctx, `
			UPDATE knowledge_lookup_assets
			SET current_version = 2, updated_at_unix_micro = updated_at_unix_micro + 1
			WHERE tenant_id = 'tenant-a' AND lookup_asset_id = 'asset-1'`); err != nil {
			t.Fatalf("unpaired advance rejected before commit: %v", err)
		}
		_, err := tx.ExecContext(ctx, `SET CONSTRAINTS ALL IMMEDIATE`)
		if !control.IsForeignKeyViolation(err) {
			t.Fatalf("checking the unpaired advance = %v, want a foreign key violation", err)
		}
	})
}

// TestPostgresLookupMultiRowStageInsertOverCapacityHitsTheLedgerCheck pins a
// documented difference from SQLite: inside one multi-row INSERT every row's
// BEFORE capacity check reads the ledger before the statement's AFTER
// accounting runs, so an overflow is caught by the ledger CHECK bound rather
// than the capacity message. lookupasset inserts one stage per statement.
func TestPostgresLookupMultiRowStageInsertOverCapacityHitsTheLedgerCheck(t *testing.T) {
	t.Parallel()
	base := lookupTriggerScenarioNamed(t, loadLookupTriggerScenarios(t), "stage_capacity_rejects_sixty_fifth_stage")
	db := testsupport.NewPostgresControlDB(t)
	inLookupTriggerTx(t, db, base.SetupSQL, func(ctx context.Context, tx *sql.Tx) {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM knowledge_lookup_asset_stages WHERE tenant_id = 'tenant-a' AND stage_id = 'bulk-64'`); err != nil {
			t.Fatalf("free one stage slot: %v", err)
		}
		_, err := tx.ExecContext(ctx, lookupStageInsert("tenant-a", "pair-1", "pair-2"))
		if !control.IsCheckViolation(err) {
			t.Fatalf("two-row insert into the last stage slot = %v, want the ledger check violation", err)
		}
	})
}

func lookupStageInsert(tenant string, stageIDs ...string) string {
	values := make([]string, 0, len(stageIDs))
	for _, stageID := range stageIDs {
		values = append(values, fmt.Sprintf(`('%s', '%s', 'owner-a',
			'\x1111111111111111111111111111111111111111111111111111111111111111'::bytea,
			'\x2222222222222222222222222222222222222222222222222222222222222222'::bytea,
			1, '\x41'::bytea, 1, 0, 1, 10, 20)`, tenant, stageID))
	}
	return `INSERT INTO knowledge_lookup_asset_stages (
		tenant_id, stage_id, owner_id, source_sha256, content_sha256, source_bytes,
		canonical_csv, canonical_bytes, row_count, column_count,
		created_at_unix_micro, expires_at_unix_micro
	) VALUES ` + strings.Join(values, ", ")
}

// TestPostgresLookupLedgerStaysExactUnderConcurrentWriters exercises the
// accounting triggers' concurrency assumption: two transactions staging for
// one tenant both update its single ledger row. The second blocks on that
// row; under SERIALIZABLE it then fails to serialize and WriteTx retries it,
// and under READ COMMITTED it re-reads the committed ledger. Either way the
// ledger ends equal to its recount.
func TestPostgresLookupLedgerStaysExactUnderConcurrentWriters(t *testing.T) {
	t.Parallel()
	seed := lookupTriggerScenarioNamed(t, loadLookupTriggerScenarios(t),
		"stage_requires_tenant_ledger_accepts_existing_ledger").SetupSQL

	for _, isolation := range []sql.IsolationLevel{sql.LevelSerializable, sql.LevelReadCommitted} {
		t.Run(isolation.String(), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			db := testsupport.NewPostgresControlDB(t)
			if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
				for _, statement := range seed {
					if _, err := tx.ExecContext(ctx, statement); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed tenant ledger: %v", err)
			}

			first, err := db.SQLDB().BeginTx(ctx, &sql.TxOptions{Isolation: isolation})
			if err != nil {
				t.Fatalf("begin first writer: %v", err)
			}
			defer func() { _ = first.Rollback() }()
			if _, err := first.ExecContext(ctx, lookupStageInsert("tenant-a", "first")); err != nil {
				t.Fatalf("first writer stage: %v", err)
			}

			var attempts atomic.Int32
			second := make(chan error, 1)
			go func() {
				if isolation == sql.LevelSerializable {
					second <- db.WriteTx(ctx, func(tx *sql.Tx) error {
						attempts.Add(1)
						_, err := tx.ExecContext(ctx, lookupStageInsert("tenant-a", "second"))
						return err
					})
					return
				}
				second <- func() error {
					attempts.Add(1)
					tx, err := db.SQLDB().BeginTx(ctx, &sql.TxOptions{Isolation: isolation})
					if err != nil {
						return err
					}
					if _, err := tx.ExecContext(ctx, lookupStageInsert("tenant-a", "second")); err != nil {
						return errors.Join(err, tx.Rollback())
					}
					return tx.Commit()
				}()
			}()
			waitForLookupLedgerLockWaiter(ctx, t, db)
			if err := first.Commit(); err != nil {
				t.Fatalf("commit first writer: %v", err)
			}
			if err := <-second; err != nil {
				t.Fatalf("second writer: %v", err)
			}
			wantAttempts := int32(1)
			if isolation == sql.LevelSerializable {
				wantAttempts = 2
			}
			if got := attempts.Load(); got != wantAttempts {
				t.Fatalf("second writer ran %d attempts, want %d", got, wantAttempts)
			}

			var staged int64
			if err := db.SQLDB().QueryRowContext(ctx, `
				SELECT staged_asset_count FROM knowledge_lookup_asset_tenant_ledgers
				WHERE tenant_id = 'tenant-a'`).Scan(&staged); err != nil {
				t.Fatalf("read ledger: %v", err)
			}
			if staged != 2 {
				t.Fatalf("staged_asset_count = %d after two committed stages, want 2", staged)
			}
			requireLookupLedgersMatchRecount(ctx, t, db.SQLDB())
		})
	}
}

// waitForLookupLedgerLockWaiter waits until another session of this database
// is blocked on a lock, which here can only be the ledger row the first
// writer holds.
func waitForLookupLedgerLockWaiter(ctx context.Context, t *testing.T, db *control.PostgresDB) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var waiting int
		if err := db.SQLDB().QueryRowContext(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
				AND pid <> pg_backend_pid()
				AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatalf("inspect lock waiters: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("second writer never blocked on the tenant ledger")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
