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
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// The knowledge_catalog_2 trigger lane ports the SQLite triggers on the
// knowledge catalog tenant, revision head, commit authority, list order key
// and list projection seal tables. Its fixture holds the scenarios that were
// replayed against both engines when the port was written; these tests replay
// the PostgreSQL side against the embedded migration set and add the checks
// the differential replay could not express.
const (
	knowledgeCatalog2FixturePath = "testdata/postgres_triggers/knowledge_catalog_2.json"
	knowledgeCatalog2Migration   = "0015_knowledge_catalog_2_triggers.sql"
	knowledgeCatalog2Lane        = "knowledge_catalog_2"
	knowledgeCatalog2RaiseState  = "P0001"
)

// knowledgeCatalog2KnownLanes names every trigger lane a scenario may depend
// on. A lane is installed when the embedded set holds NNNN_<lane>_triggers.sql.
var knowledgeCatalog2KnownLanes = []string{
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

// knowledgeCatalog2Triggers is every trigger the lane owns, by table. The
// names are the SQLite names; none exceeds PostgreSQL's identifier limit.
var knowledgeCatalog2Triggers = map[string][]string{
	"knowledge_catalog_revision_heads": {
		"knowledge_catalog_revision_head_delete_is_forbidden",
		"knowledge_catalog_revision_head_identity_collision_is_forbidden",
		"knowledge_catalog_revision_head_insert_agrees_with_tenant",
		"knowledge_catalog_revision_head_transition_is_exact",
	},
	"knowledge_catalog_tenants": {
		"knowledge_catalog_revision_requires_exact_head",
		"knowledge_catalog_revision_rotates_state_token",
		"knowledge_catalog_revision_transition_is_valid",
		"knowledge_catalog_tenant_creates_revision_head",
		"knowledge_catalog_tenant_delete_is_forbidden",
		"knowledge_catalog_tenant_identity_collision_is_forbidden",
		"knowledge_catalog_tenant_identity_is_immutable",
		"knowledge_catalog_tenant_initial_shape_is_valid",
		"knowledge_catalog_tenant_provisions_lookup_asset_ledger",
	},
	"knowledge_mutation_commit_authorities": {
		"knowledge_mutation_commit_authority_collision_is_forbidden",
		"knowledge_mutation_commit_authority_delete_is_forbidden",
		"knowledge_mutation_commit_authority_is_exact",
		"knowledge_mutation_commit_authority_update_is_forbidden",
	},
	"knowledge_object_list_order_keys": {
		"knowledge_list_order_key_agrees_with_authorities",
		"knowledge_list_order_key_identity_collision_is_forbidden",
		"knowledge_list_order_key_sealed_delete_is_forbidden",
		"knowledge_list_order_key_update_is_forbidden",
	},
	"knowledge_object_list_projection_seals": {
		"knowledge_list_projection_current_seal_delete_is_forbidden",
		"knowledge_list_projection_seal_identity_collision_is_forbidden",
		"knowledge_list_projection_seal_is_complete",
		"knowledge_list_projection_seal_requires_order_key",
		"knowledge_list_projection_seal_update_is_forbidden",
	},
}

// knowledgeCatalog2Scenario is one fixture entry. SetupSQL and Statement run
// inside one transaction that is always rolled back.
type knowledgeCatalog2Scenario struct {
	Name      string   `json:"name"`
	SetupSQL  []string `json:"setup_sql"`
	Statement string   `json:"statement"`
	Expect    string   `json:"expect"`
	Message   string   `json:"message"`
	// DependsOnLanes lists other trigger lanes whose triggers the scenario
	// needs. Such scenarios run in
	// TestPostgresKnowledgeCatalog2TriggerCrossLaneScenarios.
	DependsOnLanes []string `json:"depends_on_lanes,omitempty"`
}

func loadKnowledgeCatalog2Scenarios(t *testing.T) []knowledgeCatalog2Scenario {
	t.Helper()
	contents, err := os.ReadFile(filepath.FromSlash(knowledgeCatalog2FixturePath))
	if err != nil {
		t.Fatalf("read knowledge_catalog_2 trigger fixture: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	// An unknown field would be a scenario property this runner silently
	// ignores, so the fixture and the runner must agree on every field.
	decoder.DisallowUnknownFields()
	var scenarios []knowledgeCatalog2Scenario
	if err := decoder.Decode(&scenarios); err != nil {
		t.Fatalf("decode knowledge_catalog_2 trigger fixture: %v", err)
	}
	if len(scenarios) == 0 {
		t.Fatal("knowledge_catalog_2 trigger fixture has no scenarios")
	}
	seen := make(map[string]bool, len(scenarios))
	for index, scenario := range scenarios {
		switch {
		case strings.TrimSpace(scenario.Name) == "":
			t.Fatalf("knowledge_catalog_2 scenario %d has no name", index)
		case seen[scenario.Name]:
			t.Fatalf("knowledge_catalog_2 scenario %q is duplicated", scenario.Name)
		case strings.TrimSpace(scenario.Statement) == "":
			t.Fatalf("knowledge_catalog_2 scenario %q has no statement", scenario.Name)
		case scenario.Expect == "accept" && scenario.Message != "":
			t.Fatalf("knowledge_catalog_2 scenario %q accepts but names message %q", scenario.Name, scenario.Message)
		case scenario.Expect == "reject" && scenario.Message == "":
			t.Fatalf("knowledge_catalog_2 scenario %q rejects without a message", scenario.Name)
		case scenario.Expect != "accept" && scenario.Expect != "reject":
			t.Fatalf("knowledge_catalog_2 scenario %q expects %q, want accept or reject", scenario.Name, scenario.Expect)
		}
		for _, lane := range scenario.DependsOnLanes {
			if lane == knowledgeCatalog2Lane || !slices.Contains(knowledgeCatalog2KnownLanes, lane) {
				t.Fatalf("knowledge_catalog_2 scenario %q depends on unknown lane %q", scenario.Name, lane)
			}
		}
		seen[scenario.Name] = true
	}
	return scenarios
}

// knowledgeCatalog2InstalledLanes reports which trigger lanes the embedded
// PostgreSQL migration set carries, and fails unless it carries this lane at
// its assigned version.
func knowledgeCatalog2InstalledLanes(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Postgres(), ".")
	if err != nil {
		t.Fatalf("read embedded PostgreSQL migrations: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if !slices.Contains(names, knowledgeCatalog2Migration) {
		t.Fatalf("embedded PostgreSQL migrations %v do not carry %s", names, knowledgeCatalog2Migration)
	}
	installed := make(map[string]bool)
	for _, lane := range knowledgeCatalog2KnownLanes {
		pattern := regexp.MustCompile(`^[0-9]{4}_` + regexp.QuoteMeta(lane) + `_triggers\.sql$`)
		for _, name := range names {
			if pattern.MatchString(name) {
				installed[lane] = true
			}
		}
	}
	return installed
}

// inKnowledgeCatalog2Tx runs setup and then fn inside one transaction pinned
// to the open_splunk schema, and always rolls it back.
func inKnowledgeCatalog2Tx(
	t *testing.T,
	db *control.PostgresDB,
	setup []string,
	fn func(ctx context.Context, tx *sql.Tx),
) {
	t.Helper()
	inKnowledgeCatalog2TxWith(t, db, false, setup, fn)
}

// inKnowledgeCatalog2TxWith is inKnowledgeCatalog2Tx with a choice of
// trigger set. With laneOnly, every trigger another lane owns is disabled
// inside the transaction before setup runs, so the scenario sees exactly the
// triggers of knowledge_catalog_2, the environment the fixture's lane-local
// scenarios were written and replayed against. The other lanes install rows
// as side effects (version lifecycle, list order keys) and require a sealed
// list projection before a registry row, so with every lane enabled the
// hand-built setups would collide with those side effects instead of
// reaching the statement under test. The rollback re-enables the triggers;
// requireKnowledgeCatalog2CleanDatabase checks that none stays disabled.
func inKnowledgeCatalog2TxWith(
	t *testing.T,
	db *control.PostgresDB,
	laneOnly bool,
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
	if laneOnly {
		disableOtherLanesKnowledgeCatalog2Triggers(ctx, t, tx)
	}
	for index, statement := range setup {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("setup statement %d failed: %v\n%s", index, err, statement)
		}
	}
	fn(ctx, tx)
}

// disableOtherLanesKnowledgeCatalog2Triggers disables, inside tx, every user
// trigger in the open_splunk schema that knowledgeCatalog2Triggers does not
// list. PostgreSQL builds the DDL so identifiers stay quoted.
func disableOtherLanesKnowledgeCatalog2Triggers(ctx context.Context, t *testing.T, tx *sql.Tx) {
	t.Helper()
	var lane []string
	for _, names := range knowledgeCatalog2Triggers {
		lane = append(lane, names...)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT format('ALTER TABLE %I.%I DISABLE TRIGGER %I', n.nspname, c.relname, tg.tgname)
		FROM pg_catalog.pg_trigger AS tg
		JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND NOT tg.tgisinternal AND NOT (tg.tgname = ANY($2))
		ORDER BY c.relname, tg.tgname`, control.PostgresSchema, lane)
	if err != nil {
		t.Fatalf("list other lanes' triggers: %v", err)
	}
	var statements []string
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			_ = rows.Close()
			t.Fatalf("scan other lane trigger: %v", err)
		}
		statements = append(statements, statement)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close other lanes' triggers: %v", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read other lanes' triggers: %v", err)
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func runKnowledgeCatalog2Scenario(t *testing.T, db *control.PostgresDB, scenario knowledgeCatalog2Scenario) {
	t.Helper()
	laneOnly := len(scenario.DependsOnLanes) == 0
	inKnowledgeCatalog2TxWith(t, db, laneOnly, scenario.SetupSQL, func(ctx context.Context, tx *sql.Tx) {
		_, err := tx.ExecContext(ctx, scenario.Statement)
		if scenario.Expect == "reject" {
			requireKnowledgeCatalog2Rejection(t, err, scenario.Message)
			return
		}
		if err != nil {
			t.Fatalf("statement rejected, want accept: %v", err)
		}
	})
}

// requireKnowledgeCatalog2Rejection requires err to be exactly the trigger's
// RAISE: SQLSTATE P0001 with byte-identical message text, as the control
// plane's classifier reports it.
func requireKnowledgeCatalog2Rejection(t *testing.T, err error, message string) {
	t.Helper()
	if err == nil {
		t.Fatalf("statement accepted, want rejection %q", message)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("statement error = %v, want a PostgreSQL RAISE %q", err, message)
	}
	if pgErr.Code != knowledgeCatalog2RaiseState || pgErr.Message != message {
		t.Fatalf("statement rejected with SQLSTATE %s %q, want SQLSTATE %s %q",
			pgErr.Code, pgErr.Message, knowledgeCatalog2RaiseState, message)
	}
	if !control.IsRaise(err) {
		t.Fatalf("control.IsRaise(%v) = false, want true", err)
	}
	if got, ok := control.RaiseMessage(err); !ok || got != message {
		t.Fatalf("control.RaiseMessage() = %q, %v; want %q, true", got, ok, message)
	}
}

// TestPostgresKnowledgeCatalog2TriggerScenarios replays every fixture scenario
// that needs only this lane's triggers. Every scenario rolls back, so they
// share one database and run in order; afterwards the database must hold no
// rows and no disabled trigger, which proves no scenario leaked state into
// the next.
func TestPostgresKnowledgeCatalog2TriggerScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadKnowledgeCatalog2Scenarios(t)
	knowledgeCatalog2InstalledLanes(t)
	db := testsupport.NewPostgresControlDB(t)
	ran := 0
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) > 0 {
			// Asserted by TestPostgresKnowledgeCatalog2TriggerCrossLaneScenarios.
			continue
		}
		ran++
		t.Run(scenario.Name, func(t *testing.T) {
			runKnowledgeCatalog2Scenario(t, db, scenario)
		})
	}
	if ran == 0 {
		t.Fatal("no knowledge_catalog_2 scenario ran")
	}
	requireKnowledgeCatalog2CleanDatabase(t, db)
}

func requireKnowledgeCatalog2CleanDatabase(t *testing.T, db *control.PostgresDB) {
	t.Helper()
	ctx := t.Context()
	for _, table := range []string{
		"tenants",
		"knowledge_catalog_tenants",
		"knowledge_catalog_revision_heads",
		"knowledge_lookup_asset_tenant_ledgers",
		"knowledge_mutation_commit_authorities",
		"knowledge_object_list_order_keys",
		"knowledge_object_list_projection_seals",
	} {
		var count int64
		if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("%s holds %d rows after every scenario rolled back, want 0", table, count)
		}
	}
	var disabled []string
	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT tg.tgname
		FROM pg_catalog.pg_trigger AS tg
		JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND NOT tg.tgisinternal AND tg.tgenabled <> 'O'
		ORDER BY tg.tgname`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list disabled triggers: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan disabled trigger: %v", err)
		}
		disabled = append(disabled, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read disabled triggers: %v", err)
	}
	if len(disabled) > 0 {
		t.Errorf("triggers left disabled after the scenarios rolled back: %v", disabled)
	}
}

// TestPostgresKnowledgeCatalog2TriggerCrossLaneScenarios replays the scenarios
// that need another lane's triggers. It never skips: once every lane a
// scenario names is in the embedded migration set the scenario runs and any
// difference fails; until then it logs which lanes are missing, and the
// integrator that installs them makes the scenario binding.
func TestPostgresKnowledgeCatalog2TriggerCrossLaneScenarios(t *testing.T) {
	t.Parallel()
	scenarios := loadKnowledgeCatalog2Scenarios(t)
	installed := knowledgeCatalog2InstalledLanes(t)
	var db *control.PostgresDB
	for _, scenario := range scenarios {
		if len(scenario.DependsOnLanes) == 0 {
			continue
		}
		var missing []string
		for _, lane := range scenario.DependsOnLanes {
			if !installed[lane] {
				missing = append(missing, lane)
			}
		}
		if len(missing) > 0 {
			t.Logf("scenario %q waits for trigger lanes %v, which the embedded PostgreSQL migrations do not carry yet",
				scenario.Name, missing)
			continue
		}
		if db == nil {
			db = testsupport.NewPostgresControlDB(t)
		}
		t.Run(scenario.Name, func(t *testing.T) {
			runKnowledgeCatalog2Scenario(t, db, scenario)
		})
	}
}

// TestPostgresKnowledgeCatalog2FixtureCoversEveryRaise requires the fixture to
// reject at least one statement with each message the lane's migration
// raises, so a trigger added to the file without a scenario fails here.
func TestPostgresKnowledgeCatalog2FixtureCoversEveryRaise(t *testing.T) {
	t.Parallel()
	contents, err := fs.ReadFile(migrations.Postgres(), knowledgeCatalog2Migration)
	if err != nil {
		t.Fatalf("read %s: %v", knowledgeCatalog2Migration, err)
	}
	raised := make(map[string]bool)
	for _, match := range regexp.MustCompile(`RAISE EXCEPTION '([^']+)'`).FindAllSubmatch(contents, -1) {
		raised[string(match[1])] = true
	}
	if len(raised) == 0 {
		t.Fatalf("%s raises no messages", knowledgeCatalog2Migration)
	}
	rejected := make(map[string]bool)
	accepted := 0
	for _, scenario := range loadKnowledgeCatalog2Scenarios(t) {
		if scenario.Expect == "accept" {
			accepted++
			continue
		}
		if !raised[scenario.Message] {
			t.Errorf("scenario %q expects %q, which %s never raises", scenario.Name, scenario.Message, knowledgeCatalog2Migration)
		}
		rejected[scenario.Message] = true
	}
	for message := range raised {
		if !rejected[message] {
			t.Errorf("no scenario rejects with %q", message)
		}
	}
	if accepted == 0 {
		t.Error("fixture has no accepted statement")
	}
	wantTriggers := 0
	for _, names := range knowledgeCatalog2Triggers {
		wantTriggers += len(names)
	}
	if got := len(regexp.MustCompile(`(?m)^CREATE TRIGGER `).FindAll(contents, -1)); got != wantTriggers {
		t.Errorf("%s creates %d triggers, want %d", knowledgeCatalog2Migration, got, wantTriggers)
	}
}

// TestPostgresKnowledgeCatalog2TriggersAreInstalled checks the catalog: every
// trigger the lane owns exists on its table, enabled, and row level.
func TestPostgresKnowledgeCatalog2TriggersAreInstalled(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	ctx := t.Context()
	for table, want := range knowledgeCatalog2Triggers {
		rows, err := db.SQLDB().QueryContext(ctx, `
			SELECT tg.tgname, tg.tgenabled = 'O', (tg.tgtype & 1) = 1
			FROM pg_catalog.pg_trigger AS tg
			JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid
			JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2 AND NOT tg.tgisinternal
			ORDER BY tg.tgname`, control.PostgresSchema, table)
		if err != nil {
			t.Fatalf("list triggers on %s: %v", table, err)
		}
		var got []string
		for rows.Next() {
			var name string
			var enabled, rowLevel bool
			if err := rows.Scan(&name, &enabled, &rowLevel); err != nil {
				_ = rows.Close()
				t.Fatalf("scan trigger on %s: %v", table, err)
			}
			if !slices.Contains(want, name) {
				// Another lane may own further triggers on the table.
				continue
			}
			if !enabled || !rowLevel {
				t.Errorf("trigger %s on %s: enabled=%v row-level=%v, want both", name, table, enabled, rowLevel)
			}
			got = append(got, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read triggers on %s: %v", table, err)
		}
		_ = rows.Close()
		sorted := slices.Clone(want)
		sort.Strings(sorted)
		if !slices.Equal(got, sorted) {
			t.Errorf("triggers on %s = %v, want %v", table, got, sorted)
		}
	}
}

// knowledgeCatalog2Head reads a tenant's catalog revision and its head. The
// tables are qualified so it also works under a foreign search_path.
func knowledgeCatalog2Head(ctx context.Context, t *testing.T, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant string,
) (tenantRevision, headRevision int64, token []byte) {
	t.Helper()
	if err := queryer.QueryRowContext(ctx, `
		SELECT tenant.catalog_revision, head.catalog_revision, head.state_token
		FROM open_splunk.knowledge_catalog_tenants AS tenant
		JOIN open_splunk.knowledge_catalog_revision_heads AS head ON head.tenant_id = tenant.tenant_id
		WHERE tenant.tenant_id = $1`, tenant).Scan(&tenantRevision, &headRevision, &token); err != nil {
		t.Fatalf("read catalog revision head for %s: %v", tenant, err)
	}
	return tenantRevision, headRevision, token
}

func createKnowledgeCatalog2Tenant(t *testing.T, db *control.PostgresDB, tenant string) {
	t.Helper()
	if err := db.WriteTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `
			INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
			VALUES ($1, 1, 1)`, tenant); err != nil {
			return err
		}
		_, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ($1)`, tenant)
		return err
	}); err != nil {
		t.Fatalf("create knowledge catalog tenant %s: %v", tenant, err)
	}
}

// TestPostgresKnowledgeCatalog2TenantSideEffectsCommit is the committed
// counterpart of the fixture's side-effect probes, which the SQLite catalog
// store tests rely on: a new tenant gets a revision head at revision zero
// with a 32-byte token and an empty lookup asset ledger; each revision
// advance moves the head and replaces its token; an update that keeps the
// revision keeps the token.
func TestPostgresKnowledgeCatalog2TenantSideEffectsCommit(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	ctx := t.Context()
	createKnowledgeCatalog2Tenant(t, db, "t-side-effects")
	createKnowledgeCatalog2Tenant(t, db, "t-bystander")

	tenantRevision, headRevision, token := knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-side-effects")
	if tenantRevision != 0 || headRevision != 0 || len(token) != 32 {
		t.Fatalf("new tenant revision=%d head=%d token bytes=%d, want 0, 0, 32",
			tenantRevision, headRevision, len(token))
	}
	_, _, bystanderToken := knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-bystander")
	if bytes.Equal(token, bystanderToken) {
		t.Fatal("two tenants were issued the same initial state token")
	}
	var ledgers int
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT count(*) FROM knowledge_lookup_asset_tenant_ledgers
		WHERE tenant_id = $1 AND staged_asset_count = 0 AND asset_identity_count = 0
			AND published_version_count = 0 AND stored_content_bytes = 0`, "t-side-effects").Scan(&ledgers); err != nil {
		t.Fatalf("read lookup asset ledger: %v", err)
	}
	if ledgers != 1 {
		t.Fatalf("new tenant has %d empty lookup asset ledgers, want 1", ledgers)
	}

	seen := [][]byte{token}
	for want := int64(1); want <= 3; want++ {
		if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `
				UPDATE knowledge_catalog_tenants SET catalog_revision = catalog_revision + 1
				WHERE tenant_id = $1`, "t-side-effects")
			return err
		}); err != nil {
			t.Fatalf("advance catalog revision to %d: %v", want, err)
		}
		tenantRevision, headRevision, token = knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-side-effects")
		if tenantRevision != want || headRevision != want || len(token) != 32 {
			t.Fatalf("after advance revision=%d head=%d token bytes=%d, want %d, %d, 32",
				tenantRevision, headRevision, len(token), want, want)
		}
		for _, earlier := range seen {
			if bytes.Equal(token, earlier) {
				t.Fatalf("revision %d reused an earlier state token", want)
			}
		}
		seen = append(seen, token)
	}

	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			UPDATE knowledge_catalog_tenants SET catalog_revision = catalog_revision
			WHERE tenant_id = $1`, "t-side-effects")
		return err
	}); err != nil {
		t.Fatalf("rewrite unchanged catalog revision: %v", err)
	}
	_, headRevision, unchangedToken := knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-side-effects")
	if headRevision != 3 || !bytes.Equal(unchangedToken, token) {
		t.Fatalf("unchanged revision moved the head to %d or replaced its token", headRevision)
	}
	_, bystanderRevision, bystanderAfter := knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-bystander")
	if bystanderRevision != 0 || !bytes.Equal(bystanderAfter, bystanderToken) {
		t.Fatal("advancing one tenant changed another tenant's revision head")
	}

	// The committed rows are permanent: tenants and their heads refuse
	// deletion outside a scenario transaction too.
	_, err := db.SQLDB().ExecContext(ctx, `DELETE FROM knowledge_catalog_revision_heads WHERE tenant_id = $1`, "t-side-effects")
	requireKnowledgeCatalog2Rejection(t, err, "knowledge catalog revision head cannot be deleted")
	_, err = db.SQLDB().ExecContext(ctx, `DELETE FROM knowledge_catalog_tenants WHERE tenant_id = $1`, "t-side-effects")
	requireKnowledgeCatalog2Rejection(t, err, "knowledge catalog tenant cannot be deleted")
}

// TestPostgresKnowledgeCatalog2RejectionsAbortTheStatementOnly checks that a
// rejected statement inside WriteTx surfaces as the trigger's RAISE, is not
// retried, and leaves the committed head untouched.
func TestPostgresKnowledgeCatalog2RejectionsAbortTheStatementOnly(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	ctx := t.Context()
	createKnowledgeCatalog2Tenant(t, db, "t-reject")
	_, _, before := knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-reject")

	attempts := 0
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempts++
		_, err := tx.ExecContext(ctx, `
			UPDATE knowledge_catalog_tenants SET catalog_revision = catalog_revision + 2
			WHERE tenant_id = $1`, "t-reject")
		return err
	})
	requireKnowledgeCatalog2Rejection(t, err, "knowledge catalog revision must advance by one")
	if attempts != 1 {
		t.Fatalf("WriteTx ran a trigger rejection %d times, want 1", attempts)
	}
	tenantRevision, headRevision, after := knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-reject")
	if tenantRevision != 0 || headRevision != 0 || !bytes.Equal(before, after) {
		t.Fatal("a rejected revision advance changed the committed revision head")
	}
}

// TestPostgresKnowledgeCatalog2ConcurrentWritersSerialize drives the two
// races the port's concurrency notes rely on through WriteTx: concurrent
// revision advances on one tenant must all commit, one at a time, each
// rotating the head; concurrent creation of one tenant must commit exactly
// once, with the losers rejected rather than duplicated.
func TestPostgresKnowledgeCatalog2ConcurrentWritersSerialize(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	ctx := t.Context()
	const writers = 4

	t.Run("revision advances", func(t *testing.T) {
		createKnowledgeCatalog2Tenant(t, db, "t-race")
		_, _, initial := knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-race")
		errs := make([]error, writers)
		var wg sync.WaitGroup
		for index := range writers {
			wg.Go(func() {
				errs[index] = db.WriteTx(ctx, func(tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `
						UPDATE knowledge_catalog_tenants SET catalog_revision = catalog_revision + 1
						WHERE tenant_id = $1`, "t-race")
					return err
				})
			})
		}
		wg.Wait()
		for index, err := range errs {
			if err != nil {
				t.Errorf("revision writer %d: %v", index, err)
			}
		}
		tenantRevision, headRevision, token := knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-race")
		if tenantRevision != writers || headRevision != writers || len(token) != 32 || bytes.Equal(token, initial) {
			t.Fatalf("after %d concurrent advances revision=%d head=%d token bytes=%d rotated=%v",
				writers, tenantRevision, headRevision, len(token), !bytes.Equal(token, initial))
		}
	})

	t.Run("tenant creation", func(t *testing.T) {
		if _, err := db.SQLDB().ExecContext(ctx, `
			INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
			VALUES ($1, 1, 1)`, "t-create-race"); err != nil {
			t.Fatalf("create base tenant: %v", err)
		}
		errs := make([]error, writers)
		var wg sync.WaitGroup
		for index := range writers {
			wg.Go(func() {
				errs[index] = db.WriteTx(ctx, func(tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ($1)`, "t-create-race")
					return err
				})
			})
		}
		wg.Wait()
		committed := 0
		for index, err := range errs {
			if err == nil {
				committed++
				continue
			}
			// A loser either retried after the winner committed and met the
			// collision trigger, or met the primary key directly.
			message, _ := control.RaiseMessage(err)
			if message != "knowledge catalog tenant already exists" && !control.IsUniqueViolation(err) {
				t.Errorf("tenant creator %d: %v, want the collision RAISE or a unique violation", index, err)
			}
		}
		if committed != 1 {
			t.Fatalf("%d concurrent tenant creations committed, want 1", committed)
		}
		var heads, ledgers int
		if err := db.SQLDB().QueryRowContext(ctx, `
			SELECT (SELECT count(*) FROM knowledge_catalog_revision_heads WHERE tenant_id = $1),
				(SELECT count(*) FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = $1)`,
			"t-create-race").Scan(&heads, &ledgers); err != nil {
			t.Fatalf("count side-effect rows: %v", err)
		}
		if heads != 1 || ledgers != 1 {
			t.Fatalf("concurrent creation left %d heads and %d lookup ledgers, want 1 and 1", heads, ledgers)
		}
	})

	// The catalog writer provisions a tenant with INSERT ... SELECT WHERE NOT
	// EXISTS (knowledgecatalog.ensureMutationLedgers). Racing writers must all
	// commit: the loser's read of the missing row makes SERIALIZABLE abort it
	// with a retryable failure, and its retry sees the winner's row and
	// inserts nothing.
	t.Run("idempotent provisioning", func(t *testing.T) {
		if _, err := db.SQLDB().ExecContext(ctx, `
			INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
			VALUES ($1, 1, 1)`, "t-ensure-race"); err != nil {
			t.Fatalf("create base tenant: %v", err)
		}
		errs := make([]error, writers)
		var wg sync.WaitGroup
		for index := range writers {
			wg.Go(func() {
				errs[index] = db.WriteTx(ctx, func(tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `
						INSERT INTO knowledge_catalog_tenants (tenant_id)
						SELECT $1::text WHERE NOT EXISTS (
							SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = $1::text
						)`, "t-ensure-race")
					return err
				})
			})
		}
		wg.Wait()
		for index, err := range errs {
			if err != nil {
				t.Errorf("provisioning writer %d: %v", index, err)
			}
		}
		tenantRevision, headRevision, token := knowledgeCatalog2Head(ctx, t, db.SQLDB(), "t-ensure-race")
		if tenantRevision != 0 || headRevision != 0 || len(token) != 32 {
			t.Fatalf("provisioned tenant revision=%d head=%d token bytes=%d, want 0, 0, 32",
				tenantRevision, headRevision, len(token))
		}
	})
}

// TestPostgresKnowledgeCatalog2MultiRowStatements covers statements that touch
// several rows at once. PostgreSQL makes the rows a statement has already
// written visible to its later BEFORE ROW triggers, as SQLite does, but
// queues AFTER ROW triggers to the end of the statement; the results must
// match SQLite's row-at-a-time behavior.
func TestPostgresKnowledgeCatalog2MultiRowStatements(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	baseTenants := []string{
		`INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro)
			VALUES ('t1', 1, 1), ('t2', 1, 1), ('t3', 1, 1)`,
	}
	withCatalogTenants := append(slices.Clone(baseTenants),
		`INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('t1'), ('t2'), ('t3')`)

	t.Run("duplicate rows in one insert", func(t *testing.T) {
		inKnowledgeCatalog2Tx(t, db, baseTenants, func(ctx context.Context, tx *sql.Tx) {
			_, err := tx.ExecContext(ctx, `INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('t1'), ('t1')`)
			requireKnowledgeCatalog2Rejection(t, err, "knowledge catalog tenant already exists")
		})
	})

	t.Run("insert creates every head and ledger", func(t *testing.T) {
		inKnowledgeCatalog2Tx(t, db, withCatalogTenants, func(ctx context.Context, tx *sql.Tx) {
			var heads, distinctTokens, ledgers int
			if err := tx.QueryRowContext(ctx, `
				SELECT (SELECT count(*) FROM knowledge_catalog_revision_heads
						WHERE catalog_revision = 0 AND octet_length(state_token) = 32),
					(SELECT count(DISTINCT state_token) FROM knowledge_catalog_revision_heads),
					(SELECT count(*) FROM knowledge_lookup_asset_tenant_ledgers)`).Scan(
				&heads, &distinctTokens, &ledgers); err != nil {
				t.Fatalf("count side-effect rows: %v", err)
			}
			if heads != 3 || distinctTokens != 3 || ledgers != 3 {
				t.Fatalf("multi-row insert left heads=%d distinct tokens=%d ledgers=%d, want 3 each",
					heads, distinctTokens, ledgers)
			}
		})
	})

	t.Run("advance rotates every head", func(t *testing.T) {
		inKnowledgeCatalog2Tx(t, db, withCatalogTenants, func(ctx context.Context, tx *sql.Tx) {
			if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE kc2_multi_old ON COMMIT DROP AS
				SELECT tenant_id, state_token FROM knowledge_catalog_revision_heads`); err != nil {
				t.Fatalf("snapshot heads: %v", err)
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE knowledge_catalog_tenants SET catalog_revision = catalog_revision + 1
				WHERE tenant_id IN ('t1', 't3')`); err != nil {
				t.Fatalf("advance two tenants in one statement: %v", err)
			}
			rows, err := tx.QueryContext(ctx, `
				SELECT head.tenant_id, tenant.catalog_revision, head.catalog_revision,
					head.state_token <> old.state_token
				FROM knowledge_catalog_revision_heads AS head
				JOIN knowledge_catalog_tenants AS tenant ON tenant.tenant_id = head.tenant_id
				JOIN kc2_multi_old AS old ON old.tenant_id = head.tenant_id
				ORDER BY head.tenant_id`)
			if err != nil {
				t.Fatalf("read heads: %v", err)
			}
			defer func() { _ = rows.Close() }()
			want := map[string]int64{"t1": 1, "t2": 0, "t3": 1}
			for rows.Next() {
				var tenant string
				var tenantRevision, headRevision int64
				var rotated bool
				if err := rows.Scan(&tenant, &tenantRevision, &headRevision, &rotated); err != nil {
					t.Fatalf("scan head: %v", err)
				}
				if tenantRevision != want[tenant] || headRevision != want[tenant] || rotated != (want[tenant] == 1) {
					t.Errorf("%s: revision=%d head=%d rotated=%v, want %d, %d, %v",
						tenant, tenantRevision, headRevision, rotated, want[tenant], want[tenant], want[tenant] == 1)
				}
				delete(want, tenant)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("read heads: %v", err)
			}
			if len(want) != 0 {
				t.Fatalf("heads missing for %v", want)
			}
		})
	})

	t.Run("one bad row rejects the whole advance", func(t *testing.T) {
		setup := append(slices.Clone(withCatalogTenants),
			`UPDATE knowledge_catalog_tenants SET catalog_revision = 1 WHERE tenant_id = 't2'`)
		inKnowledgeCatalog2Tx(t, db, setup, func(ctx context.Context, tx *sql.Tx) {
			_, err := tx.ExecContext(ctx, `
				UPDATE knowledge_catalog_tenants SET catalog_revision = 2
				WHERE tenant_id IN ('t1', 't2')`)
			requireKnowledgeCatalog2Rejection(t, err, "knowledge catalog revision must advance by one")
		})
	})
}

// TestPostgresKnowledgeCatalog2TriggersIgnoreTheCallerSearchPath requires the
// lane's functions to resolve their tables in open_splunk whatever the
// session's search_path is. Every server connection pins it, but an operator
// session or a maintenance tool that qualifies table names would otherwise
// fail inside a trigger, or resolve a same-named table in another schema.
func TestPostgresKnowledgeCatalog2TriggersIgnoreTheCallerSearchPath(t *testing.T) {
	t.Parallel()
	db := testsupport.NewPostgresControlDB(t)
	ctx := t.Context()

	rows, err := db.SQLDB().QueryContext(ctx, `
		SELECT p.proname, COALESCE(array_to_string(p.proconfig, ','), '')
		FROM pg_catalog.pg_proc AS p
		JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
		WHERE n.nspname = $1 AND (p.proname = ANY($2) OR p.proname = ANY($3))
		ORDER BY p.proname`,
		control.PostgresSchema,
		knowledgeCatalog2TriggerNames(),
		[]string{
			"knowledge_list_projection_seal_complete_holds",
			"knowledge_mutation_commit_authority_exact_holds",
		})
	if err != nil {
		t.Fatalf("list lane functions: %v", err)
	}
	defer func() { _ = rows.Close() }()
	functions := 0
	for rows.Next() {
		var name, config string
		if err := rows.Scan(&name, &config); err != nil {
			t.Fatalf("scan lane function: %v", err)
		}
		functions++
		if config != "search_path="+control.PostgresSearchPath {
			t.Errorf("function %s runs with settings %q, want search_path=%s", name, config, control.PostgresSearchPath)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read lane functions: %v", err)
	}
	if want := len(knowledgeCatalog2TriggerNames()) + 2; functions != want {
		t.Fatalf("found %d lane functions, want %d", functions, want)
	}

	tx, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range []string{
		`SET LOCAL search_path TO pg_catalog`,
		`INSERT INTO open_splunk.tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro) VALUES ('t-path', 1, 1)`,
		`INSERT INTO open_splunk.knowledge_catalog_tenants (tenant_id) VALUES ('t-path')`,
		`UPDATE open_splunk.knowledge_catalog_tenants SET catalog_revision = 1 WHERE tenant_id = 't-path'`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	tenantRevision, headRevision, token := knowledgeCatalog2Head(ctx, t, tx, "t-path")
	if tenantRevision != 1 || headRevision != 1 || len(token) != 32 {
		t.Fatalf("revision=%d head=%d token bytes=%d, want 1, 1, 32", tenantRevision, headRevision, len(token))
	}
	_, err = tx.ExecContext(ctx, `UPDATE open_splunk.knowledge_catalog_tenants SET catalog_revision = 3 WHERE tenant_id = 't-path'`)
	requireKnowledgeCatalog2Rejection(t, err, "knowledge catalog revision must advance by one")
}

func knowledgeCatalog2TriggerNames() []string {
	var names []string
	for _, table := range knowledgeCatalog2Triggers {
		names = append(names, table...)
	}
	sort.Strings(names)
	return names
}
