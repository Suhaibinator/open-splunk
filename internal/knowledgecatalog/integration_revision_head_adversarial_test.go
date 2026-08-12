package knowledgecatalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

type integrationRevisionHead struct {
	revision int64
	token    []byte
}

func TestIntegrationRevisionHeadRejectsCursorFromDivergentRestoreBranch(t *testing.T) {
	database, store := newCatalogTestStore(t)
	for index, name := range []string{"alpha", "zulu"} {
		insertFixtureObject(t, database, fixtureObject{
			id:    "ko-restore-" + name,
			owner: testOwner,
			versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, name, SharingScopePrivate, nil, name+"-*"),
				state:      StateActive,
				mutation:   "create",
				timestamp:  int64(10 + index),
			}},
		})
	}

	request := ListRequest{PageSize: 1, IncludeTotal: true}
	atRestore, err := store.List(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("List(snapshot at restore point): %v", err)
	}
	if !slices.Equal(names(atRestore.Objects), []string{"alpha"}) ||
		atRestore.NextPageToken == "" || atRestore.TotalSize == nil || *atRestore.TotalSize != 2 {
		t.Fatalf("restore-point page = %#v", atRestore)
	}
	restoreHead := readIntegrationRevisionHead(t, database)
	if restoreHead.revision != int64(atRestore.CatalogRevision) || len(restoreHead.token) != 32 {
		t.Fatalf("restore-point head = revision %d/token bytes %d, page revision %d",
			restoreHead.revision, len(restoreHead.token), atRestore.CatalogRevision)
	}

	backupPath := filepath.Join(t.TempDir(), "revision-head-restore.sqlite")
	if err := database.BackupTo(context.Background(), backupPath); err != nil {
		t.Fatalf("BackupTo(restore point): %v", err)
	}

	branchATransaction, _ := stageIntegrationKnownPublication(
		t,
		database,
		"ko-restore-alpha",
		aliasDefinition(testApp, "bravo", SharingScopePrivate, nil, "branch-a-*"),
		StateActive,
		"update",
		20,
	)
	if err := branchATransaction.Commit(); err != nil {
		t.Fatalf("commit branch A: %v", err)
	}
	branchA, err := store.List(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("List(branch A): %v", err)
	}
	if !slices.Equal(names(branchA.Objects), []string{"bravo"}) || branchA.NextPageToken == "" {
		t.Fatalf("branch-A page = %#v", branchA)
	}
	branchAHead := readIntegrationRevisionHead(t, database)
	if branchAHead.revision != restoreHead.revision+1 ||
		branchAHead.revision != int64(branchA.CatalogRevision) ||
		bytes.Equal(branchAHead.token, restoreHead.token) {
		t.Fatalf("branch-A head = revision %d/token changed %t; restore revision %d",
			branchAHead.revision, !bytes.Equal(branchAHead.token, restoreHead.token), restoreHead.revision)
	}

	restored, err := control.Open(context.Background(), backupPath)
	if err != nil {
		t.Fatalf("control.Open(restored branch point): %v", err)
	}
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Errorf("close restored branch: %v", err)
		}
	})
	restoredStore, err := New(restored, Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("New(restored store): %v", err)
	}
	restoredHead := readIntegrationRevisionHead(t, restored)
	if restoredHead.revision != restoreHead.revision || !bytes.Equal(restoredHead.token, restoreHead.token) {
		t.Fatalf("exact restore changed revision head: got %d/%x, want %d/%x",
			restoredHead.revision, restoredHead.token, restoreHead.revision, restoreHead.token)
	}

	exactContinuation := request
	exactContinuation.PageToken = atRestore.NextPageToken
	exactPage, err := restoredStore.List(context.Background(), testReadScope(), exactContinuation)
	if err != nil {
		t.Fatalf("List(exact restored continuation): %v", err)
	}
	if !slices.Equal(names(exactPage.Objects), []string{"zulu"}) || exactPage.NextPageToken != "" ||
		exactPage.CatalogRevision != atRestore.CatalogRevision {
		t.Fatalf("exact restored continuation = %#v", exactPage)
	}

	branchBTransaction, _ := stageIntegrationKnownPublication(
		t,
		restored,
		"ko-restore-zulu",
		aliasDefinition(testApp, "charlie", SharingScopePrivate, nil, "branch-b-*"),
		StateActive,
		"update",
		21,
	)
	if err := branchBTransaction.Commit(); err != nil {
		t.Fatalf("commit branch B: %v", err)
	}
	branchBHead := readIntegrationRevisionHead(t, restored)
	if branchBHead.revision != branchAHead.revision || bytes.Equal(branchBHead.token, branchAHead.token) {
		t.Fatalf("divergent heads = A %d/%x, B %d/%x",
			branchAHead.revision, branchAHead.token, branchBHead.revision, branchBHead.token)
	}

	branchAContinuation := request
	branchAContinuation.PageToken = branchA.NextPageToken
	page, err := restoredStore.List(context.Background(), testReadScope(), branchAContinuation)
	if !errors.Is(err, control.ErrPageInvalidated) || !reflect.DeepEqual(page, ListPage{}) {
		t.Fatalf("List(branch-A cursor on divergent branch B) = %#v, %v, want zero/ErrPageInvalidated", page, err)
	}
}

func TestIntegrationRevisionZeroIsAuthorizedVisibilityBoundary(t *testing.T) {
	t.Run("corrupt physical ledger alone is nondisclosing", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		emptyHead := ensureIntegrationRevisionZeroTenant(t, database)
		corruptIntegrationRevisionZeroState(t, database, emptyHead, `identity_count = 1`)

		if object, err := store.Get(context.Background(), testReadScope(), "ko-ledger-only", nil); !errors.Is(err, control.ErrNotFound) || !reflect.DeepEqual(object, Object{}) {
			t.Fatalf("Get(missing with corrupt hidden ledger) = %#v, %v, want zero/ErrNotFound", object, err)
		}
		page, err := store.List(context.Background(), testReadScope(), ListRequest{})
		if err != nil || len(page.Objects) != 0 || page.NextPageToken != "" || page.CatalogRevision != 0 {
			t.Fatalf("List(empty with corrupt hidden ledger) = %#v, %v", page, err)
		}
	})

	t.Run("hidden row is nondisclosing but authorized row is corrupt", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		emptyHead := ensureIntegrationRevisionZeroTenant(t, database)
		insertFixtureObject(t, database, fixtureObject{
			id:    "ko-revision-zero-hidden",
			owner: "owner-b",
			versions: []fixtureVersion{{
				definition: aliasDefinition(
					testApp,
					"revision-zero-hidden",
					SharingScopePrivate,
					nil,
					"hidden-*",
				),
				state:     StateActive,
				mutation:  "create",
				timestamp: 10,
			}},
		})
		corruptIntegrationRevisionZeroState(t, database, emptyHead, "")

		if object, err := store.Get(
			context.Background(), testReadScope(), "ko-revision-zero-hidden", nil,
		); !errors.Is(err, control.ErrNotFound) || !reflect.DeepEqual(object, Object{}) {
			t.Fatalf("Get(hidden registry at revision zero) = %#v, %v, want zero/ErrNotFound", object, err)
		}
		page, err := store.List(context.Background(), testReadScope(), ListRequest{})
		if err != nil || len(page.Objects) != 0 || page.NextPageToken != "" || page.CatalogRevision != 0 {
			t.Fatalf("List(hidden registry at revision zero) = %#v, %v", page, err)
		}

		authorized := ReadScope{
			TenantID:       testTenant,
			OwnerID:        "owner-b",
			ReadableAppIDs: []string{testApp},
		}
		object, getErr := store.Get(
			context.Background(), authorized, "ko-revision-zero-hidden", nil,
		)
		if !errors.Is(getErr, ErrCorrupt) || !reflect.DeepEqual(object, Object{}) {
			t.Fatalf("Get(authorized registry at revision zero) = %#v, %v, want zero/ErrCorrupt", object, getErr)
		}
		page, listErr := store.List(context.Background(), authorized, ListRequest{})
		if !errors.Is(listErr, ErrCorrupt) || !reflect.DeepEqual(page, ListPage{}) {
			t.Fatalf("List(authorized registry at revision zero) = %#v, %v, want zero/ErrCorrupt", page, listErr)
		}
	})
}

func TestIntegrationRevisionHeadCorruptionIsGenericAndBodyless(t *testing.T) {
	tests := []struct {
		name       string
		corruption integrationRevisionHeadCorruption
	}{
		{name: "missing", corruption: integrationRevisionHeadMissing},
		{name: "malformed token", corruption: integrationRevisionHeadMalformed},
		{name: "revision mismatch", corruption: integrationRevisionHeadRevisionMismatch},
	}
	var genericGetError, genericListError string
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			insertFixtureObject(t, database, fixtureObject{
				id:    "ko-head-corruption",
				owner: testOwner,
				versions: []fixtureVersion{{
					definition: aliasDefinition(
						testApp,
						"head-corruption",
						SharingScopePrivate,
						nil,
						"head-*",
					),
					state:     StateActive,
					mutation:  "create",
					timestamp: 10,
				}},
			})
			corruptIntegrationRevisionHead(t, database, test.corruption)

			object, getErr := store.Get(context.Background(), testReadScope(), "ko-head-corruption", nil)
			if !errors.Is(getErr, ErrCorrupt) || !reflect.DeepEqual(object, Object{}) {
				t.Fatalf("Get(corrupt revision head) = %#v, %v, want zero/ErrCorrupt", object, getErr)
			}
			page, listErr := store.List(context.Background(), testReadScope(), ListRequest{})
			if !errors.Is(listErr, ErrCorrupt) || !reflect.DeepEqual(page, ListPage{}) {
				t.Fatalf("List(corrupt revision head) = %#v, %v, want zero/ErrCorrupt", page, listErr)
			}
			for operation, operationErr := range map[string]error{"Get": getErr, "List": listErr} {
				if strings.Contains(operationErr.Error(), integrationMalformedRevisionHeadSentinel) {
					t.Errorf("%s error disclosed malformed revision-head bytes: %v", operation, operationErr)
				}
			}
			if index == 0 {
				genericGetError = getErr.Error()
				genericListError = listErr.Error()
				return
			}
			if getErr.Error() != genericGetError || listErr.Error() != genericListError {
				t.Errorf("revision-head corruption disclosed subtype: Get %q/%q, List %q/%q",
					getErr, genericGetError, listErr, genericListError)
			}
		})
	}
}

func ensureIntegrationRevisionZeroTenant(t *testing.T, database *control.DB) integrationRevisionHead {
	t.Helper()
	tx, err := database.SQLDB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin revision-zero tenant fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	ensureIntegrationCatalogLedgers(t, tx)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit revision-zero tenant fixture: %v", err)
	}
	head := readIntegrationRevisionHead(t, database)
	if head.revision != 0 || len(head.token) != 32 {
		t.Fatalf("empty revision head = revision %d/token bytes %d, want 0/32", head.revision, len(head.token))
	}
	return head
}

func readIntegrationRevisionHead(t *testing.T, database *control.DB) integrationRevisionHead {
	t.Helper()
	var head integrationRevisionHead
	if err := database.SQLDB().QueryRowContext(context.Background(), `
		SELECT catalog_revision, state_token
		FROM knowledge_catalog_revision_heads
		WHERE tenant_id = ?
	`, testTenant).Scan(&head.revision, &head.token); err != nil {
		t.Fatalf("read integration revision head: %v", err)
	}
	head.token = bytes.Clone(head.token)
	return head
}

func corruptIntegrationRevisionZeroState(
	t *testing.T,
	database *control.DB,
	emptyHead integrationRevisionHead,
	extraTenantAssignment string,
) {
	t.Helper()
	dropIntegrationTableTriggers(t, database, "knowledge_catalog_tenants")
	dropIntegrationTableTriggers(t, database, "knowledge_catalog_revision_heads")
	connection := integrationCorruptionConnection(t, database)
	defer closeIntegrationCorruptionConnection(t, connection)
	assignment := "catalog_revision = 0"
	if extraTenantAssignment != "" {
		assignment += ", " + extraTenantAssignment
	}
	result, err := connection.ExecContext(context.Background(),
		"UPDATE knowledge_catalog_tenants SET "+assignment+" WHERE tenant_id = ?",
		testTenant,
	)
	if err != nil {
		t.Fatalf("reset corrupt tenant to revision zero: %v", err)
	}
	assertIntegrationRowsAffected(t, result, "reset corrupt tenant to revision zero")
	result, err = connection.ExecContext(context.Background(), `
		UPDATE knowledge_catalog_revision_heads
		SET catalog_revision = 0, state_token = ?
		WHERE tenant_id = ?
	`, emptyHead.token, testTenant)
	if err != nil {
		t.Fatalf("restore valid empty revision head: %v", err)
	}
	assertIntegrationRowsAffected(t, result, "restore valid empty revision head")
}

type integrationRevisionHeadCorruption int

const (
	integrationRevisionHeadMissing integrationRevisionHeadCorruption = iota + 1
	integrationRevisionHeadMalformed
	integrationRevisionHeadRevisionMismatch
	integrationMalformedRevisionHeadSentinel = "malformed-head-secret"
)

func corruptIntegrationRevisionHead(
	t *testing.T,
	database *control.DB,
	corruption integrationRevisionHeadCorruption,
) {
	t.Helper()
	dropIntegrationTableTriggers(t, database, "knowledge_catalog_revision_heads")
	connection := integrationCorruptionConnection(t, database)
	defer closeIntegrationCorruptionConnection(t, connection)
	var (
		result sql.Result
		err    error
	)
	switch corruption {
	case integrationRevisionHeadMissing:
		result, err = connection.ExecContext(context.Background(), `
			DELETE FROM knowledge_catalog_revision_heads WHERE tenant_id = ?
		`, testTenant)
	case integrationRevisionHeadMalformed:
		result, err = connection.ExecContext(context.Background(), `
			UPDATE knowledge_catalog_revision_heads SET state_token = ? WHERE tenant_id = ?
		`, []byte(integrationMalformedRevisionHeadSentinel), testTenant)
	case integrationRevisionHeadRevisionMismatch:
		result, err = connection.ExecContext(context.Background(), `
			UPDATE knowledge_catalog_revision_heads
			SET catalog_revision = catalog_revision + 1
			WHERE tenant_id = ?
		`, testTenant)
	default:
		t.Fatalf("unknown revision-head corruption %d", corruption)
	}
	if err != nil {
		t.Fatalf("apply revision-head corruption %d: %v", corruption, err)
	}
	assertIntegrationRowsAffected(t, result, "apply revision-head corruption")
}

func dropIntegrationTableTriggers(t *testing.T, database *control.DB, table string) {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(context.Background(), `
		SELECT name FROM sqlite_schema
		WHERE type = 'trigger' AND tbl_name = ?
		ORDER BY name
	`, table)
	if err != nil {
		t.Fatalf("list %s triggers: %v", table, err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan %s trigger: %v", table, err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate %s triggers: %v", table, err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close %s trigger rows: %v", table, err)
	}
	for _, name := range names {
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		if _, err := database.SQLDB().ExecContext(context.Background(), "DROP TRIGGER "+quoted); err != nil {
			t.Fatalf("drop %s trigger %q: %v", table, name, err)
		}
	}
}

func integrationCorruptionConnection(t *testing.T, database *control.DB) *sql.Conn {
	t.Helper()
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire revision-head corruption connection: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
		_ = connection.Close()
		t.Fatalf("disable revision-head fixture foreign keys: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		_ = connection.Close()
		t.Fatalf("disable revision-head fixture checks: %v", err)
	}
	return connection
}

func closeIntegrationCorruptionConnection(t *testing.T, connection *sql.Conn) {
	t.Helper()
	if _, err := connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Errorf("restore revision-head fixture checks: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		t.Errorf("restore revision-head fixture foreign keys: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Errorf("close revision-head corruption connection: %v", err)
	}
}

func assertIntegrationRowsAffected(t *testing.T, result sql.Result, operation string) {
	t.Helper()
	if result == nil {
		t.Fatalf("%s returned a nil result", operation)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		t.Fatalf("%s rows = %d, %v, want 1", operation, affected, err)
	}
}
