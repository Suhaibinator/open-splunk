package knowledgecatalog_test

import (
	"io/fs"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/migrations"
	"google.golang.org/protobuf/proto"
)

// Start the writer harness on the released schema, before it creates any data.
// The harness owns this new, disposable database; production upgrades never
// disable foreign keys or edit the migration ledger.
func initializeReleasedWriterSchema(t *testing.T, database *control.DB) {
	t.Helper()
	conn, err := database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err := conn.QueryContext(t.Context(), `SELECT type, name FROM sqlite_schema WHERE type IN ('trigger', 'table') AND name NOT LIKE 'sqlite_%' ORDER BY type DESC`)
	if err != nil {
		t.Fatal(err)
	}
	var drops []string
	for rows.Next() {
		var kind, name string
		if err := rows.Scan(&kind, &name); err != nil {
			t.Fatal(err)
		}
		drops = append(drops, "DROP "+kind+` "`+strings.ReplaceAll(name, `"`, `""`)+`";`)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), "PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE;"+strings.Join(drops, "\n")+"COMMIT; PRAGMA foreign_keys = ON;"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	released := fstest.MapFS{}
	entries, err := fs.ReadDir(migrations.SQLite(), ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() >= "0012_" {
			continue
		}
		data, err := fs.ReadFile(migrations.SQLite(), entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		released[entry.Name()] = &fstest.MapFile{Data: data}
	}
	if err := control.ApplyMigrations(t.Context(), database.SQLDB(), released); err != nil {
		t.Fatal(err)
	}
}

func TestWriterReleasedSchemaUpgradePreservesAuditReferences(t *testing.T) {
	for _, failAfterRebuild := range []bool{false, true} {
		name := "commit"
		if failAfterRebuild {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			harness := newWriterBlackboxHarness(t, func(db *control.DB) { initializeReleasedWriterSchema(t, db) })
			request, response := harness.createDraft(t, "before-upgrade", "migration-create-request-0001")
			before := readWriterAuthoritySnapshot(t, harness.database)
			assertWriterTableCounts(t, before, map[string]int64{
				"audit_events": 1, "knowledge_mutation_commit_authorities": 1, "knowledge_mutation_idempotency": 1,
			})
			events := knowledgeAuditEvents(t, harness)
			migrationFS := migrations.SQLite()
			if failAfterRebuild {
				broken := fstest.MapFS{}
				entries, err := fs.ReadDir(migrationFS, ".")
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					data, err := fs.ReadFile(migrationFS, entry.Name())
					if err != nil {
						t.Fatal(err)
					}
					broken[entry.Name()] = &fstest.MapFile{Data: data}
				}
				broken["0013_failure.sql"] = &fstest.MapFile{Data: []byte("CREATE TABL invalid;")}
				if err := control.ApplyMigrations(t.Context(), harness.database.SQLDB(), broken); err == nil {
					t.Fatal("expected migration failure")
				}
				var version int
				if err := harness.database.SQLDB().QueryRowContext(t.Context(), `SELECT max(version) FROM schema_migrations`).Scan(&version); err != nil {
					t.Fatal(err)
				}
				if version != 11 {
					t.Fatalf("rolled-back version = %d, want 11", version)
				}
				if after := readWriterAuthoritySnapshot(t, harness.database); !reflect.DeepEqual(before, after) {
					t.Fatal("rollback changed knowledge authority")
				}
			}
			if err := control.ApplyMigrations(t.Context(), harness.database.SQLDB(), migrationFS); err != nil {
				t.Fatalf("upgrade released database: %v", err)
			}
			if after := readWriterAuthoritySnapshot(t, harness.database); !reflect.DeepEqual(before, after) {
				t.Fatal("upgrade changed knowledge authority")
			}
			if after := knowledgeAuditEvents(t, harness); !reflect.DeepEqual(events, after) {
				t.Fatal("upgrade changed audit events")
			}
			var violations, enabled, version int
			if err := harness.database.SQLDB().QueryRowContext(t.Context(), `SELECT count(*) FROM pragma_foreign_key_check`).Scan(&violations); err != nil {
				t.Fatal(err)
			}
			if err := harness.database.SQLDB().QueryRowContext(t.Context(), `PRAGMA foreign_keys`).Scan(&enabled); err != nil {
				t.Fatal(err)
			}
			if err := harness.database.SQLDB().QueryRowContext(t.Context(), `SELECT max(version) FROM schema_migrations`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if violations != 0 || enabled != 1 || version != 12 {
				t.Fatalf("violations=%d foreign_keys=%d version=%d", violations, enabled, version)
			}
			replay, err := harness.writer.Create(harness.actorCtx, harness.writeScope, request)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(response, replay) {
				t.Fatal("upgrade changed idempotent replay")
			}
			harness.createDraft(t, "after-upgrade", "migration-create-request-0002")
			assertWriterCatalogIntegrity(t, harness.database)
			if err := control.ApplyMigrations(t.Context(), harness.database.SQLDB(), migrationFS); err != nil {
				t.Fatalf("reopen current migration history: %v", err)
			}
		})
	}
}
