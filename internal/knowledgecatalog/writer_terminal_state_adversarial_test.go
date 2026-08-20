package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestWriterQuarantinedTerminalStateWinsBeforeDefinitionHydration(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	reason := "root_corruption"
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-writer-quarantined-terminal",
		owner: testOwner,
		versions: []fixtureVersion{
			{
				definition: aliasDefinition(testApp, "writer-quarantined-terminal", SharingScopePrivate, nil, "secret-*"),
				state:      StateActive, mutation: "create", timestamp: 10,
			},
			{state: StateQuarantined, mutation: "quarantine", reason: &reason, timestamp: 20},
		},
	})

	// A quarantine response may never open a retained historical body. Make the
	// v1 wire bytes invalid without changing any byte ledger so an accidental
	// hydration is observable while the tenant remains otherwise healthy.
	dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_definition_blobs
		SET definition_proto = zeroblob(definition_bytes)
		WHERE tenant_id = ?`, testTenant)

	auditStore, err := audit.NewStore(database, audit.StoreOptions{})
	if err != nil {
		t.Fatalf("audit.NewStore: %v", err)
	}
	writer, err := NewWriter(database, auditStore, WriterOptions{
		Clock:                func() time.Time { return time.UnixMicro(30).UTC() },
		IDGenerator:          func() (string, error) { return "unused-terminal-id", nil },
		IdempotencyRetention: minimumIdempotencyRetention,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ctx, err := audit.WithActor(context.Background(), audit.Actor{
		Kind: audit.ActorKindBrowser, ID: testOwner, Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor: %v", err)
	}
	scope := WriteScope{TenantID: testTenant, OwnerID: testOwner, WritableAppIDs: []string{testApp}}
	before := readTerminalWriterSnapshot(t, database)
	updatedDescription := "must not be read"

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "update",
			call: func() error {
				_, err := writer.Update(ctx, scope, &opensplunk.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: "ko-writer-quarantined-terminal",
					ExpectedVersion:   2,
					Definition: aliasDefinition(
						testApp, "writer-quarantined-terminal", SharingScopePrivate, &updatedDescription, "secret-*",
					),
					UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"description"}},
					ClientRequestId: "quarantined-update-0001",
				})
				return err
			},
		},
		{
			name: "set state",
			call: func() error {
				_, err := writer.SetState(ctx, scope, &opensplunk.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: "ko-writer-quarantined-terminal",
					ExpectedVersion:   2,
					State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId:   "quarantined-state-0001",
				})
				return err
			},
		},
		{
			name: "delete",
			call: func() error {
				_, err := writer.Delete(ctx, scope, &opensplunk.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: "ko-writer-quarantined-terminal",
					ExpectedVersion:   2,
					ClientRequestId:   "quarantined-delete-0001",
				})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if !errors.Is(err, control.ErrVersionConflict) {
				t.Fatalf("terminal mutation error = %v, want ErrVersionConflict", err)
			}
			if got := readTerminalWriterSnapshot(t, database); got != before {
				t.Fatalf("terminal mutation changed authorities: got %#v, want %#v", got, before)
			}
		})
	}
}

func TestWriterActiveDependentsBlockDisableAndDeleteAtomically(t *testing.T) {
	for _, route := range []string{"disable", "delete"} {
		t.Run(route, func(t *testing.T) {
			database, _ := newCatalogTestStore(t)
			targetID := "ko-writer-dependent-target-" + route
			insertFixtureObject(t, database, fixtureObject{
				id:    targetID,
				owner: testOwner,
				versions: []fixtureVersion{{
					definition: dependencyExtractionDefinition(
						testApp, "writer-dependent-target-"+route, SharingScopePrivate,
						nil, "dependent-target-*", dependencyFixtureInputField,
					),
					state: StateActive, mutation: "create", timestamp: 10,
				}},
			})
			insertFixtureObject(t, database, fixtureObject{
				id:    "ko-writer-active-dependent-" + route,
				owner: testOwner,
				versions: []fixtureVersion{{
					definition: dependencyAliasDefinition(
						testApp, "writer-active-dependent-"+route, SharingScopePrivate,
						nil, "dependent-source-*", dependencyFixtureInputField, "dependency_alias",
					),
					state: StateActive, mutation: "create", timestamp: 20,
					dependencies: []fixtureDependency{{
						targetObjectID: targetID,
						targetVersion:  1,
					}},
				}},
			})

			auditStore, err := audit.NewStore(database, audit.StoreOptions{})
			if err != nil {
				t.Fatalf("audit.NewStore: %v", err)
			}
			writer, err := NewWriter(database, auditStore, WriterOptions{
				Clock:                func() time.Time { return time.UnixMicro(30).UTC() },
				IDGenerator:          func() (string, error) { return "unused-dependent-id", nil },
				IdempotencyRetention: minimumIdempotencyRetention,
			})
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			ctx, err := audit.WithActor(context.Background(), audit.Actor{
				Kind: audit.ActorKindBrowser, ID: testOwner, Role: audit.ActorRoleAdministrator,
			})
			if err != nil {
				t.Fatalf("audit.WithActor: %v", err)
			}
			scope := WriteScope{TenantID: testTenant, OwnerID: testOwner, WritableAppIDs: []string{testApp}}
			before := readTerminalWriterSnapshot(t, database)
			if route == "disable" {
				_, err = writer.SetState(ctx, scope, &opensplunk.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: targetID,
					ExpectedVersion:   1,
					State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId:   "dependent-disable-0001",
				})
			} else {
				_, err = writer.Delete(ctx, scope, &opensplunk.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: targetID,
					ExpectedVersion:   1,
					ClientRequestId:   "dependent-delete-0001",
				})
			}
			if !errors.Is(err, control.ErrDependencyConflict) {
				t.Fatalf("%s active dependency error = %v, want ErrDependencyConflict", route, err)
			}
			if got := readTerminalWriterSnapshot(t, database); got != before {
				t.Fatalf("%s active dependency changed authorities: got %#v, want %#v", route, got, before)
			}
		})
	}
}

type terminalWriterSnapshot struct {
	Revision       int64
	VersionCount   int64
	Idempotency    int64
	AuditEvents    int64
	StateTokenText string
}

func readTerminalWriterSnapshot(t *testing.T, database *control.DB) terminalWriterSnapshot {
	t.Helper()
	var snapshot terminalWriterSnapshot
	var token []byte
	if err := database.SQLDB().QueryRowContext(t.Context(), `SELECT
		tenant.catalog_revision,
		tenant.version_count,
		tenant.idempotency_count,
		(SELECT count(*) FROM audit_events WHERE tenant_id = tenant.tenant_id),
		head.state_token
	FROM knowledge_catalog_tenants AS tenant
	JOIN knowledge_catalog_revision_heads AS head
	  ON head.tenant_id = tenant.tenant_id
	 AND head.catalog_revision = tenant.catalog_revision
	WHERE tenant.tenant_id = ?`, testTenant).Scan(
		&snapshot.Revision,
		&snapshot.VersionCount,
		&snapshot.Idempotency,
		&snapshot.AuditEvents,
		&token,
	); err != nil {
		t.Fatalf("read terminal writer snapshot: %v", err)
	}
	snapshot.StateTokenText = string(bytes.Clone(token))
	return snapshot
}
