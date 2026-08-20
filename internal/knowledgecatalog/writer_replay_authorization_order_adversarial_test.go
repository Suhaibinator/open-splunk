package knowledgecatalog

import (
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const writerReplayRevokedApp = "app_000000000300000000002A"

func TestWriterHiddenExactReplaysAuthorizeBeforeReceiptOrBodyHydration(t *testing.T) {
	tests := []struct {
		name   string
		slug   string
		tamper func(*testing.T, *writerFaultHarness)
	}{
		{name: "healthy", slug: "healthy"},
		{
			name: "corrupt outcome and absent definition blob",
			slug: "corrupt-outcome-missing-blob",
			tamper: func(t *testing.T, harness *writerFaultHarness) {
				t.Helper()
				tamperAllWriterReplayReceipts(t, harness, `outcome_proto = x'00'`, false)
				removeWriterReplayDefinitionBlobs(t, harness)
			},
		},
		{
			name: "oversized outcome",
			slug: "oversized-outcome",
			tamper: func(t *testing.T, harness *writerFaultHarness) {
				t.Helper()
				tamperAllWriterReplayReceipts(
					t, harness, `outcome_proto = zeroblob(1025)`, true,
				)
			},
		},
		{
			name: "altered request digest",
			slug: "altered-request-digest",
			tamper: func(t *testing.T, harness *writerFaultHarness) {
				t.Helper()
				if _, err := harness.database.SQLDB().ExecContext(t.Context(), `
					UPDATE knowledge_mutation_idempotency
					SET request_digest = zeroblob(32)
					WHERE tenant_id = ?`, writerFaultTenant); err == nil {
					t.Fatal("row-only request-digest tamper succeeded with foreign keys enabled")
				}
				tamperAllWriterReplayReceipts(t, harness, `request_digest = zeroblob(32)`, false)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newWriterFaultHarness(t)
			routes := commitWriterReplayRouteSequence(t, harness, "authorization-order-"+test.slug)
			if test.tamper != nil {
				test.tamper(t, harness)
			}

			hiddenScopes := []struct {
				name  string
				scope WriteScope
			}{
				{
					name: "owner hidden",
					scope: WriteScope{
						TenantID:       writerFaultTenant,
						OwnerID:        "owner-without-replay-authority",
						WritableAppIDs: []string{writerFaultApp},
					},
				},
				{
					name: "app revoked",
					scope: WriteScope{
						TenantID:       writerFaultTenant,
						OwnerID:        writerFaultOwner,
						WritableAppIDs: []string{writerReplayRevokedApp},
					},
				},
			}
			for _, hidden := range hiddenScopes {
				for _, route := range routes {
					t.Run(hidden.name+"/"+route.name, func(t *testing.T) {
						responsePresent, err := route.invoke(hidden.scope)
						if responsePresent || !errors.Is(err, control.ErrNotFound) {
							t.Fatalf("hidden %s replay = (response:%t, err:%v), want false/ErrNotFound", route.name, responsePresent, err)
						}
						if err.Error() != control.ErrNotFound.Error() {
							t.Fatalf("hidden %s replay text = %q, want fixed absence text %q", route.name, err, control.ErrNotFound)
						}
					})
				}
			}
		})
	}
}

func TestWriterQuarantinedExactReplaysRedactBeforeCorruptReceiptOrBodyHydration(t *testing.T) {
	harness := newWriterFaultHarness(t)
	routes := commitWriterReplayRouteSequence(t, harness, "quarantine-order")
	tamperAllWriterReplayReceipts(t, harness, `outcome_proto = zeroblob(1025)`, true)
	removeWriterReplayDefinitionBlobs(t, harness)
	quarantineWriterReplayRegistry(t, harness)

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			responsePresent, err := route.invoke(harness.scope)
			if responsePresent || !errors.Is(err, ErrIdempotentOutcomeRedacted) {
				t.Fatalf("quarantined %s replay = (response:%t, err:%v), want false/fixed redaction", route.name, responsePresent, err)
			}
			if err.Error() != ErrIdempotentOutcomeRedacted.Error() {
				t.Fatalf("quarantined %s replay text = %q, want %q", route.name, err, ErrIdempotentOutcomeRedacted)
			}
		})
	}
}

type writerReplayRoute struct {
	name   string
	invoke func(WriteScope) (bool, error)
}

func commitWriterReplayRouteSequence(
	t *testing.T,
	harness *writerFaultHarness,
	slug string,
) []writerReplayRoute {
	t.Helper()
	createRequest := writerFaultCreateRequest(slug, slug+"-create-request-0001")
	created, err := harness.writer.Create(harness.actorContext, harness.scope, createRequest)
	if err != nil {
		t.Fatalf("commit replay Create baseline: %v", err)
	}
	objectID := created.GetKnowledgeObject().GetKnowledgeObjectId()

	definition := proto.Clone(created.GetKnowledgeObject().GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
	description := "authorization-ordered replay update"
	definition.Description = &description
	updateRequest := &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: objectID,
		ExpectedVersion:   1,
		Definition:        definition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   slug + "-update-request-0001",
	}
	if _, err := harness.writer.Update(harness.actorContext, harness.scope, updateRequest); err != nil {
		t.Fatalf("commit replay Update baseline: %v", err)
	}
	stateRequest := &opensplunk.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: objectID,
		ExpectedVersion:   2,
		State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		ClientRequestId:   slug + "-state-request-0001",
	}
	if _, err := harness.writer.SetState(harness.actorContext, harness.scope, stateRequest); err != nil {
		t.Fatalf("commit replay SetState baseline: %v", err)
	}
	deleteRequest := &opensplunk.DeleteKnowledgeObjectRequest{
		KnowledgeObjectId: objectID,
		ExpectedVersion:   3,
		ClientRequestId:   slug + "-delete-request-0001",
	}
	if _, err := harness.writer.Delete(harness.actorContext, harness.scope, deleteRequest); err != nil {
		t.Fatalf("commit replay Delete baseline: %v", err)
	}

	return []writerReplayRoute{
		{
			name: mutationRouteCreate,
			invoke: func(scope WriteScope) (bool, error) {
				response, err := harness.writer.Create(
					harness.actorContext,
					scope,
					proto.Clone(createRequest).(*opensplunk.CreateKnowledgeObjectRequest),
				)
				return response != nil, err
			},
		},
		{
			name: mutationRouteUpdate,
			invoke: func(scope WriteScope) (bool, error) {
				response, err := harness.writer.Update(
					harness.actorContext,
					scope,
					proto.Clone(updateRequest).(*opensplunk.UpdateKnowledgeObjectRequest),
				)
				return response != nil, err
			},
		},
		{
			name: mutationRouteSetState,
			invoke: func(scope WriteScope) (bool, error) {
				response, err := harness.writer.SetState(
					harness.actorContext,
					scope,
					proto.Clone(stateRequest).(*opensplunk.SetKnowledgeObjectStateRequest),
				)
				return response != nil, err
			},
		},
		{
			name: mutationRouteDelete,
			invoke: func(scope WriteScope) (bool, error) {
				response, err := harness.writer.Delete(
					harness.actorContext,
					scope,
					proto.Clone(deleteRequest).(*opensplunk.DeleteKnowledgeObjectRequest),
				)
				return response != nil, err
			},
		},
	}
}

func tamperAllWriterReplayReceipts(
	t *testing.T,
	harness *writerFaultHarness,
	assignment string,
	ignoreChecks bool,
) {
	t.Helper()
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open replay receipt tamper connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable replay receipt foreign keys: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
			t.Errorf("restore replay receipt foreign keys: %v", err)
		}
	}()
	if _, err := connection.ExecContext(t.Context(),
		`DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden`); err != nil {
		t.Fatalf("drop replay receipt immutability guard: %v", err)
	}
	if ignoreChecks {
		if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
			t.Fatalf("enable replay receipt check corruption: %v", err)
		}
		defer func() {
			if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
				t.Errorf("restore replay receipt checks: %v", err)
			}
		}()
	}
	// #nosec G202 -- assignment is selected from a fixed test-case corruption matrix.
	result, err := connection.ExecContext(t.Context(), `UPDATE knowledge_mutation_idempotency SET `+assignment+` WHERE tenant_id = ?`, writerFaultTenant)
	if err != nil {
		t.Fatalf("tamper replay receipts: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 4 {
		t.Fatalf("tampered replay receipt rows = %d, %v; want 4", affected, err)
	}
}

func removeWriterReplayDefinitionBlobs(t *testing.T, harness *writerFaultHarness) {
	t.Helper()
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open replay blob tamper connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `DROP TRIGGER knowledge_definition_blob_delete_is_forbidden`); err != nil {
		t.Fatalf("drop replay blob deletion guard: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable replay blob foreign keys: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
			t.Errorf("restore replay blob foreign keys: %v", err)
		}
	}()
	result, err := connection.ExecContext(t.Context(), `DELETE FROM knowledge_definition_blobs WHERE tenant_id = ?`, writerFaultTenant)
	if err != nil {
		t.Fatalf("remove replay definition blob: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected < 1 {
		t.Fatalf("removed replay definition blob rows = %d, %v; want at least 1", affected, err)
	}
}

func quarantineWriterReplayRegistry(t *testing.T, harness *writerFaultHarness) {
	t.Helper()
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open replay quarantine connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable replay quarantine foreign keys: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
			t.Errorf("restore replay quarantine foreign keys: %v", err)
		}
	}()
	if _, err := connection.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_object_registry_transition_is_valid;
		DROP TRIGGER knowledge_object_update_requires_sealed_list_projection;
		UPDATE knowledge_objects
		SET state = 'quarantined',
		    definition_digest = NULL,
		    updated_at_unix_micro = updated_at_unix_micro + 1,
		    disabled_at_unix_micro = NULL,
		    quarantined_at_unix_micro = updated_at_unix_micro + 1,
		    deleted_at_unix_micro = NULL,
		    quarantine_reason = 'root_corruption'
		WHERE tenant_id = ?`, writerFaultTenant); err != nil {
		t.Fatalf("quarantine replay registry: %v", err)
	}
}
