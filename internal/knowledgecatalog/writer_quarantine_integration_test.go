package knowledgecatalog_test

import (
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
)

func TestWriterQuarantinePublishesBodylessTerminalVersionAndRedactsReplay(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	_, created := harness.createDraft(t, "quarantine-root", "quarantine-root-create-0001")
	root := created.GetKnowledgeObject()
	// Simulate the recovery condition after normal publication. The quarantine
	// scan must bind registry/version/dependency authority without attempting to
	// decode or repair the suspect retained forensic body.
	if _, err := harness.database.SQLDB().ExecContext(
		t.Context(),
		`DROP TRIGGER knowledge_definition_blob_update_is_forbidden`,
	); err != nil {
		t.Fatalf("drop definition blob immutability trigger in recovery fixture: %v", err)
	}
	if _, err := harness.database.SQLDB().ExecContext(
		t.Context(),
		`UPDATE knowledge_definition_blobs SET definition_proto = zeroblob(definition_bytes) WHERE tenant_id = ?`,
		writerTestTenant,
	); err != nil {
		t.Fatalf("corrupt retained definition body in recovery fixture: %v", err)
	}
	if !harness.writer.ReadyForQuarantine() {
		t.Fatal("Writer.ReadyForQuarantine() = false, want true")
	}

	preparationRequest := &opensplunk.PrepareKnowledgeObjectQuarantineRequest{
		KnowledgeObjectId: root.GetKnowledgeObjectId(),
	}
	preparation, err := harness.writer.PrepareQuarantine(
		harness.actorCtx,
		harness.writeScope,
		preparationRequest,
	)
	if err != nil {
		t.Fatalf("Writer.PrepareQuarantine(): %v", err)
	}
	if preparation.GetRootKnowledgeObjectId() != root.GetKnowledgeObjectId() ||
		preparation.GetRecoveryToken() == "" ||
		preparation.GetExpiresAt() == nil || preparation.GetExpiresAt().CheckValid() != nil ||
		preparation.GetDependentCount() != 0 ||
		preparation.GetTenantCatalogRevision() != created.GetTenantCatalogRevision() {
		t.Fatalf("PrepareQuarantine() = %v", preparation)
	}

	request := &opensplunk.QuarantineKnowledgeObjectRequest{
		RecoveryToken:   preparation.GetRecoveryToken(),
		ClientRequestId: "quarantine-root-execute-0001",
	}
	response, err := harness.writer.Quarantine(
		harness.actorCtx,
		harness.writeScope,
		proto.Clone(request).(*opensplunk.QuarantineKnowledgeObjectRequest),
	)
	if err != nil {
		t.Fatalf("Writer.Quarantine(): %v", err)
	}
	if response.GetRootKnowledgeObjectId() != root.GetKnowledgeObjectId() ||
		response.GetTenantCatalogRevision() != preparation.GetTenantCatalogRevision()+1 ||
		len(response.GetTransitions()) != 1 {
		t.Fatalf("Quarantine() = %v", response)
	}
	transition := response.GetTransitions()[0]
	if transition.GetCascadeOrdinal() != 0 ||
		transition.GetKnowledgeObjectId() != root.GetKnowledgeObjectId() ||
		transition.GetPreviousVersion() != root.GetVersion() ||
		transition.GetQuarantinedVersion() != root.GetVersion()+1 ||
		transition.GetReason() != opensplunk.KnowledgeQuarantineReason_KNOWLEDGE_QUARANTINE_REASON_ROOT_CORRUPTION {
		t.Fatalf("root quarantine transition = %v", transition)
	}

	stored := getWriterObject(t, harness, root.GetKnowledgeObjectId(), nil)
	if stored.State != knowledgecatalog.StateQuarantined ||
		stored.Version != root.GetVersion()+1 ||
		stored.Definition != nil || len(stored.DefinitionSHA256) != 0 ||
		stored.QuarantinedAt == nil || stored.DeletedAt != nil ||
		stored.QuarantineReason == nil || *stored.QuarantineReason != "root_corruption" {
		t.Fatalf("quarantined current object = %#v", stored)
	}
	var recoveryCount int64
	if err := harness.database.SQLDB().QueryRowContext(
		t.Context(),
		`SELECT count(*) FROM knowledge_recovery_audit WHERE tenant_id = ? AND knowledge_object_id = ?`,
		writerTestTenant,
		root.GetKnowledgeObjectId(),
	).Scan(&recoveryCount); err != nil {
		t.Fatalf("count knowledge recovery audit: %v", err)
	}
	if recoveryCount != 1 {
		t.Fatalf("knowledge recovery audit count = %d, want 1", recoveryCount)
	}

	if _, err := harness.writer.Quarantine(
		harness.actorCtx,
		harness.writeScope,
		proto.Clone(request).(*opensplunk.QuarantineKnowledgeObjectRequest),
	); !errors.Is(err, knowledgecatalog.ErrIdempotentOutcomeRedacted) {
		t.Fatalf("exact Quarantine replay error = %v, want ErrIdempotentOutcomeRedacted", err)
	}
	altered := proto.Clone(request).(*opensplunk.QuarantineKnowledgeObjectRequest)
	altered.ClientRequestId = "quarantine-root-execute-0002"
	if _, err := harness.writer.Quarantine(
		harness.actorCtx,
		harness.writeScope,
		altered,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("reused committed recovery token error = %v, want ErrNotFound", err)
	}
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterQuarantineRejectsTamperedRecoveryToken(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	_, created := harness.createDraft(t, "quarantine-tamper", "quarantine-tamper-create-0001")
	preparation, err := harness.writer.PrepareQuarantine(
		harness.actorCtx,
		harness.writeScope,
		&opensplunk.PrepareKnowledgeObjectQuarantineRequest{
			KnowledgeObjectId: created.GetKnowledgeObject().GetKnowledgeObjectId(),
		},
	)
	if err != nil {
		t.Fatalf("Writer.PrepareQuarantine(): %v", err)
	}
	tampered := []byte(preparation.GetRecoveryToken())
	if tampered[len(tampered)-1] == 'A' {
		tampered[len(tampered)-1] = 'B'
	} else {
		tampered[len(tampered)-1] = 'A'
	}
	if _, err := harness.writer.Quarantine(
		harness.actorCtx,
		harness.writeScope,
		&opensplunk.QuarantineKnowledgeObjectRequest{
			RecoveryToken:   string(tampered),
			ClientRequestId: "quarantine-tamper-execute-0001",
		},
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Quarantine(tampered token) error = %v, want ErrNotFound", err)
	}
	stored := getWriterObject(t, harness, created.GetKnowledgeObject().GetKnowledgeObjectId(), nil)
	if stored.State != knowledgecatalog.StateDraft || stored.Version != 1 {
		t.Fatalf("object after tampered recovery token = %#v", stored)
	}
}

func TestWriterQuarantineCascadesActiveDependentsBeforeRoot(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	if _, err := harness.database.CreateIndex(t.Context(), control.IndexDefinition{
		Name:             "main",
		DisplayName:      "main",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatalf("control.DB.CreateIndex(main): %v", err)
	}
	selector := func() *opensplunk.KnowledgeSelector {
		return &opensplunk.KnowledgeSelector{
			IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "main"}},
		}
	}
	rootResponse, err := harness.writer.Create(
		harness.actorCtx,
		harness.writeScope,
		&opensplunk.CreateKnowledgeObjectRequest{
			Definition: &opensplunk.KnowledgeObjectDefinition{
				AppId:        writerTestApp,
				Name:         "quarantine-cascade-root",
				SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
				Selector:     selector(),
				Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
					FieldExtraction: &opensplunk.FieldExtractionDefinition{
						InputField:        "_raw",
						OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
						Extraction: &opensplunk.FieldExtractionDefinition_Json{
							Json: &opensplunk.JsonFieldExtractionDefinition{
								Path:        "payload.value",
								OutputField: "cascade_input",
							},
						},
					},
				},
			},
			InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId: "quarantine-cascade-root-create-0001",
		},
	)
	if err != nil {
		t.Fatalf("Writer.Create(ACTIVE root): %v", err)
	}
	root := rootResponse.GetKnowledgeObject()
	dependentResponse, err := harness.writer.Create(
		harness.actorCtx,
		harness.writeScope,
		&opensplunk.CreateKnowledgeObjectRequest{
			Definition: &opensplunk.KnowledgeObjectDefinition{
				AppId:        writerTestApp,
				Name:         "quarantine-cascade-dependent",
				SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
				Selector:     selector(),
				Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
					FieldAlias: &opensplunk.FieldAliasDefinition{
						SourceField:       "cascade_input",
						DestinationField:  "cascade_output",
						OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
					},
				},
			},
			InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId: "quarantine-cascade-dependent-create-0001",
		},
	)
	if err != nil {
		t.Fatalf("Writer.Create(ACTIVE dependent): %v", err)
	}
	dependent := dependentResponse.GetKnowledgeObject()
	preparation, err := harness.writer.PrepareQuarantine(
		harness.actorCtx,
		harness.writeScope,
		&opensplunk.PrepareKnowledgeObjectQuarantineRequest{KnowledgeObjectId: root.GetKnowledgeObjectId()},
	)
	if err != nil {
		t.Fatalf("Writer.PrepareQuarantine(root with dependent): %v", err)
	}
	if preparation.GetDependentCount() != 1 {
		t.Fatalf("PrepareQuarantine dependent_count = %d, want 1", preparation.GetDependentCount())
	}
	response, err := harness.writer.Quarantine(
		harness.actorCtx,
		harness.writeScope,
		&opensplunk.QuarantineKnowledgeObjectRequest{
			RecoveryToken:   preparation.GetRecoveryToken(),
			ClientRequestId: "quarantine-cascade-execute-0001",
		},
	)
	if err != nil {
		t.Fatalf("Writer.Quarantine(root with dependent): %v", err)
	}
	if len(response.GetTransitions()) != 2 {
		t.Fatalf("Quarantine transitions = %v, want two", response.GetTransitions())
	}
	first, second := response.GetTransitions()[0], response.GetTransitions()[1]
	if first.GetCascadeOrdinal() != 0 || first.GetKnowledgeObjectId() != dependent.GetKnowledgeObjectId() ||
		first.GetReason() != opensplunk.KnowledgeQuarantineReason_KNOWLEDGE_QUARANTINE_REASON_DEPENDENCY_RECOVERY ||
		second.GetCascadeOrdinal() != 1 || second.GetKnowledgeObjectId() != root.GetKnowledgeObjectId() ||
		second.GetReason() != opensplunk.KnowledgeQuarantineReason_KNOWLEDGE_QUARANTINE_REASON_ROOT_CORRUPTION {
		t.Fatalf("Quarantine cascade order = %v", response.GetTransitions())
	}
	for _, objectID := range []string{dependent.GetKnowledgeObjectId(), root.GetKnowledgeObjectId()} {
		stored := getWriterObject(t, harness, objectID, nil)
		if stored.State != knowledgecatalog.StateQuarantined || stored.Definition != nil || stored.Version != 2 {
			t.Fatalf("quarantined cascade object %q = %#v", objectID, stored)
		}
	}
	assertWriterCatalogIntegrity(t, harness.database)
}
