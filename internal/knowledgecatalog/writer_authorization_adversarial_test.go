package knowledgecatalog_test

import (
	"errors"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestWriterHiddenCorruptRegistryIsIndistinguishableFromAbsence(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	description := "hidden draft"
	hiddenDefinition := writerAliasDefinition(
		writerTestAppTwo,
		"hidden-corrupt-writer-object",
		&description,
		opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		"hidden-corrupt-host",
		"source_field",
		"hidden_destination",
	)
	created, err := harness.writer.Create(harness.actorCtx, harness.writeScope, &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition:      hiddenDefinition,
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "hidden-corrupt-create-0001",
	})
	if err != nil {
		t.Fatalf("create hidden object: %v", err)
	}
	hiddenID := created.GetKnowledgeObject().GetKnowledgeObjectId()

	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open corruption connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_object_registry_transition_is_valid;
		DROP TRIGGER knowledge_object_update_requires_sealed_list_projection;
		PRAGMA foreign_keys = OFF;
		PRAGMA ignore_check_constraints = ON;
		UPDATE knowledge_objects
		SET name = ?
		WHERE tenant_id = ? AND knowledge_object_id = ?;
		PRAGMA ignore_check_constraints = OFF;
		PRAGMA foreign_keys = ON`,
		strings.Repeat("x", 1<<20), writerTestTenant, hiddenID,
	); err != nil {
		t.Fatalf("inject hidden registry corruption: %v", err)
	}

	restrictedScope := harness.writeScope
	restrictedScope.WritableAppIDs = []string{writerTestApp}
	stable := readWriterAuthoritySnapshot(t, harness.database)
	missingID := "ko_missing_hidden_equivalent"
	updatedDefinition := proto.Clone(hiddenDefinition).(*opensplunkv1.KnowledgeObjectDefinition)
	updatedDescription := "updated hidden draft"
	updatedDefinition.Description = &updatedDescription

	tests := []struct {
		name   string
		hidden func() error
		absent func() error
	}{
		{
			name: "update",
			hidden: func() error {
				_, err := harness.writer.Update(harness.actorCtx, restrictedScope, &opensplunkv1.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: hiddenID,
					ExpectedVersion:   1,
					Definition:        updatedDefinition,
					UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
					ClientRequestId:   "hidden-corrupt-update-0001",
				})
				return err
			},
			absent: func() error {
				_, err := harness.writer.Update(harness.actorCtx, restrictedScope, &opensplunkv1.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: missingID,
					ExpectedVersion:   1,
					Definition:        updatedDefinition,
					UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
					ClientRequestId:   "missing-corrupt-update-0001",
				})
				return err
			},
		},
		{
			name: "set state",
			hidden: func() error {
				_, err := harness.writer.SetState(harness.actorCtx, restrictedScope, &opensplunkv1.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: hiddenID,
					ExpectedVersion:   1,
					State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId:   "hidden-corrupt-state-0001",
				})
				return err
			},
			absent: func() error {
				_, err := harness.writer.SetState(harness.actorCtx, restrictedScope, &opensplunkv1.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: missingID,
					ExpectedVersion:   1,
					State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId:   "missing-corrupt-state-0001",
				})
				return err
			},
		},
		{
			name: "delete",
			hidden: func() error {
				_, err := harness.writer.Delete(harness.actorCtx, restrictedScope, &opensplunkv1.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: hiddenID,
					ExpectedVersion:   1,
					ClientRequestId:   "hidden-corrupt-delete-0001",
				})
				return err
			},
			absent: func() error {
				_, err := harness.writer.Delete(harness.actorCtx, restrictedScope, &opensplunkv1.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: missingID,
					ExpectedVersion:   1,
					ClientRequestId:   "missing-corrupt-delete-0001",
				})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hiddenErr := test.hidden()
			absentErr := test.absent()
			if !errors.Is(hiddenErr, control.ErrNotFound) || !errors.Is(absentErr, control.ErrNotFound) {
				t.Fatalf("hidden error = %v, absent error = %v; want ErrNotFound", hiddenErr, absentErr)
			}
			if hiddenErr.Error() != absentErr.Error() {
				t.Fatalf("hidden error text = %q, absent error text = %q", hiddenErr, absentErr)
			}
			assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
		})
	}
}
