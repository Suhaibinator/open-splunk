package knowledgecatalog_test

import (
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestCatalogWriterAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledgeCatalogBlackbox, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"lifecycle-state-machine": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "lifecycle.disable-delete-reenable", Stage: "publication", Expect: "versioned-and-revalidated"},
		}, func(t *testing.T) {
			t.Run("public-state-machine", TestWriterDeterministicStateMachine)
			t.Run("reenable-revalidation", reenableLifecycleContract)
		}),
		"publication-capacity": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "resource.catalog-capacity", Stage: "publication", Expect: "rejected-capacity"},
		}, func(t *testing.T) {
			t.Run("normal-capacity", TestWriterUnexpiredIdempotencyCapacityRejectsBeforeEveryPublicationAuthority)
			t.Run("absolute-reserve", TestWriterAbsoluteCapacityReclaimsExactNeededOldestReceiptsAndPreservesReserve)
		}),
	})
}

func reenableLifecycleContract(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	if _, err := harness.database.CreateIndex(t.Context(), control.IndexDefinition{
		Name: "main", SearchEnabled: true,
	}); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	_, created := harness.createDraft(t, "compatibility-lifecycle", "compat-lifecycle-create-0001")
	object := created.GetKnowledgeObject()
	if object.GetVersion() != 1 || object.GetState() != opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT {
		t.Fatalf("created lifecycle object = %v", object)
	}

	activate := func(version uint64, requestID string) *opensplunk.KnowledgeObject {
		t.Helper()
		response, err := harness.writer.SetState(harness.actorCtx, harness.writeScope, &opensplunk.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: object.GetKnowledgeObjectId(),
			ExpectedVersion:   version,
			State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId:   requestID,
		})
		if err != nil {
			t.Fatalf("SetState(ACTIVE at version %d): %v", version, err)
		}
		return response.GetKnowledgeObject()
	}
	disable := func(version uint64, requestID string) *opensplunk.KnowledgeObject {
		t.Helper()
		response, err := harness.writer.SetState(harness.actorCtx, harness.writeScope, &opensplunk.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: object.GetKnowledgeObjectId(),
			ExpectedVersion:   version,
			State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			ClientRequestId:   requestID,
		})
		if err != nil {
			t.Fatalf("SetState(DISABLED at version %d): %v", version, err)
		}
		return response.GetKnowledgeObject()
	}

	activeV2 := activate(1, "compat-lifecycle-enable-0002")
	if activeV2.GetVersion() != 2 || activeV2.GetState() != opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE {
		t.Fatalf("active v2 = %v", activeV2)
	}
	disabledV3 := disable(2, "compat-lifecycle-disable-0003")
	if disabledV3.GetVersion() != 3 || disabledV3.GetState() != opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED {
		t.Fatalf("disabled v3 = %v", disabledV3)
	}

	editedDefinition := proto.Clone(disabledV3.GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
	description := "disabled candidate revalidated on activation"
	editedDefinition.Description = &description
	edited, err := harness.writer.Update(harness.actorCtx, harness.writeScope, &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: object.GetKnowledgeObjectId(),
		ExpectedVersion:   3,
		Definition:        editedDefinition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   "compat-lifecycle-update-0004",
	})
	if err != nil {
		t.Fatalf("Update(DISABLED): %v", err)
	}
	if edited.GetKnowledgeObject().GetVersion() != 4 ||
		edited.GetKnowledgeObject().GetState() != opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED {
		t.Fatalf("edited v4 = %v", edited.GetKnowledgeObject())
	}
	activeV5 := activate(4, "compat-lifecycle-reenable-0005")
	if activeV5.GetVersion() != 5 || activeV5.GetState() != opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE ||
		activeV5.GetDefinition().GetDescription() != description {
		t.Fatalf("reactivated v5 = %v", activeV5)
	}

	deleted, err := harness.writer.Delete(harness.actorCtx, harness.writeScope, &opensplunk.DeleteKnowledgeObjectRequest{
		KnowledgeObjectId: object.GetKnowledgeObjectId(),
		ExpectedVersion:   5,
		ClientRequestId:   "compat-lifecycle-delete-0006",
	})
	if err != nil || deleted.GetDeletedVersion() != 6 {
		t.Fatalf("Delete(ACTIVE v5) = (%v, %v), want deleted version 6", deleted, err)
	}

	wantStates := []knowledgecatalog.State{
		knowledgecatalog.StateDraft,
		knowledgecatalog.StateActive,
		knowledgecatalog.StateDisabled,
		knowledgecatalog.StateDisabled,
		knowledgecatalog.StateActive,
		knowledgecatalog.StateDeleted,
	}
	for index, wantState := range wantStates {
		version := uint64(index + 1)
		got, err := harness.reader.Get(harness.actorCtx, harness.readScope, object.GetKnowledgeObjectId(), &version)
		if err != nil || got.Version != version || got.State != wantState {
			t.Fatalf("Get(history v%d) = (%#v, %v), want state %s", version, got, err, wantState)
		}
	}
}
