package knowledgecatalog

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestWriterRoutesUseDetachedPreparedRequestSnapshot(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		harness := newWriterFaultHarness(t)
		request := writerFaultCreateRequest("detached-create", "detached-create-request-0001")
		want := proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest)
		harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
			if event.Boundary == writerHookPrepared && event.Route == mutationRouteCreate {
				*request = opensplunkv1.CreateKnowledgeObjectRequest{
					InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
					ClientRequestId: "caller-mutated-create-request",
				}
			}
			return nil
		}

		response, err := harness.writer.Create(harness.actorContext, harness.scope, request)
		harness.writer.hook = nil
		if err != nil {
			t.Fatalf("Create() after caller mutation at prepared hook: %v", err)
		}
		if response.GetKnowledgeObject().GetState() != want.GetInitialState() ||
			!proto.Equal(response.GetKnowledgeObject().GetDefinition(), want.GetDefinition()) {
			t.Fatalf("Create() used caller mutation: got %v want definition %v", response, want.GetDefinition())
		}
		assertDetachedRequestReceipt(t, harness, mutationRouteCreate, want.GetClientRequestId())
	})

	t.Run("update", func(t *testing.T) {
		harness := newWriterFaultHarness(t)
		created := commitDetachedRequestBaseline(t, harness, "detached-update")
		definition := proto.Clone(created.GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
		description := "detached update description"
		definition.Description = &description
		request := &opensplunkv1.UpdateKnowledgeObjectRequest{
			KnowledgeObjectId: created.GetKnowledgeObjectId(),
			ExpectedVersion:   created.GetVersion(),
			Definition:        definition,
			UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
			ClientRequestId:   "detached-update-request-0001",
		}
		want := proto.Clone(request).(*opensplunkv1.UpdateKnowledgeObjectRequest)
		harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
			if event.Boundary == writerHookPrepared && event.Route == mutationRouteUpdate {
				*request = opensplunkv1.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: "ko_caller_mutated_update_target",
					ExpectedVersion:   99,
					ClientRequestId:   "caller-mutated-update-request",
				}
			}
			return nil
		}

		response, err := harness.writer.Update(harness.actorContext, harness.scope, request)
		harness.writer.hook = nil
		if err != nil {
			t.Fatalf("Update() after caller mutation at prepared hook: %v", err)
		}
		if response.GetKnowledgeObject().GetKnowledgeObjectId() != want.GetKnowledgeObjectId() ||
			response.GetKnowledgeObject().GetVersion() != want.GetExpectedVersion()+1 ||
			response.GetKnowledgeObject().GetDefinition().GetDescription() != description {
			t.Fatalf("Update() used caller mutation: got %v want target %q", response, want.GetKnowledgeObjectId())
		}
		assertDetachedRequestReceipt(t, harness, mutationRouteUpdate, want.GetClientRequestId())
	})

	t.Run("set state", func(t *testing.T) {
		harness := newWriterFaultHarness(t)
		created := commitDetachedRequestBaseline(t, harness, "detached-state")
		request := &opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: created.GetKnowledgeObjectId(),
			ExpectedVersion:   created.GetVersion(),
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			ClientRequestId:   "detached-state-request-0001",
		}
		want := proto.Clone(request).(*opensplunkv1.SetKnowledgeObjectStateRequest)
		harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
			if event.Boundary == writerHookPrepared && event.Route == mutationRouteSetState {
				*request = opensplunkv1.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: "ko_caller_mutated_state_target",
					ExpectedVersion:   99,
					State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
					ClientRequestId:   "caller-mutated-state-request",
				}
			}
			return nil
		}

		response, err := harness.writer.SetState(harness.actorContext, harness.scope, request)
		harness.writer.hook = nil
		if err != nil {
			t.Fatalf("SetState() after caller mutation at prepared hook: %v", err)
		}
		if response.GetKnowledgeObject().GetKnowledgeObjectId() != want.GetKnowledgeObjectId() ||
			response.GetKnowledgeObject().GetVersion() != want.GetExpectedVersion()+1 ||
			response.GetKnowledgeObject().GetState() != want.GetState() {
			t.Fatalf("SetState() used caller mutation: got %v want state %v", response, want.GetState())
		}
		assertDetachedRequestReceipt(t, harness, mutationRouteSetState, want.GetClientRequestId())
	})

	t.Run("delete", func(t *testing.T) {
		harness := newWriterFaultHarness(t)
		created := commitDetachedRequestBaseline(t, harness, "detached-delete")
		request := &opensplunkv1.DeleteKnowledgeObjectRequest{
			KnowledgeObjectId: created.GetKnowledgeObjectId(),
			ExpectedVersion:   created.GetVersion(),
			ClientRequestId:   "detached-delete-request-0001",
		}
		want := proto.Clone(request).(*opensplunkv1.DeleteKnowledgeObjectRequest)
		harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
			if event.Boundary == writerHookPrepared && event.Route == mutationRouteDelete {
				*request = opensplunkv1.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: "ko_caller_mutated_delete_target",
					ExpectedVersion:   99,
					ClientRequestId:   "caller-mutated-delete-request",
				}
			}
			return nil
		}

		response, err := harness.writer.Delete(harness.actorContext, harness.scope, request)
		harness.writer.hook = nil
		if err != nil {
			t.Fatalf("Delete() after caller mutation at prepared hook: %v", err)
		}
		if response.GetKnowledgeObjectId() != want.GetKnowledgeObjectId() ||
			response.GetDeletedVersion() != want.GetExpectedVersion()+1 {
			t.Fatalf("Delete() used caller mutation: got %v want target %q", response, want.GetKnowledgeObjectId())
		}
		assertDetachedRequestReceipt(t, harness, mutationRouteDelete, want.GetClientRequestId())
	})
}

func TestWriterPreparedRequestSnapshotIsRaceIsolated(t *testing.T) {
	tests := []struct {
		name     string
		route    string
		exercise func(*testing.T, *writerFaultHarness) (func(), func() error)
	}{
		{
			name:  "create",
			route: mutationRouteCreate,
			exercise: func(_ *testing.T, harness *writerFaultHarness) (func(), func() error) {
				request := writerFaultCreateRequest("race-detached-create", "race-detached-create-request-0001")
				var generation uint64
				mutate := func() {
					generation++
					*request = opensplunkv1.CreateKnowledgeObjectRequest{
						InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
						ClientRequestId: fmt.Sprintf("caller-race-create-%d", generation),
					}
				}
				invoke := func() error {
					response, err := harness.writer.Create(harness.actorContext, harness.scope, request)
					if err == nil && (response == nil || response.GetKnowledgeObject().GetName() != "race-detached-create") {
						return fmt.Errorf("unexpected detached Create response: %v", response)
					}
					return err
				}
				return mutate, invoke
			},
		},
		{
			name:  "update",
			route: mutationRouteUpdate,
			exercise: func(t *testing.T, harness *writerFaultHarness) (func(), func() error) {
				created := commitDetachedRequestBaseline(t, harness, "race-detached-update")
				definition := proto.Clone(created.GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
				description := "race detached update"
				definition.Description = &description
				request := &opensplunkv1.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: created.GetKnowledgeObjectId(),
					ExpectedVersion:   created.GetVersion(),
					Definition:        definition,
					UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
					ClientRequestId:   "race-detached-update-request-0001",
				}
				var generation uint64
				mutate := func() {
					generation++
					*request = opensplunkv1.UpdateKnowledgeObjectRequest{
						KnowledgeObjectId: fmt.Sprintf("ko_caller_race_update_%d", generation),
						ExpectedVersion:   99,
					}
				}
				invoke := func() error {
					response, err := harness.writer.Update(harness.actorContext, harness.scope, request)
					if err == nil && (response == nil || response.GetKnowledgeObject().GetDefinition().GetDescription() != description) {
						return fmt.Errorf("unexpected detached Update response: %v", response)
					}
					return err
				}
				return mutate, invoke
			},
		},
		{
			name:  "set state",
			route: mutationRouteSetState,
			exercise: func(t *testing.T, harness *writerFaultHarness) (func(), func() error) {
				created := commitDetachedRequestBaseline(t, harness, "race-detached-state")
				request := &opensplunkv1.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: created.GetKnowledgeObjectId(),
					ExpectedVersion:   created.GetVersion(),
					State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId:   "race-detached-state-request-0001",
				}
				var generation uint64
				mutate := func() {
					generation++
					*request = opensplunkv1.SetKnowledgeObjectStateRequest{
						KnowledgeObjectId: fmt.Sprintf("ko_caller_race_state_%d", generation),
						ExpectedVersion:   99,
						State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
					}
				}
				invoke := func() error {
					response, err := harness.writer.SetState(harness.actorContext, harness.scope, request)
					if err == nil && (response == nil || response.GetKnowledgeObject().GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED) {
						return fmt.Errorf("unexpected detached SetState response: %v", response)
					}
					return err
				}
				return mutate, invoke
			},
		},
		{
			name:  "delete",
			route: mutationRouteDelete,
			exercise: func(t *testing.T, harness *writerFaultHarness) (func(), func() error) {
				created := commitDetachedRequestBaseline(t, harness, "race-detached-delete")
				objectID := created.GetKnowledgeObjectId()
				request := &opensplunkv1.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: objectID,
					ExpectedVersion:   created.GetVersion(),
					ClientRequestId:   "race-detached-delete-request-0001",
				}
				var generation uint64
				mutate := func() {
					generation++
					*request = opensplunkv1.DeleteKnowledgeObjectRequest{
						KnowledgeObjectId: fmt.Sprintf("ko_caller_race_delete_%d", generation),
						ExpectedVersion:   99,
					}
				}
				invoke := func() error {
					response, err := harness.writer.Delete(harness.actorContext, harness.scope, request)
					if err == nil && (response == nil || response.GetKnowledgeObjectId() != objectID) {
						return fmt.Errorf("unexpected detached Delete response: %v", response)
					}
					return err
				}
				return mutate, invoke
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newWriterFaultHarness(t)
			mutate, invoke := test.exercise(t, harness)
			exercisePreparedRequestMutationStorm(t, harness.writer, test.route, mutate, invoke)
		})
	}
}

func exercisePreparedRequestMutationStorm(
	t *testing.T,
	writer *Writer,
	route string,
	mutate func(),
	invoke func() error,
) {
	t.Helper()
	started := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan struct{})
	writer.hook = func(_ context.Context, event writerHookEvent) error {
		if event.Boundary != writerHookPrepared || event.Route != route {
			return nil
		}
		go func() {
			defer close(done)
			mutate()
			close(started)
			for {
				select {
				case <-stop:
					return
				default:
					mutate()
					runtime.Gosched()
				}
			}
		}()
		<-started
		return nil
	}
	err := invoke()
	close(stop)
	<-done
	writer.hook = nil
	if err != nil {
		t.Fatalf("%s with caller mutation storm: %v", route, err)
	}
}

func commitDetachedRequestBaseline(
	t *testing.T,
	harness *writerFaultHarness,
	name string,
) *opensplunkv1.KnowledgeObject {
	t.Helper()
	response, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		writerFaultCreateRequest(name, name+"-create-request-0001"),
	)
	if err != nil {
		t.Fatalf("create %s baseline: %v", name, err)
	}
	return response.GetKnowledgeObject()
}

func assertDetachedRequestReceipt(
	t *testing.T,
	harness *writerFaultHarness,
	route string,
	requestID string,
) {
	t.Helper()
	var count int64
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`,
		writerFaultTenant,
		"browser",
		"writer-fault-administrator",
		route,
		requestID,
	).Scan(&count); err != nil {
		t.Fatalf("count detached request receipt: %v", err)
	}
	if count != 1 {
		t.Fatalf("detached request receipt count = %d, want 1", count)
	}
}
