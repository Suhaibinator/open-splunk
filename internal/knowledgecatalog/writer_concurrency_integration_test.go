package knowledgecatalog_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestWriterConcurrentOptimisticUpdateHasExactlyOneWinner(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	_, created := harness.createDraft(t, "optimistic-race", "race-create-request-0001")
	object := created.GetKnowledgeObject()

	const contenders = 16
	start := make(chan struct{})
	results := make(chan writerUpdateRaceResult, contenders)
	var ready sync.WaitGroup
	ready.Add(contenders)
	for index := range contenders {
		go func() {
			definition := proto.Clone(object.GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
			description := fmt.Sprintf("optimistic winner %02d", index)
			definition.Description = &description
			request := &opensplunk.UpdateKnowledgeObjectRequest{
				KnowledgeObjectId: object.GetKnowledgeObjectId(),
				ExpectedVersion:   1,
				Definition:        definition,
				UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
				ClientRequestId:   fmt.Sprintf("race-update-request-%04d", index),
			}
			ready.Done()
			<-start
			response, err := harness.writer.Update(harness.actorCtx, harness.writeScope, request)
			results <- writerUpdateRaceResult{index: index, response: response, err: err}
		}()
	}
	ready.Wait()
	close(start)

	winner := -1
	var winnerResponse *opensplunk.UpdateKnowledgeObjectResponse
	conflicts := 0
	for range contenders {
		result := <-results
		switch {
		case result.err == nil:
			if winner != -1 {
				t.Fatalf("optimistic race had multiple winners: %d and %d", winner, result.index)
			}
			winner = result.index
			winnerResponse = result.response
		case errors.Is(result.err, control.ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("optimistic contender %d error = %v, want success or ErrVersionConflict", result.index, result.err)
		}
	}
	if winner == -1 || conflicts != contenders-1 {
		t.Fatalf("optimistic race winner=%d conflicts=%d, want one winner and %d conflicts", winner, conflicts, contenders-1)
	}
	wantDescription := fmt.Sprintf("optimistic winner %02d", winner)
	if winnerResponse.GetKnowledgeObject().GetVersion() != 2 ||
		winnerResponse.GetKnowledgeObject().GetDefinition().GetDescription() != wantDescription {
		t.Fatalf("winner response = %v, want description %q", winnerResponse, wantDescription)
	}
	current := getWriterObject(t, harness, object.GetKnowledgeObjectId(), nil)
	if current.Version != 2 || current.Definition.GetDescription() != wantDescription {
		t.Fatalf("current object after optimistic race = %#v, want version 2 description %q", current, wantDescription)
	}
	snapshot := readWriterAuthoritySnapshot(t, harness.database)
	if snapshot.CatalogRevision != 2 || snapshot.IdentityCount != 1 || snapshot.VersionCount != 2 ||
		snapshot.IdempotencyCount != 2 || snapshot.AuditNextSequence != 3 || snapshot.AuditEventCount != 2 ||
		snapshot.TableCounts["audit_events"] != 2 ||
		snapshot.TableCounts["knowledge_object_versions"] != 2 {
		t.Fatalf("optimistic race authority = %#v", snapshot)
	}
	for index := range contenders {
		var count int
		if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
			SELECT count(*)
			FROM knowledge_mutation_idempotency
			WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
			  AND route = 'objects.update' AND client_request_id = ?`,
			writerTestTenant,
			"browser",
			"writer-blackbox-administrator",
			fmt.Sprintf("race-update-request-%04d", index),
		).Scan(&count); err != nil {
			t.Fatalf("count contender %d receipt: %v", index, err)
		}
		want := 0
		if index == winner {
			want = 1
		}
		if count != want {
			t.Errorf("contender %d idempotency rows = %d, want %d", index, count, want)
		}
	}
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterConcurrentSameIdempotencyKeyConvergesOnOneOutcome(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	description := "same-key concurrent create"
	request := &opensplunk.CreateKnowledgeObjectRequest{
		Definition: writerAliasDefinition(
			writerTestApp,
			"same-key-race",
			&description,
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			"same-key-host",
			"source_field",
			"destination_field",
		),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "same-key-create-request-01",
	}

	const contenders = 16
	start := make(chan struct{})
	results := make(chan writerCreateRaceResult, contenders)
	var ready sync.WaitGroup
	ready.Add(contenders)
	for index := range contenders {
		go func() {
			cloned := proto.Clone(request).(*opensplunk.CreateKnowledgeObjectRequest)
			ready.Done()
			<-start
			response, err := harness.writer.Create(harness.actorCtx, harness.writeScope, cloned)
			results <- writerCreateRaceResult{index: index, response: response, err: err}
		}()
	}
	ready.Wait()
	close(start)

	var committed *opensplunk.CreateKnowledgeObjectResponse
	for range contenders {
		result := <-results
		if result.err != nil {
			t.Fatalf("same-key contender %d error = %v", result.index, result.err)
		}
		if committed == nil {
			committed = result.response
			continue
		}
		if !proto.Equal(result.response, committed) {
			t.Fatalf("same-key contender %d outcome = %v, want %v", result.index, result.response, committed)
		}
	}
	if committed == nil {
		t.Fatal("same-key race returned no committed response")
	}
	if harness.idCalls.Load() != 1 || harness.clockCalls.Load() != 1 {
		t.Fatalf("same-key generator calls: IDs=%d clocks=%d, want 1/1", harness.idCalls.Load(), harness.clockCalls.Load())
	}
	snapshot := readWriterAuthoritySnapshot(t, harness.database)
	if snapshot.CatalogRevision != 1 || snapshot.IdentityCount != 1 || snapshot.VersionCount != 1 ||
		snapshot.IdempotencyCount != 1 || snapshot.AuditNextSequence != 2 || snapshot.AuditEventCount != 1 ||
		snapshot.TableCounts["audit_events"] != 1 ||
		snapshot.TableCounts["knowledge_object_versions"] != 1 ||
		snapshot.TableCounts["knowledge_mutation_idempotency"] != 1 {
		t.Fatalf("same-key race authority = %#v", snapshot)
	}
	assertWriterProtoMatchesStored(
		t,
		committed.GetKnowledgeObject(),
		getWriterObject(t, harness, committed.GetKnowledgeObject().GetKnowledgeObjectId(), nil),
	)
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterConcurrentAlteredSameKeyCommitsExactlyOneSubmittedBody(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	requestA := writerConcurrentCreateRequest("altered-key-body-a", "altered-shared-create-key-01")
	requestB := writerConcurrentCreateRequest("altered-key-body-b", "altered-shared-create-key-01")

	const contenders = 16
	start := make(chan struct{})
	results := make(chan writerAlteredKeyRaceResult, contenders)
	var ready sync.WaitGroup
	ready.Add(contenders)
	for index := range contenders {
		go func() {
			body := "a"
			request := requestA
			if index%2 == 1 {
				body = "b"
				request = requestB
			}
			cloned := proto.Clone(request).(*opensplunk.CreateKnowledgeObjectRequest)
			ready.Done()
			<-start
			response, err := harness.writer.Create(harness.actorCtx, harness.writeScope, cloned)
			results <- writerAlteredKeyRaceResult{index: index, body: body, response: response, err: err}
		}()
	}
	ready.Wait()
	close(start)

	winningBody := ""
	winnerCount := 0
	conflictCount := 0
	var committed *opensplunk.CreateKnowledgeObjectResponse
	for range contenders {
		result := <-results
		switch {
		case result.err == nil:
			winnerCount++
			if winningBody == "" {
				winningBody = result.body
				committed = result.response
			}
			if result.body != winningBody || !proto.Equal(result.response, committed) {
				t.Fatalf("success from losing submitted body: contender=%d body=%s response=%v winner=%s/%v", result.index, result.body, result.response, winningBody, committed)
			}
		case errors.Is(result.err, knowledgecatalog.ErrIdempotencyConflict):
			conflictCount++
		default:
			t.Fatalf("altered-key contender %d error = %v, want success or ErrIdempotencyConflict", result.index, result.err)
		}
	}
	if winnerCount != contenders/2 || conflictCount != contenders/2 {
		t.Fatalf("altered-key results: winner body=%q successes=%d conflicts=%d, want %d/%d", winningBody, winnerCount, conflictCount, contenders/2, contenders/2)
	}
	wantName := "altered-key-body-" + winningBody
	if committed.GetKnowledgeObject().GetName() != wantName {
		t.Fatalf("altered-key committed name = %q, want %q", committed.GetKnowledgeObject().GetName(), wantName)
	}
	if harness.idCalls.Load() != 1 || harness.clockCalls.Load() != 1 {
		t.Fatalf("altered-key generator calls: IDs=%d clocks=%d, want 1/1", harness.idCalls.Load(), harness.clockCalls.Load())
	}
	snapshot := readWriterAuthoritySnapshot(t, harness.database)
	if snapshot.CatalogRevision != 1 || snapshot.IdentityCount != 1 || snapshot.VersionCount != 1 ||
		snapshot.IdempotencyCount != 1 || snapshot.AuditNextSequence != 2 || snapshot.AuditEventCount != 1 ||
		snapshot.TableCounts["audit_events"] != 1 {
		t.Fatalf("altered-key race authority = %#v", snapshot)
	}
	assertWriterCatalogIntegrity(t, harness.database)
}

func writerConcurrentCreateRequest(name, requestID string) *opensplunk.CreateKnowledgeObjectRequest {
	description := "concurrent definition for " + name
	return &opensplunk.CreateKnowledgeObjectRequest{
		Definition: writerAliasDefinition(
			writerTestApp,
			name,
			&description,
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			"host-"+name,
			"source_field",
			"destination_field",
		),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: requestID,
	}
}

type writerUpdateRaceResult struct {
	index    int
	response *opensplunk.UpdateKnowledgeObjectResponse
	err      error
}

type writerCreateRaceResult struct {
	index    int
	response *opensplunk.CreateKnowledgeObjectResponse
	err      error
}

type writerAlteredKeyRaceResult struct {
	index    int
	body     string
	response *opensplunk.CreateKnowledgeObjectResponse
	err      error
}
