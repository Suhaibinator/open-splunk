package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"gorm.io/gorm"
)

var errWriterAmbiguousCommit = errors.New("injected ambiguous commit result")

func TestWriterAmbiguousCommitReconcilesDurableOutcome(t *testing.T) {
	harness := newWriterFaultHarness(t)
	request := writerFaultCreateRequest("ambiguous-durable", "ambiguous-durable-request-01")
	var commitCalls int
	harness.writer.commit = func(tx *gorm.DB) error {
		commitCalls++
		if err := tx.Commit().Error; err != nil {
			return err
		}
		return errWriterAmbiguousCommit
	}

	response, err := harness.writer.Create(harness.actorContext, harness.scope, request)
	if err != nil {
		t.Fatalf("Create() after durable ambiguous commit: %v", err)
	}
	if commitCalls != 1 || response.GetKnowledgeObject().GetVersion() != 1 ||
		response.GetTenantCatalogRevision() != 1 || len(response.GetTenantCatalogStateToken()) != 32 {
		t.Fatalf("durable ambiguous response = %v, commit calls = %d", response, commitCalls)
	}
	committed := readWriterFaultSnapshot(t, harness.database)
	assertWriterFaultCommittedCounts(t, harness.database, 1, 1)

	harness.writer.commit = nil
	replayed, err := harness.writer.Create(harness.actorContext, harness.scope, request)
	if err != nil || replayed.GetKnowledgeObject().GetKnowledgeObjectId() != response.GetKnowledgeObject().GetKnowledgeObjectId() ||
		replayed.GetTenantCatalogRevision() != response.GetTenantCatalogRevision() {
		t.Fatalf("replay after reconciled commit = %v, %v; want %v", replayed, err, response)
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), committed)
	assertWriterFaultIntegrity(t, harness.database)
}

func TestWriterAmbiguousCommitWithoutReceiptDoesNotReexecute(t *testing.T) {
	harness := newWriterFaultHarness(t)
	request := writerFaultCreateRequest("ambiguous-absent", "ambiguous-absent-request-001")
	before := readWriterFaultSnapshot(t, harness.database)
	harness.writer.commit = func(tx *gorm.DB) error {
		if err := tx.Rollback().Error; err != nil {
			return err
		}
		return errWriterAmbiguousCommit
	}

	response, err := harness.writer.Create(harness.actorContext, harness.scope, request)
	if response != nil || !errors.Is(err, errWriterAmbiguousCommit) {
		t.Fatalf("Create() after absent ambiguous commit = %v, %v", response, err)
	}
	requireCatalogDisposition(t, err, ErrorDispositionIndeterminate)
	if authorized, found := AuthorizedContextFromError(err); !found ||
		authorized.AppID != writerFaultApp || authorized.Object != nil {
		t.Fatalf("ambiguous Create authorization = %#v, found %v; want app-only", authorized, found)
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)

	harness.writer.commit = nil
	retried, err := harness.writer.Create(harness.actorContext, harness.scope, request)
	if err != nil || retried.GetKnowledgeObject().GetVersion() != 1 {
		t.Fatalf("retry after definitive absent receipt = %v, %v", retried, err)
	}
	assertWriterFaultCommittedCounts(t, harness.database, 1, 1)
	assertWriterFaultIntegrity(t, harness.database)
}

func TestWriterAmbiguousCommitReturnsConcurrentDurableReplay(t *testing.T) {
	tests := []struct {
		name      string
		route     string
		requestID string
		invoke    func(*writerFaultHarness, *opensplunkv1.KnowledgeObject) (proto.Message, error)
		validate  func(*testing.T, *opensplunkv1.KnowledgeObject, proto.Message)
	}{
		{
			name:      "create",
			route:     mutationRouteCreate,
			requestID: "ambiguous-create-target-request-01",
			invoke: func(harness *writerFaultHarness, _ *opensplunkv1.KnowledgeObject) (proto.Message, error) {
				return harness.writer.Create(
					harness.actorContext,
					harness.scope,
					writerFaultCreateRequest("ambiguous-create-durable", "ambiguous-create-target-request-01"),
				)
			},
			validate: func(t *testing.T, _ *opensplunkv1.KnowledgeObject, response proto.Message) {
				t.Helper()
				typed, ok := response.(*opensplunkv1.CreateKnowledgeObjectResponse)
				if !ok || typed.GetKnowledgeObject().GetVersion() != 1 ||
					typed.GetKnowledgeObject().GetDefinition().GetName() != "ambiguous-create-durable" {
					t.Fatalf("durable ambiguous Create response = %v", response)
				}
			},
		},
		{
			name:      "update",
			route:     mutationRouteUpdate,
			requestID: "ambiguous-update-target-request-01",
			invoke: func(harness *writerFaultHarness, object *opensplunkv1.KnowledgeObject) (proto.Message, error) {
				definition := proto.Clone(object.GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
				description := "durable concurrent target update"
				definition.Description = &description
				return harness.writer.Update(harness.actorContext, harness.scope, &opensplunkv1.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: object.GetKnowledgeObjectId(),
					ExpectedVersion:   object.GetVersion(),
					Definition:        definition,
					UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
					ClientRequestId:   "ambiguous-update-target-request-01",
				})
			},
			validate: func(t *testing.T, object *opensplunkv1.KnowledgeObject, response proto.Message) {
				t.Helper()
				typed, ok := response.(*opensplunkv1.UpdateKnowledgeObjectResponse)
				if !ok || typed.GetKnowledgeObject().GetKnowledgeObjectId() != object.GetKnowledgeObjectId() ||
					typed.GetKnowledgeObject().GetVersion() != 2 ||
					typed.GetKnowledgeObject().GetDefinition().GetDescription() != "durable concurrent target update" {
					t.Fatalf("durable ambiguous Update response = %v", response)
				}
			},
		},
		{
			name:      "set_state_disabled",
			route:     mutationRouteSetState,
			requestID: "ambiguous-disable-target-request-01",
			invoke: func(harness *writerFaultHarness, object *opensplunkv1.KnowledgeObject) (proto.Message, error) {
				return harness.writer.SetState(harness.actorContext, harness.scope, &opensplunkv1.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: object.GetKnowledgeObjectId(),
					ExpectedVersion:   object.GetVersion(),
					State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId:   "ambiguous-disable-target-request-01",
				})
			},
			validate: func(t *testing.T, object *opensplunkv1.KnowledgeObject, response proto.Message) {
				t.Helper()
				typed, ok := response.(*opensplunkv1.SetKnowledgeObjectStateResponse)
				if !ok || typed.GetKnowledgeObject().GetKnowledgeObjectId() != object.GetKnowledgeObjectId() ||
					typed.GetKnowledgeObject().GetVersion() != 2 ||
					typed.GetKnowledgeObject().GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED {
					t.Fatalf("durable ambiguous SetState response = %v", response)
				}
			},
		},
		{
			name:      "delete",
			route:     mutationRouteDelete,
			requestID: "ambiguous-delete-target-request-01",
			invoke: func(harness *writerFaultHarness, object *opensplunkv1.KnowledgeObject) (proto.Message, error) {
				return harness.writer.Delete(harness.actorContext, harness.scope, &opensplunkv1.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: object.GetKnowledgeObjectId(),
					ExpectedVersion:   object.GetVersion(),
					ClientRequestId:   "ambiguous-delete-target-request-01",
				})
			},
			validate: func(t *testing.T, object *opensplunkv1.KnowledgeObject, response proto.Message) {
				t.Helper()
				typed, ok := response.(*opensplunkv1.DeleteKnowledgeObjectResponse)
				if !ok || typed.GetKnowledgeObjectId() != object.GetKnowledgeObjectId() || typed.GetDeletedVersion() != 2 {
					t.Fatalf("durable ambiguous Delete response = %v", response)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newWriterFaultHarness(t)
			target := createWriterAmbiguousCommitObject(
				t,
				harness,
				"ambiguous-"+test.name+"-target",
				"ambiguous-"+test.name+"-target-create-01",
			)
			companion := createWriterAmbiguousCommitObject(
				t,
				harness,
				"ambiguous-"+test.name+"-companion",
				"ambiguous-"+test.name+"-companion-create-01",
			)

			startConcurrent := make(chan struct{})
			concurrentDone := make(chan struct{})
			concurrentResult := make(chan writerAmbiguousConcurrentResult, 1)
			go func() {
				select {
				case <-startConcurrent:
				case <-t.Context().Done():
					return
				}
				definition := proto.Clone(companion.GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
				description := "interleaved tenant revision before exact retry"
				definition.Description = &description
				interleaved, err := harness.writer.Update(harness.actorContext, harness.scope, &opensplunkv1.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: companion.GetKnowledgeObjectId(),
					ExpectedVersion:   companion.GetVersion(),
					Definition:        definition,
					UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
					ClientRequestId:   "ambiguous-" + test.name + "-interleaved-01",
				})
				if err != nil {
					concurrentResult <- writerAmbiguousConcurrentResult{err: fmt.Errorf("interleaved Update: %w", err)}
					close(concurrentDone)
					return
				}
				replayed, err := test.invoke(harness, target)
				concurrentResult <- writerAmbiguousConcurrentResult{
					interleaved: interleaved,
					replayed:    replayed,
					err:         err,
				}
				close(concurrentDone)
			}()

			var commitCalls atomic.Int64
			var stagedRevision int64
			var stagedToken []byte
			harness.writer.commit = func(tx *gorm.DB) error {
				if commitCalls.Add(1) != 1 {
					return tx.Commit().Error
				}
				if err := tx.Raw(`SELECT committed_catalog_revision, committed_catalog_state_token
					FROM knowledge_mutation_idempotency
					WHERE tenant_id = ? AND actor_kind = 'browser'
					  AND actor_id = 'writer-fault-administrator'
					  AND route = ? AND client_request_id = ?`,
					writerFaultTenant, test.route, test.requestID,
				).Row().Scan(&stagedRevision, &stagedToken); err != nil {
					return fmt.Errorf("read staged response authority: %w", err)
				}
				if err := tx.Rollback().Error; err != nil {
					return err
				}
				close(startConcurrent)
				select {
				case <-concurrentDone:
				case <-time.After(5 * time.Second):
					return errors.New("timed out waiting for concurrent durable retry")
				}
				return errWriterAmbiguousCommit
			}

			first, err := test.invoke(harness, target)
			if err != nil {
				t.Fatalf("first %s call after ambiguous commit: %v", test.name, err)
			}
			concurrent := <-concurrentResult
			if concurrent.err != nil {
				t.Fatalf("concurrent %s retry: %v", test.name, concurrent.err)
			}
			if stagedRevision != 3 || len(stagedToken) != 32 {
				t.Fatalf("staged %s authority = revision %d token %x, want revision 3 and 32-byte token", test.name, stagedRevision, stagedToken)
			}
			if concurrent.interleaved.GetTenantCatalogRevision() != 3 {
				t.Fatalf("interleaved revision = %d, want 3", concurrent.interleaved.GetTenantCatalogRevision())
			}
			durableRevision, durableToken := writerAmbiguousResponseCatalog(t, concurrent.replayed)
			if durableRevision != 4 || len(durableToken) != 32 || bytes.Equal(durableToken, stagedToken) {
				t.Fatalf("durable %s authority = revision %d token %x, staged token %x", test.name, durableRevision, durableToken, stagedToken)
			}
			if !proto.Equal(first, concurrent.replayed) {
				t.Fatalf("first %s response retained rolled-back authority:\n first: %v\ndurable: %v", test.name, first, concurrent.replayed)
			}
			if commitCalls.Load() != 3 {
				t.Fatalf("%s commit calls = %d, want staged, interleaved, and durable retry", test.name, commitCalls.Load())
			}
			test.validate(t, target, first)

			harness.writer.commit = nil
			replayed, err := test.invoke(harness, target)
			if err != nil || !proto.Equal(replayed, concurrent.replayed) {
				t.Fatalf("post-reconciliation %s replay = (%v, %v), want %v", test.name, replayed, err, concurrent.replayed)
			}
			assertWriterFaultIntegrity(t, harness.database)
		})
	}
}

type writerAmbiguousConcurrentResult struct {
	interleaved *opensplunkv1.UpdateKnowledgeObjectResponse
	replayed    proto.Message
	err         error
}

func createWriterAmbiguousCommitObject(
	t *testing.T,
	harness *writerFaultHarness,
	name string,
	requestID string,
) *opensplunkv1.KnowledgeObject {
	t.Helper()
	request := writerFaultCreateRequest(name, requestID)
	response, err := harness.writer.Create(harness.actorContext, harness.scope, request)
	if err != nil {
		t.Fatalf("create %s baseline: %v", name, err)
	}
	return response.GetKnowledgeObject()
}

func writerAmbiguousResponseCatalog(t *testing.T, response proto.Message) (uint64, []byte) {
	t.Helper()
	switch typed := response.(type) {
	case *opensplunkv1.CreateKnowledgeObjectResponse:
		return typed.GetTenantCatalogRevision(), typed.GetTenantCatalogStateToken()
	case *opensplunkv1.UpdateKnowledgeObjectResponse:
		return typed.GetTenantCatalogRevision(), typed.GetTenantCatalogStateToken()
	case *opensplunkv1.SetKnowledgeObjectStateResponse:
		return typed.GetTenantCatalogRevision(), typed.GetTenantCatalogStateToken()
	case *opensplunkv1.DeleteKnowledgeObjectResponse:
		return typed.GetTenantCatalogRevision(), typed.GetTenantCatalogStateToken()
	default:
		t.Fatalf("unsupported ambiguous response type %T", response)
		return 0, nil
	}
}

func TestWriterRecoveredPanicRollsBackAndReleasesImmediateTransaction(t *testing.T) {
	harness := newWriterFaultHarness(t)
	request := writerFaultCreateRequest("panic-rollback", "panic-rollback-request-0001")
	before := readWriterFaultSnapshot(t, harness.database)
	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		if event.Boundary == writerHookRegistryPublished {
			panic("injected writer panic after registry publication")
		}
		return nil
	}
	panicked := false
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		_, _ = harness.writer.Create(harness.actorContext, harness.scope, request)
	}()
	if !panicked {
		t.Fatal("writer hook panic was not observed")
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)

	harness.writer.hook = nil
	retryContext, cancel := context.WithTimeout(harness.actorContext, 2*time.Second)
	defer cancel()
	retried, err := harness.writer.Create(retryContext, harness.scope, request)
	if err != nil || retried.GetKnowledgeObject().GetVersion() != 1 {
		t.Fatalf("writer did not progress immediately after recovered panic: %v, %v", retried, err)
	}
	assertWriterFaultCommittedCounts(t, harness.database, 1, 1)
	assertWriterFaultIntegrity(t, harness.database)
}
