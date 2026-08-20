package server

import (
	"net/http"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestKnowledgeHTTPPreflightsEveryMutationBeforeConfigurableWriter(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		request    proto.Message
		wantAction knowledgeattemptaudit.Action
	}{
		{
			name: "create client identity",
			path: knowledgeObjectsCreatePath,
			request: &opensplunk.CreateKnowledgeObjectRequest{
				Definition:      knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE),
				InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
				ClientRequestId: "short",
			},
			wantAction: knowledgeattemptaudit.ActionCreate,
		},
		{
			name: "update client identity",
			path: knowledgeObjectsUpdatePath,
			request: &opensplunk.UpdateKnowledgeObjectRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				Definition:        knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE),
				UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
				ClientRequestId:   "short",
			},
			wantAction: knowledgeattemptaudit.ActionUpdate,
		},
		{
			name: "state client identity",
			path: knowledgeObjectsSetStatePath,
			request: &opensplunk.SetKnowledgeObjectStateRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
				ClientRequestId:   "short",
			},
			// Semantic refinement occurs only after the entire request shape is
			// trusted, so a malformed state request retains the route fallback.
			wantAction: knowledgeattemptaudit.ActionUpdate,
		},
		{
			name: "delete client identity",
			path: knowledgeObjectsDeletePath,
			request: &opensplunk.DeleteKnowledgeObjectRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				ClientRequestId:   "short",
			},
			wantAction: knowledgeattemptaudit.ActionDelete,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			writer := &knowledgeHTTPWriter{}
			attempts := &knowledgeBoundaryAppender{}
			apps := knowledgeHTTPApps()
			_, httpHandler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				&knowledgeHTTPCatalog{},
				writer,
				apps,
				attempts,
			)
			response := knowledgeHTTPPost(t, httpHandler, test.path, test.request)
			journal := attempts.snapshot()
			if response.Code != http.StatusBadRequest ||
				writer.callCounts() != [4]int{} || apps.callCount() != 1 ||
				len(journal) != 1 ||
				journal[0].definition.Action != test.wantAction ||
				journal[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition {
				t.Fatalf(
					"status=%d body=%q writer=%v apps=%d journal=%+v",
					response.Code,
					response.Body.String(),
					writer.callCounts(),
					apps.callCount(),
					journal,
				)
			}
		})
	}
}

func TestKnowledgeHTTPActivePublicationGateRunsAfterMutationPreflight(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		request    proto.Message
		wantStatus int
		wantAction knowledgeattemptaudit.Action
		wantReason knowledgeattemptaudit.Reason
	}{
		{
			name: "valid active create is unavailable",
			path: knowledgeObjectsCreatePath,
			request: &opensplunk.CreateKnowledgeObjectRequest{
				Definition:      knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE),
				InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
				ClientRequestId: "active-create-request-0001",
			},
			wantStatus: http.StatusServiceUnavailable,
			wantAction: knowledgeattemptaudit.ActionCreate,
			wantReason: knowledgeattemptaudit.ReasonServiceUnavailable,
		},
		{
			name: "valid active state is unavailable",
			path: knowledgeObjectsSetStatePath,
			request: &opensplunk.SetKnowledgeObjectStateRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
				ClientRequestId:   "active-state-request-0001",
			},
			wantStatus: http.StatusServiceUnavailable,
			wantAction: knowledgeattemptaudit.ActionEnable,
			wantReason: knowledgeattemptaudit.ReasonServiceUnavailable,
		},
		{
			name: "malformed active create is invalid first",
			path: knowledgeObjectsCreatePath,
			request: &opensplunk.CreateKnowledgeObjectRequest{
				Definition:      knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE),
				InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
				ClientRequestId: "short",
			},
			wantStatus: http.StatusBadRequest,
			wantAction: knowledgeattemptaudit.ActionCreate,
			wantReason: knowledgeattemptaudit.ReasonInvalidDefinition,
		},
		{
			name: "malformed active state is invalid first",
			path: knowledgeObjectsSetStatePath,
			request: &opensplunk.SetKnowledgeObjectStateRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
				ClientRequestId:   "short",
			},
			wantStatus: http.StatusBadRequest,
			wantAction: knowledgeattemptaudit.ActionUpdate,
			wantReason: knowledgeattemptaudit.ReasonInvalidDefinition,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			writer := &knowledgeHTTPWriter{}
			attempts := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				&knowledgeHTTPCatalog{},
				writer,
				knowledgeHTTPApps(),
				attempts,
			)
			response := knowledgeHTTPPost(t, httpHandler, test.path, test.request)
			journal := attempts.snapshot()
			if response.Code != test.wantStatus || writer.callCounts() != [4]int{} ||
				len(journal) != 1 ||
				journal[0].definition.Action != test.wantAction ||
				journal[0].definition.Reason != test.wantReason {
				t.Fatalf(
					"status=%d body=%q writer=%v journal=%+v",
					response.Code,
					response.Body.String(),
					writer.callCounts(),
					journal,
				)
			}
			if test.wantStatus == http.StatusServiceUnavailable &&
				response.Body.String() != knowledgeManagementUnavailableBody {
				t.Fatalf("unavailable body=%q", response.Body.String())
			}
		})
	}
}
