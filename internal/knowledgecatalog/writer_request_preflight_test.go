package knowledgecatalog

import (
	"errors"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const mutationPreflightRequestID = "request-00000001"

func TestMutationRequestPreflightAcceptsWriterShapesWithoutMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request proto.Message
	}{
		{
			name: "draft create",
			request: &opensplunkv1.CreateKnowledgeObjectRequest{
				Definition:      &opensplunkv1.KnowledgeObjectDefinition{},
				InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
				ClientRequestId: mutationPreflightRequestID,
			},
		},
		{
			name: "active create remains a valid shape",
			request: &opensplunkv1.CreateKnowledgeObjectRequest{
				Definition:      &opensplunkv1.KnowledgeObjectDefinition{},
				InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
				ClientRequestId: mutationPreflightRequestID,
			},
		},
		{
			name: "update",
			request: &opensplunkv1.UpdateKnowledgeObjectRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				Definition:        &opensplunkv1.KnowledgeObjectDefinition{},
				UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
				ClientRequestId:   mutationPreflightRequestID,
			},
		},
		{
			name: "active state remains a valid shape",
			request: &opensplunkv1.SetKnowledgeObjectStateRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
				ClientRequestId:   mutationPreflightRequestID,
			},
		},
		{
			name: "disabled state",
			request: &opensplunkv1.SetKnowledgeObjectStateRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
				ClientRequestId:   mutationPreflightRequestID,
			},
		},
		{
			name: "delete",
			request: &opensplunkv1.DeleteKnowledgeObjectRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				ClientRequestId:   mutationPreflightRequestID,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := assertMutationRequestPreflightParity(t, test.request); err != nil {
				t.Fatalf("preflight: %v", err)
			}
		})
	}
}

func TestMutationRequestPreflightValidatesClientIdentityAndDetachedWireShape(
	t *testing.T,
) {
	t.Parallel()

	create := func(requestID string) *opensplunkv1.CreateKnowledgeObjectRequest {
		return &opensplunkv1.CreateKnowledgeObjectRequest{
			Definition:      &opensplunkv1.KnowledgeObjectDefinition{},
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: requestID,
		}
	}
	for _, requestID := range []string{
		"",
		"too-short",
		strings.Repeat("x", maximumClientRequestIDBytes+1),
		"request id with spaces",
		"request-id-with-\n-control",
	} {
		requestID := requestID
		t.Run("client request identity "+requestID, func(t *testing.T) {
			if err := assertMutationRequestPreflightParity(t, create(requestID)); !errors.Is(err, control.ErrInvalidArgument) ||
				!strings.Contains(err.Error(), "client request identity") {
				t.Fatalf("preflight error = %v, want client identity ErrInvalidArgument", err)
			}
		})
	}

	t.Run("recursive unknown field", func(t *testing.T) {
		request := create(mutationPreflightRequestID)
		request.GetDefinition().ProtoReflect().SetUnknown(
			protowire.AppendVarint(protowire.AppendTag(nil, 2047, protowire.VarintType), 1),
		)
		if err := assertMutationRequestPreflightParity(t, request); !errors.Is(err, control.ErrInvalidArgument) ||
			!strings.Contains(err.Error(), "unknown protobuf fields") {
			t.Fatalf("preflight error = %v, want unknown-field ErrInvalidArgument", err)
		}
	})

	t.Run("noncanonical UTF-8", func(t *testing.T) {
		request := create(mutationPreflightRequestID)
		request.Definition.Name = string([]byte{0xff})
		if err := assertMutationRequestPreflightParity(t, request); !errors.Is(err, control.ErrInvalidArgument) ||
			!strings.Contains(err.Error(), "canonically encoded") {
			t.Fatalf("preflight error = %v, want canonical-encoding ErrInvalidArgument", err)
		}
	})

	t.Run("selector entry limit", func(t *testing.T) {
		request := create(mutationPreflightRequestID)
		request.Definition.Selector = &opensplunkv1.KnowledgeSelector{
			IndexPatterns: make(
				[]*opensplunkv1.KnowledgeSelectorPattern,
				knowledge.MaximumSelectorPatternsPerDimension+1,
			),
		}
		if err := assertMutationRequestPreflightParity(t, request); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "selector exceeds its entry limit") {
			t.Fatalf("preflight error = %v, want selector capacity error", err)
		}
	})

	t.Run("extraction output entry limit", func(t *testing.T) {
		request := create(mutationPreflightRequestID)
		request.Definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{
						OutputFields: make(
							[]string,
							knowledgedefinition.MaximumFieldExtractionOutputs+1,
						),
					},
				},
			},
		}
		if err := assertMutationRequestPreflightParity(t, request); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "extraction outputs exceed their entry limit") {
			t.Fatalf("preflight error = %v, want extraction capacity error", err)
		}
	})

	t.Run("mutation byte limit", func(t *testing.T) {
		request := create(mutationPreflightRequestID)
		description := strings.Repeat("x", maximumMutationRequestBytes)
		request.Definition.Description = &description
		if err := assertMutationRequestPreflightParity(t, request); !errors.Is(err, control.ErrCapacityExceeded) ||
			!strings.Contains(err.Error(), "mutation exceeds its byte limit") {
			t.Fatalf("preflight error = %v, want mutation byte capacity error", err)
		}
	})
}

func TestMutationRequestPreflightPreservesWriterFieldPrecedence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		request  proto.Message
		contains string
	}{
		{
			name: "create definition before state and request identity",
			request: &opensplunkv1.CreateKnowledgeObjectRequest{
				InitialState: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_UNSPECIFIED,
			},
			contains: "create request and definition",
		},
		{
			name: "create state before request identity",
			request: &opensplunkv1.CreateKnowledgeObjectRequest{
				Definition: &opensplunkv1.KnowledgeObjectDefinition{},
			},
			contains: "initial state",
		},
		{
			name: "update identity before mask and request identity",
			request: &opensplunkv1.UpdateKnowledgeObjectRequest{
				Definition: &opensplunkv1.KnowledgeObjectDefinition{},
			},
			contains: "identity, version, and definition",
		},
		{
			name: "update mask before request identity",
			request: &opensplunkv1.UpdateKnowledgeObjectRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
				Definition:        &opensplunkv1.KnowledgeObjectDefinition{},
			},
			contains: "update mask",
		},
		{
			name:     "state identity before transition and request identity",
			request:  &opensplunkv1.SetKnowledgeObjectStateRequest{},
			contains: "state request identity and version",
		},
		{
			name: "state transition before request identity",
			request: &opensplunkv1.SetKnowledgeObjectStateRequest{
				KnowledgeObjectId: "ko-preflight",
				ExpectedVersion:   1,
			},
			contains: "state must be active or disabled",
		},
		{
			name:     "delete identity before request identity",
			request:  &opensplunkv1.DeleteKnowledgeObjectRequest{},
			contains: "delete request identity and version",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			err := assertMutationRequestPreflightParity(t, test.request)
			if !errors.Is(err, control.ErrInvalidArgument) ||
				!strings.Contains(err.Error(), test.contains) {
				t.Fatalf("preflight error = %v, want ErrInvalidArgument containing %q", err, test.contains)
			}
		})
	}
}

func assertMutationRequestPreflightParity(
	t *testing.T,
	request proto.Message,
) error {
	t.Helper()
	before := proto.Clone(request)
	validationErr := validateMutationPreflightRequest(request)
	if !proto.Equal(request, before) {
		t.Fatalf("public validation mutated request: got %v want %v", request, before)
	}
	_, preparationErr := prepareMutationPreflightRequest(request)
	if !proto.Equal(request, before) {
		t.Fatalf("request preparation mutated request: got %v want %v", request, before)
	}
	if (validationErr == nil) != (preparationErr == nil) ||
		validationErr != nil && validationErr.Error() != preparationErr.Error() {
		t.Fatalf(
			"public validation/preparation mismatch: validation=%v preparation=%v",
			validationErr,
			preparationErr,
		)
	}
	return validationErr
}

func validateMutationPreflightRequest(request proto.Message) error {
	switch request := request.(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		return ValidateCreateKnowledgeObjectRequest(request)
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		return ValidateUpdateKnowledgeObjectRequest(request)
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		return ValidateSetKnowledgeObjectStateRequest(request)
	case *opensplunkv1.DeleteKnowledgeObjectRequest:
		return ValidateDeleteKnowledgeObjectRequest(request)
	default:
		return errors.New("unsupported mutation preflight request type")
	}
}

func prepareMutationPreflightRequest(
	request proto.Message,
) (preparedMutation, error) {
	scope := normalizedWriteScope{
		tenantID:       testTenant,
		ownerID:        testOwner,
		writableAppIDs: []string{testApp},
	}
	actor := audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   testOwner,
		Role: audit.ActorRoleAdministrator,
	}
	switch request := request.(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		return prepareCreateMutation(scope, actor, request)
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		return prepareUpdateMutation(scope, actor, request)
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		return prepareSetStateMutation(scope, actor, request)
	case *opensplunkv1.DeleteKnowledgeObjectRequest:
		return prepareDeleteMutation(scope, actor, request)
	default:
		return preparedMutation{}, errors.New("unsupported mutation preflight request type")
	}
}
