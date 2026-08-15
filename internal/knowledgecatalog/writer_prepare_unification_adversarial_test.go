package knowledgecatalog

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const prepareUnificationRequestID = "prepare-unification-0001"

func prepareUnificationScope(ownerID string) normalizedWriteScope {
	return normalizedWriteScope{tenantID: testTenant, ownerID: ownerID, writableAppIDs: []string{testApp}}
}

func prepareUnificationActor() audit.Actor {
	return audit.Actor{Kind: audit.ActorKindBrowser, ID: testOwner, Role: audit.ActorRoleAdministrator}
}

// prepareUnificationRoute runs one request through the unified preparation and
// returns the route it took together with the clone the route retained.
func prepareUnificationRoute(
	scope normalizedWriteScope,
	request proto.Message,
) (preparedMutation, string, proto.Message, error) {
	actor := prepareUnificationActor()
	switch typed := request.(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		prepared, err := prepareCreateMutation(scope, actor, typed)
		return prepared, mutationRouteCreate, prepared.createRequest, err
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		prepared, err := prepareUpdateMutation(scope, actor, typed)
		return prepared, mutationRouteUpdate, prepared.updateRequest, err
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		prepared, err := prepareSetStateMutation(scope, actor, typed)
		return prepared, mutationRouteSetState, prepared.setStateRequest, err
	case *opensplunkv1.DeleteKnowledgeObjectRequest:
		prepared, err := prepareDeleteMutation(scope, actor, typed)
		return prepared, mutationRouteDelete, prepared.deleteRequest, err
	default:
		return preparedMutation{}, "", nil, errors.New("unsupported prepare unification request")
	}
}

type prepareUnificationCase struct {
	name     string
	build    func(requestID string) proto.Message
	scribble func(proto.Message)
}

func prepareUnificationCases() []prepareUnificationCase {
	description := "prepare unification"
	definition := func() *opensplunkv1.KnowledgeObjectDefinition {
		return aliasDefinition(testApp, "prepare-unify", SharingScopePrivate, &description, "host-a")
	}
	return []prepareUnificationCase{
		{
			name: "create",
			build: func(requestID string) proto.Message {
				return &opensplunkv1.CreateKnowledgeObjectRequest{
					Definition:      definition(),
					InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
					ClientRequestId: requestID,
				}
			},
			scribble: func(request proto.Message) {
				typed := request.(*opensplunkv1.CreateKnowledgeObjectRequest)
				typed.Definition.Selector.HostPatterns[0].Value = "scribbled"
				*typed = opensplunkv1.CreateKnowledgeObjectRequest{ClientRequestId: "scribbled-create-00000001"}
			},
		},
		{
			name: "update",
			build: func(requestID string) proto.Message {
				return &opensplunkv1.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: "ko-prepare-unify", ExpectedVersion: 7, Definition: definition(),
					UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"description", "name"}},
					ClientRequestId: requestID,
				}
			},
			scribble: func(request proto.Message) {
				typed := request.(*opensplunkv1.UpdateKnowledgeObjectRequest)
				typed.UpdateMask.Paths[0] = "scribbled"
				*typed = opensplunkv1.UpdateKnowledgeObjectRequest{KnowledgeObjectId: "ko-scribbled"}
			},
		},
		{
			name: "set state",
			build: func(requestID string) proto.Message {
				return &opensplunkv1.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: "ko-prepare-unify", ExpectedVersion: 7,
					State:           opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId: requestID,
				}
			},
			scribble: func(request proto.Message) {
				*request.(*opensplunkv1.SetKnowledgeObjectStateRequest) = opensplunkv1.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: "ko-scribbled",
				}
			},
		},
		{
			name: "delete",
			build: func(requestID string) proto.Message {
				return &opensplunkv1.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: "ko-prepare-unify", ExpectedVersion: 7, ClientRequestId: requestID,
				}
			},
			scribble: func(request proto.Message) {
				*request.(*opensplunkv1.DeleteKnowledgeObjectRequest) = opensplunkv1.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: "ko-scribbled",
				}
			},
		},
	}
}

// TestPrepareMutationDetachesEveryRouteFromHostileCallers drives all four
// mutation kinds through the unified generic preparation: the retained clone,
// the canonical bytes, the digest, and the update paths must all survive a
// caller that scribbles over its request the instant preparation returns.
func TestPrepareMutationDetachesEveryRouteFromHostileCallers(t *testing.T) {
	t.Parallel()
	for _, test := range prepareUnificationCases() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := test.build(prepareUnificationRequestID)
			want := proto.Clone(request)
			prepared, route, retained, err := prepareUnificationRoute(prepareUnificationScope(testOwner), request)
			if err != nil {
				t.Fatalf("prepare %s: %v", test.name, err)
			}
			if !proto.Equal(request, want) {
				t.Fatalf("prepare %s mutated the caller request: %v", route, request)
			}
			if retained == nil || retained == request {
				t.Fatalf("prepare %s retained %v, want a detached clone", route, retained)
			}
			if prepared.clientRequestID != prepareUnificationRequestID ||
				retained.(clientRequestMessage).GetClientRequestId() != "" {
				t.Fatalf("prepare %s key handling: prepared %q, clone %q",
					route, prepared.clientRequestID, retained.(clientRequestMessage).GetClientRequestId())
			}
			requestBytes := bytes.Clone(prepared.requestBytes)
			digest := prepared.requestDigest
			paths := slices.Clone(prepared.updatePaths)

			test.scribble(request)

			if !bytes.Equal(prepared.requestBytes, requestBytes) || prepared.requestDigest != digest ||
				!slices.Equal(prepared.updatePaths, paths) {
				t.Fatalf("prepare %s aliases caller storage: paths %v", route, prepared.updatePaths)
			}
			cleared := proto.Clone(want)
			cleared.ProtoReflect().Clear(cleared.ProtoReflect().Descriptor().Fields().ByName("client_request_id"))
			if !proto.Equal(retained, cleared) {
				t.Fatalf("prepare %s retained clone = %v, want %v", route, retained, cleared)
			}
			if digest != digestMutationRequest(route, testOwner, requestBytes) {
				t.Fatalf("prepare %s digest is not the canonical route digest", route)
			}
		})
	}
}

// TestPrepareMutationDigestIgnoresKeyAndBindsOwner pins the exact-retry
// contract the unified preparation must keep: the idempotency key stays outside
// the canonical payload while the owner stays inside the digest domain.
func TestPrepareMutationDigestIgnoresKeyAndBindsOwner(t *testing.T) {
	t.Parallel()
	for _, test := range prepareUnificationCases() {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first, route, _, err := prepareUnificationRoute(
				prepareUnificationScope(testOwner), test.build(prepareUnificationRequestID))
			rekeyed, _, _, rekeyedErr := prepareUnificationRoute(
				prepareUnificationScope(testOwner), test.build("prepare-unification-0002"))
			foreign, _, _, foreignErr := prepareUnificationRoute(
				prepareUnificationScope("owner-foreign"), test.build(prepareUnificationRequestID))
			if err != nil || rekeyedErr != nil || foreignErr != nil {
				t.Fatalf("prepare %s: %v/%v/%v", test.name, err, rekeyedErr, foreignErr)
			}
			if !bytes.Equal(first.requestBytes, rekeyed.requestBytes) || first.requestDigest != rekeyed.requestDigest {
				t.Fatalf("prepare %s digest depends on the idempotency key", route)
			}
			if rekeyed.clientRequestID != "prepare-unification-0002" {
				t.Fatalf("prepare %s dropped the rekeyed key: %q", route, rekeyed.clientRequestID)
			}
			if foreign.requestDigest == first.requestDigest {
				t.Fatalf("prepare %s digest does not bind the owner", route)
			}
		})
	}
}

// TestPrepareMutationRequestRejectsRouteTypeConfusion attacks the shared route
// switch directly with a mismatched payload type and unroutable routes.
func TestPrepareMutationRequestRejectsRouteTypeConfusion(t *testing.T) {
	t.Parallel()
	payload := &opensplunkv1.DeleteKnowledgeObjectRequest{KnowledgeObjectId: "ko-confused", ExpectedVersion: 1}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil {
		t.Fatalf("marshal confusion payload: %v", err)
	}
	validated := validatedMutationRequest{
		clientRequestID: prepareUnificationRequestID, requestBytes: encoded, request: payload,
	}
	for _, route := range []string{
		mutationRouteCreate, mutationRouteUpdate, mutationRouteSetState,
		mutationRouteQuarantine, "objects.unknown", "",
	} {
		prepared, err := prepareMutationRequest(
			prepareUnificationScope(testOwner), prepareUnificationActor(), route, validated, nil)
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("prepareMutationRequest(%q, delete payload) = (%+v, %v), want ErrInvalidArgument", route, prepared, err)
		}
		if prepared.createRequest != nil || prepared.updateRequest != nil ||
			prepared.setStateRequest != nil || prepared.deleteRequest != nil {
			t.Fatalf("prepareMutationRequest(%q) leaked a retained request on rejection", route)
		}
	}
	prepared, err := prepareMutationRequest(
		prepareUnificationScope(testOwner), prepareUnificationActor(), mutationRouteDelete, validated, nil)
	if err != nil || prepared.deleteRequest != payload {
		t.Fatalf("prepareMutationRequest(delete) = (%+v, %v), want the delete payload retained", prepared, err)
	}
}
