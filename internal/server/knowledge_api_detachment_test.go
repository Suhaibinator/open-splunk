package server

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func knowledgeHTTPDetachedResponseObject(
	t *testing.T,
	objectID string,
	version uint64,
	state opensplunk.KnowledgeObjectState,
	definition *opensplunk.KnowledgeObjectDefinition,
) *opensplunk.KnowledgeObject {
	t.Helper()
	message := knowledgeHTTPProtoObject(t)
	message.KnowledgeObjectId = objectID
	message.Version = version
	message.State = state
	message.Definition = proto.Clone(definition).(*opensplunk.KnowledgeObjectDefinition)
	message.AppId = definition.GetAppId()
	message.Name = definition.GetName()
	message.SharingScope = definition.GetSharingScope()
	message.UpdatedAt = timestamppb.New(
		message.GetCreatedAt().AsTime().Add(time.Duration(version-1) * time.Microsecond),
	)
	message.DisabledAt = nil
	if state == opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED {
		message.DisabledAt = proto.Clone(message.GetUpdatedAt()).(*timestamppb.Timestamp)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message.GetDefinition())
	if err != nil {
		t.Fatalf("marshal detached response definition: %v", err)
	}
	digest := sha256.Sum256(encoded)
	message.DefinitionSha256 = digest[:]
	return message
}

func knowledgeHTTPDetachedMutationToken() []byte {
	return make([]byte, sha256.Size)
}

func knowledgeHTTPRecomputeDefinitionDigest(
	t *testing.T,
	object *opensplunk.KnowledgeObject,
) {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(
		object.GetDefinition(),
	)
	if err != nil {
		t.Fatalf("marshal response definition: %v", err)
	}
	digest := sha256.Sum256(encoded)
	object.DefinitionSha256 = digest[:]
}

func TestCloneKnowledgeMessageBoundedRejectsOversizedBeforeDetachment(
	t *testing.T,
) {
	t.Parallel()

	small := &opensplunk.DeleteKnowledgeObjectResponse{
		KnowledgeObjectId: "ko-bounded-clone",
		DeletedVersion:    1,
	}
	cloned, ok := cloneKnowledgeMessageBounded(
		small,
		maximumKnowledgeObjectResponseBytes,
	)
	if !ok || cloned == nil || cloned == small || !proto.Equal(cloned, small) {
		t.Fatalf("small clone=%+v ok=%t", cloned, ok)
	}
	small.KnowledgeObjectId = "ko-mutated-after-clone"
	if cloned.GetKnowledgeObjectId() != "ko-bounded-clone" {
		t.Fatalf("detached object ID=%q", cloned.GetKnowledgeObjectId())
	}

	oversized := &opensplunk.CreateKnowledgeObjectResponse{
		KnowledgeObject: &opensplunk.KnowledgeObject{
			DefinitionSha256: make(
				[]byte,
				maximumKnowledgeObjectResponseBytes+1,
			),
		},
	}
	if proto.Size(oversized) <= maximumKnowledgeObjectResponseBytes {
		t.Fatalf("oversized fixture size=%d", proto.Size(oversized))
	}
	if cloned, ok := cloneKnowledgeMessageBounded(
		oversized,
		maximumKnowledgeObjectResponseBytes,
	); ok || cloned != nil {
		t.Fatalf("oversized clone=%+v ok=%t", cloned, ok)
	}
}

func TestUnavailableActiveReplayCapabilityIsSealedToCatalogWriter(t *testing.T) {
	t.Parallel()

	if replaysUnavailableActiveMutations(&knowledgeHTTPWriter{}) {
		t.Fatal("external Writer fake acquired the sealed ACTIVE replay capability")
	}
	if replaysUnavailableActiveMutations(&knowledgecatalog.Writer{}) {
		t.Fatal("zero catalog Writer acquired the sealed ACTIVE replay capability")
	}
	if !replaysUnavailableActiveMutations(
		newReadyKnowledgeWriter(t),
	) {
		t.Fatal("catalog Writer lost its sealed ACTIVE replay capability")
	}
	var typedNil *knowledgecatalog.Writer
	if replaysUnavailableActiveMutations(typedNil) {
		t.Fatal("typed-nil catalog Writer acquired ACTIVE replay capability")
	}
}

func TestKnowledgeHTTPGetRequestVersionIsDetachedFromCatalogMutation(t *testing.T) {
	t.Parallel()

	returned := knowledgeHTTPObject()
	returned.Version = 2
	returned.UpdatedAt = returned.UpdatedAt.Add(time.Microsecond)
	catalog := &knowledgeHTTPCatalog{getFn: func(
		_ context.Context,
		_ knowledgecatalog.ReadScope,
		_ string,
		version *uint64,
	) (knowledgecatalog.Object, error) {
		if version == nil || *version != 1 {
			t.Fatalf("catalog version=%v, want 1", version)
		}
		*version = returned.Version
		return returned, nil
	}}
	appender := &knowledgeBoundaryAppender{}
	_, handler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		&knowledgeHTTPWriter{},
		knowledgeHTTPApps(),
		appender,
	)
	response := knowledgeHTTPPost(
		t,
		handler,
		knowledgeObjectsGetPath,
		&opensplunk.GetKnowledgeObjectRequest{
			KnowledgeObjectId: returned.KnowledgeObjectID,
			Version:           new(uint64(1)),
		},
	)
	attempts := appender.snapshot()
	if response.Code != http.StatusServiceUnavailable ||
		response.Body.String() != knowledgeManagementUnavailableBody ||
		len(attempts) != 1 ||
		attempts[0].definition.Action != knowledgeattemptaudit.ActionGet ||
		attempts[0].definition.Reason != knowledgeattemptaudit.ReasonServiceUnavailable ||
		attempts[0].definition.AuthorizedContext != nil {
		t.Fatalf(
			"status=%d body=%q attempts=%+v",
			response.Code,
			response.Body.String(),
			attempts,
		)
	}
}

func TestKnowledgeHTTPMutationRequestAuthorityIsDetachedFromWriter(t *testing.T) {
	t.Parallel()

	const (
		mutatedCreateName = "writer-mutated-create-name"
		mutatedUpdateName = "writer-mutated-update-name"
		mutatedStateID    = "ko-writer-mutated-state"
		mutatedDeleteID   = "ko-writer-mutated-delete"
	)
	tests := []struct {
		name      string
		path      string
		request   func() proto.Message
		writer    func(*testing.T) *knowledgeHTTPWriter
		wantCalls [4]int
	}{
		{
			name: "create definition",
			path: knowledgeObjectsCreatePath,
			request: func() proto.Message {
				return &opensplunk.CreateKnowledgeObjectRequest{
					Definition: knowledgeHTTPDefinition(
						opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
					),
					InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
					ClientRequestId: "detached-create-request-0001",
				}
			},
			writer: func(t *testing.T) *knowledgeHTTPWriter {
				return &knowledgeHTTPWriter{createFn: func(
					_ context.Context,
					_ knowledgecatalog.WriteScope,
					request *opensplunk.CreateKnowledgeObjectRequest,
				) (*opensplunk.CreateKnowledgeObjectResponse, error) {
					request.Definition.Name = mutatedCreateName
					return &opensplunk.CreateKnowledgeObjectResponse{
						KnowledgeObject: knowledgeHTTPDetachedResponseObject(
							t,
							"ko-writer-created-object",
							1,
							request.GetInitialState(),
							request.GetDefinition(),
						),
						TenantCatalogRevision:   1,
						TenantCatalogStateToken: knowledgeHTTPDetachedMutationToken(),
					}, nil
				}}
			},
			wantCalls: [4]int{1, 0, 0, 0},
		},
		{
			name: "update selected definition",
			path: knowledgeObjectsUpdatePath,
			request: func() proto.Message {
				definition := knowledgeHTTPDefinition(
					opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
				)
				definition.Name = "client-selected-update-name"
				return &opensplunk.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: "ko-http-object-1",
					ExpectedVersion:   1,
					Definition:        definition,
					UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
					ClientRequestId:   "detached-update-request-0001",
				}
			},
			writer: func(t *testing.T) *knowledgeHTTPWriter {
				return &knowledgeHTTPWriter{updateFn: func(
					_ context.Context,
					_ knowledgecatalog.WriteScope,
					request *opensplunk.UpdateKnowledgeObjectRequest,
				) (*opensplunk.UpdateKnowledgeObjectResponse, error) {
					request.Definition.Name = mutatedUpdateName
					return &opensplunk.UpdateKnowledgeObjectResponse{
						KnowledgeObject: knowledgeHTTPDetachedResponseObject(
							t,
							request.GetKnowledgeObjectId(),
							request.GetExpectedVersion()+1,
							opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
							request.GetDefinition(),
						),
						TenantCatalogRevision:   2,
						TenantCatalogStateToken: knowledgeHTTPDetachedMutationToken(),
					}, nil
				}}
			},
			wantCalls: [4]int{0, 1, 0, 0},
		},
		{
			name: "set state identity",
			path: knowledgeObjectsSetStatePath,
			request: func() proto.Message {
				return &opensplunk.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: "ko-http-object-1",
					ExpectedVersion:   1,
					State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId:   "detached-state-request-0001",
				}
			},
			writer: func(t *testing.T) *knowledgeHTTPWriter {
				return &knowledgeHTTPWriter{stateFn: func(
					_ context.Context,
					_ knowledgecatalog.WriteScope,
					request *opensplunk.SetKnowledgeObjectStateRequest,
				) (*opensplunk.SetKnowledgeObjectStateResponse, error) {
					request.KnowledgeObjectId = mutatedStateID
					return &opensplunk.SetKnowledgeObjectStateResponse{
						KnowledgeObject: knowledgeHTTPDetachedResponseObject(
							t,
							request.GetKnowledgeObjectId(),
							request.GetExpectedVersion()+1,
							request.GetState(),
							knowledgeHTTPDefinition(
								opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
							),
						),
						TenantCatalogRevision:   2,
						TenantCatalogStateToken: knowledgeHTTPDetachedMutationToken(),
					}, nil
				}}
			},
			wantCalls: [4]int{0, 0, 1, 0},
		},
		{
			name: "delete identity and version",
			path: knowledgeObjectsDeletePath,
			request: func() proto.Message {
				return &opensplunk.DeleteKnowledgeObjectRequest{
					KnowledgeObjectId: "ko-http-object-1",
					ExpectedVersion:   1,
					ClientRequestId:   "detached-delete-request-0001",
				}
			},
			writer: func(*testing.T) *knowledgeHTTPWriter {
				return &knowledgeHTTPWriter{deleteFn: func(
					_ context.Context,
					_ knowledgecatalog.WriteScope,
					request *opensplunk.DeleteKnowledgeObjectRequest,
				) (*opensplunk.DeleteKnowledgeObjectResponse, error) {
					request.KnowledgeObjectId = mutatedDeleteID
					request.ExpectedVersion = 2
					return &opensplunk.DeleteKnowledgeObjectResponse{
						KnowledgeObjectId:       request.GetKnowledgeObjectId(),
						DeletedVersion:          request.GetExpectedVersion() + 1,
						TenantCatalogRevision:   request.GetExpectedVersion() + 1,
						TenantCatalogStateToken: knowledgeHTTPDetachedMutationToken(),
					}, nil
				}}
			},
			wantCalls: [4]int{0, 0, 0, 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			writer := test.writer(t)
			appender := &knowledgeBoundaryAppender{}
			_, handler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				&knowledgeHTTPCatalog{},
				writer,
				knowledgeHTTPApps(),
				appender,
			)
			response := knowledgeHTTPPost(t, handler, test.path, test.request())
			if response.Code != http.StatusServiceUnavailable ||
				response.Body.String() != knowledgeManagementUnavailableBody ||
				writer.callCounts() != test.wantCalls ||
				len(appender.snapshot()) != 0 {
				t.Fatalf(
					"status=%d body=%q writer=%v attempts=%+v",
					response.Code,
					response.Body.String(),
					writer.callCounts(),
					appender.snapshot(),
				)
			}
		})
	}
}

func TestKnowledgeHTTPSuccessResponseValidationRejectsUnavailableActiveAndStaleMarkers(
	t *testing.T,
) {
	t.Parallel()

	scopes := knowledgeScopes{
		write: knowledgecatalog.WriteScope{
			TenantID:       knowledgeBoundaryTenantID,
			OwnerID:        knowledgeBoundaryOwnerID,
			WritableAppIDs: []string{knowledgeHTTPAppID},
		},
		apps: []string{knowledgeHTTPAppID},
	}
	token := knowledgeHTTPDetachedMutationToken()

	createDefinition := knowledgeHTTPDefinition(
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	)
	draftCreateRequest := &opensplunk.CreateKnowledgeObjectRequest{
		Definition:      createDefinition,
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "response-gate-create-0001",
	}
	draftCreateResponse := &opensplunk.CreateKnowledgeObjectResponse{
		KnowledgeObject: knowledgeHTTPDetachedResponseObject(
			t,
			"ko-response-gate-create",
			1,
			draftCreateRequest.GetInitialState(),
			createDefinition,
		),
		TenantCatalogRevision:   1,
		TenantCatalogStateToken: token,
	}
	if !validKnowledgeCreateResponseWithPolicy(
		draftCreateResponse,
		draftCreateRequest,
		scopes,
		false,
		false,
	) {
		t.Fatal("valid draft Create response fixture was rejected")
	}
	missingCreatedAt := proto.Clone(draftCreateResponse).(*opensplunk.CreateKnowledgeObjectResponse)
	missingCreatedAt.KnowledgeObject.CreatedAt = nil
	if !validKnowledgeProtoDefinitionAuthority(missingCreatedAt.GetKnowledgeObject()) ||
		validKnowledgeCreateResponseAfterDefinitionAuthorityForPolicy(
			missingCreatedAt,
			draftCreateRequest,
			scopes,
			false,
		) {
		t.Fatal("prevalidated definition bypassed post-clone scalar validation")
	}
	activeCreateRequest := proto.Clone(draftCreateRequest).(*opensplunk.CreateKnowledgeObjectRequest)
	activeCreateRequest.InitialState = opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE
	activeCreateResponse := proto.Clone(draftCreateResponse).(*opensplunk.CreateKnowledgeObjectResponse)
	activeCreateResponse.KnowledgeObject.State = opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE
	if !validKnowledgeObjectScalarLifecycleEnvelope(activeCreateResponse.GetKnowledgeObject()) ||
		!validKnowledgeProtoDefinitionAuthority(activeCreateResponse.GetKnowledgeObject()) ||
		validKnowledgeCreateResponseWithPolicy(
			activeCreateResponse,
			activeCreateRequest,
			scopes,
			false,
			false,
		) {
		t.Fatal("ACTIVE Create success response did not fail closed")
	}
	if !validKnowledgeCreateResponseWithPolicy(
		activeCreateResponse,
		activeCreateRequest,
		scopes,
		true,
		false,
	) {
		t.Fatal("certified ACTIVE Create replay response was rejected")
	}

	updateDefinition := knowledgeHTTPDefinition(
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	)
	updateDefinition.Name = "response-gate-updated-name"
	updateRequest := &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: "ko-http-object-1",
		ExpectedVersion:   1,
		Definition:        updateDefinition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		ClientRequestId:   "response-gate-update-0001",
	}
	draftUpdateResponse := &opensplunk.UpdateKnowledgeObjectResponse{
		KnowledgeObject: knowledgeHTTPDetachedResponseObject(
			t,
			updateRequest.GetKnowledgeObjectId(),
			2,
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			updateDefinition,
		),
		TenantCatalogRevision:   2,
		TenantCatalogStateToken: token,
	}
	if !validKnowledgeUpdateResponseWithPolicy(
		draftUpdateResponse,
		updateRequest,
		scopes,
		false,
		false,
	) {
		t.Fatal("valid draft Update response fixture was rejected")
	}
	wrongUpdateIdentity := proto.Clone(draftUpdateResponse).(*opensplunk.UpdateKnowledgeObjectResponse)
	wrongUpdateIdentity.KnowledgeObject.KnowledgeObjectId = "ko-response-gate-wrong"
	if !validKnowledgeProtoDefinitionAuthority(wrongUpdateIdentity.GetKnowledgeObject()) ||
		validKnowledgeUpdateResponseAfterDefinitionAuthorityForPolicy(
			wrongUpdateIdentity,
			updateRequest,
			scopes,
			false,
		) {
		t.Fatal("prevalidated definition bypassed post-clone request binding")
	}
	activeUpdateResponse := proto.Clone(draftUpdateResponse).(*opensplunk.UpdateKnowledgeObjectResponse)
	activeUpdateResponse.KnowledgeObject.State = opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE
	if !validKnowledgeObjectScalarLifecycleEnvelope(activeUpdateResponse.GetKnowledgeObject()) ||
		!validKnowledgeProtoDefinitionAuthority(activeUpdateResponse.GetKnowledgeObject()) ||
		validKnowledgeUpdateResponseWithPolicy(
			activeUpdateResponse,
			updateRequest,
			scopes,
			false,
			false,
		) {
		t.Fatal("ACTIVE Update success response did not fail closed")
	}
	if !validKnowledgeUpdateResponseWithPolicy(
		activeUpdateResponse,
		updateRequest,
		scopes,
		true,
		false,
	) {
		t.Fatal("certified ACTIVE Update replay response was rejected")
	}

	disabledStateRequest := &opensplunk.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: "ko-http-object-1",
		ExpectedVersion:   1,
		State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		ClientRequestId:   "response-gate-state-0001",
	}
	disabledStateResponse := &opensplunk.SetKnowledgeObjectStateResponse{
		KnowledgeObject: knowledgeHTTPDetachedResponseObject(
			t,
			disabledStateRequest.GetKnowledgeObjectId(),
			2,
			disabledStateRequest.GetState(),
			knowledgeHTTPDefinition(
				opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			),
		),
		TenantCatalogRevision:   2,
		TenantCatalogStateToken: token,
	}
	if !validKnowledgeSetStateResponseWithPolicy(
		disabledStateResponse,
		disabledStateRequest,
		scopes,
		false,
		false,
	) {
		t.Fatal("valid disabled SetState response fixture was rejected")
	}
	activeStateRequest := proto.Clone(disabledStateRequest).(*opensplunk.SetKnowledgeObjectStateRequest)
	activeStateRequest.State = opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE
	activeStateResponse := proto.Clone(disabledStateResponse).(*opensplunk.SetKnowledgeObjectStateResponse)
	activeStateResponse.KnowledgeObject.State = opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE
	activeStateResponse.KnowledgeObject.DisabledAt = nil
	if !validKnowledgeObjectScalarLifecycleEnvelope(activeStateResponse.GetKnowledgeObject()) ||
		!validKnowledgeProtoDefinitionAuthority(activeStateResponse.GetKnowledgeObject()) ||
		validKnowledgeSetStateResponseWithPolicy(
			activeStateResponse,
			activeStateRequest,
			scopes,
			false,
			false,
		) {
		t.Fatal("ACTIVE SetState success response did not fail closed")
	}
	if !validKnowledgeSetStateResponseWithPolicy(
		activeStateResponse,
		activeStateRequest,
		scopes,
		true,
		false,
	) {
		t.Fatal("certified ACTIVE SetState replay response was rejected")
	}
	staleDisabledResponse := proto.Clone(disabledStateResponse).(*opensplunk.SetKnowledgeObjectStateResponse)
	staleDisabledResponse.KnowledgeObject.DisabledAt = timestamppb.New(
		staleDisabledResponse.GetKnowledgeObject().GetCreatedAt().AsTime(),
	)
	if !validKnowledgeObjectScalarLifecycleEnvelope(staleDisabledResponse.GetKnowledgeObject()) ||
		!validKnowledgeProtoDefinitionAuthority(staleDisabledResponse.GetKnowledgeObject()) ||
		validKnowledgeSetStateResponseWithPolicy(
			staleDisabledResponse,
			disabledStateRequest,
			scopes,
			false,
			false,
		) {
		t.Fatal("stale disabled_at SetState success response did not fail closed")
	}
	if !validKnowledgeProtoDefinitionAuthority(staleDisabledResponse.GetKnowledgeObject()) ||
		validKnowledgeSetStateResponseAfterDefinitionAuthorityForPolicy(
			staleDisabledResponse,
			disabledStateRequest,
			scopes,
			false,
		) {
		t.Fatal("prevalidated definition bypassed post-clone lifecycle binding")
	}
}

func TestKnowledgeHTTPSetStateRejectsImpossibleDefinitionAuthorityWithoutFalseRejection(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*opensplunk.KnowledgeObject)
	}{
		{
			name: "definition exceeds catalog ceiling",
			mutate: func(object *opensplunk.KnowledgeObject) {
				description := strings.Repeat(
					"x",
					knowledgedefinition.MaximumCanonicalBytes+1,
				)
				object.Definition.Description = &description
			},
		},
		{
			name: "recognized definition is noncanonical",
			mutate: func(object *opensplunk.KnowledgeObject) {
				empty := ""
				object.Definition.Description = &empty
			},
		},
		{
			name: "selector node count exceeds clone-safe ceiling",
			mutate: func(object *opensplunk.KnowledgeObject) {
				object.Definition.Selector = &opensplunk.KnowledgeSelector{
					IndexPatterns: make(
						[]*opensplunk.KnowledgeSelectorPattern,
						knowledge.MaximumSelectorPatternsPerDimension+1,
					),
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			writer := &knowledgeHTTPWriter{stateFn: func(
				context.Context,
				knowledgecatalog.WriteScope,
				*opensplunk.SetKnowledgeObjectStateRequest,
			) (*opensplunk.SetKnowledgeObjectStateResponse, error) {
				object := knowledgeHTTPDetachedResponseObject(
					t,
					"ko-http-object-1",
					2,
					opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					knowledgeHTTPDefinition(
						opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
					),
				)
				test.mutate(object)
				knowledgeHTTPRecomputeDefinitionDigest(t, object)
				response := &opensplunk.SetKnowledgeObjectStateResponse{
					KnowledgeObject:         object,
					TenantCatalogRevision:   2,
					TenantCatalogStateToken: knowledgeHTTPDetachedMutationToken(),
				}
				if proto.Size(response) > maximumKnowledgeObjectResponseBytes {
					t.Fatalf("fixture exceeds transport bound: %d", proto.Size(response))
				}
				return response, nil
			}}
			appender := &knowledgeBoundaryAppender{}
			_, handler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				&knowledgeHTTPCatalog{},
				writer,
				knowledgeHTTPApps(),
				appender,
			)
			response := knowledgeHTTPPost(
				t,
				handler,
				knowledgeObjectsSetStatePath,
				&opensplunk.SetKnowledgeObjectStateRequest{
					KnowledgeObjectId: "ko-http-object-1",
					ExpectedVersion:   1,
					State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					ClientRequestId:   "definition-authority-state-0001",
				},
			)
			if response.Code != http.StatusServiceUnavailable ||
				response.Body.String() != knowledgeManagementUnavailableBody ||
				writer.callCounts() != [4]int{0, 0, 1, 0} ||
				len(appender.snapshot()) != 0 {
				t.Fatalf(
					"status=%d body=%q calls=%v attempts=%+v",
					response.Code,
					response.Body.String(),
					writer.callCounts(),
					appender.snapshot(),
				)
			}
		})
	}
}
