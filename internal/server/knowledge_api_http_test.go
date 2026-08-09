package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	knowledgeHTTPAppID      = "app_000000000900000000001A"
	knowledgeHTTPOtherAppID = "app_000000000900000000002A"
)

type knowledgeHTTPAppCatalog struct {
	mu     sync.Mutex
	result KnowledgeAppCatalogResult
	err    error
	calls  int
}

func (catalog *knowledgeHTTPAppCatalog) ListKnowledgeApps(
	_ context.Context,
	_ string,
	_ uint32,
) (KnowledgeAppCatalogResult, error) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	catalog.calls++
	return KnowledgeAppCatalogResult{
		AppIDs:   slices.Clone(catalog.result.AppIDs),
		Complete: catalog.result.Complete,
	}, catalog.err
}

func (catalog *knowledgeHTTPAppCatalog) callCount() int {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	return catalog.calls
}

type knowledgeHTTPCatalog struct {
	mu sync.Mutex

	getFn func(
		context.Context,
		knowledgecatalog.ReadScope,
		string,
		*uint64,
	) (knowledgecatalog.Object, error)
	listFn func(
		context.Context,
		knowledgecatalog.ReadScope,
		knowledgecatalog.ListRequest,
	) (knowledgecatalog.ListPage, error)
	getCalls  int
	listCalls int
}

func (catalog *knowledgeHTTPCatalog) Get(
	ctx context.Context,
	scope knowledgecatalog.ReadScope,
	objectID string,
	version *uint64,
) (knowledgecatalog.Object, error) {
	catalog.mu.Lock()
	catalog.getCalls++
	fn := catalog.getFn
	catalog.mu.Unlock()
	if fn == nil {
		return knowledgecatalog.Object{}, errors.New("unexpected knowledge Get")
	}
	return fn(ctx, scope, objectID, version)
}

func (catalog *knowledgeHTTPCatalog) List(
	ctx context.Context,
	scope knowledgecatalog.ReadScope,
	request knowledgecatalog.ListRequest,
) (knowledgecatalog.ListPage, error) {
	catalog.mu.Lock()
	catalog.listCalls++
	fn := catalog.listFn
	catalog.mu.Unlock()
	if fn == nil {
		return knowledgecatalog.ListPage{}, errors.New("unexpected knowledge List")
	}
	return fn(ctx, scope, request)
}

func (catalog *knowledgeHTTPCatalog) calls() (int, int) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	return catalog.getCalls, catalog.listCalls
}

type knowledgeHTTPWriter struct {
	mu sync.Mutex

	createFn func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.CreateKnowledgeObjectRequest) (*opensplunkv1.CreateKnowledgeObjectResponse, error)
	updateFn func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.UpdateKnowledgeObjectRequest) (*opensplunkv1.UpdateKnowledgeObjectResponse, error)
	stateFn  func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.SetKnowledgeObjectStateRequest) (*opensplunkv1.SetKnowledgeObjectStateResponse, error)
	deleteFn func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.DeleteKnowledgeObjectRequest) (*opensplunkv1.DeleteKnowledgeObjectResponse, error)
	calls    [4]int
}

type knowledgeHTTPBlockingAuthenticator struct{}

func (*knowledgeHTTPBlockingAuthenticator) Authenticate(
	ctx context.Context,
	_ []byte,
) (auth.BrowserPrincipal, error) {
	<-ctx.Done()
	return auth.BrowserPrincipal{}, ctx.Err()
}

func (writer *knowledgeHTTPWriter) Create(ctx context.Context, scope knowledgecatalog.WriteScope, request *opensplunkv1.CreateKnowledgeObjectRequest) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
	writer.mu.Lock()
	writer.calls[0]++
	fn := writer.createFn
	writer.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected knowledge Create")
	}
	return fn(ctx, scope, request)
}

func (writer *knowledgeHTTPWriter) Update(ctx context.Context, scope knowledgecatalog.WriteScope, request *opensplunkv1.UpdateKnowledgeObjectRequest) (*opensplunkv1.UpdateKnowledgeObjectResponse, error) {
	writer.mu.Lock()
	writer.calls[1]++
	fn := writer.updateFn
	writer.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected knowledge Update")
	}
	return fn(ctx, scope, request)
}

func (writer *knowledgeHTTPWriter) SetState(ctx context.Context, scope knowledgecatalog.WriteScope, request *opensplunkv1.SetKnowledgeObjectStateRequest) (*opensplunkv1.SetKnowledgeObjectStateResponse, error) {
	writer.mu.Lock()
	writer.calls[2]++
	fn := writer.stateFn
	writer.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected knowledge SetState")
	}
	return fn(ctx, scope, request)
}

func (writer *knowledgeHTTPWriter) Delete(ctx context.Context, scope knowledgecatalog.WriteScope, request *opensplunkv1.DeleteKnowledgeObjectRequest) (*opensplunkv1.DeleteKnowledgeObjectResponse, error) {
	writer.mu.Lock()
	writer.calls[3]++
	fn := writer.deleteFn
	writer.mu.Unlock()
	if fn == nil {
		return nil, errors.New("unexpected knowledge Delete")
	}
	return fn(ctx, scope, request)
}

func (writer *knowledgeHTTPWriter) callCounts() [4]int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.calls
}

func newKnowledgeHTTPHandler(
	t *testing.T,
	role auth.BrowserRole,
	catalog KnowledgeCatalog,
	writer KnowledgeWriter,
	apps KnowledgeAppCatalog,
	attempts KnowledgeAttemptJournal,
) (*apiHandler, http.Handler) {
	t.Helper()
	handler := &apiHandler{
		browserAuthenticator: &knowledgeBoundaryAuthenticator{
			principal: knowledgeBoundaryPrincipal(t, role),
		},
		knowledgeCatalog:     catalog,
		knowledgeWriter:      writer,
		knowledgeApps:        apps,
		knowledgeAttempts:    attempts,
		tenantID:             knowledgeBoundaryTenantID,
		ownerID:              knowledgeBoundaryOwnerID,
		now:                  func() time.Time { return knowledgeBoundaryNow },
		requestGate:          make(chan struct{}, 8),
		serializationGate:    make(chan struct{}, 4),
		knowledgeAttemptGate: make(chan struct{}, 8),
		browserAllowedHosts: map[string]struct{}{
			"example.com": {},
		},
		administratorRoutes: map[string]struct{}{},
		routeTimeout:        2 * time.Second,
	}
	return handler, newKnowledgeHTTPRouter(handler)
}

// newKnowledgeHTTPRouter deliberately lives in a _test.go file: it exercises
// the management handlers, codecs, and middleware in isolation from the full
// production API router.
func newKnowledgeHTTPRouter(handler *apiHandler) http.Handler {
	noAuth := router.NoAuth
	routes := unwrapProtobufRoutes(handler.knowledgeManagementRoutes(noAuth))
	inner := router.NewRouter[string, struct{}](router.RouterConfig{
		ServiceName:       "open-splunk-knowledge-http-test",
		GlobalTimeout:     0,
		GlobalMaxBodySize: maximumKnowledgeMutationRequestBytes,
		SubRouters: []router.SubRouterConfig{{
			PathPrefix: "/api/v1",
			AuthLevel:  &noAuth,
			Middlewares: []sroutercommon.Middleware{
				disableAPICaching,
				requireProtobufContentType,
				handler.boundRequests,
				withSynchronousDeadline(handler.routeTimeout),
			},
			Routes: routes,
		}},
	}, nil, nil)
	trusted := handler.protectBrowserAPIRoutes(
		handler.protectKnowledgeManagementRoutes(inner),
	)
	return exactAPIRoutes(trusted, postAPIRoutes(
		knowledgeObjectsCreatePath,
		knowledgeObjectsGetPath,
		knowledgeObjectsListPath,
		knowledgeObjectsUpdatePath,
		knowledgeObjectsSetStatePath,
		knowledgeObjectsDeletePath,
	))
}

func knowledgeHTTPPost(
	t *testing.T,
	handler http.Handler,
	path string,
	message proto.Message,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	request.Host = "example.com"
	request.Header.Set("Origin", "http://example.com")
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func knowledgeHTTPDefinition(scope opensplunkv1.SharingScope) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        knowledgeHTTPAppID,
		Name:         "knowledge-http-object",
		SharingScope: scope,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField:       "src",
				DestinationField:  "dst",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			},
		},
	}
}

func knowledgeHTTPObject() knowledgecatalog.Object {
	created := time.Date(2026, time.August, 7, 12, 0, 0, 123456000, time.UTC)
	definition := knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(encoded)
	return knowledgecatalog.Object{
		KnowledgeObjectID: "ko-http-object-1",
		TenantID:          knowledgeBoundaryTenantID,
		AppID:             knowledgeHTTPAppID,
		OwnerID:           knowledgeBoundaryOwnerID,
		ObjectType:        knowledgecatalog.ObjectTypeFieldAlias,
		Name:              "knowledge-http-object",
		Version:           1,
		SharingScope:      knowledgecatalog.SharingScopePrivate,
		State:             knowledgecatalog.StateDraft,
		Definition:        definition,
		DefinitionSHA256:  digest[:],
		CreatedAt:         created,
		UpdatedAt:         created,
	}
}

func knowledgeHTTPProtoObject(t *testing.T) *opensplunkv1.KnowledgeObject {
	t.Helper()
	result, err := knowledgecatalog.ObjectToProto(knowledgeHTTPObject())
	if err != nil {
		t.Fatalf("ObjectToProto fixture: %v", err)
	}
	return result
}

func knowledgeHTTPApps() *knowledgeHTTPAppCatalog {
	return &knowledgeHTTPAppCatalog{result: KnowledgeAppCatalogResult{
		AppIDs:   []string{knowledgeHTTPAppID},
		Complete: true,
	}}
}

func addKnowledgeHTTPUnknown(message proto.Message) {
	message.ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 2_047, protowire.VarintType),
		1,
	))
}

func knowledgeHTTPScopeChangeMask() *fieldmaskpb.FieldMask {
	return &fieldmaskpb.FieldMask{Paths: []string{"sharing_scope"}}
}

func TestKnowledgeHTTPTestRouterServesAllSixManagementHandlers(
	t *testing.T,
) {
	t.Parallel()

	object := knowledgeHTTPObject()
	objectMessage := knowledgeHTTPProtoObject(t)
	updatedObject := object
	updatedObject.Definition = proto.Clone(object.Definition).(*opensplunkv1.KnowledgeObjectDefinition)
	updatedObject.Name = "knowledge-http-object-updated"
	updatedObject.Definition.Name = updatedObject.Name
	updatedObject.Version = 2
	updatedObject.UpdatedAt = updatedObject.UpdatedAt.Add(time.Microsecond)
	updatedDefinitionBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(updatedObject.Definition)
	if err != nil {
		t.Fatalf("marshal updated definition fixture: %v", err)
	}
	updatedDigest := sha256.Sum256(updatedDefinitionBytes)
	updatedObject.DefinitionSHA256 = updatedDigest[:]
	updatedObjectMessage, err := knowledgecatalog.ObjectToProto(updatedObject)
	if err != nil {
		t.Fatalf("updated ObjectToProto fixture: %v", err)
	}
	disabledObject := updatedObject
	disabledObject.State = knowledgecatalog.StateDisabled
	disabledAt := disabledObject.UpdatedAt
	disabledObject.DisabledAt = &disabledAt
	disabledObjectMessage, err := knowledgecatalog.ObjectToProto(disabledObject)
	if err != nil {
		t.Fatalf("disabled ObjectToProto fixture: %v", err)
	}
	token := bytes.Repeat([]byte{0x71}, 32)
	total := uint64(1)
	catalog := &knowledgeHTTPCatalog{
		getFn: func(
			_ context.Context,
			scope knowledgecatalog.ReadScope,
			objectID string,
			_ *uint64,
		) (knowledgecatalog.Object, error) {
			if scope.TenantID != knowledgeBoundaryTenantID ||
				scope.OwnerID != knowledgeBoundaryOwnerID ||
				!slices.Equal(scope.ReadableAppIDs, []string{knowledgeHTTPAppID}) ||
				objectID != object.KnowledgeObjectID {
				t.Fatalf("get scope=%+v objectID=%q", scope, objectID)
			}
			return object, nil
		},
		listFn: func(
			_ context.Context,
			scope knowledgecatalog.ReadScope,
			request knowledgecatalog.ListRequest,
		) (knowledgecatalog.ListPage, error) {
			if scope.TenantID != knowledgeBoundaryTenantID ||
				scope.OwnerID != knowledgeBoundaryOwnerID ||
				request.PageSize != 1 || !request.IncludeTotal {
				t.Fatalf("list scope=%+v request=%+v", scope, request)
			}
			return knowledgecatalog.ListPage{
				Objects:         []knowledgecatalog.Object{object},
				TotalSize:       &total,
				TotalSizeExact:  true,
				CatalogRevision: 7,
			}, nil
		},
	}
	validateWrite := func(scope knowledgecatalog.WriteScope) {
		if scope.TenantID != knowledgeBoundaryTenantID ||
			scope.OwnerID != knowledgeBoundaryOwnerID ||
			!slices.Equal(scope.WritableAppIDs, []string{knowledgeHTTPAppID}) {
			t.Fatalf("write scope=%+v", scope)
		}
	}
	writer := &knowledgeHTTPWriter{
		createFn: func(_ context.Context, scope knowledgecatalog.WriteScope, _ *opensplunkv1.CreateKnowledgeObjectRequest) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
			validateWrite(scope)
			return &opensplunkv1.CreateKnowledgeObjectResponse{KnowledgeObject: objectMessage, TenantCatalogRevision: 8, TenantCatalogStateToken: token}, nil
		},
		updateFn: func(_ context.Context, scope knowledgecatalog.WriteScope, _ *opensplunkv1.UpdateKnowledgeObjectRequest) (*opensplunkv1.UpdateKnowledgeObjectResponse, error) {
			validateWrite(scope)
			return &opensplunkv1.UpdateKnowledgeObjectResponse{KnowledgeObject: updatedObjectMessage, TenantCatalogRevision: 9, TenantCatalogStateToken: token}, nil
		},
		stateFn: func(_ context.Context, scope knowledgecatalog.WriteScope, _ *opensplunkv1.SetKnowledgeObjectStateRequest) (*opensplunkv1.SetKnowledgeObjectStateResponse, error) {
			validateWrite(scope)
			return &opensplunkv1.SetKnowledgeObjectStateResponse{KnowledgeObject: disabledObjectMessage, TenantCatalogRevision: 10, TenantCatalogStateToken: token}, nil
		},
		deleteFn: func(_ context.Context, scope knowledgecatalog.WriteScope, _ *opensplunkv1.DeleteKnowledgeObjectRequest) (*opensplunkv1.DeleteKnowledgeObjectResponse, error) {
			validateWrite(scope)
			return &opensplunkv1.DeleteKnowledgeObjectResponse{KnowledgeObjectId: object.KnowledgeObjectID, DeletedVersion: 2, TenantCatalogRevision: 11, TenantCatalogStateToken: token}, nil
		},
	}
	appender := &knowledgeBoundaryAppender{}
	apps := knowledgeHTTPApps()
	_, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		writer,
		apps,
		appender,
	)

	tests := []struct {
		name     string
		path     string
		request  proto.Message
		response proto.Message
	}{
		{
			name: "create", path: knowledgeObjectsCreatePath,
			request:  &opensplunkv1.CreateKnowledgeObjectRequest{Definition: knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE), InitialState: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT, ClientRequestId: "request-create-0001"},
			response: &opensplunkv1.CreateKnowledgeObjectResponse{},
		},
		{
			name: "get", path: knowledgeObjectsGetPath,
			request:  &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: object.KnowledgeObjectID},
			response: &opensplunkv1.GetKnowledgeObjectResponse{},
		},
		{
			name: "list", path: knowledgeObjectsListPath,
			request:  &opensplunkv1.ListKnowledgeObjectsRequest{Page: &opensplunkv1.PageRequest{PageSize: uint32Pointer(1), IncludeTotalSize: true}},
			response: &opensplunkv1.ListKnowledgeObjectsResponse{},
		},
		{
			name: "update", path: knowledgeObjectsUpdatePath,
			request: &opensplunkv1.UpdateKnowledgeObjectRequest{
				KnowledgeObjectId: object.KnowledgeObjectID,
				ExpectedVersion:   1,
				Definition:        proto.Clone(updatedObject.Definition).(*opensplunkv1.KnowledgeObjectDefinition),
				UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
				ClientRequestId:   "request-update-0001",
			},
			response: &opensplunkv1.UpdateKnowledgeObjectResponse{},
		},
		{
			name: "set state", path: knowledgeObjectsSetStatePath,
			request:  &opensplunkv1.SetKnowledgeObjectStateRequest{KnowledgeObjectId: object.KnowledgeObjectID, ExpectedVersion: 1, State: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, ClientRequestId: "request-state-0001"},
			response: &opensplunkv1.SetKnowledgeObjectStateResponse{},
		},
		{
			name: "delete", path: knowledgeObjectsDeletePath,
			request:  &opensplunkv1.DeleteKnowledgeObjectRequest{KnowledgeObjectId: object.KnowledgeObjectID, ExpectedVersion: 1, ClientRequestId: "request-delete-0001"},
			response: &opensplunkv1.DeleteKnowledgeObjectResponse{},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			response := knowledgeHTTPPost(t, httpHandler, test.path, test.request)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
			}
			if contentType := response.Header().Get("Content-Type"); contentType != "application/x-protobuf" {
				t.Fatalf("content type=%q", contentType)
			}
			if err := proto.Unmarshal(response.Body.Bytes(), test.response); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
		})
	}
	if getCalls, listCalls := catalog.calls(); getCalls != 1 || listCalls != 1 {
		t.Fatalf("catalog calls get=%d list=%d", getCalls, listCalls)
	}
	if calls := writer.callCounts(); calls != [4]int{1, 1, 1, 1} {
		t.Fatalf("writer calls=%v", calls)
	}
	if calls := apps.callCount(); calls != 6 {
		t.Fatalf("app catalog calls=%d", calls)
	}
	if calls := appender.snapshot(); len(calls) != 0 {
		t.Fatalf("successful attempts journaled=%+v", calls)
	}
}

func TestKnowledgeHTTPUserIsRejectedBeforeMalformedBodyDecode(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	catalog := &knowledgeHTTPCatalog{}
	writer := &knowledgeHTTPWriter{}
	apps := knowledgeHTTPApps()
	_, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleUser,
		catalog,
		writer,
		apps,
		appender,
	)
	body := newKnowledgeBoundaryObservedBody("not protobuf", nil)
	request := knowledgeBoundaryRequest(
		context.Background(),
		http.MethodPost,
		knowledgeObjectsCreatePath,
		body,
	)
	request.Host = "example.com"
	request.Header.Set("Origin", "http://example.com")
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	response := httptest.NewRecorder()
	httpHandler.ServeHTTP(response, request)

	calls := appender.snapshot()
	if response.Code != http.StatusForbidden ||
		response.Body.String() != knowledgeAdministratorRequiredBody ||
		body.reads() != 0 || apps.callCount() != 0 ||
		writer.callCounts() != [4]int{} || len(calls) != 1 ||
		calls[0].definition.Action != knowledgeattemptaudit.ActionCreate ||
		calls[0].definition.Reason != knowledgeattemptaudit.ReasonNotAdministrator {
		t.Fatalf(
			"status=%d body=%q reads=%d apps=%d writer=%v attempts=%+v",
			response.Code,
			response.Body.String(),
			body.reads(),
			apps.callCount(),
			writer.callCounts(),
			calls,
		)
	}
}

func TestKnowledgeHTTPDefinitionUnknownFieldsAreRejectedButOuterUnknownsAreDiscarded(
	t *testing.T,
) {
	t.Parallel()

	t.Run("nested definition unknown is rejected", func(t *testing.T) {
		appender := &knowledgeBoundaryAppender{}
		writer := &knowledgeHTTPWriter{}
		apps := knowledgeHTTPApps()
		_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, writer, apps, appender)
		request := &opensplunkv1.CreateKnowledgeObjectRequest{
			Definition:      knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE),
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: "request-unknown-definition",
		}
		addKnowledgeHTTPUnknown(request.GetDefinition().GetFieldAlias())
		response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsCreatePath, request)
		calls := appender.snapshot()
		if response.Code != http.StatusBadRequest || writer.callCounts() != [4]int{} ||
			apps.callCount() != 0 || len(calls) != 1 ||
			calls[0].definition.Action != knowledgeattemptaudit.ActionCreate ||
			calls[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition {
			t.Fatalf("status=%d body=%q writer=%v apps=%d attempts=%+v", response.Code, response.Body.String(), writer.callCounts(), apps.callCount(), calls)
		}
	})

	t.Run("amplifying repeated definition shape is rejected before traversal", func(t *testing.T) {
		appender := &knowledgeBoundaryAppender{}
		writer := &knowledgeHTTPWriter{}
		apps := knowledgeHTTPApps()
		_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, writer, apps, appender)
		const hostilePatternCount = 8 << 10
		patterns := make([]*opensplunkv1.KnowledgeSelectorPattern, hostilePatternCount)
		for index := range patterns {
			patterns[index] = &opensplunkv1.KnowledgeSelectorPattern{}
		}
		request := &opensplunkv1.CreateKnowledgeObjectRequest{
			Definition: knowledgeHTTPDefinition(
				opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			),
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: "request-amplifying-selector",
		}
		request.Definition.Selector = &opensplunkv1.KnowledgeSelector{IndexPatterns: patterns}
		if len(patterns) <= knowledge.MaximumSelectorPatternsPerDimension ||
			int64(proto.Size(request)) >= maximumKnowledgeMutationRequestBytes {
			t.Fatalf("fixture patterns=%d bytes=%d", len(patterns), proto.Size(request))
		}

		response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsCreatePath, request)
		calls := appender.snapshot()
		if response.Code != http.StatusBadRequest || writer.callCounts() != [4]int{} ||
			apps.callCount() != 0 || len(calls) != 1 ||
			calls[0].definition.Action != knowledgeattemptaudit.ActionCreate ||
			calls[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition {
			t.Fatalf("status=%d body=%q writer=%v apps=%d attempts=%+v", response.Code, response.Body.String(), writer.callCounts(), apps.callCount(), calls)
		}
	})

	t.Run("outer and non-definition nested unknowns are discarded", func(t *testing.T) {
		appender := &knowledgeBoundaryAppender{}
		apps := knowledgeHTTPApps()
		writer := &knowledgeHTTPWriter{
			createFn: func(_ context.Context, _ knowledgecatalog.WriteScope, request *opensplunkv1.CreateKnowledgeObjectRequest) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
				if len(request.ProtoReflect().GetUnknown()) != 0 {
					t.Fatal("outer request unknown fields reached Writer")
				}
				return &opensplunkv1.CreateKnowledgeObjectResponse{KnowledgeObject: knowledgeHTTPProtoObject(t), TenantCatalogRevision: 1, TenantCatalogStateToken: bytes.Repeat([]byte{1}, 32)}, nil
			},
		}
		_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, writer, apps, appender)
		request := &opensplunkv1.CreateKnowledgeObjectRequest{
			Definition:      knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE),
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: "request-outer-unknown",
		}
		addKnowledgeHTTPUnknown(request)
		response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsCreatePath, request)
		if response.Code != http.StatusOK || writer.callCounts() != [4]int{1, 0, 0, 0} || len(appender.snapshot()) != 0 {
			t.Fatalf("status=%d body=%q writer=%v attempts=%+v", response.Code, response.Body.String(), writer.callCounts(), appender.snapshot())
		}
	})
}

func TestKnowledgeHTTPDefinitiveErrorMappingAndConflictBodyUniformity(
	t *testing.T,
) {
	t.Parallel()

	authorizedApp := knowledgecatalog.AuthorizedContext{AppID: knowledgeHTTPAppID}
	object := knowledgeHTTPObject()
	authorizedObject := knowledgecatalog.AuthorizedContext{
		AppID: knowledgeHTTPAppID,
		Object: &knowledgecatalog.AuthorizedObject{
			KnowledgeObjectID: object.KnowledgeObjectID,
			ObjectType:        object.ObjectType,
			Version:           object.Version,
			SharingScope:      object.SharingScope,
		},
	}
	tests := []struct {
		name            string
		operationErr    error
		authorized      *knowledgecatalog.AuthorizedContext
		wantStatus      int
		wantReason      knowledgeattemptaudit.Reason
		wantContext     bool
		uniformConflict bool
	}{
		{name: "not found redacts context", operationErr: control.ErrNotFound, authorized: &authorizedObject, wantStatus: http.StatusNotFound, wantReason: knowledgeattemptaudit.ReasonNotFoundOrForbidden},
		{name: "redacted replay", operationErr: knowledgecatalog.ErrIdempotentOutcomeRedacted, authorized: &authorizedObject, wantStatus: http.StatusNotFound, wantReason: knowledgeattemptaudit.ReasonNotFoundOrForbidden},
		{name: "idempotency conflict", operationErr: knowledgecatalog.ErrIdempotencyConflict, authorized: &authorizedApp, wantStatus: http.StatusConflict, wantReason: knowledgeattemptaudit.ReasonIdempotencyConflict, wantContext: true, uniformConflict: true},
		{name: "version conflict", operationErr: control.ErrVersionConflict, authorized: &authorizedObject, wantStatus: http.StatusConflict, wantReason: knowledgeattemptaudit.ReasonVersionConflict, wantContext: true, uniformConflict: true},
		{name: "invalid argument", operationErr: control.ErrInvalidArgument, authorized: &authorizedApp, wantStatus: http.StatusBadRequest, wantReason: knowledgeattemptaudit.ReasonInvalidDefinition, wantContext: true},
		{name: "dependency conflict", operationErr: control.ErrDependencyConflict, authorized: &authorizedObject, wantStatus: http.StatusConflict, wantReason: knowledgeattemptaudit.ReasonForbiddenDependency, wantContext: true, uniformConflict: true},
		{name: "capacity", operationErr: control.ErrCapacityExceeded, authorized: &authorizedApp, wantStatus: http.StatusTooManyRequests, wantReason: knowledgeattemptaudit.ReasonResourceLimit, wantContext: true},
		{name: "page invalidated redacts context", operationErr: control.ErrPageInvalidated, authorized: &authorizedObject, wantStatus: http.StatusConflict, wantReason: knowledgeattemptaudit.ReasonServiceUnavailable, uniformConflict: true},
		{name: "canceled", operationErr: context.Canceled, authorized: &authorizedApp, wantStatus: http.StatusRequestTimeout, wantReason: knowledgeattemptaudit.ReasonServiceUnavailable, wantContext: true},
		{name: "unknown", operationErr: errors.New("private dependency detail"), authorized: &authorizedApp, wantStatus: http.StatusServiceUnavailable, wantReason: knowledgeattemptaudit.ReasonServiceUnavailable, wantContext: true},
	}
	const uniformConflictBody = "{\"error\":{\"message\":\"knowledge request conflicts with current state\"}}\n"
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.operationErr
			if test.authorized != nil {
				err = knowledgecatalog.WithAuthorizedContext(err, *test.authorized)
			}
			err = knowledgecatalog.WithErrorDisposition(
				err,
				knowledgecatalog.ErrorDispositionDefinitiveRejection,
			)
			if disposition, found := knowledgecatalog.ErrorDispositionFromError(err); !found || disposition != knowledgecatalog.ErrorDispositionDefinitiveRejection {
				t.Fatalf("fixture disposition=%v found=%v err=%v", disposition, found, err)
			}
			if test.authorized != nil {
				if got, found := knowledgecatalog.AuthorizedContextFromError(err); !found || got.AppID != test.authorized.AppID ||
					(got.Object != nil) != (test.authorized.Object != nil) {
					t.Fatalf("fixture context=%+v found=%v err=%v", got, found, err)
				}
			}
			writer := &knowledgeHTTPWriter{
				createFn: func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.CreateKnowledgeObjectRequest) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
					return nil, err
				},
				updateFn: func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.UpdateKnowledgeObjectRequest) (*opensplunkv1.UpdateKnowledgeObjectResponse, error) {
					return nil, err
				},
			}
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, writer, knowledgeHTTPApps(), appender)
			path := knowledgeObjectsCreatePath
			request := proto.Message(&opensplunkv1.CreateKnowledgeObjectRequest{
				Definition:      knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE),
				InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
				ClientRequestId: "request-error-mapping",
			})
			wantAction := knowledgeattemptaudit.ActionCreate
			if test.wantContext && test.authorized != nil && test.authorized.Object != nil {
				path = knowledgeObjectsUpdatePath
				request = &opensplunkv1.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: object.KnowledgeObjectID,
					ExpectedVersion:   object.Version,
					Definition:        knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE),
					UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
					ClientRequestId:   "request-error-mapping",
				}
				wantAction = knowledgeattemptaudit.ActionUpdate
			}
			response := knowledgeHTTPPost(t, httpHandler, path, request)
			calls := appender.snapshot()
			if response.Code != test.wantStatus || len(calls) != 1 ||
				calls[0].definition.Action != wantAction ||
				calls[0].definition.Reason != test.wantReason ||
				(calls[0].definition.AuthorizedContext != nil) != test.wantContext {
				t.Fatalf("status=%d body=%q attempts=%+v", response.Code, response.Body.String(), calls)
			}
			if test.wantContext && calls[0].definition.AuthorizedContext.AppID != knowledgeHTTPAppID {
				t.Fatalf("authorized context=%+v", calls[0].definition.AuthorizedContext)
			}
			if test.uniformConflict {
				if response.Body.String() != uniformConflictBody {
					t.Fatalf("conflict body=%q want=%q", response.Body.String(), uniformConflictBody)
				}
			}
		})
	}
}

func TestKnowledgeHTTPRejectsAuthorizedErrorContextUnboundFromRequest(
	t *testing.T,
) {
	t.Parallel()

	object := knowledgeHTTPObject()
	foreignObject := knowledgecatalog.AuthorizedContext{
		AppID: knowledgeHTTPAppID,
		Object: &knowledgecatalog.AuthorizedObject{
			KnowledgeObjectID: "ko-http-foreign-object",
			ObjectType:        object.ObjectType,
			Version:           object.Version,
			SharingScope:      object.SharingScope,
		},
	}
	foreignApp := knowledgecatalog.AuthorizedContext{AppID: knowledgeHTTPOtherAppID}
	apps := &knowledgeHTTPAppCatalog{result: KnowledgeAppCatalogResult{
		AppIDs:   []string{knowledgeHTTPAppID, knowledgeHTTPOtherAppID},
		Complete: true,
	}}
	definitive := func(cause error, authorized *knowledgecatalog.AuthorizedContext) error {
		if authorized != nil {
			cause = knowledgecatalog.WithAuthorizedContext(cause, *authorized)
		}
		return knowledgecatalog.WithErrorDisposition(
			cause,
			knowledgecatalog.ErrorDispositionDefinitiveRejection,
		)
	}
	validUpdate := func() *opensplunkv1.UpdateKnowledgeObjectRequest {
		definition := knowledgeHTTPDefinition(
			opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		)
		definition.Name = "knowledge-http-object-updated"
		return &opensplunkv1.UpdateKnowledgeObjectRequest{
			KnowledgeObjectId: object.KnowledgeObjectID,
			ExpectedVersion:   object.Version,
			Definition:        definition,
			UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
			ClientRequestId:   "context-binding-update-0001",
		}
	}

	tests := []struct {
		name    string
		path    string
		request proto.Message
		catalog *knowledgeHTTPCatalog
		writer  *knowledgeHTTPWriter
	}{
		{
			name: "create app differs from submitted app",
			path: knowledgeObjectsCreatePath,
			request: &opensplunkv1.CreateKnowledgeObjectRequest{
				Definition: knowledgeHTTPDefinition(
					opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				),
				InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
				ClientRequestId: "context-binding-create-0001",
			},
			catalog: &knowledgeHTTPCatalog{},
			writer: &knowledgeHTTPWriter{createFn: func(
				context.Context,
				knowledgecatalog.WriteScope,
				*opensplunkv1.CreateKnowledgeObjectRequest,
			) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
				return nil, definitive(control.ErrCapacityExceeded, &foreignApp)
			}},
		},
		{
			name: "list app differs from explicit filter",
			path: knowledgeObjectsListPath,
			request: &opensplunkv1.ListKnowledgeObjectsRequest{
				AppIdFilter: stringPointer(knowledgeHTTPAppID),
			},
			catalog: &knowledgeHTTPCatalog{listFn: func(
				context.Context,
				knowledgecatalog.ReadScope,
				knowledgecatalog.ListRequest,
			) (knowledgecatalog.ListPage, error) {
				return knowledgecatalog.ListPage{},
					knowledgecatalog.WithAuthorizedContext(
						knowledgecatalog.ErrInvalidCursor,
						foreignApp,
					)
			}},
			writer: &knowledgeHTTPWriter{},
		},
		{
			name: "get object differs from submitted object",
			path: knowledgeObjectsGetPath,
			request: &opensplunkv1.GetKnowledgeObjectRequest{
				KnowledgeObjectId: object.KnowledgeObjectID,
			},
			catalog: &knowledgeHTTPCatalog{getFn: func(
				context.Context,
				knowledgecatalog.ReadScope,
				string,
				*uint64,
			) (knowledgecatalog.Object, error) {
				return knowledgecatalog.Object{},
					knowledgecatalog.WithAuthorizedContext(
						control.ErrVersionConflict,
						foreignObject,
					)
			}},
			writer: &knowledgeHTTPWriter{},
		},
		{
			name:    "update object differs from submitted object",
			path:    knowledgeObjectsUpdatePath,
			request: validUpdate(),
			catalog: &knowledgeHTTPCatalog{},
			writer: &knowledgeHTTPWriter{updateFn: func(
				context.Context,
				knowledgecatalog.WriteScope,
				*opensplunkv1.UpdateKnowledgeObjectRequest,
			) (*opensplunkv1.UpdateKnowledgeObjectResponse, error) {
				return nil, definitive(control.ErrVersionConflict, &foreignObject)
			}},
		},
		{
			name: "bare create idempotency conflict",
			path: knowledgeObjectsCreatePath,
			request: &opensplunkv1.CreateKnowledgeObjectRequest{
				Definition: knowledgeHTTPDefinition(
					opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				),
				InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
				ClientRequestId: "context-binding-create-0002",
			},
			catalog: &knowledgeHTTPCatalog{},
			writer: &knowledgeHTTPWriter{createFn: func(
				context.Context,
				knowledgecatalog.WriteScope,
				*opensplunkv1.CreateKnowledgeObjectRequest,
			) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
				return nil, definitive(knowledgecatalog.ErrIdempotencyConflict, nil)
			}},
		},
		{
			name:    "bare update dependency conflict",
			path:    knowledgeObjectsUpdatePath,
			request: validUpdate(),
			catalog: &knowledgeHTTPCatalog{},
			writer: &knowledgeHTTPWriter{updateFn: func(
				context.Context,
				knowledgecatalog.WriteScope,
				*opensplunkv1.UpdateKnowledgeObjectRequest,
			) (*opensplunkv1.UpdateKnowledgeObjectResponse, error) {
				return nil, definitive(control.ErrDependencyConflict, nil)
			}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				test.catalog,
				test.writer,
				apps,
				appender,
			)
			response := knowledgeHTTPPost(t, httpHandler, test.path, test.request)
			attempts := appender.snapshot()
			if response.Code != http.StatusServiceUnavailable ||
				response.Body.String() != knowledgeManagementUnavailableBody ||
				len(attempts) != 1 ||
				attempts[0].definition.Reason != knowledgeattemptaudit.ReasonServiceUnavailable ||
				attempts[0].definition.AuthorizedContext != nil {
				t.Fatalf(
					"status=%d body=%q attempts=%+v",
					response.Code,
					response.Body.String(),
					attempts,
				)
			}
		})
	}
}

func TestKnowledgeHTTPAllowsIdempotencyConflictReceiptContextWithinScope(
	t *testing.T,
) {
	t.Parallel()

	object := knowledgeHTTPObject()
	apps := &knowledgeHTTPAppCatalog{result: KnowledgeAppCatalogResult{
		AppIDs:   []string{knowledgeHTTPAppID, knowledgeHTTPOtherAppID},
		Complete: true,
	}}
	tests := []struct {
		name       string
		path       string
		request    proto.Message
		writer     *knowledgeHTTPWriter
		wantObject bool
	}{
		{
			name: "create reused key belongs to another authorized app",
			path: knowledgeObjectsCreatePath,
			request: &opensplunkv1.CreateKnowledgeObjectRequest{
				Definition: knowledgeHTTPDefinition(
					opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				),
				InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
				ClientRequestId: "context-binding-create-0003",
			},
			writer: &knowledgeHTTPWriter{createFn: func(
				context.Context,
				knowledgecatalog.WriteScope,
				*opensplunkv1.CreateKnowledgeObjectRequest,
			) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
				err := knowledgecatalog.WithAuthorizedContext(
					knowledgecatalog.ErrIdempotencyConflict,
					knowledgecatalog.AuthorizedContext{AppID: knowledgeHTTPOtherAppID},
				)
				return nil, knowledgecatalog.WithErrorDisposition(
					err,
					knowledgecatalog.ErrorDispositionDefinitiveRejection,
				)
			}},
		},
		{
			name: "update reused key belongs to another authorized object",
			path: knowledgeObjectsUpdatePath,
			request: func() proto.Message {
				definition := knowledgeHTTPDefinition(
					opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				)
				definition.Name = "knowledge-http-object-updated"
				return &opensplunkv1.UpdateKnowledgeObjectRequest{
					KnowledgeObjectId: object.KnowledgeObjectID,
					ExpectedVersion:   object.Version,
					Definition:        definition,
					UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
					ClientRequestId:   "context-binding-update-0002",
				}
			}(),
			wantObject: true,
			writer: &knowledgeHTTPWriter{updateFn: func(
				context.Context,
				knowledgecatalog.WriteScope,
				*opensplunkv1.UpdateKnowledgeObjectRequest,
			) (*opensplunkv1.UpdateKnowledgeObjectResponse, error) {
				err := knowledgecatalog.WithAuthorizedContext(
					knowledgecatalog.ErrIdempotencyConflict,
					knowledgecatalog.AuthorizedContext{
						AppID: knowledgeHTTPAppID,
						Object: &knowledgecatalog.AuthorizedObject{
							KnowledgeObjectID: "ko-http-other-receipt-object",
							ObjectType:        object.ObjectType,
							Version:           object.Version,
							SharingScope:      object.SharingScope,
						},
					},
				)
				return nil, knowledgecatalog.WithErrorDisposition(
					err,
					knowledgecatalog.ErrorDispositionDefinitiveRejection,
				)
			}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				&knowledgeHTTPCatalog{},
				test.writer,
				apps,
				appender,
			)
			response := knowledgeHTTPPost(t, httpHandler, test.path, test.request)
			attempts := appender.snapshot()
			if response.Code != http.StatusConflict || len(attempts) != 1 ||
				attempts[0].definition.Reason != knowledgeattemptaudit.ReasonIdempotencyConflict ||
				attempts[0].definition.AuthorizedContext == nil ||
				(attempts[0].definition.AuthorizedContext.Object != nil) != test.wantObject {
				t.Fatalf(
					"status=%d body=%q attempts=%+v",
					response.Code,
					response.Body.String(),
					attempts,
				)
			}
		})
	}
}

func TestKnowledgeHTTPRefinesOnlyValidatedSemanticActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		request    proto.Message
		wantAction knowledgeattemptaudit.Action
		wantReason knowledgeattemptaudit.Reason
		wantStatus int
		wantWriter [4]int
	}{
		{
			name: "scope change", path: knowledgeObjectsUpdatePath,
			request:    &opensplunkv1.UpdateKnowledgeObjectRequest{KnowledgeObjectId: "ko-http-object-1", ExpectedVersion: 1, Definition: knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_APP), UpdateMask: knowledgeHTTPScopeChangeMask(), ClientRequestId: "scope-change-request-0001"},
			wantAction: knowledgeattemptaudit.ActionScopeChange, wantReason: knowledgeattemptaudit.ReasonInvalidDefinition, wantStatus: http.StatusBadRequest, wantWriter: [4]int{0, 1, 0, 0},
		},
		{
			name: "ordinary update", path: knowledgeObjectsUpdatePath,
			request:    &opensplunkv1.UpdateKnowledgeObjectRequest{KnowledgeObjectId: "ko-http-object-1", ExpectedVersion: 1, Definition: knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE), UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}}, ClientRequestId: "ordinary-update-request-0001"},
			wantAction: knowledgeattemptaudit.ActionUpdate, wantReason: knowledgeattemptaudit.ReasonInvalidDefinition, wantStatus: http.StatusBadRequest, wantWriter: [4]int{0, 1, 0, 0},
		},
		{
			name: "enable", path: knowledgeObjectsSetStatePath,
			request:    &opensplunkv1.SetKnowledgeObjectStateRequest{KnowledgeObjectId: "ko-http-object-1", ExpectedVersion: 1, State: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE, ClientRequestId: "enable-request-0001"},
			wantAction: knowledgeattemptaudit.ActionEnable, wantReason: knowledgeattemptaudit.ReasonServiceUnavailable, wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "disable", path: knowledgeObjectsSetStatePath,
			request:    &opensplunkv1.SetKnowledgeObjectStateRequest{KnowledgeObjectId: "ko-http-object-1", ExpectedVersion: 1, State: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, ClientRequestId: "disable-request-0001"},
			wantAction: knowledgeattemptaudit.ActionDisable, wantReason: knowledgeattemptaudit.ReasonInvalidDefinition, wantStatus: http.StatusBadRequest, wantWriter: [4]int{0, 0, 1, 0},
		},
		{
			name: "invalid state retains route fallback", path: knowledgeObjectsSetStatePath,
			request:    &opensplunkv1.SetKnowledgeObjectStateRequest{KnowledgeObjectId: "ko-http-object-1", ExpectedVersion: 1, State: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT, ClientRequestId: "invalid-state"},
			wantAction: knowledgeattemptaudit.ActionUpdate, wantReason: knowledgeattemptaudit.ReasonInvalidDefinition, wantStatus: http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rejection := knowledgecatalog.WithErrorDisposition(
				control.ErrInvalidArgument,
				knowledgecatalog.ErrorDispositionDefinitiveRejection,
			)
			writer := &knowledgeHTTPWriter{
				updateFn: func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.UpdateKnowledgeObjectRequest) (*opensplunkv1.UpdateKnowledgeObjectResponse, error) {
					return nil, rejection
				},
				stateFn: func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.SetKnowledgeObjectStateRequest) (*opensplunkv1.SetKnowledgeObjectStateResponse, error) {
					return nil, rejection
				},
			}
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, writer, knowledgeHTTPApps(), appender)
			response := knowledgeHTTPPost(t, httpHandler, test.path, test.request)
			calls := appender.snapshot()
			if response.Code != test.wantStatus || writer.callCounts() != test.wantWriter ||
				len(calls) != 1 || calls[0].definition.Action != test.wantAction ||
				calls[0].definition.Reason != test.wantReason {
				t.Fatalf("status=%d body=%q writer=%v attempts=%+v", response.Code, response.Body.String(), writer.callCounts(), calls)
			}
		})
	}

	t.Run("pre-decode update error keeps update", func(t *testing.T) {
		appender := &knowledgeBoundaryAppender{}
		writer := &knowledgeHTTPWriter{}
		_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, writer, knowledgeHTTPApps(), appender)
		request := &opensplunkv1.UpdateKnowledgeObjectRequest{Definition: knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE)}
		addKnowledgeHTTPUnknown(request.GetDefinition())
		response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsUpdatePath, request)
		calls := appender.snapshot()
		if response.Code != http.StatusBadRequest || len(calls) != 1 || calls[0].definition.Action != knowledgeattemptaudit.ActionUpdate || writer.callCounts() != [4]int{} {
			t.Fatalf("status=%d writer=%v attempts=%+v", response.Code, writer.callCounts(), calls)
		}
	})
}

func TestKnowledgeHTTPAppScopeFailuresCloseBeforeService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		result KnowledgeAppCatalogResult
		err    error
	}{
		{name: "incomplete", result: KnowledgeAppCatalogResult{AppIDs: []string{knowledgeHTTPAppID}}},
		{name: "empty", result: KnowledgeAppCatalogResult{Complete: true}},
		{name: "duplicate", result: KnowledgeAppCatalogResult{AppIDs: []string{knowledgeHTTPAppID, knowledgeHTTPAppID}, Complete: true}},
		{name: "noncanonical writer app", result: KnowledgeAppCatalogResult{AppIDs: []string{"readable-but-not-canonical"}, Complete: true}},
		{name: "catalog failure", err: errors.New("app database unavailable")},
		{name: "catalog not found sentinel", err: control.ErrNotFound},
		{name: "catalog invalid sentinel", err: control.ErrInvalidArgument},
		{name: "catalog capacity sentinel", err: control.ErrCapacityExceeded},
		{
			name: "catalog error cannot inject authorization",
			err: knowledgecatalog.WithAuthorizedContext(
				control.ErrInvalidArgument,
				knowledgecatalog.AuthorizedContext{AppID: knowledgeHTTPAppID},
			),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			apps := &knowledgeHTTPAppCatalog{result: test.result, err: test.err}
			catalog := &knowledgeHTTPCatalog{}
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, catalog, &knowledgeHTTPWriter{}, apps, appender)
			response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsGetPath, &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: "ko-http-object-1"})
			getCalls, listCalls := catalog.calls()
			calls := appender.snapshot()
			if response.Code != http.StatusServiceUnavailable || getCalls != 0 || listCalls != 0 ||
				len(calls) != 1 || calls[0].definition.Action != knowledgeattemptaudit.ActionGet ||
				calls[0].definition.Reason != knowledgeattemptaudit.ReasonServiceUnavailable ||
				calls[0].definition.AuthorizedContext != nil {
				t.Fatalf("status=%d body=%q catalog=%d/%d attempts=%+v", response.Code, response.Body.String(), getCalls, listCalls, calls)
			}
		})
	}
}

func TestKnowledgeHTTPListPreflightPinsCatalogRequestBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request *opensplunkv1.ListKnowledgeObjectsRequest
	}{
		{
			name: "page size",
			request: &opensplunkv1.ListKnowledgeObjectsRequest{
				Page: &opensplunkv1.PageRequest{
					PageSize: uint32Pointer(knowledgecatalog.MaximumPageSize + 1),
				},
			},
		},
		{
			name: "page token whitespace",
			request: &opensplunkv1.ListKnowledgeObjectsRequest{
				Page: &opensplunkv1.PageRequest{
					PageToken: stringPointer(" invalid-cursor "),
				},
			},
		},
		{
			name: "empty app filter",
			request: &opensplunkv1.ListKnowledgeObjectsRequest{
				AppIdFilter: stringPointer(""),
			},
		},
		{
			name: "control text filter",
			request: &opensplunkv1.ListKnowledgeObjectsRequest{
				TextFilter: stringPointer("invalid\x7ftext"),
			},
		},
		{
			name: "object type cardinality",
			request: &opensplunkv1.ListKnowledgeObjectsRequest{
				ObjectTypeFilters: []opensplunkv1.KnowledgeObjectType{
					opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
					opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
					opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
					opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
				},
			},
		},
		{
			name: "state cardinality",
			request: &opensplunkv1.ListKnowledgeObjectsRequest{
				StateFilters: []opensplunkv1.KnowledgeObjectState{
					opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
					opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
					opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
					opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED,
					opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED,
					opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
				},
			},
		},
		{
			name: "sharing scope cardinality",
			request: &opensplunkv1.ListKnowledgeObjectsRequest{
				SharingScopeFilters: []opensplunkv1.SharingScope{
					opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
					opensplunkv1.SharingScope_SHARING_SCOPE_APP,
					opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL,
					opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			catalog := &knowledgeHTTPCatalog{listFn: func(
				context.Context,
				knowledgecatalog.ReadScope,
				knowledgecatalog.ListRequest,
			) (knowledgecatalog.ListPage, error) {
				return knowledgecatalog.ListPage{}, nil
			}}
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				catalog,
				&knowledgeHTTPWriter{},
				knowledgeHTTPApps(),
				appender,
			)
			response := knowledgeHTTPPost(
				t,
				httpHandler,
				knowledgeObjectsListPath,
				test.request,
			)
			_, listCalls := catalog.calls()
			attempts := appender.snapshot()
			if response.Code != http.StatusBadRequest || listCalls != 0 ||
				len(attempts) != 1 ||
				attempts[0].definition.Action != knowledgeattemptaudit.ActionList ||
				attempts[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition ||
				attempts[0].definition.AuthorizedContext != nil {
				t.Fatalf(
					"status=%d body=%q calls=%d attempts=%+v",
					response.Code,
					response.Body.String(),
					listCalls,
					attempts,
				)
			}
		})
	}
}

func TestKnowledgeHTTPSerializationCapacityRejectsBeforeScopeAndService(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	apps := knowledgeHTTPApps()
	catalog := &knowledgeHTTPCatalog{}
	handler, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, catalog, &knowledgeHTTPWriter{}, apps, appender)
	for range cap(handler.serializationGate) {
		handler.serializationGate <- struct{}{}
	}
	response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsGetPath, &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: "ko-http-object-1"})
	for range cap(handler.serializationGate) {
		<-handler.serializationGate
	}
	getCalls, listCalls := catalog.calls()
	calls := appender.snapshot()
	if response.Code != http.StatusTooManyRequests || apps.callCount() != 0 || getCalls != 0 || listCalls != 0 ||
		len(calls) != 1 || calls[0].definition.Reason != knowledgeattemptaudit.ReasonResourceLimit {
		t.Fatalf("status=%d body=%q apps=%d catalog=%d/%d attempts=%+v", response.Code, response.Body.String(), apps.callCount(), getCalls, listCalls, calls)
	}
}

func TestKnowledgeHTTPCommittedAndIndeterminateMutationErrorsAreNeverRejected(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name        string
		disposition *knowledgecatalog.ErrorDisposition
	}{
		{name: "known committed", disposition: knowledgeDispositionPointer(knowledgecatalog.ErrorDispositionKnownCommitted)},
		{name: "indeterminate", disposition: knowledgeDispositionPointer(knowledgecatalog.ErrorDispositionIndeterminate)},
		{name: "missing disposition is indeterminate"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := errors.New("mutation boundary failed")
			if test.disposition != nil {
				err = knowledgecatalog.WithErrorDisposition(err, *test.disposition)
			}
			writer := &knowledgeHTTPWriter{createFn: func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.CreateKnowledgeObjectRequest) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
				return nil, err
			}}
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, writer, knowledgeHTTPApps(), appender)
			response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsCreatePath, &opensplunkv1.CreateKnowledgeObjectRequest{
				Definition: knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE), InitialState: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT, ClientRequestId: "outcome-test-request-0001",
			})
			if response.Code != http.StatusServiceUnavailable || response.Body.String() != knowledgeManagementUnavailableBody ||
				writer.callCounts() != [4]int{1, 0, 0, 0} || len(appender.snapshot()) != 0 {
				t.Fatalf("status=%d body=%q writer=%v attempts=%+v", response.Code, response.Body.String(), writer.callCounts(), appender.snapshot())
			}
		})
	}
}

func knowledgeDispositionPointer(value knowledgecatalog.ErrorDisposition) *knowledgecatalog.ErrorDisposition {
	return &value
}

func TestKnowledgeHTTPJournalFailureReplacesApplicationRejection(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{err: errors.New("journal unavailable")}
	_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, &knowledgeHTTPWriter{}, knowledgeHTTPApps(), appender)
	response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsSetStatePath, &opensplunkv1.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: "ko-http-object-1",
		ExpectedVersion:   1,
		State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId:   "invalid-state",
	})
	if response.Code != http.StatusServiceUnavailable ||
		response.Body.String() != knowledgeManagementUnavailableBody ||
		len(appender.snapshot()) != 1 {
		t.Fatalf("status=%d body=%q attempts=%+v", response.Code, response.Body.String(), appender.snapshot())
	}
}

func TestKnowledgeHTTPExactRouteAndOriginChecksPrecedeAuthentication(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	handler, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, &knowledgeHTTPWriter{}, knowledgeHTTPApps(), appender)
	authenticator := handler.browserAuthenticator.(*knowledgeBoundaryAuthenticator)
	tests := []struct {
		name       string
		method     string
		path       string
		host       string
		origin     string
		wantStatus int
	}{
		{name: "wrong method", method: http.MethodGet, path: knowledgeObjectsGetPath, host: "example.com", origin: "http://example.com", wantStatus: http.StatusMethodNotAllowed},
		{name: "unknown path", method: http.MethodPost, path: knowledgeObjectsGetPath + "/typo", host: "example.com", origin: "http://example.com", wantStatus: http.StatusNotFound},
		{name: "untrusted origin", method: http.MethodPost, path: knowledgeObjectsGetPath, host: "example.com", origin: "http://attacker.example", wantStatus: http.StatusForbidden},
	}
	for _, test := range tests {
		request := httptest.NewRequest(test.method, test.path, nil)
		request.Host = test.host
		request.Header.Set("Origin", test.origin)
		request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
		request.Header.Set("Content-Type", "application/x-protobuf")
		response := httptest.NewRecorder()
		httpHandler.ServeHTTP(response, request)
		if response.Code != test.wantStatus {
			t.Fatalf("%s status=%d body=%q", test.name, response.Code, response.Body.String())
		}
	}
	if authenticator.callCount() != 0 || len(appender.snapshot()) != 0 {
		t.Fatalf("auth calls=%d attempts=%+v", authenticator.callCount(), appender.snapshot())
	}
}

func TestKnowledgeHTTPSmallRouteBodyLimitIsAuditedBeforeScope(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	apps := knowledgeHTTPApps()
	_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, &knowledgeHTTPWriter{}, apps, appender)
	request := httptest.NewRequest(
		http.MethodPost,
		knowledgeObjectsGetPath,
		bytes.NewReader(bytes.Repeat([]byte{0xff}, int(maximumKnowledgeSmallRequestBytes)+1)),
	)
	request.Host = "example.com"
	request.Header.Set("Origin", "http://example.com")
	request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	httpHandler.ServeHTTP(response, request)
	calls := appender.snapshot()
	if response.Code != http.StatusRequestEntityTooLarge || apps.callCount() != 0 ||
		len(calls) != 1 || calls[0].definition.Action != knowledgeattemptaudit.ActionGet ||
		calls[0].definition.Reason != knowledgeattemptaudit.ReasonResourceLimit {
		t.Fatalf("status=%d body=%q apps=%d attempts=%+v", response.Code, response.Body.String(), apps.callCount(), calls)
	}
}

func TestKnowledgeHTTPReadCancellationReleasesSerializationPermit(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0
	catalog := &knowledgeHTTPCatalog{getFn: func(context.Context, knowledgecatalog.ReadScope, string, *uint64) (knowledgecatalog.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return knowledgecatalog.Object{}, context.Canceled
		}
		return knowledgeHTTPObject(), nil
	}}
	appender := &knowledgeBoundaryAppender{}
	_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, catalog, &knowledgeHTTPWriter{}, knowledgeHTTPApps(), appender)
	first := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsGetPath, &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: "ko-http-object-1"})
	second := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsGetPath, &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: "ko-http-object-1"})
	attempts := appender.snapshot()
	if first.Code != http.StatusRequestTimeout || second.Code != http.StatusOK ||
		len(attempts) != 1 || attempts[0].definition.Reason != knowledgeattemptaudit.ReasonServiceUnavailable {
		t.Fatalf("first=%d %q second=%d %q attempts=%+v", first.Code, first.Body.String(), second.Code, second.Body.String(), attempts)
	}
}

func TestKnowledgeHTTPRouteDeadlineBoundsBlockingAuthenticationBeforeDecode(
	t *testing.T,
) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	apps := knowledgeHTTPApps()
	handler, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, &knowledgeHTTPWriter{}, apps, appender)
	handler.browserAuthenticator = &knowledgeHTTPBlockingAuthenticator{}
	handler.routeTimeout = 20 * time.Millisecond
	body := newKnowledgeBoundaryObservedBody("unread definition secret", nil)
	request := knowledgeBoundaryRequest(context.Background(), http.MethodPost, knowledgeObjectsCreatePath, body)
	request.Host = "example.com"
	request.Header.Set("Origin", "http://example.com")
	response := httptest.NewRecorder()
	httpHandler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestTimeout || body.reads() != 0 ||
		apps.callCount() != 0 || len(appender.snapshot()) != 0 {
		t.Fatalf("status=%d body=%q reads=%d apps=%d attempts=%+v", response.Code, response.Body.String(), body.reads(), apps.callCount(), appender.snapshot())
	}
}

func TestKnowledgeHTTPGetRejectsMismatchedSameScopeProjectionWithoutLeak(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name    string
		request *opensplunkv1.GetKnowledgeObjectRequest
		mutate  func(*knowledgecatalog.Object)
	}{
		{
			name:    "wrong object identity",
			request: &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: "ko-http-object-1"},
			mutate: func(object *knowledgecatalog.Object) {
				object.KnowledgeObjectID = "ko-same-scope-secret-other"
			},
		},
		{
			name: "wrong historical version",
			request: &opensplunkv1.GetKnowledgeObjectRequest{
				KnowledgeObjectId: "ko-http-object-1",
				Version:           uint64Pointer(3),
			},
			mutate: func(object *knowledgecatalog.Object) {
				object.Version = 2
				object.UpdatedAt = object.UpdatedAt.Add(time.Microsecond)
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			returned := knowledgeHTTPObject()
			test.mutate(&returned)
			catalog := &knowledgeHTTPCatalog{getFn: func(context.Context, knowledgecatalog.ReadScope, string, *uint64) (knowledgecatalog.Object, error) {
				return returned, nil
			}}
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, catalog, &knowledgeHTTPWriter{}, knowledgeHTTPApps(), appender)
			response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsGetPath, test.request)
			attempts := appender.snapshot()
			body := response.Body.String()
			if response.Code != http.StatusServiceUnavailable || body != knowledgeManagementUnavailableBody ||
				strings.Contains(body, returned.KnowledgeObjectID) || strings.Contains(body, returned.Name) ||
				len(attempts) != 1 || attempts[0].definition.Action != knowledgeattemptaudit.ActionGet ||
				attempts[0].definition.Reason != knowledgeattemptaudit.ReasonServiceUnavailable ||
				attempts[0].definition.AuthorizedContext != nil {
				t.Fatalf("status=%d body=%q returned=%+v attempts=%+v", response.Code, body, returned, attempts)
			}
		})
	}
}

func TestKnowledgeHTTPIdentityValidationMatchesCatalogASCIIWhitespaceContract(
	t *testing.T,
) {
	t.Parallel()

	const nonBreakingName = "\u00a0knowledge-http-object\u00a0"
	if !validKnowledgeIdentity(nonBreakingName, maximumKnowledgeNameBytes) ||
		validKnowledgeIdentity(" knowledge-http-object", maximumKnowledgeNameBytes) ||
		validKnowledgeIdentity("knowledge-http-object\u0085", maximumKnowledgeNameBytes) {
		t.Fatal("knowledge HTTP identity validation diverges from the catalog contract")
	}

	returned := knowledgeHTTPObject()
	returned.Name = nonBreakingName
	returned.Definition.Name = nonBreakingName
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(returned.Definition)
	if err != nil {
		t.Fatalf("marshal non-breaking-space definition: %v", err)
	}
	digest := sha256.Sum256(encoded)
	returned.DefinitionSHA256 = digest[:]
	catalog := &knowledgeHTTPCatalog{getFn: func(
		context.Context,
		knowledgecatalog.ReadScope,
		string,
		*uint64,
	) (knowledgecatalog.Object, error) {
		return returned, nil
	}}
	appender := &knowledgeBoundaryAppender{}
	_, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		&knowledgeHTTPWriter{},
		knowledgeHTTPApps(),
		appender,
	)
	response := knowledgeHTTPPost(
		t,
		httpHandler,
		knowledgeObjectsGetPath,
		&opensplunkv1.GetKnowledgeObjectRequest{
			KnowledgeObjectId: returned.KnowledgeObjectID,
		},
	)
	if response.Code != http.StatusOK || len(appender.snapshot()) != 0 {
		t.Fatalf(
			"status=%d body=%q attempts=%+v",
			response.Code,
			response.Body.String(),
			appender.snapshot(),
		)
	}
	decoded := &opensplunkv1.GetKnowledgeObjectResponse{}
	if err := proto.Unmarshal(response.Body.Bytes(), decoded); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if decoded.GetKnowledgeObject().GetName() != nonBreakingName {
		t.Fatalf("name = %q, want %q", decoded.GetKnowledgeObject().GetName(), nonBreakingName)
	}
}

func TestKnowledgeHTTPReadScopeAcceptsTenantGlobalObjectFromUnreadableSourceApp(
	t *testing.T,
) {
	t.Parallel()

	global := knowledgeHTTPObject()
	global.AppID = knowledgeHTTPOtherAppID
	global.OwnerID = "owner-global-object-provenance"
	global.SharingScope = knowledgecatalog.SharingScopeGlobal
	global.Definition.AppId = knowledgeHTTPOtherAppID
	global.Definition.SharingScope =
		opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(
		global.Definition,
	)
	if err != nil {
		t.Fatalf("marshal global definition: %v", err)
	}
	digest := sha256.Sum256(encoded)
	global.DefinitionSHA256 = digest[:]
	catalog := &knowledgeHTTPCatalog{
		getFn: func(
			context.Context,
			knowledgecatalog.ReadScope,
			string,
			*uint64,
		) (knowledgecatalog.Object, error) {
			return global, nil
		},
		listFn: func(
			context.Context,
			knowledgecatalog.ReadScope,
			knowledgecatalog.ListRequest,
		) (knowledgecatalog.ListPage, error) {
			return knowledgecatalog.ListPage{
				Objects:         []knowledgecatalog.Object{global},
				CatalogRevision: global.Version,
			}, nil
		},
	}
	appender := &knowledgeBoundaryAppender{}
	_, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		&knowledgeHTTPWriter{},
		knowledgeHTTPApps(),
		appender,
	)
	get := knowledgeHTTPPost(
		t,
		httpHandler,
		knowledgeObjectsGetPath,
		&opensplunkv1.GetKnowledgeObjectRequest{
			KnowledgeObjectId: global.KnowledgeObjectID,
		},
	)
	list := knowledgeHTTPPost(
		t,
		httpHandler,
		knowledgeObjectsListPath,
		&opensplunkv1.ListKnowledgeObjectsRequest{},
	)
	if get.Code != http.StatusOK || list.Code != http.StatusOK ||
		len(appender.snapshot()) != 0 {
		t.Fatalf(
			"get=%d/%q list=%d/%q attempts=%+v",
			get.Code,
			get.Body.String(),
			list.Code,
			list.Body.String(),
			appender.snapshot(),
		)
	}
}

func TestKnowledgeHTTPHandlersDetachReadAndMutationResponsesBeforeEncoding(
	t *testing.T,
) {
	t.Parallel()

	t.Run("read", func(t *testing.T) {
		source := knowledgeHTTPObject()
		originalDigestByte := source.DefinitionSHA256[0]
		catalog := &knowledgeHTTPCatalog{getFn: func(context.Context, knowledgecatalog.ReadScope, string, *uint64) (knowledgecatalog.Object, error) {
			return source, nil
		}}
		handler, _ := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, catalog, &knowledgeHTTPWriter{}, knowledgeHTTPApps(), &knowledgeBoundaryAppender{})
		request := knowledgeHTTPDirectAdministratorRequest(t, knowledgeObjectsGetPath)
		serialized, err := handler.getKnowledgeObject(request, &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: source.KnowledgeObjectID})
		if err != nil {
			t.Fatalf("getKnowledgeObject: %v", err)
		}
		source.Definition.Name = "mutated-after-handler"
		source.DefinitionSHA256[0] ^= 0xff
		if serialized.message.GetKnowledgeObject().GetName() != "knowledge-http-object" ||
			serialized.message.GetKnowledgeObject().GetDefinition().GetName() != "knowledge-http-object" ||
			len(serialized.message.GetKnowledgeObject().GetDefinitionSha256()) != sha256.Size ||
			serialized.message.GetKnowledgeObject().GetDefinitionSha256()[0] != originalDigestByte {
			t.Fatalf("detached read response changed: %v", serialized.message)
		}
		response := httptest.NewRecorder()
		if err := newSerializedGetKnowledgeObjectCodec().Encode(response, serialized); err != nil {
			t.Fatalf("encode detached read: %v", err)
		}
	})

	t.Run("mutation", func(t *testing.T) {
		definition := knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE)
		source := &opensplunkv1.CreateKnowledgeObjectResponse{
			KnowledgeObject:         knowledgeHTTPProtoObject(t),
			TenantCatalogRevision:   1,
			TenantCatalogStateToken: bytes.Repeat([]byte{0x61}, 32),
		}
		writer := &knowledgeHTTPWriter{createFn: func(context.Context, knowledgecatalog.WriteScope, *opensplunkv1.CreateKnowledgeObjectRequest) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
			return source, nil
		}}
		handler, _ := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, writer, knowledgeHTTPApps(), &knowledgeBoundaryAppender{})
		request := knowledgeHTTPDirectAdministratorRequest(t, knowledgeObjectsCreatePath)
		serialized, err := handler.createKnowledgeObject(request, &opensplunkv1.CreateKnowledgeObjectRequest{
			Definition: definition, InitialState: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT, ClientRequestId: "detach-mutation-request-0001",
		})
		if err != nil {
			t.Fatalf("createKnowledgeObject: %v", err)
		}
		source.KnowledgeObject.Definition.Name = "mutated-after-handler"
		source.TenantCatalogStateToken[0] ^= 0xff
		if serialized.message.GetKnowledgeObject().GetDefinition().GetName() != "knowledge-http-object" ||
			serialized.message.GetTenantCatalogStateToken()[0] != 0x61 {
			t.Fatalf("detached mutation response changed: %v", serialized.message)
		}
		response := httptest.NewRecorder()
		if err := newSerializedCreateKnowledgeObjectCodec().Encode(response, serialized); err != nil {
			t.Fatalf("encode detached mutation: %v", err)
		}
	})
}

func knowledgeHTTPDirectAdministratorRequest(t *testing.T, path string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, nil)
	principal := knowledgeBoundaryPrincipal(t, auth.BrowserRoleAdministrator)
	return request.WithContext(context.WithValue(
		request.Context(),
		browserPrincipalContextKey{},
		principal,
	))
}

func TestKnowledgeHTTPCodecEnforcesResponseBoundAndAlwaysReleasesPermit(
	t *testing.T,
) {
	t.Parallel()

	description := strings.Repeat("x", maximumKnowledgeObjectResponseBytes+1)
	releases := 0
	result := &serializedCreateKnowledgeObjectResponse{
		message: &opensplunkv1.CreateKnowledgeObjectResponse{
			KnowledgeObject: &opensplunkv1.KnowledgeObject{
				Definition: &opensplunkv1.KnowledgeObjectDefinition{Description: &description},
			},
		},
		ctx: context.Background(),
		release: func() {
			releases++
		},
	}
	response := httptest.NewRecorder()
	err := newSerializedCreateKnowledgeObjectCodec().Encode(response, result)
	if err == nil || !strings.Contains(err.Error(), "exceeds its byte limit") ||
		releases != 1 || response.Body.Len() != 0 {
		t.Fatalf("error=%v releases=%d body bytes=%d", err, releases, response.Body.Len())
	}
}
