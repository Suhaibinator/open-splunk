package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/lookupservice"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lookupHTTPService struct {
	mu      sync.Mutex
	ready   bool
	scope   lookupservice.Scope
	getErr  error
	created *opensplunkv1.Lookup
}

func (service *lookupHTTPService) Ready() bool { return service != nil && service.ready }

func (service *lookupHTTPService) record(scope lookupservice.Scope) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.scope = scope
}

func (service *lookupHTTPService) Create(_ context.Context, scope lookupservice.Scope, input *opensplunkv1.CreateLookupRequest) (*opensplunkv1.CreateLookupResponse, error) {
	service.record(scope)
	lookup := lookupHTTPProjection(scope, "lookup-1", input.GetDefinition(), 1, opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE)
	service.mu.Lock()
	service.created = lookup
	service.mu.Unlock()
	return &opensplunkv1.CreateLookupResponse{Lookup: lookup}, nil
}

func (service *lookupHTTPService) Get(_ context.Context, scope lookupservice.Scope, input *opensplunkv1.GetLookupRequest) (*opensplunkv1.GetLookupResponse, error) {
	service.record(scope)
	service.mu.Lock()
	err := service.getErr
	service.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &opensplunkv1.GetLookupResponse{Lookup: lookupHTTPProjection(scope, input.GetLookupId(), lookupHTTPDefinition("service_catalog"), max(input.GetVersion(), 1), opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE)}, nil
}

func (service *lookupHTTPService) List(_ context.Context, scope lookupservice.Scope, _ *opensplunkv1.ListLookupsRequest) (*opensplunkv1.ListLookupsResponse, error) {
	service.record(scope)
	return &opensplunkv1.ListLookupsResponse{Lookups: []*opensplunkv1.Lookup{
		lookupHTTPProjection(scope, "lookup-list", lookupHTTPDefinition("service_catalog"), 1, opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE),
	}, Page: &opensplunkv1.PageResponse{}}, nil
}

func (service *lookupHTTPService) Replace(_ context.Context, scope lookupservice.Scope, input *opensplunkv1.ReplaceLookupRequest) (*opensplunkv1.ReplaceLookupResponse, error) {
	service.record(scope)
	return &opensplunkv1.ReplaceLookupResponse{Lookup: lookupHTTPProjection(scope, input.GetLookupId(), input.GetDefinition(), input.GetExpectedVersion()+1, opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE)}, nil
}

func (service *lookupHTTPService) SetState(_ context.Context, scope lookupservice.Scope, input *opensplunkv1.SetLookupStateRequest) (*opensplunkv1.SetLookupStateResponse, error) {
	service.record(scope)
	lookup := lookupHTTPProjection(scope, input.GetLookupId(), lookupHTTPDefinition("service_catalog"), input.GetExpectedVersion()+1, input.GetState())
	if input.GetState() == opensplunkv1.LookupState_LOOKUP_STATE_DISABLED {
		lookup.DisabledAt = timestamppb.New(knowledgeBoundaryNow)
	}
	return &opensplunkv1.SetLookupStateResponse{Lookup: lookup}, nil
}

func (service *lookupHTTPService) Delete(_ context.Context, scope lookupservice.Scope, input *opensplunkv1.DeleteLookupRequest) (*opensplunkv1.DeleteLookupResponse, error) {
	service.record(scope)
	return &opensplunkv1.DeleteLookupResponse{LookupId: input.GetLookupId(), Version: input.GetExpectedVersion() + 1}, nil
}

func (service *lookupHTTPService) Preview(_ context.Context, scope lookupservice.Scope, _ *opensplunkv1.PreviewLookupRequest) (*opensplunkv1.PreviewLookupResponse, error) {
	service.record(scope)
	return &opensplunkv1.PreviewLookupResponse{
		Columns: []string{"service_id", "owner"}, Rows: []*opensplunkv1.LookupPreviewRow{{Values: []string{"api", "alice"}}},
		TotalRows: 1, SourceSha256: make([]byte, 32), ContentSha256: make([]byte, 32),
	}, nil
}

func TestLookupRoutesAreOneAuthenticatedExactFamily(t *testing.T) {
	service := &lookupHTTPService{ready: true}
	config := knowledgeConfigBase(t)
	config.LookupManagement = service
	handler, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler(): %v", err)
	}
	paths := []string{lookupCreatePath, lookupGetPath, lookupListPath, lookupReplacePath, lookupSetStatePath, lookupDeletePath, lookupPreviewPath}
	for _, path := range paths {
		request := lookupHTTPRequestForPath(path)
		response := knowledgeHTTPPost(t, handler, path, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%q", path, response.Code, response.Body.String())
		}
	}
	service.mu.Lock()
	gotScope := service.scope
	service.mu.Unlock()
	if gotScope != (lookupservice.Scope{TenantID: knowledgeBoundaryTenantID, OwnerID: knowledgeBoundaryOwnerID}) {
		t.Fatalf("service scope = %#v", gotScope)
	}

	payload, _ := proto.Marshal(&opensplunkv1.GetLookupRequest{LookupId: "lookup-1"})
	unauthorized := httptest.NewRequestWithContext(t.Context(), http.MethodPost, lookupGetPath, strings.NewReader(string(payload)))
	unauthorized.Host = "example.com"
	unauthorized.Header.Set("Origin", "http://example.com")
	unauthorized.Header.Set("Content-Type", "application/x-protobuf")
	unauthorizedResponse := httptest.NewRecorder()
	handler.ServeHTTP(unauthorizedResponse, unauthorized)
	if unauthorizedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", unauthorizedResponse.Code)
	}

	wrongMethod := httptest.NewRequestWithContext(t.Context(), http.MethodGet, lookupGetPath, nil)
	wrongMethod.Host = "example.com"
	wrongMethod.Header.Set("Origin", "http://example.com")
	wrongMethod.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
	wrongMethodResponse := httptest.NewRecorder()
	handler.ServeHTTP(wrongMethodResponse, wrongMethod)
	if wrongMethodResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong-method status=%d", wrongMethodResponse.Code)
	}
	unknown := knowledgeHTTPPost(t, handler, lookupGetPath+"/typo", &opensplunkv1.GetLookupRequest{LookupId: "lookup-1"})
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown-path status=%d", unknown.Code)
	}
}

func TestLookupConfigurationAndErrorsFailClosed(t *testing.T) {
	config := knowledgeConfigBase(t)
	config.LookupManagement = &lookupHTTPService{}
	if _, err := NewHandler(config); err == nil || !strings.Contains(err.Error(), "lookup management service is not ready") {
		t.Fatalf("unready NewHandler() error=%v", err)
	}
	var typedNil *lookupHTTPService
	config = knowledgeConfigBase(t)
	config.LookupManagement = typedNil
	handler, err := NewHandler(config)
	if err != nil {
		t.Fatalf("typed-nil NewHandler(): %v", err)
	}
	response := knowledgeHTTPPost(t, handler, lookupGetPath, &opensplunkv1.GetLookupRequest{LookupId: "lookup-1"})
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled route status=%d", response.Code)
	}

	service := &lookupHTTPService{ready: true}
	config = knowledgeConfigBase(t)
	config.LookupManagement = service
	handler, err = NewHandler(config)
	if err != nil {
		t.Fatalf("configured NewHandler(): %v", err)
	}
	tests := []struct {
		err  error
		want int
	}{
		{lookupservice.ErrInvalid, http.StatusBadRequest},
		{lookupservice.ErrNotFound, http.StatusNotFound},
		{lookupservice.ErrConflict, http.StatusConflict},
		{lookupservice.ErrResourceLimit, http.StatusRequestEntityTooLarge},
		{lookupservice.ErrUnavailable, http.StatusServiceUnavailable},
		{errors.New("secret backend detail"), http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		service.mu.Lock()
		service.getErr = test.err
		service.mu.Unlock()
		response := knowledgeHTTPPost(t, handler, lookupGetPath, &opensplunkv1.GetLookupRequest{LookupId: "lookup-1"})
		if response.Code != test.want || strings.Contains(response.Body.String(), "secret backend detail") {
			t.Fatalf("error %v status=%d body=%q", test.err, response.Code, response.Body.String())
		}
	}
}

func TestLookupHandlerRejectsInvalidTrustedProjection(t *testing.T) {
	service := &lookupHTTPService{ready: true}
	config := knowledgeConfigBase(t)
	config.LookupManagement = service
	handler, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler(): %v", err)
	}
	service.mu.Lock()
	service.getErr = nil
	service.mu.Unlock()

	// The direct service fake normally returns a valid scoped projection. Make
	// the requested ID invalid so response authority validation rejects it even
	// though the configurable dependency reports success.
	response := knowledgeHTTPPost(t, handler, lookupGetPath, &opensplunkv1.GetLookupRequest{LookupId: " bad-id"})
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("invalid projection status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestLookupDefinitionSanitizerRejectsUnknownSemanticsBeforePersistence(t *testing.T) {
	wrappers := []struct {
		name string
		run  func(*opensplunkv1.LookupDefinition) error
	}{
		{"create", func(definition *opensplunkv1.LookupDefinition) error {
			_, err := forwardCompatibleProtoSanitizer(&opensplunkv1.CreateLookupRequest{Definition: definition})
			return err
		}},
		{"replace", func(definition *opensplunkv1.LookupDefinition) error {
			_, err := forwardCompatibleProtoSanitizer(&opensplunkv1.ReplaceLookupRequest{Definition: definition})
			return err
		}},
		{"preview", func(definition *opensplunkv1.LookupDefinition) error {
			_, err := forwardCompatibleProtoSanitizer(&opensplunkv1.PreviewLookupRequest{Definition: definition})
			return err
		}},
	}
	for _, wrapper := range wrappers {
		t.Run(wrapper.name, func(t *testing.T) {
			definition := lookupHTTPDefinition("catalog")
			addKnowledgeHTTPUnknown(definition.GetOutputMappings()[0])
			if err := wrapper.run(definition); err == nil || !strings.Contains(err.Error(), "unsupported fields") {
				t.Fatalf("sanitize unknown lookup definition error = %v", err)
			}
			if len(definition.GetOutputMappings()[0].ProtoReflect().GetUnknown()) == 0 {
				t.Fatal("rejected lookup definition was mutated")
			}
		})
	}

	oversized := lookupHTTPDefinition("catalog")
	oversized.KeyMappings = make([]*opensplunkv1.LookupFieldMapping, 5)
	oversized.KeyMappings[0] = &opensplunkv1.LookupFieldMapping{}
	addKnowledgeHTTPUnknown(oversized.KeyMappings[0])
	if _, err := forwardCompatibleProtoSanitizer(&opensplunkv1.CreateLookupRequest{Definition: oversized}); err == nil ||
		!strings.Contains(err.Error(), "entry limit") {
		t.Fatalf("sanitize oversized definition error = %v", err)
	}
	if len(oversized.KeyMappings[0].ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("shape preflight traversed the oversized definition")
	}

	envelope := &opensplunkv1.CreateLookupRequest{Definition: lookupHTTPDefinition("catalog"), CsvData: []byte("id,value\n")}
	addKnowledgeHTTPUnknown(envelope)
	if _, err := forwardCompatibleProtoSanitizer(envelope); err != nil {
		t.Fatalf("sanitize future request envelope: %v", err)
	}
	if len(envelope.ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("future request envelope field was not discarded")
	}
}

func TestLookupTrustedProjectionValidationPinsBoundsAndLifecycle(t *testing.T) {
	scope := lookupservice.Scope{TenantID: knowledgeBoundaryTenantID, OwnerID: knowledgeBoundaryOwnerID}
	projection := lookupHTTPProjection(scope, "lookup-1", lookupHTTPDefinition("catalog"), 1, opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE)
	if !validLookupProjection(projection, scope) {
		t.Fatal("valid lookup projection was rejected")
	}

	forged := proto.Clone(projection).(*opensplunkv1.Lookup)
	forged.Version = uint64(math.MaxInt64) + 1
	if validLookupProjection(forged, scope) {
		t.Fatal("high-bit SQLite version was accepted")
	}
	forged = proto.Clone(projection).(*opensplunkv1.Lookup)
	forged.Columns = []string{"service_id", "bad\u200bheader"}
	if validLookupProjection(forged, scope) {
		t.Fatal("format-bearing CSV header was accepted")
	}
	forged = proto.Clone(projection).(*opensplunkv1.Lookup)
	forged.Definition.OutputMappings = make([]*opensplunkv1.LookupFieldMapping, 17)
	if validLookupProjection(forged, scope) {
		t.Fatal("oversized repeated definition was accepted")
	}
	forged = proto.Clone(projection).(*opensplunkv1.Lookup)
	forged.State = opensplunkv1.LookupState_LOOKUP_STATE_DELETED
	forged.DeletedAt = timestamppb.New(knowledgeBoundaryNow)
	if validLookupProjection(forged, scope) {
		t.Fatal("deleted lookup without a prior disabled timestamp was accepted")
	}
}

func TestLookupPreviewValidationRejectsInconsistentAuthority(t *testing.T) {
	valid := &opensplunkv1.PreviewLookupResponse{
		Columns: []string{"id"}, Rows: []*opensplunkv1.LookupPreviewRow{{Values: []string{"one"}}},
		TotalRows: 1, SourceSha256: make([]byte, 32), ContentSha256: make([]byte, 32),
	}
	if !validLookupPreview(valid) {
		t.Fatal("valid lookup preview was rejected")
	}
	forged := proto.Clone(valid).(*opensplunkv1.PreviewLookupResponse)
	forged.TotalRows = 2
	if validLookupPreview(forged) {
		t.Fatal("inconsistent preview truncation was accepted")
	}
	forged = &opensplunkv1.PreviewLookupResponse{
		TotalRows: 1, SourceSha256: make([]byte, 32),
		Violations: []*opensplunkv1.FieldViolation{{FieldPath: "csv_data", Code: "LOOKUP_INVALID", Message: "invalid"}},
	}
	if validLookupPreview(forged) {
		t.Fatal("CSV failure carrying impossible row authority was accepted")
	}
	for _, violation := range []*opensplunkv1.FieldViolation{
		{FieldPath: strings.Repeat("é", 128), Code: "LOOKUP_INVALID", Message: "invalid path"},
		{FieldPath: "definition", Code: strings.Repeat("é", 65), Message: "invalid code"},
	} {
		forged = &opensplunkv1.PreviewLookupResponse{
			SourceSha256: make([]byte, 32),
			Violations:   []*opensplunkv1.FieldViolation{violation},
		}
		if validLookupPreview(forged) {
			t.Fatalf("oversized UTF-8 violation authority was accepted: %#v", violation)
		}
	}
}

func lookupHTTPRequestForPath(path string) proto.Message {
	switch path {
	case lookupCreatePath:
		return &opensplunkv1.CreateLookupRequest{Definition: lookupHTTPDefinition("service_catalog"), CsvData: []byte("service_id,owner\napi,alice\n")}
	case lookupGetPath:
		return &opensplunkv1.GetLookupRequest{LookupId: "lookup-1"}
	case lookupListPath:
		return &opensplunkv1.ListLookupsRequest{}
	case lookupReplacePath:
		return &opensplunkv1.ReplaceLookupRequest{LookupId: "lookup-1", ExpectedVersion: 1, Definition: lookupHTTPDefinition("service_catalog")}
	case lookupSetStatePath:
		return &opensplunkv1.SetLookupStateRequest{LookupId: "lookup-1", ExpectedVersion: 1, State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED}
	case lookupDeletePath:
		return &opensplunkv1.DeleteLookupRequest{LookupId: "lookup-1", ExpectedVersion: 1, ConfirmationName: "service_catalog"}
	case lookupPreviewPath:
		return &opensplunkv1.PreviewLookupRequest{Definition: lookupHTTPDefinition("service_catalog"), CsvData: []byte("service_id,owner\napi,alice\n")}
	default:
		panic("unknown lookup test path")
	}
}

func lookupHTTPDefinition(name string) *opensplunkv1.LookupDefinition {
	return &opensplunkv1.LookupDefinition{
		AppId: "search", Name: name, SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: "service_id", EventField: "service_id"}},
		OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: "owner", EventField: "service_owner"}},
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
	}
}

func lookupHTTPProjection(scope lookupservice.Scope, id string, definition *opensplunkv1.LookupDefinition, version uint64, state opensplunkv1.LookupState) *opensplunkv1.Lookup {
	return &opensplunkv1.Lookup{
		LookupId: id, TenantId: scope.TenantID, OwnerId: scope.OwnerID, Version: version, State: state,
		Definition: proto.Clone(definition).(*opensplunkv1.LookupDefinition), Columns: []string{"service_id", "owner"}, RowCount: 1,
		CanonicalSizeBytes: 32, SourceSha256: make([]byte, 32), ContentSha256: make([]byte, 32),
		CreatedAt: timestamppb.New(knowledgeBoundaryNow), UpdatedAt: timestamppb.New(knowledgeBoundaryNow),
	}
}
