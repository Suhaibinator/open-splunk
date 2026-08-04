package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type fakeSearchAttemptAudit struct {
	mu       sync.Mutex
	calls    int
	tenantID string
	request  searchaudit.ListRequest
	page     searchaudit.ListPage
	err      error
}

func (service *fakeSearchAttemptAudit) List(
	ctx context.Context,
	tenantID string,
	request searchaudit.ListRequest,
) (searchaudit.ListPage, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.calls++
	service.tenantID = tenantID
	service.request = request
	if err := ctx.Err(); err != nil {
		return searchaudit.ListPage{}, err
	}
	return service.page, service.err
}

func (service *fakeSearchAttemptAudit) snapshot() (
	int,
	string,
	searchaudit.ListRequest,
) {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.calls, service.tenantID, service.request
}

func TestSearchAttemptAuditProtoContractIsNarrow(t *testing.T) {
	t.Parallel()

	event := (&opensplunkv1.SearchAttemptAuditEvent{}).ProtoReflect().Descriptor()
	assertExactProtoFields(t, event.Fields(), []protoFieldContract{
		{name: "sequence", number: 1},
		{name: "occurred_at", number: 2},
		{name: "actor_kind", number: 3},
		{name: "actor_id", number: 4},
		{name: "actor_role", number: 5},
		{name: "owner_id", number: 6},
		{name: "search_job_id", number: 7},
	})
	if got := event.Fields().ByName("actor_kind").Enum().FullName(); got != "open_splunk.v1.AuditActorKind" {
		t.Fatalf("actor_kind enum = %q", got)
	}
	if got := event.Fields().ByName("actor_role").Enum().FullName(); got != "open_splunk.v1.AuditActorRole" {
		t.Fatalf("actor_role enum = %q", got)
	}
	for _, forbidden := range []protoreflect.Name{
		"action", "payload", "spl", "index", "sql", "result",
	} {
		if event.Fields().ByName(forbidden) != nil {
			t.Fatalf("search attempt audit event exposes forbidden field %q", forbidden)
		}
	}

	request := (&opensplunkv1.ListSearchAttemptAuditEventsRequest{}).
		ProtoReflect().Descriptor()
	assertExactProtoFields(t, request.Fields(), []protoFieldContract{
		{name: "page", number: 1},
		{name: "actor_id_filter", number: 2},
		{name: "owner_id_filter", number: 3},
	})
	response := (&opensplunkv1.ListSearchAttemptAuditEventsResponse{}).
		ProtoReflect().Descriptor()
	assertExactProtoFields(t, response.Fields(), []protoFieldContract{
		{name: "events", number: 1},
		{name: "page", number: 2},
	})
	if got := int32(opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT); got != 14 {
		t.Fatalf("search attempt audit feature number = %d, want 14", got)
	}
}

type protoFieldContract struct {
	name   protoreflect.Name
	number protoreflect.FieldNumber
}

func assertExactProtoFields(
	t *testing.T,
	fields protoreflect.FieldDescriptors,
	want []protoFieldContract,
) {
	t.Helper()
	if fields.Len() != len(want) {
		t.Fatalf("field count = %d, want %d", fields.Len(), len(want))
	}
	for index, expected := range want {
		field := fields.Get(index)
		if field.Name() != expected.name || field.Number() != expected.number {
			t.Fatalf(
				"field %d = %s/%d, want %s/%d",
				index,
				field.Name(),
				field.Number(),
				expected.name,
				expected.number,
			)
		}
	}
}

func TestSearchAttemptAuditListUsesAdministratorTenantAndExactFilters(
	t *testing.T,
) {
	t.Parallel()

	anchor := time.Date(2026, time.August, 4, 18, 19, 20, 123456000, time.UTC)
	total := uint64(3)
	service := &fakeSearchAttemptAudit{page: searchaudit.ListPage{
		Events: []searchaudit.Event{{
			Sequence:   9,
			TenantID:   browserGateTenantID,
			OccurredAt: anchor,
			Actor: audit.Actor{
				Kind: audit.ActorKindBrowser,
				ID:   browserGateOwnerID,
				Role: audit.ActorRoleAdministrator,
			},
			OwnerID:     "searched-owner",
			SearchJobID: "search-job-9",
		}},
		NextPageToken:  "signed-next-page",
		TotalSize:      &total,
		TotalSizeExact: true,
	}}
	handler := newSearchAttemptAuditTestHandler(t, service, 0)
	actorID := browserGateOwnerID
	ownerID := "searched-owner"
	input := &opensplunkv1.ListSearchAttemptAuditEventsRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         uint32Pointer(1),
			PageToken:        stringPointer("opaque-page"),
			IncludeTotalSize: true,
		},
		ActorIdFilter: &actorID,
		OwnerIdFilter: &ownerID,
	}
	input.ProtoReflect().SetUnknown(futureProtobufField("future-search-audit"))
	input.Page.ProtoReflect().SetUnknown(futureProtobufField("future-page"))
	response := postAuthenticatedSearchAttemptAudit(t, handler, input)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}

	calls, tenantID, request := service.snapshot()
	if calls != 1 || tenantID != browserGateTenantID || request.PageSize != 1 ||
		request.PageToken != "opaque-page" || !request.IncludeTotal ||
		request.ActorID == nil || *request.ActorID != actorID ||
		request.OwnerID == nil || *request.OwnerID != ownerID {
		t.Fatalf("search attempt audit call = %d/%q/%#v", calls, tenantID, request)
	}

	var decoded opensplunkv1.ListSearchAttemptAuditEventsResponse
	unmarshalResponse(t, response, &decoded)
	if len(decoded.GetEvents()) != 1 ||
		decoded.GetPage().GetNextPageToken() != "signed-next-page" ||
		decoded.GetPage().GetTotalSize() != total ||
		!decoded.GetPage().GetTotalSizeExact() {
		t.Fatalf("search attempt audit response = %#v", &decoded)
	}
	event := decoded.GetEvents()[0]
	if event.GetSequence() != 9 || !event.GetOccurredAt().AsTime().Equal(anchor) ||
		event.GetActorKind() != opensplunkv1.AuditActorKind_AUDIT_ACTOR_KIND_BROWSER ||
		event.GetActorId() != browserGateOwnerID ||
		event.GetActorRole() != opensplunkv1.AuditActorRole_AUDIT_ACTOR_ROLE_ADMINISTRATOR ||
		event.GetOwnerId() != ownerID || event.GetSearchJobId() != "search-job-9" {
		t.Fatalf("search attempt audit event = %#v", event)
	}
}

func TestSearchAttemptAuditListRejectsInvalidRequestsBeforeStorage(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("a", maximumIdentityBytes+1)
	for _, test := range []struct {
		name    string
		request *opensplunkv1.ListSearchAttemptAuditEventsRequest
	}{
		{
			name: "oversized page",
			request: &opensplunkv1.ListSearchAttemptAuditEventsRequest{
				Page: &opensplunkv1.PageRequest{
					PageSize: uint32Pointer(searchaudit.MaximumListPageSize + 1),
				},
			},
		},
		{name: "empty actor", request: &opensplunkv1.ListSearchAttemptAuditEventsRequest{
			ActorIdFilter: stringPointer(""),
		}},
		{name: "padded owner", request: &opensplunkv1.ListSearchAttemptAuditEventsRequest{
			OwnerIdFilter: stringPointer(" owner"),
		}},
		{name: "oversized owner", request: &opensplunkv1.ListSearchAttemptAuditEventsRequest{
			OwnerIdFilter: &oversized,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeSearchAttemptAudit{}
			handler := newSearchAttemptAuditTestHandler(t, service, 0)
			response := postAuthenticatedSearchAttemptAudit(t, handler, test.request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			calls, _, _ := service.snapshot()
			if calls != 0 {
				t.Fatalf("invalid request reached storage %d times", calls)
			}
		})
	}
}

func TestSearchAttemptAuditListRequiresAdministratorBeforeBodyOrStorage(
	t *testing.T,
) {
	t.Parallel()

	service := &fakeSearchAttemptAudit{}
	handler := newSearchAttemptAuditTestHandler(t, service, 0)
	body := &observedRequestBody{}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		searchAttemptAuditListPath,
		nil,
	)
	request.Body = body
	request.Host = "example.com"
	request.Header.Set("Content-Type", "text/plain")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized ||
		response.Header().Get("WWW-Authenticate") != administratorAuthenticationRealm {
		t.Fatalf(
			"response = %d/%q/%q",
			response.Code,
			response.Header().Get("WWW-Authenticate"),
			response.Body.String(),
		)
	}
	calls, _, _ := service.snapshot()
	if body.reads != 0 || calls != 0 {
		t.Fatalf(
			"unauthorized search attempt audit work = body reads %d, calls %d",
			body.reads,
			calls,
		)
	}
}

func TestSearchAttemptAuditListRejectsMismatchedPrincipalsBeforeBodyOrStorage(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name     string
		tenantID string
		ownerID  string
		role     auth.BrowserRole
	}{
		{
			name:     "ordinary user",
			tenantID: browserGateTenantID,
			ownerID:  browserGateOwnerID,
			role:     auth.BrowserRoleUser,
		},
		{
			name:     "wrong-tenant administrator",
			tenantID: "other-tenant",
			ownerID:  browserGateOwnerID,
			role:     auth.BrowserRoleAdministrator,
		},
		{
			name:     "wrong-owner administrator",
			tenantID: browserGateTenantID,
			ownerID:  "other-owner",
			role:     auth.BrowserRoleAdministrator,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			principal := browserGatePrincipal(
				t,
				test.tenantID,
				test.ownerID,
				test.role,
			)
			authenticator := &recordingBrowserAuthenticator{
				fn: func(context.Context, []byte) (auth.BrowserPrincipal, error) {
					return principal, nil
				},
			}
			service := &fakeSearchAttemptAudit{}
			handler := newTestHandler(t, Config{
				SearchJobs:                 &fakeSearchJobs{},
				Indexes:                    fakeIndexCatalog{},
				SearchAttemptAuditEvents:   service,
				BrowserAuthenticator:       authenticator,
				WebUI:                      testUI(),
				TenantID:                   browserGateTenantID,
				OwnerID:                    browserGateOwnerID,
				AdministrativeAllowedHosts: []string{"example.com"},
			})
			body := &observedRequestBody{}
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				searchAttemptAuditListPath,
				nil,
			)
			request.Body = body
			request.Host = "example.com"
			request.Header.Set("Content-Type", "text/plain")
			request.Header.Set(
				"Authorization",
				"Bearer "+adminIntegrationBearerToken,
			)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusForbidden {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					http.StatusForbidden,
					response.Body.String(),
				)
			}
			calls, _, _ := service.snapshot()
			if body.reads != 0 || calls != 0 {
				t.Fatalf(
					"forbidden search attempt audit work = body reads %d, calls %d",
					body.reads,
					calls,
				)
			}
			if authenticator.callCount() != 1 {
				t.Fatalf(
					"authenticator calls = %d, want 1",
					authenticator.callCount(),
				)
			}
		})
	}
}

func TestSearchAttemptAuditListRejectsOutOfContractServicePages(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, time.August, 4, 18, 19, 20, 0, time.UTC)
	valid := searchaudit.Event{
		Sequence:   3,
		TenantID:   browserGateTenantID,
		OccurredAt: anchor,
		Actor: audit.Actor{
			Kind: audit.ActorKindBrowser,
			ID:   browserGateOwnerID,
			Role: audit.ActorRoleAdministrator,
		},
		OwnerID:     "owner-a",
		SearchJobID: "search-job-3",
	}
	actorID := browserGateOwnerID
	ownerID := "owner-a"
	input := &opensplunkv1.ListSearchAttemptAuditEventsRequest{
		ActorIdFilter: &actorID,
		OwnerIdFilter: &ownerID,
	}
	for name, mutate := range map[string]func(*searchaudit.ListPage){
		"wrong tenant": func(page *searchaudit.ListPage) {
			page.Events[0].TenantID = "other-tenant"
		},
		"wrong actor": func(page *searchaudit.ListPage) {
			page.Events[0].Actor.ID = "other-actor"
		},
		"wrong owner": func(page *searchaudit.ListPage) {
			page.Events[0].OwnerID = "owner-b"
		},
		"unstable order": func(page *searchaudit.ListPage) {
			second := page.Events[0]
			second.Sequence++
			second.SearchJobID = "search-job-4"
			page.Events = append(page.Events, second)
		},
	} {
		t.Run(name, func(t *testing.T) {
			page := searchaudit.ListPage{Events: []searchaudit.Event{valid}}
			mutate(&page)
			service := &fakeSearchAttemptAudit{page: page}
			handler := newSearchAttemptAuditTestHandler(t, service, 0)
			response := postAuthenticatedSearchAttemptAudit(t, handler, input)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestSearchAttemptAuditFeatureAndRouteFollowServiceConfiguration(t *testing.T) {
	t.Parallel()

	service := &fakeSearchAttemptAudit{}
	enabled := newSearchAttemptAuditTestHandler(t, service, 0)
	bootstrap := postProto(
		t,
		enabled,
		"/api/v1/system/bootstrap",
		&opensplunkv1.GetSystemBootstrapRequest{},
	)
	var enabledResponse opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrap, &enabledResponse)
	if !slices.Contains(
		enabledResponse.GetFeatures(),
		opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT,
	) {
		t.Fatalf("enabled features = %v", enabledResponse.GetFeatures())
	}

	disabled := newTestHandler(t, Config{
		SearchJobs:           &fakeSearchJobs{},
		Indexes:              fakeIndexCatalog{},
		BrowserAuthenticator: testSearchInspectionAuthenticator(t),
		WebUI:                testUI(),
		TenantID:             browserGateTenantID,
		OwnerID:              browserGateOwnerID,
		Bootstrap: BootstrapConfig{Features: []opensplunkv1.ServerFeature{
			opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT,
		}},
	})
	disabledBootstrap := postProto(
		t,
		disabled,
		"/api/v1/system/bootstrap",
		&opensplunkv1.GetSystemBootstrapRequest{},
	)
	var disabledResponse opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, disabledBootstrap, &disabledResponse)
	if slices.Contains(
		disabledResponse.GetFeatures(),
		opensplunkv1.ServerFeature_SERVER_FEATURE_SEARCH_ATTEMPT_AUDIT,
	) {
		t.Fatalf("disabled features = %v", disabledResponse.GetFeatures())
	}
	route := postAuthenticatedSearchAttemptAudit(
		t,
		disabled,
		&opensplunkv1.ListSearchAttemptAuditEventsRequest{},
	)
	if route.Code != http.StatusNotFound {
		t.Fatalf("disabled route status = %d, body = %s", route.Code, route.Body.String())
	}
}

func TestSearchAttemptAuditErrorsAreSanitized(t *testing.T) {
	t.Parallel()

	secret := "SELECT spl, generated_sql, result FROM private_searches"
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid cursor", err: errors.Join(searchaudit.ErrInvalidCursor, errors.New(secret)), want: http.StatusBadRequest},
		{name: "corrupt", err: errors.Join(searchaudit.ErrCorrupt, errors.New(secret)), want: http.StatusServiceUnavailable},
		{name: "unknown", err: errors.New(secret), want: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeSearchAttemptAudit{err: test.err}
			handler := newSearchAttemptAuditTestHandler(t, service, 0)
			response := postAuthenticatedSearchAttemptAudit(
				t,
				handler,
				&opensplunkv1.ListSearchAttemptAuditEventsRequest{},
			)
			if response.Code != test.want {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if bytes.Contains(response.Body.Bytes(), []byte(secret)) {
				t.Fatalf("response leaked storage detail: %q", response.Body.String())
			}
		})
	}
}

func TestSearchAttemptAuditCodecEnforcesTwoMiBResponseCeiling(t *testing.T) {
	t.Parallel()

	if maximumSearchAttemptAuditResponseBytes != 2<<20 {
		t.Fatalf(
			"response ceiling = %d, want %d",
			maximumSearchAttemptAuditResponseBytes,
			2<<20,
		)
	}
	released := false
	response := httptest.NewRecorder()
	err := newSerializedSearchAttemptAuditListCodec().Encode(
		response,
		&serializedSearchAttemptAuditListResponse{
			message: &opensplunkv1.ListSearchAttemptAuditEventsResponse{
				Events: []*opensplunkv1.SearchAttemptAuditEvent{{
					OwnerId: strings.Repeat(
						"x",
						maximumSearchAttemptAuditResponseBytes+1,
					),
				}},
			},
			ctx: context.Background(),
			release: func() {
				released = true
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "exceeds its byte limit") {
		t.Fatalf("oversized encode error = %v", err)
	}
	if !released || response.Body.Len() != 0 {
		t.Fatalf(
			"oversized encode = released %t/body bytes %d",
			released,
			response.Body.Len(),
		)
	}
}

func newSearchAttemptAuditTestHandler(
	t *testing.T,
	service SearchAttemptAuditEvents,
	maximumPageSize uint32,
) *Handler {
	t.Helper()
	return newTestHandler(t, Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		SearchAttemptAuditEvents:   service,
		BrowserAuthenticator:       testSearchInspectionAuthenticator(t),
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		MaximumPageSize:            maximumPageSize,
		MaximumConcurrentResponses: 2,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
}

func postAuthenticatedSearchAttemptAudit(
	t *testing.T,
	handler http.Handler,
	message proto.Message,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal search attempt audit request: %v", err)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		searchAttemptAuditListPath,
		bytes.NewReader(payload),
	)
	request.Host = "example.com"
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Authorization", "Bearer "+adminIntegrationBearerToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
