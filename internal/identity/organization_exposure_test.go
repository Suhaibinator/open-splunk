package identity

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type organizationMutationCase struct {
	method string
	route  string
	input  func(pb.OrganizationRole) proto.Message
}

func organizationMutationCases() []organizationMutationCase {
	return []organizationMutationCase{
		{http.MethodPost, "/organizations/members/add", func(role pb.OrganizationRole) proto.Message { return &pb.AddUserToOrganizationRequest{Role: role} }},
		{http.MethodPost, "/organizations/members/add-by-identifier", func(role pb.OrganizationRole) proto.Message {
			return &pb.AddUserToOrganizationByIdentifierRequest{Role: role}
		}},
		{http.MethodPost, "/organizations/members/invite", func(role pb.OrganizationRole) proto.Message { return &pb.InviteUserToOrganizationRequest{Role: role} }},
		{http.MethodPost, "/admin/organizations/members/add", func(role pb.OrganizationRole) proto.Message { return &pb.AdminAddUserToOrganizationRequest{Role: role} }},
		{http.MethodPost, "/admin/organizations/members/add-by-identifier", func(role pb.OrganizationRole) proto.Message {
			return &pb.AdminAddUserToOrganizationByIdentifierRequest{Role: role}
		}},
		{http.MethodPut, "/organizations/members/update-role", func(role pb.OrganizationRole) proto.Message {
			return &pb.UpdateUserRoleInOrganizationRequest{Role: role}
		}},
		{http.MethodPut, "/admin/organizations/members/update-role", func(role pb.OrganizationRole) proto.Message {
			return &pb.AdminUpdateUserRoleInOrganizationRequest{Role: role}
		}},
	}
}

func TestOrganizationExposureRoleVocabulary(t *testing.T) {
	for _, mutation := range organizationMutationCases() {
		t.Run(mutation.route, func(t *testing.T) {
			for _, role := range []pb.OrganizationRole{0, 1, 2, 23, 24, 34, 43, 44, 99} {
				body, err := proto.Marshal(mutation.input(role))
				if err != nil {
					t.Fatal(err)
				}
				called := false
				downstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called = true
					actual, readErr := io.ReadAll(r.Body)
					if readErr != nil || !bytes.Equal(actual, body) {
						t.Errorf("native payload changed: %v", readErr)
					}
					w.WriteHeader(http.StatusNoContent)
				})
				request := httptest.NewRequestWithContext(t.Context(), mutation.method, Prefix+mutation.route, bytes.NewReader(body))
				record := httptest.NewRecorder()
				organizationExposureGuard(downstream, downstream).ServeHTTP(record, request)
				allowed := role == 1 || role == 23 || role == 44
				expected := http.StatusBadRequest
				if allowed {
					expected = http.StatusNoContent
				}
				if record.Code != expected || called != allowed {
					t.Fatalf("role %d status %d called %v, want %d", role, record.Code, called, expected)
				}
			}
		})
	}
}

func TestOrganizationExposureCreationAndStreamBounds(t *testing.T) {
	called := false
	downstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { called = true; w.WriteHeader(http.StatusNoContent) })
	guard := organizationExposureGuard(downstream, downstream)
	for _, route := range []string{"/organizations", "/organizations/", "/admin/organizations/create", "/admin/organizations/create/"} {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, Prefix+route, nil)
		record := httptest.NewRecorder()
		guard.ServeHTTP(record, request)
		if record.Code != http.StatusNotFound || called {
			t.Fatalf("native creation exposed: %s %d", route, record.Code)
		}
	}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, Prefix+"/organizations/members/add", bytes.NewReader(make([]byte, organizationMutationBodyLimit+1)))
	request.ContentLength = -1
	record := httptest.NewRecorder()
	guard.ServeHTTP(record, request)
	if record.Code != http.StatusRequestEntityTooLarge || called {
		t.Fatalf("unbounded mutation accepted: %d", record.Code)
	}
	request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, Prefix+"/organizations/members/add", bytes.NewReader([]byte{0xff}))
	record = httptest.NewRecorder()
	guard.ServeHTTP(record, request)
	if record.Code != http.StatusBadRequest || called {
		t.Fatalf("malformed protobuf accepted: %d", record.Code)
	}
	for _, route := range []string{"/organizations/create-with-owner", "/user/me"} {
		request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, Prefix+route, nil)
		record = httptest.NewRecorder()
		guard.ServeHTTP(record, request)
		if record.Code != http.StatusNoContent {
			t.Fatalf("unrelated route blocked: %s", route)
		}
	}
}

func TestRuntimeOrganizationMutationBoundary(t *testing.T) {
	runtime, pool, cfg := newPostgresIdentityRuntime(t)
	users := []pm.User{
		{Username: "boundarysite", Email: "boundarysite@example.com", UserType: pb.UserType_USER_TYPE_ADMIN_TIER_10},
		{Username: "boundaryowner", Email: "boundaryowner@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0},
		{Username: "boundaryother", Email: "boundaryother@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0},
	}
	for i := range users {
		if err := pool.Create(&users[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	tokens, err := token.NewTokenService(runtime.Persistence(), token.TokenServiceConfig{SessionLifetime: func() time.Duration { return time.Hour }, SecurityLifetime: func() time.Duration { return time.Hour }, SessionTokenSecret: cfg.SessionTokenSecret.StringValue()}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	sessions := make([]string, 2)
	for i := range sessions {
		sessions[i], err = tokens.GenerateSessionToken(t.Context(), users[i].ID, token.SessionAuthMethod{Provider: pb.AuthProvider_AUTH_PROVIDER_OAUTH, OAuthProviderSlug: "google"})
		if err != nil {
			t.Fatal(err)
		}
	}
	call := func(method, route string, input proto.Message, session string) *httptest.ResponseRecorder {
		t.Helper()
		body, marshalErr := proto.Marshal(input)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		request := httptest.NewRequestWithContext(t.Context(), method, Prefix+route, bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/x-protobuf")
		request.AddCookie(&http.Cookie{Name: runtime.cookieName, Value: session, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
		record := httptest.NewRecorder()
		runtime.Handler().ServeHTTP(record, request)
		return record
	}
	for _, mutation := range organizationMutationCases() {
		result := call(mutation.method, mutation.route, mutation.input(24), sessions[0])
		if result.Code != http.StatusBadRequest {
			t.Fatalf("site bypassed role vocabulary on %s: %d %s", mutation.route, result.Code, result.Body.String())
		}
	}
	for _, route := range []string{"/organizations", "/admin/organizations/create"} {
		if result := call(http.MethodPost, route, &pb.CreateOrganizationRequest{}, sessions[0]); result.Code != http.StatusNotFound {
			t.Fatalf("site bypassed explicit ownership: %s %d", route, result.Code)
		}
	}
	actorCtx := scontext.WithUser[sm.UserIdType](t.Context(), &sm.User{ID: users[0].ID, UserType: users[0].UserType})
	actorCtx = scontext.WithUserID[sm.UserIdType, sm.User](actorCtx, users[0].ID)
	org, err := identityNativeTransaction(runtime, actorCtx, func(ctx context.Context) (*organization.OrganizationResponse, error) {
		return runtime.CreateOrganizationWithInitialMemberships(ctx, &sm.Organization{Name: "Boundary", Slug: "boundary"}, []InitialMembership{{UserID: users[1].ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
	})
	if err != nil {
		t.Fatal(err)
	}
	update := &pb.AdminUpdateUserRoleInOrganizationRequest{OrganizationId: org.ID, UserId: users[1].ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0}
	result := call(http.MethodPut, "/admin/organizations/members/update-role", update, sessions[0])
	if result.Code != http.StatusConflict {
		t.Fatalf("sole Owner update status %d: %s", result.Code, result.Body.String())
	}
	role, member, err := runtime.EffectiveRole(t.Context(), users[1].ID, org.ID)
	if err != nil || !member || role != 44 {
		t.Fatalf("failed update changed Owner: %d %v %v", role, member, err)
	}
	result = call(http.MethodPost, "/admin/organizations/members/add", &pb.AdminAddUserToOrganizationRequest{OrganizationId: org.ID, UserId: users[2].ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}, sessions[0])
	if result.Code != http.StatusOK {
		t.Fatalf("second Owner add status %d: %s", result.Code, result.Body.String())
	}
	result = call(http.MethodPut, "/admin/organizations/members/update-role", update, sessions[1])
	if result.Code != http.StatusForbidden {
		t.Fatalf("regular Owner crossed site boundary: %d", result.Code)
	}
	result = call(http.MethodPut, "/admin/organizations/members/update-role", update, sessions[0])
	if result.Code != http.StatusOK {
		t.Fatalf("nonlast Owner update status %d: %s", result.Code, result.Body.String())
	}
	var response pb.AdminUpdateUserRoleInOrganizationResponse
	if err := proto.Unmarshal(result.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.GetOrganizationUser().GetExplicitRole() != pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0 {
		t.Fatalf("native admin response role %v", response.GetOrganizationUser().GetExplicitRole())
	}
	role, member, err = runtime.EffectiveRole(t.Context(), users[1].ID, org.ID)
	if err != nil || !member || role != 23 {
		t.Fatalf("successful update did not commit: %d %v %v", role, member, err)
	}
	role, member, err = runtime.EffectiveRole(t.Context(), users[2].ID, org.ID)
	if err != nil || !member || role != 44 {
		t.Fatalf("remaining Owner changed: %d %v %v", role, member, err)
	}
}
