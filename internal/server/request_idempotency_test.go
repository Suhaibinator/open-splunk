package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
)

func TestMutationIntentUsesStableActorAcrossCredentialRotation(t *testing.T) {
	principal := browserGatePrincipal(
		t, "tenant-stable-actor", "owner-stable-actor", auth.BrowserRoleAdministrator,
	)
	var authenticatedCredentials []string
	handler := &apiHandler{
		browserAuthenticator: &recordingBrowserAuthenticator{
			fn: func(_ context.Context, credential []byte) (auth.BrowserPrincipal, error) {
				authenticatedCredentials = append(authenticatedCredentials, string(credential))
				return principal, nil
			},
		},
		tenantID: "tenant-stable-actor",
	}
	requestID := "rotated credential request 01"
	canonical := &opensplunk.CreateAppRequest{
		Definition: &opensplunk.AppDefinition{Slug: "stable-actor-app"},
	}
	credentials := []string{
		"credential-before-rotation-0123456789",
		"credential-after-rotation-0123456789",
	}
	intents := make([]requestidempotency.Intent, 0, len(credentials))
	for _, credential := range credentials {
		request := httptest.NewRequest(http.MethodPost, "/api/apps/create", nil)
		request.Header.Set("Authorization", "Bearer "+credential)
		response := httptest.NewRecorder()
		authenticated, _, ok := handler.authenticateBrowser(response, request)
		if !ok {
			t.Fatalf("authenticate credential %q: status %d body %s", credential, response.Code, response.Body.String())
		}
		if authenticated.Header.Get("Authorization") != "" {
			t.Fatal("authenticated request retained credential")
		}
		intent, err := handler.mutationIntent(
			authenticated.Context(), requestidempotency.RouteCreateApp, &requestID, canonical,
		)
		if err != nil || intent == nil {
			t.Fatalf("mutationIntent(%q) = (%+v, %v)", credential, intent, err)
		}
		intents = append(intents, *intent)
	}
	if len(authenticatedCredentials) != 2 ||
		authenticatedCredentials[0] == authenticatedCredentials[1] {
		t.Fatalf("authenticated credentials = %q", authenticatedCredentials)
	}
	if intents[0] != intents[1] || intents[0].ActorID != "owner-stable-actor" ||
		intents[0].ActorKind != "browser" {
		t.Fatalf("credential rotation changed logical actor intent: %#v != %#v", intents[0], intents[1])
	}
}
