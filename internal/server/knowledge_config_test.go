package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
)

func knowledgeConfigBase(t *testing.T) Config {
	t.Helper()
	return Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		SavedSearches:              &fakeSavedSearches{},
		WebUI:                      testUI(),
		OwnerID:                    knowledgeBoundaryOwnerID,
		TenantID:                   knowledgeBoundaryTenantID,
		AdministrativeAllowedHosts: []string{"example.com"},
		BrowserAuthenticator: &knowledgeBoundaryAuthenticator{
			principal: knowledgeBoundaryPrincipal(t, auth.BrowserRoleAdministrator),
		},
	}
}

func TestKnowledgeConfigRequiresOneCompleteDependencyUnit(t *testing.T) {
	t.Parallel()

	setters := []struct {
		name string
		set  func(*Config)
	}{
		{name: "catalog only", set: func(config *Config) { config.KnowledgeCatalog = &knowledgeHTTPCatalog{} }},
		{name: "writer only", set: func(config *Config) { config.KnowledgeWriter = &knowledgeHTTPWriter{} }},
		{name: "apps only", set: func(config *Config) { config.KnowledgeApps = knowledgeHTTPApps() }},
		{name: "attempts only", set: func(config *Config) { config.KnowledgeAttempts = &knowledgeBoundaryAppender{} }},
	}
	for _, test := range setters {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := knowledgeConfigBase(t)
			test.set(&config)
			_, err := NewHandler(config)
			if err == nil || !strings.Contains(err.Error(), "knowledge management dependencies must be configured together") {
				t.Fatalf("NewHandler error=%v", err)
			}
		})
	}

	t.Run("complete unit requires authentication", func(t *testing.T) {
		config := knowledgeConfigBase(t)
		config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
		config.KnowledgeWriter = &knowledgeHTTPWriter{}
		config.KnowledgeApps = knowledgeHTTPApps()
		config.KnowledgeAttempts = &knowledgeBoundaryAppender{}
		config.BrowserAuthenticator = nil
		_, err := NewHandler(config)
		if err == nil || !strings.Contains(err.Error(), "administrative services require browser authentication") {
			t.Fatalf("NewHandler error=%v", err)
		}
	})
}

func TestKnowledgeConfigNormalizesTypedNilDependencies(t *testing.T) {
	t.Parallel()

	var catalog *knowledgeHTTPCatalog
	var writer *knowledgeHTTPWriter
	var apps *knowledgeHTTPAppCatalog
	var attempts *knowledgeBoundaryAppender
	config := knowledgeConfigBase(t)
	config.KnowledgeCatalog = catalog
	config.KnowledgeWriter = writer
	config.KnowledgeApps = apps
	config.KnowledgeAttempts = attempts
	if _, err := NewHandler(config); err != nil {
		t.Fatalf("all typed nil dependencies: %v", err)
	}

	config.KnowledgeWriter = &knowledgeHTTPWriter{}
	_, err := NewHandler(config)
	if err == nil || !strings.Contains(err.Error(), "knowledge management dependencies must be configured together") {
		t.Fatalf("partial typed nil error=%v", err)
	}
}

func TestConfiguredKnowledgeManagementRemainsPubliclyUnregisteredAndUnadvertised(
	t *testing.T,
) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	apps := knowledgeHTTPApps()
	writer := &knowledgeHTTPWriter{}
	catalog := &knowledgeHTTPCatalog{}
	config := knowledgeConfigBase(t)
	config.KnowledgeCatalog = catalog
	config.KnowledgeWriter = writer
	config.KnowledgeApps = apps
	config.KnowledgeAttempts = appender
	handler, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, knowledgeObjectsCreatePath, strings.NewReader("unread secret body"))
	request.Host = "example.com"
	request.Header.Set("Origin", "http://example.com")
	request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || writer.callCounts() != [4]int{} ||
		apps.callCount() != 0 || len(appender.snapshot()) != 0 {
		t.Fatalf("knowledge route status=%d body=%q writer=%v apps=%d attempts=%+v", response.Code, response.Body.String(), writer.callCounts(), apps.callCount(), appender.snapshot())
	}

	bootstrap := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	if bootstrap.Code != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%q", bootstrap.Code, bootstrap.Body.String())
	}
	decoded := &opensplunkv1.GetSystemBootstrapResponse{}
	unmarshalResponse(t, bootstrap, decoded)
	for _, feature := range decoded.GetFeatures() {
		if feature == opensplunkv1.ServerFeature_SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS {
			t.Fatalf("knowledge feature advertised in %v", decoded.GetFeatures())
		}
	}
}

func TestKnowledgeHTTPPrincipalIdentityMismatchIsJournaledBeforeDecode(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	handler, httpHandler := newKnowledgeHTTPHandler(t, auth.BrowserRoleAdministrator, &knowledgeHTTPCatalog{}, &knowledgeHTTPWriter{}, knowledgeHTTPApps(), appender)
	handler.tenantID = "different-tenant"
	body := newKnowledgeBoundaryObservedBody("not protobuf", nil)
	request := knowledgeBoundaryRequest(context.Background(), http.MethodPost, knowledgeObjectsCreatePath, body)
	request.Host = "example.com"
	request.Header.Set("Origin", "http://example.com")
	response := httptest.NewRecorder()
	httpHandler.ServeHTTP(response, request)
	calls := appender.snapshot()
	if response.Code != http.StatusForbidden || body.reads() != 0 || len(calls) != 1 ||
		calls[0].definition.Reason != knowledgeattemptaudit.ReasonNotFoundOrForbidden ||
		calls[0].definition.AuthorizedContext != nil {
		t.Fatalf("status=%d body=%q reads=%d attempts=%+v", response.Code, response.Body.String(), body.reads(), calls)
	}
}
