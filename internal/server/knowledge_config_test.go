package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

type embeddingKnowledgeWriterOverride struct {
	*knowledgecatalog.Writer
}

func (*embeddingKnowledgeWriterOverride) Create(
	context.Context,
	knowledgecatalog.WriteScope,
	*opensplunkv1.CreateKnowledgeObjectRequest,
) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
	return nil, nil
}

type readyKnowledgeWriterAuditAppender struct{}

func (readyKnowledgeWriterAuditAppender) AppendInTransaction(
	context.Context,
	*gorm.DB,
	string,
	audit.SuccessfulEvent,
) (audit.Event, error) {
	return audit.Event{}, control.ErrInvalidArgument
}

func newReadyKnowledgeWriter(t *testing.T) *knowledgecatalog.Writer {
	t.Helper()
	database, err := control.Open(
		t.Context(),
		filepath.Join(t.TempDir(), "control.sqlite"),
	)
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close ready knowledge writer database: %v", err)
		}
	})
	writer, err := knowledgecatalog.NewWriter(
		database,
		readyKnowledgeWriterAuditAppender{},
		knowledgecatalog.WriterOptions{},
	)
	if err != nil {
		t.Fatalf("knowledgecatalog.NewWriter(): %v", err)
	}
	return writer
}

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
		writer := newReadyKnowledgeWriter(t)
		config := knowledgeConfigBase(t)
		config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
		config.KnowledgeWriter = writer
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

func TestKnowledgeConfigRequiresExactCatalogWriter(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		writer KnowledgeWriter
	}{
		{name: "ordinary interface implementation", writer: &knowledgeHTTPWriter{}},
		{name: "embedding override", writer: &embeddingKnowledgeWriterOverride{Writer: &knowledgecatalog.Writer{}}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := knowledgeConfigBase(t)
			config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
			config.KnowledgeWriter = test.writer
			config.KnowledgeApps = knowledgeHTTPApps()
			config.KnowledgeAttempts = &knowledgeBoundaryAppender{}
			_, err := NewHandler(config)
			if err == nil || !strings.Contains(err.Error(), "requires the concrete catalog writer") {
				t.Fatalf("NewHandler error=%v", err)
			}
		})
	}

	writer := newReadyKnowledgeWriter(t)
	config := knowledgeConfigBase(t)
	config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
	config.KnowledgeWriter = writer
	config.KnowledgeApps = knowledgeHTTPApps()
	config.KnowledgeAttempts = &knowledgeBoundaryAppender{}
	if _, err := NewHandler(config); err != nil {
		t.Fatalf("concrete catalog writer: %v", err)
	}
}

func TestKnowledgeManagementRoutesFollowCompleteConfigurationAndRemainUnadvertised(
	t *testing.T,
) {
	t.Parallel()

	t.Run("configured exact routes", func(t *testing.T) {
		writer := newReadyKnowledgeWriter(t)
		appender := &knowledgeBoundaryAppender{}
		apps := knowledgeHTTPApps()
		config := knowledgeConfigBase(t)
		authenticator := config.BrowserAuthenticator.(*knowledgeBoundaryAuthenticator)
		config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
		config.KnowledgeWriter = writer
		config.KnowledgeApps = apps
		config.KnowledgeAttempts = appender
		handler, err := NewHandler(config)
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}

		for _, path := range []string{
			knowledgeObjectsCreatePath,
			knowledgeObjectsGetPath,
			knowledgeObjectsListPath,
			knowledgeObjectsDependenciesPath,
			knowledgeObjectsDependentsPath,
			knowledgeObjectsValidatePath,
			knowledgeObjectsUpdatePath,
			knowledgeObjectsSetStatePath,
			knowledgeObjectsDeletePath,
		} {
			body := newKnowledgeBoundaryObservedBody("\xff", nil)
			request := httptest.NewRequest(http.MethodPost, path, body)
			request.Host = "example.com"
			request.Header.Set("Origin", "http://example.com")
			request.Header.Set("Sec-Fetch-Site", "same-origin")
			request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
			request.Header.Set("Content-Type", "application/x-protobuf")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || body.reads() == 0 {
				t.Fatalf("route %q status=%d body=%q reads=%d", path, response.Code, response.Body.String(), body.reads())
			}
		}
		if authenticator.callCount() != 9 || apps.callCount() != 0 ||
			len(appender.snapshot()) != 9 {
			t.Fatalf("auth=%d apps=%d attempts=%+v", authenticator.callCount(), apps.callCount(), appender.snapshot())
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
	})

	t.Run("unconfigured exact routes", func(t *testing.T) {
		config := knowledgeConfigBase(t)
		authenticator := config.BrowserAuthenticator.(*knowledgeBoundaryAuthenticator)
		handler, err := NewHandler(config)
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}
		for _, path := range []string{
			knowledgeObjectsCreatePath,
			knowledgeObjectsGetPath,
			knowledgeObjectsListPath,
			knowledgeObjectsDependenciesPath,
			knowledgeObjectsDependentsPath,
			knowledgeObjectsValidatePath,
			knowledgeObjectsUpdatePath,
			knowledgeObjectsSetStatePath,
			knowledgeObjectsDeletePath,
		} {
			body := newKnowledgeBoundaryObservedBody("unread secret body", nil)
			request := httptest.NewRequest(http.MethodPost, path, body)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusNotFound || body.reads() != 0 {
				t.Fatalf("route %q status=%d body=%q reads=%d", path, response.Code, response.Body.String(), body.reads())
			}
		}
		if authenticator.callCount() != 0 {
			t.Fatalf("authentication calls=%d", authenticator.callCount())
		}
	})
}

func TestKnowledgeManagementProductionMiddlewareOrdering(t *testing.T) {
	t.Parallel()

	writer := newReadyKnowledgeWriter(t)
	appender := &knowledgeBoundaryAppender{}
	config := knowledgeConfigBase(t)
	authenticator := config.BrowserAuthenticator.(*knowledgeBoundaryAuthenticator)
	config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
	config.KnowledgeWriter = writer
	config.KnowledgeApps = knowledgeHTTPApps()
	config.KnowledgeAttempts = appender
	handler, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	for _, test := range []struct {
		name       string
		method     string
		path       string
		origin     string
		wantStatus int
	}{
		{name: "wrong method", method: http.MethodGet, path: knowledgeObjectsCreatePath, origin: "http://attacker.example", wantStatus: http.StatusMethodNotAllowed},
		{name: "unknown path", method: http.MethodPost, path: knowledgeObjectsCreatePath + "/typo", origin: "http://attacker.example", wantStatus: http.StatusNotFound},
		{name: "untrusted origin", method: http.MethodPost, path: knowledgeObjectsCreatePath, origin: "http://attacker.example", wantStatus: http.StatusForbidden},
	} {
		body := newKnowledgeBoundaryObservedBody("unread secret body", nil)
		request := httptest.NewRequest(test.method, test.path, body)
		request.Host = "example.com"
		request.Header.Set("Origin", test.origin)
		request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
		request.Header.Set("Content-Type", "application/x-protobuf")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.wantStatus || body.reads() != 0 {
			t.Fatalf("%s status=%d body=%q reads=%d", test.name, response.Code, response.Body.String(), body.reads())
		}
	}
	if authenticator.callCount() != 0 || len(appender.snapshot()) != 0 {
		t.Fatalf("auth=%d attempts=%+v", authenticator.callCount(), appender.snapshot())
	}
}

func TestKnowledgeManagementProductionRejectsNestedDefinitionUnknowns(
	t *testing.T,
) {
	t.Parallel()

	writer := newReadyKnowledgeWriter(t)
	appender := &knowledgeBoundaryAppender{}
	apps := knowledgeHTTPApps()
	config := knowledgeConfigBase(t)
	config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
	config.KnowledgeWriter = writer
	config.KnowledgeApps = apps
	config.KnowledgeAttempts = appender
	handler, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	create := &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition:      knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE),
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "production-create-unknown-0001",
	}
	addKnowledgeHTTPUnknown(create.GetDefinition().GetFieldAlias())
	update := &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: "ko-production-unknown",
		ExpectedVersion:   1,
		Definition:        knowledgeHTTPDefinition(opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE),
		ClientRequestId:   "production-update-unknown-0001",
	}
	addKnowledgeHTTPUnknown(update.GetDefinition().GetFieldAlias())
	for _, test := range []struct {
		path    string
		request proto.Message
	}{
		{path: knowledgeObjectsCreatePath, request: create},
		{path: knowledgeObjectsUpdatePath, request: update},
	} {
		response := knowledgeHTTPPost(t, handler, test.path, test.request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("route %q status=%d body=%q", test.path, response.Code, response.Body.String())
		}
	}
	calls := appender.snapshot()
	if apps.callCount() != 0 || len(calls) != 2 ||
		calls[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition ||
		calls[1].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition {
		t.Fatalf("apps=%d attempts=%+v", apps.callCount(), calls)
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
