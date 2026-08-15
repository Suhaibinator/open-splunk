package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgepreview"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
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

type dormantPreviewSearches struct{}

func (dormantPreviewSearches) AcquireExecutionFor(
	context.Context,
	searchjobs.AccessScope,
	string,
) (searchjobs.ResultLease, searchjobs.ExecutionSnapshot, error) {
	panic("configuration test must not acquire a retained execution")
}

type dormantPreviewCompiler struct{}

func (dormantPreviewCompiler) CompilePreview(
	context.Context,
	searchjobs.ExecutionSnapshot,
	knowledgeprogram.Program,
) (clickhouse.CompiledQuery, error) {
	panic("configuration test must not compile")
}

type dormantPreviewExecutor struct{}

func (dormantPreviewExecutor) Execute(
	context.Context,
	clickhouse.CompiledQuery,
	searchjobs.ResultSink,
) error {
	panic("configuration test must not execute")
}

func newReadyKnowledgePreview(
	t *testing.T,
	writer *knowledgecatalog.Writer,
) *knowledgepreview.Service {
	t.Helper()
	service, err := knowledgepreview.NewService(knowledgepreview.Config{
		Searches: dormantPreviewSearches{},
		Writer:   writer,
		Compiler: dormantPreviewCompiler{},
		Executor: dormantPreviewExecutor{},
	})
	if err != nil {
		t.Fatalf("knowledgepreview.NewService(): %v", err)
	}
	return service
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
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, body)
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
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, body)
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

func TestKnowledgePreviewRequiresCompleteFamilyAndRegistersOnlyWhenReady(t *testing.T) {
	writer := newReadyKnowledgeWriter(t)
	preview := newReadyKnowledgePreview(t, writer)

	for _, test := range []struct {
		name string
		drop func(*Config)
	}{
		{name: "catalog", drop: func(config *Config) { config.KnowledgeCatalog = nil }},
		{name: "writer", drop: func(config *Config) { config.KnowledgeWriter = nil }},
		{name: "apps", drop: func(config *Config) { config.KnowledgeApps = nil }},
		{name: "attempt journal", drop: func(config *Config) { config.KnowledgeAttempts = nil }},
	} {
		t.Run("missing "+test.name, func(t *testing.T) {
			config := knowledgeConfigBase(t)
			config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
			config.KnowledgeWriter = writer
			config.KnowledgeApps = knowledgeHTTPApps()
			config.KnowledgeAttempts = &knowledgeBoundaryAppender{}
			config.KnowledgePreview = preview
			test.drop(&config)
			if handler, err := NewHandler(config); err == nil || handler != nil {
				t.Fatalf("NewHandler(missing %s) = (%#v, %v), want no route-bearing handler", test.name, handler, err)
			}
		})
	}

	t.Run("typed nil is absent", func(t *testing.T) {
		var typedNil *knowledgepreview.Service
		config := knowledgeConfigBase(t)
		config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
		config.KnowledgeWriter = writer
		config.KnowledgeApps = knowledgeHTTPApps()
		config.KnowledgeAttempts = &knowledgeBoundaryAppender{}
		config.KnowledgePreview = typedNil
		handler, err := NewHandler(config)
		if err != nil {
			t.Fatal(err)
		}
		body := newKnowledgeBoundaryObservedBody("unread Preview body", nil)
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, knowledgeObjectsPreviewPath, body)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound || body.reads() != 0 {
			t.Fatalf("typed-nil Preview route status=%d reads=%d", response.Code, body.reads())
		}
	})

	t.Run("complete ready family", func(t *testing.T) {
		config := knowledgeConfigBase(t)
		config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
		config.KnowledgeWriter = writer
		config.KnowledgeApps = knowledgeHTTPApps()
		attempts := &knowledgeBoundaryAppender{}
		config.KnowledgeAttempts = attempts
		config.KnowledgePreview = preview
		handler, err := NewHandler(config)
		if err != nil {
			t.Fatal(err)
		}
		body := newKnowledgeBoundaryObservedBody("\xff", nil)
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, knowledgeObjectsPreviewPath, body)
		request.Host = "example.com"
		request.Header.Set("Origin", "http://example.com")
		request.Header.Set("Sec-Fetch-Site", "same-origin")
		request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
		request.Header.Set("Content-Type", "application/x-protobuf")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || body.reads() == 0 ||
			len(attempts.snapshot()) != 1 {
			t.Fatalf("ready Preview route status=%d reads=%d attempts=%+v",
				response.Code, body.reads(), attempts.snapshot())
		}
	})

	t.Run("non-administrator is journaled before body read", func(t *testing.T) {
		attempts := &knowledgeBoundaryAppender{}
		config := knowledgeConfigBase(t)
		config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
		config.KnowledgeWriter = writer
		config.KnowledgeApps = knowledgeHTTPApps()
		config.KnowledgeAttempts = attempts
		config.KnowledgePreview = preview
		config.BrowserAuthenticator = &knowledgeBoundaryAuthenticator{
			principal: knowledgeBoundaryPrincipal(t, auth.BrowserRoleUser),
		}
		handler, err := NewHandler(config)
		if err != nil {
			t.Fatal(err)
		}
		body := newKnowledgeBoundaryObservedBody("unread retained job authority", nil)
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, knowledgeObjectsPreviewPath, body)
		request.Host = "example.com"
		request.Header.Set("Origin", "http://example.com")
		request.Header.Set("Sec-Fetch-Site", "same-origin")
		request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
		request.Header.Set("Content-Type", "application/x-protobuf")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		calls := attempts.snapshot()
		if response.Code != http.StatusForbidden || body.reads() != 0 ||
			len(calls) != 1 ||
			calls[0].definition.Action != knowledgeattemptaudit.ActionPreview ||
			calls[0].definition.Reason != knowledgeattemptaudit.ReasonNotAdministrator {
			t.Fatalf("user Preview status=%d reads=%d attempts=%+v",
				response.Code, body.reads(), calls)
		}
	})

	t.Run("unready service is rejected", func(t *testing.T) {
		config := knowledgeConfigBase(t)
		config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
		config.KnowledgeWriter = writer
		config.KnowledgeApps = knowledgeHTTPApps()
		config.KnowledgeAttempts = &knowledgeBoundaryAppender{}
		config.KnowledgePreview = &knowledgepreview.Service{}
		if handler, err := NewHandler(config); err == nil || handler != nil {
			t.Fatalf("NewHandler(unready Preview) = (%#v, %v), want rejection", handler, err)
		}
	})
}

func TestKnowledgeFeatureRequiresCompleteRuntimeFamily(t *testing.T) {
	complete := func(t *testing.T) Config {
		t.Helper()
		writer := newReadyKnowledgeWriter(t)
		config := knowledgeConfigBase(t)
		config.SearchJobs = &knowledgeAdmissionSearchJobs{
			fakeSearchJobs:   &fakeSearchJobs{},
			enabled:          true,
			executionEnabled: true,
		}
		config.AppCatalog = activeHistoryRerunAppCatalog()
		config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
		config.KnowledgeWriter = writer
		config.KnowledgeApps = knowledgeHTTPApps()
		config.KnowledgeAttempts = &knowledgeBoundaryAppender{}
		config.KnowledgePreview = newReadyKnowledgePreview(t, writer)
		config.SearchInspections = &fakeSearchInspections{}
		config.SearchHistory = &fakeSearchHistory{}
		config.Exports = &fakeExports{}
		config.SearchTimelines = &fakeSearchTimelines{maximum: 100}
		config.SearchFields = &fakeSearchFields{
			maximumFields:  100,
			maximumPage:    10,
			maximumSummary: 10,
		}
		config.SearchSuggestions = &fakeSearchSuggestions{maximum: 10}
		return config
	}
	advertised := func(t *testing.T, config Config) bool {
		t.Helper()
		handler, err := NewHandler(config)
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}
		response := postProto(
			t,
			handler,
			"/api/v1/system/bootstrap",
			&opensplunkv1.GetSystemBootstrapRequest{},
		)
		if response.Code != http.StatusOK {
			t.Fatalf("bootstrap status=%d body=%q", response.Code, response.Body.String())
		}
		decoded := &opensplunkv1.GetSystemBootstrapResponse{}
		unmarshalResponse(t, response, decoded)
		return slices.Contains(
			decoded.GetFeatures(),
			opensplunkv1.ServerFeature_SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS,
		)
	}

	if !advertised(t, complete(t)) {
		t.Fatal("complete knowledge runtime family was not advertised")
	}

	for _, test := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "management catalog", mutate: func(config *Config) { config.KnowledgeCatalog = nil }},
		{name: "management writer", mutate: func(config *Config) { config.KnowledgeWriter = nil }},
		{name: "management apps", mutate: func(config *Config) { config.KnowledgeApps = nil }},
		{name: "management attempt journal", mutate: func(config *Config) { config.KnowledgeAttempts = nil }},
		{name: "preview", mutate: func(config *Config) { config.KnowledgePreview = nil }},
		{name: "admission", mutate: func(config *Config) {
			config.SearchJobs.(*knowledgeAdmissionSearchJobs).enabled = false
		}},
		{name: "execution", mutate: func(config *Config) {
			config.SearchJobs.(*knowledgeAdmissionSearchJobs).executionEnabled = false
		}},
		{name: "inspection", mutate: func(config *Config) { config.SearchInspections = nil }},
		{name: "history", mutate: func(config *Config) { config.SearchHistory = nil }},
		{name: "export", mutate: func(config *Config) { config.Exports = nil }},
		{name: "timeline", mutate: func(config *Config) { config.SearchTimelines = nil }},
		{name: "field catalog and summary", mutate: func(config *Config) { config.SearchFields = nil }},
		{name: "suggestions", mutate: func(config *Config) { config.SearchSuggestions = nil }},
	} {
		t.Run("missing "+test.name, func(t *testing.T) {
			config := complete(t)
			test.mutate(&config)
			handler, err := NewHandler(config)
			if err != nil {
				// Partial management and Preview configuration is rejected before a
				// route-bearing handler can advertise anything.
				if handler != nil || !strings.Contains(err.Error(), "knowledge") {
					t.Fatalf("NewHandler = (%#v, %v)", handler, err)
				}
				return
			}
			response := postProto(
				t,
				handler,
				"/api/v1/system/bootstrap",
				&opensplunkv1.GetSystemBootstrapRequest{},
			)
			decoded := &opensplunkv1.GetSystemBootstrapResponse{}
			unmarshalResponse(t, response, decoded)
			if slices.Contains(
				decoded.GetFeatures(),
				opensplunkv1.ServerFeature_SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS,
			) {
				t.Fatalf("partial family advertised knowledge: %v", decoded.GetFeatures())
			}
		})
	}
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
		request := httptest.NewRequestWithContext(t.Context(), test.method, test.path, body)
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
	request := knowledgeBoundaryRequest(context.Background(), knowledgeObjectsCreatePath, body)
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
