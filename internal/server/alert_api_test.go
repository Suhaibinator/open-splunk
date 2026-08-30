package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/alertstore"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

type alertAPITestResolver struct{}

func TestAlertDefinitionProjectionMaterializesDefaultTTLs(t *testing.T) {
	t.Parallel()
	definition := alerts.Definition{
		Name: "Errors", Application: "search", SPL: "index=main",
		Earliest: "-5m", Latest: "now", Cron: "*/5 * * * *", Timezone: "UTC",
		SearchTimezone: "UTC", Condition: alerts.Condition{Operator: alerts.ConditionGreaterThan},
		SampleRows: alerts.DefaultSampleRows,
	}
	projected, err := alertDefinitionToProto(definition, "hooks.example.test", 1, time.Time{})
	if err != nil {
		t.Fatalf("alertDefinitionToProto() error = %v", err)
	}
	if projected.GetDispatchTtl() != "2p" || projected.GetWebhook().GetTtl() != "10p" {
		t.Fatalf("projected default TTLs = dispatch %q webhook %q", projected.GetDispatchTtl(), projected.GetWebhook().GetTtl())
	}
}

func (alertAPITestResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
}

type alertAPITestDoer func(*http.Request) (*http.Response, error)

func (doer alertAPITestDoer) Do(request *http.Request) (*http.Response, error) { return doer(request) }

type alertAPITestCoordinator struct {
	ownerID string
	alertID string
	run     alerts.RunSummary
	err     error
}

func (coordinator *alertAPITestCoordinator) RunNow(_ context.Context, ownerID, alertID string) (alerts.RunSummary, error) {
	coordinator.ownerID = ownerID
	coordinator.alertID = alertID
	return coordinator.run, coordinator.err
}

func TestAlertAPIProjectsOnlySafeMetadataAcrossLifecycle(t *testing.T) {
	handler, closeDatabase := newAlertAPIHandler(t, nil)
	defer closeDatabase()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/alerts/create", nil)

	created, err := handler.createAlert(request, &opensplunk.CreateAlertRequest{Definition: alertAPITestDefinition("Errors", "https://hooks.example.com/alerts")})
	if err != nil {
		t.Fatalf("createAlert() error = %v", err)
	}
	if created.GetSigningSecret() == "" || created.GetAlert().GetEnabled() || created.GetAlert().GetVersion() != 1 {
		t.Fatalf("create response = %+v", created)
	}
	assertSafeAlertProjection(t, created.GetAlert(), "hooks.example.com")

	got, err := handler.getAlert(request, &opensplunk.GetAlertRequest{AlertId: created.GetAlert().GetAlertId()})
	if err != nil {
		t.Fatalf("getAlert() error = %v", err)
	}
	assertSafeAlertProjection(t, got.GetAlert(), "hooks.example.com")

	enabled, err := handler.setAlertEnabled(request, &opensplunk.SetAlertEnabledRequest{
		AlertId: created.GetAlert().GetAlertId(), ExpectedVersion: created.GetAlert().GetVersion(), Enabled: true,
	})
	if err != nil || !enabled.GetAlert().GetEnabled() {
		t.Fatalf("setAlertEnabled() = %+v, %v", enabled, err)
	}

	updatedDefinition := alertAPITestDefinition("Updated errors", "https://receiver.example.net/new")
	updated, err := handler.updateAlert(request, &opensplunk.UpdateAlertRequest{
		AlertId: enabled.GetAlert().GetAlertId(), ExpectedVersion: enabled.GetAlert().GetVersion(), Definition: updatedDefinition,
	})
	if err != nil {
		t.Fatalf("updateAlert() error = %v", err)
	}
	if updated.GetAlert().GetDefinition().GetName() != "Updated errors" {
		t.Fatalf("updated alert = %+v", updated.GetAlert())
	}
	assertSafeAlertProjection(t, updated.GetAlert(), "receiver.example.net")

	rotated, err := handler.rotateAlertSecret(request, &opensplunk.RotateAlertSecretRequest{
		AlertId: updated.GetAlert().GetAlertId(), ExpectedVersion: updated.GetAlert().GetVersion(),
	})
	if err != nil || rotated.GetSigningSecret() == "" ||
		rotated.GetAlert().GetDefinition().GetWebhook().GetSecretGeneration() != 2 ||
		rotated.GetAlert().GetStatus().GetNextRunAt() == nil {
		t.Fatalf("rotateAlertSecret() = %+v, %v", rotated, err)
	}
	assertSafeAlertProjection(t, rotated.GetAlert(), "receiver.example.net")

	deleted, err := handler.deleteAlert(request, &opensplunk.DeleteAlertRequest{
		AlertId: rotated.GetAlert().GetAlertId(), ExpectedVersion: rotated.GetAlert().GetVersion(),
	})
	if err != nil || deleted.GetAlertId() != rotated.GetAlert().GetAlertId() {
		t.Fatalf("deleteAlert() = %+v, %v", deleted, err)
	}
}

func TestAlertAPICreateReplayNeverReissuesSigningSecret(t *testing.T) {
	handler, closeDatabase := newAlertAPIHandler(t, nil)
	defer closeDatabase()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/alerts/create", nil)
	clientRequestID := "request-123456789"
	input := &opensplunk.CreateAlertRequest{
		Definition:      alertAPITestDefinition("Errors", "https://hooks.example.com/alerts"),
		ClientRequestId: &clientRequestID,
	}

	created, err := handler.createAlert(request, input)
	if err != nil || created.GetAlert() == nil || created.GetSigningSecret() == "" || created.GetReplayed() {
		t.Fatalf("first createAlert() = %+v, %v", created, err)
	}
	replayed, err := handler.createAlert(request, input)
	if err != nil || replayed.GetAlert().GetAlertId() != created.GetAlert().GetAlertId() ||
		replayed.GetSigningSecret() != "" || !replayed.GetReplayed() {
		t.Fatalf("replayed createAlert() = %+v, %v", replayed, err)
	}

	input.Definition.Name = "Different intent"
	if _, err := handler.createAlert(request, input); err == nil || !strings.Contains(err.Error(), "request identity conflict") {
		t.Fatalf("changed replay error = %v", err)
	}
}

func TestAlertSummaryProjectionKeepsIndependentEvaluationAndDeliveryTimes(t *testing.T) {
	evaluatedAt := time.Date(2026, time.August, 29, 1, 3, 0, 0, time.UTC)
	deliveredAt := evaluatedAt.Add(-time.Minute)
	createdAt := deliveredAt.Add(-time.Hour)
	definition, _, err := alertDefinitionFromProto(alertAPITestDefinition("Errors", "https://hooks.example.com/alerts"), true)
	if err != nil {
		t.Fatalf("alertDefinitionFromProto() error = %v", err)
	}
	projected, err := alertSummaryToProto(alerts.AlertSummary{
		ID: "alert-1", OwnerID: "owner-1", Version: 4, State: alerts.AlertEnabled,
		Definition: definition, WebhookHostname: "hooks.example.com", SecretGeneration: 2,
		SecretRotatedAt: createdAt, LastOutcome: alerts.RunOverlapSkipped,
		LastEvaluatedAt: &evaluatedAt, LastDeliveredAt: &deliveredAt,
		CreatedAt: createdAt, UpdatedAt: evaluatedAt,
	})
	if err != nil {
		t.Fatalf("alertSummaryToProto() error = %v", err)
	}
	status := projected.GetStatus()
	if status.GetLastOutcome() != opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_SKIPPED_OVERLAP ||
		!status.GetLastEvaluatedAt().AsTime().Equal(evaluatedAt) ||
		!status.GetLastDeliveredAt().AsTime().Equal(deliveredAt) {
		t.Fatalf("status = %+v", status)
	}
}

func TestAlertDefinitionPreservesIndependentSearchAndScheduleTimezones(t *testing.T) {
	input := alertAPITestDefinition("Cross-zone", "https://hooks.example.com/alerts")
	searchTimezone := "Pacific/Chatham"
	input.Search.TimeRange.Timezone = &searchTimezone
	input.Timezone = "America/Los_Angeles"
	definition, _, err := alertDefinitionFromProto(input, true)
	if err != nil {
		t.Fatalf("alertDefinitionFromProto() error = %v", err)
	}
	if definition.SearchTimezone != searchTimezone || definition.Timezone != input.Timezone {
		t.Fatalf("definition timezones = search %q, schedule %q", definition.SearchTimezone, definition.Timezone)
	}
	projected, err := alertDefinitionToProto(definition, "hooks.example.com", 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if projected.GetSearch().GetTimeRange().GetTimezone() != searchTimezone || projected.GetTimezone() != input.Timezone {
		t.Fatalf("projected timezones = search %q, schedule %q", projected.GetSearch().GetTimeRange().GetTimezone(), projected.GetTimezone())
	}
}

func TestAlertDefinitionDefaultsOnlyAbsentWebhookSampleRows(t *testing.T) {
	t.Parallel()
	absent := alertAPITestDefinition("Absent sample", "https://hooks.example.com/alerts")
	absent.Webhook.SampleRowCount = nil
	definition, _, err := alertDefinitionFromProto(absent, true)
	if err != nil || definition.SampleRows != alerts.DefaultSampleRows {
		t.Fatalf("absent sample rows = %d, %v", definition.SampleRows, err)
	}

	explicitZero := alertAPITestDefinition("Zero sample", "https://hooks.example.com/alerts")
	explicitZero.Webhook.SampleRowCount = proto.Uint32(0)
	definition, _, err = alertDefinitionFromProto(explicitZero, true)
	if err != nil || definition.SampleRows != 0 {
		t.Fatalf("explicit zero sample rows = %d, %v", definition.SampleRows, err)
	}
	projected, err := alertDefinitionToProto(definition, "hooks.example.com", 1, time.Now())
	if err != nil || projected.GetWebhook().SampleRowCount == nil || projected.GetWebhook().GetSampleRowCount() != 0 {
		t.Fatalf("projected explicit zero = %+v, %v", projected.GetWebhook(), err)
	}
}

func TestAlertAPIListFiltersPagesAndNeverReturnsWebhookURL(t *testing.T) {
	handler, closeDatabase := newAlertAPIHandler(t, nil)
	defer closeDatabase()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/alerts/list", nil)
	for _, name := range []string{"Database errors", "API errors", "Latency"} {
		definition := alertAPITestDefinition(name, "https://hooks.example.com/alerts")
		if _, err := handler.createAlert(request, &opensplunk.CreateAlertRequest{Definition: definition}); err != nil {
			t.Fatalf("create %q: %v", name, err)
		}
	}
	pageSize := uint32(1)
	includeTotal := true
	first, err := handler.listAlerts(request, &opensplunk.ListAlertsRequest{
		TextFilter: new("errors"), Page: &opensplunk.PageRequest{PageSize: &pageSize, IncludeTotalSize: includeTotal},
	})
	if err != nil {
		t.Fatalf("listAlerts(first) error = %v", err)
	}
	if len(first.GetAlerts()) != 1 || first.GetPage().GetTotalSize() != 2 || first.GetPage().GetNextPageToken() == "" {
		t.Fatalf("first page = %+v", first)
	}
	assertSafeAlertProjection(t, first.GetAlerts()[0], "hooks.example.com")
	if _, err := handler.listAlerts(request, &opensplunk.ListAlertsRequest{
		TextFilter: new("latency"), Page: &opensplunk.PageRequest{PageSize: &pageSize, PageToken: first.GetPage().NextPageToken},
	}); err == nil {
		t.Fatal("listAlerts() accepted a cursor under different filters")
	}
	if _, err := handler.createAlert(request, &opensplunk.CreateAlertRequest{
		Definition: alertAPITestDefinition("Newer errors", "https://hooks.example.com/alerts"),
	}); err != nil {
		t.Fatalf("create alert between pages: %v", err)
	}
	second, err := handler.listAlerts(request, &opensplunk.ListAlertsRequest{
		TextFilter: new("errors"), Page: &opensplunk.PageRequest{PageSize: &pageSize, PageToken: first.GetPage().NextPageToken},
	})
	if err != nil || len(second.GetAlerts()) != 1 || second.GetPage().GetNextPageToken() != "" {
		t.Fatalf("listAlerts(second) = %+v, %v", second, err)
	}
	if second.GetAlerts()[0].GetAlertId() == first.GetAlerts()[0].GetAlertId() {
		t.Fatal("cursor repeated the first alert")
	}
}

func TestAlertAPIListRunsProjectsCurrentRetainedResultState(t *testing.T) {
	handler, closeDatabase := newAlertAPIHandler(t, nil)
	defer closeDatabase()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/alerts/runs/list", nil)
	created, err := handler.createAlert(request, &opensplunk.CreateAlertRequest{
		Definition: alertAPITestDefinition("Retained errors", "https://hooks.example.com/alerts"),
	})
	if err != nil {
		t.Fatalf("createAlert() error = %v", err)
	}
	runRepository := handler.alertRepository.(*alertstore.SQLRepository)
	snapshot, active, err := runRepository.ClaimRunNow(
		request.Context(), handler.ownerID, created.GetAlert().GetAlertId(), handler.now(),
	)
	if err != nil || !active {
		t.Fatalf("ClaimRunNow() = %+v, %t, %v", snapshot, active, err)
	}
	initialExpiry := handler.now().Add(-time.Minute)
	if err := runRepository.AttachSearchJob(request.Context(), snapshot.AlertID, snapshot.AlertRunID, "job-retained", initialExpiry); err != nil {
		t.Fatalf("AttachSearchJob() error = %v", err)
	}
	if err := runRepository.CompleteRun(request.Context(), alerts.RunSummary{
		AlertID: snapshot.AlertID, AlertRunID: snapshot.AlertRunID, Outcome: alerts.RunNotTriggered,
		FinishedAt: handler.now(), Evaluation: alerts.EvaluationFalse, SearchJobExpiresAt: initialExpiry,
	}); err != nil {
		t.Fatalf("CompleteRun() error = %v", err)
	}
	currentExpiry := handler.now().Add(7 * 24 * time.Hour)
	handler.searchArtifacts = &batchListSearchArtifacts{records: map[string]searchartifacts.Record{
		"job-retained": {
			Job: searchjobs.Job{ID: "job-retained"}, State: searchartifacts.StateCompleted,
			ArtifactPresent: true, ExpiresAt: currentExpiry,
		},
	}}

	response, err := handler.listAlertRuns(request, &opensplunk.ListAlertRunsRequest{AlertId: snapshot.AlertID})
	if err != nil {
		t.Fatalf("listAlertRuns() error = %v", err)
	}
	if len(response.GetRuns()) != 1 {
		t.Fatalf("runs = %+v", response.GetRuns())
	}
	run := response.GetRuns()[0]
	if !run.GetSearchJobExpiresAt().AsTime().Equal(currentExpiry) ||
		run.GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_AVAILABLE {
		t.Fatalf("retained result projection = %+v", run)
	}

	handler.searchArtifacts = &batchListSearchArtifacts{records: map[string]searchartifacts.Record{}}
	response, err = handler.listAlertRuns(request, &opensplunk.ListAlertRunsRequest{AlertId: snapshot.AlertID})
	if err != nil || response.GetRuns()[0].GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_EXPIRED ||
		!response.GetRuns()[0].GetSearchJobExpiresAt().AsTime().Equal(initialExpiry) {
		t.Fatalf("expired retained result projection = %+v, %v", response.GetRuns(), err)
	}
}

func TestAlertPageRequestBoundsOmittedSizeToServerMaximum(t *testing.T) {
	t.Parallel()
	handler := &apiHandler{maximumPageSize: 2}

	for _, page := range []*opensplunk.PageRequest{nil, {}} {
		pageSize, token, includeTotal, err := handler.boundedListPageRequest(page, "alert", defaultAlertPageSize, alerts.MaximumAlertsPerOwner)
		if err != nil || pageSize != 2 || token != "" || includeTotal {
			t.Fatalf("boundedListPageRequest(%+v) = %d, %q, %t, %v", page, pageSize, token, includeTotal, err)
		}
	}

	tooLarge := uint32(3)
	if _, _, _, err := handler.boundedListPageRequest(&opensplunk.PageRequest{PageSize: &tooLarge}, "alert", defaultAlertPageSize, alerts.MaximumAlertsPerOwner); err == nil {
		t.Fatal("boundedListPageRequest() accepted an explicit size above the server maximum")
	}

	pageSize, _, _, err := (&apiHandler{maximumPageSize: 100}).boundedListPageRequest(nil, "alert", defaultAlertPageSize, 1)
	if err != nil || pageSize != 1 {
		t.Fatalf("boundedListPageRequest() service maximum = %d, %v; want 1", pageSize, err)
	}
}

func TestAlertResultTabMappingsAreExhaustive(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		proto  opensplunk.SearchResultTab
		domain alerts.ResultTab
	}{
		{opensplunk.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED, alerts.ResultTabUnspecified},
		{opensplunk.SearchResultTab_SEARCH_RESULT_TAB_EVENTS, alerts.ResultTabEvents},
		{opensplunk.SearchResultTab_SEARCH_RESULT_TAB_STATISTICS, alerts.ResultTabStatistics},
		{opensplunk.SearchResultTab_SEARCH_RESULT_TAB_VISUALIZATION, alerts.ResultTabVisualization},
	} {
		domain, err := alertResultTabFromProto(test.proto)
		if err != nil || domain != test.domain {
			t.Fatalf("alertResultTabFromProto(%v) = %v, %v", test.proto, domain, err)
		}
		projected, err := alertResultTabToProto(domain)
		if err != nil || projected != test.proto {
			t.Fatalf("alertResultTabToProto(%v) = %v, %v", domain, projected, err)
		}
	}
	if _, err := alertResultTabFromProto(opensplunk.SearchResultTab(99)); err == nil {
		t.Fatal("alertResultTabFromProto() accepted an unknown value")
	}
	if _, err := alertResultTabToProto(alerts.ResultTab(99)); err == nil {
		t.Fatal("alertResultTabToProto() accepted an unknown value")
	}
}

func TestAlertAPITestWebhookSignsAndDeliversWithoutExposingSecrets(t *testing.T) {
	var receivedBody []byte
	deliverer, err := alerts.NewDeliverer(alerts.DeliveryOptions{
		Resolver: alertAPITestResolver{}, Dialer: &net.Dialer{},
		ClientFactory: func(http.RoundTripper, time.Duration) alerts.HTTPDoer {
			return alertAPITestDoer(func(request *http.Request) (*http.Response, error) {
				receivedBody, _ = io.ReadAll(request.Body)
				if request.Header.Get(alerts.HeaderSignature) == "" || request.Header.Get(alerts.HeaderTimestamp) == "" {
					t.Fatalf("signed headers = %v", request.Header)
				}
				return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader(""))}, nil
			})
		},
	})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	handler, closeDatabase := newAlertAPIHandler(t, deliverer)
	defer closeDatabase()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/alerts/webhook/test", nil)
	created, err := handler.createAlert(request, &opensplunk.CreateAlertRequest{Definition: alertAPITestDefinition("Errors", "https://hooks.example.com/alerts")})
	if err != nil {
		t.Fatalf("createAlert() error = %v", err)
	}
	result, err := handler.testAlertWebhook(request, &opensplunk.TestAlertWebhookRequest{AlertId: created.GetAlert().GetAlertId()})
	if err != nil || !result.GetDelivered() || result.GetDeliveryId() == "" || result.FailureCategory != nil {
		t.Fatalf("testAlertWebhook() = %+v, %v", result, err)
	}
	if !bytes.Contains(receivedBody, []byte(`"event_type":"alert.test"`)) || bytes.Contains(receivedBody, []byte(created.GetSigningSecret())) {
		t.Fatalf("test webhook body = %s", receivedBody)
	}
}

func TestAlertAPIOutcomeAndCursorMappingsAreClosed(t *testing.T) {
	now := time.Date(2026, time.August, 29, 1, 2, 3, 0, time.UTC)
	run, err := alertRunToProto(alerts.RunSummary{
		AlertID: "alert-1", AlertRunID: "run-1", AlertVersion: 2,
		ScheduledAt: now, FinishedAt: now.Add(time.Minute), Outcome: alerts.RunSearchExpired,
	})
	if err != nil || run.GetOutcome() != opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_SEARCH_EXPIRED {
		t.Fatalf("alertRunToProto() = %+v, %v", run, err)
	}
	if _, err := alertRunToProto(alerts.RunSummary{AlertID: "alert-1", AlertRunID: "run-1", AlertVersion: 2, ScheduledAt: now, Outcome: "FORGED"}); err == nil {
		t.Fatal("alertRunToProto() accepted an unknown outcome")
	}
	key := bytes.Repeat([]byte{7}, 32)
	token, err := cursorcodec.Encode(key, alertListCursorDomain, alertCursorVersion, maximumAlertPageTokenBytes, alertListCursor{
		AfterAlertID: "alert-1", AppFilter: "search", TextFilter: "errors",
	})
	if err != nil {
		t.Fatalf("encode alert cursor: %v", err)
	}
	var decoded alertListCursor
	if err := cursorcodec.Decode(key, alertListCursorDomain, alertCursorVersion, maximumAlertPageTokenBytes, token, &decoded); err != nil || decoded.AfterAlertID != "alert-1" {
		t.Fatalf("decode alert cursor = %+v, %v", decoded, err)
	}
	if err := cursorcodec.Decode(key, alertRunCursorDomain, alertCursorVersion, maximumAlertPageTokenBytes, token, &alertRunCursor{}); err == nil {
		t.Fatal("alert list cursor crossed into the run-list domain")
	}
}

func TestAlertAPIRunNowUsesCoordinatorAndProjectsRun(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 1, 2, 3, 0, time.UTC)
	coordinator := &alertAPITestCoordinator{run: alerts.RunSummary{
		AlertID: "alert-1", AlertRunID: "run-1", AlertVersion: 3,
		ScheduledAt: now, StartedAt: now, FinishedAt: now.Add(time.Second),
		Outcome: alerts.RunNotTriggered, Evaluation: alerts.EvaluationFalse,
		ResultCount: 0, ResultCountExact: true,
	}}
	handler := &apiHandler{alertCoordinator: coordinator, ownerID: "owner-1"}
	response, err := handler.runAlert(httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/alerts/run", nil), &opensplunk.RunAlertRequest{AlertId: " alert-1 "})
	if err != nil {
		t.Fatalf("runAlert() error = %v", err)
	}
	if coordinator.ownerID != "owner-1" || coordinator.alertID != "alert-1" || response.GetRun().GetAlertRunId() != "run-1" || response.GetRun().GetOutcome() != opensplunk.AlertRunOutcome_ALERT_RUN_OUTCOME_NOT_TRIGGERED {
		t.Fatalf("runAlert() coordinator=%+v response=%+v", coordinator, response)
	}
}

func TestAlertCapabilityAndRoutesRequireCoordinator(t *testing.T) {
	t.Parallel()
	feature := opensplunk.ServerFeature_SERVER_FEATURE_ALERTS
	config := Config{
		SearchJobs:      &fakeSearchJobs{},
		Indexes:         fakeIndexCatalog{},
		WebUI:           testUI(),
		AlertService:    &alerts.Service{},
		AlertRepository: &alertstore.SQLRepository{},
		AlertDeliverer:  &alerts.Deliverer{},
		Bootstrap:       BootstrapConfig{Features: []opensplunk.ServerFeature{feature}},
	}
	partial := newTestHandler(t, config)
	var bootstrap opensplunk.GetSystemBootstrapResponse
	unmarshalResponse(t, postProto(t, partial, "/api/system/bootstrap", &opensplunk.GetSystemBootstrapRequest{}), &bootstrap)
	if containsServerFeature(bootstrap.GetFeatures(), feature) {
		t.Fatalf("partial alert runtime advertised capability: %v", bootstrap.GetFeatures())
	}
	response := postProto(t, partial, "/api/alerts/list", &opensplunk.ListAlertsRequest{})
	if response.Code != http.StatusNotFound {
		t.Fatalf("partial alert runtime route status = %d, want %d", response.Code, http.StatusNotFound)
	}

	config.AlertCoordinator = &alertAPITestCoordinator{}
	config.BrowserAuthenticator = testSearchInspectionAuthenticator(t)
	complete := newTestHandler(t, config)
	unmarshalResponse(t, postProto(t, complete, "/api/system/bootstrap", &opensplunk.GetSystemBootstrapRequest{}), &bootstrap)
	if !containsServerFeature(bootstrap.GetFeatures(), feature) {
		t.Fatalf("complete alert runtime did not advertise capability: %v", bootstrap.GetFeatures())
	}
}

func newAlertAPIHandler(t *testing.T, deliverer *alerts.Deliverer) (*apiHandler, func()) {
	t.Helper()
	now := time.Date(2026, time.August, 29, 1, 2, 3, 0, time.UTC)
	database, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("control.Open() error = %v", err)
	}
	repository, err := alertstore.NewSQLRepository(database, alertstore.SQLRepositoryOptions{TenantID: "tenant-1", Clock: func() time.Time { return now }})
	if err != nil {
		if closeErr := database.Close(); closeErr != nil {
			t.Fatalf("NewSQLRepository() error = %v; close control database: %v", err, closeErr)
		}
		t.Fatalf("NewSQLRepository() error = %v", err)
	}
	cipher, err := alerts.NewAESGCMCipher(bytes.Repeat([]byte{4}, 32), rand.Reader)
	if err != nil {
		if closeErr := database.Close(); closeErr != nil {
			t.Fatalf("NewAESGCMCipher() error = %v; close control database: %v", err, closeErr)
		}
		t.Fatalf("NewAESGCMCipher() error = %v", err)
	}
	var sequence atomic.Uint64
	service, err := alerts.NewService(repository, cipher, alerts.ServiceOptions{
		Clock: func() time.Time { return now }, PublicBaseURL: "https://splunk.example.com",
		IDGenerator: func() (string, error) { return fmt.Sprintf("alert-%d", sequence.Add(1)), nil },
	})
	if err != nil {
		if closeErr := database.Close(); closeErr != nil {
			t.Fatalf("NewService() error = %v; close control database: %v", err, closeErr)
		}
		t.Fatalf("NewService() error = %v", err)
	}
	return &apiHandler{
		alertService: service, alertRepository: repository, alertDeliverer: deliverer,
		alertPublicBaseURL: "https://splunk.example.com", ownerID: "owner-1", maximumPageSize: 100,
		now: func() time.Time { return now }, adminCursorKey: [32]byte{1},
	}, func() { _ = database.Close() }
}

func alertAPITestDefinition(name, webhookURL string) *opensplunk.AlertDefinition {
	earliest, latest, application, timezone := "-5m", "now", "search", "UTC"
	return &opensplunk.AlertDefinition{
		Name: name,
		Search: &opensplunk.SearchDefinition{
			Spl: "search index=main error", AppId: &application,
			TimeRange:      &opensplunk.TimeRangeSpec{Earliest: &earliest, Latest: &latest, Timezone: &timezone},
			IndexScope:     []string{"main"},
			SelectedFields: []string{"host"},
		},
		Cron: "*/5 * * * *", Timezone: "UTC", DispatchTtl: "2p",
		Condition: &opensplunk.AlertCondition{Operator: opensplunk.AlertConditionOperator_ALERT_CONDITION_OPERATOR_GREATER_THAN},
		Webhook:   &opensplunk.WebhookAlertAction{Url: &webhookURL, SampleRowCount: proto.Uint32(5), Ttl: "10p"},
	}
}

func assertSafeAlertProjection(t *testing.T, alert *opensplunk.Alert, hostname string) {
	t.Helper()
	if alert == nil || alert.GetDefinition() == nil || alert.GetDefinition().GetWebhook() == nil {
		t.Fatalf("alert projection = %+v", alert)
	}
	webhook := alert.GetDefinition().GetWebhook()
	if webhook.Url != nil || webhook.GetHostname() != hostname || webhook.GetSecretGeneration() == 0 || webhook.GetSecretRotatedAt() == nil {
		t.Fatalf("unsafe or incomplete webhook projection = %+v", webhook)
	}
}
