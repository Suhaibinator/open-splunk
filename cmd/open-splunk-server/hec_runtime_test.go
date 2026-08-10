package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/hechttp"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

type runtimeHECTestNext struct {
	mu     sync.Mutex
	calls  int
	method string
	path   string
}

func (next *runtimeHECTestNext) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	next.mu.Lock()
	next.calls++
	next.method = request.Method
	next.path = request.URL.Path
	next.mu.Unlock()
	response.Header().Set("X-Delegated", "true")
	response.WriteHeader(299)
	_, _ = io.WriteString(response, "browser")
}

func (next *runtimeHECTestNext) snapshot() (int, string, string) {
	next.mu.Lock()
	defer next.mu.Unlock()
	return next.calls, next.method, next.path
}

type runtimeHECTestAuthenticator struct {
	mu             sync.Mutex
	credentials    []string
	authentication auth.Authentication
	err            error
}

func (authenticator *runtimeHECTestAuthenticator) AuthenticateHEC(
	_ context.Context,
	credential string,
) (auth.Authentication, error) {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	authenticator.credentials = append(authenticator.credentials, credential)
	return authenticator.authentication, authenticator.err
}

func (authenticator *runtimeHECTestAuthenticator) snapshot() []string {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	return append([]string(nil), authenticator.credentials...)
}

type runtimeHECTestStore struct {
	mu                        sync.Mutex
	batches                   []ingest.StoreBatch
	result                    ingest.StageResult
	err                       error
	reconciliationUnavailable bool
	reconciliationTelemetry   clickhouse.HECReconciliationSnapshot
}

func (store *runtimeHECTestStore) HECReconciliationAvailable() bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	return !store.reconciliationUnavailable
}

func (store *runtimeHECTestStore) setReconciliationAvailable(available bool) {
	store.mu.Lock()
	store.reconciliationUnavailable = !available
	store.mu.Unlock()
}

func (store *runtimeHECTestStore) HECReconciliationTelemetry() clickhouse.HECReconciliationSnapshot {
	store.mu.Lock()
	defer store.mu.Unlock()
	snapshot := store.reconciliationTelemetry
	snapshot.Available = !store.reconciliationUnavailable
	return snapshot
}

func (store *runtimeHECTestStore) Store(
	context.Context,
	ingest.StoreBatch,
) (ingest.StoreResult, error) {
	return ingest.StoreResult{}, errors.New("HEC runtime used synchronous Store")
}

func (store *runtimeHECTestStore) Stage(
	_ context.Context,
	batch ingest.StoreBatch,
) (ingest.StageResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.batches = append(store.batches, batch)
	return store.result, store.err
}

func (store *runtimeHECTestStore) snapshot() []ingest.StoreBatch {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]ingest.StoreBatch(nil), store.batches...)
}

type runtimeHECTestSequencer struct {
	mu             sync.Mutex
	healthCalls    int
	health         visibility.HECReadinessSnapshot
	healthErr      error
	ackCalls       int
	operational    visibility.HECOperationalSnapshot
	operationalErr error
}

func (sequencer *runtimeHECTestSequencer) LookupHECAcknowledgments(
	context.Context,
	string,
	string,
	string,
	[]uint64,
) (map[uint64]bool, error) {
	sequencer.mu.Lock()
	defer sequencer.mu.Unlock()
	sequencer.ackCalls++
	return map[uint64]bool{}, nil
}

func (sequencer *runtimeHECTestSequencer) HECReadiness(
	context.Context,
) (visibility.HECReadinessSnapshot, error) {
	sequencer.mu.Lock()
	defer sequencer.mu.Unlock()
	sequencer.healthCalls++
	return sequencer.health, sequencer.healthErr
}

func (sequencer *runtimeHECTestSequencer) HECOperationalHealth(
	context.Context,
) (visibility.HECOperationalSnapshot, error) {
	sequencer.mu.Lock()
	defer sequencer.mu.Unlock()
	return sequencer.operational, sequencer.operationalErr
}

func (sequencer *runtimeHECTestSequencer) snapshot() (int, int) {
	sequencer.mu.Lock()
	defer sequencer.mu.Unlock()
	return sequencer.healthCalls, sequencer.ackCalls
}

func (sequencer *runtimeHECTestSequencer) setHealth(snapshot visibility.HECReadinessSnapshot) {
	sequencer.mu.Lock()
	sequencer.health = snapshot
	sequencer.mu.Unlock()
}

func TestNewRuntimeHECHandlerRequiresCompleteNonNilComposition(t *testing.T) {
	valid := func() runtimeHECConfig {
		return runtimeHECConfig{
			Next:                  &runtimeHECTestNext{},
			Authenticator:         &runtimeHECTestAuthenticator{},
			Store:                 &runtimeHECTestStore{},
			Sequencer:             &runtimeHECTestSequencer{},
			TenantID:              "tenant-hec",
			DefaultIndexRetention: time.Hour,
		}
	}
	var typedNilNext *runtimeHECTestNext
	var typedNilAuthenticator *runtimeHECTestAuthenticator
	var typedNilStore *runtimeHECTestStore
	var typedNilSequencer *runtimeHECTestSequencer
	tests := []struct {
		name      string
		configure func(*runtimeHECConfig)
	}{
		{name: "missing next", configure: func(config *runtimeHECConfig) { config.Next = nil }},
		{name: "typed nil next", configure: func(config *runtimeHECConfig) { config.Next = typedNilNext }},
		{name: "missing authenticator", configure: func(config *runtimeHECConfig) { config.Authenticator = nil }},
		{name: "typed nil authenticator", configure: func(config *runtimeHECConfig) { config.Authenticator = typedNilAuthenticator }},
		{name: "missing store", configure: func(config *runtimeHECConfig) { config.Store = nil }},
		{name: "typed nil store", configure: func(config *runtimeHECConfig) { config.Store = typedNilStore }},
		{name: "missing sequencer", configure: func(config *runtimeHECConfig) { config.Sequencer = nil }},
		{name: "typed nil sequencer", configure: func(config *runtimeHECConfig) { config.Sequencer = typedNilSequencer }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid()
			test.configure(&config)
			if handler, err := newRuntimeHECHandler(config); err == nil || handler != nil ||
				!strings.Contains(err.Error(), "complete dependencies") {
				t.Fatalf("newRuntimeHECHandler() = (%v, %v), want dependency error", handler, err)
			}
		})
	}

	config := valid()
	config.DefaultIndexRetention = time.Nanosecond
	if handler, err := newRuntimeHECHandler(config); err == nil || handler != nil {
		t.Fatalf("invalid default retention composition = (%v, %v), want error", handler, err)
	}
}

func TestRuntimeHECHandlerOwnsNamespaceDelegatesBrowserAndStagesDurably(t *testing.T) {
	next := &runtimeHECTestNext{}
	authenticator := &runtimeHECTestAuthenticator{authentication: auth.Authentication{
		TokenID:      "hec-token-record",
		TokenVersion: 4,
		Purpose:      auth.IngestionTokenPurposeHEC,
		HECProfile: auth.HECTokenProfile{
			DefaultIndexName: "main",
		},
		AuthorizedIndexes: []auth.AuthorizedIndexPolicy{{
			Name:            "main",
			Version:         9,
			RetentionPeriod: 24 * time.Hour,
		}},
	}}
	store := &runtimeHECTestStore{result: ingest.StageResult{
		VisibilitySequence:  17,
		State:               ingest.StoredBatchPending,
		HECRequestSequence:  1,
		HECAcknowledgmentID: 0,
	}}
	sequencer := &runtimeHECTestSequencer{health: visibility.HECReadinessSnapshot{
		QueueAvailable:          true,
		AcknowledgmentAvailable: true,
	}}
	metrics := hechttp.NewMetrics()
	handler, err := newRuntimeHECHandler(runtimeHECConfig{
		Next:                  next,
		Authenticator:         authenticator,
		Store:                 store,
		Sequencer:             sequencer,
		TenantID:              "tenant-hec",
		DefaultIndexRetention: 30 * 24 * time.Hour,
		Metrics:               metrics,
	})
	if err != nil {
		t.Fatalf("newRuntimeHECHandler(): %v", err)
	}
	if handler.Metrics() != metrics {
		t.Fatal("runtime composition did not retain the supplied bounded metrics owner")
	}

	delegated := httptest.NewRecorder()
	handler.ServeHTTP(
		delegated,
		httptest.NewRequest(http.MethodPost, "/api/v1/system/bootstrap", nil),
	)
	if delegated.Code != 299 || delegated.Header().Get("X-Delegated") != "true" ||
		delegated.Body.String() != "browser" {
		t.Fatalf("delegated response = (%d, %q, %q)", delegated.Code, delegated.Header().Get("X-Delegated"), delegated.Body.String())
	}
	if calls, method, path := next.snapshot(); calls != 1 || method != http.MethodPost ||
		path != "/api/v1/system/bootstrap" {
		t.Fatalf("browser delegation = (%d, %q, %q)", calls, method, path)
	}

	unknown := httptest.NewRecorder()
	handler.ServeHTTP(
		unknown,
		httptest.NewRequest(http.MethodPost, "/services/collector/unknown", nil),
	)
	if unknown.Code != http.StatusNotFound ||
		unknown.Header().Get("Content-Type") != "application/json; charset=utf-8" ||
		unknown.Body.String() != `{"text":"Invalid data format","code":6}` {
		t.Fatalf("owned unknown HEC response = (%d, %q, %q)", unknown.Code, unknown.Header().Get("Content-Type"), unknown.Body.String())
	}
	if calls, _, _ := next.snapshot(); calls != 1 {
		t.Fatalf("unknown HEC namespace delegated %d total calls, want 1", calls)
	}

	health := httptest.NewRecorder()
	handler.ServeHTTP(
		health,
		httptest.NewRequest(http.MethodGet, "/services/collector/health", nil),
	)
	if health.Code != http.StatusOK ||
		health.Body.String() != `{"text":"HEC is healthy","code":17}` {
		t.Fatalf("health response = (%d, %q)", health.Code, health.Body.String())
	}
	if healthCalls, ackCalls := sequencer.snapshot(); healthCalls != 1 || ackCalls != 0 {
		t.Fatalf("sequencer calls after health = (health %d, ACK %d)", healthCalls, ackCalls)
	}

	store.setReconciliationAvailable(false)
	ackHealth := httptest.NewRecorder()
	handler.ServeHTTP(
		ackHealth,
		httptest.NewRequest(http.MethodGet, "/services/collector/health?ack=true", nil),
	)
	if ackHealth.Code != http.StatusServiceUnavailable ||
		ackHealth.Body.String() != `{"text":"HEC is unhealthy, ack service unavailable","code":19}` {
		t.Fatalf("reconciliation-unavailable health response = (%d, %q)", ackHealth.Code, ackHealth.Body.String())
	}

	sequencer.setHealth(visibility.HECReadinessSnapshot{
		QueueAvailable:          false,
		AcknowledgmentAvailable: true,
	})
	bothHealth := httptest.NewRecorder()
	handler.ServeHTTP(
		bothHealth,
		httptest.NewRequest(http.MethodGet, "/services/collector/health?ack=1", nil),
	)
	if bothHealth.Code != http.StatusServiceUnavailable ||
		bothHealth.Body.String() != `{"text":"HEC is unhealthy, queues are full, ack service unavailable","code":20}` {
		t.Fatalf("queue-and-reconciliation-unavailable health response = (%d, %q)", bothHealth.Code, bothHealth.Body.String())
	}

	store.setReconciliationAvailable(true)
	sequencer.setHealth(visibility.HECReadinessSnapshot{
		QueueAvailable:          true,
		AcknowledgmentAvailable: true,
	})
	restoredHealth := httptest.NewRecorder()
	handler.ServeHTTP(
		restoredHealth,
		httptest.NewRequest(http.MethodGet, "/services/collector/health?ack=true", nil),
	)
	if restoredHealth.Code != http.StatusOK ||
		restoredHealth.Body.String() != `{"text":"HEC is healthy","code":17}` {
		t.Fatalf("restored reconciliation health response = (%d, %q)", restoredHealth.Code, restoredHealth.Body.String())
	}

	event := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/services/collector/event",
		strings.NewReader(`{"event":"hello from HEC"}`),
	)
	request.Header.Set("Authorization", "Splunk plaintext-token")
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(event, request)
	if event.Code != http.StatusOK || event.Body.String() != `{"text":"Success","code":0}` {
		t.Fatalf("event response = (%d, %q)", event.Code, event.Body.String())
	}
	if credentials := authenticator.snapshot(); len(credentials) != 1 ||
		credentials[0] != "plaintext-token" {
		t.Fatalf("authentication credentials = %#v", credentials)
	}
	batches := store.snapshot()
	if len(batches) != 1 {
		t.Fatalf("staged batches = %d, want 1", len(batches))
	}
	batch := batches[0]
	if batch.TenantID != "tenant-hec" || batch.Source != ingest.HECSource("hec-token-record") ||
		batch.CollectorID != "" || batch.BatchSequence != 1 || batch.OriginalEventCount != 1 ||
		len(batch.Events) != 1 || batch.HECAdmission == nil ||
		batch.HECAdmission.TokenID != "hec-token-record" || batch.HECAdmission.TokenVersion != 4 ||
		batch.HECAdmission.AcknowledgmentEnabled || batch.HECAdmission.Channel != "" ||
		len(batch.HECAdmission.AuthorizedIndexes) != 1 ||
		batch.HECAdmission.AuthorizedIndexes[0] != (ingest.HECIndexAuthority{Name: "main", Version: 9}) {
		t.Fatalf("staged HEC batch = %+v", batch)
	}
	if got := string(batch.Events[0].Event.GetRaw()); got != "hello from HEC" {
		t.Fatalf("staged event raw = %q", got)
	}

	store.mu.Lock()
	store.reconciliationTelemetry = clickhouse.HECReconciliationSnapshot{
		Successes:   3,
		Retries:     2,
		Ambiguities: 1,
	}
	store.mu.Unlock()
	sequencer.mu.Lock()
	sequencer.operational = visibility.HECOperationalSnapshot{
		QueueAvailable:            true,
		AcknowledgmentAvailable:   true,
		PendingOutboxReservations: 4,
		PendingOutboxBytes:        512,
		OldestPendingOutboxAge:    7 * time.Second,
		RequestCapacityAvailable:  true,
		RetainedRequests:          11,
		ActiveChannels:            5,
		RetainedChannels:          6,
		PendingAcknowledgments:    7,
		IndexedAcknowledgments:    8,
		ExpiredAcknowledgments:    9,
		TerminalFailedRequests:    10,
	}
	sequencer.mu.Unlock()
	operations, err := newRuntimeHECOperations(metrics, sequencer, store)
	if err != nil {
		t.Fatalf("newRuntimeHECOperations: %v", err)
	}
	observedAt := time.Date(2026, time.August, 10, 20, 21, 22, 0, time.UTC)
	operations.now = func() time.Time { return observedAt }
	operational, err := operations.HECOperationalSnapshot(context.Background())
	if err != nil {
		t.Fatalf("HECOperationalSnapshot: %v", err)
	}
	if !operational.ObservedAt.Equal(observedAt) || operational.AcceptedRequests != 1 ||
		operational.Events != 1 || operational.UncompressedBytes == 0 ||
		operational.PendingOutboxReservations != 4 || operational.PendingOutboxBytes != 512 ||
		operational.OldestPendingOutboxAge != 7*time.Second ||
		!operational.RequestCapacityAvailable || operational.RetainedRequests != 11 ||
		!operational.QueueAvailable || !operational.ReconciliationAvailable ||
		operational.ReconciliationSuccesses != 3 || operational.ReconciliationRetries != 2 ||
		operational.ReconciliationAmbiguities != 1 || operational.ActiveChannels != 5 ||
		operational.RetainedChannels != 6 || operational.PendingAcknowledgments != 7 ||
		operational.IndexedAcknowledgments != 8 || operational.ExpiredAcknowledgments != 9 ||
		operational.TerminalFailedRequests != 10 || !operational.AcknowledgmentAvailable {
		t.Fatalf("composed HEC operational snapshot = %+v", operational)
	}
}

func TestNewRuntimeHECOperationsRequiresCompleteDependencies(t *testing.T) {
	var typedNilSequencer *runtimeHECTestSequencer
	var typedNilStore *runtimeHECTestStore
	for _, test := range []struct {
		name      string
		metrics   *hechttp.Metrics
		sequencer runtimeHECSequencer
		store     runtimeHECStore
	}{
		{name: "missing metrics", sequencer: &runtimeHECTestSequencer{}, store: &runtimeHECTestStore{}},
		{name: "typed nil sequencer", metrics: hechttp.NewMetrics(), sequencer: typedNilSequencer, store: &runtimeHECTestStore{}},
		{name: "typed nil store", metrics: hechttp.NewMetrics(), sequencer: &runtimeHECTestSequencer{}, store: typedNilStore},
	} {
		t.Run(test.name, func(t *testing.T) {
			operations, err := newRuntimeHECOperations(test.metrics, test.sequencer, test.store)
			if err == nil || operations != nil || !strings.Contains(err.Error(), "complete dependencies") {
				t.Fatalf("newRuntimeHECOperations = (%v, %v)", operations, err)
			}
		})
	}
}

func TestHECServerFeatureOrdinalIsStable(t *testing.T) {
	feature := opensplunkv1.ServerFeature_SERVER_FEATURE_HEC_INGESTION
	if got := int32(feature); got != 16 {
		t.Fatalf("SERVER_FEATURE_HEC_INGESTION = %d, want stable ordinal 16", got)
	}
	if got := opensplunkv1.ServerFeature_name[int32(feature)]; got != "SERVER_FEATURE_HEC_INGESTION" {
		t.Fatalf("HEC server feature name = %q", got)
	}
}
