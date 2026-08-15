package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchws"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const (
	expirationPollInterval = 5 * time.Millisecond
	expirationRetention    = 100 * time.Millisecond
)

type expirationClockWaiter struct {
	deadline time.Time
	ready    chan struct{}
}

type expirationTestClock struct {
	mu      sync.Mutex
	now     time.Time
	nextID  uint64
	waiters map[uint64]expirationClockWaiter
}

func newExpirationTestClock(now time.Time) *expirationTestClock {
	return &expirationTestClock{
		now:     now,
		waiters: make(map[uint64]expirationClockWaiter),
	}
}

func (clock *expirationTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *expirationTestClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	for id, waiter := range clock.waiters {
		if waiter.deadline.After(now) {
			continue
		}
		delete(clock.waiters, id)
		close(waiter.ready)
	}
	clock.mu.Unlock()
}

func (clock *expirationTestClock) SetWithoutWake(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func (clock *expirationTestClock) WakeAll() {
	clock.mu.Lock()
	for id, waiter := range clock.waiters {
		delete(clock.waiters, id)
		close(waiter.ready)
	}
	clock.mu.Unlock()
}

func (clock *expirationTestClock) Wait(ctx context.Context, delay time.Duration) {
	if ctx.Err() != nil || delay <= 0 {
		return
	}
	clock.mu.Lock()
	if ctx.Err() != nil {
		clock.mu.Unlock()
		return
	}
	clock.nextID++
	id := clock.nextID
	waiter := expirationClockWaiter{
		deadline: clock.now.Add(delay),
		ready:    make(chan struct{}),
	}
	clock.waiters[id] = waiter
	clock.mu.Unlock()

	select {
	case <-waiter.ready:
	case <-ctx.Done():
		clock.mu.Lock()
		delete(clock.waiters, id)
		clock.mu.Unlock()
	}
}

func (clock *expirationTestClock) pendingWaiters() int {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return len(clock.waiters)
}

type countedSearchSnapshots struct {
	manager      *searchjobs.Manager
	gets         atomic.Uint64
	previewReads atomic.Uint64
}

func (snapshots *countedSearchSnapshots) GetFor(access searchjobs.AccessScope, id string) (searchjobs.Job, error) {
	snapshots.gets.Add(1)
	return snapshots.manager.GetFor(access, id)
}

func (snapshots *countedSearchSnapshots) GetForContext(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	snapshots.gets.Add(1)
	return snapshots.manager.GetForContext(ctx, access, id)
}

func (snapshots *countedSearchSnapshots) PreviewForBytes(
	access searchjobs.AccessScope,
	id string,
	limit int,
	maximumBytes uint64,
) (searchjobs.PreviewSnapshot, error) {
	snapshots.previewReads.Add(1)
	return snapshots.manager.PreviewForBytes(access, id, limit, maximumBytes)
}

func (snapshots *countedSearchSnapshots) PreviewForBytesContext(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
	limit int,
	maximumBytes uint64,
) (searchjobs.PreviewSnapshot, error) {
	snapshots.previewReads.Add(1)
	return snapshots.manager.PreviewForBytesContext(ctx, access, id, limit, maximumBytes)
}

func (snapshots *countedSearchSnapshots) MaximumPreviewRows() uint32 {
	return snapshots.manager.MaximumPreviewRows()
}

func (snapshots *countedSearchSnapshots) reads() uint64 {
	return snapshots.gets.Load() + snapshots.previewReads.Load()
}

type countedExportSnapshots struct {
	manager *exportjobs.Manager
	gets    atomic.Uint64
}

func (snapshots *countedExportSnapshots) Get(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (exportjobs.Job, error) {
	snapshots.gets.Add(1)
	return snapshots.manager.Get(ctx, access, id)
}

func (snapshots *countedExportSnapshots) Snapshot(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (exportjobs.Job, error) {
	snapshots.gets.Add(1)
	return snapshots.manager.Snapshot(ctx, access, id)
}

type expirationWebSocketFixture struct {
	*realSearchWebSocketFixture
	clock           *expirationTestClock
	access          searchjobs.AccessScope
	searches        *searchjobs.Manager
	exports         *exportjobs.Manager
	searchSnapshots *countedSearchSnapshots
	exportSnapshots *countedExportSnapshots
}

func newExpirationWebSocketFixture(
	t *testing.T,
	searchJobID string,
	diagnosticJobID string,
	exportJobID string,
) *expirationWebSocketFixture {
	t.Helper()
	const clientTimeout = 3 * time.Second

	anchor := time.Date(2026, time.July, 26, 12, 0, 0, 0, time.UTC)
	clock := newExpirationTestClock(anchor)
	access := searchjobs.AccessScope{
		TenantID: "tenant-expiration-websocket",
		OwnerID:  "owner-expiration-websocket",
	}
	executor := newRealSearchWebSocketExecutor()
	searchIDs := []string{searchJobID, diagnosticJobID}
	var searchIDIndex atomic.Uint32
	searches, err := searchjobs.New(searchjobs.Config{
		Executor:              executor,
		Snapshotter:           searchJobListIntegrationSnapshotter(29),
		MaxConcurrent:         1,
		MaxResultLeases:       1,
		MaxResultLeasesPerJob: 1,
		RetentionTTL:          expirationRetention,
		ExpiredRetention:      expirationRetention,
		CleanupInterval:       -1,
		Now:                   clock.Now,
		NewID: func() string {
			index := searchIDIndex.Add(1) - 1
			if int(index) >= len(searchIDs) {
				return ""
			}
			return searchIDs[index]
		},
		CursorKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("searchjobs.New: %v", err)
	}

	exports, err := exportjobs.New(exportjobs.Config{
		Source:                   searches,
		ArtifactDir:              t.TempDir(),
		MaxWorkers:               1,
		MaxActiveDownloads:       1,
		MaxActiveDownloadsPerJob: 1,
		ArtifactTTL:              expirationRetention,
		ExpiredRetention:         expirationRetention,
		CleanupInterval:          -1,
		Now:                      clock.Now,
		NewID:                    func() string { return exportJobID },
	})
	if err != nil {
		_ = searches.Close()
		t.Fatalf("export.New: %v", err)
	}

	searchSnapshots := &countedSearchSnapshots{manager: searches}
	exportSnapshots := &countedExportSnapshots{manager: exports}
	socketService, err := searchws.New(searchws.Config{
		Searches:              searchSnapshots,
		Exports:               exportSnapshots,
		Access:                access,
		CheckOrigin:           func(*http.Request) bool { return true },
		MaximumSubscriptions:  4,
		MaximumFrameBytes:     64 << 10,
		MaximumReplayEvents:   32,
		PollInterval:          expirationPollInterval,
		TombstonePollInterval: expirationPollInterval,
		PingInterval:          3 * time.Second,
		PongTimeout:           5 * time.Second,
		Now:                   clock.Now,
		Wait:                  clock.Wait,
	})
	if err != nil {
		_ = exports.Close()
		_ = searches.Close()
		t.Fatalf("searchws.New: %v", err)
	}
	handler, err := NewHandler(Config{
		SearchJobs:      searches,
		Exports:         exports,
		SearchWebSocket: socketService,
		Indexes: fakeIndexCatalog{indexes: []control.Index{{
			ID: "index-main",
			Definition: control.IndexDefinition{
				Name:          "main",
				DisplayName:   "Main",
				SearchEnabled: true,
			},
			State: control.IndexStateActive,
		}}},
		SavedSearches:              &fakeSavedSearches{},
		WebUI:                      testUI(),
		OwnerID:                    access.OwnerID,
		TenantID:                   access.TenantID,
		AdministrativeAllowedHosts: []string{"127.0.0.1"},
		Now:                        clock.Now,
	})
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = socketService.Close(ctx)
		cancel()
		_ = exports.Close()
		_ = searches.Close()
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := handler.Close(ctx); err != nil {
			t.Errorf("close expiration websocket handler: %v", err)
		}
		cancel()
		server.Close()
		if err := exports.Close(); err != nil {
			t.Errorf("close expiration export manager: %v", err)
		}
		if err := searches.Close(); err != nil {
			t.Errorf("close expiration search manager: %v", err)
		}
	})

	base := &realSearchWebSocketFixture{
		t:         t,
		jobID:     searchJobID,
		anchor:    anchor,
		executor:  executor,
		server:    server,
		client:    server.Client(),
		dialer:    &websocket.Dialer{HandshakeTimeout: clientTimeout},
		socketURL: webSocketURL(server.URL),
	}
	base.client.Timeout = clientTimeout
	return &expirationWebSocketFixture{
		realSearchWebSocketFixture: base,
		clock:                      clock,
		access:                     access,
		searches:                   searches,
		exports:                    exports,
		searchSnapshots:            searchSnapshots,
		exportSnapshots:            exportSnapshots,
	}
}

type expirationSubscription struct {
	id           string
	searchJobID  string
	exportJobID  string
	lastSequence uint64
}

func subscribeExpirationTarget(
	t *testing.T,
	connection *websocket.Conn,
	subscription *expirationSubscription,
	includePreviews bool,
) {
	t.Helper()
	target := &opensplunkv1.JobTarget{}
	switch {
	case subscription.searchJobID != "":
		target.Target = &opensplunkv1.JobTarget_SearchJobId{SearchJobId: subscription.searchJobID}
	case subscription.exportJobID != "":
		target.Target = &opensplunkv1.JobTarget_ExportJobId{ExportJobId: subscription.exportJobID}
	default:
		t.Fatal("expiration subscription has no target")
	}
	var previewRowLimit *uint32
	if includePreviews {
		previewRowLimit = new(uint32(1))
	}
	command := searchWebSocketTargetSubscribeCommand(
		"subscribe-"+subscription.id,
		subscription.id,
		target,
		0,
		includePreviews,
		previewRowLimit,
	)
	writeSearchWebSocketCommand(t, connection, command)
	event := readSearchWebSocketEvent(t, connection)
	acknowledgment := event.GetSubscriptionAcknowledged()
	if event.GetSequence() != 0 || acknowledgment == nil ||
		acknowledgment.GetRequestId() != command.GetRequestId() ||
		acknowledgment.GetSubscriptionId() != subscription.id ||
		!sameExpirationTarget(acknowledgment.GetTarget(), subscription) ||
		acknowledgment.GetReplayWillFollow() {
		t.Fatalf("expiration subscription acknowledgment = %+v", event)
	}
}

func readExpirationTargetEvent(
	t *testing.T,
	connection *websocket.Conn,
	subscription *expirationSubscription,
) *opensplunkv1.SearchWebSocketEvent {
	t.Helper()
	event := readSearchWebSocketEvent(t, connection)
	if event.GetSubscriptionId() != subscription.id ||
		!sameExpirationTarget(event.GetTarget(), subscription) ||
		event.GetSequence() == 0 ||
		event.GetSequence() <= subscription.lastSequence {
		t.Fatalf(
			"expiration target event = %+v after sequence %d for subscription %+v",
			event,
			subscription.lastSequence,
			subscription,
		)
	}
	subscription.lastSequence = event.GetSequence()
	return event
}

func sameExpirationTarget(target *opensplunkv1.JobTarget, subscription *expirationSubscription) bool {
	if target == nil {
		return false
	}
	if subscription.searchJobID != "" {
		return target.GetSearchJobId() == subscription.searchJobID && target.GetExportJobId() == ""
	}
	return target.GetExportJobId() == subscription.exportJobID && target.GetSearchJobId() == ""
}

func readExpirationProjectionUntil(
	t *testing.T,
	connection *websocket.Conn,
	subscription *expirationSubscription,
	terminal func(*opensplunkv1.SearchWebSocketEvent) bool,
) []*opensplunkv1.SearchWebSocketEvent {
	t.Helper()
	const maximumProjectionFrames = 8
	events := make([]*opensplunkv1.SearchWebSocketEvent, 0, maximumProjectionFrames)
	for range maximumProjectionFrames {
		event := readExpirationTargetEvent(t, connection, subscription)
		events = append(events, event)
		if terminal(event) {
			return events
		}
	}
	t.Fatalf("expiration projection did not terminate after %d frames: %+v", maximumProjectionFrames, events)
	return nil
}

func waitForExpirationSearchState(
	t *testing.T,
	manager *searchjobs.Manager,
	access searchjobs.AccessScope,
	id string,
	state searchjobs.State,
) searchjobs.Job {
	t.Helper()
	return waitForIntegrationState(t, "search "+id, state.String(), func() (searchjobs.Job, string, error) {
		job, err := manager.GetFor(access, id)
		return job, job.State.String(), err
	})
}

func waitForExpirationExportState(
	t *testing.T,
	manager *exportjobs.Manager,
	access searchjobs.AccessScope,
	id string,
	state exportjobs.State,
) exportjobs.Job {
	t.Helper()
	return waitForIntegrationState(t, "export "+id, state.String(), func() (exportjobs.Job, string, error) {
		job, err := manager.Get(context.Background(), access, id)
		return job, job.State.String(), err
	})
}

func waitForIntegrationState[T any](
	t *testing.T,
	label string,
	want string,
	read func() (T, string, error),
) T {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(expirationPollInterval)
	defer ticker.Stop()
	for {
		value, state, err := read()
		if err != nil {
			t.Fatalf("%s state: %v", label, err)
		}
		if state == want {
			return value
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("%s state = %s, want %s", label, state, want)
		}
	}
}

func waitForExpirationClockWaiters(t *testing.T, clock *expirationTestClock, want int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(expirationPollInterval)
	defer ticker.Stop()
	for {
		if got := clock.pendingWaiters(); got == want {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("expiration clock waiters = %d, want %d", clock.pendingWaiters(), want)
		}
	}
}

func assertExpirationSocketPong(t *testing.T, connection *websocket.Conn, nonce string) {
	t.Helper()
	writeSearchWebSocketCommand(t, connection, &opensplunkv1.SearchWebSocketCommand{
		RequestId: "ping-" + nonce,
		Payload: &opensplunkv1.SearchWebSocketCommand_Ping{
			Ping: &opensplunkv1.SearchWebSocketPing{Nonce: nonce},
		},
	})
	event := readSearchWebSocketEvent(t, connection)
	if event.GetSequence() != 0 || event.GetSubscriptionId() != "" || event.GetTarget() != nil ||
		event.GetPong().GetNonce() != nonce {
		t.Fatalf("expiration application pong = %+v", event)
	}
}

func assertExpirationSocketClosesWithoutEvent(t *testing.T, connection *websocket.Conn, label string) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("%s set close deadline: %v", label, err)
	}
	for {
		messageType, payload, err := connection.ReadMessage()
		if err != nil {
			var networkError net.Error
			if errors.As(err, &networkError) && networkError.Timeout() {
				t.Fatalf("%s remained open after tombstone removal: %v", label, err)
			}
			return
		}
		switch messageType {
		case websocket.BinaryMessage:
			event := new(opensplunkv1.SearchWebSocketEvent)
			_ = proto.Unmarshal(payload, event)
			t.Fatalf("%s received stale application frame after tombstone removal: bytes=%d event=%+v", label, len(payload), event)
		default:
			t.Fatalf("%s received unexpected websocket message type %d after tombstone removal", label, messageType)
		}
	}
}

func assertExpirationPollersQuiesce(
	t *testing.T,
	searches *countedSearchSnapshots,
	exports *countedExportSnapshots,
) {
	t.Helper()
	const quietWindow = 10 * expirationPollInterval
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(expirationPollInterval)
	defer ticker.Stop()
	searchReads, exportReads := searches.reads(), exports.gets.Load()
	quietSince := time.Now()
	for {
		select {
		case <-ticker.C:
			nextSearch, nextExport := searches.reads(), exports.gets.Load()
			if nextSearch != searchReads || nextExport != exportReads {
				searchReads, exportReads = nextSearch, nextExport
				quietSince = time.Now()
				continue
			}
			if time.Since(quietSince) >= quietWindow {
				return
			}
		case <-deadline.C:
			t.Fatalf(
				"expiration pollers did not quiesce: search reads=%d export reads=%d",
				searches.reads(),
				exports.gets.Load(),
			)
		}
	}
}

func TestSearchWebSocketRealManagersExpireLeasedResultsArtifactsAndTombstones(t *testing.T) {
	const (
		searchJobID     = "search-expiration-retained"
		diagnosticJobID = "search-expiration-diagnostic"
		exportJobID     = "export-expiration-retained"
	)
	fixture := newExpirationWebSocketFixture(t, searchJobID, diagnosticJobID, exportJobID)

	fixture.createRunningSearch()
	fixture.executor.addRow(t, searchjobs.StringValue("retained until final lease"))
	searchConnection := fixture.dial()
	defer searchConnection.Close()
	searchSubscription := &expirationSubscription{
		id:          "search-expiration-subscription",
		searchJobID: searchJobID,
	}
	subscribeExpirationTarget(t, searchConnection, searchSubscription, true)
	initialSearch := make([]*opensplunkv1.SearchWebSocketEvent, 0, 4)
	for range 4 {
		initialSearch = append(initialSearch, readExpirationTargetEvent(t, searchConnection, searchSubscription))
	}
	if initialSearch[0].GetSearchStateChanged().GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING ||
		initialSearch[1].GetSearchProgress() == nil ||
		initialSearch[2].GetResultSchemaAvailable() == nil ||
		len(initialSearch[3].GetResultPreview().GetRows()) != 1 ||
		initialSearch[3].GetResultPreview().GetRows()[0].GetCells()[0].GetStringValue() != "retained until final lease" {
		t.Fatalf("initial retained-result projection = %+v", initialSearch)
	}

	waitForExpirationClockWaiters(t, fixture.clock, 1)
	fixture.executor.complete(t)
	completedSearch := waitForExpirationSearchState(
		t,
		fixture.searches,
		fixture.access,
		searchJobID,
		searchjobs.StateCompleted,
	)
	fixture.clock.WakeAll()
	completedSearchEvents := readExpirationProjectionUntil(
		t,
		searchConnection,
		searchSubscription,
		func(event *opensplunkv1.SearchWebSocketEvent) bool {
			return event.GetSearchTerminal() != nil
		},
	)
	if len(completedSearchEvents) != 3 ||
		completedSearchEvents[0].GetSearchStateChanged().GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
		completedSearchEvents[1].GetSearchProgress() == nil ||
		completedSearchEvents[2].GetSearchTerminal().GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
		completedSearchEvents[2].GetSearchTerminal().GetFailure() != nil ||
		!completedSearchEvents[2].GetSearchTerminal().GetResultsExpireAt().AsTime().Equal(completedSearch.ExpiresAt) {
		t.Fatalf("completed retained-result projection = %+v", completedSearchEvents)
	}
	waitForExpirationClockWaiters(t, fixture.clock, 1)

	var createdExport opensplunkv1.CreateExportJobResponse
	fixture.post(
		"/api/v1/search/exports/create",
		&opensplunkv1.CreateExportJobRequest{Definition: csvExportDefinition(searchJobID)},
		&createdExport,
	)
	if createdExport.GetExportJob().GetExportJobId() != exportJobID {
		t.Fatalf("created export = %+v", createdExport.GetExportJob())
	}
	completedExport := waitForExpirationExportState(
		t,
		fixture.exports,
		fixture.access,
		exportJobID,
		exportjobs.StateCompleted,
	)
	if completedExport.Artifact == nil || completedExport.Artifact.RowCount != 1 ||
		!completedExport.ExpiresAt.Equal(completedSearch.ExpiresAt) {
		t.Fatalf("completed export snapshot = %+v search expiry=%s", completedExport, completedSearch.ExpiresAt)
	}
	exportConnection := fixture.dial()
	defer exportConnection.Close()
	exportSubscription := &expirationSubscription{
		id:          "export-expiration-subscription",
		exportJobID: exportJobID,
	}
	subscribeExpirationTarget(t, exportConnection, exportSubscription, false)
	initialExport := readExpirationProjectionUntil(
		t,
		exportConnection,
		exportSubscription,
		func(event *opensplunkv1.SearchWebSocketEvent) bool {
			return event.GetExportTerminal() != nil
		},
	)
	if len(initialExport) != 3 ||
		initialExport[0].GetExportStateChanged().GetState() != opensplunkv1.ExportJobState_EXPORT_JOB_STATE_COMPLETED ||
		initialExport[1].GetExportProgress() == nil ||
		initialExport[2].GetExportTerminal().GetState() != opensplunkv1.ExportJobState_EXPORT_JOB_STATE_COMPLETED ||
		initialExport[2].GetExportTerminal().GetArtifact().GetRowCount() != 1 {
		t.Fatalf("completed export projection = %+v", initialExport)
	}
	staleArtifactCheckpoint := initialExport[1].GetSequence()
	waitForExpirationClockWaiters(t, fixture.clock, 2)

	diagnosticRequest := createRequest(
		fixture.anchor.Add(-time.Hour).Format(time.RFC3339Nano),
		fixture.anchor.Format(time.RFC3339Nano),
		"main",
	)
	diagnosticRequest.Definition.Spl = "index=main | where"
	var createdDiagnostic opensplunkv1.CreateSearchJobResponse
	fixture.post("/api/v1/search/jobs/create", diagnosticRequest, &createdDiagnostic)
	if createdDiagnostic.GetSearchJob().GetSearchJobId() != diagnosticJobID {
		t.Fatalf("created diagnostic search = %+v", createdDiagnostic.GetSearchJob())
	}
	failedDiagnostic := waitForExpirationSearchState(
		t,
		fixture.searches,
		fixture.access,
		diagnosticJobID,
		searchjobs.StateFailed,
	)
	if failedDiagnostic.Failure == nil || len(failedDiagnostic.Failure.Diagnostics) == 0 {
		t.Fatalf("failed diagnostic search retained no diagnostics: %+v", failedDiagnostic)
	}
	diagnosticConnection := fixture.dial()
	defer diagnosticConnection.Close()
	diagnosticSubscription := &expirationSubscription{
		id:          "diagnostic-expiration-subscription",
		searchJobID: diagnosticJobID,
	}
	subscribeExpirationTarget(t, diagnosticConnection, diagnosticSubscription, false)
	initialDiagnostic := readExpirationProjectionUntil(
		t,
		diagnosticConnection,
		diagnosticSubscription,
		func(event *opensplunkv1.SearchWebSocketEvent) bool {
			return event.GetSearchTerminal() != nil
		},
	)
	initialFailure := initialDiagnostic[len(initialDiagnostic)-1].GetSearchTerminal().GetFailure()
	if len(initialDiagnostic) != 3 ||
		initialDiagnostic[0].GetSearchStateChanged().GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED ||
		initialDiagnostic[1].GetSearchProgress() == nil ||
		initialFailure == nil || len(initialFailure.GetDiagnostics()) == 0 {
		t.Fatalf("failed diagnostic projection = %+v", initialDiagnostic)
	}
	waitForExpirationClockWaiters(t, fixture.clock, 3)

	resultLease, err := fixture.searches.AcquireResultsFor(
		context.Background(),
		fixture.access,
		searchJobID,
	)
	if err != nil {
		t.Fatalf("AcquireResultsFor: %v", err)
	}
	resultLeaseClosed := false
	defer func() {
		if !resultLeaseClosed {
			_ = resultLease.Close()
		}
	}()
	grant, err := fixture.exports.CreateDownloadGrant(context.Background(), fixture.access, exportJobID)
	if err != nil {
		t.Fatalf("CreateDownloadGrant: %v", err)
	}
	unredeemedGrant, err := fixture.exports.CreateDownloadGrant(context.Background(), fixture.access, exportJobID)
	if err != nil {
		t.Fatalf("CreateDownloadGrant(unredeemed): %v", err)
	}
	download, err := fixture.exports.RedeemDownload(context.Background(), grant.Token)
	if err != nil {
		t.Fatalf("RedeemDownload: %v", err)
	}
	downloadClosed := false
	defer func() {
		if !downloadClosed {
			_ = download.Close()
		}
	}()

	expiry := completedSearch.ExpiresAt
	fixture.clock.Set(expiry.Add(-time.Nanosecond))
	if changed := fixture.searches.Cleanup(); changed != 0 {
		t.Fatalf("search Cleanup(before expiry) changed %d jobs, want 0", changed)
	}
	if err := fixture.exports.Cleanup(context.Background()); err != nil {
		t.Fatalf("export Cleanup(before expiry): %v", err)
	}
	if got, err := fixture.searches.GetFor(fixture.access, searchJobID); err != nil ||
		got.State != searchjobs.StateCompleted {
		t.Fatalf("search one nanosecond before expiry = (%+v, %v)", got, err)
	}
	if got, err := fixture.searches.GetFor(fixture.access, diagnosticJobID); err != nil ||
		got.State != searchjobs.StateFailed {
		t.Fatalf("diagnostic one nanosecond before expiry = (%+v, %v)", got, err)
	}
	if got, err := fixture.exports.Get(context.Background(), fixture.access, exportJobID); err != nil ||
		got.State != exportjobs.StateCompleted || got.Artifact == nil {
		t.Fatalf("export one nanosecond before expiry = (%+v, %v)", got, err)
	}
	assertExpirationSocketPong(t, searchConnection, "search-before-expiry")
	assertExpirationSocketPong(t, exportConnection, "export-before-expiry")
	assertExpirationSocketPong(t, diagnosticConnection, "diagnostic-before-expiry")
	waitForExpirationClockWaiters(t, fixture.clock, 3)

	fixture.clock.Set(expiry)
	fixture.searches.Cleanup()
	if err := fixture.exports.Cleanup(context.Background()); err != nil {
		t.Fatalf("export Cleanup(at expiry): %v", err)
	}

	expiredSearchEvents := readExpirationProjectionUntil(
		t,
		searchConnection,
		searchSubscription,
		func(event *opensplunkv1.SearchWebSocketEvent) bool {
			return event.GetSearchTerminal() != nil
		},
	)
	if len(expiredSearchEvents) != 4 {
		t.Fatalf("expired retained-result projection = %+v, want exactly four frames", expiredSearchEvents)
	}
	expiredSearchState := expiredSearchEvents[0].GetSearchStateChanged()
	expiredSearchProgress := expiredSearchEvents[1].GetSearchProgress()
	expiredSearchPreview := expiredSearchEvents[2].GetResultPreview()
	expiredSearchTerminal := expiredSearchEvents[3].GetSearchTerminal()
	if expiredSearchState.GetSearchJobId() != searchJobID ||
		expiredSearchState.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED ||
		expiredSearchProgress == nil ||
		expiredSearchPreview.GetSearchJobId() != searchJobID ||
		expiredSearchPreview.GetUpdateMode() != opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET ||
		len(expiredSearchPreview.GetRows()) != 0 ||
		expiredSearchPreview.GetTruncated() ||
		expiredSearchTerminal.GetSearchJobId() != searchJobID ||
		expiredSearchTerminal.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED ||
		expiredSearchTerminal.GetFailure() != nil {
		t.Fatalf("expired retained-result projection = %+v", expiredSearchEvents)
	}

	expiredExportEvents := readExpirationProjectionUntil(
		t,
		exportConnection,
		exportSubscription,
		func(event *opensplunkv1.SearchWebSocketEvent) bool {
			return event.GetExportTerminal() != nil
		},
	)
	if len(expiredExportEvents) != 3 ||
		expiredExportEvents[0].GetExportStateChanged().GetState() != opensplunkv1.ExportJobState_EXPORT_JOB_STATE_EXPIRED ||
		expiredExportEvents[1].GetExportProgress() == nil ||
		expiredExportEvents[2].GetExportTerminal().GetState() != opensplunkv1.ExportJobState_EXPORT_JOB_STATE_EXPIRED ||
		expiredExportEvents[2].GetExportTerminal().GetArtifact() != nil {
		t.Fatalf("expired export projection = %+v", expiredExportEvents)
	}
	staleArtifactConnection := fixture.dial()
	defer staleArtifactConnection.Close()
	writeSearchWebSocketCommand(
		t,
		staleArtifactConnection,
		searchWebSocketTargetSubscribeCommand(
			"stale-artifact-replay",
			"stale-artifact-subscription",
			&opensplunkv1.JobTarget{
				Target: &opensplunkv1.JobTarget_ExportJobId{ExportJobId: exportJobID},
			},
			staleArtifactCheckpoint,
			false,
			nil,
		),
	)
	staleArtifactAckEvent := readSearchWebSocketEvent(t, staleArtifactConnection)
	staleArtifactAck := staleArtifactAckEvent.GetSubscriptionAcknowledged()
	if staleArtifactAck == nil ||
		staleArtifactAck.GetRequestId() != "stale-artifact-replay" ||
		staleArtifactAck.GetSubscriptionId() != "stale-artifact-subscription" ||
		staleArtifactAck.GetTarget().GetExportJobId() != exportJobID ||
		staleArtifactAck.GetReplayWillFollow() {
		t.Fatalf("stale-artifact replay acknowledgment = %+v", staleArtifactAckEvent)
	}
	staleArtifactResync := readSearchWebSocketEvent(t, staleArtifactConnection)
	if staleArtifactResync.GetSequence() != 0 ||
		staleArtifactResync.GetResynchronizationRequired().GetReason() !=
			opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED ||
		staleArtifactResync.GetResynchronizationRequired().GetTarget().GetExportJobId() != exportJobID {
		t.Fatalf("stale-artifact replay response = %+v", staleArtifactResync)
	}

	expiredDiagnosticEvents := readExpirationProjectionUntil(
		t,
		diagnosticConnection,
		diagnosticSubscription,
		func(event *opensplunkv1.SearchWebSocketEvent) bool {
			return event.GetSearchTerminal() != nil
		},
	)
	expiredDiagnosticTerminal := expiredDiagnosticEvents[len(expiredDiagnosticEvents)-1].GetSearchTerminal()
	if len(expiredDiagnosticEvents) != 3 ||
		expiredDiagnosticEvents[0].GetSearchStateChanged().GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED ||
		expiredDiagnosticEvents[1].GetSearchProgress() == nil ||
		expiredDiagnosticTerminal.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED ||
		len(expiredDiagnosticTerminal.GetFailure().GetDiagnostics()) == 0 {
		t.Fatalf("expired diagnostic projection = %+v", expiredDiagnosticEvents)
	}

	expiredSearch, err := fixture.searches.GetFor(fixture.access, searchJobID)
	if err != nil || expiredSearch.State != searchjobs.StateExpired || expiredSearch.Schema != nil {
		t.Fatalf("expired search snapshot = (%+v, %v)", expiredSearch, err)
	}
	if _, err := fixture.searches.ResultsFor(
		fixture.access,
		searchJobID,
		searchjobs.PageRequest{Limit: 1},
	); !errors.Is(err, searchjobs.ErrExpired) {
		t.Fatalf("ResultsFor(expired) = %v, want ErrExpired", err)
	}
	if _, err := fixture.searches.AcquireResultsFor(
		context.Background(),
		fixture.access,
		searchJobID,
	); !errors.Is(err, searchjobs.ErrExpired) {
		t.Fatalf("AcquireResultsFor(expired) = %v, want ErrExpired", err)
	}
	expiredExport, err := fixture.exports.Get(context.Background(), fixture.access, exportJobID)
	if err != nil || expiredExport.State != exportjobs.StateExpired || expiredExport.Artifact != nil {
		t.Fatalf("expired export snapshot = (%+v, %v)", expiredExport, err)
	}
	if _, err := fixture.exports.CreateDownloadGrant(
		context.Background(),
		fixture.access,
		exportJobID,
	); !errors.Is(err, exportjobs.ErrSourceExpired) {
		t.Fatalf("CreateDownloadGrant(expired) = %v, want ErrSourceExpired", err)
	}
	if _, err := fixture.exports.RedeemDownload(
		context.Background(),
		unredeemedGrant.Token,
	); !errors.Is(err, exportjobs.ErrInvalidDownloadGrant) {
		t.Fatalf("RedeemDownload(unredeemed expired grant) = %v, want ErrInvalidDownloadGrant", err)
	}
	waitForExpirationClockWaiters(t, fixture.clock, 3)

	fixture.clock.SetWithoutWake(expiry.Add(expirationRetention))
	if changed := fixture.searches.Cleanup(); changed != 1 {
		t.Fatalf("search Cleanup(at tombstone deadline) changed %d jobs, want 1", changed)
	}
	if err := fixture.exports.Cleanup(context.Background()); err != nil {
		t.Fatalf("export Cleanup(at tombstone deadline): %v", err)
	}
	fixture.clock.WakeAll()
	assertExpirationSocketClosesWithoutEvent(t, diagnosticConnection, "diagnostic websocket")
	if _, err := fixture.searches.GetFor(fixture.access, diagnosticJobID); !errors.Is(err, searchjobs.ErrNotFound) {
		t.Fatalf("diagnostic GetFor(after tombstone removal) = %v, want ErrNotFound", err)
	}
	if got, err := fixture.searches.GetFor(fixture.access, searchJobID); err != nil || got.State != searchjobs.StateExpired {
		t.Fatalf("pinned search tombstone = (%+v, %v)", got, err)
	}
	if got, err := fixture.exports.Get(context.Background(), fixture.access, exportJobID); err != nil ||
		got.State != exportjobs.StateExpired {
		t.Fatalf("pinned export tombstone = (%+v, %v)", got, err)
	}
	assertExpirationSocketPong(t, searchConnection, "search-pin-alive")
	assertExpirationSocketPong(t, exportConnection, "export-pin-alive")
	waitForExpirationClockWaiters(t, fixture.clock, 2)

	row, ok, err := resultLease.Next(context.Background())
	if err != nil || !ok || row.Ordinal != 0 {
		t.Fatalf("pinned result Next(after expiry) = (%+v, %t, %v)", row, ok, err)
	}
	if message, _ := row.Values[0].String(); message != "retained until final lease" {
		t.Fatalf("pinned result row = %q", message)
	}
	artifactBytes, err := io.ReadAll(download)
	if err != nil || !strings.Contains(string(artifactBytes), "retained until final lease") {
		t.Fatalf("pinned artifact after expiry = %q, %v", artifactBytes, err)
	}

	if err := resultLease.Close(); err != nil {
		t.Fatalf("close result lease: %v", err)
	}
	resultLeaseClosed = true
	if err := download.Close(); err != nil {
		t.Fatalf("close download lease: %v", err)
	}
	downloadClosed = true
	if changed := fixture.searches.Cleanup(); changed != 0 {
		t.Fatalf("search Cleanup(after final lease) changed %d jobs, want 0", changed)
	}
	if err := fixture.exports.Cleanup(context.Background()); err != nil {
		t.Fatalf("export Cleanup(after final lease): %v", err)
	}
	if _, err := fixture.searches.GetFor(fixture.access, searchJobID); !errors.Is(err, searchjobs.ErrNotFound) {
		t.Fatalf("search GetFor(after final lease) = %v, want ErrNotFound", err)
	}
	if _, err := fixture.searches.ResultsFor(
		fixture.access,
		searchJobID,
		searchjobs.PageRequest{Limit: 1},
	); !errors.Is(err, searchjobs.ErrNotFound) {
		t.Fatalf("search ResultsFor(after final lease) = %v, want ErrNotFound", err)
	}
	if _, err := fixture.exports.Get(
		context.Background(),
		fixture.access,
		exportJobID,
	); !errors.Is(err, exportjobs.ErrNotFound) {
		t.Fatalf("export Get(after final lease) = %v, want ErrNotFound", err)
	}
	if _, err := fixture.exports.CreateDownloadGrant(
		context.Background(),
		fixture.access,
		exportJobID,
	); !errors.Is(err, exportjobs.ErrNotFound) {
		t.Fatalf("CreateDownloadGrant(after tombstone removal) = %v, want ErrNotFound", err)
	}

	fixture.clock.WakeAll()
	assertExpirationSocketClosesWithoutEvent(t, searchConnection, "search websocket")
	assertExpirationSocketClosesWithoutEvent(t, exportConnection, "export websocket")
	assertExpirationSocketClosesWithoutEvent(t, staleArtifactConnection, "stale-artifact websocket")
	waitForExpirationClockWaiters(t, fixture.clock, 0)
	assertExpirationPollersQuiesce(t, fixture.searchSnapshots, fixture.exportSnapshots)
	if pending := fixture.clock.pendingWaiters(); pending != 0 {
		t.Fatalf("expiration clock retained %d scheduled waiters", pending)
	}
	if calls, exits := fixture.executor.calls.Load(), fixture.executor.exits.Load(); calls != 1 || exits != 1 {
		t.Fatalf("executor lifecycle = calls %d exits %d, want 1/1", calls, exits)
	}
}
