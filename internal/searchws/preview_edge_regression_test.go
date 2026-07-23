package searchws

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type previewEdgeDemandSnapshots struct {
	mu     sync.Mutex
	job    searchjobs.Job
	limits []int
}

type previewEdgeBarrierSnapshots struct {
	jobs     map[string]searchjobs.Job
	entered  chan string
	releases map[string]chan struct{}
}

type previewEdgeProjectionGateSnapshots struct {
	entered chan string
	release <-chan struct{}
	active  atomic.Int32
	maximum atomic.Int32
}

func (reader *previewEdgeProjectionGateSnapshots) GetFor(
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	job := scopedSearchJob(id)
	if scope.TenantID != job.TenantID || scope.OwnerID != job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	active := reader.active.Add(1)
	defer reader.active.Add(-1)
	for {
		maximum := reader.maximum.Load()
		if active <= maximum || reader.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	reader.entered <- id
	<-reader.release
	return job, nil
}

func (*previewEdgeProjectionGateSnapshots) PreviewForBytes(
	searchjobs.AccessScope,
	string,
	int,
	uint64,
) (searchjobs.PreviewSnapshot, error) {
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
}

func (*previewEdgeProjectionGateSnapshots) MaximumPreviewRows() uint32 { return 1 }

func (reader *previewEdgeBarrierSnapshots) GetFor(
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	job, ok := reader.jobs[id]
	if !ok || scope.TenantID != job.TenantID || scope.OwnerID != job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	reader.entered <- id
	<-reader.releases[id]
	return cloneSearchSnapshot(job), nil
}

func (*previewEdgeBarrierSnapshots) PreviewFor(
	searchjobs.AccessScope,
	string,
	int,
) (searchjobs.PreviewSnapshot, error) {
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
}

func (reader *previewEdgeBarrierSnapshots) PreviewForBytes(
	scope searchjobs.AccessScope,
	id string,
	limit int,
	_ uint64,
) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (*previewEdgeBarrierSnapshots) MaximumPreviewRows() uint32 { return 1 }

func (reader *previewEdgeDemandSnapshots) GetFor(scope searchjobs.AccessScope, id string) (searchjobs.Job, error) {
	if id != reader.job.ID || scope.TenantID != reader.job.TenantID || scope.OwnerID != reader.job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	return cloneSearchSnapshot(reader.job), nil
}

func (reader *previewEdgeDemandSnapshots) PreviewFor(
	scope searchjobs.AccessScope,
	id string,
	limit int,
) (searchjobs.PreviewSnapshot, error) {
	job, err := reader.GetFor(scope, id)
	if err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	reader.mu.Lock()
	reader.limits = append(reader.limits, limit)
	reader.mu.Unlock()
	return searchjobs.PreviewSnapshot{Job: job, Revision: job.Version}, nil
}

func (reader *previewEdgeDemandSnapshots) PreviewForBytes(
	scope searchjobs.AccessScope,
	id string,
	limit int,
	_ uint64,
) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (*previewEdgeDemandSnapshots) MaximumPreviewRows() uint32 { return 32 }

func (reader *previewEdgeDemandSnapshots) requestedLimits() []int {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return append([]int(nil), reader.limits...)
}

func previewEdgeProjection(
	version uint64,
	incarnation time.Time,
	schemaID string,
	includePreview bool,
) targetProjection {
	events := []*opensplunkv1.SearchWebSocketEvent{
		{Payload: &opensplunkv1.SearchWebSocketEvent_SearchStateChanged{
			SearchStateChanged: &opensplunkv1.SearchJobStateChanged{
				SearchJobId: "search", State: opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, StateVersion: 1,
			},
		}},
		{Payload: &opensplunkv1.SearchWebSocketEvent_SearchProgress{
			SearchProgress: &opensplunkv1.SearchProgress{
				Phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_EXECUTING,
			},
		}},
		{Payload: &opensplunkv1.SearchWebSocketEvent_ResultSchemaAvailable{
			ResultSchemaAvailable: &opensplunkv1.ResultSchemaAvailable{
				SearchJobId: "search",
				Schema: &opensplunkv1.ResultSchema{
					SchemaId: schemaID, Revision: 1,
					ResultKind: opensplunkv1.ResultSetKind_RESULT_SET_KIND_EVENTS,
				},
			},
		}},
	}
	previewRows := uint32(0)
	if includePreview {
		previewRows = 1
		events = append(events, &opensplunkv1.SearchWebSocketEvent{
			Payload: &opensplunkv1.SearchWebSocketEvent_ResultPreview{
				ResultPreview: &opensplunkv1.ResultPreview{
					SearchJobId: "search", SchemaId: schemaID, PreviewRevision: 1,
					UpdateMode: opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET,
					Rows:       []*opensplunkv1.ResultRow{{RowId: "row", Ordinal: 0}},
				},
			},
		})
	}
	return targetProjection{
		version: version, incarnation: incarnation, previewRows: previewRows, events: events,
	}
}

func previewEdgeTerminalProjection(version uint64, incarnation time.Time) targetProjection {
	return targetProjection{
		version: version, incarnation: incarnation, previewRows: 1,
		invalidatesPreview: true, terminal: true,
		events: []*opensplunkv1.SearchWebSocketEvent{{
			Payload: &opensplunkv1.SearchWebSocketEvent_SearchTerminal{
				SearchTerminal: &opensplunkv1.SearchJobTerminal{
					SearchJobId:  "search",
					State:        opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
					StateVersion: version,
				},
			},
		}},
	}
}

func previewEdgeSocketConnection(t *testing.T, service *Service) *connection {
	t.Helper()
	serverSocket := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		socket, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(response, request, nil)
		if err != nil {
			return
		}
		serverSocket <- socket
	}))
	client, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"), nil,
	)
	if err != nil {
		server.Close()
		t.Fatalf("dial test WebSocket = %v", err)
	}
	var socket *websocket.Conn
	select {
	case socket = <-serverSocket:
	case <-time.After(time.Second):
		_ = client.Close()
		server.Close()
		t.Fatal("test WebSocket server did not publish its connection")
	}
	connection := newConnection(service, socket)
	t.Cleanup(func() {
		connection.hardClose()
		_ = client.Close()
		server.Close()
	})
	return connection
}

func TestPreviewDemandRemovalStoresReplayableClearUntilFullProjection(t *testing.T) {
	service := adversarialNewService(t, nil)
	target := adversarialNewTarget(service, "preview-gap")
	incarnation := adversarialNow.Add(-time.Hour)

	adversarialApply(t, target, previewEdgeProjection(1, incarnation, "schema", true), true)
	target.mu.Lock()
	previewSequence := target.current[eventCategoryPreview].sequence
	target.mu.Unlock()

	metadataOnly := previewEdgeProjection(2, incarnation, "schema", false)
	adversarialApply(t, target, metadataOnly, false)
	target.mu.Lock()
	cleared := target.current[eventCategoryPreview]
	if cleared == nil {
		target.mu.Unlock()
		t.Fatal("metadata-only projection did not retain a replayable preview clear")
	}
	if target.latest != cleared.sequence || cleared.sequence <= previewSequence {
		target.mu.Unlock()
		t.Fatalf("metadata-only clear/latest = %d/%d, want a sequence after %d", cleared.sequence, target.latest, previewSequence)
	}
	if target.currentEventsContinuousLocked() {
		target.mu.Unlock()
		t.Fatal("snapshot was considered continuous across its replaced preview category head")
	}
	clearData := append([]byte(nil), cleared.data...)
	target.mu.Unlock()
	var clearEvent opensplunkv1.SearchWebSocketEvent
	if err := proto.Unmarshal(clearData, &clearEvent); err != nil {
		t.Fatal(err)
	}
	clearPreview := clearEvent.GetResultPreview()
	if clearPreview == nil || len(clearPreview.GetRows()) != 0 || clearPreview.GetTruncated() {
		t.Fatalf("metadata-only preview clear = %+v", clearPreview)
	}

	adversarialApply(t, target, metadataOnly, true)
	target.mu.Lock()
	defer target.mu.Unlock()
	if !target.currentEventsContinuousLocked() {
		t.Fatal("forced full projection did not restore a continuous current snapshot")
	}
	var maximum uint64
	for _, event := range target.current {
		if event.sequence > maximum {
			maximum = event.sequence
		}
	}
	if maximum != target.latest {
		t.Fatalf("forced full projection ends at %d, target latest is %d", maximum, target.latest)
	}
}

func TestPreviewDemandClearPrecedesTerminalReplay(t *testing.T) {
	service := adversarialNewService(t, nil)
	target := adversarialNewTarget(service, "preview-terminal-replay")
	incarnation := adversarialNow.Add(-time.Hour)
	adversarialApply(t, target, previewEdgeProjection(1, incarnation, "schema", true), true)
	target.mu.Lock()
	previewSequence := target.current[eventCategoryPreview].sequence
	target.epochEstablished = true
	target.mu.Unlock()

	adversarialApply(t, target, previewEdgeProjection(2, incarnation, "schema", false), false)
	terminal := previewEdgeTerminalProjection(3, incarnation)
	terminal.invalidatesPreview = false // Model a zero-demand terminal projection.
	terminal.previewRows = 0
	adversarialApply(t, target, terminal, false)

	target.mu.Lock()
	frames, continuous := target.replayAfterLocked(previewSequence)
	target.mu.Unlock()
	if !continuous {
		t.Fatal("terminal replay unexpectedly required resynchronization")
	}
	var clearIndex, terminalIndex = -1, -1
	for index, event := range adversarialDecode(t, frames) {
		if preview := event.GetResultPreview(); preview != nil && len(preview.GetRows()) == 0 && !preview.GetTruncated() {
			clearIndex = index
		}
		if event.GetSearchTerminal() != nil {
			terminalIndex = index
		}
	}
	if clearIndex < 0 || terminalIndex < 0 || clearIndex >= terminalIndex {
		t.Fatalf("terminal replay did not clear preview first: clear=%d terminal=%d", clearIndex, terminalIndex)
	}
}

func TestCurrentBootstrapDoesNotRebroadcastDiscontinuousStateToExistingSubscriber(t *testing.T) {
	reader := &adversarialSearchSnapshots{}
	service := adversarialNewService(t, func(config *Config) { config.Searches = reader })
	target := adversarialNewTarget(service, "existing-subscriber")
	adversarialApply(t, target, adversarialProjection(
		1,
		adversarialNow.Add(-time.Hour),
		false,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING,
		1,
		true,
	), true)
	target.mu.Lock()
	target.latest++ // Model a removed category at the journal tail.
	target.mu.Unlock()

	connection := newConnection(service, nil)
	defer connection.cancel()
	subscription := adversarialAttach(target, connection, "existing")
	defer adversarialDetach(subscription)

	if _, err := target.refreshForSubscription(context.Background(), 0, true); err != nil {
		t.Fatalf("refreshForSubscription() = %v", err)
	}
	if calls := reader.calls.Load(); calls != 0 {
		t.Fatalf("current bootstrap reloaded a shared discontinuous target %d times", calls)
	}
	if events := adversarialDrain(t, connection); len(events) != 0 {
		t.Fatalf("current bootstrap rebroadcast %d events to an existing subscriber", len(events))
	}
	target.mu.Lock()
	continuous := target.currentEventsContinuousLocked()
	target.mu.Unlock()
	if continuous {
		t.Fatal("shared target was unexpectedly rebuilt instead of leaving the joiner to resynchronize")
	}
}

func TestCurrentBootstrapRevalidatesExclusivityAfterSnapshotLoad(t *testing.T) {
	job := scopedSearchJob("bootstrap-race")
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	reader := &previewEdgeBarrierSnapshots{
		jobs:     map[string]searchjobs.Job{job.ID: job},
		entered:  make(chan string, 1),
		releases: map[string]chan struct{}{job.ID: release},
	}
	service := adversarialNewService(t, func(config *Config) { config.Searches = reader })
	target := adversarialNewTarget(service, job.ID)
	projection, err := projectSearch(job, adversarialNow)
	if err != nil {
		t.Fatal(err)
	}
	adversarialApply(t, target, projection, true)
	target.mu.Lock()
	target.latest++ // Force a current-state bootstrap load while no subscriber is installed.
	target.mu.Unlock()

	result := make(chan error, 1)
	go func() {
		_, refreshErr := target.refreshForSubscription(context.Background(), 0, true)
		result <- refreshErr
	}()
	select {
	case <-reader.entered:
	case <-time.After(time.Second):
		t.Fatal("bootstrap refresh did not enter the snapshot reader")
	}

	connection := newConnection(service, nil)
	defer connection.cancel()
	target.publishMu.Lock()
	subscription := adversarialAttach(target, connection, "installed-during-load")
	target.publishMu.Unlock()
	defer adversarialDetach(subscription)
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("refreshForSubscription() = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bootstrap refresh did not finish")
	}
	if events := adversarialDrain(t, connection); len(events) != 0 {
		t.Fatalf("stale bootstrap exclusivity rebroadcast %d events", len(events))
	}
}

func TestProjectionGateBoundsConcurrencyAndHonorsCancellation(t *testing.T) {
	t.Run("bounded", func(t *testing.T) {
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		reader := &previewEdgeProjectionGateSnapshots{
			entered: make(chan string, 6),
			release: release,
		}
		service := adversarialNewService(t, func(config *Config) {
			config.Searches = reader
			config.MaximumConcurrentProjections = 2
		})
		results := make(chan error, 6)
		for index := 0; index < cap(results); index++ {
			id := fmt.Sprintf("projection-%d", index)
			go func() {
				_, err := service.loadProjection(
					context.Background(), targetKey{kind: targetKindSearch, id: id}, 0,
				)
				results <- err
			}()
		}
		for index := 0; index < 2; index++ {
			select {
			case <-reader.entered:
			case <-time.After(time.Second):
				t.Fatal("projection did not enter the configured concurrency gate")
			}
		}
		select {
		case id := <-reader.entered:
			t.Fatalf("projection %q exceeded the configured concurrency gate", id)
		case <-time.After(25 * time.Millisecond):
		}
		releaseOnce.Do(func() { close(release) })
		for index := 0; index < cap(results); index++ {
			select {
			case err := <-results:
				if err != nil {
					t.Fatalf("loadProjection() = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("projection did not finish after the gate was released")
			}
		}
		if maximum := reader.maximum.Load(); maximum != 2 {
			t.Fatalf("maximum concurrent snapshot reads = %d, want 2", maximum)
		}
	})

	t.Run("full publication lifecycle", func(t *testing.T) {
		release := make(chan struct{})
		close(release)
		reader := &previewEdgeProjectionGateSnapshots{
			entered: make(chan string, 2),
			release: release,
		}
		service := adversarialNewService(t, func(config *Config) {
			config.Searches = reader
			config.MaximumConcurrentProjections = 1
		})
		firstTarget := adversarialNewTarget(service, "projection-lifecycle-first")
		secondTarget := adversarialNewTarget(service, "projection-lifecycle-second")
		firstTarget.publishMu.Lock()
		var unlockOnce sync.Once
		unlock := func() { unlockOnce.Do(firstTarget.publishMu.Unlock) }
		t.Cleanup(unlock)

		first := make(chan error, 1)
		go func() {
			_, err := firstTarget.refreshCurrentProjection(context.Background())
			first <- err
		}()
		select {
		case id := <-reader.entered:
			if id != firstTarget.key.id {
				t.Fatalf("first snapshot read = %q, want %q", id, firstTarget.key.id)
			}
		case <-time.After(time.Second):
			t.Fatal("first projection did not enter its snapshot read")
		}

		second := make(chan error, 1)
		go func() {
			_, err := secondTarget.refreshCurrentProjection(context.Background())
			second <- err
		}()
		select {
		case id := <-reader.entered:
			t.Fatalf("projection %q entered before the first publication finished", id)
		case <-time.After(25 * time.Millisecond):
		}
		unlock()
		if err := <-first; err != nil {
			t.Fatalf("first refreshCurrentProjection() = %v", err)
		}
		select {
		case id := <-reader.entered:
			if id != secondTarget.key.id {
				t.Fatalf("second snapshot read = %q, want %q", id, secondTarget.key.id)
			}
		case <-time.After(time.Second):
			t.Fatal("second projection did not enter after publication released the permit")
		}
		if err := <-second; err != nil {
			t.Fatalf("second refreshCurrentProjection() = %v", err)
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		reader := &previewEdgeProjectionGateSnapshots{
			entered: make(chan string, 2),
			release: release,
		}
		service := adversarialNewService(t, func(config *Config) {
			config.Searches = reader
			config.MaximumConcurrentProjections = 1
		})
		first := make(chan error, 1)
		go func() {
			_, err := service.loadProjection(
				context.Background(), targetKey{kind: targetKindSearch, id: "projection-holder"}, 0,
			)
			first <- err
		}()
		select {
		case <-reader.entered:
		case <-time.After(time.Second):
			t.Fatal("first projection did not acquire the gate")
		}

		ctx, cancel := context.WithCancel(context.Background())
		second := make(chan error, 1)
		go func() {
			_, err := service.loadProjection(
				ctx, targetKey{kind: targetKindSearch, id: "projection-waiter"}, 0,
			)
			second <- err
		}()
		cancel()
		select {
		case err := <-second:
			if err != context.Canceled {
				t.Fatalf("canceled loadProjection() = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("canceled projection remained blocked on the gate")
		}
		select {
		case id := <-reader.entered:
			t.Fatalf("canceled projection %q entered the snapshot reader", id)
		default:
		}
		releaseOnce.Do(func() { close(release) })
		if err := <-first; err != nil {
			t.Fatalf("first loadProjection() = %v", err)
		}
	})
}

func TestCurrentSubscriptionRechecksContinuityAfterRefreshBarrier(t *testing.T) {
	jobs := make(map[string]searchjobs.Job, 2)
	releases := make(map[string]chan struct{}, 2)
	for _, id := range []string{"barrier-first", "barrier-second"} {
		job := scopedSearchJob(id)
		job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
		jobs[id] = job
		releases[id] = make(chan struct{})
	}
	reader := &previewEdgeBarrierSnapshots{
		jobs: jobs, entered: make(chan string, 2), releases: releases,
	}
	service := adversarialNewService(t, func(config *Config) {
		config.Searches = reader
		config.MaximumPreviewRows = 1
	})
	targets := make(map[string]*targetState, len(jobs))
	for id, job := range jobs {
		target := adversarialNewTarget(service, id)
		projection, err := projectSearch(job, adversarialNow)
		if err != nil {
			t.Fatal(err)
		}
		adversarialApply(t, target, projection, true)
		target.mu.Lock()
		target.latest++ // Model a just-removed category tail.
		target.mu.Unlock()
		targets[id] = target
	}
	released := map[string]*sync.Once{
		"barrier-first": {}, "barrier-second": {},
	}
	releaseTarget := func(id string) {
		released[id].Do(func() { close(releases[id]) })
	}
	t.Cleanup(func() {
		for id := range releases {
			releaseTarget(id)
		}
	})

	connection := newConnection(service, nil)
	defer connection.cancel()
	result := make(chan *commandFailure, 1)
	go func() {
		result <- connection.subscribe("barrier", &opensplunkv1.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunkv1.SearchSubscription{
				{SubscriptionId: "first", Target: targetKey{kind: targetKindSearch, id: "barrier-first"}.protobuf()},
				{SubscriptionId: "second", Target: targetKey{kind: targetKindSearch, id: "barrier-second"}.protobuf()},
			},
		})
	}()

	firstID := <-reader.entered
	releaseTarget(firstID)
	secondID := <-reader.entered
	if secondID == firstID {
		t.Fatalf("refresh barrier entered target %q twice", firstID)
	}

	partial, err := projectSearch(jobs[firstID], adversarialNow)
	if err != nil {
		t.Fatal(err)
	}
	partial.version++
	partial.events[1].GetSearchProgress().ProducedRows++
	adversarialApply(t, targets[firstID], partial, false)
	releaseTarget(secondID)
	if failure := <-result; failure != nil {
		t.Fatalf("subscribe() = %v", failure)
	}

	firstSubscriptionID := "first"
	if firstID == "barrier-second" {
		firstSubscriptionID = "second"
	}
	var resynchronized bool
	for _, event := range adversarialDrain(t, connection) {
		if event.GetSubscriptionId() != firstSubscriptionID {
			continue
		}
		if required := event.GetResynchronizationRequired(); required != nil {
			resynchronized = required.GetReason() == opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED
		}
	}
	if !resynchronized {
		t.Fatalf("subscription %q received a gapped current snapshot instead of resynchronizing", firstSubscriptionID)
	}
	connection.removeAllSubscriptions()
}

func TestPendingPreviewDemandCannotBeRegressedByMetadataPoll(t *testing.T) {
	job := scopedSearchJob("pending-preview")
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message", Kind: searchjobs.ValueKindString,
	}}}
	reader := &previewEdgeDemandSnapshots{job: job}
	service := adversarialNewService(t, func(config *Config) {
		config.Searches = reader
		config.MaximumPreviewRows = 32
	})
	target := adversarialNewTarget(service, job.ID)
	pending := &subscription{id: "pending", target: target, previewRows: 17}

	target.mu.Lock()
	target.addPendingPreviewLocked(pending)
	if maximum := target.maximumPreviewRowsLocked(); maximum != pending.previewRows {
		target.mu.Unlock()
		t.Fatalf("maximumPreviewRowsLocked() = %d, want pending demand %d", maximum, pending.previewRows)
	}
	target.mu.Unlock()

	if _, err := target.refreshCurrentProjection(context.Background()); err != nil {
		t.Fatalf("metadata refresh = %v", err)
	}
	if limits := reader.requestedLimits(); len(limits) != 1 || limits[0] != int(pending.previewRows) {
		t.Fatalf("metadata refresh preview limits = %v, want [%d]", limits, pending.previewRows)
	}
	target.mu.Lock()
	projected := target.projectedPreviews
	target.mu.Unlock()
	if projected != pending.previewRows {
		t.Fatalf("metadata refresh projected preview rows = %d, want %d", projected, pending.previewRows)
	}
}

func TestTerminalPreviewInvalidationRemainsAuthoritativeUnderPressure(t *testing.T) {
	t.Run("queue pressure", func(t *testing.T) {
		service := adversarialNewService(t, func(config *Config) {
			config.MaximumReplayBytes = 8 * minimumFrameBytes
			config.MaximumTotalReplayBytes = 32 * minimumFrameBytes
		})
		incarnation := adversarialNow.Add(-time.Hour)
		schemaID := strings.Repeat("s", 256)

		probe := adversarialNewTarget(service, "probe-size")
		adversarialApply(t, probe, previewEdgeProjection(1, incarnation, schemaID, true), true)
		adversarialApply(t, probe, previewEdgeTerminalProjection(2, incarnation), false)
		probe.mu.Lock()
		resetData := probe.current[eventCategoryPreview].data
		terminalData := probe.current[eventCategorySearchTerminal].data
		probe.mu.Unlock()
		resetBytes, err := stampedFrameSize(len(resetData), "pressure")
		if err != nil {
			t.Fatal(err)
		}
		terminalBytes, err := stampedFrameSize(len(terminalData), "pressure")
		if err != nil {
			t.Fatal(err)
		}
		if resetBytes <= terminalBytes {
			t.Fatalf("test fixture reset bytes = %d, want greater than terminal bytes %d", resetBytes, terminalBytes)
		}

		target := adversarialNewTarget(service, "queue-edge")
		adversarialApply(t, target, previewEdgeProjection(1, incarnation, schemaID, true), true)
		connection := previewEdgeSocketConnection(t, service)
		adversarialAttach(target, connection, "pressure").previewRows = 1

		// Leave enough global budget for the terminal, but not for its
		// authoritative empty preview RESET.
		service.config.maximumTotalQueuedBytes = terminalBytes
		adversarialApply(t, target, previewEdgeTerminalProjection(2, incarnation), false)

		connection.queueMu.Lock()
		closed := connection.hardClosed
		connection.queueMu.Unlock()
		if closed {
			return
		}
		events := adversarialDrain(t, connection)
		for _, event := range events {
			if event.GetResultPreview() != nil || event.GetResynchronizationRequired() != nil {
				return
			}
		}
		t.Fatalf("terminal invalidation silently retained prior preview rows under queue pressure: %+v", events)
	})

	t.Run("replay pressure", func(t *testing.T) {
		service := adversarialNewService(t, func(config *Config) {
			config.MaximumReplayBytes = 8 * minimumFrameBytes
			config.MaximumTotalReplayBytes = 16 * minimumFrameBytes
		})
		target := adversarialNewTarget(service, "replay-edge")
		incarnation := adversarialNow.Add(-time.Hour)
		adversarialApply(t, target, previewEdgeProjection(1, incarnation, "schema", true), true)
		connection := previewEdgeSocketConnection(t, service)
		adversarialAttach(target, connection, "pressure").previewRows = 1

		// Existing retained state was valid when written. Tightening this
		// injected limit makes the invalidation RESET impossible to retain and
		// exercises the resynchronization fallback.
		service.config.maximumReplayBytes = 1
		adversarialApply(t, target, previewEdgeTerminalProjection(2, incarnation), false)

		connection.queueMu.Lock()
		closed := connection.hardClosed
		connection.queueMu.Unlock()
		if closed {
			return
		}
		events := adversarialDrain(t, connection)
		for _, event := range events {
			if event.GetResultPreview() != nil || event.GetResynchronizationRequired() != nil {
				return
			}
		}
		t.Fatalf("terminal invalidation silently retained prior preview rows under replay pressure: %+v", events)
	})
}

func TestBoundedInitialFrameStagingOwnsGlobalQueueReservation(t *testing.T) {
	queuedBytes := func(service *Service) uint64 {
		service.queueBudgetMu.Lock()
		defer service.queueBudgetMu.Unlock()
		return service.queuedBytes
	}

	t.Run("release", func(t *testing.T) {
		service := adversarialNewService(t, nil)
		service.config.maximumTotalQueuedBytes = 100
		batch := newBoundedFrameBatch(service, 2)
		if result := batch.append(make([]byte, 80)); result != queueAccepted {
			t.Fatalf("first staged frame = %v, want accepted", result)
		}
		if result := batch.append(make([]byte, 21)); result != queuePressure {
			t.Fatalf("over-budget staged frame = %v, want pressure", result)
		}
		if got := queuedBytes(service); got != 80 {
			t.Fatalf("global queued bytes while staged = %d, want 80", got)
		}
		batch.release()
		batch.release()
		if got := queuedBytes(service); got != 0 {
			t.Fatalf("global queued bytes after idempotent release = %d, want 0", got)
		}
		if batch.bytes != 0 || batch.frames != nil {
			t.Fatalf("released batch retained state: bytes=%d frames=%v", batch.bytes, batch.frames)
		}
	})

	t.Run("transfer", func(t *testing.T) {
		service := adversarialNewService(t, nil)
		connection := newConnection(service, nil)
		defer connection.cancel()
		batch := newBoundedFrameBatch(service, 2)
		frames := [][]byte{make([]byte, 40), make([]byte, 60)}
		for index, frame := range frames {
			if result := batch.append(frame); result != queueAccepted {
				t.Fatalf("staged frame[%d] = %v, want accepted", index, result)
			}
		}
		if got := queuedBytes(service); got != 100 {
			t.Fatalf("global queued bytes before transfer = %d, want 100", got)
		}
		if result := connection.enqueueReservedBatchResult(batch); result != queueAccepted {
			t.Fatalf("reserved batch transfer = %v, want accepted", result)
		}
		if got := queuedBytes(service); got != 100 {
			t.Fatalf("global queued bytes after transfer = %d, want reservation unchanged at 100", got)
		}
		batch.release()
		if got := queuedBytes(service); got != 100 {
			t.Fatalf("transferred reservation was released by staging cleanup: %d", got)
		}
		for range frames {
			frame, _, _, state := connection.nextFrame()
			if state != writerFrame {
				t.Fatalf("transferred nextFrame() = %v, want writerFrame", state)
			}
			connection.completeFrame(uint64(len(frame)))
		}
		if got := queuedBytes(service); got != 0 {
			t.Fatalf("global queued bytes after transferred frames drained = %d, want 0", got)
		}
	})
}
