package searchws

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type mutableSearchSnapshots struct {
	mu    sync.Mutex
	jobs  map[string]searchjobs.Job
	calls map[string]int
}

func newMutableSearchSnapshots(jobs ...searchjobs.Job) *mutableSearchSnapshots {
	reader := &mutableSearchSnapshots{jobs: make(map[string]searchjobs.Job), calls: make(map[string]int)}
	for _, job := range jobs {
		reader.jobs[job.ID] = cloneSearchSnapshot(job)
	}
	return reader
}

func (reader *mutableSearchSnapshots) GetFor(scope searchjobs.AccessScope, id string) (searchjobs.Job, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.calls[id]++
	job, ok := reader.jobs[id]
	if !ok || job.TenantID != scope.TenantID || job.OwnerID != scope.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	return cloneSearchSnapshot(job), nil
}

func (reader *mutableSearchSnapshots) set(job searchjobs.Job) {
	reader.mu.Lock()
	reader.jobs[job.ID] = cloneSearchSnapshot(job)
	reader.mu.Unlock()
}

func (reader *mutableSearchSnapshots) delete(id string) {
	reader.mu.Lock()
	delete(reader.jobs, id)
	reader.mu.Unlock()
}

func (reader *mutableSearchSnapshots) callCount(id string) int {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.calls[id]
}

func cloneSearchSnapshot(job searchjobs.Job) searchjobs.Job {
	cloned := job
	cloned.RequestedIndexes = append([]string(nil), job.RequestedIndexes...)
	cloned.EffectiveIndexes = append([]string(nil), job.EffectiveIndexes...)
	if job.Schema != nil {
		schema := *job.Schema
		schema.Columns = append([]searchjobs.Column(nil), job.Schema.Columns...)
		cloned.Schema = &schema
	}
	if job.Failure != nil {
		failure := *job.Failure
		failure.Diagnostics = append([]searchjobs.Diagnostic(nil), job.Failure.Diagnostics...)
		cloned.Failure = &failure
	}
	return cloned
}

type notFoundExportSnapshots struct{}

func (notFoundExportSnapshots) Get(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
	return exportjobs.Job{}, exportjobs.ErrNotFound
}

type mutableExportSnapshots struct {
	mu   sync.Mutex
	jobs map[string]exportjobs.Job
}

func newMutableExportSnapshots(jobs ...exportjobs.Job) *mutableExportSnapshots {
	reader := &mutableExportSnapshots{jobs: make(map[string]exportjobs.Job)}
	for _, job := range jobs {
		reader.jobs[job.ID] = cloneExportSnapshot(job)
	}
	return reader
}

func (reader *mutableExportSnapshots) Get(ctx context.Context, scope searchjobs.AccessScope, id string) (exportjobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return exportjobs.Job{}, err
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	job, ok := reader.jobs[id]
	if !ok || scope.TenantID != "tenant" || scope.OwnerID != "owner" {
		return exportjobs.Job{}, exportjobs.ErrNotFound
	}
	return cloneExportSnapshot(job), nil
}

func (reader *mutableExportSnapshots) set(job exportjobs.Job) {
	reader.mu.Lock()
	reader.jobs[job.ID] = cloneExportSnapshot(job)
	reader.mu.Unlock()
}

func cloneExportSnapshot(job exportjobs.Job) exportjobs.Job {
	cloned := job
	cloned.Columns = append([]string(nil), job.Columns...)
	if job.Artifact != nil {
		artifact := *job.Artifact
		cloned.Artifact = &artifact
	}
	if job.Failure != nil {
		failure := *job.Failure
		cloned.Failure = &failure
	}
	return cloned
}

type webSocketFixture struct {
	t       *testing.T
	service *Service
	server  *httptest.Server
	reader  SearchSnapshots

	mu      sync.Mutex
	clients []*websocket.Conn
	once    sync.Once
}

func newWebSocketFixture(t *testing.T, reader SearchSnapshots, configure func(*Config)) *webSocketFixture {
	return newWebSocketFixtureWithReaders(t, reader, notFoundExportSnapshots{}, configure)
}

func newWebSocketFixtureWithReaders(t *testing.T, searches SearchSnapshots, exports ExportSnapshots, configure func(*Config)) *webSocketFixture {
	t.Helper()
	config := Config{
		Searches:     searches,
		Exports:      exports,
		Access:       searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
		PollInterval: 10 * time.Millisecond,
		PingInterval: 250 * time.Millisecond,
		PongTimeout:  time.Second,
		WriteTimeout: time.Second,
	}
	if configure != nil {
		configure(&config)
	}
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &webSocketFixture{t: t, service: service, reader: searches}
	fixture.server = httptest.NewServer(service)
	t.Cleanup(fixture.close)
	return fixture
}

func (fixture *webSocketFixture) dial() *websocket.Conn {
	fixture.t.Helper()
	address := "ws" + strings.TrimPrefix(fixture.server.URL, "http")
	client, response, err := websocket.DefaultDialer.Dial(address, nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		fixture.t.Fatalf("Dial() = %v", err)
	}
	fixture.mu.Lock()
	fixture.clients = append(fixture.clients, client)
	fixture.mu.Unlock()
	return client
}

func (fixture *webSocketFixture) close() {
	fixture.once.Do(func() {
		fixture.mu.Lock()
		clients := append([]*websocket.Conn(nil), fixture.clients...)
		fixture.mu.Unlock()
		for _, client := range clients {
			_ = client.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := fixture.service.Close(ctx); err != nil {
			fixture.t.Errorf("Service.Close() = %v", err)
		}
		fixture.server.Close()
	})
}

func scopedSearchJob(id string) searchjobs.Job {
	created := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	return searchjobs.Job{
		ID: id, Version: 1, OwnerID: "owner", TenantID: "tenant", SPL: "index=main",
		State: searchjobs.StateRunning, CreatedAt: created, StartedAt: created.Add(time.Second),
	}
}

func writeCommand(t *testing.T, client *websocket.Conn, command *opensplunkv1.SearchWebSocketCommand) {
	t.Helper()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("WriteMessage() = %v", err)
	}
}

func subscribeCommand(requestID, subscriptionID, jobID string, after uint64) *opensplunkv1.SearchWebSocketCommand {
	return &opensplunkv1.SearchWebSocketCommand{
		RequestId: requestID,
		Payload: &opensplunkv1.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunkv1.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunkv1.SearchSubscription{{
				SubscriptionId: subscriptionID,
				Target:         &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_SearchJobId{SearchJobId: jobID}},
				AfterSequence:  after,
			}},
		}},
	}
}

func subscribeExportCommand(requestID, subscriptionID, jobID string, after uint64) *opensplunkv1.SearchWebSocketCommand {
	return &opensplunkv1.SearchWebSocketCommand{
		RequestId: requestID,
		Payload: &opensplunkv1.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunkv1.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunkv1.SearchSubscription{{
				SubscriptionId: subscriptionID,
				Target:         &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_ExportJobId{ExportJobId: jobID}},
				AfterSequence:  after,
			}},
		}},
	}
}

func readEvent(t *testing.T, client *websocket.Conn) *opensplunkv1.SearchWebSocketEvent {
	t.Helper()
	if err := client.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	messageType, data, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() = %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("message type = %d, want binary", messageType)
	}
	var event opensplunkv1.SearchWebSocketEvent
	if err := proto.Unmarshal(data, &event); err != nil {
		t.Fatalf("Unmarshal() = %v", err)
	}
	if event.GetOccurredAt() == nil || event.GetOccurredAt().CheckValid() != nil {
		t.Fatalf("event timestamp = %v", event.GetOccurredAt())
	}
	return &event
}

func readInitialSearchState(t *testing.T, client *websocket.Conn, subscriptionID string) (*opensplunkv1.SearchWebSocketEvent, []*opensplunkv1.SearchWebSocketEvent) {
	t.Helper()
	ack := readEvent(t, client)
	if acknowledged := ack.GetSubscriptionAcknowledged(); acknowledged == nil || acknowledged.GetSubscriptionId() != subscriptionID {
		t.Fatalf("acknowledgment = %+v", ack)
	}
	events := []*opensplunkv1.SearchWebSocketEvent{readEvent(t, client), readEvent(t, client)}
	if events[0].GetSearchStateChanged() == nil || events[1].GetSearchProgress() == nil {
		t.Fatalf("initial events = (%+v, %+v)", events[0], events[1])
	}
	for index, event := range events {
		if event.GetSubscriptionId() != subscriptionID || event.GetSequence() == 0 ||
			(index > 0 && event.GetSequence() != events[index-1].GetSequence()+1) {
			t.Fatalf("initial event[%d] = subscription %q sequence %d", index, event.GetSubscriptionId(), event.GetSequence())
		}
	}
	return ack, events
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func TestWebSocketCoalescesTargetsAndPublishesIdenticalSequences(t *testing.T) {
	job := scopedSearchJob("search-shared")
	reader := newMutableSearchSnapshots(job)
	fixture := newWebSocketFixture(t, reader, nil)
	first := fixture.dial()
	second := fixture.dial()

	writeCommand(t, first, subscribeCommand("request-1", "first", job.ID, 0))
	firstAck, firstCurrent := readInitialSearchState(t, first, "first")
	writeCommand(t, second, subscribeCommand("request-2", "second", job.ID, 0))
	secondAck, secondCurrent := readInitialSearchState(t, second, "second")
	if firstAck.GetSubscriptionAcknowledged().GetLatestSequence() != firstCurrent[1].GetSequence() ||
		secondAck.GetSubscriptionAcknowledged().GetLatestSequence() != secondCurrent[1].GetSequence() {
		t.Fatalf("latest sequences = (%d, %d)", firstAck.GetSubscriptionAcknowledged().GetLatestSequence(), secondAck.GetSubscriptionAcknowledged().GetLatestSequence())
	}
	for index := range firstCurrent {
		if firstCurrent[index].GetSequence() != secondCurrent[index].GetSequence() {
			t.Fatalf("current sequence mismatch at %d", index)
		}
	}
	fixture.service.mu.Lock()
	if len(fixture.service.targets) != 1 {
		t.Fatalf("unique target count = %d, want 1", len(fixture.service.targets))
	}
	fixture.service.mu.Unlock()

	job.Version++
	job.RowCount = 11
	reader.set(job)
	firstProgress := readEvent(t, first)
	secondProgress := readEvent(t, second)
	if firstProgress.GetSearchProgress() == nil || secondProgress.GetSearchProgress() == nil ||
		firstProgress.GetSequence() != firstCurrent[1].GetSequence()+1 || secondProgress.GetSequence() != firstProgress.GetSequence() ||
		firstProgress.GetSearchProgress().GetProducedRows() != 11 || secondProgress.GetSearchProgress().GetProducedRows() != 11 {
		t.Fatalf("progress events = (%+v, %+v)", firstProgress, secondProgress)
	}

	job.Version++
	job.State = searchjobs.StateCompleted
	job.FinishedAt = job.StartedAt.Add(2 * time.Second)
	job.ExpiresAt = job.FinishedAt.Add(time.Hour)
	reader.set(job)
	for _, client := range []*websocket.Conn{first, second} {
		state := readEvent(t, client)
		progress := readEvent(t, client)
		terminal := readEvent(t, client)
		if state.GetSequence() != firstProgress.GetSequence()+1 || progress.GetSequence() != state.GetSequence()+1 || terminal.GetSequence() != progress.GetSequence()+1 ||
			state.GetSearchStateChanged() == nil || progress.GetSearchProgress() == nil || terminal.GetSearchTerminal() == nil {
			t.Fatalf("terminal transition = (%+v, %+v, %+v)", state, progress, terminal)
		}
	}
	waitFor(t, time.Second, func() bool {
		fixture.service.mu.Lock()
		target := fixture.service.targets[targetKey{kind: targetKindSearch, id: job.ID}]
		fixture.service.mu.Unlock()
		if target == nil {
			return false
		}
		target.mu.Lock()
		defer target.mu.Unlock()
		return !target.polling && target.terminal
	}, "terminal poller shutdown")

	// One unique poller serves both sockets. This broad bound tolerates CI
	// scheduling while catching a per-socket polling implementation.
	if calls := reader.callCount(job.ID); calls > 10 {
		t.Fatalf("snapshot calls = %d, expected one coalesced poller", calls)
	}
}

func TestWebSocketRoutesExportCurrentLiveAndTerminalEvents(t *testing.T) {
	created := time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)
	job := exportjobs.Job{
		ID: "export-live", Version: 1, State: exportjobs.StateRunning,
		CreatedAt: created, StartedAt: created.Add(time.Second),
		Progress: exportjobs.Progress{UpdatedAt: created.Add(time.Second)},
	}
	exports := newMutableExportSnapshots(job)
	fixture := newWebSocketFixtureWithReaders(t, newMutableSearchSnapshots(), exports, nil)
	client := fixture.dial()
	writeCommand(t, client, subscribeExportCommand("export-subscribe", "export", job.ID, 0))
	ack := readEvent(t, client).GetSubscriptionAcknowledged()
	state := readEvent(t, client)
	progress := readEvent(t, client)
	if ack == nil || state.GetExportStateChanged() == nil || progress.GetExportProgress() == nil ||
		state.GetTarget().GetExportJobId() != job.ID || progress.GetSubscriptionId() != "export" ||
		progress.GetSequence() != state.GetSequence()+1 {
		t.Fatalf("initial export events = ack:%+v state:%+v progress:%+v", ack, state, progress)
	}

	job.Version++
	job.Progress = exportjobs.Progress{RowsWritten: 9, BytesWritten: 512, UpdatedAt: created.Add(2 * time.Second)}
	exports.set(job)
	live := readEvent(t, client)
	if live.GetExportProgress() == nil || live.GetExportProgress().GetRowsWritten() != 9 ||
		live.GetSequence() != progress.GetSequence()+1 {
		t.Fatalf("live export progress = %+v", live)
	}

	job.Version++
	job.State = exportjobs.StateCompleted
	job.FinishedAt = created.Add(3 * time.Second)
	job.ExpiresAt = job.FinishedAt.Add(time.Hour)
	job.Progress.UpdatedAt = job.FinishedAt
	job.Artifact = &exportjobs.Artifact{
		FileName: "results.jsonl", MediaType: "application/x-ndjson", SizeBytes: 512,
		RowCount: 9, ExpiresAt: job.ExpiresAt,
	}
	exports.set(job)
	terminalState := readEvent(t, client)
	terminalProgress := readEvent(t, client)
	terminal := readEvent(t, client)
	if terminalState.GetExportStateChanged() == nil || terminalProgress.GetExportProgress() == nil ||
		terminal.GetExportTerminal() == nil || terminal.GetExportTerminal().GetArtifact().GetFileName() != "results.jsonl" ||
		terminalState.GetSequence() != live.GetSequence()+1 || terminalProgress.GetSequence() != terminalState.GetSequence()+1 ||
		terminal.GetSequence() != terminalProgress.GetSequence()+1 {
		t.Fatalf("terminal export events = (%+v, %+v, %+v)", terminalState, terminalProgress, terminal)
	}
	fixture.service.mu.Lock()
	target := fixture.service.targets[targetKey{kind: targetKindExport, id: job.ID}]
	fixture.service.mu.Unlock()
	if target == nil {
		t.Fatal("export target was not registered in its independent namespace")
	}
}

func TestWebSocketReplayRestartExpirationAndDivergence(t *testing.T) {
	job := scopedSearchJob("search-replay")
	reader := newMutableSearchSnapshots(job)
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumReplayEvents = 1
	})

	fresh := fixture.dial()
	writeCommand(t, fresh, subscribeCommand("fresh", "fresh", job.ID, 1))
	if readEvent(t, fresh).GetSubscriptionAcknowledged() == nil {
		t.Fatal("fresh acknowledgment is missing")
	}
	if got := readEvent(t, fresh).GetResynchronizationRequired(); got == nil || got.GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SERVER_RESTARTED {
		t.Fatalf("fresh resynchronization = %+v", got)
	}

	established := fixture.dial()
	writeCommand(t, established, subscribeCommand("establish", "established", job.ID, 0))
	_, establishedCurrent := readInitialSearchState(t, established, "established")

	job.Version++
	job.RowCount = 7
	reader.set(job)
	update := readEvent(t, established)
	if update.GetSequence() != establishedCurrent[1].GetSequence()+1 || update.GetSearchProgress() == nil {
		t.Fatalf("update = %+v", update)
	}

	replay := fixture.dial()
	writeCommand(t, replay, subscribeCommand("resume", "resume", job.ID, establishedCurrent[1].GetSequence()))
	ack := readEvent(t, replay).GetSubscriptionAcknowledged()
	if ack == nil || !ack.GetReplayWillFollow() {
		t.Fatalf("replay acknowledgment = %+v", ack)
	}
	if replayed := readEvent(t, replay); replayed.GetSequence() != update.GetSequence() || replayed.GetSubscriptionId() != "resume" {
		t.Fatalf("replayed event = %+v", replayed)
	}

	expired := fixture.dial()
	writeCommand(t, expired, subscribeCommand("expired", "expired", job.ID, establishedCurrent[0].GetSequence()))
	_ = readEvent(t, expired)
	if got := readEvent(t, expired).GetResynchronizationRequired(); got == nil || got.GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED {
		t.Fatalf("expired resynchronization = %+v", got)
	}

	oldLatest := update.GetSequence()
	job.CreatedAt = job.CreatedAt.Add(24 * time.Hour)
	job.StartedAt = job.CreatedAt.Add(time.Second)
	job.Version = 1
	job.RowCount = 0
	reader.set(job)
	if got := readEvent(t, established).GetResynchronizationRequired(); got == nil || got.GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED {
		t.Fatalf("live divergence = %+v", got)
	}
	diverged := fixture.dial()
	writeCommand(t, diverged, subscribeCommand("diverged", "diverged", job.ID, oldLatest))
	_ = readEvent(t, diverged)
	if got := readEvent(t, diverged).GetResynchronizationRequired(); got == nil || got.GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED {
		t.Fatalf("resume divergence = %+v", got)
	}
}

func TestWebSocketCommandsAreAtomicAndRejectPreviewOptions(t *testing.T) {
	job := scopedSearchJob("search-commands")
	reader := newMutableSearchSnapshots(job)
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumSubscriptions = 2
	})
	client := fixture.dial()

	missingBatch := &opensplunkv1.SearchWebSocketCommand{
		RequestId: "batch",
		Payload: &opensplunkv1.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunkv1.SubscribeSearchJobsCommand{Subscriptions: []*opensplunkv1.SearchSubscription{
			{SubscriptionId: "valid", Target: &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_SearchJobId{SearchJobId: job.ID}}},
			{SubscriptionId: "missing", Target: &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_SearchJobId{SearchJobId: "not-visible"}}},
		}}},
	}
	writeCommand(t, client, missingBatch)
	if got := readEvent(t, client).GetProtocolError(); got == nil || got.GetCode() != opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_JOB_NOT_FOUND {
		t.Fatalf("batch error = %+v", got)
	}

	previewLimit := uint32(10)
	preview := subscribeCommand("preview", "preview", job.ID, 0)
	preview.GetSubscribe().Subscriptions[0].IncludePreviews = true
	preview.GetSubscribe().Subscriptions[0].PreviewRowLimit = &previewLimit
	writeCommand(t, client, preview)
	if got := readEvent(t, client).GetProtocolError(); got == nil || got.GetCode() != opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_UNSUPPORTED_COMMAND {
		t.Fatalf("preview error = %+v", got)
	}

	writeCommand(t, client, subscribeCommand("valid", "valid", job.ID, 0))
	_, _ = readInitialSearchState(t, client, "valid")
	overCap := &opensplunkv1.SearchWebSocketCommand{
		RequestId: "over-cap",
		Payload: &opensplunkv1.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunkv1.SubscribeSearchJobsCommand{Subscriptions: []*opensplunkv1.SearchSubscription{
			{SubscriptionId: "second", Target: &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_SearchJobId{SearchJobId: job.ID}}},
			{SubscriptionId: "third", Target: &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_SearchJobId{SearchJobId: job.ID}}},
		}}},
	}
	writeCommand(t, client, overCap)
	if got := readEvent(t, client).GetProtocolError(); got == nil || got.GetCode() != opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS {
		t.Fatalf("capacity error = %+v", got)
	}

	unsubscribe := &opensplunkv1.SearchWebSocketCommand{
		RequestId: "remove",
		Payload: &opensplunkv1.SearchWebSocketCommand_Unsubscribe{Unsubscribe: &opensplunkv1.UnsubscribeSearchJobsCommand{
			SubscriptionIds: []string{"valid", "unknown"},
		}},
	}
	writeCommand(t, client, unsubscribe)
	for _, id := range []string{"valid", "unknown"} {
		removed := readEvent(t, client).GetSubscriptionRemoved()
		if removed == nil || removed.GetSubscriptionId() != id {
			t.Fatalf("removed event = %+v, want %q", removed, id)
		}
	}
	waitFor(t, time.Second, func() bool {
		fixture.service.mu.Lock()
		target := fixture.service.targets[targetKey{kind: targetKindSearch, id: job.ID}]
		fixture.service.mu.Unlock()
		if target == nil {
			return false
		}
		return target.subscriberCount.Load() == 0
	}, "subscription removal")
}

func TestWebSocketMalformedBinaryIsRecoverableAndApplicationPingResponds(t *testing.T) {
	reader := newMutableSearchSnapshots()
	fixture := newWebSocketFixture(t, reader, nil)
	client := fixture.dial()
	if err := client.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	if got := readEvent(t, client).GetProtocolError(); got == nil || got.GetCode() != opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND || got.GetConnectionWillClose() {
		t.Fatalf("malformed-frame error = %+v", got)
	}
	writeCommand(t, client, &opensplunkv1.SearchWebSocketCommand{
		RequestId: "ping-request",
		Payload:   &opensplunkv1.SearchWebSocketCommand_Ping{Ping: &opensplunkv1.SearchWebSocketPing{Nonce: "nonce-1"}},
	})
	if got := readEvent(t, client).GetPong(); got == nil || got.GetNonce() != "nonce-1" || got.GetServerTime().CheckValid() != nil {
		t.Fatalf("application pong = %+v", got)
	}
	knownWithUnknown := &opensplunkv1.SearchWebSocketCommand{
		RequestId: "forward-compatible",
		Payload:   &opensplunkv1.SearchWebSocketCommand_Ping{Ping: &opensplunkv1.SearchWebSocketPing{Nonce: "unknown-field"}},
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(knownWithUnknown)
	if err != nil {
		t.Fatal(err)
	}
	data = protowire.AppendTag(data, 100, protowire.VarintType)
	data = protowire.AppendVarint(data, 7)
	if err := client.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatal(err)
	}
	if got := readEvent(t, client).GetPong(); got == nil || got.GetNonce() != "unknown-field" {
		t.Fatalf("known command with unknown fields = %+v", got)
	}
}

func TestWebSocketRejectsTextAndOversizedFrames(t *testing.T) {
	for _, test := range []struct {
		name        string
		messageType int
		payload     []byte
		wantCode    opensplunkv1.SearchWebSocketProtocolErrorCode
	}{
		{name: "text", messageType: websocket.TextMessage, payload: []byte("not protobuf"), wantCode: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND},
		{name: "oversized", messageType: websocket.BinaryMessage, payload: make([]byte, minimumFrameBytes+1), wantCode: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_FRAME_TOO_LARGE},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newWebSocketFixture(t, newMutableSearchSnapshots(), func(config *Config) {
				config.MaximumFrameBytes = minimumFrameBytes
				config.MaximumQueuedBytes = minimumFrameBytes
				config.MaximumTotalQueuedBytes = minimumFrameBytes
			})
			client := fixture.dial()
			if err := client.WriteMessage(test.messageType, test.payload); err != nil {
				t.Fatal(err)
			}
			event := readEvent(t, client)
			if got := event.GetProtocolError(); got == nil || got.GetCode() != test.wantCode || !got.GetConnectionWillClose() {
				t.Fatalf("fatal protocol error = %+v", got)
			}
			_ = client.SetReadDeadline(time.Now().Add(time.Second))
			if _, _, err := client.ReadMessage(); err == nil {
				t.Fatal("connection remained open after fatal protocol error")
			}
		})
	}
}

func TestWebSocketTransportPingReceivesPong(t *testing.T) {
	fixture := newWebSocketFixture(t, newMutableSearchSnapshots(), nil)
	client := fixture.dial()
	ponged := make(chan string, 1)
	client.SetPongHandler(func(payload string) error {
		select {
		case ponged <- payload:
		default:
		}
		return nil
	})
	go func() {
		_, _, _ = client.ReadMessage()
	}()
	deadline := time.Now().Add(time.Second)
	if err := client.WriteControl(websocket.PingMessage, []byte("transport"), deadline); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-ponged:
		if payload != "transport" {
			t.Fatalf("pong payload = %q", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not answer transport ping")
	}
}

func TestServiceCloseWaitsForBlockingScopedLookupAndClearsAccounting(t *testing.T) {
	job := scopedSearchJob("search-blocking")
	reader := &blockingSearchSnapshots{
		job: job, started: make(chan struct{}), release: make(chan struct{}),
	}
	t.Cleanup(reader.unblock)
	service, err := New(Config{
		Searches: reader, Exports: notFoundExportSnapshots{},
		Access: searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(service)
	t.Cleanup(server.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	writeCommand(t, client, subscribeCommand("blocking", "blocking", job.ID, 0))
	select {
	case <-reader.started:
	case <-time.After(time.Second):
		t.Fatal("scoped lookup did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	closed := make(chan error, 1)
	go func() { closed <- service.Close(ctx) }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before scoped lookup completed: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	reader.unblock()
	select {
	case err := <-closed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close() = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after scoped lookup was released")
	}
	service.mu.Lock()
	active, targets, loads := service.activeConnections, len(service.targets), len(service.loads)
	service.mu.Unlock()
	service.replayBudgetMu.Lock()
	replayBytes := service.replayBytes
	service.replayBudgetMu.Unlock()
	service.queueBudgetMu.Lock()
	queuedBytes := service.queuedBytes
	service.queueBudgetMu.Unlock()
	if active != 0 || targets != 0 || loads != 0 || replayBytes != 0 || queuedBytes != 0 {
		t.Fatalf("post-close accounting = active:%d targets:%d loads:%d replay:%d queued:%d", active, targets, loads, replayBytes, queuedBytes)
	}
}

type blockingSearchSnapshots struct {
	job         searchjobs.Job
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	calls       atomic.Int32
}

func (reader *blockingSearchSnapshots) unblock() {
	reader.releaseOnce.Do(func() { close(reader.release) })
}

func (reader *blockingSearchSnapshots) GetFor(scope searchjobs.AccessScope, id string) (searchjobs.Job, error) {
	reader.calls.Add(1)
	reader.once.Do(func() { close(reader.started) })
	<-reader.release
	if id != reader.job.ID || scope.TenantID != reader.job.TenantID || scope.OwnerID != reader.job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	return cloneSearchSnapshot(reader.job), nil
}

func TestCoalescedLoadSurvivesInitiatorCancellationAndPinsForWaiter(t *testing.T) {
	job := scopedSearchJob("search-coalesced-load")
	reader := &blockingSearchSnapshots{job: job, started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(reader.unblock)
	service, err := New(Config{
		Searches: reader, Exports: notFoundExportSnapshots{},
		Access: searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}, MaximumTargets: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := targetKey{kind: targetKindSearch, id: job.ID}
	type result struct {
		target *targetState
		err    error
	}
	initiatorContext, cancelInitiator := context.WithCancel(context.Background())
	initiator := make(chan result, 1)
	go func() {
		target, resolveErr := service.resolveTarget(initiatorContext, key)
		initiator <- result{target: target, err: resolveErr}
	}()
	select {
	case <-reader.started:
	case <-time.After(time.Second):
		t.Fatal("initiating load did not start")
	}
	waiter := make(chan result, 1)
	go func() {
		target, resolveErr := service.resolveTarget(context.Background(), key)
		waiter <- result{target: target, err: resolveErr}
	}()
	waitFor(t, time.Second, func() bool {
		service.mu.Lock()
		defer service.mu.Unlock()
		load := service.loads[key]
		return load != nil && load.waiters == 2
	}, "second coalesced load waiter")
	cancelInitiator()
	select {
	case got := <-initiator:
		if !errors.Is(got.err, context.Canceled) || got.target != nil {
			t.Fatalf("initiator result = (%p, %v)", got.target, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled initiator remained blocked")
	}
	reader.unblock()
	var surviving *targetState
	select {
	case got := <-waiter:
		if got.err != nil || got.target == nil {
			t.Fatalf("surviving waiter result = (%p, %v)", got.target, got.err)
		}
		surviving = got.target
	case <-time.After(time.Second):
		t.Fatal("surviving waiter did not receive loaded target")
	}
	if calls := reader.calls.Load(); calls != 1 {
		t.Fatalf("coalesced reader calls = %d, want 1", calls)
	}
	if pins := surviving.resolverCount.Load(); pins != 1 {
		t.Fatalf("published target resolver pins = %d, want 1", pins)
	}
	if _, err := service.resolveTarget(context.Background(), targetKey{kind: targetKindSearch, id: "capacity-churn"}); err != errTargetCapacity {
		t.Fatalf("capacity churn resolve = %v, want target capacity", err)
	}
	service.releaseResolvedTarget(surviving)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionQueueEnforcesIntrinsicAndGlobalByteBounds(t *testing.T) {
	service, err := New(Config{
		Searches: newMutableSearchSnapshots(), Exports: notFoundExportSnapshots{},
		Access:            searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
		MaximumFrameBytes: minimumFrameBytes, MaximumQueuedFrames: minimumQueuedFrames,
		MaximumQueuedBytes: minimumFrameBytes, MaximumTotalQueuedBytes: minimumFrameBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := &connection{service: service, wake: make(chan struct{}, 1)}
	second := &connection{service: service, wake: make(chan struct{}, 1)}
	if got := first.enqueueBatchResult([][]byte{make([]byte, minimumFrameBytes/2), make([]byte, minimumFrameBytes/2)}); got != queueAccepted {
		t.Fatalf("first queue result = %v", got)
	}
	if got := second.enqueueBatchResult([][]byte{{1}}); got != queuePressure {
		t.Fatalf("global-pressure result = %v, want pressure", got)
	}
	if got := first.enqueueBatchResult([][]byte{{1}}); got != queuePressure {
		t.Fatalf("in-flight/local pressure result = %v, want pressure", got)
	}
	intrinsic := make([][]byte, minimumQueuedFrames+1)
	for index := range intrinsic {
		intrinsic[index] = []byte{byte(index)}
	}
	if got := second.enqueueBatchResult(intrinsic); got != queueIntrinsicLimit {
		t.Fatalf("intrinsic frame result = %v, want intrinsic limit", got)
	}
	for index := 0; index < 2; index++ {
		frame, _, _, state := first.nextFrame()
		if state != writerFrame {
			t.Fatalf("nextFrame[%d] state = %v, want frame", index, state)
		}
		first.completeFrame(uint64(len(frame)))
	}
	if first.queue != nil || first.queueHead != 0 || first.queuedBytes != 0 || first.inFlightFrames != 0 {
		t.Fatalf("drained queue retained backing/accounting: queue:%v head:%d bytes:%d in-flight:%d", first.queue, first.queueHead, first.queuedBytes, first.inFlightFrames)
	}
	service.queueBudgetMu.Lock()
	globalQueued := service.queuedBytes
	service.queueBudgetMu.Unlock()
	if globalQueued != 0 {
		t.Fatalf("global queued bytes after drain = %d", globalQueued)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultQueueCanAtomicallyHoldMaximumTerminalSubscriptionSnapshots(t *testing.T) {
	worstNormalFrames := int(defaultMaximumSubscriptions) * minimumQueuedFrames
	if defaultMaximumQueuedFrames < worstNormalFrames {
		t.Fatalf("default queued frames = %d, want at least %d", defaultMaximumQueuedFrames, worstNormalFrames)
	}
	service, err := New(Config{
		Searches: newMutableSearchSnapshots(), Exports: notFoundExportSnapshots{},
		Access: searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
	})
	if err != nil {
		t.Fatal(err)
	}
	connection := &connection{service: service}
	frames := make([][]byte, worstNormalFrames)
	for index := range frames {
		frames[index] = []byte{1}
	}
	if got := connection.preflightBatch(frames); got != queueAccepted {
		t.Fatalf("default maximum terminal batch preflight = %v", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestControlEventsRejectInvalidInjectedClock(t *testing.T) {
	reader := newMutableSearchSnapshots()
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.Now = func() time.Time { return time.Date(10_000, 1, 1, 0, 0, 0, 0, time.UTC) }
	})
	client := fixture.dial()
	writeCommand(t, client, &opensplunkv1.SearchWebSocketCommand{
		RequestId: "ping",
		Payload:   &opensplunkv1.SearchWebSocketCommand_Ping{Ping: &opensplunkv1.SearchWebSocketPing{Nonce: "invalid-clock"}},
	})
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := client.ReadMessage(); err == nil {
		t.Fatal("invalid injected clock produced a wire event")
	}
}
