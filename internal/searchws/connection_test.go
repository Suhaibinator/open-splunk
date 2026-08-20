package searchws

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
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

func (notFoundExportSnapshots) Snapshot(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
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

func (reader *mutableExportSnapshots) Snapshot(ctx context.Context, scope searchjobs.AccessScope, id string) (exportjobs.Job, error) {
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

func newWebSocketFixture(t *testing.T, reader synchronousSearchSnapshots, configure func(*Config)) *webSocketFixture {
	return newWebSocketFixtureWithReaders(t, reader, notFoundExportSnapshots{}, configure)
}

func newWebSocketFixtureWithReaders(
	t *testing.T,
	searches synchronousSearchSnapshots,
	exports ExportSnapshots,
	configure func(*Config),
) *webSocketFixture {
	t.Helper()
	contextualSearches := adaptSynchronousSearchSnapshots(searches)
	config := Config{
		Searches:     contextualSearches,
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
	fixture := &webSocketFixture{t: t, service: service, reader: contextualSearches}
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

func writeCommand(t *testing.T, client *websocket.Conn, command *opensplunk.SearchWebSocketCommand) {
	t.Helper()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("WriteMessage() = %v", err)
	}
}

func subscribeCommand(requestID, subscriptionID, jobID string, after uint64) *opensplunk.SearchWebSocketCommand {
	return &opensplunk.SearchWebSocketCommand{
		RequestId: requestID,
		Payload: &opensplunk.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunk.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunk.SearchSubscription{{
				SubscriptionId: subscriptionID,
				Target:         &opensplunk.JobTarget{Target: &opensplunk.JobTarget_SearchJobId{SearchJobId: jobID}},
				AfterSequence:  after,
			}},
		}},
	}
}

func subscribeExportCommand(requestID, subscriptionID, jobID string, after uint64) *opensplunk.SearchWebSocketCommand {
	return &opensplunk.SearchWebSocketCommand{
		RequestId: requestID,
		Payload: &opensplunk.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunk.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunk.SearchSubscription{{
				SubscriptionId: subscriptionID,
				Target:         &opensplunk.JobTarget{Target: &opensplunk.JobTarget_ExportJobId{ExportJobId: jobID}},
				AfterSequence:  after,
			}},
		}},
	}
}

func readEvent(t *testing.T, client *websocket.Conn) *opensplunk.SearchWebSocketEvent {
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
	var event opensplunk.SearchWebSocketEvent
	if err := proto.Unmarshal(data, &event); err != nil {
		t.Fatalf("Unmarshal() = %v", err)
	}
	if event.GetOccurredAt() == nil || event.GetOccurredAt().CheckValid() != nil {
		t.Fatalf("event timestamp = %v", event.GetOccurredAt())
	}
	return &event
}

func readInitialSearchState(t *testing.T, client *websocket.Conn, subscriptionID string) (*opensplunk.SearchWebSocketEvent, []*opensplunk.SearchWebSocketEvent) {
	t.Helper()
	ack := readEvent(t, client)
	if acknowledged := ack.GetSubscriptionAcknowledged(); acknowledged == nil || acknowledged.GetSubscriptionId() != subscriptionID {
		t.Fatalf("acknowledgment = %+v", ack)
	}
	events := []*opensplunk.SearchWebSocketEvent{readEvent(t, client), readEvent(t, client)}
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

func waitFor(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
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
	waitFor(t, func() bool {
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
	freshResynchronization := readEvent(t, fresh).GetResynchronizationRequired()
	if freshResynchronization == nil || freshResynchronization.GetReason() != opensplunk.ResynchronizationReason_RESYNCHRONIZATION_REASON_SERVER_RESTARTED {
		t.Fatalf("fresh resynchronization = %+v", freshResynchronization)
	}

	// A target can advance while the client restores authoritative HTTP state.
	// Its previously advertised boundary is still part of this fresh epoch and
	// must replay continuously rather than demanding SERVER_RESTARTED again.
	job.Version++
	job.RowCount = 1
	reader.set(job)
	freshUpdate := readEvent(t, fresh)
	if freshUpdate.GetSearchProgress() == nil ||
		freshUpdate.GetSequence() != freshResynchronization.GetLatestSequence()+1 {
		t.Fatalf("fresh epoch update = %+v", freshUpdate)
	}

	recovered := fixture.dial()
	writeCommand(t, recovered, subscribeCommand(
		"recovered", "recovered", job.ID, freshResynchronization.GetLatestSequence(),
	))
	recoveredAck := readEvent(t, recovered).GetSubscriptionAcknowledged()
	if recoveredAck == nil || !recoveredAck.GetReplayWillFollow() {
		t.Fatalf("recovered acknowledgment = %+v", recoveredAck)
	}
	replayedFreshUpdate := readEvent(t, recovered)
	if replayedFreshUpdate.GetSequence() != freshUpdate.GetSequence() ||
		replayedFreshUpdate.GetSearchProgress() == nil {
		t.Fatalf("recovered fresh epoch replay = %+v", replayedFreshUpdate)
	}
	writeCommand(t, recovered, &opensplunk.SearchWebSocketCommand{
		RequestId: "recovered-ping",
		Payload: &opensplunk.SearchWebSocketCommand_Ping{Ping: &opensplunk.SearchWebSocketPing{
			Nonce: "recovered",
		}},
	})
	if got := readEvent(t, recovered); got.GetPong() == nil {
		t.Fatalf("fresh resynchronization = %+v", got)
	}

	established := recovered
	establishedLatest := replayedFreshUpdate.GetSequence()
	job.Version++
	job.RowCount = 7
	reader.set(job)
	update := readEvent(t, established)
	if update.GetSequence() != establishedLatest+1 || update.GetSearchProgress() == nil {
		t.Fatalf("update = %+v", update)
	}

	replay := fixture.dial()
	writeCommand(t, replay, subscribeCommand("resume", "resume", job.ID, establishedLatest))
	ack := readEvent(t, replay).GetSubscriptionAcknowledged()
	if ack == nil || !ack.GetReplayWillFollow() {
		t.Fatalf("replay acknowledgment = %+v", ack)
	}
	if replayed := readEvent(t, replay); replayed.GetSequence() != update.GetSequence() || replayed.GetSubscriptionId() != "resume" {
		t.Fatalf("replayed event = %+v", replayed)
	}

	expired := fixture.dial()
	writeCommand(t, expired, subscribeCommand("expired", "expired", job.ID, freshResynchronization.GetLatestSequence()-1))
	_ = readEvent(t, expired)
	if got := readEvent(t, expired).GetResynchronizationRequired(); got == nil || got.GetReason() != opensplunk.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED {
		t.Fatalf("expired resynchronization = %+v", got)
	}

	oldLatest := update.GetSequence()
	job.CreatedAt = job.CreatedAt.Add(24 * time.Hour)
	job.StartedAt = job.CreatedAt.Add(time.Second)
	job.Version = 1
	job.RowCount = 0
	reader.set(job)
	if got := readEvent(t, established).GetResynchronizationRequired(); got == nil || got.GetReason() != opensplunk.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED {
		t.Fatalf("live divergence = %+v", got)
	}
	diverged := fixture.dial()
	writeCommand(t, diverged, subscribeCommand("diverged", "diverged", job.ID, oldLatest))
	_ = readEvent(t, diverged)
	if got := readEvent(t, diverged).GetResynchronizationRequired(); got == nil || got.GetReason() != opensplunk.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED {
		t.Fatalf("resume divergence = %+v", got)
	}
}

func TestExpiredTargetPollsUntilBackingTombstoneIsRemoved(t *testing.T) {
	job := scopedSearchJob("search-expired-retirement")
	job.Version = 2
	job.State = searchjobs.StateExpired
	job.FinishedAt = job.StartedAt.Add(time.Second)
	job.ExpiresAt = job.FinishedAt.Add(time.Second)
	reader := newMutableSearchSnapshots(job)
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.Now = func() time.Time { return job.ExpiresAt.Add(time.Second) }
		config.PollInterval = 10 * time.Millisecond
		config.TombstonePollInterval = 10 * time.Millisecond
	})
	client := fixture.dial()
	writeCommand(t, client, subscribeCommand("expired", "expired", job.ID, 0))
	_, _ = readInitialSearchState(t, client, "expired")

	reader.delete(job.ID)
	waitFor(t, func() bool {
		fixture.service.mu.Lock()
		defer fixture.service.mu.Unlock()
		return fixture.service.targets[targetKey{kind: targetKindSearch, id: job.ID}] == nil
	}, "expired target retirement")

	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for {
		if _, _, err := client.ReadMessage(); err != nil {
			break
		}
	}
}

func TestCompletedTargetRemovedAtFirstExpiryPollRetiresTarget(t *testing.T) {
	job := scopedSearchJob("search-first-expiry-retirement")
	job.Version = 2
	job.State = searchjobs.StateCompleted
	job.FinishedAt = job.StartedAt.Add(time.Second)
	job.ExpiresAt = job.FinishedAt.Add(time.Hour)
	clock := &mutableTestClock{value: job.ExpiresAt.Add(-20 * time.Millisecond)}
	reader := newMutableSearchSnapshots(job)
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.Now = clock.now
		config.PollInterval = 10 * time.Millisecond
		config.TombstonePollInterval = 10 * time.Millisecond
	})
	client := fixture.dial()
	writeCommand(t, client, subscribeCommand("completed", "completed", job.ID, 0))
	_, _ = readInitialSearchState(t, client, "completed")

	reader.delete(job.ID)
	clock.set(job.ExpiresAt.Add(time.Millisecond))
	waitFor(t, func() bool {
		fixture.service.mu.Lock()
		defer fixture.service.mu.Unlock()
		return fixture.service.targets[targetKey{kind: targetKindSearch, id: job.ID}] == nil
	}, "completed target retirement at its first expiry poll")

}

func TestWebSocketSubscriptionIdentityLimitRequiresReconnect(t *testing.T) {
	job := scopedSearchJob("search-subscription-identities")
	fixture := newWebSocketFixture(t, newMutableSearchSnapshots(job), func(config *Config) {
		config.MaximumSubscriptions = 1
	})
	client := fixture.dial()

	for index := range maximumSubscriptionIDsPerConnection {
		id := fmt.Sprintf("identity-%d", index)
		writeCommand(t, client, subscribeCommand("subscribe-"+id, id, job.ID, 0))
		_, _ = readInitialSearchState(t, client, id)
		writeCommand(t, client, &opensplunk.SearchWebSocketCommand{
			RequestId: "unsubscribe-" + id,
			Payload: &opensplunk.SearchWebSocketCommand_Unsubscribe{
				Unsubscribe: &opensplunk.UnsubscribeSearchJobsCommand{SubscriptionIds: []string{id}},
			},
		})
		if removed := readEvent(t, client).GetSubscriptionRemoved(); removed == nil || removed.GetSubscriptionId() != id {
			t.Fatalf("removed subscription %d = %+v", index, removed)
		}
	}

	exhaustedID := "identity-exhausted"
	writeCommand(t, client, subscribeCommand("exhausted", exhaustedID, job.ID, 0))
	exhausted := readEvent(t, client).GetProtocolError()
	if exhausted == nil ||
		exhausted.GetCode() != opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS ||
		!exhausted.GetConnectionWillClose() {
		t.Fatalf("exhausted subscription identities = %+v", exhausted)
	}
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.ReadMessage(); err == nil {
		t.Fatal("identity-exhausted connection remained open")
	}

	reconnected := fixture.dial()
	writeCommand(t, reconnected, subscribeCommand("reconnected", exhaustedID, job.ID, 0))
	_, _ = readInitialSearchState(t, reconnected, exhaustedID)
}

func TestWebSocketCommandsAreAtomic(t *testing.T) {
	job := scopedSearchJob("search-commands")
	reader := newMutableSearchSnapshots(job)
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumSubscriptions = 2
	})
	client := fixture.dial()

	missingBatch := &opensplunk.SearchWebSocketCommand{
		RequestId: "batch",
		Payload: &opensplunk.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunk.SubscribeSearchJobsCommand{Subscriptions: []*opensplunk.SearchSubscription{
			{SubscriptionId: "valid", Target: &opensplunk.JobTarget{Target: &opensplunk.JobTarget_SearchJobId{SearchJobId: job.ID}}},
			{SubscriptionId: "missing", Target: &opensplunk.JobTarget{Target: &opensplunk.JobTarget_SearchJobId{SearchJobId: "not-visible"}}},
		}}},
	}
	writeCommand(t, client, missingBatch)
	if got := readEvent(t, client).GetProtocolError(); got == nil || got.GetCode() != opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_JOB_NOT_FOUND {
		t.Fatalf("batch error = %+v", got)
	}

	writeCommand(t, client, subscribeCommand("valid", "valid", job.ID, 0))
	_, _ = readInitialSearchState(t, client, "valid")
	overCap := &opensplunk.SearchWebSocketCommand{
		RequestId: "over-cap",
		Payload: &opensplunk.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunk.SubscribeSearchJobsCommand{Subscriptions: []*opensplunk.SearchSubscription{
			{SubscriptionId: "second", Target: &opensplunk.JobTarget{Target: &opensplunk.JobTarget_SearchJobId{SearchJobId: job.ID}}},
			{SubscriptionId: "third", Target: &opensplunk.JobTarget{Target: &opensplunk.JobTarget_SearchJobId{SearchJobId: job.ID}}},
		}}},
	}
	writeCommand(t, client, overCap)
	if got := readEvent(t, client).GetProtocolError(); got == nil || got.GetCode() != opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS {
		t.Fatalf("capacity error = %+v", got)
	}

	unsubscribe := &opensplunk.SearchWebSocketCommand{
		RequestId: "remove",
		Payload: &opensplunk.SearchWebSocketCommand_Unsubscribe{Unsubscribe: &opensplunk.UnsubscribeSearchJobsCommand{
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
	waitFor(t, func() bool {
		fixture.service.mu.Lock()
		target := fixture.service.targets[targetKey{kind: targetKindSearch, id: job.ID}]
		fixture.service.mu.Unlock()
		if target == nil {
			return false
		}
		return target.subscriberCount.Load() == 0
	}, "subscription removal")

	writeCommand(t, client, subscribeCommand("reuse", "valid", job.ID, 0))
	reused := readEvent(t, client).GetProtocolError()
	if reused == nil ||
		reused.GetCode() != opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND ||
		len(reused.GetViolations()) != 1 ||
		reused.GetViolations()[0].GetCode() != "ALREADY_EXISTS" {
		t.Fatalf("reused subscription ID error = %+v", reused)
	}

	writeCommand(t, client, subscribeCommand("replacement", "replacement", job.ID, 0))
	_, _ = readInitialSearchState(t, client, "replacement")
}

func TestWebSocketMalformedBinaryIsRecoverableAndApplicationPingResponds(t *testing.T) {
	reader := newMutableSearchSnapshots()
	fixture := newWebSocketFixture(t, reader, nil)
	client := fixture.dial()
	if err := client.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	if got := readEvent(t, client).GetProtocolError(); got == nil || got.GetCode() != opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND || got.GetConnectionWillClose() {
		t.Fatalf("malformed-frame error = %+v", got)
	}
	writeCommand(t, client, &opensplunk.SearchWebSocketCommand{
		RequestId: "ping-request",
		Payload:   &opensplunk.SearchWebSocketCommand_Ping{Ping: &opensplunk.SearchWebSocketPing{Nonce: "nonce-1"}},
	})
	if got := readEvent(t, client).GetPong(); got == nil || got.GetNonce() != "nonce-1" || got.GetServerTime().CheckValid() != nil {
		t.Fatalf("application pong = %+v", got)
	}
	knownWithUnknown := &opensplunk.SearchWebSocketCommand{
		RequestId: "forward-compatible",
		Payload:   &opensplunk.SearchWebSocketCommand_Ping{Ping: &opensplunk.SearchWebSocketPing{Nonce: "unknown-field"}},
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
		wantCode    opensplunk.SearchWebSocketProtocolErrorCode
	}{
		{name: "text", messageType: websocket.TextMessage, payload: []byte("not protobuf"), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND},
		{name: "oversized", messageType: websocket.BinaryMessage, payload: make([]byte, minimumFrameBytes+1), wantCode: opensplunk.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_FRAME_TOO_LARGE},
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

func TestServiceCloseCancelsBlockingScopedLookupWaitsForExitAndClearsAccounting(t *testing.T) {
	job := scopedSearchJob("search-blocking")
	cancelObserved := make(chan struct{})
	exitRelease := make(chan struct{})
	var exitReleaseOnce sync.Once
	releaseExit := func() { exitReleaseOnce.Do(func() { close(exitRelease) }) }
	t.Cleanup(releaseExit)
	reader := &blockingSearchSnapshots{
		job:            job,
		started:        make(chan struct{}),
		release:        make(chan struct{}),
		cancelObserved: cancelObserved,
		exitRelease:    exitRelease,
		exited:         make(chan struct{}),
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
	client, dialResponse, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if dialResponse != nil {
		t.Cleanup(func() { _ = dialResponse.Body.Close() })
	}
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

	canceledContext, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	canceledClose := make(chan error, 1)
	go func() { canceledClose <- service.Close(canceledContext) }()
	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the scoped lookup")
	}

	healthyContext, cancelHealthy := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelHealthy()
	healthyClose := make(chan error, 1)
	go func() { healthyClose <- service.Close(healthyContext) }()
	select {
	case err := <-canceledClose:
		t.Fatalf("canceled Close returned before the provider exited: %v", err)
	case err := <-healthyClose:
		t.Fatalf("healthy Close returned before the provider exited: %v", err)
	case <-time.After(40 * time.Millisecond):
	}

	releaseExit()
	select {
	case err := <-canceledClose:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Close() = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled Close did not join the scoped lookup")
	}
	select {
	case err := <-healthyClose:
		if err != nil {
			t.Fatalf("healthy Close() = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("healthy Close did not share the completed ownership barrier")
	}
	finalContext, cancelFinal := context.WithTimeout(context.Background(), time.Second)
	defer cancelFinal()
	if err := service.Close(finalContext); err != nil {
		t.Fatalf("Close() after completion = %v", err)
	}
	select {
	case <-reader.exited:
	default:
		t.Fatal("Close returned before the scoped lookup provider exited")
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
	if calls := reader.calls.Load(); calls != 1 {
		t.Fatalf("snapshot provider calls = %d, want 1", calls)
	}
}

type blockingSearchSnapshots struct {
	job            searchjobs.Job
	started        chan struct{}
	release        chan struct{}
	once           sync.Once
	releaseOnce    sync.Once
	cancelOnce     sync.Once
	exitOnce       sync.Once
	calls          atomic.Int32
	active         atomic.Int32
	cancelObserved chan struct{}
	exitRelease    chan struct{}
	exited         chan struct{}
}

func (reader *blockingSearchSnapshots) unblock() {
	reader.releaseOnce.Do(func() { close(reader.release) })
}

func (reader *blockingSearchSnapshots) GetForContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	return reader.getForContext(ctx, scope, id)
}

func (reader *blockingSearchSnapshots) getForContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	reader.calls.Add(1)
	reader.active.Add(1)
	defer func() {
		reader.active.Add(-1)
		if reader.exited != nil {
			reader.exitOnce.Do(func() { close(reader.exited) })
		}
	}()
	reader.once.Do(func() { close(reader.started) })
	select {
	case <-reader.release:
	case <-ctx.Done():
		if reader.cancelObserved != nil {
			reader.cancelOnce.Do(func() { close(reader.cancelObserved) })
		}
		if reader.exitRelease != nil {
			<-reader.exitRelease
		}
		return searchjobs.Job{}, ctx.Err()
	}
	if id != reader.job.ID || scope.TenantID != reader.job.TenantID || scope.OwnerID != reader.job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	return cloneSearchSnapshot(reader.job), nil
}

func TestProjectionTimeoutCancelsSnapshotAndReleasesPermit(t *testing.T) {
	job := scopedSearchJob("search-projection-timeout")
	reader := &blockingSearchSnapshots{
		job: job, started: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{}),
	}
	t.Cleanup(reader.unblock)
	service, err := New(Config{
		Searches: reader, Exports: notFoundExportSnapshots{},
		Access:            searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
		ProjectionTimeout: minimumProjectionTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := service.Close(ctx); closeErr != nil {
			t.Errorf("Close() = %v", closeErr)
		}
	})

	_, err = service.loadProjection(
		context.Background(),
		targetKey{kind: targetKindSearch, id: job.ID},
		0,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("loadProjection() error = %v, want context deadline exceeded", err)
	}
	select {
	case <-reader.exited:
	default:
		t.Fatal("timed-out projection returned before its provider exited")
	}
	if got := reader.active.Load(); got != 0 {
		t.Fatalf("active snapshot calls = %d, want 0", got)
	}
	if got := len(service.projectionGate); got != 0 {
		t.Fatalf("projection gate occupancy = %d, want 0", got)
	}
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
	waitFor(t, func() bool {
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
	if _, err := service.resolveTarget(context.Background(), targetKey{kind: targetKindSearch, id: "capacity-churn"}); !errors.Is(err, errTargetCapacity) {
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
		Searches:          adaptSynchronousSearchSnapshots(newMutableSearchSnapshots()),
		Exports:           notFoundExportSnapshots{},
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
	for index := range 2 {
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
		Searches: adaptSynchronousSearchSnapshots(newMutableSearchSnapshots()),
		Exports:  notFoundExportSnapshots{},
		Access:   searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
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
	writeCommand(t, client, &opensplunk.SearchWebSocketCommand{
		RequestId: "ping",
		Payload:   &opensplunk.SearchWebSocketCommand_Ping{Ping: &opensplunk.SearchWebSocketPing{Nonce: "invalid-clock"}},
	})
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := client.ReadMessage(); err == nil {
		t.Fatal("invalid injected clock produced a wire event")
	}
}
