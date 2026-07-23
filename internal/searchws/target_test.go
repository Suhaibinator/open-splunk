package searchws

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

func TestIncompleteRetentionHasTruthfulBoundsAndNeverPublishesUnreplayableEvents(t *testing.T) {
	job := scopedSearchJob("search-incomplete")
	job.Schema = &searchjobs.Schema{}
	for index := 0; index < 40; index++ {
		job.Schema.Columns = append(job.Schema.Columns, searchjobs.Column{
			Name: fmt.Sprintf("field_%02d_%s", index, string(make([]byte, 80))),
			Kind: searchjobs.ValueKindString,
		})
	}
	// Replace NULs used only to size the names; schema validation accepts valid
	// UTF-8, but wire identifiers should remain ordinary printable strings.
	for index := range job.Schema.Columns {
		job.Schema.Columns[index].Name = fmt.Sprintf("field_%02d_%080d", index, index)
	}
	reader := newMutableSearchSnapshots(job)
	service, err := New(Config{
		Searches: reader, Exports: notFoundExportSnapshots{},
		Access:             searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
		MaximumReplayBytes: minimumFrameBytes,
		PollInterval:       time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	target, err := service.resolveTarget(context.Background(), targetKey{kind: targetKindSearch, id: job.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer service.releaseResolvedTarget(target)

	target.mu.Lock()
	if !target.currentIncomplete {
		target.mu.Unlock()
		t.Fatal("oversized current projection was reported complete")
	}
	earliest, latest := target.replayBoundsLocked()
	before := target.latest
	target.mu.Unlock()
	if earliest != 0 || latest != before {
		t.Fatalf("incomplete bounds = (%d, %d), want (0, %d)", earliest, latest, before)
	}
	target.mu.Lock()
	target.epochEstablished = true
	target.mu.Unlock()
	resume := newConnection(service, nil)
	if failure := resume.subscribe("resume-incomplete", &opensplunkv1.SubscribeSearchJobsCommand{Subscriptions: []*opensplunkv1.SearchSubscription{{
		SubscriptionId: "resume-incomplete", Target: target.key.protobuf(), AfterSequence: latest,
	}}}); failure != nil {
		t.Fatalf("resume incomplete subscribe = %v", failure)
	}
	resumeEvents := drainTestConnection(t, resume)
	if len(resumeEvents) != 2 || resumeEvents[0].GetSubscriptionAcknowledged() == nil ||
		resumeEvents[1].GetResynchronizationRequired() == nil ||
		resumeEvents[1].GetResynchronizationRequired().GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED {
		t.Fatalf("incomplete reconnect events = %+v", resumeEvents)
	}
	resume.removeAllSubscriptions()
	resume.cancel()

	fakeConnection := &connection{
		service: service, wake: make(chan struct{}, 1), subscriptions: make(map[string]*subscription),
	}
	subscription := &subscription{id: "incomplete", target: target, connection: fakeConnection, active: true}
	target.publishMu.Lock()
	target.mu.Lock()
	target.addSubscriptionLocked(subscription)
	target.mu.Unlock()
	target.publishMu.Unlock()

	projection, err := service.loadProjection(context.Background(), target.key, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.applyProjection(projection, false); err != nil {
		t.Fatal(err)
	}
	fakeConnection.queueMu.Lock()
	queued := len(fakeConnection.queue)
	fakeConnection.queueMu.Unlock()
	if queued != 0 {
		t.Fatalf("repeated failed retention published %d unreplayable frames", queued)
	}
	target.mu.Lock()
	if !target.currentIncomplete || target.latest <= before {
		t.Fatalf("retry state = incomplete:%t latest:%d before:%d", target.currentIncomplete, target.latest, before)
	}
	target.mu.Unlock()

	target.publishMu.Lock()
	subscription.mu.Lock()
	subscription.active = false
	subscription.mu.Unlock()
	target.mu.Lock()
	target.removeSubscriptionLocked(subscription)
	target.mu.Unlock()
	target.publishMu.Unlock()
}

func drainTestConnection(t *testing.T, connection *connection) []*opensplunkv1.SearchWebSocketEvent {
	t.Helper()
	var events []*opensplunkv1.SearchWebSocketEvent
	for {
		frame, _, _, state := connection.nextFrame()
		if state == writerIdle {
			return events
		}
		if state != writerFrame {
			t.Fatalf("nextFrame state = %v", state)
		}
		connection.completeFrame(uint64(len(frame)))
		var event opensplunkv1.SearchWebSocketEvent
		if err := proto.Unmarshal(frame, &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, &event)
	}
}

func TestResolvedTargetPinClosesEvictionAttachRace(t *testing.T) {
	firstJob := scopedSearchJob("search-first")
	secondJob := scopedSearchJob("search-second")
	reader := newMutableSearchSnapshots(firstJob, secondJob)
	service, err := New(Config{
		Searches: reader, Exports: notFoundExportSnapshots{},
		Access: searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"}, MaximumTargets: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.resolveTarget(context.Background(), targetKey{kind: targetKindSearch, id: firstJob.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.resolveTarget(context.Background(), targetKey{kind: targetKindSearch, id: secondJob.ID}); err != errTargetCapacity {
		t.Fatalf("resolve while first target pinned = %v, want capacity", err)
	}
	service.releaseResolvedTarget(first)
	second, err := service.resolveTarget(context.Background(), targetKey{kind: targetKindSearch, id: secondJob.ID})
	if err != nil {
		t.Fatal(err)
	}
	service.releaseResolvedTarget(second)
	if !first.isRetired() {
		t.Fatal("evicted target was not retired")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPollingBackoffResynchronizesAndRecovers(t *testing.T) {
	job := scopedSearchJob("search-recover")
	reader := newMutableSearchSnapshots(job)
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.PollInterval = minimumPollInterval
	})
	client := fixture.dial()
	writeCommand(t, client, subscribeCommand("establish", "recover", job.ID, 0))
	_, current := readInitialSearchState(t, client, "recover")

	reader.delete(job.ID)
	resync := readEvent(t, client).GetResynchronizationRequired()
	if resync == nil || resync.GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED {
		t.Fatalf("failure resynchronization = %+v", resync)
	}
	job.Version++
	job.RowCount = 19
	reader.set(job)
	recovered := readEvent(t, client)
	if recovered.GetSearchProgress() == nil || recovered.GetSearchProgress().GetProducedRows() != 19 ||
		recovered.GetSequence() <= current[1].GetSequence() {
		t.Fatalf("recovered event = %+v", recovered)
	}
	if calls := reader.callCount(job.ID); calls > 20 {
		t.Fatalf("failure polling calls = %d, backoff did not cap load", calls)
	}
}

func TestPollBackoffNeverDropsBelowConfiguredBaseline(t *testing.T) {
	baseline := time.Minute
	if got := nextPollDelay(minimumPollInterval, baseline); got != baseline {
		t.Fatalf("nextPollDelay() = %s, want baseline %s", got, baseline)
	}
	if got := nextPollDelay(baseline, baseline); got != baseline {
		t.Fatalf("capped nextPollDelay() = %s, want baseline %s", got, baseline)
	}
}

func TestTerminalReconnectReplaysAndOverdueCurrentResynchronizesBeforeRefresh(t *testing.T) {
	job := scopedSearchJob("search-terminal-cache")
	job.State = searchjobs.StateCompleted
	job.FinishedAt = job.StartedAt.Add(time.Second)
	clock := &mutableTestClock{value: job.FinishedAt.Add(time.Second)}
	job.ExpiresAt = clock.now().Add(time.Hour)
	expired := cloneSearchSnapshot(job)
	expired.Version++
	expired.State = searchjobs.StateExpired
	reader := &stagedTerminalSnapshots{completed: job, expired: expired}
	fixture := newWebSocketFixture(t, reader, func(config *Config) { config.Now = clock.now })
	client := fixture.dial()
	writeCommand(t, client, subscribeCommand("terminal-1", "terminal-1", job.ID, 0))
	_ = readEvent(t, client)
	firstState := readEvent(t, client)
	firstProgress := readEvent(t, client)
	firstTerminal := readEvent(t, client)
	if firstTerminal.GetSearchTerminal() == nil {
		t.Fatal("terminal event is missing")
	}

	writeCommand(t, client, &opensplunkv1.SearchWebSocketCommand{
		RequestId: "remove-terminal",
		Payload: &opensplunkv1.SearchWebSocketCommand_Unsubscribe{Unsubscribe: &opensplunkv1.UnsubscribeSearchJobsCommand{
			SubscriptionIds: []string{"terminal-1"},
		}},
	})
	if readEvent(t, client).GetSubscriptionRemoved() == nil {
		t.Fatal("terminal subscription removal is missing")
	}
	fixture.service.mu.Lock()
	retainedTarget := fixture.service.targets[targetKey{kind: targetKindSearch, id: job.ID}]
	fixture.service.mu.Unlock()
	if retainedTarget == nil || retainedTarget.isRetired() {
		t.Fatal("idle terminal journal was not retained for reconnect replay")
	}

	writeCommand(t, client, subscribeCommand("terminal-replay", "terminal-replay", job.ID, firstProgress.GetSequence()))
	replayAck := readEvent(t, client).GetSubscriptionAcknowledged()
	if replayAck == nil || !replayAck.GetReplayWillFollow() {
		t.Fatalf("terminal replay acknowledgment = %+v", replayAck)
	}
	replayedTerminal := readEvent(t, client)
	if replayedTerminal.GetSearchTerminal() == nil || replayedTerminal.GetSequence() != firstTerminal.GetSequence() {
		t.Fatalf("replayed terminal = %+v", replayedTerminal)
	}
	writeCommand(t, client, &opensplunkv1.SearchWebSocketCommand{
		RequestId: "remove-replay",
		Payload: &opensplunkv1.SearchWebSocketCommand_Unsubscribe{Unsubscribe: &opensplunkv1.UnsubscribeSearchJobsCommand{
			SubscriptionIds: []string{"terminal-replay"},
		}},
	})
	_ = readEvent(t, client)

	clock.set(job.ExpiresAt.Add(time.Second))
	writeCommand(t, client, subscribeCommand("terminal-2", "terminal-2", job.ID, 0))
	if readEvent(t, client).GetSubscriptionAcknowledged() == nil {
		t.Fatal("overdue terminal acknowledgment is missing")
	}
	resync := readEvent(t, client).GetResynchronizationRequired()
	if resync == nil || resync.GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED {
		t.Fatalf("overdue terminal resynchronization = %+v", resync)
	}
	secondState := readEvent(t, client)
	if secondState.GetSearchStateChanged() == nil || secondState.GetSearchStateChanged().GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED {
		t.Fatalf("refreshed state = %+v", secondState)
	}
	if secondState.GetSequence() <= firstTerminal.GetSequence() || firstState.GetSearchStateChanged() == nil {
		t.Fatalf("refreshed sequence = %d, prior terminal = %d", secondState.GetSequence(), firstTerminal.GetSequence())
	}
	if calls := reader.callCount(); calls < 3 || calls > 4 {
		t.Fatalf("terminal refresh calls = %d, want completed retry then expired refresh", calls)
	}
}

type stagedTerminalSnapshots struct {
	mu        sync.Mutex
	completed searchjobs.Job
	expired   searchjobs.Job
	calls     int
}

func (reader *stagedTerminalSnapshots) GetFor(scope searchjobs.AccessScope, id string) (searchjobs.Job, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.calls++
	if id != reader.completed.ID || scope.TenantID != reader.completed.TenantID || scope.OwnerID != reader.completed.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	if reader.calls <= 2 {
		return cloneSearchSnapshot(reader.completed), nil
	}
	return cloneSearchSnapshot(reader.expired), nil
}

func (reader *stagedTerminalSnapshots) callCount() int {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.calls
}

type mutableTestClock struct {
	mu    sync.Mutex
	value time.Time
}

func (clock *mutableTestClock) now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.value
}

func (clock *mutableTestClock) set(value time.Time) {
	clock.mu.Lock()
	clock.value = value
	clock.mu.Unlock()
}

func TestScopedLookupDoesNotRetainOrDiscloseForeignTarget(t *testing.T) {
	job := scopedSearchJob("search-foreign")
	job.OwnerID = "different-owner"
	reader := newMutableSearchSnapshots(job)
	fixture := newWebSocketFixture(t, reader, nil)
	client := fixture.dial()
	writeCommand(t, client, subscribeCommand("foreign", "foreign", job.ID, 0))
	protocolError := readEvent(t, client).GetProtocolError()
	if protocolError == nil || protocolError.GetCode() != opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_JOB_NOT_FOUND {
		t.Fatalf("foreign target response = %+v", protocolError)
	}
	fixture.service.mu.Lock()
	targets := len(fixture.service.targets)
	fixture.service.mu.Unlock()
	if targets != 0 {
		t.Fatalf("foreign lookup retained %d targets", targets)
	}
}
