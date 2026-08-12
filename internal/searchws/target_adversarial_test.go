package searchws

import (
	"context"
	"crypto/sha256"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

var adversarialNow = time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)

const adversarialSequenceBase = uint64(1) << 61

type adversarialSearchSnapshots struct {
	calls atomic.Int32
}

func (reader *adversarialSearchSnapshots) GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error) {
	reader.calls.Add(1)
	return searchjobs.Job{}, searchjobs.ErrNotFound
}

type adversarialExportSnapshots struct{}

func (*adversarialExportSnapshots) Snapshot(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
	return exportjobs.Job{}, exportjobs.ErrNotFound
}

func adversarialNewService(t *testing.T, mutate func(*Config)) *Service {
	t.Helper()
	config := Config{
		Searches:                adaptSynchronousSearchSnapshots(&adversarialSearchSnapshots{}),
		Exports:                 &adversarialExportSnapshots{},
		Access:                  searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
		MaximumFrameBytes:       minimumFrameBytes,
		MaximumQueuedFrames:     64,
		MaximumQueuedBytes:      64 * minimumFrameBytes,
		MaximumTotalQueuedBytes: 64 * minimumFrameBytes,
		MaximumTargets:          16,
		MaximumReplayEvents:     4,
		MaximumReplayBytes:      minimumFrameBytes,
		MaximumTotalReplayBytes: minimumFrameBytes,
		PollInterval:            time.Minute,
		WriteTimeout:            time.Second,
		PongTimeout:             time.Second,
		PingInterval:            250 * time.Millisecond,
		Now:                     func() time.Time { return adversarialNow },
	}
	if mutate != nil {
		mutate(&config)
	}
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Close() = %v", err)
		}
	})
	return service
}

func adversarialNewTarget(service *Service, id string) *targetState {
	key := targetKey{kind: targetKindSearch, id: id}
	ctx, cancel := context.WithCancel(service.ctx)
	target := &targetState{
		service:         service,
		key:             key,
		target:          key.protobuf(),
		ctx:             ctx,
		cancel:          cancel,
		epochStart:      adversarialSequenceBase + 1,
		latest:          adversarialSequenceBase,
		fingerprints:    make(map[eventCategory][sha256.Size]byte),
		expected:        make(map[eventCategory]struct{}),
		current:         make(map[eventCategory]*storedEvent),
		retained:        make(map[uint64]*storedEvent),
		subscriptions:   make(map[*subscription]struct{}),
		pendingPreviews: make(map[*subscription]uint32),
	}
	service.mu.Lock()
	service.targets[key] = target
	service.touchTargetLocked(target)
	service.mu.Unlock()
	return target
}

func adversarialProjection(
	version uint64,
	incarnation time.Time,
	terminal bool,
	state opensplunkv1.SearchJobState,
	rows uint64,
	includeProgress bool,
) targetProjection {
	events := []*opensplunkv1.SearchWebSocketEvent{{
		Payload: &opensplunkv1.SearchWebSocketEvent_SearchStateChanged{SearchStateChanged: &opensplunkv1.SearchJobStateChanged{
			SearchJobId: "search", State: state, StateVersion: version,
		}},
	}}
	if includeProgress {
		events = append(events, &opensplunkv1.SearchWebSocketEvent{
			Payload: &opensplunkv1.SearchWebSocketEvent_SearchProgress{SearchProgress: &opensplunkv1.SearchProgress{
				Phase: opensplunkv1.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_EXECUTING, ProducedRows: rows,
			}},
		})
	}
	return targetProjection{version: version, incarnation: incarnation, terminal: terminal, events: events}
}

func adversarialApply(t *testing.T, target *targetState, projection targetProjection, initial bool) {
	t.Helper()
	if _, err := target.applyProjection(projection, initial); err != nil {
		t.Fatalf("applyProjection() = %v", err)
	}
}

func adversarialDecode(t *testing.T, frames [][]byte) []*opensplunkv1.SearchWebSocketEvent {
	t.Helper()
	events := make([]*opensplunkv1.SearchWebSocketEvent, len(frames))
	for index, frame := range frames {
		event := new(opensplunkv1.SearchWebSocketEvent)
		if err := proto.Unmarshal(frame, event); err != nil {
			t.Fatalf("frame[%d] is not a SearchWebSocketEvent: %v", index, err)
		}
		events[index] = event
	}
	return events
}

func adversarialDrain(t *testing.T, connection *connection) []*opensplunkv1.SearchWebSocketEvent {
	t.Helper()
	var events []*opensplunkv1.SearchWebSocketEvent
	for {
		frame, _, _, state := connection.nextFrame()
		switch state {
		case writerIdle:
			return events
		case writerFrame:
			connection.completeFrame(uint64(len(frame)))
			var event opensplunkv1.SearchWebSocketEvent
			if err := proto.Unmarshal(frame, &event); err != nil {
				t.Fatalf("queued frame is not a SearchWebSocketEvent: %v", err)
			}
			events = append(events, &event)
		default:
			t.Fatalf("nextFrame() state = %d, want frame or idle", state)
		}
	}
}

func adversarialAttach(target *targetState, connection *connection, id string) *subscription {
	subscription := &subscription{id: id, target: target, connection: connection, active: true}
	target.mu.Lock()
	target.addSubscriptionLocked(subscription)
	target.mu.Unlock()
	connection.subscriptions[id] = subscription
	return subscription
}

func adversarialDetach(subscription *subscription) {
	subscription.mu.Lock()
	subscription.active = false
	subscription.mu.Unlock()
	target := subscription.target
	target.mu.Lock()
	target.removeSubscriptionLocked(subscription)
	if len(target.subscriptions) == 0 {
		target.stopPollingLocked()
	}
	target.mu.Unlock()
	delete(subscription.connection.subscriptions, subscription.id)
}

func adversarialStore(target *targetState, category eventCategory, sequence uint64, data []byte) bool {
	target.mu.Lock()
	defer target.mu.Unlock()
	return target.storeLocked(category, sequence, data)
}

func adversarialReplayBytes(service *Service) uint64 {
	service.replayBudgetMu.Lock()
	defer service.replayBudgetMu.Unlock()
	return service.replayBytes
}

func adversarialWait(t *testing.T, timeout time.Duration, condition func() bool, description string) {
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

func TestAdversarialSearchProgressStateVersionPreventsTerminalFingerprintCollapse(t *testing.T) {
	service := adversarialNewService(t, nil)
	target := adversarialNewTarget(service, "terminal-progress-version")
	connection := newConnection(service, nil)
	defer connection.cancel()
	subscription := adversarialAttach(target, connection, "terminal-progress-version")
	defer adversarialDetach(subscription)

	completed := scopedSearchJob(target.key.id)
	completed.Version = 29
	completed.State = searchjobs.StateCompleted
	completed.FinishedAt = completed.StartedAt.Add(3 * time.Second)
	completed.ExpiresAt = completed.FinishedAt.Add(time.Hour)
	completed.ScannedRows = 50
	completed.ScannedBytes = 500
	completed.RowCount = 5
	completed.ResultBytes = 75
	projectionTime := completed.FinishedAt.Add(time.Minute)

	completedProjection, err := projectSearch(completed, projectionTime)
	if err != nil {
		t.Fatalf("project completed search: %v", err)
	}
	adversarialApply(t, target, completedProjection, false)
	completedEvents := adversarialDrain(t, connection)
	if len(completedEvents) != 3 ||
		completedEvents[0].GetSearchStateChanged() == nil ||
		completedEvents[1].GetSearchProgress() == nil ||
		completedEvents[2].GetSearchTerminal() == nil {
		t.Fatalf("completed projection events = %+v", completedEvents)
	}
	completedProgress := completedEvents[1].GetSearchProgress()
	if completedProgress.GetStateVersion() != completed.Version {
		t.Fatalf("completed progress version = %d, want %d", completedProgress.GetStateVersion(), completed.Version)
	}

	expired := cloneSearchSnapshot(completed)
	expired.Version++
	expired.State = searchjobs.StateExpired
	expiredProjection, err := projectSearch(expired, projectionTime)
	if err != nil {
		t.Fatalf("project expired search: %v", err)
	}
	adversarialApply(t, target, expiredProjection, false)
	expiredEvents := adversarialDrain(t, connection)
	if len(expiredEvents) != 3 ||
		expiredEvents[0].GetSearchStateChanged().GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED ||
		expiredEvents[1].GetSearchProgress() == nil ||
		expiredEvents[2].GetSearchTerminal().GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED {
		t.Fatalf("expired projection events = %+v", expiredEvents)
	}
	expiredProgress := expiredEvents[1].GetSearchProgress()
	if expiredProgress.GetStateVersion() != expired.Version ||
		expiredEvents[1].GetSequence() <= completedEvents[2].GetSequence() {
		t.Fatalf(
			"expired progress = version:%d sequence:%d, want version:%d after sequence:%d",
			expiredProgress.GetStateVersion(), expiredEvents[1].GetSequence(),
			expired.Version, completedEvents[2].GetSequence(),
		)
	}
	completedAtExpiredVersion := proto.Clone(completedProgress).(*opensplunkv1.SearchProgress)
	completedAtExpiredVersion.StateVersion = expired.Version
	if !proto.Equal(completedAtExpiredVersion, expiredProgress) {
		t.Fatalf(
			"terminal progress changed beyond state version: completed=%+v expired=%+v",
			completedProgress, expiredProgress,
		)
	}
}

func TestAdversarialReplayRingKeepsContiguousCurrentOnlyTail(t *testing.T) {
	service := adversarialNewService(t, func(config *Config) { config.MaximumReplayEvents = 1 })
	target := adversarialNewTarget(service, "replay-one")
	base := adversarialSequenceBase
	incarnation := adversarialNow.Add(-time.Hour)

	adversarialApply(t, target, adversarialProjection(
		1, incarnation, false, opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED, 1, true,
	), true)

	target.mu.Lock()
	if target.latest != base+2 || len(target.replay) != 1 || target.replay[0].sequence != base+1 {
		t.Fatalf("initial journal = latest:%d replay:%+v", target.latest, target.replay)
	}
	progress := target.current[eventCategorySearchProgress]
	if progress == nil || progress.sequence != base+2 || progress.inReplay || target.retained[base+2] != progress {
		t.Fatalf("current-only progress = %+v", progress)
	}
	earliest, latest := target.replayBoundsLocked()
	frames, continuous := target.replayAfterLocked(base)
	target.mu.Unlock()
	if earliest != base+1 || latest != base+2 || !continuous {
		t.Fatalf("initial replay bounds/continuity = (%d, %d, %t)", earliest, latest, continuous)
	}
	events := adversarialDecode(t, frames)
	if len(events) != 2 || events[0].GetSequence() != base+1 || events[1].GetSequence() != base+2 {
		t.Fatalf("initial replay = %+v", events)
	}

	adversarialApply(t, target, adversarialProjection(
		2, incarnation, false, opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, 2, true,
	), false)
	target.mu.Lock()
	if target.latest != base+4 || len(target.replay) != 1 || target.replay[0].sequence != base+3 {
		t.Fatalf("updated journal = latest:%d replay:%+v", target.latest, target.replay)
	}
	progress = target.current[eventCategorySearchProgress]
	if progress == nil || progress.sequence != base+4 || progress.inReplay || target.retained[base+4] != progress {
		t.Fatalf("updated current-only progress = %+v", progress)
	}
	earliest, latest = target.replayBoundsLocked()
	frames, continuous = target.replayAfterLocked(base + 2)
	_, expiredContinuous := target.replayAfterLocked(base + 1)
	target.mu.Unlock()
	if earliest != base+3 || latest != base+4 || !continuous {
		t.Fatalf("updated replay bounds/continuity = (%d, %d, %t)", earliest, latest, continuous)
	}
	events = adversarialDecode(t, frames)
	if len(events) != 2 || events[0].GetSequence() != base+3 || events[1].GetSequence() != base+4 {
		t.Fatalf("updated replay = %+v", events)
	}
	if expiredContinuous {
		t.Fatal("replay across the expired sequence gap was reported continuous")
	}
}

func TestAdversarialIncompleteRetentionNeverPublishesUnretainedFramesAndRepairs(t *testing.T) {
	service := adversarialNewService(t, func(config *Config) { config.MaximumReplayEvents = 1 })
	target := adversarialNewTarget(service, "retention-retry")
	connection := newConnection(service, nil)
	defer connection.cancel()
	subscription := adversarialAttach(target, connection, "retry")
	defer adversarialDetach(subscription)
	base := adversarialSequenceBase
	projection := adversarialProjection(
		1, adversarialNow.Add(-time.Hour), false,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, 0, false,
	)

	if !service.reserveReplayBytes(minimumFrameBytes) {
		t.Fatal("could not reserve the test's synthetic global replay pressure")
	}
	adversarialApply(t, target, projection, true)
	events := adversarialDrain(t, connection)
	if len(events) != 1 || events[0].GetResynchronizationRequired() == nil {
		t.Fatalf("first failed retention delivery = %+v, want one resynchronization", events)
	}
	resync := events[0].GetResynchronizationRequired()
	if resync.GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED ||
		resync.GetEarliestAvailableSequence() != 0 || resync.GetLatestSequence() != base+1 {
		t.Fatalf("first resynchronization = %+v", resync)
	}
	target.mu.Lock()
	if !target.currentIncomplete || len(target.retained) != 0 || target.latest != base+1 {
		t.Fatalf("failed journal = incomplete:%t retained:%d latest:%d", target.currentIncomplete, len(target.retained), target.latest)
	}
	target.mu.Unlock()

	adversarialApply(t, target, projection, false)
	if events = adversarialDrain(t, connection); len(events) != 0 {
		t.Fatalf("identical incomplete retry published frames = %+v", events)
	}
	target.mu.Lock()
	if len(target.retained) != 0 || target.latest != base+2 {
		t.Fatalf("second failed journal = retained:%d latest:%d", len(target.retained), target.latest)
	}
	target.mu.Unlock()

	service.releaseReplayBytes(minimumFrameBytes)
	adversarialApply(t, target, projection, false)
	events = adversarialDrain(t, connection)
	if len(events) != 1 || events[0].GetSearchStateChanged() == nil || events[0].GetSequence() != base+3 {
		t.Fatalf("repaired delivery = %+v", events)
	}
	target.mu.Lock()
	retained := target.retained[base+3]
	if target.currentIncomplete || retained == nil || target.current[eventCategorySearchState] != retained {
		t.Fatalf("repaired journal = incomplete:%t retained:%+v current:%+v", target.currentIncomplete, retained, target.current)
	}
	target.mu.Unlock()
}

func TestAdversarialMissingStaticCategoryIsRetried(t *testing.T) {
	service := adversarialNewService(t, func(config *Config) { config.MaximumReplayEvents = 1 })
	target := adversarialNewTarget(service, "static-retry")
	base := adversarialSequenceBase
	incarnation := adversarialNow.Add(-time.Hour)
	adversarialApply(t, target, adversarialProjection(
		1, incarnation, false, opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED, 9, true,
	), true)

	target.mu.Lock()
	target.removeCurrentLocked(eventCategorySearchProgress)
	target.currentIncomplete = true
	target.mu.Unlock()
	adversarialApply(t, target, adversarialProjection(
		2, incarnation, false, opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, 9, true,
	), false)

	target.mu.Lock()
	state := target.current[eventCategorySearchState]
	progress := target.current[eventCategorySearchProgress]
	latest := target.latest
	incomplete := target.currentIncomplete
	target.mu.Unlock()
	if incomplete || latest != base+4 || state == nil || state.sequence != base+3 || progress == nil || progress.sequence != base+4 {
		t.Fatalf("static-category repair = incomplete:%t latest:%d state:%+v progress:%+v", incomplete, latest, state, progress)
	}
	events := adversarialDecode(t, [][]byte{progress.data})
	if events[0].GetSearchProgress().GetProducedRows() != 9 {
		t.Fatalf("retried static progress = %+v", events[0].GetSearchProgress())
	}
}

func TestAdversarialGlobalReplayCapReclaimsLRUInactiveTarget(t *testing.T) {
	service := adversarialNewService(t, nil)
	base := adversarialSequenceBase
	oldest := adversarialNewTarget(service, "oldest")
	untouched := adversarialNewTarget(service, "untouched")
	writer := adversarialNewTarget(service, "writer")
	if !adversarialStore(oldest, eventCategorySearchState, base+1, make([]byte, minimumFrameBytes)) {
		t.Fatal("could not fill the global replay budget")
	}
	if !adversarialStore(writer, eventCategorySearchState, base+1, []byte{1}) {
		t.Fatal("writer could not reclaim the LRU inactive target")
	}

	service.mu.Lock()
	_, hasOldest := service.targets[oldest.key]
	_, hasUntouched := service.targets[untouched.key]
	_, hasWriter := service.targets[writer.key]
	service.mu.Unlock()
	if hasOldest || !hasUntouched || !hasWriter || !oldest.isRetired() {
		t.Fatalf("targets after global reclaim = oldest:%t untouched:%t writer:%t retired:%t", hasOldest, hasUntouched, hasWriter, oldest.isRetired())
	}
	if got := adversarialReplayBytes(service); got != 1 {
		t.Fatalf("global replay bytes = %d, want 1", got)
	}
}

func TestAdversarialResolverAndSubscriberPinsPreventEviction(t *testing.T) {
	tests := []struct {
		name string
		pin  func(*testing.T, *Service, *targetState) func()
	}{
		{
			name: "resolver",
			pin: func(t *testing.T, service *Service, target *targetState) func() {
				resolved, err := service.resolveTarget(context.Background(), target.key)
				if err != nil || resolved != target {
					t.Fatalf("resolveTarget() = (%p, %v), want (%p, nil)", resolved, err, target)
				}
				return func() { target.resolverCount.Add(-1) }
			},
		},
		{
			name: "subscriber",
			pin: func(_ *testing.T, _ *Service, target *targetState) func() {
				subscription := &subscription{id: "pin", target: target, active: true}
				target.mu.Lock()
				target.addSubscriptionLocked(subscription)
				target.mu.Unlock()
				return func() {
					target.mu.Lock()
					target.removeSubscriptionLocked(subscription)
					target.mu.Unlock()
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := adversarialNewService(t, nil)
			base := adversarialSequenceBase
			pinned := adversarialNewTarget(service, "pinned")
			writer := adversarialNewTarget(service, "writer")
			if !adversarialStore(pinned, eventCategorySearchState, base+1, make([]byte, minimumFrameBytes)) {
				t.Fatal("could not fill the global replay budget")
			}
			unpin := test.pin(t, service, pinned)
			if adversarialStore(writer, eventCategorySearchState, base+1, []byte{1}) {
				t.Fatal("global replay admission evicted a pinned target")
			}
			service.mu.Lock()
			stillPresent := service.targets[pinned.key] == pinned
			service.mu.Unlock()
			if !stillPresent || pinned.isRetired() {
				t.Fatal("pinned target was removed or retired")
			}
			unpin()
			if !adversarialStore(writer, eventCategorySearchState, base+2, []byte{2}) {
				t.Fatal("global replay admission did not reclaim the unpinned target")
			}
			if !pinned.isRetired() {
				t.Fatal("unpinned target was not retired")
			}
		})
	}
}

func TestAdversarialDivergenceBoundaryRequiresResynchronization(t *testing.T) {
	service := adversarialNewService(t, nil)
	target := adversarialNewTarget(service, "divergence")
	base := adversarialSequenceBase
	adversarialApply(t, target, adversarialProjection(
		5, adversarialNow.Add(-2*time.Hour), true,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, 5, true,
	), true)
	target.mu.Lock()
	target.epochEstablished = true
	target.mu.Unlock()
	adversarialApply(t, target, adversarialProjection(
		1, adversarialNow.Add(-time.Hour), true,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, 6, true,
	), false)

	connection := newConnection(service, nil)
	defer connection.cancel()
	failure := connection.subscribe("request", &opensplunkv1.SubscribeSearchJobsCommand{Subscriptions: []*opensplunkv1.SearchSubscription{{
		SubscriptionId: "diverged",
		Target:         target.key.protobuf(),
		AfterSequence:  base + 2,
	}}})
	if failure != nil {
		t.Fatalf("subscribe() = %v", failure)
	}
	events := adversarialDrain(t, connection)
	if len(events) != 2 || events[0].GetSubscriptionAcknowledged() == nil || events[1].GetResynchronizationRequired() == nil {
		t.Fatalf("divergence response = %+v", events)
	}
	resync := events[1].GetResynchronizationRequired()
	if resync.GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED ||
		resync.GetEarliestAvailableSequence() != base+3 || resync.GetLatestSequence() != base+4 {
		t.Fatalf("divergence resynchronization = %+v", resync)
	}
	connection.removeAllSubscriptions()
}

func TestAdversarialUnsubscribeMakesLateDeliveryNoOpSuccess(t *testing.T) {
	service := adversarialNewService(t, nil)
	target := adversarialNewTarget(service, "unsubscribe")
	connection := newConnection(service, nil)
	defer connection.cancel()
	subscription := adversarialAttach(target, connection, "removed")
	if failure := connection.unsubscribe("request", &opensplunkv1.UnsubscribeSearchJobsCommand{
		SubscriptionIds: []string{"removed"},
	}); failure != nil {
		t.Fatalf("unsubscribe() = %v", failure)
	}

	connection.queueMu.Lock()
	queuedFrames, queuedBytes := len(connection.queue), connection.queuedBytes
	connection.queueMu.Unlock()
	if !subscription.deliverCanonical([]byte("not protobuf"), false) {
		t.Fatal("late inactive data delivery reported failure")
	}
	if !subscription.deliverControl(resynchronizationEvent(
		"removed", target.key,
		opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED,
		0, adversarialSequenceBase, adversarialNow,
	)) {
		t.Fatal("late inactive control delivery reported failure")
	}
	connection.queueMu.Lock()
	lateFrames, lateBytes := len(connection.queue), connection.queuedBytes
	connection.queueMu.Unlock()
	if queuedFrames != 1 || lateFrames != queuedFrames || lateBytes != queuedBytes {
		t.Fatalf("queue changed after inactive delivery = before:(%d,%d) after:(%d,%d)", queuedFrames, queuedBytes, lateFrames, lateBytes)
	}
	events := adversarialDrain(t, connection)
	if len(events) != 1 || events[0].GetSubscriptionRemoved() == nil {
		t.Fatalf("unsubscribe response = %+v", events)
	}
	if target.subscriberCount.Load() != 0 {
		t.Fatalf("subscriber count = %d, want 0", target.subscriberCount.Load())
	}
}

func TestAdversarialStaleSameIncarnationProjectionIsIgnored(t *testing.T) {
	service := adversarialNewService(t, nil)
	target := adversarialNewTarget(service, "stale-projection")
	newer := adversarialProjection(
		11, adversarialNow.Add(-time.Hour), false,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, 11, true,
	)
	adversarialApply(t, target, newer, true)
	target.mu.Lock()
	latest := target.latest
	target.mu.Unlock()

	stale := adversarialProjection(
		10, adversarialNow.Add(-time.Hour), false,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, 10, true,
	)
	adversarialApply(t, target, stale, false)
	target.mu.Lock()
	version, after, incomplete := target.version, target.latest, target.currentIncomplete
	target.mu.Unlock()
	if version != 11 || after != latest || incomplete {
		t.Fatalf("stale projection changed target = version:%d latest:%d/%d incomplete:%t",
			version, after, latest, incomplete)
	}
}

func TestPreviewQueuePressureSkipsSubscriptionSpecificCopy(t *testing.T) {
	service := adversarialNewService(t, func(config *Config) {
		config.MaximumQueuedBytes = minimumFrameBytes
		config.MaximumTotalQueuedBytes = minimumFrameBytes
	})
	connection := newConnection(service, nil)
	defer connection.cancel()
	if !connection.enqueue(make([]byte, minimumFrameBytes-32)) {
		t.Fatal("failed to seed the bounded connection queue")
	}
	before := connection.queuedBytes
	if result := connection.enqueueCanonicalPreview(make([]byte, 64), "preview"); result != queuePressure {
		t.Fatalf("enqueueCanonicalPreview() = %v, want queuePressure", result)
	}
	if connection.queuedBytes != before {
		t.Fatalf("preview pressure changed queued bytes = %d, want %d", connection.queuedBytes, before)
	}
}

func TestDisposablePreviewPressureClosesAndRetainsReplay(t *testing.T) {
	service := adversarialNewService(t, func(config *Config) {
		config.MaximumQueuedBytes = minimumFrameBytes
		config.MaximumTotalQueuedBytes = 4 * minimumFrameBytes
	})
	target := adversarialNewTarget(service, "slow-preview")
	incarnation := adversarialNow.Add(-time.Hour)
	initial := previewEdgeProjection(1, incarnation, "slow-schema", true)
	adversarialApply(t, target, initial, true)
	target.mu.Lock()
	checkpoint := target.latest
	target.mu.Unlock()

	slow := newConnection(service, nil)
	defer slow.cancel()
	slowSubscription := adversarialAttach(target, slow, "slow-preview")
	slowSubscription.previewRows = 1
	defer adversarialDetach(slowSubscription)
	healthy := newConnection(service, nil)
	defer healthy.cancel()
	healthySubscription := adversarialAttach(target, healthy, "healthy-preview")
	healthySubscription.previewRows = 1
	defer adversarialDetach(healthySubscription)
	if !slow.enqueue(make([]byte, minimumFrameBytes-32)) {
		t.Fatal("failed to seed the bounded connection queue")
	}
	changed := previewEdgeProjection(2, incarnation, "slow-schema", true)
	changed.events[len(changed.events)-1].GetResultPreview().Rows[0].RowId = "changed-row"
	adversarialApply(t, target, changed, false)

	slow.queueMu.Lock()
	closed := slow.hardClosed
	queuedFrames, queuedBytes := len(slow.queue)-slow.queueHead, slow.queuedBytes
	slow.queueMu.Unlock()
	if !closed || queuedFrames != 0 || queuedBytes != 0 {
		t.Fatalf(
			"pressured disposable preview connection = closed:%t frames:%d bytes:%d",
			closed,
			queuedFrames,
			queuedBytes,
		)
	}
	healthyEvents := adversarialDrain(t, healthy)
	var healthyRows []*opensplunkv1.ResultRow
	if len(healthyEvents) == 1 {
		healthyRows = healthyEvents[0].GetResultPreview().GetRows()
	}
	if len(healthyEvents) != 1 ||
		len(healthyRows) != 1 ||
		healthyRows[0].GetRowId() != "changed-row" {
		t.Fatalf("healthy disposable preview delivery = %+v", healthyEvents)
	}

	target.mu.Lock()
	replay, continuous := target.replayAfterLocked(checkpoint)
	_, latest := target.replayBoundsLocked()
	target.mu.Unlock()
	if !continuous || len(replay) != 1 {
		t.Fatalf("preview replay after %d = continuous:%t frames:%d", checkpoint, continuous, len(replay))
	}
	recovered := newConnection(service, nil)
	defer recovered.cancel()
	recoveredSubscription := adversarialAttach(target, recovered, "recovered-preview")
	recoveredSubscription.previewRows = 1
	defer adversarialDetach(recoveredSubscription)
	if !recoveredSubscription.deliverCanonical(replay[0], true) {
		t.Fatal("retained preview replay was not accepted")
	}
	recoveredEvents := adversarialDrain(t, recovered)
	var recoveredRows []*opensplunkv1.ResultRow
	if len(recoveredEvents) == 1 {
		recoveredRows = recoveredEvents[0].GetResultPreview().GetRows()
	}
	if len(recoveredEvents) != 1 ||
		recoveredEvents[0].GetSequence() != latest ||
		len(recoveredRows) != 1 ||
		recoveredRows[0].GetRowId() != "changed-row" {
		t.Fatalf("recovered disposable preview = %+v, latest %d", recoveredEvents, latest)
	}

	service.queueBudgetMu.Lock()
	globalQueuedBytes := service.queuedBytes
	service.queueBudgetMu.Unlock()
	if globalQueuedBytes != 0 {
		t.Fatalf("pressured disposable preview retained %d global queued bytes", globalQueuedBytes)
	}
}

func TestPreviewTailoringWorkDoesNotConsumeConnectionQueueBudget(t *testing.T) {
	service := adversarialNewService(t, func(config *Config) {
		config.MaximumQueuedBytes = minimumFrameBytes
		config.MaximumTotalQueuedBytes = minimumFrameBytes
	})
	target := adversarialNewTarget(service, "tailored-preview-isolation")
	incarnation := adversarialNow.Add(-time.Hour)
	initial := previewEdgeProjection(1, incarnation, "tailored-schema", true)
	initial.previewRows = 2
	initial.events[len(initial.events)-1].GetResultPreview().Rows = append(
		initial.events[len(initial.events)-1].GetResultPreview().Rows,
		&opensplunkv1.ResultRow{RowId: strings.Repeat("large-row-", 64), Ordinal: 1},
	)
	adversarialApply(t, target, initial, true)

	healthy := newConnection(service, nil)
	defer healthy.cancel()
	healthySubscription := adversarialAttach(target, healthy, "healthy-tailored-preview")
	healthySubscription.previewRows = 1
	defer adversarialDetach(healthySubscription)

	changed := previewEdgeProjection(2, incarnation, "tailored-schema", true)
	changed.previewRows = 2
	changedPreview := changed.events[len(changed.events)-1].GetResultPreview()
	changedPreview.Rows[0].RowId = "changed-tailored-row"
	changedPreview.Rows = append(
		changedPreview.Rows,
		&opensplunkv1.ResultRow{RowId: strings.Repeat("large-row-", 64), Ordinal: 1},
	)
	target.mu.Lock()
	nextSequence := target.latest + 1
	targetProto := proto.Clone(target.target).(*opensplunkv1.JobTarget)
	target.mu.Unlock()
	canonicalEvent := proto.Clone(changed.events[len(changed.events)-1]).(*opensplunkv1.SearchWebSocketEvent)
	canonicalEvent.Sequence = nextSequence
	canonicalEvent.Target = targetProto
	occurredAt, err := timestampToProto(adversarialNow)
	if err != nil {
		t.Fatalf("encode preview occurrence time: %v", err)
	}
	canonicalEvent.OccurredAt = occurredAt
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonicalEvent)
	if err != nil {
		t.Fatalf("marshal canonical preview: %v", err)
	}
	tailored, err := tailorPreviewEvent(canonical, healthySubscription.previewRows)
	if err != nil {
		t.Fatalf("tailor canonical preview: %v", err)
	}
	healthyFrameBytes, err := stampedFrameSize(len(tailored), healthySubscription.id)
	if err != nil || healthyFrameBytes >= minimumFrameBytes || uint64(len(canonical)) <= healthyFrameBytes {
		t.Fatalf(
			"preview sizes = canonical:%d tailored-frame:%d err:%v",
			len(canonical),
			healthyFrameBytes,
			err,
		)
	}

	unrelated := newConnection(service, nil)
	defer unrelated.cancel()
	unrelatedBytes := minimumFrameBytes - healthyFrameBytes
	if !unrelated.enqueue(make([]byte, int(unrelatedBytes))) {
		t.Fatal("failed to fill the unrelated connection to the tailored-frame boundary")
	}
	adversarialApply(t, target, changed, false)

	healthyEvents := adversarialDrain(t, healthy)
	var healthyRows []*opensplunkv1.ResultRow
	if len(healthyEvents) == 1 {
		healthyRows = healthyEvents[0].GetResultPreview().GetRows()
	}
	if len(healthyEvents) != 1 ||
		len(healthyRows) != 1 ||
		healthyRows[0].GetRowId() != "changed-tailored-row" {
		t.Fatalf("healthy tailored preview delivery = %+v", healthyEvents)
	}
	unrelated.queueMu.Lock()
	unrelatedClosed, unrelatedQueuedBytes := unrelated.hardClosed, unrelated.queuedBytes
	unrelated.queueMu.Unlock()
	if unrelatedClosed || unrelatedQueuedBytes != unrelatedBytes {
		t.Fatalf(
			"unrelated queue = closed:%t bytes:%d, want open with %d",
			unrelatedClosed,
			unrelatedQueuedBytes,
			unrelatedBytes,
		)
	}
}

func TestInitialPreviewTailoringWorkDoesNotConsumeConnectionQueueBudget(t *testing.T) {
	reader := &adversarialSearchSnapshots{}
	service := adversarialNewService(t, func(config *Config) {
		config.Searches = adaptSynchronousSearchSnapshots(reader)
		config.MaximumQueuedBytes = minimumFrameBytes
		config.MaximumTotalQueuedBytes = minimumFrameBytes
		config.MaximumReplayBytes = 8 * minimumFrameBytes
		config.MaximumTotalReplayBytes = 8 * minimumFrameBytes
		config.Wait = func(ctx context.Context, _ time.Duration) {
			<-ctx.Done()
		}
	})
	target := adversarialNewTarget(service, "initial-tailored-preview-isolation")
	incarnation := adversarialNow.Add(-time.Hour)
	initial := previewEdgeProjection(1, incarnation, "initial-tailored-schema", true)
	initial.previewRows = 2
	initial.events[len(initial.events)-1].GetResultPreview().Rows = append(
		initial.events[len(initial.events)-1].GetResultPreview().Rows,
		&opensplunkv1.ResultRow{RowId: strings.Repeat("large-row-", 32), Ordinal: 1},
	)
	adversarialApply(t, target, initial, true)
	target.mu.Lock()
	checkpoint := target.latest
	target.mu.Unlock()

	changed := previewEdgeProjection(2, incarnation, "initial-tailored-schema", true)
	changed.previewRows = 2
	changedPreview := changed.events[len(changed.events)-1].GetResultPreview()
	changedPreview.Rows[0].RowId = "changed-initial-tailored-row"
	changedPreview.Rows = append(
		changedPreview.Rows,
		&opensplunkv1.ResultRow{RowId: strings.Repeat("large-row-", 32), Ordinal: 1},
	)
	adversarialApply(t, target, changed, false)

	target.mu.Lock()
	replay, continuous := target.replayAfterLocked(checkpoint)
	earliest, latest := target.replayBoundsLocked()
	target.mu.Unlock()
	if !continuous || len(replay) != 1 {
		t.Fatalf("initial preview replay after %d = continuous:%t frames:%d", checkpoint, continuous, len(replay))
	}
	var canonicalEvent opensplunkv1.SearchWebSocketEvent
	if err := proto.Unmarshal(replay[0], &canonicalEvent); err != nil {
		t.Fatalf("decode retained preview: %v", err)
	}
	if rows := canonicalEvent.GetResultPreview().GetRows(); len(rows) != 2 {
		t.Fatalf("retained preview rows = %d, want 2", len(rows))
	}

	const requestID = "initial-tailoring-request"
	const subscriptionID = "initial-tailored-preview"
	previewRows := uint32(1)
	tailored, err := tailorPreviewEvent(replay[0], previewRows)
	if err != nil {
		t.Fatalf("tailor retained preview: %v", err)
	}
	previewFrameBytes, err := stampedFrameSize(len(tailored), subscriptionID)
	if err != nil {
		t.Fatalf("size tailored preview frame: %v", err)
	}
	ack, err := marshalEvent(subscriptionAcknowledgedEvent(requestID, requestedSubscription{
		id:            subscriptionID,
		key:           target.key,
		afterSequence: checkpoint,
		previewRows:   previewRows,
		replayFollows: true,
		earliest:      earliest,
		latest:        latest,
	}, adversarialNow), service.config.maximumFrameBytes)
	if err != nil {
		t.Fatalf("marshal expected subscription acknowledgment: %v", err)
	}
	finalBatchBytes := uint64(len(ack)) + previewFrameBytes
	if finalBatchBytes >= minimumFrameBytes || uint64(len(replay[0])) <= previewFrameBytes {
		t.Fatalf(
			"initial preview sizes = canonical:%d tailored-frame:%d batch:%d",
			len(replay[0]),
			previewFrameBytes,
			finalBatchBytes,
		)
	}

	unrelated := newConnection(service, nil)
	defer unrelated.hardClose()
	unrelatedBytes := minimumFrameBytes - finalBatchBytes
	if !unrelated.enqueue(make([]byte, int(unrelatedBytes))) {
		t.Fatal("failed to fill the unrelated queue to the exact initial-batch boundary")
	}

	healthy := newConnection(service, nil)
	defer func() {
		healthy.removeAllSubscriptions()
		healthy.hardClose()
	}()
	failure := healthy.subscribe(requestID, &opensplunkv1.SubscribeSearchJobsCommand{
		Subscriptions: []*opensplunkv1.SearchSubscription{{
			SubscriptionId:  subscriptionID,
			Target:          target.key.protobuf(),
			AfterSequence:   checkpoint,
			IncludePreviews: true,
			PreviewRowLimit: &previewRows,
		}},
	})
	if failure != nil {
		t.Fatalf("subscribe() = %v", failure)
	}
	healthy.queueMu.Lock()
	healthyClosed, healthyQueuedBytes := healthy.hardClosed, healthy.queuedBytes
	healthy.queueMu.Unlock()
	if healthyClosed || healthyQueuedBytes != finalBatchBytes {
		t.Fatalf(
			"initial replay queue = closed:%t bytes:%d, want open with %d",
			healthyClosed,
			healthyQueuedBytes,
			finalBatchBytes,
		)
	}

	events := adversarialDrain(t, healthy)
	var acknowledged *opensplunkv1.SubscriptionAcknowledged
	var rows []*opensplunkv1.ResultRow
	if len(events) == 2 {
		acknowledged = events[0].GetSubscriptionAcknowledged()
		rows = events[1].GetResultPreview().GetRows()
	}
	if len(events) != 2 ||
		acknowledged == nil ||
		acknowledged.GetRequestId() != requestID ||
		acknowledged.GetSubscriptionId() != subscriptionID ||
		!acknowledged.GetReplayWillFollow() ||
		acknowledged.GetEarliestAvailableSequence() != earliest ||
		acknowledged.GetLatestSequence() != latest ||
		events[1].GetSubscriptionId() != subscriptionID ||
		events[1].GetSequence() != latest ||
		!events[1].GetResultPreview().GetTruncated() ||
		len(rows) != 1 ||
		rows[0].GetRowId() != "changed-initial-tailored-row" {
		t.Fatalf("initial tailored replay = %+v", events)
	}

	unrelated.queueMu.Lock()
	unrelatedClosed, unrelatedQueuedBytes := unrelated.hardClosed, unrelated.queuedBytes
	unrelated.queueMu.Unlock()
	if unrelatedClosed || unrelatedQueuedBytes != unrelatedBytes {
		t.Fatalf(
			"unrelated queue = closed:%t bytes:%d, want open with %d",
			unrelatedClosed,
			unrelatedQueuedBytes,
			unrelatedBytes,
		)
	}
	service.queueBudgetMu.Lock()
	globalQueuedBytes := service.queuedBytes
	service.queueBudgetMu.Unlock()
	if globalQueuedBytes != unrelatedBytes {
		t.Fatalf("global queued bytes = %d, want unrelated reservation %d", globalQueuedBytes, unrelatedBytes)
	}
	if permits := len(service.tailoringGate); permits != 0 {
		t.Fatalf("preview tailoring permits retained after subscribe = %d", permits)
	}
	if calls := reader.calls.Load(); calls != 0 {
		t.Fatalf("subscribe unexpectedly refreshed the already projected preview %d times", calls)
	}
}

func TestAdversarialRetireAndCloseReleaseExactReplayAccountingAndTailReferences(t *testing.T) {
	t.Run("retire and replay tail", func(t *testing.T) {
		service := adversarialNewService(t, nil)
		target := adversarialNewTarget(service, "retire")
		base := adversarialSequenceBase
		target.mu.Lock()
		target.replay = make([]*storedEvent, 0, 4)
		if !target.storeLocked(eventCategorySearchState, base+1, make([]byte, 10)) ||
			!target.storeLocked(eventCategorySearchState, base+2, make([]byte, 20)) {
			target.mu.Unlock()
			t.Fatal("could not construct replay journal")
		}
		if target.retainedBytes != 30 || !target.dropOldestUnpinnedReplayLocked() {
			target.mu.Unlock()
			t.Fatalf("could not drop the oldest replay event; retained bytes = %d", target.retainedBytes)
		}
		if target.retainedBytes != 20 || len(target.replay) != 1 || target.replay[0].sequence != base+2 {
			target.mu.Unlock()
			t.Fatalf("post-drop journal = bytes:%d replay:%+v", target.retainedBytes, target.replay)
		}
		expanded := target.replay[:cap(target.replay)]
		for index := len(target.replay); index < len(expanded); index++ {
			if expanded[index] != nil {
				target.mu.Unlock()
				t.Fatalf("replay backing array retains tail event at index %d", index)
			}
		}
		target.mu.Unlock()
		if got := adversarialReplayBytes(service); got != 20 {
			t.Fatalf("global replay bytes before retire = %d, want 20", got)
		}

		target.retire()
		target.retire()
		target.mu.Lock()
		retired, retainedBytes, replayLength, retainedLength, currentLength :=
			target.retired, target.retainedBytes, len(target.replay), len(target.retained), len(target.current)
		target.mu.Unlock()
		if !retired || retainedBytes != 0 || replayLength != 0 || retainedLength != 0 || currentLength != 0 {
			t.Fatalf("retired target = retired:%t bytes:%d replay:%d retained:%d current:%d",
				retired, retainedBytes, replayLength, retainedLength, currentLength)
		}
		if got := adversarialReplayBytes(service); got != 0 {
			t.Fatalf("global replay bytes after retire = %d, want 0", got)
		}
	})

	t.Run("service close", func(t *testing.T) {
		service := adversarialNewService(t, nil)
		base := adversarialSequenceBase
		first := adversarialNewTarget(service, "close-first")
		second := adversarialNewTarget(service, "close-second")
		if !adversarialStore(first, eventCategorySearchState, base+1, make([]byte, 11)) ||
			!adversarialStore(second, eventCategorySearchState, base+1, make([]byte, 17)) {
			t.Fatal("could not construct retained journals")
		}
		if got := adversarialReplayBytes(service); got != 28 {
			t.Fatalf("global replay bytes before Close = %d, want 28", got)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Fatal(err)
		}
		service.mu.Lock()
		targetCount := len(service.targets)
		service.mu.Unlock()
		if targetCount != 0 || adversarialReplayBytes(service) != 0 {
			t.Fatalf("post-Close accounting = targets:%d replay:%d", targetCount, adversarialReplayBytes(service))
		}
		for _, target := range []*targetState{first, second} {
			target.mu.Lock()
			retired, retainedBytes, replayLength := target.retired, target.retainedBytes, len(target.replay)
			target.mu.Unlock()
			if !retired || retainedBytes != 0 || replayLength != 0 {
				t.Fatalf("closed target = retired:%t retained:%d replay:%d", retired, retainedBytes, replayLength)
			}
		}
	})
}

func TestAdversarialResolveRetiredRetryBalancesResolverPin(t *testing.T) {
	service := adversarialNewService(t, nil)
	target := adversarialNewTarget(service, "retired-pin")
	// Hold the target lock so resolveTarget has deterministically acquired its
	// pin before it can observe the synthetic retired state.
	target.mu.Lock()
	target.retired = true
	resolved := make(chan error, 1)
	go func() {
		_, err := service.resolveTarget(context.Background(), target.key)
		resolved <- err
	}()
	deadline := time.Now().Add(time.Second)
	for target.resolverCount.Load() != 1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if target.resolverCount.Load() != 1 {
		target.mu.Unlock()
		t.Fatal("resolveTarget did not acquire the target pin")
	}

	closed := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { closed <- service.Close(ctx) }()
	deadline = time.Now().Add(time.Second)
	for {
		service.mu.Lock()
		isClosed := service.closed
		service.mu.Unlock()
		if isClosed || time.Now().After(deadline) {
			break
		}
		runtime.Gosched()
	}
	target.mu.Unlock()

	select {
	case err := <-resolved:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("resolveTarget() = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("resolveTarget did not finish after service closure")
	}
	if count := target.resolverCount.Load(); count != 0 {
		t.Fatalf("retired-target retry leaked resolver pins: %d", count)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestAdversarialRestartSequenceEpochRejectsOldNumericSequenceAfterEstablishment(t *testing.T) {
	firstService := adversarialNewService(t, nil)
	secondService := adversarialNewService(t, nil)
	firstTarget := adversarialNewTarget(firstService, "restart")
	secondTarget := adversarialNewTarget(secondService, "restart")
	secondTarget.mu.Lock()
	secondTarget.epochStart = adversarialSequenceBase + (uint64(1) << 20) + 1
	secondTarget.latest = adversarialSequenceBase + (uint64(1) << 20)
	secondTarget.mu.Unlock()
	projection := adversarialProjection(
		1, adversarialNow.Add(-time.Hour), false,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, 1, true,
	)
	adversarialApply(t, firstTarget, projection, true)
	adversarialApply(t, secondTarget, projection, true)
	firstTarget.mu.Lock()
	oldSequence := firstTarget.latest
	firstTarget.mu.Unlock()

	establisher := newConnection(secondService, nil)
	if failure := establisher.subscribe("establish", &opensplunkv1.SubscribeSearchJobsCommand{Subscriptions: []*opensplunkv1.SearchSubscription{{
		SubscriptionId: "current", Target: secondTarget.key.protobuf(), AfterSequence: 0,
	}}}); failure != nil {
		t.Fatalf("establish subscribe() = %v", failure)
	}
	if events := adversarialDrain(t, establisher); len(events) != 3 {
		t.Fatalf("establish response event count = %d, want 3", len(events))
	}
	establisher.removeAllSubscriptions()
	establisher.cancel()
	secondTarget.mu.Lock()
	established := secondTarget.epochEstablished
	secondTarget.mu.Unlock()
	if !established {
		t.Fatal("after_sequence=0 did not establish the new epoch")
	}
	secondService.mu.Lock()
	registered := secondService.targets[secondTarget.key]
	secondService.mu.Unlock()
	if registered != secondTarget || secondTarget.isRetired() {
		t.Fatalf("new-epoch target disappeared before stale resume: registered:%p target:%p retired:%t", registered, secondTarget, secondTarget.isRetired())
	}

	stale := newConnection(secondService, nil)
	defer stale.cancel()
	if failure := stale.subscribe("stale", &opensplunkv1.SubscribeSearchJobsCommand{Subscriptions: []*opensplunkv1.SearchSubscription{{
		SubscriptionId: "stale", Target: secondTarget.key.protobuf(), AfterSequence: oldSequence,
	}}}); failure != nil {
		t.Fatalf("stale subscribe() = %v", failure)
	}
	events := adversarialDrain(t, stale)
	if len(events) != 2 || events[1].GetResynchronizationRequired() == nil ||
		events[1].GetResynchronizationRequired().GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED {
		t.Fatalf("stale pre-restart numeric sequence was accepted after epoch establishment: %+v", events)
	}
	stale.removeAllSubscriptions()
}

func TestAdversarialPermanentNotFoundPollBacksOffAndNotifiesOnce(t *testing.T) {
	reader := &adversarialSearchSnapshots{}
	service := adversarialNewService(t, func(config *Config) {
		config.Searches = adaptSynchronousSearchSnapshots(reader)
		config.PollInterval = minimumPollInterval
	})
	target := adversarialNewTarget(service, "gone")
	connection := newConnection(service, nil)
	defer connection.cancel()
	subscription := adversarialAttach(target, connection, "gone")
	defer adversarialDetach(subscription)
	target.mu.Lock()
	target.startPollingLocked()
	target.mu.Unlock()

	adversarialWait(t, 2*time.Second, func() bool {
		return reader.calls.Load() >= maximumConsecutivePollFailures
	}, "permanently missing target polling backoff")
	if calls := reader.calls.Load(); calls != maximumConsecutivePollFailures {
		t.Fatalf("permanently missing snapshot calls = %d, want %d", calls, maximumConsecutivePollFailures)
	}
	time.Sleep(4 * minimumPollInterval)
	if calls := reader.calls.Load(); calls != maximumConsecutivePollFailures {
		t.Fatalf("missing target did not back off after notification: calls = %d", calls)
	}
	events := adversarialDrain(t, connection)
	if len(events) != 1 || events[0].GetResynchronizationRequired() == nil ||
		events[0].GetResynchronizationRequired().GetReason() != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED {
		t.Fatalf("poll suppression notification = %+v", events)
	}
}

func TestAdversarialApplyRetireAndResolverPinsRace(t *testing.T) {
	const actorIterations = 200
	service := adversarialNewService(t, nil)
	target := adversarialNewTarget(service, "race")
	incarnation := adversarialNow.Add(-time.Hour)
	start := make(chan struct{})
	errorsSeen := make(chan error, 2*actorIterations+1)
	actorsDone := make(chan struct{})
	var actors sync.WaitGroup
	actors.Add(2)
	var wait sync.WaitGroup
	wait.Add(3)
	go func() {
		defer wait.Done()
		defer actors.Done()
		<-start
		for index := 1; index <= actorIterations; index++ {
			_, err := target.applyProjection(adversarialProjection(
				uint64(index), incarnation, false,
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING, uint64(index), true,
			), index == 1)
			if err != nil && !errors.Is(err, errTargetRetired) {
				errorsSeen <- err
			}
		}
	}()
	go func() {
		defer wait.Done()
		defer actors.Done()
		<-start
		for index := 0; index < actorIterations; index++ {
			resolved, err := service.resolveTarget(context.Background(), target.key)
			if err == nil {
				service.releaseResolvedTarget(resolved)
			} else if !errors.Is(err, errTargetNotFound) {
				errorsSeen <- err
			}
			runtime.Gosched()
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		for {
			if service.evictInactiveTarget(nil) {
				return
			}
			select {
			case <-actorsDone:
				// The actor wait proves every transient resolver pin has been
				// released. One final attempt must now retire the idle target.
				if service.evictInactiveTarget(nil) {
					return
				}
				errorsSeen <- errors.New("could not evict target after resolver pins were released")
				return
			default:
				runtime.Gosched()
			}
		}
	}()
	close(start)
	actors.Wait()
	close(actorsDone)
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent target operation = %v", err)
	}
	if count := target.resolverCount.Load(); count != 0 {
		t.Fatalf("resolver count = %d, want 0", count)
	}
	if !target.isRetired() {
		t.Fatal("target was not retired")
	}
	if got := adversarialReplayBytes(service); got != 0 {
		t.Fatalf("global replay bytes after race = %d, want 0", got)
	}
}
