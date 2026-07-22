package searchws

import (
	"context"
	"crypto/sha256"
	"errors"
	"runtime"
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

func (*adversarialExportSnapshots) Get(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
	return exportjobs.Job{}, exportjobs.ErrNotFound
}

func adversarialNewService(t *testing.T, mutate func(*Config)) *Service {
	t.Helper()
	config := Config{
		Searches:                &adversarialSearchSnapshots{},
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
		service:       service,
		key:           key,
		target:        key.protobuf(),
		ctx:           ctx,
		cancel:        cancel,
		epochStart:    adversarialSequenceBase + 1,
		latest:        adversarialSequenceBase,
		fingerprints:  make(map[eventCategory][sha256.Size]byte),
		expected:      make(map[eventCategory]struct{}),
		current:       make(map[eventCategory]*storedEvent),
		retained:      make(map[uint64]*storedEvent),
		subscriptions: make(map[*subscription]struct{}),
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
	target.subscriptions[subscription] = struct{}{}
	target.subscriberCount.Add(1)
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
	if _, exists := target.subscriptions[subscription]; exists {
		delete(target.subscriptions, subscription)
		target.subscriberCount.Add(-1)
	}
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
				target.subscriptions[subscription] = struct{}{}
				target.subscriberCount.Add(1)
				target.mu.Unlock()
				return func() {
					target.mu.Lock()
					delete(target.subscriptions, subscription)
					target.subscriberCount.Add(-1)
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
	if !subscription.deliver([]byte("not protobuf")) {
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
		config.Searches = reader
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
	service := adversarialNewService(t, nil)
	target := adversarialNewTarget(service, "race")
	incarnation := adversarialNow.Add(-time.Hour)
	start := make(chan struct{})
	errorsSeen := make(chan error, 256)
	var wait sync.WaitGroup
	wait.Add(3)
	go func() {
		defer wait.Done()
		<-start
		for index := 1; index <= 200; index++ {
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
		<-start
		for index := 0; index < 200; index++ {
			resolved, err := service.resolveTarget(context.Background(), target.key)
			if err == nil {
				resolved.resolverCount.Add(-1)
			} else if !errors.Is(err, errTargetNotFound) {
				errorsSeen <- err
			}
			runtime.Gosched()
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		for attempts := 0; attempts < 10_000; attempts++ {
			if service.evictInactiveTarget(nil) {
				return
			}
			runtime.Gosched()
		}
		errorsSeen <- errors.New("could not evict target after resolver pins were released")
	}()
	close(start)
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
