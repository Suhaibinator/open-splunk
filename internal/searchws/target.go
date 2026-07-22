package searchws

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

type targetKind uint8

const (
	targetKindSearch targetKind = iota + 1
	targetKindExport
	maximumConsecutivePollFailures = 6
	maximumPollFailureBackoff      = 30 * time.Second
)

type targetKey struct {
	kind targetKind
	id   string
}

func (key targetKey) protobuf() *opensplunkv1.JobTarget {
	if key.kind == targetKindSearch {
		return &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_SearchJobId{SearchJobId: key.id}}
	}
	return &opensplunkv1.JobTarget{Target: &opensplunkv1.JobTarget_ExportJobId{ExportJobId: key.id}}
}

func (key targetKey) recoveryPath() string {
	if key.kind == targetKindSearch {
		return "/api/v1/search/jobs/get"
	}
	return "/api/v1/search/exports/get"
}

type eventCategory uint8

const (
	eventCategorySearchState eventCategory = iota + 1
	eventCategorySearchProgress
	eventCategorySchema
	eventCategoryWarning
	eventCategorySearchTerminal
	eventCategoryExportState
	eventCategoryExportProgress
	eventCategoryExportTerminal
)

type storedEvent struct {
	sequence uint64
	data     []byte
	bytes    uint64
	category eventCategory
	inReplay bool
	current  bool
}

type targetState struct {
	service *Service
	key     targetKey
	target  *opensplunkv1.JobTarget
	ctx     context.Context
	cancel  context.CancelFunc

	publishMu         sync.Mutex
	mu                sync.Mutex
	retired           bool
	initialized       bool
	version           uint64
	incarnation       time.Time
	terminal          bool
	refreshAt         time.Time
	epochStart        uint64
	latest            uint64
	fingerprints      map[eventCategory][sha256.Size]byte
	expected          map[eventCategory]struct{}
	current           map[eventCategory]*storedEvent
	replay            []*storedEvent
	retained          map[uint64]*storedEvent
	retainedBytes     uint64
	currentIncomplete bool
	subscriptions     map[*subscription]struct{}
	subscriberCount   atomic.Int32
	resolverCount     atomic.Int32
	lastAccess        atomic.Uint64
	epochEstablished  bool
	polling           bool
	pollGeneration    uint64
	pollCancel        context.CancelFunc
}

type targetLoad struct {
	done      chan struct{}
	target    *targetState
	err       error
	waiters   int
	completed bool
}

var (
	errTargetNotFound = errors.New("search websocket target not found")
	errTargetCapacity = errors.New("search websocket target capacity exhausted")
	errTargetRetired  = errors.New("search websocket target retired")
)

func (service *Service) resolveTarget(ctx context.Context, key targetKey) (*targetState, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		service.mu.Lock()
		if service.closed {
			service.mu.Unlock()
			return nil, context.Canceled
		}
		if target := service.targets[key]; target != nil {
			target.resolverCount.Add(1)
			service.touchTargetLocked(target)
			service.mu.Unlock()
			if target.isRetired() {
				target.resolverCount.Add(-1)
				continue
			}
			return target, nil
		}
		if load := service.loads[key]; load != nil {
			load.waiters++
			service.mu.Unlock()
			return service.waitForTargetLoad(ctx, load)
		}
		if len(service.targets)+len(service.loads) >= service.config.maximumTargets {
			candidate := service.inactiveTargetLocked()
			if candidate == nil {
				service.mu.Unlock()
				return nil, errTargetCapacity
			}
			delete(service.targets, candidate.key)
			service.mu.Unlock()
			candidate.retire()
			continue
		}
		load := &targetLoad{done: make(chan struct{}), waiters: 1}
		service.loads[key] = load
		service.loadWG.Add(1)
		go service.performTargetLoad(key, load)
		service.mu.Unlock()
		return service.waitForTargetLoad(ctx, load)
	}
}

func (service *Service) waitForTargetLoad(ctx context.Context, load *targetLoad) (*targetState, error) {
	select {
	case <-load.done:
		if load.err != nil {
			return nil, load.err
		}
		if err := ctx.Err(); err != nil {
			service.releaseResolvedTarget(load.target)
			return nil, err
		}
		if service.ctx.Err() != nil {
			service.releaseResolvedTarget(load.target)
			return nil, context.Canceled
		}
		return load.target, nil
	case <-ctx.Done():
		service.cancelTargetLoadWaiter(load)
		return nil, ctx.Err()
	case <-service.ctx.Done():
		service.cancelTargetLoadWaiter(load)
		return nil, context.Canceled
	}
}

func (service *Service) cancelTargetLoadWaiter(load *targetLoad) {
	service.mu.Lock()
	if load.completed {
		if load.err == nil && load.target != nil {
			load.target.resolverCount.Add(-1)
		}
	} else if load.waiters > 0 {
		load.waiters--
	}
	service.mu.Unlock()
}

func (service *Service) performTargetLoad(key targetKey, load *targetLoad) {
	defer service.loadWG.Done()
	projection, err := service.loadProjection(service.ctx, key)
	var target *targetState
	sequenceBase := uint64(0)
	if err == nil {
		sequenceBase, err = randomSequenceBase()
		if err != nil {
			err = errors.New("search websocket sequence epoch unavailable")
		}
	}
	if err == nil {
		targetContext, cancel := context.WithCancel(service.ctx)
		target = &targetState{
			service: service, key: key, target: key.protobuf(), ctx: targetContext, cancel: cancel,
			epochStart: sequenceBase + 1, latest: sequenceBase,
			fingerprints: make(map[eventCategory][sha256.Size]byte), expected: make(map[eventCategory]struct{}),
			current: make(map[eventCategory]*storedEvent), retained: make(map[uint64]*storedEvent),
			subscriptions: make(map[*subscription]struct{}),
		}
		if _, applyErr := target.applyProjection(projection, true); applyErr != nil {
			target.retire()
			target = nil
			err = errTargetNotFound
		}
	}

	service.mu.Lock()
	delete(service.loads, key)
	if service.closed && err == nil {
		err = context.Canceled
	}
	if err == nil {
		service.targets[key] = target
		target.resolverCount.Add(int32(load.waiters))
		service.touchTargetLocked(target)
	}
	if err == nil {
		load.target = target
	}
	load.err = err
	load.completed = true
	close(load.done)
	service.mu.Unlock()
	if err != nil && target != nil {
		target.retire()
	}
}

func (service *Service) touchTargetLocked(target *targetState) {
	service.accessSequence++
	target.lastAccess.Store(service.accessSequence)
}

func (service *Service) inactiveTargetLocked() *targetState {
	var candidate *targetState
	for _, target := range service.targets {
		if target.subscriberCount.Load() != 0 || target.resolverCount.Load() != 0 {
			continue
		}
		if candidate == nil || target.lastAccess.Load() < candidate.lastAccess.Load() {
			candidate = target
		}
	}
	return candidate
}

func (service *Service) loadProjection(ctx context.Context, key targetKey) (targetProjection, error) {
	if err := ctx.Err(); err != nil {
		return targetProjection{}, err
	}
	now := canonicalTime(service.config.now())
	if key.kind == targetKindSearch {
		job, err := service.config.searches.GetFor(service.config.access, key.id)
		if err != nil || job.ID != key.id {
			return targetProjection{}, errTargetNotFound
		}
		if err := ctx.Err(); err != nil {
			return targetProjection{}, err
		}
		projection, err := projectSearch(job, now)
		if err != nil {
			return targetProjection{}, errTargetNotFound
		}
		return projection, nil
	}
	job, err := service.config.exports.Get(ctx, service.config.access, key.id)
	if err != nil || job.ID != key.id {
		if ctx.Err() != nil {
			return targetProjection{}, ctx.Err()
		}
		return targetProjection{}, errTargetNotFound
	}
	projection, err := projectExport(job, now)
	if err != nil {
		return targetProjection{}, errTargetNotFound
	}
	return projection, nil
}

func (target *targetState) pollLoop(ctx context.Context, generation uint64) {
	defer target.service.targetWG.Done()
	defer func() {
		target.mu.Lock()
		if target.pollGeneration == generation {
			target.polling = false
			target.pollCancel = nil
			if !target.retired && len(target.subscriptions) != 0 {
				target.startPollingLocked()
			}
		}
		target.mu.Unlock()
	}()
	delay := target.nextScheduledPollDelay(true)
	if delay < 0 {
		return
	}
	failures := 0
	failureNotified := false
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			_ = timer.Stop()
			return
		case <-timer.C:
		}
		projection, err := target.service.loadProjection(ctx, target.key)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			if failures >= maximumConsecutivePollFailures && !failureNotified {
				target.notifyPollingFailure()
				failureNotified = true
			}
			delay = nextPollDelay(delay, target.service.config.pollInterval)
			continue
		}
		terminal, err := target.applyProjection(projection, false)
		if err != nil {
			failures++
			if failures >= maximumConsecutivePollFailures && !failureNotified {
				target.notifyPollingFailure()
				failureNotified = true
			}
			delay = nextPollDelay(delay, target.service.config.pollInterval)
			continue
		}
		failures = 0
		failureNotified = false
		if terminal {
			delay = target.nextScheduledPollDelay(false)
			if delay < 0 {
				return
			}
			continue
		}
		delay = target.service.config.pollInterval
	}
}

func nextPollDelay(current, baseline time.Duration) time.Duration {
	ceiling := maximumPollFailureBackoff
	if baseline > ceiling {
		ceiling = baseline
	}
	if current < baseline {
		current = baseline
	}
	if current >= ceiling || current > ceiling-current {
		return ceiling
	}
	return current * 2
}

func (target *targetState) nextScheduledPollDelay(immediateOverdue bool) time.Duration {
	target.mu.Lock()
	defer target.mu.Unlock()
	if !target.terminal {
		return target.service.config.pollInterval
	}
	now := canonicalTime(target.service.config.now())
	if target.refreshAt.IsZero() {
		return -1
	}
	if !target.refreshAt.After(now) {
		if immediateOverdue {
			return 0
		}
		return target.service.config.pollInterval
	}
	return target.refreshAt.Sub(now)
}

func (target *targetState) notifyPollingFailure() {
	target.publishMu.Lock()
	target.mu.Lock()
	if target.retired {
		target.mu.Unlock()
		target.publishMu.Unlock()
		return
	}
	target.currentIncomplete = true
	subscribers := make([]*subscription, 0, len(target.subscriptions))
	for subscription := range target.subscriptions {
		subscribers = append(subscribers, subscription)
	}
	earliest, latest := target.replayBoundsLocked()
	target.mu.Unlock()
	for _, subscription := range subscribers {
		if !subscription.deliverControl(resynchronizationEvent(
			subscription.id, target.key,
			opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED,
			earliest, latest, target.service.config.now(),
		)) {
			subscription.connection.hardClose()
		}
	}
	target.publishMu.Unlock()
}

// startPollingLocked starts at most one poller. target.mu must be held. Polling
// is demand-driven so idle retained journals do not amplify manager load.
func (target *targetState) startPollingLocked() {
	if target.retired || target.polling || len(target.subscriptions) == 0 ||
		(target.terminal && target.refreshAt.IsZero()) {
		return
	}
	ctx, cancel := context.WithCancel(target.ctx)
	target.pollGeneration++
	generation := target.pollGeneration
	target.service.mu.Lock()
	if target.service.closed {
		target.service.mu.Unlock()
		cancel()
		return
	}
	target.service.targetWG.Add(1)
	target.service.mu.Unlock()
	target.polling = true
	target.pollCancel = cancel
	go target.pollLoop(ctx, generation)
}

// stopPollingLocked requests a stop without declaring the goroutine stopped.
// Its deferred handoff restarts polling if a subscriber raced with the stop.
func (target *targetState) stopPollingLocked() {
	if target.pollCancel != nil {
		target.pollCancel()
	}
}

func (target *targetState) applyProjection(projection targetProjection, initial bool) (bool, error) {
	target.publishMu.Lock()
	defer target.publishMu.Unlock()

	target.mu.Lock()
	if target.retired {
		target.mu.Unlock()
		return false, errTargetRetired
	}
	if target.initialized && !target.currentIncomplete && projection.version == target.version && projection.incarnation.Equal(target.incarnation) {
		terminal := target.terminal
		target.mu.Unlock()
		return terminal, nil
	}
	diverged := target.initialized && (!projection.incarnation.Equal(target.incarnation) || projection.version < target.version)
	if diverged {
		target.clearProjectionLocked()
		target.epochStart = target.latest + 1
	}
	priorFingerprints := make(map[eventCategory][sha256.Size]byte, len(target.fingerprints))
	for category, fingerprint := range target.fingerprints {
		priorFingerprints[category] = fingerprint
	}
	missingCurrent := make(map[eventCategory]struct{})
	for category := range target.expected {
		if target.current[category] == nil {
			missingCurrent[category] = struct{}{}
		}
	}
	wasIncomplete := target.currentIncomplete
	target.mu.Unlock()

	type candidate struct {
		category    eventCategory
		fingerprint [sha256.Size]byte
		event       *opensplunkv1.SearchWebSocketEvent
	}
	all := make([]candidate, 0, len(projection.events))
	expected := make(map[eventCategory]struct{}, len(projection.events))
	for _, event := range projection.events {
		category, err := categoryForEvent(event)
		if err != nil {
			return false, err
		}
		fingerprint, err := fingerprintEvent(event, category)
		if err != nil {
			return false, err
		}
		expected[category] = struct{}{}
		_, missing := missingCurrent[category]
		if initial || diverged || missing || fingerprint != priorFingerprints[category] {
			all = append(all, candidate{category: category, fingerprint: fingerprint, event: event})
		}
	}

	now, err := timestampToProto(target.service.config.now())
	if err != nil {
		return false, err
	}
	type prepared struct {
		category    eventCategory
		fingerprint [sha256.Size]byte
		sequence    uint64
		data        []byte
	}
	preparedEvents := make([]prepared, 0, len(all))
	oversized := make(map[eventCategory]struct{})
	for _, item := range all {
		target.mu.Lock()
		if target.latest == ^uint64(0) {
			target.mu.Unlock()
			return false, errors.New("search websocket target sequence exhausted")
		}
		target.latest++
		sequence := target.latest
		target.mu.Unlock()
		item.event.Sequence = sequence
		item.event.OccurredAt = now
		item.event.Target = proto.Clone(target.target).(*opensplunkv1.JobTarget)
		if warning := item.event.GetWarning(); warning != nil {
			warning.Target = proto.Clone(target.target).(*opensplunkv1.JobTarget)
		}
		data, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(item.event)
		if marshalErr != nil {
			return false, marshalErr
		}
		if uint64(len(data))+maximumSubscriptionIDBytes+16 > target.service.config.maximumFrameBytes {
			oversized[item.category] = struct{}{}
			continue
		}
		preparedEvents = append(preparedEvents, prepared{
			category: item.category, fingerprint: item.fingerprint, sequence: sequence, data: data,
		})
	}

	target.mu.Lock()
	if target.retired {
		target.mu.Unlock()
		return false, errTargetRetired
	}
	for category := range target.expected {
		if _, exists := expected[category]; !exists {
			target.removeCurrentLocked(category)
			delete(target.fingerprints, category)
		}
	}
	for _, item := range all {
		target.fingerprints[item.category] = item.fingerprint
	}
	for category := range oversized {
		target.removeCurrentLocked(category)
	}
	retentionFailed := len(oversized) != 0
	for _, item := range preparedEvents {
		if !target.storeLocked(item.category, item.sequence, item.data) {
			retentionFailed = true
		}
	}
	target.expected = expected
	target.currentIncomplete = false
	for category := range expected {
		if target.current[category] == nil {
			target.currentIncomplete = true
			break
		}
	}
	target.initialized = true
	target.version = projection.version
	target.incarnation = projection.incarnation
	target.terminal = projection.terminal
	target.refreshAt = projection.refreshAt
	subscribers := make([]*subscription, 0, len(target.subscriptions))
	for subscription := range target.subscriptions {
		subscribers = append(subscribers, subscription)
	}
	earliest, latest := target.replayBoundsLocked()
	target.mu.Unlock()

	if diverged || retentionFailed {
		if diverged || !wasIncomplete {
			reason := opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED
			if diverged {
				reason = opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED
			}
			for _, subscription := range subscribers {
				if !subscription.deliverControl(resynchronizationEvent(
					subscription.id, target.key, reason, earliest, latest, target.service.config.now(),
				)) {
					subscription.connection.hardClose()
				}
			}
		}
		return projection.terminal, nil
	}
	for _, item := range preparedEvents {
		for _, subscription := range subscribers {
			if !subscription.deliver(item.data) {
				subscription.connection.hardClose()
			}
		}
	}
	return projection.terminal, nil
}

// storeLocked pins the newest component independently of the replay event-count
// limit, but charges its bytes to both per-target and global retained budgets.
// This keeps after_sequence=0 useful with very small rings without creating an
// unbounded current-projection side channel.
func (target *targetState) storeLocked(category eventCategory, sequence uint64, data []byte) bool {
	target.removeCurrentLocked(category)
	bytes := uint64(len(data))
	for (bytes > target.service.config.maximumReplayBytes ||
		target.retainedBytes > target.service.config.maximumReplayBytes-bytes) && target.dropOldestUnpinnedReplayLocked() {
	}
	if bytes > target.service.config.maximumReplayBytes ||
		target.retainedBytes > target.service.config.maximumReplayBytes-bytes {
		return false
	}
	for !target.service.reserveReplayBytes(bytes) {
		if !target.dropOldestUnpinnedReplayLocked() {
			if !target.service.evictInactiveTarget(target) {
				return false
			}
		}
	}
	event := &storedEvent{sequence: sequence, data: data, bytes: bytes, category: category, current: true}
	target.retained[sequence] = event
	target.retainedBytes += bytes
	target.current[category] = event
	for len(target.replay) >= target.service.config.maximumReplayEvents && target.dropOldestUnpinnedReplayLocked() {
	}
	if len(target.replay) < target.service.config.maximumReplayEvents {
		event.inReplay = true
		target.replay = append(target.replay, event)
	}
	return true
}

func (target *targetState) removeCurrentLocked(category eventCategory) {
	event := target.current[category]
	delete(target.current, category)
	if event == nil {
		return
	}
	event.current = false
	if !event.inReplay {
		target.releaseEventLocked(event)
	}
}

func (target *targetState) dropOldestUnpinnedReplayLocked() bool {
	for index, event := range target.replay {
		if event.current {
			continue
		}
		event.inReplay = false
		copy(target.replay[index:], target.replay[index+1:])
		target.replay[len(target.replay)-1] = nil
		target.replay = target.replay[:len(target.replay)-1]
		target.releaseEventLocked(event)
		return true
	}
	return false
}

func (target *targetState) releaseEventLocked(event *storedEvent) {
	if _, exists := target.retained[event.sequence]; !exists {
		return
	}
	delete(target.retained, event.sequence)
	target.retainedBytes -= event.bytes
	target.service.releaseReplayBytes(event.bytes)
}

func (target *targetState) clearProjectionLocked() {
	for _, event := range target.retained {
		target.service.releaseReplayBytes(event.bytes)
	}
	target.replay = nil
	target.retained = make(map[uint64]*storedEvent)
	target.retainedBytes = 0
	target.current = make(map[eventCategory]*storedEvent)
	target.fingerprints = make(map[eventCategory][sha256.Size]byte)
	target.expected = make(map[eventCategory]struct{})
	target.currentIncomplete = true
}

func (target *targetState) replayBoundsLocked() (uint64, uint64) {
	if target.latest == 0 {
		return 0, 0
	}
	earliest := target.latest
	if target.retained[target.latest] == nil {
		return 0, target.latest
	}
	for sequence := target.latest; sequence > 1; sequence-- {
		if target.retained[sequence-1] == nil {
			break
		}
		earliest = sequence - 1
	}
	return earliest, target.latest
}

func (target *targetState) currentEventsLocked() [][]byte {
	events := make([]*storedEvent, 0, len(target.current))
	for _, event := range target.current {
		events = append(events, event)
	}
	sort.Slice(events, func(left, right int) bool { return events[left].sequence < events[right].sequence })
	result := make([][]byte, len(events))
	for index, event := range events {
		result[index] = event.data
	}
	return result
}

func (target *targetState) replayAfterLocked(after uint64) ([][]byte, bool) {
	if after >= target.latest {
		return nil, after == target.latest
	}
	result := make([][]byte, 0, len(target.retained))
	for sequence := after + 1; ; sequence++ {
		event := target.retained[sequence]
		if event == nil {
			return nil, false
		}
		result = append(result, event.data)
		if sequence == target.latest {
			break
		}
	}
	return result, true
}

func (target *targetState) isRetired() bool {
	target.mu.Lock()
	defer target.mu.Unlock()
	return target.retired
}

func (target *targetState) retire() {
	target.cancel()
	target.publishMu.Lock()
	target.mu.Lock()
	if !target.retired {
		target.retired = true
		target.stopPollingLocked()
		target.clearProjectionLocked()
	}
	target.mu.Unlock()
	target.publishMu.Unlock()
}

func categoryForEvent(event *opensplunkv1.SearchWebSocketEvent) (eventCategory, error) {
	switch event.GetPayload().(type) {
	case *opensplunkv1.SearchWebSocketEvent_SearchStateChanged:
		return eventCategorySearchState, nil
	case *opensplunkv1.SearchWebSocketEvent_SearchProgress:
		return eventCategorySearchProgress, nil
	case *opensplunkv1.SearchWebSocketEvent_ResultSchemaAvailable:
		return eventCategorySchema, nil
	case *opensplunkv1.SearchWebSocketEvent_Warning:
		return eventCategoryWarning, nil
	case *opensplunkv1.SearchWebSocketEvent_SearchTerminal:
		return eventCategorySearchTerminal, nil
	case *opensplunkv1.SearchWebSocketEvent_ExportStateChanged:
		return eventCategoryExportState, nil
	case *opensplunkv1.SearchWebSocketEvent_ExportProgress:
		return eventCategoryExportProgress, nil
	case *opensplunkv1.SearchWebSocketEvent_ExportTerminal:
		return eventCategoryExportTerminal, nil
	default:
		return 0, fmt.Errorf("search websocket projection: unsupported event payload %T", event.GetPayload())
	}
}

func fingerprintEvent(event *opensplunkv1.SearchWebSocketEvent, category eventCategory) ([sha256.Size]byte, error) {
	cloned := proto.Clone(event).(*opensplunkv1.SearchWebSocketEvent)
	cloned.Sequence = 0
	cloned.OccurredAt = nil
	cloned.SubscriptionId = nil
	cloned.Target = nil
	if state := cloned.GetSearchStateChanged(); state != nil {
		state.StateVersion = 0
	}
	if state := cloned.GetExportStateChanged(); state != nil {
		state.StateVersion = 0
	}
	if warning := cloned.GetWarning(); warning != nil {
		warning.Target = nil
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(cloned)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	_ = category
	return sha256.Sum256(data), nil
}

func resynchronizationEvent(
	subscriptionID string,
	key targetKey,
	reason opensplunkv1.ResynchronizationReason,
	earliest, latest uint64,
	now time.Time,
) *opensplunkv1.SearchWebSocketEvent {
	timestamp, _ := timestampToProto(now)
	target := key.protobuf()
	return &opensplunkv1.SearchWebSocketEvent{
		OccurredAt: timestamp, SubscriptionId: &subscriptionID, Target: proto.Clone(target).(*opensplunkv1.JobTarget),
		Payload: &opensplunkv1.SearchWebSocketEvent_ResynchronizationRequired{ResynchronizationRequired: &opensplunkv1.ResynchronizationRequired{
			SubscriptionId: subscriptionID, Target: target, Reason: reason,
			EarliestAvailableSequence: earliest, LatestSequence: latest, RecoveryPath: key.recoveryPath(),
		}},
	}
}

type subscription struct {
	id         string
	target     *targetState
	connection *connection
	mu         sync.Mutex
	active     bool
}

func (subscription *subscription) deliver(data []byte) bool {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if !subscription.active {
		return true
	}
	stamped, err := stampSubscriptionID(data, subscription.id, subscription.connection.service.config.maximumFrameBytes)
	return err == nil && subscription.connection.enqueue(stamped)
}

func (subscription *subscription) deliverControl(event *opensplunkv1.SearchWebSocketEvent) bool {
	data, err := marshalEvent(event, subscription.connection.service.config.maximumFrameBytes)
	if err != nil {
		return false
	}
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	return !subscription.active || subscription.connection.enqueue(data)
}

func stampSubscriptionID(data []byte, subscriptionID string, maximumFrameBytes uint64) ([]byte, error) {
	var event opensplunkv1.SearchWebSocketEvent
	if err := proto.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	event.SubscriptionId = &subscriptionID
	stamped, err := proto.MarshalOptions{Deterministic: true}.Marshal(&event)
	if err != nil {
		return nil, err
	}
	if uint64(len(stamped)) > maximumFrameBytes {
		return nil, errors.New("search websocket event exceeds frame limit after subscription stamping")
	}
	return stamped, nil
}
