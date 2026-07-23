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
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	eventCategoryPreview
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

	refreshMu         sync.Mutex
	publishMu         sync.Mutex
	mu                sync.Mutex
	retired           bool
	initialized       bool
	version           uint64
	incarnation       time.Time
	projectedPreviews uint32
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
	pendingPreviews   map[*subscription]uint32
	previewDemand     []uint32
	previewMaximum    uint32
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
	releaseProjection, err := service.acquireProjection(service.ctx)
	if err == nil {
		defer releaseProjection()
	}
	var projection targetProjection
	if err == nil {
		projection, err = service.loadProjectionWithPermit(service.ctx, key, 0)
	}
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
			subscriptions: make(map[*subscription]struct{}), pendingPreviews: make(map[*subscription]uint32),
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

func (service *Service) loadProjection(ctx context.Context, key targetKey, previewRows uint32) (targetProjection, error) {
	release, err := service.acquireProjection(ctx)
	if err != nil {
		return targetProjection{}, err
	}
	defer release()
	return service.loadProjectionWithPermit(ctx, key, previewRows)
}

func (service *Service) acquireProjection(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("search websocket projection context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case service.projectionGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-service.ctx.Done():
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		<-service.projectionGate
		return nil, err
	}
	if err := service.ctx.Err(); err != nil {
		<-service.projectionGate
		return nil, context.Canceled
	}
	return func() { <-service.projectionGate }, nil
}

// loadProjectionWithPermit materializes a bounded detached projection while
// its caller owns one service-wide projection permit. Production callers keep
// that permit through applyProjection so large object graphs cannot accumulate
// behind a target publication lock.
func (service *Service) loadProjectionWithPermit(
	ctx context.Context,
	key targetKey,
	previewRows uint32,
) (targetProjection, error) {
	now := canonicalTime(service.config.now())
	if key.kind == targetKindSearch {
		var (
			job     searchjobs.Job
			preview *searchjobs.PreviewSnapshot
			err     error
		)
		if previewRows > 0 {
			previewBytes := min(service.config.maximumFrameBytes, service.config.maximumReplayBytes)
			snapshot, previewErr := service.config.searches.PreviewForBytes(
				service.config.access, key.id, int(previewRows), previewBytes,
			)
			if previewErr == nil {
				job = snapshot.Job
				preview = &snapshot
			} else if errors.Is(previewErr, searchjobs.ErrResultsNotReady) ||
				errors.Is(previewErr, searchjobs.ErrResultsUnavailable) ||
				errors.Is(previewErr, searchjobs.ErrExpired) {
				job, err = service.config.searches.GetFor(service.config.access, key.id)
			} else {
				err = previewErr
			}
		} else {
			job, err = service.config.searches.GetFor(service.config.access, key.id)
		}
		if err != nil || job.ID != key.id {
			return targetProjection{}, errTargetNotFound
		}
		if err := ctx.Err(); err != nil {
			return targetProjection{}, err
		}
		projection, err := projectSearchWithPreview(ctx, job, preview, previewRows, now)
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
		terminal, err := target.refreshCurrentProjection(ctx)
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
	if target.projectedPreviews != target.maximumPreviewRowsLocked() {
		return 0
	}
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
	previewRefresh := target.projectedPreviews != target.maximumPreviewRowsLocked()
	if target.retired || target.polling || len(target.subscriptions) == 0 ||
		(target.terminal && target.refreshAt.IsZero() && !previewRefresh) {
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

func (target *targetState) maximumPreviewRowsLocked() uint32 {
	return target.previewMaximum
}

func (target *targetState) addPreviewDemandLocked(previewRows uint32) {
	if previewRows == 0 {
		return
	}
	if target.previewDemand == nil {
		target.previewDemand = make([]uint32, int(target.service.config.maximumPreviewRows)+1)
	}
	target.previewDemand[previewRows]++
	if previewRows > target.previewMaximum {
		target.previewMaximum = previewRows
	}
}

func (target *targetState) removePreviewDemandLocked(previewRows uint32) {
	if previewRows == 0 || int(previewRows) >= len(target.previewDemand) || target.previewDemand[previewRows] == 0 {
		return
	}
	target.previewDemand[previewRows]--
	for target.previewMaximum > 0 && target.previewDemand[target.previewMaximum] == 0 {
		target.previewMaximum--
	}
}

func (target *targetState) addPendingPreviewLocked(subscription *subscription) {
	if prior, exists := target.pendingPreviews[subscription]; exists {
		target.removePreviewDemandLocked(prior)
	}
	target.pendingPreviews[subscription] = subscription.previewRows
	target.addPreviewDemandLocked(subscription.previewRows)
}

func (target *targetState) removePendingPreviewLocked(subscription *subscription) {
	previewRows, exists := target.pendingPreviews[subscription]
	if !exists {
		return
	}
	delete(target.pendingPreviews, subscription)
	target.removePreviewDemandLocked(previewRows)

}

func (target *targetState) addSubscriptionLocked(subscription *subscription) {
	if _, exists := target.subscriptions[subscription]; exists {
		return
	}
	target.subscriptions[subscription] = struct{}{}
	target.addPreviewDemandLocked(subscription.previewRows)
	target.subscriberCount.Add(1)
}

func (target *targetState) removeSubscriptionLocked(subscription *subscription) bool {
	if _, exists := target.subscriptions[subscription]; !exists {
		return false
	}
	delete(target.subscriptions, subscription)
	target.removePreviewDemandLocked(subscription.previewRows)
	target.subscriberCount.Add(-1)
	return true
}

// refreshForSubscription serializes demand-driven loads with the poller and
// coalesces concurrent subscribers onto the largest already projected prefix.
// A current-state subscription also requests a full snapshot barrier when the
// retained category heads are not contiguous, matching the first-party
// client's target-sequence contract.
func (target *targetState) refreshForSubscription(
	ctx context.Context,
	requestedRows uint32,
	currentSnapshot bool,
) (bool, error) {
	target.refreshMu.Lock()
	defer target.refreshMu.Unlock()

	target.mu.Lock()
	previewRows := max(requestedRows, target.maximumPreviewRowsLocked())
	// Another subscription refresh may have projected demand that has not yet
	// committed its subscriber. Never let a later, smaller request regress it.
	previewRows = max(previewRows, target.projectedPreviews)
	// Rebuilding every category is safe only when no installed subscriber can
	// observe the new sequence range. Otherwise a joining client uses the
	// resynchronization path instead of amplifying one private bootstrap into a
	// full broadcast to every existing watcher.
	exclusiveBootstrap := currentSnapshot && len(target.subscriptions) == 0
	forceFull := previewRows != target.projectedPreviews ||
		(exclusiveBootstrap && !target.currentEventsContinuousLocked())
	if !forceFull {
		terminal := target.terminal
		target.mu.Unlock()
		return terminal, nil
	}
	target.mu.Unlock()

	release, err := target.service.acquireProjection(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	projection, err := target.service.loadProjectionWithPermit(ctx, target.key, previewRows)
	if err != nil {
		return false, err
	}
	return target.applyProjection(projection, exclusiveBootstrap)
}

func (target *targetState) refreshCurrentProjection(ctx context.Context) (bool, error) {
	target.refreshMu.Lock()
	defer target.refreshMu.Unlock()
	target.mu.Lock()
	previewRows := target.maximumPreviewRowsLocked()
	target.mu.Unlock()
	release, err := target.service.acquireProjection(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	projection, err := target.service.loadProjectionWithPermit(ctx, target.key, previewRows)
	if err != nil {
		return false, err
	}
	return target.applyProjection(projection, false)
}

func (target *targetState) currentEventsContinuousLocked() bool {
	if len(target.current) == 0 {
		return false
	}
	minimum, maximum := ^uint64(0), uint64(0)
	for _, event := range target.current {
		if event.sequence < minimum {
			minimum = event.sequence
		}
		if event.sequence > maximum {
			maximum = event.sequence
		}
	}
	return maximum == target.latest && maximum-minimum == uint64(len(target.current)-1)
}

func (target *targetState) applyProjection(projection targetProjection, initial bool) (bool, error) {
	target.publishMu.Lock()
	defer target.publishMu.Unlock()

	target.mu.Lock()
	if target.retired {
		target.mu.Unlock()
		return false, errTargetRetired
	}
	// A full-category rebuild is safe only while no installed subscriber can
	// observe it. A subscriber may have installed while the snapshot reader was
	// blocked, so revalidate the bootstrap decision under publishMu.
	if initial && len(target.subscriptions) != 0 {
		initial = false
	}
	sameIncarnation := projection.incarnation.Equal(target.incarnation)
	if target.initialized && sameIncarnation && projection.version < target.version {
		// Loads are serialized in production, but cancellation and test fakes can
		// still return a snapshot captured before a newer projection. Never turn
		// an out-of-order read into a false epoch regression.
		terminal := target.terminal
		target.mu.Unlock()
		return terminal, nil
	}
	if !initial && target.initialized && !target.currentIncomplete && projection.version == target.version &&
		projection.previewRows == target.projectedPreviews && projection.incarnation.Equal(target.incarnation) {
		terminal := target.terminal
		target.mu.Unlock()
		return terminal, nil
	}
	diverged := target.initialized && !sameIncarnation
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
	var priorSchemaData, priorPreviewData []byte
	if current := target.current[eventCategorySchema]; current != nil {
		priorSchemaData = current.data
	}
	if current := target.current[eventCategoryPreview]; current != nil {
		priorPreviewData = current.data
	}
	target.mu.Unlock()
	priorPreviewVisible, err := previewHasVisibleState(priorPreviewData)
	if err != nil {
		return false, err
	}
	// A target-wide empty RESET is a replayable tombstone. Emit one whenever
	// preview demand disappears or a terminal state invalidates visible rows;
	// silently deleting the category would let an old checkpoint replay newer
	// terminal state while retaining stale rows in the client.
	authoritativePreview := priorPreviewVisible && (projection.previewRows == 0 || projection.invalidatesPreview)
	if authoritativePreview {
		projection.events, err = previewInvalidationEvents(
			projection.events, priorSchemaData, priorPreviewData, projection.version, target.key.id,
		)
		if err != nil {
			return false, err
		}
	}
	now, err := timestampToProto(target.service.config.now())
	if err != nil {
		return false, err
	}
	previewLimit, err := target.previewEventByteLimit(projection.events, now)
	if err != nil {
		return false, err
	}

	type candidate struct {
		category    eventCategory
		fingerprint [sha256.Size]byte
		event       *opensplunkv1.SearchWebSocketEvent
	}
	all := make([]candidate, 0, len(projection.events))
	expected := make(map[eventCategory]struct{}, len(projection.events))
	previewChanged := false
	requiredPreviewOmitted := false
	for _, event := range projection.events {
		if event.GetResultPreview() != nil {
			if err := boundPreviewEvent(event, target.target, now, previewLimit); err != nil {
				// Ordinary previews are disposable. A terminal invalidation RESET is
				// authoritative: omission must force resynchronization so a client
				// cannot keep displaying rows that are no longer valid.
				if authoritativePreview {
					requiredPreviewOmitted = true
				}
				continue
			}
		}
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
		changed := initial || diverged || missing || fingerprint != priorFingerprints[category]
		if category == eventCategoryPreview && changed {
			previewChanged = true
		}
		if category == eventCategorySearchTerminal && previewChanged {
			changed = true
		}
		if changed {
			all = append(all, candidate{category: category, fingerprint: fingerprint, event: event})
		}
	}

	type prepared struct {
		category             eventCategory
		fingerprint          [sha256.Size]byte
		sequence             uint64
		data                 []byte
		previewRows          int
		authoritativePreview bool
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
		stampedBytes, sizeErr := stampedFrameSizeForIDBytes(len(data), maximumSubscriptionIDBytes)
		if sizeErr != nil || stampedBytes > target.service.config.maximumFrameBytes {
			oversized[item.category] = struct{}{}
			continue
		}
		previewRows := -1
		if preview := item.event.GetResultPreview(); preview != nil {
			previewRows = len(preview.GetRows())
		}
		preparedEvents = append(preparedEvents, prepared{
			category: item.category, fingerprint: item.fingerprint, sequence: sequence, data: data,
			previewRows: previewRows, authoritativePreview: item.category == eventCategoryPreview && authoritativePreview,
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
	retentionFailed := len(oversized) != 0 || requiredPreviewOmitted
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
	target.projectedPreviews = projection.previewRows
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
		if item.category == eventCategoryPreview {
			groups := make(map[uint32][]*subscription)
			for _, subscription := range subscribers {
				groups[subscription.previewRows] = append(groups[subscription.previewRows], subscription)
			}
			for limit, group := range groups {
				canonical := item.data
				var workBytes uint64
				if int(limit) < item.previewRows {
					workBytes = uint64(len(item.data))
					if !target.service.reserveQueuedBytes(workBytes) {
						if item.authoritativePreview {
							for _, subscription := range group {
								subscription.connection.hardClose()
							}
						}
						continue
					}
					var err error
					canonical, err = tailorPreviewEvent(item.data, limit)
					if err != nil {
						target.service.releaseQueuedBytes(workBytes)
						for _, subscription := range group {
							subscription.connection.hardClose()
						}
						continue
					}
					tailoredBytes := uint64(len(canonical))
					if tailoredBytes > workBytes {
						target.service.releaseQueuedBytes(workBytes)
						for _, subscription := range group {
							subscription.connection.hardClose()
						}
						continue
					}
					target.service.releaseQueuedBytes(workBytes - tailoredBytes)
					workBytes = tailoredBytes
					if !item.authoritativePreview {
						mayAccept := false
						for _, subscription := range group {
							if subscription.mayAcceptCanonicalPreview(canonical) {
								mayAccept = true
								break
							}
						}
						if !mayAccept {
							target.service.releaseQueuedBytes(workBytes)
							continue
						}
					}
				}
				for _, subscription := range group {
					if !subscription.deliverCanonical(canonical, true, !item.authoritativePreview) {
						subscription.connection.hardClose()
					}
				}
				if workBytes != 0 {
					target.service.releaseQueuedBytes(workBytes)
				}
			}
			continue
		}
		for _, subscription := range subscribers {
			if !subscription.deliverCanonical(item.data, false, false) {
				subscription.connection.hardClose()
			}
		}
	}
	return projection.terminal, nil
}

func previewHasVisibleState(data []byte) (bool, error) {
	if len(data) == 0 {
		return false, nil
	}
	var event opensplunkv1.SearchWebSocketEvent
	if err := proto.Unmarshal(data, &event); err != nil {
		return false, errors.New("search websocket retained preview is invalid")
	}
	preview := event.GetResultPreview()
	if preview == nil {
		return false, errors.New("search websocket retained preview is invalid")
	}
	return len(preview.GetRows()) != 0 || preview.GetTruncated(), nil
}

func previewInvalidationEvents(
	events []*opensplunkv1.SearchWebSocketEvent,
	priorSchemaData, priorPreviewData []byte,
	version uint64,
	jobID string,
) ([]*opensplunkv1.SearchWebSocketEvent, error) {
	if len(priorPreviewData) == 0 {
		return events, nil
	}
	if len(priorSchemaData) == 0 {
		return nil, errors.New("search websocket preview invalidation lacks its prior schema")
	}
	priorSchema := new(opensplunkv1.SearchWebSocketEvent)
	if err := proto.Unmarshal(priorSchemaData, priorSchema); err != nil || priorSchema.GetResultSchemaAvailable() == nil {
		return nil, errors.New("search websocket preview invalidation lacks its prior schema")
	}
	priorSchema.Sequence = 0
	priorSchema.OccurredAt = nil
	priorSchema.SubscriptionId = nil
	priorSchema.Target = nil

	priorPreview := new(opensplunkv1.SearchWebSocketEvent)
	if err := proto.Unmarshal(priorPreviewData, priorPreview); err != nil || priorPreview.GetResultPreview() == nil {
		return nil, errors.New("search websocket preview invalidation lacks its prior preview")
	}
	schemaID := priorPreview.GetResultPreview().GetSchemaId()
	revision := version
	if priorRevision := priorPreview.GetResultPreview().GetPreviewRevision(); revision <= priorRevision {
		revision = priorRevision
		if revision != ^uint64(0) {
			revision++
		}
	}
	reset := &opensplunkv1.SearchWebSocketEvent{Payload: &opensplunkv1.SearchWebSocketEvent_ResultPreview{
		ResultPreview: &opensplunkv1.ResultPreview{
			SearchJobId: jobID, SchemaId: schemaID, PreviewRevision: revision,
			UpdateMode: opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET,
		},
	}}

	result := make([]*opensplunkv1.SearchWebSocketEvent, 0, len(events)+2)
	inserted := false
	for _, event := range events {
		if !inserted && event.GetSearchTerminal() != nil {
			result = append(result, priorSchema, reset)
			inserted = true
		}
		result = append(result, event)
	}
	if !inserted {
		result = append(result, priorSchema, reset)
	}
	return result, nil
}

func (target *targetState) previewEventByteLimit(
	events []*opensplunkv1.SearchWebSocketEvent,
	now *timestamppb.Timestamp,
) (uint64, error) {
	var nonPreviewBytes uint64
	for _, event := range events {
		if event.GetResultPreview() != nil {
			continue
		}
		cloned := proto.Clone(event).(*opensplunkv1.SearchWebSocketEvent)
		cloned.Sequence = ^uint64(0)
		cloned.OccurredAt = now
		cloned.SubscriptionId = nil
		cloned.Target = proto.Clone(target.target).(*opensplunkv1.JobTarget)
		if warning := cloned.GetWarning(); warning != nil {
			warning.Target = proto.Clone(target.target).(*opensplunkv1.JobTarget)
		}
		size := uint64(proto.Size(cloned))
		if size > ^uint64(0)-nonPreviewBytes {
			return 0, errors.New("search websocket current projection size overflow")
		}
		nonPreviewBytes += size
	}
	if nonPreviewBytes >= target.service.config.maximumReplayBytes {
		return 0, nil
	}
	return min(
		target.service.config.maximumFrameBytes,
		target.service.config.maximumReplayBytes-nonPreviewBytes,
	), nil
}

// boundPreviewEvent shrinks a disposable preview to the largest row prefix
// that fits even after the largest legal subscription identifier is stamped.
// Envelope fields are installed temporarily so size checks include their exact
// protobuf overhead; only the bounded rows and truncation bit remain changed.
func boundPreviewEvent(
	event *opensplunkv1.SearchWebSocketEvent,
	target *opensplunkv1.JobTarget,
	now *timestamppb.Timestamp,
	maximumFrameBytes uint64,
) error {
	preview := event.GetResultPreview()
	if preview == nil {
		return nil
	}
	rows := preview.Rows
	wasTruncated := preview.Truncated
	sequence, occurredAt, eventTarget, subscriptionID := event.Sequence, event.OccurredAt, event.Target, event.SubscriptionId
	event.Sequence = ^uint64(0)
	event.OccurredAt = now
	event.Target = proto.Clone(target).(*opensplunkv1.JobTarget)
	event.SubscriptionId = nil
	defer func() {
		event.Sequence = sequence
		event.OccurredAt = occurredAt
		event.Target = eventTarget
		event.SubscriptionId = subscriptionID
	}()
	fits := func(count int) bool {
		preview.Rows = rows[:count]
		preview.Truncated = wasTruncated || count < len(rows)
		stampedBytes, err := stampedFrameSizeForIDBytes(proto.Size(event), maximumSubscriptionIDBytes)
		return err == nil && stampedBytes <= maximumFrameBytes
	}
	if !fits(0) {
		preview.Rows = nil
		preview.Truncated = wasTruncated || len(rows) != 0
		return errors.New("search websocket preview envelope exceeds frame limit")
	}
	if fits(len(rows)) {
		return nil
	}
	low, high := 0, len(rows)+1
	for low+1 < high {
		middle := low + (high-low)/2
		if fits(middle) {
			low = middle
		} else {
			high = middle
		}
	}
	preview.Rows = rows[:low]
	preview.Truncated = wasTruncated || low < len(rows)
	return nil
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
	case *opensplunkv1.SearchWebSocketEvent_ResultPreview:
		return eventCategoryPreview, nil
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
	if category == eventCategoryPreview {
		// The revision is a resume/debugging token, not visible preview content.
		// Rows appended beyond an already-truncated prefix must not retransmit an
		// otherwise identical RESET on every poll.
		cloned.GetResultPreview().PreviewRevision = 0
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(cloned)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
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
	id          string
	target      *targetState
	connection  *connection
	previewRows uint32
	mu          sync.Mutex
	active      bool
}

func (subscription *subscription) deliverCanonical(data []byte, preview, disposable bool) bool {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if !subscription.active {
		return true
	}
	if preview {
		result := subscription.connection.enqueueCanonicalPreview(data, subscription.id)
		return result == queueAccepted || (disposable && result == queuePressure)
	}
	stamped, err := stampCanonicalSubscriptionID(data, subscription.id, subscription.connection.service.config.maximumFrameBytes)
	if err != nil {
		return false
	}
	return subscription.connection.enqueue(stamped)
}

func (subscription *subscription) mayAcceptCanonicalPreview(data []byte) bool {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if !subscription.active {
		return false
	}
	frameBytes, err := stampedFrameSize(len(data), subscription.id)
	return err == nil && subscription.connection.previewQueueMayAccept(frameBytes)
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

func tailorPreviewEvent(data []byte, previewRows uint32) ([]byte, error) {
	var event opensplunkv1.SearchWebSocketEvent
	if err := proto.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	preview := event.GetResultPreview()
	if preview == nil {
		return nil, errors.New("search websocket event does not contain a preview")
	}
	available := len(preview.Rows)
	limit := int(previewRows)
	if limit >= available && event.SubscriptionId == nil {
		return data, nil
	}
	if limit < available {
		preview.Rows = preview.Rows[:limit]
		preview.Truncated = true
	}
	event.SubscriptionId = nil
	return proto.MarshalOptions{Deterministic: true}.Marshal(&event)
}

func stampCanonicalSubscriptionID(data []byte, subscriptionID string, maximumFrameBytes uint64) ([]byte, error) {
	frameBytes, err := stampedFrameSize(len(data), subscriptionID)
	if err != nil || frameBytes > maximumFrameBytes {
		return nil, errors.New("search websocket event exceeds frame limit after subscription stamping")
	}
	fieldBytes := int(frameBytes) - len(data)
	stamped := make([]byte, len(data), len(data)+fieldBytes)
	copy(stamped, data)
	stamped = protowire.AppendTag(stamped, 3, protowire.BytesType)
	stamped = protowire.AppendString(stamped, subscriptionID)
	return stamped, nil
}

func stampedFrameSize(dataBytes int, subscriptionID string) (uint64, error) {
	return stampedFrameSizeForIDBytes(dataBytes, len(subscriptionID))
}

func stampedFrameSizeForIDBytes(dataBytes, subscriptionIDBytes int) (uint64, error) {
	if dataBytes < 0 || subscriptionIDBytes < 0 {
		return 0, errors.New("search websocket event size is invalid")
	}
	fieldBytes := protowire.SizeTag(3) + protowire.SizeBytes(subscriptionIDBytes)
	if uint64(dataBytes) > ^uint64(0)-uint64(fieldBytes) {
		return 0, errors.New("search websocket event size overflow")
	}
	return uint64(dataBytes) + uint64(fieldBytes), nil
}

func canonicalEventSequence(data []byte) (uint64, error) {
	for len(data) != 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 {
			return 0, protowire.ParseError(tagBytes)
		}
		data = data[tagBytes:]
		if number == 1 {
			if wireType != protowire.VarintType {
				return 0, errors.New("search websocket canonical event has an invalid sequence")
			}
			sequence, valueBytes := protowire.ConsumeVarint(data)
			if valueBytes < 0 {
				return 0, protowire.ParseError(valueBytes)
			}
			if sequence == 0 {
				return 0, errors.New("search websocket canonical event has no sequence")
			}
			return sequence, nil
		}
		valueBytes := protowire.ConsumeFieldValue(number, wireType, data)
		if valueBytes < 0 {
			return 0, protowire.ParseError(valueBytes)
		}
		data = data[valueBytes:]
	}
	return 0, errors.New("search websocket canonical event has no sequence")
}

func hasPreviewPayload(data []byte) (bool, error) {
	for len(data) != 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 {
			return false, protowire.ParseError(tagBytes)
		}
		data = data[tagBytes:]
		valueBytes := protowire.ConsumeFieldValue(number, wireType, data)
		if valueBytes < 0 {
			return false, protowire.ParseError(valueBytes)
		}
		if number == 15 {
			return true, nil
		}
		data = data[valueBytes:]
	}
	return false, nil
}
