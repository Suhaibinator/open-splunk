package collectorfleet

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

var (
	// ErrHeartbeatRuntimeClosed means the owned heartbeat worker no longer
	// accepts collector lifecycle or telemetry work.
	ErrHeartbeatRuntimeClosed = errors.New("collector heartbeat runtime is closed")
	// ErrHeartbeatLeaseNotActive means the complete lease is not the runtime's
	// current active lease for its trusted tenant and collector key.
	ErrHeartbeatLeaseNotActive = errors.New("collector heartbeat lease is not active")
	// errHeartbeatRuntimeWriteTimeout distinguishes an owned persistence-cycle
	// deadline from ordinary caller lifecycle cancellation for error reporting.
	errHeartbeatRuntimeWriteTimeout = errors.New(
		"collector heartbeat persistence cycle timed out",
	)
)

// LivenessState is the process-local connection state for an exact durable
// lease. Administrative disabled state is deliberately overlaid by callers.
type LivenessState string

const (
	LivenessStateOffline LivenessState = "offline"
	LivenessStateOnline  LivenessState = "online"
	LivenessStateStale   LivenessState = "stale"
)

// HeartbeatWriter conditionally persists one normalized, complete telemetry
// snapshot under the exact durable lease.
type HeartbeatWriter interface {
	RecordHeartbeat(context.Context, Lease, Heartbeat) (bool, error)
}

// HeartbeatRuntimeConfig bounds the process-local latest-wins buffer and its
// independently owned persistence and monotonic liveness clocks.
type HeartbeatRuntimeConfig struct {
	MaxCollectors     int
	HeartbeatInterval time.Duration
	StaleGrace        time.Duration
	FlushInterval     time.Duration
	// WriteTimeout bounds an entire periodic or public Flush cycle and each
	// standalone Release or Close persistence attempt.
	WriteTimeout time.Duration
	MonotonicNow func() time.Time
	// OnError runs asynchronously without runtime locks. At most one callback
	// runs at a time; repeated errors are coalesced while it is still running.
	OnError func(error)
}

type heartbeatRuntimeKey struct {
	tenantID    string
	collectorID string
}

type heartbeatRuntimeEntry struct {
	lease               Lease
	active              bool
	deadline            time.Time
	highestObservation  uint64
	pending             Heartbeat
	hasPendingHeartbeat bool
}

type heartbeatRuntimeWrite struct {
	key       heartbeatRuntimeKey
	lease     Lease
	heartbeat Heartbeat
}

// HeartbeatRuntime owns one bounded latest-wins heartbeat worker and exact
// process-local liveness entries. The zero value is not usable.
type HeartbeatRuntime struct {
	writer        HeartbeatWriter
	maxCollectors int
	staleAfter    time.Duration
	flushInterval time.Duration
	writeTimeout  time.Duration
	monotonicNow  func() time.Time
	onError       func(error)
	workerContext context.Context
	cancelWorker  context.CancelFunc
	workerDone    chan struct{}
	mu            sync.RWMutex
	entries       map[heartbeatRuntimeKey]*heartbeatRuntimeEntry
	closed        bool
	flushMu       sync.Mutex
	callbackMu    sync.Mutex
	callbackAlive bool
	closeOnce     sync.Once
	closeErr      error
}

// NewHeartbeatRuntime starts one owned periodic worker. Heartbeats are
// normalized and copied before Offer returns, and the worker never retains an
// RPC or startup context.
func NewHeartbeatRuntime(
	writer HeartbeatWriter,
	config HeartbeatRuntimeConfig,
) (*HeartbeatRuntime, error) {
	if writer == nil {
		return nil, invalid("heartbeat writer is required")
	}
	if config.MaxCollectors <= 0 {
		return nil, invalid("heartbeat runtime collector capacity must be positive")
	}
	if config.HeartbeatInterval <= 0 {
		return nil, invalid("heartbeat interval must be positive")
	}
	if config.StaleGrace < 0 {
		return nil, invalid("heartbeat stale grace must not be negative")
	}
	if config.HeartbeatInterval > time.Duration(math.MaxInt64)-config.StaleGrace {
		return nil, invalid("heartbeat stale deadline is outside the supported range")
	}
	if config.FlushInterval <= 0 {
		return nil, invalid("heartbeat flush interval must be positive")
	}
	if config.WriteTimeout <= 0 {
		return nil, invalid("heartbeat write timeout must be positive")
	}
	if config.MonotonicNow == nil {
		return nil, invalid("heartbeat monotonic clock is required")
	}
	// The returned runtime owns cancelWorker and calls it exactly once from
	// idempotent Close after rejecting new work.
	workerContext, cancelWorker := context.WithCancel(context.Background()) //nolint:gosec
	runtime := &HeartbeatRuntime{
		writer:        writer,
		maxCollectors: config.MaxCollectors,
		staleAfter:    config.HeartbeatInterval + config.StaleGrace,
		flushInterval: config.FlushInterval,
		writeTimeout:  config.WriteTimeout,
		monotonicNow:  config.MonotonicNow,
		onError:       config.OnError,
		workerContext: workerContext,
		cancelWorker:  cancelWorker,
		workerDone:    make(chan struct{}),
		entries:       make(map[heartbeatRuntimeKey]*heartbeatRuntimeEntry),
	}
	go runtime.run()
	return runtime, nil
}

// Activate marks an already process-current durable lease online. A key may
// only advance to a strictly newer generation while it remains in the
// runtime; an equal or older activation cannot share or replace authority.
func (runtime *HeartbeatRuntime) Activate(lease Lease) error {
	if runtime == nil {
		return ErrHeartbeatRuntimeClosed
	}
	normalized, err := normalizeLease(lease)
	if err != nil {
		return err
	}
	key := heartbeatKey(normalized)
	deadline := runtime.monotonicNow().Add(runtime.staleAfter)

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return ErrHeartbeatRuntimeClosed
	}
	current, exists := runtime.entries[key]
	if exists && normalized.Generation <= current.lease.Generation {
		return ErrHeartbeatLeaseNotActive
	}
	if !exists && len(runtime.entries) >= runtime.maxCollectors {
		return control.ErrCapacityExceeded
	}
	runtime.entries[key] = &heartbeatRuntimeEntry{
		lease:    normalized,
		active:   true,
		deadline: deadline,
	}
	return nil
}

// Offer synchronously validates, detaches, and accepts at most the newest
// observation for the exact active lease. The returned boolean means accepted
// into this bounded runtime, not durably applied by HeartbeatWriter.
func (runtime *HeartbeatRuntime) Offer(
	ctx context.Context,
	lease Lease,
	heartbeat Heartbeat,
) (bool, error) {
	if runtime == nil {
		return false, ErrHeartbeatRuntimeClosed
	}
	if err := validateContext(ctx); err != nil {
		return false, err
	}
	normalizedLease, err := normalizeLease(lease)
	if err != nil {
		return false, err
	}
	normalizedHeartbeat, err := normalizeHeartbeat(heartbeat)
	if err != nil {
		return false, err
	}
	key := heartbeatKey(normalizedLease)
	deadline := runtime.monotonicNow().Add(runtime.staleAfter)

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return false, ErrHeartbeatRuntimeClosed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	entry, exists := runtime.entries[key]
	if !exists || !entry.active || !sameHeartbeatLease(entry.lease, normalizedLease) {
		return false, ErrHeartbeatLeaseNotActive
	}
	if normalizedHeartbeat.ObservationSequence <= entry.highestObservation {
		return false, nil
	}
	entry.highestObservation = normalizedHeartbeat.ObservationSequence
	entry.pending = normalizedHeartbeat
	entry.hasPendingHeartbeat = true
	entry.deadline = deadline
	return true, nil
}

// Release conditionally marks the exact lease offline, serializes with any
// in-flight flush, and makes one caller-bounded attempt to persist its latest
// pending snapshot. A discarded or failed final snapshot is also reported to
// OnError after runtime locks are released. Delayed release for a predecessor
// cannot touch a successor.
func (runtime *HeartbeatRuntime) Release(ctx context.Context, lease Lease) error {
	if runtime == nil {
		return ErrHeartbeatRuntimeClosed
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	normalized, err := normalizeLease(lease)
	if err != nil {
		return err
	}
	key := heartbeatKey(normalized)

	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return ErrHeartbeatRuntimeClosed
	}
	entry, exists := runtime.entries[key]
	if !exists || !entry.active || !sameHeartbeatLease(entry.lease, normalized) {
		runtime.mu.Unlock()
		return nil
	}
	entry.active = false
	runtime.mu.Unlock()

	runtime.flushMu.Lock()
	runtime.mu.Lock()
	if err := ctx.Err(); err != nil {
		var droppedErr error
		if current, ok := runtime.entries[key]; ok &&
			sameHeartbeatLease(current.lease, normalized) &&
			!runtime.closed {
			if current.hasPendingHeartbeat {
				droppedErr = fmt.Errorf(
					"drop collector %q heartbeat observation %d during release: %w",
					normalized.CollectorID,
					current.pending.ObservationSequence,
					err,
				)
			}
			delete(runtime.entries, key)
		}
		runtime.mu.Unlock()
		runtime.flushMu.Unlock()
		runtime.reportError(droppedErr)
		return err
	}
	write, hasWrite := runtime.takeExactPendingLocked(key, normalized)
	runtime.mu.Unlock()

	var flushErr error
	if hasWrite {
		flushErr = runtime.persistWrite(ctx, write)
	}
	runtime.mu.Lock()
	if current, ok := runtime.entries[key]; ok &&
		sameHeartbeatLease(current.lease, normalized) {
		delete(runtime.entries, key)
	}
	runtime.mu.Unlock()
	runtime.flushMu.Unlock()
	runtime.reportError(flushErr)
	return flushErr
}

// Liveness reports process-local state only when lease is the exact current
// active lease. The deadline is calculated solely from the injected monotonic
// clock and is never derived from collector or server wall timestamps.
func (runtime *HeartbeatRuntime) Liveness(lease Lease) (LivenessState, bool) {
	if runtime == nil {
		return LivenessStateOffline, false
	}
	normalized, err := normalizeLease(lease)
	if err != nil {
		return LivenessStateOffline, false
	}
	now := runtime.monotonicNow()
	key := heartbeatKey(normalized)
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	if runtime.closed {
		return LivenessStateOffline, false
	}
	entry, exists := runtime.entries[key]
	if !exists || !entry.active || !sameHeartbeatLease(entry.lease, normalized) {
		return LivenessStateOffline, false
	}
	if !now.Before(entry.deadline) {
		return LivenessStateStale, true
	}
	return LivenessStateOnline, true
}

// Flush makes one caller-bounded attempt to persist every currently pending
// active snapshot. Failed snapshots remain boundedly retained only while the
// same exact lease still owns the entry and no newer snapshot replaced them.
// An inactive entry may be waiting behind this flush for its final Release
// drain, so failed writes remain available to that exact lifecycle operation.
func (runtime *HeartbeatRuntime) Flush(ctx context.Context) error {
	if runtime == nil {
		return ErrHeartbeatRuntimeClosed
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	err := runtime.flush(ctx)
	if shouldReportHeartbeatRuntimeError(err) {
		runtime.reportError(err)
	}
	return err
}

// Close rejects new work, cancels and joins the periodic worker, makes one
// final caller-bounded flush, clears liveness, and guarantees that this runtime
// will make no writer calls after it returns. Concurrent callers wait for the
// same close operation and observe the same result.
func (runtime *HeartbeatRuntime) Close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		return invalid("heartbeat runtime close context is required")
	}
	runtime.closeOnce.Do(func() {
		runtime.mu.Lock()
		runtime.closed = true
		runtime.mu.Unlock()
		runtime.cancelWorker()
		<-runtime.workerDone

		// Joining the worker is not sufficient: a caller-owned Flush or an
		// already admitted Release may still be inside the writer. Hold the
		// global write barrier through both the final attempt and entry clear
		// so no delayed caller can write after Close returns.
		runtime.flushMu.Lock()
		if err := ctx.Err(); err != nil {
			runtime.closeErr = err
		} else {
			runtime.mu.Lock()
			writes := runtime.takePendingLocked(true)
			runtime.mu.Unlock()
			for _, write := range writes {
				if err := ctx.Err(); err != nil {
					runtime.closeErr = errors.Join(runtime.closeErr, err)
					break
				}
				if err := runtime.persistWrite(ctx, write); err != nil {
					runtime.closeErr = errors.Join(runtime.closeErr, err)
				}
			}
		}
		runtime.mu.Lock()
		clear(runtime.entries)
		runtime.mu.Unlock()
		runtime.flushMu.Unlock()
	})
	return runtime.closeErr
}

func (runtime *HeartbeatRuntime) run() {
	defer close(runtime.workerDone)
	ticker := time.NewTicker(runtime.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runtime.workerContext.Done():
			return
		case <-ticker.C:
			err := runtime.flush(runtime.workerContext)
			if shouldReportHeartbeatRuntimeError(err) {
				runtime.reportError(err)
			}
		}
	}
}

// flush serializes periodic and caller-requested writes into the writer.
func (runtime *HeartbeatRuntime) flush(ctx context.Context) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	runtime.flushMu.Lock()
	defer runtime.flushMu.Unlock()

	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return ErrHeartbeatRuntimeClosed
	}
	writes := runtime.takePendingLocked(false)
	runtime.mu.Unlock()
	if len(writes) == 0 {
		return nil
	}

	cycleContext, cancelCycle := context.WithTimeout(ctx, runtime.writeTimeout)
	defer cancelCycle()
	var flushErr error
	for index, write := range writes {
		if err := cycleContext.Err(); err != nil {
			runtime.requeueWrites(writes[index:])
			flushErr = errors.Join(flushErr, err)
			break
		}
		err := runtime.persistWriteContext(cycleContext, write)
		if err == nil {
			continue
		}
		runtime.requeueWrite(write)
		flushErr = errors.Join(flushErr, err)
	}
	if flushErr != nil &&
		errors.Is(cycleContext.Err(), context.DeadlineExceeded) &&
		ctx.Err() == nil {
		flushErr = errors.Join(flushErr, errHeartbeatRuntimeWriteTimeout)
	}
	return flushErr
}

func (runtime *HeartbeatRuntime) persistWrite(
	ctx context.Context,
	write heartbeatRuntimeWrite,
) error {
	writeContext, cancel := context.WithTimeout(ctx, runtime.writeTimeout)
	defer cancel()
	return runtime.persistWriteContext(writeContext, write)
}

// persistWriteContext uses an already bounded persistence context. Periodic
// flushes share one cycle deadline; standalone Release and Close calls use
// persistWrite above to install their own per-write bound.
func (runtime *HeartbeatRuntime) persistWriteContext(
	ctx context.Context,
	write heartbeatRuntimeWrite,
) error {
	_, err := runtime.writer.RecordHeartbeat(
		ctx,
		write.lease,
		write.heartbeat,
	)
	if err != nil {
		return fmt.Errorf(
			"persist collector %q heartbeat observation %d: %w",
			write.lease.CollectorID,
			write.heartbeat.ObservationSequence,
			err,
		)
	}
	// applied=false is terminal but intentionally not an error: it may mean
	// either fencing/disablement or a commit-ambiguous retry already won.
	return nil
}

func (runtime *HeartbeatRuntime) takePendingLocked(
	includeInactive bool,
) []heartbeatRuntimeWrite {
	var writes []heartbeatRuntimeWrite
	for key, entry := range runtime.entries {
		if !entry.hasPendingHeartbeat || (!includeInactive && !entry.active) {
			continue
		}
		if writes == nil {
			writes = make(
				[]heartbeatRuntimeWrite,
				0,
				len(runtime.entries),
			)
		}
		writes = append(writes, heartbeatRuntimeWrite{
			key:       key,
			lease:     entry.lease,
			heartbeat: entry.pending,
		})
		entry.pending = Heartbeat{}
		entry.hasPendingHeartbeat = false
	}
	return writes
}

func (runtime *HeartbeatRuntime) takeExactPendingLocked(
	key heartbeatRuntimeKey,
	lease Lease,
) (heartbeatRuntimeWrite, bool) {
	entry, exists := runtime.entries[key]
	if !exists || !sameHeartbeatLease(entry.lease, lease) ||
		!entry.hasPendingHeartbeat {
		return heartbeatRuntimeWrite{}, false
	}
	write := heartbeatRuntimeWrite{
		key:       key,
		lease:     entry.lease,
		heartbeat: entry.pending,
	}
	entry.pending = Heartbeat{}
	entry.hasPendingHeartbeat = false
	return write, true
}

func (runtime *HeartbeatRuntime) requeueWrites(writes []heartbeatRuntimeWrite) {
	for _, write := range writes {
		runtime.requeueWrite(write)
	}
}

func (runtime *HeartbeatRuntime) requeueWrite(write heartbeatRuntimeWrite) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	entry, exists := runtime.entries[write.key]
	if !exists || !sameHeartbeatLease(entry.lease, write.lease) {
		return
	}
	if entry.hasPendingHeartbeat &&
		entry.pending.ObservationSequence >= write.heartbeat.ObservationSequence {
		return
	}
	entry.pending = write.heartbeat
	entry.hasPendingHeartbeat = true
}

func (runtime *HeartbeatRuntime) reportError(err error) {
	if err == nil || runtime.onError == nil {
		return
	}
	runtime.callbackMu.Lock()
	if runtime.callbackAlive {
		runtime.callbackMu.Unlock()
		return
	}
	runtime.callbackAlive = true
	runtime.callbackMu.Unlock()
	go func() {
		defer func() {
			_ = recover()
			runtime.callbackMu.Lock()
			runtime.callbackAlive = false
			runtime.callbackMu.Unlock()
		}()
		runtime.onError(err)
	}()
}

func shouldReportHeartbeatRuntimeError(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if shouldReportHeartbeatRuntimeError(child) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return shouldReportHeartbeatRuntimeError(child)
		}
	}
	return !errors.Is(err, ErrHeartbeatRuntimeClosed) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}

func heartbeatKey(lease Lease) heartbeatRuntimeKey {
	return heartbeatRuntimeKey{
		tenantID:    lease.TenantID,
		collectorID: lease.CollectorID,
	}
}

func sameHeartbeatLease(left, right Lease) bool {
	return left.TenantID == right.TenantID &&
		left.CollectorID == right.CollectorID &&
		left.BootEpoch == right.BootEpoch &&
		left.StreamID == right.StreamID &&
		left.Generation == right.Generation
}
