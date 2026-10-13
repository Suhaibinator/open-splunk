package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// Lane describes one independent unit of queue-draining work.
//
// T is the task type the lane claims and processes; keeping the generic parameter on
// the lane (rather than the harness) lets a single worker combine lanes of different
// task types via the type-erased LaneRunner returned by NewLane.
type Lane[T any] struct {
	// Settings governs admission throughout the lane lifetime when supplied.
	Settings func() Admission
	// Name identifies the lane for targeted wakes (Harness.SignalLane) and logging.
	Name string
	// MaxConcurrent bounds how many items the lane processes simultaneously.
	MaxConcurrent int
	// PollInterval is the lane's idle re-check cadence; when <= 0 it falls back to the
	// harness default supplied to NewHarness.
	PollInterval time.Duration
	// Claim atomically claims the next queued item, or reports that none is available.
	Claim Claimer[T]
	// Process handles one claimed item. Tracing context, heartbeats, and metrics belong
	// here, not in the harness.
	Process Processor[T]
}

// LaneRunner is the type-erased form of a Lane. Workers build runners with NewLane,
// preserving each lane's concrete task type, then hand the erased runners to NewHarness.
// The interface's methods are unexported so only this package drives the loop; callers
// interact with lanes only through the Harness.
type LaneRunner interface {
	name() string
	run(ctx context.Context, defaultPollInterval time.Duration, logger *zap.Logger)
	signal()
}

type laneRunner[T any] struct {
	lane   Lane[T]
	wakeCh chan struct{}
}

// NewLane builds a type-erased LaneRunner for a lane of task type T. The returned
// runner owns its own coalesced (cap-1) wake channel for its entire lifetime.
//
// Parameters:
// - lane (`Lane[T]`): The lane definition (name, concurrency, optional poll interval, claim/process).
//
// Returns:
// - LaneRunner: The erased runner, ready to be passed to NewHarness.
func NewLane[T any](lane Lane[T]) LaneRunner {
	return &laneRunner[T]{
		lane:   lane,
		wakeCh: make(chan struct{}, 1),
	}
}

func (r *laneRunner[T]) name() string {
	return r.lane.Name
}

// signal requests an immediate drain via a coalesced, non-blocking send. The send is
// always safe: the channel is never closed, so a signal delivered while the lane is not
// running simply buffers a single pending wake (or is dropped if one is already pending).
func (r *laneRunner[T]) signal() {
	select {
	case r.wakeCh <- struct{}{}:
	default:
		// A wake is already pending for this lane; coalesce.
	}
}

// run eager-drains the lane once, then drains again on every wake signal or poll tick
// until the context is cancelled. The eager first drain avoids waiting a full poll
// interval for work that is already queued at startup.
func (r *laneRunner[T]) run(ctx context.Context, defaultPollInterval time.Duration, logger *zap.Logger) {
	if r.lane.Settings != nil {
		for ctx.Err() == nil {
			before := r.lane.Settings()
			r.drain(ctx, defaultPollInterval, logger)
			if !Wait(ctx, r.wakeCh, func() time.Duration {
				cfg := r.lane.Settings()
				if cfg.Enabled != before.Enabled {
					return time.Nanosecond
				}
				if !cfg.Enabled {
					return time.Second
				}
				return cfg.PollInterval
			}) {
				return
			}
		}
		return
	}
	pollInterval := r.lane.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	r.drain(ctx, pollInterval, logger)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.drain(ctx, pollInterval, logger)
		case <-r.wakeCh:
			r.drain(ctx, pollInterval, logger)
		}
	}
}

func (r *laneRunner[T]) drain(ctx context.Context, pollInterval time.Duration, logger *zap.Logger) {
	options := DrainOptions{IdlePollInterval: pollInterval, WakeCh: r.wakeCh, Logger: logger}
	var err error
	if r.lane.Settings != nil {
		err = DrainDynamic(ctx, r.lane.Settings, r.lane.Claim, r.lane.Process, options)
	} else {
		err = Drain(ctx, r.lane.MaxConcurrent, r.lane.Claim, r.lane.Process, options)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fields := []zap.Field{
			zap.String(logkeys.Operation, "drain_worker_lane"),
			zap.String(logkeys.Stage, "claim_or_process"),
			zap.String(appLogger.KeyLane, r.lane.Name),
			zap.Error(err),
		}
		if errors.Is(err, ErrClaimerPanicked) || errors.Is(err, ErrProcessorPanicked) {
			fields = append(fields,
				zap.String(logkeys.Invariant, "worker_callback_does_not_panic"),
				zap.String(logkeys.Expected, "callback returns normally"),
				zap.String(logkeys.Actual, "callback panicked"),
				zap.String(logkeys.Fallback, "stop_current_drain_and_retry_on_next_wake"),
			)
		}
		if errors.Is(err, ErrClaimerPanicked) || errors.Is(err, ErrProcessorPanicked) {
			logger.Error("worker lane drain failed", fields...)
		} else {
			fields = append(fields, zap.String(logkeys.Fallback, "retry_on_next_wake_or_tick"))
			logger.Warn("worker lane drain failed", fields...)
		}
	}
}

// Harness runs a set of lanes plus an optional maintenance callback. It owns no
// lifecycle state of its own; a worker drives it by calling Run on a background
// goroutine and cancelling the supplied context to stop.
const keyPanicStack = "panic_stack"
const keyRegisteredLanes = "registered_lanes"

type Harness struct {
	logger       *zap.Logger
	pollInterval time.Duration
	pollProvider func() time.Duration
	maintain     func(context.Context)
	lanes        []LaneRunner
}

// NewHarness constructs a Harness.
//
// It panics if pollInterval is non-positive: the value drives every lane's fallback
// ticker and the maintenance ticker, and a non-positive duration would otherwise panic
// later inside time.NewTicker at Run time, far from the misconfiguration. Failing here
// surfaces the mistake at construction with an actionable message.
//
// Parameters:
//   - logger (`*zap.Logger`): Logger for drain-error reporting; nil becomes a no-op logger.
//   - pollInterval (`time.Duration`): Default idle cadence for lanes and the maintenance loop; must be > 0.
//   - maintain (`func(context.Context)`): Optional periodic work (stale recovery, queue-depth metrics);
//     nil skips the maintenance loop entirely.
//   - lanes (`...LaneRunner`): The lanes to drain, each from NewLane.
//
// Returns:
// - *Harness: The configured harness.
func NewHarness(logger *zap.Logger, pollInterval time.Duration, maintain func(context.Context), lanes ...LaneRunner) *Harness {
	if pollInterval <= 0 {
		panic(fmt.Sprintf("worker: NewHarness requires pollInterval > 0, got %v", pollInterval))
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Harness{
		logger:       logger,
		pollInterval: pollInterval,
		maintain:     maintain,
		lanes:        lanes,
	}
}

// Run drives the harness until ctx is cancelled. The maintenance callback (if any)
// runs once up front, before any lane starts, so that work it re-queues (e.g. recovered
// stale jobs) is visible to each lane's eager first drain. It then launches one drain
// loop per lane plus a maintenance ticker loop, and blocks until every loop has exited.
//
// Run may be called again after a previous call returns (e.g. across a stop/start
// cycle); lanes reuse their wake channels.
//
// Parameters:
// - ctx (`context.Context`): Cancelled to stop all loops.
func (h *Harness) Run(ctx context.Context) {
	if h.maintain != nil && ctx.Err() == nil {
		h.runMaintain(ctx)
	}

	var wg sync.WaitGroup
	for _, lane := range h.lanes {
		wg.Go(func() {
			for {
				if h.runLane(ctx, lane) {
					return
				}
				// The lane loop panicked. Back off one poll interval so a
				// persistently failing lane cannot hot-loop, then restart it
				// unless the harness is stopping.
				select {
				case <-ctx.Done():
					return
				case <-time.After(h.pollInterval):
				}
			}
		})
	}

	if h.maintain != nil {
		wg.Go(func() {
			h.runMaintenanceTicker(ctx)
		})
	}

	wg.Wait()
}

// SetPollIntervalProvider installs the maintenance cadence before Run.
func (h *Harness) SetPollIntervalProvider(provider func() time.Duration) { h.pollProvider = provider }

func (h *Harness) runMaintenanceTicker(ctx context.Context) {
	if h.pollProvider != nil {
		for Wait(ctx, nil, h.pollProvider) {
			h.runMaintain(ctx)
		}
		return
	}
	ticker := time.NewTicker(h.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.runMaintain(ctx)
		}
	}
}

// runLane drives one lane's drain loop, containing a panic so one misbehaving lane
// cannot terminate the process or take down the other lanes. It reports whether the
// loop completed normally (context cancelled) rather than panicking.
func (h *Harness) runLane(ctx context.Context, lane LaneRunner) (completed bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.logger.Error("worker lane panicked; recovered",
				zap.String(logkeys.Invariant, "worker_lane_loop_does_not_panic"),
				zap.String(logkeys.Operation, "run_worker_lane"),
				zap.String(logkeys.Stage, "lane_loop"),
				zap.String(appLogger.KeyLane, lane.name()),
				zap.String(logkeys.Expected, "lane loop returns normally"),
				zap.String(logkeys.Actual, "lane loop panicked"),
				zap.String(logkeys.Fallback, "backoff_then_restart_lane"),
				zap.Any(logkeys.Panic, recovered),
				zap.String(appLogger.KeyPanicType, fmt.Sprintf("%T", recovered)),
				zap.ByteString(keyPanicStack, debug.Stack()))
		}
	}()
	lane.run(ctx, h.pollInterval, h.logger)
	return true
}

// runMaintain invokes the maintenance callback, containing a panic so one failing
// maintenance pass cannot terminate the process or stop future passes.
func (h *Harness) runMaintain(ctx context.Context) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.logger.Error("worker maintenance panicked; recovered",
				zap.String(logkeys.Invariant, "worker_maintenance_does_not_panic"),
				zap.String(logkeys.Operation, "run_worker_maintenance"),
				zap.String(logkeys.Stage, "maintenance_callback"),
				zap.String(logkeys.Expected, "maintenance callback returns normally"),
				zap.String(logkeys.Actual, "maintenance callback panicked"),
				zap.String(logkeys.Fallback, "skip_pass_and_continue_schedule"),
				zap.Any(logkeys.Panic, recovered),
				zap.String(appLogger.KeyPanicType, fmt.Sprintf("%T", recovered)),
				zap.ByteString(keyPanicStack, debug.Stack()))
		}
	}()
	h.maintain(ctx)
}

// Signal requests an immediate drain on every lane. Use it when work may have arrived
// but the responsible lane is unknown (an external "something changed" poke).
func (h *Harness) Signal() {
	for _, lane := range h.lanes {
		lane.signal()
	}
}

// SignalLane requests an immediate drain on a single named lane. Use it for in-pipeline
// hand-off, where one lane produces work for a specific downstream lane and wants to
// wake only that lane instead of waiting for its poll tick. Unknown names are ignored.
//
// Parameters:
// - name (`string`): The Lane.Name of the lane to wake.
func (h *Harness) SignalLane(name string) {
	registered := make([]string, 0, len(h.lanes))
	for _, lane := range h.lanes {
		if lane.name() == name {
			lane.signal()
			return
		}
		registered = append(registered, lane.name())
	}
	appLogger.LogInvariantViolation(h.logger, "worker_lane_signal_targets_registered_lane", nil,
		zap.String(logkeys.Stage, "signal_worker_lane"),
		zap.String(logkeys.Expected, "registered_lane_name"),
		zap.String(logkeys.Actual, "unregistered_lane_name"),
		zap.String(appLogger.KeyLane, name),
		zap.Strings(keyRegisteredLanes, registered),
		zap.String(logkeys.Fallback, "wait_for_poll_tick"))
}
