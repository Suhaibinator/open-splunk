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

// defaultIdlePollInterval is the fallback cadence used when DrainOptions.IdlePollInterval
// is non-positive. It bounds how long an idle worker parks while another worker is still
// active, and — critically — prevents a tight claim() loop when no interval is supplied
// (see DrainOptions.IdlePollInterval).
const defaultIdlePollInterval = 100 * time.Millisecond

// Claimer claims one queued item.
// It returns claimed=false when there is no more work available.
type Claimer[T any] func(context.Context) (item T, claimed bool, err error)

// Processor processes one claimed item.
type Processor[T any] func(context.Context, T) error

// ErrClaimerPanicked and ErrProcessorPanicked wrap panics recovered from the
// application-supplied callbacks. Drain runs those callbacks on background worker
// goroutines where no HTTP recovery middleware exists, so an unrecovered panic would
// kill the whole process; instead the panic is converted into an error (carrying the
// panic value and stack) and surfaces through Drain's normal error return.
var (
	ErrClaimerPanicked   = errors.New("worker: claimer panicked")
	ErrProcessorPanicked = errors.New("worker: processor panicked")
)

// runClaimer invokes claim with panic isolation: a panic is converted into an error
// wrapping ErrClaimerPanicked instead of unwinding the worker goroutine.
func runClaimer[T any](ctx context.Context, claim Claimer[T]) (item T, claimed bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrClaimerPanicked, recovered, debug.Stack())
		}
	}()
	return claim(ctx)
}

// runProcessor invokes process with panic isolation: a panic is converted into an
// error wrapping ErrProcessorPanicked instead of unwinding the worker goroutine.
func runProcessor[T any](ctx context.Context, process Processor[T], item T) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrProcessorPanicked, recovered, debug.Stack())
		}
	}()
	return process(ctx, item)
}

// DrainOptions controls how idle drain workers wait while other workers remain active.
type DrainOptions struct {
	// Logger reports missing callbacks as wiring invariants when supplied.
	Logger *zap.Logger
	// WakeCh, when set, lets a parked idle worker return early to re-attempt a claim as
	// soon as new work is signalled, instead of waiting out IdlePollInterval.
	WakeCh <-chan struct{}
	// IdlePollInterval is how long an idle worker waits before re-attempting a claim while
	// another worker is still active. A non-positive value is treated as
	// defaultIdlePollInterval: passing 0 (or negative) never busy-spins — Drain normalizes
	// it to the default so an idle worker always parks between claim attempts.
	IdlePollInterval time.Duration
}

// Drain runs processors concurrently until no more work can be claimed or a worker returns an error.
//
// Parameters:
// - ctx (`context.Context`): Context propagated to all worker goroutines and cancelled on the first failure.
// - maxConcurrent (`int`): Maximum number of concurrent claim/process loops to run; values <= 0 default to 1.
// - claim (`Claimer[T]`): Callback that claims a single unit of work.
// - process (`Processor[T]`): Callback that processes a claimed unit of work.
// - drainOptions (`DrainOptions`): Idle-wait tuning; a non-positive IdlePollInterval is normalized to the package default.
//
// Returns:
//   - error: The first worker error encountered, or nil when all available work is drained
//     successfully. A panic in claim or process is recovered and returned as an error wrapping
//     ErrClaimerPanicked or ErrProcessorPanicked, so one bad item cannot kill the process.
func Drain[T any](ctx context.Context, maxConcurrent int, claim Claimer[T], process Processor[T], drainOptions DrainOptions) error {
	if claim == nil || process == nil {
		logMissingCallbacks(drainOptions.Logger, claim == nil, process == nil)
		return nil
	}
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	// Normalize the idle cadence up front so no worker can busy-spin on claim() when the
	// caller leaves IdlePollInterval unset (or negative) while another worker is active.
	if drainOptions.IdlePollInterval <= 0 {
		drainOptions.IdlePollInterval = defaultIdlePollInterval
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	firstItem, claimed, err := runClaimer(workerCtx, claim)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	if maxConcurrent == 1 {
		for {
			if err := runProcessor(workerCtx, process, firstItem); err != nil {
				return err
			}
			firstItem, claimed, err = runClaimer(workerCtx, claim)
			if err != nil {
				return err
			}
			if !claimed {
				return nil
			}
		}
	}

	seededItems := make(chan T, 1)
	seededItems <- firstItem
	close(seededItems)

	errCh := make(chan error, 1)
	reportErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
			cancel()
		default:
		}
	}

	var activeMu sync.Mutex
	activeProcessors := 1 // The seeded probe item is already claimed and will be processed by one worker.
	activeChanged := make(chan struct{}, 1)
	notifyActiveChanged := func() {
		select {
		case activeChanged <- struct{}{}:
		default:
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < maxConcurrent; i++ {
		wg.Go(func() {
			for {
				item, claimed, activeAlreadyCounted, err := claimNextItem(workerCtx, seededItems, claim)
				if err != nil {
					reportErr(err)
					return
				}
				if !claimed {
					activeMu.Lock()
					otherWorkersActive := activeProcessors > 0
					activeMu.Unlock()
					if !otherWorkersActive {
						// Cascade the wake-up: activeChanged is a coalesced cap-1
						// channel, so a single notification only wakes one parked
						// worker. By re-notifying as each idle worker exits, every
						// remaining waiter is woken in turn and can also return,
						// instead of blocking until its idle-poll timer fires.
						notifyActiveChanged()
						return
					}

					waitForIdleRetry(workerCtx, drainOptions, activeChanged, func() bool {
						activeMu.Lock()
						defer activeMu.Unlock()
						return activeProcessors > 0
					})
					continue
				}

				if !activeAlreadyCounted {
					activeMu.Lock()
					activeProcessors++
					activeMu.Unlock()
				}

				err = runProcessor(workerCtx, process, item)

				activeMu.Lock()
				activeProcessors--
				activeMu.Unlock()
				if err != nil {
					reportErr(err)
					return
				}
				notifyActiveChanged()
			}
		})
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func claimNextItem[T any](ctx context.Context, seededItems <-chan T, claim Claimer[T]) (T, bool, bool, error) {
	if seededItems != nil {
		if item, ok := <-seededItems; ok {
			return item, true, true, nil
		}
	}
	item, claimed, err := runClaimer(ctx, claim)
	return item, claimed, false, err
}

// waitForIdleRetry parks an idle worker until new work may be available: it wakes on an
// active-worker completion (activeChanged), an explicit wake signal (WakeCh), context
// cancellation, or the idle-poll timer. Drain guarantees options.IdlePollInterval is
// positive, so the timer always bounds the wait and the worker never busy-spins.
func waitForIdleRetry(ctx context.Context, options DrainOptions, activeChanged <-chan struct{}, hasActiveWorker func() bool) {
	select {
	case <-activeChanged:
	default:
	}
	if hasActiveWorker != nil && !hasActiveWorker() {
		return
	}

	timer := time.NewTimer(options.IdlePollInterval)
	defer timer.Stop()

	if options.WakeCh == nil {
		select {
		case <-ctx.Done():
		case <-activeChanged:
		case <-timer.C:
		}
		return
	}

	select {
	case <-ctx.Done():
	case <-options.WakeCh:
	case <-activeChanged:
	case <-timer.C:
	}
}

func logMissingCallbacks(logger *zap.Logger, missingClaim, missingProcess bool) {
	if logger == nil {
		return
	}
	for _, dependency := range []struct {
		name    string
		missing bool
	}{{"claimer", missingClaim}, {"processor", missingProcess}} {
		if dependency.missing {
			appLogger.LogInvariantViolation(logger, "dependency_wired_at_construction", nil,
				zap.String(logkeys.Stage, "drain_worker_queue"),
				zap.String(appLogger.KeyComponent, dependency.name),
				zap.String(logkeys.Expected, "configured"),
				zap.String(logkeys.Actual, "nil"),
				zap.String(logkeys.Fallback, "skip_drain_pass"))
		}
	}
}
