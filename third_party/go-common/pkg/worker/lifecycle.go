package worker

import (
	"context"
	"sync"
)

// Lifecycle coordinates the start, stop, and finish state for a long-running
// background worker. The zero value is ready to use.
//
// Concurrency contract — read before wiring one up:
//
//   - The run closure passed to Start/StartBackground MUST call Finish exactly once
//     when it returns. Stop blocks until Finish runs (or the Stop context ends), so a
//     run closure that returns without calling Finish will hang every Stop caller until
//     their context expires. As a last-resort guard, a panic that escapes the run
//     closure is recovered and treated as an implicit Finish, so a panicking run cannot
//     wedge Stop or block future Starts.
//
//   - The configure callback (Start/StartBackground) and the cleanup callbacks (Finish)
//     run while the Lifecycle mutex is held. They MUST NOT call back into the same
//     Lifecycle (Start, Stop, Finish, IfRunning) — doing so re-enters the held mutex and
//     deadlocks. Keep them to local setup/teardown only.
type Lifecycle struct {
	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// Start launches run with a cancellable child of ctx. It returns false when the
// lifecycle is already running.
//
// configure runs synchronously under the lifecycle mutex before run is launched; it must
// not call back into this Lifecycle (see the type doc). The run closure receives the
// cancellable context and must call Finish when it returns.
func (l *Lifecycle) Start(ctx context.Context, configure func(), run func(context.Context)) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	return l.start(ctx, configure, run)
}

// StartBackground launches run with a cancellable Background context. It
// returns false when the lifecycle is already running.
//
// The same configure/run contract as Start applies (see the type doc).
func (l *Lifecycle) StartBackground(configure func(), run func(context.Context)) bool {
	return l.start(context.Background(), configure, run)
}

func (l *Lifecycle) start(ctx context.Context, configure func(), run func(context.Context)) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.running {
		return false
	}

	workerCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.done = make(chan struct{})
	l.running = true
	if configure != nil {
		configure()
	}
	done := l.done
	go func() {
		// A panic escaping the run closure would otherwise kill the whole process
		// (nothing above this background goroutine recovers) and, because Finish
		// never runs, wedge every future Stop and Start. Recover it and finish the
		// lifecycle instead. Lifecycle has no logger; layers that can log (e.g.
		// Harness) recover and report panics before they reach this last resort.
		defer func() {
			if recovered := recover(); recovered != nil {
				l.finishAfterPanic(done)
			}
		}()
		run(workerCtx)
	}()
	return true
}

// finishAfterPanic releases the lifecycle after the run closure panicked without
// calling Finish, unblocking Stop callers and allowing a future Start. It is a no-op
// when done is no longer the current generation, so a run closure that panics after
// calling Finish cannot clobber a subsequently started lifecycle.
//
// Nobody else can ever call the stored cancel on this path (Stop is a no-op once
// running is false, and a later Start overwrites l.cancel), so cancel it here to tear
// down workerCtx — otherwise goroutines blocked on workerCtx.Done() leak and their
// cleanup never runs.
func (l *Lifecycle) finishAfterPanic(done chan struct{}) {
	l.mu.Lock()
	if l.done != done {
		l.mu.Unlock()
		return
	}
	cancel := l.cancel
	l.running = false
	l.cancel = nil
	l.done = nil
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	close(done)
}

// Stop cancels the running lifecycle and waits for Finish, or for ctx to end.
//
// Stop BLOCKS until the run closure calls Finish (returning nil) or ctx is cancelled
// (returning ctx.Err()). If the run closure never calls Finish, Stop blocks for the full
// lifetime of ctx — always pass a Stop context with a deadline for shutdown paths. Stop
// is a no-op returning nil when the lifecycle is not running.
func (l *Lifecycle) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	l.mu.Lock()
	if !l.running {
		l.mu.Unlock()
		return nil
	}
	cancel := l.cancel
	done := l.done
	l.mu.Unlock()

	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Finish marks the lifecycle stopped, runs cleanup while holding the lifecycle
// lock, then closes the prior done channel after releasing the lock (unblocking any
// Stop caller). It must be called exactly once by the run closure when it returns.
//
// The cleanup callbacks run under the lifecycle mutex and must not call back into this
// Lifecycle (see the type doc).
func (l *Lifecycle) Finish(cleanup ...func()) {
	l.mu.Lock()
	l.running = false
	cancel := l.cancel
	l.cancel = nil
	done := l.done
	l.done = nil
	// Finalize even when application cleanup panics. Unlock before waking Stop;
	// the run goroutine's recovery may then safely probe this generation.
	defer func() {
		l.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if done != nil {
			close(done)
		}
	}()
	for _, fn := range cleanup {
		if fn != nil {
			fn()
		}
	}
}

// IfRunning runs fn while the lifecycle lock is held when the lifecycle is
// running. It returns whether fn was run.
//
// fn runs under the lifecycle mutex and must not call back into this Lifecycle
// (see the type doc).
func (l *Lifecycle) IfRunning(fn func()) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.running {
		return false
	}
	if fn != nil {
		fn()
	}
	return true
}
