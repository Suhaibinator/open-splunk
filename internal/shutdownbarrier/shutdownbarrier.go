// Package shutdownbarrier shares one drain across repeated Close calls so a
// service can be closed concurrently and repeatedly without draining twice.
package shutdownbarrier

import (
	"context"
	"sync"
)

// Barrier makes repeated Close calls share one completion. The zero value is
// not usable; construct with New.
type Barrier struct {
	once sync.Once
	done chan struct{}
}

// New constructs a barrier that has not started its drain.
func New() *Barrier {
	return &Barrier{done: make(chan struct{})}
}

// Wait starts the drain exactly once and blocks until it finishes or ctx ends.
// A completed drain wins over an already-canceled ctx, so a caller that closed
// successfully never observes a context error. A timed-out caller may call
// Wait again to keep waiting on the same drain.
func (barrier *Barrier) Wait(ctx context.Context, drain func()) error {
	barrier.once.Do(func() {
		go func() {
			drain()
			close(barrier.done)
		}()
	})
	select {
	case <-barrier.done:
		return nil
	default:
	}
	select {
	case <-barrier.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
