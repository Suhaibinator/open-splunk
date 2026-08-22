package main

import (
	"context"
	"time"
)

func newMaintenanceWorkerContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// runBacklogMaintenanceLoop drives one bounded prune worker: it prunes on
// every tick and re-arms a short backlog timer while prune reports more work.
func runBacklogMaintenanceLoop(
	workerContext context.Context,
	ticks <-chan time.Time,
	interval time.Duration,
	backlogDelay time.Duration,
	runImmediately bool,
	prune func() bool,
) {
	var ticker *time.Ticker
	if ticks == nil {
		ticker = time.NewTicker(interval)
		ticks = ticker.C
		defer ticker.Stop()
	}
	var backlogTimer *time.Timer
	var backlog <-chan time.Time
	defer func() {
		if backlogTimer != nil {
			backlogTimer.Stop()
		}
	}()
	scheduleBacklog := func(more bool) {
		if !more {
			if backlogTimer != nil && !backlogTimer.Stop() {
				select {
				case <-backlogTimer.C:
				default:
				}
			}
			backlog = nil
			return
		}
		if backlogTimer == nil {
			backlogTimer = time.NewTimer(backlogDelay)
		} else {
			if !backlogTimer.Stop() {
				select {
				case <-backlogTimer.C:
				default:
				}
			}
			backlogTimer.Reset(backlogDelay)
		}
		backlog = backlogTimer.C
	}
	if runImmediately {
		scheduleBacklog(prune())
	}
	for {
		select {
		case <-workerContext.Done():
			return
		case _, open := <-ticks:
			if !open || workerContext.Err() != nil {
				return
			}
			scheduleBacklog(prune())
		case <-backlog:
			backlog = nil
			scheduleBacklog(prune())
		}
	}
}
