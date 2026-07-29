package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
)

const (
	searchHistoryMaintenanceInterval     = time.Hour
	searchHistoryMaintenancePruneTimeout = 10 * time.Second
	searchHistoryMaintenanceBatchSize    = 256
	searchHistoryMaintenanceMaxBatches   = 64
	searchHistoryStartupMaxBatches       = 4
	searchHistoryMaintenanceBacklogDelay = 100 * time.Millisecond
)

type searchHistoryPruner interface {
	PruneMaintenanceBatch(
		context.Context,
		int,
		*searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error)
}

type searchHistoryMaintenanceConfig struct {
	interval       time.Duration
	pruneTimeout   time.Duration
	batchSize      int
	maximumBatches int
	backlogDelay   time.Duration
	initialCursor  *searchhistory.MaintenanceCursor
	runImmediately bool
	// ticks is an injected deterministic clock used only by tests. Production
	// leaves it nil so the worker owns and stops its ticker.
	ticks   <-chan time.Time
	onError func(error)
}

func defaultSearchHistoryMaintenanceConfig() searchHistoryMaintenanceConfig {
	return searchHistoryMaintenanceConfig{
		interval:       searchHistoryMaintenanceInterval,
		pruneTimeout:   searchHistoryMaintenancePruneTimeout,
		batchSize:      searchHistoryMaintenanceBatchSize,
		maximumBatches: searchHistoryMaintenanceMaxBatches,
		backlogDelay:   searchHistoryMaintenanceBacklogDelay,
	}
}

func (config *searchHistoryMaintenanceConfig) normalize() error {
	if config == nil {
		return errors.New("search-history maintenance config is required")
	}
	if config.interval <= 0 {
		return errors.New("search-history maintenance interval must be positive")
	}
	if config.pruneTimeout <= 0 {
		return errors.New("search-history maintenance prune timeout must be positive")
	}
	if config.batchSize == 0 {
		config.batchSize = searchHistoryMaintenanceBatchSize
	}
	if config.batchSize < 0 ||
		config.batchSize > searchHistoryMaintenanceBatchSize {
		return fmt.Errorf(
			"search-history maintenance batch size must be between 1 and %d",
			searchHistoryMaintenanceBatchSize,
		)
	}
	if config.maximumBatches == 0 {
		config.maximumBatches = searchHistoryMaintenanceMaxBatches
	}
	if config.maximumBatches < 0 ||
		config.maximumBatches > searchHistoryMaintenanceMaxBatches {
		return fmt.Errorf(
			"search-history maintenance maximum batches must be between 1 and %d",
			searchHistoryMaintenanceMaxBatches,
		)
	}
	if config.backlogDelay == 0 {
		config.backlogDelay = searchHistoryMaintenanceBacklogDelay
	}
	if config.backlogDelay < 0 {
		return errors.New(
			"search-history maintenance backlog delay must be positive",
		)
	}
	return nil
}

// searchHistoryMaintenance owns one non-overlapping physical-retention worker
// across every persisted tenant/owner scope. Startup performs a bounded
// synchronous pass, then this worker continues inherited backlog in bounded,
// briefly spaced runs before settling into periodic maintenance.
type searchHistoryMaintenance struct {
	pruner         searchHistoryPruner
	pruneTimeout   time.Duration
	batchSize      int
	maximumBatches int
	backlogDelay   time.Duration
	cursor         *searchhistory.MaintenanceCursor
	runImmediately bool
	ticks          <-chan time.Time
	interval       time.Duration
	onError        func(error)

	workerContext context.Context
	cancelWorker  context.CancelFunc
	done          chan struct{}
	closeOnce     sync.Once
	callbackMu    sync.Mutex
	callbackAlive bool
}

func newSearchHistoryMaintenance(
	pruner searchHistoryPruner,
	config searchHistoryMaintenanceConfig,
) (*searchHistoryMaintenance, error) {
	if pruner == nil {
		return nil, errors.New("search-history maintenance pruner is required")
	}
	if err := config.normalize(); err != nil {
		return nil, err
	}
	workerContext, cancelWorker := context.WithCancel(context.Background()) //nolint:gosec // Close owns cancellation.
	maintenance := &searchHistoryMaintenance{
		pruner:         pruner,
		pruneTimeout:   config.pruneTimeout,
		batchSize:      config.batchSize,
		maximumBatches: config.maximumBatches,
		backlogDelay:   config.backlogDelay,
		cursor:         config.initialCursor,
		runImmediately: config.runImmediately,
		ticks:          config.ticks,
		interval:       config.interval,
		onError:        config.onError,
		workerContext:  workerContext,
		cancelWorker:   cancelWorker,
		done:           make(chan struct{}),
	}
	go maintenance.run()
	return maintenance, nil
}

func (maintenance *searchHistoryMaintenance) run() {
	defer close(maintenance.done)

	ticks := maintenance.ticks
	var ticker *time.Ticker
	if ticks == nil {
		ticker = time.NewTicker(maintenance.interval)
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
			backlogTimer = time.NewTimer(maintenance.backlogDelay)
		} else {
			if !backlogTimer.Stop() {
				select {
				case <-backlogTimer.C:
				default:
				}
			}
			backlogTimer.Reset(maintenance.backlogDelay)
		}
		backlog = backlogTimer.C
	}
	if maintenance.runImmediately {
		scheduleBacklog(maintenance.prune())
	}
	for {
		select {
		case <-maintenance.workerContext.Done():
			return
		case _, open := <-ticks:
			if !open || maintenance.workerContext.Err() != nil {
				return
			}
			scheduleBacklog(maintenance.prune())
		case <-backlog:
			backlog = nil
			scheduleBacklog(maintenance.prune())
		}
	}
}

func (maintenance *searchHistoryMaintenance) prune() bool {
	ctx, cancel := context.WithTimeout(
		maintenance.workerContext,
		maintenance.pruneTimeout,
	)
	defer cancel()
	result, err := runSearchHistoryPruneBatches(
		ctx,
		maintenance.pruner,
		maintenance.cursor,
		maintenance.batchSize,
		maintenance.maximumBatches,
	)
	maintenance.cursor = result.cursor
	if err != nil {
		if maintenance.workerContext.Err() == nil {
			maintenance.reportError(fmt.Errorf(
				"prune expired search history: %w",
				err,
			))
		}
		return false
	}
	return result.more
}

type searchHistoryPruneRun struct {
	deleted int64
	more    bool
	cursor  *searchhistory.MaintenanceCursor
}

func runSearchHistoryPruneBatches(
	ctx context.Context,
	pruner searchHistoryPruner,
	cursor *searchhistory.MaintenanceCursor,
	batchSize int,
	maximumBatches int,
) (searchHistoryPruneRun, error) {
	result := searchHistoryPruneRun{
		cursor: cursor,
	}
	for range maximumBatches {
		batch, err := pruner.PruneMaintenanceBatch(
			ctx,
			batchSize,
			result.cursor,
		)
		result.deleted += batch.Deleted
		result.more = batch.More
		result.cursor = batch.Cursor
		if err != nil {
			return result, err
		}
		if !result.more {
			return result, nil
		}
		runtime.Gosched()
	}
	return result, nil
}

func (maintenance *searchHistoryMaintenance) reportError(err error) {
	if err == nil || maintenance.onError == nil {
		return
	}
	maintenance.callbackMu.Lock()
	if maintenance.callbackAlive {
		maintenance.callbackMu.Unlock()
		return
	}
	maintenance.callbackAlive = true
	maintenance.callbackMu.Unlock()
	go func() {
		defer func() {
			_ = recover()
			maintenance.callbackMu.Lock()
			maintenance.callbackAlive = false
			maintenance.callbackMu.Unlock()
		}()
		maintenance.onError(err)
	}()
}

// Close cancels an in-flight prune and waits for the owned worker. If the
// caller's deadline expires, a later Close may continue waiting for the same
// worker; cancellation itself remains idempotent.
func (maintenance *searchHistoryMaintenance) Close(ctx context.Context) error {
	if maintenance == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("close search-history maintenance: context is nil")
	}
	maintenance.closeOnce.Do(maintenance.cancelWorker)
	select {
	case <-maintenance.done:
		return nil
	default:
	}
	select {
	case <-maintenance.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("close search-history maintenance: %w", ctx.Err())
	}
}

func pruneSearchHistoryAtStartup(
	ctx context.Context,
	pruner searchHistoryPruner,
) (searchHistoryPruneRun, error) {
	if pruner == nil {
		return searchHistoryPruneRun{},
			errors.New("startup search-history pruner is required")
	}
	result, err := runSearchHistoryPruneBatches(
		ctx,
		pruner,
		nil,
		searchHistoryMaintenanceBatchSize,
		searchHistoryStartupMaxBatches,
	)
	if err != nil {
		return result, fmt.Errorf(
			"apply startup search-history retention: %w",
			err,
		)
	}
	return result, nil
}
