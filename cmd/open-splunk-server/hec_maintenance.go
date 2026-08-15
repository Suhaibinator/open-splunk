package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

const (
	hecTerminalMaintenanceInterval       = 15 * time.Minute
	hecTerminalMaintenanceTimeout        = 10 * time.Second
	hecTerminalMaintenanceBatchSize      = uint32(visibility.MaxPruneLimit)
	hecTerminalMaintenanceMaximumBatches = 10
	hecTerminalMaintenanceBacklogDelay   = 100 * time.Millisecond
)

type hecTerminalPruner interface {
	PruneHECTerminalRequests(context.Context, time.Time, uint32) (uint32, error)
}

type hecTerminalMaintenanceConfig struct {
	retention      time.Duration
	interval       time.Duration
	pruneTimeout   time.Duration
	batchSize      uint32
	maximumBatches int
	backlogDelay   time.Duration
	runImmediately bool
	now            func() time.Time
	ticks          <-chan time.Time
	onError        func(error)
}

func defaultHECTerminalMaintenanceConfig() hecTerminalMaintenanceConfig {
	return hecTerminalMaintenanceConfig{
		retention:      visibility.HECTerminalRetention,
		interval:       hecTerminalMaintenanceInterval,
		pruneTimeout:   hecTerminalMaintenanceTimeout,
		batchSize:      hecTerminalMaintenanceBatchSize,
		maximumBatches: hecTerminalMaintenanceMaximumBatches,
		backlogDelay:   hecTerminalMaintenanceBacklogDelay,
		runImmediately: true,
		now:            time.Now,
	}
}

func (config *hecTerminalMaintenanceConfig) normalize() error {
	if config == nil {
		return errors.New("HEC terminal maintenance config is required")
	}
	if config.retention <= 0 || config.retention > visibility.HECTerminalRetention {
		return fmt.Errorf("HEC terminal retention must be between zero and %s", visibility.HECTerminalRetention)
	}
	if config.interval <= 0 || config.pruneTimeout <= 0 || config.backlogDelay <= 0 {
		return errors.New("HEC terminal maintenance timing must be positive")
	}
	if config.batchSize == 0 || config.batchSize > visibility.MaxPruneLimit {
		return fmt.Errorf("HEC terminal maintenance batch size must be between 1 and %d", visibility.MaxPruneLimit)
	}
	if config.maximumBatches < 1 || config.maximumBatches > 1_000 {
		return errors.New("HEC terminal maintenance maximum batches must be between 1 and 1000")
	}
	if config.now == nil {
		return errors.New("HEC terminal maintenance clock is required")
	}
	return nil
}

// hecTerminalMaintenance owns the bounded 24-hour terminal request/ACK
// cleanup loop. Pending rows are excluded by the durable pruner and therefore
// remain authoritative throughout ClickHouse outages and reconciliation.
type hecTerminalMaintenance struct {
	pruner         hecTerminalPruner
	retention      time.Duration
	interval       time.Duration
	pruneTimeout   time.Duration
	batchSize      uint32
	maximumBatches int
	backlogDelay   time.Duration
	runImmediately bool
	now            func() time.Time
	ticks          <-chan time.Time
	onError        func(error)

	workerContext context.Context
	cancelWorker  context.CancelFunc
	done          chan struct{}
	closeOnce     sync.Once
}

func newHECTerminalMaintenance(
	pruner hecTerminalPruner,
	config hecTerminalMaintenanceConfig,
) (*hecTerminalMaintenance, error) {
	if nilcheck.IsNil(pruner) {
		return nil, errors.New("HEC terminal maintenance pruner is required")
	}
	if err := config.normalize(); err != nil {
		return nil, err
	}
	workerContext, cancelWorker := context.WithCancel(context.Background()) //nolint:gosec // Close owns cancellation.
	maintenance := &hecTerminalMaintenance{
		pruner:         pruner,
		retention:      config.retention,
		interval:       config.interval,
		pruneTimeout:   config.pruneTimeout,
		batchSize:      config.batchSize,
		maximumBatches: config.maximumBatches,
		backlogDelay:   config.backlogDelay,
		runImmediately: config.runImmediately,
		now:            config.now,
		ticks:          config.ticks,
		onError:        config.onError,
		workerContext:  workerContext,
		cancelWorker:   cancelWorker,
		done:           make(chan struct{}),
	}
	go maintenance.run()
	return maintenance, nil
}

func (maintenance *hecTerminalMaintenance) run() {
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

func (maintenance *hecTerminalMaintenance) prune() bool {
	ctx, cancel := context.WithTimeout(maintenance.workerContext, maintenance.pruneTimeout)
	defer cancel()
	terminalBefore := maintenance.now().Round(0).UTC().Add(-maintenance.retention)
	result, err := runHECTerminalPruneBatches(
		ctx,
		maintenance.pruner,
		terminalBefore,
		maintenance.batchSize,
		maintenance.maximumBatches,
	)
	if err != nil {
		if maintenance.workerContext.Err() == nil && maintenance.onError != nil {
			maintenance.onError(fmt.Errorf("prune expired HEC terminal requests: %w", err))
		}
		return false
	}
	return result.more
}

type hecTerminalPruneRun struct {
	deleted uint64
	more    bool
}

func runHECTerminalPruneBatches(
	ctx context.Context,
	pruner hecTerminalPruner,
	terminalBefore time.Time,
	batchSize uint32,
	maximumBatches int,
) (hecTerminalPruneRun, error) {
	if ctx == nil || nilcheck.IsNil(pruner) || terminalBefore.IsZero() ||
		batchSize == 0 || batchSize > visibility.MaxPruneLimit || maximumBatches < 1 {
		return hecTerminalPruneRun{}, errors.New("HEC terminal prune run is invalid")
	}
	result := hecTerminalPruneRun{}
	for range maximumBatches {
		deleted, err := pruner.PruneHECTerminalRequests(ctx, terminalBefore, batchSize)
		if err != nil {
			return result, err
		}
		result.deleted += uint64(deleted)
		result.more = deleted == batchSize
		if !result.more {
			return result, nil
		}
	}
	return result, nil
}

func (maintenance *hecTerminalMaintenance) Close(ctx context.Context) error {
	if maintenance == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("HEC terminal maintenance close context is required")
	}
	maintenance.closeOnce.Do(maintenance.cancelWorker)
	select {
	case <-maintenance.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
