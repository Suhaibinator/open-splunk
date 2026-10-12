package controlplane

import (
	"context"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"go.uber.org/zap"
)

const (
	recorderSweepInterval  = time.Hour
	recorderSweepGrace     = time.Minute
	recorderSweepTimeout   = 30 * time.Second
	recorderSweepBatchSize = 500
)

func (r *AttemptRecorder) sweepLoop(ctx context.Context) {
	defer r.sweepWG.Done()
	ticker := time.NewTicker(recorderSweepInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		r.sweep(ctx, time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sweep is independent of write workers: cleanup cannot consume their queue or execution budget.
func (r *AttemptRecorder) sweep(parent context.Context, now time.Time) {
	logger := r.logger.Named(recorderLoggerName).With(zap.String(logkeys.Stage, "sweep_stale_attempts"))
	defer func() {
		if value := recover(); value != nil {
			logger.Error("attempt sweep panicked", zap.Any(logkeys.Panic, value))
		}
	}()
	ctx, cancel := context.WithTimeout(parent, recorderSweepTimeout)
	defer cancel()
	var total int64
	defer func() {
		if total > 0 {
			logger.Info("attempt sweep finished", zap.Int64("updated_count", total))
		}
	}()
	for ctx.Err() == nil {
		count, err := r.store.SweepStaleAttempts(ctx, now.Add(-recorderSweepGrace), recorderSweepBatchSize)
		if err != nil {
			if parent.Err() == nil {
				logger.Warn("attempt sweep failed", zap.Error(err))
			}
			return
		}
		total += count
		if count < recorderSweepBatchSize {
			return
		}
	}
	if parent.Err() == nil {
		logger.Warn("attempt sweep budget exhausted", zap.Error(ctx.Err()))
	}
}
