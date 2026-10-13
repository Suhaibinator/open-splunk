package notification

import (
	"context"
	"errors"
	"sync"
	"time"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// fanoutTuning is one sample of the hot fan-out knobs. The coordinator samples
// it once per claim and hands the same sample to every step that claim starts,
// so the lease taken at claim and the budget the step runs under always agree;
// the maintenance loop samples it once per tick. In-flight steps keep the
// values they started with. Values travel as parameters, never in a context.
type fanoutTuning struct {
	stepBudget          time.Duration
	lease               time.Duration
	batchRecipients     int
	maintenanceInterval time.Duration
	pollInterval        time.Duration
	maxAttempts         uint32
	retirementPageSize  int
	// retirement is the per-pass chat retirement policy handed to the store.
	retirement          persistence.NotificationChatRetirementPolicy
	compactionRetention time.Duration
	compactionBatch     int
	backlogCountLimit   int
	// Claim-error, compaction, retry, and completion timing.
	claimErrorMaxBackoff time.Duration
	compactionInterval   time.Duration
	retryBackoffBase     time.Duration
	retryBackoffMax      time.Duration
	deferredRetryDelay   time.Duration
	finishTimeout        time.Duration
}

// fanoutTuningFor copies one configuration sample. The configuration is
// authoritative: pkg/config validation requires every key and its range and
// cross-key orderings, so nothing is defaulted or clamped here.
func fanoutTuningFor(cfg *Config) fanoutTuning {
	return fanoutTuning{
		stepBudget:          cfg.FanoutStepBudget,
		lease:               cfg.FanoutLease,
		batchRecipients:     cfg.FanoutBatchRecipients,
		maintenanceInterval: cfg.FanoutMaintenanceInterval,
		pollInterval:        cfg.FanoutPollInterval,
		maxAttempts:         uint32(cfg.FanoutMaxAttempts),
		retirementPageSize:  cfg.ChatRetirementPageSize,
		retirement: persistence.NotificationChatRetirementPolicy{
			PageSize:                 cfg.ChatRetirementPageSize,
			MaxAttempts:              cfg.ChatRetirementMaxAttempts,
			ExhaustedDelay:           cfg.ChatRetirementExhaustedDelay,
			RetryBackoffBase:         cfg.ChatRetirementRetryBackoffBase,
			RetryBackoffMax:          cfg.ChatRetirementRetryBackoffMax,
			RecordFailureTimeout:     cfg.ChatRetirementRecordFailureTimeout,
			MaxRecipients:            cfg.RetirementMaxRecipients,
			SourceResolutionPageSize: cfg.SourceResolutionPageSize,
			ClaimCandidates:          cfg.RetirementClaimCandidates,
		},
		compactionRetention:  cfg.FanoutCompactionRetention,
		compactionBatch:      cfg.FanoutCompactionBatch,
		backlogCountLimit:    cfg.FanoutBacklogCountLimit,
		claimErrorMaxBackoff: cfg.FanoutClaimErrorMaxBackoff,
		compactionInterval:   cfg.FanoutCompactionInterval,
		retryBackoffBase:     cfg.FanoutRetryBackoffBase,
		retryBackoffMax:      cfg.FanoutRetryBackoffMax,
		deferredRetryDelay:   cfg.FanoutDeferredRetryDelay,
		finishTimeout:        cfg.FanoutFinishTimeout,
	}
}

// sampleFanoutTuning reads the current configuration once. Callers keep the
// returned sample for the whole claim, tick, or pass it governs.
func (h *Handler) sampleFanoutTuning() fanoutTuning {
	cfg := h.deps.Config()
	return fanoutTuningFor(cfg)
}

// SetFanoutWorkersEnabled controls whether this process claims fan-out work.
// It is construction-only; application-wide concurrency and pause are hot config.
func (h *Handler) SetFanoutWorkersEnabled(enabled bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cancel != nil || h.stopped {
		return ErrRegistryFrozen
	}
	h.fanoutWorkersEnabled = enabled
	return nil
}

func fanoutCapacity(cfg *Config) int {
	return cfg.FanoutWorkerCount
}

// The coordinator leases only the capacity it can execute immediately. Each
// active step has one bounded goroutine; shrinking never interrupts a step.
func (h *Handler) runFanoutWorkers(ctx context.Context) {
	if h.store == nil {
		return
	}
	h.subscribeQueueWakes()
	store := h.store
	completed := make(chan struct{}, config.MaxNotificationFanoutWorkerCount)
	var steps sync.WaitGroup
	defer steps.Wait()
	// One configuration sample per iteration drives capacity, the claim, and the
	// steps that claim starts. The memory-only ticker follows the legacy poll key.
	interval := h.sampleFanoutTuning().pollInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	owner := uuid.New().String()
	active := 0
	var nextClaim time.Time
	var errorDelay time.Duration
	deadlineTimer := time.NewTimer(time.Hour)
	defer deadlineTimer.Stop()
	var previousCapacity int
	var previousAttempts uint32
	for {
		for len(completed) > 0 {
			<-completed
			active--
			if errorDelay == 0 {
				nextClaim = time.Time{}
			}
		}
		cfg := h.deps.Config()
		// A fresh variable per iteration: step goroutines capture this claim's
		// sample and never observe a later iteration's resample.
		tuning := fanoutTuningFor(cfg)
		if next := tuning.pollInterval; next != interval {
			interval = next
			ticker.Reset(interval)
		}
		capacity := fanoutCapacity(cfg)
		if !h.fanoutWorkersEnabled || !cfg.Enabled || cfg.FanoutPaused {
			capacity = 0
		}
		if (capacity != previousCapacity || tuning.maxAttempts != previousAttempts) && errorDelay == 0 {
			nextClaim = time.Time{}
		}
		previousCapacity, previousAttempts = capacity, tuning.maxAttempts
		h.observeFanoutActivity(cfg, active)
		if ctx.Err() != nil {
			return
		}
		if active < capacity && !time.Now().Before(nextClaim) {
			claimCtx, cancel := context.WithTimeout(ctx, tuning.stepBudget)
			jobs, err := store.ClaimNotificationEmissions(claimCtx, owner, tuning.lease, capacity-active, tuning.maxAttempts)
			cancel()
			if err == nil && len(jobs) > capacity-active {
				err = appLogger.InvariantError("notification_fanout_claim_capacity", errors.New("fan-out store exceeded available capacity"))
			}
			if err == nil && len(jobs) == 0 {
				nextClaim, err = h.nextQueueClaim(ctx, persistence.NotificationQueueFanout, tuning.stepBudget, interval, tuning.maxAttempts)
			}
			if err != nil {
				if ctx.Err() == nil {
					h.log(ctx).Warn("notification fan-out claim failed", zap.String(logkeys.Stage, "claim"), zap.Error(err))
				}
				if errorDelay == 0 {
					errorDelay = interval
				} else {
					errorDelay = min(errorDelay*2, tuning.claimErrorMaxBackoff)
				}
				nextClaim = time.Now().Add(errorDelay)
			} else {
				errorDelay = 0
				if len(jobs) > 0 {
					nextClaim = time.Time{}
					for _, job := range jobs {
						if job.State == persistence.NotificationEmissionExhausted {
							// No step ran: count the outcome but pass a negative duration,
							// which the metrics layer treats like a negative completion age.
							h.observeFanoutStep("recovery", "exhausted", 0, -1, h.now().Sub(job.DueAt), -1)
							continue
						}
						active++
						steps.Go(func() {
							h.processNotificationEmission(ctx, store, job, tuning)
							completed <- struct{}{}
						})
					}
					continue
				}
			}
		}
		// The configuration tick reads memory even when capacity is full.
		var deadline <-chan time.Time
		if active < capacity {
			deadlineTimer.Reset(max(0, time.Until(nextClaim)))
			deadline = deadlineTimer.C
		}
		select {
		case <-ctx.Done():
			return
		case <-completed:
			active--
			if errorDelay == 0 {
				nextClaim = time.Time{}
			}
		case <-h.fanoutWake:
			if errorDelay == 0 {
				nextClaim = time.Time{}
			}
		case <-deadline:
		case <-ticker.C:
		}
		deadlineTimer.Stop()
	}
}

// runFanoutMaintenance drains chat retirement, samples the fan-out backlog,
// and compacts old emissions. Tuning is sampled once per loop iteration: a
// retirement pass is bounded by that sample's step budget; while it reports
// remaining work the loop restarts at once instead of waiting for the tick, but
// backlog sampling (every maintenance interval) and compaction (when due) still
// run between passes. A post-commit enqueue nudge also starts a pass immediately.
func (h *Handler) runFanoutMaintenance(ctx context.Context) {
	if h.store == nil {
		return
	}
	h.subscribeQueueWakes()
	store := h.store
	tuning := h.sampleFanoutTuning()
	interval := tuning.pollInterval
	configTick := time.NewTicker(interval)
	defer configTick.Stop()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var nextRetirement, nextCompaction, nextSample time.Time
	var errorDelay time.Duration
	sampleInterval, compactionInterval := tuning.maintenanceInterval, tuning.compactionInterval
	for {
		if ctx.Err() != nil {
			return
		}
		tuning = h.sampleFanoutTuning()
		if tuning.pollInterval != interval {
			interval = tuning.pollInterval
			configTick.Reset(interval)
		}
		if tuning.maintenanceInterval != sampleInterval {
			sampleInterval = tuning.maintenanceInterval
			nextSample = minDeadline(nextSample, time.Now().Add(sampleInterval))
		}
		if tuning.compactionInterval != compactionInterval {
			compactionInterval = tuning.compactionInterval
			nextCompaction = minDeadline(nextCompaction, time.Now().Add(compactionInterval))
		}
		more := false
		if !time.Now().Before(nextRetirement) {
			chatMore, chatErr := h.runChatRetirement(ctx, tuning)
			scopeMore, scopeErr := h.runScopeRetirement(ctx, tuning)
			sourceMore, sourceErr := h.runSourceResolution(ctx, tuning)
			more = chatMore || scopeMore || sourceMore
			err := errors.Join(chatErr, scopeErr, sourceErr)
			if !more && err == nil {
				nextRetirement, err = h.nextQueueClaim(ctx, persistence.NotificationQueueRetirement, tuning.stepBudget, interval, tuning.maxAttempts)
				if err != nil && ctx.Err() == nil {
					h.log(ctx).Warn("notification retirement deadline lookup failed", zap.Error(err))
				}
			}
			if err != nil {
				if errorDelay == 0 {
					errorDelay = interval
				} else {
					errorDelay = min(errorDelay*2, tuning.claimErrorMaxBackoff)
				}
				nextRetirement = time.Now().Add(errorDelay)
				more = false
			} else {
				errorDelay = 0
			}
		}

		if !time.Now().Before(nextSample) {
			nextSample = time.Now().Add(sampleInterval)
			sampleCtx, cancel := context.WithTimeout(ctx, tuning.stepBudget)
			backlog, err := store.NotificationFanoutBacklog(sampleCtx, h.now(), tuning.backlogCountLimit)
			cancel()
			h.observeFanoutBacklog(backlog, err)
			if err != nil && ctx.Err() == nil {
				h.log(ctx).Warn("notification fan-out backlog sample failed", zap.String(logkeys.Stage, "backlog"), zap.Error(err))
			}
		}
		if !time.Now().Before(nextCompaction) {
			compactCtx, cancel := context.WithTimeout(ctx, tuning.stepBudget)
			var compacted int64
			var err error
			for {
				compacted, err = store.CompactNotificationEmissions(compactCtx, h.now().Add(-tuning.compactionRetention), tuning.compactionBatch)
				if err != nil || compacted < int64(tuning.compactionBatch) || compactCtx.Err() != nil {
					break
				}
			}
			cancel()
			if err != nil && ctx.Err() == nil {
				h.log(ctx).Warn("notification fan-out compaction failed", zap.String(logkeys.Stage, "compaction"), zap.Error(err))
			}
			nextCompaction = time.Now().Add(compactionInterval)
			if compacted == int64(tuning.compactionBatch) {
				nextCompaction = time.Now().Add(interval)
			}
		}
		if more {
			nextRetirement = time.Time{}
			continue
		}
		next := minDeadline(nextRetirement, minDeadline(nextSample, nextCompaction))
		timer.Reset(max(0, time.Until(next)))
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-configTick.C:
		case <-h.retirementWake:
			if errorDelay == 0 {
				nextRetirement = time.Time{}
			}
		}
		timer.Stop()
	}
}

func minDeadline(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func emissionFields(job models.NotificationEmission, stage, outcome string) []zap.Field {
	return []zap.Field{
		zap.Uint64(appLogger.KeyNotificationEmissionID, job.ID),
		zap.Int64(appLogger.KeyOrganizationID, job.OrganizationID),
		zap.Uint32(appLogger.KeyNotificationType, job.Type),
		zap.Uint32(appLogger.KeyAttempt, job.Attempts),
		zap.String(logkeys.TraceID, job.TraceID),
		zap.String(logkeys.Stage, stage),
		zap.String(appLogger.KeyOutcome, outcome),
	}
}

// processNotificationEmission runs one step under the tuning sampled by the
// claim that leased job; a configuration change applies to the next claim.
func (h *Handler) processNotificationEmission(ctx context.Context, store persistence.NotificationStore, job models.NotificationEmission, tuning fanoutTuning) {
	started := h.now()
	stage := "batch"
	if !job.SnapshotReady {
		stage = "snapshot"
	}
	outcome, count, completionAge := "progress", 0, time.Duration(-1)
	defer func() {
		h.observeFanoutStep(stage, outcome, count, h.now().Sub(started), started.Sub(job.DueAt), completionAge)
	}()
	reason, deferred := "", false
	var err error
	cfg := h.deps.Config()
	if ctx.Err() != nil {
		reason, deferred = "shutdown", true
	} else if !cfg.Enabled || cfg.FanoutPaused || (job.OrganizationID != 0 && !cfg.OrganizationsEnabled) {
		reason, deferred = "disabled", true
	} else if _, known := h.registry.get(job.Type); !known {
		reason, deferred = "type_unavailable", true
	} else if job.Event && !h.producerAvailable(job) {
		reason, deferred = "producer_unavailable", true
	} else {
		// Lease validity belongs to the database clock. A process clock skew
		// must not turn a valid database lease into an immediately expired step.
		workCtx, cancel := context.WithTimeout(ctx, tuning.stepBudget)
		var result *models.NotificationEmission
		result, err = h.advanceNotificationEmission(workCtx, store, job, tuning)
		cancel()
		if err == nil {
			count = int(result.Processed - job.Processed)
			if result.State == persistence.NotificationEmissionCompleted {
				outcome, completionAge = "completed", h.now().Sub(result.CreatedAt)
			}
			return
		}
		if errors.Is(err, persistence.ErrNotificationLeaseLost) {
			outcome = "lease_lost"
			return
		}
		reason = "step_failed"
		if ctx.Err() != nil {
			reason, deferred = "shutdown", true
		} else if errors.Is(err, ErrDisabled) {
			reason, deferred = "disabled", true
		} else if errors.Is(err, errProducerUnavailable) {
			reason, deferred = "producer_unavailable", true
		}
	}
	outcome = "retry"
	// The claim already counted this attempt, so the first failure waits the base.
	retryAfter := exponentialBackoff(tuning.retryBackoffBase, tuning.retryBackoffMax, max(job.Attempts, 1)-1)
	if deferred {
		outcome, retryAfter = "deferred", tuning.deferredRetryDelay
	} else if job.Attempts >= tuning.maxAttempts {
		outcome = "exhausted"
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tuning.finishTimeout)
	finishErr := store.FinishNotificationEmissionAttempt(finishCtx, job, deferred, reason, retryAfter, tuning.maxAttempts)
	cancel()
	if finishErr != nil {
		outcome = "finish_failed"
		if errors.Is(finishErr, persistence.ErrNotificationLeaseLost) {
			outcome = "lease_lost"
		}
		h.log(ctx).Warn("notification fan-out completion failed", append(emissionFields(job, stage, outcome), zap.Error(finishErr))...)
		return
	}
	if !deferred {
		fields := append(emissionFields(job, stage, outcome), zap.String(appLogger.KeyFailureKind, notificationFailureKind(err)))
		if outcome == "exhausted" || errors.Is(err, errCallbackPanic) {
			h.log(ctx).Error("notification fan-out step failed", fields...)
		} else {
			h.log(ctx).Warn("notification fan-out step failed", fields...)
		}
	}
}

// fanoutMetrics returns the optional fan-out collectors. A metrics provider
// implements the whole set or none of it.
func (h *Handler) fanoutMetrics() (common_metrics.NotificationFanoutMetrics, bool) {
	return h.telemetry.fanout, h.telemetry.fanout != nil
}

func (h *Handler) observeFanoutActivity(cfg *Config, active int) {
	if m, ok := h.fanoutMetrics(); ok {
		m.RecordNotificationFanoutActivity(active, cfg.FanoutPaused)
	}
}

// A failed sample records only its failure, leaving the last good backlog
// values in place so the sample-failure series marks them stale.
func (h *Handler) observeFanoutBacklog(backlog persistence.NotificationFanoutBacklog, err error) {
	m, ok := h.fanoutMetrics()
	if !ok {
		return
	}
	m.RecordNotificationFanoutBacklogSample(err == nil)
	if err == nil {
		m.RecordNotificationFanoutBacklog(int64(backlog.PendingCount), backlog.OldestPendingAge, backlog.CountSaturated)
	}
}

func (h *Handler) observeFanoutStep(stage, outcome string, count int, duration, lateness, completionAge time.Duration) {
	if m, ok := h.fanoutMetrics(); ok {
		m.RecordNotificationFanoutStep(stage, outcome, count, duration, lateness, completionAge)
	}
}
