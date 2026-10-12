package notification

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// SetWorkerCount sets startup-only delivery concurrency. Call before Start;
// enablement and notification policies continue to be sampled while running.
func (h *Handler) SetWorkerCount(count int) error {
	if count < 1 || count > config.MaxNotificationDeliveryWorkerCount {
		return ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cancel != nil || h.stopped {
		return ErrRegistryFrozen
	}
	h.workerCount = count
	return nil
}

// Start freezes registrations and starts the delivery and fan-out coordinators.
// Only idle worker capacity is claimed; no leased backlog waits in a local queue.
// The delivery pool size (unless SetWorkerCount set one) and the shutdown
// deadline are captured here once; changing them needs a restart.
func (h *Handler) Start() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cancel != nil || h.stopped {
		return
	}
	h.registry.freeze()
	h.producers.freeze()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.workerDone = make(chan struct{})
	cfg := h.deps.Config()
	count := h.workerCount
	if count == 0 {
		count = cfg.DeliveryWorkerCount
	}
	h.shutdownTimeout = cfg.WorkerShutdownTimeout
	h.subscribeQueueWakes()
	go func() {
		defer close(h.workerDone)
		if h.unsubscribeQueues != nil {
			defer h.unsubscribeQueues()
		}
		var pools sync.WaitGroup
		pools.Go(func() { h.runWorkers(ctx, count, pollJitter) })
		pools.Go(func() { h.runFanoutWorkers(ctx) })
		pools.Go(func() { h.runFanoutMaintenance(ctx) })
		pools.Wait()
	}()
}

// Stop cancels claims and attempts, and waits for bounded completion cleanup.
// Application callbacks must honor cancellation; Go cannot terminate a callback
// which ignores its context. Repeated calls reuse the same completion channel.
func (h *Handler) Stop() {
	h.mu.Lock()
	h.stopped = true
	cancel, done, deadline := h.cancel, h.workerDone, h.shutdownTimeout
	h.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		h.log(context.Background()).Warn("notification worker shutdown deadline exceeded",
			zap.String(logkeys.Stage, "shutdown"), zap.String(appLogger.KeyOutcome, "deadline_exceeded"))
	}
}

// deliveryTuning is one sample of the hot delivery knobs. The claim loop
// samples it once per iteration and hands the claim's sample to every job that
// claim leased, so the lease, the work budget, the retry schedule, and the
// exhaustion check always agree; in-flight jobs keep the values they started
// with. Values travel as parameters, never in a context.
type deliveryTuning struct {
	pollInterval         time.Duration
	claimErrorMaxBackoff time.Duration
	jitterFraction       float64
	claimTimeout         time.Duration
	lease                time.Duration
	maxAttempts          uint32
	workBudget           time.Duration
	completionMargin     time.Duration
	retryBackoffBase     time.Duration
	retryBackoffMax      time.Duration
	activeViewDefer      time.Duration
	disabledDefer        time.Duration
	typeUnavailableDefer time.Duration
	revisionChangedDefer time.Duration
	completionTimeout    time.Duration
	completionRetryBase  time.Duration
	completionRetryMax   time.Duration
	source               sourceLimits
}

// deliveryTuningFor copies one configuration sample. The configuration is
// authoritative: pkg/config validation requires every key and its range and
// cross-key orderings, so nothing is defaulted or clamped here.
func deliveryTuningFor(cfg *Config) deliveryTuning {
	return deliveryTuning{
		source:               sourceLimitsFor(cfg),
		pollInterval:         cfg.DeliveryPollInterval,
		claimErrorMaxBackoff: cfg.DeliveryClaimErrorMaxBackoff,
		jitterFraction:       cfg.DeliveryPollJitterFraction,
		claimTimeout:         cfg.DeliveryClaimTimeout,
		lease:                cfg.DeliveryLease,
		maxAttempts:          uint32(cfg.DeliveryMaxAttempts),
		workBudget:           cfg.DeliveryWorkBudget,
		completionMargin:     cfg.DeliveryLeaseCompletionMargin,
		retryBackoffBase:     cfg.DeliveryRetryBackoffBase,
		retryBackoffMax:      cfg.DeliveryRetryBackoffMax,
		activeViewDefer:      cfg.DeliveryActiveViewDefer,
		disabledDefer:        cfg.DeliveryDisabledDefer,
		typeUnavailableDefer: cfg.DeliveryTypeUnavailableDefer,
		revisionChangedDefer: cfg.DeliveryRevisionChangedDefer,
		completionTimeout:    cfg.DeliveryCompletionTimeout,
		completionRetryBase:  cfg.DeliveryCompletionRetryBackoffBase,
		completionRetryMax:   cfg.DeliveryCompletionRetryBackoffMax,
	}
}

// exponentialBackoff doubles base once per prior failure and caps it at ceiling;
// the first retry (zero prior failures) waits exactly base.
// The loop stops at the ceiling, so it is bounded however large attempts is.
func exponentialBackoff(base, ceiling time.Duration, priorFailures uint32) time.Duration {
	delay := base
	for range priorFailures {
		if delay >= ceiling {
			break
		}
		delay *= 2
	}
	return min(delay, ceiling)
}

func pollJitter(delay time.Duration, fraction float64) time.Duration {
	// Positive jitter preserves the lower bound on retry/recovery delays and avoids
	// synchronized claim bursts when several API processes start together.
	return delay + time.Duration(rand.Int64N(max(1, int64(float64(delay)*fraction))))
}

func (h *Handler) claimDeliveries(ctx context.Context, owner string, limit int, tuning deliveryTuning) ([]models.NotificationDelivery, error) {
	claimCtx, cancel := context.WithTimeout(ctx, tuning.claimTimeout)
	defer cancel()
	jobs, err := h.store.ClaimDeliveries(claimCtx, owner, h.now(), tuning.lease, limit, tuning.maxAttempts)
	if err == nil && len(jobs) > limit {
		err = errors.New("notification store exceeded claim capacity")
		appLogger.LogInvariantViolation(h.log(ctx), "notification_claim_capacity", err,
			zap.String(logkeys.Stage, "claim"), zap.Int(logkeys.Expected, limit), zap.Int(logkeys.Actual, len(jobs)),
			zap.String(logkeys.Fallback, "leave claims leased for expiry recovery"))
		return nil, err
	}
	if err != nil && ctx.Err() == nil {
		h.log(ctx).Warn("notification delivery claim failed", zap.String(logkeys.Stage, "claim"),
			zap.String(appLogger.KeyOutcome, "retry"), zap.Error(err))
	}
	return jobs, err
}

// claimedDelivery carries a leased job with the tuning sampled by its claim.
type claimedDelivery struct {
	job    models.NotificationDelivery
	tuning deliveryTuning
}

// runWorkers leases only idle capacity. The delivery tuning is resampled at
// every loop iteration; jitter is injectable so tests can make delays exact.
func (h *Handler) runWorkers(ctx context.Context, count int, jitter func(time.Duration, float64) time.Duration) {
	h.subscribeQueueWakes()
	jobs := make(chan claimedDelivery)
	completed := make(chan struct{}, count)
	var workers sync.WaitGroup
	for range count {
		workers.Go(func() {
			for claimed := range jobs {
				h.processDelivery(ctx, claimed.job, claimed.tuning)
				completed <- struct{}{}
			}
		})
	}
	defer workers.Wait()
	defer close(jobs)

	owner := uuid.New().String()
	free := count
	var nextClaim time.Time
	var errorDelay time.Duration
	interval := h.deps.Config().DeliveryPollInterval
	configTick := time.NewTicker(interval)
	defer configTick.Stop()
	var wasEnabled bool
	var lastAttempts uint32
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		// Account for completed work before deciding how much capacity to lease.
		for len(completed) > 0 {
			<-completed
			free++
			if errorDelay == 0 {
				nextClaim = time.Time{}
			}
		}
		// A fresh sample per iteration: jobs capture this claim's tuning and
		// never observe a later iteration's resample.
		cfg := h.deps.Config()
		tuning := deliveryTuningFor(cfg)
		enabled := h.store != nil && cfg.Enabled && h.emailGateOpen()
		if tuning.pollInterval != interval {
			interval = tuning.pollInterval
			configTick.Reset(interval)
		}
		if (enabled != wasEnabled || lastAttempts != tuning.maxAttempts) && errorDelay == 0 {
			nextClaim = time.Time{}
		}
		wasEnabled, lastAttempts = enabled, tuning.maxAttempts
		if free > 0 && enabled && !time.Now().Before(nextClaim) {
			claimed, err := h.claimDeliveries(ctx, owner, free, tuning)
			if err == nil && len(claimed) == 0 {
				nextClaim, err = h.nextQueueClaim(ctx, persistence.NotificationQueueDelivery, tuning.claimTimeout, jitter(tuning.pollInterval, tuning.jitterFraction), tuning.maxAttempts)
				if err != nil && ctx.Err() == nil {
					h.log(ctx).Warn("notification delivery deadline lookup failed", zap.Error(err))
				}
			}
			if err != nil {
				if errorDelay == 0 {
					errorDelay = tuning.pollInterval
				} else {
					errorDelay = min(errorDelay*2, tuning.claimErrorMaxBackoff)
				}
				nextClaim = time.Now().Add(jitter(errorDelay, tuning.jitterFraction))
				continue
			}
			errorDelay = 0
			if len(claimed) == 0 {
				continue
			}
			// Each claimed job has a reserved idle worker. The unbuffered channel
			// delivers directly, including cancellation cleanup; no goroutine
			// is created for an individual job.
			for _, job := range claimed {
				jobs <- claimedDelivery{job: job, tuning: tuning}
				free--
			}
			nextClaim = time.Time{}
			continue
		}

		// Claim errors retain their retry deadline across completions and wakes.
		// Occupied slots wait for completion while config checks stay in memory.
		var tick <-chan time.Time
		if free > 0 && enabled {
			delay := max(0, time.Until(nextClaim))
			timer.Reset(delay)
			tick = timer.C
		}
		select {
		case <-ctx.Done():
			return
		case <-completed:
			free++
			if errorDelay == 0 {
				nextClaim = time.Time{}
			}
		case <-h.deliveryWake:
			if errorDelay == 0 {
				nextClaim = time.Time{}
			}
		case <-configTick.C:
		case <-tick:
		}
		timer.Stop()
	}
}
