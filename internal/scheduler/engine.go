package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultErrorBackoff        = 250 * time.Millisecond
	defaultMaximumErrorBackoff = 30 * time.Second
	defaultPollInterval        = time.Second
)

// Stepper performs one bounded scheduling scan at the supplied wall time.
// Durable claim semantics belong to the implementation's repository.
type Stepper interface {
	Step(context.Context, time.Time) error
}

// Engine repeatedly asks a durable service to claim and dispatch due work.
// It intentionally has no in-memory cron registry: the database remains the
// source of truth across process restarts and multiple scheduler workers.
type Engine struct {
	clock        Clock
	errorBackoff time.Duration
	maxBackoff   time.Duration
	pollInterval time.Duration
	stepper      Stepper
}

// EngineOptions supplies explicit process dependencies.
type EngineOptions struct {
	Clock               Clock
	ErrorBackoff        time.Duration
	MaximumErrorBackoff time.Duration
	PollInterval        time.Duration
	Stepper             Stepper
}

// NewEngine validates and constructs an Engine.
func NewEngine(options EngineOptions) (*Engine, error) {
	if options.Stepper == nil {
		return nil, errors.New("scheduler stepper is required")
	}
	clock := options.Clock
	if clock == nil {
		clock = RealClock{}
	}
	pollInterval := options.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollInterval
	}
	if pollInterval < time.Millisecond || pollInterval > time.Minute {
		return nil, errors.New("scheduler poll interval must be between one millisecond and one minute")
	}
	errorBackoff := options.ErrorBackoff
	if errorBackoff == 0 {
		errorBackoff = defaultErrorBackoff
	}
	maximumErrorBackoff := options.MaximumErrorBackoff
	if maximumErrorBackoff == 0 {
		maximumErrorBackoff = defaultMaximumErrorBackoff
	}
	if errorBackoff < time.Millisecond || maximumErrorBackoff > time.Minute || errorBackoff > maximumErrorBackoff {
		return nil, errors.New("scheduler error backoff must be between one millisecond and one minute")
	}
	return &Engine{
		clock: clock, errorBackoff: errorBackoff, maxBackoff: maximumErrorBackoff,
		pollInterval: pollInterval, stepper: options.Stepper,
	}, nil
}

// Step performs one deterministic scan. It is exposed for tests and explicit
// wakeups after schedule mutations.
func (engine *Engine) Step(ctx context.Context) error {
	if ctx == nil {
		return errors.New("scheduler context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := engine.stepper.Step(ctx, engine.clock.Now().UTC()); err != nil {
		return fmt.Errorf("scheduler step: %w", err)
	}
	return nil
}

// Run steps immediately, then at the configured interval until cancellation.
func (engine *Engine) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("scheduler context is required")
	}
	backoff := engine.errorBackoff
	for {
		if err := engine.Step(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if err := engine.clock.Wait(ctx, backoff); err != nil {
				return err
			}
			backoff = min(backoff*2, engine.maxBackoff)
			continue
		}
		backoff = engine.errorBackoff
		if err := engine.clock.Wait(ctx, engine.pollInterval); err != nil {
			return err
		}
	}
}
