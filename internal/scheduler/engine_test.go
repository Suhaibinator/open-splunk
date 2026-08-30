package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fixedClock struct {
	now time.Time
}

func (clock fixedClock) Now() time.Time { return clock.now }

func (fixedClock) Wait(context.Context, time.Duration) error { return context.Canceled }

type recordingStepper struct {
	times  []time.Time
	err    error
	errors []error
}

func (stepper *recordingStepper) Step(_ context.Context, now time.Time) error {
	stepper.times = append(stepper.times, now)
	if len(stepper.errors) > 0 {
		err := stepper.errors[0]
		stepper.errors = stepper.errors[1:]
		return err
	}
	return stepper.err
}

type recordingClock struct {
	now       time.Time
	waits     []time.Duration
	stopAfter int
}

func (clock *recordingClock) Now() time.Time { return clock.now }

func (clock *recordingClock) Wait(_ context.Context, duration time.Duration) error {
	clock.waits = append(clock.waits, duration)
	clock.now = clock.now.Add(duration)
	if len(clock.waits) >= clock.stopAfter {
		return context.Canceled
	}
	return nil
}

func TestEngineStepUsesInjectedClock(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 20, 15, 0, 0, time.FixedZone("test", -7*60*60))
	stepper := new(recordingStepper)
	engine, err := NewEngine(EngineOptions{Clock: fixedClock{now: now}, Stepper: stepper})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if err := engine.Step(context.Background()); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if len(stepper.times) != 1 || !stepper.times[0].Equal(now) || stepper.times[0].Location() != time.UTC {
		t.Fatalf("step times = %#v, want one UTC instant equal to %v", stepper.times, now)
	}
}

func TestEngineRunPreservesCancellation(t *testing.T) {
	t.Parallel()
	stepper := new(recordingStepper)
	engine, err := NewEngine(EngineOptions{Clock: fixedClock{now: time.Now()}, Stepper: stepper})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if err := engine.Run(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if len(stepper.times) != 1 {
		t.Fatalf("step count = %d, want 1", len(stepper.times))
	}
}

func TestEngineRunRetriesTransientStepFailuresWithBoundedBackoff(t *testing.T) {
	t.Parallel()
	transient := errors.New("transient repository failure")
	clock := &recordingClock{now: time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC), stopAfter: 5}
	stepper := &recordingStepper{errors: []error{transient, transient, transient, transient, nil}}
	engine, err := NewEngine(EngineOptions{
		Clock: clock, Stepper: stepper, PollInterval: time.Second,
		ErrorBackoff: 100 * time.Millisecond, MaximumErrorBackoff: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if err := engine.Run(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 250 * time.Millisecond, 250 * time.Millisecond, time.Second}
	if len(clock.waits) != len(want) {
		t.Fatalf("waits = %v, want %v", clock.waits, want)
	}
	for index := range want {
		if clock.waits[index] != want[index] {
			t.Fatalf("waits = %v, want %v", clock.waits, want)
		}
	}
	if len(stepper.times) != 5 {
		t.Fatalf("step count = %d, want 5", len(stepper.times))
	}
}

func TestEngineRunCancellationInterruptsTransientBackoff(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	clock := cancelingClock{cancel: cancel}
	stepper := &recordingStepper{err: errors.New("transient repository failure")}
	engine, err := NewEngine(EngineOptions{Clock: clock, Stepper: stepper})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if err := engine.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
}

type cancelingClock struct{ cancel context.CancelFunc }

func (cancelingClock) Now() time.Time { return time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC) }

func (clock cancelingClock) Wait(ctx context.Context, _ time.Duration) error {
	clock.cancel()
	return ctx.Err()
}
