package loggen

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestPacerUsesAbsoluteDeadlines(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, time.July, 26, 15, 0, 0, 0, time.UTC)
	pacer, err := newPacerAt(4, start)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pacer.Close)

	for _, test := range []struct {
		ordinal uint64
		want    time.Time
	}{
		{ordinal: 0, want: start},
		{ordinal: 1, want: start.Add(250 * time.Millisecond)},
		{ordinal: 4, want: start.Add(time.Second)},
		{ordinal: 40, want: start.Add(10 * time.Second)},
	} {
		got, err := pacer.deadline(test.ordinal)
		if err != nil {
			t.Fatalf("deadline(%d) = %v", test.ordinal, err)
		}
		if !got.Equal(test.want) {
			t.Fatalf("deadline(%d) = %s, want %s", test.ordinal, got, test.want)
		}
	}
}

func TestPacerValidatesRateAndDeadlineRange(t *testing.T) {
	t.Parallel()
	for _, rate := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1), 1e-10, 1e20} {
		if _, err := newPacerAt(rate, time.Now()); err == nil {
			t.Fatalf("newPacerAt(%v) unexpectedly succeeded", rate)
		}
	}
	unpaced, err := newPacerAt(0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unpaced.Close)
	if _, err := unpaced.deadline(math.MaxUint64); err != nil {
		t.Fatalf("unpaced deadline = %v", err)
	}

	paced, err := newPacerAt(1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(paced.Close)
	if _, err := paced.deadline(math.MaxUint64); err == nil {
		t.Fatal("paced deadline overflow unexpectedly succeeded")
	}
}

func TestPacerHonorsCancellationWithoutWaitingForDeadline(t *testing.T) {
	t.Parallel()
	pacer, err := NewPacer(1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pacer.Close)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if err := pacer.Wait(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait(canceled) = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("Wait(canceled) took %s", elapsed)
	}
}

func TestPacerDoesNotBurstAfterFallingBehind(t *testing.T) {
	t.Parallel()
	pacer, err := NewPacer(100)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pacer.Close)
	if err := pacer.Wait(context.Background(), 0); err != nil {
		t.Fatal(err)
	}

	time.Sleep(130 * time.Millisecond)
	if err := pacer.Wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := pacer.Wait(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 5*time.Millisecond {
		t.Fatalf("two overdue events were released as a %s catch-up burst", elapsed)
	}
}

func TestPacerReusesTimerAfterArmedWaitIsCanceled(t *testing.T) {
	t.Parallel()
	pacer, err := NewPacer(20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pacer.Close)
	if err := pacer.Wait(context.Background(), 0); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := pacer.Wait(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait(deadline) = %v, want context.DeadlineExceeded", err)
	}
	started := time.Now()
	if err := pacer.Wait(context.Background(), 1); err != nil {
		t.Fatalf("Wait after timer cancellation = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond {
		t.Fatalf("Wait after timer cancellation returned after %s, want a newly armed wait", elapsed)
	}
}
