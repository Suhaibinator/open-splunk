package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestParseCronRejectsNonNumericOrNonFiveFieldSyntax(t *testing.T) {
	t.Parallel()
	tests := []string{
		"@hourly",
		"0 0 * *",
		"0 0 * * * *",
		"0 0 * JAN *",
		"CRON_TZ=UTC 0 0 * * *",
	}
	for _, expression := range tests {
		t.Run(expression, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseCron(expression, "UTC"); err == nil {
				t.Fatalf("ParseCron(%q) unexpectedly succeeded", expression)
			}
		})
	}
}

func TestAdvancePastContextCountsYearsOfEveryMinuteOccurrences(t *testing.T) {
	t.Parallel()
	schedule, err := ParseCron("* * * * *", "UTC")
	if err != nil {
		t.Fatalf("ParseCron: %v", err)
	}
	first := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	now := first.AddDate(5, 0, 0).Add(37 * time.Minute)
	latest, next, skipped, err := schedule.AdvancePastContext(context.Background(), first, now)
	if err != nil {
		t.Fatalf("AdvancePastContext: %v", err)
	}
	wantSkipped := uint64(now.Sub(first) / time.Minute)
	if !latest.Equal(now) || !next.Equal(now.Add(time.Minute)) || skipped != wantSkipped {
		t.Fatalf("catch-up = (%v, %v, %d), want (%v, %v, %d)", latest, next, skipped, now, now.Add(time.Minute), wantSkipped)
	}
}

func TestAdvancePastContextMatchesIterativeCronAcrossDST(t *testing.T) {
	t.Parallel()
	schedule, err := ParseCron("7,23,51 1-4 * * *", "America/Los_Angeles")
	if err != nil {
		t.Fatalf("ParseCron: %v", err)
	}
	first := schedule.Next(time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC))
	now := time.Date(2027, time.December, 31, 23, 59, 0, 0, time.UTC)
	wantLatest := first
	var wantSkipped uint64
	for candidate := schedule.Next(wantLatest); !candidate.After(now); candidate = schedule.Next(wantLatest) {
		wantLatest = candidate
		wantSkipped++
	}
	wantNext := schedule.Next(wantLatest)
	latest, next, skipped, err := schedule.AdvancePastContext(context.Background(), first, now)
	if err != nil {
		t.Fatalf("AdvancePastContext: %v", err)
	}
	if !latest.Equal(wantLatest) || !next.Equal(wantNext) || skipped != wantSkipped {
		t.Fatalf("catch-up = (%v, %v, %d), want (%v, %v, %d)", latest, next, skipped, wantLatest, wantNext, wantSkipped)
	}
}

func TestAdvancePastContextHonorsCancellationAndRangeBound(t *testing.T) {
	t.Parallel()
	schedule, err := ParseCron("* * * * *", "UTC")
	if err != nil {
		t.Fatalf("ParseCron: %v", err)
	}
	first := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := schedule.AdvancePastContext(canceled, first, first.Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled catch-up error = %v, want context.Canceled", err)
	}
	tooFar := first.Add(time.Duration(maximumCatchUpHourWindows+1) * time.Hour)
	if _, _, _, err := schedule.AdvancePastContext(context.Background(), first, tooFar); err == nil {
		t.Fatal("oversized catch-up unexpectedly succeeded")
	}
}

func TestCronScheduleUsesIANAZoneAcrossDST(t *testing.T) {
	t.Parallel()
	schedule, err := ParseCron("0 9 * * *", "America/Los_Angeles")
	if err != nil {
		t.Fatalf("ParseCron: %v", err)
	}

	beforeSpringForward := time.Date(2026, time.March, 7, 17, 0, 0, 0, time.UTC)
	period, err := schedule.Period(beforeSpringForward)
	if err != nil {
		t.Fatalf("Period: %v", err)
	}
	if period != 23*time.Hour {
		t.Fatalf("spring-forward period = %v, want 23h", period)
	}

	beforeFallBack := time.Date(2026, time.October, 31, 16, 0, 0, 0, time.UTC)
	period, err = schedule.Period(beforeFallBack)
	if err != nil {
		t.Fatalf("Period: %v", err)
	}
	if period != 25*time.Hour {
		t.Fatalf("fall-back period = %v, want 25h", period)
	}
}

func TestAdvancePastKeepsLatestDueAndSkipsOlderOccurrences(t *testing.T) {
	t.Parallel()
	schedule, err := ParseCron("*/5 * * * *", "UTC")
	if err != nil {
		t.Fatalf("ParseCron: %v", err)
	}
	first := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	latest, next, skipped, err := schedule.AdvancePast(first, first.Add(17*time.Minute))
	if err != nil {
		t.Fatalf("AdvancePast: %v", err)
	}
	if want := first.Add(15 * time.Minute); !latest.Equal(want) {
		t.Fatalf("latest = %v, want %v", latest, want)
	}
	if want := first.Add(20 * time.Minute); !next.Equal(want) {
		t.Fatalf("next = %v, want %v", next, want)
	}
	if skipped != 3 {
		t.Fatalf("skipped = %d, want 3", skipped)
	}
}
