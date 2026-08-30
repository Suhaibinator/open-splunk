package searchartifacts

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestInspectManyCanonicalizesWithoutRefreshingOrExpiring(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 13, 0, 0, 0, time.UTC)}
	store, database, _ := newTestStore(t, clock, DefaultMaximumBytes)
	for _, id := range []string{"inspect-b", "inspect-a"} {
		job := testQueuedJob(t, id, clock.now)
		if err := store.Admit(ctx, job); err != nil {
			t.Fatal(err)
		}
		if err := store.Finalize(ctx, completeTestJob(job, clock.now, time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	clock.now = clock.now.Add(time.Minute)
	input := []string{"inspect-b", "missing", "inspect-a", "inspect-b"}
	records, err := store.InspectMany(ctx, testAccess(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(input, []string{"inspect-b", "missing", "inspect-a", "inspect-b"}) {
		t.Fatalf("InspectMany mutated input: %v", input)
	}
	if len(records) != 2 || records["inspect-a"].State != StateExpired || records["inspect-b"].State != StateExpired {
		t.Fatalf("InspectMany records = %#v", records)
	}
	for id, record := range records {
		if !record.LastAccessedAt.IsZero() || !record.ExpiresAt.Equal(clock.now) {
			t.Fatalf("record %q refreshed = %#v", id, record)
		}
	}
	var persisted State
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT state FROM durable_search_jobs WHERE id = ?`, "inspect-a").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != StateQueued {
		t.Fatalf("InspectMany persisted staging state = %v, want queued", persisted)
	}
	empty, err := store.InspectMany(ctx, testAccess(), nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("InspectMany(empty) = %#v, %v", empty, err)
	}
}

func TestInspectManyEnforcesBoundsAndVisibility(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "private-job", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, MaximumInspectManyJobs+1)
	for index := range ids {
		ids[index] = "same-valid-id"
	}
	if _, err := store.InspectMany(ctx, testAccess(), ids); !errors.Is(err, ErrInvalid) {
		t.Fatalf("InspectMany(over limit) error = %v", err)
	}
	other := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "other"}
	records, err := store.InspectMany(ctx, other, []string{job.ID})
	if err != nil || len(records) != 0 {
		t.Fatalf("InspectMany(private from other owner) = %#v, %v", records, err)
	}
}

func TestStateFromJobUsesExhaustiveMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input searchjobs.State
		want  State
	}{
		{searchjobs.StateQueued, StateQueued},
		{searchjobs.StateParsing, StateParsing},
		{searchjobs.StatePlanning, StatePlanning},
		{searchjobs.StateRunning, StateRunning},
		{searchjobs.StateCompleted, StateCompleted},
		{searchjobs.StateFailed, StateFailed},
		{searchjobs.StateCanceled, StateCanceled},
		{searchjobs.StateExpired, StateExpired},
		{searchjobs.StateInvalid, StateInvalid},
		{searchjobs.State(255), StateInvalid},
	}
	for _, test := range tests {
		if got := stateFromJob(test.input); got != test.want {
			t.Errorf("stateFromJob(%d) = %d, want %d", test.input, got, test.want)
		}
	}
}
