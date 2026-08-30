package scheduledreports

import "testing"

func TestRunOutcomeTerminalUsesExplicitLifecycleSet(t *testing.T) {
	t.Parallel()
	tests := []struct {
		outcome RunOutcome
		want    bool
	}{
		{outcome: RunOutcomeInvalid},
		{outcome: RunOutcomeClaimed},
		{outcome: RunOutcomeSubmitted},
		{outcome: RunOutcomeSucceeded, want: true},
		{outcome: RunOutcomeFailed, want: true},
		{outcome: RunOutcomeCanceled, want: true},
		{outcome: RunOutcomeExpired, want: true},
		{outcome: RunOutcomeInterrupted, want: true},
		{outcome: RunOutcomeSkippedOverlap, want: true},
		{outcome: RunOutcome(255)},
	}
	for _, test := range tests {
		if got := test.outcome.terminal(); got != test.want {
			t.Fatalf("outcome %d terminal = %t, want %t", test.outcome, got, test.want)
		}
	}
}
