package ingestquota

import (
	"math"
	"testing"
	"time"
)

func TestEvaluateVirtualScheduleAndExactBoundary(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0).UTC()
	scope := ScopeKey{Kind: ScopeKindToken, TenantID: "tenant", Identity: "token"}
	first, err := Evaluate(now, Admission{Charges: []Charge{{
		Scope: scope, Limits: Limits{MaxEventsPerSecond: 10},
		Events: 5, UncompressedBytes: 100,
	}}})
	if err != nil || !first.Allowed || len(first.Updates) != 1 {
		t.Fatalf("first decision = %+v, error = %v", first, err)
	}
	state := first.Updates[0].State
	blocked, err := Evaluate(now, Admission{Charges: []Charge{{
		Scope: scope, Limits: state.Limits, Events: 1, UncompressedBytes: 1,
		State: &state,
	}}})
	if err != nil || blocked.Allowed || blocked.RetryAfter != 500*time.Millisecond {
		t.Fatalf("blocked decision = %+v, error = %v", blocked, err)
	}
	boundary, err := Evaluate(now.Add(500*time.Millisecond), Admission{Charges: []Charge{{
		Scope: scope, Limits: state.Limits, Events: 1, UncompressedBytes: 1,
		State: &state,
	}}})
	if err != nil || !boundary.Allowed {
		t.Fatalf("boundary decision = %+v, error = %v", boundary, err)
	}
}

func TestEvaluateOversizedBatchCarriesDebtAndCapsRetry(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0).UTC()
	scope := ScopeKey{Kind: ScopeKindIndex, TenantID: "tenant", Identity: "main"}
	first, err := Evaluate(now, Admission{Charges: []Charge{{
		Scope:  scope,
		Limits: Limits{MaxUncompressedBytesPerSecond: 1},
		Events: 1, UncompressedBytes: HardMaxAdmissionUncompressedBytes,
	}}})
	if err != nil || !first.Allowed {
		t.Fatalf("oversized first decision = %+v, error = %v", first, err)
	}
	state := first.Updates[0].State
	blocked, err := Evaluate(now, Admission{Charges: []Charge{{
		Scope: scope, Limits: state.Limits, Events: 1, UncompressedBytes: 1,
		State: &state,
	}}})
	if err != nil || blocked.Allowed || blocked.RetryAfter != MaximumRetryAfter {
		t.Fatalf("oversized retry decision = %+v, error = %v", blocked, err)
	}
}

func TestEvaluateMixedScopesIsAtomicAndTokenWinsTie(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0).UTC()
	future := now.Add(time.Second).UnixNano()
	limits := Limits{MaxEventsPerSecond: 10}
	token := ScopeKey{Kind: ScopeKindToken, TenantID: "tenant", Identity: "token"}
	index := ScopeKey{Kind: ScopeKindIndex, TenantID: "tenant", Identity: "main"}
	decision, err := Evaluate(now, Admission{Charges: []Charge{
		{Scope: index, Limits: limits, Events: 1, UncompressedBytes: 1, State: &State{
			Limits: limits, NextEventAdmissionUnixNano: future,
			UpdatedAtUnixMicro: now.Add(-time.Second).UnixMicro(),
		}},
		{Scope: token, Limits: limits, Events: 1, UncompressedBytes: 1, State: &State{
			Limits: limits, NextEventAdmissionUnixNano: future,
			UpdatedAtUnixMicro: now.Add(-time.Second).UnixMicro(),
		}},
	}})
	if err != nil || decision.Allowed || len(decision.Updates) != 0 ||
		decision.BlockingScope != token {
		t.Fatalf("mixed decision = %+v, error = %v", decision, err)
	}
}

func TestEvaluateIndexTieUsesLexicalIdentityRegardlessOfInputOrder(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0).UTC()
	future := now.Add(time.Second).UnixNano()
	limits := Limits{MaxEventsPerSecond: 10}
	decision, err := Evaluate(now, Admission{Charges: []Charge{
		{
			Scope:  ScopeKey{Kind: ScopeKindIndex, TenantID: "tenant", Identity: "z-last"},
			Limits: limits, Events: 1, UncompressedBytes: 1,
			State: &State{
				Limits: limits, NextEventAdmissionUnixNano: future,
				UpdatedAtUnixMicro: now.Add(-time.Second).UnixMicro(),
			},
		},
		{
			Scope:  ScopeKey{Kind: ScopeKindIndex, TenantID: "tenant", Identity: "a-first"},
			Limits: limits, Events: 1, UncompressedBytes: 1,
			State: &State{
				Limits: limits, NextEventAdmissionUnixNano: future,
				UpdatedAtUnixMicro: now.Add(-time.Second).UnixMicro(),
			},
		},
	}})
	if err != nil || decision.Allowed || decision.BlockingScope.Identity != "a-first" {
		t.Fatalf("index tie decision = %+v, error = %v", decision, err)
	}
}

func TestEvaluateBackwardClockHonorsFutureDurableSchedule(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0).UTC()
	limits := Limits{MaxEventsPerSecond: 1}
	scope := ScopeKey{Kind: ScopeKindToken, TenantID: "tenant", Identity: "token"}
	state := State{
		Limits:                     limits,
		NextEventAdmissionUnixNano: now.Add(3 * time.Second).UnixNano(),
		UpdatedAtUnixMicro:         now.Add(time.Second).UnixMicro(),
	}
	decision, err := Evaluate(now, Admission{Charges: []Charge{{
		Scope: scope, Limits: limits, Events: 1, UncompressedBytes: 1,
		State: &state,
	}}})
	if err != nil || decision.Allowed || decision.RetryAfter != 3*time.Second {
		t.Fatalf("backward-clock decision = %+v, error = %v", decision, err)
	}
}

func TestEvaluateRejectsScheduleOverflowAtomically(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, math.MaxInt64).UTC()
	scope := ScopeKey{Kind: ScopeKindToken, TenantID: "tenant", Identity: "token"}
	decision, err := Evaluate(now, Admission{Charges: []Charge{{
		Scope: scope,
		Limits: Limits{
			MaxUncompressedBytesPerSecond: HardMaxUncompressedBytesPerSecond,
		},
		Events: 1, UncompressedBytes: 1,
	}}})
	if err == nil {
		t.Fatalf("overflow decision succeeded: %+v", decision)
	}
	if decision.Allowed || decision.RetryAfter != 0 ||
		decision.BlockingScope != (ScopeKey{}) || len(decision.Updates) != 0 {
		t.Fatalf("overflow returned partial decision: %+v", decision)
	}
}

func TestScheduleDurationUsesExactCeilingArithmetic(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		cost uint64
		rate uint64
		want int64
	}{
		{name: "non-divisible", cost: 1, rate: 3, want: 333_333_334},
		{
			name: "maximum event charge and rate",
			cost: HardMaxAdmissionEvents,
			rate: HardMaxEventsPerSecond,
			want: int64(time.Millisecond),
		},
		{
			name: "maximum byte charge and rate",
			cost: HardMaxAdmissionUncompressedBytes,
			rate: HardMaxUncompressedBytesPerSecond,
			want: 7_630,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := scheduleDuration(test.cost, test.rate)
			if err != nil || got != test.want {
				t.Fatalf(
					"scheduleDuration(%d, %d) = %d, %v; want %d",
					test.cost,
					test.rate,
					got,
					err,
					test.want,
				)
			}
		})
	}
}

func TestEvaluatePolicyChangeResetsOnlyChangedDimension(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0).UTC()
	scope := ScopeKey{Kind: ScopeKindToken, TenantID: "tenant", Identity: "token"}
	state := State{
		Limits: Limits{
			MaxEventsPerSecond: 10, MaxUncompressedBytesPerSecond: 10,
		},
		NextEventAdmissionUnixNano: now.Add(time.Hour).UnixNano(),
		NextByteAdmissionUnixNano:  now.Add(2 * time.Second).UnixNano(),
		UpdatedAtUnixMicro:         now.Add(-time.Second).UnixMicro(),
	}
	decision, err := Evaluate(now, Admission{Charges: []Charge{{
		Scope: scope,
		Limits: Limits{
			MaxEventsPerSecond: 20, MaxUncompressedBytesPerSecond: 10,
		},
		Events: 1, UncompressedBytes: 1, State: &state,
	}}})
	if err != nil || decision.Allowed || decision.RetryAfter != 2*time.Second {
		t.Fatalf("policy-change decision = %+v, error = %v", decision, err)
	}
}

func TestEvaluateDisablingAllDimensionsPersistsReset(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0).UTC()
	scope := ScopeKey{Kind: ScopeKindToken, TenantID: "tenant", Identity: "token"}
	state := State{
		Limits:                     Limits{MaxEventsPerSecond: 1},
		NextEventAdmissionUnixNano: now.Add(time.Hour).UnixNano(),
		UpdatedAtUnixMicro:         now.Add(-time.Second).UnixMicro(),
	}
	decision, err := Evaluate(now, Admission{Charges: []Charge{{
		Scope: scope, Events: 1, UncompressedBytes: 1, State: &state,
	}}})
	if err != nil || !decision.Allowed || len(decision.Updates) != 1 ||
		decision.Updates[0].State.Limits != (Limits{}) ||
		decision.Updates[0].State.NextEventAdmissionUnixNano != 0 {
		t.Fatalf("disable decision = %+v, error = %v", decision, err)
	}
}

func TestEvaluateUnlimitedAndValidationFailures(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0).UTC()
	scope := ScopeKey{Kind: ScopeKindToken, TenantID: "tenant", Identity: "token"}
	unlimited, err := Evaluate(now, Admission{Charges: []Charge{{
		Scope: scope, Events: 1, UncompressedBytes: 1,
	}}})
	if err != nil || !unlimited.Allowed || len(unlimited.Updates) != 0 {
		t.Fatalf("unlimited decision = %+v, error = %v", unlimited, err)
	}
	tests := []Admission{
		{},
		{Charges: []Charge{{Scope: scope, Events: 0, UncompressedBytes: 1}}},
		{Charges: []Charge{{Scope: scope, Events: 1, UncompressedBytes: 0}}},
		{Charges: []Charge{{Scope: scope, Limits: Limits{MaxEventsPerSecond: HardMaxEventsPerSecond + 1}, Events: 1, UncompressedBytes: 1}}},
		{Charges: []Charge{{Scope: scope, Events: 1, UncompressedBytes: 1}, {Scope: scope, Events: 1, UncompressedBytes: 1}}},
		{Charges: []Charge{{Scope: scope, Limits: Limits{MaxEventsPerSecond: 1}, Events: 1, UncompressedBytes: 1, State: &State{Limits: Limits{MaxEventsPerSecond: 1}, NextEventAdmissionUnixNano: 0, UpdatedAtUnixMicro: 1}}}},
	}
	for index, admission := range tests {
		if _, evaluateErr := Evaluate(now, admission); evaluateErr == nil {
			t.Fatalf("invalid admission %d succeeded", index)
		}
	}
	if _, err := Evaluate(time.Unix(0, math.MinInt64), Admission{Charges: []Charge{{
		Scope: scope, Events: 1, UncompressedBytes: 1,
	}}}); err == nil {
		t.Fatal("unsupported time succeeded")
	}
	if _, err := Evaluate(time.Unix(0, int64(time.Microsecond)-1), Admission{Charges: []Charge{{
		Scope: scope, Events: 1, UncompressedBytes: 1,
	}}}); err == nil {
		t.Fatal("sub-microsecond epoch time succeeded")
	}
	boundary, err := Evaluate(time.Unix(0, int64(time.Microsecond)), Admission{Charges: []Charge{{
		Scope: scope, Limits: Limits{MaxEventsPerSecond: 1},
		Events: 1, UncompressedBytes: 1,
	}}})
	if err != nil || !boundary.Allowed ||
		len(boundary.Updates) != 1 || boundary.Updates[0].State.UpdatedAtUnixMicro != 1 {
		t.Fatalf("minimum persistent time decision = %+v, error = %v", boundary, err)
	}
}
