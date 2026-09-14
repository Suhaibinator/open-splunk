package ingestquota

import "testing"

func TestRejectionBudgetIsFiniteAndIndependentOfAcceptedCosts(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		token Limits
		want  Limits
	}{
		{name: "unlimited", want: Limits{MaxEventsPerSecond: 10, MaxUncompressedBytesPerSecond: 256 << 10}},
		{name: "restrictive", token: Limits{MaxEventsPerSecond: 1, MaxUncompressedBytesPerSecond: 2}, want: Limits{MaxEventsPerSecond: 1, MaxUncompressedBytesPerSecond: 2}},
		{name: "high", token: Limits{MaxEventsPerSecond: 100, MaxUncompressedBytesPerSecond: 1 << 20}, want: Limits{MaxEventsPerSecond: 10, MaxUncompressedBytesPerSecond: 256 << 10}},
	} {
		t.Run(test.name, func(t *testing.T) {
			admission := RejectionAdmission{Scope: ScopeKey{Kind: ScopeKindToken, TenantID: "tenant", Identity: "token"}, TokenLimits: test.token}
			for _, size := range []uint64{0, 1, 64 << 10} {
				charge, err := admission.Charge(size)
				if err != nil || charge.Scope != admission.Scope || charge.Limits != test.want || charge.Events != 1 || charge.UncompressedBytes != max(1, size) || charge.State != nil {
					t.Fatalf("Charge(%d) = %+v, %v", size, charge, err)
				}
			}
		})
	}
}
