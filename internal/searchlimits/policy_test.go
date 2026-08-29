package searchlimits

import (
	"context"
	"testing"
	"time"
)

func TestDefaultPolicyAndPublishedSnapshots(t *testing.T) {
	policy := Default()
	if err := Validate(policy); err != nil {
		t.Fatalf("Validate(Default()) = %v", err)
	}
	source, err := NewSource(policy)
	if err != nil {
		t.Fatal(err)
	}
	updated := policy
	updated.MaxRuntime = 3 * time.Minute
	if err := source.Store(updated); err != nil {
		t.Fatal(err)
	}
	if got := source.Snapshot(); got != updated {
		t.Fatalf("Snapshot() = %+v, want %+v", got, updated)
	}
	if policy.MaxRuntime != 2*time.Minute {
		t.Fatal("publishing a policy mutated the previous snapshot")
	}
	invalid := updated
	invalid.MaxConcurrent = 0
	if err := source.Store(invalid); err == nil {
		t.Fatal("Store() published an invalid policy")
	}
	if got := source.Snapshot(); got != updated {
		t.Fatalf("failed Store() changed snapshot to %+v", got)
	}
	ctx := WithPolicy(context.Background(), policy)
	captured, ok := FromContext(ctx)
	if !ok || captured != policy {
		t.Fatalf("FromContext() = (%+v, %t)", captured, ok)
	}
}

func TestValidateBoundsAndRelationship(t *testing.T) {
	for name, mutate := range map[string]func(*Policy){
		"runtime": func(policy *Policy) { policy.MaxRuntime = 9 * time.Second },
		"memory":  func(policy *Policy) { policy.MaxMemoryBytes = 1 },
		"rows":    func(policy *Policy) { policy.MaxRowsToRead = 10_000_000_001 },
		"result relationship": func(policy *Policy) {
			policy.MaxResultBytes = 2 << 20
			policy.MaxTotalResultBytes = 1 << 20
		},
		"concurrency": func(policy *Policy) { policy.MaxConcurrent = 0 },
		"retention":   func(policy *Policy) { policy.ResultRetention = 31 * 24 * time.Hour },
	} {
		t.Run(name, func(t *testing.T) {
			policy := Default()
			mutate(&policy)
			if Validate(policy) == nil {
				t.Fatalf("Validate(%+v) succeeded", policy)
			}
		})
	}
}
