package indexpolicy

import (
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

var policyTestNow = time.Date(2026, 7, 31, 12, 0, 0, 123_456_789, time.UTC)

func TestPolicyResolveRetentionAtValidatesAndResolvesInheritance(t *testing.T) {
	t.Parallel()

	policy := Policy{Name: "main", Version: 1, DefaultSourcetype: "go:zap:json"}
	retention, err := policy.ResolveRetentionAt(policyTestNow, DefaultRetention)
	if err != nil {
		t.Fatal(err)
	}
	if retention != DefaultRetention {
		t.Fatalf("resolved retention = %v, want %v", retention, DefaultRetention)
	}

	policy.RetentionPeriod = 100 * 365 * 24 * time.Hour
	retention, err = policy.ResolveRetentionAt(policyTestNow, DefaultRetention)
	if err != nil || retention != policy.RetentionPeriod {
		t.Fatalf("explicit retention = %v, %v", retention, err)
	}
}

func TestValidateRetentionAtRejectsStorageIncompatibleValues(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		value     time.Duration
		reference time.Time
		allowZero bool
	}{
		"resolved zero":        {reference: policyTestNow},
		"negative":             {value: -time.Millisecond, reference: policyTestNow},
		"sub-millisecond":      {value: time.Nanosecond, reference: policyTestNow},
		"past storage horizon": {value: 8_000_000_000 * time.Second, reference: policyTestNow},
		"zero reference":       {value: time.Hour},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateRetentionAt(test.value, test.reference, test.allowZero); err == nil {
				t.Fatal("ValidateRetentionAt accepted an incompatible retention")
			}
		})
	}
	if err := ValidateRetentionAt(0, time.Time{}, true); err != nil {
		t.Fatalf("zero inheritance sentinel = %v", err)
	}
}

func TestLimitsAndCanonicalTextShareHardPolicyBoundary(t *testing.T) {
	t.Parallel()

	if err := (Limits{
		MaxEventBytes:     HardMaxEventBytes,
		MaxFieldCount:     HardMaxFieldCount,
		MaxNestingDepth:   HardMaxNestingDepth,
		MaximumFutureSkew: HardMaxFutureSkew,
		MaximumEventAge:   HardMaxEventAge,
	}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Limits{MaxFieldCount: HardMaxFieldCount + 1}).Validate(); err == nil {
		t.Fatal("limits accepted a value above the hard field-count ceiling")
	}
	for _, invalid := range []string{"Main", "bad name", "kvstore_logs"} {
		if ValidName(invalid) {
			t.Fatalf("ValidName(%q) = true", invalid)
		}
	}
	for _, invalid := range []string{" surrounding ", "line\nbreak", string([]byte{0xff})} {
		if ValidDefaultSourcetype(invalid) {
			t.Fatalf("ValidDefaultSourcetype(%q) = true", invalid)
		}
	}
}

func TestPolicyValidatesIngestionRateLimits(t *testing.T) {
	t.Parallel()

	policy := Policy{
		Name:    "main",
		Version: 1,
		IngestionRateLimits: ingestquota.Limits{
			MaxEventsPerSecond:            ingestquota.HardMaxEventsPerSecond,
			MaxUncompressedBytesPerSecond: ingestquota.HardMaxUncompressedBytesPerSecond,
		},
	}
	if err := policy.ValidateStoredAt(policyTestNow); err != nil {
		t.Fatalf("ValidateStoredAt(hard limits): %v", err)
	}

	policy.IngestionRateLimits.MaxEventsPerSecond = ingestquota.HardMaxEventsPerSecond + 1
	if err := policy.ValidateStoredAt(policyTestNow); err == nil {
		t.Fatal("ValidateStoredAt accepted an ingestion event rate above the hard ceiling")
	}
	policy.IngestionRateLimits.MaxEventsPerSecond = 0
	policy.IngestionRateLimits.MaxUncompressedBytesPerSecond =
		ingestquota.HardMaxUncompressedBytesPerSecond + 1
	if err := policy.ValidateStoredAt(policyTestNow); err == nil {
		t.Fatal("ValidateStoredAt accepted an ingestion byte rate above the hard ceiling")
	}
}
