package knowledgesnapshot

import (
	"crypto/sha256"
	"testing"
)

var (
	retainedExecutionFactsSink RetainedExecutionAuthorityFacts
	retainedExecutionValidSink bool
)

func TestRetainedExecutionAuthorityFactsAllocationDoesNotScaleWithEncoding(t *testing.T) {
	authority, err := Prepare(Input{
		TenantID: "tenant-allocation", PrincipalID: "principal-allocation", AppID: "app-allocation",
		TenantCatalogStateToken:    make([]byte, sha256.Size),
		EffectiveAuthorizedIndexes: []string{"main"},
	})
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	small, err := finalize(authority, evidenceFor(authority))
	if err != nil {
		t.Fatalf("finalize(): %v", err)
	}
	large := small
	large.encoded = make([]byte, MaximumCanonicalBytes)
	for index := range large.encoded {
		large.encoded[index] = byte(index)
	}
	large.retainedExecutionFacts.encodedBytes = uint64(len(large.encoded))
	large.retainedExecutionFacts.encodedDigest = sha256.Sum256(large.encoded)

	validate := func(snapshot Snapshot) {
		facts, valid := snapshot.ValidateRetainedExecutionAuthority(
			"tenant-allocation",
			"principal-allocation",
			"app-allocation",
			[]string{"main"},
		)
		retainedExecutionFactsSink = facts
		retainedExecutionValidSink = valid
	}
	validate(small)
	if !retainedExecutionValidSink {
		t.Fatal("small retained facts are invalid")
	}
	smallFacts := retainedExecutionFactsSink
	clonedFacts, clonedValid := small.Clone().ValidateRetainedExecutionAuthority(
		"tenant-allocation",
		"principal-allocation",
		"app-allocation",
		[]string{"main"},
	)
	if !clonedValid || clonedFacts != smallFacts {
		t.Fatalf("cloned retained facts = (%#v, %t), want exact", clonedFacts, clonedValid)
	}
	if _, valid := small.ValidateRetainedExecutionAuthority(
		"tenant-allocation",
		"principal-allocation",
		"app-allocation",
		[]string{"other"},
	); valid {
		t.Fatal("retained facts accepted changed expected indexes")
	}
	digestDrift := small
	digestDrift.digest[0] ^= 0xff
	if _, valid := digestDrift.ValidateRetainedExecutionAuthority(
		"tenant-allocation",
		"principal-allocation",
		"app-allocation",
		[]string{"main"},
	); valid {
		t.Fatal("retained facts accepted changed current snapshot digest")
	}
	validate(large)
	if !retainedExecutionValidSink {
		t.Fatal("synthetic maximum-encoding retained facts are invalid")
	}
	if small.retainedExecutionFacts.EncodedDigest() ==
		large.retainedExecutionFacts.EncodedDigest() {
		t.Fatal("synthetic maximum encoding did not rotate the fixed encoding digest")
	}

	smallAllocs := testing.AllocsPerRun(1_000, func() { validate(small) })
	largeAllocs := testing.AllocsPerRun(1_000, func() { validate(large) })
	if smallAllocs != 0 || largeAllocs != 0 {
		t.Fatalf(
			"retained facts allocations = small %.2f large %.2f, want 0/0",
			smallAllocs,
			largeAllocs,
		)
	}

	benchmark := func(snapshot Snapshot) testing.BenchmarkResult {
		return testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				validate(snapshot)
			}
		})
	}
	smallResult := benchmark(small)
	largeResult := benchmark(large)
	// BenchmarkResult.AllocedBytesPerOp is derived from runtime TotalAlloc.
	// Requiring equal zero bytes proves the 4 MiB retained encoding never enters
	// the per-validation allocation path.
	if smallResult.AllocsPerOp() != 0 || largeResult.AllocsPerOp() != 0 ||
		smallResult.AllocedBytesPerOp() != 0 ||
		largeResult.AllocedBytesPerOp() != 0 {
		t.Fatalf(
			"retained facts allocation bytes/count = small %d/%d large %d/%d, want 0/0",
			smallResult.AllocedBytesPerOp(),
			smallResult.AllocsPerOp(),
			largeResult.AllocedBytesPerOp(),
			largeResult.AllocsPerOp(),
		)
	}
}
