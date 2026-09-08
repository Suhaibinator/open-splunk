Final source measurement, following review-wave-2 fixes

Source: e30af276766772548db4b1d6b5dce87ffd94f10e
Baseline: fb9cef37499bcb3706312a46d70f70a2c304721c
Go: go1.27.1; darwin/arm64; Apple M4 Max.
benchstat: golang.org/x/perf v0.0.0-20260825160852-19be9d8e6c70.
Identical decoder_benchmark_test.go SHA-256 in both snapshots:
  ba4bfb038b6e68b18d34d108a97435f02727852d79ace2769f05039b2367db09
Both compiled test binaries have identical Go version, build settings and
module dependency versions. Compilation and construction stay outside decode
loops; no implementation/review agents ran during timing.

Each process uses:
  taskpolicy -a -l 0 -t 0 /usr/bin/env GOMAXPROCS=1 <test-binary>
    -test.run='^$' -test.benchmem -test.count=1
NDJSON: -test.bench='^BenchmarkNDJSONDecoder$' -test.benchtime=1s
20 accepted pairs, alternating baseline/candidate and candidate/baseline.
Native: -test.bench='^BenchmarkNative(Decoder|MixedInputs)$'
Compilation: -test.bench='^BenchmarkNativeJavaCompile$'
Timestamp: -test.bench='^BenchmarkTimestampOffsetPosition$'
These three cohorts each use -test.benchtime=200ms and 20 accepted samples.
The mixed-input operation is two events, as documented in the parent README.

An initial unmonitored attempt overlapped an unrelated storage.test process.
The entire attempt is retained under preliminary-contended and excluded.
The repeat monitors the process table every 0.5 seconds, waiting for two quiet
observations two seconds apart before each pair/sample. It detects Go build,
test and vet commands, compiler/linker processes, .test executables, the linter,
and common frontend build/test commands. A pair/sample with any observed
competing job is retained under rejected-workload and excluded in its entirety.
Selection is solely by observed workload, never result values. Monitor decisions
are retained in workload-monitor.jsonl. Ordinary desktop/system activity is
not changed; this is observation-based isolation, not dedicated hardware.

The accepted 1-second NDJSON set retains identical allocations and has no
significant regression, but canonical timing varies enough that it is not used
alone to declare the performance gate passed. Both baseline and candidate
shifted toward shorter execution times during the run. A predeclared follow-up
uses the same unchanged binaries and monitoring, 20 additional alternating
pairs per case with 2 seconds per case. Pairs are monitored and accepted per
case so unrelated short jobs need not invalidate other cases. A six-pair
whole-suite attempt was stopped solely because of repeated workload overlap,
before inspecting its timing values, and retained in longer/whole-suite-attempt.
The partial follow-up output is in longer/. It was stopped for the newly
reproduced timestamp literal-precision defect. Canonical completed all 20
pairs and found a significant 1.31% slowdown (p<.001) with unchanged allocations;
see longer/canonical-comparison.txt. Nested completed 17 pairs, high-fields
had not started. This source does not pass the performance gate. A subsequent
production change compiles decoder format classification once to remove the
additional per-record string comparisons; final measurements follow separately.
Earlier evidence and excluded runs remain available; nothing is overwritten.

Native and timestamp comparisons have narrow confidence ranges (mostly 1-2%).
Terminal Z remains allocation-free. Nonterminal timestamps use the bounded
original-spelling prefix parse and therefore have additional parse allocations.
The retained hostile scaling and profiles in the parent folder remain applicable
because the scanner implementations are unchanged.
