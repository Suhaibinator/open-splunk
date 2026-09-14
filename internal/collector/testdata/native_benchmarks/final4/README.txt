Issue #104 frozen-source dispatch comparison

Baseline production source: fb9cef37499bcb3706312a46d70f70a2c304721c
Unoptimized production source: 7dc11bfba3035cec6ba57c298c8faf0b8cc9b860
Optimized production source: 16b3c3a7d4b818d0b864c7154b7790168f978a8b
Host: Apple M4 Max, darwin/arm64, Go 1.27.1.
benchstat: golang.org/x/perf v0.0.0-20260825160852-19be9d8e6c70.

NDJSON uses separate complete source snapshots, with collector test files
replaced IN THOSE TEMPORARY SNAPSHOTS ONLY by the identical benchmark harness.
This controls additional test registration and linker metadata; all repository
correctness tests remain intact and passed separately on the optimized source.
Harness decoder_benchmark_test.go SHA256:
ba4bfb038b6e68b18d34d108a97435f02727852d79ace2769f05039b2367db09

Each snapshot is compiled with go test -c. Decoder construction is outside the
timed loop. Canonical uses baseline/unoptimized/optimized triples; nested and
high_fields use baseline/optimized pairs. Order reverses each accepted sample.
Each case has 20 accepted samples, 2 seconds per timed benchmark:
  taskpolicy -a -l 0 -t 0 env GOMAXPROCS=1 <binary>
    -test.run='^$' -test.bench='^BenchmarkNDJSONDecoder$/^<case>$'
    -test.benchmem -test.benchtime=2s -test.count=1

No implementation/review agents, builds, integration jobs or fuzzers belonging
to this task ran during measurements. The workload monitor checks processes
every 0.5s for Go/frontend builds/tests/lint and waits for two quiet observations
2s apart before each sample group. An observed competing workload excludes the
ENTIRE pair/triple; original output is retained under rejected-workload/.
Selection is based solely on observed workload, never on the measured numbers.
workload-monitor.jsonl records every accepted/rejected attempt and wait.
Ordinary desktop background activity is not disabled. This does not guarantee
an otherwise perfectly idle operating system or generalize to every host.

before.txt/after.txt concatenate the accepted cases. Reproduce with:
  benchstat before.txt after.txt
  benchstat canonical-before.txt canonical-unoptimized.txt canonical-after.txt
The latter diagnostic tests the suspected extra per-event string comparison;
optimized NDJSON now enters its original JSON body after one integer branch.
The earlier significant slowdown and all inconclusive/partial measurements
remain in ../final3/ and are not claimed as a passing performance gate.

Native and mixed benchmarks use the full optimized collector test binary;
Java compilation is measured separately. Timestamp benchmarks use the full
optimized parserconfig test binary. These use 20 accepted 200ms samples and
the same workload monitoring. MixedInputs reports a pair of events per op.
Timestamp literal_runs exercises the new optional lexical precision walk at
increasing sizes. Historical unchanged hostile scanner scaling/profiles remain
in the parent directory. Comparison results are recorded after completion.

Diagnostic interpretation: the controlled canonical triple reports no significant
slowdown for either the unoptimized or optimized production snapshot. Therefore
the earlier full-test-binary 1.31% difference cannot be attributed conclusively
to the added string comparison alone; changed test/linker metadata is also a
confound. Integer classification removes that per-event comparison by inspection,
but these timings do not claim a statistically proven speedup from that change.
The final gate compares baseline and final production code with identical test
registration, rather than treating the old full-test-binary failure as a pass.

Completed NDJSON gate: 20 samples/case, identical allocation counts and bytes
in every sample. Canonical 1.608us -> 1.605us (p=.273), nested 3.110us -> 3.112us
(p=.324), high_fields 252.4us -> 250.9us (-.57%, p=.038). Reported confidence
ranges are 0-1%; no statistically significant throughput regression. Exact
allocs/op 55/129/6369 and B/op 2464/4808/439696 remain unchanged.

Supporting measurements completed: 20 samples each, native decoder ranges
0-1%, Java compilation 1%, timestamp cases 1-2%. The new literal precision
walk allocates zero bytes in every measured case: 0/1/64/1024 literal runs
53.28ns/162.4ns/1.733us/25.53us. Scaling is consistent with linear work.
All commands completed successfully. No code changes occurred during this run.
