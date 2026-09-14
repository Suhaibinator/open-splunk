Final comparison after wave 1 fix and merge of updated main
Baseline: fb9cef37499bcb3706312a46d70f70a2c304721c
Candidate: cade2f893c55dc75f67a855d215165600192401a
Go go1.27.1, darwin/arm64, Apple M4 Max. Same benchstat version as parent.
Both isolated go test -c binaries use the identical decoder_benchmark_test.go.

GOMAXPROCS=1 taskpolicy -a -l 0 -t 0 <binary> -test.run='^$'
  -test.bench='^BenchmarkNDJSONDecoder$' -test.benchmem
  -test.benchtime=1s -test.count=1
20 pairs, alternating baseline/candidate and candidate/baseline order.
No implementation/review agents, builds, tests, lint or fuzz work ran during
sampling. Process snapshots before and during sampling confirmed only the
benchmark test binary was active among compilation/test/lint processes.
Compilation and decoder construction stay outside timed decoding loops.

Every allocation sample is identical to baseline. No statistically significant
throughput regression: canonical p=.525, nested p=.560, high fields p=.383
(sec/op comparison; n=20). Keep confidence ranges and all samples; these results
are host measurements, not universal performance guarantees.

compile.txt repeats both Java constructor benchmarks with the same invocation,
-test.bench='^BenchmarkNativeJavaCompile$', -test.benchtime=200ms -test.count=20.
The YAML presence fix affects startup validation; native per-event scanners
are identical to the previously profiled/timed candidate in the parent folder.
