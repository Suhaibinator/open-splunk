# Build scripts

Protobuf generation, reproducible artifact builds, migration checks, and
packaging automation belong here. Artifact automation must build the Next.js
static export before compiling `open-splunk-server`.

`compile-protos.sh` implements `make proto`. Invoke the Make target so the public developer workflow remains stable.

`check-docs.mjs` implements `make docs-check`. It validates local Markdown
targets and heading anchors across the owned documentation set and rejects
retired pre-release version tokens, versioned protobuf/API/HEC identifiers,
release-era SPL rule IDs, versioned document names, and public version-floor
language. Negative-support fixtures outside the owned Markdown files remain
available to protocol tests without weakening the documentation contract.

The scalar-String `stats min`/`max` ClickHouse microbenchmark is an opt-in Go
benchmark because it starts a disposable container. It compares the production
guarded-Array and scalar-tuple SQL helpers over the same generated corpus and
reports client time plus server duration, average/peak memory, and rows read
from `system.query_log`:

```sh
OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
  go test ./internal/clickhouse -run '^$' \
  -bench '^BenchmarkStatsExtremaLowering$' -benchtime=7x -count=1 -v
```

The default corpus has 2,000,000 rows and uses the repository-pinned
ClickHouse image. Set `OPEN_SPLUNK_STATS_EXTREMA_BENCH_ROWS` for a deliberate
corpus-size change or `OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` for an explicit image
comparison. This is deliberately a helper-level lowering microbenchmark;
compiler alias/state reuse and downstream key cleanup are covered separately
by compiler assertions and pinned `EXPLAIN` tests. Benchmark output is
evidence, not a timing gate.

`style-invariants.test.mjs` is every structural invariant of the styling layer,
in one file of nine sections: the token layer and its naming grammar, the
literal ledger, one entry point and one cascade order, where a responsive rule
lives, one implementation of each primitive, class reachability (total from
rule to markup, and from markup back to rule only for the five colocated feature
prefixes `FEATURE_PREFIXES` lists), and pins on the parsers underneath. None of
it is visible to a compiler, a lint count or a screenshot.
[Theming](../docs/theming.md#guardrails-what-holds-this-in-place) describes each
section and what it can see.

`style-inventory.mjs` does all the reading and parsing, and is a library rather
than a test file so the suite can assert on stylesheet structure without opening
a stylesheet itself — which is one of the invariants it enforces.

`style-guardrails.test.mjs` guards the guardrails. Half of it pins the wiring —
that `npm run lint` still chains `lint:css` over the whole layer, that no rule
in `.stylelintrc.json` is set to null or downgraded by a `defaultSeverity`, that
the two documented exemptions name exactly the files they document, and that CI
still runs all three gates — because unwiring any of those makes the phase bar
false while every other test stays green. The other half covers the four
spellings a value can take where the property that names it never appears: a
size or a face inside the `font` shorthand, a step inside an `@container` query,
a colour keyword inside another function's parentheses, and a colour in an
inline `style`, which is the one place that outranks the whole stylesheet layer.

Three JSON ledgers record what the suite deliberately allows, each compared
against the tree in both directions so that paying a row off fails as loudly as
adding an unrecorded one:

- `css-literal-debt.json` — every colour and scale literal outside the token
  layer.
- `css-retired-classes.json` — the classes the consolidation deleted and the
  primitive that replaced each. Nothing in the toolchain reports an unmatched
  class, so this is the only place a deletion that outran its markup shows up.
- `css-dynamic-classes.json` — global classes that only ever exist at runtime.

Repeated declaration blocks have no ledger or exemption: four or more
identical declarations in the same at-rule context must be consolidated.

`safety-net.test.mjs` checks that the frontend safety net cannot stop running
unnoticed: every unit test file is named in this directory's hardcoded runner
list, and every listed test file still exists.
