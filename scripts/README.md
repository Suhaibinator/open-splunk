# Build scripts

Protobuf generation, reproducible artifact builds, migration checks, and
packaging automation belong here. Artifact automation must build the Next.js
static export before compiling `open-splunk-server`.

`compile-protos.sh` implements `make proto`. Invoke the Make target so the public developer workflow remains stable.

`build-visual-exports.mjs` and `serve-static.mjs` support `npm run test:visual`.
The first builds the static export in both the demo and backend data modes,
moves each build into `.cache/visual`, and resets `out/` to the state Git
tracks, because `out/` is the release payload `webui.go` embeds and a test
target must not leave a manifest-less demo build in it. The second serves those
directories over loopback with no dependencies, so the visual-regression
baselines need neither the Go server nor ClickHouse. See
[the integration guide](../integration/README.md#visual-regression-baselines).

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
in one file of ten sections: the token layer and its naming grammar, the literal
ledger, one entry point and one cascade order, parity with the stylesheet the
split replaced, where a responsive rule lives, one implementation of each
primitive, class reachability in both directions, and pins on the parsers
underneath. None of it is visible to a compiler, a lint count or a screenshot.
[Theming](../docs/theming.md#guardrails-what-holds-this-in-place) describes each
section and what it can see.

`style-inventory.mjs` does all the reading and parsing, and is a library rather
than a test file so the suite can assert on stylesheet structure without opening
a stylesheet itself — which is one of the invariants it enforces.

`style-guardrails.test.mjs` guards the guardrails. Half of it pins the wiring —
that `npm run lint` still chains `lint:css` over the whole layer, that no rule
in `.stylelintrc.json` is set to null or downgraded by a `defaultSeverity`, that
the two documented exemptions name exactly the files they document, and that CI
still runs all four gates — because unwiring any of those makes the phase bar
false while every other test stays green. The other half covers the four
spellings a value can take where the property that names it never appears: a
size or a face inside the `font` shorthand, a step inside an `@container` query,
a colour keyword inside another function's parentheses, and a colour in an
inline `style`, which is the one place that outranks the whole stylesheet layer.

Five JSON ledgers record what the suite deliberately allows, each compared
against the tree in both directions so that paying a row off fails as loudly as
adding an unrecorded one:

- `css-literal-debt.json` — every colour and scale literal outside the token
  layer.
- `css-retired-classes.json` — the classes the consolidation deleted and the
  primitive that replaced each. Nothing in the toolchain reports an unmatched
  class, so this is the only place a deletion that outran its markup shows up.
- `css-duplicate-blocks.json` — the restatements deliberately left, each with
  the primitive that would otherwise own it; an entry goes stale the moment
  either side of the duplication changes.
- `css-dynamic-classes.json` — global classes that only ever exist at runtime.
- `css-phase3-monolith.json` — the rule set the single application stylesheet
  stated at the commit before it was split, so the move is checked rather than
  believed: a rule dropped, copied or edited on its way out fails with its own
  text. It is a one-phase provenance proof and should be deleted with the tests
  that read it once a later phase rewrites those rules on purpose.

`safety-net.test.mjs` checks that the Phase 0 safety net cannot stop running
unnoticed: every unit test file is named in this directory's hardcoded runner
list, and every screenshot a visual spec pins has a committed baseline under
every viewport project that records it.
`visual-baseline-projects.json` records the captures that exist at one viewport
only — the mobile navigation drawer is not rendered above 760px at all — and is
itself checked, so an entry naming a missing project or an unpinned screenshot
fails the same test.

`visual-determinism.mjs` implements `npm run test:visual:determinism`. It
builds the exports once, serves them itself, and runs the visual suite twice
over that single build, comparing the two passes with no pixel budget. See
[the integration guide](../integration/README.md#screenshot-determinism).
