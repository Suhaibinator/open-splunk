# Backend vertical integration

## Development workflow smoke test

The opt-in development workflow gate generates a Git-independent development
environment on random loopback ports, starts one pinned plaintext ClickHouse
container but no Open Splunk server, builds
the current tree as `development`, reaches readiness with the host-native
server, and proves its private control state survives a graceful restart:

```sh
OPEN_SPLUNK_DEVELOPMENT_INTEGRATION=1 \
  go test ./integration -run '^TestDevelopmentWorkflow$' -count=1 -timeout=10m -v
```

It uses a unique Compose project and deletes only its own temporary volumes and
host files during cleanup.

`backend_vertical_test.go` exercises the real deployable path:

1. build the static UI in backend mode;
2. start pinned, ephemeral ClickHouse on a random loopback port;
3. build and start `open-splunk-server` from an empty working directory with
   temporary SQLite/key files and no executable runtime on `PATH`;
4. provision an index, app-scoped immutable lookup, saved lookup search, and
   one-time ingestion token over protobuf HTTP;
5. build and start `open-splunk-collector` against an empty `app.log`, append
   and durably acknowledge one primer event, then hard-kill the collector;
6. inspect the valid checkpoint/WAL crash boundary, restart the collector with
   the same state, append two generated events, and require their durable
   checkpoint and WAL acknowledgment high-water to settle;
7. hard-kill the server, append and fsync a final sentinel while it is down,
   wait for and explicitly sync the real WAL segment append, then hard-kill the
   collector;
8. reopen the WAL and require the exact pending sentinel origin, restart both
   processes with their original durable state, reload the exact app, lookup,
   and saved-search versions, and prove four distinct stable event IDs with no
   loss. One physical sentinel replay is allowed by the at-least-once contract,
   and search uses `dedup event_id` to return the four logical events;
9. require the final exact line/byte checkpoint and a drained collector WAL
   with a positive acknowledged sequence;
10. execute the persisted lookup search through protobuf HTTP with saved-search
    provenance, and subscribe to its binary protobuf WebSocket stream;
11. require an explicit subscription acknowledgment followed by monotonically
   sequenced search state/progress events and a completed terminal event, then
   fetch the authoritative typed/redacted lookup-enriched results over two
   opaque cursor pages, and verify the pinned lookup identity through
   administrator inspection;
12. launch Chromium against the UI embedded in that compiled server, run an SPL
   lookup search, observe its same-origin protobuf HTTP and binary WebSocket
   traffic, and verify the expanded final event rows contain the typed lookup
   enrichment;
13. create and poll a JSON Lines export, redeem its one-time bearer grant over
   the raw download route, validate lookup-enriched artifact headers/content,
   and reject grant replay;
14. provision a separate `gradethis` index and index-scoped token, run the
   shipped `configs/examples/collector.yaml` through `collector validate`, and
   start that exact configuration against an empty synthetic GradeThis log;
15. append and fsync the scanner-validated 20-event current GradeThis profile,
   then require exact distinct ClickHouse rows, trusted
   host/source/sourcetype/service/environment metadata, an EOF checkpoint, a
   drained acknowledged WAL, and no token leakage. The profile is rebased to a
   recent UTC window so maximum-event-age and logical-retention policy remain
   active; its default form remains byte-pinned in unit tests;
16. execute six current-source SPL investigations through protobuf HTTP with
   exact typed cursor-paged results: trace flow, severity counts, failed
   requests, path/status counts, mixed Go duration units (`µs`, `ms`, `s`), and
   top messages. This path starts no OpenTelemetry collector;
17. insert a deterministic 10,001-row fixture into a separately provisioned
   index, prove the interactive snapshot reports its 10,000-row truncation,
   and prove bounded export re-execution downloads all 10,001 unique rows.

The WebSocket subscription uses `after_sequence = 0`, so the test accepts the
current terminal snapshot when the small search finishes before the upgrade.
WebSocket frames carry only bounded notifications; full result rows are always
retrieved from the authoritative protobuf HTTP results endpoint.

Install the pinned Chromium build once, then run the test explicitly because
Docker, browser automation, and binary builds are intentionally excluded from
the default unit-test loop:

```sh
npm ci
npx --no-install playwright install chromium
OPEN_SPLUNK_BACKEND_INTEGRATION=1 go test ./integration -run '^TestBackendVertical$' -count=1 -v
```

Set `OPEN_SPLUNK_BROWSER_EXECUTABLE` to use a specific Chromium-family browser
instead of Playwright's pinned download.

The default image is
`clickhouse/clickhouse-server:26.7.5.10-alpine@sha256:0a45b864c73322d4360dea1973ee9b77f29c51af1242ad2d47409908071fa56e`.
Set `OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` to exercise another digest-pinned image
deliberately.

## Sustained native ingestion and search load

`backend_load_test.go` builds the browser/server, collector, and log generator,
then admits a deterministic 30,000-event source at 1,000 events per second. It
proves an acknowledged warm boundary, a durable WAL backlog while the server is
down, collector crash/restart with that backlog, recovery while source
generation continues, and concurrent bounded searches over monotonically
advancing visibility snapshots. Final checks require exact source, raw-row,
cardinality, checkpoint, WAL-drain, dead-letter, and secret-redaction results.
Throughput, outage/recovery, storage, and search-latency measurements are logged
as observational evidence rather than portable timing promises.

Run this resource-intensive gate explicitly:

```sh
OPEN_SPLUNK_BACKEND_LOAD=1 \
  go test ./integration -run '^TestBackendSustainedLoad$' \
    -count=1 -timeout=12m -v
```

It starts the repository-pinned ClickHouse image unless
`OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` deliberately selects another digest-pinned
image.

## Browser stream recovery and cancellation

Five deterministic shipped-browser gates exercise expired retained sequences,
live sequence gaps, REST-terminal and REST-first recovery, and authoritative
search cancellation without reconnect. They run the real compiled browser
against a bounded test server and write failure diagnostics beneath the
corresponding `test-results/browser-*` directory.

After installing the pinned Chromium build described above, run the complete
family with:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
  go test ./integration \
    -run '^TestBrowser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery))$' \
    -count=1 -timeout=20m -v
```

## HEC ingestion-to-SPL vertical

`backend_hec_test.go` builds the shipped server, enables the default-off HEC
surface, provisions an administrator-created HEC token, and sends concatenated
JSON and raw events over TLS. It pauses ClickHouse to prove ACKs remain pending,
then verifies exact typed/provenance projection, public SPL base search,
grouped `stats`, and `timechart`. A hard server restart proves retained ACK
state; an independent retry proves documented at-least-once behavior without
assuming ACK IDs are sequential. The test also checks logs and audit output for
credential leakage.

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
  go test ./integration -run '^TestBackendHECVertical$' -count=1 -timeout=8m -v
```

## ClickHouse migration integration

The opt-in migration gate starts the pinned ClickHouse image, connects over
its plaintext native listener with one account, applies the same embedded
migrations used by normal server startup twice, and verifies the migration
ledger remains idempotent:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
  go test ./migrations/clickhouse -run '^TestMigrationsAgainstClickHouse$' \
    -count=1 -timeout=8m -v
```

## Physical index deletion API lifecycle

`backend_index_data_deletion_test.go` exercises administrator `DELETE_DATA`
admission through the compiled server and its real SQLite/ClickHouse
dependencies. It rejects malformed, unauthorized, missing, stale, active, and
misconfirmed requests without durable or physical effects; then issues 16
concurrent exact admissions and requires one stable operation and one
correlated ClickHouse mutation.

The test uses `SYSTEM STOP MERGES` to hold that mutation deterministically,
hard-kills the server, reopens SQLite to prove the pending operation and
mutation attempt are durable, restarts the server, and retries through the
normalized name selector. After merges resume, only the trusted tenant/index
rows disappear: same-name rows for a foreign tenant and neighboring-index rows
for the trusted tenant survive. A terminal hard stop followed by repeated
SQLite reopen verifies the immutable completion audit, consumed work records,
retained deleting generation, tombstone, and permanent name reservation.
Stable operation-ID replay is intentionally bounded to the outstanding
operation: once terminal completion hides the catalog entry, the exact same
HTTP request returns `404 Not Found`.

The ClickHouse image must include an exact `@sha256:` digest:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
  go test ./integration -run '^TestBackendIndexDataDeletionLifecycle$' -count=1 -v
```

## Fixed-result browser rendering

`browser_rendering_test.go` isolates browser rendering from ClickHouse and
query latency. It runs the compiled backend UI and production protobuf HTTP
handler against a deterministic executor that returns exactly 1,000 statistics
rows at the browser's 64-column boundary. The test verifies the decoded
response, exact total and page contract, bounded virtualized rows and cells at
the first and last rows, fixed table width, ARIA row positions, sorting, table
density changes, and browser/API safety.
The browser rejects a result page that exceeds its requested row count or the
explicit 64-column interactive-table limit before adapting or rendering it.

Run it with the same pinned Chromium installation:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 go test ./integration -run '^TestBrowserFixedResultRendering$' -count=1 -v
```

The test waits for semantic completion, a mutation-free turn, and two animation
frames before recording rendering measurements. Durations, long tasks, layout
work, heap use, and DOM counts are observational metrics, not machine-dependent
latency acceptance thresholds. Exact payload semantics and the maximum number
of materialized rows remain correctness gates. Metrics and top/bottom
screenshots are written beneath
`test-results/browser-fixed-result-rendering/visual`.

## Visual regression baselines

`integration/visual` pins the appearance of the shipped CSS. It needs no Go
server, ClickHouse, or Docker: `scripts/build-visual-exports.mjs` produces the
static export twice, `scripts/serve-static.mjs` serves both from one
dependency-free Node process, and `playwright.visual.config.ts` drives Chromium
against them at 1440x900 and 760x1000.

Both exports land in `.cache/visual/demo-export` and
`.cache/visual/backend-export`, and `out/` is reset to the state Git tracks
afterwards. Next always exports into `out/`, which is the release payload
`webui.go` embeds, so leaving a demo-mode build there would let `go build` embed
a UI with no `out/asset-manifest.json`. Run `make build-ui` before building the
server after any visual run.

Nearly every surface renders from the demo data mode, whose fixtures are
compiled into the bundle. Only the bootstrap-advertised Knowledge Manager needs
the backend-mode export, and `knowledge-manager.visual.spec.ts` supplies its
protobuf responses through request interception.

A rule whose element renders in neither export is invisible to every page
baseline. `component-surfaces.visual.spec.ts` covers that gap by mounting the
production markup against the real stylesheet and photographing the component
alone; the statistics sparkline lives there because it needs a server-supplied
multivalue column that no fixture produces.

```sh
npm ci
npx --no-install playwright install chromium
npm run test:visual
```

Baselines live in `integration/visual/__screenshots__/<platform>/<project>/`
and are committed. They are platform-specific because font rasterization is:
a machine whose platform has no directory there must generate its own set.

Only a `darwin` set is committed today, so `npm run test:visual` is a
developer-local gate: the `ubuntu-latest` CI runner has no baselines to compare
against. Committing a `linux` set generated in the runner's own container is the
work that puts this half of the net under CI. `npm run test:contracts` below has
no such constraint and does run in CI.

The suite fails on a layout change but tolerates
`maxDiffPixelRatio: 0.002`, so normalizing a color by one or two RGB units
still passes. Update baselines only when a visual change is intended, review
the regenerated PNGs, and describe the change in the commit body:

```sh
npm run test:visual -- --update-snapshots
```

Failure artifacts (actual, expected, and diff images) are written beneath
`test-results/visual`.

## Global stylesheet computed-style contracts

`visual/css-contracts.spec.ts` pins the behaviour of the application stylesheets that
unit tests used to pin by matching the stylesheet's raw text. Text matching
tied the suite to formatting — newline placement, single-line media-query
bodies, declaration order — so any tokenising or reformatting pass broke it
without changing a rendered pixel. The spec instead loads the stylesheets into
Chromium against fixture markup that mirrors the production DOM and reads
resolved values through `getComputedStyle` at the 1280, 980, 760, and 480 pixel
breakpoints.

"The stylesheets", plural: `visual/application-stylesheets.ts` injects each
file `app/styles/index.css` imports, in that file's order, because `setContent`
cannot resolve an `@import` inside an injected `<style>` and would otherwise
render every `var()` as its fallback and every rule as nothing at all. The list
is read out of `index.css` rather than restated, and
`scripts/token-layer.test.mjs` asserts that it still is — and that every
stylesheet under `app/` is in it. `visual/token-layer.visual.spec.ts` covers
the other half — it navigates to the real export rather than injecting
anything, so a token file that never reaches `app/layout.tsx` fails there even
while every contract here is green.

The colocated feature stylesheets ride along in that list. The fixtures here
mount the shared primitives, and every feature class carries its own prefix, so
a feature file contributes rules no fixture markup can match; injecting them
anyway is what keeps the list derivable from `index.css` instead of hand-picked,
and the pages that do use them are covered by the screenshots.

It needs no server, no container, no backend fixtures, and no committed
baselines, so it is platform-independent, runs in under a second, and is the one
half of this phase's net that the CI `frontend` job enforces on every push:

```sh
npx --no-install playwright install chromium
npm run test:contracts
```

Failure artifacts are written beneath `test-results/css-contracts`.

## Screenshot determinism

A committed baseline only pins appearance if the page paints the same thing
every run. The tolerance that keeps antialiasing from turning the suite red
also hides a surface that samples the wall clock, a random value, or an
animation phase, and a baseline set that has quietly stopped describing a fixed
rendering is a safety net with a hole in it.

Two gates close that hole. `visual/screenshot-determinism.visual.spec.ts` runs
with the rest of `npm run test:visual` and photographs a handful of
representative surfaces twice — back to back, and again after a reload —
comparing the two renderings instead of a committed file. It ignores
per-channel differences of one or two units, because Chromium's text rasterizer
is not bit-reproducible, and one of its cases deliberately changes the page to
prove the comparison can still tell two renderings apart.

The second gate covers the whole suite:

```sh
npm run test:visual:determinism
```

That builds the exports once, serves them once, and runs every visual spec
twice over that single build: the first pass records each screenshot into a
temporary directory and the second compares against it with `maxDiffPixels: 0`.
Because both passes render one build on one machine, a surface that moves has
nowhere to hide behind a ratio. Add `--skip-build` to reuse the exports already
in `.cache/visual` while iterating on a spec.

When it fails, pin whatever varies rather than widening a tolerance.

## Stylesheet structural invariants

`scripts/css-invariants.test.mjs` runs inside `npm run test:frontend` and keeps
the properties this phase established true:

- no test file may read a stylesheet's characters, because appearance is pinned
  by screenshots and behaviour by the computed-style contracts above;
- every `var(--x)` in every stylesheet must resolve to a custom property some
  stylesheet declares or some component sets at runtime;
- every class an application stylesheet writes rules for must be reachable from a
  literal `className` or an interpolation base;
- the walker must reach the colocated feature stylesheets, and no `.module.css`
  may come back: a CSS module's classes are scoped to a generated hash, so none
  of the invariants above can see them at all.

The parsing lives in `scripts/css-inventory.mjs` so the test file itself never
opens a stylesheet. A class that genuinely only exists at runtime belongs in
`scripts/css-dynamic-classes.json`, with a comment naming the code that
produces it; an entry there that stops being needed fails the suite, so the
list cannot become a quiet home for dead CSS.

`scripts/safety-net.test.mjs` guards the net itself: every unit test file must
appear in the hardcoded list in `scripts/test-frontend.mjs`, and every
screenshot a visual spec pins must have a committed baseline in every project,
with no baseline left behind by a deleted test.
