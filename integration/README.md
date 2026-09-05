# Backend vertical integration

## Dashboard and shared control browser tests

Run `npm run test:dashboards` to exercise backend-mode dashboard onboarding,
app creation, app switching, error recovery, and the editor against mocked
protobuf responses. Its isolated Next development output lives in
`.cache/dashboard-next`; no running backend or Docker is required.

Run `npm run build && npm run test:workspace` for the demo export, including
the shared dropdown's filtering, keyboard, and nested-dialog behavior.
`npm run test:contracts` checks themed popup geometry and padded state cards
at desktop and mobile widths. Install the pinned browser with
`npx --no-install playwright install chromium` before these suites.

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
   traffic, confirm the final event rows land collapsed, expand the page from
   the Rows menu, and verify the expanded rows contain the typed lookup
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
as observational evidence rather than portable timing promises. The gate also
queries ClickHouse's system logs and parts after the final drain to report
physical inserts, rows-per-insert distribution, insert delays/rejections, and
active-part growth. These measurements contain no tenant, index, batch, group,
channel, token, payload, or error-text labels.

Run this resource-intensive gate explicitly:

```sh
OPEN_SPLUNK_BACKEND_LOAD=1 \
  go test ./integration -run '^TestBackendSustainedLoad$' \
    -count=1 -timeout=12m -v
```

It starts the repository-pinned ClickHouse image unless
`OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE` deliberately selects another digest-pinned
image.

Physical insert coalescing has a separate concurrent HEC qualification gate.
It excludes startup, outage, recovery, and final drain from the measured
window and enforces the documented median, coverage, hard-limit, and physical-
to-logical ratio thresholds:

```sh
OPEN_SPLUNK_HEC_QUALIFIED_LOAD=1 \
  go test ./integration -run '^TestBackendHECQualifiedLoad$' \
    -count=1 -timeout=15m -v
```

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

## Instance palette browser smoke

`palette_smoke_test.go` proves the two halves of the administrator-selectable
palette meet without Docker, ClickHouse, or a compiled server binary. It
stages and builds the backend-mode static export, opens a real SQLite control
plane in a temporary directory with the real audit journal and the real
`server_search_settings` / `server_appearance_settings` singleton stores, and
mounts the production browser API handler over that export on a random
loopback port. The search-job manager is backed by a fixed executor that
returns no rows: nothing ClickHouse-shaped is exercised, and nothing about the
palette needs it.

`palette-smoke/instance-palette.spec.ts`, run through
`playwright.palette-smoke.config.ts`, then drives Chromium against that origin
with an administrator bearer the Go test minted:

1. a fresh browser loads `/signin/`, paints `data-palette="classic"`, and
   caches it once `/api/system/bootstrap` answers;
2. the administrator updates the palette to `terminal`; the next load paints
   `terminal` after hydration and the cache follows;
3. with the bootstrap route blocked, the next load paints `terminal` from the
   cache before any network response, which an init-script mutation observer
   records as the first `data-palette` write, together with whether `<body>`
   existed yet: only the inline `<head>` boot script writes before `<body>`
   is parsed, so `beforeBody: true` is what separates the boot script from
   ThemeSync's mount-time repaint (deleting the boot script from
   `app/layout.tsx` fails this step and every other first-paint check);
4. the user picks Dark from the user menu; both attributes hold, and the boot
   script paints both on a reload with bootstrap blocked again;
5. the administrator updates to `glass`; the search workspace paints it and
   the user menu's `.floating-menu` computes a translucent background at or
   above the documented 80% floor with a `blur()` backdrop filter;
6. a raw protobuf body carrying an out-of-range `UiPalette` number is
   rejected with `400`, a stale `expected_version` with `409`, and the page
   still paints `glass`.

Back in Go, the durable singleton must read version 2 `glass`, agree with the
live snapshot bootstrap serves, and the audit journal must hold exactly two
`server_settings.update` events for `ui-palette`.

Install the pinned Chromium build once, then run the gate explicitly:

```sh
npm ci
npx --no-install playwright install chromium
OPEN_SPLUNK_PALETTE_SMOKE=1 \
  go test ./integration -run '^TestBrowserInstancePaletteSmoke$' -count=1 -timeout=10m -v
```

`OPEN_SPLUNK_BROWSER_EXECUTABLE` selects another Chromium-family browser, as
for the other browser gates. Failure artifacts are written beneath
`test-results/palette-smoke`. What this gate does not prove is the compiled
`open-splunk-server` wiring of the same stores into its runtime settings
object: the smoke restates that wiring over a test settings object.
`TestBackendVertical` (`OPEN_SPLUNK_BACKEND_INTEGRATION=1`, Docker) covers the
binary's own through `assertInstancePaletteRoundTrip`: a fresh control
database bootstraps `classic`, an administrator update lands `ocean` at
version 1, the unauthenticated bootstrap and a fresh authenticated read both
serve it, and after the server restart the reloaded snapshot still does.

## Global stylesheet computed-style contracts

`style-contracts/css-contracts.spec.ts` pins the behaviour of the application stylesheets that
unit tests used to pin by matching the stylesheet's raw text. Text matching
tied the suite to formatting — newline placement, single-line media-query
bodies, declaration order — so any tokenising or reformatting pass broke it
without changing a rendered pixel. The spec instead loads the stylesheets into
Chromium against fixture markup that mirrors the production DOM and reads
resolved values through `getComputedStyle` at the 1280, 980, 760, and 480 pixel
breakpoints, plus interior widths at 1000, 900, 500, and 450 pixels. This checks
both the canonical boundary and a representative point inside each responsive
band.

"The stylesheets", plural: `style-contracts/application-stylesheets.ts` injects each
file `app/styles/index.css` imports, in that file's order, because `setContent`
cannot resolve an `@import` inside an injected `<style>` and would otherwise
render every `var()` as its fallback and every rule as nothing at all. The list
is read out of `index.css` rather than restated, and
`scripts/style-invariants.test.mjs` asserts that it still is — and that every
stylesheet under `app/` is in it. The assertion runs the harness's own
`importedStylesheets` body and compares its result with `index.css`, rather than
implementing a second parser that could agree with itself while omitting a
shipped stylesheet.

The colocated feature stylesheets ride along in that list. The fixtures here
mount the shared primitives, and every feature class carries its own prefix, so
a feature file contributes rules no fixture markup can match; injecting them
anyway is what keeps the list derivable from `index.css` instead of hand-picked.

It needs no server, no container, and no backend fixtures, so it is
platform-independent, runs in under a second, and is enforced by the CI
`frontend` job on every push:

```sh
npx --no-install playwright install chromium
npm run test:contracts
```

Failure artifacts are written beneath `test-results/css-contracts`.

## Search workspace behaviour in the demo export

`workspace-behaviour/search-workspace.spec.ts` drives the search workspace
with real keyboard and pointer input in Chromium: the SPL completion menu
(Ctrl+Space, arrows, Enter, Escape, and reopening on a typed pipe), running a
search with Ctrl+Enter, choosing a preset in the time picker, and keyboard
navigation across the result tabs. The decisions behind the editor's key
handling live in `lib/search/spl-editor-interaction.ts` and are unit tested;
this suite checks that the workspace wires them, together with the time picker
and the tab list, to the input users actually send.

It runs against the demo-mode static export — the same files the Go binary
embeds — served by `scripts/serve-static-ui.mjs`, a `node:http` server that
maps `/search/` onto `out/search/index.html` the way the embedded file server
does. No backend, container, or fixture is involved, so it is
platform-independent and enforced by the CI `frontend` job after the export is
built:

```sh
npx --no-install playwright install chromium
npm run build
npm run test:workspace
```

The tests are `.spec.ts` on purpose: `scripts/test-frontend.mjs` runs `.test.ts`
files under node's test runner, where a Playwright test cannot run. Failure
artifacts are written beneath `test-results/workspace-behaviour`.

## Stylesheet structural invariants

`scripts/style-invariants.test.mjs` runs inside `npm run test:frontend` and
holds every structural property of the styling layer that the contracts above
cannot see. It is one file of structural tests in nine sections,
described in full under
[Guardrails](../docs/theming.md#guardrails-what-holds-this-in-place). In short:

- **Reach.** One test asserts every walk the file depends on is populated, so
  nothing below can pass by having nothing to look at.
- **The token layer.** One declaration site per name, no colour literal inside
  a semantic token, a dark block that redefines only names the light block
  declares, and no stylesheet outside `app/styles/tokens-*.css` declaring a
  token of its own.
- **The naming grammar.** Every name parses under the documented grammar, a
  step number really says how light a primitive is, a name family holds one
  kind of value, and every mandated text pairing keeps WCAG AA.
- **The literal sweep.** Colour and scale literals outside the token layer must
  match `scripts/css-literal-debt.json` exactly, in both directions.
- **The stylesheet set.** `app/styles/index.css` imports every application
  stylesheet exactly once, `app/layout.tsx` is the only file that pulls a
  stylesheet in, no `.module.css` or `:global()` comes back, no test file reads
  a stylesheet's characters, and the load order is the one documented.
- **Responsive ownership, one-of-each-primitive, and reachability** — every
  rule still has a caller, in every global stylesheet. The other
  direction is narrower: a class the markup asks for is checked against the
  stylesheets only when it carries one of the six registered feature prefixes
  (`alerts-`, `analytics-`, `operations-`, `reports-`, `visualization-`,
  `workspace-dialog-`). [Theming](../docs/theming.md#guardrails-what-holds-this-in-place)
  explains the required registration rule.
- **The parsers underneath**, pinned against the shapes that have already
  fooled a simpler implementation.

The reading and parsing live in `scripts/style-inventory.mjs` so the test file
itself never opens a stylesheet — which is the first invariant it asserts. A
class that genuinely only exists at runtime belongs in
`scripts/css-dynamic-classes.json`, with a comment naming the code that
produces it; an entry there that stops being needed fails the suite, so the
list cannot become a quiet home for dead CSS. The same is true of the other
ledgers the suite reads — `css-retired-classes.json` and
`css-literal-debt.json` — each of which is compared against the tree in both
directions. Repeated declaration blocks have no ledger: the suite requires
them to be consolidated.

`scripts/safety-net.test.mjs` guards the net itself: every unit test file must
appear in the hardcoded list in `scripts/test-frontend.mjs`, and every listed
test file must still exist.
