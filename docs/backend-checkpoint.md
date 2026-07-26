# Backend checkpoint handoff

This is the canonical restart document for backend work. Read it together
with:

- `docs/product-architecture-plan.md`
- `docs/spl-compatibility-v0.1.md`
- the latest `main` commit

## Latest checkpoint: clock-driven expiration and tombstone cleanup

Date: 2026-07-26

Implementation/proof commit:
`b2b2839` (`prove clock-driven expiration cleanup`)

Recovery foundation:
`b80bf0a` (`prove recovered socket stale-frame fencing`)

This slice closes the remaining first-release lifecycle proof:

1. Search WebSocket polling now accepts a context-aware wait function. The
   production default remains a stoppable real timer; tests can drive every
   refresh from one deterministic clock without sleeping or creating a
   goroutine per wait.
2. A real-manager HTTP/WebSocket fixture uses the production search manager,
   export manager, handler, upgraded sockets, controlled executor, active
   result lease, active artifact-download lease, and an unredeemed grant. It
   proves running preview, successful search completion, a real CSV artifact,
   and an invalid-SPL diagnostic failure before advancing time.
3. At exactly one nanosecond before expiry, completed/failed jobs and the
   export artifact remain available and all three sockets answer application
   pings. At the exact expiry boundary, the search/result, diagnostic, export,
   download-grant, and new-lease manager calls all switch to their expired
   contracts, while subscribed WebSockets publish the corresponding state.
4. The subscribed retained-result projection is required to be exactly four
   ordered frames: EXPIRED state, progress, one empty non-truncated RESET, and
   EXPIRED terminal. Every additional or row-bearing preview is rejected.
   Existing result and download leases remain readable while preventing any
   new lease or grant.
5. Expiring an export purges every retained completed terminal containing
   artifact metadata before installing the artifact-free EXPIRED terminal.
   Reconnecting from the old completed checkpoint must therefore receive
   `SEQUENCE_EXPIRED`, never a replay that can briefly restore a stale
   download-capable artifact.
6. Export tombstone retention is measured from the actual transition to
   EXPIRED, matching search behavior. A deliberately late first cleanup can no
   longer expire and immediately delete the tombstone in one pass.
7. The unpinned diagnostic tombstone retires first and closes its socket.
   Result/download leases pin the expired search/export tombstones and backing
   resources. Closing the final result lease synchronously removes the overdue
   search tombstone and rows. Closing the final download lease removes the
   artifact; the ensuing cleanup removes the export tombstone. All remaining
   sockets then close without another application frame.
8. The fixture proves all manual waiters are released, WebSocket poll reads
   quiesce, and the executor is invoked/exits exactly once. Its cleanup
   choreography advances the clock without waking registered pollers, mutates
   both managers, and only then releases those pollers, eliminating a
   stress-reproduced lost wakeup.
9. Controlled red tests exposed both production defects: stale completed
   export replay originally acknowledged `replay_will_follow=true`, and a late
   first export cleanup originally returned `ErrNotFound` immediately.
   Adversarial reviews also found and drove fixes for an assertion in the
   wrong lifecycle table, a socket-timeout false pass, duplicated fixture
   machinery, unordered stale-preview acceptance, completion/poller ordering,
   and the tombstone lost-wakeup race. Final frozen-diff correctness,
   architecture, and efficiency rereviews reported no findings.

The exact validation record is under **Latest validation evidence**. The
first-release lifecycle matrix is complete. The next correctness priority is
to make per-index event retention logically authoritative in compiled
ClickHouse scans even before background TTL merges physically remove rows.
That task and the remaining product/performance work are ordered under
**Remaining work, in priority order**. The overall backend goal remains
active.

## Previous checkpoint: recovered-socket stale-duplicate fencing

Date: 2026-07-26

Implementation/proof commit:
`b80bf0a` (`prove recovered socket stale-frame fencing`)

Recovery/progress foundation:
`522b0ac` (`fence search progress by state revision`)

This slice closes the stale-duplicate release boundary without changing the
already-correct production WebSocket client:

1. The direct client test now establishes a gap at sequence 502, resumes the
   retained subscription from 501, applies contiguous replay through 503,
   sends an equal 503 duplicate on the recovered socket, and then applies 504.
   The final checkpoint and event list are exact; no additional gap, error, or
   reconnect is allowed.
2. The ordinary real-Chrome sequence-gap fixture captures a byte-identical
   nonempty preview frame at checkpoint `K` and the original RUNNING state
   frame before inducing the existing real `K+1`/`K+2` gap, reconnect, and
   retained replay.
3. After replay has advanced the production client to `K+2`, the proxy sends
   exact old preview frame `K` through the recovered Playwright
   route-to-page WebSocket boundary. A scoped, byte-exact, bounded page-side
   recorder proves receipt. The subsequent real `K+3` and `K+4` frames apply,
   while always-updated DOM latches prove the stale text, preview table, or a
   non-`resyncing`/missing preview status never appears.
4. The same recovered socket then accepts the real completed state, final
   progress, and terminal event. Before the deliberately stale GET2 response
   is released, the proxy sends the exact old RUNNING state frame again. The
   fixture requires its sequence to be below the current transport checkpoint
   and its state revision below the completed revision.
5. Always-updated DOM latches prove that the job strip never leaves Completed,
   never returns to Queued/Preparing/Running/Canceling, and that the preview
   status stays present as `finalizing` with no stale preview rows. Bounded
   histories remain diagnostics only, so their cap cannot make the correctness
   oracle vacuously pass.
6. Both injections leave the recovered connection count at two and do not
   trigger an extra authoritative GET. The existing stale-GET, final GET,
   result publication, one terminal close, final table, executor-invocation,
   and safety assertions still complete normally.
7. A controlled mutation disabling
   `event.sequence <= previous` made the Chrome case fail exactly at
   `stale preview text never resurrected`. Restoring the production fence made
   the focused, race-enabled, and exact CI-equivalent gates pass.
8. Reviewers drove replacement of direct synthetic dispatch with the supported
   recovered `WebSocketRoute.send`, exact receipt evidence at the routed
   browser boundary, strict sequence/revision staleness checks, scoped and
   bounded instrumentation, reusable preview-status diagnostics, unbounded
   regression latches, and explicit no-gap/no-error unit assertions. Final
   correctness, efficiency, and fixture-quality rereviews reported no
   remaining concrete finding.

The exact validation record is under **Latest validation evidence**. At that
checkpoint, clock-driven terminal job/result/export expiration and tombstone
removal was next; it is now complete at `b2b2839`.

## Previous checkpoint: authoritative browser cancellation

Date: 2026-07-26

Implementation/proof commit:
`787a7f9` (`prove authoritative browser cancellation`)

Progress/recovery foundation:
`522b0ac` (`fence search progress by state revision`)

This slice closes the honest browser-cancellation boundary without changing
the already-correct production lifecycle:

1. A dedicated Chrome case reuses the real in-process SQLite control database,
   SPL parser/planner/compiler, controlled executor, search manager,
   production HTTP/WebSocket handler, and compiled backend UI. It waits for a
   real sequenced progress event and visible preview row before cancellation,
   proving the executor and subscription are active.
2. The cancel route is held before its request reaches the backend. The UI
   must synchronously dispose its one search WebSocket while the job remains
   running and the preview remains visible. Playwright then advances the page
   clock two seconds—past the client's 750 ms first reconnect delay—and
   requires exactly one connection and one close with no socket error.
3. A second discrete Cancel click is issued while the first request remains
   held. This exercises the application's synchronous pending-request guard,
   rather than the DOM double-click filter. Browser-route and server-middleware
   counters independently require exactly one cancel POST; create is also
   exactly once.
4. After the request gate opens, the test decodes the real protobuf
   `CancelSearchJobResponse` and requires the exact job ID, `CANCELED` state,
   positive job revision, matching progress revision, complete phase, and no
   failure. A second gate withholds those response bytes from the application,
   which must still show the running state and preview. This proves canceled
   presentation is authoritative rather than optimistic.
5. Releasing the same response bytes must make the job strip non-busy and
   canceled, restore the Run button, remove preview status and provisional
   rows, and issue no results POST. Browser errors, failed same-origin
   requests, external resources, and external WebSockets remain empty.
6. The Go fixture independently requires one executor invocation, one exit
   whose returned error is `context.Canceled`, zero recovery-control commands,
   one server create, one server cancel, and a retained manager snapshot in
   canceled state with a positive version, finish time, and no failure.
7. CI now includes `TestBrowserSearchCancellation` in the exact
   Docker/ClickHouse plus Chrome selector and retains
   `test-results/browser-search-cancellation` on failure.

The exact validation and adversarial review record is under **Latest validation
evidence**. Reviewers drove the pre-server request gate, second discrete click,
virtual-time reconnect proof, server-side request counter, explicit canceled
executor-exit counter, shared WebSocket URL matcher, and guarded response
waiter. Final correctness, efficiency, reuse, and fixture-quality reviews
reported no remaining blocker.

At that checkpoint, stale-duplicate injection and clock-driven expiration were
next. They are now complete at `b80bf0a` and `b2b2839`, respectively.

## Previous checkpoint: versioned progress and coalesced browser recovery

Date: 2026-07-26

Browser/recovery commit:
`522b0ac` (`fence search progress by state revision`)

Protocol/producer foundation:
`b5502a3` (`version search progress projections`)

Prior terminal-recovery foundation:
`d1286a4` (`prove REST-only terminal gap recovery`)

This slice closes the two progress/recovery boundaries left by the preceding
sequence-gap checkpoint:

1. `SearchProgress.state_version` is additive field 12 in the protobuf
   contract. The search manager populates it from the same job-state revision
   used by `SearchJob`, state-change events, and terminal envelopes. Generated
   Go and TypeScript bindings, producer tests, protocol fingerprints, replay,
   cancel, REST, WebSocket, and vertical assertions pin the field.
2. The browser owns one job-scoped progress revision. Lower progress revisions
   are acknowledged but never rendered; higher revisions apply even if an
   estimated counter decreases. Equal live or terminal projections are
   idempotent when only `elapsed`/`updated_at` drift, while an equal stable
   conflict triggers authoritative recovery. Invalid revisions and envelope
   mismatches also recover without mutating accepted state.
3. A legacy nested zero revision is inherited only from a positive
   authoritative REST or terminal envelope. A standalone unversioned live
   progress frame is not rendered and requests REST convergence. Equal-version
   REST replaces the projection because REST is the convergence authority;
   retrying the same authority on a stable conflict would otherwise loop.
4. Job phase presentation is separate from progress-metric acceptance.
   Completed/canceled/failed/expired state can advance terminal presentation
   without allowing stale or mismatched final metrics to overwrite the current
   counters. Job and progress revision state reset together at job/application
   boundaries.
5. The whole semantic REST recovery cycle is single-flight, not only its HTTP
   request. Concurrent triggers share one continuation, mutate retry/backoff
   state once, and collapse urgent traffic into at most one immediate
   follow-up after settlement. Ordinary live traffic cannot postpone an
   already-queued urgent reconciliation.
6. The new real-browser REST-first fixture drops `K+1`, forwards `K+2`, and
   captures the byte-identical retained replay. REST revision `K+2` is applied
   before replay with deliberately different projection timing. The test then
   releases replay `K+1` alone, confirms receipt on the second browser
   WebSocket, crosses a render barrier, and proves a bounded DOM observer never
   sees one scanned row. It repeats the receipt/render barrier for equal
   revision `K+2`, then uses visible live `K+3` as the ordered message-chain
   acknowledgement. No conflict notice or extra GET is allowed.
7. Existing accepted-WebSocket-terminal and REST-only-terminal companions
   retain their exact causal barriers and request/frame counts. The
   WebSocket-terminal paths now capture GET2 while the job is still running so
   the terminal revision, rather than a broad arrival epoch, deterministically
   makes it stale and requires GET3.
8. Frontend tests now include a pure revision reducer suite and total 42.
   CI runs the Docker/ClickHouse vertical, sequence expiration, ordinary gap,
   REST-only terminal gap, and REST-first progress gap under the exact selector
   below, retaining a separate REST-first Playwright artifact.

The exact validation and adversarial review record is under **Latest validation
evidence**. Reviewers found and drove fixes for duplicate recovery
continuations, repeated metric dispatches, no-op notice allocations, scenario
flag ambiguity, a potentially batched false-pass, an incorrect DOM regex, and
observer/listener cleanup. Final semantics, efficiency, reuse, and
browser-fixture reviews reported no remaining blocker.

At that checkpoint, browser cancellation, stale-duplicate injection, and
clock-driven expiration were next. They are now complete at `787a7f9`,
`b80bf0a`, and `b2b2839`, respectively.

## Previous checkpoint: browser sequence-expiration and transient recovery

Date: 2026-07-25

Transient-failure proof:
`ad4a77b` (`prove transient browser recovery failure`)

Sequence-expiration implementation:
`8f6d569` (`prove browser sequence expiration recovery`)

Preceding real-recovery checkpoint:
`a2af9e0` (`record real websocket recovery checkpoint`)

This slice proves `SEQUENCE_EXPIRED` recovery through the production React
workspace, generated protobuf HTTP/WebSocket transports, and a real search
manager:

1. A dedicated in-process fixture uses a real SQLite control database, SPL
   parser/planner/compiler, controlled executor, search manager, bounded
   one-event replay ring, production HTTP handler, and Chrome. It creates one
   browser-owned job and forwards its initial preview at checkpoint `K`.
2. The fault proxy withholds real progress `K+1` and `K+2`, closes the first
   socket, and requires the browser to reconnect the exact same job,
   subscription, preview options, and `after_sequence=K`. The real server
   responds with `SEQUENCE_EXPIRED`; the fixture validates the routed
   subscription, target, checkpoint, earliest/latest bounds, and
   acknowledgement before recovery may proceed.
3. The first authoritative recovery GET is held while real progress `K+3`
   arrives, then returns an actual HTTP 503. No snapshot or queued progress is
   applied, the UI remains `resyncing` at zero rows, and the next connection
   again subscribes from exact `K` with the original identity and options.
4. A deliberately stale second response with `state_version=0` is also
   rejected without advancing the checkpoint. The fourth connection again
   resumes from exact `K`. Its fresh authoritative snapshot is captured at
   three rows and held while real `K+4` arrives; the UI remains at zero until
   the three-row response is successfully applied and acknowledged, after
   which the queued WebSocket event is required to establish four rows.
5. A watchdog race captures an authoritative GET at four rows, delivers live
   WebSocket progress `K+5`, and only then releases the older response. The
   production live-update epoch fence rejects that in-flight snapshot, so the
   visible five-row state cannot regress even when its numeric state version
   is otherwise acceptable.
6. The same recovered connection accepts a fresh preview and terminal result.
   The final authoritative two-row projection replaces preview state. The
   executor is invoked once and exits once; job creation, sockets, recovery
   GETs, frames, commands, diagnostics, and waits are all bounded.
7. The client now rejects a resynchronization payload whose subscription does
   not match its routed envelope and rejects inverted replay bounds. Recovery
   acknowledgement remains provisional until the asynchronous recovery
   handler resolves; queued events and the checkpoint stay suspended in the
   meantime. That commit-protocol property is pinned directly by the unit
   suite, while the browser fixture proves the application handler does not
   acknowledge a failed authoritative request.
8. Integration builds use a unique staged repository per Go test process,
   with shared immutable output only inside that process. Raw frontend builds
   and parallel integration processes no longer collide through `.next` or
   `out`. Cleanup validates its exact temporary target. CI runs both real
   browser cases, preserves both artifact directories, and gives the combined
   Go package a 12-minute timeout inside the 15-minute job budget.

Final validation on the exact committed implementation passed:

```sh
go test ./... -count=1
go test -race ./integration ./internal/server ./internal/searchws -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequenceExpiredRecovery)$' \
  -count=1 -timeout=12m -v
git diff --check
```

The frontend suite contains 32 passing protocol tests. The latest combined
Docker/ClickHouse and Chrome gate completed in about 34 seconds. The real
staged recovery case also passed under the race detector, and repeated harness
race checks left no staged directory or Docker container behind.

Independent correctness, concurrency/performance, and quality rereviews
reported no remaining concrete finding. A follow-up transient-failure review
found that the first draft fetched its fresh snapshot after `K+4`, which could
have hidden a dropped queued frame; capturing the snapshot at three rows
before sending `K+4` closed that false-positive path.

At that checkpoint, the next recommended slice was explicit browser
sequence-gap injection: forward `K+2` while dropping `K+1`, require the client
to reconnect from `K`, and prove contiguous replay.

## Previous checkpoint: resumable WebSocket recovery

Date: 2026-07-25

Implementation commit:
`86d4dd8` (`complete resumable websocket recovery`)

Starting checkpoint:
`510c2e0` (`harden websocket resume recovery`)

This slice completes the in-memory protocol and first-party client contract
needed before a deterministic real-browser disruption test:

1. A connection establishing a new server epoch may resume from any retained
   sequence between the epoch start and the latest published frame. The server
   verifies that the entire retained suffix is continuous before replaying it.
   An out-of-range checkpoint still reports `SERVER_RESTARTED`; an incomplete,
   overdue, or gapped suffix reports `SEQUENCE_EXPIRED`.
2. Resynchronization is a commit protocol. A recovery listener must call the
   supplied acknowledgement before the client advances its checkpoint.
   Rejected and resolved-but-unacknowledged handlers retain the old checkpoint
   and reconnect. A same-epoch checkpoint regression is rejected, while an
   explicit `SERVER_RESTARTED` response may establish a lower new-epoch
   boundary.
3. Successful sequenced work establishes connection stability. Subscription
   acknowledgement alone arms only a delayed stability timer, and that timer
   is not armed until all generic event listeners finish successfully. This
   prevents repeated recovery failures or a slow/rejecting acknowledgement
   listener from collapsing exponential reconnect backoff.
4. Every subscription identifier is single-use for the lifetime of a
   connection, including after unsubscribe. The exact 256-identifier ceiling
   is bounded. A 257th unique identifier receives
   `TOO_MANY_SUBSCRIPTIONS` with `connectionWillClose=true`; the server closes
   with retry semantics and the first-party client preserves and retries the
   rejected local subscription on a fresh connection.
5. Positive unknown future protobuf event variants are target-fenced, exposed
   only to generic listeners, and allowed to advance the sequence checkpoint.
   Malformed or mismatched frames cannot advance it.
6. An authoritative zero-row preview or invalidating terminal projection
   removes all retained preview state and exact replay accounting. Expiration
   can therefore never resurrect older row-bearing preview frames.
7. Expired terminal targets with active subscribers continue bounded
   tombstone polling. A missing first read at or after expiry retires the
   target; later tombstone deletion does the same. Retirement removes the
   exact cached target and closes existing sockets so reconnect observes
   `JOB_NOT_FOUND` rather than remaining stranded on stale state.

The implementation adds focused Go and frontend tests for every boundary
above, including a fresh-epoch lagging replay, zero-row preview expiration,
first-expiry tombstone deletion, active-subscriber retirement, identifier
reuse and exact exhaustion, unacknowledged resync, epoch regression, lower
restart epochs, unknown future variants, repeated recovery failure backoff,
and slow acknowledgement listeners.

Final validation passed:

```sh
go test ./... -count=1
go test -race ./internal/searchws -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
```

The frontend suite contains 29 passing protocol tests. Independent backend,
frontend, correctness, performance, and reuse/quality reviews found no
remaining checkpoint blocker after their findings were fixed. The last
focused reviewers reran the first-expiry and identifier-exhaustion boundaries
20 times and rechecked the delayed-listener race and reconnect retry contract.

The real-browser retained-replay path and real-manager expiration/cancellation
fixture were subsequently completed in `7136f29` and `72bd6ca`. The remaining
fault cases are recorded under **Remaining work, in priority order**. The
overall backend goal remains active.

## Previous checkpoint: crash-safe collector and server process restarts

Date: 2026-07-25

Starting checkpoint:
`21d21cb` (`prove exact GradeThis compatibility corpus`)

Use the current `main` HEAD for the completed restart checkpoint. The commit
containing this section is created after all gates below pass and is pushed
directly to `origin/main`.

This slice closes the first release's explicit cross-process durability proof:

1. The backend vertical starts with an empty discovered `app.log`, appends one
   primer, and waits for its ClickHouse row and durable source checkpoint.
2. It hard-kills the collector at that transaction boundary, accepts either a
   drained WAL or the one valid checkpoint-before-local-WAL-ack crash state,
   reopens the WAL for inspection, and restarts against the same WAL and
   checkpoint directory.
3. After two more acknowledged events, it waits for the collector's WAL
   acknowledgment high-water to settle, hard-kills the server, appends and
   fsyncs a final sentinel, waits for an actual `segment-*.wal` append, and
   fsyncs the observed segment from the harness before the deliberate kill.
4. It hard-kills the collector, reopens its WAL, and requires exactly one
   pending event with the sentinel's exact byte and physical-line cursor.
5. The same server SQLite database/master key and collector WAL/checkpoints are
   reused. Four distinct stable event IDs must become queryable with no loss.
   The documented at-least-once boundary permits one physical sentinel replay
   in a second batch, so the acceptance SPL uses `dedup event_id` and still
   requires exactly four logical results.
6. A clean final collector shutdown must leave the exact EOF checkpoint and a
   drained WAL with a positive acknowledged sequence before the existing
   protobuf HTTP, binary WebSocket, browser, timeline, paging, and export
   assertions continue.

The acceptance test exposed a real collector bug: each poll constructed a
one-shot framer whose line counter restarted at one. Byte offsets and stable
event IDs were safe, but origins and resumed checkpoints regressed their
physical-line metadata. The fix carries an explicit cursor end to end:

- protobuf `EventOrigin` has additive optional `next_line_number`;
- every line, multiline, flush, max-lines, and oversized frame reports its
  half-open physical-line interval;
- the tailer owns the cursor across polls and resets it only for a new
  copy-truncate generation;
- decoder origins, compact WAL marks, recovery, acknowledgment aggregation,
  local terminal delivery, checkpoint persistence, and server ingestion
  validation retain and check it;
- equal or increasing byte offsets cannot carry conflicting or regressing
  present line cursors, and the reserved `uint64` maximum cannot enter a batch
  that the server would be unable to acknowledge; and
- an oversized record that reaches the current EOF retains bounded
  discard-through-delimiter state, so a later suffix is never published as a
  separate event. A crash safely reconstructs that state from the older
  durable byte checkpoint.

`start_at=end` deliberately numbers the newly monitored stream from line one;
it does not scan skipped historical bytes. Checkpoints written before
`next_line_number` are handled in O(1). Ordinary line-framing checkpoints can
derive the exact next line from the prior event and are upgraded in one atomic
batch per discovery pass. A legacy multiline checkpoint—or a legacy nonzero
discovery offset with no line anchor—cannot recover its exact ending physical
line without an unbounded prefix scan. Those sources therefore omit both
optional line fields instead of publishing an approximation; byte offsets and
stable event IDs remain exact. A new file generation restores exact line
metadata. This compatibility boundary avoids both false origin data and
stranding large existing checkpoints.

Focused collector/input/WAL/ingest/integration tests and the full collector
race suite passed after these fixes. The ordinary, frontend, build,
browser/Docker vertical, generated-protobuf, and cleanliness gates are recorded
under **Latest validation evidence**.

The next recommended release-path slice at that checkpoint was deterministic
browser disconnect/resume and replay-window-expiration coverage. The overall
backend goal remains active; it was a safe pause, not completion of the
product plan.

## Previous checkpoint: exact GradeThis v0.1 compatibility corpus

Date: 2026-07-25

Starting checkpoint:
`221b4ab` (`record GradeThis corpus pause checkpoint`)

Frontend result adaptation:
`430e6ce` (`fix top result visualization adaptation`)

Use the current `main` HEAD for the exact corpus implementation commit. It is
committed after this document is updated and must be pushed directly to
`origin/main`.

This slice completed the product plan's named ten-search GradeThis
compatibility criterion:

- one shared `v0.1` manifest owns the exact ten SPL templates used by ordinary
  compiler coverage, the legacy executor smoke, and the exact acceptance test;
- one deterministic 20-event synthetic profile generates byte-pinned NDJSON
  with a strict secret/PII/non-documentation-data scanner;
- an ordinary test decodes every event with the production collector and pins
  canonical timestamps, messages, trace IDs, and typed request fields;
- a focused integration test writes that decoded batch through the production
  ClickHouse store, then executes all ten searches through parser, planner,
  compiler, executor, search-job manager, and owner/tenant-scoped signed paging
  against ClickHouse `26.3.17.4`;
- every public schema flag, ordered row ordinal, typed cell, total, completion
  state, and terminal cursor is exact; result sets larger than three rows must
  cross opaque cursor pages; and
- required CI runs the focused pinned-ClickHouse corpus before production
  packaging. The `top message` frontend adapter now treats `percent` as a
  measure and has a real protobuf-shaped categorical-visualization regression.

The acceptance pass exposed and fixed a real sparse-result transport defect:
ClickHouse JSON columns can report paths absent from an individual row as
null when another row in the part has that path. Ordinary raw-event searches
now carry a bounded private `field_names` presence column through SQL, validate
its exact transport contract, and reconstruct only the fields present on that
row. The private column never appears in the public schema. The fixture
contains one explicit-null field beside rows where that field is missing, so
the pinned test proves the distinction across cursor pages.

The logical/physical path codec and allocation-free normalized-size accounting
for dots, backslashes, and percent escapes are shared by ingestion, storage,
and result conversion. Canonical path count, depth, segment,
aggregate-name-byte, ordering, collision, and reserved-root bounds are
enforced before ingestion acknowledgement after redaction, again at storage,
and again while converting search results. Path parsing preallocates only the
hard 16-segment maximum even when a segment contains many escaped dots.

Adversarial review also hardened the corpus scanner against fused and
multiword credential/PII keys, credential assignments and bearer tokens in
values, embedded Unix/Windows paths, production URLs, duplicate JSON keys,
and non-documentation IPv4/IPv6 addresses including bracketed IPv6 ports.
Scanner diagnostics use ordinal key locations and never echo rejected key
text. Failed owner/tenant access and tampered signed cursors are direct
acceptance assertions.

Independent correctness, security/maintainability, and performance reviewers
reported no remaining checkpoint blocker after those fixes. A final simplify
pass then removed redundant sparse-column state and allocations, centralized
raw-fields removal, and routed path encoding, byte accounting, typed-nil
handling, String comparison, and the broad executor image default through
shared utilities.

The exact corpus and the full existing query-executor ClickHouse suite both
passed at this checkpoint. No ephemeral `open-splunk-*` container remained.

## Previous pause checkpoint: collector-to-browser gate and GradeThis corpus audit

Date: 2026-07-25

Branch: `main`

Starting checkpoint:
`d0949e8` (`record scalar extrema backend checkpoint`)

Durable-ingestion commit:
`5b99c83` (`prove durable backend vertical ingestion`)

Collector-to-browser gate:
`cf37bd7` (`add collector to browser acceptance gate`)

This document is committed immediately after the acceptance-gate commit. Use
the current `main` HEAD as the document commit rather than copying an older
hash from this file.

The earlier branch repair is complete: the work is on `main`, all current
checkpoint commits were pushed directly to `origin/main`, and subsequent work
must continue on `main`.

The overall backend objective remains active. Work was intentionally paused at
this earlier green checkpoint and later resumed. The product architecture plan
is not complete.

Immediately before this pause, three read-only agents audited the product
plan's ten GradeThis searches, the current fixtures, and the compile and
ClickHouse integration coverage. Their conclusions and the exact next
test-driven slice are recorded below so a new conversation does not need to
repeat that investigation.

## What the latest slice completed

### Deterministic self-contained server build

- `make build` and `make build-server` now compile the static frontend in
  backend mode by default.
- The release-path integration test explicitly clears any ambient API base URL
  and rebuilds the frontend before compiling the Go server.
- The compiled server starts from an empty temporary working directory with a
  deliberately nonexistent `PATH`, proving it does not need Node.js,
  repository files, or another executable runtime.
- The test checks the embedded HTML is the backend workspace, exercises the
  protobuf bootstrap route, and then uses that same compiled server for every
  protocol and browser assertion.

### Live collector durability proof

- The vertical starts the collector against an empty `app.log` and waits for
  the exact zero-offset discovery checkpoint before writing data.
- It appends and fsyncs one generated primer event, waits for the ClickHouse row
  and a checkpoint at that file boundary, then appends and fsyncs the remaining
  fixture. This proves real tailing after discovery and after a durable
  acknowledgment rather than ingesting a file that was already complete.
- The final checkpoint must reach the exact synced EOF. After a clean collector
  shutdown, the WAL must be drained and retain a positive acknowledged
  sequence high-water mark.
- The server still provisions the index and scoped ingestion token over
  protobuf HTTP, and secret values are checked against collector/server logs,
  typed result pages, and downloaded export bytes.

### Protocol, paging, and real-browser proof

- The Go protocol client creates a search, observes an acknowledged binary
  protobuf WebSocket subscription with monotonic state/progress/terminal
  sequences, and fetches the four authoritative results over exactly two
  opaque cursor pages.
- Cursor tokens and row IDs cannot repeat, ordinals must be globally
  contiguous, schemas must remain stable, total size must be exact, and the
  retained snapshot must be complete.
- A pinned Playwright test launches Chromium against the UI embedded in the
  compiled Go server. The browser creates its own search through same-origin
  protobuf HTTP, receives binary WebSocket traffic, renders the timeline, and
  ends with four authoritative non-preview rows containing the fixture
  sentinel.
- The browser observer uses the repository's generated TypeScript protobuf
  codecs. It correlates the create response, outgoing subscription, positive
  sequence `search_progress` frame, results response, and rendered UI to the
  same browser-created job.
- A recorded `live` or `finalizing` preview transition proves the application
  applied live preview traffic before replacing it with authoritative results;
  paused, resynchronizing, and finalization-error states fail the test.
- Browser errors, failed same-origin API requests, external HTTP(S) resources,
  and foreign WebSockets are bounded and rejected. Playwright output is capped,
  failure screenshots/traces are retained, and timeout cancellation gives
  Playwright a bounded graceful shutdown before a hard-kill fallback.
- The existing timeline, typed redaction, one-time JSONL export, and 10,001-row
  bounded export re-execution checks remain in the same release-path test.

### Required CI gate

- CI installs the pinned Playwright dependency and Chromium build, runs the
  opt-in Docker collector-to-browser test as a required `backend-vertical`
  job, and uploads browser failure artifacts.
- Production binary packaging now depends on that gate.
- Local setup and the exact opt-in command are documented in the root and
  integration READMEs.

## Previous checkpoint reference: optimized scalar-String `stats min` and `stats max`

Open Splunk already implemented typed `stats min(field)` and
`stats max(field)` from SPL parsing through ClickHouse execution, downstream
calculated-field use, search-job transport, documentation, and editor support.
This slice removed the known singleton-Array overhead for fields proven
statically scalar String while retaining the guarded multivalue path.

### SPL, AST, and plan contract

- `min` and `max` are case-insensitive and accept exactly one unquoted, exact
  field.
- The default names are canonical `min(field)` and `max(field)`; use `AS` when
  the current downstream field grammar must reference the result.
- Empty calls, expressions, quoted fields, wildcards, multiple arguments, and
  forged aggregate metadata fail explicitly rather than being approximated.
- Separate AST and plan enum values, canonical-name mapping, required-input
  validation, reserved open-schema `fields` handling, output uniqueness, and
  relational-depth checks are covered by tests.
- A closed schema may legitimately contain a calculated field named `fields`;
  the planner accepts extrema over that field.
- The search editor and compatibility contract advertise the supported forms.

### Runtime ordering and typed results

- Missing, explicit null, an empty multivalue, and null multivalue members do
  not participate.
- Every immediate scalar member of a top-level multivalue participates without
  expanding event rows. Nested arrays and objects, generic objects, and
  flattened object parents fail atomically with the existing sanitized stats
  unsupported-value marker.
- Runtime String/Dynamic candidates use a deterministic two-class order:
  finite numeric candidates sort before lexical candidates; numeric candidates
  compare numerically and lexical candidates compare by raw bytes. Therefore
  `min` selects a numeric candidate whenever one exists, while `max` selects a
  lexical candidate whenever one exists.
- Numeric text must be valid UTF-8, at most 4 KiB, match the complete decimal
  grammar, and convert to finite `Float64`. Whitespace, `NaN`, infinity,
  overflowing exponents, invalid UTF-8, and longer values remain lexical.
- Numeric zero is normalized to positive zero. Equivalent spellings such as
  `01`, `1.0`, and `1e0` publish the same `Double(1)` result.
- Runtime winners are nullable `Mixed`: numeric winners are `Double`, lexical
  winners retain their String/Bytes representation, and no winner is null.
- Statically typed numeric, Boolean, and timestamp scalars use native
  `minIfOrNull` / `maxIfOrNull`, preserving physical type and timestamp
  nanosecond precision rather than converting through `Float64`.
- Global aggregation over no rows emits one null result row. Retained groups
  with no eligible value contain null; grouped aggregation over no rows emits
  no groups. Projected-away inputs remain absent.

Punctuation placement is deliberately documented as an Open Splunk v0.1
boundary. Public Splunk documentation warns that symbol ordering is not
standard. The pinned fixture now includes numeric text with `!` and `~` so the
chosen numeric-before-lexical/raw-byte behavior cannot drift silently.

### Efficient ClickHouse lowering

- A proven scalar String field now materializes one nullable String alias, one
  bounded finite-`Float64` classifier alias, and one
  `(class, numeric-key, lexical-key)` tuple candidate.
- Scalar extrema use conditional `minIfOrNull` / `maxIfOrNull` tuple
  aggregation. Dynamic and `StringArray` inputs retain the guarded Array path,
  including unsupported-container validation.
- One stable per-field descriptor coordinates `min`/`max`, `values`, and
  `dc`, including interleaved fields and either measure order.
- Repeated identical extrema share one physical aggregate key and one stored
  type. An immediate post-aggregate projection reconstructs every public
  `Mixed` alias and drops the tuple key before any downstream
  `SELECT *`, filter, sort, or limit can widen rows.
- `min`/`max` and `values` over the same scalar input share the original String
  materialization while values alone retains exact, bounded Array semantics.
- Both paths use the same numeric normalization, tuple ordering, and
  Null/Double/String/Bytes stored-type mapping helpers, preventing subtle
  command or lowering drift.
- The lowering performs no `ARRAY JOIN`, row expansion, sorting, or unbounded
  list aggregation.
- The numeric text grammar is shared with `bin`, preventing command semantics
  from drifting.
- Runtime result type metadata is private and survives `rename`, `eval`,
  downstream `bin`, and re-aggregation.
- A transforming aggregate no longer causes downstream `bin` to reference the
  event-only `__os_field_metadata_version` column. Calculated semantic type
  metadata is authoritative after event rows have been transformed.
- Generated placeholder count/order and the existing generated-SQL and
  relational-depth limits remain exact.

## Adversarial review record

Three independent agents reviewed the collector-to-browser change for
correctness, protocol semantics, reliability/performance, and reuse/code
quality. A second read-only pass reviewed the hardened result. Findings and
resolutions:

1. **A completed file was not a sufficient tailing proof.** The test now starts
   with an empty discovered file, appends a primer, waits for its durable
   checkpoint, and only then appends the remainder.
2. **Binary traffic alone could belong to another job.** The Playwright
   observer now decodes generated protobuf messages and requires an outgoing
   subscription plus a positive-sequence `search_progress` event for the exact
   job ID decoded from the browser's create response.
3. **Wire arrival alone did not prove UI application.** A mutation recorder
   requires the preview UI to enter `live` or `finalizing`, rejects recovery
   error states/notices, and then requires final non-preview rows.
4. **Manual field-number parsing duplicated generated code.** The browser spec
   is strict TypeScript and uses the checked-in generated codecs for create,
   results, WebSocket command, and WebSocket event messages.
5. **Failure-path promises could reject before their normal await.** Response
   waiters and the protocol completion are marked handled immediately, the
   original promises remain authoritative, and protocol listeners/timers are
   disposed in `finally`.
6. **Resource and cleanup paths were unbounded.** Browser recorders, child
   output, subscriptions, and progress events are capped. Context cancellation
   first signals the Playwright process group so it can close Chromium, then
   uses the standard bounded hard-kill fallback.
7. **Paging checks had assembled synthetic response metadata.** Each real
   protobuf page is retained and inspected directly; only stable schema and
   cloned rows are collected for cross-page assertions.
8. **Opt-in CI could silently skip without Docker.** Setting the integration
   flag now makes a missing Docker CLI fatal.

The final correctness and quality re-audits reported no remaining concrete
blocker. The performance review reported no checkpoint blocker; its remaining
harness and CI optimizations are recorded below as follow-up work rather than
weakening the release-path proof.

### Previous scalar-extrema reviews

Three independent post-implementation agents reviewed correctness and SPL
semantics, ClickHouse execution/performance, and reuse/maintainability. Their
concrete findings were handled as follows:

1. **Stop-ship downstream failure fixed.** `stats min(metric) AS low | bin low`
   originally referenced the event-only metadata-version column after `stats`
   had dropped it, causing ClickHouse Code 47. The compiler now distinguishes
   event metadata from authoritative calculated metadata. A unit assertion and
   executed pinned-ClickHouse regression cover this path.
2. **Shared logic consolidated.** Numeric-bin and extrema decimal grammar now
   use one constant. Extrema, `dc`, and `values` share one String-input
   resolution/cache helper.
3. **Documentation corrected.** “No row materialization” was too broad; the
   true invariant is no row expansion. Symbol ordering is now labeled as an
   explicit compatibility boundary.
4. **Coverage strengthened.** Native Boolean extrema execute for both values.
   Native timestamp extrema now compare distinct timestamps one nanosecond
   apart, proving ordering and precision. Punctuation, closed-schema `fields`,
   and min-plus-values input sharing gained regressions.
5. **Test-only parser mistake corrected.** An attempted Bool integration probe
   used an unsupported comparison inside `eval`; it was replaced with the
   supported Bool-literal path.

The final reviewer verdict reported no remaining stop-ship correctness,
security, argument-ordering, relational-depth, optimizer-sharing, or
unbounded-state issue.

The scalar optimization received a second three-agent adversarial pass. It
found and resolved:

1. **Per-field/result cache drift.** Independent ordinals and repeated
   expressions were replaced with stable field descriptors and one cached
   result per `(input, function)`. Tests pin interleaved `host`/`service`
   wiring, exact calculated-argument order, and the 64-measure ceiling.
2. **Private tuple-key retention.** The first cache revision could carry a
   winning String twice through downstream `SELECT *` stages. Reconstruction
   now happens in an immediate cleanup projection that excludes every tuple
   key, and the added relational level is explicitly pinned.
3. **Coverage and fixture cost.** Eight filtered scalar queries became one
   grouped result comparison. Repeated aliases are executed and scanned as
   well as explained. A 4-KiB-plus-one String pins the lexical length boundary
   through both scalar and Dynamic paths and downstream re-aggregation.
4. **Reproducible performance evidence.** An opt-in benchmark now uses the
   production classifier/ordering helpers, a generated two-million-row corpus,
   the pinned server, one thread, disabled query cache, fixed samples, and
   `system.query_log` memory/duration. Its documentation explicitly limits the
   claim to helper-level lowering; compiler shape is covered by assertions and
   pinned `EXPLAIN`.

All three reviewers reported no remaining concrete correctness, efficiency,
or code-quality finding after those fixes.

## Latest validation evidence

### Clock-driven expiration and tombstone cleanup

The proof at `b2b2839` passed:

```sh
go test ./... -count=1
go vet ./...
go test -race ./internal/export ./internal/searchws ./internal/server -count=1
go test ./internal/server \
  -run '^TestSearchWebSocketRealManagersExpireLeasedResultsArtifactsAndTombstones$' \
  -count=200
go test -race ./internal/server \
  -run '^TestSearchWebSocketRealManagersExpireLeasedResultsArtifactsAndTombstones$' \
  -count=50
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
```

All 42 frontend tests passed. The exact six-case Docker/ClickHouse plus
Playwright gate completed in 49.937 seconds:
`TestBackendVertical` in 22.04 seconds,
`TestBrowserSequenceExpiredRecovery` in 11.71 seconds,
`TestBrowserSequenceGapRecovery` in 5.11 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.07 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.23 seconds, and
`TestBrowserSearchCancellation` in 1.25 seconds. A read-only Docker check
showed no Open Splunk or ClickHouse test container afterward.

The final lifecycle fixture passed 200 consecutive ordinary runs and 50
consecutive race-enabled runs locally. Independent reviewers additionally ran
up to 100 ordinary repetitions, up to 60 semantic repetitions, and repeated
race-enabled affected-package suites. Controlled red/green evidence covered
artifact-terminal replay and late-cleanup tombstone retention. Reviewers then
found two false-pass assertions and two deterministic-clock races; each was
reproduced, fixed, stress-tested, and rereviewed on the final frozen staged
diff. Final correctness, architecture/determinism, and efficiency/leak
rereviews reported no remaining finding.

### Recovered-socket stale-duplicate fencing

The proof at `b80bf0a` passed:

```sh
go test ./... -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test -race ./integration \
  -run '^TestBrowserSequenceGapRecovery$' -count=1 -timeout=8m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
```

All 42 frontend tests passed. The final focused real-Chrome gap proof completed
in 14.074 seconds overall, with the browser case itself taking 13.54 seconds.
The final race-enabled run completed in 15.185 seconds overall, with the
browser case taking 13.56 seconds. The exact six-case CI-equivalent gate
completed in 49.920 seconds:
`TestBackendVertical` in 20.66 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.02 seconds,
`TestBrowserSequenceGapRecovery` in 5.43 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.33 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.48 seconds, and
`TestBrowserSearchCancellation` in 1.54 seconds. A read-only Docker check
showed no Open Splunk or ClickHouse test container afterward.

The controlled-mutation run disabled the production
`event.sequence <= previous` return and failed at the exact stale-preview
resurrection assertion. The fence was restored before every green gate and
before commit. Simplify and adversarial reviewers found and drove fixes for a
custom synthetic injection path, redundant receipt waits, broad unbounded
socket/DOM instrumentation, duplicated status recording, insufficient
sequence/revision proof, bounded correctness histories, missing unit
gap/error assertions, ambiguous timeout diagnostics, and transient
preview-status removal. Final frozen-diff reviews reported no remaining
correctness, performance, determinism, or code-quality finding.

### Authoritative browser cancellation

The cancellation proof at `787a7f9` passed:

```sh
go test ./... -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test -race ./integration \
  -run '^TestBrowserSearchCancellation$' -count=1 -timeout=10m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
```

All 42 frontend tests passed. The final focused real-Chrome cancellation run
completed in 9.63 seconds. The race-enabled focused run completed in 11.360
seconds overall, with the browser case itself taking 9.74 seconds. The exact
six-case CI-equivalent gate completed in 49.246 seconds:
`TestBackendVertical` in 19.99 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.04 seconds,
`TestBrowserSequenceGapRecovery` in 5.33 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.37 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.47 seconds, and
`TestBrowserSearchCancellation` in 1.53 seconds. A read-only Docker check
showed no Open Splunk or ClickHouse test container afterward.

The first real run exposed an ambiguous role locator because both the primary
button and empty-state action contained “Run search”; the proof now uses the
stable primary-button test ID. Adversarial reviews then drove independent
pre-server and pre-browser response gates, a second discrete cancel click,
virtual-time reconnect fencing, browser-route and server-side request counts,
an explicit canceled-executor-exit count, a shared WebSocket URL matcher, and
a guarded create-response waiter that cannot reject during teardown. Final
correctness, quality, efficiency, reuse, and fixture reviews reported no
remaining blocker.

### Versioned progress and coalesced recovery

The protocol slice at `b5502a3` passed the focused producer/protocol/server
tests, replay/cancellation coverage, race-enabled protocol packages, generated
binding checks, and the full Go suite. The browser slice at `522b0ac` passed:

```sh
go test ./... -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery))$' \
  -count=1 -timeout=15m -v
```

All 42 frontend tests passed. The exact five-case CI-equivalent run completed
in 48.854 seconds:
`TestBackendVertical` in 21.16 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.02 seconds,
`TestBrowserSequenceGapRecovery` in 5.34 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.32 seconds, and
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.48 seconds.

After the final test-observer cleanup, typecheck, lint, all 42 frontend tests,
and diff hygiene passed again; the focused REST-first Chrome/Docker case also
passed from a fresh staged build in 12.61 seconds. A read-only Docker check
showed no Open Splunk or ClickHouse test container afterward.

Three independent adversarial reviews covered revision semantics, terminal
phase/metric separation, recovery-cycle scheduling, request and render
efficiency, fixture causality, false-positive risk, and code reuse. The final
REST-first proof confirms browser receipt of `K+1` before permitting `K+2`,
then waits a timer task and two animation frames while `K+1` is isolated. The
observer records the exact scanned-row `<strong>` value, is disconnected on
both success and failure, and cannot let React batching hide a transient
regression. Final rereviews reported no blocker.

### REST-only terminal recovery after a sequence gap

The `d1286a4` companion first failed red after every causal barrier had
passed: the entire three-frame terminal WebSocket projection was captured but
withheld, authoritative GET2 returned the real completed job, and the decoded
result response was held before browser delivery, yet the zero-row banner
remained `resyncing` for the full 45-second assertion window. The completed
REST path now selects `finalizing` whenever the preview lifecycle is active,
including `resyncing` with no snapshot, while `disabled` and all
failed/canceled/expired paths retain their prior behavior. Repeated terminal
discovery is idempotent and does not repeat the finalizing announcement.

The exact committed state passed:

```sh
go test ./... -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test -race ./integration \
  -run '^TestBrowserSequenceGap(RESTTerminal)?Recovery$' \
  -count=1 -timeout=12m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequence(ExpiredRecovery|Gap(RESTTerminal)?Recovery))$' \
  -count=1 -timeout=15m -v
git diff --check
```

All 32 frontend tests passed. Both gap fixtures passed under the race detector
in 27.854 seconds: the WebSocket-terminal case in 20.59 seconds and the
REST-only case in 5.60 seconds after the shared staged build. The final exact
CI-equivalent gate passed in 50.833 seconds:
`TestBackendVertical` in 21.83 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.08 seconds,
`TestBrowserSequenceGapRecovery` in 11.14 seconds, and
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.38 seconds.
A read-only Docker check confirmed no Open Splunk/ClickHouse test container
remained.

Design reviewers required withholding completed state and final progress in
addition to the terminal event so WebSocket could not leak completion. The
final simplify and adversarial passes also made finalizing idempotent,
preserved the exact upstream protobuf result bytes, consolidated duplicated
assertions, isolated inherited scenario flags, and found no remaining
correctness, determinism, performance, CI, or code-quality blocker. The older
valid-zero-row announcement wording edge remains deferred until it has its
own red test; it is not exercised by the post-gap path, which has no snapshot.

### Real browser sequence-gap recovery validation

The implementation at `f72f184` passed the full Go suite, all 32 frontend
tests, TypeScript, lint, the production build, the dedicated real
Chrome gap case, the gap case under the Go race detector, the combined
expiration/gap Chrome run, and the CI-equivalent Docker/ClickHouse plus both
Chrome recovery cases:

```sh
go test ./... -count=1
go test ./integration -count=1
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test -race ./integration \
  -run '^TestBrowserSequenceGapRecovery$' -count=1 -timeout=6m -v
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequence(Expired|Gap)Recovery)$' \
  -count=1 -timeout=15m -v
git diff --check
```

The exact final combined gate passed in 46.743 seconds:
`TestBackendVertical` in 22.70 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.18 seconds, and
`TestBrowserSequenceGapRecovery` in 11.22 seconds. A read-only Docker check
confirmed that no Open Splunk/ClickHouse test container remained. Three final
adversarial rereviews found no concrete issue after the causal GET2 barrier
and explicit close-completion promises were added.

The `ed28182` follow-up first made the no-post-gap-preview case red: the real
terminal arrived, but the banner remained `resyncing` for the full assertion
window. The completed-only status fix then passed the strengthened real Chrome
case, its race-enabled run, a three-run timing repetition, both recovery cases,
the full Go/frontend/build gates, and the exact CI-equivalent gate. The final
combined run completed in 47.198 seconds:
`TestBackendVertical` in 23.33 seconds,
`TestBrowserSequenceExpiredRecovery` in 12.17 seconds, and
`TestBrowserSequenceGapRecovery` in 11.28 seconds. Three blocker-focused
rereviews found no remaining issue after completed-state gating, exact
zero-row visible and aria-live copy, immediate forbidden-append rejection, and
final status removal were pinned.

### Browser sequence-expiration recovery validation

The exact browser proof at `ad4a77b`, built on the production recovery
implementation at `8f6d569`, passed the complete ordinary suite,
affected-package race suite, all 32 frontend tests, static gates, production
build, the dedicated staged Chrome recovery case, and the combined real
Docker-backed ClickHouse/Chrome vertical:

```sh
go test ./... -count=1
go test -race ./integration ./internal/server ./internal/searchws -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^TestBrowserSequenceExpiredRecovery$' -count=1 -timeout=12m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration \
  -run '^Test(BackendVertical|BrowserSequenceExpiredRecovery)$' \
  -count=1 -timeout=12m -v
git diff --check
```

The dedicated final case completed in approximately 21 seconds; the combined
gate completed in approximately 34 seconds. The staged real-browser case also
passed under `go test -race`, the focused harness race tests passed ten
repetitions, and the process-isolation review verified that no staged
repository or raw `.next`/`out` collision remained.

The recovery test proves exact `K` retention across an expired replay,
an actual transient HTTP 503, rejection of a stale versioned snapshot,
suspension while new live events arrive, queued-frame application only after a
successful three-row authoritative snapshot, rejection of an in-flight
snapshot captured before a later live update, and normal preview-to-final
operation on the recovered connection. Final focused semantics,
fixture-correctness, and false-positive reviewers found no remaining concrete
issue after the upstream-snapshot capture order was hardened.

The preceding real-manager replay/expiration and offline-cancellation tests
had already passed 30 consecutive ordinary runs and five race-enabled
repetitions:

```sh
go test ./internal/server \
  -run '^TestSearchWebSocketRealManager(ReplaysOneEventThenExpiresSequence|ReplaysCancellationAfterOfflineSocket)$' \
  -count=30
go test -race ./internal/server \
  -run '^TestSearchWebSocketRealManager(ReplaysOneEventThenExpiresSequence|ReplaysCancellationAfterOfflineSocket)$' \
  -count=5
```

### Resumable WebSocket checkpoint validation

The final implementation passed the full ordinary Go suite, the WebSocket
package under the race detector, all 29 frontend protocol tests, TypeScript
type checking, lint, the production frontend build, and patch cleanliness:

```sh
go test ./... -count=1
go test -race ./internal/searchws -count=1
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
```

The final focused backend regressions were:

```sh
go test ./internal/searchws \
  -run '^(TestCompletedTargetRemovedAtFirstExpiryPollRetiresTarget|TestWebSocketSubscriptionIdentityLimitRequiresReconnect)$' \
  -count=1
```

The first-expiry and identifier-exhaustion tests also passed 20 repeated runs
under the focused adversarial review. A full repository race run had passed
before the last two small boundary fixes; the final changed Go package then
passed its complete race suite. No real-browser disruption claim was made by
that earlier checkpoint; it was subsequently completed by the real recovery
checkpoint above.

### Crash-restart checkpoint validation

The real collector/server restart vertical passed twice on the final restart
implementation:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration -run '^TestBackendVertical$' -count=1 -v
```

The runs completed in approximately 24 and 17 seconds. Each hard-killed and
restarted both processes while reusing the same durable state. The permitted
checkpoint-before-local-WAL-ack boundary produced five physical ClickHouse
rows: four distinct stable event IDs and one replay of only the final
sentinel. `dedup event_id` returned the required four logical rows through the
query API and browser. The final clean shutdown reached the exact EOF
checkpoint and drained the collector WAL.

The ordinary, generated-code, race, static, and frontend gates passed on the
same implementation:

```sh
go test ./... -count=1 -timeout=5m
go test -race ./internal/collector/... -count=1 -timeout=5m
go vet ./...
go build ./...
make proto
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --check
```

Focused framing/input tests were rerun after the final formatting pass.
Independent final correctness and performance rereviews reported no remaining
checkpoint blocker. The performance review confirmed constant-memory,
cancellation-aware oversized-record discard, O(1) legacy checkpoint opening,
an 80-byte compact source mark, and no regression in large acknowledgment
aggregation. A pre-existing `time.After` allocation on each tailer poll remains
a later high-source-count profiling improvement; it is not on the crash path.
No ephemeral `open-splunk-*` container remained after the real verticals.

### Exact GradeThis corpus validation

The exact GradeThis corpus passed against the production-pinned ClickHouse
image:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
go test ./internal/queryexec \
  -run '^TestGradeThisCompatibilityV0_1AgainstClickHouse$' \
  -count=1 -timeout=6m -v
```

The full existing query-executor integration suite also passed after its
compiler/executor smoke switched to the shared manifest:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' \
  -count=1 -timeout=6m -v
```

The exact test starts one container, applies production migrations, decodes
and stores one 20-event batch, runs the ten sequential jobs, and cleans up in
about seven seconds locally. The broader executor suite completed in about
twelve seconds. Do not count the ordinary environment-gated skip as database
validation.

The complete release-path test passed twice after adversarial hardening. The
final run completed in 19 seconds:

```sh
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
OPEN_SPLUNK_BROWSER_EXECUTABLE="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
go test ./integration -run '^TestBackendVertical$' -count=1 -v
```

The browser override is optional when Playwright's pinned Chromium is
installed. The test itself builds the backend-mode frontend and both Go
binaries, starts pinned ClickHouse, runs the collector/server/browser flow,
and removes its ephemeral container and processes.

The ordinary backend and frontend gates passed against the final code:

```sh
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
npm run test:frontend
npm run typecheck
npm run lint
npm run build
```

The GitHub Actions YAML parsed successfully, `git diff --check` was clean, and
the Docker inventory contained no `open-splunk-*` test container.

The latest corpus-specific review passes reported no remaining correctness,
security, performance, reuse, or code-quality checkpoint blocker. The focused
review also ran the affected packages with the race detector. The older
collector-to-browser re-audit likewise reported no remaining blocker after its
generated-code, exact-job/progress, UI-transition, promise-lifecycle,
external-resource, bounded-output, and cleanup fixes.

### Previous scalar-extrema validation evidence

The full ordinary backend suite passed after commit `90f7bbe`:

```sh
go test ./... -count=1 -timeout=5m
go vet ./...
go build ./...
```

The frontend/static-application gates passed:

```sh
npm run lint
npm run typecheck
npm run test:frontend
npm run build
```

The full Store integration suite passed against the production-pinned image:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1
```

The extrema coverage in that suite includes:

- numeric-only, lexical-only, mixed, punctuation, zero, null, missing, and
  multivalue groups;
- whitespace, `NaN`, overflowing exponents, empty String, invalid UTF-8, and
  overlong lexical fallback behavior;
- nested/container and flattened-object guards;
- incomplete `BY` eligibility, projected-away input, global/grouped empties,
  and tenant-scope poison isolation;
- downstream `bin`, re-aggregation, native Bool, and distinct nanosecond
  timestamps; and
- `EXPLAIN actions=1` proof of shared aggregate states with no `ArrayJoin`.

The full query executor and manager suite also passed against the pinned
ClickHouse image:

```sh
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' -count=1
```

This pins parser-to-manager nullable `Mixed` transport, including numeric,
String, null, and binary `_raw` extrema results.

The opt-in lowering benchmark passed against ClickHouse `26.3.17.4` from
`clickhouse/clickhouse-server:26.3.17.4`. The resolved multi-platform OCI index
digest was
`sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49`.
With 2,000,000 generated rows, seven measured queries, `max_threads=1`, and
`use_query_cache=0`, it reported:

| Lowering | Client time/op | Server median | Average memory | Peak memory | Rows/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Guarded Array | 360.2 ms | 359 ms | 8,320,064 B | 9,081,959 B | 5,571,031 |
| Scalar tuple | 333.3 ms | 331 ms | 6,860,420 B | 6,860,964 B | 6,042,296 |

For this corpus, the scalar helper was about 7.8% lower in server median,
17.5% lower in average memory, and 8.5% higher in rows/second. These are
checkpoint observations, not timing assertions or universal workload claims.
Reproduce them with:

```sh
OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
go test ./internal/clickhouse -run '^$' \
  -bench '^BenchmarkStatsExtremaLowering$' -benchtime=7x -count=1 -v
```

The focused post-review compiler/depth tests and `git diff --check` also
passed. No `open-splunk-*` test container remained at the checkpoint. Do not
count a skipped opt-in test as database validation.

## Current backend state

The backend now includes:

- durable collector queue/checkpoint and ingestion acknowledgement coupling;
- scoped ingestion tokens, canonical typed events, and ClickHouse storage;
- bounded search jobs with cancellation, history, progress, typed paging,
  result leases, timelines, field catalog/summary, and CSV/JSONL export;
- binary protobuf WebSocket preview/progress with job-state revisions, replay,
  explicit resynchronization acknowledgement, coalesced REST recovery, bounded
  single-use subscription identities, expiration retirement, and reconnect
  backoff;
- the documented SPL v0.1 base search and Boolean/comparison expressions;
- `fields`, `table`, `rename`, `sort`, `head`, `tail`, and `dedup`;
- the documented `eval`/`where` subset;
- `stats` with row `count`, exact `count(field)`, `dc`/`distinct_count`,
  `values`, typed `min`/`max`, `sum`, `avg`, and `p95`;
- `top`, `rare`, `timechart`, and bounded two-field `chart`;
- extraction-mode `rex`, explicit-span exact `bin`/`bucket`, and bounded
  explicit-path JSON `spath`;
- resource limits for rows, bytes, time, memory, commands, generated SQL,
  relational depth, extraction work, exact distinct state, and multivalue
  publication; and
- materialized-CTE single-scan lowering for runtime-wide and
  analyzer-sensitive paths.

## GradeThis v0.1 corpus completion

All ten exact searches in `docs/product-architecture-plan.md` are implemented
and share one `internal/testsupport/gradethiscorpus` manifest.
`internal/clickhouse/compatibility_corpus_test.go` sends that manifest through
parse, plan, and compile in the ordinary suite. The existing broad executor
smoke also consumes the manifest while keeping its unrelated
`distinct_count(logger)` extension clearly separate from the named ten.

`internal/queryexec/gradethis_compatibility_v0_1_integration_test.go` is the
authoritative exact acceptance test. It starts one pinned ephemeral ClickHouse
server, decodes the generated NDJSON with the collector, stores one batch
through the production Store and visibility ledger, and executes the ten jobs
sequentially through owner/tenant-scoped signed three-row pages. It checks the
terminal job schema and row count, every page's complete schema and stable
total, global row ordinals, every typed cell, cursor progress, and terminal
cursor exhaustion. Wrong-owner and wrong-tenant requests return not found, and
a modified cursor is rejected.

The committed deterministic synthetic profile has a fixed UTC base and a
pinned byte hash. Its 20 events use unique aggregate counts to avoid ambiguous
order:

- 11 `INFO`, 6 `ERROR`, and 3 `WARN` events;
- ten exact `Request metrics` events: seven
  `/api/v1/assessments` requests at `800ms` and three
  `/api/v1/submissions` requests at `300ms`;
- assessment statuses `200` four times and `503` three times; submission
  statuses `200` twice and `500` once;
- four `Heartbeat`, three `Dependency retry scheduled`, two
  `Database request failed`, and one `Request started` message; and
- one two-event synthetic trace, with only its database error containing
  `connection refused`.

This produces deterministic high-level results: severity counts
`INFO=11, ERROR=6, WARN=3`; response counts `assessments/200=4`,
`assessments/503=3`, `submissions/200=2`, `submissions/500=1`; the slow-route
result `assessments, count=7, p95_ms=800`; and top-message percentages
`50%, 20%, 15%, 10%, 5%`. NDJSON emission and decoded expected data derive
from that one profile rather than a hand-written ClickHouse fixture. A
100-year test retention keeps fixed historical rows ahead of ClickHouse TTL
without approaching `time.Duration` overflow.

The fixture contains only synthetic IDs, relative callers, TEST-NET IPs, and
reserved example data. Its ordinary scanner rejects secret-like keys,
credential-like values, email/user/session identifiers, SQL, stack traces,
workstation paths, production URLs, and non-documentation IPv4/IPv6
addresses. It also rejects duplicate JSON keys and does not echo rejected key
text in diagnostics. The local root `app.log` is large, ignored, and contains
user IDs, network metadata, SQL, stack traces, absolute paths, and secret-like
fields; never copy or commit it.

Raw-event result rows preserve sparse dynamic-field presence. The compiler
adds an internal names column only when the public raw `fields` payload is
present; the executor validates it, removes ClickHouse part-level phantom
nulls, and synthesizes explicit null only for a path named as present. Shared
canonical path parsing prevents literal-dot/backslash/percent collisions. One
fixture event carries `optional_note:null`, while neighboring events omit it,
and exact ordered-row assertions prove those two states remain distinct.

The aggregate normalized-name budget is an independent 1 MiB per durable
event. The validator applies it to the post-redaction clone with an
allocation-free early exit using the shared encoder's byte accounting, the
Store applies it defensively before writing, and result conversion applies it
before constructing a public object. Tests cover prefix-amplification within
the 1 MiB protobuf envelope and prove that a sensitive amplified subtree which
redaction collapses is still accepted.

Two compatibility facts must remain explicit:

- current local GradeThis request logs use `Request summary statistics`,
  whereas the product plan and exact v0.1 corpus use `Request metrics`; and
- real durations include microseconds, milliseconds, and seconds, whereas the
  plan's literal `replace(duration, "ms$", "")` query covers milliseconds
  only.

The corpus pins documented Open Splunk v0.1 behavior, including the current
approximate `p95`; it does not claim live Splunk oracle parity.

The frontend adapter regression uses a real generated-protobuf-shaped
`message,count,percent` result and now classifies `percent` as a measure, so
`top message` retains its statistics table and gains the intended categorical
visualization. A later release-path slice should reuse the manifest through
the protobuf server and one browser session rather than launching ten
independent stacks.

## Resume checklist

1. Work only from `main`; fast-forward it from `origin/main` and confirm the
   worktree is clean.
2. Read this file, `docs/product-architecture-plan.md`, and
   `docs/spl-compatibility-v0.1.md`.
3. Run `npm ci`, `go test ./...`, `npm run test:frontend`,
   `npm run typecheck`, and `npm run lint`.
4. If the next change touches the release path, install the pinned browser with
   `npx --no-install playwright install chromium` and run:

   ```sh
   OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
   go test ./integration \
     -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
     -count=1 -timeout=15m -v
   ```

5. Start with logical `expires_at > IndexTimeCutoff` filtering in the common
   ClickHouse scan, unless the user explicitly changes priority. Clock-driven
   job/result/export expiration is complete at `b2b2839`; stale-duplicate
   injection is complete at `b80bf0a`. Add the pinned ClickHouse red test
   before implementation, run read-only adversarial reviews, fix concrete
   findings, then commit and push `main`.

## Remaining work, in priority order

### 1. Finish the first-release product proof

The uninterrupted collector-to-browser gate, exact GradeThis v0.1 corpus,
collector/server process-restart proof, resumable WebSocket unit contract,
real-browser retained replay, real-manager one-event replay expiration, and
offline cancellation publication/replay are complete. The dedicated real
browser `SEQUENCE_EXPIRED` fixture now also proves exact checkpoint retention,
stale-snapshot rejection, a genuine transient HTTP 503, queued-frame
suspension/application, live-update fencing, and same-connection
preview-to-final behavior without re-execution. The explicit sequence-gap
fixture is also complete: it drops `K+1`, forwards `K+2`, proves exact
reconnect/replay and live continuation, fences REST-first/replay-later progress
by the manager's state revision, and requires normal terminal socket cleanup
without re-execution. Its REST-only companion withholds the full terminal
projection, accepts completion only through authoritative polling, and proves
zero-row finalization behind a gated result response. The browser now
single-flights the whole REST recovery cycle, so duplicate triggers cannot
multiply backoff or follow-up scheduling. Honest browser cancellation is also
complete: it proves exactly one cancel POST, one executor context cancellation,
authoritative canceled presentation, and zero post-cancel reconnects.
Recovered-socket stale-duplicate fencing is complete as well: the browser
receives exact old preview and RUNNING frames after contiguous recovery, never
regresses preview or terminal presentation, accepts subsequent live frames,
and neither reconnects nor issues an extra authoritative GET. Clock-driven
terminal job/result/export expiration is now complete too: exact expiry
boundaries, preview and artifact invalidation, pinned leases, tombstone
retirement, socket closure, timer release, and polling quiescence are proven
through real managers and the production HTTP/WebSocket handler.

Before adding another ad hoc SPL feature, close the remaining event-retention
correctness gap:

1. Add a pinned ClickHouse integration test which inserts an event whose
   `expires_at` is at or before the immutable search `IndexTimeCutoff`. Keep
   that expiry in the ClickHouse server's future while choosing a still-later
   artificial search cutoff, and use a raw ClickHouse query to prove the row
   physically exists immediately before the compiled-scan assertions. Then
   prove it cannot appear in search, preview, export re-execution, timeline,
   field discovery, or field-summary paths.
2. Make the common compiled scan predicate require
   `expires_at > IndexTimeCutoff`, alongside the existing tenant, index,
   event-time, index-time, and visibility cutoffs. Use the immutable job cutoff
   rather than ClickHouse `now()` so retries, previews, results, and exports
   retain one snapshot. Pin exact-millisecond equality and just-after-boundary
   behavior.
3. Rerun every pinned ClickHouse compiler/executor suite and the exact
   collector-to-browser gate. Confirm the predicate remains visible in
   generated SQL/arguments and cannot be removed by later SPL stages.

ClickHouse table TTL deletion is merge-driven physical reclamation; it is not
an immediate authorization/visibility boundary. Until the explicit predicate
above is implemented, an expired physical row may remain searchable between
its retention deadline and a background merge.

After that retention fix:

- Close any remaining preview-to-final resource-release coverage that is not
  naturally exercised by the cancellation, recovery, and expiration fixtures.
- Exercise a sanitized real GradeThis log/config migration: collector to the
  `gradethis` index with no OpenTelemetry component in the log path, then run
  trace-ID, severity, request-status, path, duration, and top-message
  investigations.
- Record a load/performance run at sustained 1,000 events/second, including
  collector offline recovery, slow WebSocket consumers, concurrent searches,
  high-cardinality exact aggregates, ClickHouse part count, scan budgets, and
  browser rendering cost. Acceptance thresholds still require the hardware,
  retention/event-size, and concurrency decisions listed below.
- During high-source-count collector profiling, replace the pre-existing
  per-poll `time.After` allocation with a safely reused timer if it is material;
  preserve cancellation and copy-truncate behavior with race coverage.
- Verify release revision consistency across embedded UI, server, protobuf
  schema, and migrations, plus byte-identical embedded frontend assets for
  Linux and macOS builds from the same source revision.
- Keep `app.log` only as local test input after a fixture secret scan. Do not
  commit unsanitized GradeThis production logs.

CI currently rebuilds the frontend in the frontend, vertical, and packaging
jobs. A follow-up should cache the vertical's Next.js work or, preferably,
build the backend-mode UI/server/collector once and pass the exact tested
artifacts to packaging without making the acceptance test depend on stale
outputs. Cache the pinned Playwright browser download where the CI environment
supports it.

The browser child output and observation counts are bounded, but build-command
failure output still uses `CombinedOutput`. Complete the harness hardening with
the same capped buffer, cap each recorded diagnostic's byte length, and bound
the number of simultaneously observed matching WebSockets.

At this checkpoint, `npm audit --omit=dev --audit-level=critical` exits
successfully but reports three high-severity findings in Next.js's transitive
PostCSS/Sharp chain. npm's offered force-fix would install the breaking
`next@9.3.3` downgrade; do not apply it blindly. Re-evaluate a safe Next.js,
PostCSS, or Sharp upgrade/override with the complete frontend and browser gates.

### 2. Continue TDD on aggregate correctness and efficiency

The scalar-String extrema optimization is complete. If SPL expansion is the
chosen next priority, implement one bounded aggregate contract at a time:

- `list(field)` must be order-sensitive, duplicate-preserving, and explicitly
  bounded; do not adapt unordered `values`;
- `earliest(field)` / `latest(field)` need explicit event-order, tie-break,
  null, multivalue, type, and precision contracts;
- broader `count` forms (`c`, wildcards, predicates, and `count(eval(...))`)
  need separate syntax and differential tests;
- remaining percentile forms need explicit approximation/error and resource
  contracts;
- `eventstats` should follow only after the aggregate library is stable;
  `streamstats` remains outside the first release unless scope changes; and
- exact Decimal comparison/aggregation remains separate work from the current
  finite-`Float64` runtime compatibility path.

Extend `chart`, `rex`, or `spath` only behind compatibility and pinned
ClickHouse tests. Deferred `spath` surface includes auto-extraction, `{}`
multivalue output, XML, terminal containers, escaped literal-dot keys, and the
`spath()` eval function.

### 3. Improve compiler maintainability

- Move pending aggregate publication/validation descriptors out of persistent
  `compileState` into a typed aggregate-compilation result.
- Replace string-prefix presence rebinding with an explicit field-presence
  policy shared by projection, rename, eval, extraction, and aggregate output.
- Consolidate exact-string/candidate descriptors without weakening dc-only
  scalar lowering, values/dc sharing, extrema type propagation, or two-stage
  validation depth.
- Split the large pinned ClickHouse fixture/bootstrap and aggregate assertions
  into smaller helpers while retaining one reusable ephemeral container.
- Keep regressions for physical aggregate sharing, later hidden overflow,
  binary multivalue transport, tenant/index/time scope poison, calculated
  overwrite, and relational depth.

### 4. Continue Phase 3/4 product hardening

- Per-index retention and permissions, index/app administration, collector
  fleet operations, reports/dashboards, HEC compatibility, RBAC, and audit
  search.
- Migration upgrades, backup/restore and disaster recovery, load shedding,
  fair scheduling, per-user concurrency, alerts/scheduled searches, packaging,
  installers, upgrades, and signed releases.
- Keep the first release single-node; distributed control/search remains
  outside the current plan.

Resource/lifecycle audit findings to retain in the backlog:

- Bound or physically prune ingestion-token tombstones so token listing does
  not remain O(all historical tokens).
- Move export artifact filesystem deletion outside the manager-wide read lock,
  surface background deletion failures, and let admission perform due cleanup
  before reporting capacity exhaustion.
- Physically prune idle search-history owner rows and replace the
  process-global SQLite visibility lease registry with ownership that cannot
  retain closed database pointers indefinitely.
- Finish the deleting-index lifecycle instead of leaving indexes permanently
  in an intermediate state.
- Give WebSocket service shutdown a bounded dependency contract; `Close`
  currently relies on snapshot providers honoring cancellation before its
  ownership barrier can complete.

The architecture plan still requires product decisions for capacity-planning
retention/event size, target hardware, concurrent search load, immediate
Windows collector support, and whether dashboards are first-release scope.
Do not guess those decisions if they materially affect the implementation.

## Known compatibility boundaries

- A live licensed Splunk differential oracle is unavailable. Public
  documentation leaves several multivalue, null, binary, symbol-ordering, and
  error behaviors unspecified; keep Open Splunk choices explicit.
- Runtime String/Dynamic extrema compare numeric candidates through `Float64`.
  Distinct very-wide integers or exact decimals can collapse to the same key.
  Do not silently claim exactness until an exact decimal comparison key and
  live oracle exist.
- Numeric candidates sort before all lexical candidates in Open Splunk v0.1;
  punctuation within the lexical class uses raw-byte order. Symbol placement
  is not claimed as verified Splunk parity.
- Public Splunk documentation establishes ordinary occurrence semantics for
  `count(field)` but does not pin null multivalue members or typed containers.
  Open Splunk counts immediate non-null containers atomically.
- Collector decoding does not preserve every original numeric token spelling.
  String-oriented aggregates operate on stored canonical values.
- Default aggregate names containing parentheses cannot be referenced by the
  downstream field grammar; use `AS`.
- The 512 KiB `values` bound is a publication bound, not an aggregate-state
  memory guarantee. ClickHouse query memory remains authoritative before the
  post-aggregate check.
- Duplicate JSON member selection follows the pinned ClickHouse parser's
  first-member behavior.
- Do not accept a changing legal agreement or start a licensed Splunk image on
  the user's behalf merely to obtain an oracle.

## Safe resume procedure

1. Confirm `main` is clean and exactly matches `origin/main`.
2. Read the three documents listed at the top and inspect the latest `main`
   commits, especially `b2b2839`, `b80bf0a`, `cdb60df`, `787a7f9`, and
   `522b0ac`; the preceding progress/recovery foundations are `b5502a3`,
   `f72f184`, `ed28182`, and `d1286a4`.
3. Confirm no stale `open-splunk-*` Docker test containers are running.
4. Run the ordinary Go/frontend gates above and the focused exact corpus:

   ```sh
   OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
   go test ./internal/queryexec \
     -run '^TestGradeThisCompatibilityV0_1AgainstClickHouse$' \
     -count=1 -timeout=6m -v
   ```

   Run both broader opt-in pinned ClickHouse suites before changing
   extrema/bin metadata behavior.
5. Unless the user changes priority, begin with the pinned ClickHouse
   logical-retention test and common `expires_at > IndexTimeCutoff` scan
   predicate described above. Clock-driven job/result/export expiration,
   recovered-socket stale-duplicate fencing, authoritative browser
   cancellation, versioned REST-first/replay-later progress, recovery-cycle
   coalescing, REST-only and accepted-WebSocket terminal discovery after a
   sequence gap, explicit browser sequence-gap recovery, browser
   `SEQUENCE_EXPIRED`, transient recovery-GET failure, retained replay,
   real-manager expiration/cancellation, the uninterrupted collector-to-browser
   path, exact GradeThis corpus, collector/server process-restart proof, and
   the protocol unit contract are already complete.
6. If extending aggregates instead, start with an explicit bounded contract
   for `list(field)`; do not reuse unordered `values`.
7. Preserve scalar/Dynamic path separation, numeric grammar sharing,
   punctuation/UTF-8/zero/overlong boundaries,
   native timestamp precision, private calculated types, downstream `bin`,
   re-aggregation, scope poison, binary transport, physical state sharing, and
   relational-depth regressions.
8. Keep working on `main`; commit and push every cohesive green slice.
9. Preserve unexpected local changes and never reset them away.
