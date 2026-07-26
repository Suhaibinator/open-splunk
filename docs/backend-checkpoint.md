# Backend checkpoint handoff

This is the canonical restart document for backend work. Read it together
with:

- `docs/product-architecture-plan.md`
- `docs/spl-compatibility-v0.1.md`
- the latest `main` commit

## Latest checkpoint: bounded browser statistics rendering

Date: 2026-07-26

Implementation/proof commit: `9d6acc11f2626f92d5ddd2b4e608a1268cc0c9e3`

This slice makes large statistics results bounded, usable, and measurable in
the real compiled browser application:

1. The production statistics table now virtualizes when either row count
   exceeds 100 or materialized data cells would exceed 2,048. It renders a
   six-row overscan around the viewport, uses native table spacer rows, and
   exposes accurate ARIA row counts and indexes.
2. Generic and timechart result tables use fixed layout, bounded column
   geometry, clipped cells, a sticky header, and a named keyboard-scrollable
   region. The table remains horizontally scrollable rather than expanding the
   page, including at the supported 64-column browser boundary.
3. Virtual scroll state resets on result generation, page, sort, and density
   changes; it deliberately survives live-preview growth. Shrink, threshold,
   column-count, and viewport transitions clamp both the logical window and
   the physical scroll offset, including transitions back to an unvirtualized
   table.
4. Browser result boundaries now reject schemas outside 1–64 columns and REST
   pages larger than the requested page size. The same width check applies to
   zero-row live previews and again inside the frontend adapter as a defensive
   boundary.
5. A deterministic real-browser fixture loads the production protobuf HTTP
   handler, search manager, compiler, and compiled backend-mode UI. Its
   executor returns exactly 1,000 rows by 64 columns with a deterministic
   schema, checks its width and key names plus every ordinal, first/last
   sentinels, unique row IDs, and terminal metadata, and records payload hash
   and size. It does not substitute a mock page or bypass the application
   adapter.
6. The browser proof checks the top and bottom sentinels, keyboard `End`,
   sticky-header behavior, ascending and descending sort, standard and compact
   density, and a 1024×768 viewport. The current initial DOM contains 18
   materialized rows, one spacer, 19 total body rows, and 1,216 materialized
   cells for a 10,240-pixel-wide table.
7. Independent mutation tracking spans initial render and the complete
   scroll/sort/density/resize interaction flow. It requires peaks of at most 32
   materialized rows and 34 total body rows; checkpoint assertions additionally
   cap spacers at two and materialized cells at 2,112. The final green run
   observed combined peaks of 25 materialized and 27 total body rows. Self-tests
   inject both 1,000 ordinary rows and 1,000 spacer rows on a disposable page,
   proving synchronous transient explosions cannot evade the observer or
   contaminate measured target-page metrics.
8. Navigation/resource timing uses the actual result-response end, stable DOM
   plus two animation frames, a cleared and enlarged resource-timing buffer,
   drained performance observers, and interval filtering. Render timings and
   long-task/layout-shift values are explicitly observational rather than
   release thresholds.
9. Browser process orchestration now shares one runner, bounds retained child
   output, cleans process groups, validates finite required metrics, and writes
   screenshots, metrics, and bounded logs for CI artifacts. The required
   backend-vertical CI job includes the fixed-rendering proof and has explicit
   job/package timeouts.
10. Three independent final reviewers recomputed staged patch SHA-256
    `3af2a8373ff9900d862415851c84ccbd35b69462bda8325c939054c99792a732`.
    After adversarial iterations covering row/cell bounds, transient DOM
    mutation escape hatches, schema/page limits, scroll transitions, observer
    lifetime, resource timing, isolation, and process cleanup, all three
    reported the frozen patch clean.

The exact validation record is under **Latest validation evidence**. Browser
rendering for the fixed first-release payload is complete at this checkpoint.
The next priority is high-source-count collector profiling and the
pre-existing per-poll timer allocation described under **Remaining work**.
The overall backend goal remains active.

## Previous checkpoint: bounded WebSocket consumers and replay recovery

Date: 2026-07-26

Implementation/proof commit: `4c4003f`

This slice makes slow WebSocket consumers deterministic, bounded, isolated,
and recoverable on both sides of the transport:

1. A real-server integration fixture wraps exactly one accepted TCP
   connection with a test-only write gate. With the production minimum
   seven-frame queue, the first progress frame is held in flight, the next six
   remain queued, and the eighth new progress frame closes only that slow
   connection. The test does not depend on kernel-buffer timing or impose a
   performance threshold.
2. A healthy sibling receives every progress sequence contiguously while the
   slow writer is blocked. Authoritative REST state advances throughout and
   the search executor is admitted exactly once, proving that transport
   backpressure cannot stall or repeat search execution.
3. The abandoned checkpoint deliberately falls outside the retained replay
   suffix. Reconnection receives exact `SEQUENCE_EXPIRED` bounds and recovery
   path, performs authoritative REST recovery, reconnects at the server's
   latest sequence, receives subsequent live progress, and converges with the
   healthy sibling on identical terminal state. The executor still runs once
   and the deterministic clock finishes with no poll waiters.
4. Server preview pressure no longer reports an undelivered disposable
   preview as success. The pressured connection is closed for replay, the
   canonical event remains retained, and healthy siblings continue. Both live
   publication and initial subscribe/replay tailoring use a dedicated,
   cancellation-aware bounded-work gate rather than charging temporary
   canonical copies to the connection queue budget; only final stamped frames
   consume queue capacity.
5. The browser client now owns a client-wide inbound bound, including the
   active listener and every pending generation: 64 frames and 4 MiB by
   default, with validated positive safe-integer overrides. Direct
   `ArrayBuffer` values and all views are copied into exact-size owned storage,
   preventing caller mutation and oversized backing-buffer retention; immutable
   `Blob` values are charged by size.
6. Inbound overflow closes the originating socket with application code 4000
   and requests replay without advancing the checkpoint. Listener work is
   serialized globally across socket generations, so a stale callback
   completes before replacement-generation callbacks. Automatic reconnect is
   deferred while that callback is active, while intentional disconnect
   cancels the deferred reconnect.
7. Every callback-triggered restart is fenced to its originating socket and,
   after awaited callbacks, to the same subscription object. Delayed close,
   sequence-gap, send-error, disconnect/reconnect, and unsubscribe/resubscribe
   races therefore cannot close a replacement socket or commit a stale
   checkpoint.
8. The browser queue uses a logical head and periodically compacts only after
   at least 64 consumed entries and proportional progress. This removes
   repeated `Array.shift()` work and bounds physical slot retention with
   amortized linear processing under a sustained nonempty backlog.
9. Controlled reds reproduced the exact server defect: with unrelated queued
   data leaving room for the final acknowledgment plus one-row preview, the
   old initial replay path closed the healthy subscriber because it charged
   the larger two-row canonical scratch copy. Browser regressions likewise
   pinned active-plus-pending frame/byte bounds, mutable buffer ownership,
   stale-listener ordering, replacement-socket fencing, delayed close, and
   queue compaction.
10. Three independent final reviewers recomputed staged patch SHA-256
    `887f03b62962c1aacb45b992f0481b1a7c5f04b6cc408f1d81fa5dc648c9670c`
    and reported no remaining correctness, replay/sequence, concurrency,
    accounting, leak, deadlock, efficiency, performance, determinism, reuse,
    or code-quality finding.

The exact validation record is under **Latest validation evidence**. No Docker
fixture was launched for this slice. The next priority is a separate browser
rendering measurement for a fixed 1,000-result payload with stable-DOM and
animation-frame gates. The overall backend goal remains active.

## Previous checkpoint: concurrent SPL searches during live recovery

Date: 2026-07-26

Implementation/proof commit: `9898b41`

This slice extends the real-process sustained-load proof with deterministic
public SPL searches while the collector is actively recovering:

1. Each concurrent cohort contains eight ready-gated goroutines. All workers
   reach the gate, signal readiness, and wait on one closed channel before
   constructing and admitting their protobuf HTTP searches. Admission errors
   are returned through a fully buffered channel and drained by the parent;
   worker goroutines never call `testing.T` methods.
2. The harness runs rolling cohorts rather than one isolated burst. Cheap
   source and physical-row samples gate up to three waves, and the proof
   requires at least two. Source records and publicly visible results must
   both advance strictly while the generator and collector remain alive.
   Every result in a later cohort must expose at least the maximum visible
   prefix from the preceding cohort, preventing a single high result from
   hiding regressions in its peers.
3. Each result proves an exact contiguous source prefix through public SPL:
   `count`, distinct event/request/user/timestamp counts, and minimum/maximum
   timestamps. The event, request, and timestamp cardinalities must agree;
   the first timestamp is the fixture start and the last is exactly
   `start + (events - 1)ms`. Schema, nullability, public job metadata,
   completion state, truncation state, scan counters, and unique job IDs are
   pinned as well.
4. Raw ClickHouse row counts are deliberately only a later observational
   upper bound. The collector calls `prepared.Send()` before its SQLite
   visibility transaction commits, so physical storage cannot safely be used
   as a lower-bound proxy for the public searchable prefix.
5. A liveness timer is derived from the remaining paced source window with
   one-second/flush headroom. It bounds only the cheap progress-poll phase,
   is created after a fresh sample, and is canceled before a search cohort.
   Search requests retain their separate 60-second deadline, and cohort count
   is independently capped at three.
6. Timing remains observational. The harness records server-internal
   `[CreatedAt, FinishedAt)` lifecycle duration, client monotonic whole-window
   wall time, scan rows/bytes, queue wait, and half-open
   `[StartedAt, FinishedAt)` maximum active overlap. It imposes no latency or
   overlap threshold and does not compare concurrent results with a final
   search over a different data set.
7. The protobuf HTTP harness now has a shared error-returning request helper;
   the existing fatal test wrapper delegates to it. Pure tests pin rejection
   of gapped timestamp prefixes, every-member cohort regression, half-open
   overlap arithmetic, and the sustained-load plan's derived liveness window.
8. Controlled reds first captured the missing validator, then showed that a
   plausible count/unique-ID result with a gapped timestamp range was wrongly
   accepted. Review iterations removed unsafe physical-row lower bounds,
   mixed-clock comparisons, weak cohort advancement, hidden latency
   thresholds, retry amplification, misleading overlap claims, and a timer
   that could outlive polling.
9. The exact final Docker run completed in 46.14 seconds. Its two cohorts
   admitted 16 public jobs while source records advanced from 11,200 to
   11,400, physical rows from 6,500 to 7,700, and exact searchable prefixes
   from 6,500 to 7,100. Client wall time was 208.172 ms, lifecycle span
   202.901 ms, and observed maximum active overlap four. The searches scanned
   108,800 rows and 61,881,298 bytes; lifecycle
   min/median/p95/max was 20.653/48.652/103.556/103.556 ms, maximum queue wait
   was 84.823 ms, and all final 30,000-event outage/restart assertions
   remained green.
10. Three independent final reviewers recomputed staged patch SHA-256
    `71d19acf2990b12db60e53cc50ecb0dc6c2abec063ceb5cdf69c7fb8845bc4fb`
    and reported no remaining correctness, determinism, concurrency,
    efficiency, performance, metrics, or code-quality finding.

The exact validation record is under **Latest validation evidence**. The next
load slice is a deterministic bounded-queue slow WebSocket consumer, followed
by separate browser rendering measurements for a fixed 1,000-result payload.
The overall backend goal remains active.

## Previous checkpoint: sustained-load outage and restart correctness

Date: 2026-07-26

Implementation/proof commit: `59b8f7c`

This slice adds the first measured real-process backend load proof and fixes
the collector restart defect that proof exposed:

1. Opt-in `TestBackendSustainedLoad` builds the real server, collector, and
   log-generator binaries, starts pinned ephemeral ClickHouse, and writes
   30,000 high-cardinality NDJSON events at a target 1,000 events/second. The
   schedule is fixed at 5,000 warm events, 6,000 server-offline events
   (5,000 required plus 1,000 source headroom), and 19,000 recovery events.
   It uses 10,000 possible users, one unique request ID per event, 1 ms
   timestamps, and 100-event output flushes.
2. The source proof independently scans the complete file, pins the three-phase
   timestamp and ordinal schedule, requires exact request-ID cardinality and a
   bounded high user cardinality, and builds a SHA-256 raw-record multiset.
   Stored rows must reproduce every raw record and its extracted request ID,
   user ID, ordinal, and event timestamp exactly.
3. After the warm prefix is stored, checkpointed, and acknowledged, the test
   kills the server, generates the bounded offline phase, and proves storage
   and the durable checkpoint cannot advance. The collector runs at debug
   level only for this proof; each counted append diagnostic is emitted after
   the `SyncAlways` WAL append returns. The test waits for diagnostics covering
   at least 5,000 durably appended events before crash-stopping the collector,
   then cold-reopens the WAL and independently requires a nonempty,
   unquarantined durable backlog covering the same offline window.
4. The first stronger controlled red found only 4,800 durable events when the
   source stopped exactly at the 5,000-event target. Adding 1,000 events of
   source headroom and synchronizing on post-append evidence made the gate
   deterministic without assuming the collector tracks source generation
   instantaneously. The final run crash-retained 5,700 events in 19 WAL
   batches from the exact 6,000-event offline source phase.
5. Restarting from that intact WAL originally produced 35,700 physical rows
   but only 29,700 distinct event IDs. The collector replayed the original
   durable batches while rereading their source bytes from the older terminal
   checkpoint; stable event IDs survived, but the reread minted new batch IDs
   and bypassed server batch-idempotency.
6. Collector startup now reconstructs the highest source coordinate owned by
   intact unacknowledged WAL batches and overlays it only on the file manager's
   resume view. The acknowledged checkpoint remains unchanged until terminal
   delivery. Manager-derived legacy cursor enrichment at or behind a pending
   coordinate is suppressed; terminal or newer-generation writes pass through,
   and acknowledged/superseded overlay entries are pruned. Resume lookup holds
   the overlay read lock across the durable lookup so a concurrent terminal
   commit cannot remove the pending coordinate after an old durable cursor was
   read. Once the overlay drains, it releases the map and switches manager and
   terminal paths to an atomic inactive fast path.
7. WAL recovery exposes pending source marks through a compile-time
   `ResumeQueue` capability. Aggregation is shared with acknowledgment
   planning, fails closed on metadata/cursor conflicts, stops at the intact
   prefix before a corrupt gap, and avoids a transient slice header per queued
   batch. The file manager receives a narrow manager-specific checkpoint
   interface rather than a proxy pretending to implement durable lifecycle
   operations.
8. Deterministic tests reproduce the restart defect as seven queued events
   where only four should exist, then pin the fixed count, unchanged terminal
   checkpoint, legacy cursor ephemerality, durable-equal preference,
   copy-truncate generation fencing, corrupt-gap fallback, and conflicting
   cursor/identity rejection. A blocking-store concurrency test also pins the
   exact old-durable-read versus terminal-prune race found by adversarial
   review.
9. The final Docker run stored exactly 30,000 rows with 30,000 distinct event
   IDs and exact raw/extracted values. It generated 12,531,099 source bytes in
   30.255 seconds, recovered 264 ms after backend health while the separately
   bounded recovery phase was actively generating, drained after generation
   in 346 ms, and left one EOF checkpoint, an empty acknowledged WAL, no
   quarantines, and an empty owner-only dead-letter file. ClickHouse reported
   three active parts, 7,299,197 compressed bytes, 34,495,468 uncompressed
   bytes, and 11,496,867 bytes on disk. Public SPL `stats count`/`dc` searches
   completed in 42.3 ms and 41.5 ms on this checkpoint machine.
10. Load polling now uses cheap row counts and runs exact distinct counting
    only at convergence, so the harness does not repeatedly allocate exact
    distinct state while measuring ingestion. Timings are observational,
    cache-affected checkpoint evidence, not universal acceptance thresholds.

The exact validation record is under **Latest validation evidence**. The
delivery contract remains at-least-once: identical durable batch retries are
server-deduplicated, while unavoidable crash-boundary source rereads retain
stable event IDs for explicit logical `dedup event_id`. Concurrent search is
now complete in the latest checkpoint above. The next load slice is a
deterministic slow WebSocket consumer, followed by separate browser rendering
measurements. The overall backend goal remains active.

## Previous checkpoint: load-generator pacing and live-output foundation

Date: 2026-07-26

Implementation/proof commit:
`860acac` (`harden log generator for sustained load`)

This slice makes `open-splunk-loggen` a trustworthy source for the next
real-process load proof:

1. Rate limiting now uses a reusable timer and absolute ordinal deadlines, so
   event generation, encoding, and ordinary writes do not accumulate as extra
   inter-event sleep. The schedule begins on the first event rather than
   before output setup.
2. A long scheduler or I/O stall cannot create an unbounded replay burst.
   Catch-up debt is capped at the smaller of 100 events or 100 milliseconds;
   the CLI describes `-rate` as a target rate with bounded catch-up rather than
   claiming a strict instantaneous maximum.
3. Invalid floating-point rates, sub-nanosecond and unrepresentably slow
   intervals, and ordinal/deadline overflow fail before output is opened.
   Context cancellation works both before a wait and while the reused timer is
   armed.
4. `-flush-events` gives a live tailer bounded user-space visibility without
   imposing a flush syscall on every unpaced fixture. `-append` uses
   `O_APPEND`, preserves complete existing records, and rejects a nonempty
   file whose trailing record lacks a newline.
5. Every post-open exit now flushes buffered bytes, syncs file output, and
   closes it while preserving all error causes. The command suppresses only
   error trees composed entirely of cancellation, so a shutdown-time
   flush/sync/close failure cannot be hidden as an ordinary interrupt.
6. A pre-canceled run returns before creating or truncating its output. Tests
   also force cancellation with a delimiter still buffered and an injected
   final-flush failure, proving the durability failure remains visible.
7. The controlled red suite reproduced relative-pacing drift, unbounded
   catch-up, cancellation-masked finalization failure, pre-cancellation
   truncation, and incomplete-record append corruption. Three independent
   adversarial reviewers then traced the timing, timer, overflow, I/O,
   cancellation, and file-lifecycle paths and reported the final patch clean.

The exact validation record is under **Latest validation evidence**. At this
checkpoint the next priority was the opt-in, pinned-ClickHouse sustained-load
integration harness; that core proof is now complete in the latest checkpoint
above. This commit remains the load-source foundation, not itself an
ingestion-capacity claim. The overall backend goal remains active.

## Previous checkpoint: shutdown-safe export artifact removal

Date: 2026-07-26

Implementation/proof commit:
`961cba2` (`serialize export artifact removal with shutdown`)

This slice closes the concrete export finding from the remaining
preview-to-final resource-release audit:

1. Export `Get` and `Cleanup` no longer hold the manager-wide lock while
   unlinking expired artifacts or partials. A blocked filesystem therefore
   cannot prevent `Close` from setting `closed`, canceling the manager, and
   rejecting new admission.
2. Both paths enroll in the existing admission barrier while `manager.mu`
   still excludes shutdown, then release the lock before filesystem I/O.
   `Close` sets `closed` under that same lock before waiting, so no positive
   `WaitGroup.Add` can race a zero-count `Wait`, and artifact storage cannot be
   torn down under an admitted remover.
3. Closing the final download lease had a separate shutdown race: it removed
   itself from manager accounting before performing a deferred unlink, so
   `Close` could miss the lease and return while that unlink was still
   blocked. A pre-shutdown final lease now admits its removal under
   `manager.mu`; once shutdown has begun it skips lease-side deletion and
   leaves the artifact to `Manager.Close`.
4. Worker-side temporary/artifact removal remains owned by `workers.Wait`,
   and secure redemption-time artifact opening remains covered by the
   preexisting admission. Per-entry removal flags continue to serialize
   concurrent cleanup, lookup, and lease-close attempts.
5. Three deterministic tests block the injected filesystem remover for
   `Cleanup`, request-triggered expiration through `Get`, and final-download
   lease close. They prove shutdown begins promptly, cannot return before the
   admitted remover, and performs exactly one artifact unlink. Bounded
   completion checks prevent a future deadlock from falling through to a
   package-wide timeout.
6. Controlled red runs reproduced both defects: `Get`/`Cleanup` blocked
   shutdown admission, and final-lease unlink allowed `Manager.Close` to
   return early. The focused cases then passed 100 ordinary repetitions and
   20 race-enabled repetitions.
7. Independent search-job/executor/snapshot review found the existing
   preview, final-result lease, cancellation, failure, expiration, and
   shutdown ownership paths clean. Two final export reviewers confirmed the
   exact staged patch SHA-256
   `8a63a57ce80fa863be19a296a66c1f1159edd1bd185a0afe0241e1ed8a8bc03c`
   and reported no remaining correctness, efficiency, deadlock, teardown, or
   test-determinism finding.

The exact validation record is under **Latest validation evidence**. This
completes the current release-path resource-release audit pass; the next
priority is the measured sustained-load proof. Known longer-term lifecycle
items remain explicitly listed under **Remaining work, in priority order**.
The overall backend goal remains active.

## Previous checkpoint: sanitized current GradeThis collector migration

Date: 2026-07-26

Implementation/proof commit:
`c576e85` (`prove current GradeThis collector migration`)

Retention foundation:
`458c8b4` (`enforce logical event retention`)

This slice proves the current GradeThis/go-common log source through the real
collector and public backend search path without changing the exact product
plan v0.1 corpus:

1. `configs/examples/collector.yaml` is now a runnable environment-substituted
   GradeThis profile with gzip transport, an explicit application host,
   trusted `gradethis` index/source/sourcetype/service/environment metadata,
   compressed-file exclusion, a durable state directory, and no OpenTelemetry
   component in the path.
2. A separate sanitized current-source manifest generates 20 deterministic
   NDJSON events with `Request summary statistics`, INFO/WARN/ERROR selection,
   Go `µs`/`ms`/`s` durations, UTC and `-07:00` timestamps, sparse root values,
   a three-layer trace, relative callers, and TEST-NET addresses. The pinned
   default fixture SHA-256 is
   `41f8f92f9170192810bdb741ed79fcf7f9f28c7966bb7e5dd9c54925c0f38f88`.
   Collector-owned metadata is absent from raw JSON.
3. Six current-source investigations are defined once with their expected row
   counts: trace following, severity counts, failed 5xx requests, path/status
   counts, duration-unit extraction with `rex`, and top messages. Ordinary
   tests keep all six progressing through parse, plan, and compile.
4. The real backend vertical creates the `gradethis` index and an index-scoped
   token, validates the committed collector config against exactly one empty
   source file, starts the actual collector binary, observes its zero
   checkpoint, appends and fsyncs the generated fixture, and requires exactly
   20 stored/distinct IDs.
5. That fixture must reach EOF checkpoint, acknowledged/drained WAL, and clean
   collector shutdown. ClickHouse assertions require every trusted metadata
   value and prove those metadata keys never enter raw. The six searches run
   through the public HTTP protobuf contract with three-row signed-cursor
   pages, stable schemas/totals/ordinals/row IDs, exact types and ordering,
   complete scope/range metadata, and no truncation.
6. The collector's last confidentiality boundary now sanitizes mandatory and
   configured policies before WAL or dead-letter persistence. Its ordered
   allow/deny/rename lineage derives from the exact compiled processor
   instances, is event-specific, preserves collisions and constant-field
   provenance, fails closed for unknown processor types, and avoids lineage
   allocation when no ordinary rename source is present.
7. Direct policies redact structured fields, raw bytes, and canonical messages
   in place. Same-replacement rules are grouped, final default policies already
   covered by mandatory names are eliminated, and the shipped GradeThis
   profile therefore performs exactly one direct pre-WAL sanitizer pass.
   Batched alias redaction canonicalizes duplicate JSON members even when the
   last member already equals the replacement, so shadowed secret bytes cannot
   cross the WAL boundary.
8. Decode-failure diagnostics no longer include decoder errors because JSON
   field names are attacker-controlled. Tests cover offline WAL contents,
   rename declassification, allow/deny order, punctuation and normalized
   lookalikes, conflicting replacements, constants, duplicate fields, and
   secret-free logs.
9. The exact product-plan GradeThis v0.1 corpus remains independent and all ten
   searches still pass through the decoder, production ClickHouse Store,
   compiler, executor, manager, and signed-cursor result paging.
10. Three final read-only adversarial reviews found no correctness,
    confidentiality, SPL-fidelity, reuse, or checkpoint-blocking performance
    issue in the frozen artifact. General configurations with multiple
    distinct custom replacement markers still require one raw/message scan per
    marker; a composite resolver is deferred until it can be differential-
    tested against every existing fail-closed encoded/duplicate/depth case.

The exact validation record is under **Latest validation evidence**. At this
checkpoint, the next release-path priority was the remaining preview-to-final
resource-release audit; that pass is now complete at `961cba2`. The overall
backend goal remains active.

At checkpoint creation, unrelated frontend changes were present in the shared
worktree and were deliberately neither staged nor committed by this backend
slice. Inventory and preserve unexpected local changes before continuing.

## Previous checkpoint: logical event retention before physical TTL cleanup

Date: 2026-07-26

Implementation/proof commit:
`458c8b4` (`enforce logical event retention`)

Lifecycle foundation:
`b2b2839` (`prove clock-driven expiration cleanup`)

This slice makes per-index retention an immediate query-visibility boundary
instead of relying on asynchronous ClickHouse TTL merges:

1. The common compiled ClickHouse scan now requires
   `expires_at > IndexTimeCutoff` alongside tenant, index, event-time,
   index-time, and visibility predicates. The cutoff is the immutable search
   snapshot, not ClickHouse `now()`, so preview, final results, retries,
   timelines, field APIs, and export re-execution see one retention boundary.
2. Compiler tests pin the SQL and argument order. Exact millisecond equality
   is expired, one millisecond after the cutoff is visible, and a
   sub-millisecond search cutoff is canonicalized once to DateTime64(3)
   precision and bound identically to the index-time and expiry predicates.
3. One pinned ClickHouse fixture inserts three still-physical rows around the
   artificial cutoff. Raw queries prove all three remain stored and that two
   are logically expired before and after the assertions. Only the
   just-after row can appear through direct execution, a real manager's
   preview/results, export re-execution, timeline, field catalog, or field
   summary; expired-only fields cannot leak through discovery.
4. The runtime default and resolved Store retention must be positive whole
   milliseconds; persisted control-plane/admin policies may also use zero as
   the deployment-default sentinel. This matches the native DateTime64(3)
   storage boundary and prevents positive retention from being silently
   shortened by driver truncation.
5. Event expiration is bounded to the timestamp interval that the pinned
   native Go driver can encode safely. A real-driver integration test writes
   and reads the upper supported boundary, while unit tests reject timestamps
   just outside either bound.
6. Reservation metadata is version 4 for new batches and records the strict
   millisecond contract. Version 3 remains decodable and preserves its legacy
   pre-driver nanosecond calculation so an ambiguous old batch can be retried,
   deduplicated, and advance contiguous visibility rather than wedge
   ingestion. The compatibility path is proven through the real native
   connection and storage.
7. SQLite migration `0009` canonicalizes preexisting retention values:
   positive sub-millisecond values become 1 ms, larger unaligned values are
   floored to the old native-storage result, and zero retains its default
   sentinel meaning. Durable insert and update triggers reject future
   unaligned values.
8. Controlled red tests first exposed the missing scan predicate, accepted
   sub-millisecond policies, unsafe driver range, incompatible legacy replay,
   and absent migration. Final correctness, architecture/performance, and
   code-quality reviewers found no remaining issue after fixes; the final
   reviewer separately verified real-driver v3 replay, both SQLite triggers,
   and every named event-row position.

The exact validation record is under **Latest validation evidence**. The next
release-path priority is a sanitized real GradeThis collector/config
migration, followed by resource-release coverage and measured load work as
ordered under **Remaining work, in priority order**. The overall backend goal
remains active.

## Previous checkpoint: clock-driven expiration and tombstone cleanup

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

The exact validation record is under **Latest validation evidence**. At that
checkpoint, logically authoritative per-index event retention was next; it is
now complete at `458c8b4`.

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

## Earlier pause slice: collector-to-browser completion

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

### Bounded browser statistics rendering

The exact implementation at `9d6acc11f2626f92d5ddd2b4e608a1268cc0c9e3`
passed:

```sh
npm run test:frontend
npm run lint
npm run typecheck
npm run build -- --webpack
go test ./...
go test -race ./...
go vet ./...
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration -run '^TestBrowser' -count=1 -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration -run '^TestBackendVertical$' -count=1 -v
git diff --cached --check
```

All 89 frontend tests passed. The production Next.js build compiled and
generated all 11 pages. The complete six-case browser matrix, including fixed
rendering, sequence expiration/gap recovery, REST terminal/first-progress
recovery, and cancellation, passed in 37.512 seconds. The final Docker-backed
vertical passed in 21.01 seconds and all six current GradeThis SPL searches
returned the expected results.

The final 1,000-by-64 response was 424,238 bytes. Initial stabilization took
74.4 ms and bottom stabilization 174.6 ms on Chromium 151.0.7922.34. The
initial table held 18 materialized rows, one spacer, 19 total body rows, and
1,216 materialized cells; interaction-wide mutation peaks were 25
materialized rows and 27 total body rows. The table was 10,240 pixels wide.
These timings are observational checkpoint-machine measurements, not
performance acceptance thresholds.

Screenshots of the top, bottom, and compact states were inspected and showed
distinct sentinel rows, a stable header, bounded table geometry, and no page
overflow. Three independent reviewers verified exact staged patch SHA-256
`3af2a8373ff9900d862415851c84ccbd35b69462bda8325c939054c99792a732`
and reported no remaining correctness, measurement-integrity, performance,
efficiency, process-lifecycle, determinism, reuse, or code-quality finding.

### Bounded WebSocket consumers and replay recovery

The exact implementation at `4c4003f` passed:

```sh
go test ./internal/searchws ./internal/server \
  -run '^Test(PreviewQueuePressureSkipsSubscriptionSpecificCopy|DisposablePreviewPressureClosesAndRetainsReplay|PreviewTailoringWorkDoesNotConsumeConnectionQueueBudget|InitialPreviewTailoringWorkDoesNotConsumeConnectionQueueBudget|TerminalPreviewInvalidationRemainsAuthoritativeUnderPressure|BoundedInitialFrameStagingOwnsGlobalQueueReservation|SearchWebSocketSlowConsumerIsBoundedAndRecoversWithoutBlockingSearch)$' \
  -count=20
go test -race ./internal/searchws ./internal/server \
  -run '^Test(PreviewQueuePressureSkipsSubscriptionSpecificCopy|DisposablePreviewPressureClosesAndRetainsReplay|PreviewTailoringWorkDoesNotConsumeConnectionQueueBudget|InitialPreviewTailoringWorkDoesNotConsumeConnectionQueueBudget|TerminalPreviewInvalidationRemainsAuthoritativeUnderPressure|BoundedInitialFrameStagingOwnsGlobalQueueReservation|SearchWebSocketSlowConsumerIsBoundedAndRecoversWithoutBlockingSearch)$' \
  -count=5
go test -race ./internal/searchws \
  -run '^TestInitialPreviewTailoringWorkDoesNotConsumeConnectionQueueBudget$' \
  -count=100
go test ./... -count=1
go test -race ./internal/searchws ./internal/server -count=1
go vet ./...
go build ./...
npm run test:frontend
npm run typecheck
npm run lint
npm run build
git diff --cached --check
```

The frontend suite passed all 80 tests. The production Next.js build compiled,
typechecked, and generated all 11 static pages. The focused preview-pressure,
initial-staging, and gated-writer cases passed 20 ordinary repetitions and
five race-enabled repetitions across `internal/searchws` and
`internal/server`; the deterministic initial subscribe/replay boundary also
passed 100 race-enabled repetitions. Final reviewers independently reran the
frontend suite and up to three complete race-enabled repetitions of both
affected Go packages.

The controlled server red failed at the intended boundary: the old initial
subscribe/replay path hard-closed the connection with zero queued bytes when
the exact final 342-byte batch fit. The fixed path queues that batch, preserves
the unrelated reservation exactly, emits a replay acknowledgment plus the
correct truncated one-row preview, and releases its tailoring permit. The
real-server fixture separately proves one blocked in-flight frame plus six
queued frames remain open and the eighth new frame triggers isolated closure.

Three independent reviewers verified the exact staged binary diff hash
`887f03b62962c1aacb45b992f0481b1a7c5f04b6cc408f1d81fa5dc648c9670c`.
They traced queue and in-flight ownership, tailoring permits, cancellation and
lock ordering, browser frame accounting and backing-buffer ownership,
amortized compaction, stale-generation fencing, sequence/replay behavior, and
the integration fixture's transport determinism. All three reported the
frozen patch clean. This slice launched no Docker fixture.

### Concurrent SPL searches during live recovery

The exact implementation at `9898b41` passed:

```sh
go test ./integration \
  -run '^(TestBackendLoadConcurrentSearchValidationRejectsInconsistentSnapshots|TestMaximumBackendLoadSearchActiveOverlapUsesHalfOpenIntervals|TestValidateBackendLoadCohortVisibilityRejectsAnyRegression|TestBackendLoadPlanPinsSustainedOutageWindow)$' \
  -count=100
go test -race ./integration \
  -run '^(TestBackendLoadConcurrentSearchValidationRejectsInconsistentSnapshots|TestMaximumBackendLoadSearchActiveOverlapUsesHalfOpenIntervals|TestValidateBackendLoadCohortVisibilityRejectsAnyRegression|TestBackendLoadPlanPinsSustainedOutageWindow)$' \
  -count=20
go test ./... -count=1
go vet ./...
go build ./...
go test -race ./integration -count=1
OPEN_SPLUNK_BACKEND_LOAD=1 \
go test ./integration -run '^TestBackendSustainedLoad$' -count=1 -v
git diff --cached --check
```

The exact final Docker run completed in 46.14 seconds and the package in
46.527 seconds. It generated all 30,000 events and 12,531,099 source bytes in
30.253 seconds, or 991.6 generated events/second and 414,209.2 bytes/second.
The accepted window was 31.028 seconds, or 966.9 events/second. Its backend
outage lasted 6.170 seconds, the durable backlog health gate covered 6,000
events, recovery first advanced 264.803 ms after health, and post-generation
drain took 340.382 ms.

The concurrent recovery window ran two eight-job cohorts. While all processes
remained alive, source records advanced 11,200 to 11,400, physical rows 6,500
to 7,700, and every public result in the second cohort retained the first
cohort's maximum prefix while the exact public prefix advanced 6,500 to
7,100. The 16 jobs scanned 108,800 rows and 61,881,298 bytes. Their client
wall was 208.172 ms, lifecycle span 202.901 ms, and observational maximum
active overlap four. Lifecycle min/median/p95/max was
20.653/48.652/103.556/103.556 ms; elapsed
min/median/p95/max was 18.624/19.893/38.340/38.340 ms; maximum queue wait was
84.823 ms.

Final convergence still required exactly 30,000 physical and distinct rows,
exact source/storage/raw-field equality, one terminal checkpoint, an empty
acknowledged WAL, no quarantines, and an empty owner-only dead-letter file.
ClickHouse reported three active parts, 7,303,879 compressed bytes,
34,495,468 uncompressed bytes, and 11,501,962 bytes on disk. Final public SPL
search lifecycle measurements were 41.655 ms and 35.116 ms.

Controlled reds and successive adversarial reviews found and fixed an
insufficient prefix validator, unsafe physical-row lower bounds, zero-delta
progress, mixed client/server clocks, timing-dependent overlap requirements,
cross-dataset ratios, a hidden three-second SLA, retry amplification, weak
cohort-wide monotonicity, and a polling timer that could remain live during
queries. Three independent final reviewers recomputed frozen staged patch
SHA-256
`71d19acf2990b12db60e53cc50ecb0dc6c2abec063ceb5cdf69c7fb8845bc4fb`
and reported it clean. The implementation is committed and pushed on `main`
at `9898b41`.

### Sustained-load outage and restart correctness

The implementation/proof checkpoint passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test -race ./internal/collector/... ./integration -count=1
go test ./internal/collector ./internal/collector/wal ./integration \
  ./cmd/open-splunk-collector \
  -run '^(TestDaemonRestartDoesNotRequeuePendingWALSourcePrefix|TestCheckpointResumeView.*|TestPrepareAckCachesAndCoalescesSourceMarksAcrossRecovery|TestRecoveryQuarantinesEverySegmentAfterCorruptGap|TestSourceCheckpointsFromWALRejectsCursorConflictWithDurableCheckpoint|TestSourceCheckpointsFromWALRejectsConflictingPendingIdentities|TestBackendLoad.*|TestManagedProcess.*|TestLockedBuffer.*|TestParseLogLevel)$' \
  -count=20
go test -race ./internal/collector ./internal/collector/wal ./integration \
  ./cmd/open-splunk-collector \
  -run '^(TestDaemonRestartDoesNotRequeuePendingWALSourcePrefix|TestCheckpointResumeView.*|TestPrepareAckCachesAndCoalescesSourceMarksAcrossRecovery|TestRecoveryQuarantinesEverySegmentAfterCorruptGap|TestSourceCheckpointsFromWALRejectsCursorConflictWithDurableCheckpoint|TestSourceCheckpointsFromWALRejectsConflictingPendingIdentities|TestBackendLoad.*|TestManagedProcess.*|TestLockedBuffer.*|TestParseLogLevel)$' \
  -count=20
OPEN_SPLUNK_BACKEND_LOAD=1 \
go test ./integration -run '^TestBackendSustainedLoad$' \
  -count=1 -timeout=12m -v
git diff --cached --check
```

The exact frozen implementation passed the complete ordinary suite, build,
vet, collector/integration race suite, and both ordinary and race-enabled
20-repetition focused regression sets. An earlier candidate also passed a
separate 20-repetition full integration/collector/WAL stress run; its WAL
package completed in 294.421 seconds. Its command was
`go test ./integration ./internal/collector ./internal/collector/wal -count=20`.
Later adversarial review added the resume-prune production linearization,
direct durable-depth test synchronization, and reviewer cleanup. Every
affected path then passed the focused ordinary and race sets 20 times, plus
the complete ordinary and affected-package race suites. The final real Docker
run completed in 49.53 seconds, cold-recovered 5,700 durable events from the
6,000-event offline source phase, and finished with exactly 30,000 physical
and distinct stored events.

The shared 100,000-batch acknowledgment aggregation benchmarks reported:

```text
compact identity: 4.85 ms/op, 2,403,077 B/op, 7 allocs/op
100,000 identities: 27.86 ms/op, 37,681,632 B/op, 537 allocs/op
```

These are local checkpoint observations. Pending-mark startup aggregation
reuses the same validated merge logic but aggregates directly under the queue
lock, avoiding the 100,000 transient group headers; its production caller runs
before sender and input goroutines start.

Controlled reds first proved that source progress alone did not guarantee the
requested WAL depth, then exposed the production duplicate replay as
35,700/29,700 physical/distinct rows and as seven instead of four queued events
in a deterministic restart test. Neither assertion was weakened. Adversarial
review added legacy-cursor write suppression, durable-equal preference,
pending-vs-durable cursor validation, corrupt-gap coverage, acknowledged
overlay pruning, a manager-specific checkpoint interface, compile-time WAL
resume capability, immutable raw expectations, lower-cost polling, direct
durable-WAL-depth synchronization, a health-gated recovery generation phase,
and atomic lookup versus terminal pruning.

The final frozen staged patch SHA-256 is
`55f1eb83d0dd12be3660a41860ce30cd56a26f6055679dd0abda5a3fd3d08a64`.
Three independent reviewers recomputed that exact hash and reported the patch
clean for crash/restart correctness, WAL/checkpoint consistency, load-test
determinism, performance, and code quality. The implementation is committed
and pushed on `main` at `59b8f7c`.

### Load-generator pacing and live-output foundation

The exact implementation at `860acac` passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test ./internal/loggen ./cmd/open-splunk-loggen -count=50
go test -race ./internal/loggen ./cmd/open-splunk-loggen -count=10
git diff --cached --check
```

The focused suites passed 50 ordinary repetitions and 10 race-enabled
repetitions after the final reviewer-requested assertion. Independent
reviewers additionally ran the focused suites 100 times and the race-enabled
suites 20 times, plus vet and diff checks.

A compiler-free scheduling observation used the built binary:

```sh
/usr/bin/time -p /private/tmp/open-splunk-loggen-rate \
  -count=10000 -format=cardinality-json \
  -rate=1000 -interval=1ms -flush-events=100 -output=- >/dev/null
```

It emitted 10,000 high-cardinality events in 10.00 seconds on the checkpoint
machine. This is a reproducibility observation, not a cross-hardware
acceptance threshold. Before the absolute scheduler, the same cardinality
workload had taken 11.88 seconds (about 842 events/second) because per-event
generation and write time accumulated on top of every relative sleep.

The exact staged patch had SHA-256
`94c9ad171e414118cfab3bcd337eceb4eae298059e6aeeabdb9087de53339b1f`.
The final timing reviewer found deadline arithmetic, overflow guards, bounded
rebasing, cancellation priority, and timer reuse sound. The I/O reviewer
found all Flush/Sync/Close, append, pre-cancellation, stdout, and error-tree
paths clean under the documented single-writer file contract. The test and
performance reviewer found the generator suitable for the next 1,000 EPS
live-tail harness when it explicitly uses `-interval=1ms` and a finite
`-flush-events` value such as 100.

### Shutdown-safe export artifact removal

The exact implementation at `961cba2` passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test -race ./internal/export ./internal/server -count=1
go test ./internal/export \
  -run 'TestDownloadLeaseDeferredUnlinkIsAdmittedBeforeManagerClose|TestManager(Cleanup|Get)DeletionDoesNotBlockShutdownAdmission' \
  -count=100
go test -race ./internal/export \
  -run 'TestDownloadLeaseDeferredUnlinkIsAdmittedBeforeManagerClose|TestManager(Cleanup|Get)DeletionDoesNotBlockShutdownAdmission' \
  -count=20
go test -race ./internal/server \
  -run '^TestSearchWebSocketRealManagersExpireLeasedResultsArtifactsAndTombstones$' \
  -count=1
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
git diff --cached --check
```

The full ordinary Go suite, build, vet, complete export/server race suites, and
the exact real-manager expiration lifecycle all passed. The three new
shutdown/removal cases passed 100 ordinary repetitions in 7.094 seconds and
20 race-enabled repetitions in 3.122 seconds.

The six-case Docker/ClickHouse plus browser release gate completed in
52.492 seconds: `TestBackendVertical` in 24.28 seconds,
`TestBrowserSequenceExpiredRecovery` in 11.80 seconds,
`TestBrowserSequenceGapRecovery` in 5.18 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.14 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.28 seconds, and
`TestBrowserSearchCancellation` in 1.31 seconds. All six current GradeThis
investigations passed inside the vertical. Read-only Docker checks before and
after the gate showed no `open-splunk-*` test container.

The frozen staged implementation patch had SHA-256
`8a63a57ce80fa863be19a296a66c1f1159edd1bd185a0afe0241e1ed8a8bc03c`.
Independent final reviewers confirmed that checksum, traced every artifact
remover and shutdown interleaving, and reported no remaining finding.

### Sanitized current GradeThis collector migration

The exact implementation at `c576e85` passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test -race \
  ./internal/collector ./internal/ingest \
  ./internal/testsupport/gradethiscorpus -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
go test ./internal/queryexec \
  -run '^TestGradeThisCompatibilityV0_1AgainstClickHouse$' \
  -count=1 -timeout=10m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
git diff --cached --check
```

The final full Go suite completed in 18.594 seconds. The race-enabled collector,
ingestion, and migration-corpus packages passed, with the longest package at
39.558 seconds. The exact ten-search v0.1 ClickHouse corpus completed in
5.99 seconds.

The final six-case Docker/ClickHouse plus browser release gate completed in
50.946 seconds: `TestBackendVertical` in 22.63 seconds,
`TestBrowserSequenceExpiredRecovery` in 11.78 seconds,
`TestBrowserSequenceGapRecovery` in 5.21 seconds,
`TestBrowserSequenceGapRESTTerminalRecovery` in 5.19 seconds,
`TestBrowserSequenceGapRESTFirstProgressRecovery` in 4.37 seconds, and
`TestBrowserSearchCancellation` in 1.36 seconds. All six current GradeThis
investigations passed inside the vertical. A read-only Docker check showed no
remaining `open-splunk-*` test container.

The final frozen staged patch had SHA-256
`c863964ed3a674b870dbe7861a1cd794b423ca1009422dd2df7200af87109ba3`.
Adversarial reviewers independently verified that exact checksum. Their
findings drove shared processor-derived lineage, a unified stored-event poller,
manifest-owned expected row counts, the no-source lineage fast path, removal
of redundant grouping state, and duplicate-key alias canonicalization. Final
correctness/SPL, confidentiality/performance, and reuse/quality rereviews were
clean. The only retained nonblocking performance backlog is the
differential-tested composite sanitizer for uncommon configurations with
multiple distinct custom replacement markers; the shipped profile uses one
direct pass.

### Logical event retention before physical TTL cleanup

The exact implementation at `458c8b4` passed:

```sh
go test ./... -count=1
go vet ./...
go build ./...
go test -race \
  ./internal/clickhouse ./internal/queryexec ./internal/export \
  ./internal/searchjobs ./internal/control ./internal/server \
  ./cmd/open-splunk-server -count=1
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
go test ./internal/clickhouse \
  -run '^TestStoreAgainstClickHouse$' -count=1 -timeout=6m -v
OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 \
OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE=clickhouse/clickhouse-server:26.3.17.4@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49 \
go test ./internal/queryexec \
  -run '^TestExecutorAndManagerAgainstClickHouse$' \
  -count=1 -timeout=6m -v
OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
go test ./integration \
  -run '^Test(BackendVertical|Browser(SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
  -count=1 -timeout=15m -v
git diff --check
```

The final pinned Store suite completed in 35.32 seconds and included the real
native-driver upper timestamp round trip plus an ambiguous version-3
reservation retry through storage and visibility completion. The pinned
executor/manager suite completed in 11.323 seconds. The exact six-case
Docker/ClickHouse plus browser release gate completed in 47.729 seconds. A
read-only Docker check showed no Open Splunk test container afterward.

The retention fixture is deliberately non-vacuous: all three rows remain
physically present, two have expired relative to the immutable artificial
cutoff, and only the just-after-boundary row is visible through every
read path. Controlled red/green evidence covered the common predicate, native
timestamp range, policy precision, metadata compatibility, and migration.
The final frozen staged diff had SHA-256
`579dfcce5a4f8c9c179fd8bc7e2bd1936d45af1eccf1605309ab3ef3d465f5b9`;
independent correctness, architecture/performance, and quality reviews
reported no remaining finding.

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
- restart-safe ephemeral input resume from intact pending WAL source
  coordinates, without advancing the terminal checkpoint;
- a deterministic load generator with absolute target-rate pacing, bounded
  catch-up, live periodic flushing, safe append boundaries, and durable
  cancellation finalization;
- an opt-in 30,000-event real server/collector/ClickHouse load proof with
  server outage, collector crash/restart, exact raw/event-ID checks, public SPL
  searches, and storage/recovery observations;
- scoped ingestion tokens, canonical typed events, and ClickHouse storage;
- logically authoritative per-index event retention at one immutable search
  cutoff, independent of asynchronous physical TTL cleanup;
- bounded search jobs with cancellation, history, progress, typed paging,
  result leases, timelines, field catalog/summary, and CSV/JSONL export;
- binary protobuf WebSocket preview/progress with job-state revisions, replay,
  explicit resynchronization acknowledgement, coalesced REST recovery, bounded
  single-use subscription identities, expiration retirement, bounded
  server-side slow-consumer eviction, independently bounded preview-tailoring
  work, client-wide inbound frame/byte bounds, replay-on-pressure, serialized
  cross-generation listeners, and reconnect backoff;
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

Event retention is evaluated against each search job's immutable
`IndexTimeCutoff`. An event with `expires_at` equal to that cutoff is not
visible; an event one stored millisecond later is visible. Physical
ClickHouse TTL cleanup remains asynchronous best-effort physical reclamation,
and direct ClickHouse reads outside compiled Open Splunk query paths may still
expose expired physical rows. New persisted policies are nonnegative whole
milliseconds, with zero meaning the deployment default; resolved retention is
positive.
Legacy version-3 reservation metadata may retain historical sub-millisecond
intent solely to finish or deduplicate an already durable batch; version 4
and all new control-plane writes use the strict contract.

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

1. Work only from `main`; fast-forward it from `origin/main`. Inventory and
   preserve unexpected local changes before editing. A shared worktree may
   contain unrelated frontend work, so do not reset it merely to make it
   clean.
2. Read this file, `docs/product-architecture-plan.md`, and
   `docs/spl-compatibility-v0.1.md`.
3. Run `npm ci`, `go test ./...`, `npm run test:frontend`,
   `npm run typecheck`, and `npm run lint`.
4. If the next change touches the release path, install the pinned browser with
   `npx --no-install playwright install chromium` and run:

   ```sh
   OPEN_SPLUNK_BACKEND_INTEGRATION=1 \
   go test ./integration \
     -run '^(TestBackendVertical|TestBrowser(FixedResultRendering|SearchCancellation|Sequence(ExpiredRecovery|Gap(REST(FirstProgress|Terminal))?Recovery)))$' \
     -count=1 -timeout=15m -v
   ```

5. The core real-process sustained-load/outage/restart proof is complete at
   `59b8f7c`, and its concurrent-search-during-recovery extension is complete
   at `9898b41`, on top of the log-generator foundation at `860acac`. The
   deterministic bounded-queue slow WebSocket consumer and browser inbound
   backpressure slice is complete at `4c4003f`; bounded fixed-payload browser
   rendering is complete at `9d6acc1`. Unless the user changes priority,
   profile the collector with a high source count and replace its pre-existing
   per-poll `time.After` allocation only if measurement shows it is material.
   The current preview-to-final resource-release audit pass is complete at
   `961cba2`, the sanitized current GradeThis collector/config migration at
   `c576e85`, logical event retention at `458c8b4`, clock-driven
   job/result/export expiration at `b2b2839`, and stale-duplicate injection at
   `b80bf0a`. Add a red unit or integration test before implementation, run
   read-only adversarial reviews, fix concrete findings, then commit and push
   `main`.

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

Logical event retention is complete at `458c8b4`. The common scan enforces
`expires_at > IndexTimeCutoff`, and the pinned non-vacuous integration fixture
proves exact-boundary exclusion across direct search, real-manager
preview/results, export re-execution, timeline, field discovery, and field
summary while the rows are still physically stored. Whole-millisecond policy,
native-driver timestamp range, version-3 reservation replay, version-4
metadata, and legacy SQLite migration boundaries are also pinned. ClickHouse
TTL remains merge-driven physical reclamation rather than the visibility
contract.

The sanitized current GradeThis migration is complete at `c576e85`. The
committed config, real collector binary, scoped token, checkpoint/WAL path,
trusted metadata, 20-event sanitized manifest, and six representative
investigations are exercised through the public backend. The separate exact
v0.1 corpus remains unchanged and green.

The current preview-to-final resource-release audit pass is complete at
`961cba2`. Search-job, executor, snapshot, and retained-result ownership were
clean; export lookup, cleanup, and final-download deletion now hand off safely
to shutdown without manager-wide lock/I/O coupling.

The core sustained-load proof is complete at `59b8f7c`. It runs 30,000 events
at a target 1,000 events/second through the real log generator, collector,
server, and pinned ClickHouse; proves a 5,000-event offline window plus
headroom; crash-restarts the collector from intact pending WAL; and requires
exact source, storage,
event-ID, raw/extracted-field, checkpoint, dead-letter, WAL, and public SPL
results. Its controlled red exposed and drove the pending-WAL resume fix.
The concurrent-search extension is complete at `9898b41`: ready-gated
eight-job waves prove an exact contiguous public source prefix, cohort-wide
monotonic visibility, strict source and searchable-prefix advancement, process
liveness, unique job identity, and public result metadata during live
recovery. Scan, queue, lifecycle, wall, and overlap measurements remain
observational until hardware and capacity decisions are made.

Continue the release proof in this order:

- The deterministic bounded-queue slow WebSocket consumer and replay recovery
  path is complete at `4c4003f`. The separate fixed 1,000-row by 64-column
  browser rendering proof, including stable-DOM/two-animation-frame gates and
  interaction-wide DOM bounds, is complete at `9d6acc1`.
- During high-source-count collector profiling, replace the pre-existing
  per-poll `time.After` allocation with a safely reused timer if it is material;
  preserve cancellation and copy-truncate behavior with race coverage.
- Replace one-pass-per-distinct-marker configured pre-WAL redaction with a
  composite resolver only after differential tests pin ambiguous encoded
  keys/values, prose-wrapped embedded JSON, duplicate-key canonicalization,
  exact-name matching, replacement precedence, and the depth-limit fail-closed
  behavior. The shipped GradeThis profile already collapses to one direct
  sanitizer pass.
- The browser child output and observation counts are bounded, but
  build-command failure output still uses `CombinedOutput`. Complete the
  harness hardening with the same capped buffer, cap each recorded
  diagnostic's byte length, and bound the number of simultaneously observed
  matching WebSockets.
- The statistics result adapter still eagerly builds events, derived fields,
  and a timeline even when a statistics-only view consumes just columns and
  rows. Specialize or lazily construct those projections before raising
  browser result limits.
- The event path constructs an `Intl.DateTimeFormat` inside `formatEventTime`
  for every valid event timestamp. Hoist or cache it behind a focused
  correctness test if profiling shows that cost matters.
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
- Surface background export-deletion failures, and let admission perform due
  cleanup before reporting capacity exhaustion.
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
- Collector checkpoints and WAL source marks are currently keyed by physical
  file identity rather than `(input_id, file identity)`. Configure at most one
  file input pipeline for a given inode/path set; overlapping globs or hard
  links across distinct inputs can otherwise let one input's higher checkpoint
  skip the other input's logical events. Either enforce non-overlap or carry
  input identity through checkpoint/WAL keys before claiming that topology.
- Default aggregate names containing parentheses cannot be referenced by the
  downstream field grammar; use `AS`.
- The 512 KiB `values` bound is a publication bound, not an aggregate-state
  memory guarantee. ClickHouse query memory remains authoritative before the
  post-aggregate check.
- Duplicate JSON member selection follows the pinned ClickHouse parser's
  first-member behavior.
- Interactive browser result schemas are deliberately limited to 1–64 columns,
  and a REST result response may not exceed its requested page size. Supporting
  broader schemas requires a product decision and column virtualization rather
  than silently materializing an unbounded row width.
- Do not accept a changing legal agreement or start a licensed Splunk image on
  the user's behalf merely to obtain an oracle.

## Safe resume procedure

1. Confirm the checked-out branch is `main` and compare it with `origin/main`.
   Inventory and preserve every unexpected local change; do not reset unrelated
   work merely to obtain a clean tree.
2. Read the three documents listed at the top and inspect the latest `main`
   commits, especially `9d6acc1`, `4c4003f`, `9898b41`, `59b8f7c`,
   `860acac`, `961cba2`, `c576e85`, `458c8b4`, `b2b2839`, `b80bf0a`,
   `cdb60df`, `787a7f9`, and `522b0ac`; the preceding progress/recovery
   foundations are `b5502a3`, `f72f184`, `ed28182`, and `d1286a4`.
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
5. The fixed-payload browser rendering measurement is complete at `9d6acc1`.
   Unless the user changes priority, proceed to high-source-count collector
   profiling and its pre-existing per-poll timer allocation, followed by the
   ordered redaction, harness-output, adapter, formatter, and release-revision
   items above. The generator foundation, current preview-to-final
   resource-release audit pass, sanitized current GradeThis collector/config
   migration, logical event retention,
   clock-driven job/result/export expiration,
   recovered-socket stale-duplicate fencing, authoritative browser
   cancellation, versioned
   REST-first/replay-later progress, recovery-cycle coalescing, REST-only and
   accepted-WebSocket terminal discovery after a sequence gap, explicit
   browser sequence-gap recovery, browser `SEQUENCE_EXPIRED`, transient
   recovery-GET failure, retained replay, real-manager
   expiration/cancellation, the uninterrupted collector-to-browser path,
   exact GradeThis corpus, collector/server process-restart proof, and the
   protocol unit contract are already complete.
6. If extending aggregates instead, start with an explicit bounded contract
   for `list(field)`; do not reuse unordered `values`.
7. Preserve scalar/Dynamic path separation, numeric grammar sharing,
   punctuation/UTF-8/zero/overlong boundaries,
   native timestamp precision, private calculated types, downstream `bin`,
   re-aggregation, scope poison, binary transport, physical state sharing, and
   relational-depth regressions.
8. Keep working on `main`; commit and push every cohesive green slice.
9. Preserve unexpected local changes and never reset them away.
