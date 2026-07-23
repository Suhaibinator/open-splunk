# Backend checkpoint handoff

This file is the restart point for backend work. The checkpoint commit is the
`main` commit that contains this document; confirm it with `git log -1` after
pulling `origin/main`.

## Safe resume procedure

1. Run `git status --short --branch` before editing anything.
2. Preserve any pre-existing frontend work in `app/search-workspace.tsx` and
   `app/search-workspace/live-preview.ts`; those files were being changed
   independently and were intentionally excluded from this backend checkpoint.
3. Run `git pull --ff-only origin main` only when doing so will not disturb
   local work.
4. Read `docs/product-architecture-plan.md` and
   `docs/spl-compatibility-v0.1.md` before expanding SPL behavior.
5. Keep changes on `main`, commit cohesive green slices, and push each
   checkpoint.

## What is complete at this checkpoint

- The collector-to-ingestion path uses durable acknowledgment/checkpoint
  coupling and has restart/ambiguous-delivery coverage.
- Search jobs expose bounded typed result pages, immutable result leases,
  cancellation/lifecycle state, progress, history, timelines, field catalogs,
  exact field summaries, and bounded CSV/JSONL export jobs.
- The executable SPL v0.1 contract and GradeThis compatibility corpus cover
  base search, field comparisons, Boolean expressions, `fields`, `table`,
  `rename`, `sort`, `head`, `tail`, `dedup`, the documented `eval`/`where`
  subset, `stats` (`count`, `sum`, `avg`, and `p95`), `top`, `rare`, and
  `timechart`.
- The binary protobuf Search WebSocket supports bounded target journals,
  replay/resynchronization, connection and global queue limits, terminal
  delivery, application/transport ping-pong, and graceful shutdown.
- This checkpoint adds coherent bounded live result previews:
  - manager snapshots capture job/schema/rows/revision atomically;
  - transport and manager row/byte ceilings are aligned before row cloning;
  - per-subscription preview limits share one canonical target sequence;
  - opted-out subscribers receive zero-row continuity markers;
  - preview invalidation is replayable before failed/canceled/expired terminal
    state;
  - bootstrap, replay, queue, and transformation allocations are bounded;
  - projection work is concurrency-limited through publication;
  - preview demand uses a bounded histogram rather than repeated full scans;
  - the TypeScript client fences stale subscription frames, isolates reconnect
    failures, and does not retry server-rejected subscriptions forever; and
  - deprecated create-time preview options are marked deprecated in protobuf
    metadata while preview policy remains subscription-scoped.

## Validation for the checkpoint

The following commands passed at this checkpoint:

```sh
make proto
go test ./... -count=1 -timeout=3m
go test -race ./internal/searchjobs ./internal/searchjobproto ./internal/searchws ./internal/server ./cmd/open-splunk-server -count=1 -timeout=5m
go vet ./...
go build ./...
npm run typecheck
npx oxlint lib/api/search-websocket.ts gen/ts/open_splunk/v1/search.ts gen/ts/open_splunk/v1/search_ws.ts
npm run build
OPEN_SPLUNK_BACKEND_INTEGRATION=1 go test ./integration -run '^TestBackendVertical$' -count=1 -timeout=3m
```

`npm run lint` currently also examines the independent, incomplete live-preview
workspace UI edits named above and reports unused state/imports there. Do not
"fix" that by deleting or overwriting those edits. The backend-owned TypeScript
client and generated bindings pass type checking and scoped linting.

## Remaining work, in priority order

1. Finish and test the browser workspace's live-preview integration. Add a
   frontend behavioral test runner so reconnect rejection, stale-frame fencing,
   resynchronization, and preview-to-authoritative-result replacement have
   deterministic tests instead of only type/lint coverage.
2. Continue the explicitly unsupported SPL surface in small conformance-first
   slices. The current contract lists `rex`, `spath`, `bin`/`bucket`, `chart`,
   `eventstats`, and `streamstats`; `rex` is a useful next vertical slice.
3. Expand aggregate compatibility beyond `count`, `sum`, `avg`, and `p95`:
   `dc`, `values`, `list`, `min`, `max`, `earliest`, `latest`, and the remaining
   documented percentile forms all need parser, plan, SQL, semantic, and
   ClickHouse integration tests.
4. Add the full browser-visible end-to-end test: generated log -> collector
   durable acknowledgment -> ingestion -> ClickHouse -> SPL job -> WebSocket
   preview/progress -> authoritative paged result rendered by the UI.
5. Run and record the performance harness against the plan's sustained 1,000
   events/second target, including slow-consumer WebSockets, concurrent preview
   subscriptions, ClickHouse scan limits, and collector offline recovery.
6. Continue Phase 3/4 product hardening: per-index permissions/retention UI,
   token and collector fleet operations, RBAC/audit search, backup/restore,
   migration upgrade tests, fair query scheduling, packaging, and upgrades.

## Review notes

Independent adversarial passes were run for concurrency/correctness,
performance/resource accounting, and TypeScript/protobuf behavior. Their
actionable findings were fixed before this checkpoint, including stale
bootstrap exclusivity, projection lifecycle gating, replayable preview clears,
duplicate-target bootstrap transformation amplification, quadratic preview
demand scans, over-retained tailoring reservations, reconnect batch coupling,
stale subscription frames, and failed-send recovery.
