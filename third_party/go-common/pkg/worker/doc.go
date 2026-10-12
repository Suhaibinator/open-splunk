// Package worker provides reusable scaffolding for background workers that drain
// one or more queues. It bundles three composable concerns that together cover the
// loop structure most queue-draining workers hand-roll:
//
//   - Drain (drain.go): a bounded fan-out claim/process primitive. Given a claim
//     callback and a process callback, it runs up to N processors concurrently until
//     no more work can be claimed or a processor returns an error.
//
//   - Harness (harness.go): a multi-lane orchestration layer built on Drain. Each
//     "lane" drains its own queue at its own concurrency in its own goroutine, so a
//     slow lane never blocks another lane from picking up freshly-queued work. The
//     harness owns the per-lane loops, coalesced wake signalling, and an optional
//     periodic maintenance callback.
//
//   - Lifecycle (lifecycle.go): start/stop/done coordination for a long-running
//     background goroutine. It guarantees a single active run, cancels via context,
//     and blocks Stop until the run reports completion.
//
// The three concerns are independent: use Drain on its own for a one-shot bounded
// fan-out, Harness when you need several independent lanes with wake signalling, and
// Lifecycle to manage the start/stop of whatever long-running goroutine drives them.
//
// The scaffolding deliberately knows nothing about task types, metrics, tracing
// context, heartbeats, or persistence. Those concerns stay inside the Claim/Process
// closures and the maintenance callback the consumer supplies, keeping this package a
// thin, reusable shell. Consumers that want dashboards and alerts for free should
// record via common_metrics' worker collectors (see RecordWorker* / ObserveWorker*)
// from inside their closures.
//
// Example: a single worker combining two lanes of different task types.
//
//	ingestLane := worker.NewLane(worker.Lane[IngestJob]{
//		Name:          "ingest",
//		MaxConcurrent: 4,
//		Claim:         store.ClaimIngestJob,
//		Process:       pipeline.Ingest,
//	})
//	notifyLane := worker.NewLane(worker.Lane[NotifyJob]{
//		Name:          "notify",
//		MaxConcurrent: 1,
//		Claim:         store.ClaimNotifyJob,
//		Process:       pipeline.Notify,
//	})
//
//	h := worker.NewHarness(logger, 30*time.Second, store.RecoverStaleJobs, ingestLane, notifyLane)
//
//	var lc worker.Lifecycle
//	lc.StartBackground(nil, func(ctx context.Context) {
//		h.Run(ctx)
//		lc.Finish()
//	})
//	// ... on shutdown:
//	_ = lc.Stop(shutdownCtx)
//
// When the ingest lane hands work to the notify lane it can wake just that lane with
// h.SignalLane("notify") instead of waiting for its poll tick.
package worker
