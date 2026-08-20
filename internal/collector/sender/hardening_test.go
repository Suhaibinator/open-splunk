package sender

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestSenderThrottleReducedLimitsDoesNotDeadLetter verifies that a batch which
// exceeds only a TEMPORARY throttle's reduced limits is held and retried after
// the throttle expires, never dead-lettered. (FIX 5)
func TestSenderThrottleReducedLimitsDoesNotDeadLetter(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	var mu sync.Mutex
	seen := 0
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		mu.Lock()
		seen++
		n := seen
		mu.Unlock()
		if n == 1 {
			// After the first batch, impose a throttle with a tiny max_batch_bytes
			// that expires shortly. The next (100-byte) batch exceeds it.
			_ = fs.send(&opensplunk.CollectResponse{
				Payload: &opensplunk.CollectResponse_Throttle{Throttle: &opensplunk.Throttle{
					Reason:         opensplunk.ThrottleReason_THROTTLE_REASON_SERVER_LOAD,
					MaxBatchBytes:  1,
					EffectiveUntil: timestamppb.New(time.Now().Add(100 * time.Millisecond)),
				}},
			})
		}
		fs.ackBatch(b.GetBatchSequence(), 1)
	}
	conn := startServer(t, fs)

	b1 := fakeBatch(1, makeEvent("e1", "main"))
	b2 := fakeBatch(2, makeEvent("e2", "main"))
	b2.UncompressedSizeBytes = 100 // exceeds the throttle's max_batch_bytes of 1
	q := newFakeQueue(b1, b2)
	sink := &memSink{}
	s := newTestSender(t, testOptions(), q, sink, nil, conn)
	cancel, done := runSender(t, s)

	// Both batches must eventually be delivered and acked; the throttle only
	// delays batch 2, it never causes a drop.
	waitFor(t, "both batches acked after throttle expiry", func() bool { return q.ackedSeq() >= 2 })
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("dead-lettered %d records under a temporary throttle, want 0", got)
	}

	cancel()
	<-done
}

// TestSenderExceedsReadyLimitsDeadLetters confirms a batch that exceeds the
// NEGOTIATED Ready limits (never satisfiable) is still permanently dead-lettered
// and acked off the queue. (FIX 5, the complementary case)
func TestSenderExceedsReadyLimitsDeadLetters(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.readyFn = func() *opensplunk.CollectorReady {
		r := defaultReady()
		r.MaxBatchBytes = 50 // permanent negotiated cap
		return r
	}
	conn := startServer(t, fs)

	b := fakeBatch(1, makeEvent("e1", "main"))
	b.UncompressedSizeBytes = 100 // exceeds the negotiated 50
	q := newFakeQueue(b)
	sink := &memSink{}
	s := newTestSender(t, testOptions(), q, sink, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "oversized batch acked off queue", func() bool { return q.ackedSeq() >= 1 })
	waitFor(t, "oversized batch dead-lettered", func() bool { return len(sink.snapshot()) == 1 })

	rec := sink.snapshot()[0]
	if rec.Code != opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE.String() {
		t.Fatalf("dead-letter code = %q, want BATCH_TOO_LARGE", rec.Code)
	}
	cancel()
	<-done
}

func TestSenderRetainsMultiEventBatchThatRequiresLosslessRepacking(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.readyFn = func() *opensplunk.CollectorReady {
		ready := defaultReady()
		ready.MaxBatchEvents = 1
		return ready
	}
	batch := fakeBatch(1, makeEvent("e1", "main"), makeEvent("e2", "main"))
	q := newFakeQueue(batch)
	sink := &memSink{}
	s := newTestSender(t, testOptions(), q, sink, nil, startServer(t, fs))
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	select {
	case err := <-done:
		var fatal *fatalError
		if !errors.As(err, &fatal) {
			t.Fatalf("Run error = %v, want fatal repacking requirement", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sender neither retained nor failed an unsplittable durable batch")
	}
	if got := q.ackedSeq(); got != 0 {
		t.Fatalf("multi-event oversized batch acked through %d, want retained", got)
	}
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("multi-event oversized batch dead-lettered %d valid events", got)
	}
}

func TestSenderDeadLettersSingleEventAboveNegotiatedEventLimit(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.readyFn = func() *opensplunk.CollectorReady {
		ready := defaultReady()
		ready.MaxEventBytes = 1
		return ready
	}
	q := newFakeQueue(fakeBatch(1, makeEvent("oversized", "main")))
	sink := &memSink{}
	s := newTestSender(t, testOptions(), q, sink, nil, startServer(t, fs))
	cancel, done := runSender(t, s)
	waitFor(t, "single oversized event terminally handled", func() bool { return q.ackedSeq() == 1 })
	records := sink.snapshot()
	if len(records) != 1 || records[0].Code != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE.String() {
		t.Fatalf("single-event negotiated dead letter = %+v", records)
	}
	cancel()
	<-done
}

// TestSenderRetryFloodCoalescesToSingleResend floods the sender with 100
// RetryBatch messages for one sequence and asserts only a single resend results,
// rather than one goroutine/resend per message. (FIX 11)
func TestSenderRetryFloodCoalescesToSingleResend(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	var mu sync.Mutex
	deliveries := 0
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		mu.Lock()
		deliveries++
		n := deliveries
		mu.Unlock()
		if n == 1 {
			// Flood identical RetryBatch messages before the (delayed) resend fires.
			for range 100 {
				_ = fs.send(&opensplunk.CollectResponse{
					Payload: &opensplunk.CollectResponse_RetryBatch{RetryBatch: &opensplunk.RetryBatch{
						BatchId:       b.GetBatchId(),
						BatchSequence: b.GetBatchSequence(),
						Reason:        opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
						RetryAfter:    durationpb.New(60 * time.Millisecond),
					}},
				})
			}
			return
		}
		fs.ackBatch(b.GetBatchSequence(), 1)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))

	var lastStats Stats
	var statsMu sync.Mutex
	reporter := StatsReporterFunc(func(s Stats) { statsMu.Lock(); lastStats = s; statsMu.Unlock() })
	s := newTestSender(t, testOptions(), q, &memSink{}, reporter, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked after coalesced resend", func() bool { return q.ackedSeq() >= 1 })

	got := 0
	for _, b := range fs.receivedBatches() {
		if b.GetBatchSequence() == 1 {
			got++
		}
	}
	if got != 2 {
		t.Fatalf("batch 1 deliveries = %d, want 2 (original + one coalesced resend), not one per RetryBatch", got)
	}
	statsMu.Lock()
	retried := lastStats.RetriedBatchesTotal
	statsMu.Unlock()
	if retried != 1 {
		t.Fatalf("RetriedBatchesTotal = %d, want exactly 1 (coalesced)", retried)
	}
	cancel()
	<-done
}
