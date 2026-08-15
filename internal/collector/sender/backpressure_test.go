package sender

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSenderRetryDelaySurvivesReconnect(t *testing.T) {
	t.Parallel()
	const retryAfter = 250 * time.Millisecond

	fs := newFakeServer()
	var mu sync.Mutex
	var receivedAt []time.Time
	fs.batchErr = func(fs *fakeServer, batch *opensplunkv1.EventBatch) error {
		mu.Lock()
		receivedAt = append(receivedAt, time.Now())
		attempt := len(receivedAt)
		mu.Unlock()
		if attempt != 1 {
			return nil
		}
		if err := fs.send(&opensplunkv1.CollectResponse{
			Payload: &opensplunkv1.CollectResponse_RetryBatch{RetryBatch: &opensplunkv1.RetryBatch{
				BatchId:       batch.GetBatchId(),
				BatchSequence: batch.GetBatchSequence(),
				Reason:        opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED,
				RetryAfter:    durationpb.New(retryAfter),
			}},
		}); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	fs.onBatch = func(fs *fakeServer, batch *opensplunkv1.EventBatch) {
		fs.ackBatch(batch.GetBatchSequence(), 1)
	}

	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, startServer(t, fs))
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked after reconnect retry", func() bool { return q.ackedSeq() == 1 })
	mu.Lock()
	times := append([]time.Time(nil), receivedAt...)
	mu.Unlock()
	if len(times) != 2 {
		t.Fatalf("batch deliveries = %d, want original plus one reconnect retry", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < retryAfter*8/10 {
		t.Fatalf("reconnect retry gap = %v, want at least retry_after %v", gap, retryAfter)
	}

	cancel()
	<-done
}

func TestCoalescedRetryExtendsPersistentDeadline(t *testing.T) {
	t.Parallel()
	const firstDelay = 40 * time.Millisecond
	const extendedDelay = 160 * time.Millisecond

	batch := fakeBatch(1, makeEvent("e1", "main"))
	s, err := New(testOptions(), newFakeQueue(batch), &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &recordingCollectClient{ctx: ctx, batchSentAt: make(chan time.Time, 1)}
	c := s.newConn(ctx, cancel, cancel, stream)
	c.inflight[1] = batch
	c.inflightN = 1

	if err := c.handleRetry(retryFor(batch, firstDelay)); err != nil {
		t.Fatalf("first handleRetry: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	extendedAt := time.Now()
	if err := c.handleRetry(retryFor(batch, extendedDelay)); err != nil {
		t.Fatalf("extended handleRetry: %v", err)
	}

	select {
	case sentAt := <-stream.batchSentAt:
		if elapsed := sentAt.Sub(extendedAt); elapsed < extendedDelay*8/10 {
			t.Fatalf("coalesced retry sent after %v, want extended delay %v", elapsed, extendedDelay)
		}
	case <-time.After(time.Second):
		t.Fatal("extended retry was not sent")
	}
}

func TestTerminalAckClearsPersistentRetryDeadline(t *testing.T) {
	t.Parallel()
	batch := fakeBatch(1, makeEvent("e1", "main"))
	s, err := New(testOptions(), newFakeQueue(batch), &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := s.newConn(context.Background(), func() {}, func() {}, nil)
	c.inflight[1] = batch
	c.inflightN = 1
	c.highestSentBatchSequence = 1
	s.deferBatchRetry(batch, time.Hour)

	if err := c.handleAck(&opensplunkv1.BatchAck{
		BatchId:            batch.GetBatchId(),
		BatchSequence:      batch.GetBatchSequence(),
		Durability:         opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
		AcceptedEventCount: 1,
	}); err != nil {
		t.Fatalf("handleAck: %v", err)
	}
	s.retryMu.Lock()
	_, retained := s.retryNotBefore[batch.GetBatchSequence()]
	s.retryMu.Unlock()
	if retained {
		t.Fatal("terminal ack retained persistent retry deadline")
	}
}

func TestThrottleExpiryUsesServerRelativeDurationUnderClockSkew(t *testing.T) {
	t.Parallel()
	serverSentAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	collectorReceivedAt := serverSentAt.Add(24 * time.Hour)
	const activeFor = 10 * time.Second
	const minimumDelay = 2 * time.Second

	s, err := New(testOptions(), newFakeQueue(), &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.now = func() time.Time { return collectorReceivedAt }
	c := s.newConn(context.Background(), func() {}, func() {}, nil)
	c.lastBatchSendAt = collectorReceivedAt

	c.handleThrottle(&opensplunkv1.CollectResponse{
		SentAt: timestamppb.New(serverSentAt),
		Payload: &opensplunkv1.CollectResponse_Throttle{Throttle: &opensplunkv1.Throttle{
			Reason:           opensplunkv1.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA,
			MinimumSendDelay: durationpb.New(minimumDelay),
			EffectiveUntil:   timestamppb.New(serverSentAt.Add(activeFor)),
		}},
	})

	c.mu.Lock()
	until := c.throttleUntil
	c.mu.Unlock()
	if want := collectorReceivedAt.Add(activeFor); !until.Equal(want) {
		t.Fatalf("local throttle expiry = %v, want %v derived from server-relative duration", until, want)
	}
	if got := c.throttleWaitDuration(); got < minimumDelay {
		t.Fatalf("throttle pacing wait = %v, want at least %v despite collector clock skew", got, minimumDelay)
	}
}

func TestPumpRechecksThrottleAfterBlockingNextBatch(t *testing.T) {
	t.Parallel()
	const minimumDelay = 150 * time.Millisecond

	base := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))
	queue := &gatedNextBatchQueue{
		Queue:   base,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	s, err := New(testOptions(), queue, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &recordingCollectClient{ctx: ctx, batchSentAt: make(chan time.Time, 1)}
	c := s.newConn(ctx, cancel, cancel, stream)
	c.ready = defaultReady()
	c.maxInFlight = int(c.ready.GetMaxInFlightBatches())
	c.maxBatchEvents = c.ready.GetMaxBatchEvents()
	c.maxBatchBytes = c.ready.GetMaxBatchBytes()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.pumpLoop()
	}()
	select {
	case <-queue.entered:
	case <-time.After(time.Second):
		t.Fatal("pump did not block in NextBatch")
	}

	serverSentAt := time.Now()
	c.handleThrottle(&opensplunkv1.CollectResponse{
		SentAt: timestamppb.New(serverSentAt),
		Payload: &opensplunkv1.CollectResponse_Throttle{Throttle: &opensplunkv1.Throttle{
			Reason:           opensplunkv1.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA,
			MinimumSendDelay: durationpb.New(minimumDelay),
			EffectiveUntil:   timestamppb.New(serverSentAt.Add(time.Second)),
		}},
	})
	throttleAppliedAt := time.Now()
	close(queue.release)

	select {
	case sentAt := <-stream.batchSentAt:
		if elapsed := sentAt.Sub(throttleAppliedAt); elapsed < minimumDelay*8/10 {
			t.Fatalf("batch sent %v after throttle arrived, want at least %v", elapsed, minimumDelay)
		}
	case <-time.After(time.Second):
		t.Fatal("pump did not send batch after throttle delay")
	}

	cancel()
	c.mu.Lock()
	c.cond.Broadcast()
	c.mu.Unlock()
	<-done
}

func TestFiniteThrottleExpiryWakesInFlightCapacityWait(t *testing.T) {
	t.Parallel()
	const activeFor = 150 * time.Millisecond

	base := newFakeQueue(fakeBatch(2, makeEvent("e2", "main")))
	queue := &gatedNextBatchQueue{
		Queue:   base,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	s, err := New(testOptions(), queue, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &recordingCollectClient{ctx: ctx, batchSentAt: make(chan time.Time, 1)}
	c := s.newConn(ctx, cancel, cancel, stream)
	c.ready = defaultReady()
	c.ready.MaxInFlightBatches = 2
	c.maxInFlight = 2
	c.maxBatchEvents = c.ready.GetMaxBatchEvents()
	c.maxBatchBytes = c.ready.GetMaxBatchBytes()
	c.inflight[1] = fakeBatch(1, makeEvent("e1", "main"))
	c.inflightN = 1

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.pumpLoop()
	}()
	select {
	case <-queue.entered:
	case <-time.After(time.Second):
		t.Fatal("pump did not block in NextBatch")
	}

	serverSentAt := time.Now()
	c.handleThrottle(&opensplunkv1.CollectResponse{
		SentAt: timestamppb.New(serverSentAt),
		Payload: &opensplunkv1.CollectResponse_Throttle{Throttle: &opensplunkv1.Throttle{
			Reason:             opensplunkv1.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA,
			EffectiveUntil:     timestamppb.New(serverSentAt.Add(activeFor)),
			MaxInFlightBatches: 1,
		}},
	})
	throttleAppliedAt := time.Now()
	close(queue.release)

	select {
	case sentAt := <-stream.batchSentAt:
		if elapsed := sentAt.Sub(throttleAppliedAt); elapsed < activeFor*3/5 {
			t.Fatalf("batch sent after %v, want temporary in-flight limit held for about %v", elapsed, activeFor)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("finite throttle expiry did not wake in-flight capacity wait")
	}

	cancel()
	<-done
}

func TestSupersedingThrottleWakesLongPacingWait(t *testing.T) {
	t.Parallel()

	base := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))
	queue := &gatedNextBatchQueue{
		Queue:   base,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	s, err := New(testOptions(), queue, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &recordingCollectClient{ctx: ctx, batchSentAt: make(chan time.Time, 1)}
	c := s.newConn(ctx, cancel, cancel, stream)
	c.ready = defaultReady()
	c.maxInFlight = int(c.ready.GetMaxInFlightBatches())
	c.maxBatchEvents = c.ready.GetMaxBatchEvents()
	c.maxBatchBytes = c.ready.GetMaxBatchBytes()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.pumpLoop()
	}()
	select {
	case <-queue.entered:
	case <-time.After(time.Second):
		t.Fatal("pump did not block in NextBatch")
	}

	serverSentAt := time.Now()
	c.handleThrottle(&opensplunkv1.CollectResponse{
		SentAt: timestamppb.New(serverSentAt),
		Payload: &opensplunkv1.CollectResponse_Throttle{Throttle: &opensplunkv1.Throttle{
			Reason:           opensplunkv1.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA,
			MinimumSendDelay: durationpb.New(time.Hour),
			EffectiveUntil:   timestamppb.New(serverSentAt.Add(time.Hour)),
		}},
	})
	close(queue.release)
	select {
	case <-stream.batchSentAt:
		t.Fatal("batch ignored the initial long pacing throttle")
	case <-time.After(25 * time.Millisecond):
	}

	liftedAt := time.Now()
	c.handleThrottle(&opensplunkv1.CollectResponse{
		SentAt: timestamppb.New(liftedAt),
		Payload: &opensplunkv1.CollectResponse_Throttle{Throttle: &opensplunkv1.Throttle{
			Reason:         opensplunkv1.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA,
			EffectiveUntil: timestamppb.New(liftedAt),
		}},
	})
	select {
	case <-stream.batchSentAt:
	case <-time.After(time.Second):
		t.Fatal("superseding throttle did not wake the previous pacing wait")
	}

	cancel()
	<-done
}

func TestReplacementThrottleReevaluatesRelaxedBatchLimit(t *testing.T) {
	t.Parallel()

	batch := fakeBatch(1, makeEvent("e1", "main"))
	batch.UncompressedSizeBytes = 100
	base := newFakeQueue(batch)
	queue := &gatedNextBatchQueue{
		Queue:   base,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	s, err := New(testOptions(), queue, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &recordingCollectClient{ctx: ctx, batchSentAt: make(chan time.Time, 1)}
	c := s.newConn(ctx, cancel, cancel, stream)
	c.ready = defaultReady()
	c.maxInFlight = int(c.ready.GetMaxInFlightBatches())
	c.maxBatchEvents = c.ready.GetMaxBatchEvents()
	c.maxBatchBytes = c.ready.GetMaxBatchBytes()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.pumpLoop()
	}()
	select {
	case <-queue.entered:
	case <-time.After(time.Second):
		t.Fatal("pump did not block in NextBatch")
	}

	c.handleThrottle(&opensplunkv1.CollectResponse{
		SentAt: timestamppb.Now(),
		Payload: &opensplunkv1.CollectResponse_Throttle{Throttle: &opensplunkv1.Throttle{
			Reason:        opensplunkv1.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA,
			MaxBatchBytes: 1,
		}},
	})
	close(queue.release)
	select {
	case <-stream.batchSentAt:
		t.Fatal("batch ignored the initial reduced size limit")
	case <-time.After(25 * time.Millisecond):
	}

	c.handleThrottle(&opensplunkv1.CollectResponse{
		SentAt: timestamppb.Now(),
		Payload: &opensplunkv1.CollectResponse_Throttle{Throttle: &opensplunkv1.Throttle{
			Reason: opensplunkv1.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA,
		}},
	})
	select {
	case <-stream.batchSentAt:
	case <-time.After(time.Second):
		t.Fatal("replacement throttle did not re-evaluate its relaxed batch limit")
	}

	cancel()
	<-done
}

func TestTerminalReleaseCancelsRetryWaitingOutThrottle(t *testing.T) {
	t.Parallel()
	batch := fakeBatch(1, makeEvent("e1", "main"))
	batch.UncompressedSizeBytes = 100
	s, err := New(testOptions(), newFakeQueue(batch), &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &recordingCollectClient{ctx: ctx, batchSentAt: make(chan time.Time, 1)}
	c := s.newConn(ctx, cancel, cancel, stream)
	c.inflight[1] = batch
	c.inflightN = 1

	serverSentAt := time.Now()
	c.handleThrottle(&opensplunkv1.CollectResponse{
		SentAt: timestamppb.New(serverSentAt),
		Payload: &opensplunkv1.CollectResponse_Throttle{Throttle: &opensplunkv1.Throttle{
			Reason:         opensplunkv1.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA,
			EffectiveUntil: timestamppb.New(serverSentAt.Add(time.Hour)),
			MaxBatchBytes:  1,
		}},
	})
	if err := c.handleRetry(retryFor(batch, 0)); err != nil {
		t.Fatalf("handleRetry: %v", err)
	}
	c.mu.Lock()
	retry := c.pendingRetry[1]
	c.mu.Unlock()
	if retry == nil {
		t.Fatal("retry was not scheduled")
	}
	time.Sleep(10 * time.Millisecond)

	c.releaseInflight(1)
	select {
	case <-retry.done:
	case <-time.After(time.Second):
		t.Fatal("terminal release did not cancel retry waiting out long throttle")
	}
}

func retryFor(batch *opensplunkv1.EventBatch, delay time.Duration) *opensplunkv1.RetryBatch {
	return &opensplunkv1.RetryBatch{
		BatchId:       batch.GetBatchId(),
		BatchSequence: batch.GetBatchSequence(),
		Reason:        opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED,
		RetryAfter:    durationpb.New(delay),
	}
}

type gatedNextBatchQueue struct {
	wal.Queue
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (q *gatedNextBatchQueue) NextBatch(ctx context.Context) (*opensplunkv1.EventBatch, error) {
	q.once.Do(func() { close(q.entered) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.release:
		return q.Queue.NextBatch(ctx)
	}
}

type recordingCollectClient struct {
	ctx         context.Context
	batchSentAt chan time.Time
}

func (c *recordingCollectClient) Send(req *opensplunkv1.CollectRequest) error {
	if req.GetBatch() != nil {
		c.batchSentAt <- time.Now()
	}
	return nil
}

func (c *recordingCollectClient) Recv() (*opensplunkv1.CollectResponse, error) {
	return nil, io.EOF
}

func (c *recordingCollectClient) Header() (metadata.MD, error) { return nil, nil }
func (c *recordingCollectClient) Trailer() metadata.MD         { return nil }
func (c *recordingCollectClient) CloseSend() error             { return nil }
func (c *recordingCollectClient) Context() context.Context     { return c.ctx }
func (c *recordingCollectClient) SendMsg(any) error            { return nil }
func (c *recordingCollectClient) RecvMsg(any) error            { return io.EOF }
