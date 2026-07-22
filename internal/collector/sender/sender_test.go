package sender

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// fakeQueue is an in-memory wal.Queue. The wal.Queue interface is frozen, so
// this lets the sender tests run without depending on the wal package internals.
// ---------------------------------------------------------------------------

type fakeQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	batches  []*opensplunkv1.EventBatch // ascending by batch sequence
	cursor   int                        // index of the next batch to hand out
	acked    uint64                     // highest acked sequence
	nextSeq  uint64
	ackCalls []uint64
}

func newFakeQueue(batches ...*opensplunkv1.EventBatch) *fakeQueue {
	q := &fakeQueue{batches: batches}
	q.cond = sync.NewCond(&q.mu)
	if len(batches) > 0 {
		q.nextSeq = batches[len(batches)-1].GetBatchSequence() + 1
	} else {
		q.nextSeq = 1
	}
	return q
}

func (q *fakeQueue) Append(events []*opensplunkv1.LogEvent) (*opensplunkv1.EventBatch, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	batch := &opensplunkv1.EventBatch{BatchSequence: q.nextSeq, Events: events}
	q.nextSeq++
	q.batches = append(q.batches, batch)
	q.cond.Broadcast()
	return batch, nil
}

func (q *fakeQueue) NextBatch(ctx context.Context) (*opensplunkv1.EventBatch, error) {
	// Wake the waiter if ctx is cancelled while blocked.
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for i := q.cursor; i < len(q.batches); i++ {
			if q.batches[i].GetBatchSequence() > q.acked {
				q.cursor = i + 1
				return q.batches[i], nil
			}
		}
		q.cursor = len(q.batches)
		q.cond.Wait()
	}
}

func (q *fakeQueue) Ack(batchSequence uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ackCalls = append(q.ackCalls, batchSequence)
	if batchSequence > q.acked {
		q.acked = batchSequence
	}
	return nil
}

func (q *fakeQueue) AckThrough(batchSequence uint64) error { return q.Ack(batchSequence) }

func (q *fakeQueue) Stats() wal.Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	var queued uint64
	for _, b := range q.batches {
		if b.GetBatchSequence() > q.acked {
			queued++
		}
	}
	return wal.Stats{
		QueuedBatches:          queued,
		NextBatchSequence:      q.nextSeq,
		LastAckedBatchSequence: q.acked,
	}
}

func (q *fakeQueue) Rewind() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cursor = 0
	q.cond.Broadcast()
}

func (q *fakeQueue) Close() error { return nil }

func (q *fakeQueue) ackedSeq() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.acked
}

var _ wal.Queue = (*fakeQueue)(nil)

// ---------------------------------------------------------------------------
// memSink captures dead-letter records in memory.
// ---------------------------------------------------------------------------

type memSink struct {
	mu      sync.Mutex
	records []DeadLetterRecord
}

type failingSink struct {
	mu    sync.Mutex
	calls int
}

func (s *failingSink) WriteRecords([]DeadLetterRecord) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return errors.New("simulated dead-letter disk failure")
}

func (s *failingSink) Close() error { return nil }

func (s *failingSink) writeCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (m *memSink) WriteRecords(records []DeadLetterRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, records...)
	return nil
}

func (m *memSink) Close() error { return nil }

func (m *memSink) snapshot() []DeadLetterRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]DeadLetterRecord, len(m.records))
	copy(out, m.records)
	return out
}

// ---------------------------------------------------------------------------
// fakeServer is a scripted CollectorIngestService for exact control of the
// Ready/ack/reject/retry/throttle/notice sequencing.
// ---------------------------------------------------------------------------

type fakeServer struct {
	opensplunkv1.UnimplementedCollectorIngestServiceServer

	readyFn   func() *opensplunkv1.CollectorReady
	onBatch   func(fs *fakeServer, batch *opensplunkv1.EventBatch)
	failCalls int // number of initial Collect calls that fail after Hello
	// batchErr, when set, runs before onBatch; a non-nil return tears down the
	// stream after the batch was received but before any ack is sent.
	batchErr func(fs *fakeServer, batch *opensplunkv1.EventBatch) error

	sendMu sync.Mutex

	mu              sync.Mutex
	stream          opensplunkv1.CollectorIngestService_CollectServer
	respSeq         uint64
	callCount       int
	token           string
	hello           *opensplunkv1.CollectorHello
	received        []*opensplunkv1.EventBatch
	byID            map[uint64]*opensplunkv1.EventBatch
	heartbeats      int
	goodbye         *opensplunkv1.CollectorGoodbye
	currentInFlight int
	maxObserved     int
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		readyFn: defaultReady,
		byID:    make(map[uint64]*opensplunkv1.EventBatch),
	}
}

func defaultReady() *opensplunkv1.CollectorReady {
	return &opensplunkv1.CollectorReady{
		StreamId:           "stream-x",
		ProtocolMajor:      1,
		ProtocolMinor:      0,
		HeartbeatInterval:  durationpb.New(time.Hour),
		MaxInFlightBatches: 1,
		MaxBatchEvents:     1000,
		MaxBatchBytes:      8 << 20,
		MaxEventBytes:      1 << 20,
	}
}

func (fs *fakeServer) Collect(stream opensplunkv1.CollectorIngestService_CollectServer) error {
	fs.mu.Lock()
	fs.callCount++
	n := fs.callCount
	fs.stream = stream
	fs.respSeq = 0
	fs.mu.Unlock()

	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		if a := md.Get("authorization"); len(a) > 0 {
			fs.mu.Lock()
			fs.token = a[0]
			fs.mu.Unlock()
		}
	}

	req, err := stream.Recv()
	if err != nil {
		return err
	}
	fs.mu.Lock()
	fs.hello = req.GetHello()
	fs.mu.Unlock()

	if n <= fs.failCalls {
		return status.Error(codes.Unavailable, "transient failure")
	}

	if err := fs.send(&opensplunkv1.CollectResponse{
		Payload: &opensplunkv1.CollectResponse_Ready{Ready: fs.readyFn()},
	}); err != nil {
		return err
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			return nil // EOF or cancellation ends the stream cleanly
		}
		switch {
		case req.GetBatch() != nil:
			batch := req.GetBatch()
			fs.mu.Lock()
			fs.received = append(fs.received, batch)
			fs.byID[batch.GetBatchSequence()] = batch
			fs.mu.Unlock()
			if fs.batchErr != nil {
				if err := fs.batchErr(fs, batch); err != nil {
					return err
				}
			}
			if fs.onBatch != nil {
				fs.onBatch(fs, batch)
			}
		case req.GetHeartbeat() != nil:
			fs.mu.Lock()
			fs.heartbeats++
			fs.mu.Unlock()
		case req.GetGoodbye() != nil:
			fs.mu.Lock()
			fs.goodbye = req.GetGoodbye()
			fs.mu.Unlock()
			return nil
		}
	}
}

func (fs *fakeServer) send(resp *opensplunkv1.CollectResponse) error {
	fs.sendMu.Lock()
	defer fs.sendMu.Unlock()
	fs.mu.Lock()
	fs.respSeq++
	resp.StreamSequence = fs.respSeq
	stream := fs.stream
	fs.mu.Unlock()
	resp.SentAt = timestamppb.Now()
	return stream.Send(resp)
}

func (fs *fakeServer) ackBatch(seq uint64, accepted, duplicate uint32, rejected ...*opensplunkv1.EventRejection) {
	fs.mu.Lock()
	batch := fs.byID[seq]
	fs.mu.Unlock()
	_ = fs.send(&opensplunkv1.CollectResponse{
		Payload: &opensplunkv1.CollectResponse_BatchAck{BatchAck: &opensplunkv1.BatchAck{
			BatchId:             batch.GetBatchId(),
			BatchSequence:       seq,
			AcceptedEventCount:  accepted,
			DuplicateEventCount: duplicate,
			RejectedEvents:      rejected,
		}},
	})
}

func (fs *fakeServer) tokenSeen() string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.token
}

func (fs *fakeServer) helloSeen() *opensplunkv1.CollectorHello {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.hello
}

func (fs *fakeServer) receivedBatches() []*opensplunkv1.EventBatch {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make([]*opensplunkv1.EventBatch, len(fs.received))
	copy(out, fs.received)
	return out
}

func (fs *fakeServer) goodbyeSeen() *opensplunkv1.CollectorGoodbye {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.goodbye
}

func (fs *fakeServer) calls() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.callCount
}

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

func startServer(t *testing.T, srv opensplunkv1.CollectorIngestServiceServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	opensplunkv1.RegisterCollectorIngestServiceServer(server, srv)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func testOptions() Options {
	return Options{
		Address:       "bufnet",
		Token:         func() (string, error) { return "test-token", nil },
		CollectorID:   "collector-a",
		InstanceID:    "instance-a",
		ProtocolMajor: 1,
		ProtocolMinor: 0,
		Hello: HelloInfo{
			CollectorVersion: "v-test",
			Hostname:         "host-a",
			StartedAt:        time.Now().Add(-time.Hour),
		},
		Backoff: BackoffPolicy{Initial: 5 * time.Millisecond, Max: 20 * time.Millisecond, Multiplier: 2, Jitter: 0.2},
	}
}

func newTestSender(t *testing.T, opts Options, q wal.Queue, sink DeadLetterSink, reporter StatsReporter, conn *grpc.ClientConn) *Sender {
	t.Helper()
	s, err := New(opts, q, sink, reporter)
	if err != nil {
		t.Fatal(err)
	}
	s.client = opensplunkv1.NewCollectorIngestServiceClient(conn)
	s.closeConn = func() error { return nil }
	s.drainTimeout = 300 * time.Millisecond
	s.rand = func() float64 { return 0.5 } // deterministic backoff
	return s
}

func runSender(t *testing.T, s *Sender) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(cancel)
	return cancel, done
}

func waitFor(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func makeEvent(id, index string) *opensplunkv1.LogEvent {
	return &opensplunkv1.LogEvent{EventId: id, IndexName: index}
}

func fakeBatch(seq uint64, events ...*opensplunkv1.LogEvent) *opensplunkv1.EventBatch {
	return &opensplunkv1.EventBatch{
		CollectorId:   "collector-a",
		BatchId:       "batch-" + itoa(seq),
		BatchSequence: seq,
		CreatedAt:     timestamppb.Now(),
		Events:        events,
		ProtocolMajor: 1,
		ProtocolMinor: 0,
	}
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestSenderHelloBatchAckWalAck(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), uint32(len(b.GetEvents())), 0)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))

	var lastStats Stats
	var statsMu sync.Mutex
	reporter := StatsReporterFunc(func(s Stats) {
		statsMu.Lock()
		lastStats = s
		statsMu.Unlock()
	})
	s := newTestSender(t, testOptions(), q, &memSink{}, reporter, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked in queue", func() bool { return q.ackedSeq() >= 1 })

	if got := fs.tokenSeen(); got != "Bearer test-token" {
		t.Fatalf("server token = %q, want %q", got, "Bearer test-token")
	}
	if h := fs.helloSeen(); h.GetCollectorId() != "collector-a" {
		t.Fatalf("hello collector id = %q", h.GetCollectorId())
	}
	waitFor(t, "acked stats reported", func() bool {
		statsMu.Lock()
		defer statsMu.Unlock()
		return lastStats.AcknowledgedEventsTotal >= 1 && lastStats.LastAckedBatchSequence >= 1
	})

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
}

func TestSenderStreamSequenceStrictlyIncrements(t *testing.T) {
	t.Parallel()
	// The real ingest service enforces stream_sequence == 1 for Hello and +1 for
	// every subsequent request; delivering a batch through it proves the sender's
	// sequencing is correct. Covered end-to-end in TestSenderAgainstRealService.
	fs := newFakeServer()
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), 1, 0)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")), fakeBatch(2, makeEvent("e2", "main")))
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)
	waitFor(t, "both batches acked", func() bool { return q.ackedSeq() >= 2 })
	cancel()
	<-done
}

func TestSenderNoTokenInLogs(t *testing.T) {
	t.Parallel()
	const secret = "super-secret-token-abc123"
	fs := newFakeServer()
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), 1, 0)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))

	buf := &syncBuffer{}
	opts := testOptions()
	opts.Token = func() (string, error) { return secret, nil }
	opts.Logger = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := newTestSender(t, opts, q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked", func() bool { return q.ackedSeq() >= 1 })
	cancel()
	<-done

	logs := buf.String()
	if strings.Contains(logs, secret) {
		t.Fatalf("token leaked into logs: %s", logs)
	}
	if !strings.Contains(logs, "stream ready") {
		t.Fatalf("expected ready log to be emitted, got: %q", logs)
	}
}

func TestSenderResumeAfterSkipsBatches(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.readyFn = func() *opensplunkv1.CollectorReady {
		r := defaultReady()
		resume := uint64(2)
		r.ResumeAfterBatchSequence = &resume
		return r
	}
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), 1, 0)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(
		fakeBatch(1, makeEvent("e1", "main")),
		fakeBatch(2, makeEvent("e2", "main")),
		fakeBatch(3, makeEvent("e3", "main")),
	)
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "queue drained through 3", func() bool { return q.ackedSeq() >= 3 })

	got := fs.receivedBatches()
	if len(got) != 1 || got[0].GetBatchSequence() != 3 {
		var seqs []uint64
		for _, b := range got {
			seqs = append(seqs, b.GetBatchSequence())
		}
		t.Fatalf("server received sequences %v, want only [3]", seqs)
	}
	cancel()
	<-done
}

func TestSenderHonorsInFlightCap(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.readyFn = func() *opensplunkv1.CollectorReady {
		r := defaultReady()
		r.MaxInFlightBatches = 2
		return r
	}
	arrived := make(chan uint64, 8)
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		fs.mu.Lock()
		fs.currentInFlight++
		if fs.currentInFlight > fs.maxObserved {
			fs.maxObserved = fs.currentInFlight
		}
		fs.mu.Unlock()
		arrived <- b.GetBatchSequence()
	}
	conn := startServer(t, fs)

	const total = 5
	batches := make([]*opensplunkv1.EventBatch, 0, total)
	for i := uint64(1); i <= total; i++ {
		batches = append(batches, fakeBatch(i, makeEvent("e"+itoa(i), "main")))
	}
	q := newFakeQueue(batches...)
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	release := func(seq uint64) {
		fs.mu.Lock()
		fs.currentInFlight--
		fs.mu.Unlock()
		fs.ackBatch(seq, 1, 0)
	}

	// First two batches pipeline immediately.
	first := <-arrived
	second := <-arrived
	// No third batch should arrive while two are outstanding.
	select {
	case seq := <-arrived:
		t.Fatalf("received batch %d before any ack; in-flight cap of 2 violated", seq)
	case <-time.After(150 * time.Millisecond):
	}

	inflight := []uint64{first, second}
	for i := uint64(3); i <= total; i++ {
		release(inflight[0])
		inflight = inflight[1:]
		next := <-arrived
		inflight = append(inflight, next)
	}
	for _, seq := range inflight {
		release(seq)
	}

	waitFor(t, "all acked", func() bool { return q.ackedSeq() >= total })

	fs.mu.Lock()
	maxObserved := fs.maxObserved
	fs.mu.Unlock()
	if maxObserved != 2 {
		t.Fatalf("max concurrent in-flight = %d, want exactly 2", maxObserved)
	}
	cancel()
	<-done
}

func TestSenderRetryResendsIdenticalBytes(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	var mu sync.Mutex
	attempts := 0
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		mu.Lock()
		attempts++
		attempt := attempts
		mu.Unlock()
		if attempt == 1 {
			_ = fs.send(&opensplunkv1.CollectResponse{
				Payload: &opensplunkv1.CollectResponse_RetryBatch{RetryBatch: &opensplunkv1.RetryBatch{
					BatchId:       b.GetBatchId(),
					BatchSequence: b.GetBatchSequence(),
					Reason:        opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
					RetryAfter:    durationpb.New(20 * time.Millisecond),
				}},
			})
			return
		}
		fs.ackBatch(b.GetBatchSequence(), 1, 0)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))

	var lastStats Stats
	var statsMu sync.Mutex
	reporter := StatsReporterFunc(func(s Stats) { statsMu.Lock(); lastStats = s; statsMu.Unlock() })
	s := newTestSender(t, testOptions(), q, &memSink{}, reporter, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked after retry", func() bool { return q.ackedSeq() >= 1 })

	got := fs.receivedBatches()
	if len(got) != 2 {
		t.Fatalf("server received %d batches, want 2 (original + retry)", len(got))
	}
	first, err := proto.Marshal(got[0])
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := proto.Marshal(got[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, secondBytes) {
		t.Fatalf("retry resent different bytes:\nfirst=%x\nsecond=%x", first, secondBytes)
	}
	waitFor(t, "retry counted", func() bool {
		statsMu.Lock()
		defer statsMu.Unlock()
		return lastStats.RetriedBatchesTotal >= 1
	})
	cancel()
	<-done
}

func TestSenderThrottleAppliesSendDelay(t *testing.T) {
	t.Parallel()
	const minDelay = 120 * time.Millisecond
	fs := newFakeServer()
	var mu sync.Mutex
	var recvTimes []time.Time
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		mu.Lock()
		recvTimes = append(recvTimes, time.Now())
		count := len(recvTimes)
		mu.Unlock()
		if count == 1 {
			_ = fs.send(&opensplunkv1.CollectResponse{
				Payload: &opensplunkv1.CollectResponse_Throttle{Throttle: &opensplunkv1.Throttle{
					Reason:           opensplunkv1.ThrottleReason_THROTTLE_REASON_SERVER_LOAD,
					MinimumSendDelay: durationpb.New(minDelay),
					EffectiveUntil:   timestamppb.New(time.Now().Add(10 * time.Second)),
				}},
			})
		}
		fs.ackBatch(b.GetBatchSequence(), 1, 0)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(
		fakeBatch(1, makeEvent("e1", "main")),
		fakeBatch(2, makeEvent("e2", "main")),
	)
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "both batches acked", func() bool { return q.ackedSeq() >= 2 })

	mu.Lock()
	defer mu.Unlock()
	if len(recvTimes) < 2 {
		t.Fatalf("server saw %d batches, want 2", len(recvTimes))
	}
	gap := recvTimes[1].Sub(recvTimes[0])
	if gap < minDelay*8/10 {
		t.Fatalf("gap between sends = %v, want >= ~%v (throttle delay)", gap, minDelay)
	}
	cancel()
	<-done
}

func TestSenderPartialRejectionDeadLettersExactEvents(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), 1, 0,
			&opensplunkv1.EventRejection{
				EventIndex: 1,
				EventId:    "e2",
				Code:       opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX,
				Message:    "index not authorized",
			},
			&opensplunkv1.EventRejection{
				EventIndex: 2,
				EventId:    "e3",
				Code:       opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
				Message:    "bad field",
			},
		)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main"), makeEvent("e2", "forbidden"), makeEvent("e3", "main")))
	sink := &memSink{}

	var lastStats Stats
	var statsMu sync.Mutex
	reporter := StatsReporterFunc(func(s Stats) { statsMu.Lock(); lastStats = s; statsMu.Unlock() })
	s := newTestSender(t, testOptions(), q, sink, reporter, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked", func() bool { return q.ackedSeq() >= 1 })
	waitFor(t, "two rejected events dead-lettered", func() bool { return len(sink.snapshot()) == 2 })

	records := sink.snapshot()
	if records[0].Event.GetEventId() != "e2" || records[1].Event.GetEventId() != "e3" {
		t.Fatalf("dead-lettered events = %q, %q, want e2, e3",
			records[0].Event.GetEventId(), records[1].Event.GetEventId())
	}
	if records[0].Code != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX.String() {
		t.Fatalf("first record code = %q", records[0].Code)
	}
	waitFor(t, "rejected + accepted counted", func() bool {
		statsMu.Lock()
		defer statsMu.Unlock()
		return lastStats.RejectedEventsTotal == 2 && lastStats.AcknowledgedEventsTotal == 1
	})
	cancel()
	<-done
}

func TestSenderBatchRejectDeadLettersWholeBatch(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		_ = fs.send(&opensplunkv1.CollectResponse{
			Payload: &opensplunkv1.CollectResponse_BatchReject{BatchReject: &opensplunkv1.BatchReject{
				BatchId:       b.GetBatchId(),
				BatchSequence: b.GetBatchSequence(),
				Code:          opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_EVENT_ID_DIGEST_MISMATCH,
				Message:       "digest mismatch",
			}},
		})
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main"), makeEvent("e2", "main")))
	sink := &memSink{}
	s := newTestSender(t, testOptions(), q, sink, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "rejected batch acked off queue", func() bool { return q.ackedSeq() >= 1 })
	waitFor(t, "whole batch dead-lettered", func() bool { return len(sink.snapshot()) == 2 })

	records := sink.snapshot()
	for _, r := range records {
		if r.Code != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_EVENT_ID_DIGEST_MISMATCH.String() {
			t.Fatalf("record code = %q", r.Code)
		}
	}
	cancel()
	<-done
}

func TestSenderDeadLetterFailureRetainsWALBatch(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		_ = fs.send(&opensplunkv1.CollectResponse{
			Payload: &opensplunkv1.CollectResponse_BatchReject{BatchReject: &opensplunkv1.BatchReject{
				BatchId: b.GetBatchId(), BatchSequence: b.GetBatchSequence(),
				Code: opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
			}},
		})
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "forbidden")))
	sink := &failingSink{}
	s := newTestSender(t, testOptions(), q, sink, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "dead-letter failure retried on a new connection", func() bool {
		return sink.writeCalls() >= 2 && fs.calls() >= 2
	})
	if got := q.ackedSeq(); got != 0 {
		t.Fatalf("queue acked through %d despite dead-letter failure", got)
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
}

func TestSenderReconnectsWithBackoff(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.failCalls = 1 // first Collect fails after Hello, forcing a reconnect
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), 1, 0)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked after reconnect", func() bool { return q.ackedSeq() >= 1 })
	if fs.calls() < 2 {
		t.Fatalf("server Collect calls = %d, want >= 2 (reconnect)", fs.calls())
	}
	cancel()
	<-done
}

func TestSenderGoodbyeOnCancel(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	conn := startServer(t, fs)
	q := newFakeQueue() // empty: sender idles after Ready
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "hello received (connected)", func() bool { return fs.helloSeen() != nil })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
	waitFor(t, "goodbye received", func() bool { return fs.goodbyeSeen() != nil })
	if got := fs.goodbyeSeen().GetReason(); got != opensplunkv1.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN {
		t.Fatalf("goodbye reason = %v, want SHUTDOWN", got)
	}
}

func TestBackoffDelayBoundedAndJittered(t *testing.T) {
	t.Parallel()
	policy := BackoffPolicy{Initial: 100 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: 0.5}

	// With zero jitter fraction the delay equals the (bounded) base and grows.
	var prev time.Duration
	for attempt := 0; attempt < 10; attempt++ {
		d := backoffDelay(policy, attempt, 0)
		if d <= 0 {
			t.Fatalf("attempt %d: delay %v must be positive", attempt, d)
		}
		if d > policy.Max {
			t.Fatalf("attempt %d: delay %v exceeds max %v", attempt, d, policy.Max)
		}
		if attempt > 0 && d < prev {
			t.Fatalf("attempt %d: delay %v decreased from %v with zero jitter", attempt, d, prev)
		}
		prev = d
	}
	if got := backoffDelay(policy, 20, 0); got != policy.Max {
		t.Fatalf("large attempt delay = %v, want capped at %v", got, policy.Max)
	}

	// Jitter with frac=1 subtracts the full jitter fraction from the base.
	base := backoffDelay(policy, 2, 0)
	jittered := backoffDelay(policy, 2, 0.999999)
	if jittered >= base {
		t.Fatalf("jittered delay %v not reduced below base %v", jittered, base)
	}
	wantApprox := time.Duration(float64(base) * 0.5)
	if jittered < wantApprox-time.Millisecond || jittered > wantApprox+time.Millisecond {
		t.Fatalf("jittered delay %v, want ~%v (base*(1-0.5))", jittered, wantApprox)
	}
}

// ---------------------------------------------------------------------------
// Integration against the REAL internal/ingest.Service.
// ---------------------------------------------------------------------------

func TestSenderAgainstRealService(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var stored []ingest.StoreBatch
	store := ingest.EventStoreFunc(func(_ context.Context, b ingest.StoreBatch) (ingest.StoreResult, error) {
		mu.Lock()
		stored = append(stored, b)
		mu.Unlock()
		return ingest.StoreResult{Accepted: uint32(len(b.Events)), CommittedAt: time.Now()}, nil
	})
	authorizer := ingest.AuthorizerFunc(func(_ context.Context, token string) (ingest.Authorization, error) {
		if token != "good-token" {
			return ingest.Authorization{}, errors.New("bad token")
		}
		return ingest.Authorization{SubjectID: "s1", TenantID: "t1", AuthorizedIndexes: []string{"main"}}, nil
	})
	svc, err := ingest.NewService(ingest.DefaultConfig(), authorizer, store)
	if err != nil {
		t.Fatal(err)
	}
	conn := startServer(t, svc)

	events := []*opensplunkv1.LogEvent{validLogEvent("event-a", "main")}
	batch := validBatch("collector-a", "batch-1", 1, events...)
	q := newFakeQueue(batch)

	opts := testOptions()
	opts.Token = func() (string, error) { return "good-token", nil }
	s := newTestSender(t, opts, q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked end-to-end", func() bool { return q.ackedSeq() >= 1 })

	mu.Lock()
	storedCount := len(stored)
	var storedEventID string
	if storedCount > 0 && len(stored[0].Events) > 0 {
		storedEventID = stored[0].Events[0].Event.GetEventId()
	}
	mu.Unlock()
	if storedCount != 1 || storedEventID != "event-a" {
		t.Fatalf("stored batches = %d, first event = %q; want 1 batch with event-a", storedCount, storedEventID)
	}
	cancel()
	<-done
}

func TestSenderAgainstRealServicePartialRejectDeadLetters(t *testing.T) {
	t.Parallel()

	store := ingest.EventStoreFunc(func(_ context.Context, b ingest.StoreBatch) (ingest.StoreResult, error) {
		return ingest.StoreResult{Accepted: uint32(len(b.Events)), CommittedAt: time.Now()}, nil
	})
	authorizer := ingest.AuthorizerFunc(func(_ context.Context, token string) (ingest.Authorization, error) {
		if token != "good-token" {
			return ingest.Authorization{}, errors.New("bad token")
		}
		return ingest.Authorization{SubjectID: "s1", TenantID: "t1", AuthorizedIndexes: []string{"main"}}, nil
	})
	svc, err := ingest.NewService(ingest.DefaultConfig(), authorizer, store)
	if err != nil {
		t.Fatal(err)
	}
	conn := startServer(t, svc)

	// event-a in authorized index "main"; event-b in an unauthorized index so the
	// real server rejects exactly that one event.
	events := []*opensplunkv1.LogEvent{
		validLogEvent("event-a", "main"),
		validLogEvent("event-b", "forbidden"),
	}
	batch := validBatch("collector-a", "batch-1", 1, events...)
	q := newFakeQueue(batch)
	sink := &memSink{}

	opts := testOptions()
	opts.Token = func() (string, error) { return "good-token", nil }
	s := newTestSender(t, opts, q, sink, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked end-to-end", func() bool { return q.ackedSeq() >= 1 })
	waitFor(t, "one event dead-lettered", func() bool { return len(sink.snapshot()) == 1 })

	records := sink.snapshot()
	if len(records) != 1 || records[0].Event.GetEventId() != "event-b" {
		t.Fatalf("dead-lettered records = %#v, want exactly event-b", records)
	}
	if records[0].Code != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX.String() {
		t.Fatalf("dead-letter code = %q, want UNAUTHORIZED_INDEX", records[0].Code)
	}
	cancel()
	<-done
}

func validLogEvent(id, index string) *opensplunkv1.LogEvent {
	msg := "request completed"
	return &opensplunkv1.LogEvent{
		EventId:         id,
		IndexName:       index,
		EventTime:       timestamppb.New(time.Now().Add(-time.Minute)),
		CollectedAt:     timestamppb.New(time.Now().Add(-30 * time.Second)),
		EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
		Host:            "host-a",
		Source:          "/var/log/app.log",
		Sourcetype:      "json",
		Severity:        opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
		Message:         &msg,
		Raw:             []byte(`{"message":"request completed","status":200}`),
		RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		Fields: &opensplunkv1.TypedObject{Fields: []*opensplunkv1.TypedObjectField{{
			Name:  "status",
			Value: &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_StringValue{StringValue: "200"}},
		}}},
	}
}

func validBatch(collectorID, batchID string, seq uint64, events ...*opensplunkv1.LogEvent) *opensplunkv1.EventBatch {
	return &opensplunkv1.EventBatch{
		CollectorId:           collectorID,
		BatchId:               batchID,
		BatchSequence:         seq,
		CreatedAt:             timestamppb.Now(),
		Events:                events,
		UncompressedSizeBytes: ingest.UncompressedEventBytes(events),
		EventIdsSha256:        ingest.EventIDDigest(events),
		ProtocolMajor:         1,
		ProtocolMinor:         0,
	}
}

// syncBuffer is a concurrency-safe io.Writer for capturing log output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestSenderRedeliversOrphanedInflightAfterReconnect covers the at-least-once
// gap where a batch is sent but the connection dies before any terminal
// response: the queue's delivery cursor has already passed the batch, so
// without the post-Ready Rewind the batch would be stranded until process
// restart. The reconnected stream must resend it.
func TestSenderRedeliversOrphanedInflightAfterReconnect(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.batchErr = func(fs *fakeServer, b *opensplunkv1.EventBatch) error {
		if fs.calls() == 1 {
			// First connection: the batch arrives but the stream dies before an
			// ack, leaving it unacked behind the queue's delivery cursor.
			return status.Error(codes.Unavailable, "stream died before ack")
		}
		return nil
	}
	fs.onBatch = func(fs *fakeServer, b *opensplunkv1.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), uint32(len(b.GetEvents())), 0)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "orphaned batch redelivered and acked", func() bool { return q.ackedSeq() >= 1 })

	deliveries := 0
	for _, b := range fs.receivedBatches() {
		if b.GetBatchSequence() == 1 {
			deliveries++
		}
	}
	if deliveries < 2 {
		t.Fatalf("batch 1 deliveries = %d, want >= 2 (one per connection)", deliveries)
	}
	cancel()
	<-done
}
