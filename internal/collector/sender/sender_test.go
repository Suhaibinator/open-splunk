package sender

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/collectorlimits"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	batches  []*opensplunk.EventBatch // ascending by batch sequence
	cursor   int                      // index of the next batch to hand out
	acked    uint64                   // highest acked sequence
	nextSeq  uint64
	ackCalls []uint64
	terminal map[uint64]struct{}
}

type nextBatchErrorQueue struct {
	*fakeQueue
	err error
}

func (q *nextBatchErrorQueue) NextBatch(context.Context) (*opensplunk.EventBatch, error) {
	return nil, q.err
}

func newFakeQueue(batches ...*opensplunk.EventBatch) *fakeQueue {
	q := &fakeQueue{batches: batches, terminal: make(map[uint64]struct{})}
	q.cond = sync.NewCond(&q.mu)
	if len(batches) > 0 {
		q.nextSeq = batches[len(batches)-1].GetBatchSequence() + 1
	} else {
		q.nextSeq = 1
	}
	return q
}

func (q *fakeQueue) Append(events []*opensplunk.LogEvent) (*opensplunk.EventBatch, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	batch := &opensplunk.EventBatch{BatchSequence: q.nextSeq, Events: events}
	q.nextSeq++
	q.batches = append(q.batches, batch)
	q.cond.Broadcast()
	return batch, nil
}

func (q *fakeQueue) NextBatch(ctx context.Context) (*opensplunk.EventBatch, error) {
	// Wake the waiter if ctx is canceled while blocked.
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
	if batchSequence <= q.acked {
		return nil
	}
	if !q.hasSequenceLocked(batchSequence) {
		return wal.ErrInvalidAck
	}
	q.terminal[batchSequence] = struct{}{}
	for _, batch := range q.batches {
		sequence := batch.GetBatchSequence()
		if sequence <= q.acked {
			continue
		}
		if _, ok := q.terminal[sequence]; !ok {
			break
		}
		q.acked = sequence
		delete(q.terminal, sequence)
	}
	return nil
}

func (q *fakeQueue) PrepareAck(batchSequence uint64) (wal.AckPreview, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.prepareAckLocked(batchSequence, false)
}

func (q *fakeQueue) AckThrough(batchSequence uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ackCalls = append(q.ackCalls, batchSequence)
	if batchSequence <= q.acked {
		return nil
	}
	if !q.hasSequenceLocked(batchSequence) {
		return wal.ErrInvalidAck
	}
	for _, batch := range q.batches {
		sequence := batch.GetBatchSequence()
		if sequence > q.acked && sequence <= batchSequence {
			q.terminal[sequence] = struct{}{}
			q.acked = sequence
			delete(q.terminal, sequence)
		}
	}
	return nil
}

func (q *fakeQueue) PrepareAckThrough(batchSequence uint64) (wal.AckPreview, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.prepareAckLocked(batchSequence, true)
}

func (q *fakeQueue) prepareAckLocked(batchSequence uint64, cumulative bool) (wal.AckPreview, error) {
	if batchSequence <= q.acked {
		return wal.AckPreview{}, nil
	}
	if !q.hasSequenceLocked(batchSequence) {
		return wal.AckPreview{}, wal.ErrInvalidAck
	}
	preview := wal.AckPreview{}
	var aggregator fakeSourceMarkAggregator
	for _, batch := range q.batches {
		sequence := batch.GetBatchSequence()
		if sequence <= q.acked {
			continue
		}
		_, terminal := q.terminal[sequence]
		if cumulative && sequence <= batchSequence {
			terminal = true
		}
		if sequence == batchSequence {
			terminal = true
		}
		if !terminal {
			break
		}
		preview.BatchCount++
		preview.ThroughBatchSequence = sequence
		if !aggregator.failed {
			for _, mark := range fakeSourceMarks(batch) {
				if !aggregator.add(mark) {
					break
				}
			}
		}
	}
	preview.Marks = aggregator.marks()
	return preview, nil
}

type fakeSourceMarkKey struct {
	inputID      string
	fileIdentity string
}

type fakeSourceMarkAggregator struct {
	bySource map[fakeSourceMarkKey]wal.SourceCheckpointMark
	failure  wal.SourceCheckpointMark
	failed   bool
}

func (aggregator *fakeSourceMarkAggregator) add(mark wal.SourceCheckpointMark) bool {
	if mark.InputID == "" || mark.FileIdentity == "" ||
		!mark.HasEndOffset || mark.ConflictingMetadata {
		aggregator.failure = mark
		aggregator.failed = true
		return false
	}
	if aggregator.bySource == nil {
		aggregator.bySource = make(map[fakeSourceMarkKey]wal.SourceCheckpointMark)
	}
	key := fakeSourceMarkKey{inputID: mark.InputID, fileIdentity: mark.FileIdentity}
	current, exists := aggregator.bySource[key]
	if exists && current.HasFingerprintLength && mark.HasFingerprintLength &&
		current.FingerprintLength != mark.FingerprintLength {
		mark.ConflictingMetadata = true
		aggregator.failure = mark
		aggregator.failed = true
		return false
	}
	if exists && fakeSourceLineCursorsConflict(current, mark) {
		mark.ConflictingMetadata = true
		aggregator.failure = mark
		aggregator.failed = true
		return false
	}
	if !exists || mark.EndOffset > current.EndOffset ||
		mark.EndOffset == current.EndOffset &&
			(mark.HasNextLineNumber && !current.HasNextLineNumber ||
				mark.HasNextLineNumber == current.HasNextLineNumber &&
					mark.BatchSequence >= current.BatchSequence) {
		aggregator.bySource[key] = mark
	}
	return true
}

func (aggregator *fakeSourceMarkAggregator) marks() []wal.SourceCheckpointMark {
	if aggregator.failed {
		return []wal.SourceCheckpointMark{aggregator.failure}
	}
	marks := make([]wal.SourceCheckpointMark, 0, len(aggregator.bySource))
	for _, mark := range aggregator.bySource {
		marks = append(marks, mark)
	}
	sort.Slice(marks, func(i, j int) bool {
		return fakeSourceCheckpointMarkLess(marks[i], marks[j])
	})
	return marks
}

func fakeSourceMarks(batch *opensplunk.EventBatch) []wal.SourceCheckpointMark {
	bySource := make(map[fakeSourceMarkKey]wal.SourceCheckpointMark)
	var invalid *wal.SourceCheckpointMark
	for index, event := range batch.GetEvents() {
		if event == nil || event.GetOrigin() == nil {
			continue
		}
		origin := event.GetOrigin()
		mark := wal.SourceCheckpointMark{
			BatchSequence: batch.GetBatchSequence(), EventIndex: uint32(index),
			InputID: origin.GetInputId(), FileIdentity: origin.GetFileIdentity(),
			SourcePath: origin.GetSourcePath(),
			EndOffset:  origin.GetEndOffset(), LineNumber: origin.GetLineNumber(),
			NextLineNumber:    origin.GetNextLineNumber(),
			FingerprintLength: origin.GetFileFingerprintLength(),
			HasSourcePath:     origin.SourcePath != nil, HasEndOffset: origin.EndOffset != nil,
			HasNextLineNumber:    origin.NextLineNumber != nil,
			HasFingerprintLength: origin.FileFingerprintLength != nil,
		}
		if mark.InputID == "" || mark.FileIdentity == "" || !mark.HasEndOffset {
			if invalid == nil {
				invalidMark := mark
				invalid = &invalidMark
			}
			continue
		}
		key := fakeSourceMarkKey{inputID: mark.InputID, fileIdentity: mark.FileIdentity}
		current, exists := bySource[key]
		if exists && current.ConflictingMetadata {
			continue
		}
		if exists && current.HasFingerprintLength && mark.HasFingerprintLength &&
			current.FingerprintLength != mark.FingerprintLength {
			mark.ConflictingMetadata = true
			bySource[key] = mark
			continue
		}
		if exists && fakeSourceLineCursorsConflict(current, mark) {
			mark.ConflictingMetadata = true
			bySource[key] = mark
			continue
		}
		if !exists || mark.EndOffset > current.EndOffset ||
			mark.EndOffset == current.EndOffset &&
				(mark.HasNextLineNumber || !current.HasNextLineNumber) {
			bySource[key] = mark
		}
	}
	marks := make([]wal.SourceCheckpointMark, 0, len(bySource)+1)
	if invalid != nil {
		marks = append(marks, *invalid)
	}
	for _, mark := range bySource {
		marks = append(marks, mark)
	}
	sort.Slice(marks, func(i, j int) bool {
		return fakeSourceCheckpointMarkLess(marks[i], marks[j])
	})
	return marks
}

func fakeSourceCheckpointMarkLess(left, right wal.SourceCheckpointMark) bool {
	if left.InputID != right.InputID {
		return left.InputID < right.InputID
	}
	if left.FileIdentity != right.FileIdentity {
		return left.FileIdentity < right.FileIdentity
	}
	return left.EventIndex < right.EventIndex
}

func fakeSourceLineCursorsConflict(left, right wal.SourceCheckpointMark) bool {
	if !left.HasNextLineNumber || !right.HasNextLineNumber {
		return false
	}
	switch {
	case left.EndOffset == right.EndOffset:
		return left.NextLineNumber != right.NextLineNumber
	case left.EndOffset < right.EndOffset:
		return left.NextLineNumber >= right.NextLineNumber
	default:
		return left.NextLineNumber <= right.NextLineNumber
	}
}

func (q *fakeQueue) hasSequenceLocked(sequence uint64) bool {
	for _, batch := range q.batches {
		if batch.GetBatchSequence() == sequence {
			return true
		}
	}
	return false
}

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

func (q *fakeQueue) ackCallsSnapshot() []uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]uint64(nil), q.ackCalls...)
}

var _ wal.Queue = (*fakeQueue)(nil)

type fixedStatsQueue struct {
	wal.Queue
	stats wal.Stats
}

func (q *fixedStatsQueue) Stats() wal.Stats { return q.stats }

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
	opensplunk.UnimplementedCollectorIngestServiceServer

	readyFn   func() *opensplunk.CollectorReady
	onBatch   func(fs *fakeServer, batch *opensplunk.EventBatch)
	failCalls int // number of initial Collect calls that fail after Hello
	failErr   error
	// batchErr, when set, runs before onBatch; a non-nil return tears down the
	// stream after the batch was received but before any ack is sent.
	batchErr func(fs *fakeServer, batch *opensplunk.EventBatch) error

	sendMu sync.Mutex

	mu              sync.Mutex
	stream          opensplunk.CollectorIngestService_CollectServer
	respSeq         uint64
	callCount       int
	token           string
	hello           *opensplunk.CollectorHello
	received        []*opensplunk.EventBatch
	byID            map[uint64]*opensplunk.EventBatch
	heartbeats      int
	goodbye         *opensplunk.CollectorGoodbye
	currentInFlight int
	maxObserved     int
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		readyFn: defaultReady,
		byID:    make(map[uint64]*opensplunk.EventBatch),
	}
}

func defaultReady() *opensplunk.CollectorReady {
	return &opensplunk.CollectorReady{
		StreamId:                 "stream-x",
		HeartbeatInterval:        durationpb.New(time.Hour),
		MaxInFlightBatches:       1,
		MaxBatchEvents:           1000,
		MaxBatchBytes:            8 << 20,
		MaxEventBytes:            1 << 20,
		AcknowledgmentDurability: opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
	}
}

func (fs *fakeServer) Collect(stream opensplunk.CollectorIngestService_CollectServer) error {
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
		if fs.failErr != nil {
			return fs.failErr
		}
		return status.Error(codes.Unavailable, "transient failure")
	}

	if err := fs.send(&opensplunk.CollectResponse{
		Payload: &opensplunk.CollectResponse_Ready{Ready: fs.readyFn()},
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

func (fs *fakeServer) send(resp *opensplunk.CollectResponse) error {
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

func (fs *fakeServer) ackBatch(seq uint64, accepted uint32, rejected ...*opensplunk.EventRejection) {
	fs.mu.Lock()
	batch := fs.byID[seq]
	fs.mu.Unlock()
	_ = fs.send(&opensplunk.CollectResponse{
		Payload: &opensplunk.CollectResponse_BatchAck{BatchAck: &opensplunk.BatchAck{
			BatchId:             batch.GetBatchId(),
			BatchSequence:       seq,
			Durability:          opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
			AcceptedEventCount:  accepted,
			DuplicateEventCount: 0,
			RejectedEvents:      rejected,
		}},
	})
}

func (fs *fakeServer) tokenSeen() string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.token
}

func (fs *fakeServer) helloSeen() *opensplunk.CollectorHello {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.hello
}

func (fs *fakeServer) receivedBatches() []*opensplunk.EventBatch {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make([]*opensplunk.EventBatch, len(fs.received))
	copy(out, fs.received)
	return out
}

func (fs *fakeServer) goodbyeSeen() *opensplunk.CollectorGoodbye {
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

func startServer(t *testing.T, srv opensplunk.CollectorIngestServiceServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	opensplunk.RegisterCollectorIngestServiceServer(server, srv)
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
		Address:     "bufnet",
		Token:       func() (string, error) { return "test-token", nil },
		CollectorID: "collector-a",
		InstanceID:  "instance-a",
		Hello: HelloInfo{
			SourceRevision: "development",
			Hostname:       "host-a",
			StartedAt:      time.Now().Add(-time.Hour),
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
	s.client = opensplunk.NewCollectorIngestServiceClient(conn)
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

func makeEvent(id, index string) *opensplunk.LogEvent {
	return &opensplunk.LogEvent{EventId: id, IndexName: index}
}

func fakeBatch(seq uint64, events ...*opensplunk.LogEvent) *opensplunk.EventBatch {
	return &opensplunk.EventBatch{
		CollectorId:   "collector-a",
		BatchId:       "batch-" + itoa(seq),
		BatchSequence: seq,
		CreatedAt:     timestamppb.Now(),
		Events:        events,
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
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), uint32(len(b.GetEvents())))
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

func TestSenderCheckpointCallbackFailureLeavesBatchReplayable(t *testing.T) {
	t.Parallel()
	batch := fakeBatch(1, makeEvent("e1", "main"))
	batch.Events[0].Origin = &opensplunk.EventOrigin{
		InputId:      "input-a",
		FileIdentity: new("dev=1;ino=2;gen=1;fp=" + strings.Repeat("ab", 32)),
		EndOffset:    proto.Uint64(1),
	}
	q := newFakeQueue(batch)
	callbackErr := errors.New("checkpoint disk unavailable")
	opts := testOptions()
	opts.OnTerminalMarks = func(marks []wal.SourceCheckpointMark) error {
		if len(marks) != 1 || marks[0].BatchSequence != 1 {
			t.Fatalf("callback marks = %v, want batch 1", markSequencesForTest(marks))
		}
		return callbackErr
	}
	s, err := New(opts, q, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := s.newConn(context.Background(), func() {}, func() {}, nil)
	c.inflight[1] = batch
	c.inflightN = 1

	err = c.handleAck(&opensplunk.BatchAck{
		BatchId:            batch.GetBatchId(),
		BatchSequence:      1,
		Durability:         opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
		AcceptedEventCount: 1,
	})
	if !errors.Is(err, callbackErr) {
		t.Fatalf("handleAck error = %v, want callback error", err)
	}
	if got := q.Stats(); got.LastAckedBatchSequence != 0 || got.QueuedBatches != 1 {
		t.Fatalf("callback failure consumed replayable batch: %+v", got)
	}
	if got := q.ackCallsSnapshot(); len(got) != 0 {
		t.Fatalf("queue Ack calls = %v, want none after callback failure", got)
	}
	if c.lookupInflight(1) == nil {
		t.Fatal("callback failure released in-flight batch")
	}
}

func TestSenderMalformedOriginCallbackFailureLeavesBatchReplayable(t *testing.T) {
	t.Parallel()
	identity := "dev=1;ino=2;gen=1;fp=" + strings.Repeat("ab", 32)
	checkpointEvent := func(id string, origin *opensplunk.EventOrigin) *opensplunk.LogEvent {
		event := makeEvent(id, "main")
		event.Origin = origin
		return event
	}
	tests := []struct {
		name       string
		events     []*opensplunk.LogEvent
		markIsFail func(wal.SourceCheckpointMark) bool
	}{
		{
			name: "missing input ID",
			events: []*opensplunk.LogEvent{checkpointEvent("e1", &opensplunk.EventOrigin{
				FileIdentity: &identity,
				EndOffset:    proto.Uint64(1),
			})},
			markIsFail: func(mark wal.SourceCheckpointMark) bool {
				return mark.InputID == "" && mark.FileIdentity == identity &&
					mark.HasEndOffset && !mark.ConflictingMetadata
			},
		},
		{
			name: "missing file identity",
			events: []*opensplunk.LogEvent{checkpointEvent("e1", &opensplunk.EventOrigin{
				InputId:   "input-a",
				EndOffset: proto.Uint64(1),
			})},
			markIsFail: func(mark wal.SourceCheckpointMark) bool {
				return mark.InputID == "input-a" && mark.FileIdentity == "" &&
					mark.HasEndOffset && !mark.ConflictingMetadata
			},
		},
		{
			name: "missing end offset",
			events: []*opensplunk.LogEvent{checkpointEvent("e1", &opensplunk.EventOrigin{
				InputId:      "input-a",
				FileIdentity: &identity,
			})},
			markIsFail: func(mark wal.SourceCheckpointMark) bool {
				return mark.InputID == "input-a" && mark.FileIdentity == identity &&
					!mark.HasEndOffset && !mark.ConflictingMetadata
			},
		},
		{
			name: "conflicting fingerprint metadata",
			events: []*opensplunk.LogEvent{
				checkpointEvent("e1", &opensplunk.EventOrigin{
					InputId:               "input-a",
					FileIdentity:          &identity,
					EndOffset:             proto.Uint64(1),
					FileFingerprintLength: proto.Uint32(64),
				}),
				checkpointEvent("e2", &opensplunk.EventOrigin{
					InputId:               "input-a",
					FileIdentity:          &identity,
					EndOffset:             proto.Uint64(2),
					FileFingerprintLength: proto.Uint32(65),
				}),
			},
			markIsFail: func(mark wal.SourceCheckpointMark) bool {
				return mark.InputID == "input-a" && mark.FileIdentity == identity &&
					mark.EventIndex == 1 && mark.ConflictingMetadata
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := fakeBatch(1, test.events...)
			q := newFakeQueue(batch)
			callbackErr := errors.New("malformed source checkpoint")
			callbackCalls := 0
			opts := testOptions()
			opts.OnTerminalMarks = func(marks []wal.SourceCheckpointMark) error {
				callbackCalls++
				if len(marks) != 1 || marks[0].BatchSequence != 1 ||
					!test.markIsFail(marks[0]) {
					t.Fatalf("callback marks = %+v, want one representative failure mark", marks)
				}
				return callbackErr
			}
			s, err := New(opts, q, &memSink{}, nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			c := s.newConn(context.Background(), func() {}, func() {}, nil)
			c.inflight[1] = batch
			c.inflightN = 1

			err = c.handleAck(&opensplunk.BatchAck{
				BatchId:            batch.GetBatchId(),
				BatchSequence:      1,
				Durability:         opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
				AcceptedEventCount: uint32(len(batch.GetEvents())),
			})
			if !errors.Is(err, callbackErr) {
				t.Fatalf("handleAck error = %v, want callback error", err)
			}
			if callbackCalls != 1 {
				t.Fatalf("checkpoint callback calls = %d, want 1", callbackCalls)
			}
			if got := q.Stats(); got.LastAckedBatchSequence != 0 || got.QueuedBatches != 1 {
				t.Fatalf("callback failure consumed replayable batch: %+v", got)
			}
			if got := q.ackCallsSnapshot(); len(got) != 0 {
				t.Fatalf("queue Ack calls = %v, want none after callback failure", got)
			}
			if c.lookupInflight(1) == nil {
				t.Fatal("callback failure released in-flight batch")
			}
		})
	}
}

func TestFakeQueueCumulativePreviewRetainsCrossBatchConflict(t *testing.T) {
	t.Parallel()
	identity := "dev=1;ino=2;gen=1;fp=" + strings.Repeat("ab", 32)
	checkpointBatch := func(sequence uint64, offset uint64, fingerprintLength uint32) *opensplunk.EventBatch {
		event := makeEvent("event-"+itoa(sequence), "main")
		event.Origin = &opensplunk.EventOrigin{
			InputId:               "input-a",
			FileIdentity:          &identity,
			EndOffset:             &offset,
			FileFingerprintLength: &fingerprintLength,
		}
		return fakeBatch(sequence, event)
	}
	q := newFakeQueue(
		checkpointBatch(1, 1, 64),
		checkpointBatch(2, 2, 65),
	)

	preview, err := q.PrepareAckThrough(2)
	if err != nil {
		t.Fatalf("PrepareAckThrough: %v", err)
	}
	if preview.BatchCount != 2 || preview.ThroughBatchSequence != 2 ||
		len(preview.Marks) != 1 || preview.Marks[0].BatchSequence != 2 ||
		!preview.Marks[0].ConflictingMetadata {
		t.Fatalf("preview = %+v, want one representative cross-batch conflict", preview)
	}
	if got := q.Stats(); got.LastAckedBatchSequence != 0 || got.QueuedBatches != 2 {
		t.Fatalf("read-only conflict preview advanced queue: %+v", got)
	}
}

func markSequencesForTest(marks []wal.SourceCheckpointMark) []uint64 {
	sequences := make([]uint64, 0, len(marks))
	for _, mark := range marks {
		sequences = append(sequences, mark.BatchSequence)
	}
	return sequences
}

func TestSenderStreamSequenceStrictlyIncrements(t *testing.T) {
	t.Parallel()
	// The real ingest service enforces stream_sequence == 1 for Hello and +1 for
	// every subsequent request; delivering a batch through it proves the sender's
	// sequencing is correct. Covered end-to-end in TestSenderAgainstRealService.
	fs := newFakeServer()
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), 1)
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
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), 1)
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))

	buf := &syncBuffer{}
	opts := testOptions()
	opts.Token = func() (string, error) { return secret, nil }
	opts.Logger = zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(buf),
		zap.DebugLevel,
	))
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

func TestSenderResumeAfterReplaysBatchesForExplicitOutcomes(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.readyFn = func() *opensplunk.CollectorReady {
		r := defaultReady()
		resume := uint64(2)
		r.ResumeAfterBatchSequence = &resume
		return r
	}
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		if b.GetBatchSequence() == 2 {
			// Model a committed response that was lost before the collector could
			// persist its rejected-event dead letter. Ingest's idempotency store
			// replays this same terminal outcome when the WAL batch is resent.
			fs.ackBatch(2, 1, &opensplunk.EventRejection{
				EventIndex: 1,
				EventId:    "e2-rejected",
				Code:       opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX,
				Message:    "index not authorized",
			})
			return
		}
		fs.ackBatch(b.GetBatchSequence(), uint32(len(b.GetEvents())))
	}
	conn := startServer(t, fs)
	q := newFakeQueue(
		fakeBatch(1, makeEvent("e1", "main")),
		fakeBatch(2, makeEvent("e2-accepted", "main"), makeEvent("e2-rejected", "forbidden")),
		fakeBatch(3, makeEvent("e3", "main")),
	)
	sink := &memSink{}
	s := newTestSender(t, testOptions(), q, sink, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "queue drained through 3", func() bool { return q.ackedSeq() >= 3 })
	waitFor(t, "replayed rejection dead-lettered", func() bool { return len(sink.snapshot()) == 1 })

	got := fs.receivedBatches()
	var seqs []uint64
	for _, b := range got {
		seqs = append(seqs, b.GetBatchSequence())
	}
	if len(seqs) != 3 || seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("server received sequences %v, want [1 2 3]", seqs)
	}
	records := sink.snapshot()
	if got := records[0].Event.GetEventId(); got != "e2-rejected" {
		t.Fatalf("dead-lettered event = %q, want e2-rejected", got)
	}
	cancel()
	<-done
}

func TestSenderHonorsInFlightCap(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.readyFn = func() *opensplunk.CollectorReady {
		r := defaultReady()
		r.MaxInFlightBatches = 2
		return r
	}
	arrived := make(chan uint64, 8)
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
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
	batches := make([]*opensplunk.EventBatch, 0, total)
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
		fs.ackBatch(seq, 1)
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
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		mu.Lock()
		attempts++
		attempt := attempts
		mu.Unlock()
		if attempt == 1 {
			_ = fs.send(&opensplunk.CollectResponse{
				Payload: &opensplunk.CollectResponse_RetryBatch{RetryBatch: &opensplunk.RetryBatch{
					BatchId:       b.GetBatchId(),
					BatchSequence: b.GetBatchSequence(),
					Reason:        opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
					RetryAfter:    durationpb.New(20 * time.Millisecond),
				}},
			})
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

func TestSenderTerminalAckCancelsScheduledRetry(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	var once sync.Once
	fs.onBatch = func(fs *fakeServer, batch *opensplunk.EventBatch) {
		once.Do(func() {
			_ = fs.send(&opensplunk.CollectResponse{
				Payload: &opensplunk.CollectResponse_RetryBatch{RetryBatch: &opensplunk.RetryBatch{
					BatchId: batch.GetBatchId(), BatchSequence: batch.GetBatchSequence(),
					Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
					RetryAfter: durationpb.New(150 * time.Millisecond),
				}},
			})
			fs.ackBatch(batch.GetBatchSequence(), 1)
		})
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch acked before retry delay", func() bool { return q.ackedSeq() == 1 })
	time.Sleep(250 * time.Millisecond)
	if got := len(fs.receivedBatches()); got != 1 {
		t.Fatalf("server received %d batches, want only the original after terminal ack", got)
	}
	cancel()
	<-done
}

func TestSenderTerminalAckCancelsRetryBeforeCheckpointCommit(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	var responses sync.Once
	fs.onBatch = func(fs *fakeServer, batch *opensplunk.EventBatch) {
		responses.Do(func() {
			_ = fs.send(&opensplunk.CollectResponse{
				Payload: &opensplunk.CollectResponse_RetryBatch{RetryBatch: &opensplunk.RetryBatch{
					BatchId: batch.GetBatchId(), BatchSequence: batch.GetBatchSequence(),
					Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
					RetryAfter: durationpb.New(20 * time.Millisecond),
				}},
			})
			fs.ackBatch(batch.GetBatchSequence(), 1)
		})
	}

	batch := fakeBatch(1, makeEvent("e1", "main"))
	batch.Events[0].Origin = &opensplunk.EventOrigin{
		InputId:      "input-a",
		FileIdentity: new("dev=1;ino=2;gen=1;fp=" + strings.Repeat("ab", 32)),
		EndOffset:    proto.Uint64(1),
	}
	q := newFakeQueue(batch)
	commitStarted := make(chan struct{})
	allowCommit := make(chan struct{})
	var allowCommitOnce sync.Once
	t.Cleanup(func() { allowCommitOnce.Do(func() { close(allowCommit) }) })
	opts := testOptions()
	opts.OnTerminalMarks = func([]wal.SourceCheckpointMark) error {
		close(commitStarted)
		<-allowCommit
		return nil
	}
	s := newTestSender(t, opts, q, &memSink{}, nil, startServer(t, fs))
	cancel, done := runSender(t, s)

	select {
	case <-commitStarted:
	case <-time.After(time.Second):
		t.Fatal("terminal checkpoint callback did not start")
	}
	time.Sleep(100 * time.Millisecond)
	if got := len(fs.receivedBatches()); got != 1 {
		t.Fatalf("server received %d batches while terminal checkpoint commit was pending, want only original", got)
	}
	allowCommitOnce.Do(func() { close(allowCommit) })
	waitFor(t, "batch acked after checkpoint commit", func() bool { return q.ackedSeq() == 1 })
	cancel()
	<-done
}

func TestReleaseInflightPromptlyCancelsLongScheduledRetry(t *testing.T) {
	t.Parallel()
	batch := fakeBatch(1, makeEvent("e1", "main"))
	q := newFakeQueue(batch)
	s, err := New(testOptions(), q, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := s.newConn(ctx, cancel, func() {}, nil)
	c.inflight[1] = batch
	c.inflightN = 1

	if err := c.handleRetry(&opensplunk.RetryBatch{
		BatchId:       batch.GetBatchId(),
		BatchSequence: 1,
		Reason:        opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
		RetryAfter:    durationpb.New(time.Hour),
	}); err != nil {
		t.Fatalf("handleRetry: %v", err)
	}
	c.mu.Lock()
	retry := c.pendingRetry[1]
	c.mu.Unlock()
	if retry == nil {
		t.Fatal("long retry was not scheduled")
	}

	c.releaseInflight(1)
	select {
	case <-retry.done:
	case <-time.After(time.Second):
		t.Fatal("terminal release did not promptly stop long retry")
	}
	c.mu.Lock()
	_, stillScheduled := c.pendingRetry[1]
	_, stillInflight := c.inflight[1]
	c.mu.Unlock()
	if stillScheduled || stillInflight {
		t.Fatalf("terminal release retained retry=%t inflight=%t", stillScheduled, stillInflight)
	}
}

func TestSenderRejectsCumulativeAckThatOmitsEarlierTerminalOutcome(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.readyFn = func() *opensplunk.CollectorReady {
		ready := defaultReady()
		ready.MaxInFlightBatches = 2
		return ready
	}
	fs.onBatch = func(fs *fakeServer, batch *opensplunk.EventBatch) {
		switch batch.GetBatchSequence() {
		case 1:
			_ = fs.send(&opensplunk.CollectResponse{
				Payload: &opensplunk.CollectResponse_RetryBatch{RetryBatch: &opensplunk.RetryBatch{
					BatchId: batch.GetBatchId(), BatchSequence: 1,
					Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
					RetryAfter: durationpb.New(150 * time.Millisecond),
				}},
			})
		case 2:
			through := uint64(2)
			_ = fs.send(&opensplunk.CollectResponse{
				Payload: &opensplunk.CollectResponse_BatchAck{BatchAck: &opensplunk.BatchAck{
					BatchId:                          batch.GetBatchId(),
					BatchSequence:                    2,
					AcknowledgedThroughBatchSequence: &through,
					Durability:                       opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
					AcceptedEventCount:               1,
				}},
			})
		}
	}

	identity := "dev=1;ino=2;gen=1;fp=" + strings.Repeat("ab", 32)
	batch1 := fakeBatch(1, makeEvent("e1", "main"))
	batch1.Events[0].Origin = &opensplunk.EventOrigin{
		InputId:      "input-a",
		FileIdentity: &identity,
		EndOffset:    proto.Uint64(1),
	}
	batch2 := fakeBatch(2, makeEvent("e2", "main"))
	batch2.Events[0].Origin = &opensplunk.EventOrigin{
		InputId:      "input-b",
		FileIdentity: &identity,
		EndOffset:    proto.Uint64(2),
	}
	q := newFakeQueue(batch1, batch2)

	var marksMu sync.Mutex
	var committed []wal.SourceCheckpointMark
	opts := testOptions()
	opts.OnTerminalMarks = func(marks []wal.SourceCheckpointMark) error {
		marksMu.Lock()
		committed = append(committed[:0], marks...)
		marksMu.Unlock()
		return nil
	}
	s := newTestSender(t, opts, q, &memSink{}, nil, startServer(t, fs))
	cancel, done := runSender(t, s)

	waitFor(t, "unsafe cumulative ack rejected and connection retried", func() bool {
		s.mu.Lock()
		lastError := s.stats.LastError
		s.mu.Unlock()
		return strings.Contains(lastError, "omits terminal outcome for in-flight batch 1") && fs.calls() >= 2
	})
	if got := q.ackedSeq(); got != 0 {
		t.Fatalf("unsafe cumulative ack advanced queue through %d, want 0", got)
	}
	marksMu.Lock()
	gotMarks := append([]wal.SourceCheckpointMark(nil), committed...)
	marksMu.Unlock()
	if len(gotMarks) != 0 {
		t.Fatalf("unsafe cumulative ack committed checkpoint marks: %+v", gotMarks)
	}
	cancel()
	<-done
}

func TestSenderRejectsCumulativeAckBeyondSentHighWater(t *testing.T) {
	t.Parallel()
	batch1 := fakeBatch(1, makeEvent("e1", "main"))
	q := newFakeQueue(batch1, fakeBatch(2, makeEvent("e2", "main")))
	s, err := New(testOptions(), q, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := s.newConn(context.Background(), func() {}, func() {}, nil)
	c.inflight[1] = batch1
	c.inflightN = 1
	c.highestSentBatchSequence = 1
	through := uint64(2)

	err = c.handleAck(&opensplunk.BatchAck{
		BatchId:                          batch1.GetBatchId(),
		BatchSequence:                    1,
		AcknowledgedThroughBatchSequence: &through,
		Durability:                       opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
		AcceptedEventCount:               1,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds highest batch sequence sent") {
		t.Fatalf("handleAck error = %v, want sent-high-water validation error", err)
	}
	if got := q.Stats(); got.LastAckedBatchSequence != 0 || got.QueuedBatches != 2 {
		t.Fatalf("invalid cumulative ack mutated queue: %+v", got)
	}
	if c.lookupInflight(1) == nil {
		t.Fatal("invalid cumulative ack released in-flight batch")
	}
}

func TestSenderSupervisesQueueReadFailure(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	queueErr := errors.New("simulated durable queue read failure")
	q := &nextBatchErrorQueue{fakeQueue: newFakeQueue(), err: queueErr}
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, startServer(t, fs))
	cancel, done := runSender(t, s)

	waitFor(t, "queue read failure tears down and retries the stream", func() bool {
		s.mu.Lock()
		lastError := s.stats.LastError
		s.mu.Unlock()
		return strings.Contains(lastError, queueErr.Error()) && fs.calls() >= 2
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run after cancellation = %v, want context.Canceled", err)
	}
}

func TestSenderThrottleAppliesSendDelay(t *testing.T) {
	t.Parallel()
	const minDelay = 120 * time.Millisecond
	fs := newFakeServer()
	var mu sync.Mutex
	var recvTimes []time.Time
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		mu.Lock()
		recvTimes = append(recvTimes, time.Now())
		count := len(recvTimes)
		mu.Unlock()
		if count == 1 {
			_ = fs.send(&opensplunk.CollectResponse{
				Payload: &opensplunk.CollectResponse_Throttle{Throttle: &opensplunk.Throttle{
					Reason:           opensplunk.ThrottleReason_THROTTLE_REASON_SERVER_LOAD,
					MinimumSendDelay: durationpb.New(minDelay),
					EffectiveUntil:   timestamppb.New(time.Now().Add(10 * time.Second)),
				}},
			})
		}
		fs.ackBatch(b.GetBatchSequence(), 1)
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
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), 1,
			&opensplunk.EventRejection{
				EventIndex: 1,
				EventId:    "e2",
				Code:       opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX,
				Message:    "index not authorized",
			},
			&opensplunk.EventRejection{
				EventIndex: 2,
				EventId:    "e3",
				Code:       opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_FIELD_NAME_INVALID,
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
	if records[0].Code != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX.String() {
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
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		_ = fs.send(&opensplunk.CollectResponse{
			Payload: &opensplunk.CollectResponse_BatchReject{BatchReject: &opensplunk.BatchReject{
				BatchId:       b.GetBatchId(),
				BatchSequence: b.GetBatchSequence(),
				Code:          opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_EVENT_ID_DIGEST_MISMATCH,
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
		if r.Code != opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_EVENT_ID_DIGEST_MISMATCH.String() {
			t.Fatalf("record code = %q", r.Code)
		}
	}
	cancel()
	<-done
}

func TestSenderDeadLetterFailureRetainsWALBatch(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		_ = fs.send(&opensplunk.CollectResponse{
			Payload: &opensplunk.CollectResponse_BatchReject{BatchReject: &opensplunk.BatchReject{
				BatchId: b.GetBatchId(), BatchSequence: b.GetBatchSequence(),
				Code: opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
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
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), 1)
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
	if got := fs.goodbyeSeen().GetReason(); got != opensplunk.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN {
		t.Fatalf("goodbye reason = %v, want SHUTDOWN", got)
	}
}

type preReadyGoodbyeServer struct {
	opensplunk.UnimplementedCollectorIngestServiceServer
	helloSeen   chan struct{}
	goodbyeSeen chan struct{}
	helloOnce   sync.Once
	goodbyeOnce sync.Once
}

func (server *preReadyGoodbyeServer) Collect(stream opensplunk.CollectorIngestService_CollectServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetHello() != nil {
		server.helloOnce.Do(func() { close(server.helloSeen) })
	}
	for {
		request, err = stream.Recv()
		if err != nil {
			return nil
		}
		if request.GetGoodbye() != nil {
			server.goodbyeOnce.Do(func() { close(server.goodbyeSeen) })
			return nil
		}
	}
}

func TestSenderPreReadyCancellationIsBoundedAndJoinsHandshake(t *testing.T) {
	t.Parallel()
	server := &preReadyGoodbyeServer{
		helloSeen:   make(chan struct{}),
		goodbyeSeen: make(chan struct{}),
	}
	s := newTestSender(t, testOptions(), newFakeQueue(), &memSink{}, nil, startServer(t, server))
	s.drainTimeout = 100 * time.Millisecond
	cancel, done := runSender(t, s)

	select {
	case <-server.helloSeen:
	case <-time.After(time.Second):
		t.Fatal("server did not receive Hello")
	}
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pre-Ready cancellation exceeded its global drain bound")
	}
	if elapsed := time.Since(started); elapsed > 400*time.Millisecond {
		t.Fatalf("pre-Ready cancellation took %v, want bounded by drain timeout", elapsed)
	}
	select {
	case <-server.goodbyeSeen:
	default:
		t.Fatal("server did not receive pre-Ready Goodbye")
	}
}

func TestSenderInitializesRestartAckHighWater(t *testing.T) {
	t.Parallel()
	q := newFakeQueue()
	q.acked = 41
	q.nextSeq = 42
	s, err := New(testOptions(), q, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	hb := s.buildHeartbeat()
	if hb.LastAcknowledgedBatchSequence == nil || hb.GetLastAcknowledgedBatchSequence() != 41 {
		t.Fatalf("restart heartbeat ack high-water = %v, want 41", hb.LastAcknowledgedBatchSequence)
	}
	if got := s.stats.LastAckedBatchSequence; got != 41 {
		t.Fatalf("sender stats restart ack high-water = %d, want 41", got)
	}
}

func TestHeartbeatIncludesLocalDroppedEventProvider(t *testing.T) {
	t.Parallel()
	var localDrops atomic.Uint64
	localDrops.Store(7)
	opts := testOptions()
	opts.LocalDroppedEventsTotal = localDrops.Load
	s, err := New(opts, newFakeQueue(), &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.stats.DroppedEventsTotal = 3
	if got := s.buildHeartbeat().GetQueue().GetDroppedEventsTotal(); got != 10 {
		t.Fatalf("heartbeat dropped_events_total = %d, want 10", got)
	}
	localDrops.Store(^uint64(0))
	if got := s.buildHeartbeat().GetQueue().GetDroppedEventsTotal(); got != collectorlimits.MaximumFleetCounter {
		t.Fatalf("overflowing heartbeat dropped_events_total = %d, want saturation", got)
	}
}

func TestHeartbeatClampsFleetCountersAndSequences(t *testing.T) {
	t.Parallel()
	maximum := ^uint64(0)
	q := &fixedStatsQueue{
		Queue: newFakeQueue(),
		stats: wal.Stats{
			QueuedEvents:           maximum,
			QueuedBytes:            maximum,
			LastAckedBatchSequence: maximum,
			NextBatchSequence:      maximum,
		},
	}
	opts := testOptions()
	opts.LocalDroppedEventsTotal = func() uint64 { return maximum }
	s, err := New(opts, q, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.stats = Stats{
		LastSentBatchSequence:   maximum,
		SentEventsTotal:         maximum,
		AcknowledgedEventsTotal: maximum,
		RetriedBatchesTotal:     maximum,
		RejectedEventsTotal:     maximum,
		DroppedEventsTotal:      maximum,
	}
	hb := s.buildHeartbeat()
	queue := hb.GetQueue()
	values := map[string]uint64{
		"queued_events": queue.GetQueuedEvents(),
		"queued_bytes":  queue.GetQueuedBytes(),
		"sent":          queue.GetSentEventsTotal(),
		"acknowledged":  queue.GetAcknowledgedEventsTotal(),
		"retried":       queue.GetRetriedBatchesTotal(),
		"rejected":      queue.GetRejectedEventsTotal(),
		"dropped":       queue.GetDroppedEventsTotal(),
		"last_sent":     hb.GetLastSentBatchSequence(),
		"last_acked":    hb.GetLastAcknowledgedBatchSequence(),
	}
	for name, got := range values {
		if got != collectorlimits.MaximumFleetCounter {
			t.Errorf("%s = %d, want fleet maximum %d", name, got, collectorlimits.MaximumFleetCounter)
		}
	}
}

func TestHelloClampsLastAcknowledgedSequence(t *testing.T) {
	t.Parallel()
	maximum := ^uint64(0)
	fs := newFakeServer()
	conn := startServer(t, fs)
	q := &fixedStatsQueue{
		Queue: newFakeQueue(),
		stats: wal.Stats{
			LastAckedBatchSequence: maximum,
			NextBatchSequence:      maximum,
		},
	}
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)
	waitFor(t, "Hello observed", func() bool { return fs.helloSeen() != nil })
	hello := fs.helloSeen()
	if hello.LastAcknowledgedBatchSequence == nil ||
		hello.GetLastAcknowledgedBatchSequence() != collectorlimits.MaximumFleetCounter {
		t.Fatalf("Hello last acknowledged sequence = %v, want %d",
			hello.LastAcknowledgedBatchSequence, collectorlimits.MaximumFleetCounter)
	}
	cancel()
	<-done
}

func TestHandleAckRejectsHostileUint32EventIndex(t *testing.T) {
	t.Parallel()
	batch := fakeBatch(1, makeEvent("one", "main"))
	q := newFakeQueue(batch)
	s, err := New(testOptions(), q, &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := s.newConn(context.Background(), func() {}, func() {}, nil)
	c.inflight[1] = batch
	c.inflightN = 1
	c.highestSentBatchSequence = 1
	err = c.handleAck(&opensplunk.BatchAck{
		BatchId:       batch.GetBatchId(),
		BatchSequence: 1,
		Durability:    opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
		RejectedEvents: []*opensplunk.EventRejection{{
			EventIndex: ^uint32(0),
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("hostile rejection index error = %v, want bounds error", err)
	}
	if q.ackedSeq() != 0 || c.lookupInflight(1) == nil {
		t.Fatal("hostile rejection index mutated terminal delivery state")
	}
}

func TestResponseAndThrottleTimestampsValidateBeforeStateMutation(t *testing.T) {
	t.Parallel()
	s, err := New(testOptions(), newFakeQueue(), &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := s.newConn(context.Background(), func() {}, func() {}, nil)
	if err := c.validateResponse(&opensplunk.CollectResponse{StreamSequence: 1}); err == nil ||
		!strings.Contains(err.Error(), "sent_at") {
		t.Fatalf("missing response sent_at = %v, want validation error", err)
	}
	serverSentAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	invalidThrottles := []*opensplunk.Throttle{
		{MinimumSendDelay: durationpb.New(-time.Second)},
		{MinimumSendDelay: durationpb.New(ingestquota.MaximumRetryAfter + time.Nanosecond)},
		{EffectiveUntil: &timestamppb.Timestamp{Seconds: 253402300800}},
		{EffectiveUntil: timestamppb.New(serverSentAt.Add(ingestquota.MaximumRetryAfter + time.Nanosecond))},
	}
	for _, throttle := range invalidThrottles {
		resp := &opensplunk.CollectResponse{
			SentAt:  timestamppb.New(serverSentAt),
			Payload: &opensplunk.CollectResponse_Throttle{Throttle: throttle},
		}
		if err := c.handleThrottle(resp); err == nil {
			t.Fatalf("handleThrottle accepted invalid timing: %+v", throttle)
		}
		c.mu.Lock()
		mutated := c.throttled
		c.mu.Unlock()
		if mutated {
			t.Fatal("invalid throttle mutated connection state")
		}
	}
}

func TestRetryRejectsDelayAboveServerMaximumBeforeStateMutation(t *testing.T) {
	t.Parallel()
	batch := fakeBatch(1, makeEvent("e1", "main"))
	s, err := New(testOptions(), newFakeQueue(batch), &memSink{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := s.newConn(context.Background(), func() {}, func() {}, nil)
	c.inflight[1] = batch
	c.inflightN = 1
	err = c.handleRetry(&opensplunk.RetryBatch{
		BatchId:       batch.GetBatchId(),
		BatchSequence: batch.GetBatchSequence(),
		RetryAfter:    durationpb.New(ingestquota.MaximumRetryAfter + time.Nanosecond),
	})
	if err == nil || !strings.Contains(err.Error(), "retry_after") {
		t.Fatalf("oversized retry_after error = %v, want validation failure", err)
	}
	c.mu.Lock()
	_, scheduled := c.pendingRetry[1]
	c.mu.Unlock()
	if scheduled {
		t.Fatal("oversized retry_after scheduled a retry")
	}
	s.mu.Lock()
	got := s.stats.RetriedBatchesTotal
	s.mu.Unlock()
	if got != 0 {
		t.Fatalf("oversized retry_after incremented retry counter to %d", got)
	}
}

func TestSenderTreatsImmutablePreReadyStatusAsFatal(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.failCalls = 1
	fs.failErr = status.Error(codes.InvalidArgument, "immutable Hello rejected")
	s := newTestSender(t, testOptions(), newFakeQueue(), &memSink{}, nil, startServer(t, fs))
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	select {
	case err := <-done:
		var fatal *fatalError
		if !errors.As(err, &fatal) || status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Run error = %v, want fatal InvalidArgument", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sender retried an immutable pre-Ready failure")
	}
	if calls := fs.calls(); calls != 1 {
		t.Fatalf("Collect calls = %d, want one fatal attempt", calls)
	}
}

func TestSenderRejectsInvalidConstructionOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		edit func(*Options)
	}{
		{name: "compression", edit: func(opts *Options) { opts.Compression = "brotli" }},
		{name: "negative dial timeout", edit: func(opts *Options) { opts.DialTimeout = -time.Second }},
		{name: "negative backoff", edit: func(opts *Options) { opts.Backoff.Initial = -time.Second }},
		{name: "backoff max below default initial", edit: func(opts *Options) {
			opts.Backoff.Initial = 0
			opts.Backoff.Max = time.Millisecond
		}},
		{name: "backoff multiplier", edit: func(opts *Options) { opts.Backoff.Multiplier = .5 }},
		{name: "backoff jitter", edit: func(opts *Options) { opts.Backoff.Jitter = 1.01 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := testOptions()
			test.edit(&opts)
			if _, err := New(opts, newFakeQueue(), &memSink{}, nil); err == nil {
				t.Fatal("New accepted invalid sender options")
			}
		})
	}
}

func TestBackoffDelayBoundedAndJittered(t *testing.T) {
	t.Parallel()
	policy := BackoffPolicy{Initial: 100 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: 0.5}

	// With zero jitter fraction the delay equals the (bounded) base and grows.
	var prev time.Duration
	for attempt := range 10 {
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
		return ingest.Authorization{
			SubjectID:         "s1",
			TenantID:          "t1",
			CollectorID:       "collector-a",
			AuthorizedIndexes: []ingest.IndexPolicy{{Name: "main", Version: 1}},
		}, nil
	})
	config := realServiceIngestConfig(ingest.Authorization{
		SubjectID:         "s1",
		TenantID:          "t1",
		CollectorID:       "collector-a",
		AuthorizedIndexes: []ingest.IndexPolicy{{Name: "main", Version: 1}},
	})
	svc, err := ingest.NewService(config, authorizer, store)
	if err != nil {
		t.Fatal(err)
	}
	conn := startServer(t, svc)

	events := []*opensplunk.LogEvent{validLogEvent("event-a", "main")}
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
		return ingest.Authorization{
			SubjectID:         "s1",
			TenantID:          "t1",
			CollectorID:       "collector-a",
			AuthorizedIndexes: []ingest.IndexPolicy{{Name: "main", Version: 1}},
		}, nil
	})
	config := realServiceIngestConfig(ingest.Authorization{
		SubjectID:         "s1",
		TenantID:          "t1",
		CollectorID:       "collector-a",
		AuthorizedIndexes: []ingest.IndexPolicy{{Name: "main", Version: 1}},
	})
	svc, err := ingest.NewService(config, authorizer, store)
	if err != nil {
		t.Fatal(err)
	}
	conn := startServer(t, svc)

	// event-a in authorized index "main"; event-b in an unauthorized index so the
	// real server rejects exactly that one event.
	events := []*opensplunk.LogEvent{
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
	if records[0].Code != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX.String() {
		t.Fatalf("dead-letter code = %q, want UNAUTHORIZED_INDEX", records[0].Code)
	}
	cancel()
	<-done
}

type realServiceSessionManager struct {
	authorization ingest.Authorization
	generation    atomic.Uint64
}

func realServiceIngestConfig(authorization ingest.Authorization) ingest.Config {
	config := ingest.DefaultConfig()
	config.SessionManager = &realServiceSessionManager{
		authorization: authorization,
	}
	return config
}

func (manager *realServiceSessionManager) Admit(
	_ context.Context,
	_ string,
	request ingest.CollectorSessionAdmissionRequest,
) (ingest.CollectorSessionAdmission, error) {
	return ingest.CollectorSessionAdmission{
		Authorization: manager.authorization,
		Lease: collectorfleet.Lease{
			Scope: collectorfleet.Scope{
				TenantID: manager.authorization.TenantID,
			},
			CollectorID: request.CollectorID,
			BootEpoch:   request.BootEpoch,
			StreamID:    request.StreamID,
			Generation:  manager.generation.Add(1),
		},
	}, nil
}

func (*realServiceSessionManager) Activate(collectorfleet.Lease) error {
	return nil
}

func (manager *realServiceSessionManager) AuthorizeLease(
	context.Context,
	string,
	collectorfleet.Lease,
	time.Time,
) (ingest.Authorization, error) {
	return manager.authorization, nil
}

func (*realServiceSessionManager) RecordHeartbeat(
	context.Context,
	collectorfleet.Lease,
	collectorfleet.Heartbeat,
) (bool, error) {
	return true, nil
}

func (*realServiceSessionManager) Disconnect(
	context.Context,
	collectorfleet.Lease,
	time.Time,
) (bool, error) {
	return true, nil
}

func validLogEvent(id, index string) *opensplunk.LogEvent {
	msg := "request completed"
	return &opensplunk.LogEvent{
		EventId:         id,
		IndexName:       index,
		EventTime:       timestamppb.New(time.Now().Add(-time.Minute)),
		CollectedAt:     timestamppb.New(time.Now().Add(-30 * time.Second)),
		EventTimeSource: opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
		Host:            "host-a",
		Source:          "/var/log/app.log",
		Sourcetype:      "json",
		Severity:        opensplunk.LogSeverity_LOG_SEVERITY_INFO,
		Message:         &msg,
		Raw:             []byte(`{"message":"request completed","status":200}`),
		RawEncoding:     opensplunk.RawEncoding_RAW_ENCODING_UTF8,
		Fields: &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{
			Name:  "status",
			Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: "200"}},
		}}},
	}
}

func validBatch(collectorID, batchID string, seq uint64, events ...*opensplunk.LogEvent) *opensplunk.EventBatch {
	return &opensplunk.EventBatch{
		CollectorId:           collectorID,
		BatchId:               batchID,
		BatchSequence:         seq,
		CreatedAt:             timestamppb.Now(),
		Events:                events,
		UncompressedSizeBytes: ingest.UncompressedEventBytes(events),
		EventIdsSha256:        ingest.EventIDDigest(events),
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
	fs.batchErr = func(fs *fakeServer, _ *opensplunk.EventBatch) error {
		if fs.calls() == 1 {
			// First connection: the batch arrives but the stream dies before an
			// ack, leaving it unacked behind the queue's delivery cursor.
			return status.Error(codes.Unavailable, "stream died before ack")
		}
		return nil
	}
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), uint32(len(b.GetEvents())))
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

// TestSenderContinuesWhenResumePointUnknown covers the fresh-state-dir edge: a
// server Ready carrying resume_after_batch_sequence ahead of anything the local
// WAL knows must not fail the connection (which would crash-loop forever) —
// the sender treats it as advisory and delivers the queue normally.
func TestSenderContinuesWhenResumePointUnknown(t *testing.T) {
	t.Parallel()
	fs := newFakeServer()
	fs.readyFn = func() *opensplunk.CollectorReady {
		ready := defaultReady()
		resume := uint64(500)
		ready.ResumeAfterBatchSequence = &resume
		return ready
	}
	fs.onBatch = func(fs *fakeServer, b *opensplunk.EventBatch) {
		fs.ackBatch(b.GetBatchSequence(), uint32(len(b.GetEvents())))
	}
	conn := startServer(t, fs)
	q := newFakeQueue(fakeBatch(1, makeEvent("e1", "main")))
	s := newTestSender(t, testOptions(), q, &memSink{}, nil, conn)
	cancel, done := runSender(t, s)

	waitFor(t, "batch delivered despite unknown resume point", func() bool { return q.ackedSeq() >= 1 })
	if calls := fs.calls(); calls != 1 {
		t.Fatalf("server Collect calls = %d, want 1 (no reconnect churn)", calls)
	}
	cancel()
	<-done
}

// stalledDialClient returns a client whose transport dial never completes
// until the dial context ends, so Collect blocks inside stream establishment.
// dialed is signaled once per dial attempt.
func stalledDialClient(t *testing.T, dialed chan<- struct{}) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///stalled",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			select {
			case dialed <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestSenderCancellationInterruptsStalledStreamOpen(t *testing.T) {
	t.Parallel()
	dialed := make(chan struct{}, 1)
	opts := testOptions()
	opts.DialTimeout = time.Minute
	s := newTestSender(t, opts, newFakeQueue(), &memSink{}, nil, stalledDialClient(t, dialed))
	cancel, done := runSender(t, s)

	select {
	case <-dialed:
	case <-time.After(time.Second):
		t.Fatal("sender did not start dialing")
	}
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not interrupt a stalled stream open")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cancellation took %v while the stream open was stalled", elapsed)
	}
}

func TestSenderDialTimeoutBoundsStalledStreamOpen(t *testing.T) {
	t.Parallel()
	dialed := make(chan struct{}, 1)
	opts := testOptions()
	opts.DialTimeout = 50 * time.Millisecond
	s := newTestSender(t, opts, newFakeQueue(), &memSink{}, nil, stalledDialClient(t, dialed))
	cancel, done := runSender(t, s)

	select {
	case <-dialed:
	case <-time.After(time.Second):
		t.Fatal("sender did not start dialing")
	}
	waitFor(t, "stream establishment timeout recorded", func() bool {
		s.mu.Lock()
		lastError := s.stats.LastError
		s.mu.Unlock()
		return strings.Contains(lastError, "stream establishment timed out after 50ms")
	})
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
