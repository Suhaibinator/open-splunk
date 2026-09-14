package sender

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip" // registers and names the gzip compressor
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"github.com/Suhaibinator/open-splunk/internal/collectorlimits"
)

// TLSConfig configures transport security for the gRPC dial.
type TLSConfig struct {
	Enabled    bool
	CAFile     string
	ServerName string
}

// BackoffPolicy bounds reconnect and retry backoff.
type BackoffPolicy struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	// Jitter is the fractional random jitter applied to each delay (0..1).
	Jitter float64
}

// HelloInfo is the static collector identity advertised in CollectorHello.
type HelloInfo struct {
	SourceRevision  string
	Hostname        string
	OperatingSystem string
	Architecture    string
	StartedAt       time.Time
	Capabilities    []opensplunk.CollectorCapability
	Inputs          []*opensplunk.CollectorInputRegistration
}

// Options configures a Sender. It is fully self-contained; the daemon builds it
// from configuration so the sender need not import the config package.
type Options struct {
	Address string
	TLS     TLSConfig
	// Token returns the current bearer token, read from the token file at dial
	// time. The returned secret is never logged or stored beyond call creds.
	Token func() (string, error)
	// Compression is the gRPC compressor name (e.g. "gzip"); empty means none.
	Compression string

	CollectorID string
	InstanceID  string
	Hello       HelloInfo

	// DialTimeout bounds one whole connection attempt: opening the Collect
	// stream (including the lazy transport dial), sending Hello, and receiving
	// Ready. Zero disables the bound.
	DialTimeout time.Duration
	Backoff     BackoffPolicy

	// Logger receives sender diagnostics. It must never be handed the bearer
	// token. A nil Logger discards output.
	Logger *zap.Logger

	// InputHealth is a nil-safe provider of per-input health for heartbeats. The
	// daemon wires it after construction; a nil provider yields no input health.
	InputHealth func() []*opensplunk.CollectorInputHealth
	// LocalDroppedEventsTotal supplies cumulative drops that occur before an
	// EventBatch reaches this sender (decode/processing failures and local
	// admission dead letters). Heartbeats saturating-add it to sender-owned drops.
	LocalDroppedEventsTotal func() uint64

	// OnTerminalMarks durably commits source checkpoints for the newly
	// contiguous terminal WAL prefix. The sender invokes it after any required
	// dead-letter write but before Ack/AckThrough mutates the queue. Returning an
	// error tears down the stream and leaves the batches replayable.
	OnTerminalMarks func([]wal.SourceCheckpointMark) error
}

// DeadLetterRecord is one rejected canonical event or pre-decode source
// artifact and why it was rejected, serialized as one JSON object per line.
type DeadLetterRecord struct {
	Event         *opensplunk.LogEvent
	SourceRecord  *RejectedSourceRecord
	BatchID       string
	BatchSequence uint64
	// Code is the string form of an EventRejectionCode or BatchRejectionCode.
	Code       string
	Reason     string
	RejectedAt time.Time
}

// RejectedSourceRecord is the bounded source artifact retained when framing or
// decoding fails before a canonical LogEvent exists. Bytes may be a bounded
// prefix for oversized framing failures; the exact source range remains in the
// durable coordinates for operator recovery.
type RejectedSourceRecord struct {
	InputID          string `json:"input_id"`
	FileIdentity     string `json:"file_identity"`
	SourcePath       string `json:"source_path"`
	StartOffset      uint64 `json:"start_offset"`
	EndOffset        uint64 `json:"end_offset"`
	LineNumber       uint64 `json:"line_number"`
	NextLineNumber   uint64 `json:"next_line_number"`
	GuardFingerprint string `json:"guard_fingerprint,omitempty"`
	GuardLength      uint32 `json:"guard_length,omitempty"`
	Bytes            []byte `json:"raw_bytes_base64"`
	Truncated        bool   `json:"truncated,omitempty"`
}

// DeadLetterSink durably records terminal delivery rejections and local source
// recovery artifacts.
type DeadLetterSink interface {
	// WriteRecords appends records as JSONL and flushes them to disk.
	WriteRecords(records []DeadLetterRecord) error
	// Close flushes and releases the sink.
	Close() error
}

// Stats is a point-in-time snapshot of delivery progress. It contributes the
// delivery counters of opensplunk.CollectorQueueStats (queue depth comes from
// wal.Stats).
type Stats struct {
	Connected               bool
	LastSentBatchSequence   uint64
	LastAckedBatchSequence  uint64
	SentEventsTotal         uint64
	AcknowledgedEventsTotal uint64
	RetriedBatchesTotal     uint64
	RejectedEventsTotal     uint64
	DroppedEventsTotal      uint64
	LastConnectedAt         time.Time
	LastError               string
}

// StatsReporter receives sender stats updates. The daemon merges them with
// wal.Stats and input health for heartbeats and diagnostics.
type StatsReporter interface {
	ReportSenderStats(Stats)
}

// StatsReporterFunc adapts a function to StatsReporter.
type StatsReporterFunc func(Stats)

// ReportSenderStats calls f.
func (f StatsReporterFunc) ReportSenderStats(s Stats) { f(s) }

// fatalError marks a non-retriable condition (bad TLS material, unsupported
// server protocol major). Run returns it instead of looping with backoff.
type fatalError struct{ err error }

func (e *fatalError) Error() string { return e.err.Error() }
func (e *fatalError) Unwrap() error { return e.err }

// Sender is the gRPC delivery client.
type Sender struct {
	opts       Options
	queue      wal.Queue
	deadLetter DeadLetterSink
	reporter   StatsReporter
	logger     *zap.Logger

	// Injected for determinism in tests; production defaults set in New.
	now          func() time.Time
	rand         func() float64
	dial         func() (opensplunk.CollectorIngestServiceClient, func() error, error)
	drainTimeout time.Duration
	// A connection must remain Ready for this long before it is allowed to reset
	// exponential reconnect backoff. Otherwise a server that accepts Hello and
	// immediately drops every stream turns a tight failure loop into attempt zero.
	backoffResetAfter time.Duration

	client    opensplunk.CollectorIngestServiceClient
	closeConn func() error

	mu    sync.Mutex
	stats Stats

	// retryMu guards retryNotBefore. Unlike conn.pendingRetry, these deadlines
	// belong to the Sender so a RetryBatch remains effective when its stream is
	// disconnected before the delayed resend. The WAL is rewound on reconnect;
	// the new connection consults this state before sending the retained batch.
	retryMu        sync.Mutex
	retryNotBefore map[uint64]batchRetryDeadline

	// terminalMu serializes PrepareAck -> OnTerminalMarks -> Ack transactions
	// across the receive loop and the pump's negotiated-limit dead-letter path.
	// Without this barrier, a concurrent queue mutation could invalidate the
	// read-only prefix preview before its checkpoint callback commits.
	terminalMu sync.Mutex
}

type batchRetryDeadline struct {
	batchID   string
	notBefore time.Time
}

// New constructs a Sender that consumes from queue, dead-letters permanent
// rejections to deadLetter, and reports progress to reporter (which may be nil).
func New(opts Options, queue wal.Queue, deadLetter DeadLetterSink, reporter StatsReporter) (*Sender, error) {
	if queue == nil {
		return nil, errors.New("collector/sender: queue is required")
	}
	if opts.Address == "" {
		return nil, errors.New("collector/sender: address is required")
	}
	if opts.CollectorID == "" {
		return nil, errors.New("collector/sender: collector id is required")
	}
	if opts.Compression != "" && opts.Compression != gzip.Name {
		return nil, fmt.Errorf("collector/sender: unsupported compression %q", opts.Compression)
	}
	if opts.DialTimeout < 0 {
		return nil, errors.New("collector/sender: dial timeout cannot be negative")
	}
	if opts.Backoff.Initial < 0 || opts.Backoff.Max < 0 {
		return nil, errors.New("collector/sender: backoff durations cannot be negative")
	}
	effectiveBackoffInitial := opts.Backoff.Initial
	if effectiveBackoffInitial == 0 {
		effectiveBackoffInitial = defaultBackoffInitial
	}
	effectiveBackoffMax := opts.Backoff.Max
	if effectiveBackoffMax == 0 {
		effectiveBackoffMax = defaultBackoffMax
	}
	if effectiveBackoffMax < effectiveBackoffInitial {
		return nil, errors.New("collector/sender: backoff max cannot be less than initial")
	}
	if math.IsNaN(opts.Backoff.Multiplier) || math.IsInf(opts.Backoff.Multiplier, 0) ||
		(opts.Backoff.Multiplier != 0 && opts.Backoff.Multiplier < 1) {
		return nil, errors.New("collector/sender: backoff multiplier must be zero/default or at least one")
	}
	if math.IsNaN(opts.Backoff.Jitter) || math.IsInf(opts.Backoff.Jitter, 0) ||
		opts.Backoff.Jitter < 0 || opts.Backoff.Jitter > 1 {
		return nil, errors.New("collector/sender: backoff jitter must be between zero and one")
	}
	if opts.Token == nil {
		opts.Token = func() (string, error) { return "", nil }
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	if deadLetter == nil {
		deadLetter = nopDeadLetterSink{}
	}
	queueStats := queue.Stats()
	s := &Sender{
		opts:              opts,
		queue:             queue,
		deadLetter:        deadLetter,
		reporter:          reporter,
		logger:            logger,
		now:               time.Now,
		drainTimeout:      3 * time.Second,
		backoffResetAfter: defaultBackoffMax,
		retryNotBefore:    make(map[uint64]batchRetryDeadline),
		stats: Stats{
			LastAckedBatchSequence: queueStats.LastAckedBatchSequence,
		},
	}
	if queueStats.QuarantinedSegments > 0 {
		logger.Error("collector WAL recovery quarantined durable segments",
			zap.Uint64("segments", queueStats.QuarantinedSegments),
			zap.Uint64("quarantined_bytes", queueStats.QuarantinedBytes),
			zap.String("recovery_warning", queueStats.RecoveryWarning))
	}

	// Backoff jitter uses CSPRNG output without shared mutable PRNG state.
	s.rand = secureRandomFloat64
	s.dial = s.grpcDial
	return s, nil
}

func secureRandomFloat64() float64 {
	var random [8]byte
	if _, err := cryptorand.Read(random[:]); err != nil {
		return 0.5
	}
	const mantissaMask = 1<<53 - 1
	return float64(binary.LittleEndian.Uint64(random[:])&mantissaMask) / (1 << 53)
}

// Run maintains the delivery stream until ctx is canceled, reconnecting with
// bounded backoff. It sends Goodbye on graceful shutdown.
func (s *Sender) Run(ctx context.Context) error {
	if err := s.ensureClient(); err != nil {
		return err
	}
	if s.closeConn != nil {
		defer func() { _ = s.closeConn() }()
	}

	attempt := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		connected, reconnectAfter, err := s.runConnection(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, ok := errors.AsType[*fatalError](err); ok {
			s.setLastError(err)
			return err
		}
		if connected && s.connectionWasStable() {
			attempt = 0
		}
		if err != nil {
			s.setLastError(err)
			s.logger.Warn("collector stream disconnected", zap.String("address", s.opts.Address), zap.Error(err))
		}

		var delay time.Duration
		if reconnectAfter > 0 {
			delay = reconnectAfter
		} else {
			delay = backoffDelay(s.opts.Backoff, attempt, s.rand())
			attempt++
		}
		if !s.sleep(ctx, delay) {
			return ctx.Err()
		}
	}
}

func (s *Sender) connectionWasStable() bool {
	s.mu.Lock()
	connectedAt := s.stats.LastConnectedAt
	s.mu.Unlock()
	return !connectedAt.IsZero() && s.now().Sub(connectedAt) >= s.backoffResetAfter
}

// ensureClient dials the gRPC target once and caches the client. Tests may
// pre-set s.client to inject a bufconn-backed client and skip dialing.
func (s *Sender) ensureClient() error {
	if s.client != nil {
		return nil
	}
	client, closer, err := s.dial()
	if err != nil {
		return &fatalError{err: fmt.Errorf("collector/sender: dial %s: %w", s.opts.Address, err)}
	}
	s.client = client
	s.closeConn = closer
	return nil
}

func (s *Sender) grpcDial() (opensplunk.CollectorIngestServiceClient, func() error, error) {
	var creds credentials.TransportCredentials
	if s.opts.TLS.Enabled {
		cfg, err := buildTLSConfig(s.opts.TLS)
		if err != nil {
			return nil, nil, err
		}
		creds = credentials.NewTLS(cfg)
	} else {
		creds = insecure.NewCredentials()
	}
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if s.opts.TLS.ServerName != "" {
		dialOpts = append(dialOpts, grpc.WithAuthority(s.opts.TLS.ServerName))
	}
	conn, err := grpc.NewClient(s.opts.Address, dialOpts...)
	if err != nil {
		return nil, nil, err
	}
	return opensplunk.NewCollectorIngestServiceClient(conn), conn.Close, nil
}

func buildTLSConfig(t TLSConfig) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.ServerName}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("collector/sender: read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("collector/sender: CA file %s contains no certificates", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// collectCallOptions returns the per-RPC call options for the Collect stream.
func (s *Sender) collectCallOptions() []grpc.CallOption {
	if s.opts.Compression == "gzip" {
		return []grpc.CallOption{grpc.UseCompressor(gzip.Name)}
	}
	return nil
}

func (s *Sender) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// deferBatchRetry records the latest server-mandated retry instant for a
// durable batch. Deadlines are connection-independent: reconnecting cannot
// turn a delayed RetryBatch into an immediate WAL replay.
func (s *Sender) deferBatchRetry(batch *opensplunk.EventBatch, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	notBefore := s.now().Add(delay)
	sequence := batch.GetBatchSequence()
	batchID := batch.GetBatchId()

	s.retryMu.Lock()
	current, ok := s.retryNotBefore[sequence]
	if !ok || current.batchID != batchID || current.notBefore.Before(notBefore) {
		s.retryNotBefore[sequence] = batchRetryDeadline{batchID: batchID, notBefore: notBefore}
	}
	s.retryMu.Unlock()
}

// batchRetryWait returns how much longer batch must remain held. Expired and
// stale sequence entries are removed eagerly to keep the map bounded.
func (s *Sender) batchRetryWait(batch *opensplunk.EventBatch, now time.Time) time.Duration {
	sequence := batch.GetBatchSequence()
	s.retryMu.Lock()
	defer s.retryMu.Unlock()

	deadline, ok := s.retryNotBefore[sequence]
	if !ok {
		return 0
	}
	if deadline.batchID != batch.GetBatchId() {
		delete(s.retryNotBefore, sequence)
		return 0
	}
	wait := deadline.notBefore.Sub(now)
	if wait <= 0 {
		delete(s.retryNotBefore, sequence)
		return 0
	}
	return wait
}

func (s *Sender) clearBatchRetry(batchSequence uint64, cumulative bool) {
	s.retryMu.Lock()
	if cumulative {
		for sequence := range s.retryNotBefore {
			if sequence <= batchSequence {
				delete(s.retryNotBefore, sequence)
			}
		}
	} else {
		delete(s.retryNotBefore, batchSequence)
	}
	s.retryMu.Unlock()
}

// --- stats accounting -------------------------------------------------------

func (s *Sender) report() {
	if s.reporter == nil {
		return
	}
	s.mu.Lock()
	snapshot := s.stats
	s.mu.Unlock()
	s.reporter.ReportSenderStats(snapshot)
}

func (s *Sender) setConnected(connected bool) {
	s.mu.Lock()
	s.stats.Connected = connected
	if connected {
		s.stats.LastConnectedAt = s.now()
	}
	s.mu.Unlock()
	s.report()
}

func (s *Sender) setLastError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.stats.LastError = err.Error()
	s.mu.Unlock()
}

func (s *Sender) markSent(batch *opensplunk.EventBatch) {
	s.mu.Lock()
	s.stats.SentEventsTotal += uint64(len(batch.GetEvents()))
	s.stats.LastSentBatchSequence = batch.GetBatchSequence()
	s.mu.Unlock()
	s.report()
}

func (s *Sender) markAcked(throughSeq uint64, acceptedAndDuplicate uint64) {
	s.mu.Lock()
	s.stats.AcknowledgedEventsTotal += acceptedAndDuplicate
	if throughSeq > s.stats.LastAckedBatchSequence {
		s.stats.LastAckedBatchSequence = throughSeq
	}
	s.mu.Unlock()
	s.report()
}

func (s *Sender) markRejected(n uint64) {
	s.mu.Lock()
	s.stats.RejectedEventsTotal += n
	s.mu.Unlock()
	s.report()
}

func (s *Sender) markDropped(n uint64) {
	s.mu.Lock()
	s.stats.DroppedEventsTotal += n
	s.mu.Unlock()
	s.report()
}

func (s *Sender) markRetried() {
	s.mu.Lock()
	s.stats.RetriedBatchesTotal++
	s.mu.Unlock()
	s.report()
}

// writeDeadLetter is part of the terminal-delivery transaction: callers must
// not acknowledge the WAL batch unless this durable write succeeds.
func (s *Sender) writeDeadLetter(records []DeadLetterRecord) error {
	if len(records) == 0 {
		return nil
	}
	if err := s.deadLetter.WriteRecords(records); err != nil {
		s.logger.Error("dead-letter write failed", zap.Error(err), zap.Int("records", len(records)))
		return fmt.Errorf("collector/sender: persist dead letter: %w", err)
	}
	return nil
}

// commitTerminal makes one exact or cumulative terminal disposition durable.
// The queue preview is intentionally read-only; source checkpoints are
// persisted first, and only then is the WAL high-water allowed to advance. A
// callback failure therefore preserves every affected batch for replay.
func (s *Sender) commitTerminal(batchSequence uint64, cumulative bool) (uint64, error) {
	s.terminalMu.Lock()
	defer s.terminalMu.Unlock()

	var preview wal.AckPreview
	var err error
	if cumulative {
		preview, err = s.queue.PrepareAckThrough(batchSequence)
	} else {
		preview, err = s.queue.PrepareAck(batchSequence)
	}
	if err != nil {
		return 0, err
	}
	if len(preview.Marks) > 0 && s.opts.OnTerminalMarks != nil {
		if err := s.opts.OnTerminalMarks(preview.Marks); err != nil {
			return 0, fmt.Errorf("collector/sender: commit source checkpoints through batch %d: %w", batchSequence, err)
		}
	}
	if cumulative {
		err = s.queue.AckThrough(batchSequence)
	} else {
		err = s.queue.Ack(batchSequence)
	}
	if err != nil {
		return 0, err
	}
	s.clearBatchRetry(batchSequence, cumulative)
	return s.queue.Stats().LastAckedBatchSequence, nil
}

func (s *Sender) buildHeartbeat() *opensplunk.CollectorHeartbeat {
	queueStats := s.queue.Stats()
	s.mu.Lock()
	delivery := s.stats
	s.mu.Unlock()

	droppedEventsTotal := collectorlimits.ClampFleetCounter(delivery.DroppedEventsTotal)
	if s.opts.LocalDroppedEventsTotal != nil {
		droppedEventsTotal = collectorlimits.SaturatingAddFleetCounters(
			droppedEventsTotal,
			s.opts.LocalDroppedEventsTotal(),
		)
	}
	qs := &opensplunk.CollectorQueueStats{
		QueuedEvents:            collectorlimits.ClampFleetCounter(queueStats.QueuedEvents),
		QueuedBytes:             collectorlimits.ClampFleetCounter(queueStats.QueuedBytes),
		SentEventsTotal:         collectorlimits.ClampFleetCounter(delivery.SentEventsTotal),
		AcknowledgedEventsTotal: collectorlimits.ClampFleetCounter(delivery.AcknowledgedEventsTotal),
		RetriedBatchesTotal:     collectorlimits.ClampFleetCounter(delivery.RetriedBatchesTotal),
		RejectedEventsTotal:     collectorlimits.ClampFleetCounter(delivery.RejectedEventsTotal),
		DroppedEventsTotal:      droppedEventsTotal,
	}
	if queueStats.OldestEventAge > 0 {
		qs.OldestEventAge = durationpb.New(queueStats.OldestEventAge)
	}
	hb := &opensplunk.CollectorHeartbeat{
		CollectorId: s.opts.CollectorID,
		InstanceId:  s.opts.InstanceID,
		ObservedAt:  timestamppb.New(s.now().UTC()),
		Queue:       qs,
	}
	if s.opts.InputHealth != nil {
		hb.Inputs = s.opts.InputHealth()
	}
	if delivery.LastSentBatchSequence > 0 {
		v := collectorlimits.ClampFleetCounter(delivery.LastSentBatchSequence)
		hb.LastSentBatchSequence = &v
	}
	if queueStats.LastAckedBatchSequence > 0 {
		v := collectorlimits.ClampFleetCounter(queueStats.LastAckedBatchSequence)
		hb.LastAcknowledgedBatchSequence = &v
	}
	return hb
}

// nopDeadLetterSink is used when the daemon supplies no sink; it discards
// records. Callers should always wire a real sink so rejected events persist.
type nopDeadLetterSink struct{}

func (nopDeadLetterSink) WriteRecords([]DeadLetterRecord) error { return nil }
func (nopDeadLetterSink) Close() error                          { return nil }
