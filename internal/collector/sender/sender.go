package sender

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip" // registers and names the gzip compressor
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	CollectorVersion string
	Hostname         string
	OperatingSystem  string
	Architecture     string
	StartedAt        time.Time
	Capabilities     []opensplunkv1.CollectorCapability
	Inputs           []*opensplunkv1.CollectorInputRegistration
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

	CollectorID   string
	InstanceID    string
	ProtocolMajor uint32
	ProtocolMinor uint32
	Hello         HelloInfo

	DialTimeout time.Duration
	Backoff     BackoffPolicy

	// Logger receives sender diagnostics. It must never be handed the bearer
	// token. A nil Logger discards output.
	Logger *slog.Logger

	// InputHealth is a nil-safe provider of per-input health for heartbeats. The
	// daemon wires it after construction; a nil provider yields no input health.
	InputHealth func() []*opensplunkv1.CollectorInputHealth
}

// DeadLetterRecord is one rejected event and why it was rejected, serialized as
// a single JSON object per line in the dead-letter file.
type DeadLetterRecord struct {
	Event         *opensplunkv1.LogEvent
	BatchID       string
	BatchSequence uint64
	// Code is the string form of an EventRejectionCode or BatchRejectionCode.
	Code       string
	Reason     string
	RejectedAt time.Time
}

// DeadLetterSink durably records events the server permanently rejected.
type DeadLetterSink interface {
	// WriteRecords appends records as JSONL and flushes them to disk.
	WriteRecords(records []DeadLetterRecord) error
	// Close flushes and releases the sink.
	Close() error
}

// Stats is a point-in-time snapshot of delivery progress. It contributes the
// delivery counters of opensplunkv1.CollectorQueueStats (queue depth comes from
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
	logger     *slog.Logger

	// Injected for determinism in tests; production defaults set in New.
	now          func() time.Time
	rand         func() float64
	dial         func() (opensplunkv1.CollectorIngestServiceClient, func() error, error)
	drainTimeout time.Duration

	client    opensplunkv1.CollectorIngestServiceClient
	closeConn func() error

	mu    sync.Mutex
	stats Stats
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
	if opts.Token == nil {
		opts.Token = func() (string, error) { return "", nil }
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if deadLetter == nil {
		deadLetter = nopDeadLetterSink{}
	}
	s := &Sender{
		opts:         opts,
		queue:        queue,
		deadLetter:   deadLetter,
		reporter:     reporter,
		logger:       logger,
		now:          time.Now,
		drainTimeout: 3 * time.Second,
	}
	seeded := rand.New(rand.NewSource(time.Now().UnixNano()))
	s.rand = seeded.Float64
	s.dial = s.grpcDial
	return s, nil
}

// Run maintains the delivery stream until ctx is cancelled, reconnecting with
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
		var fe *fatalError
		if errors.As(err, &fe) {
			s.setLastError(err)
			return err
		}
		if connected {
			attempt = 0
		}
		if err != nil {
			s.setLastError(err)
			s.logger.Warn("collector stream disconnected", "address", s.opts.Address, "error", err.Error())
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

func (s *Sender) grpcDial() (opensplunkv1.CollectorIngestServiceClient, func() error, error) {
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
	return opensplunkv1.NewCollectorIngestServiceClient(conn), conn.Close, nil
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

func (s *Sender) markSent(batch *opensplunkv1.EventBatch) {
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
		s.logger.Error("dead-letter write failed", "error", err.Error(), "records", len(records))
		return fmt.Errorf("collector/sender: persist dead letter: %w", err)
	}
	return nil
}

func (s *Sender) buildHeartbeat() *opensplunkv1.CollectorHeartbeat {
	queueStats := s.queue.Stats()
	s.mu.Lock()
	delivery := s.stats
	s.mu.Unlock()

	qs := &opensplunkv1.CollectorQueueStats{
		QueuedEvents:            queueStats.QueuedEvents,
		QueuedBytes:             queueStats.QueuedBytes,
		SentEventsTotal:         delivery.SentEventsTotal,
		AcknowledgedEventsTotal: delivery.AcknowledgedEventsTotal,
		RetriedBatchesTotal:     delivery.RetriedBatchesTotal,
		RejectedEventsTotal:     delivery.RejectedEventsTotal,
		DroppedEventsTotal:      delivery.DroppedEventsTotal,
	}
	if queueStats.OldestEventAge > 0 {
		qs.OldestEventAge = durationpb.New(queueStats.OldestEventAge)
	}
	hb := &opensplunkv1.CollectorHeartbeat{
		CollectorId: s.opts.CollectorID,
		InstanceId:  s.opts.InstanceID,
		ObservedAt:  timestamppb.New(s.now().UTC()),
		Queue:       qs,
	}
	if s.opts.InputHealth != nil {
		hb.Inputs = s.opts.InputHealth()
	}
	if delivery.LastSentBatchSequence > 0 {
		v := delivery.LastSentBatchSequence
		hb.LastSentBatchSequence = &v
	}
	if delivery.LastAckedBatchSequence > 0 {
		v := delivery.LastAckedBatchSequence
		hb.LastAcknowledgedBatchSequence = &v
	}
	return hb
}

// nopDeadLetterSink is used when the daemon supplies no sink; it discards
// records. Callers should always wire a real sink so rejected events persist.
type nopDeadLetterSink struct{}

func (nopDeadLetterSink) WriteRecords([]DeadLetterRecord) error { return nil }
func (nopDeadLetterSink) Close() error                          { return nil }
