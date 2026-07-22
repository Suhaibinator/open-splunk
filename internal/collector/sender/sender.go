package sender

import (
	"context"
	"errors"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
)

// errNotImplemented is returned by contract stubs during the skeleton phase.
var errNotImplemented = errors.New("collector/sender: not implemented")

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

// NewFileDeadLetterSink opens or creates a JSONL dead-letter file at path.
func NewFileDeadLetterSink(path string) (DeadLetterSink, error) {
	return nil, errNotImplemented
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

// Sender is the gRPC delivery client.
type Sender struct {
	_ struct{} // unexported field so the zero value is not a usable Sender.
}

// New constructs a Sender that consumes from queue, dead-letters permanent
// rejections to deadLetter, and reports progress to reporter (which may be nil).
func New(opts Options, queue wal.Queue, deadLetter DeadLetterSink, reporter StatsReporter) (*Sender, error) {
	return nil, errNotImplemented
}

// Run maintains the delivery stream until ctx is cancelled, reconnecting with
// bounded backoff. It sends Goodbye on graceful shutdown.
func (s *Sender) Run(ctx context.Context) error {
	return errNotImplemented
}
