package input

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/framing"
)

// StartPosition selects where an input begins reading a newly discovered file.
type StartPosition string

const (
	// StartAtBeginning reads a newly discovered file from offset 0.
	StartAtBeginning StartPosition = "beginning"
	// StartAtEnd reads a newly discovered file only from its current end.
	StartAtEnd StartPosition = "end"
)

// FileIdentity uniquely identifies a physical file across renames and
// copy-truncate. Device and Inode are the platform identifiers; Fingerprint is
// a hex hash over the first bytes of the file. On platforms without stable
// inode access Device and Inode are zero and identity relies on Fingerprint.
type FileIdentity struct {
	Device uint64
	Inode  uint64
	// Generation increments when the same physical file is copy-truncated or
	// otherwise rewritten in place. It keeps source coordinates unique when
	// offsets restart at zero.
	Generation  uint64
	Fingerprint string
	// FingerprintLength is the exact prefix length covered by Fingerprint. A
	// fixed length lets a growing short file be recognized after restart: only
	// that original prefix is compared, rather than hashing newly appended data.
	FingerprintLength uint32
}

// String returns the stable identity string passed to the decoder as
// SourcePosition.FileIdentity (for example
// "dev=1;ino=2;gen=3;fp=<lowercase-sha256-hex>").
func (id FileIdentity) String() string {
	return "dev=" + strconv.FormatUint(id.Device, 10) +
		";ino=" + strconv.FormatUint(id.Inode, 10) +
		";gen=" + strconv.FormatUint(id.Generation, 10) +
		";fp=" + id.Fingerprint
}

// TrackingKey identifies the physical file independently of its content
// generation. Checkpoints are indexed by this key so an in-place truncate can
// replace the old generation's offset, and a growing short file can still find
// the identity persisted when it was first discovered.
func (id FileIdentity) TrackingKey() string {
	if id.Device != 0 || id.Inode != 0 {
		return "dev=" + strconv.FormatUint(id.Device, 10) +
			";ino=" + strconv.FormatUint(id.Inode, 10)
	}
	return "fp=" + id.Fingerprint
}

// SourceRef locates one framed event within a file. Line numbers are physical
// lines relative to the collector's initial read position for that file
// generation; start_at=end therefore begins at line 1 without reading skipped
// historical bytes. When a legacy checkpoint lacks the ending physical line,
// LineNumber and NextLineNumber remain zero (unknown) rather than publishing an
// approximate coordinate. SourceRef is the input-owned analog of the
// decoder's SourcePosition; the daemon maps between them so the input package
// need not import the root collector package.
type SourceRef struct {
	Path           string
	Identity       FileIdentity
	StartOffset    uint64
	EndOffset      uint64
	LineNumber     uint64
	NextLineNumber uint64
	// GuardFingerprint is the lowercase SHA-256 digest of the exact source
	// range [EndOffset-GuardLength, EndOffset). Terminal delivery persists this
	// bounded trailing evidence with the checkpoint so restart can distinguish
	// an in-place rewrite that preserved the file's leading fingerprint.
	GuardFingerprint string
	GuardLength      uint32
}

// RawEvent is one framed, undecoded event emitted by the tailer. Bytes is owned
// by the receiver (the tailer does not retain or mutate it after send).
//
// A durability-barrier event is an internal control record: Bytes and Source
// are zero, IsDurabilityBarrier reports true, and the receiver must not decode
// it. The receiver must acknowledge it only after every earlier event received
// from this Manager has either crossed its durable ingestion boundary or
// reached a deliberate terminal disposition. Until then the originating
// tailer will not persist a checkpoint for a rewritten file generation.
type RawEvent struct {
	Bytes         []byte
	Source        SourceRef
	RejectionCode string
	Truncated     bool

	barrier *durabilityBarrier
}

// durabilityBarrier coordinates an in-process FIFO durability fence. It is
// deliberately absent from protobuf and checkpoint schemas: after a crash,
// restart derives the same ordering boundary from the durable WAL.
type durabilityBarrier struct {
	once sync.Once
	done chan struct{}
}

func newDurabilityBarrier() *durabilityBarrier {
	return &durabilityBarrier{done: make(chan struct{})}
}

// IsDurabilityBarrier reports whether event is an internal durability control
// record rather than a framed source event.
func (event RawEvent) IsDurabilityBarrier() bool {
	return event.barrier != nil
}

// AcknowledgeDurabilityBarrier releases the tailer waiting on a durability
// control record. It is safe to call more than once and is a no-op for ordinary
// events.
func (event RawEvent) AcknowledgeDurabilityBarrier() {
	if event.barrier == nil {
		return
	}
	event.barrier.once.Do(func() { close(event.barrier.done) })
}

// Checkpoint is the persisted read position for one input and file identity.
type Checkpoint struct {
	InputID        string
	Identity       FileIdentity
	Path           string
	Offset         uint64
	LineNumber     uint64
	NextLineNumber uint64
	// GuardFingerprint is the lowercase SHA-256 digest of the exact source
	// range [Offset-GuardLength, Offset). Older checkpoints omit both fields;
	// the manager upgrades them after it has validated the legacy leading
	// fingerprint.
	GuardFingerprint string `json:"guard_fingerprint,omitempty"`
	GuardLength      uint32 `json:"guard_length,omitempty"`
	UpdatedAt        time.Time
}

// ManagerCheckpointStore is the checkpoint view used by file discovery and
// tailers. The root collector may wrap the durable store with nonterminal
// resume coordinates already owned by its WAL. Such a view may suppress
// manager-originated metadata writes at those coordinates until terminal
// delivery advances the underlying durable checkpoint.
type ManagerCheckpointStore interface {
	// Get returns the checkpoint for inputID and id and whether one exists.
	Get(inputID string, id FileIdentity) (Checkpoint, bool, error)
	// Set records one discovery, generation, or compatibility-cursor update.
	Set(cp Checkpoint) error
	// SetMany records a batch of compatibility-cursor updates.
	SetMany(checkpoints []Checkpoint) error
}

// CheckpointStore persists per-input, per-file read offsets durably. Set and
// SetMany must be atomic: a crash at any point leaves either the old or the new
// checkpoint snapshot, never a torn or partially applied one. Implementations
// must be safe for concurrent use.
type CheckpointStore interface {
	ManagerCheckpointStore
	// Delete removes the checkpoint for inputID and id, if any.
	Delete(inputID string, id FileIdentity) error
	// List returns all persisted checkpoints (used for reconciliation).
	List() ([]Checkpoint, error)
	// Close flushes and releases the store.
	Close() error
}

// Health is a point-in-time snapshot of one input's status. Its fields mirror
// opensplunk.CollectorInputHealth one-to-one; the daemon converts a Health
// into that protobuf message for heartbeats.
type Health struct {
	InputID           string
	State             opensplunk.CollectorInputState
	StatusMessage     string
	DiscoveredSources uint64
	ActiveSources     uint64
	EventsReadTotal   uint64
	BytesReadTotal    uint64
	LastEventAt       time.Time
	LastErrorAt       time.Time
}

// Config configures a single file input's Manager.
type Config struct {
	InputID          string
	Include          []string
	Exclude          []string
	StartAt          StartPosition
	PollInterval     time.Duration
	FingerprintBytes int
	// Multiline enables multi-line framing; when false a newline framer is used.
	Multiline bool
	// Framing is passed through to the selected framer (size cap, patterns).
	Framing framing.Options
	// FlushAfter bounds how long a multiline framer may hold a buffered partial
	// event with no new input before the tailer force-emits it via the framer's
	// Flush capability. Zero disables inactivity flushing (a partial multiline
	// event waits indefinitely for its next start line). Ignored when Multiline
	// is false. It lives here (rather than on framing.Options) because the
	// inactivity clock is a tailer concern: the framer is a pure stream splitter
	// with no notion of wall-clock time.
	FlushAfter time.Duration
}

// Manager discovers and tails the files for one input, emitting RawEvents until
// its context is canceled. It reads initial offsets from the CheckpointStore
// but never advances them; the daemon owns advancement after terminal delivery.
type Manager interface {
	// Run blocks tailing until ctx is canceled or a fatal setup error occurs.
	// Per-file read errors are surfaced through Health, not returned.
	Run(ctx context.Context) error
	// Events returns the channel of framed raw events and internal durability
	// barriers described by RawEvent. It is closed when Run returns.
	Events() <-chan RawEvent
	// Health returns the current input health snapshot.
	Health() Health
	// Close releases resources; safe to call after Run returns.
	Close() error
}

// RejectionHandler durably records one pre-decode framing failure. Returning
// an error prevents the validated source cursor from advancing.
type RejectionHandler func(context.Context, RawEvent) error

// SetRejectionHandler installs the daemon-owned durable recovery boundary on a
// concrete file manager before Run starts.
func SetRejectionHandler(sourceManager Manager, handler RejectionHandler) error {
	implementation, ok := sourceManager.(*manager)
	if !ok || handler == nil {
		return errors.New("collector/input: rejection handler is unavailable")
	}
	implementation.rejectionHandler = handler
	return nil
}
