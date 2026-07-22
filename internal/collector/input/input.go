package input

import (
	"context"
	"strconv"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
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
	Device      uint64
	Inode       uint64
	Fingerprint string
}

// String returns the stable identity string passed to the decoder as
// SourcePosition.FileIdentity (for example "dev=1;ino=2;fp=ab12").
func (id FileIdentity) String() string {
	return "dev=" + strconv.FormatUint(id.Device, 10) +
		";ino=" + strconv.FormatUint(id.Inode, 10) +
		";fp=" + id.Fingerprint
}

// IsZero reports whether id is the zero identity.
func (id FileIdentity) IsZero() bool {
	return id.Device == 0 && id.Inode == 0 && id.Fingerprint == ""
}

// SourceRef locates one framed event within a file. It is the input-owned
// analogue of the decoder's SourcePosition; the daemon maps between them so the
// input package need not import the root collector package.
type SourceRef struct {
	Path        string
	Identity    FileIdentity
	StartOffset uint64
	EndOffset   uint64
	LineNumber  uint64
}

// RawEvent is one framed, undecoded event emitted by the tailer. Bytes is owned
// by the receiver (the tailer does not retain or mutate it after send).
type RawEvent struct {
	Bytes  []byte
	Source SourceRef
}

// Checkpoint is the persisted read position for one file identity.
type Checkpoint struct {
	Identity   FileIdentity
	Path       string
	Offset     uint64
	LineNumber uint64
	UpdatedAt  time.Time
}

// CheckpointStore persists per-file read offsets durably. Set must be atomic:
// a crash at any point leaves either the old or the new checkpoint, never a
// torn one. Implementations must be safe for concurrent use.
type CheckpointStore interface {
	// Get returns the checkpoint for id and whether one exists.
	Get(id FileIdentity) (Checkpoint, bool, error)
	// Set atomically persists cp (temp file + fsync + rename).
	Set(cp Checkpoint) error
	// Delete removes the checkpoint for id, if any.
	Delete(id FileIdentity) error
	// List returns all persisted checkpoints (used for reconciliation).
	List() ([]Checkpoint, error)
	// Close flushes and releases the store.
	Close() error
}

// Health is a point-in-time snapshot of one input's status. Its fields mirror
// opensplunkv1.CollectorInputHealth one-to-one; the daemon converts a Health
// into that protobuf message for heartbeats.
type Health struct {
	InputID           string
	State             opensplunkv1.CollectorInputState
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
// its context is cancelled. It reads initial offsets from the CheckpointStore
// but never advances them; the daemon owns advancement after WAL durability.
type Manager interface {
	// Run blocks tailing until ctx is cancelled or a fatal setup error occurs.
	// Per-file read errors are surfaced through Health, not returned.
	Run(ctx context.Context) error
	// Events returns the channel of framed raw events. It is closed when Run
	// returns.
	Events() <-chan RawEvent
	// Health returns the current input health snapshot.
	Health() Health
	// Close releases resources; safe to call after Run returns.
	Close() error
}
