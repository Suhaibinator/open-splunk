package collectorfleet

import (
	"errors"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collectorlimits"
	"gorm.io/gorm"
)

const (
	// MaximumDurableCollectorsPerTenant bounds every tenant-scoped fleet
	// catalog traversal. It is intentionally distinct from the smaller
	// process-local MaximumActiveCollectors liveness bound.
	MaximumDurableCollectorsPerTenant = 256

	maximumTenantIDBytes         = 255
	maximumDisplayNameBytes      = 255
	maximumCollectorVersionBytes = collectorlimits.MaximumCollectorVersionBytes
	maximumHostnameBytes         = collectorlimits.MaximumHostnameBytes
	maximumOperatingSystemBytes  = collectorlimits.MaximumOperatingSystemBytes
	maximumArchitectureBytes     = collectorlimits.MaximumArchitectureBytes
	maximumCapabilities          = collectorlimits.MaximumCapabilities
	maximumAuthorizedIndexes     = collectorlimits.MaximumAuthorizedIndexes
	maximumInputs                = collectorlimits.MaximumInputs
	maximumSourceBytes           = collectorlimits.MaximumSourceBytes
	maximumSourcetypeBytes       = collectorlimits.MaximumSourcetypeBytes
	maximumStatusMessageBytes    = collectorlimits.MaximumInputStatusMessageBytes
	maximumSnapshotBytes         = collectorlimits.MaximumSnapshotBytes
	maximumPersistedEnumValue    = uint32(1<<31 - 1)
	maximumPublicUnixMicro       = int64(253_402_300_799_999_999)
)

var (
	// ErrCollectorDisabled means an administrator disabled the durable
	// collector identity, so it cannot claim a new active lease.
	ErrCollectorDisabled = errors.New("collector fleet: collector is disabled")
)

// Scope is the trusted tenant boundary for every fleet operation.
type Scope struct {
	TenantID string
}

// AdministrativeState is the future administrator-controlled lifecycle state.
type AdministrativeState string

const (
	AdministrativeStateEnabled  AdministrativeState = "enabled"
	AdministrativeStateDisabled AdministrativeState = "disabled"
)

// Administration is the complete optimistic-lock-controlled definition.
type Administration struct {
	DisplayName *string
	State       AdministrativeState
}

// AdministrationSnapshot is a detached administrator-owned projection
// returned by admin-only reads and mutations. It intentionally excludes
// operational telemetry so unrelated telemetry or child-snapshot corruption
// cannot prevent or roll back a security-critical disable.
type AdministrationSnapshot struct {
	TenantID            string
	CollectorID         string
	Version             uint64
	DisplayName         *string
	AdministrativeState AdministrativeState
	FirstSeenAt         time.Time
	UpdatedAt           time.Time
}

// InputRegistration is one bounded input announced in CollectorHello.
type InputRegistration struct {
	InputID    string
	InputType  uint32
	IndexName  string
	Source     *string
	Sourcetype *string
}

// Hello is a normalized persistence-facing CollectorHello snapshot together
// with the trusted index scope resolved from the bearer credential.
type Hello struct {
	InstanceID                    string
	ProtocolMajor                 uint32
	ProtocolMinor                 uint32
	CollectorVersion              string
	Hostname                      string
	OperatingSystem               string
	Architecture                  string
	StartedAt                     time.Time
	Capabilities                  []uint32
	AuthorizedIndexes             []string
	Inputs                        []InputRegistration
	LastAcknowledgedBatchSequence *uint64
}

// ClaimRequest contains server-owned lease identity and receive time. The
// caller must validate the credential-to-collector binding before calling it.
type ClaimRequest struct {
	Scope
	CollectorID string
	BootEpoch   string
	StreamID    string
	ReceivedAt  time.Time
	Hello       Hello
}

// PreparedClaim is an opaque, detached, bounded claim snapshot whose
// credential-derived authorized-index scope has not yet been attached.
// PrepareClaim constructs valid values; the zero value is invalid.
type PreparedClaim struct {
	normalized normalizedClaim
	valid      bool
}

// Lease identifies one server-owned active stream generation. Every
// operational mutation requires all fields to match the current durable lease.
type Lease struct {
	Scope
	CollectorID string
	BootEpoch   string
	StreamID    string
	Generation  uint64
}

// ActiveLease is the safe fleet projection of a current lease.
type ActiveLease struct {
	BootEpoch  string
	StreamID   string
	InstanceID string
	Generation uint64
}

// QueueTelemetry is the bounded latest queue snapshot.
type QueueTelemetry struct {
	QueuedEvents            uint64
	QueuedBytes             uint64
	OldestEventAge          *time.Duration
	SentEventsTotal         uint64
	AcknowledgedEventsTotal uint64
	RetriedBatchesTotal     uint64
	RejectedEventsTotal     uint64
	DroppedEventsTotal      uint64
}

// InputHealth is the latest health snapshot for one registered input.
type InputHealth struct {
	InputID           string
	State             uint32
	StatusMessage     string
	DiscoveredSources uint64
	ActiveSources     uint64
	EventsReadTotal   uint64
	BytesReadTotal    uint64
	LastEventAt       *time.Time
	LastErrorAt       *time.Time
}

// Heartbeat is a persistence-facing heartbeat. ObservationSequence is the
// stream-local request sequence assigned by the server transport. It makes
// delayed coalescer flushes latest-wins without trusting client wall time.
type Heartbeat struct {
	ObservationSequence           uint64
	ObservedAt                    time.Time
	ReceivedAt                    time.Time
	Queue                         QueueTelemetry
	Inputs                        []InputHealth
	LastSentBatchSequence         *uint64
	LastAcknowledgedBatchSequence *uint64
	ProcessResidentMemoryBytes    uint64
	ProcessCPUPercent             float64
}

// Collector is a detached tenant-scoped fleet snapshot. Version belongs only
// to administrator CAS. TelemetryRevision changes independently for claims,
// applied heartbeats, disconnects, and healthy-runtime lease invalidation on
// disable.
type Collector struct {
	TenantID            string
	CollectorID         string
	Version             uint64
	DisplayName         *string
	AdministrativeState AdministrativeState
	FirstSeenAt         time.Time
	UpdatedAt           time.Time

	TelemetryRevision uint64
	LeaseGeneration   uint64
	ActiveLease       *ActiveLease

	ProtocolMajor    uint32
	ProtocolMinor    uint32
	CollectorVersion string
	Hostname         string
	OperatingSystem  string
	Architecture     string
	StartedAt        time.Time
	ConnectedAt      time.Time
	LastSeenAt       time.Time
	DisconnectedAt   *time.Time

	ObservationSequence           uint64
	ObservedAt                    time.Time
	LastAcknowledgedAtHello       *uint64
	Capabilities                  []uint32
	AuthorizedIndexes             []string
	Inputs                        []InputRegistration
	Queue                         QueueTelemetry
	InputHealth                   []InputHealth
	LastSentBatchSequence         *uint64
	LastAcknowledgedBatchSequence *uint64
	ProcessResidentMemoryBytes    uint64
	ProcessCPUPercent             float64
}

// Store owns GORM persistence over the already migrated control database.
type Store struct {
	orm *gorm.DB
}
