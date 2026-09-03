package clickhouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	sqldriver "database/sql/driver"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"fortio.org/safecast"
	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

const (
	defaultDatabase                    = "open_splunk"
	defaultTable                       = "events"
	visibilityFinalizeTimeout          = 10 * time.Second
	writeGroupTargetRows               = 10_000
	writeGroupHardMaxRows              = visibility.MaxWriteGroupRows
	writeGroupTargetDecodedBytes       = 16 << 20
	writeGroupHardMaxDecodedBytes      = visibility.MaxWriteGroupDecodedBytes
	writeGroupMaxMembers               = visibility.MaxWriteGroupMembers
	writeGroupMaxLinger                = 200 * time.Millisecond
	writeGroupShutdownDrainTimeout     = 10 * time.Second
	durableClickHouseIdempotencyWindow = 10_000
	durableBatchRejectWindow           = 10_000
	durableBatchRejectMetadataBytes    = 256 << 20
	durableBatchRejectPruneWakeBytes   = durableBatchRejectMetadataBytes / 4
	visibilityPruneBatch               = 1_000

	extendedTypeKey  = "\x00open_splunk_type"
	extendedValueKey = "\x00open_splunk_value"

	eventIndexNameColumn          = 2
	eventIndexTimeColumn          = 4
	eventExpiresAtColumn          = 25
	eventVisibilitySequenceColumn = 26

	reservationMetadataVersion = byte(1)
)

var (
	decimalValuePattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?(?:0|[1-9][0-9]*))?$`)
	eventInsertColumns  = []string{
		"event_id", "tenant_id", "index_name", "event_time", "index_time",
		"collected_at", "event_time_source", "host", "source", "sourcetype",
		"service", "severity", "level", "body", "raw", "raw_encoding",
		"trace_id", "span_id", "fields", "field_names", "collector_id",
		"ingest_source_kind", "ingest_source_id", "batch_id", "batch_sequence", "expires_at", "visibility_seq",
		"field_types", "field_metadata_version",
	}
	eventsInsertSQL = buildEventsInsertSQL(defaultDatabase, defaultTable)
)

// RetentionProvider resolves retention for trusted callers which do not carry
// an admitted StoreBatch retention snapshot. Native collector ingestion sends
// its transactional index-policy snapshot with every fresh batch, so that path
// never re-reads mutable control-plane policy here. Fallback callers must
// return the final positive duration, including any deployment default.
type RetentionProvider interface {
	RetentionForIndex(context.Context, string, string) (time.Duration, error)
}

// RetentionProviderFunc adapts a function to RetentionProvider.
type RetentionProviderFunc func(context.Context, string, string) (time.Duration, error)

func (f RetentionProviderFunc) RetentionForIndex(ctx context.Context, tenantID, indexName string) (time.Duration, error) {
	return f(ctx, tenantID, indexName)
}

// Config controls a native ClickHouse connection used by Store.
type Config struct {
	Addresses       []string
	Database        string
	Table           string
	Username        string
	Password        string
	TLS             *tls.Config
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	RetryAfter      time.Duration
}

// DefaultConfig returns conservative single-node native-protocol defaults.
// Plaintext is accepted only for loopback addresses.
func DefaultConfig() Config {
	return Config{
		Addresses:       []string{"127.0.0.1:9000"},
		Database:        defaultDatabase,
		Table:           defaultTable,
		Username:        "default",
		DialTimeout:     5 * time.Second,
		ReadTimeout:     30 * time.Second,
		MaxOpenConns:    8,
		MaxIdleConns:    4,
		ConnMaxLifetime: 30 * time.Minute,
		RetryAfter:      time.Second,
	}
}

// Open creates a Store backed by clickhouse-go's native protocol. Open is
// deliberately lazy like the driver; call Ping during application startup when
// readiness must verify credentials and network reachability.
//
// The configured database/table name must remain exclusively bound to one
// physical table generation while the Store is open. Apply migrations before
// Open, and do not concurrently rename, drop, exchange, or replace that table.
// The deletion protocol detects observable UUID drift, but ClickHouse ALTER
// targets a table by name and cannot fence privileged out-of-band DDL.
func Open(config Config, retention RetentionProvider, sequencer visibility.Sequencer) (*Store, error) {
	options, normalized, err := config.clickHouseOptions()
	if err != nil {
		return nil, err
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse connection: %w", err)
	}
	store, err := newStore(
		&nativeStoreConnection{connection: connection},
		normalized.Database,
		normalized.Table,
		retention,
		sequencer,
		time.Now,
		normalized.RetryAfter,
	)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	store.startReconciler()
	return store, nil
}

// NewStore wraps one existing clickhouse-go connection for both ordinary
// runtime traffic and index deletion. It remains useful for tests and
// compatibility; production callers that isolate ALTER DELETE privileges
// should use NewStoreWithDeletionConnection.
//
// A caller that uses WithWritesFrozen must route every writer for the physical
// events table through the returned Store for the lifetime of the frozen
// callback. It must also apply table-changing DDL before NewStore and keep the
// configured table name exclusively bound to that generation until Store.Close;
// see Open.
func NewStore(connection clickhousedriver.Conn, retention RetentionProvider, sequencer visibility.Sequencer) (*Store, error) {
	if connection == nil {
		return nil, errors.New("ClickHouse connection is required")
	}
	defaults := DefaultConfig()
	store, err := newStore(
		&nativeStoreConnection{connection: connection},
		defaults.Database,
		defaults.Table,
		retention,
		sequencer,
		time.Now,
		defaults.RetryAfter,
	)
	if err != nil {
		return nil, err
	}
	store.startReconciler()
	return store, nil
}

// NewStoreWithDeletionConnection wraps distinct ordinary-runtime and
// deletion-only clickhouse-go connections. Inserts and other Store traffic use
// connection. Physical index-deletion target resolution, reconciliation,
// absence proofs, and ALTER DELETE use deletionConnection exclusively.
//
// The caller retains ownership of both connections when construction fails.
// After construction succeeds, Store owns both and closes the deletion
// connection before the ordinary connection.
func NewStoreWithDeletionConnection(
	connection clickhousedriver.Conn,
	deletionConnection clickhousedriver.Conn,
	retention RetentionProvider,
	sequencer visibility.Sequencer,
) (*Store, error) {
	if connection == nil {
		return nil, errors.New("ClickHouse connection is required")
	}
	if deletionConnection == nil {
		return nil, errors.New("ClickHouse deletion connection is required")
	}
	if sameConnectionIdentity(connection, deletionConnection) {
		return nil, errors.New(
			"ClickHouse deletion connection must be distinct from the ordinary connection",
		)
	}
	defaults := DefaultConfig()
	store, err := newStoreWithDeletionConnection(
		&nativeStoreConnection{connection: connection},
		&nativeStoreConnection{connection: deletionConnection},
		defaults.Database,
		defaults.Table,
		retention,
		sequencer,
		time.Now,
		defaults.RetryAfter,
	)
	if err != nil {
		return nil, err
	}
	store.startReconciler()
	return store, nil
}

// Store implements ingest.EventStore by durably staging logical batches and
// coalescing them into bounded synchronous native inserts.
type Store struct {
	connection                storeConnection
	deletionConnection        storeConnection
	database                  string
	table                     string
	insertSQL                 string
	retention                 RetentionProvider
	visibility                visibility.Sequencer
	writeGroupVisibility      visibility.WriteGroupSequencer
	writeGroupLimits          visibility.WriteGroupLimits
	commitWaiters             *commitWaiters
	attemptID                 func() (string, error)
	clock                     func() time.Time
	retryAfter                time.Duration
	writeAdmission            *writeAdmission
	reconcileSlot             chan struct{}
	lifecycleMu               sync.Mutex
	lifecycleContext          context.Context
	lifecycleCancel           context.CancelCauseFunc
	operations                sync.WaitGroup
	reconcileWake             chan struct{}
	reconcileDone             chan struct{}
	reconcileCancel           context.CancelCauseFunc
	reconcileErr              error
	coalescing                bool
	closed                    bool
	closeOnce                 sync.Once
	closeErr                  error
	terminalCount             atomic.Uint64
	rejectionWakeBytes        atomic.Uint64
	reconciliationSuccesses   atomic.Uint64
	reconciliationRetries     atomic.Uint64
	reconciliationAmbiguities atomic.Uint64
	stagedLogicalBatches      atomic.Uint64
	stagedLogicalRows         atomic.Uint64
	formedGroups              atomic.Uint64
	physicalSends             atomic.Uint64
	successfulGroups          atomic.Uint64
	groupMemberBatches        atomic.Uint64
	groupRows                 atomic.Uint64
	groupDecodedBytes         atomic.Uint64
	groupMonthlyPartitions    atomic.Uint64
	groupFillReasons          writeGroupFillCounters
	groupSealLatency          coalescingDurationHistogram
	groupSendLatency          coalescingDurationHistogram
	groupCommitLatency        coalescingDurationHistogram
	waiterWakeups             atomic.Uint64
	waiterCancellations       atomic.Uint64
	waiterTerminalLookups     atomic.Uint64
}

// HECReconciliationSnapshot is a constant-shape aggregate view of the
// background outbox authority. It contains no error text or durable identity.
type HECReconciliationSnapshot struct {
	Available                 bool
	Successes                 uint64
	Retries                   uint64
	Ambiguities               uint64
	StagedLogicalBatches      uint64
	StagedLogicalRows         uint64
	FormedGroups              uint64
	PhysicalSends             uint64
	SuccessfulGroups          uint64
	GroupMemberBatches        uint64
	GroupRows                 uint64
	GroupDecodedBytes         uint64
	GroupMonthlyPartitions    uint64
	FillRowTarget             uint64
	FillByteTarget            uint64
	FillHardBoundary          uint64
	FillLinger                uint64
	FillDrain                 uint64
	FillRecovery              uint64
	NativeWaiters             uint64
	NativeWaiterWakeups       uint64
	NativeWaiterCancellations uint64
	NativeTerminalLookups     uint64
	SealLatency               CoalescingDurationHistogramSnapshot
	SendLatency               CoalescingDurationHistogramSnapshot
	CommitLatency             CoalescingDurationHistogramSnapshot
}

type hecTerminalPruner interface {
	PruneHECTerminalRequests(context.Context, time.Time, uint32) (uint32, error)
}

var _ ingest.EventStore = (*Store)(nil)
var _ ingest.StagingEventStore = (*Store)(nil)
var _ ingest.RecoverableEventStore = (*Store)(nil)

func newStore(
	connection storeConnection,
	database, table string,
	retention RetentionProvider,
	sequencer visibility.Sequencer,
	clock func() time.Time,
	retryAfter time.Duration,
) (*Store, error) {
	return newStoreWithConnections(
		connection,
		nil,
		database,
		table,
		retention,
		sequencer,
		clock,
		retryAfter,
	)
}

func newStoreWithDeletionConnection(
	connection storeConnection,
	deletionConnection storeConnection,
	database, table string,
	retention RetentionProvider,
	sequencer visibility.Sequencer,
	clock func() time.Time,
	retryAfter time.Duration,
) (*Store, error) {
	if deletionConnection == nil {
		return nil, errors.New("ClickHouse deletion connection is required")
	}
	if sameConnectionIdentity(connection, deletionConnection) {
		return nil, errors.New(
			"ClickHouse deletion connection must be distinct from the ordinary connection",
		)
	}
	return newStoreWithConnections(
		connection,
		deletionConnection,
		database,
		table,
		retention,
		sequencer,
		clock,
		retryAfter,
	)
}

func newStoreWithConnections(
	connection storeConnection,
	deletionConnection storeConnection,
	database, table string,
	retention RetentionProvider,
	sequencer visibility.Sequencer,
	clock func() time.Time,
	retryAfter time.Duration,
) (*Store, error) {
	if connection == nil {
		return nil, errors.New("ClickHouse connection is required")
	}
	if retention == nil {
		return nil, errors.New("ClickHouse retention provider is required")
	}
	if sequencer == nil {
		return nil, errors.New("ClickHouse visibility sequencer is required")
	}
	if !physicalIdentifier.MatchString(database) || !physicalIdentifier.MatchString(table) {
		return nil, errors.New("ClickHouse database and table must be simple identifiers")
	}
	if clock == nil {
		return nil, errors.New("ClickHouse store clock is required")
	}
	if retryAfter <= 0 {
		return nil, errors.New("ClickHouse retry delay must be positive")
	}
	reconcileSlot := make(chan struct{}, 1)
	reconcileSlot <- struct{}{}
	lifecycleContext, lifecycleCancel := context.WithCancelCause(context.Background())
	store := &Store{
		connection:         connection,
		deletionConnection: deletionConnection,
		database:           database,
		table:              table,
		insertSQL:          buildEventsInsertSQL(database, table),
		retention:          retention,
		visibility:         sequencer,
		attemptID:          randomAttemptID,
		clock:              clock,
		retryAfter:         retryAfter,
		writeAdmission:     newWriteAdmission(),
		writeGroupLimits: visibility.WriteGroupLimits{
			TargetRows:          writeGroupTargetRows,
			HardMaxRows:         writeGroupHardMaxRows,
			TargetDecodedBytes:  writeGroupTargetDecodedBytes,
			HardMaxDecodedBytes: writeGroupHardMaxDecodedBytes,
			MaxMembers:          writeGroupMaxMembers,
			MaxLinger:           writeGroupMaxLinger,
		},
		commitWaiters:    newCommitWaiters(visibility.MaxPendingReservations),
		reconcileSlot:    reconcileSlot,
		lifecycleContext: lifecycleContext,
		lifecycleCancel:  lifecycleCancel,
	}
	store.writeGroupVisibility, _ = sequencer.(visibility.WriteGroupSequencer)
	return store, nil
}

func sameConnectionIdentity(left, right any) bool {
	if left == nil || right == nil {
		return false
	}
	leftType := reflect.TypeOf(left)
	return leftType == reflect.TypeOf(right) &&
		leftType.Comparable() &&
		left == right
}

// LookupBatch finds the durable disposition of an exact collector wire batch
// before mutable authorization and validation policy is applied again.
func (s *Store) LookupBatch(
	ctx context.Context,
	identity ingest.StoreBatchIdentity,
) (state ingest.StoredBatchState, result ingest.StoreResult, resultErr error) {
	operationContext, finishOperation, err := s.beginOperation(ctx, &resultErr)
	if err != nil {
		return ingest.StoredBatchNotFound, ingest.StoreResult{}, err
	}
	defer finishOperation()
	return s.lookupBatch(operationContext, identity)
}

func (s *Store) lookupBatch(
	ctx context.Context,
	identity ingest.StoreBatchIdentity,
) (ingest.StoredBatchState, ingest.StoreResult, error) {
	batch := storeBatchFromIdentity(identity)
	deduplicationKey := deduplicationToken(batch)
	sequenceKey := sequenceIdentityKey(batch)
	payloadDigest, err := storePayloadDigest(batch)
	if err != nil {
		return ingest.StoredBatchNotFound, ingest.StoreResult{}, err
	}
	prior, found, err := s.visibility.Lookup(ctx, deduplicationKey, sequenceKey, payloadDigest)
	if err != nil {
		return ingest.StoredBatchNotFound, ingest.StoreResult{}, s.visibilityFailure("lookup ClickHouse visibility reservation", err)
	}
	if !found {
		return ingest.StoredBatchNotFound, ingest.StoreResult{}, nil
	}
	if prior.Rejected || prior.AlreadyCommitted {
		result, resultErr := resultForReservation(prior, true)
		if resultErr != nil {
			return ingest.StoredBatchNotFound, ingest.StoreResult{}, resultErr
		}
		if prior.Rejected {
			return ingest.StoredBatchRejected, result, nil
		}
		return ingest.StoredBatchCommitted, result, nil
	}
	return ingest.StoredBatchPending, ingest.StoreResult{}, nil
}

// ResumeBatch replays only the normalized block durably retained by the
// server. Caller-supplied policy-derived events are intentionally impossible.
func (s *Store) ResumeBatch(ctx context.Context, identity ingest.StoreBatchIdentity) (ingest.StoreResult, error) {
	return s.store(ctx, storeBatchFromIdentity(identity), true)
}

// Store inserts every normalized event in its original order. Before the
// first possible ClickHouse side effect it persists a byte-stable replay
// outbox and the exact per-source-event acknowledgment disposition.
func (s *Store) Store(ctx context.Context, batch ingest.StoreBatch) (ingest.StoreResult, error) {
	return s.store(ctx, batch, false)
}

// Stage commits the immutable replay outbox, quota charge, and visibility
// reservation without waiting for ClickHouse. It releases the request-scoped
// attempt lease before returning so the existing reconciler is the sole
// completion authority.
func (s *Store) Stage(
	ctx context.Context,
	batch ingest.StoreBatch,
) (result ingest.StageResult, resultErr error) {
	if frozenCallbackActive(ctx, s) {
		return ingest.StageResult{}, ErrWriteFreezeReentrant
	}
	operationContext, finishOperation, err := s.beginOperation(ctx, &resultErr)
	if err != nil {
		return ingest.StageResult{}, err
	}
	defer finishOperation()
	if err := s.writeAdmission.enter(operationContext); err != nil {
		return ingest.StageResult{}, err
	}
	defer s.writeAdmission.leave()
	return s.stageAdmitted(operationContext, batch)
}

func (s *Store) stageAdmitted(ctx context.Context, batch ingest.StoreBatch) (ingest.StageResult, error) {
	source, sourceErr := ingest.CanonicalIngestionSource(batch.Source, batch.CollectorID)
	if sourceErr != nil {
		return ingest.StageResult{}, fmt.Errorf("stage ClickHouse batch: %w", sourceErr)
	}
	if batch.HECAdmission != nil {
		if source.Kind != ingest.IngestionSourceKindHEC ||
			batch.HECAdmission.TokenID != source.ID ||
			batch.HECAdmission.RequestID != batch.BatchID {
			return ingest.StageResult{}, errors.New("stage ClickHouse batch: HEC admission identity does not match batch source")
		}
	}
	deduplicationKey := deduplicationToken(batch)
	sequenceKey := sequenceIdentityKey(batch)
	payloadDigest, err := storePayloadDigest(batch)
	if err != nil {
		return ingest.StageResult{}, err
	}
	prior, found, err := s.visibility.Lookup(ctx, deduplicationKey, sequenceKey, payloadDigest)
	if err != nil {
		return ingest.StageResult{}, s.visibilityFailure("lookup staged ClickHouse visibility reservation", err)
	}
	if found {
		return stageResultForReservation(prior)
	}

	payload, err := s.freshReservationPayload(ctx, batch)
	if err != nil {
		return ingest.StageResult{}, err
	}
	attemptID, err := s.attemptID()
	if err != nil {
		return ingest.StageResult{}, s.classifyError(fmt.Errorf("create staged ClickHouse visibility attempt: %w", err))
	}
	reservation, err := s.visibility.Reserve(ctx, visibility.ReserveRequest{
		BatchKey:          deduplicationKey,
		SequenceKey:       sequenceKey,
		AttemptID:         attemptID,
		IndexTime:         payload.indexTime,
		PayloadSHA256:     payloadDigest,
		Metadata:          payload.metadata,
		Outbox:            payload.outbox,
		StoredRowCount:    payload.storedRowCount,
		DecodedEventBytes: payload.decodedEventBytes,
		QuotaAdmission:    batch.QuotaAdmission,
		QuotaEvaluatedAt:  batch.QuotaEvaluatedAt,
		HECAdmission:      visibilityHECAdmission(batch),
	})
	if err != nil {
		if _, ok := errors.AsType[*ingestquota.ExceededError](err); !ok {
			s.wakeReconciler()
		}
		return ingest.StageResult{}, s.visibilityFailure("stage ClickHouse visibility reservation", err)
	}
	if reservation.AlreadyCommitted || reservation.Rejected {
		return stageResultForReservation(reservation)
	}
	if !reservation.PreviouslyReserved {
		s.noteStagedLogicalBatch(reservation.StoredRowCount)
	}
	if err := s.releaseAttempt(reservation.Sequence, attemptID, nil); err != nil {
		return ingest.StageResult{}, err
	}
	return ingest.StageResult{
		VisibilitySequence:  reservation.Sequence,
		State:               ingest.StoredBatchPending,
		HECRequestSequence:  reservation.HECRequestSequence,
		HECAcknowledgmentID: reservation.HECAcknowledgmentID,
	}, nil
}

func visibilityHECAdmission(batch ingest.StoreBatch) *visibility.HECAdmissionRequest {
	if batch.HECAdmission == nil {
		return nil
	}
	return &visibility.HECAdmissionRequest{
		TenantID:              batch.TenantID,
		TokenID:               batch.HECAdmission.TokenID,
		TokenVersion:          batch.HECAdmission.TokenVersion,
		AuthorizedIndexes:     visibilityHECIndexAuthorities(batch.HECAdmission.AuthorizedIndexes),
		RequestID:             batch.HECAdmission.RequestID,
		Acknowledgment:        batch.HECAdmission.AcknowledgmentEnabled,
		AcknowledgmentChannel: batch.HECAdmission.Channel,
		CreatedAt:             batch.HECAdmission.CreatedAt,
	}
}

func visibilityHECIndexAuthorities(source []ingest.HECIndexAuthority) []visibility.HECIndexAuthority {
	result := make([]visibility.HECIndexAuthority, len(source))
	for index, authority := range source {
		result[index] = visibility.HECIndexAuthority{Name: authority.Name, Version: authority.Version}
	}
	return result
}

func stageResultForReservation(reservation visibility.Reservation) (ingest.StageResult, error) {
	if !reservation.AlreadyCommitted && !reservation.Rejected {
		return ingest.StageResult{
			VisibilitySequence:  reservation.Sequence,
			State:               ingest.StoredBatchPending,
			HECRequestSequence:  reservation.HECRequestSequence,
			HECAcknowledgmentID: reservation.HECAcknowledgmentID,
		}, nil
	}
	outcome, err := resultForReservation(reservation, true)
	if err != nil {
		return ingest.StageResult{}, err
	}
	state := ingest.StoredBatchCommitted
	if reservation.Rejected {
		state = ingest.StoredBatchRejected
	}
	return ingest.StageResult{
		VisibilitySequence:  reservation.Sequence,
		State:               state,
		Outcome:             outcome,
		HECRequestSequence:  reservation.HECRequestSequence,
		HECAcknowledgmentID: reservation.HECAcknowledgmentID,
	}, nil
}

// RejectBatch atomically records a terminal whole-batch response in the
// SQLite visibility ledger. It never prepares or sends a ClickHouse block.
// If another exact outcome won the identity race, that first durable outcome
// is returned unchanged.
func (s *Store) RejectBatch(
	ctx context.Context,
	rejected ingest.StoreBatchRejection,
) (result ingest.StoreResult, resultErr error) {
	if frozenCallbackActive(ctx, s) {
		return ingest.StoreResult{}, ErrWriteFreezeReentrant
	}
	operationContext, finishOperation, err := s.beginOperation(ctx, &resultErr)
	if err != nil {
		return ingest.StoreResult{}, err
	}
	defer finishOperation()
	if err := s.writeAdmission.enter(operationContext); err != nil {
		return ingest.StoreResult{}, err
	}
	defer s.writeAdmission.leave()
	return s.rejectBatchAdmitted(operationContext, rejected)
}

func (s *Store) rejectBatchAdmitted(
	ctx context.Context,
	rejected ingest.StoreBatchRejection,
) (ingest.StoreResult, error) {
	batch := storeBatchFromIdentity(rejected.Identity)
	deduplicationKey := deduplicationToken(batch)
	sequenceKey := sequenceIdentityKey(batch)
	payloadDigest, err := storePayloadDigest(batch)
	if err != nil {
		return ingest.StoreResult{}, err
	}
	if rejected.ReceivedAt.IsZero() {
		return ingest.StoreResult{}, errors.New("store terminal batch rejection: received time is required")
	}
	if rejected.Rejection == nil ||
		rejected.Rejection.GetBatchId() != rejected.Identity.BatchID ||
		rejected.Rejection.GetBatchSequence() != rejected.Identity.BatchSequence {
		return ingest.StoreResult{}, errors.New("store terminal batch rejection: response identity does not match source batch")
	}
	metadata, err := encodeBatchRejectionMetadata(rejected.Rejection)
	if err != nil {
		return ingest.StoreResult{}, err
	}
	rejectedAt := s.clock().UTC().Truncate(time.Microsecond)
	reservation, err := s.visibility.Reject(ctx, visibility.RejectRequest{
		BatchKey:      deduplicationKey,
		SequenceKey:   sequenceKey,
		IndexTime:     rejected.ReceivedAt,
		PayloadSHA256: payloadDigest,
		Metadata:      metadata,
		RejectedAt:    rejectedAt,
	})
	if err != nil {
		// A failed SQLite commit can be outcome-ambiguous: the rejection may
		// already be terminal even though this caller did not observe it. Wake
		// maintenance on every ledger error so a later exact replay (which is not
		// NewlyRejected) cannot leave terminal retention dependent on restart.
		s.wakeReconciler()
		return ingest.StoreResult{}, s.visibilityFailure("commit terminal batch rejection", err)
	}
	if reservation.NewlyRejected {
		s.noteTerminalRejection(safecast.MustConv[uint64](len(metadata)))
	}
	if reservation.Rejected || reservation.AlreadyCommitted {
		return resultForReservation(reservation, true)
	}
	// A concurrently accepted batch may already own an unresolved ClickHouse
	// outbox. The first durable outcome wins; let its owner or reconciler finish
	// and make the collector retry instead of overwriting it with a rejection.
	return ingest.StoreResult{}, s.visibilityFailure(
		"wait for existing ClickHouse batch outcome",
		visibility.ErrAttemptInProgress,
	)
}

func (s *Store) store(
	ctx context.Context,
	batch ingest.StoreBatch,
	resumeOnly bool,
) (result ingest.StoreResult, resultErr error) {
	if frozenCallbackActive(ctx, s) {
		return ingest.StoreResult{}, ErrWriteFreezeReentrant
	}
	operationContext, finishOperation, err := s.beginOperation(ctx, &resultErr)
	if err != nil {
		return ingest.StoreResult{}, err
	}
	defer finishOperation()
	if s.writeGroupVisibility != nil && s.coalescing {
		return s.storeGrouped(operationContext, batch, resumeOnly)
	}
	if err := s.writeAdmission.enter(operationContext); err != nil {
		return ingest.StoreResult{}, err
	}
	defer s.writeAdmission.leave()
	return s.storeAdmitted(operationContext, batch, resumeOnly)
}

func (s *Store) storeGrouped(
	ctx context.Context,
	batch ingest.StoreBatch,
	resumeOnly bool,
) (ingest.StoreResult, error) {
	if err := s.writeAdmission.enter(ctx); err != nil {
		return ingest.StoreResult{}, err
	}
	reservation, attemptID, duplicate, terminal, err := s.reserveForGroupedStore(ctx, batch, resumeOnly)
	if err != nil || terminal {
		s.writeAdmission.leave()
		if err != nil {
			return ingest.StoreResult{}, err
		}
		return resultForReservation(reservation, true)
	}

	ready, cancelWaiter, err := s.commitWaiters.register(reservation.Sequence)
	if err != nil {
		s.writeAdmission.leave()
		capacityErr := &ingest.TransientStoreError{
			Err:        err,
			Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			RetryAfter: s.retryAfter,
		}
		if attemptID != "" {
			return ingest.StoreResult{}, s.releaseAttempt(reservation.Sequence, attemptID, capacityErr)
		}
		return ingest.StoreResult{}, capacityErr
	}
	defer func() { cancelWaiter() }()
	if attemptID != "" {
		if err := s.releaseAttempt(reservation.Sequence, attemptID, nil); err != nil {
			s.writeAdmission.leave()
			return ingest.StoreResult{}, err
		}
	} else {
		s.wakeReconciler()
	}
	s.writeAdmission.leave()

	// Registration is deliberately non-authoritative. Re-read the durable
	// ledger after installing the waiter to close the commit-before-register
	// race, and after every wake to survive lost or stale process-local signals.
	for {
		state, result, lookupErr := s.lookupBatch(ctx, storeBatchIdentity(batch))
		if lookupErr != nil {
			if ctx.Err() != nil {
				s.waiterCancellations.Add(1)
			}
			return ingest.StoreResult{}, lookupErr
		}
		switch state {
		case ingest.StoredBatchCommitted, ingest.StoredBatchRejected:
			s.waiterTerminalLookups.Add(1)
			cancelWaiter()
			if !duplicate && state == ingest.StoredBatchCommitted {
				result.Accepted = result.Duplicate
				result.Duplicate = 0
			}
			return result, nil
		case ingest.StoredBatchPending:
		case ingest.StoredBatchNotFound:
			return ingest.StoreResult{}, s.visibilityFailure(
				"wait for grouped ClickHouse batch outcome",
				visibility.ErrReservationGone,
			)
		default:
			return ingest.StoreResult{}, errors.New("wait for grouped ClickHouse batch outcome: invalid durable state")
		}
		poll := time.NewTimer(s.retryAfter)
		select {
		case <-ctx.Done():
			stopStoreTimer(poll)
			s.waiterCancellations.Add(1)
			return ingest.StoreResult{}, ctx.Err()
		case <-ready:
			stopStoreTimer(poll)
			cancelWaiter()
			ready, cancelWaiter, err = s.commitWaiters.register(reservation.Sequence)
			if err != nil {
				return ingest.StoreResult{}, &ingest.TransientStoreError{
					Err:        err,
					Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
					RetryAfter: s.retryAfter,
				}
			}
		case <-poll.C:
		}
	}
}

func stopStoreTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (s *Store) reserveForGroupedStore(
	ctx context.Context,
	batch ingest.StoreBatch,
	resumeOnly bool,
) (visibility.Reservation, string, bool, bool, error) {
	deduplicationKey := deduplicationToken(batch)
	sequenceKey := sequenceIdentityKey(batch)
	payloadDigest, err := storePayloadDigest(batch)
	if err != nil {
		return visibility.Reservation{}, "", false, false, err
	}
	prior, found, err := s.visibility.Lookup(ctx, deduplicationKey, sequenceKey, payloadDigest)
	if err != nil {
		return visibility.Reservation{}, "", false, false,
			s.visibilityFailure("lookup ClickHouse visibility reservation", err)
	}
	if found && (prior.AlreadyCommitted || prior.Rejected) {
		return prior, "", true, true, nil
	}
	if found {
		// A grouped pending reservation is owned only by its group lease. Exact
		// retries observe and wait; they must never reacquire a per-reservation
		// attempt that could race or obstruct physical replay.
		return prior, "", true, false, nil
	}

	request := visibility.ReserveRequest{
		BatchKey:          deduplicationKey,
		SequenceKey:       sequenceKey,
		ExistingOnly:      resumeOnly,
		IndexTime:         prior.IndexTime,
		PayloadSHA256:     payloadDigest,
		Metadata:          prior.Metadata,
		Outbox:            prior.Outbox,
		StoredRowCount:    prior.StoredRowCount,
		DecodedEventBytes: prior.DecodedEventBytes,
		QuotaAdmission:    batch.QuotaAdmission,
		QuotaEvaluatedAt:  batch.QuotaEvaluatedAt,
	}
	if !found && !resumeOnly {
		payload, payloadErr := s.freshReservationPayload(ctx, batch)
		if payloadErr != nil {
			return visibility.Reservation{}, "", false, false, payloadErr
		}
		applyFreshReservationPayload(&request, payload)
	}
	attemptID, err := s.attemptID()
	if err != nil {
		return visibility.Reservation{}, "", false, false,
			s.classifyError(fmt.Errorf("create ClickHouse visibility attempt: %w", err))
	}
	request.AttemptID = attemptID
	reservation, err := s.visibility.Reserve(ctx, request)
	if errors.Is(err, visibility.ErrAttemptInProgress) {
		observed, observedFound, lookupErr := s.visibility.Lookup(
			ctx,
			deduplicationKey,
			sequenceKey,
			payloadDigest,
		)
		if lookupErr != nil {
			return visibility.Reservation{}, "", false, false,
				s.visibilityFailure("recheck concurrently staged ClickHouse batch", lookupErr)
		}
		if observedFound {
			return observed, "", true,
				observed.AlreadyCommitted || observed.Rejected, nil
		}
	}
	if err != nil {
		if _, ok := errors.AsType[*ingestquota.ExceededError](err); !ok {
			s.wakeReconciler()
		}
		if !resumeOnly && errors.Is(err, visibility.ErrReservationGone) {
			return visibility.Reservation{}, "", false, false, &ingest.TransientStoreError{
				Err:        fmt.Errorf("reserve fresh ClickHouse visibility sequence: %w", err),
				Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
				RetryAfter: s.retryAfter,
			}
		}
		return visibility.Reservation{}, "", false, false,
			s.visibilityFailure("reserve ClickHouse visibility sequence", err)
	}
	duplicate := found || reservation.PreviouslyReserved || resumeOnly
	if !duplicate && !reservation.AlreadyCommitted && !reservation.Rejected {
		s.noteStagedLogicalBatch(reservation.StoredRowCount)
	}
	return reservation, attemptID, duplicate,
		reservation.AlreadyCommitted || reservation.Rejected, nil
}

func (s *Store) noteStagedLogicalBatch(rows uint32) {
	s.stagedLogicalBatches.Add(1)
	s.stagedLogicalRows.Add(uint64(rows))
}

func applyFreshReservationPayload(request *visibility.ReserveRequest, payload freshReservationPayload) {
	request.IndexTime = payload.indexTime
	request.Metadata = payload.metadata
	request.Outbox = payload.outbox
	request.StoredRowCount = payload.storedRowCount
	request.DecodedEventBytes = payload.decodedEventBytes
}

func storeBatchIdentity(batch ingest.StoreBatch) ingest.StoreBatchIdentity {
	return ingest.StoreBatchIdentity{
		TenantID:          batch.TenantID,
		Source:            batch.Source,
		CollectorID:       batch.CollectorID,
		BatchID:           batch.BatchID,
		BatchSequence:     batch.BatchSequence,
		SourceBatchSHA256: batch.SourceBatchSHA256,
	}
}

func (s *Store) storeAdmitted(
	ctx context.Context,
	batch ingest.StoreBatch,
	resumeOnly bool,
) (ingest.StoreResult, error) {
	deduplicationKey := deduplicationToken(batch)
	sequenceKey := sequenceIdentityKey(batch)
	payloadDigest, err := storePayloadDigest(batch)
	if err != nil {
		return ingest.StoreResult{}, err
	}
	prior, found, err := s.visibility.Lookup(ctx, deduplicationKey, sequenceKey, payloadDigest)
	if err != nil {
		return ingest.StoreResult{}, s.visibilityFailure("lookup ClickHouse visibility reservation", err)
	}
	if found && (prior.AlreadyCommitted || prior.Rejected) {
		return resultForReservation(prior, true)
	}

	metadata := prior.Metadata
	outbox := prior.Outbox
	indexTime := prior.IndexTime
	storedRowCount := prior.StoredRowCount
	decodedBytes := prior.DecodedEventBytes
	// Lookup intentionally does not acquire a lease. A pending row may become
	// terminal or be safely abandoned before Reserve starts, so observed pending
	// rows first use the atomic existing-only path and never recreate anything
	// from the lightweight lookup result. A full Store call can safely make one
	// fresh reservation fallback from its caller-supplied normalized batch;
	// identity-only ResumeBatch calls cannot.
	existingOnly := found || resumeOnly
	if !found && !resumeOnly {
		payload, payloadErr := s.freshReservationPayload(ctx, batch)
		err = payloadErr
		if err != nil {
			return ingest.StoreResult{}, err
		}
		metadata, outbox, indexTime = payload.metadata, payload.outbox, payload.indexTime
		storedRowCount, decodedBytes = payload.storedRowCount, payload.decodedEventBytes
	}
	attemptID, err := s.attemptID()
	if err != nil {
		return ingest.StoreResult{}, s.classifyError(fmt.Errorf("create ClickHouse visibility attempt: %w", err))
	}
	request := visibility.ReserveRequest{
		BatchKey:          deduplicationKey,
		SequenceKey:       sequenceKey,
		AttemptID:         attemptID,
		ExistingOnly:      existingOnly,
		IndexTime:         indexTime,
		PayloadSHA256:     payloadDigest,
		Metadata:          metadata,
		Outbox:            outbox,
		StoredRowCount:    storedRowCount,
		DecodedEventBytes: decodedBytes,
		QuotaAdmission:    batch.QuotaAdmission,
		QuotaEvaluatedAt:  batch.QuotaEvaluatedAt,
	}
	reservation, err := s.visibility.Reserve(ctx, request)
	if found && !resumeOnly && errors.Is(err, visibility.ErrReservationGone) {
		// ExistingOnly proves the observed row no longer has an active outcome and
		// releases the attempted lease before returning. Reuse that attempt ID for
		// one bounded fresh allocation from the complete normalized Store input.
		payload, payloadErr := s.freshReservationPayload(ctx, batch)
		err = payloadErr
		if err != nil {
			return ingest.StoreResult{}, err
		}
		metadata, outbox, indexTime = payload.metadata, payload.outbox, payload.indexTime
		storedRowCount, decodedBytes = payload.storedRowCount, payload.decodedEventBytes
		request.ExistingOnly = false
		request.IndexTime = indexTime
		request.Metadata = metadata
		request.Outbox = outbox
		request.StoredRowCount = storedRowCount
		request.DecodedEventBytes = decodedBytes
		found = false
		reservation, err = s.visibility.Reserve(ctx, request)
	}
	if err != nil {
		// A failed SQLite commit can be outcome-ambiguous. Wake the server-owned
		// reconciler so any reservation that did persist is not dependent on a
		// collector retry or process restart.
		if _, ok := errors.AsType[*ingestquota.ExceededError](err); !ok {
			s.wakeReconciler()
		}
		if !resumeOnly && errors.Is(err, visibility.ErrReservationGone) {
			return ingest.StoreResult{}, &ingest.TransientStoreError{
				Err:        fmt.Errorf("reserve fresh ClickHouse visibility sequence: %w", err),
				Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
				RetryAfter: s.retryAfter,
			}
		}
		return ingest.StoreResult{}, s.visibilityFailure("reserve ClickHouse visibility sequence", err)
	}
	if reservation.AlreadyCommitted || reservation.Rejected {
		return resultForReservation(reservation, true)
	}
	return s.writeReservation(ctx, reservation, attemptID, found || reservation.PreviouslyReserved || resumeOnly)
}

type freshReservationPayload struct {
	metadata          []byte
	outbox            []byte
	indexTime         time.Time
	storedRowCount    uint32
	decodedEventBytes uint64
}

func (s *Store) freshReservationPayload(
	ctx context.Context,
	batch ingest.StoreBatch,
) (freshReservationPayload, error) {
	if batch.OriginalEventCount == 0 && len(batch.RejectedEvents) == 0 && len(batch.Events) > 0 {
		eventCount, conversionErr := safecast.Conv[uint32](len(batch.Events))
		if conversionErr != nil {
			return freshReservationPayload{}, errors.New("store ClickHouse batch: source event count exceeds uint32")
		}
		batch.OriginalEventCount = eventCount
	}
	rows, err := s.rowsForBatch(ctx, batch, nil)
	if err != nil {
		return freshReservationPayload{}, s.classifyError(err)
	}
	metadata, err := encodeReservationMetadata(rows, batch)
	if err != nil {
		return freshReservationPayload{}, err
	}
	outbox, err := encodeStoreOutbox(batch)
	if err != nil {
		return freshReservationPayload{}, err
	}
	rowCount, err := safecast.Conv[uint32](len(rows))
	if err != nil {
		return freshReservationPayload{}, errors.New("store ClickHouse batch: accepted row count exceeds uint32")
	}
	return freshReservationPayload{
		metadata:          metadata,
		outbox:            outbox,
		indexTime:         batch.ReceivedAt,
		storedRowCount:    rowCount,
		decodedEventBytes: decodedEventBytes(batch),
	}, nil
}

func decodedEventBytes(batch ingest.StoreBatch) uint64 {
	events := make([]*opensplunk.LogEvent, 0, len(batch.Events))
	for _, stored := range batch.Events {
		if stored != nil {
			events = append(events, stored.Event)
		}
	}
	return ingest.UncompressedEventBytes(events)
}

func (s *Store) writeReservation(
	ctx context.Context,
	reservation visibility.Reservation,
	attemptID string,
	duplicate bool,
) (ingest.StoreResult, error) {
	replayBatch, err := decodeStoreOutbox(reservation.Outbox)
	if err != nil {
		return ingest.StoreResult{}, s.finishPreSend(
			reservation, attemptID,
			fmt.Errorf("decode durable ClickHouse outbox: %w", err),
		)
	}
	replayDigest, err := storePayloadDigest(replayBatch)
	if err != nil || replayDigest != reservation.PayloadSHA256 ||
		deduplicationToken(replayBatch) != reservation.BatchKey || sequenceIdentityKey(replayBatch) != reservation.SequenceKey {
		return ingest.StoreResult{}, s.finishPreSend(
			reservation, attemptID,
			errors.New("durable ClickHouse outbox identity does not match its reservation"),
		)
	}
	rows, err := s.rowsForBatch(ctx, replayBatch, &reservation)
	if err != nil {
		return ingest.StoreResult{}, s.finishPreSend(reservation, attemptID, s.classifyError(err))
	}
	if err := applyReservation(rows, reservation); err != nil {
		return ingest.StoreResult{}, s.finishPreSend(
			reservation, attemptID,
			s.visibilityFailure("apply ClickHouse visibility reservation", err),
		)
	}

	settings := insertSettings(reservation.BatchKey)
	prepared, err := s.connection.prepare(ctx, s.insertSQL, settings)
	if err != nil {
		return ingest.StoreResult{}, s.finishPreSend(
			reservation, attemptID,
			s.classifyError(fmt.Errorf("prepare ClickHouse event batch: %w", err)),
		)
	}
	closed := false
	defer func() {
		if !closed {
			_ = prepared.Close()
		}
	}()

	for i, row := range rows {
		if err := prepared.Append(row...); err != nil {
			_ = prepared.Abort()
			return ingest.StoreResult{}, s.finishPreSend(
				reservation, attemptID,
				s.classifyError(fmt.Errorf("append ClickHouse event row %d: %w", i, err)),
			)
		}
	}
	if err := s.visibility.MarkSending(ctx, reservation.Sequence, attemptID); err != nil {
		_ = prepared.Abort()
		return ingest.StoreResult{}, s.finishMarkSendingFailure(
			reservation, attemptID,
			s.finalizationFailure("mark ClickHouse visibility sequence sending", err),
		)
	}
	s.physicalSends.Add(1)
	if err := prepared.Send(); err != nil {
		// Send failures are ambiguous: ClickHouse may still finish after the
		// client loses its response. The durable outbox lets the server retry the
		// exact normalized block with the same deduplication token independently
		// of the collector.
		_ = prepared.Abort()
		return ingest.StoreResult{}, s.releaseAttempt(
			reservation.Sequence,
			attemptID,
			s.classifyError(fmt.Errorf("send ClickHouse event batch: %w", err)),
		)
	}
	committedAt := s.clock().UTC().Truncate(time.Microsecond)
	if err := s.commitVisibility(reservation.Sequence, attemptID, committedAt); err != nil {
		return ingest.StoreResult{}, s.releaseAttempt(reservation.Sequence, attemptID, err)
	}
	if err := prepared.Close(); err != nil {
		closed = true
		return ingest.StoreResult{}, s.classifyError(fmt.Errorf("close committed ClickHouse event batch: %w", err))
	}
	closed = true

	return resultForReservation(visibility.Reservation{
		Sequence:    reservation.Sequence,
		Metadata:    reservation.Metadata,
		CommittedAt: committedAt,
	}, duplicate)
}

// ReconcilePending drains durable outbox records without any collector or
// bearer-token dependency. Normal reconciliation is serialized before it
// joins shared write admission, so an exclusive frozen drain can bypass this
// slot without deadlocking behind a queued reconciler.
func (s *Store) ReconcilePending(ctx context.Context) (resultErr error) {
	if frozenCallbackActive(ctx, s) {
		return ErrWriteFreezeReentrant
	}
	operationContext, finishOperation, err := s.beginOperation(ctx, &resultErr)
	if err != nil {
		return err
	}
	defer finishOperation()
	select {
	case <-operationContext.Done():
		return operationContext.Err()
	case <-s.reconcileSlot:
	}
	defer func() { s.reconcileSlot <- struct{}{} }()
	if err := s.writeAdmission.enter(operationContext); err != nil {
		return err
	}
	defer s.writeAdmission.leave()
	if s.writeGroupVisibility != nil && s.coalescing {
		_, err := s.reconcileWriteGroups(operationContext, true, false)
		return err
	}
	return s.reconcilePending(operationContext, false)
}

func (s *Store) reconcileWriteGroups(
	ctx context.Context,
	forceSeal bool,
	proveDrained bool,
) (time.Time, error) {
	var replayed uint32
	for {
		attemptID, err := s.attemptID()
		if err != nil {
			return time.Time{}, s.classifyError(fmt.Errorf("create ClickHouse write-group attempt: %w", err))
		}
		legacyAmbiguous, foundLegacyAmbiguous, err := s.writeGroupVisibility.AcquireUngroupedAmbiguous(
			ctx,
			attemptID,
		)
		if err != nil {
			return time.Time{}, s.visibilityFailure("acquire ungrouped ambiguous ClickHouse outbox", err)
		}
		if foundLegacyAmbiguous {
			s.reconciliationAmbiguities.Add(1)
			if replayed == visibility.MaxPendingReservations {
				invariantErr := fmt.Errorf(
					"%w: acquired more than %d reservations during one drain",
					ErrPendingOutboxNotDrained,
					visibility.MaxPendingReservations,
				)
				return time.Time{}, s.releaseAttempt(
					legacyAmbiguous.Sequence,
					attemptID,
					invariantErr,
				)
			}
			replayed++
			if _, err := s.writeReservation(ctx, legacyAmbiguous, attemptID, true); err != nil {
				return time.Time{}, err
			}
			s.reconciliationSuccesses.Add(1)
			continue
		}
		limits := s.writeGroupLimits
		limits.ForceSeal = forceSeal
		group, found, nextLingerDeadline, err := s.writeGroupVisibility.FormOrAcquireWriteGroup(
			ctx,
			attemptID,
			limits,
			s.clock().UTC(),
		)
		if err != nil {
			return time.Time{}, s.visibilityFailure("form or acquire ClickHouse write group", err)
		}
		if !found {
			if proveDrained {
				usage, usageErr := s.visibility.PendingUsage(ctx)
				if usageErr != nil {
					return time.Time{}, s.visibilityFailure("prove pending ClickHouse outbox is empty", usageErr)
				}
				if !pendingWriteGroupUsageEmpty(usage) {
					return time.Time{}, pendingWriteGroupError(usage)
				}
				return time.Time{}, nil
			}
			if err := s.pruneTerminal(ctx); err != nil {
				return time.Time{}, err
			}
			return nextLingerDeadline, nil
		}
		if group.NewlyFormed {
			s.noteFormedWriteGroup(group)
		} else {
			s.groupFillReasons.add(visibility.WriteGroupFillRecovery)
		}
		if group.State == visibility.WriteGroupAmbiguous {
			s.reconciliationAmbiguities.Add(1)
		}
		memberCount, countErr := safecast.Conv[uint32](len(group.Members))
		if countErr != nil || replayed > visibility.MaxPendingReservations-memberCount {
			invariantErr := fmt.Errorf(
				"%w: acquired more than %d reservations during one drain",
				ErrPendingOutboxNotDrained,
				visibility.MaxPendingReservations,
			)
			return time.Time{}, s.releaseWriteGroup(group.ID, attemptID, invariantErr)
		}
		replayed += memberCount
		if err := s.writeGroup(ctx, group, attemptID); err != nil {
			if proveDrained {
				return time.Time{}, err
			}
			if pruneErr := s.pruneTerminal(ctx); pruneErr != nil {
				return time.Time{}, errors.Join(err, pruneErr)
			}
			return time.Time{}, err
		}
		s.reconciliationSuccesses.Add(1)
	}
}

func (s *Store) writeGroup(
	ctx context.Context,
	group visibility.WriteGroup,
	attemptID string,
) error {
	if err := s.validateWriteGroup(group, attemptID); err != nil {
		return s.releaseWriteGroup(group.ID, attemptID, err)
	}
	if group.State == visibility.WriteGroupReady && len(group.Members) != 0 {
		s.groupSendLatency.observe(nonnegativeDuration(s.clock().UTC(), group.Members[0].Reservation.CreatedAt))
	}
	rows := make([][]any, 0, group.RowCount)
	for memberIndex, member := range group.Members {
		reservation := member.Reservation
		if uint64(len(reservation.Outbox)) != member.OutboxLength ||
			sha256.Sum256(reservation.Outbox) != member.OutboxSHA256 ||
			reservation.OutboxSHA256 != member.OutboxSHA256 {
			return s.releaseWriteGroup(group.ID, attemptID, fmt.Errorf(
				"ClickHouse write group member %d outbox digest does not match sealed membership",
				memberIndex,
			))
		}
		replayBatch, err := decodeStoreOutbox(reservation.Outbox)
		if err != nil {
			return s.releaseWriteGroup(group.ID, attemptID, fmt.Errorf(
				"decode durable ClickHouse write group member %d: %w",
				memberIndex,
				err,
			))
		}
		replayDigest, err := storePayloadDigest(replayBatch)
		if err != nil || replayDigest != reservation.PayloadSHA256 ||
			deduplicationToken(replayBatch) != reservation.BatchKey ||
			sequenceIdentityKey(replayBatch) != reservation.SequenceKey {
			return s.releaseWriteGroup(group.ID, attemptID, fmt.Errorf(
				"durable ClickHouse write group member %d identity does not match its reservation",
				memberIndex,
			))
		}
		memberRows, err := s.rowsForBatch(ctx, replayBatch, &reservation)
		if err != nil {
			return s.releaseWriteGroup(group.ID, attemptID, s.classifyError(fmt.Errorf(
				"rebuild ClickHouse write group member %d: %w",
				memberIndex,
				err,
			)))
		}
		actualRowCount, err := safecast.Conv[uint32](len(memberRows))
		if err != nil || actualRowCount != member.RowCount || actualRowCount != reservation.StoredRowCount ||
			decodedEventBytes(replayBatch) != member.DecodedBytes ||
			member.DecodedBytes != reservation.DecodedEventBytes {
			return s.releaseWriteGroup(group.ID, attemptID, fmt.Errorf(
				"ClickHouse write group member %d totals do not match its durable outbox",
				memberIndex,
			))
		}
		if err := applyReservation(memberRows, reservation); err != nil {
			return s.releaseWriteGroup(group.ID, attemptID, fmt.Errorf(
				"apply ClickHouse write group member %d visibility: %w",
				memberIndex,
				err,
			))
		}
		rows = append(rows, memberRows...)
	}
	if group.NewlyFormed {
		s.groupMonthlyPartitions.Add(distinctWriteGroupMonthlyPartitions(rows))
	}

	settings := insertSettings(group.ID)
	prepared, err := s.connection.prepare(ctx, s.insertSQL, settings)
	if err != nil {
		return s.releaseWriteGroup(group.ID, attemptID,
			s.classifyError(fmt.Errorf("prepare ClickHouse write group: %w", err)))
	}
	closed := false
	defer func() {
		if !closed {
			_ = prepared.Close()
		}
	}()
	for rowIndex, row := range rows {
		if err := prepared.Append(row...); err != nil {
			_ = prepared.Abort()
			return s.releaseWriteGroup(group.ID, attemptID,
				s.classifyError(fmt.Errorf("append ClickHouse write group row %d: %w", rowIndex, err)))
		}
	}
	if err := s.writeGroupVisibility.MarkWriteGroupSending(ctx, group.ID, attemptID); err != nil {
		_ = prepared.Abort()
		return s.releaseWriteGroup(group.ID, attemptID,
			s.finalizationFailure("mark ClickHouse write group sending", err))
	}
	s.physicalSends.Add(1)
	if err := prepared.Send(); err != nil {
		_ = prepared.Abort()
		return s.releaseWriteGroup(group.ID, attemptID,
			s.classifyError(fmt.Errorf("send ClickHouse write group: %w", err)))
	}
	committedAt := s.clock().UTC().Truncate(time.Microsecond)
	physicalCompleteAt := time.Now().UTC().Truncate(time.Microsecond)
	if physicalCompleteAt.After(committedAt) {
		committedAt = physicalCompleteAt
	}
	if group.SendingAt.After(committedAt) {
		committedAt = group.SendingAt.UTC().Truncate(time.Microsecond)
	}
	commitContext, cancelCommit := context.WithTimeout(context.Background(), visibilityFinalizeTimeout)
	err = s.writeGroupVisibility.CommitWriteGroup(commitContext, group.ID, attemptID, committedAt)
	cancelCommit()
	if err != nil {
		// The transaction result can be uncertain. Wake native callers so they
		// re-read SQLite and wake the reconciler to acquire the exact group again;
		// never try to rewrite terminal state through a best-effort release.
		sequences := writeGroupSequences(group)
		s.waiterWakeups.Add(uint64(s.commitWaiters.notify(sequences)))
		s.wakeReconciler()
		return s.finalizationFailure("commit ClickHouse write group", err)
	}
	s.successfulGroups.Add(1)
	if len(group.Members) != 0 {
		s.groupCommitLatency.observe(nonnegativeDuration(committedAt, group.Members[0].Reservation.CreatedAt))
	}
	closeErr := prepared.Close()
	closed = true
	sequences := writeGroupSequences(group)
	for range group.Members {
		s.noteTerminalReservation()
	}
	s.waiterWakeups.Add(uint64(s.commitWaiters.notify(sequences)))
	if closeErr != nil {
		return s.classifyError(fmt.Errorf("close committed ClickHouse write group: %w", closeErr))
	}
	return nil
}

func (s *Store) noteFormedWriteGroup(group visibility.WriteGroup) {
	s.formedGroups.Add(1)
	s.groupMemberBatches.Add(uint64(len(group.Members)))
	s.groupRows.Add(uint64(group.RowCount))
	s.groupDecodedBytes.Add(group.DecodedBytes)
	s.groupFillReasons.add(group.FillReason)
	if len(group.Members) != 0 {
		s.groupSealLatency.observe(nonnegativeDuration(group.CreatedAt, group.Members[0].Reservation.CreatedAt))
	}
}

func distinctWriteGroupMonthlyPartitions(rows [][]any) uint64 {
	partitions := make(map[[2]int]struct{})
	for _, row := range rows {
		if len(row) <= eventIndexTimeColumn {
			continue
		}
		value, ok := row[eventIndexTimeColumn].(time.Time)
		if !ok {
			continue
		}
		partitions[[2]int{value.Year(), int(value.Month())}] = struct{}{}
	}
	return uint64(len(partitions))
}

func writeGroupSequences(group visibility.WriteGroup) []uint64 {
	sequences := make([]uint64, len(group.Members))
	for index, member := range group.Members {
		sequences[index] = member.Reservation.Sequence
	}
	return sequences
}

func (s *Store) validateWriteGroup(group visibility.WriteGroup, attemptID string) error {
	if group.ID == "" || group.AttemptID != attemptID ||
		(group.State != visibility.WriteGroupReady && group.State != visibility.WriteGroupAmbiguous) ||
		len(group.Members) == 0 || len(group.Members) > int(s.writeGroupLimits.MaxMembers) ||
		group.RowCount == 0 || group.RowCount > s.writeGroupLimits.HardMaxRows ||
		group.DecodedBytes == 0 || group.DecodedBytes > s.writeGroupLimits.HardMaxDecodedBytes {
		return errors.New("acquired ClickHouse write group violates configured bounds")
	}
	var rows uint32
	var decodedBytes uint64
	for index, member := range group.Members {
		if member.Ordinal != uint32(index) || member.Reservation.Sequence == 0 ||
			(index > 0 && member.Reservation.Sequence <= group.Members[index-1].Reservation.Sequence) ||
			member.RowCount == 0 || member.RowCount > s.writeGroupLimits.HardMaxRows ||
			member.DecodedBytes == 0 || member.DecodedBytes > s.writeGroupLimits.HardMaxDecodedBytes ||
			rows > s.writeGroupLimits.HardMaxRows-member.RowCount ||
			decodedBytes > s.writeGroupLimits.HardMaxDecodedBytes-member.DecodedBytes {
			return fmt.Errorf("acquired ClickHouse write group member %d violates ordering or bounds", index)
		}
		rows += member.RowCount
		decodedBytes += member.DecodedBytes
	}
	membershipDigest, err := visibility.ComputeWriteGroupMembershipSHA256(group.Members)
	if err != nil {
		return fmt.Errorf("compute ClickHouse write group membership digest: %w", err)
	}
	if rows != group.RowCount || decodedBytes != group.DecodedBytes ||
		group.FirstSequence != group.Members[0].Reservation.Sequence ||
		group.LastSequence != group.Members[len(group.Members)-1].Reservation.Sequence ||
		membershipDigest != group.MembershipSHA256 {
		return errors.New("acquired ClickHouse write group membership digest or totals do not match")
	}
	return nil
}

func (s *Store) releaseWriteGroup(groupID, attemptID string, operationErr error) error {
	defer s.wakeReconciler()
	ctx, cancel := context.WithTimeout(context.Background(), visibilityFinalizeTimeout)
	defer cancel()
	if err := s.writeGroupVisibility.ReleaseWriteGroup(ctx, groupID, attemptID); err != nil {
		return errors.Join(operationErr, s.finalizationFailure("release ClickHouse write group attempt", err))
	}
	return operationErr
}

func pendingWriteGroupUsageEmpty(usage visibility.PendingUsage) bool {
	return usage.Reservations == 0 && usage.UngroupedReservations == 0 &&
		usage.ReadyGroups == 0 && usage.AmbiguousGroups == 0 &&
		usage.LiveGroupLeases == 0 && usage.OutboxBytes == 0 &&
		usage.MetadataBytes == 0
}

func pendingWriteGroupError(usage visibility.PendingUsage) error {
	return fmt.Errorf(
		"%w: reservations=%d ungrouped=%d ready_groups=%d ambiguous_groups=%d live_group_leases=%d outbox_bytes=%d metadata_bytes=%d",
		ErrPendingOutboxNotDrained,
		usage.Reservations,
		usage.UngroupedReservations,
		usage.ReadyGroups,
		usage.AmbiguousGroups,
		usage.LiveGroupLeases,
		usage.OutboxBytes,
		usage.MetadataBytes,
	)
}

func (s *Store) reconcilePending(ctx context.Context, proveDrained bool) error {
	if s.writeGroupVisibility != nil && s.coalescing {
		_, err := s.reconcileWriteGroups(ctx, true, proveDrained)
		return err
	}
	var replayed uint32
	for {
		attemptID, err := s.attemptID()
		if err != nil {
			return s.classifyError(fmt.Errorf("create ClickHouse reconciliation attempt: %w", err))
		}
		reservation, found, err := s.visibility.AcquirePending(ctx, attemptID)
		if err != nil {
			return s.visibilityFailure("acquire pending ClickHouse outbox", err)
		}
		if !found {
			if proveDrained {
				usage, usageErr := s.visibility.PendingUsage(ctx)
				if usageErr != nil {
					return s.visibilityFailure("prove pending ClickHouse outbox is empty", usageErr)
				}
				if usage.Reservations != 0 || usage.OutboxBytes != 0 {
					return pendingOutboxError(usage.Reservations, usage.OutboxBytes)
				}
				return nil
			}
			return s.pruneTerminal(ctx)
		}
		if reservation.MayHaveReachedStorage {
			s.reconciliationAmbiguities.Add(1)
		}
		if proveDrained && replayed == visibility.MaxPendingReservations {
			invariantErr := fmt.Errorf(
				"%w: acquired more than %d reservations during one exclusive drain",
				ErrPendingOutboxNotDrained,
				visibility.MaxPendingReservations,
			)
			return s.releaseAttempt(reservation.Sequence, attemptID, invariantErr)
		}
		if proveDrained {
			replayed++
		}
		if _, replayErr := s.writeReservation(ctx, reservation, attemptID, true); replayErr != nil {
			if proveDrained {
				return replayErr
			}
			// Rejected outcomes are SQLite-only and may keep arriving while a
			// persistent ClickHouse failure prevents this pending replay. Perform one
			// bounded prune attempt before returning so terminal metadata retention
			// continues to make progress. Preserve the replay failure as the primary
			// error if cleanup also fails.
			if pruneErr := s.pruneTerminal(ctx); pruneErr != nil {
				return errors.Join(replayErr, pruneErr)
			}
			return replayErr
		}
		s.reconciliationSuccesses.Add(1)
	}
}

func (s *Store) pruneTerminal(ctx context.Context) error {
	if pruner, ok := s.visibility.(hecTerminalPruner); ok {
		deleted, err := pruner.PruneHECTerminalRequests(
			ctx,
			s.clock().UTC().Add(-visibility.HECTerminalRetention),
			visibilityPruneBatch,
		)
		if err != nil {
			return s.visibilityFailure("prune terminal HEC requests", err)
		}
		if deleted == visibilityPruneBatch {
			s.wakeReconciler()
		}
	}
	deleted, err := s.visibility.PruneTerminal(ctx, visibility.TerminalRetention{
		Committed:             durableClickHouseIdempotencyWindow,
		Rejected:              durableBatchRejectWindow,
		RejectedMetadataBytes: durableBatchRejectMetadataBytes,
	}, visibilityPruneBatch)
	if err != nil {
		return s.visibilityFailure("prune terminal ClickHouse visibility records", err)
	}
	if deleted == visibilityPruneBatch {
		s.wakeReconciler()
	}
	return nil
}

func (s *Store) startReconciler() {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed || s.reconcileCancel != nil {
		return
	}
	s.coalescing = s.writeGroupVisibility != nil
	ctx, cancel := context.WithCancelCause(context.Background())
	s.reconcileCancel = cancel
	s.reconcileWake = make(chan struct{}, 1)
	s.reconcileDone = make(chan struct{})
	go s.runReconciler(ctx, s.reconcileWake, s.reconcileDone)
	s.reconcileWake <- struct{}{}
}

func (s *Store) runReconciler(ctx context.Context, wake <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	timer := time.NewTimer(s.retryAfter)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	resetTimer := func(delay time.Duration) <-chan time.Time {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(delay)
		return timer.C
	}
	var scheduled <-chan time.Time
	retrying := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
			if retrying {
				continue
			}
		case <-scheduled:
			scheduled = nil
			retrying = false
		}
		var nextLingerDeadline time.Time
		var err error
		if s.writeGroupVisibility == nil {
			err = s.ReconcilePending(ctx)
		} else {
			nextLingerDeadline, err = s.reconcileWriteGroupsFromWorker(ctx)
		}
		s.lifecycleMu.Lock()
		if !errors.Is(err, context.Canceled) {
			s.reconcileErr = err
		}
		s.lifecycleMu.Unlock()
		if err != nil && !errors.Is(err, context.Canceled) {
			s.reconciliationRetries.Add(1)
			scheduled = resetTimer(s.retryAfter)
			retrying = true
		} else if !nextLingerDeadline.IsZero() {
			delay := max(time.Duration(0), nextLingerDeadline.Sub(s.clock().UTC()))
			scheduled = resetTimer(delay)
		}
	}
}

func (s *Store) reconcileWriteGroupsFromWorker(ctx context.Context) (next time.Time, resultErr error) {
	operationContext, finishOperation, err := s.beginOperation(ctx, &resultErr)
	if err != nil {
		return time.Time{}, err
	}
	defer finishOperation()
	select {
	case <-operationContext.Done():
		return time.Time{}, operationContext.Err()
	case <-s.reconcileSlot:
	}
	defer func() { s.reconcileSlot <- struct{}{} }()
	if err := s.writeAdmission.enter(operationContext); err != nil {
		return time.Time{}, err
	}
	defer s.writeAdmission.leave()
	return s.reconcileWriteGroups(operationContext, false, false)
}

func (s *Store) wakeReconciler() {
	s.lifecycleMu.Lock()
	wake := s.reconcileWake
	s.lifecycleMu.Unlock()
	if wake != nil {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func storeBatchFromIdentity(identity ingest.StoreBatchIdentity) ingest.StoreBatch {
	return ingest.StoreBatch{
		TenantID:          identity.TenantID,
		Source:            identity.Source,
		CollectorID:       identity.CollectorID,
		BatchID:           identity.BatchID,
		BatchSequence:     identity.BatchSequence,
		SourceBatchSHA256: identity.SourceBatchSHA256,
	}
}

func resultForReservation(reservation visibility.Reservation, duplicate bool) (ingest.StoreResult, error) {
	if reservation.Rejected {
		if reservation.AlreadyCommitted {
			return ingest.StoreResult{}, errors.New("visibility reservation has conflicting terminal outcomes")
		}
		rejection, err := decodeBatchRejectionMetadata(reservation.Metadata)
		if err != nil {
			return ingest.StoreResult{}, fmt.Errorf("decode durable terminal batch rejection: %w", err)
		}
		return ingest.StoreResult{BatchRejection: rejection}, nil
	}
	metadata, err := decodeReservationMetadata(reservation.Metadata)
	if err != nil {
		return ingest.StoreResult{}, fmt.Errorf("decode durable ClickHouse batch outcome: %w", err)
	}

	storedCount := metadata.OriginalEventCount -
		safecast.MustConv[uint32](len(metadata.RejectedEvents))
	result := ingest.StoreResult{
		CommittedAt:        reservation.CommittedAt,
		OriginalEventCount: metadata.OriginalEventCount,
		RejectedEvents:     cloneEventRejections(metadata.RejectedEvents),
	}
	if duplicate {
		result.Duplicate = storedCount
	} else {
		result.Accepted = storedCount
	}
	return result, nil
}

// VisibilityCutoff captures the highest fully committed batch visible to a
// new search job. The sequencer allocates only above this monotonic boundary.
func (s *Store) VisibilityCutoff(ctx context.Context) (cutoff uint64, resultErr error) {
	operationContext, finishOperation, err := s.beginOperation(ctx, &resultErr)
	if err != nil {
		return 0, err
	}
	defer finishOperation()
	return s.visibilityCutoff(operationContext)
}

func (s *Store) visibilityCutoff(ctx context.Context) (uint64, error) {
	cutoff, err := s.visibility.Cutoff(ctx)
	if err != nil {
		return 0, s.visibilityFailure("read ClickHouse visibility cutoff", err)
	}
	return cutoff, nil
}

func (s *Store) releaseAttempt(sequence uint64, attemptID string, operationErr error) error {
	defer s.wakeReconciler()
	ctx, cancel := context.WithTimeout(context.Background(), visibilityFinalizeTimeout)
	defer cancel()
	if err := s.visibility.Release(ctx, sequence, attemptID); err != nil {
		return errors.Join(operationErr, s.finalizationFailure("release ClickHouse visibility attempt", err))
	}
	return operationErr
}

func (s *Store) finishPreSend(reservation visibility.Reservation, attemptID string, operationErr error) error {
	if reservation.PreviouslyReserved {
		return s.releaseAttempt(reservation.Sequence, attemptID, operationErr)
	}
	var transient *ingest.TransientStoreError
	if reservation.MayHaveReachedStorage || errors.As(operationErr, &transient) ||
		errors.Is(operationErr, context.Canceled) || errors.Is(operationErr, context.DeadlineExceeded) {
		return s.releaseAttempt(reservation.Sequence, attemptID, operationErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), visibilityFinalizeTimeout)
	defer cancel()
	if err := s.visibility.Abandon(ctx, reservation.Sequence, attemptID); err != nil {
		s.wakeReconciler()
		return errors.Join(operationErr, s.finalizationFailure("abandon unsent ClickHouse visibility sequence", err))
	}
	s.noteTerminalReservation()
	return operationErr
}

// finishMarkSendingFailure resolves the local durability transition itself.
// If MarkSending provably left the phase unsent, Abandon succeeds and prevents
// a dead client from wedging all later batches. If SQLite applied the update but
// the caller lost its result, Abandon fails closed and the ambiguous reservation
// is retained by releasing only its attempt lease.
func (s *Store) finishMarkSendingFailure(reservation visibility.Reservation, attemptID string, operationErr error) error {
	if reservation.PreviouslyReserved {
		return s.releaseAttempt(reservation.Sequence, attemptID, operationErr)
	}
	// An ambiguity barrier proves this reservation is still unsent. Preserve its
	// durable outbox and release only the attempt so the server reconciler can
	// replay the exact policy decision after the older send is resolved.
	if errors.Is(operationErr, visibility.ErrAmbiguousBarrier) {
		return s.releaseAttempt(reservation.Sequence, attemptID, operationErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), visibilityFinalizeTimeout)
	defer cancel()
	if err := s.visibility.Abandon(ctx, reservation.Sequence, attemptID); err == nil {
		s.noteTerminalReservation()
		return operationErr
	} else if !errors.Is(err, visibility.ErrAttemptLease) {
		s.wakeReconciler()
		return errors.Join(operationErr, s.finalizationFailure("resolve failed ClickHouse sending transition", err))
	}
	return s.releaseAttempt(reservation.Sequence, attemptID, operationErr)
}

func (s *Store) commitVisibility(sequence uint64, attemptID string, committedAt time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), visibilityFinalizeTimeout)
	defer cancel()
	if err := s.visibility.Commit(ctx, sequence, attemptID, committedAt); err != nil {
		return s.finalizationFailure("commit ClickHouse visibility sequence", err)
	}
	s.noteTerminalReservation()
	return nil
}

func (s *Store) noteTerminalReservation() {
	if s.terminalCount.Add(1)%visibilityPruneBatch == 0 {
		s.wakeReconciler()
	}
}

func (s *Store) noteTerminalRejection(metadataBytes uint64) {
	s.noteTerminalReservation()

	// Keep scheduling proportional to actual rejection pressure. The row-count
	// cadence alone could otherwise retain nearly 1 GiB of maximum-size metadata
	// before reaching its first 1,000-row maintenance wake. Modulo accounting
	// preserves concurrent increments while the single-slot wake channel safely
	// coalesces redundant maintenance requests.
	forceWake := metadataBytes >= durableBatchRejectPruneWakeBytes
	metadataBytes %= durableBatchRejectPruneWakeBytes
	for {
		current := s.rejectionWakeBytes.Load()
		next := current + metadataBytes
		crossed := next >= durableBatchRejectPruneWakeBytes
		if crossed {
			next -= durableBatchRejectPruneWakeBytes
		}
		if !s.rejectionWakeBytes.CompareAndSwap(current, next) {
			continue
		}
		if forceWake || crossed {
			s.wakeReconciler()
		}
		return
	}
}

func (s *Store) finalizationFailure(operation string, err error) error {
	return &ingest.TransientStoreError{
		Err:        fmt.Errorf("%s: %w", operation, err),
		Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE,
		RetryAfter: s.retryAfter,
	}
}

func (s *Store) visibilityFailure(operation string, err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, visibility.ErrReservationGone) {
		return &ingest.StoredBatchGoneError{Err: fmt.Errorf("%s: %w", operation, err)}
	}
	if errors.Is(err, visibility.ErrConflict) {
		return &ingest.DurableIdentityConflictError{Err: fmt.Errorf("%s: %w", operation, err)}
	}
	if quotaExceeded, ok := errors.AsType[*ingestquota.ExceededError](err); ok {
		var throttleReason opensplunk.ThrottleReason
		switch quotaExceeded.Scope.Kind {
		case ingestquota.ScopeKindToken:
			throttleReason = opensplunk.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA
		case ingestquota.ScopeKindIndex:
			throttleReason = opensplunk.ThrottleReason_THROTTLE_REASON_INDEX_QUOTA
		default:
			return fmt.Errorf("%s: quota denial has an invalid scope: %w", operation, err)
		}
		retryAfter := quotaExceeded.RetryAfter
		if retryAfter <= 0 {
			retryAfter = s.retryAfter
		}
		if retryAfter > ingestquota.MaximumRetryAfter {
			retryAfter = ingestquota.MaximumRetryAfter
		}
		return &ingest.TransientStoreError{
			Err:            fmt.Errorf("%s: %w", operation, err),
			Reason:         opensplunk.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED,
			ThrottleReason: throttleReason,
			RetryAfter:     retryAfter,
		}
	}
	if errors.Is(err, visibility.ErrInvalidArgument) || errors.Is(err, visibility.ErrExhausted) {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if errors.Is(err, visibility.ErrPendingCapacity) || errors.Is(err, visibility.ErrAttemptInProgress) ||
		errors.Is(err, visibility.ErrAmbiguousBarrier) {
		return &ingest.TransientStoreError{
			Err:        fmt.Errorf("%s: %w", operation, err),
			Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			RetryAfter: s.retryAfter,
		}
	}
	return &ingest.TransientStoreError{
		Err:        fmt.Errorf("%s: %w", operation, err),
		Reason:     opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE,
		RetryAfter: s.retryAfter,
	}
}

// Ping verifies network reachability and authentication.
func (s *Store) Ping(ctx context.Context) (resultErr error) {
	operationContext, finishOperation, err := s.beginOperation(ctx, &resultErr)
	if err != nil {
		return err
	}
	defer finishOperation()
	var pingErrors []error
	if err := s.connection.Ping(operationContext); err != nil {
		pingErrors = append(
			pingErrors,
			fmt.Errorf("ping ClickHouse: %w", err),
		)
	}
	if s.deletionConnection != nil {
		if err := s.deletionConnection.Ping(operationContext); err != nil {
			pingErrors = append(
				pingErrors,
				fmt.Errorf("ping ClickHouse deletion connection: %w", err),
			)
		}
	}
	s.lifecycleMu.Lock()
	reconcileErr := s.reconcileErr
	s.lifecycleMu.Unlock()
	if reconcileErr != nil {
		pingErrors = append(
			pingErrors,
			fmt.Errorf("reconcile ClickHouse outbox: %w", reconcileErr),
		)
	}
	return errors.Join(pingErrors...)
}

// HECReconciliationAvailable returns a constant-shape, non-writing readiness
// signal for the background outbox authority. It intentionally exposes no
// error text, address, sequence, tenant, or request identity.
func (s *Store) HECReconciliationAvailable() bool {
	if s == nil {
		return false
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return !s.closed && s.reconcileErr == nil
}

// HECReconciliationTelemetry returns detached bounded counters together with
// the same readiness bit used by the public HEC health projection.
func (s *Store) HECReconciliationTelemetry() HECReconciliationSnapshot {
	if s == nil {
		return HECReconciliationSnapshot{}
	}
	s.lifecycleMu.Lock()
	available := !s.closed && s.reconcileErr == nil
	s.lifecycleMu.Unlock()
	return HECReconciliationSnapshot{
		Available:                 available,
		Successes:                 s.reconciliationSuccesses.Load(),
		Retries:                   s.reconciliationRetries.Load(),
		Ambiguities:               s.reconciliationAmbiguities.Load(),
		StagedLogicalBatches:      s.stagedLogicalBatches.Load(),
		StagedLogicalRows:         s.stagedLogicalRows.Load(),
		FormedGroups:              s.formedGroups.Load(),
		PhysicalSends:             s.physicalSends.Load(),
		SuccessfulGroups:          s.successfulGroups.Load(),
		GroupMemberBatches:        s.groupMemberBatches.Load(),
		GroupRows:                 s.groupRows.Load(),
		GroupDecodedBytes:         s.groupDecodedBytes.Load(),
		GroupMonthlyPartitions:    s.groupMonthlyPartitions.Load(),
		FillRowTarget:             s.groupFillReasons.rowTarget.Load(),
		FillByteTarget:            s.groupFillReasons.byteTarget.Load(),
		FillHardBoundary:          s.groupFillReasons.hardBoundary.Load(),
		FillLinger:                s.groupFillReasons.linger.Load(),
		FillDrain:                 s.groupFillReasons.drain.Load(),
		FillRecovery:              s.groupFillReasons.recovery.Load(),
		NativeWaiters:             uint64(s.commitWaiters.size()),
		NativeWaiterWakeups:       s.waiterWakeups.Load(),
		NativeWaiterCancellations: s.waiterCancellations.Load(),
		NativeTerminalLookups:     s.waiterTerminalLookups.Load(),
		SealLatency:               s.groupSealLatency.snapshot(),
		SendLatency:               s.groupSendLatency.snapshot(),
		CommitLatency:             s.groupCommitLatency.snapshot(),
	}
}

// Close gives graceful shutdown the standard bounded drain budget.
func (s *Store) Close() error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		writeGroupShutdownDrainTimeout,
	)
	defer cancel()
	return s.CloseContext(ctx)
}

// CloseContext stops admission, gives accepted grouped work one force-seal and
// drain attempt within the caller's budget, then releases pooled ClickHouse
// connections within that same budget. A timeout or drain failure is returned
// but does not discard durable ready or ambiguous work.
func (s *Store) CloseContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("close ClickHouse store context is required")
	}
	s.closeOnce.Do(func() {
		s.closeErr = s.closeContext(ctx)
	})
	return s.closeErr
}

func (s *Store) closeContext(ctx context.Context) error {
	s.lifecycleMu.Lock()
	s.closed = true
	lifecycleCancel := s.lifecycleCancel
	writeAdmission := s.writeAdmission
	cancel := s.reconcileCancel
	done := s.reconcileDone
	s.lifecycleMu.Unlock()
	var closeErrors []error

	// Stop the ordinary worker before acquiring the exclusive admission
	// lease. Otherwise it could retain the reconciliation slot while queued
	// behind this close-time freeze.
	if cancel != nil {
		cancel(context.Canceled)
		select {
		case <-done:
		case <-ctx.Done():
			closeErrors = append(closeErrors, fmt.Errorf("join ClickHouse write-group reconciler: %w", ctx.Err()))
		}
	}

	if ctx.Err() == nil && writeAdmission != nil && s.writeGroupVisibility != nil && s.coalescing {
		if err := writeAdmission.freeze(ctx); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("freeze ClickHouse writes for shutdown drain: %w", err))
		} else {
			_, drainErr := s.reconcileWriteGroups(ctx, true, true)
			writeAdmission.releaseFreeze()
			if drainErr != nil {
				closeErrors = append(closeErrors, fmt.Errorf("drain ClickHouse write groups during shutdown: %w", drainErr))
			}
		}
	}

	if writeAdmission != nil {
		writeAdmission.close()
	}
	if lifecycleCancel != nil {
		lifecycleCancel(context.Canceled)
	}
	if err := waitForStoreOperations(ctx, &s.operations); err != nil {
		closeErrors = append(closeErrors, err)
	}
	if s.deletionConnection != nil {
		if err := closeStoreConnection(ctx, "ClickHouse deletion connection", s.deletionConnection); err != nil {
			closeErrors = append(
				closeErrors,
				err,
			)
		}
	}
	if err := closeStoreConnection(ctx, "ClickHouse", s.connection); err != nil {
		closeErrors = append(
			closeErrors,
			err,
		)
	}
	return errors.Join(closeErrors...)
}

func waitForStoreOperations(ctx context.Context, operations *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		operations.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("join ClickHouse operations: %w", ctx.Err())
	}
}

func closeStoreConnection(ctx context.Context, label string, connection storeConnection) error {
	done := make(chan error, 1)
	go func() {
		done <- connection.Close()
	}()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("close %s: %w", label, err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("close %s: %w", label, ctx.Err())
	}
}

func (s *Store) rowsForBatch(ctx context.Context, batch ingest.StoreBatch, prior *visibility.Reservation) ([][]any, error) {
	source, err := ingest.CanonicalIngestionSource(batch.Source, batch.CollectorID)
	if err != nil {
		return nil, fmt.Errorf("store ClickHouse batch: invalid ingestion source: %w", err)
	}
	if batch.TenantID == "" || batch.BatchID == "" {
		return nil, errors.New("store ClickHouse batch: tenant, ingestion source, and batch IDs are required")
	}
	if batch.BatchSequence == 0 {
		return nil, errors.New("store ClickHouse batch: batch sequence must be positive")
	}
	if batch.ReceivedAt.IsZero() {
		return nil, errors.New("store ClickHouse batch: received time is required")
	}
	if len(batch.Events) == 0 {
		return nil, errors.New("store ClickHouse batch: at least one event is required")
	}
	if uint64(len(batch.Events)) > math.MaxUint32 {
		return nil, errors.New("store ClickHouse batch: event count exceeds result range")
	}

	admittedRetention := batch.RetentionByIndex != nil
	if admittedRetention {
		if len(batch.RetentionByIndex) > len(batch.Events) {
			return nil, errors.New("retention policy snapshot contains more indexes than accepted events")
		}
		if len(batch.RetentionByIndex) > int(ingest.HardMaxBatchEvents) {
			return nil, fmt.Errorf(
				"retention policy snapshot index count exceeds hard batch event limit %d",
				ingest.HardMaxBatchEvents,
			)
		}
	}

	retentionByIndex := make(map[string]time.Duration)
	if admittedRetention {
		retentionByIndex = make(map[string]time.Duration, len(batch.RetentionByIndex))
		maps.Copy(retentionByIndex, batch.RetentionByIndex)
	}
	metadataVersion := reservationMetadataVersion
	if prior != nil {
		metadata, err := decodeReservationMetadata(prior.Metadata)
		if err != nil {
			return nil, fmt.Errorf("decode persisted retention policy: %w", err)
		}
		retentionByIndex = metadata.RetentionByIndex
		metadataVersion = metadata.Version
	}
	rows := make([][]any, 0, len(batch.Events))
	var acceptedIndexes map[string]struct{}
	if admittedRetention {
		acceptedIndexes = make(map[string]struct{}, len(retentionByIndex))
	}
	for i, stored := range batch.Events {
		if stored == nil || stored.Event == nil {
			return nil, fmt.Errorf("store ClickHouse batch: event %d is nil", i)
		}
		storedSource, sourceErr := ingest.CanonicalIngestionSource(stored.Source, stored.CollectorID)
		if sourceErr != nil {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has invalid ingestion source: %w", i, sourceErr)
		}
		if stored.TenantID != batch.TenantID || storedSource != source || stored.BatchID != batch.BatchID {
			return nil, fmt.Errorf("store ClickHouse batch: event %d server metadata does not match its batch", i)
		}
		event := stored.Event
		if event.GetEventId() == "" || event.GetIndexName() == "" {
			return nil, fmt.Errorf("store ClickHouse batch: event %d identity is incomplete", i)
		}
		if event.GetEventTime() == nil || event.GetEventTime().CheckValid() != nil {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has invalid event_time", i)
		}
		if event.GetCollectedAt() != nil && event.GetCollectedAt().CheckValid() != nil {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has invalid collected_at", i)
		}
		if stored.IndexTime.IsZero() {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has no index time", i)
		}
		if !stored.IndexTime.Equal(batch.ReceivedAt) {
			return nil, fmt.Errorf("store ClickHouse batch: event %d index time does not match its batch", i)
		}
		if event.GetEventTimeSource() < opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED ||
			event.GetEventTimeSource() > opensplunk.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has invalid event_time_source", i)
		}
		if event.GetSeverity() < opensplunk.LogSeverity_LOG_SEVERITY_UNSPECIFIED ||
			event.GetSeverity() > opensplunk.LogSeverity_LOG_SEVERITY_FATAL {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has invalid severity", i)
		}
		if event.GetRawEncoding() != opensplunk.RawEncoding_RAW_ENCODING_UTF8 &&
			event.GetRawEncoding() != opensplunk.RawEncoding_RAW_ENCODING_BINARY {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has invalid raw_encoding", i)
		}

		period, ok := retentionByIndex[event.GetIndexName()]
		if !ok && (prior != nil || admittedRetention) {
			return nil, fmt.Errorf("retention policy snapshot has no index %q", event.GetIndexName())
		}
		if !ok {
			var retentionErr error
			period, retentionErr = s.retention.RetentionForIndex(ctx, batch.TenantID, event.GetIndexName())
			if retentionErr != nil {
				return nil, fmt.Errorf("resolve retention for index %q: %w", event.GetIndexName(), retentionErr)
			}
			retentionByIndex[event.GetIndexName()] = period
		}
		if admittedRetention {
			acceptedIndexes[event.GetIndexName()] = struct{}{}
		}

		fields, fieldNames, fieldTypes, conversionErr := convertTypedObject(event.GetFields())
		if conversionErr != nil {
			return nil, fmt.Errorf("convert fields for event %d (%q): %w", i, event.GetEventId(), conversionErr)
		}
		indexTime := eventStoreMillis(stored.IndexTime)
		expiresAt, expirationErr := eventExpirationForMetadata(indexTime, period, metadataVersion)
		if expirationErr != nil {
			return nil, fmt.Errorf("resolve retention for event %d: %w", i, expirationErr)
		}
		var collectedAt any
		if event.GetCollectedAt() != nil {
			collectedAt = event.GetCollectedAt().AsTime().UTC()
		}
		rows = append(rows, []any{
			event.GetEventId(),
			batch.TenantID,
			event.GetIndexName(),
			event.GetEventTime().AsTime().UTC(),
			indexTime,
			collectedAt,

			safecast.MustConv[uint8](event.GetEventTimeSource()),
			event.GetHost(),
			event.GetSource(),
			event.GetSourcetype(),
			cloneOptionalString(event.Service),

			safecast.MustConv[uint8](event.GetSeverity()),
			cloneOptionalString(event.Level),
			cloneOptionalString(event.Message),
			slices.Clone(event.GetRaw()),

			safecast.MustConv[uint8](event.GetRawEncoding()),
			cloneOptionalString(event.TraceId),
			cloneOptionalString(event.SpanId),
			fields,
			fieldNames,
			batch.CollectorID,
			uint8(source.Kind),
			source.ID,
			batch.BatchID,
			batch.BatchSequence,
			expiresAt,
			uint64(0), // Filled under the visibility commit lock immediately before insert.
			fieldTypes,
			eventfields.CurrentFieldMetadataVersion,
		})
	}
	if admittedRetention && len(acceptedIndexes) != len(retentionByIndex) {
		return nil, errors.New("retention policy snapshot contains an unaccepted index")
	}
	return rows, nil
}

func convertTypedObject(object *opensplunk.TypedObject) (*clickhousedriver.JSON, []string, []uint8, error) {
	document := clickhousedriver.NewJSON()
	if object == nil {
		return document, []string{}, []uint8{}, nil
	}
	fieldTypes := make(map[string]eventfields.StoredValueType)
	physicalPaths := make(map[string]string)
	if err := flattenTypedObject(document, object, nil, nil, fieldTypes, physicalPaths); err != nil {
		return nil, nil, nil, err
	}
	names := make([]string, 0, len(fieldTypes))
	for name := range fieldTypes {
		names = append(names, name)
	}
	slices.Sort(names)
	if _, err := eventfields.ParseStoredFieldNames(names); err != nil {
		return nil, nil, nil, fmt.Errorf("stored field-name metadata: %w", err)
	}
	types := make([]uint8, len(names))
	for index, name := range names {
		types[index] = uint8(fieldTypes[name])
	}
	return document, names, types, nil
}

func flattenTypedObject(
	document *clickhousedriver.JSON,
	object *opensplunk.TypedObject,
	logicalPrefix, physicalPrefix []string,
	fieldTypes map[string]eventfields.StoredValueType,
	physicalPaths map[string]string,
) error {
	if object == nil {
		return errors.New("typed object is nil")
	}
	seen := make(map[string]struct{}, len(object.GetFields()))
	for i, field := range object.GetFields() {
		if field == nil {
			return fmt.Errorf("typed object field %d is nil", i)
		}
		if err := validateStorageFieldName(field.GetName()); err != nil {
			return fmt.Errorf("typed object field %d: %w", i, err)
		}
		if len(logicalPrefix) == 0 && eventfields.IsReservedDynamicRoot(field.GetName()) {
			return fmt.Errorf("typed object root field %q is reserved event metadata", field.GetName())
		}
		if _, duplicate := seen[field.GetName()]; duplicate {
			return fmt.Errorf("typed object field %q is duplicated", field.GetName())
		}
		seen[field.GetName()] = struct{}{}
		if field.GetValue() == nil || field.GetValue().GetKind() == nil {
			return fmt.Errorf("typed object field %q has no value kind", field.GetName())
		}

		logicalPath := appendPath(logicalPrefix, field.GetName())
		physicalPath := appendPath(physicalPrefix, eventfields.EncodePhysicalPathSegment(field.GetName()))
		if nested, ok := field.GetValue().GetKind().(*opensplunk.TypedValue_ObjectValue); ok {
			if nested.ObjectValue == nil {
				return fmt.Errorf("typed object field %q has a nil object", field.GetName())
			}
			if len(nested.ObjectValue.GetFields()) != 0 {
				if err := flattenTypedObject(document, nested.ObjectValue, logicalPath, physicalPath, fieldTypes, physicalPaths); err != nil {
					return err
				}
				continue
			}
		}

		value, err := typedValueToNative(field.GetValue())
		if err != nil {
			return fmt.Errorf("typed object field %q: %w", field.GetName(), err)
		}
		dynamic, err := nativeDynamic(value)
		if err != nil {
			return fmt.Errorf("typed object field %q: %w", field.GetName(), err)
		}
		logicalName := eventfields.NormalizeDynamicPath(logicalPath)
		physicalName := strings.Join(physicalPath, ".")
		if prior, collision := physicalPaths[physicalName]; collision && prior != logicalName {
			return fmt.Errorf("typed fields %q and %q collide in ClickHouse JSON path %q", prior, logicalName, physicalName)
		}
		physicalPaths[physicalName] = logicalName
		// Always force the protobuf-declared scalar type. Without a Dynamic
		// wrapper the driver's per-path type reuse can coerce a later integral
		// Float64 into an existing Int64 subcolumn, destroying type intent.
		document.SetValueAtPath(physicalName, dynamic)
		fieldType, err := storedValueType(field.GetValue())
		if err != nil {
			return fmt.Errorf("typed object field %q: %w", field.GetName(), err)
		}
		fieldTypes[logicalName] = fieldType
	}
	return nil
}

func storedValueType(value *opensplunk.TypedValue) (eventfields.StoredValueType, error) {
	if value == nil || value.GetKind() == nil {
		return 0, errors.New("typed value kind is required")
	}
	switch value.GetKind().(type) {
	case *opensplunk.TypedValue_NullValue:
		return eventfields.StoredValueTypeNull, nil
	case *opensplunk.TypedValue_StringValue:
		return eventfields.StoredValueTypeString, nil
	case *opensplunk.TypedValue_Sint64Value:
		return eventfields.StoredValueTypeSint64, nil
	case *opensplunk.TypedValue_Uint64Value:
		return eventfields.StoredValueTypeUint64, nil
	case *opensplunk.TypedValue_DoubleValue:
		return eventfields.StoredValueTypeDouble, nil
	case *opensplunk.TypedValue_BoolValue:
		return eventfields.StoredValueTypeBool, nil
	case *opensplunk.TypedValue_BytesValue:
		return eventfields.StoredValueTypeBytes, nil
	case *opensplunk.TypedValue_TimestampValue:
		return eventfields.StoredValueTypeTimestamp, nil
	case *opensplunk.TypedValue_DurationValue:
		return eventfields.StoredValueTypeDuration, nil
	case *opensplunk.TypedValue_ListValue:
		return eventfields.StoredValueTypeList, nil
	case *opensplunk.TypedValue_ObjectValue:
		return eventfields.StoredValueTypeObject, nil
	case *opensplunk.TypedValue_DecimalValue:
		return eventfields.StoredValueTypeDecimal, nil
	default:
		return 0, errors.New("typed value kind is unsupported")
	}
}

func typedValueToNative(value *opensplunk.TypedValue) (any, error) {
	if value == nil || value.GetKind() == nil {
		return nil, errors.New("typed value kind is required")
	}
	switch kind := value.GetKind().(type) {
	case *opensplunk.TypedValue_NullValue:
		if kind.NullValue != opensplunk.NullValue_NULL_VALUE_NULL {
			return nil, errors.New("typed null value is invalid")
		}
		return nil, nil
	case *opensplunk.TypedValue_StringValue:
		if !utf8.ValidString(kind.StringValue) {
			return nil, errors.New("typed string is not valid UTF-8")
		}
		return kind.StringValue, nil
	case *opensplunk.TypedValue_Sint64Value:
		return kind.Sint64Value, nil
	case *opensplunk.TypedValue_Uint64Value:
		return kind.Uint64Value, nil
	case *opensplunk.TypedValue_DoubleValue:
		if math.IsNaN(kind.DoubleValue) || math.IsInf(kind.DoubleValue, 0) {
			return nil, errors.New("typed double must be finite")
		}
		return kind.DoubleValue, nil
	case *opensplunk.TypedValue_BoolValue:
		return kind.BoolValue, nil
	case *opensplunk.TypedValue_BytesValue:
		return extendedValue("bytes/v1", base64.RawStdEncoding.EncodeToString(kind.BytesValue)), nil
	case *opensplunk.TypedValue_TimestampValue:
		if kind.TimestampValue == nil || kind.TimestampValue.CheckValid() != nil {
			return nil, errors.New("typed timestamp is invalid")
		}
		return extendedValue("timestamp/v1", kind.TimestampValue.AsTime().UTC().Format(time.RFC3339Nano)), nil
	case *opensplunk.TypedValue_DurationValue:
		if !ingest.DurationFitsResultRange(kind.DurationValue) {
			return nil, errors.New("typed duration is invalid")
		}
		encoded := strconv.FormatInt(kind.DurationValue.GetSeconds(), 10) + ":" + strconv.FormatInt(int64(kind.DurationValue.GetNanos()), 10)
		return extendedValue("duration/v1", encoded), nil
	case *opensplunk.TypedValue_ListValue:
		if kind.ListValue == nil {
			return nil, errors.New("typed list is nil")
		}
		items := make([]clickhousedriver.Dynamic, 0, len(kind.ListValue.GetValues()))
		for i, item := range kind.ListValue.GetValues() {
			native, err := typedValueToNative(item)
			if err != nil {
				return nil, fmt.Errorf("typed list item %d: %w", i, err)
			}
			dynamic, err := nativeDynamic(native)
			if err != nil {
				return nil, fmt.Errorf("typed list item %d: %w", i, err)
			}
			items = append(items, dynamic)
		}
		return clickhousedriver.NewDynamicWithType(items, "Array(Dynamic)"), nil
	case *opensplunk.TypedValue_ObjectValue:
		if kind.ObjectValue == nil {
			return nil, errors.New("typed object is nil")
		}
		object, err := typedObjectToDynamicMap(kind.ObjectValue)
		if err != nil {
			return nil, err
		}
		return clickhousedriver.NewDynamicWithType(object, "Map(String, Dynamic)"), nil
	case *opensplunk.TypedValue_DecimalValue:
		if kind.DecimalValue == nil || !decimalValuePattern.MatchString(kind.DecimalValue.GetValue()) {
			return nil, errors.New("typed decimal is invalid")
		}
		return extendedValue("decimal/v1", kind.DecimalValue.GetValue()), nil
	case *opensplunk.TypedValue_MissingValue:
		return nil, errors.New("missing typed value cannot be stored")
	default:
		return nil, fmt.Errorf("unsupported typed value kind %T", kind)
	}
}

func typedObjectToDynamicMap(object *opensplunk.TypedObject) (map[string]clickhousedriver.Dynamic, error) {
	result := make(map[string]clickhousedriver.Dynamic, len(object.GetFields()))
	for i, field := range object.GetFields() {
		if field == nil {
			return nil, fmt.Errorf("typed object field %d is nil", i)
		}
		if err := validateStorageFieldName(field.GetName()); err != nil {
			return nil, fmt.Errorf("typed object field %d: %w", i, err)
		}
		if _, duplicate := result[field.GetName()]; duplicate {
			return nil, fmt.Errorf("typed object field %q is duplicated", field.GetName())
		}
		native, err := typedValueToNative(field.GetValue())
		if err != nil {
			return nil, fmt.Errorf("typed object field %q: %w", field.GetName(), err)
		}
		dynamic, err := nativeDynamic(native)
		if err != nil {
			return nil, fmt.Errorf("typed object field %q: %w", field.GetName(), err)
		}
		result[field.GetName()] = dynamic
	}
	return result, nil
}

func nativeDynamic(value any) (clickhousedriver.Dynamic, error) {
	switch value := value.(type) {
	case nil:
		return clickhousedriver.NewDynamic(nil), nil
	case clickhousedriver.Dynamic:
		return value, nil
	case string:
		return clickhousedriver.NewDynamicWithType(value, "String"), nil
	case int64:
		return clickhousedriver.NewDynamicWithType(value, "Int64"), nil
	case uint64:
		return clickhousedriver.NewDynamicWithType(value, "UInt64"), nil
	case float64:
		return clickhousedriver.NewDynamicWithType(value, "Float64"), nil
	case bool:
		return clickhousedriver.NewDynamicWithType(value, "Bool"), nil
	default:
		return clickhousedriver.Dynamic{}, fmt.Errorf("cannot represent %T as ClickHouse Dynamic", value)
	}
}

func extendedValue(kind, value string) clickhousedriver.Dynamic {
	return clickhousedriver.NewDynamicWithType(map[string]string{
		extendedTypeKey:  kind,
		extendedValueKey: value,
	}, "Map(String, String)")
}

func validateStorageFieldName(name string) error {
	if name == "" || strings.TrimSpace(name) != name {
		return errors.New("field name is empty or has surrounding whitespace")
	}
	if !utf8.ValidString(name) {
		return errors.New("field name is not valid UTF-8")
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return errors.New("field name contains a control character")
		}
	}
	return nil
}

func appendPath(prefix []string, segment string) []string {
	path := make([]string, len(prefix)+1)
	copy(path, prefix)
	path[len(prefix)] = segment
	return path
}

func eventStoreMillis(value time.Time) time.Time {
	return value.UTC().Truncate(time.Millisecond)
}

func eventExpiration(indexTime time.Time, retention time.Duration) (time.Time, error) {
	indexTime = eventStoreMillis(indexTime)
	if err := indexpolicy.ValidateRetentionAt(retention, indexTime, false); err != nil {
		return time.Time{}, err
	}
	return indexTime.Add(retention), nil
}

func eventExpirationForMetadata(
	indexTime time.Time,
	retention time.Duration,
	metadataVersion byte,
) (time.Time, error) {
	if metadataVersion == reservationMetadataVersion {
		return eventExpiration(indexTime, retention)
	}
	return time.Time{}, errors.New("unsupported retention metadata version; provision fresh ingestion state")
}

func cloneOptionalString(value *string) any {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func insertSettings(token string) clickhousedriver.Settings {
	return clickhousedriver.Settings{
		"async_insert":                                                           uint8(0),
		"wait_for_async_insert":                                                  uint8(1),
		"insert_deduplication_token":                                             token,
		"json_type_escape_dots_in_keys":                                          uint8(1),
		"input_format_json_read_numbers_as_strings":                              uint8(0),
		"input_format_json_read_bools_as_numbers":                                uint8(0),
		"input_format_json_read_bools_as_strings":                                uint8(0),
		"input_format_json_infer_array_of_dynamic_from_array_of_different_types": uint8(1),
		"input_format_try_infer_dates":                                           uint8(0),
		"input_format_try_infer_datetimes":                                       uint8(0),
	}
}

func randomAttemptID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}

func deduplicationToken(batch ingest.StoreBatch) string {
	source, err := ingest.CanonicalIngestionSource(batch.Source, batch.CollectorID)
	if err == nil && source.Kind == ingest.IngestionSourceKindHEC {
		hash := sha256.New()
		writeTokenPart(hash, "open-splunk-hec-request")
		writeTokenPart(hash, "1")
		writeTokenPart(hash, batch.TenantID)
		writeTokenPart(hash, source.ID)
		writeTokenPart(hash, batch.BatchID)
		return "open-splunk-ingest-hec-v1-" + hex.EncodeToString(hash.Sum(nil))
	}
	hash := sha256.New()
	writeTokenPart(hash, "open-splunk-collector-protocol")
	writeTokenPart(hash, "1")
	writeTokenPart(hash, batch.TenantID)
	writeTokenPart(hash, batch.CollectorID)
	writeTokenPart(hash, batch.BatchID)
	return "open-splunk-ingest-v1-" + hex.EncodeToString(hash.Sum(nil))
}

func sequenceIdentityKey(batch ingest.StoreBatch) string {
	source, err := ingest.CanonicalIngestionSource(batch.Source, batch.CollectorID)
	if err == nil && source.Kind == ingest.IngestionSourceKindHEC {
		hash := sha256.New()
		writeTokenPart(hash, "open-splunk-hec-request-sequence")
		writeTokenPart(hash, "1")
		writeTokenPart(hash, batch.TenantID)
		writeTokenPart(hash, source.ID)
		// HEC has no client-authored monotonic batch sequence. The server's
		// random request ID is the stable identity for one staged request;
		// using the adapter's placeholder BatchSequence would make every
		// independent request from one token collide in the visibility ledger.
		writeTokenPart(hash, batch.BatchID)
		return "open-splunk-sequence-hec-v1-" + hex.EncodeToString(hash.Sum(nil))
	}
	hash := sha256.New()
	writeTokenPart(hash, "open-splunk-collector-sequence")
	writeTokenPart(hash, "1")
	writeTokenPart(hash, batch.TenantID)
	writeTokenPart(hash, batch.CollectorID)
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], batch.BatchSequence)
	_, _ = hash.Write(number[:])
	return "open-splunk-sequence-v1-" + hex.EncodeToString(hash.Sum(nil))
}

type byteWriter interface {
	Write([]byte) (int, error)
}

func writeTokenPart(writer byteWriter, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write([]byte(value))
}

func buildEventsInsertSQL(database, table string) string {
	columns := make([]string, len(eventInsertColumns))
	for i, column := range eventInsertColumns {
		columns[i] = quoteIdentifier(column)
	}
	return "INSERT INTO " + quoteIdentifier(database) + "." + quoteIdentifier(table) + " (" + strings.Join(columns, ", ") + ")"
}

func storePayloadDigest(batch ingest.StoreBatch) ([sha256.Size]byte, error) {
	source, err := ingest.CanonicalIngestionSource(batch.Source, batch.CollectorID)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("store ClickHouse batch: invalid ingestion source: %w", err)
	}
	if batch.TenantID == "" || batch.BatchID == "" || batch.BatchSequence == 0 ||
		batch.SourceBatchSHA256 == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, errors.New("store ClickHouse batch: complete source identity is required")
	}
	hash := sha256.New()
	if source.Kind == ingest.IngestionSourceKindHEC {
		_, _ = hash.Write([]byte("open-splunk-store-payload-v2\x00"))
		writeTokenPart(hash, batch.TenantID)
		_, _ = hash.Write([]byte{byte(source.Kind)})
		writeTokenPart(hash, source.ID)
		writeTokenPart(hash, batch.BatchID)
		var number [8]byte
		binary.BigEndian.PutUint64(number[:], batch.BatchSequence)
		_, _ = hash.Write(number[:])
		_, _ = hash.Write(batch.SourceBatchSHA256[:])
		var digest [sha256.Size]byte
		copy(digest[:], hash.Sum(nil))
		return digest, nil
	}
	_, _ = hash.Write([]byte("open-splunk-store-payload-v1\x00"))
	writeTokenPart(hash, batch.TenantID)
	writeTokenPart(hash, batch.CollectorID)
	writeTokenPart(hash, batch.BatchID)
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], batch.BatchSequence)
	_, _ = hash.Write(number[:])
	_, _ = hash.Write(batch.SourceBatchSHA256[:])
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

type reservationMetadata struct {
	Version            byte
	RetentionByIndex   map[string]time.Duration
	BatchSequence      uint64
	OriginalEventCount uint32
	RejectedEvents     []*opensplunk.EventRejection
}

func encodeReservationMetadata(rows [][]any, batch ingest.StoreBatch) ([]byte, error) {
	retentionByIndex := make(map[string]time.Duration)
	for rowIndex, row := range rows {
		if len(row) != len(eventInsertColumns) {
			return nil, fmt.Errorf("store ClickHouse batch: row %d has an invalid storage shape", rowIndex)
		}
		index, indexOK := row[eventIndexNameColumn].(string)
		indexTime, timeOK := row[eventIndexTimeColumn].(time.Time)
		expiresAt, expiryOK := row[eventExpiresAtColumn].(time.Time)
		if !indexOK || !timeOK || !expiryOK || index == "" || len(index) > 255 || !utf8.ValidString(index) {
			return nil, fmt.Errorf("store ClickHouse batch: row %d has invalid retention metadata", rowIndex)
		}
		retention := expiresAt.Sub(indexTime)
		canonicalExpiresAt, retentionErr := eventExpiration(indexTime, retention)
		if retentionErr != nil || !canonicalExpiresAt.Equal(expiresAt) {
			return nil, fmt.Errorf("store ClickHouse batch: row %d has invalid retention duration", rowIndex)
		}
		if previous, exists := retentionByIndex[index]; exists && previous != retention {
			return nil, fmt.Errorf("store ClickHouse batch: index %q resolved inconsistent retention", index)
		}
		retentionByIndex[index] = retention
	}
	indexes := make([]string, 0, len(retentionByIndex))
	for index := range retentionByIndex {
		indexes = append(indexes, index)
	}
	if len(indexes) > maxDurableBatchEvents {
		return nil, errors.New("store ClickHouse batch: unique index count exceeds visibility ledger limit")
	}
	slices.Sort(indexes)
	if batch.OriginalEventCount == 0 ||
		uint64(len(rows))+uint64(len(batch.RejectedEvents)) != uint64(batch.OriginalEventCount) {
		return nil, errors.New("store ClickHouse batch: source event disposition is inconsistent")
	}
	seenRejections := make(map[uint32]struct{}, len(batch.RejectedEvents))
	for index, rejection := range batch.RejectedEvents {
		if rejection == nil || rejection.GetEventIndex() >= batch.OriginalEventCount {
			return nil, fmt.Errorf("store ClickHouse batch: rejection %d has an invalid source index", index)
		}
		if _, duplicate := seenRejections[rejection.GetEventIndex()]; duplicate {
			return nil, fmt.Errorf("store ClickHouse batch: source event %d has duplicate rejections", rejection.GetEventIndex())
		}
		seenRejections[rejection.GetEventIndex()] = struct{}{}
	}
	var metadata bytes.Buffer
	_, _ = metadata.Write([]byte{'O', 'S', 'V', 'M', reservationMetadataVersion})
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(len(indexes)))
	_, _ = metadata.Write(number[:])
	for _, index := range indexes {
		binary.BigEndian.PutUint64(number[:], uint64(len(index)))
		_, _ = metadata.Write(number[:])
		_, _ = metadata.WriteString(index)

		binary.BigEndian.PutUint64(
			number[:],
			safecast.MustConv[uint64](retentionByIndex[index]),
		)
		_, _ = metadata.Write(number[:])
	}
	binary.BigEndian.PutUint64(number[:], batch.BatchSequence)
	_, _ = metadata.Write(number[:])
	var short [4]byte
	binary.BigEndian.PutUint32(short[:], batch.OriginalEventCount)
	_, _ = metadata.Write(short[:])

	binary.BigEndian.PutUint32(
		short[:],
		safecast.MustConv[uint32](len(batch.RejectedEvents)),
	)
	_, _ = metadata.Write(short[:])
	marshal := proto.MarshalOptions{Deterministic: true}
	for index, rejection := range batch.RejectedEvents {
		encoded, err := marshal.Marshal(rejection)
		if err != nil {
			return nil, fmt.Errorf("store ClickHouse batch: encode rejection %d: %w", index, err)
		}
		binary.BigEndian.PutUint64(number[:], uint64(len(encoded)))
		_, _ = metadata.Write(number[:])
		_, _ = metadata.Write(encoded)
		if metadata.Len()+sha256.Size > visibility.MaxMetadataBytes {
			return nil, errors.New("store ClickHouse batch: outcome metadata exceeds visibility ledger limit")
		}
	}
	if metadata.Len()+sha256.Size > visibility.MaxMetadataBytes {
		return nil, errors.New("store ClickHouse batch: outcome metadata exceeds visibility ledger limit")
	}
	checksum := sha256.Sum256(metadata.Bytes())
	_, _ = metadata.Write(checksum[:])
	return metadata.Bytes(), nil
}

func decodeReservationMetadata(metadata []byte) (reservationMetadata, error) {
	if len(metadata) > visibility.MaxMetadataBytes || len(metadata) < 5+sha256.Size {
		return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid size")
	}
	payload := metadata[:len(metadata)-sha256.Size]
	var storedChecksum [sha256.Size]byte
	copy(storedChecksum[:], metadata[len(payload):])
	if sha256.Sum256(payload) != storedChecksum {
		return reservationMetadata{}, errors.New("visibility reservation metadata checksum mismatch")
	}
	reader := bytes.NewReader(payload)
	header := make([]byte, 5)
	if _, err := io.ReadFull(reader, header); err != nil ||
		!bytes.Equal(header[:4], []byte{'O', 'S', 'V', 'M'}) ||
		header[4] != reservationMetadataVersion {
		return reservationMetadata{}, errors.New("visibility reservation metadata has an unsupported version; provision fresh ingestion state")
	}
	readUint64 := func() (uint64, error) {
		var number [8]byte
		if _, err := io.ReadFull(reader, number[:]); err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint64(number[:]), nil
	}
	count, err := readUint64()
	if err != nil || count > maxDurableBatchEvents {
		return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid index count")
	}
	retentionByIndex := make(map[string]time.Duration, count)
	for range count {
		length, err := readUint64()

		if err != nil || length == 0 || length > 255 ||
			length > safecast.MustConv[uint64](reader.Len()) {
			return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid index name")
		}
		name := make([]byte, safecast.MustConv[int](length))
		if _, err := io.ReadFull(reader, name); err != nil {
			return reservationMetadata{}, errors.New("visibility reservation metadata is truncated")
		}
		duration, err := readUint64()
		if err != nil || duration == 0 || duration > math.MaxInt64 ||
			time.Duration(safecast.MustConv[int64](duration))%time.Millisecond != 0 {
			return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid retention duration")
		}
		index := string(name)
		if !utf8.ValidString(index) {
			return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid index name")
		}
		if _, duplicate := retentionByIndex[index]; duplicate {
			return reservationMetadata{}, errors.New("visibility reservation metadata contains a duplicate index")
		}
		retentionByIndex[index] = time.Duration(safecast.MustConv[int64](duration))
	}
	batchSequence, err := readUint64()
	if err != nil || batchSequence == 0 {
		return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid batch sequence")
	}
	var short [4]byte
	if _, err := io.ReadFull(reader, short[:]); err != nil {
		return reservationMetadata{}, errors.New("visibility reservation metadata has no source event count")
	}
	originalEventCount := binary.BigEndian.Uint32(short[:])
	if originalEventCount == 0 || originalEventCount > maxDurableBatchEvents {
		return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid source event count")
	}
	if _, err := io.ReadFull(reader, short[:]); err != nil {
		return reservationMetadata{}, errors.New("visibility reservation metadata has no rejection count")
	}
	rejectionCount := binary.BigEndian.Uint32(short[:])

	if rejectionCount > originalEventCount ||
		uint64(rejectionCount) > safecast.MustConv[uint64](reader.Len())/9 {
		return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid rejection count")
	}
	rejections := make([]*opensplunk.EventRejection, 0, rejectionCount)
	seenRejections := make(map[uint32]struct{}, rejectionCount)
	for index := range rejectionCount {
		length, err := readUint64()

		if err != nil || length == 0 ||
			length > safecast.MustConv[uint64](reader.Len()) ||
			length > safecast.MustConv[uint64](math.MaxInt) {
			return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid rejection payload")
		}
		encoded := make([]byte, safecast.MustConv[int](length))
		if _, err := io.ReadFull(reader, encoded); err != nil {
			return reservationMetadata{}, errors.New("visibility reservation metadata is truncated")
		}
		rejection := new(opensplunk.EventRejection)
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, rejection); err != nil ||
			rejection.GetEventIndex() >= originalEventCount {
			return reservationMetadata{}, fmt.Errorf("visibility reservation metadata rejection %d is invalid", index)
		}
		if _, duplicate := seenRejections[rejection.GetEventIndex()]; duplicate {
			return reservationMetadata{}, errors.New("visibility reservation metadata has duplicate rejection indexes")
		}
		seenRejections[rejection.GetEventIndex()] = struct{}{}
		rejections = append(rejections, rejection)
	}
	if reader.Len() != 0 {
		return reservationMetadata{}, errors.New("visibility reservation metadata has trailing bytes")
	}
	return reservationMetadata{
		Version:            reservationMetadataVersion,
		RetentionByIndex:   retentionByIndex,
		BatchSequence:      batchSequence,
		OriginalEventCount: originalEventCount,
		RejectedEvents:     rejections,
	}, nil
}

func applyReservation(rows [][]any, reservation visibility.Reservation) error {
	if reservation.Sequence == 0 || reservation.IndexTime.IsZero() {
		return errors.New("visibility reservation is incomplete")
	}
	metadata, err := decodeReservationMetadata(reservation.Metadata)
	if err != nil {
		return err
	}
	indexTime := eventStoreMillis(reservation.IndexTime)
	for rowIndex, row := range rows {
		index, ok := row[eventIndexNameColumn].(string)
		if !ok {
			return fmt.Errorf("store ClickHouse batch: row %d has an invalid index", rowIndex)
		}
		retention, exists := metadata.RetentionByIndex[index]
		if !exists {
			return fmt.Errorf("visibility reservation has no retention for index %q", index)
		}
		expiresAt, expirationErr := eventExpirationForMetadata(indexTime, retention, metadata.Version)
		if expirationErr != nil {
			return fmt.Errorf("visibility reservation retention for index %q: %w", index, expirationErr)
		}
		row[eventIndexTimeColumn] = indexTime
		row[eventExpiresAtColumn] = expiresAt
		row[eventVisibilitySequenceColumn] = reservation.Sequence
	}
	return nil
}

func (s *Store) classifyError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if _, ok := errors.AsType[*ingest.TransientStoreError](err); ok {
		return err
	}
	reason, transient := transientStoreReason(err)
	if !transient {
		return err
	}
	return &ingest.TransientStoreError{Err: err, Reason: reason, RetryAfter: s.retryAfter}
}

func transientStoreReason(err error) (opensplunk.RetryBatchReason, bool) {
	if errors.Is(err, clickhousedriver.ErrAcquireConnTimeout) {
		return opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY, true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, clickhousedriver.ErrConnectionClosed) ||
		errors.Is(err, sqldriver.ErrBadConn) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ETIMEDOUT) {
		return opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE, true
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE, true
	}
	var operationError *clickhousedriver.OpError
	if errors.As(err, &operationError) && operationError.Err != nil {
		if reason, ok := transientStoreReason(operationError.Err); ok {
			return reason, true
		}
	}
	var exception *clickhousedriver.Exception
	if !errors.As(err, &exception) {
		return opensplunk.RetryBatchReason_RETRY_BATCH_REASON_UNSPECIFIED, false
	}
	switch exception.Code {
	case 364:
		return opensplunk.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED, true
	case 202, 203, 241, 252, 745:
		return opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY, true
	case 95, 96, 159, 209, 210, 225, 242, 243, 279, 285, 286, 319, 341, 999:
		return opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE, true
	default:
		return opensplunk.RetryBatchReason_RETRY_BATCH_REASON_UNSPECIFIED, false
	}
}

type writeBatch interface {
	Append(...any) error
	Send() error
	Abort() error
	Close() error
}

type storeConnection interface {
	prepare(context.Context, string, clickhousedriver.Settings) (writeBatch, error)
	exec(
		context.Context,
		string,
		clickhousedriver.Settings,
		clickhousedriver.Parameters,
		string,
	) error
	queryRow(
		context.Context,
		string,
		clickhousedriver.Parameters,
	) storeQueryRow
	Ping(context.Context) error
	Close() error
}

type storeQueryRow interface {
	Scan(...any) error
}

type nativeStoreConnection struct {
	connection driver.Conn
}

func (c *nativeStoreConnection) prepare(ctx context.Context, query string, settings clickhousedriver.Settings) (writeBatch, error) {
	ctx = clickhousedriver.Context(ctx, clickhousedriver.WithSettings(settings))
	return c.connection.PrepareBatch(ctx, query)
}

func (c *nativeStoreConnection) exec(
	ctx context.Context,
	query string,
	settings clickhousedriver.Settings,
	parameters clickhousedriver.Parameters,
	queryID string,
) error {
	options := []clickhousedriver.QueryOption{
		clickhousedriver.WithSettings(settings),
		clickhousedriver.WithParameters(parameters),
	}
	if queryID != "" {
		options = append(options, clickhousedriver.WithQueryID(queryID))
	}
	return c.connection.Exec(clickhousedriver.Context(ctx, options...), query)
}

func (c *nativeStoreConnection) queryRow(
	ctx context.Context,
	query string,
	parameters clickhousedriver.Parameters,
) storeQueryRow {
	ctx = clickhousedriver.Context(
		ctx,
		clickhousedriver.WithParameters(parameters),
	)
	return c.connection.QueryRow(ctx, query)
}

func (c *nativeStoreConnection) Ping(ctx context.Context) error {
	return c.connection.Ping(ctx)
}

func (c *nativeStoreConnection) Close() error {
	return c.connection.Close()
}

func (config Config) clickHouseOptions() (*clickhousedriver.Options, Config, error) {
	defaults := DefaultConfig()
	if len(config.Addresses) == 0 {
		config.Addresses = slices.Clone(defaults.Addresses)
	} else {
		config.Addresses = slices.Clone(config.Addresses)
	}
	if len(config.Addresses) != 1 {
		return nil, Config{}, errors.New("exactly one ClickHouse address is required in single-node mode")
	}
	if config.Database == "" {
		config.Database = defaults.Database
	}
	if config.Table == "" {
		config.Table = defaults.Table
	}
	if config.Username == "" {
		config.Username = defaults.Username
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = defaults.DialTimeout
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = defaults.ReadTimeout
	}
	if config.MaxOpenConns == 0 {
		config.MaxOpenConns = defaults.MaxOpenConns
	}
	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = defaults.MaxIdleConns
	}
	if config.ConnMaxLifetime == 0 {
		config.ConnMaxLifetime = defaults.ConnMaxLifetime
	}
	if config.RetryAfter == 0 {
		config.RetryAfter = defaults.RetryAfter
	}
	if !physicalIdentifier.MatchString(config.Database) || !physicalIdentifier.MatchString(config.Table) {
		return nil, Config{}, errors.New("invalid ClickHouse database or table identifier")
	}
	if strings.IndexFunc(config.Username, unicode.IsControl) >= 0 {
		return nil, Config{}, errors.New("invalid ClickHouse username")
	}
	if config.DialTimeout <= 0 || config.ReadTimeout <= 0 || config.ConnMaxLifetime <= 0 || config.RetryAfter <= 0 {
		return nil, Config{}, errors.New("ClickHouse connection durations must be positive")
	}
	if config.MaxOpenConns <= 0 || config.MaxIdleConns < 0 || config.MaxIdleConns > config.MaxOpenConns {
		return nil, Config{}, errors.New("invalid ClickHouse connection pool limits")
	}
	for i, address := range config.Addresses {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return nil, Config{}, fmt.Errorf("invalid ClickHouse address at position %d", i)
		}
		portNumber, err := strconv.ParseUint(port, 10, 16)
		if err != nil || portNumber == 0 {
			return nil, Config{}, fmt.Errorf("invalid ClickHouse address at position %d", i)
		}
	}
	var tlsConfig *tls.Config
	if config.TLS != nil {
		tlsConfig = config.TLS.Clone()
	}
	return &clickhousedriver.Options{
		Protocol:         clickhousedriver.Native,
		Addr:             slices.Clone(config.Addresses),
		Auth:             clickhousedriver.Auth{Database: config.Database, Username: config.Username, Password: config.Password},
		TLS:              tlsConfig,
		DialTimeout:      config.DialTimeout,
		ReadTimeout:      config.ReadTimeout,
		MaxOpenConns:     config.MaxOpenConns,
		MaxIdleConns:     config.MaxIdleConns,
		ConnMaxLifetime:  config.ConnMaxLifetime,
		Compression:      &clickhousedriver.Compression{Method: clickhousedriver.CompressionLZ4},
		ConnOpenStrategy: clickhousedriver.ConnOpenRoundRobin,
	}, config, nil
}
