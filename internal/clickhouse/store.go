package clickhouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
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

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/protobuf/proto"
)

const (
	defaultDatabase           = "open_splunk"
	defaultTable              = "events"
	visibilityFinalizeTimeout = 10 * time.Second
	durableIdempotencyWindow  = 10_000
	visibilityPruneBatch      = 1_000

	extendedTypeKey  = "\x00open_splunk_type"
	extendedValueKey = "\x00open_splunk_value"
)

var (
	decimalValuePattern = regexp.MustCompile("^-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][+-]?(?:0|[1-9][0-9]*))?$")
	eventInsertColumns  = []string{
		"event_id", "tenant_id", "index_name", "event_time", "index_time",
		"collected_at", "event_time_source", "host", "source", "sourcetype",
		"service", "severity", "level", "body", "raw", "raw_encoding",
		"trace_id", "span_id", "fields", "field_names", "collector_id",
		"batch_id", "batch_sequence", "expires_at", "visibility_seq",
	}
	eventsInsertSQL = buildEventsInsertSQL(defaultDatabase, defaultTable)
)

// RetentionProvider resolves the authorized retention policy for one logical
// index. A Store resolves it once per unique index in a batch; callers should
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

// NewStore wraps an existing clickhouse-go connection.
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

// Store implements ingest.EventStore using one synchronous native insert per
// accepted protocol batch.
type Store struct {
	connection      storeConnection
	insertSQL       string
	retention       RetentionProvider
	visibility      visibility.Sequencer
	attemptID       func() (string, error)
	clock           func() time.Time
	retryAfter      time.Duration
	reconcileSlot   chan struct{}
	reconcileWG     sync.WaitGroup
	lifecycleMu     sync.Mutex
	reconcileWake   chan struct{}
	reconcileDone   chan struct{}
	reconcileCancel context.CancelFunc
	reconcileErr    error
	closed          bool
	closeOnce       sync.Once
	closeErr        error
	terminalCount   atomic.Uint64
}

var _ ingest.EventStore = (*Store)(nil)
var _ ingest.RecoverableEventStore = (*Store)(nil)

func newStore(
	connection storeConnection,
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
	return &Store{
		connection:    connection,
		insertSQL:     buildEventsInsertSQL(database, table),
		retention:     retention,
		visibility:    sequencer,
		attemptID:     randomAttemptID,
		clock:         clock,
		retryAfter:    retryAfter,
		reconcileSlot: reconcileSlot,
	}, nil
}

// LookupBatch finds the durable disposition of an exact collector wire batch
// before mutable authorization and validation policy is applied again.
func (s *Store) LookupBatch(ctx context.Context, identity ingest.StoreBatchIdentity) (ingest.StoredBatchState, ingest.StoreResult, error) {
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
	if prior.AlreadyCommitted {
		result, resultErr := resultForReservation(prior, true)
		if resultErr != nil {
			return ingest.StoredBatchNotFound, ingest.StoreResult{}, resultErr
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

func (s *Store) store(ctx context.Context, batch ingest.StoreBatch, resumeOnly bool) (ingest.StoreResult, error) {
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
	if found && prior.AlreadyCommitted {
		return resultForReservation(prior, true)
	}
	if resumeOnly && !found {
		return ingest.StoreResult{}, errors.New("resume ClickHouse batch: durable reservation not found")
	}

	metadata := prior.Metadata
	outbox := prior.Outbox
	indexTime := prior.IndexTime
	if !found {
		if batch.OriginalEventCount == 0 && len(batch.RejectedEvents) == 0 && len(batch.Events) > 0 {
			batch.OriginalEventCount = uint32(len(batch.Events))
		}
		rows, rowsErr := s.rowsForBatch(ctx, batch, nil)
		if rowsErr != nil {
			return ingest.StoreResult{}, s.classifyError(rowsErr)
		}
		metadata, err = encodeReservationMetadata(rows, batch)
		if err != nil {
			return ingest.StoreResult{}, err
		}
		outbox, err = encodeStoreOutbox(batch)
		if err != nil {
			return ingest.StoreResult{}, err
		}
		indexTime = batch.ReceivedAt
	}
	attemptID, err := s.attemptID()
	if err != nil {
		return ingest.StoreResult{}, s.classifyError(fmt.Errorf("create ClickHouse visibility attempt: %w", err))
	}
	reservation, err := s.visibility.Reserve(ctx, visibility.ReserveRequest{
		BatchKey:      deduplicationKey,
		SequenceKey:   sequenceKey,
		AttemptID:     attemptID,
		IndexTime:     indexTime,
		PayloadSHA256: payloadDigest,
		Metadata:      metadata,
		Outbox:        outbox,
	})
	if err != nil {
		// A failed SQLite commit can be outcome-ambiguous. Wake the server-owned
		// reconciler so any reservation that did persist is not dependent on a
		// collector retry or process restart.
		s.wakeReconciler()
		return ingest.StoreResult{}, s.visibilityFailure("reserve ClickHouse visibility sequence", err)
	}
	if reservation.AlreadyCommitted {
		return resultForReservation(reservation, true)
	}
	return s.writeReservation(ctx, reservation, attemptID, found || reservation.PreviouslyReserved || resumeOnly)
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
// bearer-token dependency. It is safe to call concurrently; the SQLite attempt
// lease and this process-local context-aware slot ensure one replay owns each
// reservation. A canceled background reconciler never waits behind a caller
// that is performing a manual drain.
func (s *Store) ReconcilePending(ctx context.Context) error {
	if ctx == nil {
		return errors.New("reconcile ClickHouse outbox: context is required")
	}
	s.lifecycleMu.Lock()
	if s.closed {
		s.lifecycleMu.Unlock()
		return errors.New("reconcile ClickHouse outbox: store is closed")
	}
	s.reconcileWG.Add(1)
	s.lifecycleMu.Unlock()
	defer s.reconcileWG.Done()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.reconcileSlot:
	}
	defer func() { s.reconcileSlot <- struct{}{} }()
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
			deleted, pruneErr := s.visibility.PruneTerminal(ctx, durableIdempotencyWindow, visibilityPruneBatch)
			if pruneErr != nil {
				return s.visibilityFailure("prune terminal ClickHouse visibility records", pruneErr)
			}
			if deleted == visibilityPruneBatch {
				s.wakeReconciler()
			}
			return nil
		}
		if _, err := s.writeReservation(ctx, reservation, attemptID, true); err != nil {
			return err
		}
	}
}

func (s *Store) startReconciler() {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed || s.reconcileCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
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
	var retry <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
			if retry != nil {
				continue
			}
		case <-retry:
			retry = nil
		}
		err := s.ReconcilePending(ctx)
		s.lifecycleMu.Lock()
		if !errors.Is(err, context.Canceled) {
			s.reconcileErr = err
		}
		s.lifecycleMu.Unlock()
		if err != nil && !errors.Is(err, context.Canceled) {
			timer.Reset(s.retryAfter)
			retry = timer.C
		}
	}
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
		CollectorID:       identity.CollectorID,
		BatchID:           identity.BatchID,
		BatchSequence:     identity.BatchSequence,
		SourceBatchSHA256: identity.SourceBatchSHA256,
	}
}

func resultForReservation(reservation visibility.Reservation, duplicate bool) (ingest.StoreResult, error) {
	metadata, err := decodeReservationMetadata(reservation.Metadata)
	if err != nil {
		return ingest.StoreResult{}, fmt.Errorf("decode durable ClickHouse batch outcome: %w", err)
	}
	storedCount := metadata.OriginalEventCount - uint32(len(metadata.RejectedEvents))
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
func (s *Store) VisibilityCutoff(ctx context.Context) (uint64, error) {
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

func (s *Store) finalizationFailure(operation string, err error) error {
	return &ingest.TransientStoreError{
		Err:        fmt.Errorf("%s: %w", operation, err),
		Reason:     opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE,
		RetryAfter: s.retryAfter,
	}
}

func (s *Store) visibilityFailure(operation string, err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, visibility.ErrConflict) {
		return &ingest.DurableIdentityConflictError{Err: fmt.Errorf("%s: %w", operation, err)}
	}
	if errors.Is(err, visibility.ErrInvalidArgument) || errors.Is(err, visibility.ErrExhausted) {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if errors.Is(err, visibility.ErrPendingCapacity) || errors.Is(err, visibility.ErrAttemptInProgress) ||
		errors.Is(err, visibility.ErrAmbiguousBarrier) {
		return &ingest.TransientStoreError{
			Err:        fmt.Errorf("%s: %w", operation, err),
			Reason:     opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
			RetryAfter: s.retryAfter,
		}
	}
	return &ingest.TransientStoreError{
		Err:        fmt.Errorf("%s: %w", operation, err),
		Reason:     opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE,
		RetryAfter: s.retryAfter,
	}
}

// Ping verifies network reachability and authentication.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.connection.Ping(ctx); err != nil {
		return fmt.Errorf("ping ClickHouse: %w", err)
	}
	s.lifecycleMu.Lock()
	reconcileErr := s.reconcileErr
	s.lifecycleMu.Unlock()
	if reconcileErr != nil {
		return fmt.Errorf("reconcile ClickHouse outbox: %w", reconcileErr)
	}
	return nil
}

// Close releases all pooled ClickHouse connections.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.lifecycleMu.Lock()
		s.closed = true
		cancel := s.reconcileCancel
		done := s.reconcileDone
		s.lifecycleMu.Unlock()
		if cancel != nil {
			cancel()
			<-done
		}
		s.reconcileWG.Wait()
		if err := s.connection.Close(); err != nil {
			s.closeErr = fmt.Errorf("close ClickHouse: %w", err)
		}
	})
	return s.closeErr
}

func (s *Store) rowsForBatch(ctx context.Context, batch ingest.StoreBatch, prior *visibility.Reservation) ([][]any, error) {
	if batch.TenantID == "" || batch.CollectorID == "" || batch.BatchID == "" {
		return nil, errors.New("store ClickHouse batch: tenant, collector, and batch IDs are required")
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

	retentionByIndex := make(map[string]time.Duration)
	if prior != nil {
		metadata, err := decodeReservationMetadata(prior.Metadata)
		if err != nil {
			return nil, fmt.Errorf("decode persisted retention policy: %w", err)
		}
		retentionByIndex = metadata.RetentionByIndex
	}
	rows := make([][]any, 0, len(batch.Events))
	for i, stored := range batch.Events {
		if stored == nil || stored.Event == nil {
			return nil, fmt.Errorf("store ClickHouse batch: event %d is nil", i)
		}
		if stored.TenantID != batch.TenantID || stored.CollectorID != batch.CollectorID || stored.BatchID != batch.BatchID {
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
		if event.GetEventTimeSource() < opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED ||
			event.GetEventTimeSource() > opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_RECEIVED_AT_FALLBACK {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has invalid event_time_source", i)
		}
		if event.GetSeverity() < opensplunkv1.LogSeverity_LOG_SEVERITY_UNSPECIFIED ||
			event.GetSeverity() > opensplunkv1.LogSeverity_LOG_SEVERITY_FATAL {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has invalid severity", i)
		}
		if event.GetRawEncoding() != opensplunkv1.RawEncoding_RAW_ENCODING_UTF8 &&
			event.GetRawEncoding() != opensplunkv1.RawEncoding_RAW_ENCODING_BINARY {
			return nil, fmt.Errorf("store ClickHouse batch: event %d has invalid raw_encoding", i)
		}

		period, ok := retentionByIndex[event.GetIndexName()]
		if !ok && prior != nil {
			return nil, fmt.Errorf("persisted retention policy has no index %q", event.GetIndexName())
		}
		if !ok {
			var retentionErr error
			period, retentionErr = s.retention.RetentionForIndex(ctx, batch.TenantID, event.GetIndexName())
			if retentionErr != nil {
				return nil, fmt.Errorf("resolve retention for index %q: %w", event.GetIndexName(), retentionErr)
			}
			if period <= 0 {
				return nil, fmt.Errorf("resolve retention for index %q: duration must be positive", event.GetIndexName())
			}
			retentionByIndex[event.GetIndexName()] = period
		}

		fields, fieldNames, conversionErr := convertTypedObject(event.GetFields())
		if conversionErr != nil {
			return nil, fmt.Errorf("convert fields for event %d (%q): %w", i, event.GetEventId(), conversionErr)
		}
		indexTime := eventStoreMillis(stored.IndexTime)
		expiresAt := indexTime.Add(period)
		if !expiresAt.After(indexTime) {
			return nil, fmt.Errorf("resolve retention for event %d: expiration overflow", i)
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
			uint8(event.GetEventTimeSource()),
			event.GetHost(),
			event.GetSource(),
			event.GetSourcetype(),
			cloneOptionalString(event.Service),
			uint8(event.GetSeverity()),
			cloneOptionalString(event.Level),
			cloneOptionalString(event.Message),
			slices.Clone(event.GetRaw()),
			uint8(event.GetRawEncoding()),
			cloneOptionalString(event.TraceId),
			cloneOptionalString(event.SpanId),
			fields,
			fieldNames,
			batch.CollectorID,
			batch.BatchID,
			batch.BatchSequence,
			expiresAt,
			uint64(0), // Filled under the visibility commit lock immediately before insert.
		})
	}
	return rows, nil
}

func convertTypedObject(object *opensplunkv1.TypedObject) (*clickhousedriver.JSON, []string, error) {
	document := clickhousedriver.NewJSON()
	if object == nil {
		return document, []string{}, nil
	}
	fieldNames := make(map[string]struct{})
	physicalPaths := make(map[string]string)
	if err := flattenTypedObject(document, object, nil, nil, fieldNames, physicalPaths); err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(fieldNames))
	for name := range fieldNames {
		names = append(names, name)
	}
	slices.Sort(names)
	return document, names, nil
}

func flattenTypedObject(
	document *clickhousedriver.JSON,
	object *opensplunkv1.TypedObject,
	logicalPrefix, physicalPrefix []string,
	fieldNames map[string]struct{},
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
		physicalPath := appendPath(physicalPrefix, encodePhysicalPathSegment(field.GetName()))
		if nested, ok := field.GetValue().GetKind().(*opensplunkv1.TypedValue_ObjectValue); ok {
			if nested.ObjectValue == nil {
				return fmt.Errorf("typed object field %q has a nil object", field.GetName())
			}
			if len(nested.ObjectValue.GetFields()) != 0 {
				if err := flattenTypedObject(document, nested.ObjectValue, logicalPath, physicalPath, fieldNames, physicalPaths); err != nil {
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
		logicalName := normalizedDynamicPath(logicalPath)
		physicalName := strings.Join(physicalPath, ".")
		if prior, collision := physicalPaths[physicalName]; collision && prior != logicalName {
			return fmt.Errorf("typed fields %q and %q collide in ClickHouse JSON path %q", prior, logicalName, physicalName)
		}
		physicalPaths[physicalName] = logicalName
		// Always force the protobuf-declared scalar type. Without a Dynamic
		// wrapper the driver's per-path type reuse can coerce a later integral
		// Float64 into an existing Int64 subcolumn, destroying type intent.
		document.SetValueAtPath(physicalName, dynamic)
		fieldNames[logicalName] = struct{}{}
	}
	return nil
}

func typedValueToNative(value *opensplunkv1.TypedValue) (any, error) {
	if value == nil || value.GetKind() == nil {
		return nil, errors.New("typed value kind is required")
	}
	switch kind := value.GetKind().(type) {
	case *opensplunkv1.TypedValue_NullValue:
		if kind.NullValue != opensplunkv1.NullValue_NULL_VALUE_NULL {
			return nil, errors.New("typed null value is invalid")
		}
		return nil, nil
	case *opensplunkv1.TypedValue_StringValue:
		if !utf8.ValidString(kind.StringValue) {
			return nil, errors.New("typed string is not valid UTF-8")
		}
		return kind.StringValue, nil
	case *opensplunkv1.TypedValue_Sint64Value:
		return kind.Sint64Value, nil
	case *opensplunkv1.TypedValue_Uint64Value:
		return kind.Uint64Value, nil
	case *opensplunkv1.TypedValue_DoubleValue:
		if math.IsNaN(kind.DoubleValue) || math.IsInf(kind.DoubleValue, 0) {
			return nil, errors.New("typed double must be finite")
		}
		return kind.DoubleValue, nil
	case *opensplunkv1.TypedValue_BoolValue:
		return kind.BoolValue, nil
	case *opensplunkv1.TypedValue_BytesValue:
		return extendedValue("bytes/v1", base64.RawStdEncoding.EncodeToString(kind.BytesValue)), nil
	case *opensplunkv1.TypedValue_TimestampValue:
		if kind.TimestampValue == nil || kind.TimestampValue.CheckValid() != nil {
			return nil, errors.New("typed timestamp is invalid")
		}
		return extendedValue("timestamp/v1", kind.TimestampValue.AsTime().UTC().Format(time.RFC3339Nano)), nil
	case *opensplunkv1.TypedValue_DurationValue:
		if !ingest.DurationFitsResultRange(kind.DurationValue) {
			return nil, errors.New("typed duration is invalid")
		}
		encoded := strconv.FormatInt(kind.DurationValue.GetSeconds(), 10) + ":" + strconv.FormatInt(int64(kind.DurationValue.GetNanos()), 10)
		return extendedValue("duration/v1", encoded), nil
	case *opensplunkv1.TypedValue_ListValue:
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
	case *opensplunkv1.TypedValue_ObjectValue:
		if kind.ObjectValue == nil {
			return nil, errors.New("typed object is nil")
		}
		object, err := typedObjectToDynamicMap(kind.ObjectValue)
		if err != nil {
			return nil, err
		}
		return clickhousedriver.NewDynamicWithType(object, "Map(String, Dynamic)"), nil
	case *opensplunkv1.TypedValue_DecimalValue:
		if kind.DecimalValue == nil || !decimalValuePattern.MatchString(kind.DecimalValue.GetValue()) {
			return nil, errors.New("typed decimal is invalid")
		}
		return extendedValue("decimal/v1", kind.DecimalValue.GetValue()), nil
	case *opensplunkv1.TypedValue_MissingValue:
		return nil, errors.New("missing typed value cannot be stored")
	default:
		return nil, fmt.Errorf("unsupported typed value kind %T", kind)
	}
}

func typedObjectToDynamicMap(object *opensplunkv1.TypedObject) (map[string]clickhousedriver.Dynamic, error) {
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

func encodePhysicalPathSegment(segment string) string {
	// Keep this transform synchronized with dynamic-path reads in the compiler.
	// ClickHouse reserves %2E for literal dots when
	// json_type_escape_dots_in_keys=1; escaping percent first makes the mapping
	// injective for user keys which already contain escape-looking text.
	segment = strings.ReplaceAll(segment, "%", "%25")
	return strings.ReplaceAll(segment, ".", "%2E")
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
	hash := sha256.New()
	writeTokenPart(hash, "open-splunk-collector-protocol")
	writeTokenPart(hash, "1")
	writeTokenPart(hash, batch.TenantID)
	writeTokenPart(hash, batch.CollectorID)
	writeTokenPart(hash, batch.BatchID)
	return "open-splunk-ingest-v1-" + hex.EncodeToString(hash.Sum(nil))
}

func sequenceIdentityKey(batch ingest.StoreBatch) string {
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
	if batch.TenantID == "" || batch.CollectorID == "" || batch.BatchID == "" || batch.BatchSequence == 0 ||
		batch.SourceBatchSHA256 == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, errors.New("store ClickHouse batch: complete source identity is required")
	}
	hash := sha256.New()
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
	RetentionByIndex   map[string]time.Duration
	BatchSequence      uint64
	OriginalEventCount uint32
	RejectedEvents     []*opensplunkv1.EventRejection
}

func encodeReservationMetadata(rows [][]any, batch ingest.StoreBatch) ([]byte, error) {
	retentionByIndex := make(map[string]time.Duration)
	for rowIndex, row := range rows {
		if len(row) != len(eventInsertColumns) {
			return nil, fmt.Errorf("store ClickHouse batch: row %d has an invalid storage shape", rowIndex)
		}
		index, indexOK := row[2].(string)
		indexTime, timeOK := row[4].(time.Time)
		expiresAt, expiryOK := row[23].(time.Time)
		if !indexOK || !timeOK || !expiryOK || index == "" || len(index) > 255 || !utf8.ValidString(index) {
			return nil, fmt.Errorf("store ClickHouse batch: row %d has invalid retention metadata", rowIndex)
		}
		retention := expiresAt.Sub(indexTime)
		if retention <= 0 {
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
	_, _ = metadata.Write([]byte{'O', 'S', 'V', 'M', 3})
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(len(indexes)))
	_, _ = metadata.Write(number[:])
	for _, index := range indexes {
		binary.BigEndian.PutUint64(number[:], uint64(len(index)))
		_, _ = metadata.Write(number[:])
		_, _ = metadata.WriteString(index)
		binary.BigEndian.PutUint64(number[:], uint64(retentionByIndex[index]))
		_, _ = metadata.Write(number[:])
	}
	binary.BigEndian.PutUint64(number[:], batch.BatchSequence)
	_, _ = metadata.Write(number[:])
	var short [4]byte
	binary.BigEndian.PutUint32(short[:], batch.OriginalEventCount)
	_, _ = metadata.Write(short[:])
	binary.BigEndian.PutUint32(short[:], uint32(len(batch.RejectedEvents)))
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
	if _, err := io.ReadFull(reader, header); err != nil || !bytes.Equal(header, []byte{'O', 'S', 'V', 'M', 3}) {
		return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid version")
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
		if err != nil || length == 0 || length > 255 || length > uint64(reader.Len()) {
			return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid index name")
		}
		name := make([]byte, int(length))
		if _, err := io.ReadFull(reader, name); err != nil {
			return reservationMetadata{}, errors.New("visibility reservation metadata is truncated")
		}
		duration, err := readUint64()
		if err != nil || duration == 0 || duration > math.MaxInt64 {
			return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid retention duration")
		}
		index := string(name)
		if !utf8.ValidString(index) {
			return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid index name")
		}
		if _, duplicate := retentionByIndex[index]; duplicate {
			return reservationMetadata{}, errors.New("visibility reservation metadata contains a duplicate index")
		}
		retentionByIndex[index] = time.Duration(duration)
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
	if rejectionCount > originalEventCount || uint64(rejectionCount) > uint64(reader.Len())/9 {
		return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid rejection count")
	}
	rejections := make([]*opensplunkv1.EventRejection, 0, rejectionCount)
	seenRejections := make(map[uint32]struct{}, rejectionCount)
	for index := uint32(0); index < rejectionCount; index++ {
		length, err := readUint64()
		if err != nil || length == 0 || length > uint64(reader.Len()) || length > uint64(math.MaxInt) {
			return reservationMetadata{}, errors.New("visibility reservation metadata has an invalid rejection payload")
		}
		encoded := make([]byte, int(length))
		if _, err := io.ReadFull(reader, encoded); err != nil {
			return reservationMetadata{}, errors.New("visibility reservation metadata is truncated")
		}
		rejection := new(opensplunkv1.EventRejection)
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
		index, ok := row[2].(string)
		if !ok {
			return fmt.Errorf("store ClickHouse batch: row %d has an invalid index", rowIndex)
		}
		retention, exists := metadata.RetentionByIndex[index]
		if !exists {
			return fmt.Errorf("visibility reservation has no retention for index %q", index)
		}
		expiresAt := indexTime.Add(retention)
		if !expiresAt.After(indexTime) {
			return fmt.Errorf("visibility reservation retention overflows for index %q", index)
		}
		row[4] = indexTime
		row[23] = expiresAt
		row[len(row)-1] = reservation.Sequence
	}
	return nil
}

func (s *Store) classifyError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var existing *ingest.TransientStoreError
	if errors.As(err, &existing) {
		return err
	}
	reason, transient := transientStoreReason(err)
	if !transient {
		return err
	}
	return &ingest.TransientStoreError{Err: err, Reason: reason, RetryAfter: s.retryAfter}
}

func transientStoreReason(err error) (opensplunkv1.RetryBatchReason, bool) {
	if errors.Is(err, clickhousedriver.ErrAcquireConnTimeout) {
		return opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY, true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, clickhousedriver.ErrConnectionClosed) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ETIMEDOUT) {
		return opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE, true
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE, true
	}
	var operationError *clickhousedriver.OpError
	if errors.As(err, &operationError) && operationError.Err != nil {
		if reason, ok := transientStoreReason(operationError.Err); ok {
			return reason, true
		}
	}
	var exception *clickhousedriver.Exception
	if !errors.As(err, &exception) {
		return opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_UNSPECIFIED, false
	}
	switch exception.Code {
	case 364:
		return opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED, true
	case 202, 203, 241, 252, 745:
		return opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY, true
	case 95, 96, 159, 209, 210, 225, 242, 243, 279, 285, 286, 319, 341, 999:
		return opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE, true
	default:
		return opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_UNSPECIFIED, false
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
	Ping(context.Context) error
	Close() error
}

type nativeStoreConnection struct {
	connection driver.Conn
}

func (c *nativeStoreConnection) prepare(ctx context.Context, query string, settings clickhousedriver.Settings) (writeBatch, error) {
	ctx = clickhousedriver.Context(ctx, clickhousedriver.WithSettings(settings))
	return c.connection.PrepareBatch(ctx, query)
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
		if config.TLS == nil && !isLoopbackHost(host) {
			return nil, Config{}, fmt.Errorf("ClickHouse TLS is required for non-loopback address at position %d", i)
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

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}
