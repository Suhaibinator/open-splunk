package featureaudit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"fortio.org/safecast"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/featureops"
)

var requiredTriggers = [...]string{
	"feature_operation_audit_state_initial_shape_is_valid",
	"feature_operation_audit_state_transition_is_valid",
	"feature_operation_audit_state_delete_is_forbidden",
	"feature_operation_audit_event_insert_requires_current_state",
	"feature_operation_audit_event_advances_and_prunes_tenant_state",
	"feature_operation_audit_event_update_is_forbidden",
	"feature_operation_audit_event_delete_requires_rolling_prune",
	"feature_operation_audit_event_prune_advances_tenant_state",
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Store is one tenant-scoped durable feature-operation observer. Appends are
// serialized so each committed event receives one deterministic next sequence.
type Store struct {
	database           *sql.DB
	tenantID           string
	clock              Clock
	observationTimeout time.Duration
	onFailure          func(Failure)
	appendPermit       chan struct{}
	stateInitialized   bool
	healthMu           sync.Mutex
	health             Health
}

var _ featureops.Observer = (*Store)(nil)

// New verifies the complete operational journal before accepting appends.
// The migrated control database remains owned by the caller.
func New(ctx context.Context, database *control.DB, options Options) (*Store, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: feature audit context is nil", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if database == nil || database.SQLDB() == nil {
		return nil, fmt.Errorf("%w: feature audit control database is required", control.ErrInvalidArgument)
	}
	if err := audit.ValidateTenantID(options.TenantID); err != nil {
		return nil, fmt.Errorf("%w: feature audit tenant ID is invalid", control.ErrInvalidArgument)
	}
	clock := options.Clock
	if clock == nil {
		clock = systemClock{}
	}
	timeout := options.ObservationTimeout
	if timeout == 0 {
		timeout = defaultObservationTimeout
	}
	if timeout < time.Millisecond || timeout > maximumObservationTimeout {
		return nil, fmt.Errorf("%w: feature audit observation timeout is invalid", control.ErrInvalidArgument)
	}
	if err := validateStartupIntegrity(ctx, database.SQLDB()); err != nil {
		return nil, err
	}
	store := &Store{
		database:           database.SQLDB(),
		tenantID:           options.TenantID,
		clock:              clock,
		observationTimeout: timeout,
		onFailure:          options.OnFailure,
		appendPermit:       make(chan struct{}, 1),
	}
	store.appendPermit <- struct{}{}
	return store, nil
}

// Observe persists one valid event without allowing an audit failure to alter
// the already-committed feature operation. Failures are projected only through
// the bounded Health value and optional payload-free callback.
func (store *Store) Observe(event featureops.Event) {
	if store == nil {
		return
	}
	reported := false
	defer func() {
		if recover() != nil && !reported {
			store.reportFailure(FailureInternal)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), store.observationTimeout)
	defer cancel()
	if _, err := store.Append(ctx, event); err != nil {
		reported = true
		store.reportFailure(classifyFailure(err))
	}
}

// Append commits one identity-free event and returns its assigned record.
func (store *Store) Append(ctx context.Context, event featureops.Event) (Record, error) {
	if ctx == nil {
		return Record{}, fmt.Errorf("%w: feature audit append context is nil", control.ErrInvalidArgument)
	}
	if store == nil || store.database == nil || store.clock == nil {
		return Record{}, fmt.Errorf("%w: feature audit store is unavailable", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if err := validateEvent(event); err != nil {
		return Record{}, err
	}
	select {
	case <-ctx.Done():
		return Record{}, ctx.Err()
	case <-store.appendPermit:
	}
	defer func() { store.appendPermit <- struct{}{} }()

	tx, err := store.database.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, fmt.Errorf("begin feature audit append: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if !store.stateInitialized {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO feature_operation_audit_tenant_state (
				tenant_id, first_sequence, next_sequence, retained_count,
				last_occurred_at_unix_micro
			) VALUES (?, 1, 1, 0, NULL)
			ON CONFLICT (tenant_id) DO NOTHING
		`, store.tenantID); err != nil {
			return Record{}, fmt.Errorf("prepare feature audit tenant state: %w", err)
		}
	}
	var first int64
	var sequence int64
	var count int64
	var last sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT first_sequence, next_sequence, retained_count,
			last_occurred_at_unix_micro
		FROM feature_operation_audit_tenant_state
		WHERE tenant_id = ?
	`, store.tenantID).Scan(&first, &sequence, &count, &last); err != nil {
		return Record{}, fmt.Errorf("read feature audit tenant state: %w", err)
	}
	if first < 1 || sequence-first != count || count < 0 ||
		count > MaximumEventsPerTenant {
		return Record{}, ErrCorrupt
	}
	if sequence == math.MaxInt64 {
		return Record{}, fmt.Errorf(
			"%w: feature audit tenant has exhausted its sequence domain",
			control.ErrCapacityExceeded,
		)
	}
	occurredAt, valid := audit.CanonicalOccurrenceTime(store.clock.Now())
	if !valid {
		return Record{}, fmt.Errorf("%w: feature audit clock returned an invalid time", control.ErrInvalidArgument)
	}
	if last.Valid && occurredAt.UnixMicro() < last.Int64 {
		occurredAt = time.UnixMicro(last.Int64).UTC()
	}
	items, err := safecast.Conv[int64](event.Items)
	if err != nil {
		return Record{}, fmt.Errorf("%w: feature audit item count is too large", control.ErrInvalidArgument)
	}
	bytes, err := safecast.Conv[int64](event.Bytes)
	if err != nil {
		return Record{}, fmt.Errorf("%w: feature audit byte count is too large", control.ErrInvalidArgument)
	}
	sequenceNumber, err := safecast.Conv[uint64](sequence)
	if err != nil {
		return Record{}, ErrCorrupt
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO feature_operation_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			feature, operation, outcome, items, bytes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		store.tenantID,
		sequence,
		occurredAt.UnixMicro(),
		int64(event.Feature),
		int64(event.Operation),
		int64(event.Outcome),
		items,
		bytes,
	); err != nil {
		return Record{}, fmt.Errorf("append feature audit event: %w", err)
	}
	var advancedFirst int64
	var advancedSequence int64
	var advancedCount int64
	var advancedTime int64
	if err := tx.QueryRowContext(ctx, `
		SELECT first_sequence, next_sequence, retained_count,
			last_occurred_at_unix_micro
		FROM feature_operation_audit_tenant_state
		WHERE tenant_id = ?
	`, store.tenantID).Scan(
		&advancedFirst, &advancedSequence, &advancedCount, &advancedTime,
	); err != nil {
		return Record{}, fmt.Errorf("verify feature audit tenant state: %w", err)
	}
	expectedFirst := first
	expectedCount := count + 1
	if expectedCount > MaximumEventsPerTenant {
		expectedFirst++
		expectedCount = MaximumEventsPerTenant
	}
	if advancedFirst != expectedFirst || advancedSequence != sequence+1 ||
		advancedCount != expectedCount ||
		advancedTime != occurredAt.UnixMicro() {
		return Record{}, ErrCorrupt
	}
	if err := tx.Commit(); err != nil {
		return Record{}, fmt.Errorf("commit feature audit append: %w", err)
	}
	committed = true
	store.stateInitialized = true
	return Record{
		Sequence:   sequenceNumber,
		OccurredAt: occurredAt,
		Event:      event,
	}, nil
}

// List returns ascending records after one sequence. It exists for bounded
// diagnostics and verification; feature runtime paths never enumerate it.
func (store *Store) List(ctx context.Context, afterSequence uint64, limit int) ([]Record, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: feature audit list context is nil", control.ErrInvalidArgument)
	}
	if store == nil || store.database == nil {
		return nil, fmt.Errorf("%w: feature audit store is unavailable", control.ErrInvalidArgument)
	}
	if limit < 1 || limit > MaximumListPageSize || afterSequence > math.MaxInt64 {
		return nil, fmt.Errorf("%w: feature audit list request is invalid", control.ErrInvalidArgument)
	}
	rows, err := store.database.QueryContext(ctx, `
		SELECT sequence, occurred_at_unix_micro, feature, operation, outcome, items, bytes
		FROM feature_operation_audit_events
		WHERE tenant_id = ? AND sequence > ?
		ORDER BY sequence ASC
		LIMIT ?
	`, store.tenantID, int64(afterSequence), limit)
	if err != nil {
		return nil, fmt.Errorf("list feature audit events: %w", err)
	}
	defer rows.Close()
	records := make([]Record, 0, limit)
	for rows.Next() {
		var sequence int64
		var occurredAt int64
		var feature int64
		var operation int64
		var outcome int64
		var items int64
		var bytes int64
		if err := rows.Scan(
			&sequence,
			&occurredAt,
			&feature,
			&operation,
			&outcome,
			&items,
			&bytes,
		); err != nil {
			return nil, fmt.Errorf("scan feature audit event: %w", err)
		}
		canonical, valid := audit.CanonicalOccurrenceTime(time.UnixMicro(occurredAt))
		if sequence < 1 || !valid || canonical.UnixMicro() != occurredAt {
			return nil, ErrCorrupt
		}
		sequenceNumber, sequenceErr := safecast.Conv[uint64](sequence)
		featureValue, featureErr := safecast.Conv[featureops.Feature](feature)
		operationValue, operationErr := safecast.Conv[featureops.Operation](operation)
		outcomeValue, outcomeErr := safecast.Conv[featureops.Outcome](outcome)
		itemCount, itemsErr := safecast.Conv[uint64](items)
		byteCount, bytesErr := safecast.Conv[uint64](bytes)
		event := featureops.Event{
			Feature: featureValue, Operation: operationValue, Outcome: outcomeValue,
			Items: itemCount, Bytes: byteCount,
		}
		if sequenceErr != nil || featureErr != nil || operationErr != nil || outcomeErr != nil ||
			itemsErr != nil || bytesErr != nil || !event.Valid() {
			return nil, ErrCorrupt
		}
		records = append(records, Record{
			Sequence:   sequenceNumber,
			OccurredAt: canonical,
			Event:      event,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate feature audit events: %w", err)
	}
	return records, nil
}

// Health returns detached, bounded failure accounting.
func (store *Store) Health() Health {
	if store == nil {
		return Health{}
	}
	store.healthMu.Lock()
	defer store.healthMu.Unlock()
	return store.health
}

func (store *Store) reportFailure(category FailureCategory) {
	store.healthMu.Lock()
	if store.health.FailedEvents < math.MaxUint64 {
		store.health.FailedEvents++
	}
	store.health.LastFailure = category
	callback := store.onFailure
	store.healthMu.Unlock()
	if callback == nil {
		return
	}
	defer func() { _ = recover() }()
	callback(Failure{Category: category})
}

func classifyFailure(err error) FailureCategory {
	switch {
	case errors.Is(err, control.ErrCapacityExceeded):
		return FailureCapacity
	case errors.Is(err, control.ErrInvalidArgument):
		return FailureInvalid
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return FailureTimeout
	case errors.Is(err, ErrCorrupt):
		return FailureIntegrity
	default:
		return FailureStorage
	}
}
