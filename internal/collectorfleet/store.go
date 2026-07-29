package collectorfleet

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// New constructs a fleet store without changing the migrated SQL schema.
func New(database *control.DB) (*Store, error) {
	if database == nil || database.GORMDB() == nil {
		return nil, invalid("control database is required")
	}
	return &Store{orm: database.GORMDB()}, nil
}

// Claim atomically creates or supersedes the active stream lease for one
// enabled collector. Administrator version is unchanged for an existing
// collector; telemetry revision and durable lease generation advance once.
func (store *Store) Claim(
	ctx context.Context,
	request ClaimRequest,
) (result Collector, lease Lease, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return Collector{}, Lease{}, err
	}
	normalized, err := normalizeClaim(request)
	if err != nil {
		return Collector{}, Lease{}, err
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return Collector{}, Lease{}, mapContextError(ctx, "begin collector lease claim", tx.Error)
	}
	finished := false
	defer finishGORMTransaction(tx, &finished, &returnedErr)

	var fleet fleetRecord
	queryErr := tx.
		Where(
			"tenant_id = ? AND collector_id = ?",
			normalized.scope.TenantID,
			normalized.collectorID,
		).
		Take(&fleet).Error
	switch {
	case errors.Is(queryErr, gorm.ErrRecordNotFound):
		fleet = fleetRecord{
			TenantID:             normalized.scope.TenantID,
			CollectorID:          normalized.collectorID,
			AdminVersion:         1,
			AdministrativeState:  AdministrativeStateEnabled,
			FirstSeenAtUnixMicro: normalized.receivedAt.UnixMicro(),
			UpdatedAtUnixMicro:   normalized.receivedAt.UnixMicro(),
		}
		if err := tx.Create(&fleet).Error; err != nil {
			return Collector{}, Lease{}, mapContextError(ctx, "create collector identity", err)
		}
		runtime := runtimeForClaim(normalized, 1, 1, normalized.receivedAt)
		if err := tx.Create(&runtime).Error; err != nil {
			return Collector{}, Lease{}, mapContextError(ctx, "create collector runtime", err)
		}
	case queryErr != nil:
		return Collector{}, Lease{}, mapContextError(ctx, "read collector identity for claim", queryErr)
	default:
		if fleet.AdministrativeState == AdministrativeStateDisabled {
			return Collector{}, Lease{}, ErrCollectorDisabled
		}
		if fleet.AdministrativeState != AdministrativeStateEnabled {
			return Collector{}, Lease{}, errors.New(
				"claim collector lease: invalid administrative state in control-plane database",
			)
		}
		var current runtimeRecord
		if err := tx.
			Where(
				"tenant_id = ? AND collector_id = ?",
				normalized.scope.TenantID,
				normalized.collectorID,
			).
			Take(&current).Error; err != nil {
			return Collector{}, Lease{}, mapContextError(ctx, "read collector runtime for claim", err)
		}
		if current.LeaseGeneration < 1 ||
			current.TelemetryRevision < 1 {
			return Collector{}, Lease{}, errors.New(
				"claim collector lease: invalid runtime revision in control-plane database",
			)
		}
		if current.LeaseGeneration == math.MaxInt64 ||
			current.TelemetryRevision >= math.MaxInt64-1 {
			return Collector{}, Lease{}, control.ErrCapacityExceeded
		}
		receivedAt := normalized.receivedAt
		if current.LastSeenAtUnixMicro > receivedAt.UnixMicro() {
			receivedAt = time.UnixMicro(current.LastSeenAtUnixMicro).UTC()
		}
		replacement := runtimeForClaim(
			normalized,
			current.TelemetryRevision+1,
			current.LeaseGeneration+1,
			receivedAt,
		)
		update := tx.Model(&runtimeRecord{}).
			Where(
				"tenant_id = ? AND collector_id = ? AND lease_generation = ? AND telemetry_revision = ?",
				normalized.scope.TenantID,
				normalized.collectorID,
				current.LeaseGeneration,
				current.TelemetryRevision,
			).
			Updates(runtimeUpdateValues(replacement))
		if update.Error != nil {
			return Collector{}, Lease{}, mapContextError(ctx, "supersede collector lease", update.Error)
		}
		if update.RowsAffected != 1 {
			return Collector{}, Lease{}, control.ErrVersionConflict
		}
	}

	if err := replaceHelloCollections(tx, normalized); err != nil {
		return Collector{}, Lease{}, err
	}
	result, err = loadCollector(
		tx,
		normalized.scope,
		normalized.collectorID,
	)
	if err != nil {
		return Collector{}, Lease{}, fmt.Errorf("read claimed collector: %w", err)
	}
	lease = Lease{
		Scope:       normalized.scope,
		CollectorID: normalized.collectorID,
		BootEpoch:   normalized.bootEpoch,
		StreamID:    normalized.streamID,
		Generation:  result.LeaseGeneration,
	}
	if err := tx.Commit().Error; err != nil {
		return Collector{}, Lease{}, mapContextError(ctx, "commit collector lease claim", err)
	}
	finished = true
	return result, lease, nil
}

// Get returns one detached collector snapshot without disclosing cross-tenant
// existence. Its short SQLite transaction keeps normalized child rows coherent
// with the base/runtime revisions.
func (store *Store) Get(
	ctx context.Context,
	scope Scope,
	collectorID string,
) (result Collector, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return Collector{}, err
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return Collector{}, err
	}
	if !validIdentifier(collectorID) {
		return Collector{}, invalid("collector ID is not a canonical identifier")
	}
	tx := store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return Collector{}, mapContextError(ctx, "begin collector read", tx.Error)
	}
	finished := false
	defer finishGORMTransaction(tx, &finished, &returnedErr)
	result, err = loadCollector(tx, scope, collectorID)
	if err != nil {
		return Collector{}, err
	}
	if err := tx.Commit().Error; err != nil {
		return Collector{}, mapContextError(ctx, "commit collector read", err)
	}
	finished = true
	return result, nil
}

// InvalidatePriorBootLeases is the server-start boundary for durable fleet
// state. It disconnects every active lease from an older boot across all
// tenants while preserving leases already claimed under currentBootEpoch.
// Administrator versions and lease generations are unchanged. Repeating the
// operation for the same boot is idempotent.
func (store *Store) InvalidatePriorBootLeases(
	ctx context.Context,
	currentBootEpoch string,
	receivedAt time.Time,
) (invalidated uint64, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if !validIdentifier(currentBootEpoch) {
		return 0, invalid("current boot epoch is not a canonical identifier")
	}
	receivedAt, err := normalizeTimestamp("server boot receive time", receivedAt)
	if err != nil {
		return 0, err
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return 0, mapContextError(ctx, "begin prior-boot lease invalidation", tx.Error)
	}
	finished := false
	defer finishGORMTransaction(tx, &finished, &returnedErr)

	update := tx.Model(&runtimeRecord{}).
		Where(
			`boot_epoch IS NOT NULL
			 AND boot_epoch <> ?
			 AND EXISTS (
				SELECT 1
				FROM collector_fleet
				WHERE collector_fleet.tenant_id = collector_runtime.tenant_id
				  AND collector_fleet.collector_id = collector_runtime.collector_id
				  AND collector_fleet.administrative_state = ?
			 )`,
			currentBootEpoch,
			AdministrativeStateEnabled,
		).
		Updates(map[string]any{
			"telemetry_revision": gorm.Expr(
				`CASE
					WHEN telemetry_revision < ? THEN telemetry_revision + 1
					ELSE telemetry_revision
				END`,
				int64(math.MaxInt64),
			),
			"boot_epoch":         nil,
			"stream_id":          nil,
			"active_instance_id": nil,
			"last_seen_at_unix_micro": gorm.Expr(
				"max(last_seen_at_unix_micro, ?)",
				receivedAt.UnixMicro(),
			),
			"disconnected_at_unix_micro": gorm.Expr(
				"max(last_seen_at_unix_micro, ?)",
				receivedAt.UnixMicro(),
			),
		})
	if update.Error != nil {
		return 0, mapContextError(ctx, "invalidate prior-boot collector leases", update.Error)
	}
	if update.RowsAffected < 0 {
		return 0, errors.New("invalidate prior-boot collector leases: invalid affected-row count")
	}
	if err := tx.Commit().Error; err != nil {
		return 0, mapContextError(ctx, "commit prior-boot lease invalidation", err)
	}
	finished = true
	return nonnegativeInt64ToUint64(update.RowsAffected), nil
}

// RecordHeartbeat persists a complete latest-wins telemetry snapshot. A stale
// boot epoch, generation, stream ID, disabled identity, or out-of-order
// observation is an expected no-op and returns applied=false with no error.
func (store *Store) RecordHeartbeat(
	ctx context.Context,
	lease Lease,
	heartbeat Heartbeat,
) (applied bool, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return false, err
	}
	lease, err := normalizeLease(lease)
	if err != nil {
		return false, err
	}
	heartbeat, err = normalizeHeartbeat(heartbeat)
	if err != nil {
		return false, err
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return false, mapContextError(ctx, "begin collector heartbeat", tx.Error)
	}
	finished := false
	defer finishGORMTransaction(tx, &finished, &returnedErr)

	current, currentLease, err := takeCurrentLease(tx, lease)
	if err != nil {
		return false, err
	}
	if !currentLease ||
		heartbeat.ObservationSequence <= nonnegativeInt64ToUint64(current.ObservationSequence) {
		return false, nil
	}
	if current.TelemetryRevision >= math.MaxInt64-1 {
		return false, control.ErrCapacityExceeded
	}
	registered, err := registeredInputSet(tx, lease.Scope, lease.CollectorID)
	if err != nil {
		return false, err
	}
	for _, input := range heartbeat.Inputs {
		if _, exists := registered[input.InputID]; !exists {
			return false, invalid("input health references an unregistered input")
		}
	}
	lastSeenAt := heartbeat.ReceivedAt
	if current.LastSeenAtUnixMicro > lastSeenAt.UnixMicro() {
		lastSeenAt = time.UnixMicro(current.LastSeenAtUnixMicro).UTC()
	}
	values := heartbeatUpdateValues(heartbeat, lastSeenAt)
	update := tx.Model(&runtimeRecord{}).
		Where(
			`tenant_id = ?
			 AND collector_id = ?
			 AND boot_epoch = ?
			 AND stream_id = ?
			 AND lease_generation = ?
			 AND observation_sequence < ?`,
			lease.TenantID,
			lease.CollectorID,
			lease.BootEpoch,
			lease.StreamID,
			boundedUint64ToInt64(lease.Generation),
			boundedUint64ToInt64(heartbeat.ObservationSequence),
		).
		Updates(values)
	if update.Error != nil {
		return false, mapContextError(ctx, "record collector heartbeat", update.Error)
	}
	if update.RowsAffected != 1 {
		return false, nil
	}
	if err := replaceInputHealth(tx, lease, heartbeat.Inputs); err != nil {
		return false, err
	}
	if err := tx.Commit().Error; err != nil {
		return false, mapContextError(ctx, "commit collector heartbeat", err)
	}
	finished = true
	return true, nil
}

// Disconnect conditionally releases exactly the supplied active lease.
// Delayed cleanup from a superseded handler is a no-op.
func (store *Store) Disconnect(
	ctx context.Context,
	lease Lease,
	receivedAt time.Time,
) (applied bool, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return false, err
	}
	lease, err := normalizeLease(lease)
	if err != nil {
		return false, err
	}
	receivedAt, err = normalizeTimestamp("disconnect receive time", receivedAt)
	if err != nil {
		return false, err
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return false, mapContextError(ctx, "begin collector disconnect", tx.Error)
	}
	finished := false
	defer finishGORMTransaction(tx, &finished, &returnedErr)
	current, currentLease, err := takeCurrentLease(tx, lease)
	if err != nil {
		return false, err
	}
	if !currentLease {
		return false, nil
	}
	if current.LastSeenAtUnixMicro > receivedAt.UnixMicro() {
		receivedAt = time.UnixMicro(current.LastSeenAtUnixMicro).UTC()
	}
	update := tx.Model(&runtimeRecord{}).
		Where(
			`tenant_id = ?
			 AND collector_id = ?
			 AND boot_epoch = ?
			 AND stream_id = ?
			 AND lease_generation = ?`,
			lease.TenantID,
			lease.CollectorID,
			lease.BootEpoch,
			lease.StreamID,
			boundedUint64ToInt64(lease.Generation),
		).
		Updates(map[string]any{
			"telemetry_revision":         nextTelemetryRevision(current.TelemetryRevision),
			"boot_epoch":                 nil,
			"stream_id":                  nil,
			"active_instance_id":         nil,
			"last_seen_at_unix_micro":    receivedAt.UnixMicro(),
			"disconnected_at_unix_micro": receivedAt.UnixMicro(),
		})
	if update.Error != nil {
		return false, mapContextError(ctx, "disconnect collector lease", update.Error)
	}
	if update.RowsAffected != 1 {
		return false, nil
	}
	if err := tx.Commit().Error; err != nil {
		return false, mapContextError(ctx, "commit collector disconnect", err)
	}
	finished = true
	return true, nil
}

// UpdateAdministration replaces display metadata and enabled/disabled state
// under administrator optimistic locking. The disabled fleet state is the
// authoritative lease fence and commits even when unrelated persisted runtime
// or child telemetry is corrupt. A healthy active runtime is cleared in the
// same transaction; re-enabling requires a valid inactive runtime so a dormant
// stale lease can never regain authority.
func (store *Store) UpdateAdministration(
	ctx context.Context,
	scope Scope,
	collectorID string,
	expectedVersion uint64,
	administration Administration,
	receivedAt time.Time,
) (result AdministrationSnapshot, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return AdministrationSnapshot{}, err
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return AdministrationSnapshot{}, err
	}
	if !validIdentifier(collectorID) {
		return AdministrationSnapshot{}, invalid("collector ID is not a canonical identifier")
	}
	if expectedVersion == 0 || expectedVersion > math.MaxInt64 {
		return AdministrationSnapshot{}, invalid(
			"expected administrator version is outside the supported range",
		)
	}
	administration, err = normalizeAdministration(administration)
	if err != nil {
		return AdministrationSnapshot{}, err
	}
	receivedAt, err = normalizeTimestamp("administrator update receive time", receivedAt)
	if err != nil {
		return AdministrationSnapshot{}, err
	}
	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return AdministrationSnapshot{}, mapContextError(
			ctx,
			"begin collector administration update",
			tx.Error,
		)
	}
	finished := false
	defer finishGORMTransaction(tx, &finished, &returnedErr)

	var fleet fleetRecord
	queryErr := tx.
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Take(&fleet).Error
	if errors.Is(queryErr, gorm.ErrRecordNotFound) {
		return AdministrationSnapshot{}, control.ErrNotFound
	}
	if queryErr != nil {
		return AdministrationSnapshot{}, mapContextError(
			ctx,
			"read collector for administration update",
			queryErr,
		)
	}
	if fleet.AdminVersion < 1 {
		return AdministrationSnapshot{}, errors.New(
			"update collector administration: invalid version in control-plane database",
		)
	}
	if fleet.AdministrativeState != AdministrativeStateEnabled &&
		fleet.AdministrativeState != AdministrativeStateDisabled {
		return AdministrationSnapshot{}, errors.New(
			"update collector administration: invalid state in control-plane database",
		)
	}
	if uint64(fleet.AdminVersion) != expectedVersion {
		return AdministrationSnapshot{}, control.ErrVersionConflict
	}
	terminalDisable := fleet.AdminVersion == math.MaxInt64 &&
		fleet.AdministrativeState == AdministrativeStateEnabled &&
		administration.State == AdministrativeStateDisabled
	if fleet.AdminVersion == math.MaxInt64 && !terminalDisable {
		return AdministrationSnapshot{}, control.ErrCapacityExceeded
	}
	if fleet.UpdatedAtUnixMicro > receivedAt.UnixMicro() {
		receivedAt = time.UnixMicro(fleet.UpdatedAtUnixMicro).UTC()
	}
	if fleet.AdministrativeState == AdministrativeStateDisabled &&
		administration.State == AdministrativeStateEnabled {
		if err := requireInactiveRuntimeLease(
			tx,
			scope,
			collectorID,
		); err != nil {
			return AdministrationSnapshot{}, fmt.Errorf(
				"prepare collector runtime for enable: %w",
				err,
			)
		}
	}
	fleetUpdate := tx.Model(&fleetRecord{}).
		Where(
			"tenant_id = ? AND collector_id = ? AND admin_version = ?",
			scope.TenantID,
			collectorID,
			int64(expectedVersion),
		)
	if terminalDisable {
		fleetUpdate = fleetUpdate.Where("administrative_state = ?", AdministrativeStateEnabled)
	}
	nextAdminVersion := fleet.AdminVersion
	if !terminalDisable {
		nextAdminVersion++
	}
	updateValues := map[string]any{
		"admin_version":         nextAdminVersion,
		"administrative_state":  administration.State,
		"updated_at_unix_micro": receivedAt.UnixMicro(),
	}
	resultDisplayName := administration.DisplayName
	if terminalDisable {
		resultDisplayName = fleet.DisplayName
	} else {
		updateValues["display_name"] = administration.DisplayName
	}
	update := fleetUpdate.Updates(updateValues)
	if update.Error != nil {
		return AdministrationSnapshot{}, mapContextError(
			ctx,
			"update collector administration",
			update.Error,
		)
	}
	if update.RowsAffected != 1 {
		return AdministrationSnapshot{}, control.ErrVersionConflict
	}
	if administration.State == AdministrativeStateDisabled {
		bestEffortInvalidateRuntimeLease(tx, scope, collectorID, receivedAt)
	}
	if err := tx.Commit().Error; err != nil {
		return AdministrationSnapshot{}, mapContextError(
			ctx,
			"commit collector administration update",
			err,
		)
	}
	finished = true
	return AdministrationSnapshot{
		TenantID:            scope.TenantID,
		CollectorID:         collectorID,
		Version:             uint64(nextAdminVersion),
		DisplayName:         cloneString(resultDisplayName),
		AdministrativeState: administration.State,
	}, nil
}

func runtimeForClaim(
	claim normalizedClaim,
	telemetryRevision int64,
	generation int64,
	receivedAt time.Time,
) runtimeRecord {
	bootEpoch := claim.bootEpoch
	streamID := claim.streamID
	instanceID := claim.hello.InstanceID
	return runtimeRecord{
		TenantID: claim.scope.TenantID, CollectorID: claim.collectorID,
		TelemetryRevision: telemetryRevision, LeaseGeneration: generation,
		BootEpoch: &bootEpoch, StreamID: &streamID, ActiveInstanceID: &instanceID,
		ProtocolMajor:        int64(claim.hello.ProtocolMajor),
		ProtocolMinor:        int64(claim.hello.ProtocolMinor),
		CollectorVersion:     claim.hello.CollectorVersion,
		Hostname:             claim.hello.Hostname,
		OperatingSystem:      claim.hello.OperatingSystem,
		Architecture:         claim.hello.Architecture,
		StartedAtUnixMicro:   claim.hello.StartedAt.UnixMicro(),
		ConnectedAtUnixMicro: receivedAt.UnixMicro(),
		LastSeenAtUnixMicro:  receivedAt.UnixMicro(),
		ObservationSequence:  0,
		LastAckedAtHelloSequence: uint64PointerToInt64(
			claim.hello.LastAcknowledgedBatchSequence,
		),
	}
}

func runtimeUpdateValues(record runtimeRecord) map[string]any {
	return map[string]any{
		"telemetry_revision":               record.TelemetryRevision,
		"lease_generation":                 record.LeaseGeneration,
		"boot_epoch":                       record.BootEpoch,
		"stream_id":                        record.StreamID,
		"active_instance_id":               record.ActiveInstanceID,
		"protocol_major":                   record.ProtocolMajor,
		"protocol_minor":                   record.ProtocolMinor,
		"collector_version":                record.CollectorVersion,
		"hostname":                         record.Hostname,
		"operating_system":                 record.OperatingSystem,
		"architecture":                     record.Architecture,
		"started_at_unix_micro":            record.StartedAtUnixMicro,
		"connected_at_unix_micro":          record.ConnectedAtUnixMicro,
		"last_seen_at_unix_micro":          record.LastSeenAtUnixMicro,
		"disconnected_at_unix_micro":       nil,
		"observation_sequence":             0,
		"observed_at_unix_micro":           nil,
		"last_acked_at_hello_sequence":     record.LastAckedAtHelloSequence,
		"queued_events":                    0,
		"queued_bytes":                     0,
		"oldest_event_age_nanoseconds":     nil,
		"sent_events_total":                0,
		"acknowledged_events_total":        0,
		"retried_batches_total":            0,
		"rejected_events_total":            0,
		"dropped_events_total":             0,
		"last_sent_batch_sequence":         nil,
		"last_acknowledged_batch_sequence": nil,
		"process_resident_memory_bytes":    0,
		"process_cpu_percent":              0,
	}
}

func replaceHelloCollections(tx *gorm.DB, claim normalizedClaim) error {
	for _, model := range []any{
		&capabilityRecord{},
		&authorizedIndexRecord{},
		&inputRecord{},
	} {
		if err := tx.
			Where(
				"tenant_id = ? AND collector_id = ?",
				claim.scope.TenantID,
				claim.collectorID,
			).
			Delete(model).Error; err != nil {
			return mapContextError(tx.Statement.Context, "replace collector hello snapshot", err)
		}
	}
	capabilities := make([]capabilityRecord, len(claim.hello.Capabilities))
	for index, capability := range claim.hello.Capabilities {
		capabilities[index] = capabilityRecord{
			TenantID: claim.scope.TenantID, CollectorID: claim.collectorID,
			Capability: int64(capability),
		}
	}
	if len(capabilities) != 0 {
		if err := tx.Create(&capabilities).Error; err != nil {
			return mapContextError(tx.Statement.Context, "store collector capabilities", err)
		}
	}
	indexes := make([]authorizedIndexRecord, len(claim.hello.AuthorizedIndexes))
	for index, indexName := range claim.hello.AuthorizedIndexes {
		indexes[index] = authorizedIndexRecord{
			TenantID: claim.scope.TenantID, CollectorID: claim.collectorID,
			IndexName: indexName,
		}
	}
	if err := tx.Create(&indexes).Error; err != nil {
		return mapContextError(tx.Statement.Context, "store collector authorized indexes", err)
	}
	inputs := make([]inputRecord, len(claim.hello.Inputs))
	for index, input := range claim.hello.Inputs {
		inputs[index] = inputRecord{
			TenantID: claim.scope.TenantID, CollectorID: claim.collectorID,
			InputID: input.InputID, InputType: int64(input.InputType),
			IndexName: input.IndexName,
			Source:    cloneString(input.Source), Sourcetype: cloneString(input.Sourcetype),
		}
	}
	if len(inputs) != 0 {
		if err := tx.Create(&inputs).Error; err != nil {
			return mapContextError(tx.Statement.Context, "store collector input registrations", err)
		}
	}
	return nil
}

func heartbeatUpdateValues(heartbeat Heartbeat, lastSeenAt time.Time) map[string]any {
	return map[string]any{
		"telemetry_revision":               gorm.Expr("telemetry_revision + 1"),
		"last_seen_at_unix_micro":          lastSeenAt.UnixMicro(),
		"observation_sequence":             boundedUint64ToInt64(heartbeat.ObservationSequence),
		"observed_at_unix_micro":           heartbeat.ObservedAt.UnixMicro(),
		"queued_events":                    boundedUint64ToInt64(heartbeat.Queue.QueuedEvents),
		"queued_bytes":                     boundedUint64ToInt64(heartbeat.Queue.QueuedBytes),
		"oldest_event_age_nanoseconds":     durationPointerToInt64(heartbeat.Queue.OldestEventAge),
		"sent_events_total":                boundedUint64ToInt64(heartbeat.Queue.SentEventsTotal),
		"acknowledged_events_total":        boundedUint64ToInt64(heartbeat.Queue.AcknowledgedEventsTotal),
		"retried_batches_total":            boundedUint64ToInt64(heartbeat.Queue.RetriedBatchesTotal),
		"rejected_events_total":            boundedUint64ToInt64(heartbeat.Queue.RejectedEventsTotal),
		"dropped_events_total":             boundedUint64ToInt64(heartbeat.Queue.DroppedEventsTotal),
		"last_sent_batch_sequence":         uint64PointerToInt64(heartbeat.LastSentBatchSequence),
		"last_acknowledged_batch_sequence": uint64PointerToInt64(heartbeat.LastAcknowledgedBatchSequence),
		"process_resident_memory_bytes":    boundedUint64ToInt64(heartbeat.ProcessResidentMemoryBytes),
		"process_cpu_percent":              heartbeat.ProcessCPUPercent,
	}
}

func nextTelemetryRevision(current int64) any {
	if current == math.MaxInt64 {
		return current
	}
	return gorm.Expr("telemetry_revision + 1")
}

func bestEffortInvalidateRuntimeLease(
	tx *gorm.DB,
	scope Scope,
	collectorID string,
	receivedAt time.Time,
) {
	const savepoint = "collector_disable_runtime_cleanup"
	if err := tx.SavePoint(savepoint).Error; err != nil {
		return
	}
	if err := invalidateRuntimeLease(tx, scope, collectorID, receivedAt); err != nil {
		_ = tx.RollbackTo(savepoint).Error
	}
}

func invalidateRuntimeLease(
	tx *gorm.DB,
	scope Scope,
	collectorID string,
	receivedAt time.Time,
) error {
	var runtime runtimeRecord
	if err := tx.
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Take(&runtime).Error; err != nil {
		return mapContextError(
			tx.Statement.Context,
			"read collector runtime for lease invalidation",
			err,
		)
	}
	active, err := validatePersistedRuntimeLease(runtime, scope, collectorID)
	if err != nil {
		return err
	}
	if !active {
		return nil
	}
	disconnectedAt := receivedAt
	if runtime.LastSeenAtUnixMicro > disconnectedAt.UnixMicro() {
		disconnectedAt = time.UnixMicro(runtime.LastSeenAtUnixMicro).UTC()
	}
	update := tx.Model(&runtimeRecord{}).
		Where(
			`tenant_id = ?
			 AND collector_id = ?
			 AND telemetry_revision = ?
			 AND lease_generation = ?
			 AND boot_epoch = ?
			 AND stream_id = ?`,
			scope.TenantID,
			collectorID,
			runtime.TelemetryRevision,
			runtime.LeaseGeneration,
			*runtime.BootEpoch,
			*runtime.StreamID,
		).
		Updates(map[string]any{
			"telemetry_revision":         nextTelemetryRevision(runtime.TelemetryRevision),
			"boot_epoch":                 nil,
			"stream_id":                  nil,
			"active_instance_id":         nil,
			"last_seen_at_unix_micro":    disconnectedAt.UnixMicro(),
			"disconnected_at_unix_micro": disconnectedAt.UnixMicro(),
		})
	if update.Error != nil {
		return mapContextError(
			tx.Statement.Context,
			"invalidate collector runtime lease",
			update.Error,
		)
	}
	if update.RowsAffected != 1 {
		return control.ErrVersionConflict
	}
	return nil
}

func requireInactiveRuntimeLease(
	tx *gorm.DB,
	scope Scope,
	collectorID string,
) error {
	var runtime runtimeRecord
	if err := tx.
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Take(&runtime).Error; err != nil {
		return mapContextError(
			tx.Statement.Context,
			"read collector runtime before enable",
			err,
		)
	}
	active, err := validatePersistedRuntimeLease(runtime, scope, collectorID)
	if err != nil {
		return err
	}
	if active {
		return errors.New(
			"collector runtime still has an active lease in control-plane database",
		)
	}
	return nil
}

func validatePersistedRuntimeLease(
	runtime runtimeRecord,
	scope Scope,
	collectorID string,
) (active bool, err error) {
	if runtime.TenantID != scope.TenantID ||
		runtime.CollectorID != collectorID {
		return false, errors.New(
			"collector runtime lease identity is inconsistent in control-plane database",
		)
	}
	if runtime.TelemetryRevision < 1 ||
		runtime.LeaseGeneration < 1 ||
		runtime.ObservationSequence < 0 {
		return false, errors.New(
			"collector runtime lease revision is invalid in control-plane database",
		)
	}
	if (runtime.ObservationSequence == 0) != (runtime.ObservedAtUnixMicro == nil) {
		return false, errors.New(
			"collector runtime observation snapshot is inconsistent in control-plane database",
		)
	}
	if runtime.ConnectedAtUnixMicro <= 0 ||
		runtime.ConnectedAtUnixMicro > maximumPublicUnixMicro ||
		runtime.LastSeenAtUnixMicro > maximumPublicUnixMicro ||
		runtime.LastSeenAtUnixMicro < runtime.ConnectedAtUnixMicro {
		return false, errors.New(
			"collector runtime lease lifecycle is inconsistent in control-plane database",
		)
	}
	activeLease := runtime.BootEpoch != nil &&
		runtime.StreamID != nil &&
		runtime.ActiveInstanceID != nil &&
		runtime.DisconnectedAtUnixMicro == nil
	inactiveLease := runtime.BootEpoch == nil &&
		runtime.StreamID == nil &&
		runtime.ActiveInstanceID == nil &&
		runtime.DisconnectedAtUnixMicro != nil
	if !activeLease && !inactiveLease {
		return false, errors.New(
			"collector runtime lease is inconsistent in control-plane database",
		)
	}
	if inactiveLease {
		if *runtime.DisconnectedAtUnixMicro <= 0 ||
			*runtime.DisconnectedAtUnixMicro > maximumPublicUnixMicro ||
			*runtime.DisconnectedAtUnixMicro < runtime.LastSeenAtUnixMicro {
			return false, errors.New(
				"collector runtime disconnect time is inconsistent in control-plane database",
			)
		}
		return false, nil
	}
	if !validIdentifier(*runtime.BootEpoch) ||
		!validIdentifier(*runtime.StreamID) ||
		!validIdentifier(*runtime.ActiveInstanceID) {
		return false, errors.New(
			"collector runtime active lease is invalid in control-plane database",
		)
	}
	return true, nil
}

func replaceInputHealth(tx *gorm.DB, lease Lease, inputs []InputHealth) error {
	if err := tx.
		Where(
			"tenant_id = ? AND collector_id = ?",
			lease.TenantID,
			lease.CollectorID,
		).
		Delete(&inputHealthRecord{}).Error; err != nil {
		return mapContextError(tx.Statement.Context, "replace collector input health", err)
	}
	records := make([]inputHealthRecord, len(inputs))
	for index, input := range inputs {
		records[index] = inputHealthRecord{
			TenantID: lease.TenantID, CollectorID: lease.CollectorID,
			InputID: input.InputID, State: int64(input.State),
			StatusMessage:        input.StatusMessage,
			DiscoveredSources:    boundedUint64ToInt64(input.DiscoveredSources),
			ActiveSources:        boundedUint64ToInt64(input.ActiveSources),
			EventsReadTotal:      boundedUint64ToInt64(input.EventsReadTotal),
			BytesReadTotal:       boundedUint64ToInt64(input.BytesReadTotal),
			LastEventAtUnixMicro: timePointerToUnixMicro(input.LastEventAt),
			LastErrorAtUnixMicro: timePointerToUnixMicro(input.LastErrorAt),
		}
	}
	if len(records) != 0 {
		if err := tx.Create(&records).Error; err != nil {
			return mapContextError(tx.Statement.Context, "store collector input health", err)
		}
	}
	return nil
}

func takeCurrentLease(
	tx *gorm.DB,
	lease Lease,
) (runtimeRecord, bool, error) {
	var fleet fleetRecord
	fleetErr := tx.
		Select("admin_version", "administrative_state").
		Where("tenant_id = ? AND collector_id = ?", lease.TenantID, lease.CollectorID).
		Take(&fleet).Error
	if errors.Is(fleetErr, gorm.ErrRecordNotFound) {
		return runtimeRecord{}, false, nil
	}
	if fleetErr != nil {
		return runtimeRecord{}, false, mapContextError(
			tx.Statement.Context,
			"read collector state at lease boundary",
			fleetErr,
		)
	}
	if fleet.AdminVersion < 1 {
		return runtimeRecord{}, false, errors.New(
			"read collector state at lease boundary: invalid administrator version in control-plane database",
		)
	}
	if fleet.AdministrativeState != AdministrativeStateEnabled {
		if fleet.AdministrativeState == AdministrativeStateDisabled {
			return runtimeRecord{}, false, nil
		}
		return runtimeRecord{}, false, errors.New(
			"read collector state at lease boundary: invalid administrative state in control-plane database",
		)
	}
	var runtime runtimeRecord
	runtimeErr := tx.
		Where("tenant_id = ? AND collector_id = ?", lease.TenantID, lease.CollectorID).
		Take(&runtime).Error
	if errors.Is(runtimeErr, gorm.ErrRecordNotFound) {
		return runtimeRecord{}, false, errors.New(
			"read collector runtime at lease boundary: missing runtime in control-plane database",
		)
	}
	if runtimeErr != nil {
		return runtimeRecord{}, false, mapContextError(
			tx.Statement.Context,
			"read collector runtime at lease boundary",
			runtimeErr,
		)
	}
	activeLease, err := validatePersistedRuntimeLease(
		runtime,
		lease.Scope,
		lease.CollectorID,
	)
	if err != nil {
		return runtimeRecord{}, false, fmt.Errorf(
			"read collector runtime at lease boundary: %w",
			err,
		)
	}
	if !activeLease {
		return runtime, false, nil
	}
	if *runtime.BootEpoch != lease.BootEpoch ||
		*runtime.StreamID != lease.StreamID ||
		runtime.LeaseGeneration != boundedUint64ToInt64(lease.Generation) {
		return runtime, false, nil
	}
	return runtime, true, nil
}

func registeredInputSet(
	tx *gorm.DB,
	scope Scope,
	collectorID string,
) (map[string]struct{}, error) {
	var records []inputRecord
	if err := tx.
		Select("input_id").
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Limit(maximumInputs + 1).
		Find(&records).Error; err != nil {
		return nil, mapContextError(tx.Statement.Context, "read registered collector inputs", err)
	}
	if len(records) > maximumInputs {
		return nil, errors.New("registered collector inputs exceed persisted bounds")
	}
	result := make(map[string]struct{}, len(records))
	for _, record := range records {
		result[record.InputID] = struct{}{}
	}
	return result, nil
}

func loadCollector(
	database *gorm.DB,
	scope Scope,
	collectorID string,
) (Collector, error) {
	var fleet fleetRecord
	queryErr := database.
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Take(&fleet).Error
	if errors.Is(queryErr, gorm.ErrRecordNotFound) {
		return Collector{}, control.ErrNotFound
	}
	if queryErr != nil {
		return Collector{}, mapContextError(database.Statement.Context, "get collector identity", queryErr)
	}
	var runtime runtimeRecord
	if err := database.
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Take(&runtime).Error; err != nil {
		return Collector{}, mapContextError(database.Statement.Context, "get collector runtime", err)
	}
	var capabilities []capabilityRecord
	if err := database.
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Order("capability").
		Limit(maximumCapabilities + 1).
		Find(&capabilities).Error; err != nil {
		return Collector{}, mapContextError(database.Statement.Context, "get collector capabilities", err)
	}
	var indexes []authorizedIndexRecord
	if err := database.
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Order("index_name").
		Limit(maximumAuthorizedIndexes + 1).
		Find(&indexes).Error; err != nil {
		return Collector{}, mapContextError(database.Statement.Context, "get collector authorized indexes", err)
	}
	var inputs []inputRecord
	if err := database.
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Order("input_id").
		Limit(maximumInputs + 1).
		Find(&inputs).Error; err != nil {
		return Collector{}, mapContextError(database.Statement.Context, "get collector inputs", err)
	}
	var health []inputHealthRecord
	if err := database.
		Where("tenant_id = ? AND collector_id = ?", scope.TenantID, collectorID).
		Order("input_id").
		Limit(maximumInputs + 1).
		Find(&health).Error; err != nil {
		return Collector{}, mapContextError(database.Statement.Context, "get collector input health", err)
	}
	if len(capabilities) > maximumCapabilities ||
		len(indexes) > maximumAuthorizedIndexes ||
		len(inputs) > maximumInputs ||
		len(health) > maximumInputs {
		return Collector{}, errors.New("collector child snapshot exceeds persisted bounds")
	}
	return collectorFromRecords(fleet, runtime, capabilities, indexes, inputs, health)
}

func collectorFromRecords(
	fleet fleetRecord,
	runtime runtimeRecord,
	capabilityRows []capabilityRecord,
	indexRows []authorizedIndexRecord,
	inputRows []inputRecord,
	healthRows []inputHealthRecord,
) (Collector, error) {
	if fleet.AdminVersion < 1 ||
		runtime.TelemetryRevision < 1 ||
		runtime.LeaseGeneration < 1 ||
		runtime.ObservationSequence < 0 {
		return Collector{}, errors.New("invalid collector revision in control-plane database")
	}
	normalizedScope, scopeErr := normalizeScope(Scope{TenantID: fleet.TenantID})
	if scopeErr != nil || normalizedScope.TenantID != fleet.TenantID ||
		!validIdentifier(fleet.CollectorID) {
		return Collector{}, errors.New("invalid collector identity in control-plane database")
	}
	if fleet.AdministrativeState != AdministrativeStateEnabled &&
		fleet.AdministrativeState != AdministrativeStateDisabled {
		return Collector{}, errors.New("invalid collector administrative state in control-plane database")
	}
	firstSeen := time.UnixMicro(fleet.FirstSeenAtUnixMicro).UTC()
	updatedAt := time.UnixMicro(fleet.UpdatedAtUnixMicro).UTC()
	startedAt := time.UnixMicro(runtime.StartedAtUnixMicro).UTC()
	connectedAt := time.UnixMicro(runtime.ConnectedAtUnixMicro).UTC()
	lastSeenAt := time.UnixMicro(runtime.LastSeenAtUnixMicro).UTC()
	for name, value := range map[string]time.Time{
		"first seen": firstSeen,
		"updated":    updatedAt,
		"started":    startedAt,
		"connected":  connectedAt,
		"last seen":  lastSeenAt,
	} {
		if !timestampIsPublic(value) {
			return Collector{}, fmt.Errorf("invalid collector %s time in control-plane database", name)
		}
	}
	if updatedAt.Before(firstSeen) ||
		lastSeenAt.Before(connectedAt) {
		return Collector{}, errors.New("inconsistent collector lifecycle time in control-plane database")
	}
	if fleet.DisplayName != nil {
		normalizedAdministration, err := normalizeAdministration(Administration{
			DisplayName: fleet.DisplayName,
			State:       fleet.AdministrativeState,
		})
		if err != nil ||
			normalizedAdministration.DisplayName == nil ||
			*normalizedAdministration.DisplayName != *fleet.DisplayName {
			return Collector{}, errors.New("invalid collector display name in control-plane database")
		}
	}
	if runtime.ProtocolMajor < 0 || runtime.ProtocolMajor > math.MaxUint32 ||
		runtime.ProtocolMinor < 0 || runtime.ProtocolMinor > math.MaxUint32 {
		return Collector{}, errors.New("invalid collector protocol version in control-plane database")
	}
	helloSnapshotBytes := 0
	for _, metadata := range []struct {
		name    string
		value   string
		maximum int
	}{
		{"collector version", runtime.CollectorVersion, maximumCollectorVersionBytes},
		{"hostname", runtime.Hostname, maximumHostnameBytes},
		{"operating system", runtime.OperatingSystem, maximumOperatingSystemBytes},
		{"architecture", runtime.Architecture, maximumArchitectureBytes},
	} {
		if err := validateText(metadata.name, metadata.value, metadata.maximum, true); err != nil {
			return Collector{}, errors.New("invalid collector hello metadata in control-plane database")
		}
		if !addBoundedBytes(
			&helloSnapshotBytes,
			len(metadata.value),
			maximumHelloSnapshotBytes,
		) {
			return Collector{}, errors.New("collector hello snapshot exceeds persisted bounds")
		}
	}
	for _, optional := range []*int64{
		runtime.LastAckedAtHelloSequence,
		runtime.OldestEventAgeNanoseconds,
		runtime.LastSentBatchSequence,
		runtime.LastAcknowledgedBatchSequence,
	} {
		if optional != nil && *optional < 0 {
			return Collector{}, errors.New("negative collector telemetry in control-plane database")
		}
	}
	for _, value := range []int64{
		runtime.QueuedEvents,
		runtime.QueuedBytes,
		runtime.SentEventsTotal,
		runtime.AcknowledgedEventsTotal,
		runtime.RetriedBatchesTotal,
		runtime.RejectedEventsTotal,
		runtime.DroppedEventsTotal,
		runtime.ProcessResidentMemoryBytes,
	} {
		if value < 0 {
			return Collector{}, errors.New("negative collector telemetry in control-plane database")
		}
	}
	result := Collector{
		TenantID: fleet.TenantID, CollectorID: fleet.CollectorID,
		Version:             uint64(fleet.AdminVersion),
		DisplayName:         cloneString(fleet.DisplayName),
		AdministrativeState: fleet.AdministrativeState,
		FirstSeenAt:         firstSeen, UpdatedAt: updatedAt,
		TelemetryRevision: uint64(runtime.TelemetryRevision),
		LeaseGeneration:   uint64(runtime.LeaseGeneration),
		ProtocolMajor:     uint32(runtime.ProtocolMajor),
		ProtocolMinor:     uint32(runtime.ProtocolMinor),
		CollectorVersion:  runtime.CollectorVersion,
		Hostname:          runtime.Hostname,
		OperatingSystem:   runtime.OperatingSystem,
		Architecture:      runtime.Architecture,
		StartedAt:         startedAt, ConnectedAt: connectedAt, LastSeenAt: lastSeenAt,
		ObservationSequence:     uint64(runtime.ObservationSequence),
		LastAcknowledgedAtHello: int64PointerToUint64(runtime.LastAckedAtHelloSequence),
		Queue: QueueTelemetry{
			QueuedEvents:            nonnegativeInt64ToUint64(runtime.QueuedEvents),
			QueuedBytes:             nonnegativeInt64ToUint64(runtime.QueuedBytes),
			OldestEventAge:          int64PointerToDuration(runtime.OldestEventAgeNanoseconds),
			SentEventsTotal:         nonnegativeInt64ToUint64(runtime.SentEventsTotal),
			AcknowledgedEventsTotal: nonnegativeInt64ToUint64(runtime.AcknowledgedEventsTotal),
			RetriedBatchesTotal:     nonnegativeInt64ToUint64(runtime.RetriedBatchesTotal),
			RejectedEventsTotal:     nonnegativeInt64ToUint64(runtime.RejectedEventsTotal),
			DroppedEventsTotal:      nonnegativeInt64ToUint64(runtime.DroppedEventsTotal),
		},
		LastSentBatchSequence:         int64PointerToUint64(runtime.LastSentBatchSequence),
		LastAcknowledgedBatchSequence: int64PointerToUint64(runtime.LastAcknowledgedBatchSequence),
		ProcessResidentMemoryBytes:    nonnegativeInt64ToUint64(runtime.ProcessResidentMemoryBytes),
		ProcessCPUPercent:             runtime.ProcessCPUPercent,
	}
	if runtime.ObservedAtUnixMicro != nil {
		result.ObservedAt = time.UnixMicro(*runtime.ObservedAtUnixMicro).UTC()
		if !timestampIsPublic(result.ObservedAt) {
			return Collector{}, errors.New("invalid collector observed time in control-plane database")
		}
	}
	if (runtime.ObservationSequence == 0) != (runtime.ObservedAtUnixMicro == nil) {
		return Collector{}, errors.New(
			"inconsistent collector observation snapshot in control-plane database",
		)
	}
	if runtime.DisconnectedAtUnixMicro != nil {
		value := time.UnixMicro(*runtime.DisconnectedAtUnixMicro).UTC()
		if value.Before(lastSeenAt) || !timestampIsPublic(value) {
			return Collector{}, errors.New("invalid collector disconnect time in control-plane database")
		}
		result.DisconnectedAt = &value
	}
	switch {
	case runtime.BootEpoch != nil &&
		runtime.StreamID != nil &&
		runtime.ActiveInstanceID != nil &&
		runtime.DisconnectedAtUnixMicro == nil:
		if !validIdentifier(*runtime.BootEpoch) ||
			!validIdentifier(*runtime.StreamID) ||
			!validIdentifier(*runtime.ActiveInstanceID) {
			return Collector{}, errors.New("invalid active collector lease in control-plane database")
		}
		result.ActiveLease = &ActiveLease{
			BootEpoch: *runtime.BootEpoch, StreamID: *runtime.StreamID,
			InstanceID: *runtime.ActiveInstanceID,
			Generation: uint64(runtime.LeaseGeneration),
		}
	case runtime.BootEpoch == nil &&
		runtime.StreamID == nil &&
		runtime.ActiveInstanceID == nil &&
		runtime.DisconnectedAtUnixMicro != nil:
	default:
		return Collector{}, errors.New("inconsistent collector lease in control-plane database")
	}
	if fleet.AdministrativeState == AdministrativeStateDisabled &&
		result.ActiveLease != nil {
		return Collector{}, errors.New(
			"disabled collector has an active lease in control-plane database",
		)
	}
	if math.IsNaN(runtime.ProcessCPUPercent) ||
		math.IsInf(runtime.ProcessCPUPercent, 0) ||
		runtime.ProcessCPUPercent < 0 ||
		runtime.ProcessCPUPercent > 1_000_000 {
		return Collector{}, errors.New("invalid collector CPU telemetry in control-plane database")
	}
	if !addBoundedBytes(
		&helloSnapshotBytes,
		len(capabilityRows)*8,
		maximumHelloSnapshotBytes,
	) {
		return Collector{}, errors.New("collector hello snapshot exceeds persisted bounds")
	}
	for _, row := range capabilityRows {
		if row.Capability < 1 || row.Capability > int64(maximumPersistedEnumValue) {
			return Collector{}, errors.New("invalid collector capability in control-plane database")
		}
		result.Capabilities = append(result.Capabilities, uint32(row.Capability))
	}
	if len(indexRows) == 0 {
		return Collector{}, errors.New(
			"collector authorized index snapshot is empty in control-plane database",
		)
	}
	authorizedIndexes := make(map[string]struct{}, len(indexRows))
	for _, row := range indexRows {
		canonical, err := control.NormalizeIndexName(row.IndexName)
		if err != nil || canonical != row.IndexName {
			return Collector{}, errors.New("invalid collector authorized index in control-plane database")
		}
		if !addBoundedBytes(
			&helloSnapshotBytes,
			len(row.IndexName),
			maximumHelloSnapshotBytes,
		) {
			return Collector{}, errors.New("collector hello snapshot exceeds persisted bounds")
		}
		authorizedIndexes[row.IndexName] = struct{}{}
		result.AuthorizedIndexes = append(result.AuthorizedIndexes, row.IndexName)
	}
	registeredInputs := make(map[string]struct{}, len(inputRows))
	for _, row := range inputRows {
		if !validIdentifier(row.InputID) ||
			row.InputType < 1 ||
			row.InputType > int64(maximumPersistedEnumValue) {
			return Collector{}, errors.New("invalid collector input in control-plane database")
		}
		canonicalIndex, err := control.NormalizeIndexName(row.IndexName)
		if err != nil || canonicalIndex != row.IndexName {
			return Collector{}, errors.New("invalid collector input index in control-plane database")
		}
		if _, authorized := authorizedIndexes[row.IndexName]; !authorized {
			return Collector{}, errors.New("collector input index is outside its authorized snapshot")
		}
		if _, err := normalizeOptionalText("input source", row.Source, maximumSourceBytes); err != nil {
			return Collector{}, errors.New("invalid collector input source in control-plane database")
		}
		if _, err := normalizeOptionalText(
			"input sourcetype",
			row.Sourcetype,
			maximumSourcetypeBytes,
		); err != nil {
			return Collector{}, errors.New("invalid collector input sourcetype in control-plane database")
		}
		recordBytes := len(row.InputID) + len(row.IndexName) + 16
		if row.Source != nil {
			recordBytes += len(*row.Source)
		}
		if row.Sourcetype != nil {
			recordBytes += len(*row.Sourcetype)
		}
		if !addBoundedBytes(
			&helloSnapshotBytes,
			recordBytes,
			maximumHelloSnapshotBytes,
		) {
			return Collector{}, errors.New("collector hello snapshot exceeds persisted bounds")
		}
		registeredInputs[row.InputID] = struct{}{}
		result.Inputs = append(result.Inputs, InputRegistration{
			InputID: row.InputID, InputType: uint32(row.InputType),
			IndexName: row.IndexName,
			Source:    cloneString(row.Source), Sourcetype: cloneString(row.Sourcetype),
		})
	}
	heartbeatSnapshotBytes := 128
	for _, row := range healthRows {
		if !validIdentifier(row.InputID) ||
			row.State < 1 ||
			row.State > int64(maximumPersistedEnumValue) ||
			row.DiscoveredSources < 0 ||
			row.ActiveSources < 0 ||
			row.ActiveSources > row.DiscoveredSources ||
			row.EventsReadTotal < 0 ||
			row.BytesReadTotal < 0 {
			return Collector{}, errors.New("invalid collector input health in control-plane database")
		}
		if _, registered := registeredInputs[row.InputID]; !registered {
			return Collector{}, errors.New("collector input health references an unregistered input")
		}
		if err := validateText(
			"input health status message",
			row.StatusMessage,
			maximumStatusMessageBytes,
			true,
		); err != nil {
			return Collector{}, errors.New("invalid collector input health message in control-plane database")
		}
		if !addBoundedBytes(
			&heartbeatSnapshotBytes,
			len(row.InputID)+len(row.StatusMessage)+64,
			maximumHeartbeatSnapshotBytes,
		) {
			return Collector{}, errors.New("collector heartbeat snapshot exceeds persisted bounds")
		}
		for _, observedAt := range []*int64{
			row.LastEventAtUnixMicro,
			row.LastErrorAtUnixMicro,
		} {
			if observedAt != nil &&
				(*observedAt <= 0 || *observedAt > maximumPublicUnixMicro) {
				return Collector{}, errors.New("invalid collector input health time in control-plane database")
			}
		}
		result.InputHealth = append(result.InputHealth, InputHealth{
			InputID: row.InputID, State: uint32(row.State),
			StatusMessage:     row.StatusMessage,
			DiscoveredSources: uint64(row.DiscoveredSources),
			ActiveSources:     uint64(row.ActiveSources),
			EventsReadTotal:   uint64(row.EventsReadTotal),
			BytesReadTotal:    uint64(row.BytesReadTotal),
			LastEventAt:       unixMicroPointerToTime(row.LastEventAtUnixMicro),
			LastErrorAt:       unixMicroPointerToTime(row.LastErrorAtUnixMicro),
		})
	}
	return result, nil
}

func finishGORMTransaction(
	tx *gorm.DB,
	finished *bool,
	returnedErr *error,
) {
	if tx == nil || finished == nil || *finished {
		return
	}
	rollbackErr := tx.Rollback().Error
	*finished = true
	if rollbackErr == nil {
		return
	}
	wrapped := fmt.Errorf("roll back fleet transaction: %w", rollbackErr)
	if returnedErr != nil && *returnedErr != nil {
		*returnedErr = errors.Join(*returnedErr, wrapped)
		return
	}
	if returnedErr != nil {
		*returnedErr = wrapped
	}
}

func cloneString(input *string) *string {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func uint64PointerToInt64(input *uint64) *int64 {
	if input == nil {
		return nil
	}
	value := boundedUint64ToInt64(*input)
	return &value
}

func int64PointerToUint64(input *int64) *uint64 {
	if input == nil || *input < 0 {
		return nil
	}
	value := uint64(*input)
	return &value
}

func durationPointerToInt64(input *time.Duration) *int64 {
	if input == nil {
		return nil
	}
	value := int64(*input)
	return &value
}

func int64PointerToDuration(input *int64) *time.Duration {
	if input == nil || *input < 0 {
		return nil
	}
	value := time.Duration(*input)
	return &value
}

func timePointerToUnixMicro(input *time.Time) *int64 {
	if input == nil {
		return nil
	}
	value := input.UnixMicro()
	return &value
}

func unixMicroPointerToTime(input *int64) *time.Time {
	if input == nil {
		return nil
	}
	value := time.UnixMicro(*input).UTC()
	return &value
}

func boundedUint64ToInt64(value uint64) int64 {
	// #nosec G115 -- every caller first bounds the value by math.MaxInt64.
	return int64(value)
}

func nonnegativeInt64ToUint64(value int64) uint64 {
	// #nosec G115 -- persisted values are checked nonnegative before conversion.
	return uint64(value)
}
