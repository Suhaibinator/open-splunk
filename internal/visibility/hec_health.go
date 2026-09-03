package visibility

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"fortio.org/safecast"
)

// HECReadinessSnapshot is the constant-shape projection used by the public
// health endpoint. It deliberately excludes counts and identifiers.
type HECReadinessSnapshot struct {
	QueueAvailable          bool
	AcknowledgmentAvailable bool
}

// HECOperationalSnapshot is an administrator-only aggregate projection. It
// contains no tenant, token, channel, index, or request identifiers.
type HECOperationalSnapshot struct {
	QueueAvailable            bool
	AcknowledgmentAvailable   bool
	PendingOutboxReservations uint64
	PendingOutboxBytes        uint64
	OldestPendingOutboxAge    time.Duration
	RequestCapacityAvailable  bool
	RetainedRequests          uint64
	ActiveChannels            uint64
	RetainedChannels          uint64
	PendingAcknowledgments    uint64
	IndexedAcknowledgments    uint64
	ExpiredAcknowledgments    uint64
	TerminalFailedRequests    uint64
}

// HECReadiness performs only the bounded pending-usage read and an indexed
// existence probe for unresolved terminal HEC failures. Public health probes
// therefore do not count retained channel or acknowledgment history.
func (sequencer *SQLiteSequencer) HECReadiness(
	ctx context.Context,
) (snapshot HECReadinessSnapshot, resultErr error) {
	if err := sequencer.beginOperation(); err != nil {
		return HECReadinessSnapshot{}, err
	}
	defer sequencer.endOperation()
	if err := validateContext(ctx); err != nil {
		return HECReadinessSnapshot{}, err
	}
	return readHECReadiness(ctx, sequencer.db)
}

func readHECReadiness(ctx context.Context, database *sql.DB) (HECReadinessSnapshot, error) {
	usage, err := readPendingUsage(ctx, database)
	if err != nil {
		return HECReadinessSnapshot{}, err
	}
	acknowledgmentAvailable, err := readHECAcknowledgmentAvailability(ctx, database)
	if err != nil {
		return HECReadinessSnapshot{}, err
	}
	return HECReadinessSnapshot{
		QueueAvailable: usage.Reservations < MaxPendingReservations &&
			usage.OutboxBytes < MaxPendingOutboxBytes &&
			usage.MetadataBytes < MaxPendingMetadataBytes,
		AcknowledgmentAvailable: acknowledgmentAvailable,
	}, nil
}

func readHECAcknowledgmentAvailability(ctx context.Context, database queryer) (bool, error) {
	var terminalFailureExists int
	if err := database.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM hec_requests
			WHERE state = 'terminal_failure'
			LIMIT 1
		)`).Scan(&terminalFailureExists); err != nil {
		return false, fmt.Errorf("read HEC acknowledgment readiness: %w", err)
	}
	return terminalFailureExists == 0, nil
}

// HECOperationalHealth reads bounded aggregate state without performing a
// ClickHouse write. Any corrupt count or query failure fails the observation
// closed and returns an error.
func (sequencer *SQLiteSequencer) HECOperationalHealth(
	ctx context.Context,
) (snapshot HECOperationalSnapshot, resultErr error) {
	if err := sequencer.beginOperation(); err != nil {
		return HECOperationalSnapshot{}, err
	}
	defer sequencer.endOperation()
	if err := validateContext(ctx); err != nil {
		return HECOperationalSnapshot{}, err
	}
	// Every field belongs to one SQLite read snapshot. Without this boundary,
	// reconciliation or terminal pruning between queries could publish
	// contradictory counts and availability bits under one observed_at value.
	tx, err := sequencer.db.BeginTx(
		ctx,
		&sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true},
	)
	if err != nil {
		return HECOperationalSnapshot{}, fmt.Errorf("begin HEC operational health read: %w", err)
	}
	defer rollback(tx)
	usage, err := readPendingUsage(ctx, tx)
	if err != nil {
		return HECOperationalSnapshot{}, err
	}
	snapshot.PendingOutboxReservations = uint64(usage.Reservations)
	snapshot.PendingOutboxBytes = usage.OutboxBytes
	snapshot.QueueAvailable = usage.Reservations < MaxPendingReservations &&
		usage.OutboxBytes < MaxPendingOutboxBytes &&
		usage.MetadataBytes < MaxPendingMetadataBytes
	observedAt := time.Now().UTC()
	if sequencer.now != nil {
		observedAt = sequencer.now().Round(0).UTC()
	}
	acknowledgmentAvailable, err := readHECAcknowledgmentAvailability(ctx, tx)
	if err != nil {
		return HECOperationalSnapshot{}, err
	}
	terminalBefore := observedAt.Add(-HECTerminalRetention).UnixMicro()
	var oldestPending sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT min(created_at_unix_micro)
		FROM ingest_visibility_reservations
		WHERE state = 'reserved'`,
	).Scan(&oldestPending); err != nil {
		return HECOperationalSnapshot{}, fmt.Errorf("read oldest pending HEC outbox: %w", err)
	}
	if oldestPending.Valid {
		if oldestPending.Int64 <= 0 {
			return HECOperationalSnapshot{}, fmt.Errorf("invalid oldest pending HEC outbox timestamp")
		}
		oldestAt := time.UnixMicro(oldestPending.Int64)
		if observedAt.After(oldestAt) {
			snapshot.OldestPendingOutboxAge = observedAt.Sub(oldestAt)
		}
	}
	var retainedRequests, requestCapacityAvailable int64
	var channels, activeChannels, pending, indexed, expired, failed int64
	if err := tx.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM hec_requests),
			(SELECT NOT EXISTS (
				 SELECT 1
				 FROM hec_requests
				 GROUP BY tenant_id, ingestion_token_id
				 HAVING count(*) >= ?
				 LIMIT 1
			 )),
			(SELECT count(*) FROM hec_channels),
			(SELECT count(*)
			 FROM hec_channels AS channel
			 WHERE EXISTS (
				 SELECT 1
				 FROM hec_acknowledgments AS acknowledgment
				 JOIN hec_requests AS request
				   ON request.tenant_id = acknowledgment.tenant_id
				  AND request.ingestion_token_id = acknowledgment.ingestion_token_id
				  AND request.request_sequence = acknowledgment.request_sequence
				 WHERE acknowledgment.tenant_id = channel.tenant_id
				   AND acknowledgment.ingestion_token_id = channel.ingestion_token_id
				   AND acknowledgment.channel_id = channel.channel_id
				   AND (request.state = 'pending'
				        OR request.terminal_at_unix_micro >= ?)
			 )),
			(SELECT count(*)
			 FROM hec_acknowledgments AS acknowledgment
			 JOIN hec_requests AS request
			   ON request.tenant_id = acknowledgment.tenant_id
			  AND request.ingestion_token_id = acknowledgment.ingestion_token_id
			  AND request.request_sequence = acknowledgment.request_sequence
			 WHERE request.state = 'pending'),
			(SELECT count(*)
			 FROM hec_acknowledgments AS acknowledgment
			 JOIN hec_requests AS request
			   ON request.tenant_id = acknowledgment.tenant_id
			  AND request.ingestion_token_id = acknowledgment.ingestion_token_id
			  AND request.request_sequence = acknowledgment.request_sequence
			 WHERE request.state = 'indexed'
			   AND request.terminal_at_unix_micro >= ?),
			(SELECT count(*)
			 FROM hec_acknowledgments AS acknowledgment
			 JOIN hec_requests AS request
			   ON request.tenant_id = acknowledgment.tenant_id
			  AND request.ingestion_token_id = acknowledgment.ingestion_token_id
			  AND request.request_sequence = acknowledgment.request_sequence
			 WHERE request.state IN ('indexed', 'terminal_failure')
			   AND request.terminal_at_unix_micro < ?),
			(SELECT count(*) FROM hec_requests WHERE state = 'terminal_failure')`,
		MaxHECRequestsPerToken,
		terminalBefore,
		terminalBefore,
		terminalBefore,
	).Scan(
		&retainedRequests,
		&requestCapacityAvailable,
		&channels,
		&activeChannels,
		&pending,
		&indexed,
		&expired,
		&failed,
	); err != nil {
		return HECOperationalSnapshot{}, fmt.Errorf("read HEC operational health: %w", err)
	}
	if retainedRequests < 0 || requestCapacityAvailable < 0 || requestCapacityAvailable > 1 ||
		channels < 0 || activeChannels < 0 || pending < 0 || indexed < 0 ||
		expired < 0 || failed < 0 || activeChannels > channels {
		return HECOperationalSnapshot{}, fmt.Errorf("invalid HEC operational aggregate")
	}
	snapshot.RetainedRequests = safecast.MustConv[uint64](retainedRequests)
	snapshot.RequestCapacityAvailable = requestCapacityAvailable == 1
	snapshot.ActiveChannels = safecast.MustConv[uint64](activeChannels)
	snapshot.RetainedChannels = safecast.MustConv[uint64](channels)
	snapshot.PendingAcknowledgments = safecast.MustConv[uint64](pending)
	snapshot.IndexedAcknowledgments = safecast.MustConv[uint64](indexed)
	snapshot.ExpiredAcknowledgments = safecast.MustConv[uint64](expired)
	snapshot.TerminalFailedRequests = safecast.MustConv[uint64](failed)
	snapshot.AcknowledgmentAvailable = acknowledgmentAvailable
	if err := tx.Commit(); err != nil {
		return HECOperationalSnapshot{}, fmt.Errorf("commit HEC operational health read: %w", err)
	}
	return snapshot, nil
}
