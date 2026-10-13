package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// NotificationIdentityMatches reports whether recorded transaction metadata
// belongs to the running database incarnation. An unreadable system identifier
// falls back to the epoch; absent metadata is deliberately not a valid fence.
func NotificationIdentityMatches(current, recorded models.NotificationTransactionIdentity) bool {
	return current.Epoch != 0 && current.Epoch == recorded.Epoch &&
		(current.SystemIdentifier == "" || recorded.SystemIdentifier == "" || current.SystemIdentifier == recorded.SystemIdentifier)
}

// CaptureNotificationTransaction captures the top-level xid and the statement's
// visibility snapshot in the caller's transaction. It does not open or commit a
// transaction and never writes the bootstrap singleton.
func CaptureNotificationTransaction(ctx context.Context, db *gorm.DB, identity models.NotificationTransactionIdentity) (models.NotificationTransactionStamp, models.NotificationVisibilitySnapshot, error) {
	var result struct {
		XID        string `gorm:"column:xid"`
		Snapshot   string
		UpperBound string
		OwnOrdinal uint64
	}
	err := db.WithContext(ctx).Raw(notificationSnapshotCaptureSQL).Scan(&result).Error
	return models.NotificationTransactionStamp{NotificationTransactionIdentity: identity, XID: result.XID, Ordinal: result.OwnOrdinal},
		models.NotificationVisibilitySnapshot{NotificationTransactionIdentity: identity, Snapshot: result.Snapshot, UpperBound: result.UpperBound, OwnXID: result.XID, OwnOrdinal: result.OwnOrdinal}, err
}

// CaptureNotificationSnapshot must be called in a separate statement after the
// recipient/type advisory lock is acquired. READ COMMITTED makes that statement
// observe commits that became visible while waiting for the lock.
func CaptureNotificationSnapshot(ctx context.Context, db *gorm.DB, identity models.NotificationTransactionIdentity) (models.NotificationVisibilitySnapshot, error) {
	_, snapshot, err := CaptureNotificationTransaction(ctx, db, identity)
	return snapshot, err
}

// PostgreSQL omits its own XID from snapshot xip. When another transaction has
// committed a larger XID, pg_visible_in_snapshot can therefore call the still
// uncommitted owner visible. Exclude it explicitly, and use a transaction-local
// ordinal for notification operations within that owner. SET LOCAL state rolls
// back with savepoints and disappears on commit; no cross-transaction clock or
// global sequence is involved.
const notificationSnapshotCaptureSQL = `WITH operation AS MATERIALIZED (
 SELECT set_config('gocommon.notification_operation_ordinal',
  (COALESCE(NULLIF(current_setting('gocommon.notification_operation_ordinal',true),''),'0')::bigint + 1)::text,true)::bigint AS ordinal
), captured AS MATERIALIZED (
 SELECT pg_current_xact_id() AS own_xid,pg_current_snapshot() AS snapshot,ordinal FROM operation
), normalized AS (
 SELECT own_xid,ordinal,CASE WHEN own_xid < pg_snapshot_xmax(snapshot)
 THEN (pg_snapshot_xmin(snapshot)::text || ':' || pg_snapshot_xmax(snapshot)::text || ':' ||
  (SELECT string_agg(x::text,',' ORDER BY x) FROM (SELECT pg_snapshot_xip(snapshot) x UNION SELECT own_xid) all_xids))::pg_snapshot
 ELSE snapshot END AS snapshot FROM captured
) SELECT own_xid::text AS xid,ordinal AS own_ordinal,snapshot::text AS snapshot,
 pg_snapshot_xmax(snapshot)::text AS upper_bound FROM normalized`

// notificationFenceComparable reports whether a stamp and snapshot both belong
// to the running database incarnation and to each other. Foreign or unstamped
// metadata is a baseline, never a PostgreSQL transaction identity, so it must
// not reach pg_visible_in_snapshot. False means no applicable fence, not a claim
// about numeric order.
func notificationFenceComparable(current models.NotificationTransactionIdentity, stamp models.NotificationTransactionStamp, snapshot models.NotificationVisibilitySnapshot) bool {
	return stamp.XID != "" && stamp.XID != "0" && snapshot.Snapshot != "" &&
		NotificationIdentityMatches(current, stamp.NotificationTransactionIdentity) &&
		NotificationIdentityMatches(current, snapshot.NotificationTransactionIdentity) &&
		NotificationIdentityMatches(stamp.NotificationTransactionIdentity, snapshot.NotificationTransactionIdentity)
}

// NotificationTransactionIdentity returns the immutable bootstrap identity.
// A zero identity on a manually constructed handler means unstamped baseline.
func (h *CommonPersistenceHandler[U]) NotificationTransactionIdentity() models.NotificationTransactionIdentity {
	return h.notificationTransactionIdentity
}

func (h *CommonPersistenceHandler[U]) CaptureNotificationTransaction(ctx context.Context) (models.NotificationTransactionStamp, models.NotificationVisibilitySnapshot, error) {
	return CaptureNotificationTransaction(ctx, h.DB(ctx), h.notificationTransactionIdentity)
}

func (h *CommonPersistenceHandler[U]) CaptureNotificationSnapshot(ctx context.Context) (models.NotificationVisibilitySnapshot, error) {
	return CaptureNotificationSnapshot(ctx, h.DB(ctx), h.notificationTransactionIdentity)
}

type notificationIdentityProvider interface {
	NotificationTransactionIdentity() models.NotificationTransactionIdentity
}

// CurrentNotificationIdentity uses a bootstrap cache when the provider has one.
// Alternate GORM providers may use the singleton directly without mutating it.
func (s *GormNotificationStore[U]) CurrentNotificationIdentity(ctx context.Context) (models.NotificationTransactionIdentity, error) {
	if provider, ok := s.db.(notificationIdentityProvider); ok {
		return provider.NotificationTransactionIdentity(), nil
	}
	var epoch models.NotificationTransactionEpoch
	err := s.db.DB(ctx).First(&epoch, 1).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return models.NotificationTransactionIdentity{}, nil
	}
	return models.NotificationTransactionIdentity{Epoch: epoch.Epoch, SystemIdentifier: epoch.LastSystemIdentifier}, err
}

func (s *GormNotificationStore[U]) CaptureNotificationTransaction(ctx context.Context) (models.NotificationTransactionStamp, models.NotificationVisibilitySnapshot, error) {
	identity, err := s.CurrentNotificationIdentity(ctx)
	if err != nil {
		return models.NotificationTransactionStamp{}, models.NotificationVisibilitySnapshot{}, err
	}
	return CaptureNotificationTransaction(ctx, s.db.DB(ctx), identity)
}

func (s *GormNotificationStore[U]) CaptureNotificationSnapshot(ctx context.Context) (models.NotificationVisibilitySnapshot, error) {
	identity, err := s.CurrentNotificationIdentity(ctx)
	if err != nil {
		return models.NotificationVisibilitySnapshot{}, err
	}
	return CaptureNotificationSnapshot(ctx, s.db.DB(ctx), identity)
}

// These are fixed, shared-schema identifiers, never application input. Each
// pair has an epoch/value index, so refreshing the high-water mark is bounded
// by the number of stamp families rather than the size of the retained inbox.
var notificationTransactionHighWaterColumns = []struct{ table, epoch, xid string }{
	{"gocommon_notification_emissions", "source_epoch", "source_xid"},
	{"gocommon_notification_emissions", "source_snapshot_epoch", "source_snapshot_upper_bound"},
	{"gocommon_notification_emissions", "source_snapshot_epoch", "source_snapshot_own_xid"},
	{"gocommon_notification_group_states", "ack_epoch", "ack_upper_bound"},
	{"gocommon_notification_group_states", "ack_epoch", "ack_own_xid"},
	{"gocommon_notification_retirement_cutoffs", "cutoff_epoch", "cutoff_upper_bound"},
	{"gocommon_notification_retirement_cutoffs", "cutoff_epoch", "cutoff_own_xid"},
	{"gocommon_notification_scope_retirement_fences", "cutoff_epoch", "cutoff_upper_bound"},
	{"gocommon_notification_scope_retirement_fences", "cutoff_epoch", "cutoff_own_xid"},
	{"gocommon_notification_recipient_retirements", "cutoff_epoch", "cutoff_upper_bound"},
	{"gocommon_notification_recipient_retirements", "cutoff_epoch", "cutoff_own_xid"},
	{"gocommon_notifications", "materialization_epoch", "materialization_xid"},
	{"gocommon_chat_channel_participants", "notification_join_epoch", "notification_join_xid"},
}

// The assigned owner can equal or exceed a snapshot's exclusive upper bound.
// Index its high-water family separately rather than trusting xmax to cover it.
// Guard absent columns for narrow schema consumers, as reconciliation does.
var notificationOwnSnapshotIndexFamilies = []struct{ table, epoch, xid, index string }{
	{"gocommon_notification_emissions", "source_snapshot_epoch", "source_snapshot_own_xid", "idx_notification_source_snapshot_owner_highwater"},
	{"gocommon_notification_group_states", "ack_epoch", "ack_own_xid", "idx_notification_ack_owner_highwater"},
	{"gocommon_notification_retirement_cutoffs", "cutoff_epoch", "cutoff_own_xid", "idx_notification_retirement_owner_highwater"},
	{"gocommon_notification_scope_retirement_fences", "cutoff_epoch", "cutoff_own_xid", "idx_notification_scope_fence_owner_highwater"},
	{"gocommon_notification_recipient_retirements", "cutoff_epoch", "cutoff_upper_bound", "idx_notification_recipient_retirement_highwater"},
	{"gocommon_notification_recipient_retirements", "cutoff_epoch", "cutoff_own_xid", "idx_notification_recipient_retirement_owner_highwater"},
}

func notificationOwnSnapshotIndexStatements() []string {
	var statements []string
	for _, family := range notificationOwnSnapshotIndexFamilies {
		statements = append(statements, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s" ON "%s" ("%s", "%s" DESC)`, family.index, family.table, family.epoch, family.xid))
	}
	return statements
}

func ensureNotificationOwnSnapshotIndexes(db *gorm.DB) error {
	for _, family := range notificationOwnSnapshotIndexFamilies {
		var exists bool
		if err := db.Raw(`SELECT count(*) = 2 FROM pg_attribute WHERE attrelid = to_regclass(?)
			AND attname IN (?, ?) AND NOT attisdropped`, family.table, family.epoch, family.xid).Scan(&exists).Error; err != nil {
			return err
		}
		if !exists {
			continue
		}
		statement := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%s" ON "%s" ("%s", "%s" DESC)`, family.index, family.table, family.epoch, family.xid)
		if err := executeSharedDDL(db, statement); err != nil {
			return err
		}
	}
	return nil
}

// initializeNotificationTransactionIdentity runs only while the caller holds
// the database-scoped schema bootstrap lock. The system identifier query is
// deliberately outside the transaction: insufficient privilege must not poison
// the fallback transaction on hardened or managed PostgreSQL installations.
func initializeNotificationTransactionIdentity(ctx context.Context, db *gorm.DB, logger *zap.Logger) (models.NotificationTransactionIdentity, error) {
	var systemIdentifier string
	if err := db.WithContext(ctx).Raw("SELECT system_identifier::text FROM pg_control_system()").Scan(&systemIdentifier).Error; err != nil {
		if ctx.Err() != nil {
			return models.NotificationTransactionIdentity{}, ctx.Err()
		}
		if logger != nil {
			logger.Warn("PostgreSQL system identifier unavailable; notification transaction fences use the application epoch", zap.Error(err))
		}
	}
	return reconcileNotificationTransactionIdentity(ctx, db, systemIdentifier, logger)
}

func reconcileNotificationTransactionIdentity(ctx context.Context, db *gorm.DB, systemIdentifier string, logger *zap.Logger) (models.NotificationTransactionIdentity, error) {
	var identity models.NotificationTransactionIdentity
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var epoch models.NotificationTransactionEpoch
		err := tx.First(&epoch, 1).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			epoch = models.NotificationTransactionEpoch{ID: 1, Epoch: 1, HighWaterXID: "0"}
		}
		highWater, err := strconv.ParseUint(epoch.HighWaterXID, 10, 64)
		if err != nil {
			return fmt.Errorf("parse notification transaction high-water mark: %w", err)
		}
		for _, source := range notificationTransactionHighWaterColumns {
			// Tests and narrow schema consumers can legitimately omit a whole
			// stamp family. Production bootstrap has migrated all families.
			var exists bool
			if err := tx.Raw(`SELECT count(*) = 2 FROM pg_attribute WHERE attrelid = to_regclass(?)
				AND attname IN (?, ?) AND NOT attisdropped`, source.table, source.epoch, source.xid).Scan(&exists).Error; err != nil {
				return err
			}
			if !exists {
				continue
			}
			var rows []struct {
				XID string `gorm:"column:xid"`
			}
			query := fmt.Sprintf(`SELECT "%s"::text AS xid FROM "%s" WHERE "%s" = ? ORDER BY "%s" DESC LIMIT 1`, source.xid, source.table, source.epoch, source.xid)
			if err := tx.Raw(query, epoch.Epoch).Scan(&rows).Error; err != nil {
				return err
			}
			if len(rows) != 0 {
				xid, err := strconv.ParseUint(rows[0].XID, 10, 64)
				if err != nil {
					return fmt.Errorf("parse persisted notification transaction stamp: %w", err)
				}
				highWater = max(highWater, xid)
			}
		}
		// Allocate only after reading persisted stamp/snapshot maxima. The
		// surrounding bootstrap advisory lock serializes singleton access
		// without allocating an xid early through SELECT FOR UPDATE.
		var currentText string
		if err := tx.Raw("SELECT pg_current_xact_id()::text").Scan(&currentText).Error; err != nil {
			return err
		}
		current, err := strconv.ParseUint(currentText, 10, 64)
		if err != nil {
			return err
		}
		changedSystem := systemIdentifier != "" && epoch.LastSystemIdentifier != "" && systemIdentifier != epoch.LastSystemIdentifier
		// Stamps written while the cluster was unknown may belong to a different
		// incarnation whose xid counter was lower than this one. The first time
		// an identifier becomes readable, those stamps cannot be trusted.
		unknownSystem := systemIdentifier != "" && epoch.LastSystemIdentifier == "" && highWater > 0
		if unknownSystem && logger != nil {
			logger.Warn("PostgreSQL system identifier became readable after notification stamps were written under an unknown cluster; advancing the notification transaction epoch",
				zap.Uint64("epoch", epoch.Epoch), zap.String("system_identifier", systemIdentifier))
		}
		if changedSystem || unknownSystem || current < highWater {
			epoch.Epoch++
			highWater = current
		}
		epoch.HighWaterXID = strconv.FormatUint(max(highWater, current), 10)
		if systemIdentifier != "" {
			epoch.LastSystemIdentifier = systemIdentifier
		}
		if err := tx.Save(&epoch).Error; err != nil {
			return err
		}
		identity = models.NotificationTransactionIdentity{Epoch: epoch.Epoch, SystemIdentifier: systemIdentifier}
		return nil
	})
	return identity, err
}

var _ notificationIdentityProvider = (*CommonPersistenceHandler[*common_service_models.User])(nil)
