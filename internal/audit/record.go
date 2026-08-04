package audit

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"gorm.io/gorm"
)

const (
	maximumActorKindBytes  = len(ActorKindBrowser)
	maximumActorRoleBytes  = len(ActorRoleAdministrator)
	maximumActionBytes     = len(ActionIngestionTokenCreate)
	maximumTargetKindBytes = len(TargetKindIngestionToken)
	maximumIntegrityBatch  = 512
)

// auditEventSizeRecord is intentionally scalar-only. It lets reads reject an
// externally corrupted oversized row before asking a driver to allocate its
// attacker-controlled text values.
type auditEventSizeRecord struct {
	Sequence        int64 `gorm:"column:sequence"`
	TenantIDBytes   int64 `gorm:"column:tenant_id_bytes"`
	ActorKindBytes  int64 `gorm:"column:actor_kind_bytes"`
	ActorIDBytes    int64 `gorm:"column:actor_id_bytes"`
	ActorRoleBytes  int64 `gorm:"column:actor_role_bytes"`
	ActionBytes     int64 `gorm:"column:action_bytes"`
	TargetKindBytes int64 `gorm:"column:target_kind_bytes"`
	TargetIDBytes   int64 `gorm:"column:target_id_bytes"`
}

func readPreflightedAuditRecords(
	query *gorm.DB,
	limit int,
) ([]auditEventRecord, error) {
	if query == nil || limit < 1 || limit > maximumIntegrityBatch {
		return nil, fmt.Errorf("%w: invalid bounded audit read", ErrCorrupt)
	}
	var sizes []auditEventSizeRecord
	preflight := query.Session(&gorm.Session{}).
		Select(`
			sequence,
			length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
			length(CAST(actor_kind AS BLOB)) AS actor_kind_bytes,
			length(CAST(actor_id AS BLOB)) AS actor_id_bytes,
			length(CAST(actor_role AS BLOB)) AS actor_role_bytes,
			length(CAST(action AS BLOB)) AS action_bytes,
			length(CAST(target_kind AS BLOB)) AS target_kind_bytes,
			length(CAST(target_id AS BLOB)) AS target_id_bytes
		`).
		Limit(limit).
		Find(&sizes)
	if preflight.Error != nil {
		return nil, preflight.Error
	}
	for _, size := range sizes {
		if !validAuditEventSize(size) {
			return nil, fmt.Errorf("%w: audit event text width is invalid", ErrCorrupt)
		}
	}

	var records []auditEventRecord
	hydrated := query.Session(&gorm.Session{}).
		Limit(limit).
		Find(&records)
	if hydrated.Error != nil {
		return nil, hydrated.Error
	}
	if len(records) != len(sizes) {
		return nil, fmt.Errorf("%w: audit preflight result changed", ErrCorrupt)
	}
	for index := range records {
		if records[index].Sequence != sizes[index].Sequence {
			return nil, fmt.Errorf("%w: audit preflight order changed", ErrCorrupt)
		}
	}
	return records, nil
}

func validAuditEventSize(record auditEventSizeRecord) bool {
	return record.Sequence >= 1 &&
		record.Sequence <= MaximumEventsPerTenant &&
		record.TenantIDBytes >= 1 &&
		record.TenantIDBytes <= maximumTenantIDBytes &&
		record.ActorKindBytes >= 1 &&
		record.ActorKindBytes <= int64(maximumActorKindBytes) &&
		record.ActorIDBytes >= 1 &&
		record.ActorIDBytes <= maximumActorIDBytes &&
		record.ActorRoleBytes >= 1 &&
		record.ActorRoleBytes <= int64(maximumActorRoleBytes) &&
		record.ActionBytes >= 1 &&
		record.ActionBytes <= int64(maximumActionBytes) &&
		record.TargetKindBytes >= 1 &&
		record.TargetKindBytes <= int64(maximumTargetKindBytes) &&
		record.TargetIDBytes >= 1 &&
		record.TargetIDBytes <= maximumTargetIDBytes
}

func auditEventDigest(record auditEventRecord) (string, error) {
	payload, err := json.Marshal(struct {
		TenantID            string     `json:"n"`
		Sequence            int64      `json:"s"`
		OccurredAtUnixMicro int64      `json:"o"`
		ActorKind           ActorKind  `json:"k"`
		ActorID             string     `json:"i"`
		ActorRole           ActorRole  `json:"r"`
		Action              Action     `json:"a"`
		TargetKind          TargetKind `json:"t"`
		TargetID            string     `json:"d"`
		TargetVersion       int64      `json:"v"`
	}{
		TenantID:            record.TenantID,
		Sequence:            record.Sequence,
		OccurredAtUnixMicro: record.OccurredAtUnixMicro,
		ActorKind:           record.ActorKind,
		ActorID:             record.ActorID,
		ActorRole:           record.ActorRole,
		Action:              record.Action,
		TargetKind:          record.TargetKind,
		TargetID:            record.TargetID,
		TargetVersion:       record.TargetVersion,
	})
	if err != nil {
		return "", fmt.Errorf("encode audit high-water identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}
