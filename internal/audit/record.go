package audit

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"gorm.io/gorm"
)

const (
	maximumActorKindBytes             = len(ActorKindBrowser)
	maximumActorRoleBytes             = len(ActorRoleAdministrator)
	maximumActionBytes                = len(ActionKnowledgeObjectScopeChange)
	maximumTargetKindBytes            = len(TargetKindKnowledgeObject)
	maximumKnowledgeObjectTypeBytes   = len(KnowledgeObjectTypeFieldExtraction)
	maximumKnowledgeSharingScopeBytes = len(KnowledgeSharingScopePrivate)
	maximumIntegrityBatch             = 512
)

// auditEventSizeRecord is intentionally scalar-only. It lets reads reject an
// externally corrupted oversized row before asking a driver to allocate its
// attacker-controlled text values.
type auditEventSizeRecord struct {
	Sequence            int64 `gorm:"column:sequence"`
	TenantIDBytes       int64 `gorm:"column:tenant_id_bytes"`
	ActorKindBytes      int64 `gorm:"column:actor_kind_bytes"`
	ActorIDBytes        int64 `gorm:"column:actor_id_bytes"`
	ActorRoleBytes      int64 `gorm:"column:actor_role_bytes"`
	ActionBytes         int64 `gorm:"column:action_bytes"`
	TargetKindBytes     int64 `gorm:"column:target_kind_bytes"`
	TargetIDBytes       int64 `gorm:"column:target_id_bytes"`
	AppIDPresent        int64 `gorm:"column:app_id_present"`
	AppIDBytes          int64 `gorm:"column:app_id_bytes"`
	ObjectTypePresent   int64 `gorm:"column:object_type_present"`
	ObjectTypeBytes     int64 `gorm:"column:object_type_bytes"`
	SharingScopePresent int64 `gorm:"column:sharing_scope_present"`
	SharingScopeBytes   int64 `gorm:"column:sharing_scope_bytes"`
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
			length(CAST(target_id AS BLOB)) AS target_id_bytes,
			app_id IS NOT NULL AS app_id_present,
			coalesce(length(CAST(app_id AS BLOB)), 0) AS app_id_bytes,
			object_type IS NOT NULL AS object_type_present,
			coalesce(length(CAST(object_type AS BLOB)), 0) AS object_type_bytes,
			sharing_scope IS NOT NULL AS sharing_scope_present,
			coalesce(length(CAST(sharing_scope AS BLOB)), 0) AS sharing_scope_bytes
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
		record.TargetIDBytes <= maximumTargetIDBytes &&
		validOptionalAuditWidth(
			record.AppIDPresent,
			record.AppIDBytes,
			maximumKnowledgeAppIDBytes,
		) &&
		validOptionalAuditWidth(
			record.ObjectTypePresent,
			record.ObjectTypeBytes,
			maximumKnowledgeObjectTypeBytes,
		) &&
		validOptionalAuditWidth(
			record.SharingScopePresent,
			record.SharingScopeBytes,
			maximumKnowledgeSharingScopeBytes,
		) &&
		record.AppIDPresent == record.ObjectTypePresent &&
		record.AppIDPresent == record.SharingScopePresent
}

func validOptionalAuditWidth(present, width int64, maximum int) bool {
	return (present == 0 && width == 0) ||
		(present == 1 && width >= 1 && width <= int64(maximum))
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
		// Omitting absent knowledge fields preserves the pre-0026 digest for
		// legacy events, so an upgrade does not invalidate their continuations.
		AppID        *string                `json:"p,omitempty"`
		ObjectType   *KnowledgeObjectType   `json:"y,omitempty"`
		SharingScope *KnowledgeSharingScope `json:"h,omitempty"`
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
		AppID:               record.AppID,
		ObjectType:          record.ObjectType,
		SharingScope:        record.SharingScope,
	})
	if err != nil {
		return "", fmt.Errorf("encode audit high-water identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}
