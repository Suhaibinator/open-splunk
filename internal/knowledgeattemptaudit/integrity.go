package knowledgeattemptaudit

import (
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"gorm.io/gorm"
)

type eventSizeRecord struct {
	Sequence               int64 `gorm:"column:sequence"`
	TenantIDBytes          int64 `gorm:"column:tenant_id_bytes"`
	ActorKindBytes         int64 `gorm:"column:actor_kind_bytes"`
	ActorIDBytes           int64 `gorm:"column:actor_id_bytes"`
	ActorRoleBytes         int64 `gorm:"column:actor_role_bytes"`
	ActionBytes            int64 `gorm:"column:action_bytes"`
	ResultBytes            int64 `gorm:"column:result_bytes"`
	ReasonBytes            int64 `gorm:"column:reason_bytes"`
	AppIDBytes             int64 `gorm:"column:app_id_bytes"`
	KnowledgeObjectIDBytes int64 `gorm:"column:knowledge_object_id_bytes"`
	ObjectTypeBytes        int64 `gorm:"column:object_type_bytes"`
	SharingScopeBytes      int64 `gorm:"column:sharing_scope_bytes"`
}

func validateAllTenantIntegrity(database *gorm.DB) error {
	var invalidTenantWidth int64
	if err := database.Raw(`
		SELECT EXISTS (
			SELECT 1 FROM knowledge_attempt_audit_tenant_state
			WHERE typeof(tenant_id) <> 'text'
			   OR length(CAST(tenant_id AS BLOB)) NOT BETWEEN 1 AND ?
			LIMIT 1
		)
	`, maximumTenantIDBytes).Scan(&invalidTenantWidth).Error; err != nil {
		return err
	}
	if invalidTenantWidth != 0 {
		return fmt.Errorf("%w: tenant identity width is invalid", ErrCorrupt)
	}
	var orphaned int64
	if err := database.Raw(`
		SELECT EXISTS (
			SELECT 1
			FROM knowledge_attempt_audit_events AS event
			LEFT JOIN knowledge_attempt_audit_tenant_state AS state
			  ON state.tenant_id = event.tenant_id
			WHERE state.tenant_id IS NULL LIMIT 1
		)
	`).Scan(&orphaned).Error; err != nil {
		return err
	}
	if orphaned != 0 {
		return fmt.Errorf("%w: event has no tenant state", ErrCorrupt)
	}

	var after string
	haveAfter := false
	for {
		var states []tenantStateRecord
		query := database.Model(&tenantStateRecord{}).
			Order("tenant_id ASC").Limit(maximumIntegrityBatch)
		if haveAfter {
			query = query.Where("tenant_id > ?", after)
		}
		if err := query.Find(&states).Error; err != nil {
			return err
		}
		if len(states) == 0 {
			return nil
		}
		for index, state := range states {
			if !validTenantState(state, state.TenantID) ||
				(haveAfter && state.TenantID <= after) ||
				(index > 0 && state.TenantID <= states[index-1].TenantID) {
				return fmt.Errorf("%w: tenant scan is invalid", ErrCorrupt)
			}
			if err := validateTenantIntegrity(database, state); err != nil {
				return err
			}
		}
		after = strings.Clone(states[len(states)-1].TenantID)
		haveAfter = true
		if len(states) < maximumIntegrityBatch {
			return nil
		}
	}
}

func validateTenantIntegrity(database *gorm.DB, state tenantStateRecord) error {
	for expected := state.FirstSequence; expected < state.NextSequence; {
		limit := int(min(int64(maximumIntegrityBatch), state.NextSequence-expected))
		records, err := readPreflightedRecords(database.
			Model(&eventRecord{}).
			Where(
				"tenant_id = ? AND sequence >= ? AND sequence < ?",
				state.TenantID,
				expected,
				state.NextSequence,
			).
			Order("sequence ASC"), limit)
		if err != nil {
			return err
		}
		if len(records) != limit {
			return fmt.Errorf("%w: tenant scan ended early", ErrCorrupt)
		}
		for _, record := range records {
			if record.TenantID != state.TenantID || record.Sequence != expected {
				return fmt.Errorf("%w: sequence scan is not dense", ErrCorrupt)
			}
			if _, err := validateEventRecord(record); err != nil {
				return err
			}
			expected++
		}
	}
	var outsideWindow int64
	if err := database.Raw(`
		SELECT EXISTS (
			SELECT 1 FROM knowledge_attempt_audit_events
			WHERE tenant_id = ? AND sequence < ? LIMIT 1
		) OR EXISTS (
			SELECT 1 FROM knowledge_attempt_audit_events
			WHERE tenant_id = ? AND sequence >= ? LIMIT 1
		)
	`, state.TenantID, state.FirstSequence,
		state.TenantID, state.NextSequence,
	).Scan(&outsideWindow).Error; err != nil {
		return err
	}
	if outsideWindow != 0 {
		return fmt.Errorf("%w: event lies outside retained window", ErrCorrupt)
	}
	return nil
}

func readPreflightedRecords(query *gorm.DB, limit int) ([]eventRecord, error) {
	if query == nil || limit < 1 || limit > maximumIntegrityBatch {
		return nil, fmt.Errorf("%w: invalid bounded record read", ErrCorrupt)
	}
	var sizes []eventSizeRecord
	preflight := query.Session(&gorm.Session{}).Select(`
		sequence,
		length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
		length(CAST(actor_kind AS BLOB)) AS actor_kind_bytes,
		length(CAST(actor_id AS BLOB)) AS actor_id_bytes,
		length(CAST(actor_role AS BLOB)) AS actor_role_bytes,
		length(CAST(action AS BLOB)) AS action_bytes,
		length(CAST(result AS BLOB)) AS result_bytes,
		length(CAST(reason AS BLOB)) AS reason_bytes,
		coalesce(length(CAST(app_id AS BLOB)), 0) AS app_id_bytes,
		coalesce(length(CAST(knowledge_object_id AS BLOB)), 0) AS knowledge_object_id_bytes,
		coalesce(length(CAST(object_type AS BLOB)), 0) AS object_type_bytes,
		coalesce(length(CAST(sharing_scope AS BLOB)), 0) AS sharing_scope_bytes
	`).Limit(limit).Find(&sizes)
	if preflight.Error != nil {
		return nil, preflight.Error
	}
	for _, size := range sizes {
		if !validEventSize(size) {
			return nil, fmt.Errorf("%w: event text width is invalid", ErrCorrupt)
		}
	}
	var records []eventRecord
	if err := query.Session(&gorm.Session{}).Limit(limit).Find(&records).Error; err != nil {
		return nil, err
	}
	if len(records) != len(sizes) {
		return nil, fmt.Errorf("%w: preflight result changed", ErrCorrupt)
	}
	for index := range records {
		if records[index].Sequence != sizes[index].Sequence {
			return nil, fmt.Errorf("%w: preflight order changed", ErrCorrupt)
		}
	}
	return records, nil
}

func validEventSize(record eventSizeRecord) bool {
	return record.Sequence >= 1 && record.Sequence <= maximumPersistedSequence &&
		record.TenantIDBytes >= 1 && record.TenantIDBytes <= maximumTenantIDBytes &&
		record.ActorKindBytes == int64(len(audit.ActorKindBrowser)) &&
		record.ActorIDBytes >= 1 && record.ActorIDBytes <= 255 &&
		record.ActorRoleBytes >= int64(len(audit.ActorRoleUser)) &&
		record.ActorRoleBytes <= int64(len(audit.ActorRoleAdministrator)) &&
		record.ActionBytes >= 1 && record.ActionBytes <= int64(len(ActionDependencies)) &&
		record.ResultBytes == int64(len("rejected")) &&
		record.ReasonBytes >= 1 && record.ReasonBytes <= int64(len(ReasonNotFoundOrForbidden)) &&
		record.AppIDBytes >= 0 && record.AppIDBytes <= maximumAppIDBytes &&
		record.KnowledgeObjectIDBytes >= 0 && record.KnowledgeObjectIDBytes <= maximumObjectIDBytes &&
		record.ObjectTypeBytes >= 0 && record.ObjectTypeBytes <= int64(len(ObjectTypeFieldExtraction)) &&
		record.SharingScopeBytes >= 0 && record.SharingScopeBytes <= int64(len(SharingScopePrivate))
}

func validateEventRecord(record eventRecord) (time.Time, error) {
	if record.Sequence < 1 || record.Sequence > maximumPersistedSequence ||
		!validIdentity(record.TenantID, maximumTenantIDBytes) ||
		record.Result != ResultRejected || !record.Action.valid() || !record.Reason.valid() {
		return time.Time{}, fmt.Errorf("%w: event scalar is invalid", ErrCorrupt)
	}
	actor := audit.Actor{Kind: record.ActorKind, ID: record.ActorID, Role: record.ActorRole}
	var authorizedValue AuthorizedContext
	var authorized *AuthorizedContext
	if record.AppID != nil {
		authorizedValue.AppID = *record.AppID
		authorized = &authorizedValue
	}
	objectColumnsPresent := record.KnowledgeObjectID != nil || record.ObjectType != nil ||
		record.ObjectVersion != nil || record.SharingScope != nil
	objectColumnsComplete := record.KnowledgeObjectID != nil && record.ObjectType != nil &&
		record.ObjectVersion != nil && record.SharingScope != nil
	var objectValue AuthorizedObject
	if objectColumnsPresent {
		if !objectColumnsComplete || authorized == nil || *record.ObjectVersion < 1 {
			return time.Time{}, fmt.Errorf("%w: authorized object metadata is incomplete", ErrCorrupt)
		}
		objectValue = AuthorizedObject{
			KnowledgeObjectID: *record.KnowledgeObjectID,
			ObjectType:        *record.ObjectType,
			// #nosec G115 -- the completeness check above requires a positive persisted version.
			Version:      uint64(*record.ObjectVersion),
			SharingScope: *record.SharingScope,
		}
		authorized.Object = &objectValue
	}
	occurredAt, ok := audit.CanonicalOccurrenceTime(time.UnixMicro(record.OccurredAtUnixMicro))
	if !ok || occurredAt.UnixMicro() != record.OccurredAtUnixMicro {
		return time.Time{}, fmt.Errorf("%w: event timestamp is invalid", ErrCorrupt)
	}
	if !validRejectedActor(actor, record.Reason) ||
		!validAuthorizedContext(authorized, record.Action, record.Reason) {
		return time.Time{}, fmt.Errorf("%w: persisted event is invalid", ErrCorrupt)
	}
	return occurredAt, nil
}

func eventFromRecord(record eventRecord) (Event, error) {
	occurredAt, err := validateEventRecord(record)
	if err != nil {
		return Event{}, err
	}
	event := Event{
		// #nosec G115 -- validateEventRecord bounds the persisted sequence to a positive int64.
		Sequence:   uint64(record.Sequence),
		TenantID:   strings.Clone(record.TenantID),
		OccurredAt: occurredAt,
		Actor: audit.Actor{
			Kind: record.ActorKind,
			ID:   strings.Clone(record.ActorID),
			Role: record.ActorRole,
		},
		Action: record.Action,
		Result: ResultRejected,
		Reason: record.Reason,
	}
	if record.AppID != nil {
		event.AuthorizedContext = &AuthorizedContext{AppID: strings.Clone(*record.AppID)}
		if record.KnowledgeObjectID != nil {
			event.AuthorizedContext.Object = &AuthorizedObject{
				KnowledgeObjectID: strings.Clone(*record.KnowledgeObjectID),
				ObjectType:        *record.ObjectType,
				// #nosec G115 -- validateEventRecord requires a complete object with a positive version.
				Version:      uint64(*record.ObjectVersion),
				SharingScope: *record.SharingScope,
			}
		}
	}
	return event, nil
}
