package knowledgecatalog

import (
	"fmt"
	"slices"

	"gorm.io/gorm"
)

type versionLifecycleRecord struct {
	TenantID               string  `gorm:"column:tenant_id"`
	KnowledgeObjectID      string  `gorm:"column:knowledge_object_id"`
	ObjectVersion          int64   `gorm:"column:object_version"`
	State                  State   `gorm:"column:state"`
	DisabledAtUnixMicro    *int64  `gorm:"column:disabled_at_unix_micro"`
	QuarantinedAtUnixMicro *int64  `gorm:"column:quarantined_at_unix_micro"`
	DeletedAtUnixMicro     *int64  `gorm:"column:deleted_at_unix_micro"`
	QuarantineReason       *string `gorm:"column:quarantine_reason"`
}

type versionLifecycleWidth struct {
	TenantIDBytes           int64 `gorm:"column:tenant_id_bytes"`
	ObjectIDBytes           int64 `gorm:"column:object_id_bytes"`
	StateBytes              int64 `gorm:"column:state_bytes"`
	QuarantineReasonBytes   int64 `gorm:"column:quarantine_reason_bytes"`
	DisabledTransitionValid int64 `gorm:"column:disabled_transition_valid"`
}

const versionLifecycleWidthProjection = `
	length(CAST(lifecycle.tenant_id AS BLOB)) AS tenant_id_bytes,
	length(CAST(lifecycle.knowledge_object_id AS BLOB)) AS object_id_bytes,
	length(CAST(lifecycle.state AS BLOB)) AS state_bytes,
	coalesce(length(CAST(lifecycle.quarantine_reason AS BLOB)), 0) AS quarantine_reason_bytes,
	CASE
		WHEN lifecycle.state <> 'disabled' THEN 1
		WHEN EXISTS (
			SELECT 1
			FROM knowledge_object_versions AS transition
			WHERE transition.tenant_id = lifecycle.tenant_id
			  AND transition.knowledge_object_id = lifecycle.knowledge_object_id
			  AND transition.object_version BETWEEN 1 AND lifecycle.object_version
			  AND transition.mutation_kind = 'disable'
			  AND transition.created_at_unix_micro = lifecycle.disabled_at_unix_micro
			  AND NOT EXISTS (
				  SELECT 1
				  FROM knowledge_object_versions AS later_transition
				  WHERE later_transition.tenant_id = transition.tenant_id
					AND later_transition.knowledge_object_id = transition.knowledge_object_id
					AND later_transition.object_version > transition.object_version
					AND later_transition.object_version <= lifecycle.object_version
					AND later_transition.mutation_kind IN ('disable', 'enable')
			  )
		) THEN 1
		ELSE 0
	END AS disabled_transition_valid
`

func validVersionLifecycleWidth(width versionLifecycleWidth) bool {
	return width.TenantIDBytes >= 1 && width.TenantIDBytes <= maximumTenantIDBytes &&
		width.ObjectIDBytes >= 1 && width.ObjectIDBytes <= maximumObjectIDBytes &&
		width.StateBytes >= int64(len(StateDraft)) && width.StateBytes <= int64(len(StateQuarantined)) &&
		width.QuarantineReasonBytes <= maximumPersistedQuarantineReasonBytes &&
		width.DisabledTransitionValid == 1
}

func readVersionLifecycle(database *gorm.DB, version versionRecord) (versionLifecycleRecord, error) {
	query := database.Table("knowledge_object_version_lifecycle AS lifecycle").Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version = ?",
		version.TenantID,
		version.KnowledgeObjectID,
		version.ObjectVersion,
	)
	var widths []versionLifecycleWidth
	if err := query.Session(&gorm.Session{}).Select(versionLifecycleWidthProjection).
		Limit(2).Find(&widths).Error; err != nil {
		return versionLifecycleRecord{}, err
	}
	if len(widths) != 1 || !validVersionLifecycleWidth(widths[0]) {
		return versionLifecycleRecord{}, fmt.Errorf("%w: immutable lifecycle authority is invalid", ErrCorrupt)
	}
	var records []versionLifecycleRecord
	if err := query.Session(&gorm.Session{}).Limit(2).Find(&records).Error; err != nil {
		return versionLifecycleRecord{}, err
	}
	if len(records) != 1 || validateVersionLifecycle(version, records[0]) != nil {
		return versionLifecycleRecord{}, fmt.Errorf("%w: immutable lifecycle authority is invalid", ErrCorrupt)
	}
	return records[0], nil
}

func readCurrentVersionLifecyclesBatch(
	database *gorm.DB,
	tenantID string,
	objectIDs []string,
	versions map[string]versionRecord,
) (map[string]versionLifecycleRecord, error) {
	corrupt := func() error {
		return fmt.Errorf("%w: immutable lifecycle authority is invalid", ErrCorrupt)
	}
	recordsByID := make(map[string]versionLifecycleRecord, len(objectIDs))
	err := listChunks(objectIDs, func(chunk []string) error {
		if err := listDatabaseContextError(database); err != nil {
			return err
		}
		query := database.Table("knowledge_object_version_lifecycle AS lifecycle").
			Joins(`JOIN knowledge_objects AS object
				ON object.tenant_id = lifecycle.tenant_id
			   AND object.knowledge_object_id = lifecycle.knowledge_object_id
			   AND object.current_version = lifecycle.object_version`).
			Where("lifecycle.tenant_id = ? AND lifecycle.knowledge_object_id IN ?", tenantID, chunk).
			Order("lifecycle.knowledge_object_id ASC")
		var widths []versionLifecycleWidth
		if err := query.Session(&gorm.Session{}).Select(versionLifecycleWidthProjection).
			Limit(len(chunk) + 1).Find(&widths).Error; err != nil {
			return err
		}
		if len(widths) > len(chunk) {
			return corrupt()
		}
		for _, width := range widths {
			if !validVersionLifecycleWidth(width) {
				return corrupt()
			}
		}
		var records []versionLifecycleRecord
		if err := query.Session(&gorm.Session{}).Select("lifecycle.*").
			Limit(len(chunk) + 1).Find(&records).Error; err != nil {
			return err
		}
		if len(records) != len(widths) {
			return corrupt()
		}
		for _, record := range records {
			version, found := versions[record.KnowledgeObjectID]
			if !found || validateVersionLifecycle(version, record) != nil {
				return corrupt()
			}
			if _, duplicate := recordsByID[record.KnowledgeObjectID]; duplicate {
				return corrupt()
			}
			recordsByID[record.KnowledgeObjectID] = record
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(recordsByID) != len(objectIDs) {
		return nil, corrupt()
	}
	return recordsByID, nil
}

func validateVersionLifecycle(version versionRecord, lifecycle versionLifecycleRecord) error {
	if lifecycle.TenantID != version.TenantID ||
		lifecycle.KnowledgeObjectID != version.KnowledgeObjectID ||
		lifecycle.ObjectVersion != version.ObjectVersion || lifecycle.State != version.State ||
		!equalOptionalString(lifecycle.QuarantineReason, version.QuarantineReason) {
		return fmt.Errorf("%w: immutable lifecycle identity disagrees", ErrCorrupt)
	}
	validMarker := func(value *int64) bool {
		if value == nil || *value > version.CreatedAtUnixMicro {
			return false
		}
		_, err := canonicalTime(*value)
		return err == nil
	}
	switch version.State {
	case StateDraft, StateActive:
		if lifecycle.DisabledAtUnixMicro != nil || lifecycle.QuarantinedAtUnixMicro != nil ||
			lifecycle.DeletedAtUnixMicro != nil || lifecycle.QuarantineReason != nil {
			return fmt.Errorf("%w: immutable lifecycle state shape disagrees", ErrCorrupt)
		}
	case StateDisabled:
		if !validMarker(lifecycle.DisabledAtUnixMicro) || lifecycle.QuarantinedAtUnixMicro != nil ||
			lifecycle.DeletedAtUnixMicro != nil || lifecycle.QuarantineReason != nil ||
			(version.MutationKind == "disable" && *lifecycle.DisabledAtUnixMicro != version.CreatedAtUnixMicro) {
			return fmt.Errorf("%w: immutable disabled lifecycle disagrees", ErrCorrupt)
		}
	case StateQuarantined:
		if lifecycle.DisabledAtUnixMicro != nil || !validMarker(lifecycle.QuarantinedAtUnixMicro) ||
			*lifecycle.QuarantinedAtUnixMicro != version.CreatedAtUnixMicro ||
			lifecycle.DeletedAtUnixMicro != nil || lifecycle.QuarantineReason == nil {
			return fmt.Errorf("%w: immutable quarantined lifecycle disagrees", ErrCorrupt)
		}
	case StateDeleted:
		if lifecycle.DisabledAtUnixMicro != nil || lifecycle.QuarantinedAtUnixMicro != nil ||
			!validMarker(lifecycle.DeletedAtUnixMicro) ||
			*lifecycle.DeletedAtUnixMicro != version.CreatedAtUnixMicro || lifecycle.QuarantineReason != nil {
			return fmt.Errorf("%w: immutable deleted lifecycle disagrees", ErrCorrupt)
		}
	default:
		return fmt.Errorf("%w: immutable lifecycle state is invalid", ErrCorrupt)
	}
	return nil
}

func validateCurrentLifecycle(
	registry registryRecord,
	version versionRecord,
	lifecycle versionLifecycleRecord,
) error {
	if err := validateVersionLifecycle(version, lifecycle); err != nil {
		return fmt.Errorf("%w: immutable lifecycle authority is invalid", ErrCorrupt)
	}
	if !equalOptionalInt64(registry.DisabledAtUnixMicro, lifecycle.DisabledAtUnixMicro) ||
		!equalOptionalInt64(registry.QuarantinedAtUnixMicro, lifecycle.QuarantinedAtUnixMicro) ||
		!equalOptionalInt64(registry.DeletedAtUnixMicro, lifecycle.DeletedAtUnixMicro) ||
		!equalOptionalString(registry.QuarantineReason, lifecycle.QuarantineReason) {
		return fmt.Errorf("%w: immutable lifecycle authority is invalid", ErrCorrupt)
	}
	return nil
}

type versionHistoryAggregate struct {
	KnowledgeObjectID      string `gorm:"column:knowledge_object_id"`
	VersionCount           int64  `gorm:"column:version_count"`
	MinimumVersion         int64  `gorm:"column:minimum_version"`
	MaximumVersion         int64  `gorm:"column:maximum_version"`
	CreationTimestamp      int64  `gorm:"column:creation_timestamp"`
	CurrentTimestamp       int64  `gorm:"column:current_timestamp"`
	InvalidTransitionCount int64  `gorm:"column:invalid_transition_count"`
}

func validateCurrentChronology(
	database *gorm.DB,
	registry registryRecord,
	version versionRecord,
) error {
	return validateCurrentChronologiesBatch(
		database,
		registry.TenantID,
		[]string{registry.KnowledgeObjectID},
		map[string]registryRecord{registry.KnowledgeObjectID: registry},
		map[string]versionRecord{version.KnowledgeObjectID: version},
	)
}

func validateCurrentChronologiesBatch(
	database *gorm.DB,
	tenantID string,
	objectIDs []string,
	registries map[string]registryRecord,
	versions map[string]versionRecord,
) error {
	corrupt := func() error {
		return fmt.Errorf("%w: immutable version chronology is invalid", ErrCorrupt)
	}
	if len(objectIDs) == 0 {
		return nil
	}
	var historyWork int64
	for _, objectID := range objectIDs {
		registry, found := registries[objectID]
		if !found || registry.CurrentVersion < 1 || registry.CurrentVersion > maximumVersionsPerTenant ||
			historyWork > maximumVersionsPerTenant-registry.CurrentVersion {
			return corrupt()
		}
		historyWork += registry.CurrentVersion
	}
	history := database.Table("knowledge_object_versions AS version").
		Joins(`JOIN knowledge_objects AS object
			ON object.tenant_id = version.tenant_id
		   AND object.knowledge_object_id = version.knowledge_object_id
		   AND version.object_version BETWEEN 1 AND object.current_version`).
		Where("version.tenant_id = ? AND version.knowledge_object_id IN ?", tenantID, objectIDs).
		Select(`
			version.knowledge_object_id,
			version.object_version,
			object.current_version,
			version.state,
			version.mutation_kind,
			version.created_at_unix_micro,
			lag(version.state) OVER (
				PARTITION BY version.knowledge_object_id
				ORDER BY version.object_version
			) AS previous_state,
			lag(version.created_at_unix_micro) OVER (
				PARTITION BY version.knowledge_object_id
				ORDER BY version.object_version
			) AS previous_timestamp
		`)
	var aggregates []versionHistoryAggregate
	if err := database.Table("(?) AS history", history).
		Select(`
			history.knowledge_object_id,
			count(*) AS version_count,
			min(history.object_version) AS minimum_version,
			max(history.object_version) AS maximum_version,
			max(CASE WHEN history.object_version = 1
				THEN history.created_at_unix_micro ELSE 0 END) AS creation_timestamp,
			max(CASE WHEN history.object_version = history.current_version
				THEN history.created_at_unix_micro ELSE 0 END) AS current_timestamp,
			sum(CASE WHEN
				history.created_at_unix_micro NOT BETWEEN 1 AND 253402300799999999
				OR (history.previous_timestamp IS NOT NULL
					AND history.created_at_unix_micro < history.previous_timestamp)
				OR (history.object_version < history.current_version
					AND history.state IN ('quarantined', 'deleted'))
				OR (history.object_version = 1 AND NOT (
					history.mutation_kind = 'create'
					AND history.state IN ('draft', 'active')
				))
				OR (history.object_version > 1 AND NOT (
					(history.mutation_kind IN ('update', 'scope_change')
						AND history.state = history.previous_state
						AND history.state IN ('draft', 'active', 'disabled'))
					OR (history.mutation_kind = 'enable'
						AND history.state = 'active'
						AND history.previous_state IN ('draft', 'disabled'))
					OR (history.mutation_kind = 'disable'
						AND history.state = 'disabled'
						AND history.previous_state IN ('draft', 'active'))
					OR (history.mutation_kind = 'quarantine'
						AND history.state = 'quarantined'
						AND history.previous_state IN ('draft', 'active', 'disabled'))
					OR (history.mutation_kind = 'delete'
						AND history.state = 'deleted'
						AND history.previous_state IN ('draft', 'active', 'disabled'))
				))
				THEN 1 ELSE 0 END) AS invalid_transition_count
		`).Group("history.knowledge_object_id").Order("history.knowledge_object_id ASC").
		Limit(len(objectIDs) + 1).Find(&aggregates).Error; err != nil {
		return err
	}
	if len(aggregates) != len(objectIDs) {
		return corrupt()
	}
	seen := make(map[string]struct{}, len(aggregates))
	for _, aggregate := range aggregates {
		registry, registryFound := registries[aggregate.KnowledgeObjectID]
		version, versionFound := versions[aggregate.KnowledgeObjectID]
		if !registryFound || !versionFound || registry.TenantID != tenantID ||
			version.TenantID != tenantID || registry.CurrentVersion != version.ObjectVersion ||
			aggregate.VersionCount != registry.CurrentVersion || aggregate.MinimumVersion != 1 ||
			aggregate.MaximumVersion != registry.CurrentVersion ||
			aggregate.CreationTimestamp != registry.CreatedAtUnixMicro ||
			aggregate.CurrentTimestamp != registry.UpdatedAtUnixMicro ||
			aggregate.InvalidTransitionCount != 0 {
			return corrupt()
		}
		if _, duplicate := seen[aggregate.KnowledgeObjectID]; duplicate {
			return corrupt()
		}
		seen[aggregate.KnowledgeObjectID] = struct{}{}
	}
	return nil
}

func validateHistoricalChronology(
	database *gorm.DB,
	current registryRecord,
	requested versionRecord,
) error {
	corrupt := func() error {
		return fmt.Errorf("%w: immutable historical chronology is invalid", ErrCorrupt)
	}
	if requested.ObjectVersion >= current.CurrentVersion || requested.ObjectVersion < 1 {
		return corrupt()
	}
	versions := []int64{1, requested.ObjectVersion, current.CurrentVersion}
	if requested.ObjectVersion > 1 {
		versions = append(versions, requested.ObjectVersion-1)
	}
	if requested.ObjectVersion+1 < current.CurrentVersion {
		versions = append(versions, requested.ObjectVersion+1)
	}
	slices.Sort(versions)
	versions = slices.Compact(versions)

	var rows []struct {
		ObjectVersion      int64 `gorm:"column:object_version"`
		CreatedAtUnixMicro int64 `gorm:"column:created_at_unix_micro"`
	}
	if err := database.Table("knowledge_object_versions").Select(
		"object_version, created_at_unix_micro",
	).Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version IN ?",
		current.TenantID,
		current.KnowledgeObjectID,
		versions,
	).Order("object_version ASC").Limit(len(versions) + 1).Find(&rows).Error; err != nil {
		return err
	}
	if len(rows) != len(versions) {
		return corrupt()
	}
	timestamps := make(map[int64]int64, len(rows))
	for index, row := range rows {
		if row.ObjectVersion != versions[index] || row.CreatedAtUnixMicro < current.CreatedAtUnixMicro ||
			row.CreatedAtUnixMicro > current.UpdatedAtUnixMicro {
			return corrupt()
		}
		if _, err := canonicalTime(row.CreatedAtUnixMicro); err != nil {
			return corrupt()
		}
		timestamps[row.ObjectVersion] = row.CreatedAtUnixMicro
	}
	if timestamps[1] != current.CreatedAtUnixMicro ||
		timestamps[current.CurrentVersion] != current.UpdatedAtUnixMicro ||
		timestamps[requested.ObjectVersion] != requested.CreatedAtUnixMicro {
		return corrupt()
	}
	if requested.ObjectVersion > 1 &&
		timestamps[requested.ObjectVersion-1] > requested.CreatedAtUnixMicro {
		return corrupt()
	}
	if requested.ObjectVersion < current.CurrentVersion &&
		requested.CreatedAtUnixMicro > timestamps[requested.ObjectVersion+1] {
		return corrupt()
	}
	return nil
}

func equalOptionalInt64(left, right *int64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
