package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// validatePublicationTransitionPersistenceTargets revalidates the rich
// compiler authority for one ACTIVE publication candidate against the exact
// current target registry before its smaller database projection is used.
// The returned projection is detached from both the caller and the authority.
func validatePublicationTransitionPersistenceTargets(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	expectedCandidate publicationCandidateAuthority,
	expectedSourceStage opensplunkv1.KnowledgeSearchStage,
	authority candidateDependencyAuthority,
) ([]publicationDependency, error) {
	if ctx == nil || tx == nil || tx.Statement == nil ||
		!validIdentity(tenantID, maximumTenantIDBytes) ||
		authority.IsZero() || !validPublicationTransitionPersistenceCandidate(expectedCandidate) {
		return nil, fmt.Errorf(
			"%w: publication transition persistence authority is invalid",
			control.ErrInvalidArgument,
		)
	}
	if _, transactional := tx.Statement.ConnPool.(*sql.Tx); !transactional {
		return nil, fmt.Errorf(
			"%w: publication transition persistence requires one fixed transaction snapshot",
			control.ErrInvalidArgument,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sourceRank, sourceStageValid := publicationTransitionPersistenceStageRank(expectedSourceStage)
	if !sourceStageValid {
		return nil, fmt.Errorf(
			"%w: publication transition persistence source stage is invalid",
			control.ErrInvalidArgument,
		)
	}
	state := authority.state
	if state.candidate != expectedCandidate || state.sourceStage != expectedSourceStage {
		return nil, fmt.Errorf(
			"%w: publication transition persistence candidate authority disagrees",
			control.ErrDependencyConflict,
		)
	}
	if len(state.targets) > maximumDependenciesPerVersion ||
		len(state.targets) != len(state.projection) {
		return nil, invalidPublicationTransitionPersistenceTargets(
			"has an invalid target or projection count",
		)
	}
	seenObjectIDs := make(map[string]struct{}, len(state.targets))
	for index, target := range state.targets {
		targetRank, targetStageValid := publicationTransitionPersistenceStageRank(target.targetStage)
		if !validIdentity(target.objectID, maximumObjectIDBytes) ||
			target.version < 1 || target.version > math.MaxInt64 ||
			target.definitionDigest == ([sha256.Size]byte{}) ||
			!validIdentity(target.ownerID, maximumOwnerIDBytes) ||
			target.role != opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT ||
			!targetStageValid || sourceRank <= targetRank ||
			(target.objectID == expectedCandidate.objectID && target.version == expectedCandidate.version) ||
			state.projection[index].targetObjectID != target.objectID ||
			state.projection[index].targetVersion != target.version {
			return nil, invalidPublicationTransitionPersistenceTargets(
				"contains malformed rich target authority",
			)
		}
		if _, duplicate := seenObjectIDs[target.objectID]; duplicate {
			return nil, invalidPublicationTransitionPersistenceTargets(
				"duplicates a target object identity",
			)
		}
		seenObjectIDs[target.objectID] = struct{}{}
		if index > 0 && (state.targets[index-1].objectID > target.objectID ||
			state.targets[index-1].objectID == target.objectID &&
				state.targets[index-1].version >= target.version) {
			return nil, invalidPublicationTransitionPersistenceTargets(
				"contains noncanonical rich target authority",
			)
		}
	}

	// Accessors detach strings and slices. Do not call them until the private
	// state above has proved their count and scalar widths are bounded.
	targets := authority.derivedTargets()
	projection := authority.databaseProjection()
	if len(targets) == 0 {
		return projection, nil
	}
	objectIDs := make([]string, len(targets))
	for index := range targets {
		objectIDs[index] = targets[index].objectID
	}

	versions, err := readPublicationTransitionCurrentTargetVersions(
		ctx,
		tx,
		tenantID,
		objectIDs,
	)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		version, found := versions[target.objectID]
		if !found || version.ObjectVersion != target.version || version.State != StateActive {
			return nil, fmt.Errorf(
				"%w: publication transition target is outside the exact current ACTIVE closure",
				control.ErrDependencyConflict,
			)
		}
		objectType, typeValid := objectTypeToProto(version.ObjectType)
		targetStage, _, stageValid := publicationStageForObjectType(objectType)
		if !typeValid || !stageValid || targetStage != target.targetStage ||
			version.OwnerID != target.ownerID ||
			!bytes.Equal(version.DefinitionDigest, target.definitionDigest[:]) {
			return nil, invalidPublicationTransitionPersistenceTargets(
				"rich target authority disagrees with current persistence",
			)
		}
	}
	return projection, nil
}

func validPublicationTransitionPersistenceCandidate(candidate publicationCandidateAuthority) bool {
	return validIdentity(candidate.objectID, maximumObjectIDBytes) &&
		candidate.version >= 1 && candidate.version <= math.MaxInt64 &&
		candidate.definitionDigest != ([sha256.Size]byte{}) &&
		validIdentity(candidate.ownerID, maximumOwnerIDBytes)
}

func publicationTransitionPersistenceStageRank(
	stage opensplunkv1.KnowledgeSearchStage,
) (uint8, bool) {
	switch stage {
	case opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION:
		return 1, true
	case opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS:
		return 2, true
	case opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD:
		return 3, true
	default:
		return 0, false
	}
}

type publicationTransitionCurrentTargetWidth struct {
	VersionFound      int64 `gorm:"column:version_found"`
	CurrentStateValid int64 `gorm:"column:current_state_valid"`
	CurrentActive     int64 `gorm:"column:current_active"`
	CurrentExact      int64 `gorm:"column:current_exact"`
	ObjectIDBytes     int64 `gorm:"column:knowledge_object_id_bytes"`
	TenantIDBytes     int64 `gorm:"column:tenant_id_bytes"`
	AppIDBytes        int64 `gorm:"column:app_id_bytes"`
	OwnerIDBytes      int64 `gorm:"column:owner_id_bytes"`
	ObjectTypeBytes   int64 `gorm:"column:object_type_bytes"`
	NameBytes         int64 `gorm:"column:name_bytes"`
	SharingScopeBytes int64 `gorm:"column:sharing_scope_bytes"`
	StateBytes        int64 `gorm:"column:state_bytes"`
	DigestBytes       int64 `gorm:"column:digest_bytes"`
	QuarantineBytes   int64 `gorm:"column:quarantine_reason_bytes"`
	MutationKindBytes int64 `gorm:"column:mutation_kind_bytes"`
}

func (width publicationTransitionCurrentTargetWidth) versionSize() versionSizeRecord {
	return versionSizeRecord{
		RegistrySizeRecord: registrySizeRecord{
			KnowledgeObjectIDBytes: width.ObjectIDBytes,
			TenantIDBytes:          width.TenantIDBytes,
			AppIDBytes:             width.AppIDBytes,
			OwnerIDBytes:           width.OwnerIDBytes,
			ObjectTypeBytes:        width.ObjectTypeBytes,
			NameBytes:              width.NameBytes,
			SharingScopeBytes:      width.SharingScopeBytes,
			StateBytes:             width.StateBytes,
			DigestBytes:            width.DigestBytes,
			QuarantineReasonBytes:  width.QuarantineBytes,
		},
		MutationKindBytes: width.MutationKindBytes,
	}
}

func readPublicationTransitionCurrentTargetVersions(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	objectIDs []string,
) (map[string]versionRecord, error) {
	result := make(map[string]versionRecord, len(objectIDs))
	database := tx.WithContext(ctx)
	err := listChunks(objectIDs, func(chunk []string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		query := publicationTransitionCurrentTargetQuery(database, tenantID, chunk)
		var widths []publicationTransitionCurrentTargetWidth
		if err := query.Session(&gorm.Session{}).Select(`
			CASE WHEN version.knowledge_object_id IS NULL THEN 0 ELSE 1 END AS version_found,
			CASE WHEN current.state IN ('draft', 'active', 'disabled', 'quarantined', 'deleted')
				THEN 1 ELSE 0 END AS current_state_valid,
			CASE WHEN current.state = 'active' THEN 1 ELSE 0 END AS current_active,
			CASE WHEN version.knowledge_object_id IS NOT NULL
				AND current.app_id = version.app_id
				AND current.owner_id = version.owner_id
				AND current.object_type = version.object_type
				AND current.name = version.name
				AND current.sharing_scope = version.sharing_scope
				AND current.state = version.state
				AND current.definition_digest IS version.definition_digest
				AND current.updated_at_unix_micro = version.created_at_unix_micro
				AND current.created_at_unix_micro BETWEEN 1 AND current.updated_at_unix_micro
				AND current.disabled_at_unix_micro IS NULL
				AND current.quarantined_at_unix_micro IS NULL
				AND current.deleted_at_unix_micro IS NULL
				AND current.quarantine_reason IS version.quarantine_reason
				THEN 1 ELSE 0 END AS current_exact,
			coalesce(length(CAST(version.knowledge_object_id AS BLOB)), 0)
				AS knowledge_object_id_bytes,
			coalesce(length(CAST(version.tenant_id AS BLOB)), 0) AS tenant_id_bytes,
			coalesce(length(CAST(version.app_id AS BLOB)), 0) AS app_id_bytes,
			coalesce(length(CAST(version.owner_id AS BLOB)), 0) AS owner_id_bytes,
			coalesce(length(CAST(version.object_type AS BLOB)), 0) AS object_type_bytes,
			coalesce(length(CAST(version.name AS BLOB)), 0) AS name_bytes,
			coalesce(length(CAST(version.sharing_scope AS BLOB)), 0) AS sharing_scope_bytes,
			coalesce(length(CAST(version.state AS BLOB)), 0) AS state_bytes,
			coalesce(length(version.definition_digest), 0) AS digest_bytes,
			coalesce(length(CAST(version.quarantine_reason AS BLOB)), 0)
				AS quarantine_reason_bytes,
			coalesce(length(CAST(version.mutation_kind AS BLOB)), 0) AS mutation_kind_bytes
		`).Limit(len(chunk) + 1).Find(&widths).Error; err != nil {
			return err
		}
		if len(widths) > len(chunk) {
			return invalidPublicationTransitionPersistenceTargets(
				"current target query returned duplicate identities",
			)
		}
		if len(widths) != len(chunk) {
			return fmt.Errorf(
				"%w: publication transition target is missing from its tenant current registry",
				control.ErrDependencyConflict,
			)
		}
		for _, width := range widths {
			switch {
			case width.CurrentStateValid != 1:
				return invalidPublicationTransitionPersistenceTargets(
					"current target lifecycle authority is invalid",
				)
			case width.CurrentActive != 1:
				return fmt.Errorf(
					"%w: publication transition target is not currently ACTIVE",
					control.ErrDependencyConflict,
				)
			case width.VersionFound != 1 || !validVersionSize(width.versionSize()) ||
				width.CurrentExact != 1:
				return invalidPublicationTransitionPersistenceTargets(
					"current target registry and immutable version disagree",
				)
			}
		}

		var records []versionRecord
		if err := query.Session(&gorm.Session{}).Select("version.*").
			Limit(len(chunk) + 1).Find(&records).Error; err != nil {
			return err
		}
		if len(records) != len(widths) {
			return invalidPublicationTransitionPersistenceTargets(
				"current target preflight changed",
			)
		}
		for _, record := range records {
			if err := validateVersion(record); err != nil {
				return invalidPublicationTransitionPersistenceTargets(
					"current target immutable version is invalid",
				)
			}
			if _, duplicate := result[record.KnowledgeObjectID]; duplicate {
				return invalidPublicationTransitionPersistenceTargets(
					"current target result duplicates an identity",
				)
			}
			result[record.KnowledgeObjectID] = record
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(result) != len(objectIDs) {
		return nil, fmt.Errorf(
			"%w: publication transition target closure is incomplete",
			control.ErrDependencyConflict,
		)
	}
	return result, nil
}

func publicationTransitionCurrentTargetQuery(
	database *gorm.DB,
	tenantID string,
	objectIDs []string,
) *gorm.DB {
	return database.Table("knowledge_objects AS current").
		Joins(`LEFT JOIN knowledge_object_versions AS version
			ON version.tenant_id = current.tenant_id
		   AND version.knowledge_object_id = current.knowledge_object_id
		   AND version.object_version = current.current_version`).
		Where("current.tenant_id = ? AND current.knowledge_object_id IN ?", tenantID, objectIDs).
		Order("current.knowledge_object_id ASC")
}

func invalidPublicationTransitionPersistenceTargets(reason string) error {
	return fmt.Errorf(
		"%w: publication transition persistence targets %s",
		ErrCorrupt,
		reason,
	)
}
