package knowledgecatalog

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

func (store *Store) objectFromCurrentRegistry(database *gorm.DB, registry registryRecord) (Object, error) {
	if err := validateRegistry(registry); err != nil {
		return Object{}, err
	}
	query := baseProjectionQuery(database).Where(
		"projection.tenant_id = ? AND projection.knowledge_object_id = ? AND projection.object_version = ?",
		registry.TenantID, registry.KnowledgeObjectID, registry.CurrentVersion,
	)
	projections, err := readProjectionRecords(query, 2)
	if err != nil {
		return Object{}, err
	}
	if len(projections) != 1 {
		return Object{}, fmt.Errorf("%w: current list projection is missing or ambiguous", ErrCorrupt)
	}
	return objectFromRegistryAndProjection(database, registry, projections[0])
}

func registryFromProjection(projection projectionRecord) registryRecord {
	return registryRecord{
		TenantID:               projection.TenantID,
		KnowledgeObjectID:      projection.KnowledgeObjectID,
		CurrentVersion:         projection.ObjectVersion,
		AppID:                  projection.AppID,
		OwnerID:                projection.OwnerID,
		ObjectType:             projection.ObjectType,
		Name:                   projection.Name,
		SharingScope:           projection.SharingScope,
		State:                  projection.State,
		DefinitionDigest:       bytes.Clone(projection.DefinitionDigest),
		CreatedAtUnixMicro:     projection.CreatedAtUnixMicro,
		UpdatedAtUnixMicro:     projection.UpdatedAtUnixMicro,
		DisabledAtUnixMicro:    cloneInt64(projection.DisabledAtUnixMicro),
		QuarantinedAtUnixMicro: cloneInt64(projection.QuarantinedAtUnixMicro),
		DeletedAtUnixMicro:     cloneInt64(projection.DeletedAtUnixMicro),
		QuarantineReason:       cloneString(projection.QuarantineReason),
	}
}

func objectFromRegistryAndProjection(
	database *gorm.DB,
	registry registryRecord,
	projection projectionRecord,
) (Object, error) {
	if err := validateProjectionScalars(projection, registry); err != nil {
		return Object{}, err
	}
	version, found, err := readVersionRecord(
		database, registry.TenantID, registry.KnowledgeObjectID, registry.CurrentVersion,
	)
	if err != nil {
		return Object{}, err
	}
	if !found || validateCurrentVersion(registry, version) != nil {
		return Object{}, fmt.Errorf("%w: current immutable version disagrees with registry", ErrCorrupt)
	}
	if err := validateCurrentChronology(database, registry, version); err != nil {
		return Object{}, err
	}
	lifecycle, err := readVersionLifecycle(database, version)
	if err != nil {
		return Object{}, err
	}
	if err := validateCurrentLifecycle(registry, version, lifecycle); err != nil {
		return Object{}, err
	}
	dependencies, err := readValidatedVersionDependencies(database, version)
	if err != nil {
		return Object{}, err
	}
	selectors, err := readSelectorRecords(
		database, registry.TenantID, registry.KnowledgeObjectID, registry.CurrentVersion,
	)
	if err != nil {
		return Object{}, err
	}
	if registry.State == StateQuarantined {
		if err := validateQuarantinedProjection(projection, selectors); err != nil {
			return Object{}, err
		}
		return objectFromCurrentScalars(registry, nil, nil)
	}
	normalized, definitionBytes, err := decodeVersionDefinition(database, version)
	if err != nil {
		return Object{}, err
	}
	if definitionBytes != projection.DefinitionBytes {
		return Object{}, fmt.Errorf("%w: projected definition byte authority disagrees", ErrCorrupt)
	}
	if err := validateDecodedIdentity(normalized, version); err != nil {
		return Object{}, err
	}
	if err := validateProjectionDefinition(projection, selectors, normalized); err != nil {
		return Object{}, err
	}
	if err := validateDecodedVersionDependencies(database, version, normalized, dependencies); err != nil {
		return Object{}, err
	}
	return objectFromCurrentScalars(registry, normalized.Definition, normalized.Digest)
}

func objectFromHistoricalVersion(
	database *gorm.DB,
	current registryRecord,
	version versionRecord,
) (Object, error) {
	if err := validateVersion(version); err != nil ||
		version.TenantID != current.TenantID || version.KnowledgeObjectID != current.KnowledgeObjectID ||
		version.State == StateQuarantined {
		return Object{}, fmt.Errorf("%w: historical immutable version is invalid", ErrCorrupt)
	}
	if err := validateHistoricalChronology(database, current, version); err != nil {
		return Object{}, err
	}
	lifecycle, err := readVersionLifecycle(database, version)
	if err != nil {
		return Object{}, err
	}
	dependencies, err := readValidatedVersionDependencies(database, version)
	if err != nil {
		return Object{}, err
	}
	normalized, _, err := decodeVersionDefinition(database, version)
	if err != nil {
		return Object{}, err
	}
	if err := validateDecodedIdentity(normalized, version); err != nil {
		return Object{}, err
	}
	if err := validateDecodedVersionDependencies(database, version, normalized, dependencies); err != nil {
		return Object{}, err
	}
	createdAt, err := canonicalTime(current.CreatedAtUnixMicro)
	if err != nil {
		return Object{}, err
	}
	updatedAt, err := canonicalTime(version.CreatedAtUnixMicro)
	if err != nil || updatedAt.Before(createdAt) {
		return Object{}, fmt.Errorf("%w: historical version timestamp is invalid", ErrCorrupt)
	}
	object := Object{
		KnowledgeObjectID: strings.Clone(version.KnowledgeObjectID),
		TenantID:          strings.Clone(version.TenantID),
		AppID:             strings.Clone(version.AppID),
		OwnerID:           strings.Clone(version.OwnerID),
		ObjectType:        version.ObjectType,
		Name:              strings.Clone(version.Name),
		// #nosec G115 -- immutable version validation requires a positive persisted version.
		Version:          uint64(version.ObjectVersion),
		SharingScope:     version.SharingScope,
		State:            version.State,
		Definition:       proto.Clone(normalized.Definition).(*opensplunkv1.KnowledgeObjectDefinition),
		DefinitionSHA256: bytes.Clone(normalized.Digest),
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
		QuarantineReason: cloneString(version.QuarantineReason),
	}
	object.DisabledAt, err = optionalCanonicalTime(lifecycle.DisabledAtUnixMicro)
	if err != nil {
		return Object{}, err
	}
	object.QuarantinedAt, err = optionalCanonicalTime(lifecycle.QuarantinedAtUnixMicro)
	if err != nil {
		return Object{}, err
	}
	object.DeletedAt, err = optionalCanonicalTime(lifecycle.DeletedAtUnixMicro)
	if err != nil {
		return Object{}, err
	}
	return object, nil
}

type decodedDefinition struct {
	Definition *opensplunkv1.KnowledgeObjectDefinition
	Digest     []byte
	ObjectType opensplunkv1.KnowledgeObjectType
	// ObjectTypeKnown is false only for an inactive opaque future body. In that
	// case the sealed registry/version ObjectType may be returned as safe
	// administrative display metadata, but this decoder provides no executable
	// semantic type authority.
	ObjectTypeKnown bool
	AppID           string
	Name            string
	SharingScope    opensplunkv1.SharingScope
	Description     *string
	Selector        *knowledge.Selector
}

func decodeVersionDefinition(database *gorm.DB, version versionRecord) (decodedDefinition, int64, error) {
	if len(version.DefinitionDigest) != persistedKnowledgeDefinitionDigestBytes {
		return decodedDefinition{}, 0, fmt.Errorf("%w: version definition digest is invalid", ErrCorrupt)
	}
	blob, err := readDefinitionBlob(database, version.TenantID, version.DefinitionDigest)
	if err != nil {
		return decodedDefinition{}, 0, err
	}
	return decodeVersionDefinitionBlob(version, blob)
}

func decodeVersionDefinitionBlob(
	version versionRecord,
	blob definitionBlobRecord,
) (decodedDefinition, int64, error) {
	if blob.TenantID != version.TenantID || !bytes.Equal(blob.DefinitionDigest, version.DefinitionDigest) {
		return decodedDefinition{}, 0, fmt.Errorf("%w: definition blob identity disagrees", ErrCorrupt)
	}
	if blob.DefinitionBytes < 1 || blob.DefinitionBytes > maximumDefinitionBytes ||
		int64(len(blob.DefinitionProto)) != blob.DefinitionBytes {
		return decodedDefinition{}, 0, fmt.Errorf("%w: definition blob shape disagrees", ErrCorrupt)
	}
	if _, err := canonicalTime(blob.CreatedAtUnixMicro); err != nil {
		return decodedDefinition{}, 0, fmt.Errorf("%w: definition blob timestamp is invalid", ErrCorrupt)
	}
	normalized, err := knowledgedefinition.DecodeCanonical(blob.DefinitionProto, version.DefinitionDigest)
	if err == nil {
		return decodedDefinition{
			Definition:      normalized.Definition,
			Digest:          bytes.Clone(normalized.Digest[:]),
			ObjectType:      normalized.ObjectType,
			ObjectTypeKnown: true,
			AppID:           normalized.AppID,
			Name:            normalized.Name,
			SharingScope:    normalized.SharingScope,
			Description:     cloneString(normalized.Description),
			Selector:        normalized.Selector,
		}, blob.DefinitionBytes, nil
	}
	protoState, ok := stateToProto(version.State)
	if !ok {
		return decodedDefinition{}, 0, fmt.Errorf("%w: immutable version state is unknown", ErrCorrupt)
	}
	future, futureErr := knowledgedefinition.DecodeCanonicalInactiveFutureBody(
		blob.DefinitionProto,
		version.DefinitionDigest,
		protoState,
	)
	if futureErr != nil {
		return decodedDefinition{}, 0, fmt.Errorf(
			"%w: definition decode failed: strict=%w; future=%w",
			ErrCorrupt,
			err,
			futureErr,
		)
	}
	return decodedDefinition{
		Definition:   future.Definition,
		Digest:       bytes.Clone(future.Digest[:]),
		AppID:        future.AppID,
		Name:         future.Name,
		SharingScope: future.SharingScope,
		Description:  cloneString(future.Description),
		Selector:     future.Selector,
	}, blob.DefinitionBytes, nil
}

func readValidatedVersionDependencies(
	database *gorm.DB,
	version versionRecord,
) ([]dependencyRecord, error) {
	seal, err := readDependencySeal(database, version.TenantID, version.KnowledgeObjectID, version.ObjectVersion)
	if err != nil {
		return nil, err
	}
	if seal.DependencyCount != version.DependencyCount {
		return nil, fmt.Errorf("%w: immutable dependency seal count disagrees with version", ErrCorrupt)
	}
	records, err := readDependencyRecords(database, version)
	if err != nil {
		return nil, err
	}
	if err := validateDependencyRecords(version, records); err != nil {
		return nil, err
	}
	return records[:len(records):len(records)], nil
}

func validateDependencyRecords(version versionRecord, records []dependencyRecord) error {
	if int64(len(records)) != version.DependencyCount {
		return fmt.Errorf("%w: immutable dependency row count disagrees with version", ErrCorrupt)
	}
	for index, record := range records {
		if record.TenantID != version.TenantID || record.SourceObjectID != version.KnowledgeObjectID ||
			record.SourceObjectVersion != version.ObjectVersion || record.Ordinal != int64(index) ||
			record.TargetKind != "object" || !validIdentity(record.TargetObjectID, maximumObjectIDBytes) ||
			record.TargetObjectVersion < 1 || record.DependencyRole != "field_input" || record.TargetFound != 1 ||
			(record.TargetObjectID == version.KnowledgeObjectID && record.TargetObjectVersion == version.ObjectVersion) {
			return fmt.Errorf("%w: immutable dependency row is invalid", ErrCorrupt)
		}
		if index > 0 {
			previous := records[index-1]
			if previous.TargetObjectID > record.TargetObjectID ||
				previous.TargetObjectID == record.TargetObjectID &&
					previous.TargetObjectVersion >= record.TargetObjectVersion {
				return fmt.Errorf("%w: immutable dependency order is noncanonical", ErrCorrupt)
			}
		}
	}
	return nil
}

func validateRegistry(record registryRecord) error {
	if !validIdentity(record.TenantID, maximumTenantIDBytes) ||
		!validIdentity(record.KnowledgeObjectID, maximumObjectIDBytes) ||
		record.CurrentVersion < 1 || record.CurrentVersion > maximumVersionsPerTenant ||
		!validIdentity(record.AppID, maximumAppIDBytes) ||
		!validIdentity(record.OwnerID, maximumOwnerIDBytes) || !record.ObjectType.Valid() ||
		!validIdentity(record.Name, maximumFilterBytes) || !record.SharingScope.Valid() || !record.State.valid() {
		return fmt.Errorf("%w: current registry scalar is invalid", ErrCorrupt)
	}
	createdAt, err := canonicalTime(record.CreatedAtUnixMicro)
	if err != nil {
		return err
	}
	updatedAt, err := canonicalTime(record.UpdatedAtUnixMicro)
	if err != nil || updatedAt.Before(createdAt) {
		return fmt.Errorf("%w: current registry timestamps are invalid", ErrCorrupt)
	}
	if !validStateShape(stateShape{
		state:              record.State,
		definitionDigest:   record.DefinitionDigest,
		disabledAt:         record.DisabledAtUnixMicro,
		quarantinedAt:      record.QuarantinedAtUnixMicro,
		deletedAt:          record.DeletedAtUnixMicro,
		quarantineReason:   record.QuarantineReason,
		createdAtUnixMicro: record.CreatedAtUnixMicro,
		updatedAtUnixMicro: record.UpdatedAtUnixMicro,
	}) {
		return fmt.Errorf("%w: current registry state shape is invalid", ErrCorrupt)
	}
	return nil
}

func validateVersion(record versionRecord) error {
	if !validIdentity(record.TenantID, maximumTenantIDBytes) ||
		!validIdentity(record.KnowledgeObjectID, maximumObjectIDBytes) || record.ObjectVersion < 1 ||
		record.ObjectVersion > maximumVersionsPerTenant ||
		!validIdentity(record.AppID, maximumAppIDBytes) || !validIdentity(record.OwnerID, maximumOwnerIDBytes) ||
		!record.ObjectType.Valid() || !validIdentity(record.Name, maximumFilterBytes) ||
		!record.SharingScope.Valid() || !record.State.valid() || record.DependencyCount < 0 ||
		record.DependencyCount > maximumDependenciesPerVersion {
		return fmt.Errorf("%w: immutable version scalar is invalid", ErrCorrupt)
	}
	if _, err := canonicalTime(record.CreatedAtUnixMicro); err != nil {
		return err
	}
	if (record.State == StateQuarantined) != (len(record.DefinitionDigest) == 0) ||
		(record.State == StateQuarantined) != (record.QuarantineReason != nil) {
		return fmt.Errorf("%w: immutable version definition shape is invalid", ErrCorrupt)
	}
	validMutation := (record.ObjectVersion == 1 && record.MutationKind == "create" &&
		(record.State == StateDraft || record.State == StateActive)) ||
		(record.ObjectVersion > 1 && record.MutationKind != "create" &&
			((record.MutationKind == "enable" && record.State == StateActive) ||
				(record.MutationKind == "disable" && record.State == StateDisabled) ||
				(record.MutationKind == "quarantine" && record.State == StateQuarantined) ||
				(record.MutationKind == "delete" && record.State == StateDeleted) ||
				((record.MutationKind == "update" || record.MutationKind == "scope_change") &&
					(record.State == StateDraft || record.State == StateActive || record.State == StateDisabled))))
	if !validMutation {
		return fmt.Errorf("%w: immutable version mutation shape is invalid", ErrCorrupt)
	}
	return nil
}

func validateCurrentVersion(registry registryRecord, version versionRecord) error {
	if err := validateVersion(version); err != nil {
		return err
	}
	if registry.TenantID != version.TenantID || registry.KnowledgeObjectID != version.KnowledgeObjectID ||
		registry.CurrentVersion != version.ObjectVersion || registry.AppID != version.AppID ||
		registry.OwnerID != version.OwnerID || registry.ObjectType != version.ObjectType ||
		registry.Name != version.Name || registry.SharingScope != version.SharingScope ||
		registry.State != version.State || !bytes.Equal(registry.DefinitionDigest, version.DefinitionDigest) ||
		!equalOptionalString(registry.QuarantineReason, version.QuarantineReason) ||
		registry.UpdatedAtUnixMicro != version.CreatedAtUnixMicro {
		return fmt.Errorf("%w: current version does not exactly match registry", ErrCorrupt)
	}
	return nil
}

func validateProjectionScalars(projection projectionRecord, registry registryRecord) error {
	if projection.TenantID != registry.TenantID || projection.KnowledgeObjectID != registry.KnowledgeObjectID ||
		projection.ObjectVersion != registry.CurrentVersion || projection.AppID != registry.AppID ||
		projection.OwnerID != registry.OwnerID || projection.ObjectType != registry.ObjectType ||
		projection.Name != registry.Name || projection.SharingScope != registry.SharingScope ||
		projection.State != registry.State || projection.DescriptionPresent < 0 || projection.DescriptionPresent > 1 ||
		projection.IndexSelectorCount < 0 || projection.IndexSelectorCount > maximumSelectorPatternsPerDimension ||
		projection.HostSelectorCount < 0 || projection.HostSelectorCount > maximumSelectorPatternsPerDimension ||
		projection.SourceSelectorCount < 0 || projection.SourceSelectorCount > maximumSelectorPatternsPerDimension ||
		projection.SourcetypeSelectorCount < 0 || projection.SourcetypeSelectorCount > maximumSelectorPatternsPerDimension ||
		projection.SelectorValueBytes < 0 || projection.SelectorValueBytes > maximumSelectorAggregateValueBytes ||
		projection.CanonicalSelectorBytes < 0 || projection.CanonicalSelectorBytes > maximumCanonicalSelectorBytes ||
		projection.ProjectionBytes < 0 || projection.ProjectionBytes > maximumProjectionBytes ||
		projection.DependencyCount < 0 || projection.DependencyCount > maximumDependenciesPerVersion ||
		projection.SealProjectionBytes != projection.ProjectionBytes ||
		projection.SealCanonicalSelectorBytes != projection.CanonicalSelectorBytes {
		return fmt.Errorf("%w: current list projection scalar is invalid", ErrCorrupt)
	}
	if projection.ProjectionBytes != int64(len(projection.Description))+projection.SelectorValueBytes {
		return fmt.Errorf("%w: current list projection byte accounting is invalid", ErrCorrupt)
	}
	return nil
}

func validateQuarantinedProjection(projection projectionRecord, selectors []selectorRecord) error {
	if projection.DescriptionPresent != 0 || projection.Description != "" || len(selectors) != 0 ||
		projection.IndexSelectorCount != 0 || projection.HostSelectorCount != 0 ||
		projection.SourceSelectorCount != 0 || projection.SourcetypeSelectorCount != 0 ||
		projection.SelectorValueBytes != 0 || projection.CanonicalSelectorBytes != 0 || projection.ProjectionBytes != 0 {
		return fmt.Errorf("%w: quarantined projection exposes definition-derived data", ErrCorrupt)
	}
	return nil
}

func validateDecodedIdentity(normalized decodedDefinition, version versionRecord) error {
	objectType, ok := objectTypeFromProto(normalized.ObjectType)
	sharingScope, scopeOK := sharingScopeFromProto(normalized.SharingScope)
	// An opaque inactive body has no type the older binary can derive. All
	// registry/version/projection scalar authorities have already been required
	// to agree; skipping only this body-derived comparison is the documented
	// forward-compatibility exception and is never used on an executable object.
	if !scopeOK || normalized.ObjectTypeKnown && (!ok || objectType != version.ObjectType) ||
		sharingScope != version.SharingScope ||
		normalized.AppID != version.AppID || normalized.Name != version.Name ||
		!bytes.Equal(normalized.Digest, version.DefinitionDigest) {
		return fmt.Errorf("%w: decoded definition disagrees with immutable version", ErrCorrupt)
	}
	return nil
}

func validateProjectionDefinition(
	projection projectionRecord,
	selectors []selectorRecord,
	normalized decodedDefinition,
) error {
	description := ""
	descriptionPresent := int64(0)
	if normalized.Description != nil {
		description = *normalized.Description
		descriptionPresent = 1
	}
	if projection.DescriptionPresent != descriptionPresent || projection.Description != description {
		return fmt.Errorf("%w: projected description disagrees with definition", ErrCorrupt)
	}
	dimensions := []struct {
		name      string
		dimension knowledge.Dimension
		count     int64
	}{
		{"index", knowledge.DimensionIndex, projection.IndexSelectorCount},
		{"host", knowledge.DimensionHost, projection.HostSelectorCount},
		{"source", knowledge.DimensionSource, projection.SourceSelectorCount},
		{"sourcetype", knowledge.DimensionSourcetype, projection.SourcetypeSelectorCount},
	}
	expected := make([]selectorRecord, 0, normalized.Selector.Stats().Patterns)
	for _, dimension := range dimensions {
		values := normalized.Selector.Patterns(dimension.dimension)
		if int64(len(values)) != dimension.count {
			return fmt.Errorf("%w: projected selector count disagrees with definition", ErrCorrupt)
		}
		for ordinal, value := range values {
			pattern, err := knowledge.NormalizePattern(value)
			if err != nil {
				return fmt.Errorf("%w: canonical selector pattern is invalid", ErrCorrupt)
			}
			matchKind := "wildcard"
			if pattern.IsLiteral() {
				matchKind = "exact"
			}
			expected = append(expected, selectorRecord{
				Dimension:  dimension.name,
				Ordinal:    int64(ordinal),
				MatchKind:  matchKind,
				Value:      value,
				ValueBytes: int64(len(value)),
			})
		}
	}
	if len(expected) != len(selectors) {
		return fmt.Errorf("%w: projected selector rows disagree with definition", ErrCorrupt)
	}
	var selectorValueBytes int64
	for index := range expected {
		if expected[index] != selectors[index] {
			return fmt.Errorf("%w: projected selector row disagrees with definition", ErrCorrupt)
		}
		selectorValueBytes += expected[index].ValueBytes
	}
	if selectorValueBytes != projection.SelectorValueBytes ||
		int64(len(normalized.Selector.CanonicalBytes())) != projection.CanonicalSelectorBytes {
		return fmt.Errorf("%w: projected selector byte charge disagrees with definition", ErrCorrupt)
	}
	return nil
}

func objectFromCurrentScalars(
	record registryRecord,
	definition *opensplunkv1.KnowledgeObjectDefinition,
	digest []byte,
) (Object, error) {
	createdAt, err := canonicalTime(record.CreatedAtUnixMicro)
	if err != nil {
		return Object{}, err
	}
	updatedAt, err := canonicalTime(record.UpdatedAtUnixMicro)
	if err != nil {
		return Object{}, err
	}
	object := Object{
		KnowledgeObjectID: strings.Clone(record.KnowledgeObjectID),
		TenantID:          strings.Clone(record.TenantID),
		AppID:             strings.Clone(record.AppID),
		OwnerID:           strings.Clone(record.OwnerID),
		ObjectType:        record.ObjectType,
		Name:              strings.Clone(record.Name),
		// #nosec G115 -- registry validation requires a positive current version.
		Version:          uint64(record.CurrentVersion),
		SharingScope:     record.SharingScope,
		State:            record.State,
		DefinitionSHA256: bytes.Clone(digest),
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
		QuarantineReason: cloneString(record.QuarantineReason),
	}
	if definition != nil {
		object.Definition = proto.Clone(definition).(*opensplunkv1.KnowledgeObjectDefinition)
	}
	object.DisabledAt, err = optionalCanonicalTime(record.DisabledAtUnixMicro)
	if err != nil {
		return Object{}, err
	}
	object.QuarantinedAt, err = optionalCanonicalTime(record.QuarantinedAtUnixMicro)
	if err != nil {
		return Object{}, err
	}
	object.DeletedAt, err = optionalCanonicalTime(record.DeletedAtUnixMicro)
	if err != nil {
		return Object{}, err
	}
	return object, nil
}

type stateShape struct {
	state              State
	definitionDigest   []byte
	disabledAt         *int64
	quarantinedAt      *int64
	deletedAt          *int64
	quarantineReason   *string
	createdAtUnixMicro int64
	updatedAtUnixMicro int64
}

func validStateShape(shape stateShape) bool {
	if shape.updatedAtUnixMicro < shape.createdAtUnixMicro {
		return false
	}
	switch shape.state {
	case StateDraft, StateActive:
		return len(shape.definitionDigest) == persistedKnowledgeDefinitionDigestBytes &&
			shape.disabledAt == nil && shape.quarantinedAt == nil && shape.deletedAt == nil &&
			shape.quarantineReason == nil
	case StateDisabled:
		return len(shape.definitionDigest) == persistedKnowledgeDefinitionDigestBytes &&
			shape.disabledAt != nil && *shape.disabledAt >= shape.createdAtUnixMicro &&
			*shape.disabledAt <= shape.updatedAtUnixMicro &&
			shape.quarantinedAt == nil && shape.deletedAt == nil && shape.quarantineReason == nil
	case StateQuarantined:
		return len(shape.definitionDigest) == 0 && shape.disabledAt == nil &&
			shape.quarantinedAt != nil && *shape.quarantinedAt == shape.updatedAtUnixMicro &&
			shape.deletedAt == nil && shape.quarantineReason != nil &&
			(*shape.quarantineReason == "root_corruption" || *shape.quarantineReason == "dependency_recovery")
	case StateDeleted:
		return len(shape.definitionDigest) == persistedKnowledgeDefinitionDigestBytes &&
			shape.disabledAt == nil && shape.quarantinedAt == nil && shape.deletedAt != nil &&
			*shape.deletedAt == shape.updatedAtUnixMicro && shape.quarantineReason == nil
	default:
		return false
	}
}

func canonicalTime(unixMicro int64) (time.Time, error) {
	value, ok := audit.CanonicalOccurrenceTime(time.UnixMicro(unixMicro))
	if !ok || value.UnixMicro() != unixMicro {
		return time.Time{}, fmt.Errorf("%w: persisted timestamp is invalid", ErrCorrupt)
	}
	return value, nil
}

func optionalCanonicalTime(value *int64) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	converted, err := canonicalTime(*value)
	if err != nil {
		return nil, err
	}
	return &converted, nil
}

func objectTypeFromProto(value opensplunkv1.KnowledgeObjectType) (ObjectType, bool) {
	switch value {
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return ObjectTypeFieldExtraction, true
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return ObjectTypeFieldAlias, true
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return ObjectTypeCalculatedField, true
	default:
		return "", false
	}
}

func sharingScopeFromProto(value opensplunkv1.SharingScope) (SharingScope, bool) {
	switch value {
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE:
		return SharingScopePrivate, true
	case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
		return SharingScopeApp, true
	case opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
		return SharingScopeGlobal, true
	default:
		return "", false
	}
}

func stateToProto(value State) (opensplunkv1.KnowledgeObjectState, bool) {
	switch value {
	case StateDraft:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT, true
	case StateActive:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE, true
	case StateDisabled:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, true
	case StateQuarantined:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED, true
	case StateDeleted:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED, true
	default:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_UNSPECIFIED, false
	}
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := strings.Clone(*value)
	return &cloned
}

func equalOptionalString(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
