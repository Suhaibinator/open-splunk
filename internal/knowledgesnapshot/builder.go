// Package knowledgesnapshot prepares the canonical immutable knowledge
// authority admitted for one search. Database authorization and final compiler
// sealing remain outside this package's KO-0G surface.
package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"google.golang.org/protobuf/proto"
)

const (
	FormatVersion                = 1
	CompilerCompatibilityVersion = "0.1"

	MaximumExecutableObjects        = 256
	MaximumShadows                  = 512
	MaximumWarnings                 = MaximumShadows
	MaximumCanonicalBytes           = 4 << 20
	MaximumCandidateDefinitionBytes = 8 << 20
	MaximumDependencyNodes          = 256
	MaximumDependencyEdges          = 1024
	MaximumDependencyDepth          = 16
	MaximumGeneratedOperators       = 256
	MaximumGeneratedFields          = 512
	MaximumExtractionOutputs        = 64
	MaximumJSONEvaluationWorkUnits  = splpath.MaximumEvaluationWorkUnits
	MaximumRegexPrograms            = 64
	MaximumRegexWorkUnits           = MaximumRegexPrograms * splregex.MaximumExtractionProgramWorkUnits
	MaximumRegexCaptureBytes        = 4 << 20
	MaximumScalarExpressions        = 32
	MaximumScalarExpressionNodes    = MaximumScalarExpressions * 1024
	MaximumScalarPredicates         = 32
	MaximumGeneratedSQLBytes        = 256 << 10

	maximumIdentityBytes   = 255
	maximumAppIDBytes      = 128
	maximumObjectIDBytes   = 128
	maximumCatalogRevision = uint64(math.MaxInt64 - 1)
)

const digestDomain = "open-splunk-knowledge-snapshot-v0.1\x00"

var (
	ErrInvalidInput  = errors.New("invalid knowledge snapshot input")
	ErrResourceLimit = errors.New("knowledge snapshot resource limit exceeded")
)

// Object is one already-authorized, visible, active, immutable winning object.
// Prepare revalidates every scalar, canonical definition, and executable
// semantic authority before retaining detached data.
type Object struct {
	KnowledgeObjectID string
	Version           uint64
	ObjectType        opensplunkv1.KnowledgeObjectType
	Name              string
	AppID             string
	OwnerID           string
	SharingScope      opensplunkv1.SharingScope
	Definition        *opensplunkv1.KnowledgeObjectDefinition
	DefinitionSHA256  []byte
}

// ObjectSummary is detached definition-free inventory for one winning object.
// It preserves the canonical stage order assigned by Prepare without cloning
// the retained executable definition body.
type ObjectSummary struct {
	KnowledgeObjectID string
	Version           uint64
	ObjectType        opensplunkv1.KnowledgeObjectType
	Name              string
	AppID             string
	OwnerID           string
	SharingScope      opensplunkv1.SharingScope
	DefinitionSHA256  []byte
}

// Shadow is one authorized visible active object which lost whole-object
// precedence. WinnerObjectID and WinnerVersion identify an entry in
// Input.Objects.
type Shadow struct {
	WinnerObjectID    string
	WinnerVersion     uint64
	KnowledgeObjectID string
	Version           uint64
	ObjectType        opensplunkv1.KnowledgeObjectType
	Name              string
	AppID             string
	OwnerID           string
	SharingScope      opensplunkv1.SharingScope
	Definition        *opensplunkv1.KnowledgeObjectDefinition
	DefinitionSHA256  []byte
}

// Dependency is one direct immutable object-to-object edge. KO-0 supports
// only FIELD_INPUT object dependencies; exact lookup versions enter through
// the sealed compiler evidence consumed by Finalize rather than this input.
type Dependency struct {
	SourceObjectID string
	SourceVersion  uint64
	TargetObjectID string
	TargetVersion  uint64
	Role           opensplunkv1.KnowledgeDependencyRole
}

// Input contains the complete scalar and immutable catalog authority for one
// snapshot. EffectiveAuthorizedIndexes may be unsorted and contain duplicates;
// Prepare normalizes them with the search-admission index-scope authority.
// Compiler-produced evidence is deliberately absent from this constructible
// input.
type Input struct {
	TenantID                   string
	PrincipalID                string
	AppID                      string
	TenantCatalogRevision      uint64
	TenantCatalogStateToken    []byte
	AppCatalogRevision         *uint64
	EffectiveAuthorizedIndexes []string
	Objects                    []Object
	Dependencies               []Dependency
	Shadows                    []Shadow
}

// StaticCharges are the exact compiler-independent knowledge contributions of
// winning definitions. KO-0H must exact-combine and cross-check them with sealed
// whole-query evidence. They do not include generated operators, SQL bytes, or
// runtime regex capture reservation.
type StaticCharges struct {
	GeneratedFields          uint32
	ExtractionRegexPrograms  uint32
	ExtractionRegexWorkUnits uint64
	ExtractionOutputs        uint32
	JSONEvaluationWorkUnits  uint32
	ScalarExpressions        uint32
	ScalarExpressionNodes    uint32
	ScalarPredicates         uint32
}

// AuthoritySummary is detached definition-free inventory for an unfinalized
// canonical authority.
type AuthoritySummary struct {
	TenantID                   string
	PrincipalID                string
	AppID                      string
	TenantCatalogRevision      uint64
	TenantCatalogStateToken    []byte
	AppCatalogRevision         *uint64
	EffectiveAuthorizedIndexes []string
	ExecutableObjects          uint32
	Dependencies               uint32
	Shadows                    uint32
}

// Authority is an opaque, immutable, compiler-unfinalized catalog authority.
// It cannot be marshaled or digested through the public KO-0G API. KO-0H will
// pair it with sealed whole-query compiler evidence before snapshot
// finalization.
type Authority struct {
	base         *opensplunkv1.KnowledgeSnapshot
	objects      []Object
	dependencies []Dependency
	shadows      []Shadow
	static       StaticCharges
	prelude      knowledgeprogram.Program
}

// IsZero reports whether no prepared catalog authority is present.
func (authority Authority) IsZero() bool { return authority.base == nil }

// Summary returns detached definition-free authority.
func (authority Authority) Summary() AuthoritySummary {
	if authority.base == nil {
		return AuthoritySummary{}
	}
	return AuthoritySummary{
		TenantID:                   strings.Clone(authority.base.TenantId),
		PrincipalID:                strings.Clone(authority.base.PrincipalId),
		AppID:                      strings.Clone(authority.base.AppId),
		TenantCatalogRevision:      authority.base.TenantCatalogRevision,
		TenantCatalogStateToken:    bytes.Clone(authority.base.TenantCatalogStateToken),
		AppCatalogRevision:         cloneUint64(authority.base.AppCatalogRevision),
		EffectiveAuthorizedIndexes: slices.Clone(authority.base.EffectiveAuthorizedIndexes),
		ExecutableObjects:          uint32(len(authority.objects)),      // #nosec G115 -- prepared authority collections are resource-bounded.
		Dependencies:               uint32(len(authority.dependencies)), // #nosec G115 -- prepared authority collections are resource-bounded.
		Shadows:                    uint32(len(authority.shadows)),      // #nosec G115 -- prepared authority collections are resource-bounded.
	}
}

// Objects returns detached executable definitions in canonical stage order.
func (authority Authority) Objects() []Object { return cloneObjects(authority.objects) }

// ObjectSummaries returns detached definition-free winner inventory in
// canonical stage order without cloning executable definition bodies.
func (authority Authority) ObjectSummaries() []ObjectSummary {
	result := make([]ObjectSummary, len(authority.objects))
	for index, object := range authority.objects {
		result[index] = ObjectSummary{
			KnowledgeObjectID: strings.Clone(object.KnowledgeObjectID),
			Version:           object.Version,
			ObjectType:        object.ObjectType,
			Name:              strings.Clone(object.Name),
			AppID:             strings.Clone(object.AppID),
			OwnerID:           strings.Clone(object.OwnerID),
			SharingScope:      object.SharingScope,
			DefinitionSHA256:  bytes.Clone(object.DefinitionSHA256),
		}
	}
	return result
}

// Dependencies returns detached exact winner edges in canonical order.
func (authority Authority) Dependencies() []Dependency {
	return slices.Clone(authority.dependencies)
}

// Shadows returns detached visible loser scalars in canonical shadow order.
// Definition is always nil: Prepare validates every submitted shadow body but
// the finalized snapshot retains only its immutable digest authority.
func (authority Authority) Shadows() []Shadow { return cloneShadows(authority.shadows) }

// StaticCharges returns the immutable compiler-independent semantic inventory.
func (authority Authority) StaticCharges() StaticCharges { return authority.static }

// Prelude returns the exact detached backend-neutral executable program
// independently reconstructed from this canonical authority.
func (authority Authority) Prelude() knowledgeprogram.Program {
	return authority.prelude.Clone()
}

// Snapshot is an opaque, immutable finalized snapshot. KO-0G deliberately
// exposes no public constructor or finalization method for a nonzero value.
type Snapshot struct {
	message                *opensplunkv1.KnowledgeSnapshot
	encoded                []byte
	digest                 [sha256.Size]byte
	prelude                knowledgeprogram.Program
	retainedExecutionFacts RetainedExecutionAuthorityFacts
}

// Proto returns a detached protobuf representation.
func (snapshot Snapshot) Proto() *opensplunkv1.KnowledgeSnapshot {
	if snapshot.message == nil {
		return nil
	}
	cloned, _ := proto.Clone(snapshot.message).(*opensplunkv1.KnowledgeSnapshot)
	return cloned
}

// Encoded returns the deterministic protobuf encoding including its digest.
func (snapshot Snapshot) Encoded() []byte { return bytes.Clone(snapshot.encoded) }

// Digest returns the fixed snapshot digest.
func (snapshot Snapshot) Digest() [sha256.Size]byte { return snapshot.digest }

// IsZero reports whether no finalized authority is present.
func (snapshot Snapshot) IsZero() bool { return snapshot.message == nil }

// CanonicalBytes returns the frozen self-excluding canonical byte charge.
func (snapshot Snapshot) CanonicalBytes() uint64 {
	if snapshot.message == nil || snapshot.message.BudgetCharges == nil {
		return 0
	}
	return snapshot.message.BudgetCharges.CanonicalSnapshotBytes
}

// Clone returns a fully detached immutable snapshot. The canonical encoding
// and digest remain byte-for-byte identical.
func (snapshot Snapshot) Clone() Snapshot {
	if snapshot.message == nil {
		return Snapshot{}
	}
	message, ok := proto.Clone(snapshot.message).(*opensplunkv1.KnowledgeSnapshot)
	if !ok || message == nil {
		return Snapshot{}
	}
	return Snapshot{
		message:                message,
		encoded:                bytes.Clone(snapshot.encoded),
		digest:                 snapshot.digest,
		prelude:                snapshot.prelude.Clone(),
		retainedExecutionFacts: snapshot.retainedExecutionFacts,
	}
}

// Prelude returns the exact detached backend-neutral executable program
// sealed into this finalized authority.
func (snapshot Snapshot) Prelude() knowledgeprogram.Program {
	return snapshot.prelude.Clone()
}

type canonicalObject struct {
	input      Object
	normalized knowledgedefinition.Normalized
	semantics  definitionSemantics
	stage      opensplunkv1.KnowledgeSearchStage
	stageRank  uint8
	ordinal    uint32
}

type definitionSemantics struct {
	inputFields  []string
	outputFields []string
	charges      StaticCharges
}

type objectKey struct {
	id      string
	version uint64
}

type canonicalDependency struct {
	input       Dependency
	source      *canonicalObject
	target      *canonicalObject
	sourceDepth uint32
}

type trustedAuthoredCompilerEvidence struct {
	regexPrograms      uint32
	regexWorkUnits     uint64
	extractionOutputs  uint32
	jsonEvaluationWork uint32
	scalarPredicates   uint32
}

// trustedCompilerEvidence is the package-private detached form of the exact
// compiler seal. Knowledge-program identity and charges stay separate from the
// authored suffix so finalization cannot accept an equal aggregate assembled
// from different authorities.
type trustedCompilerEvidence struct {
	knowledgeProgramPresent    bool
	knowledgeProgramCommitment [sha256.Size]byte
	knowledgeProgramObjects    uint32
	knowledgeProgramCharges    knowledgeprogram.Charges
	lookupAssets               []trustedLookupAssetEvidence
	authored                   trustedAuthoredCompilerEvidence
	regexCaptureBytes          uint64
	generatedSQLBytes          uint64
}

type trustedCompilerTotals struct {
	regexPrograms      uint32
	regexWorkUnits     uint64
	extractionOutputs  uint32
	jsonEvaluationWork uint32
	scalarPredicates   uint32
}

// Prepare validates, semantically compiles, orders, and detaches one complete
// catalog authority. It does not accept compiler evidence, populate
// compiler-dependent charges, calculate canonical bytes, or produce a digest.
func Prepare(input Input) (Authority, error) {
	if err := validateTopLevel(input); err != nil {
		return Authority{}, err
	}
	indexScope, err := indexread.NormalizeScope(input.TenantID, input.EffectiveAuthorizedIndexes)
	if err != nil {
		return Authority{}, fmt.Errorf("%w: effective authorized indexes: %w", ErrInvalidInput, err)
	}

	var candidateDefinitionBytes uint64
	objects, byKey, selectorPatterns, selectorWork, static, err := canonicalizeObjects(
		input,
		&candidateDefinitionBytes,
	)
	if err != nil {
		return Authority{}, err
	}
	if err := validateParallelStageSemantics(objects); err != nil {
		return Authority{}, err
	}
	dependencies, dependencyNodes, dependencyDepth, err := canonicalizeDependencies(input.Dependencies, byKey)
	if err != nil {
		return Authority{}, err
	}
	shadows, err := canonicalizeShadows(input, byKey, &candidateDefinitionBytes)
	if err != nil {
		return Authority{}, err
	}

	snapshotObjects := make([]*opensplunkv1.KnowledgeSnapshotObject, len(objects))
	authorityObjects := make([]Object, len(objects))
	stageOrdinals := make(map[opensplunkv1.KnowledgeSearchStage]uint32, 3)
	for index, object := range objects {
		stageOrdinal := stageOrdinals[object.stage]
		stageOrdinals[object.stage] = stageOrdinal + 1
		snapshotObjects[index] = &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             object.stage,
			StageOrdinal:      stageOrdinal,
			KnowledgeObjectId: strings.Clone(object.input.KnowledgeObjectID),
			Version:           object.input.Version,
			ObjectType:        object.input.ObjectType,
			Name:              strings.Clone(object.input.Name),
			AppId:             strings.Clone(object.input.AppID),
			OwnerId:           strings.Clone(object.input.OwnerID),
			SharingScope:      object.input.SharingScope,
			Definition:        object.normalized.Definition,
			DefinitionSha256:  bytes.Clone(object.input.DefinitionSHA256),
		}
		authorityObjects[index] = canonicalAuthorityObject(object)
	}

	snapshotShadows := make([]*opensplunkv1.KnowledgeSnapshotShadow, len(shadows))
	snapshotWarnings := make([]*opensplunkv1.KnowledgeSnapshotWarning, len(shadows))
	for index, shadow := range shadows {
		winner := byKey[objectKey{id: shadow.WinnerObjectID, version: shadow.WinnerVersion}]
		snapshotShadows[index] = &opensplunkv1.KnowledgeSnapshotShadow{
			ShadowOrdinal:           uint32(index),
			WinnerResolutionOrdinal: winner.ordinal,
			KnowledgeObjectId:       strings.Clone(shadow.KnowledgeObjectID),
			Version:                 shadow.Version,
			ObjectType:              shadow.ObjectType,
			Name:                    strings.Clone(shadow.Name),
			AppId:                   strings.Clone(shadow.AppID),
			OwnerId:                 strings.Clone(shadow.OwnerID),
			SharingScope:            shadow.SharingScope,
			DefinitionSha256:        bytes.Clone(shadow.DefinitionSHA256),
		}
		shadowOrdinal := uint32(index)
		snapshotWarnings[index] = &opensplunkv1.KnowledgeSnapshotWarning{
			WarningOrdinal: uint32(index),
			Kind:           opensplunkv1.KnowledgeSnapshotWarningKind_KNOWLEDGE_SNAPSHOT_WARNING_KIND_SHADOWED_OBJECT,
			ShadowOrdinal:  &shadowOrdinal,
		}
	}

	snapshotDependencies := make([]*opensplunkv1.KnowledgeObjectDependency, len(dependencies))
	authorityDependencies := make([]Dependency, len(dependencies))
	for index, dependency := range dependencies {
		snapshotDependencies[index] = &opensplunkv1.KnowledgeObjectDependency{
			Source: versionReference(dependency.source),
			Target: &opensplunkv1.KnowledgeDependencyTarget{
				Target: &opensplunkv1.KnowledgeDependencyTarget_Object{
					Object: versionReference(dependency.target),
				},
			},
			Role:             dependency.input.Role,
			SourceStage:      dependency.source.stage,
			TargetStage:      dependency.target.stage,
			TopologicalDepth: dependency.sourceDepth,
			CanonicalOrdinal: uint32(index),
		}
		authorityDependencies[index] = cloneDependency(dependency.input)
	}
	prelude, err := knowledgeprogram.Prepare(knowledgeprogram.Input{
		Objects:      snapshotObjects,
		Dependencies: snapshotDependencies,
	})
	if err != nil {
		if errors.Is(err, knowledgeprogram.ErrResourceLimit) {
			return Authority{}, fmt.Errorf("%w: prepare knowledge prelude: %w", ErrResourceLimit, err)
		}
		return Authority{}, fmt.Errorf("%w: prepare knowledge prelude: %w", ErrInvalidInput, err)
	}
	if err := validatePreludeAuthority(prelude, static, uint32(len(objects))); err != nil { // #nosec G115 -- objects are bounded by MaximumObjects.
		return Authority{}, err
	}

	base := &opensplunkv1.KnowledgeSnapshot{
		FormatVersion:                FormatVersion,
		TenantId:                     strings.Clone(input.TenantID),
		PrincipalId:                  strings.Clone(input.PrincipalID),
		AppId:                        strings.Clone(input.AppID),
		TenantCatalogRevision:        input.TenantCatalogRevision,
		AppCatalogRevision:           cloneUint64(input.AppCatalogRevision),
		CompilerCompatibilityVersion: CompilerCompatibilityVersion,
		Objects:                      snapshotObjects,
		Dependencies:                 snapshotDependencies,
		LookupAssets:                 nil,
		EffectiveAuthorizedIndexes:   slices.Clone(indexScope.IndexNames),
		Shadows:                      snapshotShadows,
		Warnings:                     snapshotWarnings,
		BudgetCharges: &opensplunkv1.KnowledgeSnapshotBudgetCharges{
			ExecutableObjects:         uint32(len(objects)), // #nosec G115 -- objects are bounded by MaximumObjects.
			SelectorPatterns:          selectorPatterns,
			SelectorWildcardWorkUnits: selectorWork,
			DependencyNodes:           dependencyNodes,
			DependencyEdges:           uint32(len(dependencies)), // #nosec G115 -- dependencies are bounded by MaximumDependencies.
			DependencyDepth:           dependencyDepth,
		},
		TenantCatalogStateToken: bytes.Clone(input.TenantCatalogStateToken),
	}
	if err := validateBaseSkeletonSize(base); err != nil {
		return Authority{}, err
	}
	return Authority{
		base:         base,
		objects:      authorityObjects,
		dependencies: authorityDependencies,
		shadows:      shadowAuthorities(shadows),
		static:       static,
		prelude:      prelude,
	}, nil
}

func validatePreludeAuthority(
	prelude knowledgeprogram.Program,
	static StaticCharges,
	objects uint32,
) error {
	if prelude.IsZero() || prelude.ObjectCount() != objects || prelude.IsEmpty() != (objects == 0) {
		return fmt.Errorf("%w: prepared knowledge prelude inventory disagrees", ErrInvalidInput)
	}
	charges := prelude.Charges()
	if charges.GeneratedFields != static.GeneratedFields ||
		charges.RegexPrograms != static.ExtractionRegexPrograms ||
		charges.RegexWorkUnits != static.ExtractionRegexWorkUnits ||
		charges.ExtractionOutputs != static.ExtractionOutputs ||
		charges.JSONEvaluationWork != static.JSONEvaluationWorkUnits ||
		charges.ScalarExpressions != static.ScalarExpressions ||
		charges.ScalarExpressionNodes != static.ScalarExpressionNodes ||
		charges.ScalarPredicates != static.ScalarPredicates ||
		(objects == 0) != (charges.GeneratedOperators == 0) {
		return fmt.Errorf("%w: prepared knowledge prelude charges disagree", ErrInvalidInput)
	}
	if commitment, ok := prelude.Commitment(); !ok || commitment == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: prepared knowledge prelude commitment is absent", ErrInvalidInput)
	}
	return nil
}

func validateTopLevel(input Input) error {
	if !validIdentity(input.TenantID, maximumIdentityBytes) ||
		!validIdentity(input.PrincipalID, maximumIdentityBytes) ||
		!validIdentity(input.AppID, maximumAppIDBytes) {
		return fmt.Errorf("%w: tenant, principal, or app identity is invalid", ErrInvalidInput)
	}
	if input.TenantCatalogRevision > maximumCatalogRevision || len(input.TenantCatalogStateToken) != sha256.Size {
		return fmt.Errorf("%w: tenant catalog authority is invalid", ErrInvalidInput)
	}
	if input.AppCatalogRevision != nil && (*input.AppCatalogRevision == 0 || *input.AppCatalogRevision > math.MaxInt64) {
		return fmt.Errorf("%w: present app catalog revision must be positive", ErrInvalidInput)
	}
	if len(input.Objects) > MaximumExecutableObjects {
		return fmt.Errorf("%w: executable objects exceed %d", ErrResourceLimit, MaximumExecutableObjects)
	}
	if len(input.Dependencies) > MaximumDependencyEdges {
		return fmt.Errorf("%w: dependencies exceed %d", ErrResourceLimit, MaximumDependencyEdges)
	}
	if len(input.Shadows) > MaximumShadows {
		return fmt.Errorf("%w: shadows exceed %d", ErrResourceLimit, MaximumShadows)
	}
	return nil
}

func canonicalizeObjects(input Input, candidateDefinitionBytes *uint64) (
	[]*canonicalObject,
	map[objectKey]*canonicalObject,
	uint32,
	uint64,
	StaticCharges,
	error,
) {
	objects := make([]*canonicalObject, 0, len(input.Objects))
	byKey := make(map[objectKey]*canonicalObject, len(input.Objects))
	identities := make(map[string]struct{}, len(input.Objects))
	names := make(map[string]struct{}, len(input.Objects))
	var selectorPatterns uint64
	var selectorWork uint64
	var aggregate StaticCharges
	for position := range input.Objects {
		candidate := input.Objects[position]
		stage, stageRank, err := stageForObjectType(candidate.ObjectType)
		if err != nil {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: object %d: %w", ErrInvalidInput, position, err)
		}
		if !validIdentity(candidate.KnowledgeObjectID, maximumObjectIDBytes) ||
			candidate.Version == 0 || candidate.Version > math.MaxInt64 ||
			!validIdentity(candidate.AppID, maximumAppIDBytes) ||
			!validIdentity(candidate.OwnerID, maximumIdentityBytes) {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: object %d identity is invalid", ErrInvalidInput, position)
		}
		name, err := knowledge.NormalizeName(candidate.Name)
		if err != nil || name.String() != candidate.Name {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: object %d name is not canonical", ErrInvalidInput, position)
		}
		if !authorizedIdentity(input, candidate.AppID, candidate.OwnerID, candidate.SharingScope) {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: object %d is outside the authorized scope", ErrInvalidInput, position)
		}
		if len(candidate.DefinitionSHA256) != sha256.Size {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: object %d digest is invalid", ErrInvalidInput, position)
		}
		normalized, err := knowledgedefinition.Normalize(candidate.Definition)
		if err != nil {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: object %d definition: %w", ErrInvalidInput, position, err)
		}
		if err := chargeCandidateDefinitionBytes(candidateDefinitionBytes, len(normalized.Bytes)); err != nil {
			return nil, nil, 0, 0, StaticCharges{}, err
		}
		if !proto.Equal(candidate.Definition, normalized.Definition) ||
			!bytes.Equal(candidate.DefinitionSHA256, normalized.Digest[:]) ||
			normalized.ObjectType != candidate.ObjectType ||
			normalized.Name != candidate.Name || normalized.AppID != candidate.AppID ||
			normalized.SharingScope != candidate.SharingScope {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: object %d definition authority disagrees", ErrInvalidInput, position)
		}
		semantics, err := compileDefinitionSemantics(normalized)
		if err != nil {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: object %d definition is not executable: %w", ErrInvalidInput, position, err)
		}
		key := objectKey{id: candidate.KnowledgeObjectID, version: candidate.Version}
		if _, duplicate := byKey[key]; duplicate {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: duplicate object version", ErrInvalidInput)
		}
		if _, duplicate := identities[candidate.KnowledgeObjectID]; duplicate {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: duplicate object identity", ErrInvalidInput)
		}
		resolvedNameKey := fmt.Sprintf("%d\x00%s", candidate.ObjectType, candidate.Name)
		if _, duplicate := names[resolvedNameKey]; duplicate {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: resolved object name is ambiguous", ErrInvalidInput)
		}
		if err := addStaticCharges(&aggregate, semantics.charges); err != nil {
			return nil, nil, 0, 0, StaticCharges{}, err
		}
		identities[candidate.KnowledgeObjectID] = struct{}{}
		names[resolvedNameKey] = struct{}{}
		object := &canonicalObject{
			input: candidate, normalized: normalized, semantics: semantics,
			stage: stage, stageRank: stageRank,
		}
		objects = append(objects, object)
		byKey[key] = object
		stats := normalized.Selector.Stats()
		selectorPatterns += stats.Patterns
		if selectorPatterns > math.MaxUint32 {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: selector pattern charge overflows", ErrResourceLimit)
		}
		selectorWork, err = knowledge.ChargeSnapshotSelectorWork(selectorWork, normalized.Selector)
		if err != nil {
			return nil, nil, 0, 0, StaticCharges{}, fmt.Errorf("%w: selector work: %w", ErrResourceLimit, err)
		}
	}
	sort.Slice(objects, func(left, right int) bool {
		if objects[left].stageRank != objects[right].stageRank {
			return objects[left].stageRank < objects[right].stageRank
		}
		if objects[left].input.Name != objects[right].input.Name {
			return objects[left].input.Name < objects[right].input.Name
		}
		return objects[left].input.KnowledgeObjectID < objects[right].input.KnowledgeObjectID
	})
	// Winner ordinals must be final before shadow ordering observes them.
	for index := range objects {
		objects[index].ordinal = uint32(index)
	}
	return objects, byKey, uint32(selectorPatterns), selectorWork, aggregate, nil
}

func compileDefinitionSemantics(normalized knowledgedefinition.Normalized) (definitionSemantics, error) {
	if normalized.Definition == nil || normalized.Selector == nil {
		return definitionSemantics{}, errors.New("normalized definition authority is incomplete")
	}
	var semantics definitionSemantics
	switch body := normalized.Definition.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		if body == nil || body.FieldExtraction == nil {
			return definitionSemantics{}, errors.New("field extraction is nil")
		}
		semantics.inputFields = normalizedFields([]string{body.FieldExtraction.GetInputField()})
		switch extraction := body.FieldExtraction.GetExtraction().(type) {
		case *opensplunkv1.FieldExtractionDefinition_Regex:
			if extraction == nil || extraction.Regex == nil {
				return definitionSemantics{}, errors.New("regex extraction is nil")
			}
			compiled, err := splregex.CompileExtractionPattern(extraction.Regex.GetPattern())
			if err != nil {
				return definitionSemantics{}, fmt.Errorf("regex extraction: %w", err)
			}
			outputs := extraction.Regex.GetOutputFields()
			if compiled.GroupCount != len(compiled.Captures) || len(outputs) != len(compiled.Captures) {
				return definitionSemantics{}, errors.New("regex captures do not exactly match declared named outputs")
			}
			for index, capture := range compiled.Captures {
				if outputs[index] != capture.Name {
					return definitionSemantics{}, errors.New("regex capture order disagrees with declared outputs")
				}
			}
			semantics.outputFields = normalizedFields(outputs)
			semantics.charges.GeneratedFields = uint32(len(outputs)) // #nosec G115 -- regex outputs are bounded by the extraction limit.
			semantics.charges.ExtractionRegexPrograms = 1
			// The regex compiler returns a positive value bounded by MaximumExtractionProgramWorkUnits.
			semantics.charges.ExtractionRegexWorkUnits = uint64(compiled.ProgramWorkUnits) // #nosec G115 -- the compiler-enforced bound is far below MaxUint64.
			semantics.charges.ExtractionOutputs = uint32(len(outputs))                     // #nosec G115 -- regex outputs are bounded by the extraction limit.
		case *opensplunkv1.FieldExtractionDefinition_Json:
			if extraction == nil || extraction.Json == nil {
				return definitionSemantics{}, errors.New("JSON extraction is nil")
			}
			steps, err := splpath.ParseJSON(extraction.Json.GetPath())
			if err != nil {
				return definitionSemantics{}, fmt.Errorf("JSON extraction: %w", err)
			}
			work := splpath.EvaluationWorkUnits(steps)
			if work < 1 || work > MaximumJSONEvaluationWorkUnits {
				return definitionSemantics{}, errors.New("JSON extraction work is invalid")
			}
			semantics.outputFields = normalizedFields([]string{extraction.Json.GetOutputField()})
			semantics.charges.GeneratedFields = 1
			semantics.charges.ExtractionOutputs = 1
			semantics.charges.JSONEvaluationWorkUnits = uint32(work)
		default:
			return definitionSemantics{}, errors.New("field extraction kind is unsupported")
		}
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		if body == nil || body.FieldAlias == nil {
			return definitionSemantics{}, errors.New("field alias is nil")
		}
		semantics.inputFields = normalizedFields([]string{body.FieldAlias.GetSourceField()})
		semantics.outputFields = normalizedFields([]string{body.FieldAlias.GetDestinationField()})
		semantics.charges.GeneratedFields = 1
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		if body == nil || body.CalculatedField == nil {
			return definitionSemantics{}, errors.New("calculated field is nil")
		}
		parsed, err := spl.ParseScalarExpression(body.CalculatedField.GetExpression())
		if err != nil {
			return definitionSemantics{}, fmt.Errorf("calculated expression: %w", err)
		}
		analysis, err := spl.AnalyzeScalarExpression(parsed)
		if err != nil {
			return definitionSemantics{}, fmt.Errorf("calculated expression: %w", err)
		}
		if analysis.Nodes < 1 || analysis.Nodes > MaximumScalarExpressionNodes ||
			analysis.Predicates > MaximumScalarPredicates {
			return definitionSemantics{}, errors.New("calculated expression charge is invalid")
		}
		semantics.inputFields = slices.Clone(analysis.InputFields)
		semantics.outputFields = normalizedFields([]string{body.CalculatedField.GetDestinationField()})
		semantics.charges.GeneratedFields = 1
		semantics.charges.ScalarExpressions = 1
		semantics.charges.ScalarExpressionNodes = analysis.Nodes
		semantics.charges.ScalarPredicates = analysis.Predicates
	default:
		return definitionSemantics{}, errors.New("definition body is unsupported")
	}
	if len(semantics.outputFields) == 0 {
		return definitionSemantics{}, errors.New("definition has no semantic output field")
	}
	return semantics, nil
}

func validateParallelStageSemantics(objects []*canonicalObject) error {
	for leftIndex, left := range objects {
		if left == nil || left.normalized.Selector == nil {
			return fmt.Errorf("%w: parallel-stage authority is incomplete", ErrInvalidInput)
		}
		for rightIndex := leftIndex + 1; rightIndex < len(objects); rightIndex++ {
			right := objects[rightIndex]
			if right == nil || right.normalized.Selector == nil {
				return fmt.Errorf("%w: parallel-stage authority is incomplete", ErrInvalidInput)
			}
			if left.stage != right.stage {
				break
			}
			if knowledge.SelectorsProvablyDisjoint(left.normalized.Selector, right.normalized.Selector) {
				continue
			}
			if fieldsIntersect(left.semantics.outputFields, right.semantics.outputFields) {
				return fmt.Errorf(
					"%w: same-stage objects %q and %q may write the same destination",
					ErrInvalidInput,
					left.input.KnowledgeObjectID,
					right.input.KnowledgeObjectID,
				)
			}
			if fieldsIntersect(left.semantics.outputFields, right.semantics.inputFields) ||
				fieldsIntersect(right.semantics.outputFields, left.semantics.inputFields) {
				return fmt.Errorf(
					"%w: same-stage objects %q and %q form a possible data dependency",
					ErrInvalidInput,
					left.input.KnowledgeObjectID,
					right.input.KnowledgeObjectID,
				)
			}
		}
	}
	return nil
}

func addStaticCharges(aggregate *StaticCharges, add StaticCharges) error {
	if aggregate == nil {
		return fmt.Errorf("%w: static charge destination is nil", ErrInvalidInput)
	}
	aggregate.GeneratedFields += add.GeneratedFields
	aggregate.ExtractionRegexPrograms += add.ExtractionRegexPrograms
	aggregate.ExtractionRegexWorkUnits += add.ExtractionRegexWorkUnits
	aggregate.ExtractionOutputs += add.ExtractionOutputs
	aggregate.JSONEvaluationWorkUnits += add.JSONEvaluationWorkUnits
	aggregate.ScalarExpressions += add.ScalarExpressions
	aggregate.ScalarExpressionNodes += add.ScalarExpressionNodes
	aggregate.ScalarPredicates += add.ScalarPredicates
	if aggregate.GeneratedFields > MaximumGeneratedFields ||
		aggregate.ExtractionRegexPrograms > MaximumRegexPrograms ||
		aggregate.ExtractionRegexWorkUnits > MaximumRegexWorkUnits ||
		aggregate.ExtractionOutputs > MaximumExtractionOutputs ||
		aggregate.JSONEvaluationWorkUnits > MaximumJSONEvaluationWorkUnits ||
		aggregate.ScalarExpressions > MaximumScalarExpressions ||
		aggregate.ScalarExpressionNodes > MaximumScalarExpressionNodes ||
		aggregate.ScalarPredicates > MaximumScalarPredicates {
		return fmt.Errorf("%w: winning definition semantics exceed query-wide limits", ErrResourceLimit)
	}
	return nil
}

func canonicalizeDependencies(
	input []Dependency,
	byKey map[objectKey]*canonicalObject,
) ([]canonicalDependency, uint32, uint32, error) {
	dependencies := make([]canonicalDependency, len(input))
	adjacency := make(map[objectKey][]objectKey, len(input))
	// Isolated executable roots are dependency nodes even without an edge.
	nodes := make(map[objectKey]struct{}, len(byKey))
	for key := range byKey {
		nodes[key] = struct{}{}
	}
	type edgeKey struct {
		source objectKey
		target objectKey
		role   opensplunkv1.KnowledgeDependencyRole
	}
	expected := make(map[edgeKey]struct{})
	for sourceKey, source := range byKey {
		for targetKey, target := range byKey {
			if sourceKey == targetKey || source.stageRank <= target.stageRank ||
				!fieldsIntersect(source.semantics.inputFields, target.semantics.outputFields) ||
				knowledge.SelectorsProvablyDisjoint(source.normalized.Selector, target.normalized.Selector) {
				continue
			}
			if !dependencyScopeAllows(source.input, target.input) {
				return nil, 0, 0, fmt.Errorf(
					"%w: derived dependency violates sharing authority",
					ErrInvalidInput,
				)
			}
			if !knowledge.SelectorImplies(source.normalized.Selector, target.normalized.Selector) {
				return nil, 0, 0, fmt.Errorf(
					"%w: derived dependency selector implication is unproven",
					ErrInvalidInput,
				)
			}
			expected[edgeKey{
				source: sourceKey,
				target: targetKey,
				role:   opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
			}] = struct{}{}
		}
	}
	if len(expected) > MaximumDependencyEdges {
		return nil, 0, 0, fmt.Errorf(
			"%w: derived dependencies exceed %d",
			ErrResourceLimit,
			MaximumDependencyEdges,
		)
	}
	seen := make(map[edgeKey]struct{}, len(input))
	for position, edge := range input {
		if edge.Role != opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT {
			return nil, 0, 0, fmt.Errorf("%w: dependency %d role is unsupported", ErrInvalidInput, position)
		}
		sourceKey := objectKey{id: edge.SourceObjectID, version: edge.SourceVersion}
		targetKey := objectKey{id: edge.TargetObjectID, version: edge.TargetVersion}
		source, sourceFound := byKey[sourceKey]
		target, targetFound := byKey[targetKey]
		if !sourceFound || !targetFound {
			return nil, 0, 0, fmt.Errorf("%w: dependency %d is outside the exact winner closure", ErrInvalidInput, position)
		}
		if source.stageRank <= target.stageRank {
			return nil, 0, 0, fmt.Errorf("%w: dependency %d does not point to an earlier stage", ErrInvalidInput, position)
		}
		if !dependencyScopeAllows(source.input, target.input) {
			return nil, 0, 0, fmt.Errorf("%w: dependency %d violates sharing authority", ErrInvalidInput, position)
		}
		if !fieldsIntersect(source.semantics.inputFields, target.semantics.outputFields) {
			return nil, 0, 0, fmt.Errorf("%w: dependency %d field identity disagrees", ErrInvalidInput, position)
		}
		if !knowledge.SelectorImplies(source.normalized.Selector, target.normalized.Selector) {
			return nil, 0, 0, fmt.Errorf("%w: dependency %d selector implication is unproven", ErrInvalidInput, position)
		}
		identity := edgeKey{source: sourceKey, target: targetKey, role: edge.Role}
		if _, duplicate := seen[identity]; duplicate {
			return nil, 0, 0, fmt.Errorf("%w: duplicate dependency", ErrInvalidInput)
		}
		seen[identity] = struct{}{}
		adjacency[sourceKey] = append(adjacency[sourceKey], targetKey)
		dependencies[position] = canonicalDependency{
			input: cloneDependency(edge), source: source, target: target,
		}
	}
	if len(seen) != len(expected) {
		return nil, 0, 0, fmt.Errorf("%w: submitted dependencies do not equal derived FIELD_INPUT edges", ErrInvalidInput)
	}
	for edge := range expected {
		if _, found := seen[edge]; !found {
			return nil, 0, 0, fmt.Errorf("%w: a derived FIELD_INPUT edge is missing", ErrInvalidInput)
		}
	}
	if len(nodes) > MaximumDependencyNodes {
		return nil, 0, 0, fmt.Errorf("%w: dependency nodes exceed %d", ErrResourceLimit, MaximumDependencyNodes)
	}

	depths := make(map[objectKey]uint32, len(nodes))
	states := make(map[objectKey]uint8, len(nodes))
	var depthOf func(objectKey) (uint32, error)
	depthOf = func(key objectKey) (uint32, error) {
		switch states[key] {
		case 1:
			return 0, fmt.Errorf("%w: dependency graph contains a cycle", ErrInvalidInput)
		case 2:
			return depths[key], nil
		}
		states[key] = 1
		var depth uint32
		for _, target := range adjacency[key] {
			targetDepth, err := depthOf(target)
			if err != nil {
				return 0, err
			}
			if targetDepth >= MaximumDependencyDepth {
				return 0, fmt.Errorf("%w: dependency depth exceeds %d", ErrResourceLimit, MaximumDependencyDepth)
			}
			depth = max(depth, targetDepth+1)
		}
		states[key] = 2
		depths[key] = depth
		return depth, nil
	}
	var maximumDepth uint32
	for key := range nodes {
		depth, err := depthOf(key)
		if err != nil {
			return nil, 0, 0, err
		}
		maximumDepth = max(maximumDepth, depth)
	}
	for index := range dependencies {
		key := objectKey{id: dependencies[index].input.SourceObjectID, version: dependencies[index].input.SourceVersion}
		dependencies[index].sourceDepth = depths[key]
	}
	sort.Slice(dependencies, func(left, right int) bool {
		l, r := dependencies[left], dependencies[right]
		if l.sourceDepth != r.sourceDepth {
			return l.sourceDepth < r.sourceDepth
		}
		if l.source.stageRank != r.source.stageRank {
			return l.source.stageRank < r.source.stageRank
		}
		if l.input.SourceObjectID != r.input.SourceObjectID {
			return l.input.SourceObjectID < r.input.SourceObjectID
		}
		if l.input.SourceVersion != r.input.SourceVersion {
			return l.input.SourceVersion < r.input.SourceVersion
		}
		if l.input.TargetObjectID != r.input.TargetObjectID {
			return l.input.TargetObjectID < r.input.TargetObjectID
		}
		if l.input.TargetVersion != r.input.TargetVersion {
			return l.input.TargetVersion < r.input.TargetVersion
		}
		return l.input.Role < r.input.Role
	})
	return dependencies, uint32(len(nodes)), maximumDepth, nil // #nosec G115 -- dependency nodes are bounded by MaximumDependencies.
}

func canonicalizeShadows(
	input Input,
	byKey map[objectKey]*canonicalObject,
	candidateDefinitionBytes *uint64,
) ([]Shadow, error) {
	shadows := make([]Shadow, len(input.Shadows))
	seen := make(map[string]struct{}, len(shadows))
	precedenceSlots := make(map[string]struct{}, len(shadows))
	for position, submitted := range input.Shadows {
		winner := byKey[objectKey{id: submitted.WinnerObjectID, version: submitted.WinnerVersion}]
		if winner == nil {
			return nil, fmt.Errorf("%w: shadow %d winner is missing", ErrInvalidInput, position)
		}
		if !validIdentity(submitted.KnowledgeObjectID, maximumObjectIDBytes) ||
			submitted.Version == 0 || submitted.Version > math.MaxInt64 ||
			!validIdentity(submitted.AppID, maximumAppIDBytes) ||
			!validIdentity(submitted.OwnerID, maximumIdentityBytes) || len(submitted.DefinitionSHA256) != sha256.Size {
			return nil, fmt.Errorf("%w: shadow %d identity is invalid", ErrInvalidInput, position)
		}
		name, err := knowledge.NormalizeName(submitted.Name)
		if err != nil || name.String() != submitted.Name ||
			submitted.ObjectType != winner.input.ObjectType || submitted.Name != winner.input.Name {
			return nil, fmt.Errorf("%w: shadow %d resolved identity disagrees", ErrInvalidInput, position)
		}
		if !authorizedIdentity(input, submitted.AppID, submitted.OwnerID, submitted.SharingScope) ||
			precedenceRank(winner.input.SharingScope) <= precedenceRank(submitted.SharingScope) {
			return nil, fmt.Errorf("%w: shadow %d precedence is invalid", ErrInvalidInput, position)
		}
		normalized, err := knowledgedefinition.Normalize(submitted.Definition)
		if err != nil {
			return nil, fmt.Errorf("%w: shadow %d definition authority disagrees", ErrInvalidInput, position)
		}
		if err := chargeCandidateDefinitionBytes(candidateDefinitionBytes, len(normalized.Bytes)); err != nil {
			return nil, err
		}
		if !proto.Equal(submitted.Definition, normalized.Definition) ||
			!bytes.Equal(submitted.DefinitionSHA256, normalized.Digest[:]) ||
			normalized.ObjectType != submitted.ObjectType || normalized.Name != submitted.Name ||
			normalized.AppID != submitted.AppID || normalized.SharingScope != submitted.SharingScope {
			return nil, fmt.Errorf("%w: shadow %d definition authority disagrees", ErrInvalidInput, position)
		}
		if _, err := compileDefinitionSemantics(normalized); err != nil {
			return nil, fmt.Errorf("%w: shadow %d definition is not executable: %w", ErrInvalidInput, position, err)
		}
		if submitted.KnowledgeObjectID == winner.input.KnowledgeObjectID {
			return nil, fmt.Errorf("%w: shadow %d aliases its winner", ErrInvalidInput, position)
		}
		if objectIDIsWinner(byKey, submitted.KnowledgeObjectID) {
			return nil, fmt.Errorf("%w: shadow %d is also executable", ErrInvalidInput, position)
		}
		if _, duplicate := seen[submitted.KnowledgeObjectID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate shadow", ErrInvalidInput)
		}
		seen[submitted.KnowledgeObjectID] = struct{}{}
		slot := fmt.Sprintf("%s\x00%d\x00%d", submitted.WinnerObjectID, submitted.WinnerVersion, submitted.SharingScope)
		if _, duplicate := precedenceSlots[slot]; duplicate {
			return nil, fmt.Errorf("%w: duplicate shadow precedence slot", ErrInvalidInput)
		}
		precedenceSlots[slot] = struct{}{}
		canonical := cloneShadow(submitted)
		canonical.Definition = normalized.Definition
		canonical.DefinitionSHA256 = bytes.Clone(normalized.Digest[:])
		shadows[position] = canonical
	}
	sort.Slice(shadows, func(left, right int) bool {
		lWinner := byKey[objectKey{id: shadows[left].WinnerObjectID, version: shadows[left].WinnerVersion}]
		rWinner := byKey[objectKey{id: shadows[right].WinnerObjectID, version: shadows[right].WinnerVersion}]
		if lWinner.ordinal != rWinner.ordinal {
			return lWinner.ordinal < rWinner.ordinal
		}
		leftRank, rightRank := precedenceRank(shadows[left].SharingScope), precedenceRank(shadows[right].SharingScope)
		if leftRank != rightRank {
			return leftRank > rightRank
		}
		return shadows[left].KnowledgeObjectID < shadows[right].KnowledgeObjectID
	})
	return shadows, nil
}

func dependencyScopeAllows(source, target Object) bool {
	switch source.SharingScope {
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE:
		return target.SharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL ||
			target.SharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_APP && target.AppID == source.AppID ||
			target.SharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE &&
				target.AppID == source.AppID && target.OwnerID == source.OwnerID
	case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
		return target.SharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL ||
			target.SharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_APP && target.AppID == source.AppID
	case opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
		return target.SharingScope == opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL
	default:
		return false
	}
}

func normalizedFields(fields []string) []string {
	normalized := slices.Clone(fields)
	sort.Strings(normalized)
	write := 0
	for _, field := range normalized {
		if field == "" || write > 0 && normalized[write-1] == field {
			continue
		}
		normalized[write] = field
		write++
	}
	normalized = normalized[:write]
	return normalized[:len(normalized):len(normalized)]
}

func fieldsIntersect(left, right []string) bool {
	for leftIndex, rightIndex := 0, 0; leftIndex < len(left) && rightIndex < len(right); {
		switch {
		case left[leftIndex] < right[rightIndex]:
			leftIndex++
		case left[leftIndex] > right[rightIndex]:
			rightIndex++
		default:
			return true
		}
	}
	return false
}

func objectIDIsWinner(byKey map[objectKey]*canonicalObject, objectID string) bool {
	for key := range byKey {
		if key.id == objectID {
			return true
		}
	}
	return false
}

func chargeCandidateDefinitionBytes(total *uint64, size int) error {
	if total == nil || size <= 0 || uint64(size) > MaximumCandidateDefinitionBytes ||
		*total > MaximumCandidateDefinitionBytes-uint64(size) {
		return fmt.Errorf(
			"%w: candidate definition bytes exceed %d",
			ErrResourceLimit,
			MaximumCandidateDefinitionBytes,
		)
	}
	*total += uint64(size)
	return nil
}

func validateBaseSkeletonSize(base *opensplunkv1.KnowledgeSnapshot) error {
	if base == nil || base.BudgetCharges == nil {
		return fmt.Errorf("%w: prepared snapshot skeleton is incomplete", ErrInvalidInput)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(base)
	if err != nil {
		return fmt.Errorf("%w: deterministic snapshot skeleton marshal: %w", ErrInvalidInput, err)
	}
	if len(encoded) > MaximumCanonicalBytes {
		return fmt.Errorf("%w: canonical snapshot bytes exceed %d", ErrResourceLimit, MaximumCanonicalBytes)
	}
	return nil
}

func finalize(authority Authority, evidence trustedCompilerEvidence) (Snapshot, error) {
	if authority.base == nil || authority.base.BudgetCharges == nil {
		return Snapshot{}, fmt.Errorf("%w: prepared authority is absent", ErrInvalidInput)
	}
	totals, err := validateTrustedCompilerEvidence(authority, evidence)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot, ok := proto.Clone(authority.base).(*opensplunkv1.KnowledgeSnapshot)
	if !ok || snapshot == nil || snapshot.BudgetCharges == nil {
		return Snapshot{}, fmt.Errorf("%w: prepared authority cannot be detached", ErrInvalidInput)
	}
	lookupAssets, err := canonicalSnapshotLookupAssets(
		authority.base.GetTenantId(),
		evidence.lookupAssets,
	)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.LookupAssets = lookupAssets
	knowledge := evidence.knowledgeProgramCharges
	snapshot.BudgetCharges.GeneratedOperators = knowledge.GeneratedOperators
	snapshot.BudgetCharges.GeneratedFields = knowledge.GeneratedFields
	snapshot.BudgetCharges.RegexPrograms = totals.regexPrograms
	snapshot.BudgetCharges.RegexWorkUnits = totals.regexWorkUnits
	snapshot.BudgetCharges.RegexCaptureBytes = evidence.regexCaptureBytes
	snapshot.BudgetCharges.ScalarExpressions = knowledge.ScalarExpressions
	snapshot.BudgetCharges.ScalarExpressionNodes = knowledge.ScalarExpressionNodes
	snapshot.BudgetCharges.GeneratedSqlBytes = evidence.generatedSQLBytes
	return digestSnapshot(snapshot, authority.prelude)
}

// Finalize seals this exact prepared authority against one exact compiler-
// produced ClickHouse execution. It accepts no caller-constructible budget
// counters and requires the exact present knowledge-program commitment even
// for an admitted empty authority.
func (authority Authority) Finalize(compiled clickhouse.CompiledQuery) (Snapshot, error) {
	if authority.base == nil || authority.base.BudgetCharges == nil {
		return Snapshot{}, fmt.Errorf("%w: prepared authority is absent", ErrInvalidInput)
	}
	compilerEvidence, ok := compiled.KnowledgeSnapshotEvidenceFor(authority.prelude)
	if !ok {
		return Snapshot{}, fmt.Errorf("%w: compiled query knowledge evidence is absent or unsealed", ErrInvalidInput)
	}
	if compilerEvidence.TenantID() != authority.base.GetTenantId() ||
		!slices.Equal(compilerEvidence.EffectiveIndexes(), authority.base.GetEffectiveAuthorizedIndexes()) {
		return Snapshot{}, fmt.Errorf("%w: compiled query read scope disagrees with prepared authority", ErrInvalidInput)
	}
	commitment, commitmentOK := compilerEvidence.KnowledgeProgramCommitment()
	if !commitmentOK {
		return Snapshot{}, fmt.Errorf("%w: compiled query knowledge commitment is absent", ErrInvalidInput)
	}
	lookupVersions, lookupVersionsOK := compiled.LookupAssetVersions()
	if !lookupVersionsOK {
		return Snapshot{}, fmt.Errorf(
			"%w: compiled query lookup evidence is absent or unsealed",
			ErrInvalidInput,
		)
	}
	lookupAssets := make([]trustedLookupAssetEvidence, len(lookupVersions))
	for index, version := range lookupVersions {
		lookupAssets[index] = trustedLookupAssetEvidence{
			tenantID:      version.TenantID(),
			lookupID:      version.LookupID(),
			lookupVersion: version.LookupVersion(),
			objectID:      version.AssetID(),
			version:       version.AssetVersion(),
			sizeBytes:     version.SizeBytes(),
			contentSHA256: version.ContentSHA256(),
		}
	}
	return finalize(authority, trustedCompilerEvidence{
		knowledgeProgramPresent:    compilerEvidence.KnowledgeProgramPresent(),
		knowledgeProgramCommitment: commitment,
		knowledgeProgramObjects:    compilerEvidence.KnowledgeProgramObjectCount(),
		knowledgeProgramCharges:    compilerEvidence.KnowledgeProgramCharges(),
		lookupAssets:               lookupAssets,
		authored: trustedAuthoredCompilerEvidence{
			regexPrograms:      compilerEvidence.AuthoredRegexPrograms(),
			regexWorkUnits:     compilerEvidence.AuthoredRegexWorkUnits(),
			extractionOutputs:  compilerEvidence.AuthoredExtractionOutputs(),
			jsonEvaluationWork: compilerEvidence.AuthoredJSONEvaluationWork(),
			scalarPredicates:   compilerEvidence.AuthoredScalarPredicates(),
		},
		regexCaptureBytes: compilerEvidence.RegexCaptureBytes(),
		generatedSQLBytes: compilerEvidence.GeneratedSQLBytes(),
	})
}

func validateTrustedCompilerEvidence(
	authority Authority,
	evidence trustedCompilerEvidence,
) (trustedCompilerTotals, error) {
	invalid := func(message string) (trustedCompilerTotals, error) {
		return trustedCompilerTotals{}, fmt.Errorf("%w: %s", ErrInvalidInput, message)
	}
	resource := func() (trustedCompilerTotals, error) {
		return trustedCompilerTotals{}, fmt.Errorf(
			"%w: trusted compiler evidence exceeds compatibility limits",
			ErrResourceLimit,
		)
	}
	if authority.base == nil || authority.base.BudgetCharges == nil {
		return invalid("prepared authority is absent")
	}
	objects := uint32(len(authority.objects)) // #nosec G115 -- prepared authority objects are bounded by MaximumObjects.
	if err := validatePreludeAuthority(authority.prelude, authority.static, objects); err != nil {
		return trustedCompilerTotals{}, err
	}
	commitment, commitmentOK := authority.prelude.Commitment()
	knowledge := evidence.knowledgeProgramCharges
	if !commitmentOK || !evidence.knowledgeProgramPresent ||
		evidence.knowledgeProgramCommitment != commitment ||
		evidence.knowledgeProgramObjects != objects ||
		knowledge != authority.prelude.Charges() {
		return invalid("trusted compiler knowledge authority disagrees")
	}
	if knowledge.GeneratedFields != authority.static.GeneratedFields ||
		knowledge.RegexPrograms != authority.static.ExtractionRegexPrograms ||
		knowledge.RegexWorkUnits != authority.static.ExtractionRegexWorkUnits ||
		knowledge.ExtractionOutputs != authority.static.ExtractionOutputs ||
		knowledge.JSONEvaluationWork != authority.static.JSONEvaluationWorkUnits ||
		knowledge.ScalarExpressions != authority.static.ScalarExpressions ||
		knowledge.ScalarExpressionNodes != authority.static.ScalarExpressionNodes ||
		knowledge.ScalarPredicates != authority.static.ScalarPredicates {
		return invalid("trusted compiler knowledge charges disagree with static semantics")
	}
	if knowledge.GeneratedOperators > MaximumGeneratedOperators ||
		knowledge.GeneratedFields > MaximumGeneratedFields ||
		knowledge.ScalarExpressions > MaximumScalarExpressions ||
		knowledge.ScalarExpressionNodes > MaximumScalarExpressionNodes ||
		evidence.generatedSQLBytes > MaximumGeneratedSQLBytes {
		return resource()
	}

	addUint32 := func(knowledgeValue, authoredValue, maximum uint32) (uint32, bool) {
		if knowledgeValue > maximum || authoredValue > maximum-knowledgeValue {
			return 0, false
		}
		return knowledgeValue + authoredValue, true
	}
	addUint64 := func(knowledgeValue, authoredValue, maximum uint64) (uint64, bool) {
		if knowledgeValue > maximum || authoredValue > maximum-knowledgeValue {
			return 0, false
		}
		return knowledgeValue + authoredValue, true
	}
	authored := evidence.authored
	var totals trustedCompilerTotals
	var ok bool
	if totals.regexPrograms, ok = addUint32(
		knowledge.RegexPrograms,
		authored.regexPrograms,
		MaximumRegexPrograms,
	); !ok {
		return resource()
	}
	if totals.regexWorkUnits, ok = addUint64(
		knowledge.RegexWorkUnits,
		authored.regexWorkUnits,
		MaximumRegexWorkUnits,
	); !ok {
		return resource()
	}
	if totals.extractionOutputs, ok = addUint32(
		knowledge.ExtractionOutputs,
		authored.extractionOutputs,
		MaximumExtractionOutputs,
	); !ok {
		return resource()
	}
	if totals.jsonEvaluationWork, ok = addUint32(
		knowledge.JSONEvaluationWork,
		authored.jsonEvaluationWork,
		MaximumJSONEvaluationWorkUnits,
	); !ok {
		return resource()
	}
	if totals.scalarPredicates, ok = addUint32(
		knowledge.ScalarPredicates,
		authored.scalarPredicates,
		MaximumScalarPredicates,
	); !ok {
		return resource()
	}

	if authored.regexPrograms == 0 {
		if authored.regexWorkUnits != 0 {
			return invalid("zero authored regex programs require zero work")
		}
	} else if authored.regexWorkUnits < uint64(authored.regexPrograms) ||
		authored.regexWorkUnits > uint64(authored.regexPrograms)*
			splregex.MaximumExtractionProgramWorkUnits ||
		authored.extractionOutputs < authored.regexPrograms {
		return invalid("authored regex compiler evidence is incoherent")
	}
	if totals.regexPrograms == 0 {
		if totals.regexWorkUnits != 0 || evidence.regexCaptureBytes != 0 {
			return invalid("zero regex programs require zero work and capture bytes")
		}
	} else if totals.regexWorkUnits < uint64(totals.regexPrograms) ||
		totals.regexWorkUnits > uint64(totals.regexPrograms)*
			splregex.MaximumExtractionProgramWorkUnits ||
		totals.extractionOutputs < totals.regexPrograms ||
		evidence.regexCaptureBytes != MaximumRegexCaptureBytes {
		return invalid("regex compiler evidence is incoherent")
	}
	if knowledge.ScalarExpressions == 0 && knowledge.ScalarExpressionNodes != 0 ||
		knowledge.ScalarExpressionNodes < knowledge.ScalarExpressions {
		return invalid("scalar compiler evidence is incoherent")
	}
	return totals, nil
}

func digestSnapshot(
	snapshot *opensplunkv1.KnowledgeSnapshot,
	prelude knowledgeprogram.Program,
) (Snapshot, error) {
	if prelude.IsZero() {
		return Snapshot{}, fmt.Errorf("%w: finalized knowledge prelude is absent", ErrInvalidInput)
	}
	marshal := proto.MarshalOptions{Deterministic: true}
	withoutCharge, err := marshal.Marshal(snapshot)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: deterministic snapshot marshal: %w", ErrInvalidInput, err)
	}
	if len(withoutCharge) > MaximumCanonicalBytes {
		return Snapshot{}, fmt.Errorf("%w: canonical snapshot bytes exceed %d", ErrResourceLimit, MaximumCanonicalBytes)
	}
	snapshot.BudgetCharges.CanonicalSnapshotBytes = uint64(len(withoutCharge))
	digestInput, err := marshal.Marshal(snapshot)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: deterministic digest marshal: %w", ErrInvalidInput, err)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(digestDomain))
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(digestInput)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(digestInput)
	digestBytes := hash.Sum(nil)
	snapshot.SnapshotSha256 = bytes.Clone(digestBytes)
	encoded, err := marshal.Marshal(snapshot)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: final deterministic snapshot marshal: %w", ErrInvalidInput, err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], digestBytes)
	result := Snapshot{
		message: snapshot,
		encoded: bytes.Clone(encoded),
		digest:  digest,
		prelude: prelude.Clone(),
	}
	facts, err := mintRetainedExecutionAuthorityFacts(result)
	if err != nil {
		return Snapshot{}, err
	}
	result.retainedExecutionFacts = facts
	return result, nil
}

func stageForObjectType(objectType opensplunkv1.KnowledgeObjectType) (opensplunkv1.KnowledgeSearchStage, uint8, error) {
	switch objectType {
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION, 1, nil
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS, 2, nil
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD, 3, nil
	default:
		return 0, 0, fmt.Errorf("unsupported object type %d", objectType)
	}
}

func precedenceRank(scope opensplunkv1.SharingScope) uint8 {
	switch scope {
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE:
		return 3
	case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
		return 2
	case opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
		return 1
	default:
		return 0
	}
}

func authorizedIdentity(input Input, appID, ownerID string, scope opensplunkv1.SharingScope) bool {
	switch scope {
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE:
		return appID == input.AppID && ownerID == input.PrincipalID
	case opensplunkv1.SharingScope_SHARING_SCOPE_APP:
		return appID == input.AppID
	case opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
		return true
	default:
		return false
	}
}

func validIdentity(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) ||
		trimASCIIWhitespace(value) != value || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if character <= 0x1f || character >= 0x7f && character <= 0x9f {
			return false
		}
	}
	return true
}

func trimASCIIWhitespace(value string) string {
	return strings.TrimFunc(value, func(character rune) bool {
		return character == ' ' || character >= '\t' && character <= '\r'
	})
}

func canonicalAuthorityObject(object *canonicalObject) Object {
	return Object{
		KnowledgeObjectID: strings.Clone(object.input.KnowledgeObjectID),
		Version:           object.input.Version,
		ObjectType:        object.input.ObjectType,
		Name:              strings.Clone(object.input.Name),
		AppID:             strings.Clone(object.input.AppID),
		OwnerID:           strings.Clone(object.input.OwnerID),
		SharingScope:      object.input.SharingScope,
		Definition:        object.normalized.Definition,
		DefinitionSHA256:  bytes.Clone(object.input.DefinitionSHA256),
	}
}

func versionReference(object *canonicalObject) *opensplunkv1.KnowledgeObjectVersionReference {
	return &opensplunkv1.KnowledgeObjectVersionReference{
		KnowledgeObjectId: strings.Clone(object.input.KnowledgeObjectID),
		Version:           object.input.Version,
		DefinitionSha256:  bytes.Clone(object.input.DefinitionSHA256),
	}
}

func cloneObjects(objects []Object) []Object {
	cloned := make([]Object, len(objects))
	for index, object := range objects {
		cloned[index] = object
		cloned[index].KnowledgeObjectID = strings.Clone(object.KnowledgeObjectID)
		cloned[index].Name = strings.Clone(object.Name)
		cloned[index].AppID = strings.Clone(object.AppID)
		cloned[index].OwnerID = strings.Clone(object.OwnerID)
		cloned[index].DefinitionSHA256 = bytes.Clone(object.DefinitionSHA256)
		if object.Definition != nil {
			cloned[index].Definition, _ = proto.Clone(object.Definition).(*opensplunkv1.KnowledgeObjectDefinition)
		}
	}
	return cloned
}

func cloneShadows(shadows []Shadow) []Shadow {
	cloned := make([]Shadow, len(shadows))
	for index, shadow := range shadows {
		cloned[index] = cloneShadow(shadow)
	}
	return cloned
}

func shadowAuthorities(shadows []Shadow) []Shadow {
	cloned := make([]Shadow, len(shadows))
	for index, shadow := range shadows {
		cloned[index] = cloneShadow(shadow)
		cloned[index].Definition = nil
	}
	return cloned
}

func cloneShadow(shadow Shadow) Shadow {
	cloned := shadow
	cloned.WinnerObjectID = strings.Clone(shadow.WinnerObjectID)
	cloned.KnowledgeObjectID = strings.Clone(shadow.KnowledgeObjectID)
	cloned.Name = strings.Clone(shadow.Name)
	cloned.AppID = strings.Clone(shadow.AppID)
	cloned.OwnerID = strings.Clone(shadow.OwnerID)
	cloned.DefinitionSHA256 = bytes.Clone(shadow.DefinitionSHA256)
	if shadow.Definition != nil {
		cloned.Definition, _ = proto.Clone(shadow.Definition).(*opensplunkv1.KnowledgeObjectDefinition)
	}
	return cloned
}

func cloneDependency(dependency Dependency) Dependency {
	dependency.SourceObjectID = strings.Clone(dependency.SourceObjectID)
	dependency.TargetObjectID = strings.Clone(dependency.TargetObjectID)
	return dependency
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
