// Package knowledgeprogram owns the backend-neutral, immutable executable
// meaning of one resolved knowledge-object prelude. It deliberately does not
// import plan, clickhouse, catalog, snapshot, or search lifecycle packages.
package knowledgeprogram

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	MaximumObjects               = 256
	MaximumDependencies          = 1024
	MaximumDefinitionBytes       = 4 << 20
	MaximumGeneratedFields       = 512
	MaximumRegexPrograms         = 64
	MaximumRegexWorkUnits        = MaximumRegexPrograms * splregex.MaximumExtractionProgramWorkUnits
	MaximumExtractionOutputs     = 64
	MaximumJSONEvaluationWork    = splpath.MaximumEvaluationWorkUnits
	MaximumScalarExpressions     = 32
	MaximumScalarExpressionNodes = MaximumScalarExpressions * 1024
	MaximumScalarPredicates      = 32
	MaximumDependencyDepth       = 16
	commitmentDomain             = "open-splunk-knowledge-prelude-v0.1\x00"
	retainedObjectCharge         = 32 << 10
	retainedDependencyCharge     = 256
	retainedOperatorSlotCharge   = 64
)

var (
	ErrInvalidProgram = errors.New("invalid knowledge program")
	ErrResourceLimit  = errors.New("knowledge program exceeds a resource limit")
)

// Input is the executable subset of a canonical KnowledgeSnapshot. Shadows
// and warnings are intentionally absent because they cannot affect execution.
type Input struct {
	Objects      []*opensplunkv1.KnowledgeSnapshotObject
	Dependencies []*opensplunkv1.KnowledgeObjectDependency
}

// Charges are exact compiler-independent contributions of the program.
type Charges struct {
	GeneratedOperators    uint32
	GeneratedFields       uint32
	RegexPrograms         uint32
	RegexWorkUnits        uint64
	ExtractionOutputs     uint32
	JSONEvaluationWork    uint32
	ScalarExpressions     uint32
	ScalarExpressionNodes uint32
	ScalarPredicates      uint32
}

// OverwriteBehavior is the closed merge policy after a selected object
// produces a present value.
type OverwriteBehavior uint8

const (
	OverwriteInvalid OverwriteBehavior = iota
	PreserveExisting
	ReplaceExisting
)

// Origin is immutable private provenance for one generated operation.
type Origin struct {
	resolutionOrdinal uint32
	stageOrdinal      uint32
	objectID          string
	version           uint64
	objectType        opensplunkv1.KnowledgeObjectType
	name              string
	appID             string
	ownerID           string
	sharingScope      opensplunkv1.SharingScope
	stage             opensplunkv1.KnowledgeSearchStage
	definitionDigest  [sha256.Size]byte
	location          string
}

func (origin Origin) ResolutionOrdinal() uint32 { return origin.resolutionOrdinal }
func (origin Origin) StageOrdinal() uint32      { return origin.stageOrdinal }
func (origin Origin) ObjectID() string          { return strings.Clone(origin.objectID) }
func (origin Origin) Version() uint64           { return origin.version }
func (origin Origin) ObjectType() opensplunkv1.KnowledgeObjectType {
	return origin.objectType
}
func (origin Origin) Name() string                            { return strings.Clone(origin.name) }
func (origin Origin) AppID() string                           { return strings.Clone(origin.appID) }
func (origin Origin) OwnerID() string                         { return strings.Clone(origin.ownerID) }
func (origin Origin) SharingScope() opensplunkv1.SharingScope { return origin.sharingScope }
func (origin Origin) Stage() opensplunkv1.KnowledgeSearchStage {
	return origin.stage
}
func (origin Origin) DefinitionDigest() [sha256.Size]byte { return origin.definitionDigest }
func (origin Origin) DefinitionLocation() string          { return strings.Clone(origin.location) }

// Selector is an immutable compiler-facing selector. Its underlying Selector
// is itself immutable and race-safe; every exposed slice remains detached.
type Selector struct{ compiled *knowledge.Selector }

func (selector Selector) CanonicalBytes() []byte {
	if selector.compiled == nil {
		return nil
	}
	return selector.compiled.CanonicalBytes()
}

func (selector Selector) RuntimeProgram(dimension knowledge.Dimension) (knowledge.DimensionRuntimeProgram, bool) {
	if selector.compiled == nil {
		return knowledge.DimensionRuntimeProgram{}, false
	}
	return selector.compiled.RuntimeProgram(dimension)
}

func (selector Selector) IsUnrestricted() bool {
	return selector.compiled != nil && selector.compiled.Stats().Dimensions == 0
}

// ProvablyDisjoint reports whether the immutable selectors are conservatively
// proven never to match the same event. The proof is intentionally exposed as
// a boolean instead of leaking the mutable compiler representation.
func (selector Selector) ProvablyDisjoint(other Selector) bool {
	return knowledge.SelectorsProvablyDisjoint(selector.compiled, other.compiled)
}

// Capture is one regex output and its one-based group.
type Capture struct {
	name     string
	group    uint16
	location string
}

func (capture Capture) Name() string  { return strings.Clone(capture.name) }
func (capture Capture) Group() uint16 { return capture.group }
func (capture Capture) DefinitionLocation() string {
	return strings.Clone(capture.location)
}

type RegexExtraction struct {
	origin    Origin
	selector  Selector
	overwrite OverwriteBehavior
	input     string
	pattern   string
	captures  []Capture
	workUnits uint64
}

func (operation RegexExtraction) Origin() Origin               { return operation.origin }
func (operation RegexExtraction) Selector() Selector           { return operation.selector }
func (operation RegexExtraction) Overwrite() OverwriteBehavior { return operation.overwrite }
func (operation RegexExtraction) Input() string                { return strings.Clone(operation.input) }
func (operation RegexExtraction) Pattern() string              { return strings.Clone(operation.pattern) }
func (operation RegexExtraction) Captures() []Capture          { return slices.Clone(operation.captures) }
func (operation RegexExtraction) ProgramWorkUnits() uint64     { return operation.workUnits }

type JSONExtraction struct {
	origin         Origin
	selector       Selector
	overwrite      OverwriteBehavior
	input          string
	path           string
	steps          []splpath.Step
	output         string
	outputLocation string
	workUnits      uint32
}

func (operation JSONExtraction) Origin() Origin               { return operation.origin }
func (operation JSONExtraction) Selector() Selector           { return operation.selector }
func (operation JSONExtraction) Overwrite() OverwriteBehavior { return operation.overwrite }
func (operation JSONExtraction) Input() string                { return strings.Clone(operation.input) }
func (operation JSONExtraction) Path() string                 { return strings.Clone(operation.path) }
func (operation JSONExtraction) Steps() []splpath.Step        { return slices.Clone(operation.steps) }
func (operation JSONExtraction) Output() string               { return strings.Clone(operation.output) }
func (operation JSONExtraction) OutputDefinitionLocation() string {
	return strings.Clone(operation.outputLocation)
}
func (operation JSONExtraction) EvaluationWorkUnits() uint32 { return operation.workUnits }

type Alias struct {
	origin      Origin
	selector    Selector
	overwrite   OverwriteBehavior
	source      string
	destination string
}

func (operation Alias) Origin() Origin               { return operation.origin }
func (operation Alias) Selector() Selector           { return operation.selector }
func (operation Alias) Overwrite() OverwriteBehavior { return operation.overwrite }
func (operation Alias) Source() string               { return strings.Clone(operation.source) }
func (operation Alias) Destination() string          { return strings.Clone(operation.destination) }

type Calculated struct {
	origin      Origin
	selector    Selector
	overwrite   OverwriteBehavior
	destination string
	expression  string
	inputFields []string
	nodes       uint32
	predicates  uint32
}

func (operation Calculated) Origin() Origin               { return operation.origin }
func (operation Calculated) Selector() Selector           { return operation.selector }
func (operation Calculated) Overwrite() OverwriteBehavior { return operation.overwrite }
func (operation Calculated) Destination() string          { return strings.Clone(operation.destination) }
func (operation Calculated) Expression() string           { return strings.Clone(operation.expression) }
func (operation Calculated) InputFields() []string        { return slices.Clone(operation.inputFields) }
func (operation Calculated) Nodes() uint32                { return operation.nodes }
func (operation Calculated) Predicates() uint32           { return operation.predicates }

// OperatorKind is one occurrence in the canonical pre-optimization prelude.
// Alias and calculated stages are each fused into at most one occurrence.
type OperatorKind uint8

const (
	OperatorInvalid OperatorKind = iota
	OperatorConditionalExtract
	OperatorConditionalExtractJSON
	OperatorCopyFieldAlias
	OperatorParallelExtend
)

type programState struct {
	regex         []RegexExtraction
	json          []JSONExtraction
	aliases       []Alias
	calculated    []Calculated
	operatorKinds []OperatorKind
	dependencies  []*opensplunkv1.KnowledgeObjectDependency
	charges       Charges
	canonical     []byte
	commitment    [sha256.Size]byte
	objectCount   uint32
}

// Program is a pointer-backed immutable authority. Its zero value is absent;
// Compile or Prepare with an empty input returns a present, valid empty program.
type Program struct{ state *programState }

func (program Program) IsZero() bool { return program.state == nil }

func (program Program) IsEmpty() bool { return program.state != nil && program.state.objectCount == 0 }

func (program Program) ObjectCount() uint32 {
	if program.state == nil {
		return 0
	}
	return program.state.objectCount
}

func (program Program) Commitment() ([sha256.Size]byte, bool) {
	if program.state == nil {
		return [sha256.Size]byte{}, false
	}
	return program.state.commitment, true
}

func (program Program) Charges() Charges {
	if program.state == nil {
		return Charges{}
	}
	return program.state.charges
}

func (program Program) RegexExtractions() []RegexExtraction {
	if program.state == nil {
		return nil
	}
	return cloneRegex(program.state.regex)
}

func (program Program) JSONExtractions() []JSONExtraction {
	if program.state == nil {
		return nil
	}
	return cloneJSON(program.state.json)
}

func (program Program) Aliases() []Alias {
	if program.state == nil {
		return nil
	}
	return slices.Clone(program.state.aliases)
}

func (program Program) CalculatedFields() []Calculated {
	if program.state == nil {
		return nil
	}
	return cloneCalculated(program.state.calculated)
}

// OperatorKinds returns the exact generated operator order. Individual
// extraction objects retain canonical stage/name/ID order even when regex and
// JSON definitions are interleaved.
func (program Program) OperatorKinds() []OperatorKind {
	if program.state == nil {
		return nil
	}
	return slices.Clone(program.state.operatorKinds)
}

func (program Program) Dependencies() []*opensplunkv1.KnowledgeObjectDependency {
	if program.state == nil {
		return nil
	}
	return cloneDependencies(program.state.dependencies)
}

func (program Program) Clone() Program {
	if program.state == nil {
		return Program{}
	}
	state := *program.state
	state.regex = cloneRegex(state.regex)
	state.json = cloneJSON(state.json)
	state.aliases = slices.Clone(state.aliases)
	state.calculated = cloneCalculated(state.calculated)
	state.operatorKinds = slices.Clone(state.operatorKinds)
	state.dependencies = cloneDependencies(state.dependencies)
	state.canonical = bytes.Clone(state.canonical)
	return Program{state: &state}
}

func (program Program) Equal(other Program) bool {
	if program.state == nil || other.state == nil {
		return program.state == nil && other.state == nil
	}
	return bytes.Equal(program.state.canonical, other.state.canonical)
}

// RetainedBytes returns a conservative heap charge for the typed immutable
// program, its canonical commitment input, selectors, and dependency graph.
// The per-object reservation deliberately covers the compiled selector/NFA
// authority which is not reproduced byte-for-byte in canonical.
func (program Program) RetainedBytes() uint64 {
	if program.state == nil {
		return 0
	}
	return uint64(cap(program.state.canonical)) + uint64(len(program.state.canonical)) +
		uint64(program.state.objectCount)*retainedObjectCharge +
		uint64(len(program.state.dependencies))*retainedDependencyCharge +
		uint64(cap(program.state.operatorKinds))*retainedOperatorSlotCharge
}

// Compile revalidates one complete canonical winner set, independently derives
// its exact FIELD_INPUT graph, and returns one immutable semantic program. It
// is the publication-facing boundary: callers provide object authorities, not
// dependency authority. Every returned dependency is version- and
// definition-digest-pinned and is available through Program.Dependencies.
func Compile(objects []*opensplunkv1.KnowledgeSnapshotObject) (Program, error) {
	state, _, err := compileObjects(objects)
	if err != nil {
		return Program{}, err
	}
	dependencies, err := deriveCanonicalDependencies(state)
	if err != nil {
		return Program{}, err
	}
	return sealProgram(state, dependencies), nil
}

// Prepare revalidates canonical snapshot objects and a separately submitted
// dependency authority before creating one immutable semantic program. It
// derives the graph through the same compiler as Compile and requires exact
// protobuf equality, so persisted or detached dependencies remain an
// independently checked authority rather than input to semantic derivation.
func Prepare(input Input) (Program, error) {
	if len(input.Dependencies) > MaximumDependencies {
		return Program{}, fmt.Errorf("%w: more than %d dependencies", ErrResourceLimit, MaximumDependencies)
	}
	state, objects, err := compileObjects(input.Objects)
	if err != nil {
		return Program{}, err
	}
	dependencies, err := prepareDependencies(input.Dependencies, objects)
	if err != nil {
		return Program{}, err
	}
	if err := validateProgramSemantics(state, dependencies); err != nil {
		return Program{}, err
	}
	return sealProgram(state, dependencies), nil
}

func compileObjects(
	input []*opensplunkv1.KnowledgeSnapshotObject,
) (*programState, map[string]*opensplunkv1.KnowledgeSnapshotObject, error) {
	if len(input) > MaximumObjects {
		return nil, nil, fmt.Errorf("%w: more than %d objects", ErrResourceLimit, MaximumObjects)
	}
	state := &programState{}
	state.objectCount = uint32(len(input))
	stageOrdinals := map[opensplunkv1.KnowledgeSearchStage]uint32{}
	var previous *opensplunkv1.KnowledgeSnapshotObject
	objects := make(map[string]*opensplunkv1.KnowledgeSnapshotObject, len(input))
	names := make(map[string]struct{}, len(input))
	var definitionBytes uint64
	var selectorWork uint64
	for index, object := range input {
		// Definition normalization performs its own repeated-shape/size preflight
		// before recursive unknown-field inspection. Do not walk that definition
		// here first and reintroduce malformed-input amplification.
		if object == nil || len(object.ProtoReflect().GetUnknown()) != 0 {
			return nil, nil, invalid(index, "object is nil or contains unknown fields")
		}
		if object.GetResolutionOrdinal() != uint32(index) || object.GetVersion() == 0 ||
			object.GetVersion() > math.MaxInt64 || !validIdentity(object.GetKnowledgeObjectId(), 128) ||
			!validIdentity(object.GetAppId(), 128) || !validIdentity(object.GetOwnerId(), 255) ||
			len(object.GetDefinitionSha256()) != sha256.Size {
			return nil, nil, invalid(index, "object authority is invalid")
		}
		expectedStage, _, err := stageForType(object.GetObjectType())
		if err != nil || object.GetStage() != expectedStage ||
			object.GetStageOrdinal() != stageOrdinals[expectedStage] {
			return nil, nil, invalid(index, "stage or stage ordinal is invalid")
		}
		stageOrdinals[expectedStage]++
		if previous != nil && !objectAfter(previous, object) {
			return nil, nil, invalid(index, "objects are not in canonical stage/name/ID order")
		}
		if _, duplicate := objects[object.GetKnowledgeObjectId()]; duplicate {
			return nil, nil, invalid(index, "object ID is duplicated")
		}
		nameKey := fmt.Sprintf("%d\x00%s", object.GetObjectType(), object.GetName())
		if _, duplicate := names[nameKey]; duplicate {
			return nil, nil, invalid(index, "resolved object name is ambiguous")
		}
		normalized, err := knowledgedefinition.Normalize(object.GetDefinition())
		if err != nil || normalized.ObjectType != object.GetObjectType() ||
			normalized.Name != object.GetName() || normalized.AppID != object.GetAppId() ||
			normalized.SharingScope != object.GetSharingScope() ||
			!bytes.Equal(normalized.Digest[:], object.GetDefinitionSha256()) ||
			!proto.Equal(normalized.Definition, object.GetDefinition()) {
			return nil, nil, invalid(index, "definition authority disagrees")
		}
		if uint64(len(normalized.Bytes)) > MaximumDefinitionBytes-definitionBytes {
			return nil, nil, fmt.Errorf("%w: canonical definitions exceed %d bytes", ErrResourceLimit, MaximumDefinitionBytes)
		}
		definitionBytes += uint64(len(normalized.Bytes))
		work := normalized.Selector.Stats().WildcardWorkUnits
		if work > knowledge.MaximumSelectorWildcardWorkUnits-selectorWork {
			return nil, nil, fmt.Errorf("%w: selector work exceeds %d", ErrResourceLimit, knowledge.MaximumSelectorWildcardWorkUnits)
		}
		selectorWork += work
		canonicalObject := &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: object.GetResolutionOrdinal(),
			Stage:             object.GetStage(),
			StageOrdinal:      object.GetStageOrdinal(),
			KnowledgeObjectId: strings.Clone(object.GetKnowledgeObjectId()),
			Version:           object.GetVersion(),
			ObjectType:        object.GetObjectType(),
			Name:              strings.Clone(object.GetName()),
			AppId:             strings.Clone(object.GetAppId()),
			OwnerId:           strings.Clone(object.GetOwnerId()),
			SharingScope:      object.GetSharingScope(),
			Definition:        normalized.Definition,
			DefinitionSha256:  bytes.Clone(normalized.Digest[:]),
		}
		origin := originFor(canonicalObject, expectedStage, locationForType(object.GetObjectType()))
		selector := Selector{compiled: normalized.Selector}
		if err := appendObject(state, normalized, origin, selector); err != nil {
			return nil, nil, invalid(index, err.Error())
		}
		objects[canonicalObject.GetKnowledgeObjectId()] = canonicalObject
		names[nameKey] = struct{}{}
		previous = canonicalObject
	}
	if len(state.aliases) > 0 {
		state.charges.GeneratedOperators++
		state.operatorKinds = append(state.operatorKinds, OperatorCopyFieldAlias)
	}
	if len(state.calculated) > 0 {
		state.charges.GeneratedOperators++
		state.operatorKinds = append(state.operatorKinds, OperatorParallelExtend)
	}
	if state.charges.GeneratedOperators > MaximumObjects {
		return nil, nil, fmt.Errorf("%w: more than %d generated operators", ErrResourceLimit, MaximumObjects)
	}
	if err := validateCharges(state.charges); err != nil {
		return nil, nil, err
	}
	return state, objects, nil
}

func sealProgram(
	state *programState,
	dependencies []*opensplunkv1.KnowledgeObjectDependency,
) Program {
	state.dependencies = dependencies
	state.canonical = canonicalProgram(state)
	state.commitment = sha256.Sum256(state.canonical)
	return Program{state: state}
}

func appendObject(state *programState, normalized knowledgedefinition.Normalized, origin Origin, selector Selector) error {
	switch body := normalized.Definition.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		if body == nil || body.FieldExtraction == nil {
			return errors.New("field extraction is nil")
		}
		overwrite, err := overwriteBehavior(body.FieldExtraction.GetOverwriteBehavior())
		if err != nil {
			return err
		}
		switch extraction := body.FieldExtraction.GetExtraction().(type) {
		case *opensplunkv1.FieldExtractionDefinition_Regex:
			compiled, err := splregex.CompileExtractionPattern(extraction.Regex.GetPattern())
			if err != nil || compiled.GroupCount != len(compiled.Captures) ||
				len(compiled.Captures) != len(extraction.Regex.GetOutputFields()) {
				return errors.New("regex extraction is not executable")
			}
			captures := make([]Capture, len(compiled.Captures))
			for index, capture := range compiled.Captures {
				if capture.Name != extraction.Regex.GetOutputFields()[index] || capture.Group <= 0 || capture.Group > math.MaxUint16 {
					return errors.New("regex captures disagree with declared outputs")
				}
				captures[index] = Capture{
					name:     capture.Name,
					group:    uint16(capture.Group),
					location: fmt.Sprintf("field_extraction.regex.output_fields[%d]", index),
				}
			}
			origin.location = "field_extraction.regex.pattern"
			state.regex = append(state.regex, RegexExtraction{origin: origin, selector: selector, overwrite: overwrite, input: body.FieldExtraction.GetInputField(), pattern: compiled.Pattern, captures: captures, workUnits: uint64(compiled.ProgramWorkUnits)})
			state.operatorKinds = append(state.operatorKinds, OperatorConditionalExtract)
			state.charges.GeneratedOperators++
			state.charges.GeneratedFields += uint32(len(captures))
			state.charges.RegexPrograms++
			state.charges.RegexWorkUnits += uint64(compiled.ProgramWorkUnits)
			state.charges.ExtractionOutputs += uint32(len(captures))
		case *opensplunkv1.FieldExtractionDefinition_Json:
			steps, err := splpath.ParseJSON(extraction.Json.GetPath())
			if err != nil {
				return errors.New("JSON extraction is not executable")
			}
			origin.location = "field_extraction.json.path"
			workUnits := uint32(splpath.EvaluationWorkUnits(steps))
			state.json = append(state.json, JSONExtraction{
				origin:         origin,
				selector:       selector,
				overwrite:      overwrite,
				input:          body.FieldExtraction.GetInputField(),
				path:           extraction.Json.GetPath(),
				steps:          slices.Clone(steps),
				output:         extraction.Json.GetOutputField(),
				outputLocation: "field_extraction.json.output_field",
				workUnits:      workUnits,
			})
			state.operatorKinds = append(state.operatorKinds, OperatorConditionalExtractJSON)
			state.charges.GeneratedOperators++
			state.charges.GeneratedFields++
			state.charges.ExtractionOutputs++
			state.charges.JSONEvaluationWork += workUnits
		default:
			return errors.New("field extraction kind is unsupported")
		}
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		overwrite, err := overwriteBehavior(body.FieldAlias.GetOverwriteBehavior())
		if err != nil {
			return err
		}
		origin.location = "field_alias.destination_field"
		state.aliases = append(state.aliases, Alias{origin: origin, selector: selector, overwrite: overwrite, source: body.FieldAlias.GetSourceField(), destination: body.FieldAlias.GetDestinationField()})
		state.charges.GeneratedFields++
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		overwrite, err := overwriteBehavior(body.CalculatedField.GetOverwriteBehavior())
		if err != nil {
			return err
		}
		parsed, err := spl.ParseScalarExpression(body.CalculatedField.GetExpression())
		if err != nil {
			return errors.New("calculated expression is not executable")
		}
		if spl.ScalarExpressionMayReturnBooleanFunction(parsed) {
			return errors.New("calculated expression cannot directly assign a Boolean function result")
		}
		analysis, err := spl.AnalyzeScalarExpression(parsed)
		if err != nil || analysis.Nodes == 0 {
			return errors.New("calculated expression analysis failed")
		}
		origin.location = "calculated_field.expression"
		state.calculated = append(state.calculated, Calculated{origin: origin, selector: selector, overwrite: overwrite, destination: body.CalculatedField.GetDestinationField(), expression: body.CalculatedField.GetExpression(), inputFields: slices.Clone(analysis.InputFields), nodes: analysis.Nodes, predicates: analysis.Predicates})
		state.charges.GeneratedFields++
		state.charges.ScalarExpressions++
		state.charges.ScalarExpressionNodes += analysis.Nodes
		state.charges.ScalarPredicates += analysis.Predicates
	default:
		return errors.New("definition body is unsupported")
	}
	return nil
}

func validateCharges(charges Charges) error {
	if charges.GeneratedOperators > MaximumObjects ||
		charges.GeneratedFields > MaximumGeneratedFields ||
		charges.RegexPrograms > MaximumRegexPrograms ||
		charges.RegexWorkUnits > MaximumRegexWorkUnits ||
		charges.ExtractionOutputs > MaximumExtractionOutputs ||
		charges.JSONEvaluationWork > MaximumJSONEvaluationWork ||
		charges.ScalarExpressions > MaximumScalarExpressions ||
		charges.ScalarExpressionNodes > MaximumScalarExpressionNodes ||
		charges.ScalarPredicates > MaximumScalarPredicates {
		return fmt.Errorf("%w: aggregate semantic charges exceed compatibility limits", ErrResourceLimit)
	}
	return nil
}

func prepareDependencies(input []*opensplunkv1.KnowledgeObjectDependency, objects map[string]*opensplunkv1.KnowledgeSnapshotObject) ([]*opensplunkv1.KnowledgeObjectDependency, error) {
	result := make([]*opensplunkv1.KnowledgeObjectDependency, len(input))
	seen := make(map[string]struct{}, len(input))
	var previous *opensplunkv1.KnowledgeObjectDependency
	for index, dependency := range input {
		if dependency == nil || hasUnknown(dependency.ProtoReflect()) || dependency.GetCanonicalOrdinal() != uint32(index) ||
			dependency.GetRole() != opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT ||
			dependency.GetTopologicalDepth() > MaximumDependencyDepth {
			return nil, fmt.Errorf("%w: dependency %d authority is invalid", ErrInvalidProgram, index)
		}
		source := dependency.GetSource()
		target := dependency.GetTarget().GetObject()
		sourceObject := objects[source.GetKnowledgeObjectId()]
		targetObject := objects[target.GetKnowledgeObjectId()]
		if sourceObject == nil || targetObject == nil || source.GetVersion() != sourceObject.GetVersion() || target.GetVersion() != targetObject.GetVersion() ||
			!bytes.Equal(source.GetDefinitionSha256(), sourceObject.GetDefinitionSha256()) ||
			!bytes.Equal(target.GetDefinitionSha256(), targetObject.GetDefinitionSha256()) ||
			dependency.GetSourceStage() != sourceObject.GetStage() || dependency.GetTargetStage() != targetObject.GetStage() ||
			dependency.GetSourceStage() <= dependency.GetTargetStage() {
			return nil, fmt.Errorf("%w: dependency %d endpoints are invalid", ErrInvalidProgram, index)
		}
		key := fmt.Sprintf("%s\x00%d\x00%s\x00%d\x00%d", source.GetKnowledgeObjectId(), source.GetVersion(), target.GetKnowledgeObjectId(), target.GetVersion(), dependency.GetRole())
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("%w: dependency %d is duplicated", ErrInvalidProgram, index)
		}
		if previous != nil && !dependencyAfter(previous, dependency) {
			return nil, fmt.Errorf("%w: dependency %d is not in canonical order", ErrInvalidProgram, index)
		}
		cloned, ok := proto.Clone(dependency).(*opensplunkv1.KnowledgeObjectDependency)
		if !ok || cloned == nil {
			return nil, fmt.Errorf("%w: dependency %d cannot be detached", ErrInvalidProgram, index)
		}
		result[index] = cloned
		seen[key] = struct{}{}
		previous = cloned
	}
	return result, nil
}

func canonicalProgram(state *programState) []byte {
	var buffer bytes.Buffer
	buffer.WriteString(commitmentDomain)
	writeUint64(&buffer, uint64(len(state.operatorKinds)))
	regexIndex, jsonIndex := 0, 0
	for _, kind := range state.operatorKinds {
		writeUint64(&buffer, uint64(kind))
		switch kind {
		case OperatorConditionalExtract:
			operation := state.regex[regexIndex]
			regexIndex++
			writeOrigin(&buffer, operation.origin)
			writeSelector(&buffer, operation.selector)
			writeUint64(&buffer, uint64(operation.overwrite))
			writeString(&buffer, operation.input)
			writeString(&buffer, operation.pattern)
			writeUint64(&buffer, operation.workUnits)
			writeUint64(&buffer, uint64(len(operation.captures)))
			for _, capture := range operation.captures {
				writeString(&buffer, capture.name)
				writeUint64(&buffer, uint64(capture.group))
				writeString(&buffer, capture.location)
			}
		case OperatorConditionalExtractJSON:
			operation := state.json[jsonIndex]
			jsonIndex++
			writeOrigin(&buffer, operation.origin)
			writeSelector(&buffer, operation.selector)
			writeUint64(&buffer, uint64(operation.overwrite))
			writeString(&buffer, operation.input)
			writeString(&buffer, operation.path)
			writeUint64(&buffer, uint64(operation.workUnits))
			writeString(&buffer, operation.output)
			writeString(&buffer, operation.outputLocation)
			writeUint64(&buffer, uint64(len(operation.steps)))
			for _, step := range operation.steps {
				writeString(&buffer, step.Key)
				writeBool(&buffer, step.HasIndex)
				writeUint64(&buffer, step.Index)
			}
		case OperatorCopyFieldAlias:
			writeUint64(&buffer, uint64(len(state.aliases)))
			for _, operation := range state.aliases {
				writeOrigin(&buffer, operation.origin)
				writeSelector(&buffer, operation.selector)
				writeUint64(&buffer, uint64(operation.overwrite))
				writeString(&buffer, operation.source)
				writeString(&buffer, operation.destination)
			}
		case OperatorParallelExtend:
			writeUint64(&buffer, uint64(len(state.calculated)))
			for _, operation := range state.calculated {
				writeOrigin(&buffer, operation.origin)
				writeSelector(&buffer, operation.selector)
				writeUint64(&buffer, uint64(operation.overwrite))
				writeString(&buffer, operation.destination)
				writeString(&buffer, operation.expression)
				writeStringSlice(&buffer, operation.inputFields)
				writeUint64(&buffer, uint64(operation.nodes))
				writeUint64(&buffer, uint64(operation.predicates))
			}
		}
	}
	writeUint64(&buffer, uint64(len(state.dependencies)))
	for _, dependency := range state.dependencies {
		writeDependency(&buffer, dependency)
	}
	writeCharges(&buffer, state.charges)
	return buffer.Bytes()
}

func writeOrigin(buffer *bytes.Buffer, origin Origin) {
	writeUint64(buffer, uint64(origin.resolutionOrdinal))
	writeUint64(buffer, uint64(origin.stageOrdinal))
	writeString(buffer, origin.objectID)
	writeUint64(buffer, origin.version)
	writeUint64(buffer, uint64(origin.objectType))
	writeString(buffer, origin.name)
	writeString(buffer, origin.appID)
	writeString(buffer, origin.ownerID)
	writeUint64(buffer, uint64(origin.sharingScope))
	writeUint64(buffer, uint64(origin.stage))
	writeBytes(buffer, origin.definitionDigest[:])
	writeString(buffer, origin.location)
}

func writeSelector(buffer *bytes.Buffer, selector Selector) {
	writeBytes(buffer, selector.CanonicalBytes())
	for dimension := knowledge.DimensionIndex; dimension <= knowledge.DimensionSourcetype; dimension++ {
		program, constrained := selector.RuntimeProgram(dimension)
		writeBool(buffer, constrained)
		if !constrained {
			continue
		}
		writeStringSlice(buffer, program.ExactLiterals)
		writeString(buffer, program.WildcardRE2)
		writeUint64(buffer, program.Assessment.Initial)
		writeUint64(buffer, program.Assessment.PerInputByte)
		writeUint64(buffer, program.Assessment.Final)
	}
}

func writeDependency(buffer *bytes.Buffer, dependency *opensplunkv1.KnowledgeObjectDependency) {
	writeVersionReference(buffer, dependency.GetSource())
	writeVersionReference(buffer, dependency.GetTarget().GetObject())
	writeUint64(buffer, uint64(dependency.GetRole()))
	writeUint64(buffer, uint64(dependency.GetSourceStage()))
	writeUint64(buffer, uint64(dependency.GetTargetStage()))
	writeUint64(buffer, uint64(dependency.GetTopologicalDepth()))
	writeUint64(buffer, uint64(dependency.GetCanonicalOrdinal()))
}

func writeVersionReference(buffer *bytes.Buffer, reference *opensplunkv1.KnowledgeObjectVersionReference) {
	writeString(buffer, reference.GetKnowledgeObjectId())
	writeUint64(buffer, reference.GetVersion())
	writeBytes(buffer, reference.GetDefinitionSha256())
}

func writeCharges(buffer *bytes.Buffer, charges Charges) {
	writeUint64(buffer, uint64(charges.GeneratedOperators))
	writeUint64(buffer, uint64(charges.GeneratedFields))
	writeUint64(buffer, uint64(charges.RegexPrograms))
	writeUint64(buffer, charges.RegexWorkUnits)
	writeUint64(buffer, uint64(charges.ExtractionOutputs))
	writeUint64(buffer, uint64(charges.JSONEvaluationWork))
	writeUint64(buffer, uint64(charges.ScalarExpressions))
	writeUint64(buffer, uint64(charges.ScalarExpressionNodes))
	writeUint64(buffer, uint64(charges.ScalarPredicates))
}

func writeString(buffer *bytes.Buffer, value string) { writeBytes(buffer, []byte(value)) }
func writeStringSlice(buffer *bytes.Buffer, values []string) {
	writeUint64(buffer, uint64(len(values)))
	for _, value := range values {
		writeString(buffer, value)
	}
}
func writeBool(buffer *bytes.Buffer, value bool) {
	if value {
		_ = buffer.WriteByte(1)
		return
	}
	_ = buffer.WriteByte(0)
}
func writeBytes(buffer *bytes.Buffer, value []byte) {
	writeUint64(buffer, uint64(len(value)))
	_, _ = buffer.Write(value)
}
func writeUint64(buffer *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = buffer.Write(encoded[:])
}

func stageForType(objectType opensplunkv1.KnowledgeObjectType) (opensplunkv1.KnowledgeSearchStage, uint8, error) {
	switch objectType {
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION, 1, nil
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS, 2, nil
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD, 3, nil
	default:
		return 0, 0, errors.New("object type is unsupported")
	}
}

func objectAfter(previous, current *opensplunkv1.KnowledgeSnapshotObject) bool {
	_, previousRank, previousErr := stageForType(previous.GetObjectType())
	_, currentRank, currentErr := stageForType(current.GetObjectType())
	if previousErr != nil || currentErr != nil || previousRank > currentRank {
		return false
	}
	if previousRank < currentRank {
		return true
	}
	if previous.GetName() != current.GetName() {
		return previous.GetName() < current.GetName()
	}
	return previous.GetKnowledgeObjectId() < current.GetKnowledgeObjectId()
}

func dependencyAfter(previous, current *opensplunkv1.KnowledgeObjectDependency) bool {
	if previous.GetTopologicalDepth() != current.GetTopologicalDepth() {
		return previous.GetTopologicalDepth() < current.GetTopologicalDepth()
	}
	if previous.GetSourceStage() != current.GetSourceStage() {
		return previous.GetSourceStage() < current.GetSourceStage()
	}
	previousSource, currentSource := previous.GetSource(), current.GetSource()
	if previousSource.GetKnowledgeObjectId() != currentSource.GetKnowledgeObjectId() {
		return previousSource.GetKnowledgeObjectId() < currentSource.GetKnowledgeObjectId()
	}
	if previousSource.GetVersion() != currentSource.GetVersion() {
		return previousSource.GetVersion() < currentSource.GetVersion()
	}
	previousTarget, currentTarget := previous.GetTarget().GetObject(), current.GetTarget().GetObject()
	if previousTarget.GetKnowledgeObjectId() != currentTarget.GetKnowledgeObjectId() {
		return previousTarget.GetKnowledgeObjectId() < currentTarget.GetKnowledgeObjectId()
	}
	if previousTarget.GetVersion() != currentTarget.GetVersion() {
		return previousTarget.GetVersion() < currentTarget.GetVersion()
	}
	return previous.GetRole() < current.GetRole()
}

func originFor(object *opensplunkv1.KnowledgeSnapshotObject, stage opensplunkv1.KnowledgeSearchStage, location string) Origin {
	var digest [sha256.Size]byte
	copy(digest[:], object.GetDefinitionSha256())
	return Origin{resolutionOrdinal: object.GetResolutionOrdinal(), stageOrdinal: object.GetStageOrdinal(), objectID: strings.Clone(object.GetKnowledgeObjectId()), version: object.GetVersion(), objectType: object.GetObjectType(), name: strings.Clone(object.GetName()), appID: strings.Clone(object.GetAppId()), ownerID: strings.Clone(object.GetOwnerId()), sharingScope: object.GetSharingScope(), stage: stage, definitionDigest: digest, location: location}
}

func locationForType(objectType opensplunkv1.KnowledgeObjectType) string {
	switch objectType {
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return "field_extraction"
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return "field_alias"
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return "calculated_field"
	default:
		return ""
	}
}

func overwriteBehavior(value opensplunkv1.KnowledgeOverwriteBehavior) (OverwriteBehavior, error) {
	switch value {
	case opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING:
		return PreserveExisting, nil
	case opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING:
		return ReplaceExisting, nil
	default:
		return OverwriteInvalid, errors.New("overwrite behavior is invalid")
	}
}

func validIdentity(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) ||
		strings.TrimFunc(value, func(character rune) bool {
			return character == ' ' || character >= '\t' && character <= '\r'
		}) != value || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if character <= 0x1f || character >= 0x7f && character <= 0x9f {
			return false
		}
	}
	return true
}

func cloneRegex(input []RegexExtraction) []RegexExtraction {
	result := slices.Clone(input)
	for index := range result {
		result[index].captures = slices.Clone(result[index].captures)
	}
	return result
}

func cloneJSON(input []JSONExtraction) []JSONExtraction {
	result := slices.Clone(input)
	for index := range result {
		result[index].steps = slices.Clone(result[index].steps)
	}
	return result
}

func cloneCalculated(input []Calculated) []Calculated {
	result := slices.Clone(input)
	for index := range result {
		result[index].inputFields = slices.Clone(result[index].inputFields)
	}
	return result
}

func cloneDependencies(input []*opensplunkv1.KnowledgeObjectDependency) []*opensplunkv1.KnowledgeObjectDependency {
	result := make([]*opensplunkv1.KnowledgeObjectDependency, len(input))
	for index, dependency := range input {
		result[index], _ = proto.Clone(dependency).(*opensplunkv1.KnowledgeObjectDependency)
	}
	return result
}

func hasUnknown(message protoreflect.Message) bool {
	if !message.IsValid() || len(message.GetUnknown()) != 0 {
		return true
	}
	fields := message.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if field.Kind() != protoreflect.MessageKind || !message.Has(field) {
			continue
		}
		if field.IsList() {
			list := message.Get(field).List()
			for position := 0; position < list.Len(); position++ {
				if hasUnknown(list.Get(position).Message()) {
					return true
				}
			}
			continue
		}
		if hasUnknown(message.Get(field).Message()) {
			return true
		}
	}
	return false
}

func invalid(index int, message string) error {
	return fmt.Errorf("%w: object %d %s", ErrInvalidProgram, index, message)
}
